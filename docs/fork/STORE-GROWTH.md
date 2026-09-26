# TODO: bound the LMDB store

**Status: designed, not built. No deadline passed yet; the runway is weeks.**

The store grows monotonically and nothing in the stack removes anything. This is
the plan, with the measurements that decide each choice. Figures are tagged
**[M]** measured, **[C]** read from code, **[E]** estimated.

---

## 1. Where it stands

```
mdb_stat -ef, 2026-09-25 17:37 UTC                                        [M]
  Pages used 25,469,279 x 4096 = 97.2 GiB      Free pages ~195,498 (0.77%)
  Entries 511,718,161                          Tree depth 6
  Branch 610,776   Leaf 20,036,265   Overflow 4,626,738
  Map size 2 TiB (elmdb default; the 906 G filesystem is the real ceiling)
```

- **1,459,788 slots across 343 processes** (max 121,542 on one), confirmed five
  independent ways. **350.5 entries and 69.8 KiB of file per slot.** **[M]**
- Growth: **~38 GiB/day** long-run, ~88 GiB/day loaded, ~0.2 GiB/day idle. **[M]**
- Runway on 419 G free: **~11 days at average, ~5 days loaded.** **[M]/[E]**
- **There is no bloat in the free list.** 195,498 free pages is 0.77%; a
  `mdb_copy -c` recovers ~800 MB and is not worth running. **[M]**

**But the file is only 69.3% content.** Leaf fill is **64.2%** **[M]**, because
the Rust flush worker iterates an `SccHashMap` with `iter_sync`
(`native/elmdb_nif/src/lib.rs:424`) and so writes each 5,000-key batch in **hash
order, not key order**, scattering it across the B-tree. `mdb_copy -c` copies
leaf pages as-is including that slack; only a *sorted reload* recovers it. **[C]**

### Entry census (mutually exclusive) [M]

| class | entries | % | key GiB | value GiB |
|---|---|---|---|---|
| signed assignment chain | 36.50M | 7.1% | 3.1 | 1.1 |
| computed process results | 220.85M | 43.2% | 12.9 | 4.3 |
| `~match@1.0&` reverse index | 140.91M | 27.5% | **13.66** | 0.12 |
| everything else (incl. `data/` blobs 26.5M) | 109.14M | 21.3% | 5.9 | **23.5** |

Two facts drive the plan: **`~match@1.0&` is 40.4% of all key bytes for 0.12 GiB
of values** — the cheapest thing in the store to delete, and a rebuildable index,
not primary data. And **`data/` blobs hold 73.8% of all value bytes** in 5.2% of
entries; their keep/drop split was not measurable by key inspection.

---

## 2. The constraint: there is no delete primitive [C]

| layer | fact |
|---|---|
| `elmdb` NIF | `put, put_batch, get, flush, iterator, foreach, fold, map, list, read_prefix, match`. No `mdb_del`, no `mdb_drop`. |
| Rust NIF | every txn call is `put` or `get`; the only `ffi::` call is `mdb_cursor_open`. |
| `hb_store` behaviour | `start, stop, reset, group, link, type, read, write, list, match, resolve`. No `delete`. |
| `hb_store_lmdb:reset/3` | `rm -Rf` of the whole directory. |
| the overlay | `SccHashMap<Vec<u8>, Vec<u8>>` — the value type cannot express a tombstone. |

Two traps for anyone adding one:

- `hb_store:apply_store_function/5` catches `undef`, but the `undef` path runs
  `retry/6`, which **stops and restarts the store**. A naive optional callback is
  actively harmful; use the `erlang:function_exported` guard already used for
  `start`/`stop`/`scope`.
- `hb_store_volatile` *does* have a reachable prefix delete
  (`maybe_delete_subtree/2`, ETS only, never reaches LMDB). So the behaviour is
  not delete-free, and a future `delete` has one implementation to imitate.

---

## 3. The correctness constraint that outranks everything

`dev_process_cache:materialize/3` recurses into `read(ProcID, Slot - 1, Opts)`:
the delta chain is a strictly backward-linked list. **[C]**

- `dev_process:compute/3` **recovers** from `{error, not_found}` by re-executing
  forward from assignments.
- `dev_process_cache:latest_from_store/4` does `{ok, Msg} = read(...)` — **a hard
  match. A broken chain raises `badmatch`, it does not degrade.** `/now` goes
  through `latest/2` → `latest_from_store/4`. **[C]**

**So a retained slot window must be contiguous and gapless from a full
checkpoint to the head, per process.** A single missing slot inside it makes
every later delta unreadable and crashes `/now`. This is the constraint that
makes naive "keep the last N slots" wrong.

---

## 4. Root set [C]

| # | root | why |
|---|---|---|
| R1 | every `~scheduler@1.0/assignments/<P>/<N>` and the graph it resolves to (assignment, bundle commitment, `body+link → data/<hash>`) | the only way to serve history. `scheduler-publish-remote: false`, no uploaders started, `store-arweave` empty — **this node is the sole custodian of the signed chain** |
| R1b | the message at key `<ProcID>` for all 343 processes | `lib_process:verified_id/3` verifies the signed definition on every `now`/compute/scheduler call |
| R2 | per process, the newest K slots carrying `snapshot+link`, whole | `dev_process:ensure_loaded_state/3` probes for `snapshot+link`; without one a cold resume replays from slot 0. K ≥ 2 |
| R3 | per process, the **contiguous gapless** run from the newest retained full checkpoint to the head — *both* alias links per slot (`computed/<P>/slot/<N>` and `computed/<P>/<RootID>`) | §3 |
| R4 | whole small namespaces: `~meta@1.0/preloaded-devices-index/`, `~location@1.0/`, `~bundler@1.0/`, `~arweave@2.9/` — **741 entries total** | keeping all of them costs nothing and removes a risk class |

Corrections to the intuitive root set: there is **no "latest state" record** (the
head is usually a delta envelope); a *full checkpoint* is enough for reads but
**not** for resume — only a `snapshot+link`-bearing one is a resume point, and
111,800 checkpoints exist against a nominal 1000-slot cadence because
`should_checkpoint/4` also fires on `HasSnapshot` and `MissingBase`.

**Droppable:** the entire `~match@1.0&` index; delta envelopes and checkpoints
older than the retained window; their alias links. **Group markers and their
member rows must be dropped or kept together** — a half-kept group returns
`{error, not_found}` either way.

---

## 5. Method: offline algorithm, online execution

**Do not read the live store.** `iterator_next`, `list`, `read_prefix_rows` and
`match_pattern` all call `flush_sync` when the overlay is non-empty, so every
scan step forces a synchronous commit on a busy node. **[C]**

1. `mdb_copy` the live store to a work dir (**~7 min** at the measured 262 MB/s
   write) — node keeps serving, LMDB MVCC gives a consistent snapshot. **[E]**
2. Compact **against the copy**, as an escript in `scripts/` (precedent:
   `scripts/build-preloaded-store.escript`). The copy is idle: no overlay, no
   `flush_sync`, full speed, no page-cache competition.
3. Catch up online for slots past the high-water mark — bounded to a few thousand.
4. Stop node → final catch-up → `mv` → start. **Downtime is minutes**, not
   proportional to the copy.
5. Keep the old directory. Rollback is `mv`.

**Prefer mark-and-sweep (one ordered pass over the copy) to mark-and-copy.** It
is O(total) where total is affordable, and it eliminates the class of
"derived row I forgot" bugs — parent group markers, and the **alias rows** that
make a message reachable by both its signed and unsigned ID (2.95M of them; miss
one and requests by signed ID 404 silently).

**Throughputs [M]:** `read_prefix` 1.33–1.59M rows/s; `iterator_next` only
115,000 rows/s (new RO txn + depth-6 descent per row); disk 2.1 GB/s read,
262 MB/s write. A full single-threaded pass is ~74 min; `elmdb`'s cursor token is
a plain `{iterator, LastKey}` tuple that seeks with `MDB_SET_RANGE`, so the
keyspace **can be range-partitioned across cores** — ~5–15 min. **Verify that
seek works with one call before relying on it.** **[C]**

**Also viable, zero Erlang and zero Rust:** `mdb_dump | filter | mdb_load -a`
using the existing `lmdbtools` image. `-a` uses `MDB_APPEND` for ~100% leaf fill;
the filter is a streaming merge-join against a sorted keep-list, O(1) memory.
Prototype it first — it may be the whole answer.

**Reachability memory.** ~70.4M distinct roots. The graph is a **DAG** (content
addressing forbids cycles), so traversal terminates without a visited set. A
naive Erlang map is 7.3–9.0 GiB and would evict the page cache the `now` path
depends on. Use a bounded **exact** LRU visited cache (4–8M entries, ~0.5–1.0
GiB); a miss only costs re-expansion. **A bloom filter is safe in the sweep
direction (false positive = keep garbage) and catastrophic as the visited set
(false positive = silently lose a subtree).** For an exact full set, spill 32-byte
IDs to a file (~2.3 GiB), `sort -u`, and merge-join against the ordered key
stream. **[E]**

**Trap:** a `data/<hash>` link target's first path segment is `data`, and
`read_prefix(Db, <<"data">>)` pulls all 26.5M blobs (~21 GiB) into one term. This
broke `hb eval` on the live node during research. **Special-case `data/` targets,
and never call `read_prefix`/`list`/`match` on a short prefix through `hb eval` —
it runs inside the production VM with no result-size bound.**

---

## 6. Verification before the swap

Exhaustive where it is cheap, sampled only where it is not:

1. All **1,459,788** assignment keys resolve to the **same root ID**. ~5 min.
2. Every retained `computed/<P>/slot/<N>` link resolves **and**
   `dev_process_cache:read/3` returns `{ok, _}` — this exercises the delta chain.
3. Per process, the retained slot set is **contiguous** from a full checkpoint.
4. Per process, `latest/2` returns the same slot and a byte-identical message.
5. Per process, a `snapshot+link` resume point exists, ≥ K.
6. Every `<ProcID>` root present and `verified_id/3` succeeds.
7. **Sampled deep read: N = 5,000 slots**, stratified across all 343 processes
   (proportional, min 5 each), byte-equal after `ensure_all_loaded`. For 99%
   confidence the per-slot miss rate is under p, N ≥ 4.6/p; 5,000 covers p=10⁻³.
8. **Shadow-replay** — the check that catches the unanticipated. Capture an hour
   of real access log (741 of 765 requests are `~process@1.0/now/<key>`), replay
   against a second node on the new store, diff every response body.

Rename, never delete. Keep the old directory for at least one checkpoint interval.

---

## 7. What it reclaims — three levers, compounding

| lever | risk | result |
|---|---|---|
| 1. sorted reload, **keep every row** | **none** — no policy, no reachability | 97.2 → **~70 GiB [E]** |
| 2. + drop `~match@1.0&` | index only, rebuildable | → **~56 GiB [E]** |
| 3. + drop superseded computed results | needs §3–§4 correctness | → **~40 GiB [E]**, range 25–55 |

**Realistic target 97 → ~40 GiB: 2–4x, not 5x.** Do not promise a number until a
trial mark measures the **content-address sharing factor** between consecutive
checkpoints — deltas carry `{set, Value}`, so a checkpoint's field data was
already written by the patch that changed it, and the marginal reclaim from
dropping one is only its unique sub-messages. That factor is unmeasured and is
the largest driver of lever 3.

**Lever 1 is worth doing on its own.** It needs no policy decision at all.

---

## 8. The finding that outranks the compactor

**No reachability scheme bounds this store.** R1 is permanent and unarchived:
`scheduler-publish-remote: false`, no uploaders, `store-arweave` empty. The chain
is 36.50M entries / ~4.2 GiB at 1,459,788 slots — **~3 KiB per slot**, so **~2.5
GiB/day irreducible** at the average rate and ~6 GiB/day loaded. **[M]/[E]**

Even with a perfect compactor running continuously, 419 GiB is consumed in
**~170 days average, ~70 days loaded.** Compaction changes the constant, not the
asymptote.

**To actually bound the store, pick one:**
- **upload assignments** (`scheduler-publish-remote: true` + a bundler) so
  Arweave is the archive and the node serves cold history by fetching;
- **ship them off-box** on a cadence and serve cold history from there;
- **accept that this node stops serving history beyond N slots**, and say so.

**This decision is upstream of the compactor and should be made alongside it.**

---

## 9. Staged plan

- **Stage 0 — measure, no changes.** Restore `hb eval` (needs a node restart).
  Against an `mdb_copy` snapshot: verify `{iterator, Key}` seek; get the
  per-process slot histogram and `Σ min(slots_p, W)` for W ∈ {100,500,1000,5000};
  per process record the newest full checkpoint, newest 3 `snapshot+link` slots,
  and **whether checkpoint and snapshot are co-located**; trial-mark one large
  process and measure the **sharing factor** and visited-cache hit rate; resolve
  the `data/` blob and list-container splits by following links.
  *Gate: a projected output size, ±20%.*
- **Stage 1 — build and dry-run.** `scripts/compact-store.escript`,
  mark-and-sweep against a snapshot; also prototype the `mdb_dump | mdb_load -a`
  pipeline and pick on measured throughput. Run §6 in full. Separately produce a
  **keep-everything sorted reload** as the zero-risk option.
  *Gate: all structural checks pass, zero deep-read mismatches, within 20% of
  stage 0.*
- **Stage 2 — shadow.** Second node on the compacted store, replay an hour of
  real log against both, diff bodies. *Gate: zero differences on `/now`.*
- **Stage 3 — cut over.** Copy → compact → verify → catch-up → stop, final
  catch-up, `mv`, start. Measure after: file size, entry count, `now` p50/p99,
  cold historical latency at several depths, new growth slope.
- **Stage 4 — the actual bound.** Decide §8, or stage 3 repeats every few months
  forever. Also consider the ~5-line Rust change to sort each flush batch before
  putting — it attacks the slope and the 64.2% fill at once, with no API change.

---

## 10. Unmeasured

- **The content-address sharing factor** — biggest driver of lever 3.
- The `data/` blob keep/drop split — 21.19 GiB unattributed.
- The list-container split — ±20M entries, ±4% of the store.
- The per-process slot histogram (total, count and max are known; the
  distribution is not).
- Whether `{iterator, Key}` seek works in practice — read from the Rust, never run.
- `put_batch` write throughput — deliberately not benchmarked, since every
  available path would have written to the live store or inside the live node.

## 11. Loose ends found on the way

- A second, **empty** LMDB store is open at the release-relative default path
  inside the container's ephemeral layer. Nothing is lost today, but anything
  reaching the default store instead of the configured one dies on `docker rm`.
- `/~query@1.0/info`, `/~match@1.0/keys`, `/~query-arweave@1.0` and
  `/~copycat-arweave@1.0` hung past 15–25 s with no `sent` log line during
  research (not reproduced later). Unrelated to storage; looks like a real bug.
- Metrics cardinality: 10,205 unbounded `system_monitor_long_schedule_total`
  series dominate the scrape. Bound the MFA label before attaching any periodic
  scraper.

---

## 12. Note: the slot-draining verdict needs revisiting

Not a storage matter, recorded here because it is the other half of the
performance picture and the spec that concluded it is not in this repo.

A research fleet concluded **"do not build slot draining"** on the grounds that
its benefit class -- two or more requests queued simultaneously naming *distinct
uncached slots above `at-slot`* -- was empty: it measured **matchable queue depth
0**, 4 resident messages node-wide, and all 4 unmatchable (`ungrouped_exec` atom
against a worker bound to a binary name).

**Measured under real load on 2026-09-26, that premise is inverted [M]:**

```
worst single worker mailbox        1,220 (later 1,169, 1,050, 968)
message shapes, 200 sampled        200/200 matchable_binary
distinct slots, 200 sampled        200/200
queued slot range                  14,733 - 15,033
process at-slot                    14,732     <- every queued slot is AHEAD
```

Every queued request is for a slot the worker must still compute, every one is
matchable, and every one names a distinct slot. That is exactly the stated
benefit class, 1,000+ deep.

Two reasons the original measurement came back empty:

1. It sampled an **idle** node. The condition only exists under load.
2. The unmatchable `ungrouped_exec` messages it *did* find were the
   `hb_persistent:await/4` group-name defect, since fixed in `fcb314ed7`. With
   that fixed, queued messages are matchable -- which is the precondition the
   drain needs.

Also worth recording, because it was misdiagnosed twice in one hour: **the
bottleneck here is serialization, not CPU.** At the moment of the 1,220-deep
mailbox, schedulers were 96-99% with run queue 44; twenty minutes later, with a
968-deep mailbox, they were **48% with run queue 0**. The queue persists across
both, because a single Erlang process occupies at most one scheduler regardless
of how many are free. Raising or lowering `push-max-workers` does not move it.

**Before building it, re-read the original spec's do-not list.** Its guards were
derived carefully and remain valid -- particularly that a push-bearing request
drained as a non-max member loses its outbox dispatch, which is the same class of
silent loss this node already had. The verdict was wrong; the guards were not.

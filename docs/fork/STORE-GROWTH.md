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
- Growth: **~38 GiB/day** long-run average, **~0.2 GiB/day** idle, and **85-99
  GiB/day under a 1,000-wallet load** measured on 2026-09-26. **[M]**
- Runway on 411 G free: ~11 days at the long-run average, **~4 days under load.** **[M]**

**The load figure changes what the compactor is for.** At 38 GiB/day the full
compactor (97 -> ~40 GiB) buys months and lever 1 alone buys ~9 days. At 89
GiB/day lever 1 buys **~7 hours** and the full compactor buys ~7 days.
Compaction cannot keep pace with sustained load: it is a one-off reclaim, not a
rate change. At this load level the archival decision in §8 is not the
follow-up to compaction, it is the prerequisite.

Isolated by measuring components separately over 90 s **[M]**: `store/lmdb`
88.73 GiB/day against a filesystem loss of 99.48 GiB/day, so the LMDB store is
**89%** of it and everything else 11%. Most of that remainder is `cache-http`
under `store-all-signed: true`, which has **213,148 top-level directories**.
Disabling that key is the only lever here that needs no build -- but note the
audit found ~95% of POSTs carry `codec-device=ans104@1.0` and never reach the
write site, so the real yield may be well under 11%, and `dev_auth_hook` is a
second consumer writing to the **main** store with a test asserting
recallability.
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

## 13. Wipe experiment and the corrected verdict (2026-09-26)

**[M] = measured this session. The store was wiped and then restored; both
halves of that are recorded here because the wipe's numbers are misleading.**

### What the wipe showed, and why its headline number was wrong

With `match-index: false` on an **empty** store the node grew **16.1 GiB/day**
against an **85-99 GiB/day** loaded baseline. That 83% figure must not be
quoted: it conflates two changes. A fresh store writes sequentially into a
depth-4 tree at near-perfect leaf fill; the old store was depth 6 at 64.2%
fill. Most of the improvement was the empty tree, not the index.

The fix *is* active, and that part is solid: a full dump of all 562,669 keys on
the fresh store found **zero `~match@1.0` keys**.

### The wipe broke the node and does not self-heal

Wiping took the **process definitions** along with the history, so processes
could not be computed at all -- there was nothing to rebuild *from*. `/now/*`
returned 500 for every process. The store was restored from
`lmdb.WIPED-20260926T053113Z` and the node recovered fully.

**A wipe is therefore not a growth strategy.** Any retention scheme must keep
each process's definition and a contiguous delta chain from a checkpoint
(see §5 on `latest_from_store/4` hard-matching).

### Corrected steady-state figures on the restored store

Measured over a clean 241 s window under live load, `match-index: false`:

| quantity | value |
|---|---|
| growth | **59.13 GiB/day** |
| scheduled writes | 10.4/s (2,530 POSTs, all 200) |
| cost per scheduled write | **68.3 KiB** |
| disk headroom | 409.5 GiB free = **~6.9 days** |

`68.3 KiB/write` is *total* store growth per write -- assignment plus computed
state plus deltas. It is not comparable to the assignment-only 65 -> 46 KiB/slot
figure in §7; the assignment is only a slice of it.

### The verdict that matters

**`match-index: false` trims growth; it does not stop it.** It removes a
derived structure no reader needs, and that is worth keeping, but the dominant
per-slot cost is state and delta writes. At 59 GiB/day the node has ~7 days of
disk.

Growth cannot be *stopped* by any config flag. It needs the compactor plus an
**archival decision**, and that decision is not an engineering choice: the node
is the sole custodian of the signed assignment chain
(`scheduler-publish-remote: false`, no uploaders, `store-arweave` empty). Until
those assignments are published somewhere, deleting them destroys the only copy.

### Measurement traps hit while producing the above

- `~scheduler@1.0/slot` and `GET ~scheduler@1.0/schedule` return **500**;
  `status` and all of `~process@1.0/*` return 200. The throw is in
  `lib_process:verified_id/3` via `dev_scheduler:find_target_id/3` -- process-ID
  *verification*, not a match/index lookup, so it is **unrelated to
  `match-index: false`**. POST `/schedule` succeeds 2,530/2,530, so the write
  path resolves the same process. No client uses the GET form; all 22 such 500s
  were from this session's own probes. Pre-existing, left as a TODO.
- Scraping a slot number out of that 500 page yields digits from the arweave
  icon URLs in the HTML, which produced a bogus "slots +0" reading and a false
  conclusion that growth was not slot-driven. **Parse slot counts only from a
  200, or count schedule POSTs from the log instead.**
- GiB/day is not comparable across load rates. Normalize to bytes per
  scheduled write.

## 14. Where the space actually is (2026-09-26)

Answering the operator's question directly: *what are assignments for, where is the
space, and what is needed?*

### The model is correct

Keep (a) process definitions, (b) all messages **and their assignments**, (c) periodic
checkpoints -- and every intermediate state is rebuildable. An **assignment** is the
scheduler's small signed statement binding *message -> slot -> process* with a
hash-chain link to slot N-1. It is not the message; it is the **ordering commitment**,
and it is the part that makes this a chain rather than a database. Assignments are
therefore **ledger, not garbage**: they are never the thing to delete.

An earlier session note framed the blocker as "decide whether to archive old
assignments." **That was the wrong question** -- it asked whether to discard the chain.
The right question is how much of what we write is ledger versus derived.

### Measured composition of the 148.8 GiB store

From `mdb_stat -ef` (reliable, whole-store):

| | pages | size | note |
|---|---|---|---|
| leaf | 25,218,193 | **96.2 GiB** | 645.4M entries, mean **~160 B/entry** |
| overflow | 7,177,989 | **27.4 GiB** | values > ~2 KB: Lua snapshots, computed states |
| branch | 751,912 | 2.9 GiB | b-tree interior, depth 6 |
| free pages | 5,858,977 | **22.3 GiB** | reusable; file never shrinks |

**The space is not in big state blobs.** Those are the 27.4 GiB of overflow. It is in
**645 million ~160-byte rows** filling 96 GiB of leaf pages.

### The decisive ratio

Measured under load (2,491 scheduled writes, entries +710,834):

> **285 LMDB entries per single scheduled game message.**

A message explodes into roughly 8 entries (one per field, plus group markers and
`link:` rows), so an assignment + message is order **16** entries. The remaining
**~269 of 285 -- about 94% -- is derived**: computed state fields, delta envelopes,
group markers and `link:` rows.

This metric is immune to page reuse (it counts entries, not file bytes), which is why
it is the one to trust.

### What this changes

**No archival decision is required to win most of this back.** The ledger is a small
minority of what we store. The lever is to stop writing, and to GC, the derived
material -- exactly the operator's model: keep definitions + messages + assignments +
checkpoints, mark-and-sweep everything else.

### Sampling caveat -- do not quote link/group percentages

A bounded `mdb_dump | head` sample of ~150,000 keys showed `link:data/<ID43>`,
`link:<ID43>`, `group`, bare integers and repeated constants (`httpsig@1.0`,
`hmac-sha256`, `rsa-pss-sha512`, `ao.N.1`, `ans104@1.0`) as separate rows. **The
proportions in that sample are not valid**: it is the first contiguous slice of the
b-tree in key order, and structured keys such as `link:` sort together, so their share
is an artifact of where the slice fell. The sample proves these row *types* exist and
dominate the shape of the keyspace; it does not measure their share. The 285
entries/write figure carries the quantitative claim instead.

It did confirm **zero `match@` keys**, consistent with the full-keyspace dump on the
fresh store: `match-index: false` is active.

### Operator hazard found the hard way: stale LMDB readers

A `mdb_stat -a` killed by a `timeout` left a **dead process holding a reader slot**
(pid 1615455, pinned at txnid 63,989,561 while the node ran to 64,097,522 -- ~108,000
transactions pinned). LMDB cannot recycle a page still visible to any reader, so the
writer allocated fresh pages indefinitely:

- growth went to **10.2 GiB per 240 s (~3.6 TiB/day)**, and apparent cost per write to
  68.3 KiB, while leaf+overflow pages grew by only ~41k of 2.61M new pages -- the
  giveaway that the new pages held no data
- the store went **104 -> 148.8 GiB**; disk 409 -> 365 GiB free
- `mdb_stat -rr` clears slots whose owner is dead, and fixed it immediately

**Rules.** Never kill `mdb_dump`/`mdb_stat` mid-transaction -- and `| head` sends
SIGPIPE, which does exactly that. After any bounded dump, run `mdb_stat -rr` and
confirm zero readers hold a txn. Check `mdb_stat -r` for a dead pid before believing
any growth measurement; the file over-extension it causes is permanent (LMDB never
shrinks), though the pages return to the free list and are reused.

**Refuted while investigating:** the node does *not* hold long read transactions during
slow requests. Sampling the reader table over 48 s, with p99 reads at 14 s and a max of
31.5 s in flight, found no reader in a transaction at any point. `hb_store_lmdb` uses
short per-operation transactions, so slow reads do **not** inflate growth.

## 15. Where the space really goes, and a retention planner (2026-09-26)

Section 14 claimed "~94% derived" from an entries-per-write ratio. Direct
measurement on the 149 GiB corpus refines that materially. **[M]**

### Anatomy of a slot

Sampling one busy process (`ZZUNq5...`, 64,601 slots):

| slot | bytes | kind |
|---|---|---|
| 60000 | 21,429,945 | checkpoint |
| 60001 | 24,320 | delta |
| 60010 | 9,031 | delta |
| 60100 | 11,719 | delta |
| 61000 | 22,455,773 | checkpoint |
| 63000 | 24,146,646 | checkpoint |
| 64595 | 8,686 | delta |

Checkpoints land every `process-delta-checkpoint-slots` (default 1000). Inside
a 24.1 MB checkpoint, by top-level field:

| field | bytes |
|---|---|
| `snapshot` | **18,876,953** (78%) |
| `process` | 502,852 |
| `module` | 500,603 |
| `economystate` | 178,802 |
| `economy` | 54,470 |
| `leaderboard` | 22,133 |
| `factions` | 16,131 |
| (1,629 fields total) | |

`snapshot` is `#{<<"body">> => Binary}` -- a single opaque Lua VM image. It is
**not compressible** (gzip returns 99% of input) and shares **nothing** between
consecutive checkpoints (zero identical values between slots 63000 and 64000).
**CORRECTED 2026-09-27.** An earlier version of this line claimed the actual
game state is "under 1 MB". **That was wrong** and it propagated into
`SPEED-UPGRADES.md` Item 2, where it produced a 4x-optimistic cost estimate and
a wrong default. The error: the field table above lists only the seven largest
fields, and I summed those while ignoring ~1,622 unlisted ones.

Correct arithmetic for this checkpoint:

```
listed fields sum        20,151,944 B
checkpoint total         24,146,646 B
UNLISTED remainder        3,994,702 B  (3.81 MB) across ~1,622 fields
non-snapshot total        5,269,693 B  (5.03 MB)
  minus process+module    4,266,238 B  (4.07 MB)  <- what a state-only anchor carries
```

So the non-snapshot public state is **~4-5 MB, not under 1 MB**. Independently
confirmed by direct measurement of a synthetic anchor at **3.95-6.2 MB**
(`perf/delta-state-anchors`), against a calibration control whose synthetic
checkpoint measured 25,436,160 B vs this real one's 24,146,646 B.

### Corpus totals

370 processes, 357 with slots, **1,985,934 computed slots** and 1,985,957
assignment slots -- **1:1**, one computed state per assignment.

| | pages | size |
|---|---|---|
| leaf | 25.2M | **96.6 GiB** |
| overflow | 7.2M | **27.6 GiB** |
| branch | 0.75M | 2.9 GiB |
| free | 5.9M | 21.8 GiB |
| **live** | | **127.1 GiB** |

So roughly **23% VM snapshots** (the overflow pages) and **~73% delta
field-explosion** (the leaf pages): ~48 KB of leaf pages per slot to hold
~12 KB of logical delta, across ~325 rows of ~160 bytes.

### `hb_store_gc:plan/2`

New module, planning only -- it writes and deletes nothing, because **no delete
primitive exists anywhere in the stack**. Enacting a plan means copying the
retained set to a new store and swapping.

Policy: retain every slot within `keep_recent` of the head (contiguously --
the chain is backward-linked and `dev_process_cache` hard-matches, so a hole
makes reads fail rather than degrade), plus one anchor every `anchor_every`
slots below it.

Against the corpus at `keep_recent = 1000`, `anchor_every = 10000`:

| quantity | value | confidence |
|---|---|---|
| slots counted | 1,985,934 | **exact** |
| slots retained | 202,502 | **exact** |
| slots dropped | **1,783,432 (89.8%)** | **exact** |
| checkpoints retained / dropped | 651 / 1,584 | **exact** |
| bytes reclaimable | **~79%** (39 of 49 GiB logical) | projected |

**The slot figures are counted; the byte figure is projected** from a sampled
mean checkpoint size and must not be quoted as measured.

### Unresolved discrepancy -- do not treat the byte projection as settled

Projected logical total is 49 GiB against a 127.1 GiB live store, i.e. 2.6x
on-disk amplification, which is consistent with 64.2% leaf fill plus ~160 B of
overhead per small entry. But **overflow pages are 27.6 GiB while the sampled
checkpoint mean (7.9 MB x 2,235 checkpoints, 78% snapshot) implies only
~13.8 GiB of snapshots -- a 2x gap.** Either there are more checkpoints than
the 1000-slot cadence implies, some processes checkpoint on a different
cadence, or large `results` values also land in overflow. Resolve this before
anyone sizes hardware or a retention window on the byte figure.

### Estimator traps hit while building this

- **Classifying checkpoints by the retention anchor rather than the writer's
  cadence** priced ~1,600 real 24 MB checkpoints as 12 KB deltas and inflated
  the corpus projection to 252 GB -- 1.7x the store's actual size. The writer's
  cadence is not a policy choice; `project/4` now takes it separately.
- **Sampling blind and splitting on a size threshold** let checkpoints
  contaminate the delta mean. Sample the two kinds separately.
- **Sampling checkpoints per process** is ~34 GB of reads and never finishes;
  one global mean suffices. But **sampling the first few processes in list
  order** gave a 5.7 MB mean against 14-24 MB measured on a large process --
  list order is small processes first. Sample the *largest* processes: the mean
  tracks game-state size and the big processes hold most of the bytes.
- `hb_cache` writes to a second LMDB store silently persist nothing unless that
  store's flush worker is started; `{ok, _}` is returned regardless.
- `hb_opts:default_message()` carries no store, so cache listings return `[]`
  and look like an empty store. Pass the LMDB store explicitly; the node's
  configured `store` is a *list* (LMDB, then fs, then arweave/gateway) and the
  latter two make remote calls.
- The release runs in **embedded** mode, so `code:add_patha/1` returns
  `{error, embedded}`. Load test modules with `code:load_binary/3`.

## 16. The ledger is only 5% — collection CAN bound this store (2026-09-27)

Measured on live process `-DoLi-Og1-bV` (41,276 slots) at 712 writes/60s
(~1,025,280 slots/day), store 93.5 GiB live over 1,486,751 slots. **[M]**

### Per-slot budget

| component | bytes (logical) | share | fate |
|---|---|---|---|
| ledger (assignment + message) | **2,648** | **5.0%** | **irreducible** |
| delta | 28,853 | 55.0% | collectable |
| checkpoint, amortised over cadence 1000 | 20,972 | 40.0% | thinnable |
| **total logical** | **52,473** | | |
| on-disk measured | 65.9 KiB | | 1.3x amplification |

Store composition confirms the split: **overflow (snapshots) 41%, leaf (rows) 57%**
of 93.5 GiB live. **[M]**

### This corrects a fear recorded earlier in this file

§14 framed the open question as whether the signed chain was so large that no
compactor could bound the store. **It is not.** An assignment is **2,648 bytes**.
At ~1.03M slots/day the irreducible ledger costs **2.7 GB/day**, so 266 GB of
free disk lasts **~98 days on ledger alone**. Against ~85 GiB/day today, full
collection is a **~31x reduction**. Collection is decisively worth building.

### Retention window economics (anchors ON at cadence 250)

Steady state, with old deltas and anchors deleted outside the window:

| window | deltas retained | anchors retained | fixed total | fits 266 GB? |
|---|---|---|---|---|
| 1 day | 29.6 GB | 21.5 GB | **51.1 GB** | yes, comfortably |
| 2 days | 59.2 GB | 43.0 GB | **102.2 GB** | yes |
| 3 days | 88.7 GB | 64.5 GB | **153.3 GB** | yes |
| 7 days | 207.1 GB | 150.5 GB | **357.6 GB** | **no** |

Plus ledger accumulating 2.7 GB/day forever, and checkpoints which must be
thinned beyond the window or they remain unbounded at 21.5 GB/day.

### A delete primitive is what makes anchors worth enabling

This is the key interaction. Without deletion, anchors at cadence 250 cost
**+21.5 GB/day accumulating forever** — which is why §15's implementation ships
with `process-delta-anchor-slots = 0`. With a retention window they cost a
**fixed 21.5 GB per day-of-window**, because old anchors are deleted as new ones
are written. A growth-rate problem becomes a constant-size problem.

So the build order is: **delete primitive + collector first, then enable
anchors.** Anchors are already implemented and tested on
`perf/delta-state-anchors`, waiting on exactly this.

### Cost of the cold path (reads outside the window)

Beyond the retention window, deltas are gone and a historical read must
re-execute from the nearest retained checkpoint. Upper bound from CPU
accounting: container used 4.585 cores at 11.9 slots/s = **<=385 ms per slot
execution**, against ~128 ms per delta apply **[M]** — so re-execution is at
most ~3x delta replay, not the order of magnitude feared. A 1000-slot gap costs
~6 minutes worst case. Acceptable for cold archival reads.

**Measurement note:** an earlier attempt at this figure multiplied request count
by request *latency* to get CPU, which is wrong — a request queued for 450 ms
burns no CPU. That produced "56 cores" on a 4.6-core container. The bound above
uses cgroup CPU accounting divided by slot rate instead.

## 17. DECIDED (2026-09-28): 1-day retention, collector is next

Operator decisions:

- **Retention window: 1 day.** Deltas and anchors retained for 24 h (~51 GB
  fixed at current rates), older reads fall back to re-execution from the
  nearest retained checkpoint (bounded <=385 ms/slot, ~6 min for a 1000-slot
  gap). Chosen over 2-3 days deliberately for maximum headroom against write
  rates climbing above today's ~1.03M slots/day.
- **Build next: the delete primitive + collector.** It is the keystone: bounds
  growth ~31x, removes the ~3.5-day disk clock, and unlocks the anchors already
  built on `perf/delta-state-anchors`.

### Retention policy to implement

| class | retention | why |
|---|---|---|
| process definitions | forever | without these nothing computes — the wipe proved it |
| assignments + messages | forever | the signed chain; only 2,648 B/slot, 2.7 GB/day |
| checkpoints (VM snapshot bearing) | forever initially, thin later | the only resume points for re-execution |
| deltas | **24 h** | 55% of per-slot cost; rebuildable |
| anchors (once enabled) | **24 h** | read accelerator only, never a resume base |
| computed states outside window | drop | rebuildable by re-execution |

### Why it must be copy-based

There is **no delete primitive at any layer**: not in the pinned `elmdb` Rust
NIF, not in the `hb_store` behaviour, and `hb_store_lmdb:reset/3` is `rm -Rf` of
the whole store. So a collection pass must copy the retained set into a fresh
store and swap, keeping the old one until the new one is validated.

### The corpus is the test bed

`/opt/runerealm-hb/store/lmdb.CORPUS-20260926T134420Z` (149 GB, 648,289,032
entries, 370 processes, 1,985,957 assignment slots) exists precisely for this.
The collector must be built and validated against it, **never against the live
store**, until it has demonstrated a correct round trip.

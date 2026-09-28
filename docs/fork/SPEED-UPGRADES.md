# Throughput upgrades — specs

Written 2026-09-27 after a 25-hour continuous load test (2.3M+ requests,
~135 req/s, zero genuine failures). All figures marked **[M]** are measured on
this node; everything else is reasoning and labelled as such.

## The bottleneck is serialization, not resources

Three independent readings under live load, all confirming CPU is not the limit:

| | value |
|---|---|
| host CPU | **75% idle**, 20% user **[M]** |
| hyperbeam container | 470% of one core = **4.7 of 16 cores** **[M]** |
| BEAM schedulers | avg 28%, max 42%, `run_queue=0` **[M]** |

During the worst incident of the run — p99 **70.7 s** — there were **373
processes parked in `dev_process_worker:await/5` while `run_queue` was 0** and
all 16 cores idle. **[M]** Nothing was working; everything was queueing.

`dev_process_worker` is one worker per AO process and inherently serial, so
compute concurrency is capped at the number of live processes (28 **[M]**),
regardless of core count. **More cores cannot help a queue that is one deep by
design.** Erlang processes are not OS processes (~300 words, ~100 ns
cooperative switches, 716 live on 16 schedulers at 28% **[M]**), so process
count far above core count is normal and costs nothing.

### Already implemented — do not re-build
`dev_process_worker:compute_group/3` **already** routes cache-*hit* reads around
the worker via `ungrouped_exec`. The 70 s incident was cache *misses* needing
~896-delta replay, which serialize correctly but expensively.

---

## Item 1 — Run historical replay concurrently, outside the worker

**Problem.** A read of `slot < head` is a pure function of (anchor, deltas):
read-only, mutating no shared state. It nonetheless serializes through the
process's singleton worker, so N concurrent historical reads cost N x 115 s.
**[M]** 30 such reads = ~57 minutes of queue while the box idles.

**Change.** In `dev_process_worker:compute_group/3`, extend the existing
ungrouped path: a compute request for a slot strictly below the process head
routes to `ungrouped_exec` even on a cache miss, under a concurrency cap
(new `process-historical-read-workers`, suggest 8). Head-advancing compute stays
in the worker.

**Why it is safe.** Historical replay never advances head state and never
becomes the worker's continuation state. `dev_process_cache:recent_put/5` and
`mark_cached_state/1` already treat replayed states as public-only and forbid
computing onward from them.

**Risk.** Two requests replaying overlapping ranges duplicate work. Accept it:
the replay cache absorbs the second, and the alternative (a per-range lock)
reintroduces the queue.

**Expected gain.** 30 queued historical reads: ~57 min -> ~2 x 115 s, using
cores that are currently idle.

---

## Item 2 — Cheap state-only anchors, decoupled from VM snapshots  ← BUILD FIRST

**Problem.** Replay cost is linear in distance from the last full state. Full
checkpoints land every `process-delta-checkpoint-slots` (default 1000), so
worst case is ~896 deltas x ~128 ms = **~115 s** **[M]**, observed live.

Checkpoints cannot simply be made more frequent: a checkpoint is **~21-24 MB, of
which ~18.9-21 MB is the Lua VM `snapshot/body`** **[M]**. Measured cost of
tightening full checkpoints 1000 -> 64 was one process going from ~10 to
~162 GiB/day. **[M]**

**And the snapshot cannot be compressed.** Measured on a real 20,983,226-byte
snapshot body: **gzip 99%, `term_to_binary(_, [compressed])` 99%** **[M]**. The
payload is dominated by 43-char base64 Arweave addresses — high entropy, nothing
to find. This **refutes the premise of the in-flight `perf/lua-snapshot-
compression` branch** (`f2828e6a7`), whose commit claims "roughly an order of
magnitude" using exactly `term_to_binary(Externalized, [compressed])`. That
branch should be re-measured before merge.

**Change.** Historical *reads* need public state, not the VM image. Write a
**state-only anchor** — full public state with the `snapshot` key omitted — at a
separate, tighter cadence:

- new opt `process-delta-anchor-slots` (suggest 250; 0 disables)
- new cache-format tag, e.g. `process-anchor@1.0`, so anchors are
  distinguishable from both deltas and full checkpoints
- in `dev_process_cache:write_delta/5`, add the anchor case between the
  checkpoint and delta branches
- in `materialize/3`, when walking back, stop at the nearest anchor **or**
  checkpoint, whichever is closer

**The critical safety property.** An anchor has no VM snapshot, so it must never
be selected as a resume base. This falls out of existing code:
`ensure_loaded_state/3` asks `dev_process_cache:latest/4` for the latest state
carrying **`snapshot+link`**, which an anchor lacks by construction. Add a test
asserting an anchor is never returned by that lookup.

**MEASURE FIRST, then choose the cadence.** The non-snapshot remainder of a
checkpoint is ~3-5 MB by subtraction (24.1 total - 18.9 snapshot **[M]**), but
`process` and `module` (~1 MB **[M]**) are content-addressed and dedupe across
checkpoints, so the true marginal anchor size is unknown. Measure a real
anchor's stored size before fixing the default; if it is ~1 MB, every-250 costs
~4 MB per 1000 slots against 21 MB for the checkpoint, and cuts worst-case
replay to ~250 deltas (**115 s -> ~32 s**). If it is ~5 MB, every-250 costs
~20 MB per 1000 slots — roughly doubling checkpoint bytes — and the cadence
should be loosened accordingly. **Do not ship a default that was not measured.**

---

### MEASURED OUTCOME — Item 2 is NOT nearly free, and ships disabled

Implemented on `perf/delta-state-anchors` (stacked on `feat/lua-5.3b-delta-state`,
base `08c1268da`). **The cost estimate above was wrong by ~4x** and the
"suggest 250" default was withdrawn as a result.

Measured with one fresh LMDB store per scenario, byte-identical content across
scenarios, `hb_store:stop/1` to force the async flush, then allocated blocks.
Calibration control: synthetic checkpoint **25,436,160 B** vs the real one's
**24,146,646 B**, so the model is sound. **[M]**

| public state | per anchor | checkpoint | ratio |
|---|---|---|---|
| 1.0 MB | **3,951,616 B** | 25,436,160 B | **0.16** |
| 4.3 MB | **6,223,872 B** | 28,676,096 B | **0.22** |

An anchor is **16-22% of a checkpoint, not the ~4% assumed**. Per 1000 slots at
1.0 MB state: cadence 250 adds 15.8 MB on top of the window's 25.4 MB
checkpoint = **+62%** store growth for 115 s -> ~32 s; cadence 500 adds 7.9 MB =
**+31%** for ~64 s. At 4.3 MB state, cadence 250 = **+98%**.

**Default shipped: `process-delta-anchor-slots` = 0 — mechanism present, off.**
+62% to +98% store growth is not a safe default on a node with ~3.5 days of disk
runway and no delete primitive at any layer. Enable deliberately once Item 5
bounds the store.

**Root cause of the bad estimate:** `STORE-GROWTH.md` claimed the public state is
"under 1 MB" by summing only the seven largest fields and ignoring ~1,622 others
worth 3.81 MB. Corrected in that file.

### Spec errors found during implementation
- **`materialize/3` needs no change.** It follows one backward link at a time via
  `read/3`, so the first non-delta entry already terminates the walk and is by
  construction the nearest anchor-or-checkpoint. Only a clause returning the
  anchor's state was needed.
- The function is `dev_process:ensure_loaded/3`, not `ensure_loaded_state/3`.
- `dfd199702` (`perf/lua-process-hot-path`) does **not** overlap: it gates whether
  reaching a target slot forces a VM snapshot, in `dev_process.erl`. Disjoint
  files, disjoint options; they compose.

### Pre-existing bug found: the shared-store test suite is silently EMPTY
`dev_process_cache:process_cache_suite_test_/0` generates **zero** tests.
`hb_store:test_stores/0` returns a list of **maps** (built with `#{...}`
map-update syntax), but line 499 destructures them with a `{Name, Opts}` tuple
pattern, so the comprehension matches nothing. **The entire shared-store suite,
including the existing "delta roundtrip and checkpoint" case, has never run.**
Verified by source inspection. Fix separately — fixing it inside a feature PR
would surface unrelated pre-existing failures.

### Revised build order
**Item 1 first, not Item 2.** Item 1 uses cores that are 75% idle and costs zero
store bytes; Item 2 buys a 1.8x-3.6x replay improvement for +31% to +98% bytes
against a 3.5-day disk clock. Anchors are ready to enable once the collector
(Item 5) bounds the store.

---

## Item 3 — Raise the replay cache bounds

**Problem.** ETS cycled **644 -> 312 -> 628 -> 269 -> 628 MB** through the
historical-read bursts **[M]**: evicting and refilling constantly. The bound is
`process-replay-cache-slots` = 128, but a single replay produces ~896
intermediate states, so we retain ~14% of one chain.

**Change.** Raise `process-replay-cache-slots` to ~1024 and let the existing
byte bound (`process-replay-cache-mb`, 1536) govern. Both are compiled defaults
today — neither appears in `node-config.json` **[M]**.

**Headroom.** RssAnon 4.7 GB of 62 GB, and 22 GB of the resident set is
reclaimable LMDB mmap **[M]**.

**Expected gain.** A repeat visit to a slot range becomes a lookup instead of a
re-replay. Note this is cheaper after Item 2, which shortens the chains being
cached.

---

## Item 4 — Fast-fail requests for absent processes

**Problem.** A request for a process not in the store returns 500 after
**~96 s** **[M]**: `hb_ao:ensure_message_loaded/2` (`hb_ao.erl:936`)
`case_clause`s on `{error, not_found}`, and the ~96 s is the configured store
list reaching `hb_store_arweave`/`hb_store_gateway` and searching arweave.net
first. Live processes answer the same path in 0.003 s **[M]**.

With only ~28 workers, this is an availability foot-gun: a client pointed at a
stale process ID occupies a worker for 96 s per request. Observed live after the
wipe, when stale polling of `ZZUNq5...` produced sustained 500s **[M]**.

**Change.** Two parts:
1. Handle `{error, not_found}` in `hb_ao:ensure_message_loaded/2` instead of
   falling through to `case_clause`.
2. For a local-scope process resolve, do not fall through to remote stores — or
   bound that lookup hard (low single-digit seconds).

**Expected gain.** 96 s -> milliseconds, and a client typo stops being an
availability event.

---

## Item 5 — The collector (already specced)

Not throughput directly, but page-cache residency is what holds reads at
~0.35 ms, and the collector is what keeps the store inside RAM. See
`STORE-GROWTH.md` §15 for `hb_store_gc:plan/2` and the retention policy, and
note §15's caveat that the byte projection is unverified.

Also the disk clock: store 76 GB growing ~80 GiB/day with 285 GB free **[M]**
— roughly 3.5 days.

---

### MEASURED OUTCOME — Item 5 works, and it re-ran the ledger through re-execution

Implemented as `hb_store_gc:collect/3` on `feat/store-collector` (base
`9d7a958e1`), built and validated only against
`/opt/runerealm-hb/store/lmdb.CORPUS-20260926T134420Z`. The live store was never
opened by it. Full numbers and the corrected corpus figures are in
`STORE-GROWTH.md` §18; what belongs here is what it costs to read a slot the
collector dropped.

**`dev_process:rewind/4` fired for the first time, and it works.** Verified by
`dbg:tpl` on the packaged device module (the devices load under
`_hb_device_process_1_0_<hash>` names, so tracing `dev_process` matches
nothing — that is why an earlier attempt saw no trace), against an isolated node
booted on the collected store with a local-only store list:

| read | slots re-executed | wall | rewind fired |
|---|---|---|---|
| in-window slot, retained | 0 | **5.7 s** | no (pure delta materialize) |
| dropped slot, cold worker | 500 | **73.4 s** | no — `ensure_loaded` already lands at or below the target |
| dropped slot, worker above it | 500 | **55.9 s** | **yes, 1 call** |
| next dropped slot, worker above it | 500 | **65.9 s** | **yes, 1 call** |

All four returned **HTTP 200**. **[M]**

`rewind/4` is only reachable when a live worker holds a state *above* the target:
from a cold base, `ensure_loaded_state/3` is handed the target slot and returns
the newest snapshot at or below it, so `compute_to_slot/6` never enters the
`CurrentSlot > TargetSlot` branch. Both paths end in the same re-execution.

**Re-execution is exact.** The states rebuilt through `rewind/4` are
byte-identical to the originals in the corpus: `dev_process_cache:read/3` on both
stores, `ensure_all_loaded`, `term_to_binary` — equal for every slot checked,
including the two that were rebuilt after a rewind. **[M]**

**Cost: ~112-147 ms per re-executed slot** (500 slots in 56-73 s). §16's upper
bound of <=385 ms/slot from cgroup accounting is conservative by ~2.6x, so its
"~6 minutes for a 1000-slot gap" is really **~2 minutes**. **[M]**

**Operational consequence worth knowing before the swap:** re-execution writes
its results back through `dev_process_cache`, so every cold historical read
re-inflates the store with the slots it replayed. Six such reads on one process
added ~1 GB to the collected store. The collector bounds *growth from
scheduling*; it does not bound growth from archival reads.

**Anchors (Item 2) are still the right thing to enable next.** §16's argument
holds: with a window, anchors cost a fixed 21.5 GB per day-of-window instead of
+21.5 GB/day forever. Nothing in the collector needs changing for them —
`classify_root/2` already recognises `process-anchor@1.0` and treats it as
window-only, and `lower_to_full_state/4` already treats an anchor as a valid
terminator for the backward delta walk, which is correct: an anchor is a full
public state, it just cannot serve as a VM resume base.

---

## Build order and base branches

**Order: 2, then 1, then 3, then 4.** Item 2 attacks the 115 s at its root;
Item 1 parallelises whatever replay remains; Item 3 is cheaper once chains are
shorter; Item 4 is independent and small.

**Base matters.** `origin/edge` has **no delta machinery at all** — no
`write_delta`, no `materialize`, no `hb_process_delta.erl` **[M]**. Items 1, 2
and 3 therefore cannot PR onto bare `edge`; they must stack on
`feat/lua-5.3b-delta-state` (`08c1268da`, worktree
`/root/HyperBEAM-prs/lua-5.3b-delta-state`). Item 4 touches only `hb_ao.erl` and
**can** target `origin/edge` directly.

**Check before implementing** (per FORK-LEDGER §3): `perf/lua-process-hot-path`
carries `dfd199702 perf(process): make target snapshots cadence-aware`, which is
adjacent to Item 2 — read it first.

## Hardware note

The planned 7950X3D is the same 16C/32T already present, of which 4.7 cores are
in use **[M]** — the CPU will not help. The parts that will: **128 GB ECC** for
page-cache residency as the store grows, and the X3D's **128 MB L3**, which
suits LMDB's pointer-chasing through a depth-6 b-tree. Neither is blocking; the
serialization and replay-distance limits are in software.

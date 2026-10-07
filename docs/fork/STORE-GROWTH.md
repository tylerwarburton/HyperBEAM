# Store growth and how to bound it

The LMDB store grows monotonically and nothing in HyperBEAM ever removes
anything from it. This documents what was measured, why there is no reaper
today, and the two designs that could become one.

## Measured, 2026-09-25

Against the live node (`/opt/runerealm-hb/store/lmdb`, created 2026-09-23 04:03):

```
mdb_stat -ef:
  Number of pages used : 22,263,026   (~91 GB at 4 KiB pages)
  Free pages           : 7,209        (~29 MB)
  Entries              : 453,471,415
```

**There is no bloat.** 7,209 free pages against 22.3M used means nothing is
wasted, unreclaimed, or fragmented. Every byte is a live record. An `mdb_copy -c`
compaction would recover ~29 MB and is not worth running.

Growth is bursty. During load: 19.3 slots/s, ~88 GiB/day. Idle: ~0.2 GiB/day.
Long-run average over the store's life: **~38 GiB/day**.

## Where the entries go

HyperBEAM stores a message as one LMDB entry per field, not one row per message:

```
 ----56H1rRdyh0DTW9uwaDfHZu1OBkybR4FebVBIHw4              -> group
 ----56H1rRdyh0DTW9uwaDfHZu1OBkybR4FebVBIHw4/commitments  -> link:Rgro...
 ----56H1rRdyh0DTW9uwaDfHZu1OBkybR4FebVBIHw4/path         -> /battle-op-fb317
 ----56H1rRdyh0DTW9uwaDfHZu1OBkybR4FebVBIHw4/value        -> link:data/683F...
```

Classifying a 300,000-key sample:

| class | share |
|---|---|
| data fields | 53.8% |
| commitment fields (signature, keyid, committer, ...) | 17.7% |
| group markers | 12.9% |
| list elements (`1+link`, `2+link`, ... median depth 6, max 21) | 9.9% |
| other links | 5.8% |

38,649 group markers per 300,000 keys means **~7.8 LMDB entries per stored
message**. A slot stores the assignment, the signed user message inside it,
their commitments, the delta envelope, its `patches` and its `results` - every
nested map becomes its own message with its own group marker and per-field rows.

Snapshots are not the driver. A full checkpoint every
`process-delta-checkpoint-slots` (default 1000) amortizes to a small share.

## There is no delete primitive anywhere

Verified at every layer:

| layer | fact |
|---|---|
| `elmdb` NIF | exports `put, put_batch, get, flush, iterator, list, read_prefix, match`. No `mdb_del`, no delete of any kind. |
| `hb_store` behavior | callbacks are `start, stop, reset, group, link, type, read, write, list, match, resolve`. No `delete`. |
| `hb_store_lmdb:reset/3` | `rm -Rf` of the whole data directory. Not a prefix delete. |

So an online reaper is not a small change. It needs, in order: a `mdb_del`
binding in the forked Rust NIF (`permaweb/elmdb-rs`), `hb_store_lmdb:delete/3`,
a dispatch path that does not force a new callback onto all eleven store
backends, and only then the reaper policy itself.

## Two designs

### A. Online reaper (needs the primitive above)

Deletes as it goes. LMDB reuses freed pages, so the file would **plateau rather
than shrink** - which is the bound we want. Correct long-term answer, but it
touches the durability path of a production node and a pinned Rust dependency.

### B. Offline mark-and-sweep compactor (needs nothing new)

The store is content-addressed with links, so reachability is well defined.
Walk from a root set, copy what is reachable into a fresh store, swap
directories, restart. `elmdb` already exports everything required
(`iterator`, `foreach`, `read_prefix`, `put`, `put_batch`).

Root set:

- every assignment, for every process, for every slot - **the signed chain is
  the source of truth and is never dropped**
- the latest state per process
- the last K checkpoints per process
- the last N slot records per process

Everything transitively linked from those roots is copied. Everything else is
garbage: superseded intermediate deltas, orphaned blobs, and the entire
`~match@1.0` index if it is not being rebuilt.

**Why this is safe:** a third-party replayer reconstructs state from the signed
assignment chain and never reads our cache. Dropping intermediate process-state
records costs local rebuild time, not correctness. Dropping an assignment would
break the chain, so assignments are roots.

**Cost of dropping deltas:** a request for a dropped slot is served by loading
the nearest surviving checkpoint and re-executing forward from the assignments.
That requires the Lua VM snapshot at that checkpoint, which shares the
checkpoint's cadence, so checkpoints must be kept as whole units.

**Verification before swapping:** sample N slots across each process, read them
from the old and the new store, and require identical results. Keep the old
directory until that passes.

## Recommended order

1. Skip the reverse match index on assignments (done - see the ops changelog).
   Derived data, no reader, roughly a quarter to a third of entries.
2. Build B, validate it against a copy, and run it in a maintenance window.
3. Consider A only if B's cadence turns out to be too coarse.

Note that (1) changes the slope and not the outcome: while nothing deletes, any
write reduction only postpones the wall. (2) is what actually bounds the store.

## 18. The collector, built and measured against the corpus (2026-09-28)

`hb_store_gc:collect/3` on `feat/store-collector` (base `9d7a958e1`). It copies
the retained set into a fresh store and leaves both in place; nothing is
deleted and the source is opened through a store marked
`read-only => true, access => [<<"read">>]`, so `write`, `link`, `group` and
`reset` are inadmissible before they reach `hb_store_lmdb`.

### The delete primitive really is absent — checked, not assumed

The pinned `elmdb` (`faa762323be6bad2db26abfa1d4243863a877f0f`) exports exactly
`env_open`, `env_sync`, `env_close`, `env_close_by_name`, `env_status`,
`db_open`, `db_close`, `put`, `put_batch`, `get`, `flush`, `overlay_count`,
`iterator`, `iterator_next`, `foreach`, `fold`, `map`, `list`, `read_prefix`,
`match`. **[M]** Grepping the Rust NIF for `del`/`delete`/`remove`/`drop` finds
two hits, both unrelated: `remove_file` of a `.lmdb_test` probe, and
`HashMap::remove` of an environment handle. The `hb_store` behaviour's callbacks
are `start/3`, `stop/3`, `reset/3`, `group/3`, `link/3`, `type/3`, `read/3`,
`write/3`, `list/3`, `match/3`, `resolve/3`. `hb_store_lmdb:reset/3` is
`os:cmd("rm -Rf " ++ DataDir)`. §17's premise stands.

### The bug that would have destroyed the chain silently

**There are two kinds of reference in this store and only one of them looks like
one.** The store's own is `"link:<key>"`, written by `hb_store_lmdb:link/3`. The
message layer's is `hb_link:normalize/3`'s `<key>+link => <ID>`, which
`hb_cache:ensure_loaded/3` follows by reading the message at `<ID>`. Because
`hb_cache:is_immediate_value/2` excludes `+link` keys from inline storage, that
ID is itself held behind a `link:data/<hash>` row — so a collector that copies
`link:` targets reaches the row that *names* the submessage and stops there.

Measured on the corpus: assignment `hBTUgAD6...` has
`body+link => link:data/tdsn3UtJ...`, and `data/tdsn3UtJ...` holds the 43-byte
ID `pDXEJvHbxSSBA_e_6nVlZKK1dVjmoBle-2p8gIKyTOo`. That root is the signed game
message — 5,241 bytes with its own commitments and RSA signature. **[M]** The
first version of the collector copied every assignment envelope, reported
**zero misses**, and left every message body behind. A checkpoint's ~16 MB VM
image hides the same way, one level deeper:
`snapshot+link -> link:data/4bQwJcS3... -> lCsalH5u...`.

The symptom is not a silent wrong answer — `hb_cache:ensure_all_loaded/3` throws
`{necessary_message_not_found, ...}` on the missing target, so an incompletely
copied store **crashes reads rather than serving partial ones**. That is the
only reason this was recoverable. Removing the `+link` rule from
`follow_row/3` fails two tests.

### Retention classes are read, not inferred

§15's `plan/2` prices slots by the writer's cadence. **That is fine for a
projection and wrong for enactment.** Classifying all 1,985,938 computed slots
by reading them: **[M]**

| class | slots | what it is | fate |
|---|---|---|---|
| `delta` | 1,898,775 | `cache-format: process-delta@1.0` | window only |
| `state` | 83,069 | full public state, **no** VM image | window only |
| `checkpoint` | **4,094** | carries `snapshot+link` | **forever** |
| `anchor` | 0 | `process-anchor@1.0`; not enabled yet | window only |

`checkpoint` is defined by `snapshot+link`, which is exactly the key
`dev_process_cache:latest/4` searches for when `dev_process:rewind/4` needs a
resume base. Nothing else can restart execution.

**Cadence is not uniform, so arithmetic would have dropped real snapshots.**
Of the 370 processes: 162 checkpoint at exactly every 1000 slots, 161 carry a
single checkpoint (slot 0, which `should_checkpoint/4` forces), and the rest land
irregularly at gaps of 50 or less. **[M]** That split is the two execution
devices: `dev_process:should_snapshot/3` routes a delta-bearing result
(`lua@5.3b`) to `should_snapshot_delta_slots/2` and the 1000 cadence, and
everything else to `process-snapshot-slots` (50) or `process-snapshot-time`
(900 s). Every process with >20,000 slots is on the clean 1000 cadence.

### This resolves §15's 2x overflow gap

§15 recorded an unexplained discrepancy: 27.6 GiB of overflow pages against a
sampled checkpoint mean implying only ~13.8 GiB of snapshots. The cause is the
checkpoint count. §15 assumed the 1000-slot cadence and arrived at ~2,235
checkpoints; there are **4,094**. 4,094 / 2,235 = **1.83x**, which is the gap.
The sampled mean was not the main error — the denominator was.

### Corrections to earlier figures in this file

- §14: "It did confirm **zero `match@` keys**, consistent with
  `match-index: false`". **Wrong.** `elmdb:list(DB, <<"~">>)` — safe to
  enumerate, since base64url never contains `~` — returns thousands of
  `match@1.0&<hash>=data` keys alongside `scheduler@1.0`. **[M]** They are a
  derived reverse index, and the collector drops them.
- §15: "1,985,934 computed slots". Measured **1,985,938**. Assignment slots
  confirmed at 1,985,957 exactly.
- §15's 2.6x on-disk amplification does not hold for the retained set. On one
  real collection (below) it is **1.363x**, closer to §16's 1.3x.
- `~scheduler@1.0` has exactly one child, `assignments` **[M]**, so the root set
  covers the whole ledger namespace.

### What the 1-day policy actually retains on this corpus

Counted, not projected — every computed slot classified, every process's window
start derived from assignment timestamps: **[M]**

| quantity | value |
|---|---|
| computed slots | 1,985,938 |
| assignment slots | 1,985,957 |
| retained: inside the window | 855,469 |
| retained: checkpoints below the window | 1,965 |
| **dropped** | **1,128,504 (56.8%)** |
| corpus timestamp span | **2.40 days** |

**56.8% is not the steady-state figure and must not be quoted as one.** The
corpus holds 2.40 days of history, so a 1-day window can only reach 57% of it by
arithmetic, and `keep_floor = 1000` holds a floor under every process regardless
of age. Several processes were still live when the corpus was cut and retain
their entire history. On a store that has run for weeks the same policy drops
proportionally more; §16's steady-state estimate of ~51 GB fixed for a 1-day
window is the number to plan capacity on, not this one.

### The retention window has a second job, and it is the one that can break reads

A time window alone is not safe. `dev_process_cache:materialize/3` walks a delta
chain backwards one `base-slot` at a time and stops at the first non-delta entry,
and `dev_process_cache` hard-matches, so a window whose *bottom* slot is a delta
whose base was dropped fails to read rather than degrading. `collect/3` therefore
lowers the window start to the nearest slot holding a full state before
retaining, and if the search cap is reached it falls back to the process's
**lowest** slot -- never the lowest slot searched. 161 of 370 processes carry
exactly one full state (slot 0, forced by `should_checkpoint/4`), which is the
shape that finds this bug.

### Round trip, measured

Over **232 of the 370 processes** (the portion of the copy that had completed;
the rest was cut for wall-clock, not for any failure): **[M]**

| check | result |
|---|---|
| process definition resolves | **232 / 232** |
| latest retained slot readable | 223 / 232 -- the 9 shortfalls are the 9 processes that have **zero** computed slots in the source, so nothing was lost |
| **assignment slots, source** | **1,211,993** |
| **assignment slots, collection** | **1,211,993** -- equal per process, not just in total |
| sampled assignment reads byte-identical | 657 / 657 |
| sampled in-window states byte-identical | 648 / 648 |
| sampled dropped slots read cleanly | 228 / 228, **0 crashes** |

And not sampled: **every** assignment of two whole processes compared field for
field through `dev_scheduler_cache:read/3` + `ensure_all_loaded` +
`term_to_binary` -- **571 / 571** and **16,162 / 16,162** byte-identical, zero
differences. **[M]**

The collector's own counters agree: across the batches that reported them,
`unfollowed_link_keys` is **0**, `drop_checkpoint` is **0** and `drop_unknown` is
**0** -- no VM snapshot was ever dropped and no slot was ever unclassifiable. The
only `misses` are the `computed/<id>` and `computed/<id>/slot` group markers of
the 13 processes that never computed a slot, which are absent from the source too.

`dev_process:rewind/4` fired for the first time and worked -- see
`SPEED-UPGRADES.md` Item 5 for the traces, timings and the byte-for-byte
comparison of the re-executed states.

### Size

The source was never written. `data.mdb` and `lock.mdb` in the corpus still carry
their 2026-09-26 mtimes, because `read-only => true` opens the environment with
`MDB_NOLOCK` and nothing in the collector calls a write. `hyperbeam-prod` was not
restarted or reconfigured at any point. **[M]**

| | pages used | free | **live** |
|---|---|---|---|
| corpus (§15) | 39.0M -- 159.9 GB file | 5.9M / 21.8 GiB | **127.1 GiB** |
| collection, 232+ processes | 18,354,971 | 3,589,171 / 14.7 GB | **60.5 GB** |

The collection's free pages are one abandoned batch: LMDB never shrinks, so a
killed run's pages stay in the file and return to the free list. **Quote the live
figure, not the file size.**

Extrapolated over the whole corpus that is **~80 GB against the corpus's
136.5 GB, about 41% reclaimed** -- and that ratio is a property of *this corpus*,
not of the policy. The corpus holds 2.40 days; a 1-day window can only reach the
older 1.4 of them. §16's model is the one to plan on: a 1-day window costs a
**fixed** ~51 GB of deltas and anchors however long the node runs, plus the ledger
at 2.7 GB/day and the checkpoints. The reclaim ratio grows with the age of the
store.

### Operational notes for whoever runs the swap

- **Copying is bound by random-read latency at queue depth one, not by bandwidth
  or CPU.** A serial pass held the NVMe mirror at **8.7k IOPS / 68 MB/s at 73%
  utilisation with 29% of one core** **[M]** -- every assignment costs ~10 random
  reads issued one at a time. `workers => 12` gets **69k IOPS / 355 MB/s,
  `aqu-sz` 6.0, 99% utilisation**: an **8x** speedup on the same hardware, which
  is the difference between most of a day and under an hour. **[M]** The unit of
  concurrency is one whole process, so no two workers share a retention
  decision; the default is still 1. `collect/3` also takes a `progress' fun,
  because a multi-hour pass with no way to watch it is a pass an operator kills
  on suspicion.
- **Re-execution writes its results back.** A cold historical read re-caches
  every slot it replayed. The collector bounds growth from *scheduling*; it does
  not bound growth from archival reads.
- **The collector's own memory is the thing that will bite an operator**, and it
  took four failed whole-corpus runs to bound it. In order: the visited set held
  across the run (**RssAnon 20.7 GB + 4.6 GB swapped at 167 of 370 processes**);
  the same set keyed by 45-80 byte paths instead of digests (**~430 B/entry**);
  a set recreated per process rather than cleared, whose freed carriers never
  returned (**36 GB at 236 of 370**); and finally the write path itself
  (**15 GB in 13 minutes inside one large process**). Only the last is
  interesting: `elmdb:put/3' queues into a Rust overlay drained by a background
  worker, so a copy with no backpressure produces rows faster than LMDB commits
  them, and `elmdb:read_prefix/2' returns one packed buffer per subtree whose
  sub-binaries pin it until a collection runs. A periodic `elmdb:flush/1' plus
  `erlang:garbage_collect/0' holds the same work at **155 MB**. **[M]**
- **What ruled the alternatives out**, since three of those four were guesses:
  sampling `elmdb:overlay_count/1', `erlang:memory(binary)', `(processes)' and
  `(ets)' every 20 s through a four-worker run. The overlay oscillated between 8
  and 8,585 entries, binary stayed at 0.01 GB and processes at 0.15 GB; only
  `ets' climbed. Measure before changing anything here.
- **Run it in batches.** A fresh VM per slice of the process list carries nothing
  over and makes the pass restartable, which after four memory failures is worth
  more than the page cache it gives up. `collect/4' takes the process list.
- LMDB never shrinks, so a copy that runs the disk out leaves a file that cannot
  be reclaimed except by deleting it. `collect/3` checks free space between
  processes against `min_free_bytes` (64 GiB default) and stops, flushing first.
- The source must carry `read-only => true` and `access => [<<"read">>]` or
  `collect/3` refuses to start, and it refuses if source and destination name the
  same store.

## 19. Built: delete primitive, essentials store, online retention (2026-10-07)

Branch `feat/retention` (base `fix/audit-all` = prod image `audit-fixes-20261006c`).
Everything is off by default; with no new opt set the node behaves as before.

### Pieces

| piece | where | what |
|---|---|---|
| delete primitive | `patches/elmdb-delete.patch` (applied in `Dockerfile.prod` after `get-deps`), `hb_store:delete/3`, `hb_store_lmdb:delete/3` | `delete_batch/2`: the write worker commits the overlay and deletes in one txn, so a pending put is never resurrected. `delete_batch_guarded/2` + `track/2`, `track_watch/2`, `track_take/1`: vetoed atomically if a key was written or referenced since tracking began. `scan_refs/3`, `scan_watched/3`, `scan_units/3`, `scan_unit_refs/3`, `scan_rows/3`: bounded key-order scans in short read txns, Rust-filtered. |
| essentials store | `hb_store_essentials`, routing in `dev_scheduler_cache`, `dev_location_cache`, `dev_bundler_cache`, `dev_arweave_block_cache` | `essentials-store`: written first, self-contained (`hb_cache:write` puts the whole graph there); node store list gains it read-only after its first store. `migrate/3` + `verify/3`. |
| export | `hb_store_export` | `essentials-export`: local journal -> few-MB segments shipped async to a (slow, remote) path; backlog bounded, base image closes gaps; backoff on EIO/ETIMEDOUT/ESTALE; fill ceiling (default 75%) and byte budget; `manifest.log`; `restore/3`. |
| retention | `hb_store_gc:retain/1` (+ background server) | per-process window, guarded alias-then-content sweep, protection scan + write tracking, journal for crash recovery, opt-in orphan pass, optional checkpoint archive. |

### Namespace classification (census of real stores)

Full census of `archives/reset-20261001T195924Z` (32.3M entries) and a
`~`-namespace skip-scan plus 20,000 random probes of `store.pre-wipe-20261006`
(201 GB) found only four top-level classes: 43-byte IDs, `data/`, `computed`,
`~scheduler@1.0` (only `assignments`).

| namespace | class | retention |
|---|---|---|
| `~scheduler@1.0/assignments`, `/uploaded` | essential | never deleted; written to the essentials store when configured |
| `<ProcID>` (definition) | essential | pinned |
| trusted-device IDs (`token@1.0`, `process-outbox@1.0`, `security@1.0`) | essential | pinned; copied by `migrate/3` |
| `~location@1.0`, `~bundler@1.0`, `~arweave@2.9` | essential (small) | routed to the essentials store; never deleted |
| `~meta@1.0` (`preloaded-devices-index`) | essential (small) | never deleted (normally in the preloaded store) |
| `computed`, `computed/<P>`, `computed/<P>/slot` markers | structure | kept |
| `computed/<P>/slot/<N>`, `computed/<P>/<Root>` | derived | dropped outside the window |
| 43-byte IDs, `data/<h>` | content | deleted only when reached from a dropped slot (or, opt-in, orphaned) AND referenced by nothing outside the candidates |
| `~match@1.0&...` | derived index | not collected (grows ~9 rows/slot when `match-index` is on; prod has it off) |
| anything else | unknown | kept; its rows protect what they reference |

### Steady state (in-VM, lua@5.3b, cadence 50, recent 32, essentials store on)

| slots | main rows, off | main rows, computed only | main rows, computed+orphans | essentials rows |
|---|---|---|---|---|
| 300 | 25,736 | 12,675 | 12,675 | 15,074 |
| 1,500 | 128,768 | 36,675 | 12,675 | 75,074 |
| 3,000 | 257,558 | 66,675 | 12,666 | 150,074 |

Off: 86 rows/slot. Computed-only: 20 rows/slot -- the offloaded copies
(`hb_link:normalize/3` writes every converted message's nested messages to the
node store). With orphans: flat. Essentials: 50 rows/slot, forever.

### POST /schedule with a throttled export target

Node on tmpfs, export target on a disk throttled to 256 KB/s and 20 IOPS, 16
clients, pre-signed requests, 20 s: base 93.6 req/s p99 207 ms vs export
91.0/s p99 212 ms (repeat: 101.8/194 vs 97.8/361; 81.0/436 vs 83.2/454 --
host noise); backlog 31.5 MB at the end, drained in 117 s, 0 dropped, 0
errors. `hb_store_fs` directly on the same throttled dir: 0 of 16 requests
succeeded in 20 s (all `scheduler_timeout`).

Real mount (`/mnt/pxe-backup`, NFS 4.2 over WireGuard, ~115 ms RTT): 3,000
messages, 4 segments of 2 MB + a 6.9 MB base = 14 MB shipped in 12.9 s, restore
in 0.7 s, all 3,000 messages identical.

### Limits

- The protection scan reads every reference row of the main store once per
  batch of `store-retention-batch-slots`. Bounded when the essentials live
  elsewhere; with them in the main store it grows with the ledger.
- Orphan collection deletes content reachable only by ID; opt-in for that
  reason.
- A checkpoint archive restore is an offline tool; a process with fewer than K
  snapshot checkpoints keeps everything from its oldest one.
- LMDB never shrinks: expect a plateau at the high-water mark, not a smaller file.

### After the independent review (2026-10-07)

Fixed: aliases and late references (slot aliases and unit aliases watched,
overlay flushed before every scan, alias rows swept with their unit, elmdb
records the key of any put that references a watched key, and tracks after
the overlay insert); exporter unclean stops (gap mark + forced base image,
restore refuses an uncovered gap); manifest strictness (torn tail repaired,
unlisted files are errors). The reviewer's tests live in
`hb_store_gc_rev_tests` (fuzz with `HB_RETENTION_FUZZ=<ms>`).

**Prod values:** `store-retention-grace-ms` 120000 (default); the FIRST run
with `store-retention-dry-run` true; `store-retention-orphans` false until a
week of clean computed-only retention.

### After the stage swarm run (2026-10-07)

- **Node froze 82-148 s after restarts / during bases.** Cause: the export's
  file calls used the plain `file` API, which every process shares through
  one file server process; one call stuck on the NFS mount queued every
  other file operation in the node (the `hb_store_fs` store reads fall
  through to, code loading). And the first write after a start waited up to
  30 s, inside a `global:trans`, for the exporter to list and stat the target.
  Fixed: raw/prim_file only, one target worker, writer local-only, exporter
  registered before it initialises, CRC while writing, no read-back. With a
  target blocked forever: old -- first write 30.0 s and an unrelated
  `file:read_file` never returned; new -- start 0.6 ms, write+sync p99 0.5 ms,
  unrelated read 30 us; at node level (HTTP schedule + lua compute, 60 s)
  schedule p99 313-720 ms vs 541-636 ms healthy, compute p99 1.5-1.8 s vs
  1.4-2.3 s healthy (same host noise).
- **Restarts no longer need a base.** Clean stop (docker stop -> SIGTERM ->
  init:stop -> hb_app:stop) is recorded; verified in a container: after
  `docker stop` the next start does nothing; after `docker kill` it catches up
  from per-process assignment watermarks (no base) and restore has all 200
  assignments. A completed base prunes everything older (verified: two
  resyncs leave exactly one base).
- **Retention run cost.** The live 1,104 s run was 8 batches x 120 s grace
  (960 s) + ~144 s of passes. Now: one grace per run, chunks bounded by
  `store-retention-max-candidates` (2M rows), a scheduled run is one chunk:
  one Rust-filtered pass over the main store's reference rows plus one
  Rust-filtered alias pass. The pass is O(main store), which retention keeps
  bounded; offline pass rate 3.1-3.4M rows/s. A first run over a big store
  takes one pass per 2M candidate rows.
- **Essentials volume** (game-like ans104 messages, 1.3 KB on the wire):
  90 rows and 8.7 KB of rows per slot in the essentials store (3.3x the
  2.6 KB logical assignment: field explosion, two commitments, the
  `original-tags' list), 15.8 KB/slot of LMDB file. The export journal was
  17.6 KB/slot raw; deduplicated within a segment 10.1 KB, gzipped
  **2.8 KB/slot** (6.0x). At stage's ~9 slots/s (~780k/day): export
  13.7 GB/day before -> 2.2 GB/day, local essentials LMDB ~12 GB/day. A 1 TB
  mount at the 75% ceiling lasts ~340 days at that load.

## 20. Essentials retention (`essentials-retention-days`)

Unset (the default) keeps the essentials store forever. Set to N (prod: 7),
each retention run (same schedule, after the main-store sweep, same
`store-retention-dry-run`, delete pacing and grace) prunes from the
essentials store the assignments older than N days (`timestamp`, else
`block-timestamp`; neither = kept) and the closure rows nothing else
references, via the same journaled, guarded mark/protect sweep. Never pruned:
definitions (pins), small namespaces (never candidates), anything another row
references, each process's head slot, every slot from the oldest checkpoint the
main store keeps for it (a process with no kept checkpoint keeps everything),
at most `essentials-retention-max-slots` (100000) per run. With the export
on, nothing goes until the target carries `local-pruned` (after which bases
supersede nothing and restore replays the whole chain from the newest base
before the marker), and only slots at or below the process's mark as of the
newest shipped segment. Report: `essentials` in the run report /
`retention_status()`.

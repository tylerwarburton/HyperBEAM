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

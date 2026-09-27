# perf(process): cheap state-only anchors, decoupled from VM snapshots

## This PR is stacked on `feat/lua-5.3b-delta-state`

**It must land after that branch.** It cannot target `edge`.

`origin/edge` and `fork/edge` carry **no delta machinery at all** — no
`dev_process_cache:write_delta/5`, no `dev_process_cache:materialize/3`, no
`src/core/util/hb_process_delta.erl`. There is nothing on `edge` for an anchor to
be an alternative to: without deltas every slot is already a full write, so a
"cheaper full write at a tighter cadence" is meaningless there.

Base commit: `08c1268da` (`feat/lua-5.3b-delta-state`, *feat(lua): add
request-only delta process state*).

### What this PR depends on from that branch, by name

| Depended-on piece | Where | Why this PR needs it |
|---|---|---|
| `dev_process_cache:write_delta/5` | `src/preloaded/process/dev_process_cache.erl` | The anchor case is a third branch inside it, between the checkpoint branch and the delta branch. |
| `dev_process_cache:should_checkpoint/4` | same | Decides checkpoint-vs-not. The anchor cadence is only consulted after it says no, so a slot due for both gets the checkpoint. It also promotes any snapshot-carrying state to a checkpoint, which is what keeps a VM image out of an anchor. |
| `dev_process_cache:materialize/3` and the `cache-format` tag convention | same | Anchors reuse the same dispatch: `process-anchor@1.0` alongside `process-delta@1.0`. |
| The backward-linked delta chain (`base-slot` = `slot - 1`, walked via `read/3`) | same | This is what an anchor shortens, and it is why "stop at the nearest anchor or checkpoint" needs no new code (see below). |
| `hb_process_delta:apply/5` and `validate/2` | `src/core/util/hb_process_delta.erl` | Used by the new tests to build the states that get written. |
| `dev_process:should_snapshot_delta_slots/2` (`process-delta-checkpoint-slots`) | `src/preloaded/process/dev_process.erl` | The checkpoint cadence the anchor cadence sits *inside*. |

## What it does

A **state-only anchor**: the full public process state with the `snapshot` key
omitted, written at a tighter cadence than full checkpoints, so a historical read
replays a shorter delta chain.

- new opt `process-delta-anchor-slots` (0 disables; default 0 — see below)
- new cache-format tag `process-anchor@1.0`, distinct from `process-delta@1.0`
  and from a bare checkpoint (which carries no tag)
- `write_delta/5` gains the anchor case between the checkpoint and delta branches
- the state lives in an envelope (`cache-format` / `slot` / `state`) rather than
  at the slot root the way a checkpoint does, so `cache-format` never becomes a
  key of process state

### "Stop at the nearest anchor or checkpoint, whichever is closer" needed no code

`materialize/3` does not scan for a base. It walks one backward link at a time
through `read/3`, so the first entry that is not a delta terminates the walk —
which is by construction the nearest of {anchor, checkpoint}. The new clause just
has to return the anchor's state instead of applying a patch set. This is worth
saying explicitly because the spec describes it as a change to the walk.

## The safety property, and the test that pins it

**An anchor has no VM snapshot, so it must never be selected as the state to
resume from.** If it were, the next slot would execute against a freshly
initialised Lua VM and the process's state would silently reset — wrong answers,
no error anywhere.

This falls out of existing code rather than needing a new guard:
`dev_process:ensure_loaded/3` asks
`dev_process_cache:latest(ProcID, [<<"snapshot+link">>], TargetSlot, Opts)`, and
`first_with_path/5` selects a slot only if `computed/<id>/slot/<N>/snapshot+link`
resolves in the store. An anchor's envelope has no `snapshot` key at any level,
so it has no `snapshot+link` row and is invisible to that lookup.

`anchor_never_resume_base/1` pins it, arranged so that it would fail if the
lookup were ever slot-ordered rather than path-filtered:

- slot 0 is the only checkpoint and the only slot carrying a snapshot
- anchors at slots 2 and 4 are the **newest full states in the cache**
- `latest(ProcID, [<<"snapshot+link">>], …)` must still return slot 0 — both
  unlimited and with an explicit limit of 4, the shape `ensure_loaded/3` uses
  when a request names a target slot
- and neither the anchor envelope nor the state inside it holds a `snapshot` at
  all, so there is nothing for a future lookup to latch onto
- while the unqualified `latest/2` still returns the head, so anchors have not
  broken ordinary reads in the course of being invisible to the resume path

Two further tests: `anchor_roundtrip/1` (slot 3 is tagged `process-anchor@1.0`,
slot 2 is still `process-delta@1.0`, slot 6 is still a bare checkpoint; the
anchor materializes as full public state; a delta stacked on an anchor replays
from it; an anchor is addressable by logical ID) and `anchor_cadence_test/0`
(0 disables, multiples only, a garbage value errors rather than silently
disabling anchors).

## The measured anchor size, and why the default is 0

The spec this implements reasoned that an anchor is "~3-5 MB by subtraction, but
`process` and `module` dedupe, so the true marginal cost is unknown — if it is
~1 MB, ship every-250". **Measured, it is not ~1 MB.**

Method: `hb_store_lmdb` batches writes asynchronously, so measuring growth
between steps inside one store attributes bytes to the wrong step (a first
attempt credited the checkpoint's 18.9 MB snapshot to the delta written after
it). So instead: one fresh LMDB store per scenario, byte-identical content in
every scenario, `hb_store:stop/1` to force the flush, then `du` of allocated
blocks. Scenarios differ by exactly one slot's kind, so the difference is that
slot's cost. State shape reproduces a real production checkpoint — 1,629 fields,
an 18,876,953-byte VM snapshot, `process` 502,852 B, `module` 500,603 B,
`economystate` 178,802 B, `economy` 54,470 B, `leaderboard` 22,133 B,
`factions` 16,131 B.

**Control first:** the synthetic full checkpoint measured **25,436,160 B**
against the real one's **24,146,646 B**. The model is calibrated, so the anchor
number taken from it means something.

| public state | per anchor | full checkpoint | anchor / checkpoint |
|---|---|---|---|
| 1.0 MB | **3,951,616 B** | 25,436,160 B | **0.16** |
| 4.3 MB | **6,223,872 B** | 28,676,096 B | **0.22** |

(Per-anchor figures are from four anchors written against four deltas in
otherwise identical stores, which also lifts the signal clear of `du` page
rounding. A single-anchor run agreed independently: 3,964,928 B at 0% churn,
4,460,544 B at 25%, 6,561,792 B at 100%.)

### Cadence arithmetic, per 1000 slots

| cadence | anchors / 1000 slots | added bytes (1.0 MB state) | vs the window's 25.4 MB checkpoint | worst-case replay |
|---|---|---|---|---|
| 250 | 4 | 15.8 MB | **+62%** | ~896 → ~250 deltas, 115 s → ~32 s |
| 500 | 2 | 7.9 MB | **+31%** | ~896 → ~500 deltas, 115 s → ~64 s |
| 0 (default) | 0 | 0 | — | unchanged |

For a 4.3 MB public state, cadence 250 costs 24.9 MB per 1000 slots — **+98%**,
roughly doubling the window's bytes. That is exactly the case the spec said
should loosen the cadence rather than ship 250.

**So the default is 0.** The mechanism ships, off. An anchor is 16-22% of a
checkpoint rather than the ~4% the "1 MB of game state" framing implies, the
right cadence depends on the operator's disk budget, and there is no delete
primitive anywhere in the stack to undo the choice later. A node with the bytes
sets 500 or 250 deliberately; a node that does nothing sees no change at all.

One honest caveat on the figure: writing a *second* anchor whose large values are
byte-identical to the first costs ~0 (measured −0.47 MB and +0.57 MB for a second
and third — noise around zero). Content-addressed dedupe does work. So
3.95-6.2 MB is an upper bound, and a process whose public state barely moves
between anchors will pay less. It is the right bound to set a default from.

## Testing

Compiled and run in a container off the production image, `-pa /tmp` first so
the branch module shadows the image's. **Baseline (unmodified branch source,
same harness): 3 tests, all passed. After: 6 tests, all passed. Zero new
failures.**

Pre-existing, not introduced here: `process_cache_suite_test_/0` generates **0**
tests. `hb_store:generate_test_suite/2` is handed
`[{Name, Opts} || {Name, Opts} <- hb_store:test_stores()]`, but `test_stores/0`
returns a list of **maps**, so the comprehension matches nothing and the whole
shared-store suite is silently empty — including the existing
`"delta roundtrip and checkpoint"` entry. The two new suite entries are
registered for when that is fixed; the coverage that actually runs today comes
from the standalone `anchor_roundtrip_test_/0` and
`anchor_never_resume_base_test_/0` wrappers, against a real LMDB store.

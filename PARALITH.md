# Paralith on HyperBEAM v0.11

This branch (`paralith/v0.11`) is upstream HyperBEAM v0.11-RC7 with Paralith's
node work rebuilt on top of it, one change at a time, each measured.
`upstream/v0.11-rc7` is the untouched upstream tree; `git diff
upstream/v0.11-rc7` is everything that is ours.

The previous fork (`hyperbeam-upgrades`, 44c016ae, on upstream 5142ab951) is
the source being ported from. It stays the production node until this branch
replaces it on a fresh deploy.

## Device and module names

Renamed on the move. The old names are not kept as aliases: processes that use
them belong to the old node and are replaced at the next fresh deploy.

| Old | New | What makes it different |
|---|---|---|
| `lua@5.3b` | `lua-patch@1.0` | State stays resident in the Lua VM; Lua receives only the request and returns an ordered patch list, not the whole state |
| `lua@5.3c` | `lua-atomic@1.0` | `lua-patch@1.0` plus all-or-nothing in-VM batches and a mandatory strict sandbox |
| `bridge@1.0` | `bridge-attestor@1.0` | This node as one k-of-n attestor of the PUSD bridge (Base, Solana, Sui) |
| `eth-client@1.0` | `evm-rpc@1.0` | EVM JSON-RPC client used by the attestor |
| `hb_store_essentials` | `hb_store_irreplaceable` | Separate store for what the node can never regenerate: assignments, the messages they carry, process definitions |
| `hb_store_export` | `hb_store_offsite_journal` | Wraps a store and journals every write, append-only and resumable, to a slow or remote target |
| `hb_store_gc` | `hb_store_retention` | Retention planning: which computed slots can be collected |
| `hb_process_delta` | `hb_state_patch` | The patch-list format shared by `lua-patch@1.0` and the process cache |

## Node config the port requires

Settings our v0.11 nodes run with, each one decided or measured:

| Key | Value | Why |
|---|---|---|
| `scheduler-publish-remote` | `false` | RC7 otherwise uploads every message and assignment to Arweave inside the scheduler loop (8 msg/s) |
| `spawn-worker` | `true` | process workers on (decided 2026-10-26) |
| `process-workers` | `true` | group process computes under one worker |
| `process-worker-spawn` | `true` | without it no process worker ever starts on RC7 |
| `scheduler-lookahead` | `false` | with workers live, lookahead waits 1.5 s per request |
| `process-snapshot-target` | `false` | no whole-VM snapshot per message once a worker holds the VM (decided 2026-10-26) |
| `match-index` | `false` | RC7 indexes every key/value for `~match@1.0`, which we do not use: 23 of 100 rows a message. Off: 23.7 -> 18.1 KB/msg, 84 -> 78.5 ms |

## Port order

Safe and small first. Every step: port, run its tests, re-run the bench, record
the numbers below with the reason they moved.

1. **Build and ops**: prod Dockerfile, rebar profiles, `luerl` incremental GC
   patch, `elmdb` patch rebased on RC7's elmdb pin.
2. **Scheduler off the upload path**: RC7 uploads every message and its
   assignment to Arweave inside the scheduler loop, which serialises at
   ~120 ms per message (8 msg/s). Make the upload asynchronous or off.
3. **`lua-patch@1.0`**: port `dev_lua_53b` + `hb_state_patch`. Keep its
   constant `excludes` list (`lua@5.3a` still lists every published key, the
   per-key cost in the monorepo's CLAUDE.md). Inherit RC7's `dev_lua` fixes:
   node wallet kept out of snapshots (`put_private`), clean errors for
   unexpected return shapes.
4. **`lua-atomic@1.0`**.
5. **Worker, process cache, `hb_persistent`**: each measured on its own.
6. **Push**.
7. **Stores**: crash-safe LMDB sync, `hb_store_irreplaceable`,
   `hb_store_offsite_journal`, `hb_store_retention`. RC7 rewrote the storage
   layout (no links or groups), so these are rewritten, not copied.
8. **`bridge-attestor@1.0`**, `evm-rpc@1.0`, chain signing. Carry the
   2026-10-26 polling fix (uncommitted in the old fork and monorepo when
   noted): 2-minute idle scan, nudges still give 1 s scans for 2 minutes, a
   fixed 15 s nudge cooldown, one fewer `eth_blockNumber` per Base scan in
   `dev_eth_client`. It ships with the next node deploy either way.
9. **Clients and contracts** (monorepo): httpsig keyids are base64url, and
   the committer is the `publickey:` URI, not an address, so `signer()` has to
   map it. Lands with the fresh deploy.

Already better in RC7, so we do not port our version: the `hb_escape` header
escaping, the `hb_name` delete fix, RSA CRT signing.

## Speed log

One trivial Lua counter process, ANS-104 signed, bench node on the paralith
box (shared with the attestor, 6 CPUs at low priority), config
`{"port": 8734, "bundler-ans104": "http://127.0.0.1:9"}`. Round trip is sign
to changed state back. Compute is one `now` over a 500-message backlog.

| Date | Build | Round trip | Schedule /s | Compute /s | Why it moved |
|---|---|---|---|---|---|
| 2026-10-26 | old fork 44c016ae | 96-130 ms | 31-66 | 87-94 | baseline |
| 2026-10-26 | RC7 stock config | 170 ms | 8 | 62 | scheduler uploads to Arweave inside its loop |
| 2026-10-26 | RC7, uploads off | 95 ms | 43 | 67 | baseline for this branch |

Storage and memory, same counter process (store size inside the container
before and after 500 messages; container memory over 1,000 messages):

| Date | Build | Store per message | Memory |
|---|---|---|---|
| 2026-10-26 | old fork 44c016ae | 13.6 KB | 131 -> 192 MiB |
| 2026-10-26 | RC7 + scheduler fix | 21.5 KB | 137 -> 165 MiB |

The counter's own state is a few bytes, so this is all overhead: the message,
its assignment, commitments and a computed state per slot.

`process-snapshot-target` (unit 2), measured BEFORE unit 3, so without live
workers. 200 sequential messages, each read back (the live-traffic shape),
two runs each:

| Setting | Store per message | Round trip | Sequential |
|---|---|---|---|
| `true` (RC7 default) | 24.3 KB | 92 ms | 10.9/s |
| `false` | 26.1-26.8 KB | 107 ms | 8.9-9.5/s |

Off is worse here: with no worker holding the VM, each request rebuilds its
state, and the forced snapshot is what makes that cheap. Leave it on until
re-measured with units 3 and 4 (live workers), where it should be pure cost.
A burst of 500 computed in one `now` cannot show this (only its last slot is
a target): 22.4 KB vs 22.3 KB.

Every step is judged on speed, reliability, memory and storage. A step that
improves one and costs another goes to Tyler before it lands.

After units 1-4, measured again with `spawn-worker` and `process-workers`
on: no change from before (round trip ~90 ms, 24 KB per message). A trace
showed why: **no process worker ever starts on RC7.** `spawn-worker` is a
temporary option that `hb_ao` strips from every nested execution, and a
process compute is always nested (inside `now`, a push, the scheduler). The
old fork got live workers from compute-on-schedule (unit 12) and from handing
compute to a worker before execution (unit 5). RC7's hb_opts fallback is its
built-in defaults and environment only, not the node config.

Fixed by an opt-in `process-worker-spawn` (committed): process computes
carry `spawn-worker`, verified with a trace (one worker started, every request
delegated to it). It needs `scheduler-lookahead => false`: with lookahead on,
a live worker waits 1.5 s (`LOOKAHEAD_TIMEOUT`) for a slot that does not exist
yet, and the round trip went 90 -> 1,590 ms. Our node configs already set it.

With workers live, 200 sequential messages, two runs each:

| `process-snapshot-target` | Store per message | Round trip |
|---|---|---|
| `true` (RC7 default) | 24.0-24.1 KB | 87 ms |
| `false` | 24.0 KB | 84-85 ms |

Off no longer costs anything once a worker holds the VM. This counter's
snapshot is tiny, so the ~24 KB per message is other overhead, still to be
attributed. For the game, whose snapshot is megabytes, off should matter a
lot.

Every bench now prints `worker_starts`, so a run without workers is visible.

Where a message's storage goes on RC7 (LMDB census before and after 200
messages, workers live, snapshot-target off): 12 KB and 100 rows a message,
logically (~24 KB on disk with B-tree overhead). By row kind:

| Rows/msg | Bytes/msg | What |
|---:|---:|---|
| 23 | 2,785 | `~match@1.0` reverse index |
| 21 | 2,542 | each signed message's `committed` key list, one row per key |
| 3 | 1,726 | message data blobs |
| ~40 | ~3,500 | each commitment's fields (signature, committer, type, owner, keyid, ...), one row each |
| rest | | assignment, computed-slot links, hashpaths |

The counter's own state is a few bytes of this. RC7 stores every field of every
message and commitment as its own row; that layout is what the storage unit has
to change. Tool: `bench/census.erl` (run via `hb eval`), diffed per row kind.

## The real game on RC7 (2026-10-26)

Game on `lua-patch@1.0`, 28 fixture players, the port's required node config,
`contract-latency.mjs --game-phase basic`. The monorepo client signs with
`HB_HTTPSIG=v011`.

Before push spawned a worker, every slot was computed twice: once as its own
target, then again by the next request, which started from the state before
it (`slot N, target N+1`, store 150-185 ms). Each request was a new process;
no worker was used. With `process-worker-spawn` covering push's compute too,
the game reached 43 slots with 44 patch applications: each slot computed
once.

Round trip per action, same harness, same SSH tunnel, 5 samples (absolute
numbers include the tunnel and the harness's 250 ms poll):

| Action | Old fork, bench config | RC7 branch |
|---|---:|---:|
| `User.Info` | 1,126 ms | 656 ms |
| `Sprite.Update` | 1,144 ms | 690 ms |
| `Monster.LevelUp` | 1,253 ms | 820 ms |
| `Monster.Feed` | 1,172 ms | 898 ms |
| `Market.List` | 1,223 ms | 862 ms |
| `Market.Cancel` | 1,222 ms | 861 ms |

Against the old fork running the test node's real config (rendered/test.json
without its host and bridge hook), polling every 25 ms, two alternating runs:

| Action | Old fork, test-node config | RC7 branch |
|---|---:|---:|
| `User.Info` | 1,108-1,114 ms | 715-915 ms |
| `Sprite.Update` | 1,115-1,118 ms | 820-825 ms |
| `Monster.Feed` | 1,112-1,134 ms | 767-848 ms |
| `Monster.LevelUp` | 1,131-1,140 ms | 870-884 ms |
| `Market.List` | 1,179-1,203 ms | 956-969 ms |
| `Market.Cancel` | 1,228 ms | 921-974 ms |

RC7 is 15-30% faster on every action. The ~700+ ms floor is the WAN tunnel
times the several requests each action makes; it is the same on both. A worker's slots print no
`computed_slot` line (its options drop `debug-print`), so per-phase node
timing for worker-computed slots is still to do.

Where a game slot's time goes inside the worker (dbg trace of outermost calls,
70 game slots, RC7 branch). **Inflated, do not use:** dbg copies a traced
call's arguments into each trace message, and these phases take the whole
process state as an argument, so the trace itself costs most of what it
measures. Superseded by `hb_phase_timer` (below):

| Phase | Per slot |
|---|---:|
| store (`hb_cache:write`, whole state each slot) | 54 ms |
| Lua (`luerl:call_function_dec`) | 48 ms |
| patch apply (`hb_state_patch:apply`) | 28 ms |
| `hb_message:normalize_commitments` | 18 ms |
| total | ~148 ms |

Order of attack: delta persistence (store), then patch apply (each patch goes
through the AO-Core resolver), then the contract's own Lua (offline it is 5-10
ms; Luerl runs it at ~48). Across all request processes,
`normalize_commitments` was called 53,092 times in one run: it adds an
unsigned (hmac) commitment to every nested message that has no signed one,
hashing each.

Per game slot, measured with `hb_phase_timer` (no tracing), 72 game slots,
two alternating runs per build:

| Phase | Before patch fast path | With it |
|---|---:|---:|
| store (`slot_store`) | 50-52 ms | 50-51 ms |
| Lua (`lua_patch_lua`) | 32-33 ms | 31 ms |
| patch apply | 1.0-1.1 ms | 0.25-0.45 ms |
| decode | 1.5 ms | 1.5 ms |
| prep | 1.2 ms | 1.2-1.5 ms |
| whole execution | 44-45 ms | 41-42 ms |

Store is the largest cost of a slot: RC7 writes the whole process state every
slot. Next is delta persistence.

RC7's built-in benchmarks on the same box: scheduler alone 258 msg/s (no
compute, no HTTP; this is where "200-400 msg/s" comes from), pure Lua process
46 slots/s, aos process 20 slots/s.

Across processes the old fork scales: 1 process 99/s, 4 processes 251/s total.

## Where the real game spends a slot

Old fork on the bench node, game on `lua@5.3b`, 28 fixture players, node's own
`computed_slot` timings (`contract-latency.mjs --only game --game-phase basic`
in the monorepo). A trivial Lua slot is ~11 ms (94/s), so anything above that
is the contract.

| Handler | Execution | Store | Total | Per process |
|---|---|---|---|---|
| `User.Info` (read) | 10-12 ms | 2-3 ms | 12-15 ms | ~70/s |
| `Sprite.Update` | 18 ms | 3 ms | 21 ms | ~48/s |
| `Monster.Feed` | 20 ms | 5 ms | 27 ms | ~37/s |
| `Monster.LevelUp` | 28 ms | 7 ms | 35 ms | ~29/s |
| `Market.List` | 79 ms | 8 ms | 86 ms | ~12/s |
| `Market.Cancel` | 112 ms | 8 ms | 119 ms | ~8/s |

Execution, not store, is where it goes. The same handlers measured as pure
Lua offline (`run-local-profile.mjs 28 30`, WASM Lua, so indicative only):
`User.Info` 4.9 ms, `Monster.Feed` 10.6 ms. Our Lua is about half of a slot,
not a rounding error. The market handlers are tiny; their cost is in what
they publish afterwards, not yet attributed.

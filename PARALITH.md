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
| `process-delta-cache` | `true` | store a slot as its patches between checkpoints: store 50 -> 4 ms a slot, disk -63% (2026-10-26) |
| `rate-limit-requests` / `-period` / `-max` / `-min` / `-exempt` | as prod (`node-config.base.json`: 60000 per 60 s; exempt our servers' IPs) | RC7's default limiter allows a burst of 1,000 then ~17/s per client, and refuses beyond: game backends, indexers and bots get 429s under load. Same keys as the old fork |
| `irreplaceable-store` | a local `hb_store_lmdb` (was `essentials-store`), e.g. `/app/cache-mainnet/irreplaceable` | assignments, their messages and process definitions in their own store, so a damaged or wiped main store loses nothing that cannot be recomputed. Never on a network filesystem |
| `irreplaceable-export` | `{path, journal, max-fill-pct}` (was `essentials-export`, same three keys) | journals every irreplaceable write and ships it to the backup mount. Main only |
| LMDB `sync` | leave unset (now `true` by default; `false` opts out) | RC7 opened every LMDB `no_sync`: a host crash could corrupt the tree, as on the old fork's main node 2026-10-20. +3 ms a game slot |

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
4. **`lua-atomic@1.0`**: ported (2026-10-27) from `dev_lua_53c` as
   `dev_lua_atomic`, on `lua-patch@1.0`. What v0.11 changed:
   - A snapshot restore drops every Erlang function in the VM, so
     `normalize` checks the version witness, then installs `ao.atomic` and
     the host-call guards again. A `lua-patch@1.0` VM or snapshot is refused.
   - `collectgarbage` does nothing inside a batch. Stock Luerl (the GC
     patch is reverted) cannot see the suspended caller's frames from a
     nested call, and a collection there freed the caller's locals. The
     replacement is installed before any module code (`lua-device-preload`),
     so a module cannot keep Luerl's own, and a restore drops Luerl's own
     wherever a snapshot holds it.
   - The `ao` library and the guards are installed with raw table writes
     (`dev_lua_lib` too, so every Lua device): a `__newindex` on `_G` or
     `ao` used to receive the unguarded host functions.
   - `math.random`, `math.randomseed`, `os.time`, `os.clock` and `os.date`
     are gone from every `lua-patch@1.0` and `lua-atomic@1.0` VM, whatever
     `requires-determinism` says (lua-atomic also removes all of `os`). The
     only use in the monorepo's contracts is `battle.lua:596`, the `rand()`
     fallback for an unseeded stream; the game seeds at load (`game.lua:31`)
     and the hunt and battle workers seed before every roll, so it is never
     reached.
   - `dev_lua` takes `lua-device-sandbox` again, on top of the node's
     minimum sandbox.
   - **Contract rule:** do not keep `ao.get`/`resolve`/`set`/`event` in a
     variable or table of your own. They are host closures, and a restore
     sets them to `nil` everywhere except in `ao`, so a live VM and a
     restored one would behave differently. Call them through `ao.` each
     time. `ao.atomic` and `collectgarbage` are `#erl_mfa{}` values and do
     survive a restore on the same build; a build whose module name differs
     drops them too, so the rule holds for them as well.
   - A contract that sets the global `ao` to a non-table is armed with a new
     empty `ao` on restore instead of failing it.
   - **Garbage collection is linear.** Luerl's own collector keeps its seen
     sets in `ordsets`, so it is quadratic in live tables (5-10 s for
     20,000). `hb_luerl_gc` is the same collector with map sets (same
     result, compared in a test); every Lua device uses it. lua-atomic runs
     at most one collection per compute, when the Lua call returns. Found
     by the monorepo's native atomic preflight, which hit nginx's 300 s.
   - **Published keys are lowercase.** RC7 stores and reads every message
     key lowercased, and `message@1.0` looks a key up by its lowercase form
     only. `hb_state_patch` now lowercases patch paths, value keys at every
     depth and result keys, and refuses keys that differ only in case.
     Before, a mixed-case key (every `player-<address>`) turned into a list
     on its second write in a later slot. `/now/<key>` reads by any
     spelling. Clients that index a whole-state JSON body, or a published
     map value keyed by addresses, must lowercase the key first.
   - Found on the way: `lua-patch@1.0` never gave RC7's `ao` library its
     node message, so every `ao.get`/`resolve`/`set`/`event` from a
     lua-patch contract failed. Fixed, with a test.
   - The monorepo's deploy probe (`contract-device.mjs`, `ATOMIC_PROBE`)
     must POST to `/~lua-atomic@1.0/compute/body`. On `/compute`, RC7
     sends the reply as multipart (its `results` is a link), so the body
     is never exactly `lua-atomic-v1-ready`. The test covers both.

   Tests (`hb-v11-dev`, `--module=dev_lua_atomic,dev_lua_patch,
   hb_state_patch,lib_process_cache`): 60 passed, 1 failed. The failure is
   `incremental_gc_step_test` (needs the reverted Luerl patch). All 14
   `lua-atomic@1.0` tests pass; each guard and restore test fails on the
   code before its fix.
   `dev_lua,dev_lua_test`: two timing benchmarks fail on the shared box,
   with or without this port: `pure_lua_process_benchmark_test_` every run
   (30 slots in 666-880 ms, limit 500 ms) and
   `aos_process_benchmark_test_` sometimes (6 slots in 178-316 ms, limit
   250 ms).
5. **Worker, process cache, `hb_persistent`**: each measured on its own.
6. **Push**.
7. **Stores**: ported (2026-10-27), rewritten for RC7's layout (plain
   key/value writes, no links or groups):
   - **LMDB sync**: every commit is synced unless a store sets
     `sync: false` (RC7 defaulted to `no_sync`). A synced store's flush is
     its commit, without RC7's second `env_sync`.
   - **`hb_store_irreplaceable`** (from `hb_store_essentials`, ~100 lines
     instead of ~240). RC7 already routes the scheduler cache, location
     cache and spawn definitions through `scheduler-store`, so the module
     only points `scheduler-store` at the irreplaceable store (after the
     start hook) and adds it read-only after the node's first store. A
     definition POSTed to `/schedule` now goes through the scheduler cache
     too. Not ported: `migrate`/`verify` (v0.11 lands on a fresh deploy
     only) and routing for the bundler and Arweave block caches (unused on
     our nodes; regenerable).
   - **`hb_store_offsite_journal`** (from `hb_store_export`, ~860 lines plus tests,
     instead of ~2,400). Write-ahead: the writer appends a write to the
     local journal, then writes the inner store; `flush` (the scheduler's,
     before it confirms a slot) syncs the journal, then flushes the inner
     store. A separate shipper copies sealed segments to the target
     (`.tmp`, sync, rename, then `manifest.log` with size and CRC), never
     above `max-fill-pct`, and deletes the local copy. Torn and zero-filled
     tails are cut on start; a failed append is truncated away and the write
     refused. An unclean start journals every assignment the inner store
     holds above the per-process marks, with its closure (the one gap a
     power loss can leave). `restore/2,3` replays the manifest, then any
     unshipped local segments. Not ported: base images and gap marks (the
     write-ahead order and the reconcile replace them), local pruning after
     export, the target-op gate.
   - **`hb_store_retention` not ported.** It is not redundant: nothing in
     RC7 ever deletes a row, and with delta storage the main store still
     grows ~16 MiB per 1,000 game slots, the irreplaceable store ~14 MiB.
     But it cannot be built on RC7 as it is: the pinned `elmdb` (94f6d30)
     has no delete, and the fork's collector stood on an 851-line `elmdb`
     patch (delete, guarded delete, write tracking, reference scans), on
     the fork's link/group layout and on `process-state-blobs`, none of
     which exist here. **Decision for Tyler**: port a delete into `elmdb`
     and write a v0.11 retention, or launch main without one and watch
     disk (the old fork's main ran `store-retention: false` anyway, and
     pruned essentials to 6 h only because they were exported).

   Tests (`hb-v11-dev`, `--with-core --module=hb_store_offsite_journal,
   hb_store_irreplaceable,dev_scheduler_cache`): 17/17 (5 journal, 2
   routing, 2 new scheduler-cache: an assignment lands whole in the
   irreplaceable store, and one shipped and restored into an empty store
   reads back and verifies). `hb_store_lmdb,hb_store,hb_cache,dev_scheduler`:
   115/117; the two failures (`http_get_schedule_redirect`,
   `redirect_from_graphql`) fail identically without this unit.

   Crash tests (outside eunit, `kill -9` of the whole VM):
   - 30 kills at random points, writing assignment-shaped rows (assignment
     link, message rows, a 2 KiB blob) with a flush per slot, segments
     sealed and shipped every 300 ms, a zero-filled tail appended to the
     open segment on every fourth kill: 6,674 confirmed slots, none missing
     from the inner store or from a restore of target plus journal; slots
     written but not yet confirmed restored too. LMDB opened every time.
   - Disk full (tmpfs): journal disk full: the write is refused, the inner
     store does not have it, the writer survives, writes resume once space
     is freed, and the journal restores everything. Target disk really
     full: shipping fails with `enospc`, no `.tmp` left, the manifest still
     restores cleanly, target plus journal restore everything.
     `max-fill-pct` 50: shipping stopped at 50% with the rest held locally.
     Inner LMDB disk full: refused with `transaction_error`, confirmed rows
     intact, all of them there after a restart.
   - Not testable in a container: a real power loss. That is what the LMDB
     sync and the journal sync before each confirmation are for.

   Cost, `bench/durability_bench.erl`, game-shaped `lua-patch@1.0`
   process, 1,000 slots each scheduled then computed in the VM, two runs
   each, medians:

   | Mode | Slot | Schedule | Compute | p95 | Main | Irreplaceable | Offsite |
   |---|---:|---:|---:|---:|---:|---:|---:|
   | RC7 (`no_sync`) | 95.8-97.0 ms | 46.2-46.7 | 50.0-50.8 | 110-112 | 25.0 MiB | | |
   | sync | 99.8-99.9 ms | 46.6-47.3 | 53.3 | 112-122 | 24.8-25.0 MiB | | |
   | + irreplaceable store | 105.1-105.3 ms | 47.0-47.6 | 57.8-58.3 | 120-125 | 16.2 MiB | 13.8-13.9 MiB | |
   | + offsite journal | 106.9-109.4 ms | 48.8-49.9 | 58.0-59.6 | 119-133 | 16.0 MiB | 13.8-14.0 MiB | ~1.25 MiB gz |

   (disk per 1,000 slots; Schedule includes the bench signing two RSA
   messages.) Memory: no difference, BEAM 121-132 MiB and RSS 236-253 MiB
   at the end of every run. The local journal stays at about one segment
   (8 MiB) plus whatever the target has not taken.

   **Trade-offs, for Tyler:**
   - Sync: +3 ms a slot (+3%), all in compute (its lists commit the
     overlay, now synced). The scheduler already synced every slot in RC7.
   - Irreplaceable store: +5 ms a slot. A traced slot makes ~40 more LMDB
     reads and one more list: reads that used to hit the main store first
     now miss it and fall to the second store. Not fsync (an unsynced
     irreplaceable store measured the same) and not order (reading it
     first measured the same).
   - Irreplaceable store: +20% disk (30 vs 25 MiB per 1,000 slots), as
     signing the assignment leaves a copy of the body in the main store.
   - Offsite journal: +2-4 ms a slot (an `fdatasync` per confirmed slot,
     and the write-ahead hop through the writer). The offsite copy is ~9%
     of the irreplaceable store's size (gzip).
   - Everything at once: ~97 -> ~108 ms a game slot in the VM (+11%).
8. **`bridge-attestor@1.0`**, `evm-rpc@1.0`, chain signing: ported
   (2026-10-27) from `dev_bridge`, `hb_bridge_sign`, `hb_bridge_chains` and
   `dev_eth_client`, with the 2026-10-26 polling fix (fixed 15 s nudge
   cooldown; Base `range` scans skip `eth_blockNumber`; the 2-minute idle
   `interval` is set by the monorepo's config). What it signs and sends is
   unchanged: same digests (the viem/node/Move vectors pass), PUSD actions,
   tags, epoch and namespace handling, config and state files.
   - Signing and chain scanning are the device's helpers
     (`dev_bridge_attestor_sign`, `dev_bridge_attestor_chains`), packaged
     with it. Base goes to `evm-rpc@1.0` by name through `hb_ao:raw`.
   - On v0.11 the node's httpsig committer is a `publickey:` URI;
     `lua-patch@1.0` hands Lua the address, so a PUSD on lua-patch (or
     lua-atomic) sees the attestor as before. Tested end to end.
   - Hardened after audit (f8b9c43ff): `start` reads config only from the
     node message, never a request; one chain's crash fails that chain
     only; failures show and log no URLs, maps or call arguments; an
     unsignable withdrawal or proposal is skipped and listed in `status`
     under `skipped` instead of failing every pass.
   - v0.11's relay refuses internal hosts (`hosts-block-internal`, on by
     default): a `pusd-node` or RPC URL on localhost or a private IP is
     refused. Ours are public.

   Node config for an attestor: `on.start.device` = `bridge-attestor@1.0`
   (was `bridge@1.0`); the `bridge` block is unchanged (`pusd`, `pusd-node`,
   `interval`, `fast-interval`, `fast-window`, `keys-file`, `state-file`,
   `follow-file`, `base`, `solana`, `sui`). Endpoints moved to
   `/~bridge-attestor@1.0/{keys,status,nudge}`.

   Tests (`hb-v11-dev`, `--module=dev_bridge_attestor,
   dev_bridge_attestor_sign,dev_bridge_attestor_chains,dev_evm_rpc`): 39/39,
   twice. They include a stand-in PUSD on lua-patch@1.0 (one pass reads its
   `bridgeinfo`, votes, and PUSD records the node's address as committer),
   locally and through `pusd-node` over HTTP, and the start hook serving
   `/keys`. Base head/getLogs and Sui GraphQL were also scanned live from
   the box through v0.11's relay (public endpoints; Solana not, as its RPC
   is private). `evm-rpc`'s network tests use publicnode: eth.merkle.io
   refuses the box.
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

Delta storage (`process-delta-cache`), game, 98 slots a run, two runs each:

| | Delta off (RC7) | Delta on |
|---|---:|---:|
| store per slot | 50-52 ms | 4 ms |
| node time per slot (execution + store) | ~92 ms | ~47 ms |
| disk growth per run | 19 MiB | 7 MiB |
| Lua per slot | 29-34 ms | 32-33 ms |

One process now computes ~21 game actions a second instead of ~11. The 7 MiB
left is mostly fixed: the 620 KB module, the slot-0 checkpoint, the helper
process. Round trips through the WAN tunnel improve less (640-950 vs
730-990 ms), as the tunnel and polling dominate them.

The old fork's Luerl incremental-GC patch was tried and reverted
(2026-10-26): with delta storage on, Lua per game slot went 31 -> 34-35 ms and
slot execution 42 -> 46-47 ms, two runs each, with no memory gain (BEAM 135
MiB both). Its benefit on the old fork was tied to snapshotting the VM every
message, which this node no longer does.

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

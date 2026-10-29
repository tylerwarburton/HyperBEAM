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
| (new) | `timelock@1.0` | Stateless time-lock puzzle checks (secret reveal, Wesolowski crack), BLS12-381 beacon-signature check (`blst` NIF) and seed derivation for Lua processes; was `vrf@1.0`; see `docs/devices/timelock-at-1-0.md`. **Soundness fix landed** (2026-10-28, after the RandAO audit): one fixed unknown-order modulus (RSA-2048) built in, commit `c = g^alpha`, reveal `alpha`, sound Wesolowski cracks, canonical outputs `min(y, N - y)`; the per-validator modulus keys (`derive-x`, `p` reveals) are gone. A node whose `blst_nif` does not load refuses to start |

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
| `process-state-blobs` | `true` | computed slots as one row each under `pstate/`, the only rows retention may delete. Every node |
| `store-retention` (+ `-checkpoints` 2, `-interval-ms` 600000, `-first-ms` 60000, `-batch-keys` 1000, `-max-cpu-pct` 25, `-max-candidates` 1000000) | `true`, defaults | delete computed states below the newest two snapshot checkpoints. Every node. Needs `patches/elmdb-delete.patch` in the build |
| LMDB `sync` | leave unset (now `true` by default; `false` opts out) | RC7 opened every LMDB `no_sync`: a host crash could corrupt the tree, as on the old fork's main node 2026-10-20. +3 ms a game slot |
| `max-writes-in-flight` | leave unset (default 8 per scheduler) | see "Write admission" |

## Write admission

A node holds at most `max-writes-in-flight` writes at once (default 8 per
scheduler: 16 on a 2-CPU node). A write is any request that is not `GET`,
`HEAD` or `OPTIONS`. One past the bound is answered at once with **503 and
`retry-after: 1`**, before its body is read, verified or stored: it was never
accepted, so a client may re-send the same signed item. Reads are never
refused. The bound is read when the server starts.

Why: past its capacity a node that accepts every write spends its cores on
writes that then time out, and confirmed throughput falls instead of levelling
off. Measured 2026-10-29 on a 2-CPU scratch node (`--cpus 2`), one game of 300
players, composed writes (`POST /<pid>~process@1.0/push`, what the app and the
swarm send), round trip of the confirmed writes:

| offered | no bound: confirmed, p50 / p95 | bound 16: confirmed, p50 / p95, 503s |
|---:|---|---|
| 20 msg/s (1x) | 18.6 msg/s, 3.5 / 6.4 s | 19.1 msg/s, 0.6 / 1.0 s, 3% |
| 40 msg/s (2x) | 15.3 msg/s, >=30 / >=30 s (62% timed out) | 22.1 msg/s, 0.9 / 1.0 s, 44% |
| 60 msg/s (3x) | 11.0 msg/s, >=30 / >=30 s (82% timed out) | 22.1 msg/s, 0.9 / 1.0 s, 63% |
| 4 shards, 60 msg/s | 13.9 msg/s, >=30 / >=30 s | 18.7 msg/s, 0.8 / 2.0 s |

A bound of 32 gave 20.0 msg/s at p95 2.0 s at 3x: an accepted write waits
about bound / throughput, so 8 per scheduler keeps it near a second at the
~11 msg/s per core this game reaches. The load gate (15 msg/s) still passes
with no 503.

What it does not cover: a client that schedules (`POST .../schedule`) and then
reads the slot. That POST returns before the slot is computed, so the bound
never sees the backlog; the load gate's own path collapses as before (40 msg/s:
15.1 confirmed; 60: 11.1).

## Ship gate: ms per game slot (required before a node build ships)

Every functional suite passed on the build that made the test-net game 14x
slower (2026-10-27: ~1.5 msg/s, round trip p50 174 s under a 300-wallet
swarm). The cost grew with the size of the game's state, and every bench we
had used 28 players. So before any build of this branch replaces a node:

```bash
# on the box that built the image (or GAME_BENCH_SSH="root@<host> -i <key>")
bench/game-slot-bench.sh runerealm-hb:<tag>        # 300 players, 200 slots, max 20
```

It runs `bench/game_slot_bench.erl` in a throwaway container of the built
image (3 CPUs, ~2 minutes): a `lua-patch@1.0` process seeded with 300 players
of ~3 KB, each its own published key (the test-net game: ~1 MB, ~430 keys),
then 200 single-player mutations, each scheduled and computed through the
process worker with the test node's store options. It FAILS when the
worker's mean busy time per slot (`worker_execute`) is above 20 ms. That is
the number every message to one process queues behind: 1000 / it is the
process's ceiling in slots a second.

Baseline, paralith box, 2026-10-27, two runs each:

| Build | Worker ms/slot | Compute round trip | Slot execution | Schedule (bench client) |
|---|---:|---:|---:|---:|
| 5d5dbbf90 (before; timer added) | 167 | 183-184 ms | 57-61 ms | 52 ms |
| 3a83a1509 (worker without state hashing) | 11.6-12.1 | 40-41 ms | 4.1-4.3 ms | 50-51 ms |

The monorepo's deploy runs the matching live check against the target node
(`backend/native/load-gate.mjs`, `npm run load:gate`): a throwaway game with
300 players, `Sprite.Update` at 15 msg/s for 60 s from 20 burners, failing on
round trip p95 over 2,000 ms or under 14 confirmed msg/s. On hb-test:

| Build | Offered | Confirmed | Round trip p50 | p95 |
|---|---:|---:|---:|---:|
| 5d5dbbf90 | 20 msg/s | 0.6 msg/s | >= 30 s | >= 30 s |
| 3a83a1509 | 20 msg/s | 18.4 msg/s | 3.9 s | 7.7 s |
| 3a83a1509 | 15 msg/s | 14.9 msg/s | 435 ms | 685 ms |

With the hashing gone, the real game's own Lua is most of a worker slot
(live: ~30 ms of ~51 ms for `Sprite.Update` at 300 players), so one game
process tops out near 20 msg/s. Neither check covers a restart onto a new
build: after deploying 3a83a1509 every existing Lua process failed to restore
its snapshot (fixed in b91066290, `dev_lua:safe_binary_to_term/1`). After any
node build lands, compute one slot of an existing process before trusting it.

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
     On the real game bundle (offline Luerl, after 200 messages of play):

     | Players | Live tables | Luerl's collector | `hb_luerl_gc` |
     |---:|---:|---:|---:|
     | 28 | 1,095 | 95 ms | 9 ms |
     | 1,000 | 7,965 | 1,790 ms | 117 ms |

     On lua-patch the game collects in full only on checkpoint slots
     (`lua53b.lua`); its per-slot `collectgarbage("step")` is a no-op on
     stock Luerl, so ordinary slots do not pay this, but every checkpoint
     slot did, quadratically in players. lua@5.3a collects after every call.

     So lua-patch now collects every `lua-gc-interval-slots` slots (default
     25, or the process definition's value; never a node option). The
     game leaves ~390 dead tables a slot. 1,000 slots of play (alternating
     `User.Info` and `Monster.Feed`, game's own collect off as on lua-patch),
     offline Luerl on the shared bench box (GC times noisy):

     | Players | N | GC per slot (amortised) | GC avg / max | Max tables (x live) | Snapshot before GC / live |
     |---:|---:|---:|---:|---:|---:|
     | 1,000 | none | 0 | - | 395,469 (50x) | 9.9 / 2.4 MB |
     | 1,000 | 1 | 14.8 ms | 15 / 46 ms | 8,478 (1.06x) | 2.4 / 2.4 MB |
     | 1,000 | 25 | 1.6 ms | 40 / 118 ms | 17,778 (2.2x) | 2.6 / 2.4 MB |
     | 1,000 | 100 | 1.0 ms | 100 / 320 ms | 46,719 (5.9x) | 3.1 / 2.4 MB |
     | 1,000 | 250 | 0.2 ms | 53 / 65 ms | 104,844 (13x) | 4.3 / 2.4 MB |
     | 1,000 | 1000 | 0.2 ms | 160 ms | 395,469 (50x) | 9.9 / 2.4 MB |
     | 5,000 | none | 0 | - | 423,549 (12x) | 58.9 / 51.4 MB |
     | 5,000 | 1 | 71.8 ms | 72 / 112 ms | 36,558 (1.01x) | 51.5 / 51.4 MB |
     | 5,000 | 25 | 2.4 ms | 61 / 99 ms | 45,858 (1.27x) | 51.6 / 51.4 MB |
     | 5,000 | 100 | 1.8 ms | 181 / 479 ms | 74,799 (2.1x) | 52.2 / 51.4 MB |
     | 5,000 | 250 | 0.3 ms | 78 / 86 ms | 132,924 (3.7x) | 53.3 / 51.4 MB |
     | 5,000 | 1000 | 0.3 ms | 302 ms | 423,549 (12x) | 58.9 / 51.4 MB |

     25 is the largest interval that keeps the heap near 2x live at 1,000
     players, with the smallest spike among those that do (~100 ms). The game
     slot itself is 24-70 ms here, so ~2 ms is a few percent.

     The cadence is visible to Lua: a freed table slot is reused, and
     `tostring` of a table and the order `pairs` visits table keys show
     slot numbers. Luerl has no weak tables, finalizers or `count`, so that
     is the only channel, but it is enough: every node computing a process
     must use the same interval, which is why it follows the slot number
     and only the process definition can set it. A VM restored from a
     checkpoint has the same heap layout as one that never stopped
     (externalize/internalize keep the free lists; restore writes host
     functions into existing keys), checked slot by slot across collections
     in `restored_vm_same_heap_layout_test_`. One edge: lua-atomic gives a
     restored VM whose contract set `ao` to a non-table a new `ao` table,
     which the live VM does not have.
   - **Collection inside running Lua is safe.** Luerl's collector missed the
     values a suspended frame's instructions hold, so collecting from inside
     an open `for ... in ipairs(...)` freed the iterated table (`badkey`).
     `hb_luerl_gc` marks them. And every call Luerl makes from Erlang
     (`pcall`, an error value's `__tostring`, `table.sort`'s comparator,
     `string.gsub`'s replacement, `__eq`, `__index` from library code)
     started on a new call stack, so the suspended Lua beneath it was not a
     root: a contract that `pcall`s a function that collects lost its
     caller's locals and the slot failed on every node.
     `patches/luerl-nested-call-roots.patch` (applied after get-deps, as for
     elmdb; `bench/Dockerfile.branch` does) puts the new frame on top of the
     caller's stack instead. Every build of this branch needs it.
   - **Direct computes have a deadline.** `lua-compute-deadline` (ms,
     default 120000, `infinity`/`false` off) stops a direct call to
     lua-patch or lua-atomic with a 504; the Lua runs in its own process and
     is killed, so nothing keeps computing after the client has gone.
     Process slots never have one: a slot must compute the same on every
     node, whatever its speed.
   - **Host crashes.** Outside a slot, an `ao.*` call that crashes on the
     node is a Lua error (`ao.<name> failed on this node`) the contract's
     `pcall` catches. Inside a slot it still fails the slot: catching a
     node-dependent failure would let that node's state differ.
     `hb_store_arweave` answers `not_found` for a non-canonical 43-character
     id instead of crashing.
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
   - **`hb_store_offsite_journal`** (from `hb_store_export`, ~1,150 lines plus tests,
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
   - **`hb_store_retention`** (from `hb_store_gc`, ~260 lines instead of
     ~3,800; required before prod, decided 2026-10-27). Three parts:
     - `patches/elmdb-delete.patch` (237 lines, the fork's was 851):
       `elmdb:delete_batch/2`, one write-worker command that commits the
       overlay and deletes the keys in one transaction. No guarded delete,
       write tracking or reference scans: nothing here needs them. Applied
       after `rebar3 get-deps` (`patch -p1 -d _build/default/lib/elmdb`);
       verified from a clean `get-deps`, patch, `compile --deps_only`.
       Without it retention refuses to start, and its test fails.
     - `process-state-blobs` in `lib_process_cache`: a delta process's slot
       is one row under `pstate/<proc>/{d,f,s}/<slot>` (delta envelope,
       full state, snapshot marker written after the state), owned by that
       process and slot and referenced by nothing. RC7's content-addressed
       layout shares rows between states and the definition, so no row of
       it is ever safe to delete.
     - The collector: keeps the newest `store-retention-checkpoints` (2)
       snapshot checkpoints and every slot from the oldest of them up;
       deletes `pstate/` rows below, highest slot first (what a crash leaves
       is still a readable chain), in one-transaction batches, throttled to
       `store-retention-max-cpu-pct`. Only keys of the exact
       `pstate/<id>/{s,d,f}/<12 digits>` form can reach the delete; the
       irreplaceable store is never chosen. Assignments, messages,
       definitions and kept checkpoints are therefore never candidates.
   - Audit fixes (2026-10-27, each with a test that failed first): a failed
     journal sync poisons the run (never retried); only confirmed writes
     restore (`flushed`/`start`/`revoke` markers) and confirmed-only marks
     make reconcile journal a reassigned slot as the inner store has it;
     frames carry a magic, so mid-file damage stops the writer, the
     shipper and a restore while a torn tail of the open segment is cut;
     the manifest carries a hash chain and tolerates a torn last line;
     restore refuses gaps in a process's assignments; directories are
     synced after renames; spawns are flushed before they are answered; a
     definition's `image`/`module` given by ID is copied into the
     scheduler's store; the closure follows `ao://with-commitments=`.
     Signing an assignment no longer leaves a copy of its body in the main
     store (it went to the node's first store: ~1.8 KB a slot that no
     retention may touch).
   - Deliberately not in the irreplaceable store: `~bundler@1.0` (our nodes
     send no bundles), `~arweave@2.9` blocks (refetched from a gateway),
     `~meta@1.0` (the node config file is the source). `~location@1.0`
     goes there already, through `scheduler-store`. Our Lua processes carry
     their module inline in the definition.

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
     signing the assignment left a copy of the body in the main store.
     Since fixed (8d997fbd9); about half of that figure was also the
     bench client's own signing, now kept out of the node's stores.
   - Offsite journal: +2-4 ms a slot (an `fdatasync` per confirmed slot,
     and the write-ahead hop through the writer). The offsite copy is ~9%
     of the irreplaceable store's size (gzip).
   - Everything at once: ~97 -> ~108 ms a game slot in the VM (+11%).

   Retention tests (`--with-core --module=hb_store_retention,
   lib_process_cache,hb_store_offsite_journal,hb_store_irreplaceable,
   dev_scheduler,dev_scheduler_cache,dev_scheduler_server,dev_process`):
   74/76, the same two redirect failures as without this unit.
   - `kill -9` in the middle of a run, 20 rounds: 20 processes, 90,180
     doomed rows among 102,000 assignments and kept rows, batches of 50,
     throttled to 50%, killed after 1-4.5 s (3,650-47,090 rows deleted).
     Every time: LMDB opens, every assignment and definition and every kept
     row is intact, what was deleted is exactly a prefix of each process's
     top-down plan, and the next run deletes the rest.
   - Recompute (`bench/retention_bench.erl recompute`, game-shaped process,
     3,000 slots, a snapshot checkpoint every 500): retention deleted 2,505
     rows (7 checkpoints -> 2, 2,994 deltas -> 499) in 58 ms. Then, with
     the worker and hot cache dropped, all equal on every published key:
     the head recomputed from an empty main store (the irreplaceable store
     alone, 121 s for 3,001 slots); the next slot resumed from the kept
     checkpoint (1.1 s) against the same from scratch; slot 100, below the
     floor, recomputed against the same from scratch.
   - Disk, 20,000 game-shaped slots, retention every 60 s (main store):

     | Slot | RC7 layout | blobs, no retention | blobs + retention |
     |---:|---:|---:|---:|
     | 4,000 | 213 MiB | 33 MiB | 23 MiB |
     | 10,000 | 1,756 MiB | 229 MiB | 102 MiB |
     | 16,000 | 4,299 MiB | 599 MiB | 136 MiB |
     | 20,000 | 6,327 MiB | 878 MiB | 193 MiB |

     (RC7 layout from the first run, before the bench client's signing was
     moved out of the node's store: ~3.7 KB a slot of it is that.) With
     retention the main store holds, logically, 92 MiB at the end: 3
     checkpoints (the 2 kept and 1 written since the last run) and 998
     deltas, nothing else. The file is about twice that because LMDB
     never shrinks: it holds the peak (new checkpoints written before the
     next run frees old ones) and reuses the space after. Its size tracks
     the checkpoint size, which in this bench grew from ~15 to ~31 MiB
     (the Lua VM image grows); bound ~= 2 x (kept + new) x checkpoint.
   - Slot cost while a run is in progress: 8 of 20,001 slots overlapped
     one, median 186 vs 119 ms (p95 263 vs 230). Slot median with
     retention on is the same as without (119 ms, same box, same time).

   Trade-offs, retention: a slot that overlaps a run waits for a delete
   batch's synced commit (+~70 ms, rare); `process-state-blobs` writes a
   checkpoint as one compressed row instead of deduplicated
   content-addressed rows (on this bench far smaller: 878 vs 6,327 MiB);
   a read below the floor recomputes from the start; LMDB keeps its
   high-water mark.

   Re-audit fixes (each with a test that failed first):
   - The journal wrapper sets `max-retries => 0`: `hb_store` treated a
     refused write as a store failure and restarted the writer to retry it,
     about once a second on a full disk. A writer that cannot journal its
     start marker (or reconcile, or rebase) stays up, refuses and tries
     again at the next call; disk-full test: refused, never crashed, runs
     once space is back.
   - A poisoning is written to `<journal>/poisoned` and refuses every later
     run until `hb_store_offsite_journal:clear_poison(Store)`, after which
     the next run journals every row of the irreplaceable store again (a
     rebase) before it takes a write. An inner-store flush failure poisons
     too, so a full irreplaceable disk needs `clear_poison` once space is
     back.
   - Full-state processes (`lua@5.3a`, WASM, `token@1.0`, `carrier@1.0`)
     now write `pstate/.../f` rows too; retention keeps their newest 2
     snapshot checkpoints and newest `store-retention-recent-slots` (32)
     states. Game-shaped `lua@5.3a`, 1,000 slots, two runs each: 96-97 ->
     89-90 ms a slot, main store 12.8 -> 4.1-4.2 MiB per 1,000 slots; one
     retention run then deleted 989 of 1,024 rows.
   - A failed delete is logged at error level as leaving the main store
     refusing writes until restart (as a failed flush does), and kept in
     the retention status.
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
10. **A signed message is assigned at most once per process** (ours, not
   from the old fork). Signed messages are public, so anyone could post one
   again and run it again (a `Monster.Feed`, a shop buy). The scheduler now
   refuses it: `409`, `{status, body, process, message, slot}`, the slot it
   already has; no slot is taken.
   - **Which ID:** every signature commitment ID of the message (those with a
     `committer`). Each is `sha256` of its signature and checked against it on
     verify, so the ANS-104 item and the httpsig form of one signature have
     the same ID, and a key cannot be relabelled. A second signature added to
     a message does not make it new (its first ID is still assigned). The
     same content signed again is a new signature and is assigned. Unsigned
     messages (only on a node that accepts them) are not deduplicated.
   - **Index:** `~scheduler@1.0/message-slots/<proc>/<sig-id>` -> slot, in the
     scheduler's store (the irreplaceable store when set, so journalled
     offsite). The row is written right after its assignment, inside the
     grouped flush's batch (below), and committed by that batch's synced
     flush before anyone is answered. A lookup is one exact-key `get` per
     signature (`exact => true`: it sees the batch's unflushed writes, and
     never takes the prefix scan, which commits pending writes first: 13 us
     clean, 1.3 ms dirty, and it would have broken the batch). Nothing in
     memory.
   - **Within a batch:** a duplicate of a message earlier in the same batch
     is found from the unflushed writes. Its 409 is queued and sent in the
     confirm phase, after the flush; a failed flush sends nothing.
   - **Crash:** commits keep write order, so the disk holds a prefix of the
     writes and an assignment can lack its rows only among the last batch:
     a starting server writes the rows of the last 32 slots again. A hit
     counts only if that slot is at or below the latest and its assignment
     carries the ID, so a row whose assignment never landed refuses
     nothing. A failed flush stops the scheduler server (the batch may be on
     disk all the same, e.g. a commit whose sync failed), and the next
     request starts one from the store, never reusing a slot number.
   - **Aggressive mode** (not ours: `local_confirmation`) writes the rows
     too, but answers before writing, so a re-post inside that window can
     still be assigned.
   - **Node config:** no node config sets `verify-assignments` (default
     `true`: a message must be signed and verify), so unsigned messages
     never reach the scheduler on our nodes.
   - Tests: re-post (as ANS-104 item, httpsig form, with an extra
     signature), re-signed content, 20 concurrent identical posts, restart
     from disk, torn rows, a miss reads no prefix, a duplicate inside one
     batch refused only after its flush, a failed flush answers nobody and
     stops the server, the last batch's rows repaired at start (the last
     four failed before their fix). `dev_scheduler,dev_scheduler_server,
     dev_scheduler_cache,dev_process,hb_store_irreplaceable,
     hb_store_offsite_journal,hb_store_lmdb,hb_process_test_vectors` 92/94,
     the same two redirect failures as ff752f851. Tests that posted one
     signed message several times now sign each. Test LMDB stores in
     `dev_scheduler_server` map 1 GiB: a VM's address space holds ~60 of the
     default 2 TiB, and the full run went past it (`io_error` at open).
   - kill -9 mid-burst with every third message posted twice at once (1,500
     messages, c=16, killed at 3 s and 6 s), then every message tried posted
     again after the restart: no confirmed slot lost (276 and 550, each
     refused again naming its slot), no message with two slots, no 409
     naming a slot the restart lost (93 and 183 refusals). ff752f851 on the
     same run gives 244 messages two slots or more.

11. **A slot is computed, pushed or served only once it is durable** (ours).
   Contracts key items on (sender process, slot), so nothing derived from
   slot N may leave the node before N's assignment is flushed: a crash
   would lose N, and the scheduler would give N to another message. With
   the grouped flush, an assignment is in the store (readable by `get`)
   from its write until its batch's flush, ~6 ms of signing per later
   request; a `now` in that window read it, computed it and could push it.
   - The scheduling server keeps each process's newest durable slot in a
     node-wide table: the latest on disk when it starts, the batch's last
     slot after each flush that succeeds (before anyone is answered). A
     failed flush leaves it where it was.
   - Compute's local read (`next`, lookahead), the schedule served to other
     nodes and clients (`schedule`, also `next`'s fallback) and `slot`'s
     `current` stop at it. A process not scheduled here is not limited.
     `aggressive` mode answers before writing, so it sets no limit.
   - Test: a batch whose flush fails (journal `fail_sync`, as a crash after
     the write would leave it) has its slot in the store, readable, but not
     given to compute or served; failed before the change.
     `dev_scheduler,dev_scheduler_server,dev_scheduler_cache,
     hb_store_irreplaceable,hb_store_offsite_journal,hb_store_lmdb` 65/67
     (the two redirect failures as before), `dev_process,
     dev_process_worker,dev_push,hb_process_test_vectors` 64/64. All ten in
     one VM fail with `io_error` on base too (19 failures): more LMDB stores
     than a VM's address space maps at 2 TiB each.

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

Node CPU per scheduled message (2026-10-28, paralith box, scratch container,
3 CPUs, `split-sync` bench config, burst of 1,000 signed messages to one
`lua@5.3a` process at c=16 then one compute; cgroup CPU / messages, three
interleaved runs each):

| Build | Node CPU / msg | RSA verifies / msg | RSA signs / msg | Schedule /s (one process) |
|---|---:|---:|---:|---:|
| 07c23006f | 36.1-36.8 ms | 12 | 2 | 82-83 |
| + verify cache (3b0a6a9c8) | 34.3-35.5 ms | 2 | 2 | 82-85 |
| + `force-signed` from config (d4f4731e8), unset | 34.6-36.7 ms | 2 | 2 | 83-84 |
| same, `"force-signed": false` | 30.8-31.1 ms (one run 34.6) | 2 | 1 | 72-76 |

One process's schedule rate is capped by its scheduler server, which is
serial: per slot ~1.2 ms building the assignment, 5.4-6.2 ms its RSA-4096
signature, 1.6 ms the cache write and 3.4-3.7 ms the irreplaceable-store
flush (call_time, 500 messages). The base-hashpath chain is taken over the
assignment's signed id (`hb_message:id(Assignment, all)`), so the signature
cannot leave that path without changing every assignment id after it.
`hb_ao:normalize_key/2` and `has_only_binary_keys/1` are called ~2,600 times
a message but take 0.13% of perf samples; their large call_time in traced
profiles is the tracing itself.

Ship gate unchanged by these (worker 7.9-8.2 ms/slot base, 8.1 after): it
schedules and computes inside the node and never crosses HTTP.

Grouped scheduler flush (2026-10-28, same box and bench, three interleaved
runs each, base ff752f851): the per-process scheduler takes every schedule
request already queued (at most 32, never waiting for more), builds, signs and
writes each, flushes the irreplaceable store once, then confirms them in slot
order.

| Build | Schedule /s (one process, c=16) | Node CPU / msg | Post p50 | Idle round trip p50 |
|---|---:|---:|---:|---:|
| ff752f851 | 73.9-74.3 | 37.9-41.0 ms | 213-215 ms | 43.6-46.8 ms |
| grouped flush | 91.0-102.7 | 29.5-33.5 ms | 155-175 ms | 44.6-51.9 ms |

With the replay rule on top (`paralith/v0.11-sched`, `"force-signed": false`,
1,000 messages at c=16, three interleaved runs, 2026-10-28):

| Build | Schedule /s (one process, c=16) | Node CPU / msg | Post p50 |
|---|---:|---:|---:|
| ff752f851 | 75.3-78.7 | 29.0-30.0 ms | 201-208 ms |
| grouped flush + replay rule | 114.2-115.9 | 23.9-25.5 ms | 137-139 ms |

kill -9 mid-burst then restart on the same store (`docker kill -s KILL`, two
timings): no confirmed slot missing on either build, and the scheduler resumes
at or above the newest confirmed slot.

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

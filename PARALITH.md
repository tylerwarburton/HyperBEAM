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
8. **`bridge-attestor@1.0`**, `evm-rpc@1.0`, chain signing.
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

Every step is judged on speed, reliability, memory and storage. A step that
improves one and costs another goes to Tyler before it lands.

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

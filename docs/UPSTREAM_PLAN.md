# Upstream plan: the Paralith fork on edge

Branch `paralith/rebase-edge` (fork `tylerwarburton/HyperBEAM`) is the Paralith
fork rebuilt on upstream `edge` at acb90b141 (v0.11.1, 2026-11-07; still the
edge tip on 2026-11-21). Its tree is the live build 51dd818e plus 92c9b1368
(hypertoken v2 attestor, needed by the ~11-23 redeploy), with the three
conflict resolutions listed below. Nothing here has been proposed upstream.

The old history (one 28k-line "re-apply" commit, Codex conflict resolutions,
a landed-reverted-relanded admission, test fixups, then 75 topic commits) is
replaced by 21 commits grouped by topic, ordered so each one compiles and its
own suites pass (see "Test evidence").

Divergence from edge: 0 behind, 21 ahead; 123 files, +44,770 / -2,968.
Of that, Paralith-only (bridge, evm-rpc, notes, benches): 19 files, +8,374.

## Classification

- **A**: a general fix or improvement upstream would want. Proposed as PRs.
- **B**: our own optional device or store. Kept in the fork, offered upstream
  only as optional devices once it touches core minimally.
- **C**: Paralith-specific, or fixup noise squashed into its topic.

| Branch commit | Class | Old commits folded in |
|---|---|---|
| tls: encode CSRs with public_key:der_encode (OTP 27) | A | re-apply 83a766a78 (part) |
| format: compile the device-name escape pattern once | A | 0ffb907a5 |
| store_fs: read file info raw | A | re-apply (part) |
| test: bounded LMDB test stores | A | re-apply (part) |
| http: one gate per host honours 429 retry-after | A | fd61ac0e6, 8f46b1f16 |
| cache: peer reads can be local-only | A | 8d1630220 (part), 627d01586 (C) |
| ar_wallet: bounded RSA verification cache | A | re-apply (part) |
| httpsig: reuse a message's uncommitted encoding | A | re-apply (part) |
| location: validate and expire scheduler locations | A | 8d1630220 (part) |
| observability: phase timer, slot log, trace, logger | A (logger, metrics, phase timer) / B (slot log, trace pages) | 2b84d17c0, 8b732a73b, 339cd193e (stats) |
| arweave: verify indep_hash/tx_root; authenticated scheduler ordering | A | 4ebf79466, c7e1668ff, 479d82808, dbf714d11, ce76dbd99, a980ce1ac, 153fa4b35, 335c22e15 (part) |
| store: exact LMDB reads, held batches, honest gateway misses | A | re-apply (part, with the elmdb patches) |
| store: offsite journal, retention, irreplaceable | B | re-apply (part) |
| resolver: normalized bases, bounded leader re-election, private reset, cache-control, hb_name handoff | A | 37a853cd5 (part), re-apply (part) |
| lua: incremental GC, nested-call roots, entry-point lockdown, direct keys | A | fde226fc6, 0a6784b4c, 37a853cd5 (hb_device) |
| lua-patch@1.0, lua-atomic@1.0 | B | ae6c385c9, a1b69942d, dbab761b9, 8d1630220 (part); c8aba2e2a (C) |
| process: durable outbox delivery, scheduler admission and follower, write admission | A (delivery, admission, slot cost) + B (follower, uploader, admission device) | 42c934887, 50df40cb0, 05e72e44b, efbce1663, bef6f626f, 37a853cd5, 9dfeb406a, 51dd818e1, 70aa54662, 4d00f78a8, 7208a407e, b9d5923f3, dc7f3caeb, 339cd193e, 2ed7678a0, 8aba898dd, f94f42651, 4fa65530f, c103350c9; C: c8d0d8171, 0f71db37a + revert 5223b44b5 |
| crypto: blst NIF, timelock@1.0, rng@1.0 | B | re-apply (part) |
| rng-validator@1.0 | B | a6f63eae8 |
| paralith: bridge-attestor, bridge root, evm-rpc | C | f68a4d762 b74c3eadb 0da367b1b 8e90f8dbf c49899252 3be743ed6 3889e13cd fb97e427f 97417d0a6 80d8ee1b1 6e25ee703 a6375a14d 713396406 a0c89770e 812c4eb5a 4c6faedbc d35ad2288 71f7ee90b cc570d8ae e34805d0a 70fb5ffc8 9a11e96ca 424438627 5a9e17cec 92c9b1368 |
| paralith: fork notes, benches, VM flags | C | 2379a9337, bench parts of re-apply |

## Conflicts with upstream behaviour

1. **`dev_arweave_scheduler_sync`**: upstream 21d6c51df indexes scheduler
   blocks from the transaction headers `/block2` carries (`include-tx-headers`,
   `include-proofs => false`). Ours verifies `indep_hash`, `tx_root` and
   confirmation depth and needs the proofs, so the branch keeps ours whole and
   drops upstream's change. To get upstream's saving back, feed the carried
   headers into our `tx_root` check instead of fetching each header (PR 6).
2. **`dev_push` / committer URIs**: upstream b2282f89c maps an outbox target
   that is a committer URI to its process ID. Our `dev_push` is a rewrite over
   `lib_process_delivery`, so the mapping is ported there (`process_target/2`
   before evaluation, and in `entry_target/1`). A raw-ID target is unchanged,
   so live behaviour is identical.
3. **`dev_arweave` block decoding**: edge split `/block` handling into
   `block_message/5` and added `/block2`; ours had moved the JSON decode into
   `ar_block:block_message/2` so every package reads blocks the same way. The
   branch keeps edge's structure and calls `ar_block:block_message/2` in it.

## Proposed PR series, in order

Each PR is one branch commit (or a short run), cherry-picked onto the edge of
the day. Small, independent fixes first.

| # | Title | Files | Why upstream wants it | Risk |
|---|---|---|---|---|
| 1 | tls: encode CSRs with `public_key:der_encode` | hb_tls, hb_tls_examples, dev_tls | `'PKCS-10':encode` does not exist on OTP 27, so CSR issuance fails | Low |
| 2 | format: compile the device-name pattern once | hb_format | every printed line recompiled a regex | Low |
| 3 | store_fs: raw `read_file_info` | hb_store_fs | every cache miss serialised on the node's one `file_server` | Low |
| 4 | test: bounded LMDB test stores | hb_test_utils | hundreds of test stores exhaust virtual address space | Low (tests) |
| 5 | http: per-host gate honouring 429 `retry-after` | hb_http_gate, hb_http_client, hb_client_remote, hb_opts, ar_timestamp, hb_mock_server | a refusing host is hammered, and arweave.net counts refused requests against the limit | Medium: new default rate for arweave.net |
| 6 | arweave: verify blocks; authenticated scheduler ordering | ar_block, dev_arweave, dev_arweave_scheduler*, lib_arweave_scheduler_blocks, test/arweave-blocks | the Arweave scheduler trusted whatever a gateway served; this recomputes `indep_hash` and `tx_root` and pins an anchor | Medium: conflicts with 21d6c51df (above) |
| 7 | cache/location: local-only peer reads; signed, expiring locations | hb_store_remote_node, dev_cache, dev_location, dev_location_cache | two nodes that fall back to each other recurse forever; unsigned or stale locations were served | Low |
| 8 | perf: RSA verify cache, httpsig encoding reuse, exact LMDB reads | ar_wallet, dev_httpsig, hb_store, hb_store_lmdb, hb_store_gateway, hb_store_arweave | measured CPU on the verify and read paths (PARALITH.md) | Medium: the elmdb patches must land in permaweb/elmdb-rs first |
| 9 | resolver: normalized bases, bounded leader re-election, cache-control, `hb_name:handoff` | hb_ao, hb_private, hb_cache_control, hb_persistent, hb_name | a dying leader re-elected forever; the resolver re-normalised already-normal bases | Medium |
| 10 | lua: Luerl GC, nested-call roots, entry-point lockdown | dev_lua, dev_lua_lib, hb_luerl_gc, hb_device, luerl patch | a scheduled message could pick the slot's entry point (security); nested calls lost GC roots | Medium: the luerl patch goes to the luerl fork |
| 11 | observability: non-blocking logger, metrics, phase timer | hb_event, hb_metrics_collector, hb_phase_timer | a log line made the work it described wait | Low |
| 12 | process: durable outbox delivery | the process commit, split first into (a) write admission in hb_http_server, (b) unsigned GETs without a hashpath, (c) a slot costs what it touches, (d) `lib_process_delivery` + `dev_push` | pushes survive restarts and are delivered once by the scheduling node | High: 13k lines; needs the decoupling below first |

Each PR carries the eunit modules listed against its commit in "Test evidence".

## Group B: keeping it out of core

Group B: `dev_rng`, `dev_timelock` (+ vectors, blst NIF), `dev_rng_validator`,
`dev_lua_patch`, `dev_lua_atomic`, `hb_state_patch`, `dev_scheduler_admission`,
`dev_scheduler_follower`, `dev_scheduler_uploader`, `lib_bridge_replica`,
`hb_store_offsite_journal`, `hb_store_retention`, `hb_store_irreplaceable`,
`hb_slot_log`, `hb_msg_trace`. Group C (bridge, evm-rpc) stays in the fork.

Where B still reaches into core, and how to cut it:

- **The process commit is one unit today.** `lib_process_delivery` calls
  `dev_scheduler_follower:retry_after/1` and `lib_bridge_replica:execution_opts/2`;
  `dev_scheduler_server` calls `dev_scheduler_admission` and
  `dev_scheduler_uploader`; `dev_scheduler_admission` calls the follower; the
  follower lists `lib_process_delivery` as a device library. Splitting them
  into separate commits made 118 process tests fail with `undef`, so they ship
  together. Cut: give `dev_scheduler_server` a scheduler option naming an
  admission device and an uploader device (default none), move
  `execution_opts/2` out of `lib_bridge_replica` into `lib_process_cache`, and
  move `retry_after/1` into `lib_process_delivery`.
- `lib_bridge_replica` is a generic "replay a foreign schedule and verify it"
  library used by the follower and the bridge. Rename it (`lib_schedule_replica`)
  so B never depends on a C name.
- `hb_http_server:new_server/1` calls `hb_store_irreplaceable`,
  `hb_store_retention`, the delivery sweep and the Arweave follower directly.
  Move each behind an `on.start` hook so the server only runs hooks.
- `hb_app:start/2` refuses to start without `blst_nif`. Make that part of
  `dev_timelock` (a start hook) so a node without timelock still starts.
- `hb_msg_trace` and `hb_slot_log` are called from the request path and from
  `hb_store_lmdb`. Replace with `hb_event` events the trace modules subscribe to.
- `rebar.config` builds the blst NIF unconditionally; make it a profile.

Goal: a rebase touches only `rebar.config`, the device list in `hb_opts`, and
files that are ours.

## Rebase cadence

- Fetch `upstream` weekly, and on every upstream release tag, and rebase
  `paralith/rebase-edge` onto the new edge. Never merge edge into the fork.
- Keep the A commits first, so each rebase shrinks the series as PRs land:
  drop a commit once its PR merges.
- After each rebase: compile every commit (the preloaded-store build fails on
  a missing device library, which is how ordering bugs show), run each
  commit's suites, run all suites on the tip and compare with the previous
  tip, build the node image and run the paralith-mono gates (`run-smoke.sh`,
  `test:rune`, `test:lua`, `e2e.mjs`, the load gate) before any rollout.
- Live nodes stay pinned in `infra/nodes/nodes.json`; they move to this branch
  only after a gated rehearsal.

## Test evidence (2026-11-21)

Docker, erlang:27 (the node Dockerfile's base), 6 CPUs on a shared laptop.
`rebar3 device test --with-core --module=...`.

Every commit compiles, including the preloaded-store build, which refuses a
device whose device library is not yet in the tree.

Per commit (its own suites, at that commit):

| Commit | Suites | Result |
|---|---|---|
| tls | hb_tls | 1/1 |
| format | hb_format | 10/10 |
| store_fs | hb_store_fs | 1/1 |
| test: LMDB test stores | hb_store_lmdb | 5/5 |
| http gate | hb_http_gate, hb_http_client, ar_timestamp, hb_client_remote | 23/23 |
| cache | hb_store_remote_node, dev_cache | 5/5 |
| ar_wallet | ar_wallet | 2/2 |
| httpsig | dev_httpsig | 11/11 |
| location | dev_location, dev_location_cache | 10/10 |
| observability | hb_phase_timer, hb_slot_log, hb_msg_trace, hb_event, hb_metrics_collector | 39/39 |
| arweave | ar_block, dev_arweave, dev_arweave_scheduler_sync, dev_arweave_scheduler_cache | 94/94 |
| store | hb_store, hb_store_lmdb, hb_store_gateway, hb_store_arweave | 62/62 |
| journal/retention | hb_store_offsite_journal, hb_store_retention, hb_store_irreplaceable | 53/53 |
| resolver | hb_ao, hb_private, hb_ao_test_vectors, hb_cache_control, hb_persistent, hb_name | 386/386 |
| lua | dev_lua, hb_luerl_gc | 34/34 |
| lua-patch | compile only; its suites need the process commit and run there | - |
| process | process, push, scheduler, delivery, admission, follower, uploader, replica, http server, lua-patch, lua-atomic, hb_metadata_test_vectors | 483/483 |
| crypto | dev_rng, dev_timelock | 125/125 |
| rng-validator | dev_rng_validator | 33/33 |

On the tip, against the live build 51dd818e on the same box:

| Batch | 51dd818e | Tip |
|---|---|---|
| core small (20 modules) | 213/214 (slot-log microbench, timing) | 217/217 |
| arweave + stores (12) | 209/209 | 211/211 |
| resolver + Lua (10) | 374/374 | 374/374 |
| process + scheduler (16) | 402/402 | 405/405 |
| crypto + bridge (9) | 267/268 | 267/268 |
| upstream-touched: dev_token, dev_security, their vectors, hb_committer, dev_arweave_data, ar_tx | n/a | 105/105 |

The one failure in both builds is `bridge-attestor@1.0`
`root_endorse_endpoint` (`publish_root_certificate(<<"retry-check">>, ...)`
does not return `{ok, _}`); it fails identically on 51dd818e.

Node smoke (release image from Dockerfile.v011, `hbnode:rebase-edge`, local,
node-config.v011.json): starts, `/~meta@1.0/build` names the source;
paralith-mono `run-smoke.sh` 18/18; `test:rune` 279/279. `test:lua` got no
verdict: the node hangs up the single request at 300 s before the suite
ends, and the 51dd818e image does the same on this laptop, so it is the box,
not the build. `e2e.mjs` and the load gate need a deployed game and were not
run.

### Breakpoint fixes (CAPACITY_LOG_1118), 2026-11-21

Three commits on top, each with a test that failed on the code before it:

| Commit | Breakpoint | Test | Before | After |
|---|---|---|---|---|
| follower: wanted slot once per higher slot | 15 | `wanted_is_sent_once_per_higher_slot_test` (800 refusals, mailbox < 10) | failed: queue not bounded | passes |
| bridge replica: 500-slot pages, failures say why | 19 | `replica_pages_are_bounded_and_say_why_test_` (mock 504) | failed: `Asked =< 500` | passes |
| push: follow only targets this node schedules | 20 | `downstream_skips_targets_scheduled_elsewhere_test_` | failed: foreign target followed | passes |

Suites after (dev_scheduler_follower, lib_bridge_replica, dev_push,
dev_scheduler_admission, lib_process_delivery, dev_bridge_attestor): 189/190,
the one failure the known `root_endorse_endpoint`. Image `hbnode:rebase-edge`
rebuilt at the tip: `run-smoke.sh` 18/18, `test:rune` 279/279.

## Node scheduling policy (`scheduler-policy`)

Which processes a node schedules, as one node option
(`dev_scheduler_policy`). Without it a node schedules everything, as before.

```json
"scheduler-policy": {
  "mode": "owners",
  "owners": ["<address>", "..."],
  "refusal-message": "This node only schedules its operator's processes. ...",
  "deny": { "<process id>": "<message>" }
}
```

```json
"scheduler-policy": {
  "mode": "open",
  "deny": { "<process id>": "<message>" }
}
```

- `deny` (both modes): a spawn of, or a message to, a listed process is
  refused with **402** and its message, before anything else. Keys are
  compared lowercased, because loading a JSON config lowercases map keys; a
  deny list can only refuse, so that costs nothing.
- `owners` mode: a spawn is admitted when an owner signed it, or when the
  scheduler's door admitted its `from-process` (`admit-process`) and that
  parent is admitted, so processes spawned by admitted processes (hunt
  workers, DAOs spawned through Yap) keep working and a stranger cannot claim
  an admitted parent. A message is admitted when its process is. Anything else
  gets **403** with `refusal-message`.
- Whether an existing process is admitted is read back from its slot-0
  assignment (the spawn's signers and the door's `admit-process`), so it
  survives restarts and covers processes spawned before the policy was set.
- Reads are never refused.
- Order in `dev_scheduler:post_local_schedule/4`: deny, then the door
  (`dev_scheduler_admission:admit/3`), then the policy. The child rule needs
  the door's verified origin, so the policy decision comes after the door;
  the door does real work only for a message a trusted or verified scheduler
  signed, so a stranger's spawn costs no more than before.

Upstream fit: a small, self-contained module and two calls in
`post_local_schedule/4`; proposable on its own after PR 7.

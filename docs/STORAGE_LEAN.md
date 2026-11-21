# Lean storage and uploads (paralith/storage-lean)

Branch `paralith/storage-lean`, from `51dd818e1` (the live build). It
implements the seven changes of the 2026-11-21 storage audit
(`paralith-harness-prep/docs/STORAGE_UPLOAD_AUDIT.md`) on one principle:

> A process is its spawn plus its ordered signed messages. Everything else
> is cache.

- Kept forever, locally: spawns, modules, signed messages and assignments,
  one packed row per slot.
- Kept on a rolling basis: the head state, recent slots, and a bounded set
  of snapshot checkpoints. Anything older is rebuilt by replay from the
  nearest kept checkpoint.
- Uploaded: spawns and modules for every process (so Lunar can open them
  by ID), and the schedule windows the operator lists.

The next deploy wipes every node, so none of this migrates old stores.

## Commits

| Commit | What |
|---|---|
| `bench: storage_bench` | Bench: bytes per slot by row family, plus the `kill -9` crash test |
| `retention: thin old snapshot checkpoints by age tiers` | Snapshots below the floor are thinned to hourly for a day, daily for a week, weekly for a year. Defaults: keep 1 day of slots, 1 hour for a full-state process |
| `process: rebuild a pruned slot by replay instead of a 410` | An old slot is rebuilt by replay and stored nothing; now an async job (below) |
| `replica: keep only the head and newest snapshots` | Bridge replica `.computed` is pruned every 100 slots |
| `follower: replicas on the store volume` | The follower's `admission-replica` moves beside the first LMDB store, and is pruned the same way |
| `process: snapshot by replay time` | Snapshot once 60 s of compute has passed since the last one (`process-snapshot-replay-ms`) |
| `scheduler: one packed row per assignment` | An assignment and its message are one row (`cache-pack`), plus one link row per extra ID |
| `journal: journal a large content-addressed row once per run` | A module is no longer re-journaled with every message that carries it |
| `uploader: spawns and modules, unfunded backoff, cost per day` | Spawn and module upload, 402 backoff, fingerprint latency, `status/0` with bytes and $ per day |
| `lmdb: survive a full disk` | The elmdb writer survives ENOSPC and a full map; `health/0`, plus disk and map fill gauges |
| `push: never rebuild a pruned slot for a push` | A push of a pruned slot stays a 410; an old outbox is never delivered twice |
| `cold: hb_store_cold, the share as a cold tier` | Slots, checkpoints and replica snapshots shipped to the node's own folder. Old local slots are pruned once shipped. Reads fall back to the share, then verified peers, then verified Arweave |
| `process: old-slot rebuilds as async jobs` | `hb_history`: 202 with ETA, coalesced, cached, bounded; replaces the one-at-a-time 503 |
| `uploader: bytes and $ per process` | `status/0` also lists `processes` |

Each commit is one topic and touches the files it names, so each can be
cherry-picked onto `paralith/rebase-edge` on its own. The packed-row commit
is the one that touches core (`hb_cache`, `hb_link`), so it is the likeliest
to need a hand merge.

## Measured: before and after

`bench/storage_bench.erl` runs 300 rounds (900 slots) on a local node in
Docker, with the live node config and three process shapes:

- **game**: `lua-patch`, 300 players of about 3 KB each;
- **book**: `lua-patch`, 2,000 orders, publishing a 50-order key;
- **token**: `lua@5.3a` full state, 8,000 balances, about 330 KB compressed
  per slot.

Every input is a client-signed ANS-104 item of about 1.7 KB. A bridge-style
replica follows the token over HTTP, and the irreplaceable store is
journaled. State data is pseudo-random: a repeated pad compressed the game's
1 MB state to 18 KB, which real state does not do.

### Bytes on disk per slot

| Store | Before (51dd818e1) | After | How |
|---|---:|---:|---|
| Irreplaceable, logical | 12.1 KB, 93 rows | **4.0 KB, 4 rows** | One packed row per slot holds the assignment and its message; a link row for each other ID |
| Irreplaceable, file growth | 22.4 KB | **5.2 KB** | Fewer, smaller rows fill LMDB pages tighter |
| Offsite journal (gz) | 3.6 KB | 3.6 KB | gzip had already removed the exploded form's redundancy. The live 14 MB-per-minute module rewrites are fixed separately |
| Replica `.computed`, kept | 326 KB per token slot, forever (99.5 MB after 305 slots) | **0.7 MB in total**, bounded | Head plus the 2 newest snapshots and 2 newest states |
| Replica verified store | 12.3 KB per slot | **4.0 KB per slot** | Packed |
| Main, delta process (game / book) | 2.4 / 6.8 KB per slot, kept 3 days, every snapshot forever | Same per slot, kept **1 day**, snapshots **bounded** | Retention tiers |
| Main, full-state token | 326 KB per slot, kept 3 days, every snapshot forever | Same per slot, kept **1 hour**, snapshots **bounded** | `store-retention-full-keep-seconds` |

Slot time did not move: game, book and token medians were 45, 37 and 196 ms
before, and 41, 33 and 188 ms after (run-to-run noise).

The replica file still grew 120 KB per slot during the run. That is LMDB's
high-water mark before freed pages are reused, and it stops at about
100 slots of states (one prune interval), about 35 MB for this token. Before,
it grew 372 KB per slot with no limit.

### At 1,000-user rates

Rates are the audit's: about 13 slots/s across the fleet (paralith about 7,
rng about 6, public about 0.5), and each attestor replicates the PUSD lanes.
Live message sizes come from the audit: today the irreplaceable store holds
about 30 KB per slot. Packed, it holds about 5-6 KB, the size of the signed
items plus about 1 KB of row overhead.

| | Before | After |
|---|---:|---:|
| Messages and assignments (forever) | ~30 KB × 13/s = **34 GB/day** | ~5.5 KB × 13/s = **6.2 GB/day** |
| Bridge replicas (3 nodes) | ~85 GB/day per node = **255 GB/day** | **0** growth; ~4 MB per lane per node, bounded |
| Full-state per-slot states (tokens, followers) | 3 days kept, then every snapshot forever: **~30+ GB/day** | **0** growth; a 1-hour window, ~1-3 GB per node, bounded |
| Snapshot checkpoints | every 1,000 slots or 15 min, forever | at most 2 + 24 + 7 + 52 = 85 per process, about 1 MB each |
| Delta processes | 3 days of deltas | a 1-day window, about 2.5 GB on paralith, bounded |
| **Fleet growth** | **~270 GB/day** (audit) | **~6 GB/day** of local disk, plus about 4 GB/day of journal on the share |

A 1 TB authority disk now lasts years for messages, against about a week
before. The fixed footprint is roughly 10-15 GB per node.

## Recovery test

Run in Docker (`storage_bench:crash_load/1`, `crash_verify/1`). The load
schedules and computes one message to each of the game, book and token per
round. The test killed the VM with `kill -9` at round 156, mid-load, then
restarted it on the same stores:

| | game | book | token |
|---|---|---|---|
| Every slot present and in order (0..head, each assignment naming its slot) | yes (head 157) | yes (156) | yes (156) |
| Every acknowledged slot present | yes | yes | yes |
| Head after restart equals a recompute from the irreplaceable store alone | yes | yes | yes |
| Head after restart (resume from newest snapshot) | 1.3 s | 0.6 s | 8.4 s |
| Head recomputed from the irreplaceable store alone, empty main store | 4.9 s | 0.9 s | 15.9 s |
| Old slot (78) after retention at keep-seconds 0, equals a fresh recompute | yes | yes | yes |
| Old slot read time | 13 ms (still stored) | 9 ms (still stored) | **5.5 s**, rebuilt by replay from slot 0 |

The token's old slot was gone and was rebuilt by replaying 79 slots, at
about 70 ms per slot. The game and book had only their slot-0 snapshot, so
retention had no floor to prune below in this short run. The
`dev_process` eunit `pruned_slot_is_replayed_not_stored_test_` covers the
delta case: slots 1, 6 and 24 are deleted, then rebuilt from slot 0 with
exactly 31 executions, nothing written, and the head still advances.

Replay cost: a restart replays at most about 60 s of compute
(`process-snapshot-replay-ms`). An old-slot read replays at most one tier
bucket: an hour of slots within the last day, a day within the last week, a
week within the last year, and from the spawn beyond that. Only one old-slot
replay runs per node; a second gets 503 with `retry-after: 5`.

A full disk was tested on a 220 MB tmpfs (`enospc.erl`, not committed). The
filesystem was filled until a commit failed with ENOSPC. `health/0` then
reported the store unhealthy (error logged once), and writes were still
accepted and held. After the junk file was deleted, the next flush succeeded
with no restart, health cleared, and every row, including those written
while full, read back. Before, the writer thread exited and the store refused
writes until a restart (breakpoint 50).

## Tests

Eunit, in Docker (erlang 27):

- Passed: `hb_store_retention` (19, with new `thin_test` and
  `thinned_below_swept_floor_test`), `dev_process` (10),
  `dev_process_worker` (17), `lib_process_cache` (100),
  `lib_process_delivery` (15), `lib_bridge_replica` (24, with
  `prune_keeps_the_head_test`), `dev_scheduler_follower` (21),
  `dev_scheduler` (20), `dev_scheduler_uploader` (15, with
  `unfunded_backs_off_test`), `hb_cache` (61, with `packed_message_test`),
  `hb_link`, `hb_store_offsite_journal` (35, with
  `large_blob_journaled_once_test`), `hb_store_lmdb` (5),
  `dev_scheduler_server` (24), `dev_scheduler_cache` (11),
  `dev_scheduler_admission` (18), `hb_store_irreplaceable`, `dev_lua_patch`
  (34), `hb_message`, `dev_push` (52).
- `dev_push`: a push of a pruned slot never history-replays
  (`process-history-replay` false in `slot_results/4`). A push must not
  deliver an old outbox again; `late_push_is_410_test_` pins this.
- `dev_bridge_attestor`: 57 of 58 pass. The failure,
  `root_endorse_endpoint` (a mock gateway's second post answers 429), fails
  the same way on `51dd818e1`, so it predates this branch.
- Not run: paralith-mono's `backend/native/e2e.mjs`. It plays the deployed
  game graph (`live-process.txt`), which a local node does not have without
  a full local deploy of every contract. It is the one check of the real
  signature path, so run it against the first test node this build lands on.

## What we cannot shrink, and what it costs

These remain after the changes. Each is either the data itself or the price
of a guarantee we keep.

1. **The signed messages and their assignments: about 4-6 KB per slot,
   forever, about 6 GB/day across the fleet at 1,000 users.** This is the
   process. Per slot it is two signed ANS-104 items, and their floor is:

   | Part | Bytes per slot | Shrinkable? |
   |---|---:|---|
   | RSA-4096 signatures, message and assignment | 2 × 512 | Only with a smaller key type |
   | RSA-4096 owners (public keys), message and assignment | 2 × 512 | Yes: the scheduler's key repeats in every assignment, and a client's in every message it sends |
   | Assignment tags (process, slot, block height, hash and timestamp, base-hashpath, body ID, epoch) | ~450 | No: these are what the signature covers |
   | Message tags and data | 200-2,000 | No: they are the input |
   | Packed-form overhead (`committed` lists, `original-tags` kept to rebuild exact bytes) | ~0.4-0.5 KB compressed | A little: store the raw ANS-104 bytes and decode on read |
   | LMDB keys, link rows, page slack | ~1.2 KB | A little |

   So about half of every slot is RSA key material, which compression cannot
   touch. **Message size is the next lever**, in this order:

   - Store each signer's public key once instead of in every row: about
     1 KB/slot, about 20%.
   - Sign assignments with a secp256k1 scheduler key instead of RSA-4096:
     about 0.9 KB/slot. This is a protocol and trust decision.
   - Store the raw ANS-104 bytes instead of the structured term: about
     0.4 KB/slot.

   Together these reach about 3 KB/slot. Below that is the input itself.

2. **The offsite journal: about 3.6 KB/slot gzipped, about 4 GB/day fleet.**
   It is the crash-proof copy of item 1. It belongs on the shared cold tier
   (next section), not on local disk.

3. **A rolling window of recent states: about 1-3 GB per node, bounded.**
   It holds 1 day of deltas and 1 hour of full states, plus the newest 32
   full states. This is what serves recent `compute&slot=` reads (Lunar's
   rows, pushes) without replay. A shorter window means more replays, so it
   is a latency choice, not a floor.

4. **Snapshot checkpoints: at most 85 per process, about 1 MB each, about
   4 GB per node at about 50 processes, bounded.** This cost buys bounded
   replay time for old reads (at most one bucket). Dropping tiers makes old
   reads replay from the spawn: hours for the game.

5. **Full-state processes write their whole state every slot: 0.3-0.8 MB
   per slot of I/O, not of retained disk.** `lua@5.3a` tokens (RNG token,
   WIN, YAP) cost this until they move to `lua-patch`, which is a contract
   change and not a storage one. With retention it no longer accumulates,
   but it still sets each slot's write cost and the replica's file
   high-water mark.

6. **Three replicas of every PUSD ledger, one per attestor.** This is the
   price of independent k-of-n endorsement. Each is now bounded (a few MB per
   lane).

7. **Uploads.** At 1,000 users, full archive windows for every process
   would be about 2.3 GB/day: about $106/day by Turbo card or $70/day in AR.
   Fingerprints cost only the item fee. Spawns and modules are a one-time
   cost per process of a few KB, up to about 0.7 MB for a module. The cost
   now shows in `dev_scheduler_uploader:status/0`.

## Cold tier on the shared NFS (`hb_store_cold`)

Every node mounts the shared NFS (about 3 TB) over WireGuard. It is a
residential link and will go down. Local disk keeps only hot data. Each node
writes cold data only to its own folder, `hyperbeam/<node>/`, which is
mounted at `/app/export`. The other nodes' folders are mounted read-only at
`/app/cold-peers/<node>`.

### What lands where

| Data | Local (hot) | Share, own folder |
|---|---|---|
| Head state, newest snapshots, rolling window | yes | no |
| Packed messages and assignments | the newest `keep-local-seconds` (7 days) | `slots/<pid>/<from>-<to>.hbc`, every slot |
| Offsite journal | until shipped | `irreplaceable/` |
| Snapshot checkpoints | local tiers | `checkpoints/<pid>/<slot>-<ms>.hbc`: **every** snapshot for `checkpoint-days` (30), then the oldest per day |
| Bridge replica snapshots | head and newest 2 | `replica/<lane>/`: the newest 2, so a new or wiped node starts from one instead of slot 0 |

Each file starts with `HBC1` and the SHA-256 of the rest. It is written to
`.tmp`, synced, renamed, and read back before its slots count as shipped.

### Rules, and how each is met

1. **Never block on NFS.** A server ships on a timer (`interval-ms`, 60 s).
   Every filesystem call runs in a process of its own and is abandoned after
   `io-timeout-ms` (10 s), so a hung mount costs a timeout, never a blocked
   request. A failed tick marks the share unavailable: `status/0` reports
   `available` false with `unavailable_since` and `last_error`, and the
   `hb_cold_available` gauge drops to 0. Retries back off to 10 minutes.
   Local slots are deleted only after their file is confirmed **and** they
   are older than `keep-local-seconds`. An outage therefore costs local disk,
   never data, and the shipper catches up from its markers when the share
   returns (`hb_cold_lag_slots`). Checkpoints and replica snapshots are only
   *added* locally from the share, never removed for it.
2. **Each node writes only its own folder.** Anything read from a peer's
   folder or from Arweave is verified before use. Every signature must
   verify. It must name the process and slot asked for. One of the
   process's schedulers (from the definition this node holds) must have
   signed the assignment, and the message must verify too. A checkpoint or
   replica snapshot is taken only from the node's own folder or a peer marked
   `trusted`, and public is never trusted. A replica snapshot must also come
   from the same build.
3. **Reads fall back: local, then cold, then Arweave.**
   `dev_scheduler_cache:read/3` tries this order:
   - the local store;
   - the node's own folder;
   - the peers' folders;
   - the process's archive windows on Arweave (`ao-schedule-archive`, format
     2, full windows).

   A replay with no local checkpoint below its target restores the nearest
   shipped one first (`restore_checkpoint/3`).
4. **Bytes per day to the share at 1,000 users:**

   | | GB/day |
   |---|---:|
   | Packed slots (`slots/`), all nodes, ~4-6 KB per slot | ~5.5 |
   | Journal (`irreplaceable/`, gzipped) | ~4.0 |
   | Every snapshot checkpoint, busy processes (game shard ~0.4, book ~0.2, token ~0.07 each) plus idle ones (at most 96 a day) | ~2-3, bounded: after 30 days thinned to 1 a day per process (~0.05 GB/day) |
   | Replica snapshots, newest 2 per lane | bounded, ~10 MB per node |
   | **Total** | **~12 GB/day while checkpoints are inside their 30 days**, then **~9.5 GB/day** of permanent growth |

   The journal and the `slots/` files hold the same messages twice. The
   journal exists for crash restore; the slot files for reads. Dropping the
   journal once a range's slot file is confirmed would save ~4 GB/day; that
   is a decision for Tyler. At ~9.5 GB/day, 3 TB lasts about 10 months at
   1,000 users; without the journal copy it lasts about 18 months.

### Configuration (`cold-tier`, per node in paralith-deploy1117 `infra/nodes/nodes.json`)

| Node | Own folder (`exportMount` → `/app/export`) | Peers (`coldPeersMount` → `/app/cold-peers`, read-only) |
|---|---|---|
| paralith | `/mnt/pxe-backup/hyperbeam/paralith` | `/mnt/pxe-backup/hyperbeam`: rng (trusted), public (untrusted) |
| rng | `/mnt/pxe-backup/rng` | `/mnt/pxe-backup`: paralith (trusted), public (untrusted) |
| public | `/mnt/pxe-own` | none: it cannot read the others |

Keys: `path`, `peers` (`[#{node, path, trusted}]`), `keep-local-seconds`
(604,800), `checkpoint-days` (30), `interval-ms` (60,000), `io-timeout-ms`
(10,000), `range-slots` (1,000), `arweave-gateway` (`https://arweave.net`,
or `false`).

### Tests (`hb_store_cold`, 7)

- **Ship, prune, read back:** slots are shipped, deleted locally once
  confirmed and old, and read back. The newest 16 stay local.
- **Share goes away:** the folder becomes a file mid-run. The tick fails,
  status says unavailable, every slot stays local, and reads miss without
  failing. When the folder returns, the next tick ships the backlog, prunes
  it, and reads return.
- **Hung call:** a hung filesystem call costs its timeout.
- **Peer reads:** a peer's slot is used only when the process's scheduler
  signed it; a forged one is refused.
- **Checkpoints:** every one is shipped, restored nearest below the target,
  taken from a peer only when trusted, and thinned after `checkpoint-days`.
- **Replica snapshot:** shipped, then seeds an empty computed store of the
  same build; another build's is refused.
- **Arweave fallback:** the slot is found in a stub gateway's archive window
  and verified.

## Old-slot reads: async rebuilds (`hb_history`)

A read of a slot whose state was pruned never replays in the request:

- **Asynchronous.** The request starts one rebuild job and is answered at
  once with **202**, `retry-after`, and "Rebuilding slot S of P by replay: N
  of M slots done, ETA X s". The job replays at low priority from the nearest
  snapshot, using local first, then the share, with assignments read local,
  then share, then Arweave.
- **Coalesced.** A repeat request for the same slot, or for any slot the
  running job of that process will pass, joins the job instead of starting
  one. The job keeps those slots' states when it reaches them.
- **Cached.** Rebuilt states stay in memory for `process-history-ttl-ms`
  (30 min), at most `process-history-cache` (32) of them, so follow-up reads
  are instant.
- **Bounded.** At most `process-history-jobs` (2) run per node; past that a
  request gets 503 with `retry-after: 30`.
- **Observable.** `hb_history:status/0` lists jobs (done, total, slots/s,
  ETA) and cached states. Gauges `hb_history_jobs` and `hb_history_cached`,
  and the counter `hb_history_replayed_slots_total`, follow them.
- **Pushes excluded.** A push never takes this path, so it never delivers an
  old outbox again.

Test (`dev_process`, `pruned_slot_is_replayed_not_stored_test_`):

1. Slots 24, 1 and 6 are deleted.
2. All three are answered 202.
3. **One** job of exactly 24 slot executions serves all three.
4. A repeat read is instant from the cache, with no execution.

## Replay window: how far an old read replays

An old read replays from the nearest snapshot at or below its slot.
Snapshots are taken every `process-snapshot-slots` (1,000), every 15 minutes
(`-time` 900), and after 60 s of compute (`-replay-ms`), whichever comes
first. So the snapshot gap is at most `min(1000, 900 s × rate, 60 s ÷ cost)`
slots. Measured replay cost: game 31 ms/slot, token 70 ms/slot (kill -9
bench).

Worst case, with **local tiers only** (no share):

| Age of the slot | Nearest kept snapshot | Game at 7 slots/s | Token at 1 slot/s |
|---|---|---|---|
| < 1 day (delta) / < 1 h (full state) | the slot is stored | 0 | 0 |
| 1 h – 1 day (full state) | hourly | (stored) | 3,600 slots, **4.2 min** |
| 1 – 7 days | daily | 604,800 slots, **5.2 h** | 86,400 slots, **1.7 h** |
| 7 days – 1 year | weekly | 4.2 M slots, **36 h** | 0.6 M slots, **12 h** |

Worst case, with **every snapshot on the share** (`checkpoint-days` 30, now
the default):

| Age | Game | Token |
|---|---|---|
| up to 30 days | ≤ 999 slots, **≤ 31 s** | ≤ 856 slots (60 s cap), **≤ 60 s** |
| beyond 30 days (daily) | ≤ 1 day of slots, 5.2 h | ≤ 1.7 h |

So the time-based tiers alone are too sparse for a busy process: a day of
game slots is hours of replay. This build does not make the local tiers
denser, which would cost ~0.4 GB/day of local disk per busy process.
Instead it ships **every** snapshot to the share. For 30 days the worst case
is then Tyler's "X−1 slots since the last snapshot", where X is at most
1,000 slots or 60 s of compute, so an old read takes at most about a minute.
To keep that bound for older reads, raise `checkpoint-days`. Each extra day
costs a busy process ~0.4 GB on the share, and the 1,000-slot snapshot
interval already bounds the slot count.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `store-retention-keep-seconds` | 86,400 (was 259,200) | Slots newer than this are kept whole |
| `store-retention-full-keep-seconds` | min(keep-seconds, 3,600) | The same, for a full-state process |
| `store-retention-checkpoint-tiers` | `[[3600,24],[86400,7],[604800,52]]` | Below the floor, keep the oldest snapshot of each of the newest N buckets. `[]` keeps only the newest |
| `process-history-replay` | `true` | Rebuild a pruned slot by replay (a job, `hb_history`). `false` keeps the old 410 |
| `process-history-jobs` / `-ttl-ms` / `-cache` | 2 / 1,800,000 / 32 | Rebuild jobs per node; how long and how many rebuilt states stay cached |
| `process-snapshot-replay-ms` | 60,000 | Snapshot after this much compute since the last snapshot. `false` turns it off |
| `scheduler-pack-messages` | `true` | Write assignments and their messages packed |
| `scheduler-upload-spawns` | `true` | Post each published process's spawn and module once |
| `scheduler-fingerprint-latency-ms` | 3,600,000 | How long a fingerprint window waits |
| `scheduler-upload-unfunded-backoff-ms` | 900,000 | Wait after a 402 or free-tier refusal |
| `scheduler-upload-usd-per-gib` | 47.29 | Price used for `status/0` cost per day |
| LMDB store `disk-alarm-pct` | 85 | Disk fill that raises `lmdb_store_disk_alarm` |

Build: `patches/elmdb-out-of-space.patch` must be applied after
`elmdb-delete.patch`. `bench/Dockerfile.branch` applies it, and so must
paralith-mono's `infra/nodes/Dockerfile.v011`. That file is not changed
here.

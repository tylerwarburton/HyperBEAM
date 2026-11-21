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
| `process: rebuild a pruned slot by replay instead of a 410` | An old slot is replayed for the one request, stores nothing, runs one at a time per node |
| `replica: keep only the head and newest snapshots` | Bridge replica `.computed` is pruned every 100 slots |
| `follower: replicas on the store volume` | The follower's `admission-replica` moves beside the first LMDB store, and is pruned the same way |
| `process: snapshot by replay time` | Snapshot once 60 s of compute has passed since the last one (`process-snapshot-replay-ms`) |
| `scheduler: one packed row per assignment` | An assignment and its message are one row (`cache-pack`), plus one link row per extra ID |
| `journal: journal a large content-addressed row once per run` | A module is no longer re-journaled with every message that carries it |
| `uploader: spawns and modules, unfunded backoff, cost per day` | Spawn and module upload, 402 backoff, fingerprint latency, `status/0` with bytes and $ per day |
| `lmdb: survive a full disk` | The elmdb writer survives ENOSPC and a full map; `health/0`, plus disk and map fill gauges |

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

## Cold tier on the shared NFS (design; local hot tier landed)

Every node mounts the same NFS share over WireGuard (paralith:
`/mnt/pxe-backup`). It is a residential link and will go down. Local disk
keeps only hot data; cold data moves under `<cold>/<node-name>/`.

### What lands where

| Data | Local (hot) | Share (cold) |
|---|---|---|
| Head state, newest 2 snapshots, rolling window | yes | no |
| Packed messages and assignments | newest N days (`cold-after-seconds`, default 7 days) | all, as journal segments |
| Offsite journal | until shipped and confirmed | all (already `irreplaceable-export.path`) |
| Daily and weekly snapshot checkpoints | newest of each tier | all |
| Replica snapshots | head only | the newest per lane, so a new or wiped node starts from it instead of slot 0 |

### Rules, and how each is met

1. **Never block on NFS.** Every write to the share is made by a shipper
   process off the write path, as the offsite journal already does: copy to
   `.tmp`, sync, rename, then append to `manifest.log` with CRC and a hash
   chain. Data stays local until its copy is confirmed in the manifest. A
   failed or hung share stops only the shipper. `status/1` and a
   `hb_cold_lag_bytes` gauge report how far behind it is, and the node stays
   healthy. Local pruning of cold-eligible data waits for the manifest, so a
   share outage costs local disk, never data.
2. **Each node writes only its own directory and may read the others'.**
   Signed messages and assignments read from another node's directory are
   verified before use: signatures, the scheduler's key, slot order and the
   `base-hashpath` chain. That is the same check `lib_bridge_replica:step/2`
   applies to a schedule it fetches. A checkpoint from another node is used
   only if that node is named in `cold-trusted-nodes`. Public is never on
   that list. Otherwise the checkpoint must match an attested state hash, or
   it is recomputed.
3. **Reads fall back: local, then cold, then Arweave.** A slot read
   (`dev_scheduler_cache:read/3`) that misses locally reads the share's
   segment index for that process and slot, then the uploader's Arweave
   windows (the follower already reads these). A checkpoint read for replay
   does the same.
4. **Bytes per day to the share at 1,000 users:**

   | | GB/day |
   |---|---:|
   | Journal (packed messages and assignments, gzipped), all nodes | ~4.0 (paralith ~2.2, rng ~1.6, public ~0.2) |
   | Daily snapshot checkpoints, ~50 processes × ~1 MB | ~0.05 |
   | Replica snapshots, 4 lanes × 1 MB × hourly × 3 nodes | ~0.3 |
   | **Total** | **~4.4 GB/day, about 1.6 TB/year** |

   The share is being enlarged to about 3 TB, which lasts about 1.9 years at
   this rate. Growth is linear in users. The journal is append-only and never pruned, because it is the
   irreplaceable copy.

### Status

- **Available now, with config only.** Every node ships its journal to its
  own folder, `hyperbeam/<node>/`. The shipper never blocks, keeps segments
  local until they are shipped, and poisons only itself on a failed sync.
  The folders are set per node in paralith-deploy1117
  `infra/nodes/nodes.json`: `exportMount` is mounted at `/app/export`, with
  `irreplaceable-export.path` `/app/export/irreplaceable`:

  | Node | Share mounted at | `exportMount` (its own folder) |
  |---|---|---|
  | paralith | share root at `/mnt/pxe-backup` | `/mnt/pxe-backup/hyperbeam/paralith` |
  | rng | `hyperbeam` at `/mnt/pxe-backup` | `/mnt/pxe-backup/rng` |
  | public | only `hyperbeam/public` at `/mnt/pxe-own` | `/mnt/pxe-own` |

  The next commits write their cold data under the same folder:
  `irreplaceable/` (the journal), `checkpoints/<pid>/`, and `replica/<lane>/`.
  Public cannot read the other folders, and is untrusted anyway.
- **Next commits, in order:**
  1. `cold: prune local packed slots older than cold-after-seconds once their journal segment is in the manifest, and read them back from the share`.
  2. `cold: ship daily/weekly checkpoints and replica head snapshots; restore a replica from the newest one`.
  3. `cold: read other nodes' directories, verified (and trusted-node checkpoints only)`.
  4. `cold: Arweave as the last fallback for a slot read`.

  These were not started in this pass, which went to the hot path.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `store-retention-keep-seconds` | 86,400 (was 259,200) | Slots newer than this are kept whole |
| `store-retention-full-keep-seconds` | min(keep-seconds, 3,600) | The same, for a full-state process |
| `store-retention-checkpoint-tiers` | `[[3600,24],[86400,7],[604800,52]]` | Below the floor, keep the oldest snapshot of each of the newest N buckets. `[]` keeps only the newest |
| `process-history-replay` | `true` | Rebuild a pruned slot by replay. `false` keeps the old 410 |
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

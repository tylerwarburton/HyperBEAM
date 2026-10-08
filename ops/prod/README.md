# Production node: config, deploy script and runbooks

The exact configuration and deploy tooling of the production HyperBEAM node
(`hyperbeam-prod`, served at hyperbeam.tylerw.ai via nginx -> 127.0.0.1:10000).
This branch (`hyperbeam-upgrades`) is the exact source of the running image;
these files are what it runs with. Keys are NOT here: they live in
`/opt/runerealm-hb/keys` on the host and never go in git.

| File | Live location on the host |
|---|---|
| `node-config.json` | `/opt/runerealm-hb/config/node-config.json` (mounted read-only into the container) |
| `hb-prod.sh` | `/opt/runerealm-hb/hb-prod.sh` (start/stop/restart/recreate/no-swap) |
| `scripts/cutover.sh` | wipe the store and move to a new image (2026-10-06) |
| `scripts/enable-essentials.sh`, `scripts/migrate.sh` | enable the separate essentials store and migrate a single-store node into it (2026-10-07) |
| `scripts/deploy-export.sh` | enable the essentials export to the NFS share and essentials pruning (2026-10-07) |

Every script backs up the config, the deploy script and the previous image id
beside the originals and has a `rollback <STAMP>` mode.

## Storage layout

| Store | Where | What | Bounded by |
|---|---|---|---|
| main LMDB | `/opt/runerealm-hb/store/lmdb` (local NVMe) | computed process state, checkpoints, deltas | `store-retention` (mark-and-sweep, every 10 min) |
| essentials LMDB | `/opt/runerealm-hb/store/essentials` (local NVMe) | assignments, process definitions, location/bundler/arweave records, upload marks | `essentials-retention-days` (only slots already shipped to the export) |
| essentials export | `/mnt/pxe-backup/hyperbeam/essentials` (NFS over WireGuard, slow) | compressed append-only segments + bases + manifest | 75% fill ceiling of the share |

## Rules (each one cost real time to learn)

- **Stop the bridge relayer before any node restart** (`systemctl stop
  paralith-bridge-relayer`) and start it after the node is serving.
- **Stop and restart with `-t 120`** (hb-prod.sh does). A node killed by
  docker's default 10 s timeout counts as an unclean stop and forces an export
  catch-up.
- **A restart stalls busy processes for about a minute** (cold resume from the
  last checkpoint). So does the first load after a long idle. Restart in quiet
  windows.
- **Never put LMDB on the NFS share** (mmap/locking unsupported) and never write
  one file per key there (~350 ms per file). Only large append-only segments.
- **Never delete or move `local-pruned` in the export directory.** Once essentials
  pruning has run, the share holds the only copy of older assignments.
- **Never lower an LMDB `capacity` below the live `data.mdb` size**: LMDB opens
  fine and then the store is silently write-dead (MDB_MAP_FULL).
- **`hb-prod.sh`'s `HB_IMAGE` default must match the running image**, or
  `recreate` silently downgrades prod (the deploy scripts update it).
- **`push-max-depth` must stay unset**: an explicit depth brings back the silent
  skip past 16 hops; the implicit bound defers instead.
- `process-now-from-cache: true` means `/now` serves the last computed state;
  clients must `push&slot` (or `compute&slot`) after scheduling to drive compute.
- Long `hb eval` calls hit an "Alarm clock" timeout: spawn the work inside the
  node and have it write a result file instead.
- The node still looks up unknown push targets (wallets, deleted processes) in
  remote stores; when arweave.net is slow, pushes can take ~90 s. Removing the
  remote stores from `store` ends that dependency (not yet done).

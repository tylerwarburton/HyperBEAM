# Bounded local storage (storage/bounded-local)

Branch `storage/bounded-local`, from `e5c12a8a5` (perf/view-reads `c8ff2b9cb`
plus Codex's two checkpoint-validation commits). The copy-only canary
scheduling commits (`bd51b3c80`..`703de2ef7`) are not in it: the continuous
shipper replaces them.

## The invariant

Local disk is bounded at steady state, whatever the length of the history.
The cold tier (the home NFS) is the only thing that grows.

- A **signed row** (an assignment and its message) leaves local disk only
  below its process's hot window, and only once a verified copy is on the
  share.
- A **derived row** (a computed state, a snapshot) never waits for the
  share. The share gets a sample of snapshots, so a slow link costs replay
  depth for old reads, never local disk.

Why Paralith filled: retention kept every snapshot the cold tier had not
shipped, wasm snapshots are 23 MB, and the link ships about 1 MB/s. And the
shipper was one all-or-nothing pass, so a timeout lost the whole pass.

## What a node keeps

| Row | Local | Share |
|---|---|---|
| Assignments and messages | the newest `hot-slots` (10,000) per process, plus slot 0 | every slot, `slots/<pid>/<from>-<to>.hbc` |
| Computed states, deltas | from the newest snapshot that is at least `store-retention-keep-slots` (2,000) below the head | none |
| Snapshots | the newest 2 per process | the newest one, at most once per `checkpoint-every-seconds` (3,600), thinned with age |
| Irreplaceable journal | until shipped | `irreplaceable/` |
| `message-slots` (dedupe index) | every row, about 100 B per message | none |

The dedupe index is the one local thing that still grows with history. At
150 writes/s that is about 1.3 GB a day. It is kept for now (Tyler's
decision 3, 2026-11-28).

## How a range ships

1. **Written.** The shipper writes the range's rows to a file (`.tmp`, fsync,
   rename) and records a `pending` row with the digest it wrote.
2. **Verified.** `verify` proves the share holds those bytes:
   - `readback` drops the page cache and reads the file back;
   - `coldcheck` asks the share's host (`verify-url`) to read it from its own
     disk, up to 64 files per round trip, with nothing read back over the link.

   Then, in one LMDB transaction, the range row is written and the pending
   row deleted.
3. **Deleted.** A range wholly below the hot window loses only the rows its
   slots own: the slot path, the packed row, and the rows that link to it.
   This happens in one transaction. Slot 0 stays, because the process's own
   ID links to slot 0's row.

A file's name is fixed by its process and range, so a crash between the file
and its mark writes the same file again. Only verified range rows ever decide
a delete. If verification is missing (no URL, a refused path, an unreachable
host), nothing is decided and nothing is deleted.

The shipper runs continuously. Each round gives every process
`ranges-per-round` ranges, with `concurrency` processes at once. Every item
has its own timeout (`io-timeout-ms` plus its bytes at `min-rate-bytes`) and
its own mark. A timeout costs one item. The round's workers are linked to it.

## The disk budget (`hb_store_budget`)

An LMDB file never shrinks, so file size cannot show pruning. Each store's
monitor reads its live pages (`elmdb:env_pages/1`, from
`patches/elmdb-env-pages.patch`) every 30 s. Its pressure is the worse of
two numbers:

- live bytes against the map;
- disk use, not counting the file's own free pages.

| Level | At | Effect |
|---|---|---|
| soft | 70% | hot window shrinks to `hot-slots-floor` (2,000); no checkpoint is shipped |
| high | 85% | retention keeps only the newest snapshot and the rows above it, every 10 s |
| hard | 95% | the scheduler refuses new messages: 503, `retry-after: 30` |

The thresholds are per store: `budget-soft-pct`, `-high-pct`, `-hard-pct`.
The gauges are `hb_store_live_percent`, `hb_store_pressure_percent` and
`hb_store_budget_level`. For a drill, `hb_store_budget:override(hard)` sets
the level by hand.

## Configuration (`cold-tier`)

| Key | Default | |
|---|---|---|
| `hot-slots` / `hot-slots-floor` | 10,000 / 2,000 | slots per process kept locally; under pressure |
| `prune-local-slots` | true | false holds every local slot |
| `verify` / `verify-url` | `readback` / none | `coldcheck` needs the URL |
| `checkpoint-every-seconds` | 3,600 | |
| `concurrency`, `ranges-per-round` | 4, 4 | |
| `range-slots`, `range-bytes` | 1,000, 8 MiB | |
| `io-timeout-ms`, `min-rate-bytes` | 10,000, 262,144 | |

`keep-local-seconds` is gone. Time cannot bound disk under load.

## Tests

- `hb_store_cold` (21 tests), among them:
  - a prune takes only the rows its slots own;
  - one failing process does not block another;
  - `coldcheck` with no URL, with a host reporting other bytes, then proven;
  - pressure shrinks the hot window;
  - a round killed at 14 moments (mid-file, between a file and its mark,
    mid-delete) loses nothing, and the next round deletes nothing again.
- `hb_store_retention` (25): an unshipped snapshot goes, and `high` keeps
  only the newest.
- `hb_store_budget`: the levels.
- `dev_scheduler` (22): at `hard` a new message gets a 503 and nothing is
  written.
- Also passing: `hb_store_lmdb`, `hb_file`, `hb_store_offsite_journal` (39),
  `dev_process` (11), `lib_bridge_replica` (28), `dev_scheduler_server` (32),
  `dev_process_worker` (17).
- Failing before this branch too: `lib_process_cache`
  `million_rows_bounded_test_`, and `dev_scheduler_cache`
  `cold_foreign_slot_verified_test_` (an `undef`: it calls TEST-only exports
  from a device test).

## Lab measurement (2026-11-28, Docker, 12 cores)

`bench/bounded_bench.erl`: 150 client-signed RSA writes/s across 6
`lua-patch` processes, computed every second, for 15 minutes; retention on,
`hot-slots` 10,000, share on local disk. Each process passes its window at
about 400 s.

| t (s) | writes | irreplaceable live MB | share MB | ship MB/s | lag slots | pruned |
|---:|---:|---:|---:|---:|---:|---:|
| 120 | 17,167 | 64.6 | 56.4 | 0.54 | 96 | 0 |
| 360 | 52,362 | 241.4 | 176.4 | 0.52 | 96 | 0 |
| 480 | 70,208 | 299.3 | 237.4 | 0.53 | 96 | 9,615 |
| 600 | 88,044 | 307.7 | 296.7 | 0.55 | 96 | 26,880 |
| 720 | 105,892 | 312.1 | 358.1 | 0.54 | 96 | 44,960 |
| 901 | 132,492 | 318.9 | 448.8 | 0.54 | 96 | 71,679 |

- **The irreplaceable store levels off** at its hot window: about 310 to 320 MB
  (6 × 10,000 slots × about 5.2 KB) once each process has passed it. Before
  that it grew at 0.73 MB/s. The share grows on at 0.53 MB/s.
- **Bytes per write:**
  - about 5.2 KB live in the irreplaceable store;
  - 3.6 KB on the share (compressed range files);
  - about 6 KB in the main store (below).
- **Shipping keeps up with writes at 150/s.** Lag holds at 96 slots, the 16
  newest per process that are not shipped yet. This is the node's own cost;
  the share was local disk, so the home link is not measured here.
- **Computed state is bounded:** 6.3 MB of `pstate/` rows after 132k writes.
- **Not bounded in this harness: the main store** grew 0.9 MB/s. Every input
  message was written to it exploded: 14 commitment rows plus fields, about
  3.4 KB logical per message. The probe showed it comes from scheduling
  through the AO resolver (`hb_ao:resolve(P, schedule POST)`), the harness's
  path; compute writes none. Paralith's key scan (11-28) found under 1 GiB of
  messages in its main store, which points to the HTTP path not doing this.
  Confirm on the AX102 with a real swarm (`hb_store_live_percent`, a prefix
  scan) before relying on it.

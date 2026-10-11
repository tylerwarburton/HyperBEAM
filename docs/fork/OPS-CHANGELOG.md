# Production operations changelog

Every change made to the live node, newest first, each with the exact command to
undo it. Add an entry **before** making the change, and record the outcome after.

Node: `hyperbeam-prod` on Box A · `hyperbeam.tylerw.ai` · store `/opt/runerealm-hb`

---

## Pending - not deployed: `now/<key>` reads cost the key, not the state (`process-now-key-reads`)

Source only (branch `perf/now-read-cache`); `ops/prod/node-config.json`
already carries `process-now-key-reads: true`, so it is ahead of the live
config until this ships. An image without the code ignores the key.

Why: on 2026-10-11 21:31-21:39Z the game process's `now/player-<addr>` reads
(~5 KB answers) took 50-350 ms each (avg 345 ms, max 3 s), and every other
process's `now/` reads slowed with them (all `now/` reads, 4 min: p50 26 ms,
p90 929 ms, max 9.2 s). Each read copied the newest state whole out of the hot
cache ETS table into the HTTP request process
(`dev_process_cache:hot_newest/2`, `ets:lookup`), then walked every key of it
again: `without_snapshot/2` (`hb_ao:set`) and AO-Core's key normalization on
the `now` result, and for a missing key `dev_message`'s case-insensitive
fallback normalizes the whole state once more. 1,072 reads/min on a state of
tens of MB is GBs/min of copying and garbage beside the game worker.

What changes, with the option on:

1. `hb_ao` names the next key in a `now` request's private `ao-next-key` when
   the next step is a single plain key (private keys are in no ID or hashpath).
2. `dev_process:now/3` then answers with a state holding only that key (plus
   `device`, `cache-control`, `hashpath-alg` if set), matched out of the hot
   cache entry by a match specification that copies only the value
   (`dev_process_cache:latest_fields/3`). AO-Core resolves the key on it as on
   the full state: same value, hashpath, status, 404. It falls back to the old
   path for a key a device exports (`now/compute`, `now/keys`, ...), a private
   key, a process with no hot cache entry, and a reader with messages queued
   (`notify_compute/4` sends listeners the whole state). Key reads run
   ungrouped. No new memory: it reads the entry the hot cache already holds.

Measured (`dev_process_now_tests`, `HB_NOW_BENCH=1:50`, 2 CPUs), per read p50:

```
state          in-process  old -> new     HTTP  old -> new
1 MB              0.51 ms -> 0.44 ms        5.1 ms -> 3.3 ms
49 MB (23,500     31.5 ms -> 0.25 ms       36.6 ms -> 3.1 ms
  players)
```

With 4 readers at ~20 reads/s on the 49 MB state, a worker-like allocation loop
went from p99 22 ms alone to 48 ms (old reads, read p50 57 ms) and to 13 ms
(new, read p50 0.6 ms). 50 reads of a 5,000-player (11 MB) state left 590 MB
of garbage before, 6 MB after.

Same responses: HTTP byte for byte (status, headers but `date`, body) and
in-process term for term, on a Lua process, for found keys, nested paths,
`at-slot`, missing keys, case-insensitive fallback, exported keys, and after
the process advances.

Undo: set `process-now-key-reads: false` (or remove it) and restart; or
redeploy the previous image.

---

## Pending - not deployed - compute-on-schedule catch-up spawns workers with the node's request options (branch `fix/catch-up-worker-opts`)

**Live state now:** prod runs with `process-compute-on-schedule: false` since
2026-10-11 22:09Z (config flag + restart only, image unchanged `upgrades-p`).
Undo of that: `cos-off.sh rollback 20261011T220823Z`.

The regression (A/B under the same swarm load, `upgrades-p`, 2026-10-11/12):
with `process-compute-on-schedule: true`, per-slot `store_ms` (the
`computed_slot ... store_ms` line) averaged 117-183 ms on every process and
every action (e.g. Lootbox.Open 10 ms -> 143 ms), `execution_ms` rose too, the
game fell behind at ~2 slots/s and the disk did ~2,300 reads/s. With it false:
store p50 6 ms, p90 17 ms, exec p50 11 ms, game live. Before `upgrades-o`
(6c78128f8, "catch-up hands its state to a worker") store was 7-40 ms.

Re-read of the prod logs (20:40Z-22:09Z on, 22:12Z- off): the averages are a
tail, not a uniform per-slot cost. On: store p50 9 ms, p75 17, **p90 531**;
~12% of slots stall 400-1300 ms (mode 550-600 ms), several processes in the
same instant, in `execution_ms` as well as `store_ms`. Off: p50 7, p75 11,
p90 20. No slot was computed twice in either window (two computes per slot
appear only across the 22:09 restart, as the cold replay from checkpoint 1000).
The sampler shows the queue and p99 rising from ~21:39Z as the swarm's request
rate rose; RSS stayed 3-4.5 GB, so not memory pressure.

What the branch changes: `catch_up_opts/1` (`dev_process_worker`) set
`spawn-worker` on the *scheduler call's* options, and a worker keeps its spawn
options for every slot it computes (they override each request's in
`server/4`), for up to `process-worker-max-idle` (24 h). Those options were
the waking HTTP request's with `hb_ao`'s temporary options stripped, or a
push's. The catch-up now resolves under the node's request options
(`hb_http_server:get_opts/1` + `http-extra-opts` + `force-message`, as
`dev_meta` does), so a worker it spawns is the worker an HTTP request would
spawn; with a live worker it just queues on it. A catch-up woken from inside a
worker (`process-workers`/`await-inprogress` off, reachable only via a
`compute&push=...` request) now delegates too instead of computing in its own
process (by construction; no test drives that path). Without an HTTP server
nothing changes.

Measured (`catch_up_spawns_request_worker_test_`, real HTTP node, A computed on
schedule, B by HTTP `compute`), spawn-option differences catch-up vs request:

```
before: cache-control  absent vs [always]   (worker overrides it to no-store/no-cache)
        force-message  absent vs true
        only           absent vs local      (same values: the node message carries every default)
  (catch-up woken by a push also adds push-in-worker=true, push-max-depth=16,
   push-depth-implicit=true)
after:  none
```

Per-slot store traffic of the worker (hb_store write/link/read calls, bytes of
values written), slots 4-8, A vs B:

```
before: write 20 / 1112 B, link 15, read 19   =   write 20 / 1112 B, link 15, read 19
after:  write 20 / 1112 B, link 15, read 19   =   write 20 / 1112 B, link 15, read 19
```

**So the option drift is real but is NOT what drives the prod store cost**:
in the harness neither the request-path differences nor the push-path ones
change a worker's per-slot store work (also checked with all hb_store calls,
and for a worker spawned by a push-woken catch-up: identical). This branch
removes the drift; it is not expected to fix the regression, and
`process-compute-on-schedule` should stay false until the 400-1300 ms stalls
are explained. Next places to look: what else runs only with cos on and
contends with the workers' writes (the catch-up processes themselves -- each
wake re-reads the process from the store, and each target copies the full
public state back from the worker; `scheduler-durable-confirm: commit` syncs
racing the workers' LMDB writes), measured on stage under swarm load.

Tests: see the commit message. Undo: do not deploy; nothing live changed.

---

## 2026-10-11 20:42Z - `hyperbeam:upgrades-p` (b2f84873fa82, source 3fa4ff1a6): worker GC policy and low-garbage Luerl collect

Deployed with config `process-worker-gc: checkpoint+idle`, `process-worker-gc-idle-ms: 5000`
(and `process-worker-fullsweep-after` absent). Game under light swarm load, before -> after:
exec avg ~1000 ms -> ~100-230 ms (p50 35 ms), store ~900 ms -> ~30-110 ms per slot; game
level with its schedule, 0 replay resets, 0 crash reports. Remaining tail: p90 ~750 ms.
Undo: `bash deploy-p.sh rollback 20261011T204101Z`.

What was deployed:

Two changes, both in source only; `ops/prod/node-config.json` already carries
the new keys, so it is ahead of the live config until this ships.

1. `process-worker-gc` (default `checkpoint+idle`; also `off`, `checkpoint`,
   `idle`) and `process-worker-gc-idle-ms` (default 5000). A process worker
   runs one full collection plus one minor (which promotes the state at the
   quiet point instead of inside the next request) after the request that
   handed off a checkpoint has been answered, and once after it has been idle
   for the delay; never inside the per-slot path, never repeatedly while idle.
   A compute-on-schedule catch-up that computed as leader drops its dead copy
   of the state. `process-worker-fullsweep-after` stays available but unset.
2. `patches/luerl-incremental-gc.patch`: a full `collectgarbage("collect")`
   (which the game runs at the end of every message) no longer rebuilds the
   VM's table/env/function stores, keeps its seen-sets off-heap (atomics), and
   marks without listing every table entry. Same stores, same free lists, same
   table indices as before: checked term-for-term in lockstep
   (`scripts/luerl_gc_detcheck.erl`), and the Lua/process suites pass.

Measured with `scripts/luerl_gc_bench.erl` (1 CPU, 286 MB live Lua state,
900k tables, an allocation-heavy action per message):

```
collect per message       msg p50   majors/msg  minor GC total  heap mean
old luerl, default        3980 ms      4.47         36.3 s       526 MB
old luerl, fullsweep 0    4014 ms      1.70           -         1040 MB
new luerl, default        1020 ms      0.47          2.2 s       492 MB
no collect, default         28 ms      0.03          0.3 s       543 MB
no collect, fullsweep 0     29 ms      0.05           -          411 MB
no collect, GC every 10     28 ms      0.13          0.9 s       433 MB
```

The forced collection costs ~250-290 ms on this state, once per checkpoint
interval (1000 slots on prod) and once per idle period. On prod a 1-2.5 s
phase would be several majors or a heap of GBs: measure the game worker's
`process_info(Pid, [total_heap_size, garbage_collection])` before and after.

Undo: redeploy the previous image; to keep the image and stop the forced
collections only, set `process-worker-gc: off`.

---

## 2026-10-11 19:42Z - `hyperbeam:upgrades-o` (d9925e2d9a44, source 6c78128f8)

Fixes the compute-on-schedule catch-up shipped in upgrades-n. It ran without
`spawn-worker` (hb_ao strips it from device-call opts), so it computed each wake
itself from the newest checkpoint and never left a live worker: every new message
to a hunt or vault replayed it from slot 0 (~500 slots), 5-7 cores under swarm
load, starving the game into a backlog spiral. It also crashed with `undef` at the
end of every catch-up (~2,163/h). After deploy: 0 resets, hunts compute only new
slots, CPU 5.4 -> 1.2 cores, 0 crashes. Config unchanged.

Undo: `bash deploy-o.sh rollback 20261011T194042Z`.

---

## 2026-10-11 18:35Z - removed `process-worker-fullsweep-after: 0` from live config

With `{fullsweep_after, 0}` every minor collection of a process worker is a
full copy of its heap. That cut the node's anonymous RSS from ~12 GB to
~3.8 GB, but under a heavy swarm the game worker (one large Lua state, hundreds
of MB) slowed 10-50x: per-action execution ~20 ms -> 300-1000 ms, store time
~10 -> ~400 ms, compute below 1 msg/s, schedule replies to the game 5-15 s.
Without the option (VM default `fullsweep_after`) it is back to ~5 msg/s and
~100 ms per action. Config only, one restart; no image change.

Undo: `fullsweep-off.sh rollback 20261011T183527Z`.

---

## 2026-10-11 16:51Z - `hyperbeam:upgrades-n` (61196afbbbc8, source 852c669d4)

Prod was `runerealm-hb:bridge-v2` (2b3f18fe, source 014bb536a). Added on top:
push-reply fix, linear `hb_escape` headers, worker GC options, bounded/budgeted
store retention, and the opt-in `process-compute-on-schedule` (default `false`):
compute each slot this node assigns in the background instead of waiting for a
`compute`/`push` (a schedule-only client left a game process at computed slot 4
with 2,004 assignments). With no live worker the catch-up replays from the newest
checkpoint up to `process-compute-on-schedule-replay-limit` slots (default 2 x
`process-snapshot-slots` = 2000 here; `infinity` for none), independent of
`process-historical-replay-limit` (64).

Config keys added (live and `ops/prod/node-config.json`):
`process-compute-on-schedule: true`, `process-worker-fullsweep-after: 0`,
`store-retention-max-cpu-pct: 25`. One clean restart (relayer was already stopped).

Undo: `bash deploy-n.sh rollback 20261011T165108Z` (restores config, `hb-prod.sh`
and the bridge-v2 image from the `.pre-n-20261011T165108Z` backups).

---

## Baseline captured 2026-09-25, before any change

Recorded so that every number below has something to be compared against.

```
git branch          fix/computed-cache-no-match
git HEAD            4c3ca2dc19f4f16f137e049693fec628faf06ae7  (clean tree)
running image       runerealm-hb:early-worker-handoff-v1  (built 2026-09-24 06:05)
image /app git sha  4c3ca2dc19f4f16f137e049693fec628faf06ae7  (clean)
                    -> prod is exactly at branch HEAD; it is NOT behind.

filesystem /        906G total, 600G used, 261G avail, 70%   (md2, shared with ~20 containers)
store/lmdb           71G   (live, data.mdb mtime 2026-09-25 04:34)
store/lmdb.OLD-...   199G  (orphaned, data.mdb mtime 2026-09-23 04:01)
store/gw-cache       2.0M
cache-http           4.2G

vm.swappiness       60      (not configured in /etc/sysctl.d - kernel default)
swap                32734 MB total, 5561 MB used
BEAM RssAnon        1731196 kB
BEAM RssFile       41736456 kB   (clean LMDB mmap - this is page cache, want it kept)
BEAM VmSwap         3429700 kB   (live Erlang heap paged out - this is the problem)
container limits    NanoCpus=0  CpusetCpus=""  Memory=0  memory.swap.max=max
```

Audit that motivated these changes:
`https://claude.ai/code/artifact/54d93efc-0937-46e2-9e61-6847a2e4bd63`

---

## 2026-09-25 - 5. Historical slot reads: replay cache

Commits `0025c376c` (match index) and `2a866ea2c` (replay cache). Images
`runerealm-hb:crt-matchindex-v1` then `runerealm-hb:replay-cache-v1`. Rollback
tags `rollback-pre-crt-matchindex-v1` and `rollback-pre-replay-cache-v1`.

**Symptom:** under load, `compute&slot=N/results/output/data` reads for
historical slots took 103-105 s (n=22, mean 72 s), all returning 200. Writes and
pushes were unaffected, there was no backlog, and no errors.

**Confirmed by experiment**, against a process with its head at 10806 and a
checkpoint at 9000 - service time is linear in distance past the checkpoint:

```
slot 9001 (  1 delta )   0.22 s
slot 9005 (  5 deltas)   0.59 s
slot 9020 ( 20 deltas)   2.36 s
slot 9060 ( 60 deltas)   8.08 s
slot 9120 (120 deltas)  12.90 s        -> ~107-135 ms per delta
```

Extrapolated to a slot just before the next checkpoint (999 deltas) that is
~107 s, which is the observed tail. The same table shows `process-snapshot-slots`
is dead for these processes: slot 9060 cost 60 steps, not the 10 it would have
cost if a checkpoint existed at 9050. `dev_process.erl:619` routes any
delta-bearing result - which `lua@5.3b` always produces - to
`should_snapshot_delta_slots/2`, which reads only
`process-delta-checkpoint-slots`. That key is unset, so the compiled default of
1000 applies and the configured 50 and 900 never execute.

**Cause:** `recent_put/5` anchors its retention window to the head of the
process, so the states a historical replay rebuilds are all below the window and
were discarded as fast as they were built. Nine near-consecutive slots requested
together each replayed the same chain from the same checkpoint.

**Fix:** `replay_put/4` keeps those states in a window that follows the slot
being rebuilt. Measured on the deployed build:

```
before:  cold 12060 = 12.13 s  ->  neighbour 12059 =  9.01 s
         cold 12160 = 31.00 s  ->  neighbour 12159 = 25.99 s
after :  cold 12560 = 16.35 s  ->  neighbour 12559 =  0.04 s
```

**Rejected alternative:** checkpointing more often. At the measured state size,
moving the cadence from 1000 to 64 slots takes a single process from ~10 GiB/day
to ~162 GiB/day, against a whole-node figure of ~30 GiB/day. That trades the
storage problem for the latency one.

**Final result**, nine consecutive historical slots never read before, on
`replay-cache-v2`:

```
slot 13400 = 19.07 s     <- cold, pays the full walk
slot 13401 =  0.10 s
slot 13402 =  0.09 s
slot 13403 =  0.10 s
slot 13404 =  0.07 s
slot 13405 =  0.08 s
slot 13406 =  0.11 s
slot 13407 =  0.26 s
slot 13408 =  0.17 s
TOTAL      = 20.1 s      (~104 s before)
```

Eight of nine reads are served from the cache. With the earlier count-based
bound only five of nine were, because the bound was reached constantly and each
flush dropped every process's window.

**Known limit:** a first read into a cold region is unchanged, 16-31 s. Only
repeated walks are removed. Making the first walk cheap needs more checkpoints,
which is the rejected alternative above.

**Follow-up in the same work:** the global bound was initially a count of 1024
entries. Live introspection showed 290 entries holding 96 MB - ~331 KB each -
so a count bounds memory only by accident, and reaching it flushed every
process's window at once. The bound now counts bytes
(`process-replay-cache-mb`, default 512).

**Rollback:** redeploy `rollback-pre-replay-cache-v1`, or revert the commit and
rebuild. The cache is derived entirely from durable deltas and checkpoints, so
there is no state migration either way.

---

## 2026-09-25 - 4. Deploy script: swap ban and a stale image default

`/opt/runerealm-hb/hb-prod.sh` (out of tree; backup
`hb-prod.sh.bak-pre-noswap-1790308657`).

**Change A:** added `no_swap()`, called from `start` and `restart` and exposed
as a `no-swap` verb. It writes `memory.swap.max=0` to the container's cgroup,
which is recreated with the container and so loses the setting on every
recreate. A Docker-initiated restart (`--restart unless-stopped`, e.g. after a
host reboot) does not run the script: `hb-prod.sh no-swap` must be run by hand
afterwards. This is the durable half of entry 2.

**Change B:** the default image was `runerealm-hb:process-id-verify-once-v1`
(2026-09-19) while production runs `early-worker-handoff-v1` (2026-09-24), so
`hb-prod.sh recreate` without `HB_IMAGE` set would have silently rolled
production back five days. Corrected to the running image, with a comment saying
it must track it.

**Rollback:** restore the backup listed above.

---

## 2026-09-25 - 3. CRT signing for RSA assignments

Commit `e846cd2f0`.

**Change:** `ar_wallet:sign/3` built `#'RSAPrivateKey'{}` with no CRT
parameters, so `rsa_pss:dp/2` performed a full-modulus `crypto:mod_pow/3`. Two
signatures are taken per pushed slot, one of them inline on the serialized
per-process scheduler loop. `rsa_pss:dp/2` gains a CRT clause that fires only
when the key record carries all five parameters; `ar_wallet` records those
parameters for keys that supply them.

No prime recovery was needed: the node's own keyfile
(`/opt/runerealm-hb/keys/node-key.json`) already carries `p`, `q`, `dp`, `dq`,
`qi`. The repo-local `hyperbeam-key.json` does not, which is why it serves as
the slow-path control below.

**Measured, A/B on two real 4096-bit keys in the build image:**

```
node-key.json      (carries p,q,dp,dq,qi)   verify=true   3.20 ms mean
hyperbeam-key.json (d,e,n only)             verify=true   9.62 ms mean
```

3.0x, inclusive of the verify-after-sign check. At two signatures per pushed
slot that is roughly 12.8 ms/slot.

**Test evidence** (`rebar3 as rocksdb,genesis_wasm eunit-all` in the build
image, against the same suite on the unmodified image):

| Run | Passed | Failed |
|---|---|---|
| stock image, no changes | 3584 | 12 |
| with CRT, run 1 | 3587 | 14 |
| with CRT, run 2 | 3589 | 12 |

3584 + the 5 new `ar_wallet` tests = 3589, against an identical set of 12
pre-existing failures (5 `push@1.0`, 3 `scheduler@1.0`, 4 `upload_failed:
assignment_result` from the absent test bundler). Run 1's two extra failures
were `bundler@1.0` chunk-retry count assertions that did not recur in run 2;
both suites in that pair ran at load average 16-19.

**Why this cannot sign incorrectly:** `dp/2` checks each CRT result with the
public exponent and falls back to the full-modulus path if it does not hold, so
a bad parameter costs speed and never correctness. `remember_crt_params/2`
refuses primes whose product is not the modulus.
`crt_matches_full_modulus_test` holds the PSS salt fixed and asserts the two
paths agree byte-for-byte.

**Not yet deployed.** Production still runs `runerealm-hb:early-worker-handoff-v1`
built from `4c3ca2dc1`. Deploying requires an image rebuild; tag
`rollback-pre-crt-signing-v1` against the running image first.

**Rollback:** revert the commit and rebuild, or redeploy the prior image tag.
Signature *output* is unchanged by this work - CRT and non-CRT compute the same
integer - so a rollback has no on-chain effect and no state migration.

---

## 2026-09-25 - 2. Stop swapping the BEAM's live heap

**Change:** set `vm.swappiness=1` host-wide, persisted in
`/etc/sysctl.d/30-hyperbeam-swap.conf`, then drained existing swap with
`swapoff -a && swapon -a` (42 s for 5.5 GB, with 59 GB available).

**Why not a container memory limit:** ~38 GB of the container's RSS is clean,
file-backed LMDB mmap. A `--memory` cap below the working set forces continuous
eviction of exactly the pages that keep LMDB reads at ~0.35 ms. The problem was
never total memory - it was the kernel treating GBs of live Erlang heap as
equally evictable as file cache.

**Result:** BEAM `RssAnon` 1.73 GB -> 5.67 GB, `VmSwap` 3.82 GB -> 0. Swap left
enabled as an overflow reserve, at 0 used. Lifetime counters before the change,
for later comparison: `pswpin` 17,683,430 and `pswpout` 17,585,158 pages over
13 days of uptime.

**This did not hold under memory pressure.** Running two full `eunit-all` suites
on the box afterwards pushed `VmSwap` back to 3.6 GB. `vm.swappiness=1` biases
reclaim away from anonymous memory but does not forbid swapping it, so under
genuine pressure the Erlang heap goes out again. The global setting is worth
keeping, but it is not the fix on a box shared with ~20 other containers.

**The durable fix is per-container** and is still OUTSTANDING:

```bash
CG=/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' hyperbeam-prod).scope
echo 0 > $CG/memory.swap.max     # forbid swap for this container only
swapoff -a && swapon -a          # drain what is already out
```

Clean page cache stays fully reclaimable, so the kernel evicts file pages
instead of the Erlang heap. **This does not survive `docker rm`** - it must be
re-applied whenever the container is recreated, so it belongs in the deploy
path, not just in a shell.

**Rollback:**
```bash
rm /etc/sysctl.d/30-hyperbeam-swap.conf && sysctl -w vm.swappiness=60
echo max > $CG/memory.swap.max
```

---

## 2026-09-25 - 1. Delete the orphaned 199 GB LMDB store

**Change:** deleted `/opt/runerealm-hb/store/lmdb.OLD-throughput-20260923T020338Z`
(`data.mdb` 212800102400 bytes apparent / 199 GiB on disk, plus `lock.mdb`).

**Why it was safe:** superseded by `store/lmdb` when the throughput build was cut
on 2026-09-23 04:03; last written 2026-09-23 04:01; `lsof` and `fuser` showed no
process holding it; not referenced by `node-config.json` or any container mount.

**Why it was urgent:** the root filesystem was 70% full with 261 GB free and
growing 37-83 GB/day, shared with ~20 other containers including six postgres
instances. Projected ENOSPC in 3-7 days.

**Result:** 198 GB freed in 0.2 s. Filesystem 72% -> 48% used, 249 GB -> 448 GB
available. The node stayed up and served 200s at 50-64 ms throughout.

**Rollback:** none - deletion is irreversible. The data was a superseded copy of
state that `store/lmdb` holds current. Accepted deliberately.

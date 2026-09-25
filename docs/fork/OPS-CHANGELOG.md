# Production operations changelog

Every change made to the live node, newest first, each with the exact command to
undo it. Add an entry **before** making the change, and record the outcome after.

Node: `hyperbeam-prod` on Box A · `hyperbeam.tylerw.ai` · store `/opt/runerealm-hb`

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

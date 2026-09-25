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

## 2026-09-25 — 3. CRT signing for RSA assignments

**Change:** `ar_wallet:sign/3` built `#'RSAPrivateKey'{}` with no CRT parameters,
so `rsa_pss:dp/2` performed a full-modulus `crypto:mod_pow/3`. Measured
10.3–12.1 ms per signature against 2.7–3.7 ms with CRT (3.3–4.0x). Two
signatures per pushed slot, one of them inline on the serialized per-process
scheduler loop.

Status: see the commit(s) on the branch recorded below.

**Rollback:** revert the commit and rebuild, or redeploy the prior image tag.
Signature *output* is unchanged by this work — CRT and non-CRT compute the same
integer — so a rollback has no on-chain effect and no state migration.

---

## 2026-09-25 — 2. Stop swapping the BEAM's live heap

**Change:** set `vm.swappiness=1` host-wide (persisted in
`/etc/sysctl.d/30-hyperbeam-swap.conf`), then drained existing swap.

**Why not a container memory limit:** 41.7 GB of the container's RSS is clean,
file-backed LMDB mmap. A `--memory` cap below the working set forces continuous
eviction of exactly the pages that keep LMDB reads at 0.35 ms. The problem was
never total memory — it was the kernel treating 3.4 GB of live Erlang heap as
equally evictable as file cache.

**Rollback:**
```bash
rm /etc/sysctl.d/30-hyperbeam-swap.conf
sysctl -w vm.swappiness=60
```

---

## 2026-09-25 — 1. Delete the orphaned 199 GB LMDB store

**Change:** deleted `/opt/runerealm-hb/store/lmdb.OLD-throughput-20260923T020338Z`
(`data.mdb` 212800102400 bytes apparent / 199 GiB on disk, plus `lock.mdb`).

**Why it was safe:** superseded by `store/lmdb` when the throughput build was cut
on 2026-09-23 04:03; last written 2026-09-23 04:01; `lsof`/`fuser` showed no
process holding it; not referenced by `node-config.json` or any container mount.

**Why it was urgent:** the root filesystem was 70% full with 261 GB free and
growing 37–83 GB/day, shared with ~20 other containers including six postgres
instances. Projected ENOSPC in 3–7 days.

**Rollback:** none — deletion is irreversible. The data was a superseded copy of
state that `store/lmdb` holds current. Accepted deliberately.

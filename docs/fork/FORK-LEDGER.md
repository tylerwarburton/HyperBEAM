# Fork ledger — how far this node is from vanilla HyperBEAM

Purpose: a single place that records **every way this deployment differs from
upstream `permaweb/HyperBEAM`**, so that any change can be rolled back and any
upstream merge can be planned rather than discovered.

Keep this file current. If you add a commit that prod depends on, add it here in
the same change.

---

## 1. The three bases

| Ref | Commit | Date | Meaning |
|---|---|---|---|
| `origin/edge` | `7db26c99f` | 2026-09-15 | Vanilla upstream tip (PR #1135) |
| prod fork point | `14e9f68a6` | 2026-08-25 | Where prod's branch left upstream (PR #1103) |
| prod branch tip | `4c3ca2dc1` | 2026-09-17 | `fix/computed-cache-no-match` — **what is deployed** |

**Prod is missing 37 upstream commits** and **carries 44 of its own.**

The gap matters in two specific ways:

- Upstream `5dbe16cf5` (*impr(cache): write the match index through the
  `cache-write` hook*) restructures the match index that our own
  `ec1bbf121` bypasses for computed process records. Any upstream merge has to
  reconcile these two deliberately, not textually.
- Upstream `ac146339f` (*deps(elmdb): pin feat/list-range*) moves the elmdb
  pin. Prod is still on `faa7623`. Any elmdb change (see the audit's §3.2
  `read_prefix_rows` → DirtyIo) must decide which pin it targets first.

The 7 in-flight PR branches are rebased onto **current** `origin/edge`
(0 commits behind), so they are *not* directly mergeable into prod's older base.

---

## 2. Deployed divergence: the 44 commits on prod

### 2a. Experiments and their reverts (10 commits)

These pairs cancel out *textually*, but do not read them as a dead end: the idea
behind the first pair was re-landed 32 hours later in a corrected form that is
deployed today. Read both sides plus the follow-up before re-attempting anything
in this area.

| Reverted | Original | Subject |
|---|---|---|
| `8d4fab95f` | `89a2f1ffc` | perf(process): keep private worker state resident |
| `05b6d99ab` | `032f157e7` | perf(process): resolve worker responses from cache |
| `745832af9` | `6e4382aca` | test(process-worker): resolve cached notifications |
| `9b613d5f0` | `179782b74` | test(process-worker): use a process id for cache reads |
| `2e4d851f7` | `d908a3182` | test(process-worker): derive the process cache key |

**What `8d4fab95f` actually reverted, and what replaced it.** `89a2f1ffc`
changed the notification *payload* sent to listeners, not the residency of
worker state; the reverted risk was that a listener could be handed the VM. The
corrected form landed as `be2c3bf4b` (*send listeners the public result, never
the VM*), which is in §2b and is running in the deployed image. The failure
worth reading before touching this area is not either perf commit but
`b0669b4cf` (*never continue from a cached public state*): its diff shows the
prior `server/3` adopted a cache-hit answer as live worker state, which is the
production failure that motivates the guards around any multi-slot walk.

### 2b. Live local-only production changes (the throughput work)

Not upstreamed. These are what make this node fast, and each is a rollback
candidate if prod regresses.

| Commit | Subject |
|---|---|
| `4c3ca2dc1` | fix: hand compute ownership to workers before execution |
| `ec1bbf121` | perf: skip reverse index for computed process records |
| `1bc65550d` | feat: allow local-only scheduler publication |
| `9d43b0754` | perf: remove Lua key walk and publish assignments once |
| `7bbafc5b7` | fix(process): verify a process definition once, not on every request |
| `bade1667d` | fix(process-cache): serve recent slots and the latest state from memory |
| `356581aff` | fix(push): resolve outbox targets locally first |
| `be2c3bf4b` | fix(process-worker): send listeners the public result, never the VM |
| `37e0a0ed7` | fix(push): leave a process worker behind after a pushed compute |
| `788db4959` | fix(process-cache): keep the hot state cache alive and newest-first |
| `61a1b5037` | fix(process): do not write cached process state back on now reads |
| `b0669b4cf` | fix(process-worker): never continue from a cached public state |
| `d738ba39b` | impr(scheduler): parallelize assignment publication |
| `52b7b037b` | perf(process): stop sizing every computed state |
| `3e5fc688c` | fix(process): check delta snapshot presence structurally |
| `5ca69c76e` | feat(process): persist lua 5.3b state deltas |
| `992da5723` | perf(lua): add incremental Luerl collection |
| `f7c5ff150` | feat(lua): add request-only delta device experiment |
| `6a46722a8` | perf(process): remove duplicate hot-path work |
| `8220887eb` | perf(lua): make Luerl full collection linear |
| `5358be468` | fix(process): hand off persistent worker names without a gap |
| `8fe807539` | fix(process): finish the worker slot-read merge the fork left half-applied |
| `1ba98bffc` | fix(process): keep the per-process worker alive past its first job |
| `d7cabfb8e` | fix(node): slot-unwrap and registration-race edits live-but-uncommitted on Box A |
| `838be3179` | fix(ao): spawn the persistent worker a resolution asks for |
| `1f3caa081` | fix(process-worker): treat uncomputed/wrapped slot as cache-miss |
| `41015e0c2` | perf(process): gate target-slot snapshot on configured cadence |
| `051a5a9b3` | perf(lua): compress serialized Luerl snapshots |
| `82d0049e8` | chore(lua): normalize patch artifacts |
| `924164be4` | chore(deploy): sync scheduler + lua snapshot to reviewed PR variants |

### 2c. Build/deploy plumbing

| Commit | Subject |
|---|---|
| `a174b0ec9` | build: track the Dockerfile that builds the production image |

### 2d. Prod-side versions of work now in flight upstream

Prod carries earlier, pre-rebase versions of these. The upstream PR branches
hold the rebased copies (§3). Do not fix a bug in one without the other.

| Prod commit | Upstream PR branch | Subject |
|---|---|---|
| `4dc12c067` | `fix/bundler-chunk-post-reason` | fix(bundler): return the reason from a failed chunk post |
| `e559d6dd1` | `fix/push-outbox-metadata` | fix(push): keep outbox entries' commitment IDs intact |
| `17e3046c5` | `fix/push-outbox-metadata` | fix(push): stop pushing a result's own metadata as outbox entries |

---

## 3. In-flight upstream PRs

Each has a worktree under `/root/HyperBEAM-prs/<name>` and a branch rebased onto
current `origin/edge`. These are **good changes we want upstream** — they are not
rollback candidates.

| Branch | Tip | Worktree |
|---|---|---|
| `fix/bundler-chunk-post-reason` | `6594b2d66` | `bundler-chunk-post-reason` |
| `feat/lua-5.3b-delta-state` | `08c1268da` | `lua-5.3b-delta-state` |
| `perf/lua-process-hot-path` | `dfd199702` | `lua-process-hot-path` |
| `perf/lua-snapshot-compression` | `f2828e6a7` | `lua-snapshot-compression` |
| `fix/process-worker-slot-normalization` | `bb2e50eb5` | `process-worker-slot-normalization` |
| `fix/gateway-location-fallback` | `67329b16d` | `push-legacy` |
| `fix/push-outbox-metadata` | `7150219d1` | `push-outbox-metadata` |

`fix/process-worker-slot-normalization` additionally carries hardening upstream
does not have yet: atomic leader election (`e25063267`), waiter-enrollment
hardening (`fa77af983`), and a collision-resistant execution identity replacing
`phash2` (`d6a8d72c7`). The audit's §1.2 (bind the execution group name once)
lands in the same code — **check that branch before implementing it.**

---

## 4. Out-of-tree state prod depends on

Not in git. Losing any of these loses the deployment.

| Path | What |
|---|---|
| `/opt/runerealm-hb/config/node-config.json` | Live node config (mounted read-only at `/app/node-config.json`) |
| `/opt/runerealm-hb/keys/node-key.json` | Node wallet — **no backup implies no node identity** |
| `/opt/runerealm-hb/store/lmdb` | The only durable copy of signed assignments |
| `/opt/runerealm-hb/cache-http` | `store-all-signed` audit log |
| Docker image tags `runerealm-hb:*` | Built artifacts; `rollback-pre-*-v1` tags are the rollback points |

Loose patch files in `/root/*.patch` and `/tmp/hyperbeam-*.patch` are historical
scratch, superseded by the branches above. They are not authoritative.

---

## 5. Rollback

Prod runs one container off a tagged image. To roll back, retag and restart —
no git operation is needed on the live box.

```bash
# What is running now:
docker inspect hyperbeam-prod --format '{{.Config.Image}}'
docker exec hyperbeam-prod git -C /app log -1 --format='%H %s'

# Roll back to the previous image (tags are chronological; docker images runerealm-hb):
docker stop hyperbeam-prod && docker rm hyperbeam-prod
# then re-run with the prior tag, e.g. runerealm-hb:computed-no-match-v1
```

Every image built for a deploy **must** be preceded by a `rollback-pre-<name>-v1`
tag pointing at the currently-running image. That convention is already in use;
keep it.

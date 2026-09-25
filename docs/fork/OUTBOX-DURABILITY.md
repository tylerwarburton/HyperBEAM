# TODO: make outbox delivery durable

**Status: partially shipped. The dominant loss source is closed; the guarantee is
not built.**

Figures are tagged **[M]** measured, **[C]** read from code, **[E]** estimated.

---

## 1. What was wrong, and what is fixed

A node operator reported outbox messages silently lost when a client's push was
cut short. Confirmed here.

**Mechanism [C].** `hb_http_server:handle_request/3` resolves the whole request
inline in the cowboy request process, so `dev_push:do_push/3` walks the outbox and
recurses through every downstream hop on that one call stack. Cowboy spawns that
process with `proc_lib:spawn_link` (`cowboy_stream_h.erl:49`) and tears it down
with `exit(Pid, shutdown)` then `exit(Pid, kill)` (`cowboy_children.erl:108`,
`:151`). `grep -rn process_flag src/` returns **zero hits** — HyperBEAM never
traps exits, and cowboy's own comment notes the shutdown grace only helps
trapping children, so delivery dies at the *instant* of the signal.

**Why it was silent [C].** The outbox belongs to the *compute* result and is
cached by `store_result` independently of delivery. **Nothing anywhere records
whether an outbox entry reached a target.** The per-entry `{id, target, slot}` map
exists only in the HTTP response body, which dies with the connection.

**Rate [M].** Over 75 minutes: `push_computing_outbox` 14,387 against
`push_compute_result` 13,765 — **622 pushes, 4.3%, killed in flight**. The step
between those events is wrapped in `try ... catch Class:Reason:Trace`, so only an
external exit signal can skip the completion event, and an exception reaching
`handle_error/6` would have emitted a 500 that these never did. **It is bursty,
not sustained**: across three later samples spanning ~20 min and ~4,400 requests
the gap stayed at exactly 622.

**The operator's case [M].** All 1,400 assignments of the target process across
slots 5883–7282 were read from the scheduler cache and compared by
`hb_message:id/3`: zero matches. The message was never scheduled.

### Shipped (commits 07180ceed, 9f4480147, 1a3e8c1cf, fcb314ed7)

- **Delivery detached from the caller.** `push_with_mode/3`'s sync branch runs
  `do_push/3` under a monitored, **unlinked** worker and waits for its result.
  A caller killed while waiting no longer takes delivery with it.
  `await_detached_push/2` re-raises the worker's exception with its original
  stacktrace; `push-in-worker` keeps recursive hops in-line so one client push
  costs one extra process, not one per hop.
- **Bounded in both dimensions.** Detaching removes the only cancellation the
  push path had. The first deployment bounded neither and reached **47.5 GB of
  process memory within an hour, and stopped serving**; rollback returned it to
  120 MB. Now: `push-max-depth` defaults to 16 for detached workers only (an
  in-line push is still cancellable and did not need it), and
  `push-max-workers` holds the node to 32 live deliveries, **falling back to
  in-line delivery over the bound** — which is a node without detachment, so
  refusing is never worse than not having the feature.
- **A second silent drop fixed.** `do_push/3` matches
  `#{<<"target">> := Target}`; entries are no longer lower-cased inside, so an
  entry naming its target `Target` fell to the catch-all and was rejected as an
  unavailable target with no event, counter or log line. Measured effect:
  `push@1.0` suite failures **5 → 1**.
- **nginx `proxy_read_timeout` 300s → 900s** on the catch-all location, matching
  `~lua@5.3[ab]`. `push&slot` resolves there, and a 300 s cut kills the request
  process exactly like a client disconnect. Slowest measured push is 167 s, so it
  was not firing yet.

### Client side, by the UI team

Callers no longer `abort()` the push at 90 s — the fetch stays pending on its own
connection (browser cap 10 min; Node's client cap ~300 s). Recovery tools fire
and do not wait, but hold the request open. Plus a keeper (`keeper:outbox`) that
every 2 minutes re-sends stuck battle Opens/Finals and hunt finals, waits 3 min
before acting, backs off, gives up after 5, dry-run unless `--apply`.

---

## 2. What is still not guaranteed

The shipped work converts "delivery dies with the client connection" into
"delivery continues until it finishes or the node stops". It is **not**
at-least-once:

- **A node that stops mid-delivery loses the hops in flight.** Restart, deploy,
  OOM, `docker rm` — there is no delivery record and **no retry**.
- **Past the depth bound** (16 hops) the chain is not followed. The target at the
  boundary is still scheduled; the hop beyond it is not.
- **Over the worker cap** (32) delivery is in-line, so it dies with its client
  again — safe only because the UI now holds the socket.
- **Duplicates widened.** There is no dedup by message ID anywhere in the
  schedule path (`dev_scheduler_server:do_assign/3` takes `current + 1`
  unconditionally), and push resolutions get `ungrouped_exec` so concurrent
  pushes for one slot are not collapsed. Previously a timed-out push stopped
  partway and its retry re-sent the prefix; now the orphan **runs to completion
  concurrently with the retry**. Correct for AO given duplicate-safe receivers,
  but it is a trade, not neutral. **[C]**

**So the operator's keeper stays necessary.** It is also the wrong long-term
shape: it only works because their protocol carries per-link sequence numbers and
their receivers drop repeats. A team without that cannot safely run one. And a
per-client keeper leaves every *other* process on the node unprotected — their
owners will not know, because the failure is silent.

---

## 3. The three steps that give the guarantee

### Step 1 — delivery record (the operator's option 3). Precondition for the rest.

In `push_result_message/4`, after `schedule_result` returns `{ok, Assignment}`,
write a small message keyed by `<source>/<slot>/outbox/<key>` holding
`{target, target-slot, id}`. Expose it as `push/status&slot=N`.

Decisions to make: **where it lives** (process cache vs scheduler metadata) — a
maintainer call; and whether it is written before or after the target's slot is
durable (after, or it can claim a delivery that a crash then loses).

### Step 2 — resume.

A `push@1.0` key that, given a slot, schedules **only the entries with no
record**. Idempotent and cheap once step 1 exists. This is what makes delivery
survive a node restart, and it collapses the operator's keeper from "compare seq
against mark/gaps and re-push whole slots" to "ask the node what is missing".

### Step 3 — cranker (the operator's option 1). Protects everyone by default.

A supervised node worker, modelled on `dev_cron:every`, that drives each newly
scheduled slot's outbox to its targets and retries until each has a slot.
`max-depth=0` keeps the fan-out flat; each target's own cranker continues the
chain. **Needs step 2 to be crash-safe** — without a record it would re-push
blindly forever.

**Half of it already exists [C].** `dev_process:maybe_trigger_push/4`
(`dev_process.erl:507`) → `dispatch_push/5` (`:554`) already spawns an
**unlinked**, per-slot, post-cache-visibility delivery trigger. Three reasons it
never fires here, stacked:

1. It fires only when the **compute request** carries `push=true|N`
   (`hb_maps:get(<<"push">>, Req, ...)` — request-only, not process definition,
   not `hb_opts`). There is no node-config switch.
2. The one production path that forwards the key is `now/3` (`dev_process.erl:703`,
   `hb_maps:with([<<"push">>], Req, Opts)`) — and it is on the **`false` branch of
   `process_now_from_cache`**. This node sets `process-now-from-cache: true`, so
   `/now` serves from cache and never reaches it. **Turning that off is not an
   option: 345 of the last 349 requests were `/now` reads, cache-served in ~10 ms.**
3. Even armed it is not a cranker — it fires on *fresh compute only*, so anything
   arriving via `/schedule` from elsewhere triggers nothing.

`dispatch_push/5`'s own comment at `:535-541` claims the default sync mode
propagates back-pressure to the compute path. It does not — `:554` is an unlinked
`spawn`. The comment describes an intent the code does not implement.

**So "arm it from config" is not sufficient.** The cranker needs its own driver
that does not depend on `/now`, plus retry, plus step 2.

---

## 4. Order, and why

1. **Step 1 (record)** — unblocks everything else and is independently useful:
   it is the first time the node can answer "did this get delivered?".
2. **Step 2 (resume)** — turns the record into durability, and immediately
   simplifies the operator's keeper.
3. **Step 3 (cranker)** — the only step that protects processes whose owners are
   not running a keeper. Do it last; it is the largest, and it is unsafe without 2.

Deliberately not bundled: each is one safe change. Step 1 introduces new
persistent state and a new public key surface, which is where the maintainer
decision sits.

---

## 5. Watch items on what is already live

| signal | where | meaning |
|---|---|---|
| `event{topic="push",event="push_workers"}` | metrics | live vs max detached workers; sustained at 32 means the cap is binding and in-line fallback is carrying load |
| `event{topic="push",event="push_worker_refused"}` | metrics | count of pushes that fell back to in-line |
| `push_computing_outbox` − `push_compute_result` | metrics | the loss gap. Was 622 over 75 min; should stop growing |
| `RssAnon` | `/proc/<pid>/status` | the failure mode of the first deployment. Normal 0.1–6 GB; the unbounded build reached 47.5 GB. A watchdog at 16 GB auto-rolls back and captures evidence |
| concurrent held pushes | client side | the UI expects **250–500** from a 1,000-wallet swarm. `max_connections` is `infinity` and the node has not served that before — ramp, do not go cold |

---

## 6. Unverified

- That the 622 killed pushes are specifically client disconnects. What is
  measured is that they were killed by an external exit signal; disconnect is the
  only routine producer of that for a cowboy request process, and the client-side
  timing matched. **[E]**
- The operator's "19 links with at least one message that never arrived" figure —
  not independently re-checked.
- Whether the UI's held-open sockets behave as described under load; they had not
  captured `ss`/`netstat` proof at time of writing.

# Schedule anchoring on Arweave (deferred, work in progress)

Status: paused 2026-10-27, waiting on a conversation with the HyperBEAM team.
`src/preloaded/node/dev_anchor.erl` (`anchor@1.0`) is unfinished: it compiles,
but its 4 tests fail. Packaged as a device, it cannot call
`dev_scheduler_registry` (`undef`), and `dev_scheduler_cache` probably has the
same problem. It is not on `paralith/v0.11`.

## Turbo facts (checked 2026-10-27)

| Fact | Value | Source |
|---|---|---|
| Upload endpoint | `POST https://upload.ardrive.io/tx` (also `turbo.ardrive.io/tx`; `/v1/tx` still answers), body is the raw signed item, `content-type: application/octet-stream`; `/tx/:token` for `arweave`/`ethereum`/`solana` | [docs.ar.io upload API](https://docs.ar.io/apis/turbo/upload-service/upload/), [Turbo APIs](https://docs.ar.io/apis/turbo/) |
| Format | Signed ANS-104 data items only, detected from the first 2 bytes (signature types 1-8). Raw unsigned data goes only through the separate x402 `/x402/data-item/unsigned` path | upload API page; [x402 uploading](https://docs.ar.io/build/upload/x402-uploading-to-turbo/) |
| Free tier | **Items up to 107,520 bytes (105 KiB) are free, with a cap of 10 MiB per wallet over its lifetime and 10 MiB per IP.** Live `GET /info` returned `freeUploadLimitBytes: 107520` and `freeTier: {lifetimeBytes: 10485760, ipBytes: 10485760, maxItemBytes: 107520}`. The docs only say "free under 105 KiB" | `curl https://upload.ardrive.io/info`; [docs.ar.io/build/upload](https://docs.ar.io/build/upload/) |
| Receipt | 200 JSON: `id, timestamp, version, owner, deadlineHeight, dataCaches, fastFinalityIndexes, winc, signature, public` (`signature` is Turbo's signature over the receipt) | upload API page |
| Duplicates | Re-posting an item ID that is already held returns **202 "Data Item Exists"**, and no payment is reserved for the in-memory duplicate check | turbo-upload-service `src/routes/dataItemPost.ts` (2023 public mirror `hahzterry/turbo-upload-service`; the ardriveapp repo is no longer public) |
| Errors | 400 parse error, 402 insufficient balance (x402 offer in the body), 503 | upload API page |
| Rate limits | Not documented anywhere I found | none |
| Latency | Not documented. The receipt is returned synchronously; the item is in `dataCaches` (turbo-gateway.com) right away and settles on Arweave by `deadlineHeight` | upload API page |
| Paying beyond free | Turbo Credits, bought with card (fiat), AR, ETH, SOL or POL, or per upload with x402 USDC. There is a ~23% "Turbo Infrastructure Fee" on card payments ($10 buys 20.0 T winc) | [docs.ar.io/build/upload](https://docs.ar.io/build/upload/); `GET payment.ardrive.io/v1/price/usd/10` |
| Price | $79.86 per GiB (`/v1/rates`), plus a fee of 5.7 M winc per item. A 4 KiB item costs 62.3 M winc, about **$0.00003** | `payment.ardrive.io/v1/rates`, `/v1/price/bytes/4096` |

Cost: at ~4 KiB per anchored slot (an assignment plus its message as JSON),
the 10 MiB free allowance lasts about 2,500 slots per node wallet. After that
the cost is about **$31 per million slots**, so PUSD volume (thousands of
slots a month) comes to cents a month.

## Design (as built, unfinished)

- A node allowlist, `anchor.processes`, rather than a key in the process
  definition. Anchoring is paid for and signed by this node, so a process
  definition must not be able to opt a node in. PUSD's definition is also
  already fixed (changing it would change its ID).
- Nothing on the scheduling path. The outbox is the scheduler store itself:
  assignments are flushed before their slot is confirmed, and retention
  never deletes them. A shipper started by the node's `start` hook keeps
  only a cursor.
- One ANS-104 item per slot, signed by the node wallet. Its data is the
  assignment in `json@1.0`, with its body (the original message) inline and
  every commitment kept. Tags: `Process`, `Slot`, `Assignment`, `Message`,
  `Type: Schedule-Anchor`.
- Exactly once: the signed bytes go to `pending/<slot>` and are synced
  before the first POST. Every retry re-sends the same bytes, so Turbo
  answers 202 and nothing is posted twice.
- The index is `<dir>/<pid>/index`: a fixed 104-byte record per slot
  (`slot, item id, assignment id, message id`), so `anchored(Proc, Slot,
  Opts)` is a single O(1) read. HTTP: `/~anchor@1.0/lookup?process=&slot=`
  and `/~anchor@1.0/status`.
- Config `anchor`: `processes`, `bundler` (`https://upload.ardrive.io`),
  `dir`, `interval`, `max-backoff`, `window`, `timeout`.

## Open questions for the HyperBEAM team

- How a preloaded device should reach the scheduler cache and registry
  (through `hb_ao:raw` on `scheduler@1.0`, or by listing them as
  `-device_libraries`).
- Whether upstream wants this in `dev_scheduler_server` itself (the
  existing synchronous `scheduler-publish-remote`), made asynchronous.
- Whether the 10 MiB lifetime free cap makes a funded Turbo wallet, or the
  node's own bundler (`bundler@1.0`), the better target.

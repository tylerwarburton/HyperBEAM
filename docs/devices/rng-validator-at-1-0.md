# Device: ~rng-validator@1.0

## Overview

`~rng-validator@1.0` runs this node as one validator of an Any-Honest RNG
beacon (Random-Process `v2/ANY_HONEST.md`). The node's `start` hook spawns a
loop that reads the beacon's published `beaconinfo` and, on every pass:

1. makes a fresh puzzle when a round can take one: a secret `alpha` and the
   commit `c = g^alpha mod N` in `~rng@1.0`'s fixed group. The secret is
   written to disk before the commit is sent;
2. reveals each stored secret once the beacon shows its round revealing;
3. says `Ready` whenever it can work and `Pause` when it should stop taking
   rounds (see below);
4. sends `Close` once a round's commit window has passed;
5. with `exit` set, sends `Exit`, then `Finalize-Exit` until it lands;
6. follows a successor beacon (see below).

Cracking (solving a withheld puzzle without its secret) is not part of this
device. One crack is minutes of a busy CPU core and needs no validator key, so
it runs as a separate service that anyone may operate for the beacon's crack
bounty.

On a beacon that lists `act` in `features`, the commit and every reveal of a
pass go as one `Act` message, which halves what a round costs the beacon.

After every scheduled message the device asks the node to compute it
(`push&slot=<slot>`), from one pusher process: one push in flight, queued
requests fold into the highest slot. A message is resent only after the
beacon has computed past its slot (`now/at-slot`) and still shows no effect, so
a lagging beacon is never flooded.

## Ready and Pause

On a beacon that lists `ready` in `features`, a ready validator is given
rounds and is slashed in full for every one it fails (a missed commit or a
missed reveal). The device:

- says `Ready` when it is a member in good standing (owes no late output), is
  not exiting, is not following a successor, and there is no `pause` file;
- also needs, before `Ready`: the full stake (bond plus unclaimed earnings,
  `earnedAtoms`, at least `params.stake`), the beacon's cooldown (`rejoinAt`)
  passed, its own backoff passed (`rejoinBaseMs` doubled per incident, up to
  `rejoinCapMs`), and a healthy round trip (below);
- says `Pause` when any of those stops being true, and then still commits to a
  round whose set it is in and reveals everything it committed to.

Health is measured on the local clock only: the time from sending a commit
to the first read that shows it (a commit not shown within 30 s counts as
30 s), over the last 10 minutes, or twice the slowest of the last 20 reads
when it has not committed lately. `Ready` waits until that is under half of
`commitWindowMs`. Beacon times (`rejoinAt`, `nextRoundAt`, `commitDeadline`)
are compared with the local clock moved by the clock offset, the smallest
`local read time - beaconinfo.at` seen. A local clock that is off by minutes
therefore neither rejoins early nor looks healthy when it is not.

On a beacon in `mode: request-driven` (its reserve spent), the device opens
only rounds that have requests. The beacon's `health` is shown in `status`
and logged when it is not `ok`.

## Joining and topping up

A validator joins by bonding at least `params.stake` RNG to the beacon. With
`top-up: true` the device does this itself from its wallet: when it has no
entry yet, or its bond (after a slash) plus earnings is below `stake`, it
sends one `Transfer` on the beacon's `token` with `Recipient` = the beacon,
`Quantity` = the shortfall in atoms and `X-Bond: true`. It sends at most one
per bond value every 10 minutes, and never while exiting, paused or moving.
The wallet must hold the RNG. Without `top-up`, bond by hand and the device
says `Ready` once the bond shows.

To take a node offline safely, create `pause` in `secrets-dir`, wait until
`status` shows `ready: false` and the beacon no longer lists rounds the
validator owes (a few seconds), then stop the node. Delete the file after the
restart to rejoin. Stopping a ready validator without pausing costs a slash
for every round it was given and did not finish.

A beacon without the `ready` feature gets the older suspend/rejoin behaviour.

## Following a successor

When the beacon publishes a `successor` whose own `beaconinfo` names this
beacon as its `predecessor` (and, on a beacon that publishes
`successorAck`, once that is true), the device pauses, reveals what it still owes,
then moves and records the move in `secrets-dir/beacon.json`. Setting a
different `beacon` in the node config starts the walk again from there.

## Configuration

Under the node option `rng-validator`:

| Key | Meaning |
| --- | --- |
| `beacon` | The beacon process id (required). |
| `node` | URL of the node the beacon is scheduled on (required). |
| `wallet-file` | JWK the validator signs with. Default: the node's wallet. |
| `secrets-dir` | Secrets, `beacon.json` and the `pause` file (default `rng-validator-secrets`). Must survive restarts. |
| `interval` | Milliseconds between passes (default 100). |
| `exit` | `true` to leave the validator set. |
| `top-up` | `true` to bond from the wallet: join, and refill a slashed bond to `stake`. Default `false`. |

```json
"on": { "start": { "device": "rng-validator@1.0" } },
"rng-validator": { "beacon": "<id>", "node": "https://<node>",
  "wallet-file": "/app/keys/rng-validator-wallet.json",
  "secrets-dir": "/app/rng-validator-secrets" }
```

Several start hooks run in order when `on.start` is a list.

Moving from the `anyhonest-validator.mjs` daemon: stop the daemon, merge its
`secrets/*.json` into `secrets-dir` (same format: `<round>.json` holding
`{c: alpha}`; merge the objects, never skip a file that exists), then start
the device with the daemon's wallet as `wallet-file`. Without the merge,
rounds the daemon committed cannot be revealed.

## Keys

### `status`

`GET /~rng-validator@1.0/status`: the validator's address, the beacon it
follows, the beacon's round and phase, `ready`, `paused`, `stake`, `faults`,
`at-slot`, `rejoin-at` (local clock), `round-trip`, `clock-offset`,
`stake-short` (RNG missing for `Ready`), the beacon's `health`, and the last
pass and error.

### `start`

The node `start` hook. Configuration is read from the node message only, never
from the request.

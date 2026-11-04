# Device: ~rng-validator@1.0

## Overview

`~rng-validator@1.0` runs this node as one validator of an Any-Honest RNG
beacon. The node's `start` hook spawns a loop that reads the beacon's published
`beaconinfo` and, on every pass:

1. makes a fresh puzzle when a round can take one: a secret `alpha` and the
   commit `c = g^alpha mod N` in `~rng@1.0`'s fixed group. The secret is
   written to disk before `Commit` is sent;
2. sends `Close` once a round's commit window has passed;
3. sends `Reveal` with the stored secret once the beacon shows the round
   revealing;
4. sends `Rejoin` after a suspension while its bond covers the next slash;
5. with `exit` set, sends `Exit`, then `Finalize-Exit` until it lands.

Cracking (solving a withheld puzzle without its secret) is not part of this
device. One crack is minutes of a busy CPU core and needs no validator key, so
it runs as a separate service that anyone may operate for the beacon's crack
bounty.

A message the node scheduled is resent only after the beacon has computed past
its slot (`now/at-slot`) and still shows no effect, so a lagging beacon is never
flooded.

## Configuration

Under the node option `rng-validator`:

| Key | Meaning |
| --- | --- |
| `beacon` | The beacon process id (required). |
| `node` | URL of the node the beacon is scheduled on (required). |
| `wallet-file` | JWK the validator signs with. Default: the node's wallet. |
| `secrets-dir` | Where secrets are kept (default `rng-validator-secrets`). Must survive restarts. |
| `interval` | Milliseconds between passes (default 100). |
| `exit` | `true` to leave the validator set. |

```json
"on": { "start": { "device": "rng-validator@1.0" } },
"rng-validator": { "beacon": "<id>", "node": "https://<node>",
  "wallet-file": "/app/keys/rng-validator-wallet.json",
  "secrets-dir": "/app/rng-validator-secrets" }
```

Several start hooks run in order when `on.start` is a list.

Moving from the `anyhonest-validator.mjs` daemon: stop the daemon, copy its
`secrets/*.json` into `secrets-dir` (same format: `<round>.json` holding
`{c: alpha}`), then start the device with the daemon's wallet as `wallet-file`.
Without the copy, rounds the daemon committed cannot be revealed: the
validator stays overdue until they are cracked or abandoned.

## Keys

### `status`

`GET /~rng-validator@1.0/status`: the validator's address, the beacon's round
and phase, `at-slot`, and the last pass and error.

### `start`

The node `start` hook. Configuration is read from the node message only, never
from the request.

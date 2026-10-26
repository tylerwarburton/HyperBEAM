# Device: ~vrf@1.0

## Overview

The `~vrf@1.0` device checks randomness-beacon signatures and derives bytes from a seed. It is stateless and every key is a pure function of its inputs, so a process can call it from inside its computation (for example with `ao.resolve` from Lua) and every node that replays the process gets the same answer.

Signatures are BLS12-381 in the IETF "minimal-pubkey-size" variant: public keys are compressed G1 points (48 bytes), signatures are compressed G2 points (96 bytes), and messages hash to G2 with the ciphersuite `BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_`. This is what noble-curves calls `bls12_381.longSignatures`. A t-of-n threshold group signature verifies as an ordinary signature under the group public key. Verification uses the audited [`blst`](https://github.com/supranational/blst) library (v0.3.16, `native/lib/blst`) through `native/blst_nif`, on a dirty CPU scheduler. Both points are checked to be on the curve and in their prime-order subgroup, and the identity is refused as a public key.

Inputs are read from the request, then from the base message. Bad or missing inputs never raise an error: the result is `valid: false` with a `reason`.

## Core Functions (Keys)

### `verify-bls`

*   **Inputs:** `public-key` (hex, 48 bytes), `message` (binary), `signature` (hex, 96 bytes).
*   **Returns:** `valid` (boolean). When valid, `value` is the hex of `SHA-256("round|" ++ hex(signature))`, the beacon's round value, with the signature hex in lowercase. When not, `reason` names the check that failed (`verify-failed`, `signature-not-in-group`, `invalid-public-key`, `missing-signature`, ...).
*   **Example:** `GET /~vrf@1.0/verify-bls?public-key=8085...&message=<id>|1&signature=ae5c...`

### `derive`

*   **Inputs:** `seed` (binary), `counter` (integer, at least 0), `bytes` (integer, 1 to 1024). Integers may be given in decimal.
*   **Returns:** `valid` and `value`: the hex of the first `bytes` bytes of `SHA-256(seed ++ "C") ++ SHA-256(seed ++ "C+1") ++ ...`, where each counter `C`, `C+1`, ... is written in decimal.
*   **Example:** `GET /~vrf@1.0/derive?seed=abc&counter=0&bytes=64`

## Calling it from Lua

From a `lua@5.3a` process, put the inputs next to the path in one `ao.resolve` message:

```lua
local status, res = ao.resolve({
    path = "/~vrf@1.0/verify-bls",
    ["public-key"] = groupKey,
    message = processId .. "|" .. round,
    signature = signatureHex
})
-- status == "ok"; res.valid == true; res.value is the round value.
```

To name the device with a `device` key instead, pass the inputs and the path as two messages: `ao.resolve({ device = "vrf@1.0", ["public-key"] = k, message = m, signature = s }, { path = "verify-bls" })`. A single message holding both `device` and `path = "verify-bls"` does not reach the device (it resolves to `not_found`). `derive` takes the first shape: `ao.resolve({ path = "/~vrf@1.0/derive", seed = s, counter = 0, bytes = 32 })`.

Do not name a Lua handler `verify`: `verify` is a key every message answers itself (commitment verification), so a request for it never calls the Lua function.

## Tests

`scripts/vrf-test.sh` builds the tree in an `erlang:27` container and runs `rebar3 device test --devices dev_vrf,dev_lua`. The BLS vectors in `dev_vrf_test_vectors` come from the beacon's own JavaScript (noble-curves): a 2-of-3 DKG group signature, a single-key signature, a wrong message, a wrong key, a corrupted signature, on-curve points outside the subgroups, the identity, and garbage. They include round 1 of the live beacon process with the value it published.

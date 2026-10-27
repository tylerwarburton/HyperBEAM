# Device: ~timelock@1.0

## Overview

The `~timelock@1.0` device checks time-lock puzzle answers for a randomness beacon, checks BLS beacon signatures, and derives bytes from a seed. It is stateless and every key is a pure function of its inputs, so a process can call it from inside its computation (for example with `ao.resolve` from Lua) and every node that replays the process gets the same answer. It was called `vrf@1.0`; `verify-bls` and `derive` behave as they did.

The time-lock keys implement the Random-Process protocol (`SPEC.md` §2.3–2.5) and match its JavaScript reference (`provider/timelock.mjs`) bit for bit. A validator commits to an RSA modulus `N = p · q`; the round fixes the puzzle input `x` from a seed; the answer is `y = x^(2^t) mod N`. The validator reveals `y` with its trapdoor `p`, or anyone who spent `t` sequential squarings posts `y` with a Wesolowski proof.

Hex integers (`modulus`, `y`, `p`, `proof`, and the returned `x`) are lowercase without leading zeros, as JavaScript's `BigInt.toString(16)` writes them. Other spellings are refused with `non-canonical-<key>`, because the Wesolowski challenge hashes the hex as given. `t` is an integer or its decimal, from 0 to 2^32. Moduli and hex inputs are capped at 4096 bits and seeds at 4096 bytes. Arithmetic is Erlang bignums and `crypto:mod_pow`.

Inputs are read from the request, then from the base message. Bad or missing inputs never raise an error: the result is `valid: false` with a `reason`.

## Core Functions (Keys)

### `derive-x`

*   **Inputs:** `modulus` (hex), `seed` (binary).
*   **Returns:** `valid` and `x` (hex): `x = (int(expand(seed, bits(N) + 64)) mod N)^2 mod N`, where `expand(s, n)` is the first `n` bits (big-endian) of `SHA-256(s ++ "|0") ++ SHA-256(s ++ "|1") ++ ...`. The beacon's seed is `beaconId ++ "|" ++ round ++ "|" ++ validatorAddress`.

### `verify-reveal`

*   **Inputs:** `modulus`, `seed`, `t` (decimal), `y` (hex), `p` (hex).
*   **Returns:** `valid`, and `x` once the modulus and seed have been read. Valid iff `1 < p < N`, `N mod p = 0` and `y = x^(2^t mod (p-1)(q-1)) mod N` with `q = N / p`. Reasons include `p-out-of-range`, `p-not-a-factor`, `verify-failed`, `missing-<key>`, `invalid-<key>`, `non-canonical-<key>`, `<key>-too-large`, `t-out-of-range`.

### `verify-crack`

*   **Inputs:** `modulus`, `seed`, `t`, `y` (hex), `proof` (hex).
*   **Returns:** `valid`, and `x` once the modulus and seed have been read. Valid iff `0 < y < N`, `0 < proof < N` and `proof^l · x^(2^t mod l) ≡ y (mod N)`, where `l` is the first prime at or above the first 128 bits of `SHA-256("rng/wesolowski|" ++ N ++ "|" ++ x ++ "|" ++ y ++ "|" ++ t)` with bits 127 and 0 set (hex as above, `t` in decimal). Primality is trial division by the primes below 1000 and Miller–Rabin with the first 20 primes as bases. Extra reasons: `y-out-of-range`, `proof-out-of-range`.

### `verify-bls`

Signatures are BLS12-381 in the IETF "minimal-pubkey-size" variant: public keys are compressed G1 points (48 bytes), signatures are compressed G2 points (96 bytes), and messages hash to G2 with the ciphersuite `BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_`. This is what noble-curves calls `bls12_381.longSignatures`. A t-of-n threshold group signature verifies as an ordinary signature under the group public key. Verification uses the audited [`blst`](https://github.com/supranational/blst) library (v0.3.16, `native/lib/blst`) through `native/blst_nif`, on a dirty CPU scheduler. Both points are checked to be on the curve and in their prime-order subgroup, and the identity is refused as a public key.

*   **Inputs:** `public-key` (hex, 48 bytes), `message` (binary), `signature` (hex, 96 bytes).
*   **Returns:** `valid` (boolean). When valid, `value` is the hex of `SHA-256("round|" ++ hex(signature))`, the beacon's round value, with the signature hex in lowercase. When not, `reason` names the check that failed (`verify-failed`, `signature-not-in-group`, `invalid-public-key`, `missing-signature`, ...).
*   **Example:** `GET /~timelock@1.0/verify-bls?public-key=8085...&message=<id>|1&signature=ae5c...`

### `derive`

*   **Inputs:** `seed` (binary), `counter` (integer, at least 0), `bytes` (integer, 1 to 1024). Integers may be given in decimal.
*   **Returns:** `valid` and `value`: the hex of the first `bytes` bytes of `SHA-256(seed ++ "C") ++ SHA-256(seed ++ "C+1") ++ ...`, where each counter `C`, `C+1`, ... is written in decimal.
*   **Example:** `GET /~timelock@1.0/derive?seed=abc&counter=0&bytes=64`

## Calling it from Lua

From a `lua@5.3a` process, put the inputs next to the path in one `ao.resolve` message. Pass `t` as a string (or a Lua integer):

```lua
local status, res = ao.resolve({
    path = "/~timelock@1.0/verify-reveal",
    modulus = commit.modulus,   -- hex N from the Commit
    seed = beaconId .. "|" .. round .. "|" .. validator,
    t = tostring(T),
    y = reveal.y,
    p = reveal.p
})
-- status == "ok"; res.valid == true; res.x is the puzzle input.

local status, res = ao.resolve({
    path = "/~timelock@1.0/verify-crack",
    modulus = commit.modulus,
    seed = beaconId .. "|" .. round .. "|" .. withheld,
    t = tostring(T),
    y = crack.y,
    proof = crack.proof
})

local status, res = ao.resolve({
    path = "/~timelock@1.0/verify-bls",
    ["public-key"] = groupKey,
    message = processId .. "|" .. round,
    signature = signatureHex
})
-- res.value is the round value.
```

To name the device with a `device` key instead, pass the inputs and the path as two messages: `ao.resolve({ device = "timelock@1.0", ["public-key"] = k, message = m, signature = s }, { path = "verify-bls" })`. A single message holding both `device` and `path = "verify-bls"` does not reach the device (it resolves to `not_found`). `derive` takes the first shape: `ao.resolve({ path = "/~timelock@1.0/derive", seed = s, counter = 0, bytes = 32 })`.

Do not name a Lua handler `verify`: `verify` is a key every message answers itself (commitment verification), so a request for it never calls the Lua function.

## Tests

`scripts/timelock-test.sh` builds the tree in an `erlang:27` container and runs `rebar3 device test --devices dev_timelock,dev_lua`. The vectors are in `dev_timelock_test_vectors`.

The time-lock vectors come from the reference implementation (Random-Process `test/gen-timelock-vectors.mjs`): 1024- and 2048-bit moduli with `t` = 1024 and 4096, covering `derive-x`, reveals with `p` and with `q`, a wrong `y`, a wrong `p`, `p = 1`, `p = N`, a wrong seed, a wrong `t`, a valid crack, a corrupted proof, a corrupted `y`, out-of-range values and garbage hex, plus a reveal and a crack at the protocol's real size (2048 bits, `t` = 2^27; the crack's proof is made with the trapdoor). The challenge primes are checked against the reference's.

The BLS vectors come from the beacon's own JavaScript (noble-curves): a 2-of-3 DKG group signature, a single-key signature, a wrong message, a wrong key, a corrupted signature, on-curve points outside the subgroups, the identity, and garbage. They include round 1 of the live beacon process with the value it published.

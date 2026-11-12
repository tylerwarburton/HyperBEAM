# Device: ~timelock@1.0

## Overview

The `~timelock@1.0` device checks time-lock puzzle answers for a randomness beacon, checks BLS beacon signatures, and derives bytes from a seed. It is stateless and every key is a pure function of its inputs, so a process can call it from inside its computation (for example with `ao.resolve` from Lua) and every node that replays the process gets the same answer. It was called `vrf@1.0`; `verify-bls` and `derive` behave as they did.

The time-lock keys implement the Random-Process protocol (`SPEC.md` §2.3–2.5 and §3) and match its JavaScript reference (`provider/timelock.mjs`) bit for bit. Every puzzle lives in one group of unknown order: the integers modulo the RSA-2048 challenge number `N`, whose factors were never published, so nobody (validators included) can shortcut the squarings or forge a proof. `N` and the generator `g = (int(expand("rng/timelock/g", 2112)) mod N)^2 mod N` are built in, where `expand(s, n)` is the first `n` bits (big-endian) of `SHA-256(s ++ "|0") ++ SHA-256(s ++ "|1") ++ ...`. The beacon sets `h = g^(2^T) mod N`. A validator commits `c = g^alpha mod N` for a fresh secret `alpha`; its output is `y = c^(2^T) = h^alpha mod N`. It reveals `alpha`, or anyone who spent `T` sequential squarings of `c` posts `y` with a Wesolowski proof. Outputs are returned in canonical form `y* = min(y, N − y)`, since `y` and `N − y` pass the same checks.

Hex integers (`c`, `alpha`, `h`, `x`, `y`, `proof`, and the returned `y`, `g`, `n`) are lowercase without leading zeros, as JavaScript's `BigInt.toString(16)` writes them. Other spellings are refused with `non-canonical-<key>`, because the Wesolowski challenge hashes the hex as given. Group elements are capped at 512 hex digits (`N` has 2048 bits) and `alpha` at 128 (512 bits), so every modular exponentiation stays small. `t` is an integer or its decimal, from 0 to 2^32. Arithmetic is Erlang bignums and `crypto:mod_pow`.

Inputs are read from the request, then from the base message. Bad or missing inputs never raise an error: the result is `valid: false` with a `reason`.

## Core Functions (Keys)

### `generator`

*   **Inputs:** none.
*   **Returns:** `valid` (always `true`), `g` and `n` (hex): the built-in generator and modulus.

### `verify-reveal`

*   **Inputs:** `c` (hex, the commit), `alpha` (hex, the revealed secret), `h` (hex, `g^(2^T)`).
*   **Returns:** `valid`, and when valid `y`, the hex of `canonical(h^alpha mod N)`. Valid iff `0 < c < N`, `0 < h < N`, `alpha` has at most 512 bits and `g^alpha ≡ c (mod N)`. Checking `h` itself is the beacon's job (at `Configure`). Reasons include `verify-failed`, `c-out-of-range`, `h-out-of-range`, `alpha-too-large`, `missing-<key>`, `invalid-<key>`, `non-canonical-<key>`, `<key>-too-large`.

### `verify-crack`

*   **Inputs:** `x` (hex, the puzzle input: a commit `c`, or `g` for the beacon's `h`), `t` (decimal), `y` (hex), `proof` (hex).
*   **Returns:** `valid`, and when valid `y`, the hex of `canonical(y)`. Valid iff `0 < x, y, proof < N`, `t ≤ 2^32` and `proof^l · x^(2^t mod l) ≡ y (mod N)`, where `l` is the first prime at or above the first 128 bits of `SHA-256("rng/wesolowski|" ++ x ++ "|" ++ y ++ "|" ++ t)` with bits 127 and 0 set (`x` and `y` as submitted, `t` in decimal). Primality is trial division by the primes below 1000 and Miller–Rabin with the first 20 primes as bases. Extra reasons: `x-out-of-range`, `y-out-of-range`, `proof-out-of-range`, `t-out-of-range`, `invalid-t`.

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
    c = commits[validator],     -- hex c from the Commit
    alpha = reveal.alpha,
    h = H                       -- hex g^(2^T) set with Configure
})
-- status == "ok"; res.valid == true; res.y is the canonical output y*.

local status, res = ao.resolve({
    path = "/~timelock@1.0/verify-crack",
    x = commits[withheld],
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

The time-lock vectors come from the reference implementation (Random-Process `test/gen-timelock-vectors.mjs`) in the built-in group with `T` = 1024: the generator; reveals that are valid (including a 512-bit `alpha`, `alpha = 0` and another `h`) and invalid (wrong `alpha`, wrong `c`, `c` or `h` equal to 0 or `N`, `c + N`, a 513-bit `alpha`, garbage, uppercase and leading-zero hex); and cracks that are valid (including `N − y` with its own proof, and `h` from `g`) and invalid (corrupted proof or `y`, `N − y` with the old proof, wrong `x`, wrong `t`, `t` at and above 2^32, out-of-range values, garbage). The challenge primes are checked against the reference's.

The BLS vectors come from the beacon's own JavaScript (noble-curves): a 2-of-3 DKG group signature, a single-key signature, a wrong message, a wrong key, a corrupted signature, on-curve points outside the subgroups, the identity, and garbage. They include round 1 of the live beacon process with the value it published.

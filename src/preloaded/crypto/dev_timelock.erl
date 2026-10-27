%%% @doc A stateless device for verifiable randomness: it checks time-lock
%%% puzzle answers (a validator's trapdoor reveal, or anyone's Wesolowski
%%% crack), checks a BLS randomness-beacon signature, and derives bytes from a
%%% seed. Every key is a pure function of its inputs, so a process may call it
%%% from inside its computation (for example `ao.resolve' from Lua) and every
%%% node that replays the process reaches the same result. It was `vrf@1.0'.
%%%
%%% Keys:
%%% <pre>
%%%     derive-x       Inputs: `modulus' (hex), `seed' (binary). Returns
%%%                    `valid' and `x' (hex), the puzzle input
%%%                    (int(expand(seed, bits(N) + 64)) mod N)^2 mod N, where
%%%                    expand(s, n) is the first n bits of
%%%                    SHA-256(s ++ "|0") ++ SHA-256(s ++ "|1") ++ ...
%%%     verify-reveal  Inputs: `modulus', `seed', `t' (decimal), `y' (hex),
%%%                    `p' (hex). Valid iff 1 < p < N, N rem p = 0 and
%%%                    y = x^(2^t mod (p-1)(q-1)) mod N. Returns `valid', `x'.
%%%     verify-crack   Inputs: `modulus', `seed', `t', `y', `proof' (hex).
%%%                    Valid iff 0 < y < N, 0 < proof < N and
%%%                    proof^l * x^(2^t mod l) = y (mod N), with l the
%%%                    Wesolowski challenge prime. Returns `valid', `x'.
%%%     verify-bls     Inputs: `public-key' (hex, 48-byte compressed G1 point),
%%%                    `message' (binary), `signature' (hex, 96-byte compressed
%%%                    G2 point). Returns `valid' (boolean). When the signature
%%%                    is valid, `value' is the hex of
%%%                    SHA-256("round|" ++ hex(signature)), the beacon's round
%%%                    value; otherwise `reason' names the failed check.
%%%     derive         Inputs: `seed' (binary), `counter' (integer >= 0),
%%%                    `bytes' (integer, 1 to 1024). Returns `valid' and
%%%                    `value', the hex of the first `bytes' bytes of
%%%                    SHA-256(seed ++ "counter") ++ SHA-256(seed ++ "counter+1")
%%%                    ++ ..., each counter written in decimal.
%%% </pre>
%%%
%%% The time-lock keys follow the Random-Process protocol (SPEC.md 2.3-2.5)
%%% and match its JavaScript reference (`provider/timelock.mjs') bit for bit.
%%% Hex integers are lowercase without leading zeros, as `BigInt.toString(16)'
%%% writes them; other spellings are refused, since the challenge prime hashes
%%% the hex as given. Sizes are capped: moduli and hex inputs at 4096 bits,
%%% seeds at 4096 bytes, `t' at 2^32.
%%%
%%% The BLS scheme is the IETF "minimal-pubkey-size" variant over BLS12-381
%%% (`blst_nif'): keys in G1, signatures in G2, ciphersuite
%%% `BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_'. A t-of-n threshold group
%%% signature verifies as an ordinary signature under the group key. Both
%%% points are subgroup-checked. Inputs are read from the request, then from
%%% the base message. Malformed inputs give `valid: false' with a `reason',
%%% never an error.
-module(dev_timelock).
-export([derive_x/3, verify_reveal/3, verify_crack/3]).
-export([verify_bls/3, derive/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MAX_DERIVE_BYTES, 1024).
-define(MAX_HEX_CHARS, 1024). % 4096 bits
-define(MAX_SEED_BYTES, 4096).
-define(MAX_T, (1 bsl 32)).

%% @doc The puzzle input `x' for a modulus and a seed.
-spec derive_x(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ valid := boolean(), _ => _ }}.
derive_x(Base, Req, Opts) ->
    safe(
        fun() ->
            maybe
                {ok, {_, N}} ?= modulus_input(Base, Req, Opts),
                {ok, Seed} ?= seed_input(Base, Req, Opts),
                {checked, puzzle_x(N, Seed), ok}
            end
        end
    ).

%% @doc Check a validator's reveal: the factor `p' of the modulus and the
%% answer `y' it computed with that trapdoor.
-spec verify_reveal(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ valid := boolean(), _ => _ }}.
verify_reveal(Base, Req, Opts) ->
    safe(
        fun() ->
            maybe
                {ok, {_, N}} ?= modulus_input(Base, Req, Opts),
                {ok, Seed} ?= seed_input(Base, Req, Opts),
                {ok, T} ?= t_input(Base, Req, Opts),
                {ok, {_, Y}} ?= hex_int_input(<<"y">>, Base, Req, Opts),
                {ok, {_, P}} ?= hex_int_input(<<"p">>, Base, Req, Opts),
                X = puzzle_x(N, Seed),
                {checked, X, check_reveal(N, X, T, Y, P)}
            end
        end
    ).

%% @doc Check a crack: the answer `y' found by `t' squarings and its
%% Wesolowski proof.
-spec verify_crack(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ valid := boolean(), _ => _ }}.
verify_crack(Base, Req, Opts) ->
    safe(
        fun() ->
            maybe
                {ok, {NHex, N}} ?= modulus_input(Base, Req, Opts),
                {ok, Seed} ?= seed_input(Base, Req, Opts),
                {ok, T} ?= t_input(Base, Req, Opts),
                {ok, {YHex, Y}} ?= hex_int_input(<<"y">>, Base, Req, Opts),
                {ok, {_, Proof}} ?= hex_int_input(<<"proof">>, Base, Req, Opts),
                X = puzzle_x(N, Seed),
                {checked, X, check_crack(NHex, N, X, T, YHex, Y, Proof)}
            end
        end
    ).

%% @doc SPEC 2.4 step 2: `p' is a proper factor of N, and `y' is x raised to
%% 2^t, reduced through phi(N) = (p-1)(q-1).
check_reveal(N, X, T, Y, P) ->
    maybe
        true ?= (P > 1 andalso P < N) orelse {error, <<"p-out-of-range">>},
        true ?= (N rem P =:= 0) orelse {error, <<"p-not-a-factor">>},
        Phi = (P - 1) * (N div P - 1),
        true ?= mod_pow(X, mod_pow(2, T, Phi), N) =:= Y,
        ok
    end.

%% @doc SPEC 2.5: proof^l * x^(2^t mod l) = y (mod N).
check_crack(NHex, N, X, T, YHex, Y, Proof) ->
    maybe
        true ?= (Y > 0 andalso Y < N) orelse {error, <<"y-out-of-range">>},
        true ?=
            (Proof > 0 andalso Proof < N)
                orelse {error, <<"proof-out-of-range">>},
        L = challenge_prime(NHex, to_hex(X), YHex, T),
        R = mod_pow(2, T, L),
        true ?= (mod_pow(Proof, L, N) * mod_pow(X, R, N)) rem N =:= Y,
        ok
    end.

%% @doc SPEC 2.3: x = (int(expand(seed, bits(N) + 64)) mod N)^2 mod N.
puzzle_x(N, Seed) ->
    V = expand(Seed, bit_length(N) + 64) rem N,
    (V * V) rem N.

%% @doc The first `Bits' bits, big-endian, of
%% SHA-256(Seed ++ "|0") ++ SHA-256(Seed ++ "|1") ++ ...
expand(Seed, Bits) ->
    Stream =
        <<
            <<(crypto:hash(
                sha256,
                <<Seed/binary, "|", (integer_to_binary(K))/binary>>
            ))/binary>>
        ||
            K <- lists:seq(0, (Bits + 255) div 256 - 1)
        >>,
    <<V:Bits, _/bitstring>> = Stream,
    V.

%% @doc The Wesolowski challenge: the first prime at or above the first 128
%% bits of SHA-256("rng/wesolowski|N|x|y|t"), with bits 127 and 0 set.
challenge_prime(NHex, XHex, YHex, T) ->
    <<C:128, _/binary>> =
        crypto:hash(
            sha256,
            <<
                "rng/wesolowski|", NHex/binary, "|", XHex/binary, "|",
                YHex/binary, "|", (integer_to_binary(T))/binary
            >>
        ),
    next_prime(C bor (1 bsl 127) bor 1, small_primes()).

next_prime(C, Small) ->
    case is_probable_prime(C, Small) of
        true -> C;
        false -> next_prime(C + 2, Small)
    end.

%% @doc Trial division by the primes below 1000, then Miller-Rabin with the
%% first 20 primes as bases, exactly as the reference does it.
is_probable_prime(N, _Small) when N < 2 -> false;
is_probable_prime(N, Small) ->
    case trial_division(N, Small) of
        prime -> true;
        composite -> false;
        unknown -> miller_rabin(N, lists:sublist(Small, 20))
    end.

trial_division(_N, []) -> unknown;
trial_division(N, [N | _]) -> prime;
trial_division(N, [P | _]) when N rem P =:= 0 -> composite;
trial_division(N, [_ | Rest]) -> trial_division(N, Rest).

miller_rabin(N, Bases) ->
    {D, S} = split_twos(N - 1, 0),
    lists:all(fun(A) -> mr_round(mod_pow(A, D, N), N, S) end, Bases).

split_twos(D, S) when D band 1 =:= 0 -> split_twos(D bsr 1, S + 1);
split_twos(D, S) -> {D, S}.

mr_round(1, _N, _S) -> true;
mr_round(X, N, _S) when X =:= N - 1 -> true;
mr_round(X, N, S) -> mr_square(X, N, S - 1).

mr_square(_X, _N, 0) -> false;
mr_square(X, N, Left) ->
    case (X * X) rem N of
        Sq when Sq =:= N - 1 -> true;
        Sq -> mr_square(Sq, N, Left - 1)
    end.

%% @doc The primes below 1000.
small_primes() ->
    lists:foldl(
        fun(K, Acc) ->
            case lists:any(fun(P) -> K rem P =:= 0 end, Acc) of
                true -> Acc;
                false -> Acc ++ [K]
            end
        end,
        [],
        lists:seq(2, 999)
    ).

%% @doc B^E mod M on non-negative integers.
mod_pow(_B, _E, 1) -> 0;
mod_pow(B, E, M) -> binary:decode_unsigned(crypto:mod_pow(B, E, M)).

%% @doc The number of bits of a positive integer.
bit_length(N) ->
    <<First, Rest/binary>> = binary:encode_unsigned(N),
    byte_size(Rest) * 8 + length(integer_to_list(First, 2)).

%% @doc Lowercase hex without leading zeros, as `BigInt.toString(16)'.
to_hex(N) -> string:lowercase(integer_to_binary(N, 16)).

%% @doc Run a time-lock key, turning its outcome (or any crash) into a result
%% message. `x' is included whenever it was computed.
safe(Fun) ->
    Result =
        try Fun()
        catch Class:Reason ->
            ?event(timelock, {crashed, Class, Reason}),
            {error, <<"internal-error">>}
        end,
    case Result of
        {checked, X, ok} ->
            {ok, #{ <<"valid">> => true, <<"x">> => to_hex(X) }};
        {checked, X, Failure} ->
            {ok, (invalid(Failure))#{ <<"x">> => to_hex(X) }};
        Failure ->
            {ok, invalid(Failure)}
    end.

%% @doc Verify a BLS signature over a message under a public key.
-spec verify_bls(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ valid := boolean(), _ => _ }}.
verify_bls(Base, Req, Opts) ->
    Result =
        maybe
            {ok, PubKey} ?= hex_input(<<"public-key">>, Base, Req, Opts),
            {ok, Message} ?= binary_input(<<"message">>, Base, Req, Opts),
            {ok, Sig} ?= hex_input(<<"signature">>, Base, Req, Opts),
            true ?= blst_nif:verify(PubKey, Message, Sig),
            {ok, Sig}
        end,
    case Result of
        {ok, ValidSig} ->
            {ok, #{ <<"valid">> => true, <<"value">> => round_value(ValidSig) }};
        Failure ->
            ?event(timelock, {verify_bls_failed, Failure}),
            {ok, invalid(Failure)}
    end.

%% @doc Derive up to `?MAX_DERIVE_BYTES' bytes from a seed and a counter.
-spec derive(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ valid := boolean(), _ => _ }}.
derive(Base, Req, Opts) ->
    Result =
        maybe
            {ok, Seed} ?= binary_input(<<"seed">>, Base, Req, Opts),
            {ok, Counter} ?= int_input(<<"counter">>, Base, Req, Opts),
            {ok, Bytes} ?= int_input(<<"bytes">>, Base, Req, Opts),
            true ?= Counter >= 0 orelse {error, <<"counter-negative">>},
            true ?=
                (Bytes >= 1 andalso Bytes =< ?MAX_DERIVE_BYTES)
                    orelse {error, <<"bytes-out-of-range">>},
            {ok, derive_bytes(Seed, Counter, Bytes)}
        end,
    case Result of
        {ok, Derived} ->
            {ok, #{ <<"valid">> => true, <<"value">> => hb_util:to_hex(Derived) }};
        Failure ->
            {ok, invalid(Failure)}
    end.

%% @doc The beacon's value for a round: the hex of SHA-256 over `round|'
%% followed by the lowercase hex of the round's group signature.
round_value(Sig) ->
    SigHex = hb_util:to_hex(Sig),
    hb_util:to_hex(crypto:hash(sha256, <<"round|", SigHex/binary>>)).

%% @doc Concatenate SHA-256(Seed ++ decimal(Counter + K)) blocks for K = 0, 1,
%% ... and keep the first `Bytes' bytes.
derive_bytes(Seed, Counter, Bytes) ->
    Stream =
        <<
            <<(crypto:hash(
                sha256,
                <<Seed/binary, (integer_to_binary(Counter + K))/binary>>
            ))/binary>>
        ||
            K <- lists:seq(0, (Bytes + 31) div 32 - 1)
        >>,
    binary:part(Stream, 0, Bytes).

%% @doc The result message for a failed verification or derivation.
invalid({error, Reason}) when is_atom(Reason) ->
    Dashed = binary:replace(hb_util:bin(Reason), <<"_">>, <<"-">>, [global]),
    invalid({error, Dashed});
invalid({error, Reason}) when is_binary(Reason) ->
    #{ <<"valid">> => false, <<"reason">> => Reason };
invalid(false) ->
    #{ <<"valid">> => false, <<"reason">> => <<"verify-failed">> }.

%% @doc Find an input key in the request, then in the base message.
input(Key, Base, Req, Opts) ->
    case hb_maps:find(Key, Req, Opts) of
        {ok, Value} -> {ok, Value};
        error ->
            case hb_maps:find(Key, Base, Opts) of
                {ok, Value} -> {ok, Value};
                error -> {error, <<"missing-", Key/binary>>}
            end
    end.

%% @doc Find a binary input.
binary_input(Key, Base, Req, Opts) ->
    case input(Key, Base, Req, Opts) of
        {ok, Value} when is_binary(Value) -> {ok, Value};
        {ok, _} -> {error, <<"invalid-", Key/binary>>};
        Error -> Error
    end.

%% @doc Find a hex input and decode it.
hex_input(Key, Base, Req, Opts) ->
    maybe
        {ok, Hex} ?= binary_input(Key, Base, Req, Opts),
        try {ok, binary:decode_hex(Hex)}
        catch _:_ -> {error, <<"invalid-", Key/binary>>}
        end
    end.

%% @doc Find an integer input, given as an integer or in decimal.
int_input(Key, Base, Req, Opts) ->
    maybe
        {ok, Value} ?= input(Key, Base, Req, Opts),
        {ok, _Int} ?= hb_util:safe_int(Value)
    else
        {error, invalid} -> {error, <<"invalid-", Key/binary>>};
        Error -> Error
    end.

%% @doc Find a hex integer, lowercase without leading zeros, of at most
%% `?MAX_HEX_CHARS' digits. Returns the hex as given and its value.
hex_int_input(Key, Base, Req, Opts) ->
    maybe
        {ok, Hex} ?= binary_input(Key, Base, Req, Opts),
        true ?=
            byte_size(Hex) =< ?MAX_HEX_CHARS
                orelse {error, <<Key/binary, "-too-large">>},
        true ?= is_hex(Hex) orelse {error, <<"invalid-", Key/binary>>},
        true ?= is_canonical_hex(Hex)
            orelse {error, <<"non-canonical-", Key/binary>>},
        {ok, {Hex, binary_to_integer(Hex, 16)}}
    end.

is_hex(<<>>) -> false;
is_hex(Hex) ->
    lists:all(
        fun(C) ->
            (C >= $0 andalso C =< $9) orelse (C >= $a andalso C =< $f)
                orelse (C >= $A andalso C =< $F)
        end,
        binary_to_list(Hex)
    ).

is_canonical_hex(<<"0">>) -> true;
is_canonical_hex(<<"0", _/binary>>) -> false;
is_canonical_hex(Hex) -> string:lowercase(Hex) =:= Hex.

%% @doc The modulus: a hex integer above 1.
modulus_input(Base, Req, Opts) ->
    maybe
        {ok, {_, N}} = Ok ?= hex_int_input(<<"modulus">>, Base, Req, Opts),
        true ?= N > 1 orelse {error, <<"modulus-out-of-range">>},
        Ok
    end.

%% @doc The seed: a binary of at most `?MAX_SEED_BYTES' bytes.
seed_input(Base, Req, Opts) ->
    maybe
        {ok, Seed} ?= binary_input(<<"seed">>, Base, Req, Opts),
        true ?=
            byte_size(Seed) =< ?MAX_SEED_BYTES
                orelse {error, <<"seed-too-large">>},
        {ok, Seed}
    end.

%% @doc The squaring count: an integer from 0 to 2^32, or its decimal
%% without leading zeros.
t_input(Base, Req, Opts) ->
    maybe
        {ok, Value} ?= input(<<"t">>, Base, Req, Opts),
        {ok, T} ?= parse_t(Value),
        true ?= (T >= 0 andalso T =< ?MAX_T) orelse {error, <<"t-out-of-range">>},
        {ok, T}
    end.

parse_t(T) when is_integer(T) -> {ok, T};
parse_t(<<"0">>) -> {ok, 0};
parse_t(<<First, _/binary>> = Dec)
        when First >= $1, First =< $9, byte_size(Dec) =< 11 ->
    case lists:all(fun(C) -> C >= $0 andalso C =< $9 end, binary_to_list(Dec)) of
        true -> {ok, binary_to_integer(Dec)};
        false -> {error, <<"invalid-t">>}
    end;
parse_t(<<First, _/binary>> = Dec) when First >= $1, First =< $9 ->
    case lists:all(fun(C) -> C >= $0 andalso C =< $9 end, binary_to_list(Dec)) of
        true -> {error, <<"t-out-of-range">>};
        false -> {error, <<"invalid-t">>}
    end;
parse_t(_) -> {error, <<"invalid-t">>}.

%%% Tests

%% @doc Resolve a key of the device with the given inputs.
resolve_device(Key, Inputs) ->
    hb_ao:resolve(
        Inputs#{ <<"path">> => <<"~timelock@1.0/", Key/binary>> },
        #{}
    ).

%% @doc Every time-lock vector from the reference implementation gives the
%% same `valid' and `x' as it does there.
timelock_vectors_test_() ->
    [
        {
            Name,
            fun() ->
                {ok, Res} = resolve_device(Key, Inputs),
                ?assertEqual(
                    maps:get(<<"valid">>, Expected),
                    hb_maps:get(<<"valid">>, Res, undefined, #{})
                ),
                case maps:find(<<"x">>, Expected) of
                    {ok, X} ->
                        ?assertEqual(X, hb_maps:get(<<"x">>, Res, undefined, #{}));
                    error -> ok
                end,
                case maps:get(<<"valid">>, Expected) of
                    true -> ok;
                    false ->
                        ?assert(is_binary(hb_maps:get(<<"reason">>, Res, undefined, #{})))
                end
            end
        }
    ||
        {Name, Key, Inputs, Expected} <- dev_timelock_test_vectors:timelock_vectors()
    ].

%% @doc The challenge primes of the valid cracks, and of the reference's own
%% fixed case, are the reference's.
challenge_prime_test() ->
    Cracks =
        [
            {I, E}
        ||
            {_, <<"verify-crack">>, I, #{ <<"l">> := _ } = E}
                <- dev_timelock_test_vectors:timelock_vectors()
        ],
    ?assert(length(Cracks) >= 3),
    lists:foreach(
        fun({I, E}) ->
            ?assertEqual(
                binary_to_integer(maps:get(<<"l">>, E), 16),
                challenge_prime(
                    maps:get(<<"modulus">>, I),
                    maps:get(<<"x">>, E),
                    maps:get(<<"y">>, I),
                    binary_to_integer(maps:get(<<"t">>, I))
                )
            )
        end,
        Cracks
    ),
    {{N, X, Y, T}, L} = dev_timelock_test_vectors:fixed_challenge_prime(),
    Prime = challenge_prime(N, X, Y, T),
    ?assertEqual(binary_to_integer(L, 16), Prime),
    ?assertEqual(128, bit_length(Prime)).

%% @doc The primality test refuses Carmichael numbers and small composites,
%% and accepts small and large primes.
primality_test() ->
    Small = small_primes(),
    ?assertEqual(168, length(Small)),
    ?assertNot(is_probable_prime(561, Small)),
    ?assertNot(is_probable_prime(1, Small)),
    ?assertNot(is_probable_prime(1009 * 1013, Small)),
    ?assert(is_probable_prime(2, Small)),
    ?assert(is_probable_prime(997, Small)),
    ?assert(is_probable_prime((1 bsl 127) - 1, Small)),
    ?assertNot(is_probable_prime((1 bsl 128) + 1, Small)).

%% @doc Malformed and oversized inputs give `valid: false' with a reason, and
%% `t' may also be given as an integer.
timelock_malformed_inputs_test() ->
    [{_, <<"verify-reveal">>, Good, _}] =
        [
            V
        ||
            {<<"1024-t1024-reveal-valid-p">>, _, _, _} = V
                <- dev_timelock_test_vectors:timelock_vectors()
        ],
    Reveal =
        fun(Inputs) ->
            {ok, Res} = resolve_device(<<"verify-reveal">>, Inputs),
            {
                hb_maps:get(<<"valid">>, Res, undefined, #{}),
                hb_maps:get(<<"reason">>, Res, undefined, #{})
            }
        end,
    ?assertEqual({true, undefined}, Reveal(Good)),
    ?assertEqual(
        {true, undefined},
        Reveal(Good#{ <<"t">> => binary_to_integer(maps:get(<<"t">>, Good)) })
    ),
    Upper = string:uppercase(maps:get(<<"y">>, Good)),
    Huge = binary:copy(<<"f">>, ?MAX_HEX_CHARS + 1),
    lists:foreach(
        fun({Inputs, Reason}) ->
            ?assertEqual({false, Reason}, Reveal(Inputs))
        end,
        [
            {maps:remove(<<"p">>, Good), <<"missing-p">>},
            {maps:remove(<<"modulus">>, Good), <<"missing-modulus">>},
            {Good#{ <<"y">> => Upper }, <<"non-canonical-y">>},
            {Good#{ <<"y">> => <<"0", (maps:get(<<"y">>, Good))/binary>> },
                <<"non-canonical-y">>},
            {Good#{ <<"y">> => <<>> }, <<"invalid-y">>},
            {Good#{ <<"y">> => <<"0x12">> }, <<"invalid-y">>},
            {Good#{ <<"y">> => 12 }, <<"invalid-y">>},
            {Good#{ <<"modulus">> => Huge }, <<"modulus-too-large">>},
            {Good#{ <<"modulus">> => <<"1">> }, <<"modulus-out-of-range">>},
            {Good#{ <<"p">> => Huge }, <<"p-too-large">>},
            {Good#{ <<"seed">> => binary:copy(<<"s">>, ?MAX_SEED_BYTES + 1) },
                <<"seed-too-large">>},
            {Good#{ <<"t">> => <<"4294967297">> }, <<"t-out-of-range">>},
            {Good#{ <<"t">> => <<"99999999999999999999">> }, <<"t-out-of-range">>},
            {Good#{ <<"t">> => -1 }, <<"t-out-of-range">>},
            {Good#{ <<"t">> => <<"01024">> }, <<"invalid-t">>},
            {Good#{ <<"t">> => <<"-1">> }, <<"invalid-t">>},
            {Good#{ <<"t">> => <<"1e3">> }, <<"invalid-t">>},
            {Good#{ <<"t">> => 1.5 }, <<"invalid-t">>},
            {Good#{ <<"p">> => <<"2">> }, <<"p-not-a-factor">>},
            {Good#{ <<"p">> => <<"1">> }, <<"p-out-of-range">>},
            {Good#{ <<"y">> => <<"1">> }, <<"verify-failed">>}
        ]
    ),
    % `x' comes back whenever it was computed, even for a wrong answer.
    {ok, Wrong} =
        resolve_device(<<"verify-reveal">>, Good#{ <<"y">> => <<"1">> }),
    {ok, Derived} =
        resolve_device(
            <<"derive-x">>,
            maps:with([<<"modulus">>, <<"seed">>], Good)
        ),
    ?assertEqual(
        hb_maps:get(<<"x">>, Derived, undefined, #{}),
        hb_maps:get(<<"x">>, Wrong, undefined, #{})
    ),
    {ok, NoSeed} = resolve_device(<<"derive-x">>, maps:with([<<"modulus">>], Good)),
    ?assertEqual(false, hb_maps:get(<<"valid">>, NoSeed, undefined, #{})),
    ?assertEqual(<<"missing-seed">>, hb_maps:get(<<"reason">>, NoSeed, undefined, #{})).

%% @doc Every interop vector from the beacon's JavaScript (noble-curves)
%% verifies, or fails, exactly as it does there.
interop_vectors_test_() ->
    [
        {
            Name,
            fun() ->
                {ok, Res} = resolve_device(<<"verify-bls">>, Inputs),
                ?assertEqual(Valid, hb_maps:get(<<"valid">>, Res, undefined, #{})),
                case Valid of
                    true ->
                        ?assertEqual(
                            round_value(
                                binary:decode_hex(
                                    maps:get(<<"signature">>, Inputs)
                                )
                            ),
                            hb_maps:get(<<"value">>, Res, undefined, #{})
                        );
                    false ->
                        ?assertNot(hb_maps:is_key(<<"value">>, Res, #{}))
                end
            end
        }
    ||
        {Name, Inputs, Valid} <- dev_timelock_test_vectors:bls_vectors()
    ].

%% @doc A round signature of the live beacon process verifies under its group
%% key, and the device gives the value the beacon published for it.
live_beacon_round_test() ->
    {Inputs, Value} = dev_timelock_test_vectors:live_round(),
    {ok, Res} = resolve_device(<<"verify-bls">>, Inputs),
    ?assertEqual(true, hb_maps:get(<<"valid">>, Res, undefined, #{})),
    ?assertEqual(Value, hb_maps:get(<<"value">>, Res, undefined, #{})).

%% @doc On-curve points outside the prime-order subgroups are rejected by the
%% subgroup checks, before any pairing.
subgroup_check_test() ->
    Vectors = maps:from_list(
        [{N, I} || {N, I, _} <- dev_timelock_test_vectors:bls_vectors()]
    ),
    Raw =
        fun(Name) ->
            I = maps:get(Name, Vectors),
            blst_nif:verify(
                binary:decode_hex(maps:get(<<"public-key">>, I)),
                maps:get(<<"message">>, I),
                binary:decode_hex(maps:get(<<"signature">>, I))
            )
        end,
    ?assertEqual({error, signature_not_in_group}, Raw(<<"non-subgroup-sig">>)),
    ?assertEqual({error, public_key_not_in_group}, Raw(<<"non-subgroup-key">>)),
    ?assertEqual({error, public_key_is_infinity}, Raw(<<"infinity-key">>)).

%% @doc Missing and mistyped inputs give `valid: false' with a reason.
malformed_inputs_test() ->
    {_Name, Good, true} = hd(dev_timelock_test_vectors:bls_vectors()),
    Check =
        fun(Inputs, Reason) ->
            {ok, Res} = resolve_device(<<"verify-bls">>, Inputs),
            ?assertEqual(false, hb_maps:get(<<"valid">>, Res, undefined, #{})),
            ?assertEqual(Reason, hb_maps:get(<<"reason">>, Res, undefined, #{}))
        end,
    Check(maps:remove(<<"signature">>, Good), <<"missing-signature">>),
    Check(Good#{ <<"public-key">> => <<"0x12">> }, <<"invalid-public-key">>),
    Check(Good#{ <<"message">> => 7 }, <<"invalid-message">>),
    Check(
        Good#{ <<"signature">> => <<"00">> },
        <<"bad-signature-size">>
    ).

%% @doc `derive' gives SHA-256(seed ++ decimal(counter + k)) blocks, cut to
%% the requested length, and refuses out-of-range requests.
derive_test() ->
    Seed = <<"seed">>,
    Expected =
        binary:part(
            <<
                (crypto:hash(sha256, <<"seed7">>))/binary,
                (crypto:hash(sha256, <<"seed8">>))/binary,
                (crypto:hash(sha256, <<"seed9">>))/binary
            >>,
            0,
            70
        ),
    {ok, Res} =
        resolve_device(
            <<"derive">>,
            #{ <<"seed">> => Seed, <<"counter">> => 7, <<"bytes">> => 70 }
        ),
    ExpectedHex = hb_util:to_hex(Expected),
    ?assertEqual(ExpectedHex, hb_maps:get(<<"value">>, Res, undefined, #{})),
    % Integers given in decimal, as they arrive over HTTP, are the same.
    {ok, FromDecimal} =
        resolve_device(
            <<"derive">>,
            #{
                <<"seed">> => Seed,
                <<"counter">> => <<"7">>,
                <<"bytes">> => <<"70">>
            }
        ),
    ?assertEqual(
        ExpectedHex,
        hb_maps:get(<<"value">>, FromDecimal, undefined, #{})
    ),
    {ok, Max} =
        resolve_device(
            <<"derive">>,
            #{ <<"seed">> => Seed, <<"counter">> => 0, <<"bytes">> => 1024 }
        ),
    ?assertEqual(2048, byte_size(hb_maps:get(<<"value">>, Max, undefined, #{}))),
    lists:foreach(
        fun({Inputs, Reason}) ->
            {ok, Bad} = resolve_device(<<"derive">>, Inputs),
            ?assertEqual(false, hb_maps:get(<<"valid">>, Bad, undefined, #{})),
            ?assertEqual(Reason, hb_maps:get(<<"reason">>, Bad, undefined, #{}))
        end,
        [
            {#{ <<"seed">> => Seed, <<"counter">> => 0, <<"bytes">> => 1025 },
                <<"bytes-out-of-range">>},
            {#{ <<"seed">> => Seed, <<"counter">> => 0, <<"bytes">> => 0 },
                <<"bytes-out-of-range">>},
            {#{ <<"seed">> => Seed, <<"counter">> => -1, <<"bytes">> => 8 },
                <<"counter-negative">>},
            {#{ <<"seed">> => Seed, <<"counter">> => <<"x">>, <<"bytes">> => 8 },
                <<"invalid-counter">>},
            {#{ <<"counter">> => 0, <<"bytes">> => 8 }, <<"missing-seed">>}
        ]
    ).

%% @doc The Lua module used by the `lua@5.3a' tests: its functions call the
%% device through `ao.resolve', as a beacon process does.
lua_module() ->
    <<"""
    function check_round(base, req)
        local status, res = ao.resolve({
            path = "/~timelock@1.0/verify-bls",
            ["public-key"] = req["public-key"],
            message = req.message,
            signature = req.signature
        })
        return res
    end
    function check_round_device_key(base, req)
        local status, res = ao.resolve(
            {
                device = "timelock@1.0",
                ["public-key"] = req["public-key"],
                message = req.message,
                signature = req.signature
            },
            { path = "verify-bls" }
        )
        return res
    end
    function derive_seed(base, req)
        local status, res = ao.resolve({
            path = "/~timelock@1.0/derive",
            seed = req.seed,
            counter = 7,
            bytes = 70
        })
        return res
    end
    function check_reveal(base, req)
        local status, res = ao.resolve({
            path = "/~timelock@1.0/verify-reveal",
            modulus = req.modulus,
            seed = req.seed,
            t = req.t,
            y = req.y,
            p = req.p
        })
        return res
    end
    function check_crack(base, req)
        local status, res = ao.resolve({
            path = "/~timelock@1.0/verify-crack",
            modulus = req.modulus,
            seed = req.seed,
            t = req.t,
            y = req.y,
            proof = req.proof
        })
        return res
    end
    """>>.

%% @doc A `lua@5.3a' process with the test module loaded.
lua_base() ->
    #{
        <<"device">> => <<"lua@5.3a">>,
        <<"module">> => #{
            <<"content-type">> => <<"application/lua">>,
            <<"body">> => lua_module()
        }
    }.

%% @doc A `lua@5.3a' process verifies a beacon round, and derives bytes,
%% by calling the device from Lua.
lua_resolve_test() ->
    Base = lua_base(),
    {Live, Value} = dev_timelock_test_vectors:live_round(),
    lists:foreach(
        fun(Func) ->
            {ok, Res} = hb_ao:resolve(Base, Live#{ <<"path">> => Func }, #{}),
            ?assertEqual(true, hb_ao:get(<<"valid">>, Res, #{})),
            ?assertEqual(Value, hb_ao:get(<<"value">>, Res, #{})),
            {ok, Bad} =
                hb_ao:resolve(
                    Base,
                    Live#{ <<"path">> => Func, <<"message">> => <<"forged|1">> },
                    #{}
                ),
            ?assertEqual(false, hb_ao:get(<<"valid">>, Bad, #{}))
        end,
        [<<"check_round">>, <<"check_round_device_key">>]
    ),
    {ok, Derived} =
        hb_ao:resolve(
            Base,
            #{ <<"path">> => <<"derive_seed">>, <<"seed">> => <<"seed">> },
            #{}
        ),
    {ok, Direct} =
        resolve_device(
            <<"derive">>,
            #{ <<"seed">> => <<"seed">>, <<"counter">> => 7, <<"bytes">> => 70 }
        ),
    ?assertEqual(
        hb_maps:get(<<"value">>, Direct, undefined, #{}),
        hb_ao:get(<<"value">>, Derived, #{})
    ).

%% @doc A `lua@5.3a' process checks a reveal and a crack from the reference
%% vectors by calling the device from Lua, and refuses a wrong answer.
lua_timelock_test() ->
    Base = lua_base(),
    Vectors =
        maps:from_list(
            [
                {Name, {Inputs, Expected}}
            ||
                {Name, _, Inputs, Expected}
                    <- dev_timelock_test_vectors:timelock_vectors()
            ]
        ),
    {_, #{ <<"x">> := X }} = maps:get(<<"1024-t1024-derive-x">>, Vectors),
    lists:foreach(
        fun({Func, Name, Valid}) ->
            {Inputs, _} = maps:get(Name, Vectors),
            {ok, Res} = hb_ao:resolve(Base, Inputs#{ <<"path">> => Func }, #{}),
            ?assertEqual(Valid, hb_ao:get(<<"valid">>, Res, #{})),
            ?assertEqual(X, hb_ao:get(<<"x">>, Res, #{}))
        end,
        [
            {<<"check_reveal">>, <<"1024-t1024-reveal-valid-p">>, true},
            {<<"check_reveal">>, <<"1024-t1024-reveal-wrong-y">>, false},
            {<<"check_crack">>, <<"1024-t1024-crack-valid">>, true},
            {<<"check_crack">>, <<"1024-t1024-crack-corrupt-proof">>, false}
        ]
    ).

-module(ar_wallet).
-export([sign/2, sign/3, hmac/1, hmac/2, verify/3, verify/4]).
-export([to_pubkey/1, to_pubkey/2, to_address/1, to_address/2, new/0, new_ecdsa/0, new/1]).
-export([new_keyfile/2, load_keyfile/1, load_keyfile/2, load_key/1, load_key/2]).
-export([to_json/1, from_json/1, from_json/2]).
-export([recover_key/3]).
-export([compress_ecdsa_pubkey/1]).
-include("include/ar.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% @doc Utilities for manipulating wallets.

-define(WALLET_DIR, ".").
-define(WALLET_POOL_TARGET, 6).
-define(CRT_TABLE, ar_wallet_crt_params).
-define(CRT_TABLE_LIMIT, 64).

%%% Public interface.

new() ->
    new({rsa, 65537}).
new(KeyType) when KeyType =:= {rsa, 65537} orelse KeyType =:= {eddsa, ed25519} orelse KeyType =:= ethereum orelse KeyType =:= solana ->
    case request_pooled_wallet(KeyType) of
        {ok, Wallet} -> Wallet;
        timeout -> generate_wallet(KeyType)
    end;
new(KeyType = {?ECDSA_SIGN_ALG, secp256k1}) ->
    case request_pooled_wallet(KeyType) of
        {ok, Wallet} -> Wallet;
        timeout -> generate_wallet(KeyType)
    end.

new_ecdsa() ->
    new({?ECDSA_SIGN_ALG, secp256k1}).

generate_wallet(KeyType = {KeyAlg, PublicExpnt}) when KeyType =:= {rsa, 65537} ->
    {[_, Pub], [_, Pub, Priv, P1, P2, E1, E2, C]} =
        crypto:generate_key(KeyAlg, {4096, PublicExpnt}),
    remember_crt_params(Pub, {P1, P2, E1, E2, C}),
    {{KeyType, Priv, Pub}, {KeyType, Pub}};
generate_wallet(KeyType = {KeyAlg, KeyCrv}) when KeyAlg =:= ?ECDSA_SIGN_ALG andalso KeyCrv =:= secp256k1 ->
    {OrigPub, Priv} = crypto:generate_key(ecdh, KeyCrv),
    Pub = compress_ecdsa_pubkey(OrigPub),
    {{KeyType, Priv, Pub}, {KeyType, Pub}};
generate_wallet(ethereum)  ->
    {Pub, Priv} = crypto:generate_key(ecdh, secp256k1),
    {{ethereum, Priv, Pub}, {ethereum, Pub}};
generate_wallet(solana) ->
    generate_wallet({eddsa, ed25519});
generate_wallet(KeyType = {KeyAlg, Curve}) when KeyType =:= {?EDDSA_SIGN_ALG, ed25519} ->
    {Pub, Priv} = crypto:generate_key(KeyAlg, Curve),
    {{KeyType, Priv, Pub}, {KeyType, Pub}}.

request_pooled_wallet(KeyType) ->
    Pool = ensure_wallet_pool(KeyType),
    Ref = make_ref(),
    Pool ! {wallet, self(), Ref},
    receive
        {wallet, Ref, Wallet} -> {ok, Wallet}
    after 30000 ->
        timeout
    end.

ensure_wallet_pool(KeyType) ->
    PoolName = wallet_pool_name(KeyType),
    case whereis(PoolName) of
        undefined ->
            Pid = spawn(fun() -> wallet_pool_loop(KeyType, queue:new(), queue:new(), 0) end),
            case catch register(PoolName, Pid) of
                true -> Pid;
                _ -> whereis(PoolName)
            end;
        Pid ->
            Pid
    end.

wallet_pool_loop(KeyType, Wallets, Waiters, InFlight) ->
    {Wallets1, InFlight1} = maybe_spawn_wallet_workers(KeyType, Wallets, Waiters, InFlight),
    receive
        {wallet, From, Ref} ->
            case queue:out(Wallets1) of
                {{value, Wallet}, Rest} ->
                    From ! {wallet, Ref, Wallet},
                    wallet_pool_loop(KeyType, Rest, Waiters, InFlight1);
                {empty, _} ->
                    wallet_pool_loop(KeyType, Wallets1, queue:in({From, Ref}, Waiters), InFlight1)
            end;
        {wallet_generated, Wallet} ->
            case queue:out(Waiters) of
                {{value, {From, Ref}}, RestWaiters} ->
                    From ! {wallet, Ref, Wallet},
                    wallet_pool_loop(KeyType, Wallets1, RestWaiters, InFlight1 - 1);
                {empty, _} ->
                    wallet_pool_loop(KeyType, queue:in(Wallet, Wallets1), Waiters, InFlight1 - 1)
            end
    end.

maybe_spawn_wallet_workers(KeyType, Wallets, Waiters, InFlight) ->
    Desired = ?WALLET_POOL_TARGET + queue:len(Waiters),
    Available = queue:len(Wallets) + InFlight,
    Needed = max(0, Desired - Available),
    Parent = self(),
    lists:foreach(
        fun(_) ->
            spawn(fun() -> Parent ! {wallet_generated, generate_wallet(KeyType)} end)
        end,
        lists:seq(1, Needed)
    ),
    {Wallets, InFlight + Needed}.

wallet_pool_name({rsa, 65537}) ->
    ar_wallet_pool_rsa_65537;
wallet_pool_name({?EDDSA_SIGN_ALG, ed25519}) ->
    ar_wallet_pool_ed25519;
wallet_pool_name({?ECDSA_SIGN_ALG, secp256k1}) ->
    ar_wallet_pool_ecdsa_secp256k1;
wallet_pool_name(solana) ->
    ar_wallet_pool_solana;
wallet_pool_name(ethereum) ->
    ar_wallet_pool_ethereum.

%% @doc Sign some data with a private key.
sign(Key, Data) ->
    sign(Key, Data, sha256).

%% @doc sign some data, hashed using the provided DigestType.
%% RSA and ECDSA signatures use wallet-level wrappers.
sign({{rsa, PublicExpnt}, Priv, Pub}, Data, DigestType) when PublicExpnt =:= 65537 ->
    rsa_pss:sign(Data, DigestType, rsa_private_key(PublicExpnt, Priv, Pub));
sign({{KeyAlg, KeyCrv}, Priv, _Pub}, Data, _DigestType)
        when KeyAlg =:= ?ECDSA_SIGN_ALG andalso KeyCrv =:= secp256k1 ->
    secp256k1_nif:sign(Data, Priv);
sign({KeyType = {KeyAlg, Curve}, Priv, _Pub}, Data, _DigestType) when KeyType =:= {?EDDSA_SIGN_ALG, ed25519} ->
    crypto:sign(KeyAlg, none, Data, [Priv, Curve]);
sign({ethereum, Priv, Pub}, Data, _DigestType) ->
    secp256k1_nif:sign(Data, Priv, ethereum);
sign({{KeyType, Priv, Pub}, {KeyType, Pub}}, Data, DigestType) ->
    sign({KeyType, Priv, Pub}, Data, DigestType).

hmac(Data) ->
    hmac(Data, sha256).

hmac(Data, DigestType) -> crypto:mac(hmac, DigestType, <<"ar">>, Data).

%% @doc Verify that a signature is correct.
verify(Key, Data, Sig) ->
    verify(Key, Data, Sig, sha256).

verify({{rsa, PublicExpnt}, Pub}, Data, Sig, DigestType) when PublicExpnt =:= 65537 ->
    rsa_pss:verify(
        Data,
        DigestType,
        Sig,
        #'RSAPublicKey'{
            publicExponent = PublicExpnt,
            modulus = binary:decode_unsigned(Pub)
        }
    );
%% NOTE: We will not write pubkey for ECDSA signature. So don't use verify function 
%% for ECDSA directly, use ecrecover pattern. This function will return always false 
%% if called with no Pub.
verify({{KeyAlg, KeyCrv}, Pub}, Data, Sig, _DigestType)
        when KeyAlg =:= ?ECDSA_SIGN_ALG andalso KeyCrv =:= secp256k1 ->
    {Pass, PubExtracted} = secp256k1_nif:ecrecover(Data, Sig),
    Pass andalso PubExtracted =:= Pub;
verify({{KeyAlg, Curve}, Pub}, Data, Sig, _DigestType) when
      byte_size(Pub) == 32 andalso byte_size(Sig) == 64 andalso Curve =:= ed25519 andalso KeyAlg =:= ?EDDSA_SIGN_ALG ->
    crypto:verify(eddsa, none, Data, Sig, [Pub, Curve]);
verify({ethereum, Pub}, Data, Sig, _DigestType) ->
    {Pass, PubExtracted} = secp256k1_nif:ecrecover(Data, Sig, ethereum),
    Pass andalso PubExtracted =:= compress_ecdsa_pubkey(Pub);
verify({solana, Pub}, Data, Sig, _DigestType) when
      byte_size(Pub) == 32 andalso byte_size(Sig) == 64 ->
    HexData = hb_util:to_hex(Data),
    crypto:verify(eddsa, none, HexData, Sig, [Pub, ed25519]).

%% @doc Find a public key from a wallet.
to_pubkey(Pubkey) ->
    to_pubkey(Pubkey, ?DEFAULT_KEY_TYPE).
to_pubkey(PubKey, {rsa, 65537}) when bit_size(PubKey) == 256 ->
    % Small keys are not secure, nobody is using them, the clause
    % is for backwards-compatibility.
    PubKey;
to_pubkey({{_, _, PubKey}, {_, PubKey}}, {rsa, 65537}) ->
    PubKey;
to_pubkey(PubKey, {rsa, 65537}) ->
    PubKey.

%% @doc Generate an address from a public key.
to_address(Pubkey) ->
    to_address(Pubkey, ?DEFAULT_KEY_TYPE).
to_address(PubKey, {rsa, 65537}) when bit_size(PubKey) == 256 ->
    PubKey;
to_address({{_, _, PubKey}, {_, PubKey}}, _) ->
    to_address(PubKey);
to_address(PubKey, {rsa, 65537}) ->
    to_rsa_address(PubKey);
to_address(PubKey, {?ECDSA_SIGN_ALG, secp256k1}) ->
	%% For Arweave L1 ECDSA transactions, address is SHA256 hash of public key
	%% (same as RSA). The keccak-based Ethereum address is used elsewhere.
	hash_address(PubKey);
to_address(PubKey, {?EDDSA_SIGN_ALG, ed25519}) ->
    to_eddsa_address(PubKey);
to_address(PubKey, solana) ->
    to_solana_address(PubKey);
to_address(PubKey, ethereum) ->
    to_ethereum_address(PubKey);
to_address(PubKey, typed_ethereum) ->
    to_ethereum_address(PubKey).

%% @doc Generate a new wallet public and private key, with a corresponding keyfile.
%% The provided key is used as part of the file name.
new_keyfile(KeyType, WalletName) when is_list(WalletName) ->
    new_keyfile(KeyType, list_to_binary(WalletName));
new_keyfile(KeyType, WalletName) ->
    {Pub, Priv, Key} =
        case KeyType of
            {?RSA_SIGN_ALG, PublicExpnt} ->
                {[Expnt, Pb], [Expnt, Pb, Prv, P1, P2, E1, E2, C]} =
                    crypto:generate_key(rsa, {?RSA_PRIV_KEY_SZ, PublicExpnt}),
                remember_crt_params(Pb, {P1, P2, E1, E2, C}),
                PrivKey = {KeyType, Prv, Pb},
                Ky = to_json(PrivKey),
                {Pb, Prv, Ky};
            {?ECDSA_SIGN_ALG, secp256k1} ->
                {OrigPub, Prv} = crypto:generate_key(ecdh, secp256k1),
                CompressedPub = compress_ecdsa_pubkey(OrigPub),
                PrivKey = {KeyType, Prv, CompressedPub},
                Ky = to_json(PrivKey),
                {CompressedPub, Prv, Ky};
            {?EDDSA_SIGN_ALG, ed25519} ->
                {{_, Prv, Pb}, _} = new(KeyType),
                PrivKey = {KeyType, Prv, Pb},
                Ky = to_json(PrivKey),
                {Pb, Prv, Ky};
            ethereum ->
                {Pb, Prv} = crypto:generate_key(ecdh, secp256k1),
                PrivKey = {KeyType, Prv, Pb},
                Ky = to_json(PrivKey),
                {Pb, Prv, Ky}
        end,
    Filename = wallet_filepath(WalletName, Pub, KeyType),
    filelib:ensure_dir(Filename),
    file:write_file(Filename, Key),
    {{KeyType, Priv, Pub}, {KeyType, Pub}}.

wallet_filepath(Wallet) ->
    filename:join([?WALLET_DIR, binary_to_list(Wallet)]).

wallet_filepath2(Wallet) ->
    filename:join([?WALLET_DIR, binary_to_list(Wallet)]).

%% @doc Read the keyfile for the key with the given address from disk.
%% Return not_found if arweave_keyfile_[addr].json or [addr].json is not found
%% in [data_dir]/?WALLET_DIR.
load_key(Addr) ->
    load_key(Addr, #{}).

%% @doc Read the keyfile for the key with the given address from disk.
%% Return not_found if arweave_keyfile_[addr].json or [addr].json is not found
%% in [data_dir]/?WALLET_DIR.
load_key(Addr, Opts) ->
    Path = hb_util:encode(Addr),
    case filelib:is_file(Path) of
        false ->
            Path2 = wallet_filepath2(hb_util:encode(Addr)),
            case filelib:is_file(Path2) of
                false ->
                    not_found;
                true ->
                    load_keyfile(Path2, Opts)
            end;
        true ->
            load_keyfile(Path, Opts)
    end.

%% @doc Extract the public and private key from a keyfile.
load_keyfile(File) ->
    load_keyfile(File, #{}).

%% @doc Extract the public and private key from a keyfile.
load_keyfile(File, Opts) ->
    {ok, Body} = file:read_file(File),
    from_json(Body, Opts).

%% @doc Convert a wallet private key to JSON (JWK) format
to_json({PrivKey, _PubKey}) ->
    to_json(PrivKey);
to_json({{?RSA_SIGN_ALG, PublicExpnt}, Priv, Pub}) when PublicExpnt =:= 65537 ->
    hb_json:encode(#{
        kty => <<"RSA">>,
        ext => true,
        e => hb_util:encode(<<PublicExpnt:32>>),
        n => hb_util:encode(Pub),
        d => hb_util:encode(Priv)
    });
to_json({{?ECDSA_SIGN_ALG, secp256k1}, Priv, CompressedPub}) ->
    % For ECDSA, we need to expand the compressed pubkey to get X,Y coordinates
    % This is a simplified version - ideally we'd implement pubkey expansion
    hb_json:encode(#{
        kty => <<"EC">>,
        crv => <<"secp256k1">>,
        d => hb_util:encode(Priv)
        % TODO: Add x and y coordinates from expanded pubkey
    });
to_json({{?EDDSA_SIGN_ALG, ed25519}, Priv, Pub}) ->
    hb_json:encode(#{
        kty => <<"OKP">>,
        alg => <<"EdDSA">>,
        crv => <<"Ed25519">>,
        x => hb_util:encode(Pub),
        d => hb_util:encode(Priv)
    }).

%% @doc Parse a wallet from JSON (JWK) format
from_json(JsonBinary) ->
    from_json(JsonBinary, #{}).

%% @doc Parse a wallet from JSON (JWK) format with options
from_json(JsonBinary, Opts) ->
    Key = hb_json:decode(JsonBinary),
    {Pub, Priv, KeyType} =
        case hb_maps:get(<<"kty">>, Key, undefined, Opts) of
            <<"EC">> ->
                XEncoded = hb_maps:get(<<"x">>, Key, undefined, Opts),
                YEncoded = hb_maps:get(<<"y">>, Key, undefined, Opts),
                PrivEncoded = hb_maps:get(<<"d">>, Key, undefined, Opts),
                OrigPub = iolist_to_binary([<<4:8>>, hb_util:decode(XEncoded),
                        hb_util:decode(YEncoded)]),
                Pb = compress_ecdsa_pubkey(OrigPub),
                Prv = hb_util:decode(PrivEncoded),
                KyType = {?ECDSA_SIGN_ALG, secp256k1},
                {Pb, Prv, KyType};
            <<"OKP">> ->
                PubEncoded = hb_maps:get(<<"x">>, Key, undefined, Opts),
                PrivEncoded = hb_maps:get(<<"d">>, Key, undefined, Opts),
                Pb = hb_util:decode(PubEncoded),
                Prv = hb_util:decode(PrivEncoded),
                KyType = {?EDDSA_SIGN_ALG, ed25519},
                {Pb, Prv, KyType};
            _ ->
                PubEncoded = hb_maps:get(<<"n">>, Key, undefined, Opts),
                PrivEncoded = hb_maps:get(<<"d">>, Key, undefined, Opts),
                Pb = hb_util:decode(PubEncoded),
                Prv = hb_util:decode(PrivEncoded),
                KyType = {?RSA_SIGN_ALG, 65537},
                remember_jwk_crt_params(Pb, Key, Opts),
                {Pb, Prv, KyType}
        end,
    {{KeyType, Priv, Pub}, {KeyType, Pub}}.

%% @doc Recover the public key from a signature (for ECDSA).
%% For ECDSA transactions, the public key is not included in the transaction,
%% it must be recovered from the signature.
recover_key(_Data, <<>>, ?ECDSA_KEY_TYPE) ->
    <<>>;
recover_key(Data, Signature, ?ECDSA_KEY_TYPE) ->
    {_Pass, PubKey} = secp256k1_nif:ecrecover(Data, Signature),
    %% Note: if Pass = false, then PubKey will be <<>>
    PubKey.

%%%===================================================================
%%% Private functions.
%%%===================================================================

to_rsa_address(PubKey) ->
    hash_address(PubKey).

hash_address(PubKey) ->
    crypto:hash(sha256, PubKey).

to_ethereum_address(PubKey) ->
	hb_keccak:key_to_ethereum_address(PubKey).

to_eddsa_address(PubKey) ->
    hash_address(PubKey).

to_solana_address(PubKey) ->
    hb_util:base58_encode(PubKey).
%%%===================================================================
%%% Private functions.
%%%===================================================================

wallet_filepath(WalletName, PubKey, KeyType) ->
    wallet_filepath(wallet_name(WalletName, PubKey, KeyType)).

wallet_name(wallet_address, PubKey, KeyType) ->
    hb_util:encode(to_address(PubKey, KeyType));
wallet_name(WalletName, _, _) ->
    WalletName.

compress_ecdsa_pubkey(<<4:8, PubPoint/binary>>) ->
    PubPointMid = byte_size(PubPoint) div 2,
    <<X:PubPointMid/binary, Y:PubPointMid/integer-unit:8>> = PubPoint,
    PubKeyHeader =
        case Y rem 2 of
            0 -> <<2:8>>;
            1 -> <<3:8>>
        end,
    iolist_to_binary([PubKeyHeader, X]).

%% @doc Build the private key record for an RSA wallet, carrying the Chinese
%% Remainder Theorem parameters when this node knows them for the key. They make
%% `rsa_pss:sign/3' roughly three times faster, and it verifies its own result
%% before returning it, so a key without them signs more slowly but never less
%% correctly.
rsa_private_key(PublicExpnt, Priv, Pub) ->
    Base =
        #'RSAPrivateKey'{
            publicExponent = PublicExpnt,
            modulus = binary:decode_unsigned(Pub),
            privateExponent = binary:decode_unsigned(Priv)
        },
    case crt_params(Pub) of
        not_found ->
            Base;
        {P, Q, DP, DQ, QInv} ->
            Base#'RSAPrivateKey'{
                prime1 = P,
                prime2 = Q,
                exponent1 = DP,
                exponent2 = DQ,
                coefficient = QInv
            }
    end.

%% @doc Record the Chinese Remainder Theorem parameters of an RSA key against
%% its modulus, given as the binaries that `crypto' and JWK fields both provide.
%% These are secret key material: they stay in memory for the lifetime of the
%% node and must never reach the AO-Core store, so they are held beside it
%% rather than in it. A pair of primes that does not multiply to the modulus
%% belongs to a different key and is refused. The table is bounded because
%% `from_json/2' also parses keys supplied by callers, and a node registers its
%% own key when it loads it at startup, ahead of any of those.
remember_crt_params(Pub, {P, Q, DP, DQ, QInv}) ->
    Params =
        list_to_tuple(
            [ crypto:bytes_to_integer(Param) || Param <- [P, Q, DP, DQ, QInv] ]
        ),
    Modulus = binary:decode_unsigned(Pub),
    case element(1, Params) * element(2, Params) =:= Modulus of
        true ->
            ensure_crt_table(),
            case ets:info(?CRT_TABLE, size) < ?CRT_TABLE_LIMIT of
                true -> ets:insert(?CRT_TABLE, {key_id(Pub), Params}), ok;
                false -> ok
            end;
        false ->
            ok
    end.

%% @doc Record the Chinese Remainder Theorem parameters of a JWK RSA private
%% key, if it carries the complete set that RFC 7518 section 6.3.2 defines. A
%% key that omits any of them signs through the full-modulus path.
remember_jwk_crt_params(Pub, Key, Opts) ->
    Encoded =
        [
            hb_maps:get(Field, Key, undefined, Opts)
        ||
            Field <- [<<"p">>, <<"q">>, <<"dp">>, <<"dq">>, <<"qi">>]
        ],
    case lists:member(undefined, Encoded) of
        true ->
            ok;
        false ->
            remember_crt_params(
                Pub,
                list_to_tuple([ hb_util:decode(Param) || Param <- Encoded ])
            )
    end.

%% @doc Look up the Chinese Remainder Theorem parameters held for a modulus.
%% Returns `not_found' when the key was built without them, or before any key
%% has registered any, in which case signing takes the full-modulus path.
crt_params(Pub) ->
    try ets:lookup(?CRT_TABLE, key_id(Pub)) of
        [{_, Params}] -> Params;
        [] -> not_found
    catch
        error:badarg -> not_found
    end.

%% @doc Identify a key by its modulus, without holding the modulus as a key.
key_id(Pub) ->
    crypto:hash(sha256, Pub).

%% @doc The table is owned by a process that never exits: a table dies with its
%% owner, and the first caller is usually a short-lived request.
ensure_crt_table() ->
    case ets:whereis(?CRT_TABLE) of
        undefined ->
            Parent = self(),
            Ref = make_ref(),
            {Owner, Mon} =
                spawn_monitor(
                    fun() ->
                        try ets:new(?CRT_TABLE,
                                [named_table, public, set,
                                    {read_concurrency, true}]) of
                            _ ->
                                Parent ! {Ref, created},
                                receive after infinity -> ok end
                        catch error:badarg -> Parent ! {Ref, exists}
                        end
                    end
                ),
            receive
                {Ref, _} -> ok;
                {'DOWN', Mon, process, Owner, _} -> ok
            after 5000 -> ok
            end,
            erlang:demonitor(Mon, [flush]),
            ok;
        _ -> ok
    end.

%%%===================================================================
%%% Tests.
%%%===================================================================

%% @doc Generate a small RSA wallet for the tests and register its Chinese
%% Remainder Theorem parameters, returning the wallet and the raw parameters.
%% 2048 bits keeps the suite quick; none of this code depends on the key size.
generate_test_key() ->
    {[_, N], [_, N, D, P, Q, DP, DQ, QInv]} =
        crypto:generate_key(rsa, {2048, 65537}),
    KeyType = {?RSA_SIGN_ALG, 65537},
    remember_crt_params(N, {P, Q, DP, DQ, QInv}),
    {{{KeyType, D, N}, {KeyType, N}}, {P, Q, DP, DQ, QInv}}.

%% @doc A key that carries its CRT parameters produces a signature that verifies
%% against its public key.
crt_signature_verifies_test() ->
    {{PrivKey, PubKey}, _} = generate_test_key(),
    Data = crypto:strong_rand_bytes(256),
    ?assert(verify(PubKey, Data, sign(PrivKey, Data))).

%% @doc The CRT and full-modulus paths compute the same function. Holding the
%% PSS salt fixed makes the encoded message deterministic, so the two must
%% agree byte-for-byte.
crt_matches_full_modulus_test() ->
    {{{_, PrivBin, PubBin}, _}, _} = generate_test_key(),
    WithCRT = rsa_private_key(65537, PrivBin, PubBin),
    WithoutCRT =
        #'RSAPrivateKey'{
            publicExponent = 65537,
            modulus = binary:decode_unsigned(PubBin),
            privateExponent = binary:decode_unsigned(PrivBin)
        },
    ?assertNotEqual(undefined, WithCRT#'RSAPrivateKey'.prime1),
    Salt = crypto:strong_rand_bytes(32),
    Digest = {digest, crypto:hash(sha256, <<"fixed message">>)},
    ?assertEqual(
        rsa_pss:sign(Digest, sha256, Salt, WithoutCRT),
        rsa_pss:sign(Digest, sha256, Salt, WithCRT)
    ).

%% @doc A CRT parameter that does not hold yields a signature the public
%% exponent rejects, so signing falls back to the full modulus, still correct.
crt_fallback_on_bad_parameter_test() ->
    {{PrivKey, PubKey}, {P, Q, DP, DQ, QInv}} = generate_test_key(),
    {_, _, PubBin} = PrivKey,
    % The primes still multiply to the modulus, so these are accepted, but the
    % exchanged exponents produce the wrong residue for each prime.
    remember_crt_params(PubBin, {P, Q, DQ, DP, QInv}),
    Data = crypto:strong_rand_bytes(256),
    ?assert(verify(PubKey, Data, sign(PrivKey, Data))).

%% @doc Primes that do not multiply to the modulus belong to a different key and
%% are refused, leaving that key to sign through the full modulus.
mismatched_primes_refused_test() ->
    {_, {P, Q, DP, DQ, QInv}} = generate_test_key(),
    {[_, OtherN], _} = crypto:generate_key(rsa, {2048, 65537}),
    remember_crt_params(OtherN, {P, Q, DP, DQ, QInv}),
    ?assertEqual(not_found, crt_params(OtherN)).

%% @doc A JWK that omits any CRT field registers nothing for the key.
partial_jwk_crt_params_refused_test() ->
    {[_, N], [_, N, D, P, Q, DP, DQ, _]} =
        crypto:generate_key(rsa, {2048, 65537}),
    Key =
        #{
            <<"n">> => hb_util:encode(N),
            <<"d">> => hb_util:encode(D),
            <<"p">> => hb_util:encode(P),
            <<"q">> => hb_util:encode(Q),
            <<"dp">> => hb_util:encode(DP),
            <<"dq">> => hb_util:encode(DQ)
        },
    remember_jwk_crt_params(N, Key, #{}),
    ?assertEqual(not_found, crt_params(N)).

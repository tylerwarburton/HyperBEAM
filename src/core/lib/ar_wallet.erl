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
-define(CRT_OWNER, ar_wallet_crt_owner).
-define(CRT_TABLE_LIMIT, 64).
-define(CRT_IMPORTED_LIMIT, 32).

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
    remember_crt_params(Pub, Priv, {P1, P2, E1, E2, C}, local),
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
                remember_crt_params(Pb, Prv, {P1, P2, E1, E2, C}, local),
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
    from_json(Body, Opts, local).

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

%% @doc Parse a wallet from JSON (JWK) format with options. The key may have
%% been supplied by a caller, so it shares a smaller part of the CRT parameter
%% table than keys the node generates or loads from its own keyfiles.
from_json(JsonBinary, Opts) ->
    from_json(JsonBinary, Opts, imported).

from_json(JsonBinary, Opts, Origin) ->
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
                remember_jwk_crt_params(Pb, Prv, Key, Origin, Opts),
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
    Modulus = binary:decode_unsigned(Pub),
    PrivExpnt = binary:decode_unsigned(Priv),
    Base =
        #'RSAPrivateKey'{
            publicExponent = PublicExpnt,
            modulus = Modulus,
            privateExponent = PrivExpnt
        },
    case crt_params(Modulus, PrivExpnt) of
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

%% @doc Record the Chinese Remainder Theorem parameters of an RSA key, given as
%% the binaries that `crypto' and JWK fields both provide. These are secret key
%% material: they stay in memory for the lifetime of the node and must never
%% reach the AO-Core store, so they are held beside it rather than in it.
%%
%% `from_json/2' also parses keys supplied by callers, so nothing here may let
%% one key's entry influence another's signing. Entries are found by the
%% modulus together with the private exponent, which only a holder of the key
%% knows; the table owner refuses any set that is not exactly the key's own
%% (see `valid_crt_params/3') and never replaces an entry. `Origin' is `local'
%% for keys the node generates or reads from its keyfiles and `imported' for
%% keys parsed from caller JSON, which may hold at most half of the table, so a
%% flood of valid imported keys cannot crowd out the node's own.
remember_crt_params(Pub, Priv, {P, Q, DP, DQ, QInv}, Origin) ->
    Params =
        list_to_tuple(
            [ crypto:bytes_to_integer(Param) || Param <- [P, Q, DP, DQ, QInv] ]
        ),
    Modulus = binary:decode_unsigned(Pub),
    PrivExpnt = binary:decode_unsigned(Priv),
    case valid_crt_params(Modulus, PrivExpnt, Params) of
        true -> call_crt_owner({remember, Modulus, PrivExpnt, Params, Origin});
        false -> ok
    end.

%% @doc Record the Chinese Remainder Theorem parameters of a JWK RSA private
%% key, if it carries the complete set that RFC 7518 section 6.3.2 defines. A
%% key that omits any of them signs through the full-modulus path.
remember_jwk_crt_params(Pub, Priv, Key, Origin, Opts) ->
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
                Priv,
                list_to_tuple([ hb_util:decode(Param) || Param <- Encoded ]),
                Origin
            )
    end.

%% @doc A set of CRT parameters is accepted only if it is exactly the one that
%% the modulus and private exponent determine: two factors of the modulus, the
%% private exponent reduced by each less one, and the inverse of the second
%% factor modulo the first. Every value is then smaller than the modulus, so
%% no accepted set can make a signature cost more than the full-modulus path.
valid_crt_params(N, D, {P, Q, DP, DQ, QInv}) ->
    is_integer(N) andalso is_integer(D) andalso D > 0
        andalso P > 1 andalso Q > 1
        andalso P * Q =:= N
        andalso DP =:= D rem (P - 1)
        andalso DQ =:= D rem (Q - 1)
        andalso QInv > 0 andalso QInv < P
        andalso (QInv * Q) rem P =:= 1;
valid_crt_params(_, _, _) ->
    false.

%% @doc Look up the Chinese Remainder Theorem parameters held for a key.
%% Returns `not_found' when the key was built without them, or before any key
%% has registered any, in which case signing takes the full-modulus path.
crt_params(Modulus, PrivExpnt) ->
    try ets:lookup(?CRT_TABLE, key_id(Modulus, PrivExpnt)) of
        [{_, Params, _Origin}] -> Params;
        [] -> not_found
    catch
        error:badarg -> not_found
    end.

%% @doc Identify a key by its modulus and private exponent, without holding
%% either as a table key. The modulus is public, so it alone would let anyone
%% address the entry of any key.
key_id(Modulus, PrivExpnt) ->
    NBin = binary:encode_unsigned(Modulus),
    crypto:hash(
        sha256,
        [<<(byte_size(NBin)):32>>, NBin, binary:encode_unsigned(PrivExpnt)]
    ).

%% @doc Send a request to the table owner and wait for its reply. A request
%% that cannot be answered leaves the key on the full-modulus path.
call_crt_owner(Request) ->
    Owner = ensure_crt_owner(),
    Mon = erlang:monitor(process, Owner),
    Owner ! {Request, self(), Mon},
    receive
        {Mon, Reply} ->
            erlang:demonitor(Mon, [flush]),
            Reply;
        {'DOWN', Mon, process, _, _} ->
            ok
    after 5000 ->
        erlang:demonitor(Mon, [flush]),
        ok
    end.

%% @doc The table is `protected': every process may read it, but only its
%% owner writes, and the owner checks each set itself, so no other process can
%% place parameters in it. The owner never exits, because a table dies with its
%% owner and the first caller is usually a short-lived request.
ensure_crt_owner() ->
    case whereis(?CRT_OWNER) of
        undefined ->
            Parent = self(),
            Ref = make_ref(),
            {Owner, Mon} =
                spawn_monitor(
                    fun() ->
                        try register(?CRT_OWNER, self()) of
                            true ->
                                ets:new(?CRT_TABLE,
                                    [named_table, protected, set,
                                        {read_concurrency, true}]),
                                Parent ! {Ref, created},
                                crt_owner_loop(0)
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
            case whereis(?CRT_OWNER) of
                undefined -> Owner;
                Registered -> Registered
            end;
        Owner -> Owner
    end.

%% @doc Serve writes to the table, counting the entries that came from
%% imported keys.
crt_owner_loop(Imported) ->
    receive
        {{remember, N, D, Params, Origin}, From, Ref} ->
            {Reply, NewImported} = crt_owner_remember(N, D, Params, Origin, Imported),
            From ! {Ref, Reply},
            crt_owner_loop(NewImported);
        _ ->
            crt_owner_loop(Imported)
    end.

crt_owner_remember(N, D, Params, Origin, Imported) ->
    HasRoom =
        ets:info(?CRT_TABLE, size) < ?CRT_TABLE_LIMIT andalso
            (Origin =:= local orelse Imported < ?CRT_IMPORTED_LIMIT),
    case HasRoom andalso valid_crt_params(N, D, Params) of
        false ->
            {ok, Imported};
        true ->
            Entry = {key_id(N, D), Params, Origin},
            case ets:insert_new(?CRT_TABLE, Entry) of
                true when Origin =:= imported -> {ok, Imported + 1};
                _ -> {ok, Imported}
            end
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
    remember_crt_params(N, D, {P, Q, DP, DQ, QInv}, local),
    {{{KeyType, D, N}, {KeyType, N}}, {P, Q, DP, DQ, QInv}}.

%% @doc A JWK for an RSA key, with the given CRT fields beside `n' and `d'.
test_jwk(N, D, CRT) ->
    hb_json:encode(
        maps:merge(
            #{
                <<"kty">> => <<"RSA">>,
                <<"e">> => <<"AQAB">>,
                <<"n">> => hb_util:encode(N),
                <<"d">> => hb_util:encode(D)
            },
            maps:map(fun(_, V) -> hb_util:encode(V) end, CRT)
        )
    ).

%% @doc A freshly generated RSA key as a complete JWK, with its raw parts.
full_test_jwk(Bits) ->
    {[_, N], [_, N, D, P, Q, DP, DQ, QInv]} =
        crypto:generate_key(rsa, {Bits, 65537}),
    CRT =
        #{
            <<"p">> => P, <<"q">> => Q, <<"dp">> => DP,
            <<"dq">> => DQ, <<"qi">> => QInv
        },
    {test_jwk(N, D, CRT), N, D, CRT}.

%% @doc The median time, in microseconds, of `Count' runs of `Fun'.
median_us(Fun, Count) ->
    Times = [ element(1, timer:tc(Fun)) || _ <- lists:seq(1, Count) ],
    lists:nth((Count + 1) div 2, lists:sort(Times)).

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
%% exponent rejects, so `rsa_pss' falls back to the full modulus, still
%% correct. The table refuses such a set, so the record is built directly.
crt_fallback_on_bad_parameter_test() ->
    {[_, N], [_, N, D, P, Q, DP, DQ, QInv]} =
        crypto:generate_key(rsa, {2048, 65537}),
    I = fun crypto:bytes_to_integer/1,
    Key =
        #'RSAPrivateKey'{
            publicExponent = 65537,
            modulus = I(N),
            privateExponent = I(D),
            prime1 = I(P),
            prime2 = I(Q),
            exponent1 = I(DQ),
            exponent2 = I(DP),
            coefficient = I(QInv)
        },
    Data = crypto:strong_rand_bytes(256),
    ?assert(verify({{?RSA_SIGN_ALG, 65537}, N}, Data,
        rsa_pss:sign(Data, sha256, Key))).

%% @doc Primes that do not multiply to the modulus belong to a different key and
%% are refused, leaving that key to sign through the full modulus.
mismatched_primes_refused_test() ->
    {_, {P, Q, DP, DQ, QInv}} = generate_test_key(),
    {[_, OtherN], [_, OtherN, OtherD | _]} =
        crypto:generate_key(rsa, {2048, 65537}),
    remember_crt_params(OtherN, OtherD, {P, Q, DP, DQ, QInv}, local),
    ?assertEqual(
        not_found,
        crt_params(binary:decode_unsigned(OtherN), binary:decode_unsigned(OtherD))
    ).

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
    remember_jwk_crt_params(N, D, Key, imported, #{}),
    ?assertEqual(
        not_found,
        crt_params(binary:decode_unsigned(N), binary:decode_unsigned(D))
    ).

%% @doc Every field of a CRT set is checked against the key: a set that differs
%% from the key's own in any one of them is refused, even though the factors
%% still multiply to the modulus.
inconsistent_crt_params_refused_test() ->
    {_, N, D, #{<<"p">> := P, <<"q">> := Q, <<"dp">> := DP, <<"dq">> := DQ,
        <<"qi">> := QInv}} = full_test_jwk(2048),
    One = <<1>>,
    Bad =
        [
            {DQ, DP, QInv},
            {DP, DQ, One},
            {DP, crypto:strong_rand_bytes(300), QInv},
            {<<(crypto:bytes_to_integer(DP) + crypto:bytes_to_integer(P) - 1):2048>>,
                DQ, QInv}
        ],
    lists:foreach(
        fun({BDP, BDQ, BQI}) ->
            remember_crt_params(N, D, {P, Q, BDP, BDQ, BQI}, local),
            ?assertEqual(
                not_found,
                crt_params(binary:decode_unsigned(N), binary:decode_unsigned(D))
            )
        end,
        Bad
    ),
    remember_crt_params(N, D, {One, N, One, One, One}, local),
    ?assertEqual(
        not_found,
        crt_params(binary:decode_unsigned(N), binary:decode_unsigned(D))
    ).

%% @doc A caller-supplied JWK naming another key's modulus cannot change how
%% that key signs. The hostile set (factors 1 and the modulus, a huge second
%% exponent) used to replace the key's entry, so that every later signature ran
%% a long exponentiation, failed its check and was then computed again.
hostile_jwk_cannot_replace_crt_params_test() ->
    {JWK, N, _D, _} = full_test_jwk(2048),
    {Priv, Pub} = from_json(JWK),
    SignVerify =
        fun() ->
            Sig = sign(Priv, <<"x">>),
            true = verify(Pub, <<"x">>, Sig)
        end,
    Before = median_us(SignVerify, 9),
    Hostile =
        test_jwk(N, <<1>>,
            #{
                <<"p">> => <<1>>, <<"q">> => N, <<"dp">> => <<1>>,
                <<"dq">> => crypto:strong_rand_bytes(3000), <<"qi">> => <<1>>
            }
        ),
    _ = from_json(Hostile),
    After = median_us(SignVerify, 9),
    ?assert(After < 3 * Before + 2000, {before_us, Before, after_us, After}).

%% @doc Only the table's owner can write to it.
crt_table_not_writable_by_callers_test() ->
    {JWK, _, _, _} = full_test_jwk(2048),
    _ = from_json(JWK),
    ?assertError(badarg, ets:insert(?CRT_TABLE, {<<"id">>, {1, 1, 1, 1, 1}})).

%% @doc Imported keys can fill at most half of the table, leaving the rest for
%% the keys the node generates or loads from its own keyfiles.
imported_keys_bounded_test_() ->
    {timeout, 120, fun() ->
        case ets:whereis(?CRT_TABLE) of
            undefined -> ok;
            _ ->
                Owner = ets:info(?CRT_TABLE, owner),
                Mon = erlang:monitor(process, Owner),
                exit(Owner, kill),
                receive {'DOWN', Mon, process, Owner, _} -> ok end
        end,
        lists:foreach(
            fun(_) -> _ = from_json(element(1, full_test_jwk(1024))) end,
            lists:seq(1, ?CRT_TABLE_LIMIT)
        ),
        % Other processes (in a full suite, nodes left by earlier tests) may
        % add their own keys meanwhile, so only the imported entries count.
        ?assert(
            ets:select_count(?CRT_TABLE, [{{'_', '_', imported}, [], [true]}])
                =< ?CRT_TABLE_LIMIT div 2
        ),
        {{{_, D, N}, _}, _} = generate_test_key(),
        ?assertNotEqual(
            not_found,
            crt_params(binary:decode_unsigned(N), binary:decode_unsigned(D))
        )
    end}.

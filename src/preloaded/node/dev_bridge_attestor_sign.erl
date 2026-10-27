%%% @doc The PUSD bridge's chain signatures, made inside the node. A helper of
%%% `bridge-attestor@1.0' (formerly `hb_bridge_sign' on the old fork).
%%%
%%% An attestor node signs two things on each chain: a release of one
%%% withdrawal, and a replacement attestor set. The byte layouts are fixed by
%%% the chain contracts (paralith-mono products/bridge/BRIDGE_NATIVE.md):
%%%
%%%   Base    EIP-191 personal-sign over keccak256(abi.encode(TYPE, chainid,
%%%           bridge, ...)), returned as r || s || v with v in {27, 28}.
%%%   Solana  ed25519 over D || program id || ...
%%%   Sui     ed25519 over D || bridge object id || ...
%%%
%%% Pure functions: no node state, no network.
-module(dev_bridge_attestor_sign).
-export([base_release/6, base_attestors/4, base_sign/2, base_address/1]).
-export([solana_release/5, solana_attestors/3]).
-export([sui_release/5, sui_attestors/3]).
-export([ed25519_sign/2, ed25519_public/1, threshold/1, address/2]).
-export([base58_encode/1, base58_decode/1, hex/1, unhex/1]).
-include_lib("eunit/include/eunit.hrl").

-define(RELEASE, <<"PARALITH_RELEASE_V1">>).
-define(ATTESTORS, <<"PARALITH_ATTESTORS_V1">>).

%% @doc k = ceil(60% of n), as every side computes it.
threshold(N) -> (3 * N + 4) div 5.

%%% Base ----------------------------------------------------------------------

%% @doc The inner hash `ParaBridge.releaseDigest` wraps in the EIP-191 prefix.
base_release(ChainId, Bridge, WithdrawalId, Token, To, Amount) ->
    hb_keccak:keccak_256(<<
        (hb_keccak:keccak_256(?RELEASE))/binary,
        (word(ChainId))/binary, (evm(Bridge))/binary, (word(WithdrawalId))/binary,
        (evm(Token))/binary, (evm(To))/binary, (word(Amount))/binary
    >>).

%% @doc The inner hash of `ParaBridge.attestorsDigest`: the set is the one
%% dynamic argument, so its head slot holds the offset of its tail (5 words).
base_attestors(ChainId, Bridge, Epoch, Set) ->
    Tail = << <<(evm(A))/binary>> || A <- Set >>,
    hb_keccak:keccak_256(<<
        (hb_keccak:keccak_256(?ATTESTORS))/binary,
        (word(ChainId))/binary, (evm(Bridge))/binary, (word(Epoch))/binary,
        (word(5 * 32))/binary, (word(length(Set)))/binary, Tail/binary
    >>).

%% @doc Personal-sign `Inner` with a 32-byte private key: what `ECDSA.recover`
%% on the EIP-191 hash accepts.
base_sign(Inner, Priv) ->
    <<RS:64/binary, V:8>> = secp256k1_nif:sign(Inner, Priv, ethereum),
    <<RS/binary, (V + 27):8>>.

%% @doc The 0x address of a 32-byte private key, lowercase.
base_address(Priv) ->
    {<<4, Pub:64/binary>>, _} = crypto:generate_key(ecdh, secp256k1, Priv),
    <<_:12/binary, Addr:20/binary>> = hb_keccak:keccak_256(Pub),
    <<"0x", (hex(Addr))/binary>>.

word(N) when is_integer(N) -> <<N:256>>;
word(N) when is_binary(N) -> word(binary_to_integer(N)).

%% An address as an ABI word.
evm(Text) -> <<0:96, (address(base, Text))/binary>>.

%%% Solana --------------------------------------------------------------------

%% @doc `D || program id || withdrawal_id u64le || mint || recipient || amount u64le`
solana_release(Program, WithdrawalId, Mint, Recipient, Amount) ->
    <<?RELEASE/binary, (address(solana, Program))/binary, (int(WithdrawalId)):64/little,
      (address(solana, Mint))/binary, (address(solana, Recipient))/binary, (int(Amount)):64/little>>.

%% @doc `D || program id || epoch u64le || n u8 || pubkey * n`
solana_attestors(Program, Epoch, Set) ->
    <<?ATTESTORS/binary, (address(solana, Program))/binary, (int(Epoch)):64/little,
      (length(Set)):8, << <<(address(solana, K))/binary>> || K <- Set >>/binary>>.

%%% Sui -----------------------------------------------------------------------

%% @doc `D || bridge object id || withdrawal_id u64le || coin type || to || amount u64le`
sui_release(BridgeId, WithdrawalId, CoinType, To, Amount) ->
    <<?RELEASE/binary, (address(sui, BridgeId))/binary, (int(WithdrawalId)):64/little,
      CoinType/binary, (address(sui, To))/binary, (int(Amount)):64/little>>.

%% @doc `D || bridge object id || epoch u64le || n u8 || pubkey * n`
sui_attestors(BridgeId, Epoch, Set) ->
    <<?ATTESTORS/binary, (address(sui, BridgeId))/binary, (int(Epoch)):64/little,
      (length(Set)):8, << <<(address(sui, K))/binary>> || K <- Set >>/binary>>.

%%% Addresses -----------------------------------------------------------------

%% @doc An address's bytes, read in its chain's one text form, the rule PUSD's
%% `ADDRESS' table applies before it burns:
%%
%%   base    `0x' and 40 hex, not all zero        20 bytes
%%   solana  base58, 33 to 44 characters          exactly 32 bytes
%%   sui     `0x' and 64 hex                      32 bytes
%%
%% Anything else raises: nothing is read by its length alone, so 32 base58
%% characters are never taken for 32 raw bytes. The signed bytes of every
%% address in these forms are what they always were.
address(base, <<"0x", Hex:40/binary>> = Text) when Hex =/= <<"0000000000000000000000000000000000000000">> ->
    from_hex(base, Text, Hex);
address(solana, Text) when is_binary(Text), byte_size(Text) > 32, byte_size(Text) =< 44 ->
    case catch base58_decode(Text) of
        <<Key:32/binary>> -> Key;
        _ -> erlang:error({bad_address, solana, Text})
    end;
address(sui, <<"0x", Hex:64/binary>> = Text) ->
    from_hex(sui, Text, Hex);
address(Chain, Text) ->
    erlang:error({bad_address, Chain, Text}).

from_hex(Chain, Text, Hex) ->
    case catch unhex(Hex) of
        Bytes when is_binary(Bytes) -> Bytes;
        _ -> erlang:error({bad_address, Chain, Text})
    end.

%%% ed25519 -------------------------------------------------------------------

ed25519_sign(Msg, Seed) -> crypto:sign(eddsa, none, Msg, [Seed, ed25519]).

ed25519_public(Seed) ->
    {Pub, _} = crypto:generate_key(eddsa, ed25519, Seed),
    Pub.

%%% Encoding ------------------------------------------------------------------

int(N) when is_integer(N) -> N;
int(N) when is_binary(N) -> binary_to_integer(N).

hex(Bin) -> list_to_binary([io_lib:format("~2.16.0b", [B]) || <<B>> <= Bin]).

unhex(Hex) -> binary:decode_hex(Hex).

-define(B58, <<"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz">>).

base58_encode(Bin) -> hb_util:base58_encode(Bin).

%% @doc Base58 to bytes; each leading '1' is a leading zero byte.
base58_decode(Text) ->
    {Ones, Rest} = lists:splitwith(fun(C) -> C =:= $1 end, binary_to_list(Text)),
    N = lists:foldl(
        fun(C, Acc) ->
            case binary:match(?B58, <<C>>) of
                {I, 1} -> Acc * 58 + I;
                nomatch -> error({bad_base58, Text})
            end
        end, 0, Rest),
    Body = case N of 0 -> <<>>; _ -> binary:encode_unsigned(N) end,
    <<(binary:copy(<<0>>, length(Ones)))/binary, Body/binary>>.

%%% Tests: vectors from viem and node:crypto (paralith-mono, 2026-10-10) -----

-define(PRIV, binary:copy(<<1>>, 32)).
-define(BRIDGE, <<"0x1111111111111111111111111111111111111111">>).
-define(TOKEN, <<"0x2222222222222222222222222222222222222222">>).
-define(TO, <<"0x3333333333333333333333333333333333333333">>).

threshold_test() ->
    ?assertEqual([1, 2, 2, 3, 3, 4, 5, 6, 10], [threshold(N) || N <- [1, 2, 3, 4, 5, 6, 7, 10, 16]]).

base_address_test() ->
    ?assertEqual(<<"0x1a642f0e3c3af545e7acbd38b07251b3990914f1">>, base_address(?PRIV)).

base_release_test() ->
    Inner = base_release(8453, ?BRIDGE, (5 bsl 32) bor 7, ?TOKEN, ?TO, 1234567),
    ?assertEqual(<<"a61906f5074094ea205dc029fd44476ccb6058b48b7e160334aa954a7e9f6870">>, hex(Inner)),
    ?assertEqual(
        <<"78338d741889a13ef8a5523422e3a9827f958407e44e028613ec426bb6ea4a00",
          "2ac282b54427fc3c6b5b2e80ec6cd10288c4bf425443d5f163e6b343eaca773d1c">>,
        hex(base_sign(Inner, ?PRIV))).

base_attestors_test() ->
    Set = [base_address(?PRIV), ?TO],
    Inner = base_attestors(8453, ?BRIDGE, 1, Set),
    ?assertEqual(<<"6874bf901ad3e2cadd760dee6bc512feade39e4146384817eb1e094e2c187139">>, hex(Inner)),
    ?assertEqual(
        <<"66f0af18ec915812a789c8b5c857420abb7bea4083fd04309092202189ad578a",
          "545e11b3b556b7db0d0d94214c7bb6b57b51fed7fb4e478b2c2cb5443a09862c1b">>,
        hex(base_sign(Inner, ?PRIV))).

ed25519_test() ->
    Seed = ?PRIV,
    ?assertEqual(<<"8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c">>,
        hex(ed25519_public(Seed))),
    ?assertEqual(
        <<"d80d2d6a620402304607a5508a3a17669d6be877bfd79fa3be4dee9f8882d4a0",
          "1b2b7900118f5a692b5655dad4e0e342852b3ef5ccda821f99263356f5fb8903">>,
        hex(ed25519_sign(<<"abc">>, Seed))).

%% The Sui message the Move tests sign (para_bridge_tests print_signing_inputs).
sui_release_test() ->
    Bridge = <<"0x034401905bebdf8c04f3cd5f04f442a39372c8dc321c29edfb4f9cb30b23ab96">>,
    CoinType = <<"0x0000000000000000000000000000000000000000000000000000000000000000::test_usdc::TEST_USDC">>,
    ?assertEqual(
        <<"504152414c4954485f52454c454153455f5631034401905bebdf8c04f3cd5f04f442a39372c8dc321c29edfb4f9cb30b23ab96",
          "0100000000000000",
          "3078303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303a3a746573745f757364633a3a544553545f55534443",
          "000000000000000000000000000000000000000000000000000000000000000d0100000000000000">>,
        hex(sui_release(Bridge, 1, CoinType,
            <<"0x000000000000000000000000000000000000000000000000000000000000000d">>, 1))).

base58_test() ->
    Key = <<"9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin">>,
    ?assertEqual(32, byte_size(base58_decode(Key))),
    ?assertEqual(Key, base58_encode(base58_decode(Key))),
    ?assertEqual(binary:copy(<<0>>, 32), base58_decode(binary:copy(<<"1">>, 32))).

solana_release_test() ->
    B58 = fun(Byte) -> base58_encode(binary:copy(<<Byte>>, 32)) end,
    Msg = solana_release(B58(1), 9, B58(2), B58(3), 5),
    ?assertEqual(<<?RELEASE/binary, (binary:copy(<<1>>, 32))/binary, 9:64/little,
        (binary:copy(<<2>>, 32))/binary, (binary:copy(<<3>>, 32))/binary, 5:64/little>>, Msg),
    ?assertEqual(byte_size(?RELEASE) + 32 + 8 + 32 + 32 + 8, byte_size(Msg)),
    ?assertMatch(<<_:19/binary, _:32/binary, 9:64/little, _/binary>>, Msg).

%% Each chain's one form, and nothing else: a 32-character base58 text (32
%% '1's) is not 32 raw bytes, and a short or raw form is refused, not guessed.
address_forms_test() ->
    Bad = fun(Chain, Text) -> ?assertError({bad_address, Chain, Text}, address(Chain, Text)) end,
    Bad(solana, binary:copy(<<"1">>, 32)),
    Bad(solana, binary:copy(<<1>>, 32)),
    Bad(solana, <<"0OIl", (binary:copy(<<"1">>, 40))/binary>>),
    Bad(solana, binary:copy(<<"z">>, 44)),
    Bad(solana, binary:copy(<<"1">>, 33)),
    Bad(base, binary:copy(<<1>>, 20)),
    Bad(base, <<"0x0000000000000000000000000000000000000000">>),
    Bad(base, <<"0xzz22222222222222222222222222222222222222">>),
    Bad(sui, <<"0xd">>),
    Bad(sui, binary:copy(<<1>>, 32)),
    ?assertEqual(binary:copy(<<16#22>>, 20), address(base, ?TOKEN)),
    ?assertEqual(<<0:248, 13>>,
        address(sui, <<"0x000000000000000000000000000000000000000000000000000000000000000d">>)),
    Key = <<"9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin">>,
    ?assertEqual(base58_decode(Key), address(solana, Key)),
    % A release to a malformed address is never signed.
    ?assertError({bad_address, solana, _},
        solana_release(Key, 1, Key, binary:copy(<<"1">>, 32), 1)).

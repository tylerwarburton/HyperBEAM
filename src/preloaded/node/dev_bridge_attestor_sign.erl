%%% @doc The PUSD bridge's chain signatures, made inside the node. A helper of
%%% `bridge-attestor@1.0' (formerly `hb_bridge_sign' on the old fork).
%%%
%%% An attestor node signs releases, replacement attestor sets, canonical root
%%% successors and majority-controlled chain administration.
%%% The byte layouts are fixed by the chain contracts
%%% (paralith-mono products/bridge/BRIDGE_NATIVE.md):
%%%
%%%   Base    EIP-191 personal-sign over keccak256(abi.encode(TYPE, chainid,
%%%           bridge, ...)), returned as r || s || v with v in {27, 28}.
%%%   Solana  ed25519 over D || program id || ...
%%%   Sui     ed25519 over D || bridge object id || ...
%%%
%%% Pure functions: no node state, no network.
-module(dev_bridge_attestor_sign).
-export([base_release/8, base_attestors/6, base_root/5, base_admin/7,
    base_sign/2, base_address/1]).
-export([solana_release/7, solana_attestors/5, solana_root/4, solana_admin/6]).
-export([sui_release/7, sui_attestors/5, sui_root/4, sui_admin/6]).
-export([ed25519_sign/2, ed25519_public/1, threshold/1, address/2]).
-export([base58_encode/1, base58_decode/1, hex/1, unhex/1]).
-include_lib("eunit/include/eunit.hrl").

-define(RELEASE, <<"PARALITH_RELEASE_V2">>).
-define(ATTESTORS, <<"PARALITH_ATTESTORS_V2">>).
-define(ROOT, <<"PARALITH_ROOT_V1">>).
-define(ADMIN, <<"PARALITH_ADMIN_V1">>).
-define(ALLOW_TOKEN, <<"ALLOW_TOKEN">>).
-define(UNPAUSE, <<"UNPAUSE">>).

%% @doc A strict majority, as every side computes it.
threshold(N) -> N div 2 + 1.

%%% Base ----------------------------------------------------------------------

%% @doc The inner hash `ParaBridge.releaseDigest` wraps in the EIP-191 prefix.
base_release(ChainId, Bridge, Root, Epoch, WithdrawalId, Token, To, Amount) ->
    hb_keccak:keccak_256(<<
        (hb_keccak:keccak_256(?RELEASE))/binary,
        (word(ChainId))/binary, (evm(Bridge))/binary, (ao_id(Root))/binary,
        (word(Epoch))/binary,
        (word(WithdrawalId))/binary, (evm(Token))/binary,
        (evm(To))/binary, (word(Amount))/binary
    >>).

%% @doc The inner hash of `ParaBridge.attestorsDigest`: the set is the one
%% dynamic argument, so its head slot holds the offset of its tail (7 words).
base_attestors(ChainId, Bridge, Root, CurrentEpoch, NewEpoch, Set) ->
    Tail = << <<(evm(A))/binary>> || A <- Set >>,
    hb_keccak:keccak_256(<<
        (hb_keccak:keccak_256(?ATTESTORS))/binary,
        (word(ChainId))/binary, (evm(Bridge))/binary, (ao_id(Root))/binary,
        (word(CurrentEpoch))/binary, (word(NewEpoch))/binary,
        (word(7 * 32))/binary, (word(length(Set)))/binary, Tail/binary
    >>).

%% @doc The inner hash of `ParaBridge.rootDigest`.
base_root(ChainId, Bridge, Epoch, Root, Successor) ->
    hb_keccak:keccak_256(<<
        (hb_keccak:keccak_256(?ROOT))/binary,
        (word(ChainId))/binary, (evm(Bridge))/binary, (word(Epoch))/binary,
        (ao_id(Root))/binary, (ao_id(Successor))/binary
    >>).

%% @doc The inner hash of `ParaBridge.adminDigest'.
base_admin(ChainId, Bridge, Root, Epoch, Action, Token, Value) ->
    hb_keccak:keccak_256(<<
        (hb_keccak:keccak_256(?ADMIN))/binary,
        (word(ChainId))/binary, (evm(Bridge))/binary, (ao_id(Root))/binary,
        (word(Epoch))/binary, (base_admin_action(Action))/binary,
        (base_admin_token(Action, Token))/binary, (word(boolean(Value)))/binary
    >>).

base_admin_token(2, _) -> <<0:256>>;
base_admin_token(1, Token) -> evm(Token).

base_admin_action(1) -> hb_keccak:keccak_256(?ALLOW_TOKEN);
base_admin_action(2) -> hb_keccak:keccak_256(?UNPAUSE);
base_admin_action(Action) -> error({bad_admin_action, Action}).

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

%% @doc `D || program id || root || epoch u64le || withdrawal_id u64le || mint || recipient || amount u64le`
solana_release(Program, Root, Epoch, WithdrawalId, Mint, Recipient, Amount) ->
    <<?RELEASE/binary, (address(solana, Program))/binary, (ao_id(Root))/binary,
      (int(Epoch)):64/little,
      (int(WithdrawalId)):64/little,
      (address(solana, Mint))/binary, (address(solana, Recipient))/binary, (int(Amount)):64/little>>.

%% @doc `D || program id || root || current epoch u64le || new epoch u64le || n u8 || pubkey * n`
solana_attestors(Program, Root, CurrentEpoch, NewEpoch, Set) ->
    <<?ATTESTORS/binary, (address(solana, Program))/binary, (ao_id(Root))/binary,
      (int(CurrentEpoch)):64/little,
      (int(NewEpoch)):64/little,
      (length(Set)):8, << <<(address(solana, K))/binary>> || K <- Set >>/binary>>.

%% @doc `D || program id || epoch u64le || old root || new root`
solana_root(Program, Epoch, Root, Successor) ->
    <<?ROOT/binary, (address(solana, Program))/binary, (int(Epoch)):64/little,
      (ao_id(Root))/binary, (ao_id(Successor))/binary>>.

%% @doc A Solana `PARALITH_ADMIN_V1' message.
solana_admin(Program, Root, Epoch, Action, Token, Value) ->
    <<?ADMIN/binary, (address(solana, Program))/binary, (ao_id(Root))/binary,
      (int(Epoch)):64/little, (admin_action(Action)):8,
      (solana_admin_token(Action, Token))/binary, (boolean(Value)):8>>.

solana_admin_token(2, _) -> <<0:256>>;
solana_admin_token(1, Token) -> address(solana, Token).

%%% Sui -----------------------------------------------------------------------

%% @doc `D || bridge object id || root || epoch u64le || withdrawal_id u64le || coin type || to || amount u64le`
sui_release(BridgeId, Root, Epoch, WithdrawalId, CoinType, To, Amount) ->
    <<?RELEASE/binary, (address(sui, BridgeId))/binary, (ao_id(Root))/binary,
      (int(Epoch)):64/little,
      (int(WithdrawalId)):64/little,
      CoinType/binary, (address(sui, To))/binary, (int(Amount)):64/little>>.

%% @doc `D || bridge object id || root || current epoch u64le || new epoch u64le || n u8 || pubkey * n`
sui_attestors(BridgeId, Root, CurrentEpoch, NewEpoch, Set) ->
    <<?ATTESTORS/binary, (address(sui, BridgeId))/binary, (ao_id(Root))/binary,
      (int(CurrentEpoch)):64/little,
      (int(NewEpoch)):64/little,
      (length(Set)):8, << <<(address(sui, K))/binary>> || K <- Set >>/binary>>.

%% @doc `D || bridge object id || epoch u64le || old root || new root`
sui_root(BridgeId, Epoch, Root, Successor) ->
    <<?ROOT/binary, (address(sui, BridgeId))/binary, (int(Epoch)):64/little,
      (ao_id(Root))/binary, (ao_id(Successor))/binary>>.

%% @doc A Sui `PARALITH_ADMIN_V1' message. Token is the canonical coin type
%% string, and is empty for actions that do not name a coin.
sui_admin(BridgeId, Root, Epoch, Action, Token, Value) when is_binary(Token) ->
    <<?ADMIN/binary, (address(sui, BridgeId))/binary, (ao_id(Root))/binary,
      (int(Epoch)):64/little, (admin_action(Action)):8,
      Token/binary, (boolean(Value)):8>>.

admin_action(1) -> 1;
admin_action(2) -> 2;
admin_action(Action) -> error({bad_admin_action, Action}).

boolean(true) -> 1;
boolean(false) -> 0;
boolean(Value) -> error({bad_boolean, Value}).

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

ao_id(Text) when is_binary(Text), byte_size(Text) =:= 43 ->
    case hb_util:safe_decode(Text) of
        {ok, <<Raw:32/binary>>} -> Raw;
        _ -> error({bad_ao_id, Text})
    end;
ao_id(Text) -> error({bad_ao_id, Text}).

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
-define(ROOT_ID, <<"qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo">>).
-define(SUCCESSOR, <<"u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7s">>).

threshold_test() ->
    ?assertEqual([1, 2, 2, 3, 3, 4, 4, 6, 9], [threshold(N) || N <- [1, 2, 3, 4, 5, 6, 7, 10, 16]]).

base_address_test() ->
    ?assertEqual(<<"0x1a642f0e3c3af545e7acbd38b07251b3990914f1">>, base_address(?PRIV)).

base_release_test() ->
    Inner = base_release(8453, ?BRIDGE, ?ROOT_ID, 3,
        (5 bsl 32) bor 7, ?TOKEN, ?TO, 1234567),
    ?assertEqual(<<"f53e75b115a27fd01694a31d9b0a2067b9602c93d2fc991e961cdae2c62e4891">>, hex(Inner)),
    ?assertEqual(
        <<"7763e686307224e8f0791bd0b59c7ae7bcb4efdb77d903891d2032fc8cb5f129",
          "253a9cc81621497731181da8760a91e117de44990e15dfc8681ce92222ded9861b">>,
        hex(base_sign(Inner, ?PRIV))).

base_attestors_test() ->
    Set = [base_address(?PRIV), ?TO],
    Inner = base_attestors(8453, ?BRIDGE, ?ROOT_ID, 1, 2, Set),
    ?assertEqual(<<"dc9f6fd189650c18be03953cda035dc1592d8c5b579c12e0c7bc9339f5e5bef8">>, hex(Inner)),
    ?assertEqual(
        <<"ec7cf4a21f75ff22718dd8d61cbbc90990e611a66810fc7b81236ef282374a3e",
          "16bd6a1b2a024031bd974dba0f282993b537274344674f61cd1f7e1cfe0dfad31c">>,
        hex(base_sign(Inner, ?PRIV))).

base_root_test() ->
    Inner = base_root(8453, ?BRIDGE, 3, ?ROOT_ID, ?SUCCESSOR),
    ?assertEqual(<<"6fe98d06c406a7351e2fac90761471fb80b03a3e13d4efb6df9ba039b81a4dbe">>, hex(Inner)),
    ?assertEqual(
        <<"6f59587cdbe9d6e1fcfda2520964bcc3703931b24539afb9516a3e3d2df298b9",
          "7e28b5dcf9a848353271698c09638cd051d1ac9154197720bf4550df1986e7531b">>,
        hex(base_sign(Inner, ?PRIV))).

base_admin_test() ->
    Allow = base_admin(8453, ?BRIDGE, ?ROOT_ID, 4, 1, ?TOKEN, true),
    ?assertEqual(
        <<"983a0bddf7524c157f5d2bb6cdaf30eae1baa4d03ab4c00ca0590fbe6108147c">>,
        hex(Allow)),
    ?assertEqual(
        <<"6e34706165a3a9170a0d54b1f5ed5600e516cc2e98c111ac2664d75b46ddbb44",
          "0c48762fee0aee83ca6b4d6344230e8bf4694ea6601dc90d33db19f61d9e1a561c">>,
        hex(base_sign(Allow, ?PRIV))),
    Unpause = base_admin(8453, ?BRIDGE, ?ROOT_ID, 4, 2,
        <<"0x0000000000000000000000000000000000000000">>, true),
    ?assertEqual(
        <<"079f8a13a555549ca5589e6dae7c3f195076c90556b26021d9f11c17e73fe2bf">>,
        hex(Unpause)).

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
        <<"504152414c4954485f52454c454153455f5632034401905bebdf8c04f3cd5f04f442a39372c8dc321c29edfb4f9cb30b23ab96",
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "0000000000000000",
          "0100000000000000",
          "3078303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303a3a746573745f757364633a3a544553545f55534443",
          "000000000000000000000000000000000000000000000000000000000000000d0100000000000000">>,
        hex(sui_release(Bridge, ?ROOT_ID, 0, 1, CoinType,
            <<"0x000000000000000000000000000000000000000000000000000000000000000d">>, 1))).

base58_test() ->
    Key = <<"9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin">>,
    ?assertEqual(32, byte_size(base58_decode(Key))),
    ?assertEqual(Key, base58_encode(base58_decode(Key))),
    ?assertEqual(binary:copy(<<0>>, 32), base58_decode(binary:copy(<<"1">>, 32))).

solana_release_test() ->
    B58 = fun(Byte) -> base58_encode(binary:copy(<<Byte>>, 32)) end,
    Msg = solana_release(B58(1), ?ROOT_ID, 4, 9, B58(2), B58(3), 5),
    ?assertEqual(<<?RELEASE/binary, (binary:copy(<<1>>, 32))/binary,
        (binary:copy(<<16#aa>>, 32))/binary, 4:64/little, 9:64/little,
        (binary:copy(<<2>>, 32))/binary, (binary:copy(<<3>>, 32))/binary, 5:64/little>>, Msg),
    ?assertEqual(byte_size(?RELEASE) + 64 + 16 + 32 + 32 + 8, byte_size(Msg)),
    ?assertMatch(<<_:19/binary, _:32/binary, _:32/binary,
        4:64/little, 9:64/little, _/binary>>, Msg),
    ?assertEqual(<<?ROOT/binary, (binary:copy(<<1>>, 32))/binary, 4:64/little,
        (binary:copy(<<16#aa>>, 32))/binary, (binary:copy(<<16#bb>>, 32))/binary>>,
        solana_root(B58(1), 4, ?ROOT_ID, ?SUCCESSOR)).

admin_messages_test() ->
    B58 = fun(Byte) -> base58_encode(binary:copy(<<Byte>>, 32)) end,
    ?assertEqual(
        <<"504152414c4954485f41444d494e5f563101010101010101010101010101010101",
          "01010101010101010101010101010101aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0400000000000000010202020202020202",
          "020202020202020202020202020202020202020202020202020202020202020201">>,
        hex(solana_admin(B58(1), ?ROOT_ID, 4, 1, B58(2), true))),
    Bridge = <<"0x034401905bebdf8c04f3cd5f04f442a39372c8dc321c29edfb4f9cb30b23ab96">>,
    Coin = <<"0x0000000000000000000000000000000000000000000000000000000000000000::test_usdc::TEST_USDC">>,
    ?assertEqual(
        <<"504152414c4954485f41444d494e5f5631034401905bebdf8c04f3cd5f04f442a",
          "39372c8dc321c29edfb4f9cb30b23ab96aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0400000000000000013078303030303030",
          "3030303030303030303030303030303030303030303030303030303030303030",
          "3030303030303030303030303030303030303030303030303030303030303030",
          "3030303030303030303a3a746573745f757364633a3a544553545f5553444301">>,
        hex(sui_admin(Bridge, ?ROOT_ID, 4, 1, Coin, true))),
    ?assertEqual(
        <<"504152414c4954485f41444d494e5f5631034401905bebdf8c04f3cd5f04f442a",
          "39372c8dc321c29edfb4f9cb30b23ab96aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa04000000000000000201">>,
        hex(sui_admin(Bridge, ?ROOT_ID, 4, 2, <<>>, true))).

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
        solana_release(Key, ?ROOT_ID, 0, 1, Key, binary:copy(<<"1">>, 32), 1)).

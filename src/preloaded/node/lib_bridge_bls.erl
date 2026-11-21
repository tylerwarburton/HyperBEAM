%%% @doc The PUSD committee's BLS12-381 domain, shared by `bridge-root@1.0'
%%% (which verifies) and `bridge-attestor@1.0' (which signs). Pure functions.
%%%
%%% Both messages carry their own domain separation tag, distinct from the
%%% RNG/timelock ciphersuite (`BLS_SIG_..._NUL_'), and their own message
%%% prefix, so a signature made for one use verifies in no other:
%%%   certificate  ?DST over PUSD_ROOT_CERTIFICATE_V2 || root || epoch || intent
%%%   possession   ?POP_DST over PUSD_BLS_POSSESSION_V1 || member address || key
-module(lib_bridge_bls).
-export([dst/0, pop_dst/0, signing_bytes/3, possession_bytes/2, native_id/1]).

-define(DOMAIN, <<"PUSD_ROOT_CERTIFICATE_V2">>).
-define(POSSESSION, <<"PUSD_BLS_POSSESSION_V1">>).
-define(DST, <<"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_PUSD_ROOT_">>).
-define(POP_DST, <<"BLS_POP_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_PUSD_ROOT_">>).

dst() -> ?DST.
pop_dst() -> ?POP_DST.

%% @doc What a member endorses: one intent, at one root and one epoch.
signing_bytes(Root, Epoch, IntentID) when is_integer(Epoch), Epoch >= 0, Epoch < 18446744073709551616 ->
    <<?DOMAIN/binary, (native_id(Root))/binary, Epoch:64/big, (native_id(IntentID))/binary>>.

%% @doc What a member signs to prove it holds its BLS key.
possession_bytes(AO, Key) when byte_size(Key) =:= 48 ->
    <<?POSSESSION/binary, (native_id(AO))/binary, Key/binary>>.

%% @doc Reject noncanonical IDs, including alternate encodings of padding bits.
native_id(ID) when is_binary(ID), byte_size(ID) =:= 43 ->
    Native = hb_util:decode(ID),
    32 = byte_size(Native),
    ID = hb_util:human_id(Native),
    Native.

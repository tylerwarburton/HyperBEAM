%%% @doc BLS12-381 signature verification over the `blst' library
%%% (supranational/blst, `native/lib/blst'), through `native/blst_nif'.
%%%
%%% The scheme is the IETF BLS signature "minimal-pubkey-size" variant:
%%% public keys are compressed G1 points (48 bytes), signatures are compressed
%%% G2 points (96 bytes), and messages hash to G2 with the ciphersuite
%%% `BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_'. A threshold group signature
%%% verifies the same way under the group public key.
-module(blst_nif).
-export([verify/3, keygen/1, sign/3, aggregate/1, fast_aggregate_verify/4]).

-on_load(init/0).

%% @doc Load the NIF. If it does not load, neither does this module, and
%% `hb_app' refuses to start the node: a node either has `timelock@1.0' whole
%% or does not run.
init() ->
	PrivDir = code:priv_dir(hb),
	erlang:load_nif(filename:join([PrivDir, "blst_nif"]), 0).

%% @doc Verify `Signature' over `Message' under `PublicKey'. Both points must
%% decode, lie on the curve and in their prime-order subgroup, and the key
%% must not be the identity; otherwise `{error, Reason}' names the failed
%% check. Runs on a dirty CPU scheduler.
-spec verify(binary(), binary(), binary()) -> boolean() | {error, atom()}.
verify(_PublicKey, _Message, _Signature) ->
	erlang:nif_error(nif_not_loaded).

%% @doc A secret key (32 bytes) and its compressed G1 public key (48 bytes),
%% derived from at least 32 bytes of key material (IETF KeyGen).
-spec keygen(binary()) -> {binary(), binary()} | {error, atom()}.
keygen(_IKM) ->
	erlang:nif_error(nif_not_loaded).

%% @doc Sign `Message' under the caller's domain separation tag `DST'.
-spec sign(binary(), binary(), binary()) -> binary() | {error, atom()}.
sign(_Secret, _Message, _DST) ->
	erlang:nif_error(nif_not_loaded).

%% @doc The sum of 1 to 64 signatures, each checked to be in G2.
-spec aggregate([binary()]) -> binary() | {error, atom()}.
aggregate(_Signatures) ->
	erlang:nif_error(nif_not_loaded).

%% @doc Verify one aggregate signature over one `Message' under the sum of
%% 1 to 64 public keys, each checked to be in G1 and not the identity. Only
%% keys whose possession was proven may be passed: that is what makes the
%% sum safe against rogue keys.
-spec fast_aggregate_verify([binary()], binary(), binary(), binary()) -> boolean() | {error, atom()}.
fast_aggregate_verify(_PublicKeys, _Message, _Aggregate, _DST) ->
	erlang:nif_error(nif_not_loaded).

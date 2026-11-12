%%% @doc Copied and adapted from the arweave codebase. 
%%% Should track: https://github.com/ArweaveTeam/arweave/blob/master/apps/arweave/src/ar_block.erl
-module(ar_block).

-export([strict_data_split_threshold/0, get_chunk_padded_offset/1, generate_size_tagged_list_from_txs/2]).
-export([indep_hash/1, tx_root/2, block_message/2]).

-include("include/ar.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

strict_data_split_threshold() -> ?STRICT_DATA_SPLIT_THRESHOLD.

%% @doc Return Offset if it is smaller than or equal to ar_block:strict_data_split_threshold().
%% Otherwise, return the offset of the last byte of the chunk + the size of the padding.
-spec get_chunk_padded_offset(Offset :: non_neg_integer()) -> non_neg_integer().
get_chunk_padded_offset(Offset) ->
	case Offset > ar_block:strict_data_split_threshold() of
		true ->
			ar_poa:get_padded_offset(Offset, ar_block:strict_data_split_threshold());
		false ->
			Offset
	end.

generate_size_tagged_list_from_txs(TXs, Height) ->
	lists:reverse(
		element(2,
			lists:foldl(
				fun(TX, {Pos, List}) ->
					DataSize = TX#tx.data_size,
					End = Pos + DataSize,
					case Height >= ar_fork:height_2_5() of
						true ->
							Padding = ar_tx:get_weave_size_increase(DataSize, Height)
									- DataSize,
							%% Encode the padding information in the Merkle tree.
							case Padding > 0 of
								true ->
									PaddingRoot = <<>>,
									{End + Padding, [{{padding, PaddingRoot}, End + Padding},
											{{TX, get_tx_data_root(TX)}, End} | List]};
								false ->
									{End, [{{TX, get_tx_data_root(TX)}, End} | List]}
							end;
						false ->
							{End, [{{TX, get_tx_data_root(TX)}, End} | List]}
					end
				end,
				{0, []},
				lists:sort(TXs)
			)
		)
	).

get_tx_data_root(#tx{ format = 2, data_root = DataRoot }) ->
    DataRoot;
get_tx_data_root(TX) ->
    (ar_tx:generate_chunk_tree(TX))#tx.data_root.

%% @doc A block's JSON as the message `~arweave@2.9/block' returns for it. In
%% core, not in that device, so every package that verifies blocks reads them
%% the same way.
block_message(Body, Opts) ->
    block_keys(
        hb_message:convert(
            Body,
            <<"structured@1.0">>,
            % `convert/4' asks the source codec to produce TABM first; put
            % `bundle' on json@1.0 so that step does not linkify block fields.
            #{
                <<"device">> => <<"json@1.0">>,
                <<"bundle">> => true
            },
            Opts
        )
    ).

%% @doc Name the fields of a block as message keys: `-' in place of `_', as in
%% `indep-hash', in the block and in the messages nested in it, and
%% `block-signature' and `block-nonce' for the block's `signature' and `nonce'.
%% In `httpsig@1.0', `signature' is the header that carries the signatures
%% of a message and `nonce' one of their parameters.
block_keys(Block) ->
    maps:from_list(
        lists:map(
            fun({<<"signature">>, Value}) -> {<<"block-signature">>, Value};
               ({<<"nonce">>, Value}) -> {<<"block-nonce">>, Value};
               ({Key, Value}) ->
                    {hb_util_string:dash_chars(Key), dash_keys(Value)}
            end,
            maps:to_list(Block)
        )
    ).

%% @doc Put `-' in place of `_' in the keys of a message nested in a block.
dash_keys(Msg) when is_map(Msg) ->
    maps:from_list(
        [
            {hb_util_string:dash_chars(Key), dash_keys(Value)}
        ||
            {Key, Value} <- maps:to_list(Msg)
        ]
    );
dash_keys(Value) -> Value.

%% @doc A block's independent hash, recomputed from its fields: Arweave's
%% `indep_hash/1' and `generate_signed_hash/1' for a block from fork 2.9 on.
%% The block is the message `~arweave@2.9/block' returns with its proofs:
%% the JSON fields, `-' for `_', and `block-nonce' and `block-signature' for
%% `nonce' and `signature'. An earlier block is not a block this reads.
indep_hash(B) ->
    Height = hb_util:int(maps:get(<<"height">>, B)),
    true = Height >= ar_fork:height_2_9(),
    NL = maps:get(<<"nonce-limiter-info">>, B),
    PoA = maps:get(<<"poa">>, B),
    PoA2 = maps:get(<<"poa2">>, B),
    I = fun(M, K) -> hb_util:int(maps:get(K, M)) end,
    D = fun(M, K) -> hb_util:decode(maps:get(K, M)) end,
    Int = fun(K, Bits) -> encode_int(I(B, K), Bits) end,
    Bin = fun(K, Bits) -> encode_bin(D(B, K), Bits) end,
    %% Absent from the JSON when undefined, as in Arweave's record.
    OptInt = fun(K, Bits) -> encode_int(opt(K, B, fun hb_util:int/1), Bits) end,
    OptBin = fun(K, Bits) -> encode_bin(opt(K, B, fun hb_util:decode/1), Bits) end,
    Rate = fun(K) ->
        [Dividend, Divisor] = maps:get(K, B),
        << (encode_int(hb_util:int(Dividend), 8))/binary,
            (encode_int(hb_util:int(Divisor), 8))/binary >>
    end,
    Steps = fun(K) ->
        L = [hb_util:decode(S) || S <- maps:get(K, NL)],
        << (length(L)):16, (iolist_to_binary(L))/binary >>
    end,
    Segment = <<
        (Bin(<<"previous-block">>, 8))/binary, (Int(<<"timestamp">>, 8))/binary,
        (Bin(<<"block-nonce">>, 16))/binary, (encode_int(Height, 8))/binary,
        (Int(<<"diff">>, 16))/binary, (Int(<<"cumulative-diff">>, 16))/binary,
        (Int(<<"last-retarget">>, 8))/binary, (Bin(<<"hash">>, 8))/binary,
        (Int(<<"block-size">>, 16))/binary, (Int(<<"weave-size">>, 16))/binary,
        (Bin(<<"reward-addr">>, 8))/binary, (Bin(<<"tx-root">>, 8))/binary,
        (Bin(<<"wallet-list">>, 8))/binary,
        (Bin(<<"hash-list-merkle">>, 8))/binary, (Int(<<"reward-pool">>, 8))/binary,
        (Int(<<"packing-2-5-threshold">>, 8))/binary,
        (Int(<<"strict-data-split-threshold">>, 8))/binary,
        (Rate(<<"usd-to-ar-rate">>))/binary,
        (Rate(<<"scheduled-usd-to-ar-rate">>))/binary,
        (encode_bin_list([hb_util:decode(T) || T <- maps:get(<<"tags">>, B)], 16, 16))/binary,
        (encode_bin_list([hb_util:decode(T) || T <- maps:get(<<"txs">>, B)], 16, 8))/binary,
        (Int(<<"reward">>, 8))/binary,
        (Int(<<"recall-byte">>, 16))/binary, (Bin(<<"hash-preimage">>, 8))/binary,
        (OptInt(<<"recall-byte2">>, 16))/binary, (Bin(<<"reward-key">>, 16))/binary,
        (Int(<<"partition-number">>, 8))/binary,
        (D(NL, <<"output">>)):32/binary, (I(NL, <<"global-step-number">>)):64,
        (D(NL, <<"seed">>)):48/binary, (D(NL, <<"next-seed">>)):48/binary,
        (I(NL, <<"zone-upper-bound">>)):256, (I(NL, <<"next-zone-upper-bound">>)):256,
        (encode_bin(D(NL, <<"prev-output">>), 8))/binary,
        (Steps(<<"checkpoints">>))/binary, (Steps(<<"last-step-checkpoints">>))/binary,
        (Bin(<<"previous-solution-hash">>, 8))/binary,
        (Int(<<"price-per-gib-minute">>, 8))/binary,
        (Int(<<"scheduled-price-per-gib-minute">>, 8))/binary,
        (D(B, <<"reward-history-hash">>)):32/binary, (Int(<<"debt-supply">>, 8))/binary,
        (I(B, <<"kryder-plus-rate-multiplier">>)):24,
        (I(B, <<"kryder-plus-rate-multiplier-latch">>)):8,
        (I(B, <<"denomination">>)):24, (Int(<<"redenomination-height">>, 8))/binary,
        (double_signing_proof(maps:get(<<"double-signing-proof">>, B, #{})))/binary,
        (Int(<<"previous-cumulative-diff">>, 16))/binary,
        (Int(<<"merkle-rebase-support-threshold">>, 16))/binary,
        (encode_bin(D(PoA, <<"data-path">>), 24))/binary,
        (encode_bin(D(PoA, <<"tx-path">>), 24))/binary,
        (encode_bin(D(PoA2, <<"data-path">>), 24))/binary,
        (encode_bin(D(PoA2, <<"tx-path">>), 24))/binary,
        (D(B, <<"chunk-hash">>)):32/binary, (OptBin(<<"chunk2-hash">>, 8))/binary,
        (D(B, <<"block-time-history-hash">>)):32/binary,
        (encode_int(I(NL, <<"vdf-difficulty">>), 8))/binary,
        (encode_int(I(NL, <<"next-vdf-difficulty">>), 8))/binary,
        (I(B, <<"packing-difficulty">>)):8,
        (OptBin(<<"unpacked-chunk-hash">>, 8))/binary,
        (OptBin(<<"unpacked-chunk2-hash">>, 8))/binary,
        (I(B, <<"replica-format">>)):8
    >>,
    SignedHash = crypto:hash(sha256, Segment),
    crypto:hash(sha384, << SignedHash/binary, (D(B, <<"block-signature">>))/binary >>).

%% @doc Arweave's `encode_double_signing_proof/2' from fork 2.9 on.
double_signing_proof(Proof) when map_size(Proof) =:= 0 -> << 0:8 >>;
double_signing_proof(Proof) ->
    D = fun(K) -> hb_util:decode(maps:get(K, Proof)) end,
    I = fun(K) -> encode_int(hb_util:int(maps:get(K, Proof)), 16) end,
    << 1:8, (encode_bin(D(<<"pub-key">>), 16))/binary,
        (encode_bin(D(<<"sig1">>), 16))/binary, (I(<<"cdiff1">>))/binary,
        (I(<<"prev-cdiff1">>))/binary, (D(<<"preimage1">>)):64/binary,
        (encode_bin(D(<<"sig2">>), 16))/binary, (I(<<"cdiff2">>))/binary,
        (I(<<"prev-cdiff2">>))/binary, (D(<<"preimage2">>)):64/binary >>.

%% @doc The `tx_root' of a block's transactions: Arweave's
%% `generate_tx_root_for_block/2' over the signed headers.
tx_root([], _Height) -> <<>>;
tx_root(TXs, Height) ->
    {Root, _Tree} =
        ar_merkle:generate_tree(
            [{DataRoot, Offset}
                || {{_, DataRoot}, Offset} <- generate_size_tagged_list_from_txs(TXs, Height)]
        ),
    Root.

opt(K, B, Parse) ->
    case maps:find(K, B) of
        {ok, V} -> Parse(V);
        error -> undefined
    end.

encode_int(undefined, Bits) -> << 0:Bits >>;
encode_int(N, Bits) ->
    Bin = binary:encode_unsigned(N, big),
    << (byte_size(Bin)):Bits, Bin/binary >>.

encode_bin(undefined, Bits) -> << 0:Bits >>;
encode_bin(Bin, Bits) -> << (byte_size(Bin)):Bits, Bin/binary >>.

%% Arweave encodes the list in reverse.
encode_bin_list(Bins, LenBits, ElemBits) ->
    << (length(Bins)):LenBits,
        (iolist_to_binary([encode_bin(E, ElemBits) || E <- lists:reverse(Bins)]))/binary >>.

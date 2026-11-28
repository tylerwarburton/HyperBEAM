%%% @doc The scheduler's dedupe index for slots that left local disk.
%%%
%%% A signed message is assigned at most once per process, which the
%%% scheduler checks against `message-slots' rows: one row per signature ID,
%%% about 120 B each, forever. When the cold tier (`hb_store_cold') deletes a
%%% range of slots locally, it moves those rows here, into filters that say
%%% "maybe assigned before" or "never assigned", at about 9 B per ID:
%%%
%%%   a range filter    per deleted range, a Bloom filter over the IDs of its
%%%                     slots (32 bits and 16 probes an ID: about 3 in 10^7
%%%                     false), with the range's first slot; it names the
%%%                     range whose file on the share to read
%%%   segment filters   per process, blocked Bloom filters (4,096-bit blocks,
%%%                     one block an ID, 16 probes in it), the newest filling.
%%%                     The first holds `?BASE' IDs and each next twice the
%%%                     last, so a process pays for the IDs it has and a new
%%%                     message costs one block read per segment, a number
%%%                     that grows with the log of the history
%%%
%%% `candidates/3' is what the scheduler asks once its own rows find nothing:
%%% the ranges that may carry one of a message's IDs. No ID added here is
%%% ever missing from its filters (a Bloom filter has no false negatives), so
%%% a message assigned before is always a candidate, and the share settles it
%%% (`hb_store_cold:seen/4'). The filters are written in the same LMDB
%%% transaction that deletes the rows they replace, and no row is deleted
%%% without them.
%%%
%%% Rows, in the scheduler's store:
%%%   ~cold@1.0/dedupe/<pid>/n                   {Segment, IDs in it}
%%%   ~cold@1.0/dedupe/<pid>/s/<seg>/<block>     512 bytes (absent: empty)
%%%   ~cold@1.0/dedupe/<pid>/r/<seg>/<to>        <<From:64, Bloom bits>>
-module(hb_store_dedupe).
-export([add/5, candidates/3]).
-include_lib("eunit/include/eunit.hrl").

-define(PREFIX, "~cold@1.0/dedupe/").
%% IDs in the first segment (each next holds twice the last), and the
%% blocks of 4,096 bits that hold a segment's IDs at 32 bits an ID. A fixed
%% segment of 4M IDs allocated its 16 MB within the first 100k IDs, for every
%% process (AX102, 2026-11-28).
-define(BASE, 65536).
-define(BLOCK_BITS, 4096).
-define(PROBES, 16).
-define(BITS_PER_ID, 32).

%% @doc The rows that record `IDs', the dedupe IDs of the slots `From'..`To'
%% of `P', in its filters: `#{ Key => Value }', to be written in the
%% transaction that deletes the rows they replace.
add(Store, P, From, To, IDs0) ->
    IDs = lists:usort(IDs0),
    N = length(IDs),
    {Seg0, Count0} = count(Store, P),
    {Seg, Count} =
        case Count0 > 0 andalso Count0 + N > capacity(Seg0) of
            true -> {Seg0 + 1, 0};
            false -> {Seg0, Count0}
        end,
    ByBlock = maps:groups_from_list(fun(ID) -> element(1, block_probes(ID, Seg)) end,
        fun(ID) -> element(2, block_probes(ID, Seg)) end, IDs),
    Blocks =
        maps:from_list(
            [ {Key, set_bits(read_block(Store, Key), lists:append(Probes), ?BLOCK_BITS)}
            || {B, Probes} <- maps:to_list(ByBlock),
               Key <- [block_key(P, Seg, B)] ]),
    Blocks#{
        range_key(P, Seg, To) => <<From:64, (range_filter(IDs))/binary>>,
        count_key(P) => term_to_binary({Seg, Count + N})
    }.

%% @doc The ranges of `P' that may hold one of `IDs': `[{ID, From, To}]',
%% empty when none was ever moved here.
candidates(Store, P, IDs) ->
    {Segs, _} = count(Store, P),
    case read(Store, count_key(P)) of
        not_found -> [];
        _ ->
            lists:append(
                [ range_candidates(Store, P, Seg, ID)
                || ID <- IDs, Seg <- lists:seq(0, Segs), in_segment(Store, P, Seg, ID) ])
    end.

in_segment(Store, P, Seg, ID) ->
    {B, Probes} = block_probes(ID, Seg),
    case read(Store, block_key(P, Seg, B)) of
        not_found -> false;
        Block -> all_set(Block, Probes)
    end.

range_candidates(Store, P, Seg, ID) ->
    Prefix = range_prefix(P, Seg),
    [ {ID, From, binary_to_integer(Name)}
    || Name <- list(Store, Prefix),
       <<From:64, Bits/binary>> <- [read(Store, <<Prefix/binary, "/", Name/binary>>)],
       all_set(Bits, range_probes(ID, bit_size(Bits))) ].

%%% Filters

capacity(Seg) -> ?BASE bsl Seg.
blocks(Seg) -> capacity(Seg) * ?BITS_PER_ID div ?BLOCK_BITS.

%% The block of segment `Seg' an ID falls in and its `?PROBES' bits there,
%% from the SHA-256 of the ID: 32 bits choose the block, then 12 bits a probe.
block_probes(ID, Seg) ->
    <<B:32, Rest:192/bitstring, _/bitstring>> = crypto:hash(sha256, ID),
    {B rem blocks(Seg), [ P || <<P:12>> <= Rest ]}.

%% A range filter's probes: double hashing over its `M' bits.
range_probes(ID, M) ->
    <<H1:64, H2:64, _/binary>> = crypto:hash(sha256, ID),
    [ (H1 + I * (H2 bor 1)) rem M || I <- lists:seq(0, ?PROBES - 1) ].

range_filter(IDs) ->
    M = max(64, (length(IDs) * ?BITS_PER_ID + 7) div 8 * 8),
    set_bits(<<0:M>>, lists:append([ range_probes(ID, M) || ID <- IDs ]), M).

%% Bits are numbered from the first byte's high bit, as `<<_:P, B:1, _/bits>>'
%% reads them.
set_bits(Bin, Positions, M) ->
    Mask = lists:foldl(fun(P, A) -> A bor (1 bsl (M - 1 - P)) end, 0, Positions),
    <<V:M>> = Bin,
    <<(V bor Mask):M>>.

all_set(Bin, Positions) ->
    lists:all(fun(P) -> <<_:P, B:1, _/bitstring>> = Bin, B =:= 1 end, Positions).

%%% Rows

count(Store, P) ->
    case read(Store, count_key(P)) of
        not_found -> {0, 0};
        V -> binary_to_term(V, [safe])
    end.

read_block(Store, Key) ->
    case read(Store, Key) of
        not_found -> <<0:?BLOCK_BITS>>;
        V -> V
    end.

%% A store, or the scheduler's list of them.
read(Store, Key) ->
    case hb_store:read(stores(Store), #{ <<"read">> => Key, <<"exact">> => true }, #{}) of
        {ok, V} -> V;
        _ -> not_found
    end.

list(Store, Path) ->
    case hb_store:list(stores(Store), Path, #{}) of
        {ok, L} -> L;
        _ -> []
    end.

stores(L) when is_list(L) -> L;
stores(S) -> [S].

pid_prefix(P) -> <<?PREFIX, P/binary>>.
count_key(P) -> <<(pid_prefix(P))/binary, "/n">>.
block_key(P, Seg, B) ->
    <<(pid_prefix(P))/binary, "/s/", (integer_to_binary(Seg))/binary, "/", (integer_to_binary(B))/binary>>.
range_prefix(P, Seg) -> <<(pid_prefix(P))/binary, "/r/", (integer_to_binary(Seg))/binary>>.
range_key(P, Seg, To) -> <<(range_prefix(P, Seg))/binary, "/", (integer_to_binary(To))/binary>>.

%%% Tests

%% Never a false negative; false positives at about the designed rate.
filters_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"dedupe-filters">>),
    ok = hb_store:start([Store], #{}, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ID = fun() -> hb_util:human_id(crypto:strong_rand_bytes(32)) end,
    Ranges = [ {R * 1000, R * 1000 + 999, [ ID() || _ <- lists:seq(1, 1000) ]} || R <- lists:seq(0, 4) ],
    lists:foreach(
        fun({From, To, IDs}) ->
            ok = hb_store:write([Store], add(Store, P, From, To, IDs), #{})
        end,
        Ranges),
    ok = hb_store:flush([Store], #{}),
    % Every ID added is a candidate, in its own range.
    lists:foreach(
        fun({From, To, IDs}) ->
            [ ?assert(lists:member({X, From, To}, candidates(Store, P, [X]))) || X <- IDs ]
        end,
        Ranges),
    % Ten thousand IDs never added: hardly any is a candidate.
    False = length([ x || _ <- lists:seq(1, 10000), candidates(Store, P, [ID()]) =/= [] ]),
    ?assert(False =< 2),
    % Another process shares nothing.
    ?assertEqual([], candidates(Store, ID(), [hd(element(3, hd(Ranges)))])).

%% A full segment starts the next, twice as large; both stay searched.
segments_roll_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"dedupe-roll">>),
    ok = hb_store:start([Store], #{}, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    A = hb_util:human_id(crypto:strong_rand_bytes(32)),
    B = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok = hb_store:write([Store], add(Store, P, 1, 10, [A]), #{}),
    ok = hb_store:write([Store], #{ count_key(P) => term_to_binary({0, capacity(0)}) }, #{}),
    ok = hb_store:write([Store], add(Store, P, 11, 20, [B]), #{}),
    ok = hb_store:flush([Store], #{}),
    ?assertEqual({1, 1}, count(Store, P)),
    ?assertEqual([{A, 1, 10}], candidates(Store, P, [A])),
    ?assertEqual([{B, 11, 20}], candidates(Store, P, [B])),
    ?assertEqual(2 * blocks(0), blocks(1)).

%% A process pays for the IDs it has: 5,000 IDs stay in the first segment,
%% at most its 512 blocks (256 KB), where a fixed 4M-ID segment was 16 MB.
small_process_is_small_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"dedupe-small">>),
    ok = hb_store:start([Store], #{}, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    IDs = [ hb_util:human_id(crypto:strong_rand_bytes(32)) || _ <- lists:seq(1, 5000) ],
    Rows = add(Store, P, 1, 5000, IDs),
    Blocks = [ K || K <- maps:keys(Rows), binary:match(K, <<"/s/">>) =/= nomatch ],
    ?assert(length(Blocks) =< blocks(0)),
    ?assert(lists:sum([ byte_size(V) || {K, V} <- maps:to_list(Rows), lists:member(K, Blocks) ])
        =< blocks(0) * ?BLOCK_BITS div 8).

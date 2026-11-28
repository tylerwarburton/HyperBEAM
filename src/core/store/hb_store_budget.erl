%%% @doc The node's disk budget: how close its LMDB stores are to filling,
%%% as one level that every writer and pruner of local disk reads.
%%%
%%% An LMDB file never shrinks: a deleted row's pages go on its free list and
%%% are written again before the file grows. So the file's size says how big
%%% the store has ever been, not what it holds, and pruning cannot lower it.
%%% What a store holds is its LIVE pages, `(last page + 1 - free pages) *
%%% page size' (`elmdb:env_pages/1'), and a store's pressure is the worse of:
%%%
%%%   - its live pages against its map (`capacity'), and
%%%   - its filesystem's use, not counting the file's own free pages (they are
%%%     space the store reuses before it asks the disk for more).
%%%
%%% Each `hb_store_lmdb' monitor measures this every 30 s
%%% (`hb_store_lmdb:fill/2') and stores the store's level beside its fill:
%%%
%%%   ok    below `budget-soft-pct' (70)
%%%   soft  the cold tier keeps only `hot-slots-floor' slots per process and
%%%         ships no checkpoints: the link carries what local disk waits on
%%%   high  (`budget-high-pct', 85) retention keeps only the head and the
%%%         newest snapshot of every process, and runs at once
%%%   hard  (`budget-hard-pct', 95) the scheduler refuses new messages with a
%%%         503 and `retry-after': writes back-pressure before the disk fills.
%%%         Reads are still answered
%%%
%%% A signed row the cold tier has not verified is never deleted at any
%%% level: past `hard' the node stops taking writes instead.
%%%
%%% A build whose `elmdb' has no `env_pages/1' (`patches/elmdb-env-pages.patch'
%%% not applied) measures the file instead of its live pages: never lower
%%% than the truth, so the levels can only come early.
-module(hb_store_budget).
-export([level/0, rank/1, store_level/2, pressure/1, at_least/1, override/1]).
-include_lib("eunit/include/eunit.hrl").

-define(OVERRIDE, {?MODULE, override}).
-define(SOFT, 70).
-define(HIGH, 85).
-define(HARD, 95).

%% @doc The node's level: the worst of its LMDB stores'.
level() ->
    case persistent_term:get(?OVERRIDE, undefined) of
        undefined ->
            lists:foldl(
                fun(H, Worst) -> worst(maps:get(level, H, ok), Worst) end,
                ok,
                maps:values(hb_store_lmdb:health()));
        Level -> Level
    end.

%% @doc Whether the node's level is `Level' or worse.
at_least(Level) -> rank(level()) >= rank(Level).

rank(ok) -> 0;
rank(soft) -> 1;
rank(high) -> 2;
rank(hard) -> 3.

worst(A, B) ->
    case rank(A) >= rank(B) of
        true -> A;
        false -> B
    end.

%% @doc A store's level, from its fill and its own thresholds.
store_level(Fill, StoreOpts) ->
    Pct = pressure(Fill),
    Get = fun(K, D) -> hb_util:int(maps:get(K, StoreOpts, D)) end,
    Hard = Get(<<"budget-hard-pct">>, ?HARD),
    High = min(Hard, Get(<<"budget-high-pct">>, ?HIGH)),
    Soft = min(High, Get(<<"budget-soft-pct">>, ?SOFT)),
    if
        Pct >= Hard -> hard;
        Pct >= High -> high;
        Pct >= Soft -> soft;
        true -> ok
    end.

%% @doc A store's pressure, in percent (`hb_store_lmdb:fill/2'): the worse of
%% its live bytes against its map, and its filesystem's use less the store
%% file's reusable pages.
pressure(Fill) ->
    File = maps:get(file_bytes, Fill, 0),
    Live = maps:get(live_bytes, Fill, File),
    Map = max(1, maps:get(map_bytes, Fill, 1)),
    MapPct = 100 * Live / Map,
    DiskPct =
        case Fill of
            #{ disk_total_bytes := Total, disk_free_bytes := Free } when Total > 0 ->
                100 * (Total - Free - max(0, File - Live)) / Total;
            _ -> 0
        end,
    max(MapPct, DiskPct).

%% @doc Force the node's level, as a drill or a test (`undefined' to measure
%% again). From a remote shell: `hb_store_budget:override(hard)' refuses new
%% messages at once.
override(undefined) -> _ = persistent_term:erase(?OVERRIDE), ok;
override(Level) -> rank(Level), persistent_term:put(?OVERRIDE, Level), ok.

%%% Tests

-define(GIB, 1073741824).

levels_test() ->
    Fill = fun(Live, File, Free) ->
        #{ live_bytes => Live * ?GIB, file_bytes => File * ?GIB, map_bytes => 100 * ?GIB,
           disk_total_bytes => 1000 * ?GIB, disk_free_bytes => Free * ?GIB }
    end,
    ?assertEqual(ok, store_level(Fill(10, 90, 900), #{})),
    ?assertEqual(soft, store_level(Fill(70, 90, 900), #{})),
    ?assertEqual(high, store_level(Fill(86, 90, 900), #{})),
    ?assertEqual(hard, store_level(Fill(95, 96, 900), #{})),
    % A file's free pages are not disk use: of 1,000 GiB, 700 used, but 80
    % of those are the 90 GiB file's own free pages: 62%, not 70%.
    ?assertEqual(ok, store_level(Fill(10, 90, 300), #{})),
    ?assertEqual(soft, store_level(Fill(10, 10, 300), #{})),
    % The disk itself filling (other files) is pressure, whatever the store.
    ?assertEqual(hard, store_level(Fill(10, 10, 40), #{})),
    % Thresholds are the store's own, never inverted.
    ?assertEqual(hard, store_level(Fill(50, 90, 900), #{ <<"budget-hard-pct">> => 40 })),
    % No live count (an old elmdb): the file is the live size.
    ?assertEqual(high,
        store_level(maps:remove(live_bytes, Fill(10, 90, 900)), #{})).

%%% @doc Retention for the process cache: delete the computed states a node
%%% no longer needs, online, in the background, with bounded CPU.
%%%
%%% It deletes only rows of the `pstate/' namespace (`process-state-blobs' in
%%% `lib_process_cache'): one row per computed slot of a delta process, owned
%%% by that process and slot and referenced by nothing else. Nothing outside
%%% that namespace is ever a candidate -- not an assignment, a message, a
%%% process definition or anything content-addressed -- and the irreplaceable
%%% store is never even opened for writing. A build whose `elmdb' has no
%%% `delete_batch/2' (`patches/elmdb-delete.patch' not applied) never deletes:
%%% retention refuses to start and says so.
%%%
%%% A full-state process (no delta rows: `lua@5.3a', WASM, native devices)
%%% keeps its newest `store-retention-checkpoints' snapshot checkpoints and
%%% its newest `store-retention-recent-slots' (default 32) states; every
%%% other row goes. A delta process (`lua-patch', `lua-atomic') keeps the
%%% newest `store-retention-checkpoints' (default
%%% 2) checkpoints that carry a VM snapshot (the `s' markers: the only states
%%% a worker can resume from) and every slot from the oldest of them to the
%%% head. Everything below that floor is deleted, top-down: the highest slot
%%% first, and within a slot the `s' marker before the state. So whatever a
%%% run (or a `kill -9' in the middle of one) leaves behind is still a
%%% readable chain: every remaining delta still reaches the checkpoint below
%%% it. A process with no more than that many snapshot checkpoints, or whose
%%% floor checkpoint has no state row, is left alone. Recomputing any slot
%%% needs only the assignments and the definition, which are never touched;
%%% a slot below the floor is recomputed from the start when asked for.
%%%
%%% Each batch of `store-retention-batch-keys' (default 1,000) keys is one
%%% LMDB write transaction (`elmdb:delete_batch/2'), so a crash deletes a
%%% whole batch or none of it. After each batch the run sleeps long enough to
%%% keep its share of one scheduler at `store-retention-max-cpu-pct' (default
%%% 25). A run stops after `store-retention-max-candidates' keys (default
%%% 1,000,000); the next run continues.
%%%
%%% Configuration: `store-retention' (true to run), `store-retention-
%%% interval-ms' (600,000), `store-retention-first-ms' (60,000), and the keys
%%% above. The store is the first writable `hb_store_lmdb' of the node's
%%% `store' list that is not an irreplaceable store.
-module(hb_store_retention).
-export([maybe_start/1, start/1, stop/0, run_now/1, status/0, running/0]).
-export([retain/1, plan/3, plan/4, store/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NS, <<"pstate">>).
%% States a full-state process keeps besides its snapshot checkpoints.
-define(RECENT, 32).
-define(SERVER, hb_store_retention_server).
-define(STATUS, {?MODULE, status}).
-define(RUNNING, {?MODULE, running}).

%%% The background server.

%% @doc Start the retention server when `store-retention' is true.
maybe_start(Opts) ->
    case hb_util:atom(hb_opts:get(<<"store-retention">>, false, Opts)) of
        true -> start(Opts);
        _ -> ok
    end.

start(Opts) ->
    case delete_supported() of
        false ->
            ?event(error, {store_retention_not_started, elmdb_has_no_delete_batch}),
            {error, no_delete_primitive};
        true ->
            case whereis(?SERVER) of
                undefined ->
                    Pid = spawn(fun() -> server_init(Opts) end),
                    try register(?SERVER, Pid), ok
                    catch error:badarg -> exit(Pid, kill), ok
                    end;
                _ -> ok
            end
    end.

stop() ->
    case whereis(?SERVER) of
        undefined -> ok;
        Pid -> exit(Pid, kill), ok
    end.

server_init(Opts) ->
    erlang:send_after(int(<<"store-retention-first-ms">>, 60000, Opts), self(), run),
    server(Opts).

server(Opts) ->
    receive
        run ->
            _ = safe_retain(Opts),
            erlang:send_after(int(<<"store-retention-interval-ms">>, 600000, Opts), self(), run),
            server(Opts);
        {run_now, From, Ref} ->
            From ! {Ref, safe_retain(Opts)},
            server(Opts)
    end.

%% @doc Run once now: in the server when it runs, else inline.
run_now(Opts) ->
    case whereis(?SERVER) of
        undefined -> safe_retain(Opts);
        Pid ->
            Ref = make_ref(),
            Pid ! {run_now, self(), Ref},
            receive {Ref, R} -> R end
    end.

status() ->
    (persistent_term:get(?STATUS, #{}))#{ running => running(),
                                          server => whereis(?SERVER) =/= undefined }.

running() -> persistent_term:get(?RUNNING, false).

safe_retain(Opts) ->
    persistent_term:put(?RUNNING, true),
    Report =
        try retain(Opts)
        catch C:R:St ->
            ?event(error, {store_retention_failed, C, R, {trace, St}}),
            #{ status => failed, error => {C, R} }
        after
            persistent_term:put(?RUNNING, false)
        end,
    Prev = persistent_term:get(?STATUS, #{}),
    persistent_term:put(?STATUS, Prev#{
        last_run => Report,
        runs => maps:get(runs, Prev, 0) + 1,
        total_deleted => maps:get(total_deleted, Prev, 0) + maps:get(deleted, Report, 0)
    }),
    ?event(store_retention, {retention_run, Report}),
    Report.

%%% One run.

%% @doc One retention run over the node's process-cache store.
retain(Opts) ->
    case delete_supported() of
        false -> #{ status => no_delete_primitive, deleted => 0 };
        true ->
            Start = erlang:monotonic_time(millisecond),
            Store = store(Opts),
            ok = hb_store:start([Store], #{}, Opts),
            #{ <<"db">> := DB } = hb_store:find(Store),
            Keep = max(1, int(<<"store-retention-checkpoints">>, 2, Opts)),
            Recent = max(1, int(<<"store-retention-recent-slots">>, ?RECENT, Opts)),
            Max = int(<<"store-retention-max-candidates">>, 1000000, Opts),
            Batch = max(1, int(<<"store-retention-batch-keys">>, 1000, Opts)),
            Pct = min(100, max(1, int(<<"store-retention-max-cpu-pct">>, 25, Opts))),
            Procs = list(Store, ?NS),
            {Deleted, Slept, Planned} =
                lists:foldl(
                    fun(_P, Acc = {D, _, _}) when D >= Max -> Acc;
                       (P, {D, Sl, Pl}) ->
                            Keys = lists:sublist(plan(Store, P, Keep, Recent), Max - D),
                            {D1, Sl1} = delete(DB, Keys, Batch, Pct),
                            {D + D1, Sl + Sl1, Pl + length(Keys)}
                    end,
                    {0, 0, 0},
                    Procs
                ),
            #{
                status => ok,
                processes => length(Procs),
                planned => Planned,
                deleted => Deleted,
                duration_ms => erlang:monotonic_time(millisecond) - Start,
                throttle_ms => Slept,
                store => maps:get(<<"name">>, Store)
            }
    end.

%% @doc The keys of one process to delete, in the order to delete them.
plan(Store, P, Keep) -> plan(Store, P, Keep, ?RECENT).
plan(Store, P, Keep, Recent) ->
    Slots = fun(Kind) -> slots(Store, P, Kind) end,
    Snaps = lists:reverse(Slots(<<"s">>)),
    Full = Slots(<<"f">>),
    case Slots(<<"d">>) of
        [] -> plan_full(P, Snaps, Full, Keep, Recent);
        _ -> plan_delta(P, Snaps, Full, Slots, Keep)
    end.

%% A full-state process (no delta rows): every row stands alone. Keep the
%% newest `Keep' snapshot checkpoints and the newest `Recent' states; every
%% other row goes, newest first.
plan_full(P, Snaps, Full, Keep, Recent) ->
    KeepS = lists:sublist(Snaps, Keep),
    KeepF = KeepS ++ lists:sublist(lists:reverse(Full), Recent),
    Doomed =
        [ {S, 0, <<"s">>} || S <- Snaps, not lists:member(S, KeepS) ] ++
        [ {S, 2, <<"f">>} || S <- Full, not lists:member(S, KeepF) ],
    order(P, Doomed).

order(P, Doomed) ->
    [ key(P, Kind, S)
    || {S, _, Kind} <- lists:sort(
           fun({A, KA, _}, {B, KB, _}) -> {A, -KA} >= {B, -KB} end,
           Doomed) ].

%% A delta process: keep everything from the oldest kept snapshot checkpoint
%% up, so every kept delta still reaches a full state.
plan_delta(P, Snaps, Full, Slots, Keep) ->
    case length(Snaps) > Keep of
        false -> [];
        true ->
            Floor = lists:nth(Keep, Snaps),
            case lists:member(Floor, Full) of
                false ->
                    ?event(warning, {store_retention_skipped, {process, P},
                                     {floor_without_state, Floor}}),
                    [];
                true ->
                    order(P,
                        [ {S, 0, <<"s">>} || S <- Snaps, S < Floor ] ++
                        [ {S, 1, <<"d">>} || S <- Slots(<<"d">>), S < Floor ] ++
                        [ {S, 2, <<"f">>} || S <- Full, S < Floor ])
            end
    end.

%% Delete in batches, top-down, throttled to `Pct' percent of one scheduler.
delete(_DB, [], _Batch, _Pct) -> {0, 0};
delete(DB, Keys, Batch, Pct) ->
    {Now, Rest} = split(Batch, Keys),
    % Only rows of the process-cache namespace, ever.
    true = lists:all(fun is_retention_key/1, Now),
    T0 = erlang:monotonic_time(microsecond),
    N = case elmdb:delete_batch(DB, Now) of
            {ok, Count} -> Count;
            Error ->
                % elmdb fails the store like a failed flush: it refuses
                % writes until the node restarts.
                ?event(error,
                    {store_retention_delete_failed,
                        {main_store_refuses_writes_until_restart, true},
                        {error, Error}}),
                persistent_term:put(?STATUS,
                    (persistent_term:get(?STATUS, #{}))#{ store_failed => Error }),
                erlang:error({store_retention_delete_failed, Error})
        end,
    Work = (erlang:monotonic_time(microsecond) - T0) div 1000,
    Sleep = Work * (100 - Pct) div Pct,
    timer:sleep(Sleep),
    {N2, S2} = delete(DB, Rest, Batch, Pct),
    {N + N2, Sleep + S2}.

split(N, L) when length(L) =< N -> {L, []};
split(N, L) -> lists:split(N, L).

is_retention_key(<<"pstate/", Rest/binary>>) ->
    case binary:split(Rest, <<"/">>, [global]) of
        [P, Kind, Slot] ->
            ?IS_ID(P) andalso lists:member(Kind, [<<"s">>, <<"d">>, <<"f">>])
                andalso byte_size(Slot) == 12
                andalso is_integer(catch binary_to_integer(Slot));
        _ -> false
    end;
is_retention_key(_) -> false.

key(P, Kind, Slot) ->
    iolist_to_binary([?NS, "/", P, "/", Kind, "/", io_lib:format("~12..0B", [Slot])]).

slots(Store, P, Kind) ->
    lists:sort([ binary_to_integer(N) || N <- list(Store, <<?NS/binary, "/", P/binary, "/", Kind/binary>>),
                 is_integer(catch binary_to_integer(N)) ]).

list(Store, Path) ->
    case hb_store:list([Store], Path, #{}) of
        {ok, L} -> L;
        _ -> []
    end.

%% @doc The store retention acts on: the first writable `hb_store_lmdb' of
%% the node's store list, never an irreplaceable store.
store(Opts) ->
    Stores =
        hb_store:normalize_stores(
            case hb_opts:get(<<"store">>, [], Opts) of
                L when is_list(L) -> L;
                M when is_map(M) -> [M]
            end),
    Irr = case hb_store_irreplaceable:store(Opts) of undefined -> []; I -> I end,
    IrrNames = lists:flatmap(
        fun(S) -> [maps:get(<<"name">>, S, undefined),
                   maps:get(<<"name">>, maps:get(<<"inner">>, S, #{}), undefined)] end, Irr),
    case [ S || S = #{ <<"store-module">> := hb_store_lmdb } <- Stores,
                not is_map_key(<<"access">>, S),
                maps:get(<<"read-only">>, S, false) =/= true,
                not lists:member(maps:get(<<"name">>, S, undefined), IrrNames) ] of
        [Store | _] -> Store;
        [] -> erlang:error(store_retention_needs_a_writable_lmdb_store)
    end.

delete_supported() ->
    _ = code:ensure_loaded(elmdb),
    erlang:function_exported(elmdb, delete_batch, 2).

int(Key, Default, Opts) -> hb_util:int(hb_opts:get(Key, Default, Opts)).

%%% Tests

delete_primitive_present_test() ->
    ?assert(delete_supported()).

%% @doc Only `pstate/' rows below the newest two snapshot checkpoints go;
%% the floor checkpoint, everything above it, every other namespace and the
%% irreplaceable store stay.
retain_deletes_only_below_floor_test() ->
    Main = hb_test_utils:test_store(hb_store_lmdb, <<"ret-main">>),
    Irr = hb_test_utils:test_store(hb_store_lmdb, <<"ret-irr">>),
    ok = hb_store:start([Main, Irr], #{}, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Q = hb_util:human_id(crypto:strong_rand_bytes(32)),
    % P: checkpoints with snapshots at 0, 10, 20, 30; deltas between.
    Rows =
        maps:from_list(
            [ {key(P, <<"f">>, S), <<"full">>} || S <- [0, 10, 20, 30] ] ++
            [ {key(P, <<"s">>, S), <<>>} || S <- [0, 10, 20, 30] ] ++
            [ {key(P, <<"d">>, S), <<"delta">>} || S <- lists:seq(1, 35), S rem 10 =/= 0 ] ++
            % Q has only two snapshot checkpoints: kept whole.
            [ {key(Q, <<"f">>, S), <<"full">>} || S <- [0, 5] ] ++
            [ {key(Q, <<"s">>, S), <<>>} || S <- [0, 5] ] ++
            [ {key(Q, <<"d">>, S), <<"delta">>} || S <- [1, 2, 3] ]),
    Other = #{
        <<"~scheduler@1.0/assignments/", P/binary, "/0">> => <<"ao:x">>,
        P => <<"definition">>,
        <<"computed/", P/binary, "/slot/0">> => <<"ao:y">>
    },
    ok = hb_store:write([Main], maps:merge(Rows, Other), #{}),
    ok = hb_store:write([Irr], Rows, #{}),
    ok = hb_store:flush([Main, Irr], #{}),
    Opts = #{ <<"store">> => [Main], <<"irreplaceable-store">> => Irr,
              <<"store-retention-batch-keys">> => 3 },
    #{ deleted := Deleted } = retain(Opts),
    Gone = [ K || K <- maps:keys(Rows), hb_store:read([Main], K, #{}) =/= {ok, maps:get(K, Rows)} ],
    Expected =
        lists:sort(
            [ key(P, K, S) || K <- [<<"f">>, <<"s">>], S <- [0, 10] ] ++
            [ key(P, <<"d">>, S) || S <- lists:seq(1, 19), S rem 10 =/= 0 ]),
    ?assertEqual(Expected, lists:sort(Gone)),
    ?assertEqual(length(Expected), Deleted),
    [ ?assertEqual({ok, V}, hb_store:read([Main], K, #{})) || {K, V} <- maps:to_list(Other) ],
    [ ?assertEqual({ok, V}, hb_store:read([Irr], K, #{})) || {K, V} <- maps:to_list(Rows) ],
    % A second run finds nothing more.
    ?assertMatch(#{ deleted := 0 }, retain(Opts)).

%% @doc Deletion runs top-down: the highest doomed slot first, a slot's
%% marker before its state.
plan_order_test() ->
    Main = hb_test_utils:test_store(hb_store_lmdb, <<"ret-order">>),
    ok = hb_store:start([Main], #{}, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok = hb_store:write([Main], maps:from_list(
        [ {key(P, K, S), <<"x">>} || K <- [<<"f">>, <<"s">>], S <- [0, 4, 8] ] ++
        [ {key(P, <<"d">>, S), <<"x">>} || S <- [1, 2, 3, 5] ]), #{}),
    ?assertEqual(
        [ key(P, <<"d">>, 5),
          key(P, <<"s">>, 4), key(P, <<"f">>, 4), key(P, <<"d">>, 3), key(P, <<"d">>, 2),
          key(P, <<"d">>, 1), key(P, <<"s">>, 0), key(P, <<"f">>, 0) ],
        plan(Main, P, 1)).

never_a_foreign_key_test() ->
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assert(is_retention_key(key(P, <<"d">>, 5))),
    ?assertNot(is_retention_key(<<"~scheduler@1.0/assignments/", P/binary, "/5">>)),
    ?assertNot(is_retention_key(<<"pstate/", P/binary, "/x/000000000005">>)),
    ?assertNot(is_retention_key(P)).

%% @doc A full-state process keeps its newest snapshot checkpoints and its
%% newest states; everything else goes, newest first.
plan_full_state_test() ->
    Main = hb_test_utils:test_store(hb_store_lmdb, <<"ret-full">>),
    ok = hb_store:start([Main], #{}, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok = hb_store:write([Main], maps:from_list(
        [ {key(P, <<"f">>, S), <<"x">>} || S <- lists:seq(0, 12) ] ++
        [ {key(P, <<"s">>, S), <<>>} || S <- [0, 5, 10] ]), #{}),
    ?assertEqual(
        [ key(P, <<"f">>, 9), key(P, <<"f">>, 8), key(P, <<"f">>, 7), key(P, <<"f">>, 6),
          key(P, <<"f">>, 4), key(P, <<"f">>, 3), key(P, <<"f">>, 2), key(P, <<"f">>, 1),
          key(P, <<"s">>, 0), key(P, <<"f">>, 0) ],
        plan(Main, P, 2, 3)),
    % Without any snapshot checkpoint, only the newest states stay.
    Q = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok = hb_store:write([Main], maps:from_list(
        [ {key(Q, <<"f">>, S), <<"x">>} || S <- lists:seq(0, 4) ]), #{}),
    ?assertEqual([ key(Q, <<"f">>, 1), key(Q, <<"f">>, 0) ], plan(Main, Q, 2, 3)).

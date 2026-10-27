%%% @doc Store retention on a game-shaped process: recompute equality and disk
%%% over a long run.
%%%
%%% Same process and node config as `durability_bench' (28 players of
%%% ~2.6 KB, one player key rewritten per slot, irreplaceable store on).
%%% Run inside a build (copy to `src/core/test/', then `rebar3 device test
%%% --with-core --module=retention_bench') with RET_BENCH set to:
%%%   recompute:<slots>:<dir>   checkpoint and snapshot every 500 slots; run
%%%       retention; recompute the head, a slot below the floor and the next
%%%       slot, from an empty main store and from what retention kept, and
%%%       compare every published key.
%%%   long:<legacy|blobs|retention>:<slots>:<dir>   checkpoint and snapshot
%%%       every 1,000 slots; retention (if on) in its server every 60 s;
%%%       main-store size every 2,000 slots, and slot time split by whether
%%%       a retention run was in progress.
-module(retention_bench).
-include_lib("eunit/include/eunit.hrl").

bench_test_() ->
    case os:getenv("RET_BENCH") of
        false -> [];
        Spec ->
            {timeout, 14400, fun() -> run(string:split(Spec, ":", all)) end}
    end.

lmdb(Dir) ->
    #{ <<"store-module">> => hb_store_lmdb, <<"name">> => list_to_binary(Dir) }.

opts(Dir, Main, Variant, Every) ->
    Base = #{
        <<"store">> => [lmdb(Dir ++ "/" ++ Main)],
        <<"irreplaceable-store">> => lmdb(Dir ++ "/irr"),
        <<"priv-wallet">> => wallet(Dir),
        <<"spawn-worker">> => true,
        <<"process-worker-spawn">> => true,
        <<"process-workers">> => true,
        <<"await-inprogress">> => named,
        <<"scheduler-lookahead">> => false,
        <<"scheduler-publish-remote">> => false,
        <<"process-snapshot-target">> => false,
        <<"process-snapshot-slots">> => Every,
        <<"process-delta-cache">> => true,
        <<"process-delta-checkpoint-slots">> => Every,
        <<"match-index">> => false,
        <<"process-state-blobs">> => Variant =/= "legacy",
        <<"store-retention">> => Variant =:= "retention",
        <<"store-retention-first-ms">> => 5000,
        <<"store-retention-interval-ms">> => 60000
    },
    hb_store_irreplaceable:node_message(Base).

wallet(Dir) ->
    case get({wallet, Dir}) of
        undefined -> W = ar_wallet:new(), put({wallet, Dir}, W), W;
        W -> W
    end.

script() ->
    <<
        "Players = Players or {}\n"
        "Count = Count or 0\n"
        "local pad = string.rep('x', 2400)\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  local i = Count % 28\n"
        "  local p = Players[i] or { level = 0, wins = 0 }\n"
        "  p.level = p.level + 1\n"
        "  p.wins = p.wins + (Count % 3)\n"
        "  Players[i] = p\n"
        "  local rec = '{\"level\":' .. p.level .. ',\"wins\":' .. p.wins ..\n"
        "    ',\"slot\":' .. Count .. ',\"pad\":\"' .. pad .. '\"}'\n"
        "  return {\n"
        "    patches = {\n"
        "      { path = '/player-' .. i, value = rec },\n"
        "      { path = '/count', value = tostring(Count) }\n"
        "    },\n"
        "    results = { output = { data = tostring(Count) } }\n"
        "  }\n"
        "end\n"
    >>.

process(Opts) ->
    Address = hb_util:human_id(ar_wallet:to_address(hb_opts:get(<<"priv-wallet">>, undefined, Opts))),
    hb_message:commit(#{
        <<"device">> => <<"process@1.0">>, <<"type">> => <<"Process">>,
        <<"scheduler-device">> => <<"scheduler@1.0">>,
        <<"execution-device">> => <<"lua-patch@1.0">>,
        <<"module">> => #{ <<"content-type">> => <<"application/lua">>, <<"body">> => script() },
        <<"authority">> => [Address], <<"scheduler-location">> => Address,
        <<"test-random-seed">> => rand:uniform(1000000)
    }, Opts).

schedule(Process, N, Opts) ->
    ProcID = hb_message:id(Process, all, Opts),
    {ok, _} = hb_ao:resolve(Process, hb_message:commit(#{
        <<"path">> => <<"schedule">>, <<"method">> => <<"POST">>,
        <<"body">> => hb_message:commit(#{
            <<"target">> => ProcID, <<"type">> => <<"Message">>,
            <<"action">> => <<"Play">>, <<"number">> => N }, Opts) }, Opts), Opts).

compute(Process, N, Opts) ->
    {ok, State} = hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => N }, Opts),
    State.

spawn_process(Opts) ->
    Process = process(Opts),
    Sched = hb_opts:get(<<"scheduler-store">>, [], Opts),
    {ok, _} = hb_cache:write(Process, Opts#{ <<"store">> => Sched }),
    Process.

%% The published state: every player key and the count.
published(State, Opts) ->
    L = hb_cache:ensure_all_loaded(State, Opts),
    maps:with([<<"count">> | [ <<"player-", (integer_to_binary(I))/binary>> || I <- lists:seq(0, 27) ]], L).

%% Drop the live worker and the hot cache, so the next compute starts from
%% what the store holds.
forget(Process, Opts) ->
    Group = hb_util:human_id(hb_message:id(Process, all, Opts)),
    case hb_name:lookup(Group) of
        Pid when is_pid(Pid) ->
            Ref = erlang:monitor(process, Pid), exit(Pid, kill),
            receive {'DOWN', Ref, _, _, _} -> ok end;
        _ -> ok
    end,
    catch ets:delete_all_objects(lib_process_cache_hot),
    ok.

pstate(Store, ProcID, Kind) ->
    case hb_store:list([Store], <<"pstate/", ProcID/binary, "/", Kind/binary>>, #{}) of
        {ok, L} -> length(L);
        _ -> 0
    end.

du(Dir) ->
    case string:lexemes(os:cmd("du -sk " ++ Dir ++ " 2>/dev/null"), "\t\n") of
        [K | _] -> list_to_integer(K);
        _ -> 0
    end.

say(F, A) -> io:format(user, F ++ "~n", A).

run(["recompute", N0, Dir]) ->
    hb:init(),
    N = list_to_integer(N0),
    os:cmd("rm -rf " ++ Dir),
    Opts = opts(Dir, "main", "retention", 500),
    Process = spawn_process(Opts),
    ProcID = hb_util:human_id(hb_message:id(Process, all, Opts)),
    [Main | _] = hb_opts:get(<<"store">>, [], Opts),
    lists:foreach(fun(I) -> schedule(Process, I, Opts), compute(Process, I, Opts) end,
                  lists:seq(0, N)),
    Head = published(compute(Process, N, Opts), Opts),
    Before = {pstate(Main, ProcID, <<"s">>), pstate(Main, ProcID, <<"d">>), pstate(Main, ProcID, <<"f">>)},
    MainBefore = du(Dir ++ "/main"),
    Report = hb_store_retention:retain(Opts),
    After = {pstate(Main, ProcID, <<"s">>), pstate(Main, ProcID, <<"d">>), pstate(Main, ProcID, <<"f">>)},
    say("RECOMPUTE slots=~p pstate(s,d,f) before=~p after=~p retain=~p", [N, Before, After,
        maps:with([deleted, duration_ms, throttle_ms], Report)]),
    % 1. The head, recomputed from nothing but the irreplaceable store.
    forget(Process, Opts),
    Fresh = opts(Dir, "fresh", "blobs", 500),
    T0 = erlang:monotonic_time(millisecond),
    HeadFresh = published(compute(Process, N, Fresh), Fresh),
    T1 = erlang:monotonic_time(millisecond),
    say("RECOMPUTE head from empty main store: equal=~p (~p ms)", [HeadFresh == Head, T1 - T0]),
    % 2. The next slot, resumed from what retention kept.
    forget(Process, Fresh),
    schedule(Process, N + 1, Opts),
    T2 = erlang:monotonic_time(millisecond),
    NextKept = published(compute(Process, N + 1, Opts), Opts),
    T3 = erlang:monotonic_time(millisecond),
    forget(Process, Opts),
    NextFresh = published(compute(Process, N + 1, Fresh), Fresh),
    say("RECOMPUTE next slot from kept checkpoint: equal_to_fresh=~p count=~p (~p ms)",
        [NextKept == NextFresh, maps:get(<<"count">>, NextKept), T3 - T2]),
    % 3. A slot below the floor, whose rows retention deleted.
    forget(Process, Fresh),
    Low = 100,
    LowKept = (catch published(compute(Process, Low, Opts), Opts)),
    forget(Process, Opts),
    LowFresh = published(compute(Process, Low, Fresh), Fresh),
    say("RECOMPUTE slot ~p below the floor: equal_to_fresh=~p count=~p",
        [Low, LowKept == LowFresh, maps:get(<<"count">>, LowFresh)]),
    say("RECOMPUTE main store before/after retention: ~p / ~p KiB", [MainBefore, du(Dir ++ "/main")]);
run(["long", Variant, N0, Dir0]) ->
    hb:init(),
    N = list_to_integer(N0),
    Dir = Dir0 ++ "/" ++ Variant,
    os:cmd("rm -rf " ++ Dir),
    Opts = opts(Dir, "main", Variant, 1000),
    _ = hb_store_retention:maybe_start(Opts),
    Process = spawn_process(Opts),
    {Times, _} =
        lists:foldl(
            fun(I, {Acc, _}) ->
                Running0 = hb_store_retention:running(),
                {T, _} = timer:tc(fun() -> schedule(Process, I, Opts), compute(Process, I, Opts) end),
                Running = Running0 orelse hb_store_retention:running(),
                case I rem 2000 of
                    0 when I > 0 ->
                        say("LONG ~s slot=~p main_kib=~p irr_kib=~p", [Variant, I, du(Dir ++ "/main"), du(Dir ++ "/irr")]);
                    _ -> ok
                end,
                {[{T div 1000, Running} | Acc], ok}
            end,
            {[], ok},
            lists:seq(0, N)),
    Med = fun([]) -> 0; (L) -> lists:nth(length(L) div 2 + 1, lists:sort(L)) end,
    P95 = fun([]) -> 0; (L) -> lists:nth(max(1, trunc(length(L) * 0.95)), lists:sort(L)) end,
    During = [ T || {T, true} <- Times ],
    Outside = [ T || {T, false} <- Times ],
    say("LONG ~s done slots=~p main_kib=~p slot_med=~p p95=~p | outside retention: n=~p med=~p p95=~p "
        "| during retention: n=~p med=~p p95=~p | retention=~p",
        [Variant, N, du(Dir ++ "/main"), Med([T || {T, _} <- Times]), P95([T || {T, _} <- Times]),
         length(Outside), Med(Outside), P95(Outside), length(During), Med(During), P95(During),
         maps:without([last_run], hb_store_retention:status())]),
    hb_store_retention:stop().

%%% @doc Per-slot cost of the durability stores on a game-like process.
%%%
%%% A `lua-patch@1.0' process holding 28 players of ~2.6 KB each; every
%%% message rewrites one player and publishes it as its own key (the game's
%%% `player-<address>' shape). Each slot is scheduled and then computed, in
%%% sequence, as live traffic does, with the port's required node config.
%%% Run inside a build (`rebar3 device test --with-core --module=
%%% durability_bench' with DURAB_BENCH=<mode>:<slots>:<dir>), modes:
%%%   nosync | sync | irr | journal, and two probes: irrnosync (the
%%%   irreplaceable store unsynced) and irrfirst (it read before the main
%%%   store). DURAB_TRACE=1 also counts LMDB calls per store and phase.
-module(durability_bench).
-include_lib("eunit/include/eunit.hrl").

bench_test_() ->
    case os:getenv("DURAB_BENCH") of
        false -> [];
        Spec ->
            [Mode, N, Dir] = string:split(Spec, ":", all),
            {timeout, 3600, fun() -> run(Mode, list_to_integer(N), Dir) end}
    end.

lmdb(Dir, Sync) ->
    #{ <<"store-module">> => hb_store_lmdb, <<"name">> => list_to_binary(Dir),
       <<"sync">> => Sync }.

opts(Mode, Dir) ->
    Sync = Mode =/= "nosync",
    Base = #{
        <<"store">> => [lmdb(Dir ++ "/main", Sync)],
        <<"priv-wallet">> => ar_wallet:new(),
        <<"spawn-worker">> => true,
        <<"process-worker-spawn">> => true,
        <<"process-workers">> => true,
        <<"await-inprogress">> => named,
        <<"scheduler-lookahead">> => false,
        <<"scheduler-publish-remote">> => false,
        <<"process-snapshot-target">> => false,
        <<"process-delta-cache">> => true,
        <<"process-delta-checkpoint-slots">> => 1000,
        <<"match-index">> => false
    },
    Extra =
        case Mode of
            "irr" -> #{ <<"irreplaceable-store">> => lmdb(Dir ++ "/irr", true) };
            "irrnosync" -> #{ <<"irreplaceable-store">> => lmdb(Dir ++ "/irr", false) };
            "irrfirst" -> #{ <<"irreplaceable-store">> => lmdb(Dir ++ "/irr", true) };
            "journal" ->
                #{ <<"irreplaceable-store">> => lmdb(Dir ++ "/irr", true),
                   <<"irreplaceable-export">> =>
                       #{ <<"path">> => list_to_binary(Dir ++ "/target"),
                          <<"journal">> => list_to_binary(Dir ++ "/journal") } };
            _ -> #{}
        end,
    Node = hb_store_irreplaceable:node_message(maps:merge(Base, Extra)),
    case Mode of
        "irrfirst" ->
            [M, I] = maps:get(<<"store">>, Node),
            Node#{ <<"store">> => [I, M] };
        _ -> Node
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
    Wallet = hb_opts:get(<<"priv-wallet">>, undefined, Opts),
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    hb_message:commit(
        #{
            <<"device">> => <<"process@1.0">>,
            <<"type">> => <<"Process">>,
            <<"scheduler-device">> => <<"scheduler@1.0">>,
            <<"execution-device">> => <<"lua-patch@1.0">>,
            <<"module">> => #{ <<"content-type">> => <<"application/lua">>,
                               <<"body">> => script() },
            <<"authority">> => [Address],
            <<"scheduler-location">> => Address,
            <<"test-random-seed">> => rand:uniform(1000000)
        },
        Opts).

schedule(Process, N, Opts) ->
    ProcID = hb_message:id(Process, all, Opts),
    Req = hb_message:commit(#{
        <<"path">> => <<"schedule">>, <<"method">> => <<"POST">>,
        <<"body">> => hb_message:commit(#{
            <<"target">> => ProcID, <<"type">> => <<"Message">>,
            <<"action">> => <<"Play">>, <<"number">> => N }, Opts) }, Opts),
    {ok, _} = hb_ao:resolve(Process, Req, Opts).

du(Dir) ->
    case string:lexemes(os:cmd("du -sk " ++ Dir ++ " 2>/dev/null"), "\t\n") of
        [K | _] -> list_to_integer(K);
        _ -> 0
    end.

rss() ->
    {ok, B} = file:read_file("/proc/self/status"),
    {match, [_, L]} = re:run(B, "VmRSS:[ \t]+([0-9]+)", [{capture, all, binary}]),
    binary_to_integer(L).

run(Mode, N, Dir0) ->
    hb:init(),
    Dir = Dir0 ++ "/" ++ Mode,
    os:cmd("rm -rf " ++ Dir),
    Opts = opts(Mode, Dir),
    Process = process(Opts),
    {ok, _} = dev_scheduler_cache_write_spawn(Process, Opts),
    % Warm up: spawn the worker and reach steady state.
    lists:foreach(
        fun(I) ->
            schedule(Process, I, Opts),
            {ok, _} = hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => I }, Opts)
        end,
        lists:seq(0, 20)),
    First =
        case os:getenv("DURAB_TRACE") of
            false -> 21;
            _ -> trace_slots(Process, Opts), 41
        end,
    Dirs = [ Dir ++ "/" ++ D || D <- ["main", "irr", "journal", "target"] ],
    erlang:garbage_collect(),
    Disk0 = [ du(D) || D <- Dirs ],
    Mem0 = erlang:memory(total), Rss0 = rss(),
    {Sched, Comp} =
        lists:foldl(
            fun(I, {S, C}) ->
                {TS, _} = timer:tc(fun() -> schedule(Process, I, Opts) end),
                {TC, {ok, _}} = timer:tc(fun() ->
                    hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => I }, Opts)
                end),
                {[TS | S], [TC | C]}
            end,
            {[], []},
            lists:seq(First, First - 1 + N)),
    timer:sleep(2500),   % let flushers and the shipper settle
    Disk1 = [ du(D) || D <- Dirs ],
    erlang:garbage_collect(),
    Mem1 = erlang:memory(total), Rss1 = rss(),
    Med = fun(L) -> lists:nth(length(L) div 2 + 1, lists:sort(L)) / 1000 end,
    P95 = fun(L) -> lists:nth(trunc(length(L) * 0.95), lists:sort(L)) / 1000 end,
    Tot = lists:zipwith(fun(A, B) -> A + B end, Sched, Comp),
    Growth = [ (B - A) * 1000 div N || {A, B} <- lists:zip(Disk0, Disk1) ],
    io:format(user,
        "BENCH mode=~s slots=~p sched_med=~.2f comp_med=~.2f slot_med=~.2f slot_p95=~.2f "
        "mean=~.2f ms | KiB/1000 slots main=~p irr=~p journal=~p target=~p | "
        "beam_mem ~p->~p MiB rss ~p->~p MiB~n",
        [Mode, N, Med(Sched), Med(Comp), Med(Tot), P95(Tot),
         lists:sum(Tot) / length(Tot) / 1000 | Growth] ++
        [Mem0 div 1048576, Mem1 div 1048576, Rss0 div 1024, Rss1 div 1024]).

dev_scheduler_cache_write_spawn(Process, Opts) ->
    hb_cache:write(Process, Opts#{ <<"store">> =>
        hb_opts:get(<<"scheduler-store">>, hb_opts:get(<<"store">>, [], Opts), Opts) }).

%% Count LMDB calls per store and phase over 20 slots.
trace_slots(Process, Opts) ->
    Self = self(),
    Col = spawn(fun() -> collect(#{}, Self) end),
    erlang:trace_pattern({hb_store_lmdb, write, 3}, true, [global]),
    erlang:trace_pattern({hb_store_lmdb, read, 3}, true, [global]),
    erlang:trace_pattern({hb_store_lmdb, list, 3}, true, [global]),
    Run = fun(Phase, F) ->
        Col ! {phase, Phase},
        erlang:trace(all, true, [call, {tracer, Col}]),
        F(),
        erlang:trace(all, false, [call]),
        timer:sleep(50)
    end,
    lists:foreach(
        fun(I) ->
            Run(sched, fun() -> schedule(Process, I, Opts) end),
            Run(comp, fun() -> {ok, _} = hb_ao:resolve(Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => I }, Opts) end)
        end, lists:seq(21, 40)),
    erlang:trace_pattern({hb_store_lmdb, '_', '_'}, false, [global]),
    Col ! {done, self()},
    receive {counts, C} ->
        io:format(user, "TRACE ~p~n", [lists:sort(maps:to_list(C))])
    end.

collect(C, Phase0) ->
    receive
        {phase, P} -> put(phase, P), collect(C, Phase0);
        {trace, _, call, {hb_store_lmdb, F, [#{ <<"name">> := N } | Args]}} ->
            Store = lists:last(binary:split(N, <<"/">>, [global])),
            Bytes = case {F, Args} of
                        {write, [Req, _]} when is_map(Req) ->
                            maps:fold(fun(_, V, A) when is_binary(V) -> A + byte_size(V); (_, _, A) -> A end, 0, Req);
                        _ -> 0
                    end,
            Rows = case {F, Args} of {write, [Req2, _]} when is_map(Req2) -> map_size(Req2); _ -> 1 end,
            K = {get(phase), Store, F},
            {Cn, Rn, Bn} = maps:get(K, C, {0, 0, 0}),
            collect(C#{ K => {Cn + 1, Rn + Rows, Bn + Bytes} }, Phase0);
        {done, From} -> From ! {counts, C};
        _ -> collect(C, Phase0)
    end.

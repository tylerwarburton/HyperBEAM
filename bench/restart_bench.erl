%%% @doc After a node restart, does a process resume from its latest
%%% checkpoint, and how long until it is readable again?
%%%
%%% Two VMs on the same store directories, our node config
%%% (`process-state-blobs', `store-retention', irreplaceable store,
%%% checkpoint and snapshot every 1,000 slots). Copy to `src/core/test/',
%%% then `rebar3 device test --with-core --module=restart_bench' with
%%% RESTART_BENCH set to:
%%%   build:<device>:<slots>:<dir>   spawn a game-shaped process on
%%%       lua-patch@1.0, lua-atomic@1.0 or lua@5.3a, schedule and compute
%%%       <slots> slots with retention running, then `kill -9' this VM.
%%%   resume:<device>:<dir>   a new VM: schedule one more message and
%%%       compute it. Prints the time until the new slot is readable, the
%%%       Lua calls it took (a resume from the newest checkpoint replays at
%%%       most the slots after it; a replay from 0 calls Lua for every slot),
%%%       and the checkpoints retention left.
-module(restart_bench).
-include_lib("eunit/include/eunit.hrl").

bench_test_() ->
    case os:getenv("RESTART_BENCH") of
        false -> [];
        Spec -> {timeout, 36000, fun() -> run(string:split(Spec, ":", all)) end}
    end.

lmdb(Dir) -> #{ <<"store-module">> => hb_store_lmdb, <<"name">> => list_to_binary(Dir) }.

opts(Dir) ->
    hb_store_irreplaceable:node_message(#{
        <<"store">> => [lmdb(Dir ++ "/main")],
        <<"irreplaceable-store">> => lmdb(Dir ++ "/irr"),
        <<"priv-wallet">> => hb:wallet(),
        <<"spawn-worker">> => true,
        <<"process-worker-spawn">> => true,
        <<"process-workers">> => true,
        <<"await-inprogress">> => named,
        <<"scheduler-lookahead">> => false,
        <<"scheduler-publish-remote">> => false,
        <<"scheduling-mode">> => local_confirmation,
        <<"process-snapshot-target">> => false,
        <<"process-snapshot-slots">> => cadence(),
        <<"process-snapshot-time">> => 900,
        <<"process-delta-cache">> => true,
        <<"process-delta-checkpoint-slots">> => cadence(),
        <<"match-index">> => false,
        <<"process-state-blobs">> => true,
        <<"store-retention">> => true,
        <<"store-retention-first-ms">> => 5000,
        <<"store-retention-interval-ms">> => 60000
    }).

%% RESTART_CADENCE: checkpoint and snapshot every this many slots (1000).
cadence() -> list_to_integer(os:getenv("RESTART_CADENCE", "1000")).

patch_script() ->
    <<
        "Players = Players or {}\n"
        "Count = Count or 0\n"
        "local pad = string.rep('x', 2400)\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  local i = Count % 28\n"
        "  local p = Players[i] or { level = 0 }\n"
        "  p.level = p.level + 1\n"
        "  Players[i] = p\n"
        "  return {\n"
        "    patches = {\n"
        "      { path = '/player-' .. i, value = '{\"level\":' .. p.level .. ',\"pad\":\"' .. pad .. '\"}' },\n"
        "      { path = '/count', value = tostring(Count) }\n"
        "    },\n"
        "    results = { output = { data = tostring(Count) } }\n"
        "  }\n"
        "end\n"
    >>.

full_script() ->
    <<
        "local pad = string.rep('x', 2400)\n"
        "function compute(base, req, opts)\n"
        "  base.count = math.tointeger((base.count or 0) + 1)\n"
        "  local i = base.count % 28\n"
        "  base['player-' .. i] = '{\"slot\":' .. base.count .. ',\"pad\":\"' .. pad .. '\"}'\n"
        "  base.results = { output = { data = tostring(base.count) } }\n"
        "  return base\n"
        "end\n"
    >>.

script(<<"lua@5.3a">>) -> full_script();
script(_) ->
    case os:getenv("RESTART_PLAYERS") of
        false -> patch_script();
        N -> game_script(list_to_integer(N))
    end.

%% A game-shaped state of `Players' players (~300 bytes each, all published
%% at the first slot); every slot updates and publishes one of them.
game_script(Players) ->
    iolist_to_binary([
        "Players = Players or {}
"
        "Count = Count or 0
"
        "local N = ", integer_to_list(Players), "
"
        "local pad = string.rep('p', 200)
"
        "local function rec(i, p)
"
        "  return '{\"id\":' .. i .. ',\"level\":' .. p.level .. ',\"xp\":' .. p.xp .. ',\"pad\":\"' .. pad .. '\"}'
"
        "end
"
        "function compute(req)
"
        "  local patches = {}
"
        "  if Count == 0 then
"
        "    for i = 1, N do
"
        "      Players[i] = { level = 1, xp = 0 }
"
        "      patches[#patches + 1] = { path = '/player-' .. i, value = rec(i, Players[i]) }
"
        "    end
"
        "  end
"
        "  Count = Count + 1
"
        "  local i = (Count % N) + 1
"
        "  local p = Players[i]
"
        "  p.xp = p.xp + 7
"
        "  if p.xp > 100 then p.level = p.level + 1; p.xp = 0 end
"
        "  patches[#patches + 1] = { path = '/player-' .. i, value = rec(i, p) }
"
        "  patches[#patches + 1] = { path = '/count', value = tostring(Count) }
"
        "  return { patches = patches, results = { output = { data = tostring(Count) } } }
"
        "end
"]).

process(Device, Opts) ->
    Address = hb_util:human_id(ar_wallet:to_address(hb_opts:get(<<"priv-wallet">>, undefined, Opts))),
    hb_message:commit(#{
        <<"device">> => <<"process@1.0">>, <<"type">> => <<"Process">>,
        <<"scheduler-device">> => <<"scheduler@1.0">>,
        <<"execution-device">> => Device,
        <<"module">> => #{ <<"content-type">> => <<"application/lua">>, <<"body">> => script(Device) },
        <<"authority">> => [Address], <<"scheduler-location">> => Address,
        <<"test-random-seed">> => rand:uniform(1 bsl 40)
    }, Opts).

schedule(Process, N, Opts) ->
    ProcID = hb_message:id(Process, all, Opts),
    C = client_opts(Opts),
    {ok, _} = hb_ao:resolve(Process, #{
        <<"path">> => <<"schedule">>, <<"method">> => <<"POST">>,
        <<"body">> => hb_message:commit(#{
            <<"target">> => ProcID, <<"type">> => <<"Message">>,
            <<"action">> => <<"Play">>, <<"number">> => N }, C) }, Opts).

client_opts(Opts) ->
    case get(client_store) of
        undefined -> put(client_store, hb_test_utils:test_store(hb_store_lmdb, <<"restart-client">>));
        _ -> ok
    end,
    Opts#{ <<"store">> => [get(client_store)] }.

compute(Process, N, Opts) ->
    {ok, State} = hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => N }, Opts),
    State.

pstate(Store, ProcID, Kind) ->
    case hb_store:list([Store], <<"pstate/", ProcID/binary, "/", Kind/binary>>, #{}) of
        {ok, L} -> lists:sort([ binary_to_integer(X) || X <- L ]);
        _ -> []
    end.

say(F, A) -> io:format(user, F ++ "~n", A).

du(Dir) ->
    case string:lexemes(os:cmd("du -sk " ++ Dir ++ " 2>/dev/null"), "	
") of
        [K | _] -> list_to_integer(K);
        _ -> 0
    end.

run(["serve", Dir, Port]) ->
    % A real node on the same stores, for an HTTP client: runs until killed.
    % RESTART_CONFIG: a node config file (the test node's rendered JSON),
    % its store paths moved under `Dir'.
    hb:init(),
    ok = filelib:ensure_dir(Dir ++ "/x"),
    Base =
        case os:getenv("RESTART_CONFIG") of
            false ->
                (opts(Dir))#{ <<"process-now-from-cache">> => true,
                              <<"process-worker-max-idle">> => 86400000 };
            File ->
                {ok, Conf} = hb_opts:load(File, #{}),
                relocate(Conf, list_to_binary(Dir))
        end,
    Url = hb_http_server:start_node(Base#{
        <<"port">> => list_to_integer(Port),
        <<"priv-wallet">> => hb:wallet(),
        <<"rate-limit-requests">> => 10000000,
        <<"rate-limit-max">> => 10000000,
        % The restore's own events (`snapshot_load_res', `found_state_checkpoint').
        <<"debug-print">> => [compute]
    }),
    say("SERVING ~s", [Url]),
    receive after infinity -> ok end;
run(["probe", Dir, P, Limit0]) ->
    % Ask the stored process cache what a cold node's restore asks it.
    hb:init(),
    {ok, Conf} = hb_opts:load(os:getenv("RESTART_CONFIG"), #{}),
    Opts0 = hb_store_irreplaceable:node_message(
        hb_store:normalize_opts(relocate(Conf, list_to_binary(Dir)))),
    Opts = Opts0#{ <<"priv-wallet">> => hb:wallet() },
    _ = hb_device:message_to_device(#{ <<"device">> => <<"process@1.0">> }, Opts),
    [Cache | _] = [ M || {M, _} <- code:all_loaded(),
                    lists:prefix("_hb_device_process_1_0", atom_to_list(M)),
                    lists:suffix("__process_cache", atom_to_list(M)) ],
    Limit = list_to_integer(Limit0),
    PB = list_to_binary(P),
    Res = [ {L, catch element(1, Cache:latest(PB, [<<"snapshot">>], L, Opts)),
             catch element(2, Cache:latest(PB, [<<"snapshot">>], L, Opts))}
          || L <- [Limit, integer_to_binary(Limit), undefined] ],
    say("PROBE store=~p~nPROBE latest(snapshot)=~p", [hb_opts:get(<<"store">>, [], Opts), Res]);
run(["build", Device0, N0, Dir]) ->
    hb:init(),
    Device = list_to_binary(Device0),
    N = list_to_integer(N0),
    os:cmd("rm -rf " ++ Dir),
    Opts = opts(Dir),
    ok = hb_store_retention:maybe_start(Opts),
    Process = process(Device, Opts),
    Sched = hb_opts:get(<<"scheduler-store">>, [], Opts),
    {ok, _} = hb_cache:write(Process, Opts#{ <<"store">> => Sched }),
    ProcID = hb_util:human_id(hb_message:id(Process, all, Opts)),
    ok = file:write_file(Dir ++ "/proc", term_to_binary({ProcID, N})),
    T0 = erlang:monotonic_time(millisecond),
    Cadence = cadence(),
    Times =
        lists:map(
            fun(I) ->
                {T, _} = timer:tc(fun() -> schedule(Process, I, Opts), _ = compute(Process, I, Opts) end),
                case I rem 5000 of
                    0 when I > 0 -> say("BUILD ~s slot=~p", [Device, I]);
                    _ -> ok
                end,
                {I, T div 1000}
            end,
            lists:seq(0, N - 1)),
    [Main | _] = hb_opts:get(<<"store">>, [], Opts),
    Med = fun([]) -> 0; (L) -> lists:nth(length(L) div 2 + 1, lists:sort(L)) end,
    Mean = fun([]) -> 0; (L) -> lists:sum(L) div length(L) end,
    Ckpt = [ T || {I, T} <- Times, I > 0, I rem Cadence == 0 ],
    Other = [ T || {I, T} <- Times, I > 0, I rem Cadence =/= 0 ],
    say("BUILD ~s cadence=~p done slots=~p ms_per_slot=~p median_other=~p checkpoint_slot_mean=~p "
        "(n=~p) main_kib=~p irr_kib=~p snapshots=~p",
        [Device, Cadence, N, (erlang:monotonic_time(millisecond) - T0) div N, Med(Other), Mean(Ckpt),
         length(Ckpt), du(Dir ++ "/main"), du(Dir ++ "/irr"), pstate(Main, ProcID, <<"s">>)]),
    % A crash, not a clean stop.
    os:cmd("kill -9 " ++ os:getpid());
run(["resume", Device0, Dir]) ->
    T0 = erlang:monotonic_time(millisecond),
    hb:init(),
    Opts = opts(Dir),
    {ok, B} = file:read_file(Dir ++ "/proc"),
    {ProcID, N} = binary_to_term(B),
    [Main | _] = hb_opts:get(<<"store">>, [], Opts),
    Snaps = pstate(Main, ProcID, <<"s">>),
    {ok, P0} = hb_cache:read(ProcID, Opts),
    Process = hb_cache:ensure_all_loaded(P0, Opts),
    % A read of the newest stored slot: served from the state rows.
    TR0 = erlang:monotonic_time(millisecond),
    Last = hb_ao:get(<<"count">>, hb_cache:ensure_all_loaded(compute(Process, N - 1, Opts), Opts), Opts),
    ReadMs = erlang:monotonic_time(millisecond) - TR0,
    Self = self(),
    Counter = spawn(fun() -> count(0, Self) end),
    % A pattern on a module not loaded yet matches nothing.
    {module, luerl} = code:ensure_loaded(luerl),
    erlang:trace_pattern({luerl, call_function_dec, 3}, true, [global]),
    erlang:trace(all, true, [call, {tracer, Counter}]),
    T1 = erlang:monotonic_time(millisecond),
    schedule(Process, N, Opts),
    State = compute(Process, N, Opts),
    T2 = erlang:monotonic_time(millisecond),
    erlang:trace(all, false, [call]),
    erlang:trace_pattern({luerl, call_function_dec, 3}, false, [global]),
    Counter ! {count, self()},
    Calls = receive {calls, C} -> C after 5000 -> timeout end,
    Count = hb_ao:get(<<"count">>, hb_cache:ensure_all_loaded(State, Opts), Opts),
    Newest = case Snaps of [] -> none; _ -> lists:last(Snaps) end,
    say("RESUME ~s cadence=~p slots=~p read_last_ms=~p (count ~p) next_slot_ms=~p "
        "(from VM start ~p) lua_calls=~p newest=~p count=~p resumed_from_checkpoint=~p snapshots_on_disk=~p",
        [Device0, cadence(), N + 1, ReadMs, Last, T2 - T1, T2 - T0, Calls, Newest, Count,
         is_integer(Calls) andalso Newest =/= none andalso Calls =< (N - Newest) + 10, Snaps]).

count(N, Parent) ->
    receive
        {trace, _, call, _} -> count(N + 1, Parent);
        {count, From} -> From ! {calls, N}
    end.

relocate(<<"/app/cache-mainnet", Rest/binary>>, Dir) -> <<Dir/binary, "/cm", Rest/binary>>;
relocate(<<"/app/cache-arweave", Rest/binary>>, Dir) -> <<Dir/binary, "/ca", Rest/binary>>;
relocate(M, Dir) when is_map(M) -> maps:map(fun(_, V) -> relocate(V, Dir) end, M);
relocate(L, Dir) when is_list(L) -> [ relocate(V, Dir) || V <- L ];
relocate(V, _Dir) -> V.

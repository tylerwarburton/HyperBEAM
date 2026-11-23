%%% @doc ms per game slot, on a game-SIZED `lua-patch@1.0' process.
%%%
%%% The required check before a node build ships (PARALITH.md, "Ship gate").
%%% Slot 0 seeds `Players' players of ~3 KB each, each published as its own
%%% `player-<n>' key (the test-net game: 300 players, ~1 MB, ~430 keys); every
%%% later message rewrites one player, as `Sprite.Update' does. Each slot is
%%% scheduled and then computed through the process worker, in sequence, with
%%% the test node's store options (state blobs, retention, LMDB sync).
%%%
%%% The player count is the point. `durability_bench' holds 28 players, and
%%% a per-slot cost that grows with the state (the worker committing the state
%%% ~70 times a slot, 167 ms of worker time here) is invisible at that size:
%%% it measured ~47 ms/slot while the 300-player test-net game ran ~500.
%%%
%%% Run inside a build:
%%%   GAME_BENCH=<players>:<slots>:<dir>[:<max-worker-ms>] \
%%%     rebar3 device test --with-core --module=game_slot_bench
%%% Prints one `GAME_BENCH' line; fails when the worker's mean busy time per
%%% slot (`worker_execute') is above <max-worker-ms> (default 20).
-module(game_slot_bench).
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_MAX_WORKER_MS, 20).

bench_test_() ->
    case os:getenv("GAME_BENCH") of
        false -> [];
        Spec ->
            {Players, N, Dir, Max} =
                case string:split(Spec, ":", all) of
                    [P, S, D] -> {list_to_integer(P), list_to_integer(S), D,
                                  ?DEFAULT_MAX_WORKER_MS};
                    [P, S, D, M] -> {list_to_integer(P), list_to_integer(S), D,
                                     list_to_integer(M)}
                end,
            {timeout, 3600, fun() -> run(Players, N, Dir, Max) end}
    end.

%% `GAME_BENCH_BURST=<n>' then schedules n more messages before computing
%% any, so the worker starts with a backlog of n, and `GAME_BENCH_DUMP=<file>'
%% writes the message trace (`/~hyperbuddy@1.0/trace') to a file.
burst(Process, Last, Opts) ->
    case os:getenv("GAME_BENCH_BURST") of
        false -> ok;
        B ->
            Burst = list_to_integer(B),
            [ schedule(Process, <<"Play">>, I, Opts)
            || I <- lists:seq(Last + 1, Last + Burst) ],
            compute(Process, Last + Burst, Opts)
    end,
    case os:getenv("GAME_BENCH_DUMP") of
        false -> ok;
        File ->
            ok = file:write_file(File,
                hb_json:encode(hb_msg_trace:report(0, 5000)))
    end.

%% `GAME_BENCH_FS=true' puts an `hb_store_fs' store under the LMDB one, as
%% every node runs (`/app/cache-mainnet'): a miss in LMDB (a message's
%% commitments, every compute) is then a read or a list of the filesystem.
opts(Dir) ->
    FS = case os:getenv("GAME_BENCH_FS") of
             "true" -> [#{ <<"store-module">> => hb_store_fs,
                           <<"name">> => list_to_binary(Dir ++ "/fs") }];
             _ -> []
         end,
    #{
        <<"store">> => [#{ <<"store-module">> => hb_store_lmdb,
                           <<"name">> => list_to_binary(Dir ++ "/main") } | FS],
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
        <<"process-hot-cache-slots">> => 512,
        <<"process-state-blobs">> => true,
        <<"store-retention">> => true,
        <<"match-index">> => false
    }.

script(Players) ->
    iolist_to_binary([
        "NPlayers = ", integer_to_list(Players), "\n"
        "Players = Players or {}\n"
        "Count = Count or 0\n"
        "local pad = string.rep('x', 2800)\n"
        "local function rec(i, p)\n"
        "  return '{\"level\":' .. p.level .. ',\"wins\":' .. p.wins ..\n"
        "    ',\"sprite\":' .. p.sprite .. ',\"pad\":\"' .. pad .. '\"}'\n"
        "end\n"
        "function compute(req)\n"
        "  local body = req.body or req\n"
        "  Count = Count + 1\n"
        "  local patches = {}\n"
        "  if tostring(body.action) == 'Seed' then\n"
        "    for i = 1, NPlayers do\n"
        "      Players[i] = { level = 1, wins = 0, sprite = 0 }\n"
        "      patches[#patches + 1] = { path = '/player-' .. i, value = rec(i, Players[i]) }\n"
        "    end\n"
        "  else\n"
        "    local i = (Count % NPlayers) + 1\n"
        "    local p = Players[i]\n"
        "    p.sprite = p.sprite + 1\n"
        "    p.wins = p.wins + (Count % 3)\n"
        "    patches[1] = { path = '/player-' .. i, value = rec(i, p) }\n"
        "  end\n"
        "  patches[#patches + 1] = { path = '/count', value = tostring(Count) }\n"
        "  return { patches = patches, results = { output = { data = tostring(Count) } } }\n"
        "end\n"
    ]).

process(Players, Opts) ->
    Wallet = hb_opts:get(<<"priv-wallet">>, undefined, Opts),
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    hb_message:commit(
        #{
            <<"device">> => <<"process@1.0">>,
            <<"type">> => <<"Process">>,
            <<"scheduler-device">> => <<"scheduler@1.0">>,
            <<"execution-device">> => <<"lua-patch@1.0">>,
            <<"module">> => #{ <<"content-type">> => <<"application/lua">>,
                               <<"body">> => script(Players) },
            <<"authority">> => [Address],
            <<"scheduler-location">> => Address,
            <<"test-random-seed">> => rand:uniform(1000000)
        },
        Opts).

schedule(Process, Action, N, Opts) ->
    ProcID = hb_message:id(Process, all, Opts),
    C = client_opts(Opts),
    Req = hb_message:commit(#{
        <<"path">> => <<"schedule">>, <<"method">> => <<"POST">>,
        <<"body">> => hb_message:commit(#{
            <<"target">> => ProcID, <<"type">> => <<"Message">>,
            <<"action">> => Action, <<"number">> => N }, C) }, C),
    {ok, _} = hb_ao:resolve(Process, Req, Opts).

client_opts(Opts) ->
    case get(client_store) of
        undefined ->
            put(client_store,
                hb_test_utils:test_store(hb_store_lmdb, <<"game-bench-client">>));
        _ -> ok
    end,
    Opts#{ <<"store">> => [get(client_store)] }.

compute(Process, Slot, Opts) ->
    {ok, _} = hb_ao:resolve(Process,
        #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, Opts).

%% `GAME_BENCH_FS_STALL=<hold-ms>:<every-ms>' holds the node's one
%% `file_server_2' for `hold-ms' of every `every-ms' while the slots run, as
%% a call on a slow or hung NFS share holds it (prod, 2026-11-23: >= 110 ms a
%% call, up to ~60 s hung). Every non-`raw' `file' call waits behind it.
stall_file_server() ->
    case os:getenv("GAME_BENCH_FS_STALL") of
        false -> spawn(fun() -> receive stop -> ok end end);
        Spec ->
            [Hold, Every] = [ list_to_integer(X) || X <- string:split(Spec, ":") ],
            FS = whereis(file_server_2),
            Loop =
                fun L() ->
                    true = erlang:suspend_process(FS),
                    timer:sleep(Hold),
                    true = erlang:resume_process(FS),
                    receive stop -> ok after max(0, Every - Hold) -> L() end
                end,
            spawn_link(Loop)
    end.

run(Players, N, Dir0, MaxWorkerMs) ->
    hb:init(),
    % `GAME_BENCH_TRACE=false' measures the same build with the message
    % trace off (`hb_msg_trace'), for its overhead.
    hb_phase_timer:configure(
        #{ <<"msg-trace">> => os:getenv("GAME_BENCH_TRACE") =/= "false" }),
    Dir = Dir0 ++ "/game",
    os:cmd("rm -rf " ++ Dir),
    Opts = opts(Dir),
    Process = process(Players, Opts),
    {ok, _} = hb_cache:write(Process, Opts),
    schedule(Process, <<"Seed">>, 0, Opts),
    compute(Process, 0, Opts),
    % Warm up: the worker is live and the state is game-sized.
    lists:foreach(
        fun(I) -> schedule(Process, <<"Play">>, I, Opts), compute(Process, I, Opts) end,
        lists:seq(1, 10)),
    hb_phase_timer:reset(),
    Stall = stall_file_server(),
    % Schedule and compute are timed apart: the compute is the one process
    % worker every message to the process queues behind, so it sets the
    % process's ceiling (1000 / compute ms messages a second). The schedule
    % includes this bench signing two RSA messages as a client.
    {Sched, Comp} =
        lists:unzip(
            [ begin
                  {TS, _} = timer:tc(fun() -> schedule(Process, <<"Play">>, I, Opts) end),
                  {TC, _} = timer:tc(fun() -> compute(Process, I, Opts) end),
                  {TS, TC}
              end
            || I <- lists:seq(11, 10 + N) ]),
    Stall ! stop,
    Med = fun(L) -> lists:nth(length(L) div 2 + 1, lists:sort(L)) / 1000 end,
    P95 = fun(L) -> lists:nth(max(1, trunc(length(L) * 0.95)), lists:sort(L)) / 1000 end,
    Phases = [ {P, round(Mean / 100) / 10} || {P, _T, _C, Mean} <- hb_phase_timer:read() ],
    {ok, Count} = hb_ao:resolve(Process, <<"now/count">>, Opts),
    % The worker's busy time per slot is the gated number: it is serial, so
    % it is the process's ceiling (1000 / worker_ms slots a second). The
    % compute round trip adds the caller's side, which runs in parallel.
    WorkerMs = proplists:get_value(worker_execute, Phases, 0.0),
    io:format(user,
        "GAME_BENCH players=~p slots=~p worker_ms=~.1f (max ~p) compute_med=~.1f ms "
        "compute_p95=~.1f ms schedule_med=~.1f ms count=~s | phase means ms ~p~n",
        [Players, N, WorkerMs, MaxWorkerMs, Med(Comp), P95(Comp), Med(Sched),
         Count, Phases]),
    burst(Process, 11 + N, Opts),
    ?assertEqual(integer_to_binary(11 + N), Count),
    ?assert(WorkerMs > 0),
    ?assert(WorkerMs =< MaxWorkerMs).

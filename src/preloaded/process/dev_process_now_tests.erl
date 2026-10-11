%%% @doc Tests and benchmarks for `now/<key>' key reads
%%% (`process-now-key-reads', `dev_process:latest_key/3').
%%%
%%% Run with `rebar3 device test --module dev_process_now_tests'. The
%%% benchmark report runs only with `HB_NOW_BENCH' set to a `:'-separated list
%%% of state sizes in MB, e.g. `HB_NOW_BENCH=1:50'.
-module(dev_process_now_tests).
-device_libraries([lib_process]).
-include_lib("eunit/include/eunit.hrl").
-include("include/hb.hrl").

%% Players per MB of state (`player/1' is ~2.2 KB on the heap).
-define(PLAYERS_PER_MB, 470).

%%% Fixtures

opts(Extra) ->
    hb:init(),
    application:ensure_all_started(hb),
    application:ensure_all_started(inets),
    Store = hb_test_utils:test_store(hb_store_lmdb),
    ok = hb_store:start([Store], #{}, #{}),
    maps:merge(
        #{
            <<"store">> => [Store],
            <<"priv-wallet">> => ar_wallet:new(),
            <<"process-now-from-cache">> => true,
            <<"process-workers">> => true,
            <<"spawn-worker">> => true,
            <<"await-inprogress">> => <<"named">>,
            <<"match-index">> => false
        },
        Extra
    ).

process(Script, Opts) ->
    Wallet = hb_opts:get(<<"priv-wallet">>, hb:wallet(), Opts),
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    Process =
        hb_message:commit(
            #{
                <<"device">> => <<"process@1.0">>,
                <<"type">> => <<"Process">>,
                <<"scheduler-device">> => <<"scheduler@1.0">>,
                <<"execution-device">> => <<"lua@5.3b">>,
                <<"module">> => #{
                    <<"content-type">> => <<"application/lua">>,
                    <<"body">> => Script
                },
                <<"authority">> => [Address],
                <<"scheduler-location">> => Address,
                <<"test-random-seed">> => rand:uniform(1000000)
            },
            Opts
        ),
    {ok, _} = hb_cache:write(Process, Opts),
    Process.

process_id(Process, Opts) ->
    lib_process:process_id(lib_process:ensure_process_key(Process, Opts), #{}, Opts).

%% A Lua process whose state gains a player table (nested maps and a list) and
%% a mixed-case key every slot.
lua_script() ->
    <<
        "Count = Count or 0\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  return {\n"
        "    patches = {\n"
        "      { path = '/count', value = tostring(Count) },\n"
        "      { path = '/player-' .. Count, value = {\n"
        "          name = 'p' .. Count, hp = Count * 3,\n"
        "          inv = { 'sword', 'shield' },\n"
        "          stats = { str = Count, dex = 2 } } },\n"
        "      { path = '/Mixed-Case', value = 'mc' .. Count }\n"
        "    },\n"
        "    results = { output = { data = tostring(Count) } }\n"
        "  }\n"
        "end\n"
    >>.

schedule(Process, Slots, Opts) ->
    ProcID = hb_message:id(Process, all, Opts),
    lists:foreach(
        fun(N) ->
            Req =
                hb_message:commit(
                    #{
                        <<"path">> => <<"schedule">>,
                        <<"method">> => <<"POST">>,
                        <<"body">> =>
                            hb_message:commit(
                                #{
                                    <<"target">> => ProcID,
                                    <<"type">> => <<"Message">>,
                                    <<"action">> => <<"Increment">>,
                                    <<"number">> => N
                                },
                                Opts
                            )
                    },
                    Opts
                ),
            {ok, _} = hb_ao:resolve(Process, Req, Opts)
        end,
        Slots
    ).

%% Compute `Slot' in the test process, as the worker would: the result lands
%% in the process cache (store and hot cache).
compute(Process, Slot, Opts) ->
    {ok, _} =
        hb_ao:resolve(
            Process,
            #{ <<"path">> => <<"compute">>, <<"slot">> => Slot },
            Opts#{ <<"process-workers">> => false, <<"spawn-worker">> => false }
        ).

player(I) ->
    Addr = hb_util:human_id(crypto:hash(sha256, integer_to_binary(I))),
    #{
        <<"address">> => Addr,
        <<"element">> => <<"water">>,
        <<"faction">> => <<"Sky Nomads">>,
        <<"level">> => I rem 50,
        <<"losses">> => I rem 7,
        <<"wins">> => I rem 11,
        <<"gold">> => I * 3,
        <<"energy">> => 100,
        <<"happiness">> => 90,
        <<"lastDaily">> => 1760000000 + I,
        <<"monster">> =>
            #{
                <<"attack">> => 12, <<"defense">> => 4,
                <<"elementType">> => <<"water">>,
                <<"entryKey">> => <<"brookfrog-s1">>, <<"entryNo">> => 16,
                <<"evolutionStage">> => 1, <<"faction">> => <<"Wild">>,
                <<"health">> => 5, <<"level">> => 5,
                <<"moves">> =>
                    #{
                        <<"Battle Cry">> => #{ <<"count">> => 3 },
                        <<"Ocean Mist">> => #{ <<"count">> => 3 },
                        <<"Whirlpool">> => #{ <<"count">> => 4 }
                    },
                <<"name">> => <<"Brookfrog">>, <<"nextLevelExp">> => 13,
                <<"speed">> => 7, <<"exp">> => I rem 13
            },
        <<"inventory">> =>
            #{
                <<"berries">> => #{ <<"red">> => 2, <<"blue">> => 1 },
                <<"scrolls">> => 1
            },
        <<"history">> => [ I + K || K <- lists:seq(1, 8) ]
    }.

%% A game-like state of `Players' player records written as the newest slot
%% of a process, the way `dev_process' caches a computed slot.
big_state(Players, Extra) ->
    Opts = opts(Extra),
    Process = process(<<"function compute(r) return {} end">>, Opts),
    ProcID = process_id(Process, Opts),
    State = big_state_at(Process, Players, 7),
    {ok, _} = dev_process_cache:write(ProcID, 7, State, Opts),
    {Process, ProcID, State, Opts}.

big_state_at(Process, Players, Slot) ->
    maps:merge(
        Process#{
            <<"device">> => <<"process@1.0">>,
            <<"process">> => Process,
            <<"at-slot">> => Slot,
            <<"results">> =>
                #{ <<"output">> => #{ <<"data">> => integer_to_binary(Slot) } },
            <<"leaderboard">> => [ player(I) || I <- lists:seq(1, 10) ]
        },
        maps:from_list(
            [ {<<"player-", (integer_to_binary(I))/binary>>, player(I)}
            || I <- lists:seq(1, Players) ]
        )
    ).

with_mode(Opts, Mode) -> Opts#{ <<"process-now-key-reads">> => Mode }.

%% Two nodes on one store, key reads off and on. A node's server is named by
%% its wallet, so each needs its own.
test_nodes(Opts) ->
    {
        hb_http_server:start_node(
            (with_mode(Opts, false))#{ <<"priv-wallet">> => ar_wallet:new() }),
        hb_http_server:start_node(
            (with_mode(Opts, true))#{ <<"priv-wallet">> => ar_wallet:new() })
    }.

now_path(ProcID, <<>>) ->
    <<"/", ProcID/binary, "~process@1.0/now">>;
now_path(ProcID, Key) ->
    <<"/", ProcID/binary, "~process@1.0/now/", Key/binary>>.

%% The raw HTTP response: status, headers but `date', body.
http_raw(Node, Path) ->
    Local = binary:replace(Node, <<"localhost">>, <<"127.0.0.1">>),
    URL = binary_to_list(<<Local/binary, (binary:part(Path, 1, byte_size(Path) - 1))/binary>>),
    {ok, {{_, Status, _}, Headers, Body}} =
        httpc:request(get, {URL, []}, [{timeout, 60000}], [{body_format, binary}]),
    {Status, lists:sort([ H || H = {K, _} <- Headers, K =/= "date" ]), Body}.

%%% Tests

%% @doc Every response -- found key, nested path, list, at-slot, missing key,
%% case-insensitive fallback, exported keys, the whole state -- is identical
%% with key reads on and off: over HTTP byte for byte, and in-process as
%% terms, hashpath included. Also after the process advances.
same_responses_test_() ->
    {timeout, 300, fun() ->
        Opts = opts(#{}),
        Process = process(lua_script(), Opts),
        ProcID = process_id(Process, Opts),
        schedule(Process, lists:seq(0, 4), Opts),
        compute(Process, 2, Opts),
        {Off, On} = test_nodes(Opts),
        Keys =
            [
                <<"at-slot">>, <<"count">>, <<"player-1">>, <<"player-3">>,
                <<"player-2/stats/str">>, <<"player-1/inv">>,
                <<"results">>, <<"results/output/data">>,
                <<"Mixed-Case">>, <<"mixed-case">>, <<"MIXED-CASE">>,
                <<"no-such-key">>, <<"player-99/name">>,
                <<"keys">>, <<"process/type">>, <<"device">>
            ],
        Compare =
            fun(Label) ->
                lists:foreach(
                    fun(Key) ->
                        Path = now_path(ProcID, Key),
                        ?assertEqual(
                            {Label, Key, http_raw(Off, Path)},
                            {Label, Key, http_raw(On, Path)}
                        ),
                        InProc = <<"now/", Key/binary>>,
                        ?assertEqual(
                            {Label, Key,
                                hb_ao:resolve(Process, InProc, with_mode(Opts, false))},
                            {Label, Key,
                                hb_ao:resolve(Process, InProc, with_mode(Opts, true))}
                        )
                    end,
                    Keys
                ),
                % The whole state (no key: no key read) is unchanged too. Over
                % HTTP it renders the node's own index page, which differs
                % between the two nodes, so it is compared in-process.
                ?assertMatch({200, _, _}, http_raw(On, now_path(ProcID, <<"">>))),
                ?assertEqual(
                    hb_ao:resolve(Process, <<"now">>, with_mode(Opts, false)),
                    hb_ao:resolve(Process, <<"now">>, with_mode(Opts, true))
                )
            end,
        Compare(slot_2),
        ?assertMatch({200, _, <<"2">>}, http_raw(On, now_path(ProcID, <<"at-slot">>))),
        ?assertMatch({404, _, _}, http_raw(On, now_path(ProcID, <<"no-such-key">>))),
        % The process advances: the next read sees the new slot at once.
        compute(Process, 4, Opts),
        ?assertMatch({200, _, <<"4">>}, http_raw(On, now_path(ProcID, <<"at-slot">>))),
        ?assertMatch({200, _, <<"5">>}, http_raw(On, now_path(ProcID, <<"count">>))),
        ?assertMatch({200, _, <<"p5">>}, http_raw(On, now_path(ProcID, <<"player-5/name">>))),
        Compare(slot_4)
    end}.

%% @doc The `now' step of a key read returns a state holding only that key
%% (and the fields that decide how it resolves), never the whole state.
key_read_returns_only_the_key_test() ->
    % A reader with anything in its mailbox takes the full path (see
    % `key_read_with_listeners_sends_whole_state_test'): read from a fresh one.
    in_fresh_process(fun key_read_returns_only_the_key/0).

key_read_returns_only_the_key() ->
    {Process, _ProcID, _State, Opts} =
        big_state(200, #{ <<"process-now-key-reads">> => true }),
    Now =
        fun(Key) ->
            hb_ao:resolve(
                Process,
                #{
                    <<"path">> => <<"now">>,
                    <<"priv">> => #{ <<"ao-next-key">> => Key }
                },
                Opts
            )
        end,
    {ok, Found} = Now(<<"player-7">>),
    ?assertEqual(
        lists:sort([<<"device">>, <<"player-7">>, <<"priv">>]),
        lists:sort(maps:keys(Found))
    ),
    ?assertEqual(player(7), maps:get(<<"player-7">>, Found)),
    ?assert(dev_process:is_cached_state(Found)),
    {ok, Missing} = Now(<<"no-such-key">>),
    ?assertEqual(lists:sort([<<"device">>, <<"priv">>]), lists:sort(maps:keys(Missing))),
    % An exported key is no plain read: the whole state answers.
    {ok, Whole} = Now(<<"keys">>),
    ?assert(maps:size(Whole) > 200),
    % Without the option, as before.
    {ok, Old} = hb_ao:resolve(Process, #{
            <<"path">> => <<"now">>,
            <<"priv">> => #{ <<"ao-next-key">> => <<"player-7">> }
        },
        with_mode(Opts, false)
    ),
    ?assert(maps:size(Old) > 200),
    % The hint is set by AO-Core for `now' followed by one key.
    {ok, Read} = hb_ao:resolve(Process, <<"now/player-7">>, Opts),
    ?assertEqual(player(7), hb_private:reset(Read)).

%% @doc A reader with listeners in its mailbox takes the full path, so that
%% `notify_compute/4' sends them the whole state, as before.
key_read_with_listeners_sends_whole_state_test() ->
    {Process, ProcID, _State, Opts} =
        big_state(50, #{ <<"process-now-key-reads">> => true }),
    self() ! {resolve, self(), ProcID, #{ <<"path">> => <<"compute">> }, #{}},
    ?assertEqual({ok, 7}, hb_ao:resolve(Process, <<"now/at-slot">>, Opts)),
    receive
        {resolved, _, ProcID, {slot, 7}, {ok, Notified}} ->
            ?assert(maps:is_key(<<"player-50">>, Notified))
    after 1000 -> erlang:error(listener_not_notified)
    end.

%% @doc Key reads run ungrouped, so readers of different keys of one process
%% never share a result, even under `await-inprogress: true'.
key_read_is_ungrouped_test() ->
    {Process, _ProcID, _State, Opts} =
        big_state(5, #{ <<"process-now-key-reads">> => true }),
    Req =
        #{
            <<"path">> => <<"now">>,
            <<"priv">> => #{ <<"ao-next-key">> => <<"player-1">> }
        },
    AwaitAll =
        Opts#{ <<"await-inprogress">> => true, <<"spawn-worker">> => false },
    ?assertEqual(ungrouped_exec, hb_persistent:group(Process, Req, AwaitAll)),
    ?assertNotEqual(
        ungrouped_exec,
        hb_persistent:group(Process, Req, with_mode(AwaitAll, false))
    ),
    % Readers of different keys in parallel each get their own key.
    Self = self(),
    lists:foreach(
        fun(I) ->
            spawn(fun() ->
                Key = <<"now/player-", (integer_to_binary(I))/binary>>,
                Self ! {read, I, hb_ao:resolve(Process, Key, AwaitAll)}
            end)
        end,
        lists:seq(1, 5)
    ),
    lists:foreach(
        fun(I) ->
            receive {read, I, {ok, Res}} -> ?assertEqual(player(I), hb_private:reset(Res))
            after 30000 -> erlang:error({no_read, I})
            end
        end,
        lists:seq(1, 5)
    ).

%% @doc A key read copies only its key: the garbage 50 reads leave behind is a
%% fraction of one copy of the state, where the full path leaves 50 copies.
%% It holds nothing of its own either: the hot cache is no bigger after them.
key_read_does_not_copy_state_test_() ->
    {timeout, 300, fun() ->
        {Process, _ProcID, State, Opts} =
            big_state(5000, #{ <<"process-now-key-reads">> => true }),
        StateBytes = erts_debug:flat_size(State) * erlang:system_info(wordsize),
        HotBytes =
            fun() ->
                ets:info(dev_process_delta_hot_cache, memory)
                    * erlang:system_info(wordsize)
            end,
        Garbage =
            fun(Mode) ->
                ModeOpts = with_mode(Opts, Mode),
                {ok, _} = hb_ao:resolve(Process, <<"now/player-9">>, ModeOpts),
                Run =
                    fun() ->
                        {_, Before, _} = erlang:statistics(garbage_collection),
                        [
                            {ok, _} = hb_ao:resolve(Process, <<"now/player-9">>, ModeOpts)
                        ||
                            _ <- lists:seq(1, 50)
                        ],
                        erlang:garbage_collect(),
                        {_, After, _} = erlang:statistics(garbage_collection),
                        (After - Before) * erlang:system_info(wordsize)
                    end,
                {Pid, Ref} = spawn_monitor(fun() -> exit({garbage, Run()}) end),
                receive {'DOWN', Ref, process, Pid, {garbage, Bytes}} -> Bytes end
            end,
        Hot0 = HotBytes(),
        New = Garbage(true),
        ?assertEqual(Hot0, HotBytes()),
        Old = Garbage(false),
        ?event(debug_test, {garbage, {state, StateBytes}, {key_reads, New}, {full, Old}}),
        ?assert(Old > 10 * StateBytes),
        ?assert(New < StateBytes)
    end}.

in_fresh_process(Fun) ->
    {Pid, Ref} = spawn_monitor(fun() -> exit({done, catch Fun()}) end),
    receive
        {'DOWN', Ref, process, Pid, {done, ok}} -> ok;
        {'DOWN', Ref, process, Pid, {done, {'EXIT', Reason}}} -> erlang:error(Reason);
        {'DOWN', Ref, process, Pid, {done, Other}} -> Other
    end.

%%% Benchmark

benchmark_report_test_() ->
    {timeout, 3600, fun() ->
        case os:getenv("HB_NOW_BENCH") of
            false -> ok;
            Spec ->
                Sizes = [ list_to_integer(S) || S <- string:tokens(Spec, ":") ],
                [ io:format(user, "NOW_BENCH ~p~n", [bench(MB)]) || MB <- Sizes ],
                io:format(user, "NOW_BENCH_ETS ~p~n", [bench_ets_tables()])
        end
    end}.

%% Per-read cost, in-process and over HTTP, with key reads off and on; then a
%% worker-like loop's iteration times while readers read at a fixed rate.
bench(MB) ->
    Players = max(1, MB * ?PLAYERS_PER_MB),
    {Process, ProcID, State, Opts} = big_state(Players, #{}),
    Key = <<"player-", (integer_to_binary(Players div 2))/binary>>,
    {NodeOff, Node} = test_nodes(Opts),
    Path = now_path(ProcID, Key),
    PerRead =
        fun(F, N) ->
            F(),
            Times = [ element(1, timer:tc(F)) || _ <- lists:seq(1, N) ],
            pct(Times)
        end,
    Res =
        [
            {Mode,
                #{
                    in_process_us =>
                        PerRead(fun() ->
                            {ok, _} =
                                hb_ao:resolve(
                                    Process,
                                    <<"now/", Key/binary>>,
                                    with_mode(Opts, Mode)
                                )
                        end, 30),
                    http_us =>
                        PerRead(fun() ->
                            {200, _, _} =
                                http_raw(
                                    case Mode of true -> Node; false -> NodeOff end,
                                    Path
                                )
                        end, 30),
                    interference =>
                        interference(Process, Key, with_mode(Opts, Mode))
                }
            }
        ||
            Mode <- [false, true]
        ],
    #{
        state_mb => erts_debug:flat_size(State) * 8 div 1048576,
        players => Players,
        results => Res
    }.

%% A stand-in for the process worker: each iteration builds and drops a ~2 MB
%% term, as a Lua slot allocates. Measured alone and while four readers read
%% `now/<key>' at 20 reads/s in total (prod's game process: ~18/s at peak).
interference(Process, Key, Opts) ->
    Work =
        fun() ->
            [
                element(1, timer:tc(fun() ->
                    erlang:phash2(
                        maps:from_list([ {I, {I, <<"x">>}} || I <- lists:seq(1, 40000) ])
                    )
                end))
            ||
                _ <- lists:seq(1, 100)
            ]
        end,
    Alone = pct(Work()),
    Parent = self(),
    Readers =
        [
            spawn(fun() -> reader(Parent, Process, Key, Opts, []) end)
        ||
            _ <- lists:seq(1, 4)
        ],
    {_, GC0, _} = erlang:statistics(garbage_collection),
    T0 = erlang:monotonic_time(millisecond),
    Loaded = pct(Work()),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    {_, GC1, _} = erlang:statistics(garbage_collection),
    [ R ! stop || R <- Readers ],
    ReadTimes =
        lists:append([ receive {reads, R, Ts} -> Ts end || R <- Readers ]),
    #{
        worker_alone_us => Alone,
        worker_with_readers_us => Loaded,
        reads => length(ReadTimes),
        read_us => pct(ReadTimes),
        gc_reclaimed_mb_per_s =>
            (GC1 - GC0) * 8 div 1048576 * 1000 div max(1, Elapsed)
    }.

reader(Parent, Process, Key, Opts, Acc) ->
    receive stop -> Parent ! {reads, self(), Acc}
    after 200 ->
        {Us, {ok, _}} =
            timer:tc(fun() -> hb_ao:resolve(Process, <<"now/", Key/binary>>, Opts) end),
        reader(Parent, Process, Key, Opts, [Us | Acc])
    end.

%% Small-key lookups in a table while another process keeps replacing a large
%% entry in it: `set' as created before, and with read/write concurrency.
bench_ets_tables() ->
    Big = maps:from_list([ {I, player(I)} || I <- lists:seq(1, 10000) ]),
    [
        {Spec, ets_stall(Spec, Big)}
    ||
        Spec <- [[], [{read_concurrency, true}, {write_concurrency, true}]]
    ].

ets_stall(Spec, Big) ->
    T = ets:new(now_bench_table, [set, public | Spec]),
    [ ets:insert(T, {{small, I}, I, #{ <<"v">> => I }}) || I <- lists:seq(1, 64) ],
    Writer =
        spawn(fun Loop() ->
            ets:insert(T, {big, erlang:unique_integer(), Big}),
            receive stop -> ok after 0 -> Loop() end
        end),
    Times =
        [
            element(1, timer:tc(fun() ->
                ets:lookup_element(T, {small, I rem 64 + 1}, 2)
            end))
        ||
            I <- lists:seq(1, 20000)
        ],
    Writer ! stop,
    ets:delete(T),
    pct(Times).

pct(Times) ->
    Sorted = lists:sort(Times),
    N = length(Sorted),
    At = fun(P) -> lists:nth(max(1, (N * P + 99) div 100), Sorted) end,
    #{ p50 => At(50), p99 => At(99), max => lists:last(Sorted), n => N }.

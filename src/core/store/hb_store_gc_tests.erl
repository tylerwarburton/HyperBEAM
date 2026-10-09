%%% @doc Integration tests for in-place store retention (`hb_store_gc:retain/1')
%%% and the essentials store (`hb_store_essentials'), on real `lua@5.3b'
%%% processes with a small checkpoint cadence.
-module(hb_store_gc_tests).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CADENCE, 5).

%%% Fixtures

node_opts(Extra) ->
    hb:init(),
    application:ensure_all_started(hb),
    Store = hb_test_utils:test_store(hb_store_lmdb),
    ok = hb_store:start([Store], #{}, #{}),
    maps:merge(
        #{
            <<"store">> => [Store],
            <<"priv-wallet">> => ar_wallet:new(),
            <<"hashpath">> => ignore,
            <<"spawn-worker">> => false,
            <<"process-workers">> => false,
            % As production runs: the reverse match index is a derived
            % structure retention does not collect (it grows ~9 rows/slot).
            <<"match-index">> => false,
            <<"process-delta-checkpoint-slots">> => ?CADENCE,
            <<"process-hot-cache-slots">> => 4,
            <<"store-retention">> => true,
            <<"store-retention-recent-slots">> => 4,
            <<"store-retention-checkpoints">> => 2,
            <<"store-retention-grace-ms">> => 0,
            <<"store-retention-max-deletes-per-sec">> => 0,
            <<"store-retention-scan-rows">> => 5000
        },
        Extra
    ).

main_db(Opts) ->
    [Store | _] = hb_opts:get(<<"store">>, [], Opts),
    #{ <<"db">> := DB } = hb_store:find(Store),
    DB.

new_process(Opts) ->
    Process = lua_process(Opts),
    {ok, _} = hb_cache:write(Process, Opts),
    Process.

lua_process(Opts) ->
    Wallet = hb_opts:get(<<"priv-wallet">>, hb:wallet(), Opts),
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    hb_message:commit(
        #{
            <<"device">> => <<"process@1.0">>,
            <<"type">> => <<"Process">>,
            <<"scheduler-device">> => <<"scheduler@1.0">>,
            <<"execution-device">> => <<"lua@5.3b">>,
            <<"module">> => #{
                <<"content-type">> => <<"application/lua">>,
                <<"body">> => script()
            },
            <<"authority">> => [Address],
            <<"scheduler-location">> => Address,
            <<"test-random-seed">> => rand:uniform(1000000)
        },
        Opts
    ).

%% A counter plus a ledger table the patches grow, so states share most of
%% their content across slots and deltas carry real sub-messages.
script() ->
    <<
        "Count = Count or 0\n"
        "function compute(first, second)\n"
        "  Count = Count + 1\n"
        "  local value = tostring(Count)\n"
        "  local req = second or first\n"
        "  local body = req.body or req\n"
        "  local action = tostring(body.action or '')\n"
        "  return {\n"
        "    patches = {\n"
        "      { path = '/count', value = value },\n"
        "      { path = '/last-action', value = action },\n"
        "      { path = '/ledger/k' .. tostring(Count % 7), value = string.rep(value, 20) }\n"
        "    },\n"
        "    results = { output = { data = value } }\n"
        "  }\n"
        "end\n"
    >>.

schedule(Process, N, Opts) ->
    ProcID = hb_message:id(Process, all, Opts),
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
                            <<"number">> => N,
                            <<"data">> => crypto:strong_rand_bytes(96)
                        },
                        Opts
                    )
            },
            Opts
        ),
    {ok, _} = hb_ao:resolve(Process, Req, Opts),
    ok.

compute(Process, Slot, Opts) ->
    hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, Opts).

count_at(Process, Slot, Opts) ->
    {ok, State} = compute(Process, Slot, Opts),
    hb_ao:get(<<"count">>, State, Opts).

%% Schedule `N' messages and compute every slot, returning the next slot.
run_slots(Process, From, N, Opts) ->
    lists:foreach(
        fun(Slot) ->
            ok = schedule(Process, Slot, Opts),
            ?assertEqual(integer_to_binary(Slot + 1), count_at(Process, Slot, Opts))
        end,
        lists:seq(From, From + N - 1)
    ),
    From + N.

clear_process_caches() ->
    [ catch ets:delete_all_objects(T)
    || T <- [dev_process_delta_hot_cache, dev_process_delta_recent_cache,
             dev_process_delta_replay_cache] ],
    ok.

proc_id(Process, Opts) -> hb_util:human_id(hb_message:id(Process, all, Opts)).

computed_slots(Process, Opts) ->
    lists:sort(hb_cache:list_numbered(
        <<"computed/", (proc_id(Process, Opts))/binary, "/slot">>, Opts)).

%% Every assignment, fully loaded and serialized, by its stored path.
assignments(Process, Opts) ->
    P = proc_id(Process, Opts),
    Prefix = <<"~scheduler@1.0/assignments/", P/binary>>,
    [ {S, term_to_binary(
            hb_cache:ensure_all_loaded(
                hb_util:ok(hb_cache:read(<<Prefix/binary, "/", (integer_to_binary(S))/binary>>, Opts)),
                Opts),
            [deterministic])}
    || S <- lists:sort(hb_cache:list_numbered(Prefix, Opts)) ].

%% A slot's public state as `compute&slot=N' serves it.
state_digest(Process, Slot, Opts) ->
    {ok, Msg} = compute(Process, Slot, Opts),
    Loaded = hb_cache:ensure_all_loaded(maps:without([<<"snapshot">>], Msg), Opts),
    {hb_ao:get(<<"count">>, Loaded, Opts), hb_ao:get(<<"ledger">>, Loaded, Opts),
     hb_ao:get(<<"results">>, Loaded, Opts)}.

store_rows(DB) ->
    ok = elmdb:flush(DB),
    Loop =
        fun L(From, N) ->
            case elmdb:scan_rows(DB, From, 50000) of
                {ok, _, C, done} -> N + C;
                {ok, _, C, Next} -> L(Next, N + C)
            end
        end,
    Loop(<<>>, 0).

%%% Tests

%% @doc After a run, every slot of the window is read back identically from a
%% cold cache, `now' is unchanged, every assignment is byte-identical, the
%% definition and the shared module blob survive, the process keeps
%% computing, and a dropped slot is either re-executed correctly or refused
%% fast, depending on `process-historical-replay-limit'.
retention_keeps_processes_correct_test_() ->
    {timeout, 300, fun() ->
        Opts = node_opts(#{}),
        Process = new_process(Opts),
        Next = run_slots(Process, 0, 32, Opts),
        Head = Next - 1,
        Before = assignments(Process, Opts),
        {ok, NowBefore} = hb_ao:resolve(Process, <<"now">>, Opts),
        DB = main_db(Opts),
        RowsBefore = store_rows(DB),
        Report = hb_store_gc:retain(Opts),
        ?event(debug_retention, {report, Report}),
        ?assert(maps:get(dropped_slots, Report) > 0),
        ?assert(maps:get(deleted_keys, Report) > 0),
        RowsAfter = store_rows(DB),
        ?assert(RowsAfter < RowsBefore),
        Kept = computed_slots(Process, Opts),
        % The newest two checkpoints and everything above the lower one.
        Ckpt = ((Head div ?CADENCE) - 1) * ?CADENCE,
        ?assertEqual(lists:seq(Ckpt, Head), Kept),
        clear_process_caches(),
        lists:foreach(
            fun(S) ->
                {Count, _, _} = state_digest(Process, S, Opts),
                ?assertEqual(integer_to_binary(S + 1), Count)
            end,
            Kept),
        {ok, NowAfter} = hb_ao:resolve(Process, <<"now">>, Opts),
        ?assertEqual(hb_ao:get(<<"count">>, NowBefore, Opts),
                     hb_ao:get(<<"count">>, NowAfter, Opts)),
        ?assertEqual(Before, assignments(Process, Opts)),
        ProcID = hb_message:id(Process, all, Opts),
        {ok, Def} = hb_cache:read(ProcID, Opts),
        ?assert(hb_message:verify(hb_cache:ensure_all_loaded(Def, Opts), all, Opts)),
        % Shared content-addressed blobs: the module body is reachable from
        % every checkpoint and from the definition.
        ModuleBlob = <<"data/", (hb_path:hashpath(script(), Opts))/binary>>,
        ?assertMatch({ok, _}, elmdb:get(DB, ModuleBlob)),
        % It keeps computing, across a checkpoint boundary.
        Next2 = run_slots(Process, Next, 8, Opts),
        ?assertEqual(integer_to_binary(Next2), count_at(Process, Next2 - 1, Opts)),
        % A dropped slot is rebuilt by re-execution from the assignments.
        clear_process_caches(),
        ?assertEqual(<<"4">>, count_at(Process, 3, Opts)),
        ?assertEqual(<<"10">>, count_at(Process, 9, Opts)),
        % A second run finds the historical re-execution's writes and removes
        % them again; the window is unchanged.
        R2 = hb_store_gc:retain(Opts),
        ?assertEqual(false, maps:get(recovered_journal, R2)),
        Kept2 = computed_slots(Process, Opts),
        ?assertEqual(lists:max(Kept2), Next2 - 1),
        ?assert(lists:min(Kept2) > 3)
    end}.

%% @doc With a live process worker at the head, a read of a slot retention
%% dropped is either rebuilt correctly or, beyond
%% `process-historical-replay-limit', refused at once with a 503 -- never a
%% crash, a hang, or a change to the worker's live state.
retention_historical_reads_with_worker_test_() ->
    {timeout, 300, fun() ->
        Opts = node_opts(#{
            <<"spawn-worker">> => true,
            <<"process-workers">> => true,
            <<"await-inprogress">> => named
        }),
        Process = new_process(Opts),
        Next = run_slots(Process, 0, 27, Opts),
        R = hb_store_gc:retain(Opts),
        ?assert(maps:get(deleted_keys, R) > 0),
        ?assertNot(lists:member(12, computed_slots(Process, Opts))),
        LimitOpts = Opts#{ <<"process-historical-replay-limit">> => 2 },
        {T, Refused} = timer:tc(fun() -> compute(Process, 12, LimitOpts) end),
        ?assertMatch({error, #{ <<"status">> := 503 }}, Refused),
        ?assert(T < 2000000),
        ?assertEqual(<<"13">>, count_at(Process, 12, Opts)),
        % The worker's live state is untouched: the head computes on.
        _ = run_slots(Process, Next, 3, Opts)
    end}.

%% @doc A COLD worker (after a restart) asked for a slot retention dropped,
%% below every retained checkpoint, of a process already past it: answered
%% with a 503 at once beyond `process-historical-replay-limit', never replayed
%% from slot 0, and never taken as the worker's base -- the next head request
%% resumes from the newest checkpoint. In production a durable push resumed
%% after a restart asked for slot 43,827 of a process at 50,737 (checkpoints
%% kept near 49,000 and 50,000) and replayed from 0 for hours, blocking every
%% live request for the process. A slot whose result is stored is still
%% served, and a process with no checkpoint past the target still computes.
retention_cold_historical_read_is_bounded_test_() ->
    {timeout, 300, fun() ->
        Opts = node_opts(#{
            <<"spawn-worker">> => true,
            <<"process-workers">> => true,
            <<"await-inprogress">> => named
        }),
        Process = new_process(Opts),
        Next = run_slots(Process, 0, 27, Opts),
        _ = hb_store_gc:retain(Opts),
        Kept = computed_slots(Process, Opts),
        ?assertNot(lists:member(12, Kept)),
        ?assert(lists:min(Kept) > 12),
        Restart =
            fun() ->
                stop_worker(Process, Opts),
                clear_process_caches()
            end,
        Restart(),
        LimitOpts = Opts#{ <<"process-historical-replay-limit">> => 2 },
        {{T, Refused}, Executed} =
            executed_slots(fun() -> timer:tc(fun() -> compute(Process, 12, LimitOpts) end) end),
        ?debugFmt("cold read of 12: ~p slots executed, ~p us, ~p",
            [Executed, T, element(1, Refused)]),
        ?assertMatch({error, #{ <<"status">> := 503 }}, Refused),
        ?assertEqual(0, Executed),
        ?assert(T < 2000000),
        % A stored slot older than the newest checkpoint is served as stored.
        Stored = lists:max([ S || S <- Kept, S < lists:max(Kept) - 1 ]),
        {{ok, StoredState}, 0} =
            executed_slots(fun() -> compute(Process, Stored, LimitOpts#{ <<"process-historical-replay-limit">> => 0 }) end),
        ?assertEqual(integer_to_binary(Stored + 1), hb_ao:get(<<"count">>, StoredState, Opts)),
        % The head resumes from the newest checkpoint, not from slot 0.
        {Next2, HeadExecuted} = executed_slots(fun() -> run_slots(Process, Next, 2, Opts) end),
        ?debugFmt("head after the refused read: ~p slots executed", [HeadExecuted]),
        ?assert(HeadExecuted =< 2 + ?CADENCE),
        % Within the limit the slot is rebuilt as an answer only: a cold worker
        % that serves it does not keep it as its base.
        Restart(),
        ?assertEqual(<<"13">>, count_at(Process, 12, Opts)),
        {_, HeadExecuted2} = executed_slots(fun() -> run_slots(Process, Next2, 1, Opts) end),
        ?debugFmt("head after a rebuilt read: ~p slots executed", [HeadExecuted2]),
        ?assert(HeadExecuted2 =< 1 + ?CADENCE),
        % A process with no checkpoint past the target computes from init,
        % however far that is.
        Fresh = new_process(Opts),
        lists:foreach(fun(N) -> ok = schedule(Fresh, N, Opts) end, lists:seq(0, 11)),
        ?assertEqual(<<"12">>, count_at(Fresh, 11, LimitOpts))
    end}.

stop_worker(Process, Opts) ->
    Group = proc_id(Process, Opts),
    case await_worker(Group, 100) of
        Worker when is_pid(Worker) ->
            Ref = erlang:monitor(process, Worker),
            Worker ! stop,
            receive {'DOWN', Ref, process, Worker, _} -> ok
            after 30000 -> erlang:error(worker_did_not_stop)
            end;
        _ -> erlang:error(no_worker_to_stop)
    end,
    hb_util:wait_until(fun() -> hb_name:lookup(Group) == undefined end, 30000).

await_worker(_Group, 0) -> undefined;
await_worker(Group, N) ->
    case hb_name:lookup(Group) of
        Pid when is_pid(Pid) -> Pid;
        _ -> timer:sleep(100), await_worker(Group, N - 1)
    end.

%% Run `Fun' and return its result with the number of slots it executed.
executed_slots(Fun) ->
    % Devices are loaded from the preloaded store, under their own module
    % names: find the process device by its exports.
    [Mod | _] =
        [ M || {M, _} <- code:all_loaded(),
               erlang:function_exported(M, is_cached_state, 1),
               erlang:function_exported(M, request_slot, 2) ],
    MFA = {Mod, compute_slot, 6},
    true = erlang:trace_pattern(MFA, true, [local, call_count]) >= 1,
    try
        Res = Fun(),
        {call_count, N} = erlang:trace_info(MFA, call_count),
        {Res, N}
    after
        erlang:trace_pattern(MFA, false, [local, call_count])
    end.

%% @doc A run sleeps the grace period once, however many chunks it sweeps, and
%% in steady state sweeps everything in one chunk (one protection scan).
%% `store-retention-max-candidates' bounds a chunk's memory.
retention_one_grace_per_run_test_() ->
    {timeout, 300, fun() ->
        Opts = node_opts(#{ <<"store-retention-grace-ms">> => 300 }),
        Process = new_process(Opts),
        Next = run_slots(Process, 0, 42, Opts),
        R = hb_store_gc:retain(Opts#{ <<"store-retention-max-candidates">> => 200 }),
        ?assert(maps:get(chunks, R) >= 3),
        ?assertEqual(1, maps:get(graces, R)),
        ?assert(maps:get(duration_ms, R) < 300 * 2 + 5000),
        clear_process_caches(),
        ?assertEqual(integer_to_binary(Next), count_at(Process, Next - 1, Opts)),
        Next2 = run_slots(Process, Next, 12, Opts),
        R2 = hb_store_gc:retain(Opts),
        ?assertEqual(1, maps:get(chunks, R2)),
        ?assertEqual(1, maps:get(scans, R2)),
        clear_process_caches(),
        [ ?assertEqual(integer_to_binary(S + 1), element(1, state_digest(Process, S, Opts)))
        || S <- computed_slots(Process, Opts) ],
        ?assertEqual(integer_to_binary(Next2), count_at(Process, Next2 - 1, Opts))
    end}.

%% @doc A slot computed after the run planned, whose state has the same root
%% as a dropped slot, keeps the `computed/<P>/<Root>' alias it shares (N4):
%% the run watches its aliases from before it plans.
retention_keeps_root_alias_shared_with_a_new_slot_test_() ->
    {timeout, 300, fun() ->
        Opts = node_opts(#{}),
        Process = new_process(Opts),
        _ = run_slots(Process, 0, 27, Opts),
        P = proc_id(Process, Opts),
        DB = main_db(Opts),
        {ok, <<"link:", Root/binary>>} = elmdb:get(DB, <<"computed/", P/binary, "/slot/2">>),
        RootAlias = <<"computed/", P/binary, "/", Root/binary>>,
        % After planning, before the aliases go: a new slot lands on that root.
        Hook = fun(journal_written) ->
                       ok = elmdb:put(DB, <<"computed/", P/binary, "/slot/9999">>, <<"link:", Root/binary>>),
                       ok = elmdb:put(DB, RootAlias, <<"link:", Root/binary>>);
                  (_) -> ok
               end,
        _ = hb_store_gc:retain(Opts#{ <<"store-retention-test-hook">> => Hook }),
        ok = elmdb:flush(DB),
        ?assertEqual({ok, <<"link:", Root/binary>>}, elmdb:get(DB, RootAlias)),
        {ok, M} = hb_cache:read(RootAlias, Opts),
        ?assert(is_map(hb_cache:ensure_all_loaded(M, Opts)))
    end}.

%% @doc A cold restart -- fresh in-memory caches, store closed and reopened --
%% resumes from the retained checkpoint and computes on correctly.
retention_cold_resume_test_() ->
    {timeout, 300, fun() ->
        Opts = node_opts(#{}),
        Process = new_process(Opts),
        Next = run_slots(Process, 0, 23, Opts),
        _ = hb_store_gc:retain(Opts),
        [Store | _] = hb_opts:get(<<"store">>, [], Opts),
        ok = hb_store:stop([Store], #{}, Opts),
        clear_process_caches(),
        ok = schedule(Process, Next, Opts),
        ?assertEqual(integer_to_binary(Next + 1), count_at(Process, Next, Opts)),
        _ = run_slots(Process, Next + 1, 6, Opts)
    end}.

%% @doc Retention running repeatedly while slots are scheduled and computed
%% continuously: no compute fails, no read finds a missing link, and every
%% kept slot reads back afterwards.
retention_under_concurrent_load_test_() ->
    {timeout, 600, fun() ->
        Opts = node_opts(#{ <<"store-retention-delete-batch">> => 50 }),
        Process = new_process(Opts),
        Start = run_slots(Process, 0, 12, Opts),
        Parent = self(),
        Loader =
            spawn_link(fun() ->
                Loop =
                    fun L(Slot) ->
                        receive {stop, From} -> From ! {loaded, Slot}
                        after 0 ->
                            ok = schedule(Process, Slot, Opts),
                            ?assertEqual(integer_to_binary(Slot + 1),
                                         count_at(Process, Slot, Opts)),
                            % Reads of recent slots race the sweep too.
                            {ok, _} = compute(Process, max(0, Slot - 2), Opts),
                            L(Slot + 1)
                        end
                    end,
                Loop(Start)
            end),
        Reports = [ begin timer:sleep(200), hb_store_gc:retain(Opts) end
                  || _ <- lists:seq(1, 6) ],
        Loader ! {stop, Parent},
        Last = receive {loaded, S} -> S - 1 after 120000 -> error(loader_hung) end,
        ?assert(Last > Start + 10),
        ?assert(lists:sum([ maps:get(deleted_keys, R) || R <- Reports ]) > 0),
        clear_process_caches(),
        Kept = computed_slots(Process, Opts),
        ?assertEqual(Last, lists:max(Kept)),
        ?assertEqual(lists:seq(lists:min(Kept), Last), Kept),
        [ ?assertEqual(integer_to_binary(S + 1), element(1, state_digest(Process, S, Opts)))
        || S <- Kept ]
    end}.

%% @doc A run killed after its aliases are gone, or in the middle of deleting
%% candidates, leaves a journal; the next run finishes it, and the process
%% reads and computes normally throughout.
retention_crash_mid_sweep_test_() ->
    {timeout, 300, fun() ->
        lists:foreach(
            fun(KillAt) ->
                Opts = node_opts(#{}),
                Process = new_process(Opts),
                Next = run_slots(Process, 0, 27, Opts),
                Kill =
                    fun(Phase) ->
                        case Phase of
                            KillAt -> exit(simulated_crash);
                            {deleted, _} when KillAt == deleting -> exit(simulated_crash);
                            _ -> ok
                        end
                    end,
                ?assertExit(simulated_crash,
                    hb_store_gc:retain(Opts#{ <<"store-retention-test-hook">> => Kill,
                                              <<"store-retention-delete-batch">> => 20 })),
                clear_process_caches(),
                % Mid-crash, the process is fully usable.
                ?assertEqual(integer_to_binary(Next), count_at(Process, Next - 1, Opts)),
                R = hb_store_gc:retain(Opts),
                ?assert(maps:get(recovered_journal, R)),
                clear_process_caches(),
                Kept = computed_slots(Process, Opts),
                [ ?assertEqual(integer_to_binary(S + 1),
                               element(1, state_digest(Process, S, Opts)))
                || S <- Kept ],
                _ = run_slots(Process, Next, 4, Opts)
            end,
            [aliases_deleted, deleting]
        )
    end}.

%% @doc A content-addressed blob that a dropped state shares with a row
%% retention knows nothing about is kept: the protection scan finds the
%% reference.
retention_keeps_externally_referenced_content_test_() ->
    {timeout, 300, fun() ->
        Opts = node_opts(#{}),
        Process = new_process(Opts),
        _ = run_slots(Process, 0, 17, Opts),
        P = proc_id(Process, Opts),
        DB = main_db(Opts),
        % Slot 2 will be dropped. Point an unknown namespace at its state root.
        {ok, <<"link:", Root/binary>>} =
            elmdb:get(DB, <<"computed/", P/binary, "/slot/2">>),
        ok = elmdb:put(DB, <<"~other@1.0/keeps">>, <<"link:", Root/binary>>),
        ok = elmdb:flush(DB),
        R = hb_store_gc:retain(Opts),
        ?assert(maps:get(protected_by_scan, R) > 0),
        ?assertEqual(not_found, elmdb:get(DB, <<"computed/", P/binary, "/slot/2">>)),
        {ok, Kept} = hb_cache:read(<<"~other@1.0/keeps">>, Opts),
        ?assertMatch(#{}, hb_cache:ensure_all_loaded(Kept, Opts))
    end}.

%% @doc Assignments, their messages and the definition land in the essentials
%% store, self-contained; with the main store wiped and the essentials store
%% intact, every slot is rebuilt by replay to the same state.
essentials_store_suffices_to_rebuild_test_() ->
    {timeout, 300, fun() ->
        Ess = hb_test_utils:test_store(hb_store_lmdb, <<"essentials">>),
        Opts0 = node_opts(#{}),
        Opts = Opts0#{
            <<"essentials-store">> => Ess,
            <<"store">> => hb_store_essentials:node_store(Opts0#{ <<"essentials-store">> => Ess })
        },
        ok = hb_store:start([Ess], #{}, Opts),
        % The definition is written by the scheduler when it first sees the
        % process -- into the essentials store.
        Process = lua_process(Opts),
        Next = run_slots(Process, 0, 12, Opts),
        Digests = [ state_digest(Process, S, Opts) || S <- lists:seq(0, Next - 1) ],
        P = proc_id(Process, Opts),
        #{ <<"db">> := EssDB } = hb_store:find(Ess),
        MainDB = main_db(Opts),
        ok = elmdb:flush(EssDB), ok = elmdb:flush(MainDB),
        AKey = <<"~scheduler@1.0/assignments/", P/binary, "/3">>,
        ?assertMatch({ok, <<"link:", _/binary>>}, elmdb:get(EssDB, AKey)),
        ?assertEqual(not_found, elmdb:get(MainDB, AKey)),
        % Every row an assignment reaches is in the essentials store.
        ?assertEqual(assignments(Process, Opts),
                     assignments(Process, Opts#{ <<"store">> => [Ess] })),
        % Wipe the main store; keep the essentials store.
        [Main | _] = hb_opts:get(<<"store">>, [], Opts0),
        ok = hb_store:reset([Main], #{}, Opts),
        ok = hb_store:start([Main], #{}, Opts),
        clear_process_caches(),
        ?assertEqual([], computed_slots(Process, Opts)),
        Rebuilt = [ begin {ok, _} = compute(Process, S, Opts), state_digest(Process, S, Opts) end
                  || S <- lists:seq(0, Next - 1) ],
        ?assertEqual(Digests, Rebuilt)
    end}.

%% @doc With an essentials store and `store-retention-orphans', the main-store
%% copies nothing references (offloaded messages, their commitments) go after
%% two runs, while everything the node reads survives: every assignment
%% byte-identical through the node's own store list, `now', and computing on.
%% Content a named key references is kept.
orphan_retention_keeps_what_is_used_test_() ->
    {timeout, 300, fun() ->
        Ess = hb_test_utils:test_store(hb_store_lmdb, <<"ess-orphans">>),
        Opts0 = node_opts(#{ <<"store-retention-orphans">> => true }),
        Opts = Opts0#{
            <<"essentials-store">> => Ess,
            <<"store">> => hb_store_essentials:node_store(Opts0#{ <<"essentials-store">> => Ess })
        },
        ok = hb_store:start([Ess], #{}, Opts),
        Process = lua_process(Opts),
        Next = run_slots(Process, 0, 22, Opts),
        Before = assignments(Process, Opts),
        DB = main_db(Opts),
        % A message only a named key references, and one nothing references.
        Named = hb_message:commit(#{ <<"kept">> => <<"yes">> }, Opts),
        {ok, NamedID} = hb_cache:write(Named, Opts0),
        ok = elmdb:put(DB, <<"~other@1.0/named">>, <<"link:", NamedID/binary>>),
        Loose = hb_message:commit(#{ <<"kept">> => <<"no">> }, Opts),
        {ok, LooseID} = hb_cache:write(Loose, Opts0),
        Rows0 = store_rows(DB),
        R1 = hb_store_gc:retain(Opts),
        ?assert(maps:get(orphan_units, R1) > 0),
        ?assertEqual(0, maps:get(orphan_deleted_units, R1)),
        R2 = hb_store_gc:retain(Opts),
        ?assert(maps:get(orphan_deleted_units, R2) > 0),
        Rows2 = store_rows(DB),
        ?assert(Rows2 < Rows0),
        ?assertEqual(not_found, elmdb:get(DB, LooseID)),
        ?assertMatch({ok, _}, elmdb:get(DB, NamedID)),
        clear_process_caches(),
        ?assertEqual(Before, assignments(Process, Opts)),
        {ok, Now} = hb_ao:resolve(Process, <<"now">>, Opts),
        ?assertEqual(integer_to_binary(Next), hb_ao:get(<<"count">>, Now, Opts)),
        _ = run_slots(Process, Next, 6, Opts)
    end}.

%% @doc Sparse checkpoints go to the archive before retention deletes them
%% locally; an archived one restores (offline) and reads back. At the fill
%% ceiling nothing is archived and the checkpoints stay local.
retention_archives_sparse_checkpoints_test_() ->
    {timeout, 300, fun() ->
        Dir = "cache-TEST/archive-" ++ integer_to_list(erlang:unique_integer([positive])),
        Full = fun(_) -> {1000, 100} end,
        Free = fun(_) -> {1000, 900} end,
        Run =
            fun(Stat) ->
                Opts = node_opts(#{ <<"store-retention-archive">> =>
                                        #{ <<"path">> => hb_util:bin(Dir ++ "/" ++ pid_to_list(self()) ++ atom_to_list(element(2, erlang:fun_info(Stat, name)))),
                                           <<"every">> => 10,
                                           <<"stat-fun">> => Stat } }),
                Process = new_process(Opts),
                _ = run_slots(Process, 0, 42, Opts),
                _ = hb_store_gc:retain(Opts),
                {Opts, Process}
            end,
        {OptsF, ProcF} = Run(Full),
        KeptF = computed_slots(ProcF, OptsF),
        % At the ceiling: the selected checkpoints were not dropped.
        ?assert(lists:member(0, KeptF) andalso lists:member(10, KeptF) andalso lists:member(20, KeptF)),
        {Opts, Process} = Run(Free),
        Kept = computed_slots(Process, Opts),
        ?assertNot(lists:member(10, Kept)),
        #{ <<"path">> := Path } = hb_opts:get(<<"store-retention-archive">>, x, Opts),
        P = proc_id(Process, Opts),
        File = filename:join([hb_util:list(Path), P, "10.ckpt"]),
        ?assert(filelib:is_file(File)),
        ?assert(filelib:is_file(filename:join(hb_util:list(Path), "manifest.log"))),
        [Store | _] = hb_opts:get(<<"store">>, [], Opts),
        {ok, #{ slot := 10 }} = hb_store_gc:restore_checkpoint(File, Opts#{ <<"store">> => [Store] }),
        clear_process_caches(),
        ?assert(lists:member(10, computed_slots(Process, Opts))),
        {Count, _, _} = state_digest(Process, 10, Opts),
        ?assertEqual(<<"11">>, Count)
    end}.

%% @doc With an essentials store, the essentials of a node that kept everything
%% in one store are copied over by `migrate/3' and verified byte-for-byte.
essentials_migration_verifies_test_() ->
    {timeout, 300, fun() ->
        Opts = node_opts(#{}),
        Process = new_process(Opts),
        _ = run_slots(Process, 0, 9, Opts),
        [Main | _] = hb_opts:get(<<"store">>, [], Opts),
        Ess = hb_test_utils:test_store(hb_store_lmdb, <<"migrated">>),
        Src = Opts#{ <<"store">> => [Main#{ <<"access">> => [<<"read">>] }] },
        Dst = Opts#{ <<"store">> => [Ess] },
        Report = hb_store_essentials:migrate(Src, Dst, #{ live_source => true }),
        ?assertEqual(9, maps:get(assignment_slots, Report)),
        ?assertMatch({ok, #{ assignments := 9 }},
            hb_store_essentials:verify(Opts#{ <<"store">> => [Main] }, Dst)),
        % Nothing computed was copied.
        ?assertEqual([], computed_slots(Process, Dst))
    end}.

%% @doc Main-store growth with retention on flattens; without it, it does not.
%% Prints the curve. Set `HB_RETENTION_STEADY=<slots>' to run it.
retention_steady_state_report_test_() ->
    {timeout, 3600, fun() ->
        case os:getenv("HB_RETENTION_STEADY") of
            false -> ok;
            NStr ->
                N = list_to_integer(NStr),
                Curve =
                    fun(Mode) ->
                        Retain = Mode =/= off,
                        Ess = hb_test_utils:test_store(hb_store_lmdb, <<"ess-steady">>),
                        Base = node_opts(#{ <<"process-delta-checkpoint-slots">> => 50,
                                            <<"store-retention-recent-slots">> => 32,
                                            <<"store-retention-orphans">> => Mode == orphans }),
                        Opts = Base#{
                            <<"essentials-store">> => Ess,
                            <<"store">> => hb_store_essentials:node_store(
                                Base#{ <<"essentials-store">> => Ess })
                        },
                        ok = hb_store:start([Ess], #{}, Opts),
                        Process = lua_process(Opts),
                        #{ <<"db">> := EssDB } = hb_store:find(Ess),
                        MainDB = main_db(Opts),
                        Step = max(1, N div 10),
                        lists:map(
                            fun(I) ->
                                _ = run_slots(Process, I * Step, Step, Opts),
                                case Retain of
                                    true -> _ = hb_store_gc:retain(Opts);
                                    false -> ok
                                end,
                                {(I + 1) * Step, store_rows(MainDB), store_rows(EssDB)}
                            end,
                            lists:seq(0, 9)
                        )
                    end,
                Orph = Curve(orphans),
                On = Curve(computed),
                Off = Curve(off),
                io:format(user, "~nRETENTION_STEADY slots main_rows_off main_rows_computed "
                                "main_rows_computed+orphans ess_rows~n", []),
                [ io:format(user, "RETENTION_STEADY ~p ~p ~p ~p ~p~n", [S, MOff, MOn, MOr, E])
                || {{S, MOr, E}, {S, MOn, _}, {S, MOff, _}} <- lists:zip3(Orph, On, Off) ]
        end
    end}.

%% @doc POST /schedule throughput and latency over HTTP under concurrent load,
%% for each essentials layout. Run with
%% `HB_ESS_BENCH=base,lmdb,export,fs HB_ESS_FAST=<local dir>
%% HB_ESS_REMOTE=<slow dir> HB_ESS_BENCH_MS=<duration>'.
%% `base': no essentials store. `lmdb': a local LMDB essentials store.
%% `export': the same plus the asynchronous export to `HB_ESS_REMOTE'.
%% `fs': an `hb_store_fs' essentials store directly on `HB_ESS_REMOTE'.
essentials_schedule_benchmark_test_() ->
    {timeout, 3600, fun() ->
        case os:getenv("HB_ESS_BENCH") of
            false -> ok;
            Spec ->
                Fast = os:getenv("HB_ESS_FAST", "cache-TEST/bench-fast"),
                Remote = os:getenv("HB_ESS_REMOTE", "cache-TEST/bench-remote"),
                Dur = list_to_integer(os:getenv("HB_ESS_BENCH_MS", "20000")),
                Clients = list_to_integer(os:getenv("HB_ESS_BENCH_CLIENTS", "16")),
                [ ess_bench(list_to_atom(V), Fast, Remote, Dur, Clients)
                || V <- string:tokens(Spec, ",") ]
        end
    end}.

ess_bench(Variant, Fast, Remote, Dur, Clients) ->
    application:ensure_all_started(hb),
    Tag = atom_to_list(Variant) ++ "-" ++ integer_to_list(erlang:unique_integer([positive])),
    Dir = fun(Root, Name) -> hb_util:bin(filename:join([Root, Tag, Name])) end,
    Main = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => Dir(Fast, "main") },
    LocalEss = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => Dir(Fast, "ess") },
    Extra =
        case Variant of
            base -> #{};
            lmdb -> #{ <<"essentials-store">> => LocalEss };
            export ->
                #{ <<"essentials-store">> => LocalEss,
                   <<"essentials-export">> =>
                       #{ <<"path">> => Dir(Remote, "export"),
                          <<"journal">> => Dir(Fast, "journal") } };
            blocked ->
                % The target blocks forever (a FIFO manifest: the worker's
                % first read never returns), as a stalled soft mount does.
                ok = filelib:ensure_dir(binary_to_list(Dir(Remote, "export")) ++ "/x"),
                "" = os:cmd("mkfifo " ++ binary_to_list(Dir(Remote, "export")) ++ "/manifest.log"),
                #{ <<"essentials-store">> => LocalEss,
                   <<"essentials-export">> =>
                       #{ <<"path">> => Dir(Remote, "export"),
                          <<"journal">> => Dir(Fast, "journal") } };
            fs ->
                #{ <<"essentials-store">> =>
                       #{ <<"store-module">> => hb_store_fs, <<"name">> => Dir(Remote, "fs") } }
        end,
    Port = 20000 + rand:uniform(20000),
    Opts = maps:merge(#{
        <<"priv-wallet">> => ar_wallet:new(),
        % As production: a filesystem store behind LMDB, which reads fall
        % through to (and which the VM's one file server serves).
        <<"store">> => [Main, #{ <<"store-module">> => hb_store_fs, <<"name">> => Dir(Fast, "fs") }],
        <<"port">> => Port,
        <<"scheduling-mode">> => local_confirmation,
        <<"scheduler-publish-remote">> => false,
        <<"scheduler-default-commitment-spec">> => <<"ans104@1.0">>,
        <<"scheduler-durable-confirm">> => commit,
        <<"match-index">> => false,
        % Production's limits: the default would throttle the benchmark.
        <<"rate-limit-requests">> => 60000,
        <<"rate-limit-period">> => 60,
        <<"rate-limit-max">> => 60000,
        <<"rate-limit-min">> => 0
    }, Extra),
    W = hb_opts:get(priv_wallet, x, Opts),
    Node = hb_http_server:start_node(Opts),
    Addr = hb_util:human_id(ar_wallet:to_address(W)),
    Pools =
        [ begin
            PMsg = hb_message:commit(#{
                <<"device">> => <<"scheduler@1.0">>,
                <<"type">> => <<"Process">>,
                <<"scheduler-location">> => Addr,
                <<"scheduler">> => Addr,
                <<"r">> => rand:uniform(1 bsl 40) }, Opts),
            {ok, _} = hb_http:post(Node, hb_message:commit(#{
                <<"path">> => <<"/~scheduler@1.0/schedule">>,
                <<"method">> => <<"POST">>, <<"body">> => PMsg }, Opts), Opts),
            Target = hb_util:human_id(hb_message:id(PMsg, all, Opts)),
            Target
          end
        || _ <- lists:seq(1, 4) ],
    Self = self(),
    % Compute traffic alongside: a lua@5.3b process scheduled and computed in
    % the node's VM, its latencies recorded.
    CompOpts = (hb_http_server:get_opts(#{ <<"http-server">> => hb_util:human_id(ar_wallet:to_address(W)) }))#{
        <<"spawn-worker">> => false, <<"process-workers">> => false, <<"hashpath">> => ignore },
    CProc = new_process(CompOpts),
    Computer =
        spawn_link(fun() ->
            Loop = fun L(Slot, Acc) ->
                receive {stop, From} -> From ! {compute_lats, Acc}
                after 0 ->
                    T0 = erlang:monotonic_time(microsecond),
                    ok = schedule(CProc, Slot, CompOpts),
                    {ok, _} = compute(CProc, Slot, CompOpts),
                    L(Slot + 1, [erlang:monotonic_time(microsecond) - T0 | Acc])
                end
            end,
            Loop(0, [])
        end),
    PerClient = list_to_integer(os:getenv("HB_ESS_BENCH_REQS", "150")),
    % Requests are signed before the clock starts, so the measurement is the
    % node's, not the client's RSA.
    Pids =
        [ spawn_link(fun() ->
              Target = lists:nth((C rem length(Pools)) + 1, Pools),
              Reqs = [ ess_request(Target, Opts) || _ <- lists:seq(1, PerClient) ],
              Self ! {ready, self()},
              receive {go, Deadline} -> ok end,
              Self ! {lat, self(), ess_client(Node, Reqs, Opts, Deadline, [])}
          end)
        || C <- lists:seq(1, Clients) ],
    [ receive {ready, P} -> ok end || P <- Pids ],
    Deadline = erlang:monotonic_time(millisecond) + Dur,
    [ P ! {go, Deadline} || P <- Pids ],
    Lats = lists:append([ receive {lat, P, L} -> L end || P <- Pids ]),
    Computer ! {stop, Self},
    CLats = lists:sort(receive {compute_lats, CL} -> CL end),
    CN = max(1, length(CLats)),
    io:format(user, "ESS_BENCH variant=~p compute n=~p p50=~.2fms p99=~.2fms max=~.2fms~n",
        [Variant, length(CLats), lists:nth(max(1, CN div 2), CLats ++ [0]) / 1000,
         lists:nth(max(1, (CN * 99) div 100), CLats ++ [0]) / 1000, lists:last([0 | CLats]) / 1000]),
    Elapsed = max(1, erlang:monotonic_time(millisecond) - (Deadline - Dur)),
    Errors = length([ E || {error, E} <- Lats ]),
    Sorted = case lists:sort([ L || L <- Lats, is_integer(L) ]) of [] -> [0]; L -> L end,
    N = length(Sorted),
    P = fun(Q) -> lists:nth(max(1, min(N, round(Q * N))), Sorted) / 1000 end,
    Export =
        case Variant of
            export ->
                [Wrapped | _] = hb_store_essentials:store(Opts),
                AtEnd = hb_store_export:status(Wrapped),
                % How long the slow target takes to drain the backlog once
                % the load stops.
                {DrainUs, Drained} = timer:tc(fun() -> hb_store_export:sync_export(Wrapped) end),
                #{ at_end => maps:with([lag_ms, local_backlog_bytes, local_segments,
                                        shipped_bytes, dropped_records, errors], AtEnd),
                   drain_ms => DrainUs div 1000,
                   after_drain => maps:with([lag_ms, local_backlog_bytes, shipped_bytes,
                                             shipped_segments, errors], Drained) };
            _ -> none
        end,
    io:format(user,
        "ESS_BENCH variant=~p clients=~p n=~p errors=~p thr=~.1f/s p50=~.2fms p99=~.2fms "
        "p999=~.2fms max=~.2fms export=~0p~n",
        [Variant, Clients, N, Errors, N * 1000 / min(Dur, Elapsed), P(0.5), P(0.99), P(0.999),
         lists:last(Sorted) / 1000, Export]).

ess_request(Target, Opts) ->
    hb_message:commit(#{
        <<"path">> => <<"/~scheduler@1.0/schedule">>,
        <<"method">> => <<"POST">>,
        <<"body">> => hb_message:commit(#{
            <<"target">> => Target, <<"type">> => <<"Message">>,
            <<"data">> => hb_util:encode(crypto:strong_rand_bytes(384)),
            <<"n">> => rand:uniform(1 bsl 40) }, Opts) }, Opts).

ess_client(_Node, [], _Opts, _Deadline, Acc) -> Acc;
ess_client(Node, [Req | Reqs], Opts, Deadline, Acc) ->
    T0 = erlang:monotonic_time(microsecond),
    Lat =
        case hb_http:post(Node, Req, Opts) of
            {ok, _} -> erlang:monotonic_time(microsecond) - T0;
            _ -> {error, erlang:monotonic_time(microsecond) - T0}
        end,
    case erlang:monotonic_time(millisecond) > Deadline of
        true -> Acc;
        false -> ess_client(Node, Reqs, Opts, Deadline, [Lat | Acc])
    end.

%% @doc Diagnostic: what the main store holds after retention, by the field
%% signature of each top-level unit. Set `HB_RETENTION_DIAG=<slots>'.
retention_diagnostic_test_() ->
    {timeout, 3600, fun() ->
        case os:getenv("HB_RETENTION_DIAG") of
            false -> ok;
            NStr ->
                N = list_to_integer(NStr),
                Ess = hb_test_utils:test_store(hb_store_lmdb, <<"ess-diag">>),
                Base = node_opts(#{ <<"process-delta-checkpoint-slots">> => 50,
                                    <<"store-retention-recent-slots">> => 32,
                                    <<"store-retention-orphans">> => true }),
                Opts = Base#{
                    <<"essentials-store">> => Ess,
                    <<"store">> => hb_store_essentials:node_store(
                        Base#{ <<"essentials-store">> => Ess })
                },
                ok = hb_store:start([Ess], #{}, Opts),
                Process = lua_process(Opts),
                DB = main_db(Opts),
                lists:foldl(
                    fun(Step, From) ->
                        Next = run_slots(Process, From, N, Opts),
                        R = hb_store_gc:retain(Opts),
                        ok = elmdb:flush(DB),
                        Rows = all_rows(DB),
                        io:format(user, "~nDIAG step=~p slots=~p rows=~p deleted=~p orphans=~p/~p~n",
                            [Step, Next, length(Rows), maps:get(deleted_keys, R),
                             maps:get(orphan_deleted_units, R), maps:get(orphan_units, R)]),
                        [ io:format(user, "DIAG ~6b ~p~n", [C, Sig])
                        || {Sig, C} <- lists:sublist(signatures(Rows), 4) ],
                        Named =
                            lists:foldl(
                                fun({K, _}, Acc) ->
                                    Parts = binary:split(K, <<"/">>, [global]),
                                    Class =
                                        case Parts of
                                            [<<"computed">>, _, <<"slot">>, _ | _] -> computed_slot;
                                            [<<"computed">>, _, X | _] when byte_size(X) == 43 -> computed_alias;
                                            [<<"computed">> | _] -> computed_other;
                                            [<<"data">> | _] -> data;
                                            [T | _] when byte_size(T) == 43 -> unit_rows;
                                            [T | _] -> T
                                        end,
                                    maps:update_with(Class, fun(C) -> C + 1 end, 1, Acc)
                                end, #{}, Rows),
                        io:format(user, "DIAG classes ~p~n", [Named]),
                        Next
                    end,
                    0,
                    lists:seq(1, 4)
                )
        end
    end}.

%% Top-level units of a store by the sorted set of their field names.
signatures(Rows) ->
    Units =
        lists:foldl(
            fun({K, V}, Acc) ->
                case binary:split(K, <<"/">>) of
                    [<<"data">>, _] -> maps:update_with(<<"data/*">>, fun(S) -> S end, sets:new(), Acc);
                    [Top, Rest] when byte_size(Top) == 43 ->
                        Field = hd(binary:split(Rest, <<"/">>)),
                        maps:update_with(Top, fun(S) -> sets:add_element(Field, S) end,
                            sets:from_list([Field]), Acc);
                    [Top] when byte_size(Top) == 43 ->
                        Tag = case V of <<"group">> -> []; <<"link:", _/binary>> -> [<<"=alias">>]; _ -> [<<"=value">>] end,
                        maps:update_with(Top, fun(S) -> sets:union(S, sets:from_list(Tag)) end, sets:from_list(Tag), Acc);
                    [Top | _] -> maps:update_with(<<"ns:", Top/binary>>, fun(S) -> S end, sets:new(), Acc)
                end
            end,
            #{},
            Rows
        ),
    Counts =
        maps:fold(
            fun(<<"ns:", _/binary>> = K, _, Acc) -> maps:update_with([K], fun(C) -> C + 1 end, 1, Acc);
               (<<"data/*">>, _, Acc) -> Acc;
               (_, Fields, Acc) ->
                   maps:update_with(lists:sort(sets:to_list(Fields)), fun(C) -> C + 1 end, 1, Acc)
            end,
            #{ [<<"data/*">>] => length([ K || {<<"data/", _/binary>> = K, _} <- Rows ]) },
            Units),
    lists:reverse(lists:keysort(2, maps:to_list(Counts))).

%% Summarise each traced call by the chain of hb/dev functions on its stack.
diag_tracer(Parent, Acc) ->
    receive
        {trace, _Pid, call, {hb_cache, write, _}, Dump} when is_binary(Dump) ->
            Lines = binary:split(Dump, <<"\n">>, [global]),
            Frames =
                [ hd(binary:split(L, <<" + ">>))
                || L <- Lines,
                   binary:match(L, <<"Return addr">>) =/= nomatch
                     orelse binary:match(L, <<"CP:">>) =/= nomatch ],
            Mods =
                [ F || F <- [ case re:run(L, <<"\\(([a-z_0-9]+:[a-z_0-9]+/[0-9]+)">>,
                                          [{capture, all_but_first, binary}]) of
                                  {match, [M]} -> M; _ -> <<>> end
                              || L <- Frames ],
                       F =/= <<>>,
                       binary:match(F, [<<"hb_cache:">>, <<"lists:">>, <<"maps:">>]) == nomatch ],
            Key = iolist_to_binary(lists:join(<<" < ">>, lists:sublist(Mods, 6))),
            diag_tracer(Parent, maps:update_with(Key, fun(C) -> C + 1 end, 1, Acc));
        {trace, _, _, _, _} -> diag_tracer(Parent, Acc);
        {done, From} -> From ! {callers, Acc}
    end.

all_rows(DB) ->
    Loop =
        fun L(From, Acc) ->
            case elmdb:scan_rows(DB, From, 50000) of
                {ok, Rows, _, done} -> Acc ++ Rows;
                {ok, Rows, _, Next} -> L(Next, Acc ++ Rows)
            end
        end,
    Loop(<<>>, []).

%% @doc What one scheduled message costs in the essentials store, and why.
%% `HB_ESS_SIZE=<messages>'. Game-like messages: ans104-signed by a user
%% wallet, eight tags, a JSON-ish data field; scheduled through
%% `~scheduler@1.0' with an ans104 assignment commitment, as on stage. Prints
%% rows and bytes per slot, the logical size, and the rows by unit kind.
essentials_size_report_test_() ->
    {timeout, 1800, fun() ->
        case os:getenv("HB_ESS_SIZE") of
            false -> ok;
            NStr ->
                N = list_to_integer(NStr),
                Ess = hb_test_utils:test_store(hb_store_lmdb, <<"ess-size">>),
                ExpDir = hb_util:bin("cache-TEST/ess-size-export-" ++ integer_to_list(erlang:unique_integer([positive]))),
                Base = node_opts(#{ <<"scheduler-default-commitment-spec">> => <<"ans104@1.0">>,
                                    <<"scheduling-mode">> => local_confirmation,
                                    <<"essentials-export">> =>
                                        #{ <<"path">> => <<ExpDir/binary, "/remote">>,
                                           <<"journal">> => <<ExpDir/binary, "/journal">> } }),
                Opts = Base#{
                    <<"essentials-store">> => Ess,
                    <<"store">> => hb_store_essentials:node_store(Base#{ <<"essentials-store">> => Ess })
                },
                [EssW] = hb_store_essentials:store(Opts),
                ok = hb_store:start([EssW], #{}, Opts),
                Wallet = hb_opts:get(<<"priv-wallet">>, x, Opts),
                Addr = hb_util:human_id(ar_wallet:to_address(Wallet)),
                Proc = hb_message:commit(#{ <<"device">> => <<"scheduler@1.0">>,
                    <<"type">> => <<"Process">>, <<"scheduler">> => Addr,
                    <<"scheduler-location">> => Addr, <<"r">> => rand:uniform(1 bsl 40) },
                    Opts, <<"ans104@1.0">>),
                PID = hb_util:human_id(hb_message:id(Proc, all, Opts)),
                User = ar_wallet:new(),
                UOpts = Opts#{ <<"priv-wallet">> => User },
                #{ <<"db">> := EssDB } = hb_store:find(Ess),
                MainDB = main_db(Opts),
                Before = {store_rows(EssDB), store_rows(MainDB)},
                Logical =
                    lists:map(
                        fun(I) ->
                            Body = hb_message:commit(#{
                                <<"target">> => PID, <<"type">> => <<"Message">>,
                                <<"action">> => <<"Battle-Op">>, <<"data-protocol">> => <<"ao">>,
                                <<"variant">> => <<"ao.TN.1">>, <<"op">> => <<"move">>,
                                <<"x">> => integer_to_binary(rand:uniform(1000)),
                                <<"y">> => integer_to_binary(rand:uniform(1000)),
                                <<"nonce">> => integer_to_binary(I),
                                <<"data">> => iolist_to_binary(
                                    ["{\"unit\":\"", hb_util:encode(crypto:strong_rand_bytes(24)),
                                     "\",\"path\":[", lists:join(",", [ integer_to_list(rand:uniform(99)) || _ <- lists:seq(1, 40) ]), "]}"])
                            }, UOpts, <<"ans104@1.0">>),
                            Req = hb_message:commit(#{ <<"path">> => <<"schedule">>,
                                <<"method">> => <<"POST">>, <<"body">> => Body }, UOpts),
                            {ok, _} = hb_ao:resolve(Proc, Req, Opts),
                            byte_size(ar_bundles:serialize(hb_message:convert(Body, <<"ans104@1.0">>, Opts)))
                        end,
                        lists:seq(1, N)),
                ok = elmdb:flush(EssDB), ok = elmdb:flush(MainDB),
                ExpSt = hb_store_export:sync_export(EssW),
                io:format(user, "~nESS_SIZE export raw_bytes/slot=~p shipped_bytes/slot=~p ratio=~p~n",
                    [maps:get(raw_bytes, ExpSt) div N, maps:get(shipped_bytes, ExpSt) div N,
                     maps:get(compression_ratio, ExpSt)]),
                Rows = all_rows(EssDB),
                {E0, M0} = Before,
                MainAfter = store_rows(MainDB),
                Bytes = lists:sum([ byte_size(K) + byte_size(V) || {K, V} <- Rows ]),
                {ok, A1} = hb_cache:read(<<"~scheduler@1.0/assignments/", PID/binary, "/1">>, Opts),
                ALoaded = hb_cache:ensure_all_loaded(A1, Opts),
                io:format(user,
                    "~nESS_SIZE messages=~p ess_rows=~p (~.1f/slot) ess_bytes=~p (~p/slot) "
                    "main_rows_added=~p (~.1f/slot) ans104_body_bytes=~p assignment_term_bytes=~p "
                    "ess_lmdb_file=~p~n",
                    [N, length(Rows) - E0, (length(Rows) - E0) / N, Bytes, Bytes div N,
                     MainAfter - M0, (MainAfter - M0) / N, lists:sum(Logical) div N,
                     byte_size(term_to_binary(ALoaded)),
                     filelib:file_size(filename:join(hb_util:list(maps:get(<<"name">>, Ess)), "data.mdb"))]),
                [ io:format(user, "ESS_SIZE ~8b ~p~n", [C, Sig])
                || {Sig, C} <- lists:sublist(signatures(Rows), 16) ],
                ByKind =
                    lists:foldl(
                        fun({K, V}, Acc) ->
                            Kind = case binary:split(K, <<"/">>) of
                                       [<<"data">>, _] -> data_blob;
                                       [<<"~", _/binary>> | _] -> namespace;
                                       [T] when byte_size(T) == 43 ->
                                           case V of <<"link:", _/binary>> -> id_alias; <<"group">> -> unit_marker; _ -> top_value end;
                                       [_, _] -> unit_field;
                                       _ -> other
                                   end,
                            maps:update_with(Kind, fun({C, B}) -> {C + 1, B + byte_size(K) + byte_size(V)} end,
                                             {1, byte_size(K) + byte_size(V)}, Acc)
                        end, #{}, Rows),
                io:format(user, "ESS_SIZE by_kind ~p~n", [ByKind])
        end
    end}.

%%% Essentials retention (`essentials-retention-days').

%% An essentials store with an export (pruning refuses to run without one),
%% pruning live (`essentials-retention-dry-run' false) and, as in production,
%% no remote publishing (the export is then the only guard).
ess_node(Tag, Extra0) ->
    Ess = hb_test_utils:test_store(hb_store_lmdb, Tag),
    Dir = "cache-TEST/ess-exp-" ++ integer_to_list(erlang:unique_integer([positive])),
    Extra = maps:merge(
        #{ <<"essentials-export">> =>
               #{ <<"path">> => hb_util:bin(Dir ++ "/remote"),
                  <<"journal">> => hb_util:bin(Dir ++ "/journal"),
                  <<"segment-ms">> => 100, <<"ship-interval-ms">> => 20 },
           <<"essentials-retention-dry-run">> => false,
           <<"scheduler-publish-remote">> => false },
        Extra0),
    Opts0 = node_opts(Extra),
    Opts1 = Opts0#{ <<"essentials-store">> => Ess },
    Opts = Opts1#{ <<"store">> => hb_store_essentials:node_store(Opts1) },
    [EssStore | _] = hb_store_essentials:store(Opts),
    ok = hb_store:start([EssStore], #{}, Opts),
    Inner = case EssStore of #{ <<"inner">> := I } -> I; _ -> EssStore end,
    #{ <<"db">> := EssDB } = hb_store:find(Inner),
    {Opts, EssStore, EssDB}.

ess_slots(EssDB, P) ->
    ok = elmdb:flush(EssDB),
    case elmdb:list(EssDB, <<"~scheduler@1.0/assignments/", P/binary, "/">>) of
        {ok, L} -> lists:sort([ binary_to_integer(S) || S <- L ]);
        _ -> []
    end.

%% Everything written so far shipped, and the target marked for pruning.
ess_shipped(Opts) ->
    [Ess | _] = hb_store_essentials:store(Opts),
    ?assert(hb_util:wait_until(fun() -> hb_store_export:prepare_prune(Ess) end, 5000)),
    _ = hb_store_export:sync_export(Ess),
    ok.

%% The lowest checkpoint the main store keeps for a process.
oldest_kept_checkpoint(Process, Opts) ->
    hd([ S || S <- computed_slots(Process, Opts),
              hb_store_gc:classify_slot(#{ src_db => main_db(Opts) }, proc_id(Process, Opts), S)
                  == checkpoint ]).

%% @doc Old assignments go; the ones a process needs from its oldest kept
%% checkpoint stay (even though they too are older than the cutoff); recent
%% assignments are untouched by a longer retention; the definition, a small
%% namespace, and content a kept row shares with a pruned assignment survive;
%% a cold node resumes and computes on.
essentials_retention_prunes_old_assignments_test_() ->
    {timeout, 300, fun() ->
        {Opts, _Ess, EssDB} = ess_node(<<"ess-ret">>, #{}),
        Process = lua_process(Opts),
        Next = run_slots(Process, 0, 23, Opts),
        P = proc_id(Process, Opts),
        Loc = <<"~location@1.0/", P/binary>>,
        ok = elmdb:put(EssDB, Loc, <<"here">>),
        {ok, <<"link:", Root1/binary>>} =
            elmdb:get(EssDB, <<"~scheduler@1.0/assignments/", P/binary, "/1">>),
        ok = elmdb:put(EssDB, <<"~other@1.0/keeps">>, <<"link:", Root1/binary>>),
        Before = assignments(Process, Opts),
        ok = ess_shipped(Opts),
        % A week of retention: everything is younger, nothing goes.
        R1 = hb_store_gc:retain(Opts#{ <<"essentials-retention-days">> => 7 }),
        ?assertMatch(#{ pruned_assignments := 0 }, maps:get(essentials, R1)),
        ?assertEqual(lists:seq(0, Next - 1), ess_slots(EssDB, P)),
        % Zero days: everything is older than the cutoff.
        ok = ess_shipped(Opts),
        R2 = hb_store_gc:retain(Opts#{ <<"essentials-retention-days">> => 0 }),
        Need = oldest_kept_checkpoint(Process, Opts),
        ?assert(Need > 0),
        ?assertMatch(#{ status := ok }, maps:get(essentials, R2)),
        ?assertEqual(Need, maps:get(pruned_assignments, maps:get(essentials, R2))),
        ?assert(maps:get(deleted_keys, maps:get(essentials, R2)) > Need),
        ?assertEqual(lists:seq(Need, Next - 1), ess_slots(EssDB, P)),
        % What stays is byte-identical.
        ?assertEqual([ A || A = {S, _} <- Before, S >= Need ], assignments(Process, Opts)),
        % The definition (with its module blob), the small namespace, and a
        % pruned assignment's message another row references are intact.
        EssOpts = Opts#{ <<"store">> => [hd(hb_store_essentials:store(Opts))] },
        ?assertMatch(#{ <<"module">> := #{} },
                     hb_cache:ensure_all_loaded(hb_util:ok(hb_cache:read(P, EssOpts)), EssOpts)),
        ?assertEqual({ok, <<"here">>}, elmdb:get(EssDB, Loc)),
        ?assertMatch(#{}, hb_cache:ensure_all_loaded(
                              hb_util:ok(hb_cache:read(<<"~other@1.0/keeps">>, EssOpts)), EssOpts)),
        % Cold resume: caches gone, main store reopened; compute on.
        [Main | _] = hb_opts:get(<<"store">>, [], Opts),
        ok = hb_store:stop([Main], #{}, Opts),
        clear_process_caches(),
        [ ?assertEqual(integer_to_binary(S + 1), element(1, state_digest(Process, S, Opts)))
        || S <- computed_slots(Process, Opts) ],
        _ = run_slots(Process, Next, 6, Opts),
        ok = ess_shipped(Opts),
        % A second run has nothing more to take below the (moved) window
        % than what the new checkpoint frees.
        R3 = hb_store_gc:retain(Opts#{ <<"essentials-retention-days">> => 0 }),
        ?assertEqual(lists:seq(oldest_kept_checkpoint(Process, Opts), Next + 5),
                     ess_slots(EssDB, P)),
        ?assertMatch(#{ status := ok }, maps:get(essentials, R3))
    end}.

%% @doc A process the main store keeps no checkpoint for keeps every
%% assignment.
essentials_retention_keeps_processes_without_checkpoints_test_() ->
    {timeout, 300, fun() ->
        {Opts, _Ess, EssDB} = ess_node(<<"ess-nockpt">>, #{}),
        Process = lua_process(Opts),
        % Fewer slots than the checkpoint cadence: no checkpoint yet.
        Next = run_slots(Process, 0, 4, Opts),
        ok = ess_shipped(Opts),
        R = hb_store_gc:retain(Opts#{ <<"essentials-retention-days">> => 0 }),
        ?assertEqual(0, maps:get(pruned_assignments, maps:get(essentials, R))),
        ?assertEqual(lists:seq(0, Next - 1), ess_slots(EssDB, proc_id(Process, Opts)))
    end}.

%% @doc A prune killed after its aliases are gone, or mid-delete, is
%% finished by the next run; the process computes throughout.
essentials_retention_restart_mid_prune_test_() ->
    {timeout, 300, fun() ->
        lists:foreach(
            fun(KillAt) ->
                {Opts, _Ess, EssDB} = ess_node(<<"ess-crash">>, #{}),
                Process = lua_process(Opts),
                Next = run_slots(Process, 0, 23, Opts),
                P = proc_id(Process, Opts),
                _ = hb_store_gc:retain(Opts),
                Need = oldest_kept_checkpoint(Process, Opts),
                ok = ess_shipped(Opts),
                Kill = fun(Phase) ->
                           case Phase of
                               KillAt -> exit(simulated_crash);
                               {deleted, _} when KillAt == deleting -> exit(simulated_crash);
                               _ -> ok
                           end
                       end,
                EOpts = Opts#{ <<"essentials-retention-days">> => 0,
                               <<"store-retention-delete-batch">> => 20 },
                ?assertMatch(#{ status := failed },
                    hb_store_gc:retain_essentials(
                        EOpts#{ <<"store-retention-test-hook">> => Kill }, #{ P => Need })),
                clear_process_caches(),
                ?assertEqual(integer_to_binary(Next), count_at(Process, Next - 1, Opts)),
                R = hb_store_gc:retain_essentials(EOpts, #{ P => Need }),
                ?assert(maps:get(recovered_journal, R)),
                ?assertEqual(lists:seq(Need, Next - 1), ess_slots(EssDB, P)),
                ?assertEqual(not_found,
                    elmdb:get(EssDB, <<"~scheduler@1.0/assignments/", P/binary, "/0">>)),
                _ = run_slots(Process, Next, 4, Opts)
            end,
            [aliases_deleted, deleting])
    end}.

%% @doc With the export on, nothing is pruned before the target carries the
%% `local-pruned' marker, nor above what has durably shipped; a restore of
%% the export (even after a base) still has every assignment from slot 0.
essentials_retention_never_prunes_ahead_of_the_export_test_() ->
    {timeout, 300, fun() ->
        Dir = "cache-TEST/ess-ret-export-" ++ integer_to_list(erlang:unique_integer([positive])),
        Remote = Dir ++ "/remote",
        Export = #{ <<"path">> => hb_util:bin(Remote),
                    <<"journal">> => hb_util:bin(Dir ++ "/journal"),
                    <<"segment-ms">> => 3600000, <<"ship-interval-ms">> => 20 },
        {Opts0, Ess, EssDB} = ess_node(<<"ess-export">>, #{ <<"essentials-export">> => Export }),
        _ = Opts0,
        Opts = Opts0#{ <<"essentials-retention-days">> => 0 },
        ?assertMatch(#{ <<"store-module">> := hb_store_export }, Ess),
        Process = lua_process(Opts),
        Next = run_slots(Process, 0, 23, Opts),
        P = proc_id(Process, Opts),
        % First run: the marker is not on the target yet.
        R1 = hb_store_gc:retain(Opts),
        ?assertEqual(#{ status => waiting_for_export_marker }, maps:get(essentials, R1)),
        ?assert(hb_util:wait_until(fun() -> hb_store_export:prepare_prune(Ess) end, 5000)),
        ?assert(filelib:is_file(Remote ++ "/local-pruned")),
        % Nothing has shipped (the open segment is an hour long): nothing goes.
        R2 = hb_store_gc:retain(Opts),
        ?assertEqual(0, maps:get(pruned_assignments, maps:get(essentials, R2))),
        ?assertEqual(lists:seq(0, Next - 1), ess_slots(EssDB, P)),
        % Ship everything; now the prune may proceed.
        _ = hb_store_export:sync_export(Ess),
        R3 = hb_store_gc:retain(Opts),
        Need = oldest_kept_checkpoint(Process, Opts),
        ?assertEqual(Need, maps:get(pruned_assignments, maps:get(essentials, R3))),
        ?assertEqual(lists:seq(Need, Next - 1), ess_slots(EssDB, P)),
        % A base (resync) after the prune supersedes nothing.
        ok = hb_store_export:resync(Ess),
        _ = run_slots(Process, Next, 3, Opts),
        _ = hb_store_export:sync_export(Ess),
        ?assert(maps:get(resyncs, hb_store_export:status(Ess)) >= 1),
        Target = hb_test_utils:test_store(hb_store_lmdb, <<"ess-export-restored">>),
        {ok, _} = hb_store_export:restore(Remote, Target, #{}),
        TOpts = Opts#{ <<"store">> => [Target] },
        ?assertEqual(lists:seq(0, Next + 2),
                     lists:sort(hb_cache:list_numbered(
                         <<"~scheduler@1.0/assignments/", P/binary>>, TOpts))),
        [ ?assertMatch(#{}, hb_cache:ensure_all_loaded(hb_util:ok(hb_cache:read(
              <<"~scheduler@1.0/assignments/", P/binary, "/", (integer_to_binary(S))/binary>>,
              TOpts)), TOpts))
        || S <- lists:seq(0, Next + 2) ]
    end}.

%% @doc Review regressions (rev_essret): without an export nothing is pruned,
%% whatever the age; with the scheduler publishing remotely nothing above its
%% upload mark goes; and the dry-run default deletes nothing.
essentials_retention_guards_test_() ->
    {timeout, 600, fun() ->
        % No export: refused, every assignment kept, the process rebuilds
        % from the essentials store after a main-store wipe.
        Ess = hb_test_utils:test_store(hb_store_lmdb, <<"ess-noexport">>),
        O0 = node_opts(#{ <<"essentials-retention-days">> => 0,
                          <<"essentials-retention-dry-run">> => false }),
        O1 = O0#{ <<"essentials-store">> => Ess },
        Opts = O1#{ <<"store">> => hb_store_essentials:node_store(O1) },
        ok = hb_store:start([Ess], #{}, Opts),
        Process = lua_process(Opts),
        Next = run_slots(Process, 0, 23, Opts),
        Before = assignments(Process, Opts),
        R = hb_store_gc:retain(Opts),
        ?assertMatch(#{ status := refused_no_export }, maps:get(essentials, R)),
        ?assertEqual(Before, assignments(Process, Opts)),
        [Main | _] = hb_opts:get(<<"store">>, [], O0),
        ok = hb_store:reset([Main], #{}, Opts),
        ok = hb_store:start([Main], #{}, Opts),
        clear_process_caches(),
        ?assertEqual(integer_to_binary(Next), count_at(Process, Next - 1, Opts)),
        % Remote publishing on, uploaded up to slot 2: only slots 0..2 may go.
        {OptsU0, _EssU, EssDB} = ess_node(<<"ess-upload">>,
                                         #{ <<"scheduler-publish-remote">> => true }),
        OptsU = OptsU0#{ <<"essentials-retention-days">> => 0 },
        PU = lua_process(OptsU),
        _ = run_slots(PU, 0, 23, OptsU),
        P = proc_id(PU, OptsU),
        ok = elmdb:put(EssDB, <<"~scheduler@1.0/uploaded/", P/binary>>, <<"2">>),
        ok = ess_shipped(OptsU),
        RU = hb_store_gc:retain(OptsU),
        ?assertEqual(3, maps:get(pruned_assignments, maps:get(essentials, RU))),
        ?assertEqual(lists:seq(3, 22), ess_slots(EssDB, P)),
        % Dry run by default: nothing deleted.
        {OptsD0, _EssD, EssDBD} = ess_node(<<"ess-dry">>, #{}),
        OptsD = maps:remove(<<"essentials-retention-dry-run">>,
                            OptsD0#{ <<"essentials-retention-days">> => 0 }),
        PD = lua_process(OptsD),
        _ = run_slots(PD, 0, 23, OptsD),
        ok = ess_shipped(OptsD),
        RD = hb_store_gc:retain(OptsD),
        ?assertMatch(#{ status := dry_run, dry_run := true }, maps:get(essentials, RD)),
        ?assertEqual(lists:seq(0, 22), ess_slots(EssDBD, proc_id(PD, OptsD)))
    end}.

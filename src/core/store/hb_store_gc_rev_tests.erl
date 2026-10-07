%%% Adversarial regression tests for hb_store_gc retention and the export,
%%% adopted from the independent review of feat/retention. The fuzz tests run
%%% only with HB_RETENTION_FUZZ=<ms> set.
-module(hb_store_gc_rev_tests).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile([export_all, nowarn_export_all]).

-define(CADENCE, 5).

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

with_ess(Opts0, Name) ->
    Ess = hb_test_utils:test_store(hb_store_lmdb, Name),
    Opts = Opts0#{
        <<"essentials-store">> => Ess,
        <<"store">> => hb_store_essentials:node_store(Opts0#{ <<"essentials-store">> => Ess })
    },
    ok = hb_store:start([Ess], #{}, Opts),
    {Opts, Ess}.

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

schedule(Process, N, Opts) -> schedule(Process, N, #{}, Opts).
schedule(Process, N, ExtraBody, Opts) ->
    ProcID = hb_message:id(Process, all, Opts),
    Req =
        hb_message:commit(
            #{
                <<"path">> => <<"schedule">>,
                <<"method">> => <<"POST">>,
                <<"body">> =>
                    hb_message:commit(
                        maps:merge(#{
                            <<"target">> => ProcID,
                            <<"type">> => <<"Message">>,
                            <<"action">> => <<"Increment">>,
                            <<"number">> => N,
                            <<"data">> => crypto:strong_rand_bytes(96)
                        }, ExtraBody),
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

run_slots(Process, From, N, Opts) -> run_slots(Process, From, N, #{}, Opts).
run_slots(Process, From, N, Extra, Opts) ->
    lists:foreach(
        fun(Slot) ->
            ok = schedule(Process, Slot, Extra, Opts),
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

assignments(Process, Opts) ->
    P = proc_id(Process, Opts),
    Prefix = <<"~scheduler@1.0/assignments/", P/binary>>,
    [ {S, assignment(P, S, Opts)}
    || S <- lists:sort(hb_cache:list_numbered(Prefix, Opts)) ].

assignment(P, S, Opts) ->
    Path = <<"~scheduler@1.0/assignments/", P/binary, "/", (integer_to_binary(S))/binary>>,
    try
        term_to_binary(
            hb_cache:ensure_all_loaded(hb_util:ok(hb_cache:read(Path, Opts)), Opts),
            [deterministic])
    catch C:R -> {unreadable, C, R}
    end.

state_digest(Process, Slot, Opts) ->
    {ok, Msg} = compute(Process, Slot, Opts),
    Loaded = hb_cache:ensure_all_loaded(maps:without([<<"snapshot">>], Msg), Opts),
    {hb_ao:get(<<"count">>, Loaded, Opts), hb_ao:get(<<"ledger">>, Loaded, Opts),
     hb_ao:get(<<"results">>, Loaded, Opts)}.

all_rows(DB) ->
    ok = elmdb:flush(DB),
    Loop =
        fun L(From, Acc) ->
            case elmdb:scan_rows(DB, From, 50000) of
                {ok, Rows, _, done} -> Acc ++ Rows;
                {ok, Rows, _, Next} -> L(Next, Acc ++ Rows)
            end
        end,
    Loop(<<>>, []).

%% Links whose target neither exists as a key nor as a group prefix, nor as a
%% path below an existing link (resolved by hb_store).
dangling(DB, Opts) ->
    [Store | _] = hb_opts:get(<<"store">>, [], Opts),
    Rows = all_rows(DB),
    Keys = maps:from_list(Rows),
    [ {K, T} || {K, <<"link:", T/binary>>} <- Rows,
                not maps:is_key(T, Keys),
                hb_store:resolve(Store, T) == T orelse
                    hb_store:type(Store, T) == not_found ]
    ++
    %% bare-ID values in data/ blobs that +link keys name
    [].

%%% 1. An alias rewritten after the watch is set must survive (the design says
%%% so: `delete_aliases' keeps an alias written since the run began).
alias_rewrite_survives_test_() ->
    {timeout, 300, fun() ->
        Opts = node_opts(#{}),
        Process = new_process(Opts),
        _ = run_slots(Process, 0, 27, Opts),
        P = proc_id(Process, Opts),
        DB = main_db(Opts),
        Alias = <<"computed/", P/binary, "/slot/2">>,
        {ok, V} = elmdb:get(DB, Alias),
        Hook = fun(journal_written) -> ok = elmdb:put(DB, Alias, V), ok = elmdb:flush(DB);
                  (_) -> ok end,
        R = hb_store_gc:retain(Opts#{ <<"store-retention-test-hook">> => Hook }),
        io:format(user, "~nALIAS_REWRITE conflicts=~p alias_after=~p~n",
                  [maps:get(conflicts, R), elmdb:get(DB, Alias)]),
        ?assertEqual({ok, V}, elmdb:get(DB, Alias))
    end}.

%%% 2. A soft reference written during a run to the signed-ID alias of a
%%% candidate unit. `Where' = when it is written; `Target' = alias | unit.
softref_setup(Opts) ->
    Process = new_process(Opts),
    _ = run_slots(Process, 0, 27, Opts),
    P = proc_id(Process, Opts),
    DB = main_db(Opts),
    {ok, <<"link:", Root/binary>>} = elmdb:get(DB, <<"computed/", P/binary, "/slot/2">>),
    M = hb_message:commit(#{ <<"payload">> => crypto:strong_rand_bytes(200),
                             <<"note">> => <<"held by a dropped state only">> }, Opts),
    {ok, X} = hb_cache:write(M, Opts),
    Y = hb_util:human_id(hb_message:id(M, signed, Opts)),
    ?assertEqual({ok, <<"link:", X/binary>>}, elmdb:get(DB, Y)),
    ok = elmdb:put(DB, <<Root/binary, "/rev-extra">>, <<"link:", X/binary>>),
    ok = elmdb:flush(DB),
    {Process, DB, X, Y, M}.

softref_case(Phase, Target, Flush) ->
    Opts = node_opts(#{ <<"store-retention-delete-batch">> => 1 }),
    {_Process, DB, X, Y, _M} = softref_setup(Opts),
    Ref = <<"~other@1.0/softref">>,
    T = case Target of alias -> Y; unit -> X end,
    Done = make_ref(),
    Hook =
        fun(Ph) ->
            Match = case {Phase, Ph} of
                        {deleting, {deleted, _}} -> true;
                        {Same, Same} -> true;
                        _ -> false
                    end,
            case Match andalso get(Done) == undefined of
                true ->
                    put(Done, true),
                    ok = elmdb:put(DB, Ref, <<"link:", T/binary>>),
                    case Flush of true -> ok = elmdb:flush(DB); false -> ok end;
                false -> ok
            end
        end,
    Rep = hb_store_gc:retain(Opts#{ <<"store-retention-test-hook">> => Hook }),
    ok = elmdb:flush(DB),
    Readable =
        try
            {ok, Msg} = hb_cache:read(Ref, Opts),
            _ = hb_cache:ensure_all_loaded(Msg, Opts),
            true
        catch _:_ -> false
        end,
    Res = #{ phase => Phase, target => Target, flush => Flush,
             ref_row => elmdb:get(DB, Ref) =/= not_found,
             alias_y => elmdb:get(DB, Y) =/= not_found,
             unit_x => elmdb:get(DB, X) =/= not_found,
             readable => Readable,
             conflicts => maps:get(conflicts, Rep),
             protected_by_writes => maps:get(protected_by_writes, Rep) },
    io:format(user, "~nSOFTREF ~p~n", [Res]),
    Res.

softref_unit_during_grace_test_() ->
    {timeout, 300, fun() ->
        #{ readable := R } = softref_case(aliases_deleted, unit, false),
        ?assert(R)
    end}.

softref_alias_during_grace_unflushed_test_() ->
    {timeout, 300, fun() ->
        #{ readable := R } = softref_case(aliases_deleted, alias, false),
        ?assert(R)
    end}.

softref_alias_during_grace_flushed_test_() ->
    {timeout, 300, fun() ->
        #{ readable := R } = softref_case(aliases_deleted, alias, true),
        ?assert(R)
    end}.

softref_alias_during_sweep_test_() ->
    {timeout, 300, fun() ->
        #{ readable := R } = softref_case(deleting, alias, true),
        ?assert(R)
    end}.

softref_unit_during_sweep_test_() ->
    {timeout, 300, fun() ->
        #{ readable := R } = softref_case(deleting, unit, true),
        ?assert(R)
    end}.

%%% 4. forget_process_cache addresses the real tables/keys.
forget_names_test_() ->
    {timeout, 300, fun() ->
        Opts = node_opts(#{}),
        Process = new_process(Opts),
        _ = run_slots(Process, 0, 12, Opts),
        P = proc_id(Process, Opts),
        Keys = fun(T) -> [ element(1, E) || E <- ets:tab2list(T) ] end,
        io:format(user, "~nHOT ~p~nRECENT ~p~n",
            [Keys(dev_process_delta_hot_cache), Keys(dev_process_delta_recent_cache)]),
        ok = hb_store_gc:forget_process_cache(P, 100, #{}),
        Left = [ K || K <- Keys(dev_process_delta_recent_cache) ++
                           Keys(dev_process_delta_hot_cache),
                      element(1, case K of {KK, _} when is_tuple(KK) -> KK; _ -> K end) == P ],
        ?assertEqual([], Left)
    end}.

%%% 3. Essentials store + orphans: assignments carrying nested messages and a
%%% soft reference to a message that lives only in the main store.
ess_nested_softref_test_() ->
    {timeout, 600, fun() ->
        Opts0 = node_opts(#{ <<"store-retention-orphans">> => true }),
        {Opts, Ess} = with_ess(Opts0, <<"rev-ess">>),
        Process = lua_process(Opts),
        MainOnly = hb_message:commit(#{ <<"main-only">> => crypto:strong_rand_bytes(64) }, Opts0),
        {ok, _} = hb_cache:write(MainOnly, Opts0),
        MainOnlyID = hb_message:id(MainOnly, all, Opts0),
        Extra = #{
            <<"nested">> => #{ <<"a">> => crypto:strong_rand_bytes(80),
                               <<"deep">> => #{ <<"b">> => crypto:strong_rand_bytes(80) } },
            <<"ref">> => {link, MainOnlyID, #{ <<"type">> => <<"link">>, <<"lazy">> => false }}
        },
        Next = run_slots(Process, 0, 14, Extra, Opts),
        Before = assignments(Process, Opts),
        EssOnly = assignments(Process, Opts#{ <<"store">> => [Ess] }),
        io:format(user, "~nESS self-contained before retention: ~p~n", [Before == EssOnly]),
        [ hb_store_gc:retain(Opts) || _ <- lists:seq(1, 3) ],
        clear_process_caches(),
        After = assignments(Process, Opts),
        Bad = [ S || {{S, A}, {S, B}} <- lists:zip(Before, After), A =/= B ],
        io:format(user, "ESS assignments changed after orphans: ~p~n", [Bad]),
        ?assertEqual([], Bad),
        ?assertEqual(Before, EssOnly),
        _ = run_slots(Process, Next, 3, Extra, Opts)
    end}.

%%% Single store (no essentials) with orphans on: nothing essential may go.
single_store_orphans_keep_essentials_test_() ->
    {timeout, 600, fun() ->
        Opts = node_opts(#{ <<"store-retention-orphans">> => true }),
        Process = new_process(Opts),
        Other = hb_message:commit(#{ <<"other">> => crypto:strong_rand_bytes(64) }, Opts),
        {ok, _} = hb_cache:write(Other, Opts),
        OtherID = hb_message:id(Other, all, Opts),
        Extra = #{ <<"nested">> => #{ <<"x">> => crypto:strong_rand_bytes(80) },
                   <<"ref">> => {link, OtherID, #{ <<"type">> => <<"link">>, <<"lazy">> => false }} },
        Next = run_slots(Process, 0, 22, Extra, Opts),
        Before = assignments(Process, Opts),
        [ hb_store_gc:retain(Opts) || _ <- lists:seq(1, 3) ],
        clear_process_caches(),
        ?assertEqual(Before, assignments(Process, Opts)),
        ProcID = hb_message:id(Process, all, Opts),
        {ok, Def} = hb_cache:read(ProcID, Opts),
        ?assert(hb_message:verify(hb_cache:ensure_all_loaded(Def, Opts), all, Opts)),
        _ = run_slots(Process, Next, 3, Extra, Opts)
    end}.

%%% Rust-level: guarded delete vs concurrent reference puts. A ref put that
%%% RETURNED before a delete call began must veto that delete (or be seen by the
%%% take before it). Puts overlapping the delete call are the inherent window;
%%% counted, not asserted.
guarded_race_fuzz_test_() ->
    {timeout, 600, fun() ->
        hb:init(),
        Store = hb_test_utils:test_store(hb_store_lmdb),
        ok = hb_store:start([Store], #{}, #{}),
        #{ <<"db">> := DB } = hb_store:find(Store),
        N = 20000,
        Cands = [ <<"data/c", (integer_to_binary(I))/binary>> || I <- lists:seq(1, N) ],
        CT = list_to_tuple(Cands),
        ok = elmdb:put_batch(DB, [ {C, <<"v">>} || C <- Cands ]),
        ok = elmdb:flush(DB),
        ok = elmdb:track(DB, true),
        ok = elmdb:track_watch(DB, Cands),
        Log = ets:new(revlog, [public, bag, {write_concurrency, true}]),
        Self = self(),
        Writer =
            fun(W) ->
                spawn_link(fun() ->
                    Loop = fun L(I) ->
                        receive stop -> Self ! {wdone, self()}
                        after 0 ->
                            C = element(rand:uniform(N), CT),
                            T0 = erlang:monotonic_time(nanosecond),
                            ok = elmdb:put(DB, <<"r/", (integer_to_binary(W))/binary, "/",
                                                 (integer_to_binary(I))/binary>>,
                                           <<"link:", C/binary>>),
                            T1 = erlang:monotonic_time(nanosecond),
                            ets:insert(Log, {put, C, T0, T1}),
                            L(I + 1)
                        end
                    end,
                    Loop(0)
                end)
            end,
        Ws = [ Writer(W) || W <- lists:seq(1, 4) ],
        Prot = ets:new(revprot, [set]),
        Batches = [ lists:sublist(Cands, I, 50) || I <- lists:seq(1, N, 50) ],
        lists:foreach(
            fun(Batch) ->
                Del = fun D(B) ->
                    {ok, Tr} = elmdb:track_take(DB),
                    [ ets:insert(Prot, {K}) || K <- Tr ],
                    Live = [ K || K <- B, not ets:member(Prot, K) ],
                    D0 = erlang:monotonic_time(nanosecond),
                    R = elmdb:delete_batch_guarded(DB, Live),
                    D1 = erlang:monotonic_time(nanosecond),
                    case R of
                        {ok, _} -> [ ets:insert(Log, {del, K, D0, D1}) || K <- Live ];
                        {error, conflict, Found} ->
                            [ ets:insert(Prot, {K}) || K <- Found ],
                            D(Live -- Found)
                    end
                end,
                Del(Batch)
            end,
            Batches),
        [ W ! stop || W <- Ws ],
        [ receive {wdone, W} -> ok end || W <- Ws ],
        Dels = maps:from_list([ {K, {D0, D1}} || {del, K, D0, D1} <- ets:lookup(Log, del) ]),
        Puts = ets:lookup(Log, put),
        Viol = [ P || P = {put, C, _T0, T1} <- Puts,
                      case maps:get(C, Dels, undefined) of
                          {D0, _} -> T1 < D0;
                          _ -> false
                      end ],
        Overlap = [ P || P = {put, C, T0, T1} <- Puts,
                         case maps:get(C, Dels, undefined) of
                             {D0, D1} -> T1 >= D0 andalso T0 =< D1;
                             _ -> false
                         end ],
        io:format(user, "~nRACE puts=~p deleted=~p violations=~p overlapping_window=~p~n",
                  [length(Puts), maps:size(Dels), length(Viol), length(Overlap)]),
        ?assertEqual([], Viol)
    end}.

%%% Randomized concurrent fuzz: several processes computing continuously with
%%% workers, historical reads, retention running repeatedly with small
%%% parameters and random kills. Then: every assignment byte-identical to what
%%% was read right after scheduling, every kept slot readable and correct,
%%% `now' correct, cold resume computes on.
fuzz_ess_test_() -> {timeout, 1800, fun() -> maybe_fuzz(ess) end}.
fuzz_single_test_() -> {timeout, 1800, fun() -> maybe_fuzz(single) end}.

maybe_fuzz(Mode) ->
    case os:getenv("HB_RETENTION_FUZZ") of
        false -> ok;
        Ms -> fuzz(Mode, list_to_integer(Ms))
    end.

fuzz(Mode, DurMs) ->
    Seed = erlang:unique_integer([positive]),
    rand:seed(exsss, {Seed, 7, 11}),
    Base = node_opts(#{
        <<"spawn-worker">> => true,
        <<"process-workers">> => true,
        <<"await-inprogress">> => named,
        <<"store-retention-orphans">> => true,
        <<"store-retention-delete-batch">> => 7,
        <<"store-retention-scan-rows">> => 400,
        <<"store-retention-max-candidates">> => 300
    }),
    {Opts, _Ess} =
        case Mode of
            ess -> with_ess(Base, <<"fuzz-ess-", (integer_to_binary(Seed))/binary>>);
            single -> {Base, none}
        end,
    Rec = ets:new(fuzzrec, [public, set]),
    Procs = [ case Mode of ess -> lua_process(Opts); single -> new_process(Opts) end
            || _ <- lists:seq(1, 3) ],
    Self = self(),
    Errors = ets:new(fuzzerr, [public, bag]),
    Loader =
        fun(Process) ->
            spawn_link(fun() ->
                P = proc_id(Process, Opts),
                L = fun Loop(Slot) ->
                    receive {stop, From} -> From ! {loaded, Process, Slot}
                    after 0 ->
                        try
                            ok = schedule(Process, Slot, Opts),
                            ets:insert(Rec, {{P, Slot}, assignment(P, Slot, Opts)}),
                            C = count_at(Process, Slot, Opts),
                            case C == integer_to_binary(Slot + 1) of
                                true -> ok;
                                false -> ets:insert(Errors, {wrong_now, P, Slot, C})
                            end,
                            case rand:uniform(4) of
                                1 ->
                                    H = rand:uniform(Slot + 1) - 1,
                                    Want = integer_to_binary(H + 1),
                                    case catch count_at(Process, H, Opts) of
                                        Want -> ok;
                                        Other -> ets:insert(Errors, {hist, P, H, Slot, Other})
                                    end;
                                _ -> ok
                            end
                        catch Cl:Re:St ->
                            ets:insert(Errors, {crash, P, Slot, Cl, Re, St})
                        end,
                        Loop(Slot + 1)
                    end
                end,
                L(0)
            end)
        end,
    Loaders = [ Loader(Pr) || Pr <- Procs ],
    Deadline = erlang:monotonic_time(millisecond) + DurMs,
    RunLoop =
        fun RL(Runs, Kills, Fails) ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true -> {Runs, Kills, Fails};
                false ->
                    timer:sleep(rand:uniform(300)),
                    Grace = lists:nth(rand:uniform(3), [0, 5, 50]),
                    O = Opts#{ <<"store-retention-grace-ms">> => Grace },
                    {Pid, MRef} = spawn_monitor(fun() -> exit({done, hb_store_gc:retain(O)}) end),
                    KillAfter = case rand:uniform(2) of 1 -> rand:uniform(1500); _ -> infinity end,
                    receive
                        {'DOWN', MRef, _, _, {done, _}} -> RL(Runs + 1, Kills, Fails);
                        {'DOWN', MRef, _, _, Why} ->
                            ets:insert(Errors, {retain_failed, Why}),
                            RL(Runs + 1, Kills, Fails + 1)
                    after KillAfter ->
                        exit(Pid, kill),
                        receive {'DOWN', MRef, _, _, _} -> ok end,
                        RL(Runs + 1, Kills + 1, Fails)
                    end
            end
        end,
    {Runs, Kills, Fails} = RunLoop(0, 0, 0),
    Heads =
        [ begin L ! {stop, Self}, receive {loaded, Pr, S} -> {Pr, S - 1} end end
        || {L, Pr} <- lists:zip(Loaders, Procs) ],
    _ = hb_store_gc:retain(Opts),
    _ = hb_store_gc:retain(Opts),
    clear_process_caches(),
    Bad =
        lists:flatmap(
            fun({Pr, Head}) ->
                P = proc_id(Pr, Opts),
                AssignBad = [ {assignment_changed, P, S}
                            || S <- lists:seq(0, Head),
                               [{_, A}] <- [ets:lookup(Rec, {P, S})],
                               assignment(P, S, Opts) =/= A ],
                Kept = computed_slots(Pr, Opts),
                KeptBad = [ {kept_wrong, P, S, D}
                          || S <- Kept,
                             D <- [catch element(1, state_digest(Pr, S, Opts))],
                             D =/= integer_to_binary(S + 1) ],
                % Re-executed historical slots land below the window again, so
                % the kept set need not be contiguous; it must not be empty.
                Hole = case Kept of
                           [] -> [{no_kept, P}];
                           _ -> []
                       end,
                AssignBad ++ KeptBad ++ Hole ++ [ {closure, P, X} || X <- full_closure_bad(Pr, Opts) ]
            end,
            Heads),
    %% Cold resume.
    [Main | _] = hb_opts:get(<<"store">>, [], Opts),
    ok = hb_store:stop([Main], #{}, Opts),
    clear_process_caches(),
    Resume =
        [ try
              ok = schedule(Pr, H + 1, Opts),
              count_at(Pr, H + 1, Opts) == integer_to_binary(H + 2)
          catch C2:R2 -> {C2, R2}
          end
        || {Pr, H} <- Heads ],
    Errs = ets:tab2list(Errors),
    io:format(user, "~nFUZZ ~p runs=~p kills=~p failed_runs=~p heads=~p~n  bad=~p~n  resume=~p~n  errors(~p)=~P~n",
              [Mode, Runs, Kills, Fails, [ H || {_, H} <- Heads ], Bad, Resume,
               length(Errs), Errs, 60]),
    ?assertEqual([], Bad),
    ?assertEqual([true, true, true], Resume),
    ?assertEqual([], [ E || E <- Errs, element(1, E) =/= retain_failed ]).

%%% Exporter: an exporter that dies (node crash, OOM kill, a badmatch in the
%%% writer) loses the records in its mailbox. Does the restart notice and
%%% close the gap, or does restore silently miss rows? Adapted from the review:
%%% the essentials store holds essential shapes -- assignments and what they
%%% reach -- which is what the catch-up re-exports; arbitrary keys written to
%%% an essentials store would need `resync/1'.
export_crash_gap_test_() ->
    {timeout, 300, fun() ->
        application:ensure_all_started(hb),
        Dir = "cache-TEST/rev-export-" ++ integer_to_list(erlang:unique_integer([positive])),
        Inner = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => hb_util:bin(Dir ++ "/lmdb") },
        Store = hb_store_export:wrap(Inner, #{
            <<"path">> => hb_util:bin(Dir ++ "/remote"),
            <<"journal">> => hb_util:bin(Dir ++ "/journal"),
            <<"segment-ms">> => 100000, <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        _ = hb_store_export:sync_export(Store),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        Key = fun(I) -> <<"~scheduler@1.0/assignments/", P/binary, "/", (integer_to_binary(I))/binary>> end,
        W = fun(From, To) ->
                [ begin
                    {ok, ID} = hb_cache:write(#{ <<"v">> => integer_to_binary(I) }, Opts),
                    ok = hb_store:link([Store], #{ Key(I) => ID }, Opts)
                  end
                || I <- lists:seq(From, To) ] end,
        W(1, 3000),
        [{_, Pid, _}] = ets:lookup(hb_store_export_registry, maps:get(<<"name">>, Store)),
        exit(Pid, kill),
        timer:sleep(50),
        W(3001, 3100),
        St = hb_store_export:sync_export(Store),
        Target = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => hb_util:bin(Dir ++ "/restored") },
        R = hb_store_export:restore(Dir ++ "/remote", Target, #{}),
        TOpts = #{ <<"store">> => [Target] },
        Missing = [ I || I <- lists:seq(1, 3100),
                         case hb_cache:read(Key(I), TOpts) of
                             {ok, M} -> hb_cache:ensure_all_loaded(M, TOpts) =/= #{ <<"v">> => integer_to_binary(I) };
                             _ -> true
                         end ],
        io:format(user, "~nEXPORT_CRASH restore=~p catchups=~p missing=~p (first ~p)~n",
                  [R, maps:get(catchups, St), length(Missing), lists:sublist(Missing, 5)]),
        ?assertEqual([], Missing)
    end}.

%% Every kept slot, read raw from the store and loaded WHOLE, snapshot included.
full_closure_bad(Process, Opts) ->
    P = proc_id(Process, Opts),
    [ {S, E} || S <- computed_slots(Process, Opts),
                E <- [try
                          {ok, M} = hb_cache:read(<<"computed/", P/binary, "/slot/",
                                                     (integer_to_binary(S))/binary>>, Opts),
                          _ = hb_cache:ensure_all_loaded(M, Opts),
                          ok
                      catch C:R -> {C, R}
                      end],
                E =/= ok ].

%%% Cold resume from a stored checkpoint, compute past many checkpoints with
%%% retention between, with a worker; every kept slot's whole closure (snapshot
%%% included) must stay loadable, and a second cold resume must work.
snapshot_closure_after_resume_test_() ->
    {timeout, 900, fun() ->
        Opts = node_opts(#{ <<"spawn-worker">> => true, <<"process-workers">> => true,
                            <<"await-inprogress">> => named }),
        Process = new_process(Opts),
        N1 = run_slots(Process, 0, 23, Opts),
        _ = hb_store_gc:retain(Opts),
        [Main | _] = hb_opts:get(<<"store">>, [], Opts),
        ok = hb_store:stop([Main], #{}, Opts),
        clear_process_caches(),
        N2 = lists:foldl(
            fun(_, N) ->
                N3 = run_slots(Process, N, 5, Opts),
                _ = hb_store_gc:retain(Opts),
                N3
            end, N1, lists:seq(1, 8)),
        Bad1 = full_closure_bad(Process, Opts),
        ok = hb_store:stop([Main], #{}, Opts),
        clear_process_caches(),
        N4 = run_slots(Process, N2, 7, Opts),
        _ = hb_store_gc:retain(Opts),
        Bad2 = full_closure_bad(Process, Opts),
        io:format(user, "~nSNAPCLOSURE kept=~p bad1=~P bad2=~P~n",
                  [computed_slots(Process, Opts), Bad1, 12, Bad2, 12]),
        ?assertEqual([], Bad1),
        ?assertEqual([], Bad2),
        ?assertEqual(integer_to_binary(N4), count_at(Process, N4 - 1, Opts))
    end}.

%%% A complete hb_cache:write of a message whose unit is a candidate, returning
%%% during the sweep: the unit is protected, but is its freshly rewritten
%%% signed-ID alias kept?
rewrite_during_sweep_keeps_alias_test_() ->
    {timeout, 300, fun() ->
        Opts = node_opts(#{ <<"store-retention-delete-batch">> => 1 }),
        {_Process, DB, X, Y, M} = softref_setup(Opts),
        Done = make_ref(),
        Hook = fun({deleted, _}) ->
                       case get(Done) of
                           undefined -> put(Done, true), {ok, X} = hb_cache:write(M, Opts),
                                        ok = elmdb:flush(DB);
                           _ -> ok
                       end;
                  (_) -> ok end,
        _ = hb_store_gc:retain(Opts#{ <<"store-retention-test-hook">> => Hook }),
        ok = elmdb:flush(DB),
        ByID = try {ok, Msg} = hb_cache:read(Y, Opts), is_map(hb_cache:ensure_all_loaded(Msg, Opts))
               catch _:_ -> false end,
        io:format(user, "~nREWRITE unit_x=~p alias_y=~p read_by_signed_id=~p~n",
                  [elmdb:get(DB, X) =/= not_found, elmdb:get(DB, Y), ByID]),
        ?assert(ByID)
    end}.

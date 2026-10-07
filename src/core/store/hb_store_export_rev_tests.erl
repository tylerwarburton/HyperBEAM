%%% Adversarial tests of the essentials export, adopted from the third review
%%% of feat/retention.
-module(hb_store_export_rev_tests).
-include_lib("eunit/include/eunit.hrl").

new_store(Tag) ->
    application:ensure_all_started(hb),
    Dir = "cache-TEST/rev-ex-" ++ Tag ++ "-" ++ integer_to_list(erlang:unique_integer([positive])),
    Inner = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => hb_util:bin(Dir ++ "/lmdb") },
    Store = hb_store_export:wrap(Inner, #{
        <<"path">> => hb_util:bin(Dir ++ "/remote"),
        <<"journal">> => hb_util:bin(Dir ++ "/journal"),
        <<"segment-ms">> => 100000000, <<"ship-interval-ms">> => 50 }),
    Opts = #{ <<"store">> => [Store] },
    ok = hb_store:start([Store], #{}, Opts),
    {Dir, Store, Opts}.

writer(Store) ->
    [{_, Pid, _}] = ets:lookup(hb_store_export_registry, maps:get(<<"name">>, Store)),
    Pid.

key(P, I) -> <<"~scheduler@1.0/assignments/", P/binary, "/", (integer_to_binary(I))/binary>>.

w(Store, Opts, P, From, To) ->
    [ begin
        {ok, ID} = hb_cache:write(#{ <<"v">> => integer_to_binary(I), <<"p">> => P }, Opts),
        ok = hb_store:link([Store], #{ key(P, I) => ID }, Opts)
      end || I <- lists:seq(From, To) ].

settle(Store, 0) -> hb_store_export:status(Store);
settle(Store, K) ->
    St = hb_store_export:sync_export(Store),
    case {maps:get(catchup_running, St, false), maps:get(dropping, St, false),
          maps:get(local_segments, St, 0)} of
        {false, false, 0} -> St;
        _ -> timer:sleep(200), settle(Store, K - 1)
    end.

missing(Dir, Store, Opts, P, N) ->
    Fin = settle(Store, 100),
    io:format(user, "~nSETTLED ~P~n", [maps:with([catchup_running, dropping, local_segments, catchups, dropped_records], Fin), 10]),
    Target = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => hb_util:bin(Dir ++ "/restored-" ++ integer_to_list(erlang:unique_integer([positive]))) },
    R = (catch hb_store_export:restore(Dir ++ "/remote", Target, #{ <<"require-definitions">> => false, <<"allow-partial-processes">> => true })),
    TOpts = #{ <<"store">> => [Target] },
    Miss = [ I || I <- lists:seq(1, N),
                  case catch hb_cache:read(key(P, I), TOpts) of
                      {ok, M} -> (catch hb_cache:ensure_all_loaded(M, TOpts)) =/=
                                     #{ <<"v">> => integer_to_binary(I), <<"p">> => P };
                      _ -> true
                  end ],
    _ = Opts,
    {R, Miss}.

%% A second crash while the catch-up of the first is running, after a live
%% assignment has advanced the watermark past the slots the first crash lost.
crash_during_catchup_test_() ->
    {timeout, 600, fun() ->
        {Dir, Store, Opts} = new_store("cc"),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        w(Store, Opts, P, 1, 200),
        _ = hb_store_export:sync_export(Store),
        W1 = writer(Store),
        erlang:suspend_process(W1),
        w(Store, Opts, P, 201, 3200),          % lost with the mailbox
        exit(W1, kill),
        timer:sleep(20),
        %% Restart (unclean) and catch the catch-up process.
        w(Store, Opts, P, 3201, 3201),
        W2 = writer(Store),
        {links, Links} = erlang:process_info(W2, links),
        Others = [ L || L <- Links, is_pid(L), L =/= self() ],
        [ catch erlang:suspend_process(L) || L <- Others,
            element(2, erlang:process_info(L, current_function)) =/= {hb_store_export, target_worker_loop, 1} ],
        io:format(user, "~nW2 links ~p~n", [[ {L, catch erlang:process_info(L, current_function)} || L <- Others ]]),
        %% A live assignment journaled meanwhile.
        w(Store, Opts, P, 3202, 3202),
        _ = hb_store_export:status(Store),
        exit(W2, kill),
        timer:sleep(20),
        %% Restart again and let everything finish.
        w(Store, Opts, P, 3203, 3210),
        {R, Miss} = missing(Dir, Store, Opts, P, 3210),
        io:format(user, "~nCATCHUP_CRASH restore=~P missing=~p (first ~p)~n",
                  [R, 8, length(Miss), lists:sublist(Miss, 5)]),
        ?assertEqual([], Miss)
    end}.

%% Overflow: past max-pending a record is dropped, but `drain/2' pulls later
%% records past the `overflow' message, so the dropped closure row's link (the
%% assignment) can be journaled -- moving the watermark past it.
overflow_drop_test_() ->
    {timeout, 600, fun() ->
        application:ensure_all_started(hb),
        Dir = "cache-TEST/rev-ex-of-" ++ integer_to_list(erlang:unique_integer([positive])),
        Inner = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => hb_util:bin(Dir ++ "/lmdb") },
        Store = hb_store_export:wrap(Inner, #{
            <<"path">> => hb_util:bin(Dir ++ "/remote"),
            <<"journal">> => hb_util:bin(Dir ++ "/journal"),
            <<"max-pending">> => 50,
            <<"segment-ms">> => 100000000, <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        %% (Slots 1..100 first, so the process's assignments are contiguous,
        %% as a scheduler writes them, and restore's contiguity check applies.)
        w(Store, Opts, P, 1, 100),
        _ = hb_store_export:sync_export(Store),
        %% Many concurrent writers overflow the 50-record bound.
        Self = self(),
        Ps = [ spawn_link(fun() -> w(Store, Opts, P, K * 100 + 1, K * 100 + 100), Self ! {d, self()} end)
             || K <- lists:seq(1, 20) ],
        [ receive {d, X} -> ok end || X <- Ps ],
        St0 = hb_store_export:status(Store),
        {R, Miss} = missing(Dir, Store, Opts, P, 2100),
        Miss2 = Miss,
        io:format(user, "~nOVERFLOW dropped=~p restore=~P missing=~p (first ~p)~n",
                  [maps:get(dropped_records, St0, x), R, 8, length(Miss2), lists:sublist(Miss2, 5)]),
        ?assertEqual([], Miss2)
    end}.

%% Control: one crash, then let the catch-up finish.
one_crash_control_test_() ->
    {timeout, 600, fun() ->
        {Dir, Store, Opts} = new_store("one"),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        w(Store, Opts, P, 1, 200),
        _ = hb_store_export:sync_export(Store),
        W1 = writer(Store),
        erlang:suspend_process(W1),
        w(Store, Opts, P, 201, 3200),
        exit(W1, kill),
        timer:sleep(20),
        w(Store, Opts, P, 3201, 3210),
        {R, Miss} = missing(Dir, Store, Opts, P, 3210),
        io:format(user, "~nONE_CRASH restore=~P missing=~p~n", [R, 8, length(Miss)]),
        ?assertEqual([], Miss)
    end}.

%% Two crashes: the second right after the restart's first live assignment
%% is journaled, while the first crash's catch-up may still be running.
two_crash_test_() ->
    {timeout, 600, fun() ->
        {Dir, Store, Opts} = new_store("two"),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        w(Store, Opts, P, 1, 200),
        _ = hb_store_export:sync_export(Store),
        W1 = writer(Store),
        erlang:suspend_process(W1),
        w(Store, Opts, P, 201, 3200),
        exit(W1, kill),
        timer:sleep(20),
        w(Store, Opts, P, 3201, 3201),
        W2 = writer(Store),
        St = hb_store_export:status(Store),
        exit(W2, kill),
        timer:sleep(20),
        w(Store, Opts, P, 3202, 3210),
        {R, Miss} = missing(Dir, Store, Opts, P, 3210),
        io:format(user, "~nTWO_CRASH catchup_running_at_kill=~p restore=~P missing=~p (range ~p..~p)~n",
                  [maps:get(catchup_running, St, x), R, 8, length(Miss),
                   catch lists:min(Miss), catch lists:max(Miss)]),
        ?assertEqual([], Miss)
    end}.

%% Realistic overflow: one sequential writer per process (as the scheduler
%% server writes), 20 processes at once, a small max-pending.
overflow_per_process_test_() ->
    {timeout, 600, fun() ->
        application:ensure_all_started(hb),
        Dir = "cache-TEST/rev-ex-opp-" ++ integer_to_list(erlang:unique_integer([positive])),
        Inner = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => hb_util:bin(Dir ++ "/lmdb") },
        Store = hb_store_export:wrap(Inner, #{
            <<"path">> => hb_util:bin(Dir ++ "/remote"),
            <<"journal">> => hb_util:bin(Dir ++ "/journal"),
            <<"max-pending">> => list_to_integer(os:getenv("REV_MAXPEND", "50")),
            <<"segment-ms">> => 100000000, <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        Procs = [ hb_util:human_id(crypto:strong_rand_bytes(32)) || _ <- lists:seq(1, 20) ],
        [ w(Store, Opts, P, 1, 2) || P <- Procs ],
        _ = hb_store_export:sync_export(Store),
        Self = self(),
        Ps = [ spawn_link(fun() -> w(Store, Opts, P, 3, 150), Self ! {d, self()} end) || P <- Procs ],
        [ receive {d, X} -> ok end || X <- Ps ],
        St0 = hb_store_export:status(Store),
        Fin = settle(Store, 100),
        Target = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => hb_util:bin(Dir ++ "/restored") },
        R = (catch hb_store_export:restore(Dir ++ "/remote", Target, #{ <<"require-definitions">> => false, <<"allow-partial-processes">> => true })),
        TOpts = #{ <<"store">> => [Target] },
        Miss = [ {P, I} || P <- Procs, I <- lists:seq(1, 150),
                  case catch hb_cache:read(key(P, I), TOpts) of
                      {ok, M} -> (catch hb_cache:ensure_all_loaded(M, TOpts)) =/=
                                     #{ <<"v">> => integer_to_binary(I), <<"p">> => P };
                      _ -> true
                  end ],
        io:format(user, "~nOVERFLOW_PP dropped=~p catchups=~p restore=~P missing=~p (first ~p)~n",
                  [maps:get(dropped_records, St0, x), maps:get(catchups, Fin, x), R, 8,
                   length(Miss), lists:sublist(Miss, 3)]),
        ?assertEqual([], Miss)
    end}.

%%% End-to-end randomized export fuzz. Concurrent assignment writers across
%%% many processes; random writer crashes (with and without a lost mailbox,
%%% so also during catch-ups), random clean stops, random target outages, and
%%% a small `max-pending' that forces overflows. Afterwards the restore must
%%% either equal the local essentials store exactly or fail loudly -- never
%%% succeed incomplete. `HB_EXPORT_FUZZ=<rounds>' (`HB_EXPORT_FUZZ_MS' per
%%% round, default 20000).
export_fuzz_test_() ->
    {timeout, 7200, fun() ->
        case os:getenv("HB_EXPORT_FUZZ") of
            false -> ok;
            R ->
                Ms = list_to_integer(os:getenv("HB_EXPORT_FUZZ_MS", "20000")),
                Results = [ fuzz_round(I, Ms) || I <- lists:seq(1, list_to_integer(R)) ],
                io:format(user, "~nEXPORT_FUZZ rounds=~p restored_exact=~p failed_loudly=~p~n",
                          [length(Results), length([ x || ok <- Results ]),
                           length([ x || {failed, _} <- Results ])])
        end
    end}.

fuzz_round(Round, Ms) ->
    Seed = erlang:unique_integer([positive]),
    rand:seed(exsss, {Seed, Round, 17}),
    application:ensure_all_started(hb),
    Dir = "cache-TEST/rev-ex-fuzz-" ++ integer_to_list(Seed),
    Remote = Dir ++ "/remote",
    Inner = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => hb_util:bin(Dir ++ "/lmdb") },
    MaxPending = lists:nth(rand:uniform(3), [10, 40, 200]),
    Store = hb_store_export:wrap(Inner, #{
        <<"path">> => hb_util:bin(Remote),
        <<"journal">> => hb_util:bin(Dir ++ "/journal"),
        <<"max-pending">> => MaxPending,
        <<"segment-bytes">> => 65536,
        <<"segment-ms">> => 200,
        <<"ship-interval-ms">> => 20 }),
    Opts = #{ <<"store">> => [Store] },
    ok = hb_store:start([Store], #{}, Opts),
    Procs = [ hb_util:human_id(crypto:strong_rand_bytes(32)) || _ <- lists:seq(1, 8) ],
    Self = self(),
    Writers =
        [ spawn_link(fun() ->
              Loop = fun L(I) ->
                  receive {stop, From} -> From ! {written, P, I - 1}
                  after 0 ->
                      {ok, ID} = hb_cache:write(#{ <<"v">> => integer_to_binary(I), <<"p">> => P,
                                                   <<"blob">> => crypto:strong_rand_bytes(80) }, Opts),
                      ok = hb_store:link([Store], #{ key(P, I) => ID }, Opts),
                      case rand:uniform(50) of 1 -> timer:sleep(rand:uniform(5)); _ -> ok end,
                      L(I + 1)
                  end
              end,
              Loop(0)
          end)
        || P <- Procs ],
    Deadline = erlang:monotonic_time(millisecond) + Ms,
    Chaos =
        fun C(Events) ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true -> Events;
                false ->
                    timer:sleep(rand:uniform(400)),
                    E = case rand:uniform(6) of
                            1 -> % crash, losing the mailbox
                                case catch writer(Store) of
                                    W when is_pid(W) ->
                                        catch erlang:suspend_process(W),
                                        timer:sleep(rand:uniform(50)),
                                        exit(W, kill), crash;
                                    _ -> none
                                end;
                            2 -> % plain crash
                                case catch writer(Store) of
                                    W when is_pid(W) -> exit(W, kill), kill;
                                    _ -> none
                                end;
                            3 -> % target outage
                                case filelib:is_dir(Remote) of
                                    true ->
                                        ok = file:rename(Remote, Remote ++ ".off"),
                                        ok = file:write_file(Remote, <<"down">>),
                                        outage;
                                    false -> none
                                end;
                            4 -> restore_target(Remote), up;
                            5 -> catch hb_store_export:shutdown_all(), clean_stop;
                            6 -> none
                        end,
                    C([E | Events])
            end
        end,
    Events = Chaos([]),
    [ W ! {stop, Self} || W <- Writers ],
    Written = maps:from_list([ receive {written, P, N} -> {P, N} end || P <- Procs ]),
    restore_target(Remote),
    _ = settle(Store, 300),
    Target = #{ <<"store-module">> => hb_store_lmdb,
                <<"name">> => hb_util:bin(Dir ++ "/restored") },
    Result = (catch hb_store_export:restore(Remote, Target, #{ <<"require-definitions">> => false })),
    Local = snapshot(Opts, Procs, Written),
    Counts = maps:from_list([ {K, length([ x || X <- Events, X == K ])}
                              || K <- [crash, kill, outage, up, clean_stop] ]),
    case Result of
        {ok, _} ->
            Restored = snapshot(#{ <<"store">> => [Target] }, Procs, Written),
            Diff = [ K || {K, V} <- maps:to_list(Local), maps:get(K, Restored, missing) =/= V ],
            io:format(user, "~nEXPORT_FUZZ round=~p max_pending=~p events=~p written=~p restore=ok diff=~p~n",
                      [Round, MaxPending, Counts, lists:sum(maps:values(Written)), length(Diff)]),
            ?assertEqual([], lists:sublist(Diff, 10)),
            ok;
        Other ->
            io:format(user, "~nEXPORT_FUZZ round=~p max_pending=~p events=~p restore=FAILED ~P~n",
                      [Round, MaxPending, Counts, Other, 12]),
            {failed, Other}
    end.

restore_target(Remote) ->
    case filelib:is_dir(Remote ++ ".off") of
        true ->
            ok = file:delete(Remote),
            ok = file:rename(Remote ++ ".off", Remote);
        false -> ok
    end.

snapshot(Opts, Procs, Written) ->
    maps:from_list(
        [ {{P, I}, case catch hb_cache:read(key(P, I), Opts) of
                       {ok, M} -> catch hb_cache:ensure_all_loaded(M, Opts);
                       E -> {missing, E}
                   end}
        || P <- Procs, I <- lists:seq(0, maps:get(P, Written)) ]).

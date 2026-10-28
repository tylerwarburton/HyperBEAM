%%% @doc A long-lived server that schedules messages for a process.
%%% It acts as a deliberate 'bottleneck' to prevent the server accidentally
%%% assigning multiple messages to the same slot.
-module(dev_scheduler_server).
-export([start/3, schedule/2, stop/1]).
-export([info/1]).
-include_lib("eunit/include/eunit.hrl").
-include("include/hb.hrl").

%%% By default, we wait 10 seconds for a response from the scheduler before
%%% throwing an error on the client. If the scheduler is not able to sequence
%%% the message within this time, it will be discarded upon recipient by the
%%% server. This avoids situations in which the client did not receive 
%%% confirmation of the assignment, but the scheduler still processes it.
-define(DEFAULT_TIMEOUT, 10000).

%% @doc Start a scheduling server for a given computation. Once the server has
%% started it attempts to register on the message ID for the process definition.
%% If there is already a scheduler registered on the message ID, it will return
%% the existing PID and log a warning. If the read of the latest assignment
%% raises, the server does not start, and the exception is raised again in the
%% caller.
start(ProcID, Proc, Opts) ->
    ?event(scheduling, {starting_scheduling_server, {proc_id, ProcID}}),
    case mode(Opts, disabled) of
        disabled ->
            throw({scheduling_disabled_on_node, {requested_for, ProcID}});
        _ -> ok
    end,
    Ref = make_ref(),
    Caller = self(),
    {Server, MonRef} = spawn_monitor(
        fun() ->
            % Before we start, register the scheduler name.
            case hb_name:register(dev_scheduler_registry:name(ProcID, Opts)) of
                ok -> ok;
                error ->
                    % Another scheduler is already registered on the process
                    % message ID, so we return the existing PID to the caller
                    % rather than our own.
                    ExistingPid = dev_scheduler_registry:find(ProcID, false, Opts),
                    ?event(
                        warning,
                        {another_scheduler_is_already_registered,
                            {process_message_id, ProcID},
                            {existing_pid, ExistingPid}
                        }
                    ),
                    % ...and stop here. Going on would write the spawn again
                    % and run a second, unregistered server for the process,
                    % which could hand out a slot the registered one also
                    % hands out.
                    case is_pid(ExistingPid) of
                        true ->
                            Caller ! {ok, Ref, ExistingPid},
                            exit(normal);
                        false ->
                            % The registered server exited between our
                            % registration and the lookup: take the name.
                            case hb_name:register(dev_scheduler_registry:name(ProcID, Opts)) of
                                ok -> ok;
                                error ->
                                    Caller !
                                        {error, Ref, error,
                                            {scheduler_registration_race, ProcID}, []},
                                    exit(normal)
                            end
                    end
            end,
            % Write the process to the cache. We are the provider-of-last-resort
            % for this data.
            % A failure answers the caller instead of leaving it waiting.
            try
                {ok, _} = dev_scheduler_cache:write_spawn(Proc, Opts),
                ok = dev_scheduler_cache:flush(Opts)
            catch SpawnClass:SpawnReason:SpawnStack ->
                hb_name:unregister(dev_scheduler_registry:name(ProcID, Opts)),
                Caller ! {error, Ref, SpawnClass, SpawnReason, SpawnStack},
                exit(normal)
            end,
            HashpathAlg = hb_path:hashpath_alg(Proc, Opts),
            {CurrentSlot, BaseStateHashpath} =
                try dev_scheduler_cache:latest(ProcID, Opts) of
                    not_found ->
                        ?event({starting_new_schedule, {proc_id, ProcID}}),
                        {-1, undefined};
                    {Slot, Latest} ->
                        % Only the latest slot can have been written without
                        % its message index (each slot is flushed before the
                        % next is assigned): write it again.
                        ok = dev_scheduler_cache:write_message_slot(
                            ProcID, hb_maps:get(<<"body">>, Latest, #{}, Opts),
                            Slot, Opts),
                        ok = dev_scheduler_cache:flush(Opts),
                        % The next assignment applies to the state after the
                        % latest one, as `do_assign/3' sets it.
                        {
                            Slot,
                            next_hashpath(
                                hb_ao:get_first(
                                    [
                                        {Latest, <<"base-hashpath">>},
                                        {Latest, <<"hash-chain">>}
                                    ],
                                    #{ <<"hashpath">> => ignore }
                                ),
                                Latest,
                                #{ hashpath_alg => HashpathAlg, opts => Opts }
                            )
                        }
                catch Class:Reason:Stacktrace ->
                    Caller ! {error, Ref, Class, Reason, Stacktrace},
                    exit(normal)
                end,
            ?event(
                {scheduler_got_process_info,
                    {proc_id, ProcID},
                    {initial_slot, CurrentSlot},
                    {base_state_hashpath, BaseStateHashpath}
                }
            ),
            Caller ! {ok, Ref, self()},
            server(
                #{
                    id => ProcID,
                    current => CurrentSlot,
                    base_state_hashpath => BaseStateHashpath,
                    hashpath_alg => HashpathAlg,
                    wallets => commitment_wallets(Proc, Opts),
                    committment_spec => commitment_spec(Proc, Opts),
                    mode => mode(Opts, remote_confirmation),
                    opts => Opts
                }
            )
        end
    ),
    % The server answers before it runs; if it dies first, so does the wait.
    receive
        {ok, Ref, ServerPID} ->
            erlang:demonitor(MonRef, [flush]),
            ServerPID;
        {error, Ref, Class, Reason, Stacktrace} ->
            erlang:demonitor(MonRef, [flush]),
            erlang:raise(Class, Reason, Stacktrace);
        {'DOWN', MonRef, process, Server, Why} ->
            erlang:error({scheduler_server_failed_to_start, ProcID, Why})
    end.

%% @doc Determine the appropriate list of keys to use to commit assignments for
%% a process.
commitment_wallets(ProcMsg, Opts) ->
    SchedulerVal =
        hb_ao:get_first(
            [
                {ProcMsg, <<"scheduler">>},
                {ProcMsg, <<"scheduler-location">>}
            ],
            [],
            Opts
        ),
    lists:filtermap(
        fun(Scheduler) ->
            case hb_opts:as(Scheduler, Opts) of
                {ok, SchedulerOpts} ->
                    case hb_opts:get(priv_wallet, not_found, SchedulerOpts) of
                        not_found -> false;
                        Wallet -> {true, Wallet}
                    end;
                _ ->
                    false
            end
        end,
        dev_scheduler:parse_schedulers(SchedulerVal)
    ).

%% @doc Returns the commitment specification which should be used to commit
%% assignments for a process.
commitment_spec(Proc, Opts) ->
    Default =
        hb_opts:get(
            scheduler_default_commitment_spec,
            <<"ans104@1.0">>,
            Opts
        ),
    case
        hb_ao:get(
            <<"scheduler-commitment-spec">>,
            {as, <<"message@1.0">>, Proc},
            Default,
            Opts
        )
    of
        Spec when is_binary(Spec) -> Spec;
        _ -> Default
    end.

%% @doc Call the appropriate scheduling server to assign a message.
schedule(AOProcID, Message) when is_binary(AOProcID) ->
    schedule(dev_scheduler_registry:find(AOProcID), Message);
schedule(ErlangProcID, Message) ->
    ?event(
        {scheduling_message,
            {proc_id, ErlangProcID},
            {message, Message},
            {is_alive, is_process_alive(ErlangProcID)}
        }
    ),
    AbortTime = scheduler_time() + ?DEFAULT_TIMEOUT,
    ErlangProcID ! {schedule, Message, self(), AbortTime},
    receive
        {scheduled, Message, Assignment} ->
            Assignment;
        {refused, Message, Error} ->
            {error, Error}
    after ?DEFAULT_TIMEOUT ->
        throw({scheduler_timeout, {proc_id, ErlangProcID}, {message, Message}})
    end.

%% @doc Get the current slot from the scheduling server.
info(ProcID) ->
    ?event({getting_info, {proc_id, ProcID}}),
    ProcID ! {info, self()},
    receive {info, Info} -> Info end.

stop(ProcID) ->
    ?event({stopping_scheduling_server, {proc_id, ProcID}}),
    ProcID ! stop.

%% @doc The main loop of the server. Simply waits for messages to assign and
%% returns the current slot.
server(State) ->
    receive
        {schedule, Message, Reply, AbortTime} ->
            case SchedTime = scheduler_time() > AbortTime of
                true ->
                    % Ignore scheduling requests if they are too old. The
                    % `abort-time' signals to us that the client has already
                    % given up on the request, so in order to maintain
                    % predictability we ignore it.
                    ?event(error,
                        {received_old_schedule_request,
                            {abort_time, AbortTime},
                            {sched_time, SchedTime}
                        }
                    ),
                    server(State);
                false ->
                    server(assign(State, Message, Reply))
            end;
        {info, Reply} ->
            Reply ! {info, State},
            server(State);
        stop ->
            ?event({stopping_scheduler_server, {proc_id, maps:get(id, State)}}),
            ok
    end.

%% @doc Assign a message to the next slot.
assign(State, Message, ReplyPID) ->
    try
        do_assign(State, Message, ReplyPID)
    catch
        _Class:Reason:Stack ->
            ?event({error_scheduling, {reason, Reason}, {trace, Stack}}),
            State
    end.

%% @doc Generate and store the actual assignment message.
do_assign(State, Message, ReplyPID) ->
    % Ensure that only committed keys from the message are included in the
    % assignment.
    {ok, OnlyAttested} =
        hb_message:with_only_committed(
            Message,
            Opts = maps:get(opts, State)
        ),
    % A signed message is assigned at most once per process: anyone can post
    % a message again once it is public, and it would run again.
    case dev_scheduler_cache:find_assigned(
            maps:get(id, State), OnlyAttested, maps:get(current, State), Opts) of
        not_found -> do_assign(State, Message, OnlyAttested, ReplyPID);
        {ok, Slot, ID} ->
            ?event(scheduling, {refused_already_assigned,
                {proc_id, maps:get(id, State)}, {slot, Slot}, {message, ID}}),
            ReplyPID ! {refused, Message, #{
                <<"status">> => 409,
                <<"body">> => <<"Message already scheduled.">>,
                <<"process">> => hb_util:human_id(maps:get(id, State)),
                <<"message">> => ID,
                <<"slot">> => Slot
            }},
            State
    end.
do_assign(State, Message, OnlyAttested, ReplyPID) ->
    Opts = maps:get(opts, State),
    % Generate parameters for the assignment message and commit to it.
    BaseStateHashpath = base_state(State),
    NextSlot = maps:get(current, State) + 1,
    {Timestamp, Height, Hash} = ar_timestamp:get(),
    Assignment =
        commit_assignment(
            #{
                <<"path">> =>
                    case hb_path:from_message(request, Message, Opts) of
                        undefined -> <<"compute">>;
                        Path -> hb_path:to_binary(Path)
                    end,
                <<"data-protocol">> => <<"ao">>,
                <<"variant">> => <<"ao.N.1">>,
                <<"process">> => hb_util:id(maps:get(id, State)),
                <<"epoch">> => <<"0">>,
                <<"slot">> => NextSlot,
                <<"block-height">> => Height,
                <<"block-hash">> => Hash,
                <<"block-timestamp">> => Timestamp,
                % Note: Local time on the SU, not Arweave
                <<"timestamp">> => scheduler_time(),
                <<"base-hashpath">> => BaseStateHashpath,
                <<"body">> => OnlyAttested,
                <<"type">> => <<"Assignment">>
            },
            State
        ),
    DispatchFun =
        fun() ->
            AssignmentID = hb_message:id(Assignment, all),
            ?event(scheduling,
                {assigned,
                    {proc_id, maps:get(id, State)},
                    {slot, NextSlot},
                    {assignment, AssignmentID}
                }
            ),
            maybe_inform_recipient(
                aggressive,
                ReplyPID,
                Message,
                Assignment,
                State
            ),
            ?event(starting_message_write),
            ok = dev_scheduler_cache:write(maps:get(id, State), Assignment, Opts),
            % Committed by the flush below, with the assignment.
            ok = dev_scheduler_cache:write_message_slot(
                maps:get(id, State), OnlyAttested, NextSlot, Opts),
            % The store may keep the write in memory: commit it before the
            % slot is confirmed. A failed commit raises, and `assign/3' keeps
            % the previous slot without answering the client.
            ok = dev_scheduler_cache:flush(Opts),
            maybe_inform_recipient(
                local_confirmation,
                ReplyPID,
                Message,
                Assignment,
                State
            ),
            ?event(writes_complete),
            % Publishing is a network round trip made inside the scheduling
            % loop, so it holds every later slot behind this one's upload:
            % measured at ~120 ms a message, 8 msg/s, on a node with no
            % reachable bundler. `scheduler-publish-remote => false' keeps a
            % node's schedule local.
            case hb_opts:get(scheduler_publish_remote, true, Opts) of
                false ->
                    ?event(remote_publication_disabled);
                _ ->
                    ?event(uploading_message),
                    upload(Message, Opts),
                    upload(Assignment, Opts),
                    ?event(uploads_complete)
            end,
            maybe_inform_recipient(
                remote_confirmation,
                ReplyPID,
                Message,
                Assignment,
                State
            )
        end,
    case maps:get(mode, State) of
        aggressive ->
            spawn(DispatchFun);
        Other ->
            ?event({scheduling_mode, Other}),
            DispatchFun()
    end,
    % Update the state with the next hashpath.
    State#{
        current := NextSlot,
        base_state_hashpath := next_hashpath(BaseStateHashpath, Assignment, State)
    }.

%% @doc Commit to the assignment using all of our appropriate wallets.
commit_assignment(BaseAssignment, State) ->
    Wallets = maps:get(wallets, State),
    % Signing writes the nested messages it links (the body) to the first
    % store of its options. Give it the scheduler's store, where the
    % assignment and its body are written next anyway, so no copy of the
    % body lands in the node's store (the irreplaceable store, when one is
    % set, is the scheduler's).
    RawOpts = maps:get(opts, State),
    Opts =
        RawOpts#{
            <<"store">> =>
                hb_opts:get(
                    scheduler_store,
                    hb_opts:get(store, no_viable_store, RawOpts),
                    RawOpts
                )
        },
    CommittmentSpec = maps:get(committment_spec, State),
    lists:foldr(
        fun(Wallet, Assignment) ->
            hb_message:commit(
                Assignment,
                Opts#{ <<"priv-wallet">> => Wallet },
                CommittmentSpec
            )
        end,
        BaseAssignment,
        Wallets
    ).

%% @doc The node's scheduling mode as an atom. Options set in code carry the
%% atom, but a JSON or flat config file delivers the same value as a binary,
%% and the confirmation branches below compare against atoms: a binary matched
%% none of them, so no client was ever told its message had been scheduled and
%% every request timed out. An unknown value is refused here instead of
%% failing the same silent way.
mode(Opts, Default) ->
    case hb_opts:get(scheduling_mode, Default, Opts) of
        Mode when is_atom(Mode) -> Mode;
        <<"aggressive">> -> aggressive;
        <<"local_confirmation">> -> local_confirmation;
        <<"local-confirmation">> -> local_confirmation;
        <<"remote_confirmation">> -> remote_confirmation;
        <<"remote-confirmation">> -> remote_confirmation;
        <<"disabled">> -> disabled;
        Other -> throw({unknown_scheduling_mode, Other})
    end.

%% @doc Potentially inform the caller that the assignment has been scheduled.
%% The main assignment loop calls this function repeatedly at different stages
%% of the assignment process. The scheduling mode determines which stages
%% trigger an update.
maybe_inform_recipient(Mode, ReplyPID, Message, Assignment, State) ->
    case maps:get(mode, State) of
        Mode -> ReplyPID ! {scheduled, Message, Assignment};
        _ -> ok
    end.

%% @doc Upload a message once its assignment is written. The slot is taken by
%% then, so an exception from the upload is logged and returned as an error,
%% and the slot counter still moves past the written slot.
upload(Msg, Opts) ->
    try hb_client_remote:upload(Msg, Opts)
    catch
        Class:Reason:Stack ->
            ?event(warning,
                {upload_failed,
                    {class, Class},
                    {reason, Reason},
                    {trace, Stack}
                }
            ),
            {error, Reason}
    end.

%% @doc Find the hashpath of the base state upon which a new assignment should
%% be applied.
base_state(S = #{ base_state_hashpath := undefined }) ->
    hb_util:id(maps:get(id, S));
base_state(#{ base_state_hashpath := BaseStateHashpath }) ->
    BaseStateHashpath.

%% @doc Generate the next hashpath for a new assignment.
next_hashpath(
        BaseStateHashpath,
        NewAssignment,
        #{ hashpath_alg := HashpathAlg, opts := Opts }
    ) ->
    hb_path:hashpath(
        BaseStateHashpath,
        hb_message:id(NewAssignment, all, Opts),
        HashpathAlg,
        Opts
    ).

%% @doc Return the current time in milliseconds.
scheduler_time() ->
    erlang:system_time(millisecond).

%%% Tests

%% @doc Test the basic functionality of the server.
new_proc_test() ->
    Wallet = ar_wallet:new(),
    SignedItem = hb_message:commit(
        #{ <<"data">> => <<"test">>, <<"random-key">> => rand:uniform(10000) },
        #{ <<"priv-wallet">> => Wallet }
    ),
    SignedItem2 = hb_message:commit(
        #{ <<"data">> => <<"test2">> },
        #{ <<"priv-wallet">> => Wallet }
    ),
    SignedItem3 = hb_message:commit(
        #{
            <<"data">> => <<"test2">>,
            <<"deep-key">> =>
                #{ <<"data">> => <<"test3">> }
        },
        #{ <<"priv-wallet">> => Wallet }
    ),
    dev_scheduler_registry:find(hb_message:id(SignedItem, all), SignedItem),
    schedule(ID = hb_message:id(SignedItem, all), SignedItem),
    schedule(ID, SignedItem2),
    schedule(ID, SignedItem3),
    ?assertMatch(
        #{ current := 2 },
        dev_scheduler_server:info(dev_scheduler_registry:find(ID))
    ).
    

benchmark_test() ->
    BenchTime = 1,
    Wallet = hb:wallet(),
    Opts = #{ <<"priv-wallet">> => Wallet },
    SignedItem = hb_message:commit(
        #{ <<"data">> => <<"test">>, <<"random-key">> => rand:uniform(10000) },
        Opts
    ),
    ID = hb_message:id(SignedItem, all, Opts),
    dev_scheduler_registry:find(ID, SignedItem, Opts),
    ?event({benchmark_start, ?MODULE}),
    Iterations = hb_test_utils:benchmark(
        fun(X) ->
            MsgX = #{
                <<"path">> => <<"Schedule">>,
                <<"method">> => <<"POST">>,
                <<"body">> =>
                    #{
                        <<"type">> => <<"Message">>,
                        <<"test-val">> => X
                    }
            },
            schedule(ID, MsgX)
        end,
        BenchTime
    ),
    hb_format:eunit_print(
        "Scheduled ~p messages in ~p seconds (~.2f msg/s)",
        [Iterations, BenchTime, Iterations / BenchTime]
    ),
    ?assertMatch(
        #{ current := X } when X == Iterations - 1,
        dev_scheduler_server:info(dev_scheduler_registry:find(ID))
    ),
    ?assert(Iterations > 30).

mode_test() ->
    ?assertEqual(aggressive, mode(#{ <<"scheduling-mode">> => aggressive }, x)),
    ?assertEqual(aggressive, mode(#{ <<"scheduling-mode">> => <<"aggressive">> }, x)),
    ?assertEqual(
        local_confirmation,
        mode(#{ <<"scheduling-mode">> => <<"local_confirmation">> }, x)
    ),
    ?assertEqual(disabled, mode(#{ <<"scheduling-mode">> => <<"disabled">> }, x)),
    ?assertThrow(
        {unknown_scheduling_mode, <<"fast">>},
        mode(#{ <<"scheduling-mode">> => <<"fast">> }, x)
    ).

%% @doc Concurrent starts for one process leave exactly one scheduler: every
%% caller gets the same server, the definition is written once, and
%% concurrent schedules get unique, consecutive slots.
concurrent_start_one_server_test_() ->
    {timeout, 90, fun() ->
        Wallet = hb:wallet(),
        Store = hb_test_utils:test_store(hb_store_lmdb, <<"sched-race">>),
        Opts = #{ <<"priv-wallet">> => Wallet, <<"store">> => [Store],
                  <<"scheduling-mode">> => local_confirmation },
        ok = hb_store:start([Store], #{}, Opts),
        Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
        Proc = hb_message:commit(#{
            <<"device">> => <<"process@1.0">>, <<"type">> => <<"Process">>,
            <<"scheduler-device">> => <<"scheduler@1.0">>,
            <<"scheduler-location">> => Address,
            <<"test-random-seed">> => rand:uniform(1 bsl 40) }, Opts),
        ProcID = hb_util:human_id(hb_message:id(Proc, all, Opts)),
        {module, CacheMod} = erlang:fun_info(fun dev_scheduler_cache:write_spawn/2, module),
        Parent = self(),
        Counter = spawn(fun() -> count_calls(0, Parent) end),
        erlang:trace_pattern({CacheMod, write_spawn, 2}, true, [global]),
        erlang:trace(all, true, [call, {tracer, Counter}]),
        Servers0 = count_servers(),
        N = 20,
        [ spawn(fun() -> Parent ! {started, catch start(ProcID, Proc, Opts)} end)
        || _ <- lists:seq(1, N) ],
        Pids = [ receive {started, P} -> P after 30000 -> timeout end
               || _ <- lists:seq(1, N) ],
        timer:sleep(1000),
        erlang:trace(all, false, [call]),
        erlang:trace_pattern({CacheMod, write_spawn, 2}, false, [global]),
        Counter ! {count, self()},
        Writes = receive {calls, C} -> C after 5000 -> timeout end,
        Servers = count_servers() - Servers0,
        ?assertMatch([Pid] when is_pid(Pid), lists:usort(Pids)),
        ?assertEqual(1, Servers),
        ?assertEqual(1, Writes),
        M = 30,
        [ spawn(fun() ->
              Msg = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => I }, Opts),
              A = schedule(hd(Pids), Msg),
              Parent ! {slot, hb_util:int(hb_ao:get(<<"slot">>, A, Opts))}
          end)
        || I <- lists:seq(1, M) ],
        Slots = [ receive {slot, S} -> S after 30000 -> timeout end || _ <- lists:seq(1, M) ],
        ?assertEqual(lists:seq(0, M - 1), lists:sort(Slots))
    end}.

count_servers() ->
    length([ P || P <- processes(),
                  process_info(P, current_function) == {current_function, {?MODULE, server, 1}} ]).

count_calls(N, Parent) ->
    receive
        {trace, _, call, _} -> count_calls(N + 1, Parent);
        {count, From} -> From ! {calls, N}
    end.

%%% A message is assigned at most once per process.

dedupe_setup(Tag) ->
    Wallet = hb:wallet(),
    Store = hb_test_utils:test_store(hb_store_lmdb, Tag),
    Opts = #{ <<"priv-wallet">> => Wallet, <<"store">> => [Store],
              <<"scheduling-mode">> => local_confirmation },
    ok = hb_store:start([Store], #{}, Opts),
    Proc = dedupe_proc(Opts),
    ProcID = hb_util:human_id(hb_message:id(Proc, all, Opts)),
    {Opts, ProcID, Proc, start(ProcID, Proc, Opts)}.

dedupe_proc(Opts) ->
    hb_message:commit(#{
        <<"device">> => <<"process@1.0">>, <<"type">> => <<"Process">>,
        <<"scheduler-device">> => <<"scheduler@1.0">>,
        <<"scheduler-location">> =>
            hb_util:human_id(ar_wallet:to_address(hb_opts:get(priv_wallet, x, Opts))),
        <<"test-random-seed">> => rand:uniform(1 bsl 40) }, Opts).

slot_of(Assignment) -> hb_util:int(hb_ao:get(<<"slot">>, Assignment, #{})).

%% @doc Posting a signed message again is refused with the slot it already
%% has, also as another encoding of the same signature, and with a second
%% signature added. No slot is taken.
reposted_message_refused_test_() ->
    {timeout, 60, fun() ->
        {Opts, _ProcID, _Proc, Server} = dedupe_setup(<<"dedupe-repost">>),
        Msg = hb_message:commit(
            #{ <<"type">> => <<"Message">>, <<"action">> => <<"Feed">> },
            Opts, <<"ans104@1.0">>),
        [ID] = dev_scheduler_cache:signature_ids(Msg, Opts),
        Slot = slot_of(schedule(Server, Msg)),
        ?assertMatch(
            {error, #{ <<"status">> := 409, <<"slot">> := Slot, <<"message">> := ID }},
            schedule(Server, Msg)),
        % The same signature as an ANS-104 item, serialized and read back.
        TX = ar_bundles:deserialize(ar_bundles:serialize(
            hb_message:convert(Msg, <<"ans104@1.0">>, Opts))),
        FromItem = hb_message:convert(TX, <<"structured@1.0">>, <<"ans104@1.0">>, Opts),
        ?assertEqual([ID], dev_scheduler_cache:signature_ids(FromItem, Opts)),
        ?assertMatch({error, #{ <<"slot">> := Slot }}, schedule(Server, FromItem)),
        % ...and through HTTP signature form.
        ViaHTTPSig = hb_message:convert(
            hb_message:convert(Msg, <<"httpsig@1.0">>, Opts),
            <<"structured@1.0">>, <<"httpsig@1.0">>, Opts),
        ?assertMatch({error, #{ <<"slot">> := Slot }}, schedule(Server, ViaHTTPSig)),
        % A second signature does not make it a new message.
        Twice = hb_message:commit(Msg, Opts#{ <<"priv-wallet">> => ar_wallet:new() }),
        ?assertMatch({error, #{ <<"slot">> := Slot, <<"message">> := ID }},
            schedule(Server, Twice)),
        ?assertMatch(#{ current := Slot }, info(Server))
    end}.

%% @doc The same content signed twice is two messages: both are assigned.
same_content_new_signature_assigned_test_() ->
    {timeout, 60, fun() ->
        {Opts, _ProcID, _Proc, Server} = dedupe_setup(<<"dedupe-resign">>),
        Content = #{ <<"type">> => <<"Message">>, <<"action">> => <<"Feed">> },
        A = hb_message:commit(Content, Opts),
        B = hb_message:commit(Content, Opts),
        ?assertNotEqual(dev_scheduler_cache:signature_ids(A, Opts),
            dev_scheduler_cache:signature_ids(B, Opts)),
        S1 = slot_of(schedule(Server, A)),
        S2 = slot_of(schedule(Server, B)),
        ?assertEqual(S1 + 1, S2)
    end}.

%% @doc Concurrent posts of one message take exactly one slot.
concurrent_identical_posts_one_slot_test_() ->
    {timeout, 60, fun() ->
        {Opts, ProcID, _Proc, Server} = dedupe_setup(<<"dedupe-concurrent">>),
        Msg = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => 1 }, Opts),
        Before = length(dev_scheduler_cache:list(ProcID, Opts)),
        Parent = self(),
        N = 20,
        [ spawn(fun() -> Parent ! {res, schedule(Server, Msg)} end) || _ <- lists:seq(1, N) ],
        Res = [ receive {res, R} -> R after 30000 -> timeout end || _ <- lists:seq(1, N) ],
        {Refused, Assigned} = lists:partition(fun(R) -> element(1, R) == error end,
            [ case R of {error, _} -> R; _ -> {ok, R} end || R <- Res ]),
        ?assertEqual(1, length(Assigned)),
        [{ok, A}] = Assigned,
        Slot = slot_of(A),
        ?assert(lists:all(fun({error, #{ <<"slot">> := S }}) -> S == Slot end, Refused)),
        ?assertEqual(Before + 1, length(dev_scheduler_cache:list(ProcID, Opts)))
    end}.

%% @doc An index row whose assignment was never committed does not refuse the
%% message; an assignment committed without its index row (a crash between
%% the two) gets one when the server starts again.
torn_index_test_() ->
    {timeout, 60, fun() ->
        {Opts, ProcID, Proc, Server} = dedupe_setup(<<"dedupe-torn">>),
        #{ current := Current } = info(Server),
        % A row past the latest slot (its assignment never committed).
        Stale = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => 1 }, Opts),
        ok = dev_scheduler_cache:write_message_slot(ProcID, Stale, Current + 1, Opts),
        ?assertEqual(Current + 1, slot_of(schedule(Server, Stale))),
        % A row naming a slot that carries another message (the slot reused).
        Other = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => 2 }, Opts),
        ok = dev_scheduler_cache:write_message_slot(ProcID, Other, Current + 1, Opts),
        ?assertEqual(Current + 2, slot_of(schedule(Server, Other))),
        % An assignment written as a crash would leave it: no index row.
        Unindexed = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => 3 }, Opts),
        #{ base_state_hashpath := Base } = info(Server),
        ok = dev_scheduler_cache:write(ProcID, #{
            <<"variant">> => <<"ao.N.1">>, <<"type">> => <<"Assignment">>,
            <<"process">> => ProcID, <<"slot">> => Current + 3,
            <<"base-hashpath">> => Base, <<"body">> => Unindexed }, Opts),
        ok = dev_scheduler_cache:flush(Opts),
        ?assertEqual(not_found,
            dev_scheduler_cache:find_assigned(ProcID, Unindexed, Current + 3, Opts)),
        exit(Server, kill),
        timer:sleep(100),
        Server2 = start(ProcID, Proc, Opts),
        ?assertMatch({error, #{ <<"slot">> := S }} when S == Current + 3,
            schedule(Server2, Unindexed)),
        ?assertMatch({error, #{ <<"slot">> := S }} when S == Current + 1,
            schedule(Server2, Stale))
    end}.

%% @doc After the server is killed and the store closed and opened again, a
%% message that is not the latest slot is still refused, from its index row
%% on disk. (A real `kill -9' of the VM loses only what is not flushed, and
%% a slot is flushed before it is confirmed; that case is run outside eunit.)
refused_after_restart_test_() ->
    {timeout, 60, fun() ->
        {Opts, ProcID, Proc, Server} = dedupe_setup(<<"dedupe-restart">>),
        [Store] = hb_opts:get(store, [], Opts),
        Msg = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => 1 }, Opts),
        Later = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => 2 }, Opts),
        Slot = slot_of(schedule(Server, Msg)),
        _ = schedule(Server, Later),
        exit(Server, kill),
        timer:sleep(100),
        hb_store:stop(Store),
        ok = hb_store:start([Store], #{}, Opts),
        Server2 = start(ProcID, Proc, Opts),
        ?assertMatch({error, #{ <<"status">> := 409, <<"slot">> := Slot }},
            schedule(Server2, Msg)),
        ?assertMatch(#{ current := S } when S == Slot + 1, info(Server2))
    end}.

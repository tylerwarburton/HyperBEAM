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

%%% At most this many queued schedule requests share one store flush.
-define(MAX_BATCH, 32).

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
                        % A crash can leave the last assignments written
                        % without their index rows: the rows follow each
                        % assignment, and what is on disk is a prefix of
                        % the writes, so at most the last batch's. Write
                        % those rows again.
                        ok = repair_index(ProcID, Slot, Latest, Opts),
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
            % What is on disk now is durable; in `aggressive' mode a client
            % is answered before its write, so nothing is held back.
            ok =
                case mode(Opts, remote_confirmation) of
                    aggressive -> dev_scheduler_cache:clear_durable(ProcID);
                    _ -> dev_scheduler_cache:set_durable(ProcID, CurrentSlot)
                end,
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

%% @doc Write the message index rows of the last `?MAX_BATCH' assignments,
%% up to and including the latest, again.
repair_index(ProcID, Latest, LatestAssignment, Opts) ->
    lists:foreach(
        fun(Slot) ->
            Read =
                case Slot of
                    Latest -> {ok, LatestAssignment};
                    _ -> dev_scheduler_cache:read(ProcID, Slot, Opts)
                end,
            case Read of
                {ok, Assignment} ->
                    ok = dev_scheduler_cache:write_message_slot(
                        ProcID,
                        dev_scheduler_cache:signature_ids(
                            hb_maps:get(<<"body">>, Assignment, #{}, Opts), Opts),
                        Slot, Opts);
                not_found -> ok
            end
        end,
        lists:seq(max(0, Latest - ?MAX_BATCH + 1), Latest)
    ),
    dev_scheduler_cache:flush(Opts).

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

%% @doc The main loop of the server. Waits for messages to assign. Requests
%% already queued when one arrives are assigned with it, as one batch (see
%% `assign_batch/2'); the server never waits for a batch to fill.
server(State) ->
    receive
        {schedule, Message, Reply, AbortTime} ->
            Requests =
                [{Message, Reply, AbortTime} | queued(?MAX_BATCH - 1)],
            server(assign_batch(State, Requests));
        {info, Reply} ->
            Reply ! {info, State},
            server(State);
        stop ->
            ?event({stopping_scheduling_server, {proc_id, maps:get(id, State)}}),
            ok
    end.

%% @doc The schedule requests already in the mailbox, oldest first, at most
%% `N'. Never waits.
queued(0) -> [];
queued(N) ->
    receive
        {schedule, Message, Reply, AbortTime} ->
            [{Message, Reply, AbortTime} | queued(N - 1)]
    after 0 -> []
    end.

%% @doc Assign a batch of requests in arrival order. Each one is built, signed
%% and written as before; then the store is flushed ONCE for the batch, and
%% only then is each client told its slot, in slot order. A confirmation never
%% precedes the flush of its assignment. A request that fails is skipped and
%% keeps no slot. If the flush fails, nobody is answered and the slot counter
%% stays where it was before the batch, as a failed flush did per message.
%% In `aggressive' mode a client is told before the write, so there is nothing
%% to batch: each request goes the old way.
assign_batch(State = #{ mode := aggressive }, Requests) ->
    lists:foldl(
        fun({Message, Reply, AbortTime}, S) ->
            case too_old(AbortTime) of
                true -> S;
                false -> assign(S, Message, Reply)
            end
        end,
        State,
        Requests
    );
assign_batch(State, Requests) ->
    {Assigned, Written} =
        lists:foldl(
            fun({Message, Reply, AbortTime}, {S, Acc}) ->
                case too_old(AbortTime) of
                    true -> {S, Acc};
                    false ->
                        try write_assignment(S, Message) of
                            {refused, Error} ->
                                {S, [{refused, Reply, Message, Error} | Acc]};
                            {S2, Assignment} ->
                                {S2, [{Reply, Message, Assignment} | Acc]}
                        catch
                            _Class:Reason:Stack ->
                                ?event({error_scheduling,
                                    {reason, Reason}, {trace, Stack}}),
                                {S, Acc}
                        end
                end
            end,
            {State, []},
            Requests
        ),
    case Written of
        [] -> Assigned;
        _ ->
            try ok = dev_scheduler_cache:flush(maps:get(opts, State)) of
                ok ->
                    % The batch is durable: only now may its slots be
                    % computed, pushed or served.
                    ok = dev_scheduler_cache:set_durable(
                        maps:get(id, State), maps:get(current, Assigned)),
                    % A refusal can name a slot of this batch, so it waits
                    % for the flush too.
                    lists:foreach(
                        fun({refused, Reply, Message, Error}) ->
                               Reply ! {refused, Message, Error};
                           ({Reply, Message, Assignment}) ->
                            confirm(Reply, Message, Assignment, Assigned)
                        end,
                        lists:reverse(Written)
                    ),
                    Assigned
            catch
                _FClass:FReason:FStack ->
                    % Nobody is answered. The batch may be on disk all the
                    % same (a commit whose sync then failed), so the slot
                    % counter cannot be moved back: stop, and the next
                    % request starts a server from what the store holds.
                    ?event(error, {scheduler_flush_failed,
                        {reason, FReason}, {trace, FStack}}),
                    exit({scheduler_flush_failed, FReason})
            end
    end.

%% @doc Whether the client has already given up on a request. Such requests
%% are ignored, so that no slot is taken for a message nobody was told about.
too_old(AbortTime) ->
    case (SchedTime = scheduler_time()) > AbortTime of
        true ->
            ?event(error,
                {received_old_schedule_request,
                    {abort_time, AbortTime},
                    {sched_time, SchedTime}
                }
            ),
            true;
        false -> false
    end.

%% @doc Build, sign and write the next assignment, and its message index row
%% right after it; return the next state. A signed message is assigned at
%% most once per process (anyone can post a public message again, and it
%% would run again): one already assigned is `{refused, Error}'.
write_assignment(State, Message) ->
    Opts = maps:get(opts, State),
    ProcID = maps:get(id, State),
    {ok, OnlyAttested} = hb_message:with_only_committed(Message, Opts),
    IDs = dev_scheduler_cache:signature_ids(OnlyAttested, Opts),
    case dev_scheduler_cache:find_assigned(
            ProcID, IDs, maps:get(current, State), Opts) of
        {ok, Slot, ID} -> {refused, refusal(State, Slot, ID)};
        not_found ->
            {Assignment, Next} = build_assignment(State, Message, OnlyAttested),
            ok = dev_scheduler_cache:write(ProcID, Assignment, Opts),
            ok = dev_scheduler_cache:write_message_slot(
                ProcID, IDs, maps:get(current, Next), Opts),
            {Next, Assignment}
    end.

refusal(State, Slot, ID) ->
    ?event(scheduling, {refused_already_assigned,
        {proc_id, maps:get(id, State)}, {slot, Slot}, {message, ID}}),
    #{
        <<"status">> => 409,
        <<"body">> => <<"Message already scheduled.">>,
        <<"process">> => hb_util:human_id(maps:get(id, State)),
        <<"message">> => ID,
        <<"slot">> => Slot
    }.

%% @doc Answer a client whose assignment is flushed, publishing it first if
%% the node publishes remotely.
confirm(ReplyPID, Message, Assignment, State) ->
    Opts = maps:get(opts, State),
    maybe_inform_recipient(local_confirmation, ReplyPID, Message, Assignment, State),
    % Publishing is a network round trip made inside the scheduling loop:
    % measured at ~120 ms a message on a node with no reachable bundler.
    % `scheduler-publish-remote => false' keeps a node's schedule local.
    case hb_opts:get(scheduler_publish_remote, true, Opts) of
        false -> ok;
        _ ->
            upload(Message, Opts),
            upload(Assignment, Opts)
    end,
    maybe_inform_recipient(remote_confirmation, ReplyPID, Message, Assignment, State).

%% @doc Assign a message to the next slot (`aggressive' mode).
assign(State, Message, ReplyPID) ->
    try
        do_assign(State, Message, ReplyPID)
    catch
        _Class:Reason:Stack ->
            ?event({error_scheduling, {reason, Reason}, {trace, Stack}}),
            State
    end.

%% @doc Tell the client first, then write the assignment in the background.
do_assign(State, Message, ReplyPID) ->
    Opts = maps:get(opts, State),
    ProcID = maps:get(id, State),
    {ok, OnlyAttested} = hb_message:with_only_committed(Message, Opts),
    IDs = dev_scheduler_cache:signature_ids(OnlyAttested, Opts),
    case dev_scheduler_cache:find_assigned(
            ProcID, IDs, maps:get(current, State), Opts) of
        {ok, Slot, ID} ->
            ReplyPID ! {refused, Message, refusal(State, Slot, ID)},
            State;
        not_found -> do_assign(State, Message, OnlyAttested, IDs, ReplyPID)
    end.
do_assign(State, Message, OnlyAttested, IDs, ReplyPID) ->
    Opts = maps:get(opts, State),
    {Assignment, Next} = build_assignment(State, Message, OnlyAttested),
    spawn(
        fun() ->
            maybe_inform_recipient(aggressive, ReplyPID, Message, Assignment, State),
            ok = dev_scheduler_cache:write(maps:get(id, State), Assignment, Opts),
            ok = dev_scheduler_cache:write_message_slot(
                maps:get(id, State), IDs, maps:get(current, Next), Opts),
            ok = dev_scheduler_cache:flush(Opts),
            confirm(ReplyPID, Message, Assignment, State)
        end
    ),
    Next.

%% @doc Generate and sign the assignment for the next slot, and the state that
%% follows it.
build_assignment(State, Message, OnlyAttested) ->
    % Only the committed keys of the message are in the assignment.
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
    ?event(scheduling,
        {assigned,
            {proc_id, maps:get(id, State)},
            {slot, NextSlot},
            {assignment, hb_message:id(Assignment, all)}
        }
    ),
    {
        Assignment,
        State#{
            current := NextSlot,
            base_state_hashpath :=
                next_hashpath(BaseStateHashpath, Assignment, State)
        }
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
    Store = small_store(hb_test_utils:test_store(hb_store_lmdb, Tag)),
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

ids(Msg, Opts) -> dev_scheduler_cache:signature_ids(Msg, Opts).

%% @doc A test LMDB store maps 1 GiB, not the default 2 TiB: a VM's address
%% space holds about 60 of the default, and a full test run opens more.
small_store(Store) -> Store#{ <<"capacity">> => 1 bsl 30 }.

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
        ok = dev_scheduler_cache:write_message_slot(ProcID, ids(Stale, Opts), Current + 1, Opts),
        ?assertEqual(Current + 1, slot_of(schedule(Server, Stale))),
        % A row naming a slot that carries another message (the slot reused).
        Other = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => 2 }, Opts),
        ok = dev_scheduler_cache:write_message_slot(ProcID, ids(Other, Opts), Current + 1, Opts),
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
            dev_scheduler_cache:find_assigned(ProcID, ids(Unindexed, Opts), Current + 3, Opts)),
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

%% @doc A fresh process with its own LMDB store and a started scheduler.
batch_test_server(Name) ->
    Wallet = hb:wallet(),
    Store = small_store(hb_test_utils:test_store(hb_store_lmdb, Name)),
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
    {start(ProcID, Proc, Opts), Opts}.

%% @doc Send `N' schedule requests while the server is suspended, so that all
%% of them are queued when it next runs. Returns the messages in send order.
queue_requests(Server, N, Opts) ->
    Msgs = [ hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => I }, Opts)
           || I <- lists:seq(1, N) ],
    erlang:suspend_process(Server),
    Abort = scheduler_time() + ?DEFAULT_TIMEOUT,
    [ Server ! {schedule, M, self(), Abort} || M <- Msgs ],
    Msgs.

%% @doc Every confirmation in arrival order, as `{Message, Slot}'.
confirmations(Msgs, Opts) ->
    [ receive
          {scheduled, M, A} -> {M, hb_util:int(hb_ao:get(<<"slot">>, A, Opts))}
      after 30000 -> timeout
      end
    || M <- Msgs ].

%% @doc Queued requests share one flush, and are confirmed in slot order,
%% which is the order they arrived in.
queued_requests_share_one_flush_test_() ->
    {timeout, 90, fun() ->
        {Server, Opts} = batch_test_server(<<"sched-batch">>),
        {module, CacheMod} = erlang:fun_info(fun dev_scheduler_cache:flush/1, module),
        N = 10,
        Msgs = queue_requests(Server, N, Opts),
        erlang:trace_pattern({CacheMod, flush, 1}, true, [local, call_count]),
        erlang:trace(Server, true, [call]),
        erlang:resume_process(Server),
        Confirmed = confirmations(Msgs, Opts),
        erlang:trace(Server, false, [call]),
        {call_count, Flushes} = erlang:trace_info({CacheMod, flush, 1}, call_count),
        erlang:trace_pattern({CacheMod, flush, 1}, false, [local, call_count]),
        ?assertEqual(lists:zip(Msgs, lists:seq(0, N - 1)), Confirmed),
        ?assertEqual(1, Flushes)
    end}.

%% @doc No client hears of its slot before the flush that follows the write of
%% its assignment has returned: the server's own calls and sends, in order.
no_confirmation_before_its_flush_test_() ->
    {timeout, 90, fun() ->
        {Server, Opts} = batch_test_server(<<"sched-batch-order">>),
        {module, CacheMod} = erlang:fun_info(fun dev_scheduler_cache:flush/1, module),
        Parent = self(),
        Tracer = spawn(fun() -> collect_trace([], Parent) end),
        Msgs = queue_requests(Server, 6, Opts),
        erlang:trace_pattern({CacheMod, write, 3}, true, [global]),
        erlang:trace_pattern({CacheMod, flush, 1}, [{'_', [], [{return_trace}]}], [global]),
        erlang:trace(Server, true, [call, send, {tracer, Tracer}]),
        erlang:resume_process(Server),
        Confirmed = confirmations(Msgs, Opts),
        erlang:trace(Server, false, [call, send]),
        erlang:trace_pattern({CacheMod, '_', '_'}, false, [global]),
        Tracer ! {done, self()},
        Events = receive {events, E} -> E after 5000 -> [] end,
        % Walk the server's events: a write makes its slot pending, a flush
        % that returns makes every pending slot durable, and a confirmation
        % is only allowed for a durable slot.
        Check =
            fun Walk([], _Pending, _Durable) -> ok;
                Walk([{write, Slot} | Rest], Pending, Durable) ->
                    Walk(Rest, [Slot | Pending], Durable);
                Walk([flushed | Rest], Pending, Durable) ->
                    Walk(Rest, [], Pending ++ Durable);
                Walk([{confirmed, Slot} | Rest], Pending, Durable) ->
                    ?assert(lists:member(Slot, Durable)),
                    Walk(Rest, Pending, Durable)
            end,
        Classified =
            [ Ev || Raw <- Events, Ev <- [classify(Raw, CacheMod, Opts)], Ev =/= skip ],
        ?assertEqual(6, length([ x || {confirmed, _} <- Classified ])),
        ?assertEqual(ok, Check(Classified, [], [])),
        ?assertEqual(lists:seq(0, 5), [ S || {_, S} <- Confirmed ])
    end}.

classify({trace, _, call, {Mod, write, [_, Assignment, _]}}, Mod, Opts) ->
    {write, hb_util:int(hb_ao:get(<<"slot">>, Assignment, Opts))};
classify({trace, _, return_from, {Mod, flush, 1}, ok}, Mod, _Opts) ->
    flushed;
classify({trace, _, send, {scheduled, _, Assignment}, _}, _Mod, Opts) ->
    {confirmed, hb_util:int(hb_ao:get(<<"slot">>, Assignment, Opts))};
classify(_, _, _) -> skip.

collect_trace(Acc, Parent) ->
    receive
        {done, From} -> From ! {events, lists:reverse(Acc)};
        Ev -> collect_trace([Ev | Acc], Parent)
    end.

%% @doc A request to an idle server is flushed and confirmed at once: it does
%% not wait for others to share its flush.
idle_request_confirmed_alone_test_() ->
    {timeout, 60, fun() ->
        {Server, Opts} = batch_test_server(<<"sched-batch-idle">>),
        {module, CacheMod} = erlang:fun_info(fun dev_scheduler_cache:flush/1, module),
        erlang:trace_pattern({CacheMod, flush, 1}, true, [local, call_count]),
        erlang:trace(Server, true, [call]),
        Times =
            [ begin
                  Msg = hb_message:commit(
                      #{ <<"type">> => <<"Message">>, <<"n">> => I }, Opts),
                  {T, _} = timer:tc(fun() -> schedule(Server, Msg) end),
                  T div 1000
              end
            || I <- lists:seq(1, 3) ],
        erlang:trace(Server, false, [call]),
        {call_count, Flushes} = erlang:trace_info({CacheMod, flush, 1}, call_count),
        erlang:trace_pattern({CacheMod, flush, 1}, false, [local, call_count]),
        ?assertEqual(3, Flushes),
        ?assert(lists:max(Times) < 2000)
    end}.

%% @doc A lookup that misses reads its row by exact key: no prefix scan, which
%% would commit the batch's pending writes before reading.
index_miss_reads_no_prefix_test_() ->
    {timeout, 60, fun() ->
        {Opts, ProcID, _Proc, Server} = dedupe_setup(<<"dedupe-exact">>),
        _ = schedule(Server, hb_message:commit(#{ <<"type">> => <<"Message">> }, Opts)),
        % A pending write, as in the middle of a batch.
        ok = dev_scheduler_cache:write_message_slot(
            ProcID, [hb_util:human_id(crypto:strong_rand_bytes(32))], 0, Opts),
        New = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => 2 }, Opts),
        erlang:trace_pattern({hb_store_lmdb, read_prefix_rows, 2}, true, [local, call_count]),
        erlang:trace(self(), true, [call]),
        ?assertEqual(not_found,
            dev_scheduler_cache:find_assigned(ProcID, ids(New, Opts), 0, Opts)),
        erlang:trace(self(), false, [call]),
        {call_count, Prefix} =
            erlang:trace_info({hb_store_lmdb, read_prefix_rows, 2}, call_count),
        erlang:trace_pattern({hb_store_lmdb, read_prefix_rows, 2}, false, [local, call_count]),
        ?assertEqual(0, Prefix)
    end}.

%% @doc A message posted twice inside one batch takes one slot; the second post
%% is refused, naming it, and only after the flush that made it durable.
duplicate_in_batch_refused_after_flush_test_() ->
    {timeout, 90, fun() ->
        {Opts, _ProcID, _Proc, Server} = dedupe_setup(<<"dedupe-batch">>),
        {module, CacheMod} = erlang:fun_info(fun dev_scheduler_cache:flush/1, module),
        M = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => 1 }, Opts),
        M2 = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => 2 }, Opts),
        Parent = self(),
        Tracer = spawn(fun() -> collect_trace([], Parent) end),
        erlang:suspend_process(Server),
        Abort = scheduler_time() + ?DEFAULT_TIMEOUT,
        [ Server ! {schedule, X, self(), Abort} || X <- [M, M, M2] ],
        erlang:trace_pattern({CacheMod, flush, 1}, [{'_', [], [{return_trace}]}], [global]),
        erlang:trace(Server, true, [call, send, {tracer, Tracer}]),
        erlang:resume_process(Server),
        Replies = [ receive {R, _, V} when R == scheduled; R == refused -> {R, V}
                    after 30000 -> timeout end || _ <- lists:seq(1, 3) ],
        erlang:trace(Server, false, [call, send]),
        erlang:trace_pattern({CacheMod, '_', '_'}, false, [global]),
        Tracer ! {done, self()},
        Events = receive {events, E} -> E after 5000 -> [] end,
        [{scheduled, A1}, {refused, #{ <<"status">> := 409, <<"slot">> := S1 }},
            {scheduled, A2}] = Replies,
        ?assertEqual(S1, slot_of(A1)),
        ?assertEqual(S1 + 1, slot_of(A2)),
        Order =
            [ case Ev of
                  {trace, _, return_from, {CacheMod, flush, 1}, ok} -> flushed;
                  {trace, _, send, {refused, _, _}, _} -> refused;
                  _ -> skip
              end || Ev <- Events ],
        ?assertEqual([flushed, refused], [ X || X <- Order, X =/= skip ])
    end}.

%% @doc If a batch's flush fails, nobody is answered, not even a refusal, and
%% the server stops: a new one resumes from what the store holds.
failed_flush_answers_nobody_and_stops_test_() ->
    {timeout, 90, fun() ->
        Wallet = hb:wallet(),
        Inner = small_store(hb_test_utils:test_store(hb_store_lmdb, <<"dedupe-flushfail">>)),
        Store = hb_store_offsite_journal:wrap(Inner,
            #{ <<"path">> => <<(maps:get(<<"name">>, Inner))/binary, "-export">> }),
        Opts = #{ <<"priv-wallet">> => Wallet, <<"store">> => [Store],
                  <<"scheduling-mode">> => local_confirmation },
        ok = hb_store:start([Store], #{}, Opts),
        Proc = dedupe_proc(Opts),
        ProcID = hb_util:human_id(hb_message:id(Proc, all, Opts)),
        Server = start(ProcID, Proc, Opts),
        M = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => 1 }, Opts),
        Ref = monitor(process, Server),
        erlang:suspend_process(Server),
        Abort = scheduler_time() + ?DEFAULT_TIMEOUT,
        [ Server ! {schedule, M, self(), Abort} || _ <- [1, 2] ],
        persistent_term:put({hb_store_offsite_journal, fail_sync}, true),
        try
            erlang:resume_process(Server),
            ?assertMatch({'DOWN', Ref, process, Server, {scheduler_flush_failed, _}},
                receive {'DOWN', Ref, _, _, _} = D -> D after 10000 -> timeout end),
            ?assertEqual(none,
                receive {R, M, _} when R == scheduled; R == refused -> R after 500 -> none end)
        after
            persistent_term:erase({hb_store_offsite_journal, fail_sync})
        end
    end}.

%% @doc Assignments written without their index rows, as many as one batch,
%% get them when the server starts again.
start_repairs_last_batch_test_() ->
    {timeout, 90, fun() ->
        {Opts, ProcID, Proc, Server} = dedupe_setup(<<"dedupe-repair">>),
        Msgs = [ hb_message:commit(#{ <<"type">> => <<"Message">>, <<"n">> => I }, Opts)
               || I <- lists:seq(1, 5) ],
        Slots = [ slot_of(schedule(Server, X)) || X <- Msgs ],
        exit(Server, kill),
        timer:sleep(100),
        [Store] = hb_opts:get(store, [], Opts),
        #{ <<"db">> := DB } = hb_store:find(Store),
        {ok, _} = elmdb:delete_batch(DB,
            [ <<"~scheduler@1.0/message-slots/", ProcID/binary, "/", ID/binary>>
            || X <- Msgs, ID <- ids(X, Opts) ]),
        ok = hb_store:flush(Store, #{}),
        Server2 = start(ProcID, Proc, Opts),
        ?assertEqual(
            [ {error, S} || S <- Slots ],
            [ case schedule(Server2, X) of
                  {error, #{ <<"slot">> := S }} -> {error, S};
                  Other -> Other
              end || X <- Msgs ])
    end}.

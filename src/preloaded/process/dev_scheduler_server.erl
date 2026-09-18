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
%% the existing PID and log a warning.
start(ProcID, Proc, Opts) ->
    ?event(scheduling, {starting_scheduling_server, {proc_id, ProcID}}),
    Ref = make_ref(),
    Caller = self(),
    spawn(
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
                    Caller ! {ok, Ref, ExistingPid}
            end,
            % Write the process to the cache. We are the provider-of-last-resort
            % for this data.
            dev_scheduler_cache:write_spawn(Proc, Opts),
            case hb_opts:get(scheduling_mode, disabled, Opts) of
                disabled ->
                    throw({scheduling_disabled_on_node, {requested_for, ProcID}});
                _ -> ok
            end,
            HashpathAlg = hb_path:hashpath_alg(Proc, Opts),
            {CurrentSlot, BaseStateHashpath} =
                case dev_scheduler_cache:latest(ProcID, Opts) of
                    not_found ->
                        ?event({starting_new_schedule, {proc_id, ProcID}}),
                        {-1, undefined};
                    {Slot, Base} ->
                        {Slot, Base}
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
                    mode =>
                        hb_opts:get(
                            scheduling_mode,
                            remote_confirmation,
                            Opts
                        ),
                    opts => Opts,
                    uploaders => start_uploaders(Opts)
                }
            )
        end
    ),
    receive
        {ok, Ref, ServerPID} -> ServerPID
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
    hb_ao:get(
        <<"scheduler-commitment-spec">>,
        {as, <<"message@1.0">>, Proc},
        hb_opts:get(
            scheduler_default_commitment_spec,
            <<"ans104@1.0">>,
            Opts
        ),
        Opts
    ).

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
            Assignment
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
        {'DOWN', _Ref, process, Uploader, Reason} ->
            Uploaders = maps:get(uploaders, State),
            case lists:member(Uploader, Uploaders) of
                true ->
                    ?event(warning,
                        {scheduler_uploader_down, {reason, Reason}}
                    ),
                    NewUploader = start_uploader(maps:get(opts, State)),
                    server(
                        State#{
                            uploaders :=
                                [
                                    case PID == Uploader of
                                        true -> NewUploader;
                                        false -> PID
                                    end
                                ||
                                    PID <- Uploaders
                                ]
                        }
                    );
                false ->
                    server(State)
            end;
        stop ->
            ?event({stopping_scheduler_server, {proc_id, maps:get(id, State)}}),
            lists:foreach(
                fun(Uploader) -> Uploader ! stop end,
                maps:get(uploaders, State)
            ),
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
                <<"block-hash">> => hb_util:human_id(Hash),
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
            ok = dev_scheduler_cache:write(Assignment, Opts),
            maybe_inform_recipient(
                local_confirmation,
                ReplyPID,
                Message,
                Assignment,
                State
            ),
            ?event(writes_complete),
            ?event(uploading_message),
            % Uploading is a network round trip; running it from the loop
            % holds the next slot behind the current slot's upload. Hand the
            % uploads to a bounded pool that drains them off the loop. The
            % default pool size of one preserves upload order. Operators can
            % opt into parallel, content-addressed publication when one remote
            % connection cannot keep up with local assignment throughput.
            % Slot order, local persistence and `local_confirmation' are
            % unaffected; `remote_confirmation' informs from the worker that
            % published its assignment. The queued-message bound applies to
            % the pool as a whole; each worker adds at most one in-flight item.
            Max = hb_opts:get(scheduler_upload_queue_max, 64, Opts),
            case select_uploader(maps:get(uploaders, State), Max) of
                {ok, Uploader} ->
                    Uploader !
                        {upload,
                            Message,
                            Assignment,
                            maps:get(mode, State),
                            ReplyPID
                        };
                _ ->
                    UploadOpts = upload_opts(Opts),
                    upload(
                        Message,
                        Assignment,
                        maps:get(mode, State),
                        ReplyPID,
                        UploadOpts
                    )
            end
        end,
    case hb_opts:get(scheduling_mode, sync, Opts) of
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
    Opts = maps:get(opts, State),
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

%% @doc Potentially inform the caller that the assignment has been scheduled.
%% The main assignment loop calls this function repeatedly at different stages
%% of the assignment process. The scheduling mode determines which stages
%% trigger an update.
maybe_inform_recipient(Mode, ReplyPID, Message, Assignment, State) ->
    case maps:get(mode, State) of
        Mode -> ReplyPID ! {scheduled, Message, Assignment};
        _ -> ok
    end.

%% @doc Start the configured number of monitored uploader processes.
start_uploaders(Opts) ->
    Count =
        case hb_opts:get(scheduler_upload_workers, 1, Opts) of
            N when is_integer(N) andalso N > 0 -> N;
            _ -> 1
        end,
    [start_uploader(Opts) || _ <- lists:seq(1, Count)].

%% @doc Start one monitored uploader with request-local options removed.
start_uploader(Opts) ->
    {Uploader, _Ref} = spawn_monitor(fun() -> uploader(upload_opts(Opts)) end),
    Uploader.

%% @doc Select the least-loaded live uploader while enforcing one pool bound.
select_uploader(Uploaders, Max) ->
    QueueLengths =
        lists:filtermap(
            fun(Uploader) ->
                case erlang:process_info(Uploader, message_queue_len) of
                    {message_queue_len, Pending} ->
                        {true, {Pending, Uploader}};
                    undefined ->
                        false
                end
            end,
            Uploaders
        ),
    % Treat every live worker as in-flight when enforcing the bound. This is
    % conservative for idle workers, but ensures that increasing parallelism
    % never widens the maximum number of retained upload jobs.
    Retained =
        length(QueueLengths)
        + lists:sum([Pending || {Pending, _} <- QueueLengths]),
    case QueueLengths =/= [] andalso Retained < Max of
        true ->
            {_Pending, Uploader} = lists:min(QueueLengths),
            {ok, Uploader};
        false ->
            full
    end.

%% @doc Remove request-local monitoring state from asynchronous uploads.
upload_opts(Opts) ->
    maps:remove(<<"http-monitor">>, Opts).

%% @doc Drain assignment uploads to the bundler, off the scheduling loop and in
%% the order each worker receives them. Uploading is a network round trip; run
%% from the loop it would bound a process's assignment rate to upload latency.
%% Upload failures are swallowed, so a slow or failed bundler never stalls or
%% crashes the scheduler. `remote_confirmation' is informed from here, after
%% the upload.
uploader(Opts) ->
    receive
        {upload, Message, Assignment, Mode, ReplyPID} ->
            upload(Message, Assignment, Mode, ReplyPID, Opts),
            uploader(Opts);
        stop ->
            ok
    end.

%% @doc Upload one message and assignment without allowing remote failures to
%% roll back a slot that has already been committed to the local schedule.
upload(Message, Assignment, Mode, ReplyPID, Opts) ->
    try
        MessageResult = hb_client_remote:upload(Message, Opts),
        AssignmentResult = hb_client_remote:upload(Assignment, Opts),
        case source_upload_succeeded(MessageResult)
                andalso upload_succeeded(AssignmentResult) of
            true ->
                ?event(uploads_complete),
                maybe_inform_recipient(
                    remote_confirmation,
                    ReplyPID,
                    Message,
                    Assignment,
                    #{ mode => Mode }
                );
            false ->
                ?event(error,
                    {upload_failed,
                        {message_result, MessageResult},
                        {assignment_result, AssignmentResult}
                    }
                )
        end
    catch
        Class:Reason:Stack ->
            ?event(error,
                {upload_failed,
                    {class, Class},
                    {reason, Reason},
                    {trace, Stack}
                }
            )
    end.

%% @doc Confirm that every commitment-specific upload returned successfully.
upload_succeeded({ok, Results}) when is_list(Results), Results =/= [] ->
    lists:all(
        fun
            ({ok, _}) -> true;
            (_) -> false
        end,
        Results
    );
upload_succeeded(_) ->
    false.

%% @doc Source messages may use a commitment without a configured publisher.
%% The successfully published assignment carries that committed message in its
%% body, so this optional source upload does not invalidate publication.
source_upload_succeeded({ok, Results}) when is_list(Results) ->
    lists:all(
        fun
            ({ok, _}) -> true;
            ({error, no_httpsig_bundler}) -> true;
            (_) -> false
        end,
        Results
    );
source_upload_succeeded(_) ->
    false.

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

%% @doc Assignments are sequenced on the loop while their uploads are drained
%% off it, so a run of scheduling requests still advances the slot by one each
%% time and leaves every assignment in the local cache -- independently of
%% whether the bundler upload has completed.
async_upload_preserves_sequence_test() ->
    Wallet = ar_wallet:new(),
    Proc = hb_message:commit(
        #{ <<"data">> => <<"test">>, <<"random-key">> => rand:uniform(10000) },
        #{ <<"priv-wallet">> => Wallet }
    ),
    ID = hb_message:id(Proc, all),
    dev_scheduler_registry:find(ID, Proc),
    Messages =
        [
            hb_message:commit(
                #{ <<"data">> => <<"message">>, <<"index">> => N },
                #{ <<"priv-wallet">> => Wallet }
            )
        ||
            N <- lists:seq(1, 5)
        ],
    lists:foreach(fun(Message) -> schedule(ID, Message) end, Messages),
    State = dev_scheduler_server:info(dev_scheduler_registry:find(ID)),
    % The five messages land on slots 0..4 in order.
    ?assertMatch(#{ current := 4 }, State),
    % Every assignment is readable from the local cache, written on the loop
    % and not gated on the upload.
    Opts = maps:get(opts, State),
    lists:foreach(
        fun(Slot) ->
            ?assertMatch({ok, _}, dev_scheduler_cache:read(ID, Slot, Opts))
        end,
        lists:seq(0, 4)
    ).

%% @doc The pool selects the shortest live queue and enforces a total bound.
select_uploader_test() ->
    Hold = fun() -> receive stop -> ok end end,
    Busy = spawn(Hold),
    Idle = spawn(Hold),
    Busy ! queued,
    ?assertEqual({ok, Idle}, select_uploader([Busy, Idle], 4)),
    ?assertEqual(full, select_uploader([Busy, Idle], 3)),
    exit(Busy, kill),
    timer:sleep(10),
    ?assertEqual({ok, Idle}, select_uploader([Busy, Idle], 2)),
    Idle ! stop.

%% @doc Nested commitment upload errors must not count as confirmation.
upload_succeeded_test() ->
    ?assert(upload_succeeded({ok, [{ok, #{ <<"status">> => 200 }}]})),
    ?assertNot(upload_succeeded({ok, [{error, no_bundler}]})),
    ?assertNot(upload_succeeded({ok, []})),
    ?assertNot(upload_succeeded({error, timeout})),
    ?assert(source_upload_succeeded({ok, [{error, no_httpsig_bundler}]})),
    ?assert(source_upload_succeeded({ok, []})),
    ?assertNot(source_upload_succeeded({ok, [{error, timeout}]})).

%% @doc A configured pool starts every worker and replaces a dead member.
uploader_pool_respawns_test() ->
    Wallet = ar_wallet:new(),
    Proc = hb_message:commit(
        #{ <<"data">> => <<"test">>, <<"random-key">> => rand:uniform(10000) },
        #{ <<"priv-wallet">> => Wallet }
    ),
    ID = hb_message:id(Proc, all),
    dev_scheduler_registry:find(
        ID,
        Proc,
        #{ <<"scheduler-upload-workers">> => 3 }
    ),
    Server = dev_scheduler_registry:find(ID),
    #{ uploaders := Uploaders } = dev_scheduler_server:info(Server),
    ?assertEqual(3, length(Uploaders)),
    ?assert(lists:all(fun erlang:is_process_alive/1, Uploaders)),
    [Killed | _] = Uploaders,
    exit(Killed, kill),
    {ok, Replaced} = wait_for_replacement(Server, Killed, 100),
    ?assertEqual(3, length(Replaced)),
    ?assertNot(lists:member(Killed, Replaced)),
    ?assert(lists:all(fun erlang:is_process_alive/1, Replaced)).

%% @doc Wait for a monitored uploader to be replaced without a timing race.
wait_for_replacement(_Server, _Killed, 0) ->
    timeout;
wait_for_replacement(Server, Killed, Attempts) ->
    #{ uploaders := Uploaders } = dev_scheduler_server:info(Server),
    case lists:member(Killed, Uploaders) of
        false ->
            {ok, Uploaders};
        true ->
            timer:sleep(10),
            wait_for_replacement(Server, Killed, Attempts - 1)
    end.

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

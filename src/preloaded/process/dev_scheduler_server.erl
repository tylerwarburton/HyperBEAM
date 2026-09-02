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
                    uploader => spawn(fun() -> uploader(Opts) end)
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
        stop ->
            ?event({stopping_scheduler_server, {proc_id, maps:get(id, State)}}),
            maps:get(uploader, State) ! stop,
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
            % uploads to a single uploader process that drains them off the
            % loop, in slot order. The slot, commitment, and local cache write
            % are synchronous above, so ordering and `local_confirmation' are
            % unaffected; `remote_confirmation' informs from the uploader.
            % When the uploader has fallen behind the bundler -- its mailbox
            % has grown past `scheduler_upload_queue_max', or it is gone --
            % upload on the loop instead, so a slow bundler becomes
            % backpressure on the caller rather than unbounded memory.
            Uploader = maps:get(uploader, State),
            Max = hb_opts:get(scheduler_upload_queue_max, 64, Opts),
            case erlang:process_info(Uploader, message_queue_len) of
                {message_queue_len, Pending} when Pending < Max ->
                    Uploader !
                        {upload,
                            Message,
                            Assignment,
                            maps:get(mode, State),
                            ReplyPID
                        };
                _ ->
                    hb_client_remote:upload(Message, Opts),
                    hb_client_remote:upload(Assignment, Opts),
                    ?event(uploads_complete),
                    maybe_inform_recipient(
                        remote_confirmation,
                        ReplyPID,
                        Message,
                        Assignment,
                        State
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

%% @doc Drain assignment uploads to the bundler, off the scheduling loop and in
%% the order they were sequenced. Uploading is a network round trip; run from
%% the loop it would bound a process's assignment rate to upload latency. A
%% single uploader per server keeps uploads in slot order and holds the
%% in-flight upload count to one. Upload failures are swallowed, so a slow or
%% failed bundler never stalls or crashes the scheduler. `remote_confirmation'
%% is informed from here, after the upload.
uploader(Opts) ->
    receive
        {upload, Message, Assignment, Mode, ReplyPID} ->
            try
                hb_client_remote:upload(Message, Opts),
                hb_client_remote:upload(Assignment, Opts),
                ?event(uploads_complete),
                maybe_inform_recipient(
                    remote_confirmation,
                    ReplyPID,
                    Message,
                    Assignment,
                    #{ mode => Mode }
                )
            catch
                Class:Reason:Stack ->
                    ?event(error,
                        {upload_failed,
                            {class, Class},
                            {reason, Reason},
                            {trace, Stack}
                        }
                    )
            end,
            uploader(Opts);
        stop ->
            ok
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

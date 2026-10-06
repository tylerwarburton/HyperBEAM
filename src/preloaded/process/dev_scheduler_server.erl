%%% @doc A long-lived server that schedules messages for a process.
%%% It acts as a deliberate 'bottleneck' to prevent the server accidentally
%%% assigning multiple messages to the same slot.
%%%
%%% Each server may be accompanied by two helpers:
%%% <ul>
%%%   <li>A `confirmer', which makes written assignments durable in the store
%%%   (grouping every assignment that is waiting into one store sync) before
%%%   the client is told about them or they are published. A client must never
%%%   hold a signed assignment for a slot that a crash could erase, as the slot
%%%   would then be signed again for a different message.</li>
%%%   <li>An `uploader', which publishes durable assignments to the bundler in
%%%   slot order. Its progress is recorded as a per-process watermark in the
%%%   store, so unpublished assignments are retried after failures and across
%%%   restarts.</li>
%%% </ul>
%%% With `scheduler-durable-confirm' set to `false' and remote publication
%%% disabled, neither helper is started and assignments are confirmed inline.
-module(dev_scheduler_server).
-export([start/3, schedule/2, stop/1]).
-export([info/1, upload_info/1]).
-export([pending_uploads/1, pending_uploads/2, backfill/3]).
-include_lib("eunit/include/eunit.hrl").
-include("include/hb.hrl").

%%% By default, we wait 10 seconds for a response from the scheduler before
%%% throwing an error on the client. If the scheduler is not able to sequence
%%% the message within this time, it will be discarded upon recipient by the
%%% server. This avoids situations in which the client did not receive 
%%% confirmation of the assignment, but the scheduler still processes it.
-define(DEFAULT_TIMEOUT, 10000).

%%% The maximum number of written assignments a confirmer makes durable with a
%%% single store sync.
-define(MAX_CONFIRM_BATCH, 1024).

%%% Bounds of the exponential backoff between attempts to publish a slot.
-define(UPLOAD_BACKOFF_MIN, 1000).
-define(UPLOAD_BACKOFF_MAX, 300000).
%%% How long `upload_info/1' waits for the uploader to answer.
-define(UPLOAD_INFO_TIMEOUT, 1000).

%% @doc Start a scheduling server for a given computation. Once the server has
%% started it attempts to register on the message ID for the process definition.
%% If there is already a scheduler registered on the message ID, it will return
%% the existing PID and log a warning.
start(ProcID, Proc, Opts) ->
    ?event(scheduling, {starting_scheduling_server, {proc_id, ProcID}}),
    Ref = make_ref(),
    Caller = self(),
    {PID, MonRef} =
        spawn_monitor(fun() -> init(Caller, Ref, ProcID, Proc, Opts) end),
    receive
        {ok, Ref, ServerPID} ->
            erlang:demonitor(MonRef, [flush]),
            ServerPID;
        {'DOWN', MonRef, process, PID, Reason} ->
            throw({scheduler_start_failed, {proc_id, ProcID}, {reason, Reason}})
    end.

%% @doc Register the scheduler name, then initialize and run the server. If the
%% name is already taken, return the registered server to the caller and exit
%% without becoming a second server for the process.
init(Caller, Ref, ProcID, Proc, Opts) ->
    case hb_name:register(dev_scheduler_registry:name(ProcID, Opts)) of
        ok ->
            init_registered(Caller, Ref, ProcID, Proc, Opts);
        error ->
            case dev_scheduler_registry:find(ProcID, false, Opts) of
                ExistingPid when is_pid(ExistingPid) ->
                    % Another scheduler is already registered on the process
                    % message ID, so we return the existing PID to the caller
                    % rather than our own.
                    ?event(
                        warning,
                        {another_scheduler_is_already_registered,
                            {process_message_id, ProcID},
                            {existing_pid, ExistingPid}
                        }
                    ),
                    Caller ! {ok, Ref, ExistingPid};
                _ ->
                    % The registered server exited between our attempt to
                    % register and the lookup. Try to register again.
                    init(Caller, Ref, ProcID, Proc, Opts)
            end
    end.

%% @doc Initialize the state of a registered server and enter its loop.
init_registered(Caller, Ref, ProcID, Proc, Opts) ->
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
    Confirmer = start_confirmer(ProcID, CurrentSlot, Opts),
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
            confirmer => Confirmer
        }
    ).

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
            Assignment;
        {schedule_failed, Message, Reason} ->
            throw({scheduler_error, {proc_id, ErlangProcID}, Reason})
    after ?DEFAULT_TIMEOUT ->
        throw({scheduler_timeout, {proc_id, ErlangProcID}, {message, Message}})
    end.

%% @doc Get the current slot from the scheduling server.
info(ProcID) ->
    ?event({getting_info, {proc_id, ProcID}}),
    ProcID ! {info, self()},
    receive {info, Info} -> Info end.

%% @doc Get the state of the uploader of a scheduling server, or `undefined' if
%% remote publication is disabled. The confirmer forwards the request to its
%% uploader, which may have died before the confirmer has noticed and replaced
%% it: the reply goes to a monitor alias, so such a request returns
%% `unavailable' after a timeout and a late reply is dropped.
upload_info(ProcID) ->
    case info(ProcID) of
        #{ confirmer := undefined } -> undefined;
        #{ confirmer := Confirmer } ->
            Mon = erlang:monitor(process, Confirmer, [{alias, reply_demonitor}]),
            Confirmer ! {upload_info, Mon},
            receive
                {upload_info, Info} -> Info;
                {'DOWN', Mon, process, _, _} -> unavailable
            after ?UPLOAD_INFO_TIMEOUT ->
                erlang:demonitor(Mon, [flush]),
                unavailable
            end
    end.

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
                    server(assign(State, Message, Reply, AbortTime))
            end;
        {info, Reply} ->
            Reply ! {info, State},
            server(State);
        {rewind_uploads, To, Reply} ->
            case maps:get(confirmer, State) of
                undefined ->
                    Reply ! {rewound, {error, remote_publication_disabled}};
                Confirmer ->
                    Confirmer ! {rewind_uploads, To, Reply}
            end,
            server(State);
        stop ->
            ?event({stopping_scheduler_server, {proc_id, maps:get(id, State)}}),
            case maps:get(confirmer, State) of
                undefined -> ok;
                Confirmer -> Confirmer ! stop
            end,
            ok
    end.

%% @doc Assign a message to the next slot.
assign(State, Message, ReplyPID, AbortTime) ->
    try
        do_assign(State, Message, ReplyPID, AbortTime)
    catch
        _Class:Reason:Stack ->
            ?event({error_scheduling, {reason, Reason}, {trace, Stack}}),
            State
    end.

%% @doc Generate and store the actual assignment message.
do_assign(State, Message, ReplyPID, AbortTime) ->
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
            ?event(writes_complete),
            Confirmation =
                {confirm,
                    NextSlot,
                    Message,
                    Assignment,
                    ReplyPID,
                    maps:get(mode, State),
                    AbortTime
                },
            case maps:get(confirmer, State) of
                undefined ->
                    % Neither durable confirmation nor remote publication is
                    % enabled: confirm on the loop, as soon as the write
                    % has returned.
                    confirmed(Confirmation, undefined);
                Confirmer ->
                    % The confirmer replies once the write is durable, and
                    % hands the assignment on to be published.
                    Confirmer ! Confirmation
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

%% @doc Act on an assignment that is (as durable as configured) in the store:
%% inform a `local_confirmation' client, and either hand the assignment to the
%% uploader or, if nothing will be published, inform a `remote_confirmation'
%% client too, as there is no remote confirmation to wait for.
confirmed(Confirmation, Uploader) ->
    {confirm, Slot, Message, Assignment, ReplyPID, Mode, AbortTime} =
        Confirmation,
    ModeState = #{ mode => Mode },
    maybe_inform_recipient(
        local_confirmation,
        ReplyPID,
        Message,
        Assignment,
        ModeState
    ),
    case Uploader of
        undefined ->
            maybe_inform_recipient(
                remote_confirmation,
                ReplyPID,
                Message,
                Assignment,
                ModeState
            );
        _ ->
            Reply =
                case Mode of
                    remote_confirmation ->
                        {ReplyPID, Message, Assignment, AbortTime};
                    _ ->
                        undefined
                end,
            Uploader ! {upload, Slot, Assignment, Reply}
    end.

%%% Durable confirmation.

%% @doc Return the level at which assignments are made durable before they are
%% confirmed or published: `commit' (survive the node process being killed),
%% `fsync' (survive a host crash) or `false' (not ensured). Publication always
%% requires at least `commit': a published assignment that a crash erased
%% locally would be published again for a different message.
durable_level(Opts) ->
    Level =
        case hb_opts:get(scheduler_durable_confirm, commit, Opts) of
            Off when Off == false; Off == <<"false">> -> false;
            FSync when FSync == fsync; FSync == <<"fsync">> -> fsync;
            _ -> commit
        end,
    case {Level, publish_remote(Opts)} of
        {false, true} -> commit;
        {fsync, _} -> fsync;
        {false, false} -> false;
        _ -> commit
    end.

%% @doc Is remote publication of assignments enabled?
publish_remote(Opts) ->
    hb_opts:get(scheduler_publish_remote, true, Opts) =/= false.

%% @doc Start the confirmer of a server, if one is needed. It is linked to the
%% server: a failure to make assignments durable must stop the server, as it has
%% already advanced past slots that may not survive a crash.
start_confirmer(ProcID, CurrentSlot, Opts) ->
    case durable_level(Opts) of
        false -> undefined;
        Level ->
            Server = self(),
            spawn_link(
                fun() ->
                    process_flag(trap_exit, true),
                    Uploader =
                        case publish_remote(Opts) of
                            false -> undefined;
                            true -> start_uploader(ProcID, CurrentSlot, Opts)
                        end,
                    confirmer(
                        #{
                            server => Server,
                            proc_id => ProcID,
                            level => Level,
                            durable => CurrentSlot,
                            uploader => Uploader,
                            opts => Opts
                        }
                    )
                end
            )
    end.

%% @doc Wait for written assignments, make every one that is waiting durable
%% with a single store sync, then confirm them in the order they were written.
confirmer(S = #{ server := Server, uploader := Uploader, opts := Opts }) ->
    receive
        {confirm, _, _, _, _, _, _} = First ->
            Batch = collect_confirms([First], 1),
            case dev_scheduler_cache:sync(maps:get(level, S), Opts) of
                ok ->
                    lists:foreach(
                        fun(Confirm) -> confirmed(Confirm, Uploader) end,
                        Batch
                    ),
                    Durable =
                        lists:max(
                            [maps:get(durable, S) |
                                [Slot || {confirm, Slot, _, _, _, _, _} <- Batch]]
                        ),
                    confirmer(S#{ durable := Durable });
                {error, Reason} ->
                    ?event(error,
                        {scheduler_sync_failed,
                            {proc_id, maps:get(proc_id, S)},
                            {reason, Reason}
                        }
                    ),
                    exit({scheduler_sync_failed, Reason})
            end;
        {upload_info, Reply} ->
            case Uploader of
                undefined -> Reply ! {upload_info, undefined};
                _ -> Uploader ! {info, Reply}
            end,
            confirmer(S);
        {rewind_uploads, To, Reply} ->
            case Uploader of
                undefined ->
                    Reply ! {rewound, {error, remote_publication_disabled}};
                _ ->
                    Uploader ! {rewind, To, Reply}
            end,
            confirmer(S);
        {'EXIT', Uploader, Reason} when Uploader =/= undefined ->
            % Resume publication from the persisted watermark, up to the last
            % slot known to be durable.
            ?event(warning, {scheduler_uploader_down, {reason, Reason}}),
            confirmer(
                S#{
                    uploader :=
                        start_uploader(
                            maps:get(proc_id, S),
                            maps:get(durable, S),
                            Opts
                        )
                }
            );
        {'EXIT', Server, Reason} ->
            exit(Reason);
        stop ->
            case Uploader of
                undefined -> ok;
                _ -> Uploader ! stop
            end,
            ok
    end.

%% @doc Gather the confirmations already queued behind the first, in order.
collect_confirms(Acc, ?MAX_CONFIRM_BATCH) ->
    lists:reverse(Acc);
collect_confirms(Acc, N) ->
    receive
        {confirm, _, _, _, _, _, _} = Next -> collect_confirms([Next | Acc], N + 1)
    after 0 -> lists:reverse(Acc)
    end.

%%% Remote publication.

%% @doc Start the uploader of a process, linked to the calling confirmer.
%% Publication resumes after the persisted watermark. A process without a
%% watermark has never been published by this node: unless the
%% `scheduler-upload-backfill' option is set, its existing slots are treated as
%% out of scope (see `backfill/3') and the watermark is made durable at the
%% current slot, so that a crash cannot later move it past unpublished slots.
start_uploader(ProcID, Latest, Opts) ->
    UploadOpts = upload_opts(Opts),
    spawn_link(
        fun() ->
            Mark =
                case dev_scheduler_cache:read_upload_mark(ProcID, UploadOpts) of
                    {ok, Slot} -> Slot;
                    not_found ->
                        Backfill =
                            hb_opts:get(scheduler_upload_backfill, false, Opts),
                        Initial =
                            case Backfill of
                                true -> -1;
                                _ -> Latest
                            end,
                        ok =
                            dev_scheduler_cache:write_upload_mark(
                                ProcID,
                                Initial,
                                UploadOpts
                            ),
                        ok = dev_scheduler_cache:sync(commit, UploadOpts),
                        Initial
                end,
            uploader(
                #{
                    proc_id => ProcID,
                    opts => UploadOpts,
                    mark => Mark,
                    latest => max(Mark, Latest),
                    window => upload_window(Opts),
                    cache_max => hb_opts:get(scheduler_upload_queue_max, 64, Opts),
                    entries => #{},
                    inflight => #{}
                }
            )
        end
    ).

%% @doc The number of slots that may be published concurrently. Attempts are
%% started in slot order and only within this many slots of the watermark, so
%% the default of one publishes strictly in order.
upload_window(Opts) ->
    case hb_opts:get(scheduler_upload_workers, 1, Opts) of
        N when is_integer(N) andalso N > 0 -> N;
        _ -> 1
    end.

%% @doc Remove request-local monitoring state from asynchronous uploads.
upload_opts(Opts) ->
    maps:remove(<<"http-monitor">>, Opts).

%% @doc Publish durable assignments in slot order, off the scheduling loop.
%% Every slot after the watermark (`mark') up to `latest' is pending. Each
%% attempt runs in its own monitored process; failed slots are retried with a
%% capped exponential backoff, and the watermark advances only over a
%% contiguous run of published slots. Assignments are kept in memory up to
%% `cache_max' pending slots and are otherwise re-read from the store.
uploader(S0) ->
    S = dispatch_uploads(S0),
    receive
        {upload, Slot, Assignment, Reply} ->
            uploader(enqueue_upload(Slot, Assignment, Reply, S));
        {'DOWN', Ref, process, _, Result} ->
            uploader(upload_result(Ref, Result, S));
        {rewind, To, Reply} ->
            NewMark = min(To, maps:get(mark, S)),
            ok = write_mark(NewMark, S),
            Reply ! {rewound, ok},
            uploader(S#{ mark := NewMark });
        {info, Reply} ->
            Reply ! {upload_info, S#{ pid => self() }},
            uploader(S);
        stop ->
            ok
    after next_upload_timeout(S) ->
        uploader(S)
    end.

%% @doc Record a newly durable slot, with its assignment if the in-memory bound
%% allows, and the client waiting for its publication, if any.
enqueue_upload(Slot, Assignment, Reply, S = #{ entries := Entries }) ->
    Entry0 = maps:get(Slot, Entries, #{}),
    Entry1 =
        case map_size(Entries) < maps:get(cache_max, S) of
            true -> Entry0#{ assignment => Assignment };
            false -> Entry0
        end,
    Entry2 =
        case Reply of
            undefined -> Entry1;
            _ -> Entry1#{ replies => [Reply | maps:get(replies, Entry1, [])] }
        end,
    S#{
        latest := max(Slot, maps:get(latest, S)),
        entries := Entries#{ Slot => Entry2 }
    }.

%% @doc Start an attempt for every pending slot within the window that is not
%% already in flight, published, or waiting out a backoff.
dispatch_uploads(S = #{ mark := Mark, latest := Latest, window := Window }) ->
    Now = scheduler_time(),
    lists:foldl(
        fun(Slot, AccS = #{ entries := Entries, inflight := Inflight }) ->
            Entry = maps:get(Slot, Entries, #{}),
            Ready =
                not maps:get(done, Entry, false) andalso
                not maps:is_key(ref, Entry) andalso
                maps:get(not_before, Entry, 0) =< Now,
            case Ready of
                false -> AccS;
                true ->
                    {_, Ref} =
                        spawn_monitor(
                            fun() ->
                                exit(
                                    {upload_result,
                                        upload_slot(Slot, Entry, AccS)}
                                )
                            end
                        ),
                    AccS#{
                        entries := Entries#{ Slot => Entry#{ ref => Ref } },
                        inflight := Inflight#{ Ref => Slot }
                    }
            end
        end,
        S,
        lists:seq(Mark + 1, min(Latest, Mark + Window))
    ).

%% @doc The time until the earliest backoff in the window expires.
next_upload_timeout(S = #{ mark := Mark, latest := Latest }) ->
    #{ window := Window, entries := Entries } = S,
    Now = scheduler_time(),
    Waits =
        [
            max(0, NotBefore - Now)
        ||
            Slot <- lists:seq(Mark + 1, min(Latest, Mark + Window)),
            #{ not_before := NotBefore } = Entry <- [maps:get(Slot, Entries, #{})],
            not maps:is_key(ref, Entry),
            not maps:get(done, Entry, false)
        ],
    case Waits of
        [] -> infinity;
        _ -> lists:min(Waits)
    end.

%% @doc Publish one slot: its message (unless already published by an earlier
%% attempt) and then its assignment. Runs in the attempt's own process.
upload_slot(Slot, Entry, #{ proc_id := ProcID, opts := Opts }) ->
    try
        Assignment =
            case maps:find(assignment, Entry) of
                {ok, Cached} -> Cached;
                error ->
                    case dev_scheduler_cache:read(ProcID, Slot, Opts) of
                        {ok, Read} -> Read;
                        not_found -> throw({assignment_not_found, Slot})
                    end
            end,
        MessageStatus =
            case maps:get(message_published, Entry, false) of
                true -> published;
                false -> upload_message(Assignment, Opts)
            end,
        case MessageStatus of
            {error, MessageReason} ->
                {error, false, {message, MessageReason}};
            _ ->
                case upload_status(upload(Assignment, Opts)) of
                    {error, AssignmentReason} ->
                        {error, true, {assignment, AssignmentReason}};
                    _ ->
                        ok
                end
        end
    catch
        Class:Reason:Stack ->
            {error, false, {Class, Reason, Stack}}
    end.

%% @doc Publish the message carried in the body of an assignment as its own
%% item, as upstream does, so that it is retrievable by its own ID. Disabled by
%% `scheduler-upload-message' set to `false'.
upload_message(Assignment, Opts) ->
    case hb_opts:get(scheduler_upload_message, true, Opts) of
        false -> skipped;
        _ ->
            case hb_ao:get(<<"body">>, Assignment, not_found, Opts) of
                Body when is_map(Body) ->
                    upload_status(
                        upload(hb_cache:ensure_all_loaded(Body, Opts), Opts)
                    );
                _ ->
                    skipped
            end
    end.

%% @doc Upload an item's signed commitments. A message read back from the store
%% also carries its derived `hmac' commitment, which is not part of the signed
%% item and would otherwise be sent to the bundler as a second item format.
upload(Msg, Opts) ->
    hb_client_remote:upload(
        hb_message:without_commitments(
            #{ <<"type">> => <<"hmac-sha256">> },
            Msg,
            Opts
        ),
        Opts
    ).

%% @doc Classify the result of `hb_client_remote:upload/2'. Each commitment is
%% uploaded separately: any error fails the item, except that an `httpsig'
%% commitment is not publishable without an `httpsig' bundler. An item with no
%% publishable commitment is `skipped': there is nothing to publish and nothing
%% to retry.
upload_status({ok, Results}) when is_list(Results) ->
    Errors =
        [
            Result
        ||
            Result <- Results,
            not is_tuple(Result) orelse element(1, Result) =/= ok,
            Result =/= {error, no_httpsig_bundler}
        ],
    Accepted =
        [
            Result
        ||
            Result <- Results,
            is_tuple(Result),
            element(1, Result) == ok
        ],
    case {Errors, Accepted =/= []} of
        {[], true} -> ok;
        {[], false} -> skipped;
        {_, _} -> {error, Errors}
    end;
upload_status(Other) ->
    {error, Other}.

%% @doc Apply the result of an attempt: advance the watermark over published
%% slots and inform their clients, or schedule a retry. A client waiting for
%% remote confirmation is told of the failure once its deadline would pass
%% before the next attempt.
upload_result(Ref, Result, S = #{ inflight := Inflight, entries := Entries }) ->
    case maps:take(Ref, Inflight) of
        error -> S;
        {Slot, RestInflight} ->
            Entry = maps:remove(ref, maps:get(Slot, Entries, #{})),
            S1 = S#{ inflight := RestInflight },
            case Result of
                {upload_result, ok} ->
                    lists:foreach(
                        fun({ReplyPID, Message, Assignment, _Abort}) ->
                            ReplyPID ! {scheduled, Message, Assignment}
                        end,
                        maps:get(replies, Entry, [])
                    ),
                    advance_mark(
                        S1#{ entries := Entries#{ Slot => #{ done => true } } }
                    );
                _ ->
                    {MessagePublished, Reason} =
                        case Result of
                            {upload_result, {error, MP, R}} -> {MP, R};
                            Other -> {false, {upload_crashed, Other}}
                        end,
                    Attempts = maps:get(attempts, Entry, 0) + 1,
                    Backoff =
                        min(
                            ?UPLOAD_BACKOFF_MAX,
                            ?UPLOAD_BACKOFF_MIN bsl min(Attempts - 1, 20)
                        ),
                    NotBefore = scheduler_time() + Backoff,
                    ?event(error,
                        {upload_failed,
                            {proc_id, maps:get(proc_id, S)},
                            {slot, Slot},
                            {attempts, Attempts},
                            {retry_in_ms, Backoff},
                            {reason, Reason}
                        }
                    ),
                    Replies =
                        lists:filter(
                            fun({ReplyPID, Message, _Assignment, Abort}) ->
                                case NotBefore >= Abort of
                                    true ->
                                        ReplyPID !
                                            {schedule_failed,
                                                Message,
                                                {remote_confirmation_failed,
                                                    {slot, Slot},
                                                    {retrying, true},
                                                    {reason, Reason}
                                                }
                                            },
                                        false;
                                    false ->
                                        true
                                end
                            end,
                            maps:get(replies, Entry, [])
                        ),
                    S1#{
                        entries :=
                            Entries#{
                                Slot =>
                                    Entry#{
                                        attempts => Attempts,
                                        not_before => NotBefore,
                                        message_published =>
                                            MessagePublished orelse
                                                maps:get(
                                                    message_published,
                                                    Entry,
                                                    false
                                                ),
                                        replies => Replies
                                    }
                            }
                    }
            end
    end.

%% @doc Move the watermark over the contiguous run of published slots after it,
%% persisting it if it moved.
advance_mark(S = #{ mark := Mark }) ->
    case advance_mark(Mark, maps:get(entries, S)) of
        {Mark, _} -> S;
        {NewMark, Entries} ->
            ok = write_mark(NewMark, S),
            S#{ mark := NewMark, entries := Entries }
    end.

advance_mark(Mark, Entries) ->
    case maps:get(Mark + 1, Entries, #{}) of
        #{ done := true } -> advance_mark(Mark + 1, maps:remove(Mark + 1, Entries));
        _ -> {Mark, Entries}
    end.

write_mark(Mark, #{ proc_id := ProcID, opts := Opts }) ->
    dev_scheduler_cache:write_upload_mark(ProcID, Mark, Opts).

%% @doc Report the publication backlog of a process from the store: the slots
%% after its watermark up to its latest assignment. A process without a
%% watermark has never been published by this node, so all of its slots are
%% pending.
pending_uploads(ProcID, Opts) ->
    Latest =
        case dev_scheduler_cache:latest(ProcID, Opts) of
            not_found -> -1;
            {Slot, _} -> Slot
        end,
    Mark =
        case dev_scheduler_cache:read_upload_mark(ProcID, Opts) of
            {ok, M} -> M;
            not_found -> -1
        end,
    #{
        <<"process">> => hb_util:human_id(ProcID),
        <<"uploaded-to">> => Mark,
        <<"latest">> => Latest,
        <<"pending">> => max(0, Latest - Mark)
    }.

%% @doc Report the publication backlog of every process in the scheduler store
%% that has one.
pending_uploads(Opts) ->
    [
        Pending
    ||
        ProcID <- dev_scheduler_cache:processes(Opts),
        #{ <<"pending">> := N } = Pending <- [pending_uploads(ProcID, Opts)],
        N > 0
    ].

%% @doc (Re)publish every assignment of a process from `FromSlot' onwards, by
%% moving its watermark back. Assignments that are already published are
%% published again, which bundlers treat as a duplicate of the same item. If
%% the process has a running scheduler, its uploader is rewound immediately;
%% otherwise the watermark is written and publication resumes when it starts.
backfill(ProcID, FromSlot, Opts) when is_integer(FromSlot), FromSlot >= 0 ->
    case dev_scheduler_registry:find(ProcID, false, Opts) of
        PID when is_pid(PID) ->
            PID ! {rewind_uploads, FromSlot - 1, self()},
            receive {rewound, Result} -> Result
            after ?DEFAULT_TIMEOUT -> {error, timeout}
            end;
        _ ->
            To =
                case dev_scheduler_cache:read_upload_mark(ProcID, Opts) of
                    {ok, Mark} -> min(Mark, FromSlot - 1);
                    not_found -> FromSlot - 1
                end,
            dev_scheduler_cache:write_upload_mark(ProcID, To, Opts)
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

%% @doc Local-only scheduling starts no uploaders, never enters the upload
%% function, and still persists ordered assignments before replying.
remote_publication_disabled_preserves_local_schedule_test() ->
    Wallet = ar_wallet:new(),
    Opts = #{
        <<"priv-wallet">> => Wallet,
        <<"scheduling-mode">> => local_confirmation,
        <<"scheduler-publish-remote">> => false
    },
    Proc = hb_message:commit(
        #{ <<"data">> => <<"test">>, <<"random-key">> => rand:uniform(10000) },
        Opts
    ),
    ID = hb_message:id(Proc, all, Opts),
    Server = dev_scheduler_registry:find(ID, Proc, Opts),
    ?assertEqual(undefined, dev_scheduler_server:upload_info(Server)),
    #{ confirmer := Confirmer } = dev_scheduler_server:info(Server),
    {module, hb_client_remote} = code:ensure_loaded(hb_client_remote),
    1 = erlang:trace_pattern({hb_client_remote, upload, 2}, true, [local]),
    1 = erlang:trace(Server, true, [call]),
    1 = erlang:trace(Confirmer, true, [call]),
    try
        Messages =
            [
                hb_message:commit(
                    #{ <<"data">> => <<"message">>, <<"index">> => N },
                    Opts
                )
            ||
                N <- lists:seq(1, 5)
            ],
        lists:foreach(fun(Message) -> schedule(Server, Message) end, Messages),
        ?assertMatch(#{ current := 4 }, dev_scheduler_server:info(Server)),
        AssignmentIDs = lists:map(
            fun(Slot) ->
                {ok, Assignment} = dev_scheduler_cache:read(ID, Slot, Opts),
                ?assertEqual(Slot, hb_ao:get(<<"slot">>, Assignment, Opts)),
                hb_message:id(Assignment, all, Opts)
            end,
            lists:seq(0, 4)
        ),
        ?assertEqual(5, length(lists:usort(AssignmentIDs))),
        receive
            {trace, _, call, {hb_client_remote, upload, _}} ->
                erlang:error(remote_upload_called)
        after 0 -> ok
        end
    after
        erlang:trace(Server, false, [call]),
        catch erlang:trace(Confirmer, false, [call]),
        erlang:trace_pattern({hb_client_remote, upload, 2}, false, [local]),
        dev_scheduler_server:stop(Server)
    end.

%% @doc Upload results are classified per commitment: any error fails the
%% item, except a missing `httpsig' bundler; an item with nothing publishable
%% is skipped rather than failed.
upload_status_test() ->
    ?assertEqual(ok, upload_status({ok, [{ok, #{ <<"status">> => 200 }}]})),
    ?assertEqual(
        ok,
        upload_status(
            {ok, [
                {error, no_httpsig_bundler},
                {ok, #{ <<"status">> => 200 }}
            ]}
        )
    ),
    ?assertMatch({error, _}, upload_status({ok, [{error, no_bundler}]})),
    ?assertMatch(
        {error, _},
        upload_status({ok, [{ok, #{}}, {failure, timeout}]})
    ),
    ?assertEqual(skipped, upload_status({ok, [{error, no_httpsig_bundler}]})),
    ?assertEqual(skipped, upload_status({ok, []})),
    ?assertMatch({error, _}, upload_status({error, timeout})).

%% @doc Remote publication defaults to enabled: the uploader is started with
%% the configured window and replaced if it dies.
uploader_respawns_test() ->
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
    #{ pid := Uploader, window := 3 } = dev_scheduler_server:upload_info(Server),
    exit(Uploader, kill),
    ?assert(
        wait_for(
            fun() ->
                case dev_scheduler_server:upload_info(Server) of
                    #{ pid := New } -> New =/= Uploader andalso is_process_alive(New);
                    _ -> false
                end
            end,
            1000
        )
    ),
    ?assert(is_process_alive(Server)).

%% @doc `upload_info/1' must not block its caller when no answer comes back,
%% as when the uploader died before the confirmer replaced it.
upload_info_unanswered_returns_test() ->
    Wallet = ar_wallet:new(),
    Proc = hb_message:commit(
        #{ <<"data">> => <<"test">>, <<"random-key">> => rand:uniform(10000) },
        #{ <<"priv-wallet">> => Wallet }
    ),
    ID = hb_message:id(Proc, all),
    dev_scheduler_registry:find(ID, Proc, #{}),
    Server = dev_scheduler_registry:find(ID),
    #{ confirmer := Confirmer } = info(Server),
    erlang:suspend_process(Confirmer),
    try
        {Caller, Mon} =
            spawn_monitor(fun() -> exit({result, upload_info(Server)}) end),
        receive
            {'DOWN', Mon, process, Caller, Result} ->
                ?assertEqual({result, unavailable}, Result)
        after 5000 ->
            exit(Caller, kill),
            ?assert(false)
        end
    after
        erlang:resume_process(Confirmer)
    end,
    ?assertMatch(#{ pid := _ }, upload_info(Server)).

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

%%% Durable confirmation and remote publication tests.

%% @doc Options for a node scheduling with a fresh store and its own wallet.
durability_test_opts(Extra) ->
    maps:merge(
        #{
            <<"priv-wallet">> => ar_wallet:new(),
            <<"store">> => [hb_test_utils:test_store()],
            <<"scheduler-default-commitment-spec">> => <<"ans104@1.0">>
        },
        Extra
    ).

%% @doc A process naming the node's wallet as its scheduler, so that its
%% assignments are signed (and therefore publishable).
durability_test_proc(Opts) ->
    Wallet = hb_opts:get(priv_wallet, no_wallet, Opts),
    hb_message:commit(
        #{
            <<"type">> => <<"Process">>,
            <<"scheduler">> => hb_util:human_id(ar_wallet:to_address(Wallet)),
            <<"random-key">> => rand:uniform(1000000)
        },
        Opts,
        <<"ans104@1.0">>
    ).

durability_test_message(N, Opts) ->
    hb_message:commit(
        #{ <<"type">> => <<"Message">>, <<"index">> => N },
        Opts,
        <<"ans104@1.0">>
    ).

%% @doc Start a mock bundler whose status code is read from a persistent term on
%% every request, returning its URL, handle and the term's key.
mock_bundler(InitialStatus) ->
    Key = {?MODULE, bundler_status, make_ref()},
    persistent_term:put(Key, InitialStatus),
    {ok, URL, Handle} =
        hb_mock_server:start(
            [
                {
                    "/~bundler@1.0/tx",
                    tx,
                    fun(_Req) -> {persistent_term:get(Key), <<"OK">>} end
                }
            ]
        ),
    {URL, Handle, Key}.

%% @doc Wait until a condition holds, polling for up to `Ms' milliseconds.
wait_for(Fun, Ms) when Ms =< 0 -> Fun();
wait_for(Fun, Ms) ->
    case Fun() of
        true -> true;
        false -> timer:sleep(20), wait_for(Fun, Ms - 20)
    end.

%% @doc A local confirmation is only sent once the assignment is committed to
%% the LMDB store, rather than left in its in-memory write overlay where a
%% crash of the node would lose it.
local_confirmation_waits_for_commit_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"sched-durable">>),
    Opts =
        durability_test_opts(
            #{
                <<"store">> => [Store],
                <<"scheduling-mode">> => local_confirmation,
                <<"scheduler-publish-remote">> => false
            }
        ),
    Proc = durability_test_proc(Opts),
    ProcID = hb_message:id(Proc, all, Opts),
    Server = dev_scheduler_registry:find(ProcID, Proc, Opts),
    #{ <<"db">> := DB } = hb_store:find(Store),
    try
        lists:foreach(
            fun(N) ->
                Assignment = schedule(Server, durability_test_message(N, Opts)),
                ?assertEqual(N, hb_ao:get(<<"slot">>, Assignment, Opts)),
                % Nothing is left uncommitted when the client is answered.
                ?assertEqual(0, elmdb:overlay_count(DB))
            end,
            lists:seq(0, 4)
        ),
        ?assertMatch({4, _}, dev_scheduler_cache:latest(ProcID, Opts))
    after
        stop(Server)
    end.

%% @doc With durable confirmation disabled and nothing to publish, no helper
%% processes are started and confirmation happens inline, as before.
durable_confirm_disabled_is_inline_test() ->
    Opts =
        durability_test_opts(
            #{
                <<"scheduling-mode">> => local_confirmation,
                <<"scheduler-publish-remote">> => false,
                <<"scheduler-durable-confirm">> => false
            }
        ),
    Proc = durability_test_proc(Opts),
    ProcID = hb_message:id(Proc, all, Opts),
    Server = dev_scheduler_registry:find(ProcID, Proc, Opts),
    try
        ?assertMatch(#{ confirmer := undefined }, info(Server)),
        ?assertMatch(#{ <<"slot">> := 0 }, schedule(Server, durability_test_message(0, Opts)))
    after
        stop(Server)
    end.

%% @doc Starting a server for a process that already has one returns the
%% existing server, and leaves no second server running.
duplicate_start_leaves_no_orphan_test() ->
    Opts = durability_test_opts(#{ <<"scheduling-mode">> => local_confirmation }),
    Proc = durability_test_proc(Opts),
    ProcID = hb_message:id(Proc, all, Opts),
    Server = dev_scheduler_registry:find(ProcID, Proc, Opts),
    CountServers =
        fun() ->
            length(
                [
                    P
                ||
                    P <- erlang:processes(),
                    erlang:process_info(P, current_function) ==
                        {current_function, {?MODULE, server, 1}}
                ]
            )
        end,
    timer:sleep(100),
    Before = CountServers(),
    try
        ?assertEqual(Server, start(ProcID, Proc, Opts)),
        timer:sleep(200),
        ?assertEqual(Before, CountServers())
    after
        stop(Server)
    end.

%% @doc Under `remote_confirmation', an assignment that has nothing publishable
%% (here, an unsigned assignment of a message signed only with `httpsig') is
%% confirmed rather than leaving the client to time out.
remote_confirmation_without_publishable_commitments_test_() ->
    {timeout, 30, fun remote_confirmation_without_publishable_commitments/0}.
remote_confirmation_without_publishable_commitments() ->
    {URL, Handle, _Key} = mock_bundler(200),
    Opts =
        durability_test_opts(
            #{
                <<"scheduling-mode">> => remote_confirmation,
                <<"bundler-ans104">> => URL
            }
        ),
    % No `scheduler' key: the assignment carries no commitment.
    Proc =
        hb_message:commit(
            #{ <<"type">> => <<"Process">>, <<"random-key">> => rand:uniform(1000000) },
            Opts
        ),
    ProcID = hb_message:id(Proc, all, Opts),
    Server = dev_scheduler_registry:find(ProcID, Proc, Opts),
    try
        Msg = hb_message:commit(#{ <<"index">> => 0 }, Opts, <<"httpsig@1.0">>),
        ?assertMatch(#{ <<"slot">> := 0 }, schedule(Server, Msg))
    after
        stop(Server),
        hb_mock_server:stop(Handle)
    end.

%% @doc Under `remote_confirmation', publication is retried after a failure and
%% the client is answered once it succeeds. Both the message and the assignment
%% are published, one slot at a time, in slot order.
remote_confirmation_retries_failed_upload_test_() ->
    {timeout, 60, fun remote_confirmation_retries_failed_upload/0}.
remote_confirmation_retries_failed_upload() ->
    {URL, Handle, Key} = mock_bundler(500),
    Opts =
        durability_test_opts(
            #{
                <<"scheduling-mode">> => remote_confirmation,
                <<"bundler-ans104">> => URL
            }
        ),
    Proc = durability_test_proc(Opts),
    ProcID = hb_message:id(Proc, all, Opts),
    Server = dev_scheduler_registry:find(ProcID, Proc, Opts),
    try
        Self = self(),
        Msg0 = durability_test_message(0, Opts),
        spawn(fun() -> Self ! {res, catch schedule(Server, Msg0)} end),
        % The first attempt fails; let the bundler recover before the retry.
        hb_mock_server:get_requests(tx, 1, Handle, 5000),
        persistent_term:put(Key, 200),
        receive {res, Res} -> ?assertMatch(#{ <<"slot">> := 0 }, Res)
        after 15000 -> erlang:error(no_reply)
        end,
        lists:foreach(
            fun(N) ->
                ?assertMatch(
                    #{ <<"slot">> := N },
                    schedule(Server, durability_test_message(N, Opts))
                )
            end,
            lists:seq(1, 3)
        ),
        % One failed attempt, then a message and an assignment per slot.
        Requests = hb_mock_server:get_requests(tx, 9, Handle, 5000),
        ?assertEqual(9, length(Requests)),
        ?assertEqual(
            {ok, 3},
            dev_scheduler_cache:read_upload_mark(ProcID, Opts)
        )
    after
        stop(Server),
        hb_mock_server:stop(Handle)
    end.

%% @doc A client waiting for remote confirmation receives an explicit error,
%% rather than a timeout, when publication keeps failing. The slot stays
%% pending, survives a restart of the server, and is published once the
%% bundler recovers.
remote_confirmation_failure_is_explicit_and_resumes_test_() ->
    {timeout, 60, fun remote_confirmation_failure_is_explicit_and_resumes/0}.
remote_confirmation_failure_is_explicit_and_resumes() ->
    {URL, Handle, Key} = mock_bundler(500),
    Opts =
        durability_test_opts(
            #{
                <<"scheduling-mode">> => remote_confirmation,
                <<"bundler-ans104">> => URL
            }
        ),
    Proc = durability_test_proc(Opts),
    ProcID = hb_message:id(Proc, all, Opts),
    Server = dev_scheduler_registry:find(ProcID, Proc, Opts),
    try
        Msg = durability_test_message(0, Opts),
        ?assertThrow(
            {scheduler_error, _, {remote_confirmation_failed, {slot, 0}, _, _}},
            schedule(Server, Msg)
        ),
        % The slot is committed locally but not published.
        ?assertMatch({ok, _}, dev_scheduler_cache:read(ProcID, 0, Opts)),
        ?assertMatch(
            #{ <<"uploaded-to">> := -1, <<"latest">> := 0, <<"pending">> := 1 },
            ?MODULE:pending_uploads(ProcID, Opts)
        ),
        stop(Server),
        ?assert(wait_for(fun() -> not is_process_alive(Server) end, 1000)),
        persistent_term:put(Key, 200),
        % A new server resumes publication from the persisted watermark.
        Server2 = dev_scheduler_registry:find(ProcID, Proc, Opts),
        ?assertNotEqual(Server, Server2),
        ?assert(
            wait_for(
                fun() ->
                    dev_scheduler_cache:read_upload_mark(ProcID, Opts) == {ok, 0}
                end,
                10000
            )
        ),
        ?assertMatch(
            #{ <<"pending">> := 0 },
            ?MODULE:pending_uploads(ProcID, Opts)
        ),
        % The message re-read from the store is published byte-for-byte as the
        % failed attempts sent it from memory.
        [#{ <<"body">> := FromMemory } | _] = Requests =
            hb_mock_server:get_requests(Handle, tx),
        #{ <<"body">> := FromStore } = lists:nth(length(Requests) - 1, Requests),
        ?assertEqual(FromMemory, FromStore),
        stop(Server2)
    after
        stop(Server),
        hb_mock_server:stop(Handle)
    end.

%% @doc A process first published by this node starts its watermark at the
%% current slot; `backfill/3' then republishes its earlier slots.
backfill_publishes_earlier_slots_test_() ->
    {timeout, 60, fun backfill_publishes_earlier_slots/0}.
backfill_publishes_earlier_slots() ->
    {URL, Handle, _Key} = mock_bundler(200),
    BaseOpts =
        durability_test_opts(
            #{
                <<"scheduling-mode">> => local_confirmation,
                <<"bundler-ans104">> => URL,
                <<"scheduler-upload-message">> => false
            }
        ),
    Proc = durability_test_proc(BaseOpts),
    ProcID = hb_message:id(Proc, all, BaseOpts),
    % Schedule two slots with publication disabled.
    LocalOpts = BaseOpts#{ <<"scheduler-publish-remote">> => false },
    Server1 = dev_scheduler_registry:find(ProcID, Proc, LocalOpts),
    lists:foreach(
        fun(N) -> schedule(Server1, durability_test_message(N, LocalOpts)) end,
        [0, 1]
    ),
    stop(Server1),
    ?assert(wait_for(fun() -> not is_process_alive(Server1) end, 1000)),
    ?assertMatch(
        [#{ <<"pending">> := 2 }],
        [P || P = #{ <<"process">> := ID } <- ?MODULE:pending_uploads(BaseOpts),
            ID == hb_util:human_id(ProcID)]
    ),
    % Enabling publication starts from the current slot.
    Server2 = dev_scheduler_registry:find(ProcID, Proc, BaseOpts),
    try
        ?assert(
            wait_for(
                fun() ->
                    dev_scheduler_cache:read_upload_mark(ProcID, BaseOpts) == {ok, 1}
                end,
                5000
            )
        ),
        schedule(Server2, durability_test_message(2, BaseOpts)),
        ?assert(
            wait_for(
                fun() ->
                    dev_scheduler_cache:read_upload_mark(ProcID, BaseOpts) == {ok, 2}
                end,
                5000
            )
        ),
        ?assertEqual(1, length(hb_mock_server:get_requests(Handle, tx))),
        % Backfilling republishes the earlier slots, read from the store.
        ?assertEqual(ok, ?MODULE:backfill(ProcID, 0, BaseOpts)),
        ?assert(
            wait_for(
                fun() ->
                    dev_scheduler_cache:read_upload_mark(ProcID, BaseOpts) == {ok, 2}
                        andalso length(hb_mock_server:get_requests(Handle, tx)) == 4
                end,
                5000
            )
        ),
        % Slots are republished in order, and an assignment re-read from the
        % store is published byte-for-byte as it was from memory.
        [#{ <<"body">> := First }, _, _, #{ <<"body">> := Again }] =
            hb_mock_server:get_requests(Handle, tx),
        ?assertEqual(First, Again)
    after
        stop(Server2),
        hb_mock_server:stop(Handle)
    end.

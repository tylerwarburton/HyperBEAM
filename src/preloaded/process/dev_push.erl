%%% @doc `push@1.0' takes a message or slot number, evaluates it, and recursively
%%% pushes the resulting messages to other processes. The `push'ing mechanism
%%% continues until the there are no remaining messages to push.
-module(dev_push).
-device_libraries([lib_process]).
%%% Public API
-export([push/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc The keys that an outbox carries as its own metadata rather than as
%% entries to push downstream. `hb_message:normalize_commitments/3' recurses
%% into every submessage, so the outbox map is given its own `commitments' key,
%% and the AO-Core and structured-field keys are legal on any message. Iterating
%% them as if they were entries pushes messages the process never emitted. This
%% is the same exclusion that every other walk of a message-as-collection
%% applies: see `hb_util:message_to_ordered_key/1' and `dev_trie''s
%% `RESERVED_KEYS'.
-define(PUSH_WORKERS, dev_push_detached_workers).
-define(DEFAULT_MAX_PUSH_WORKERS, 32).
-define(DEFAULT_DETACHED_MAX_DEPTH, 16).
%% The node-wide delivery queue: hops past the detached depth bound and
%% entries whose target could not be read yet are driven from here, so a
%% bound on memory never becomes a bound on delivery.
-define(CRANKER, dev_push_cranker).
-define(CRANKER_STATE, dev_push_cranker_state).
-define(DEFAULT_CRANKER_WORKERS, 4).
-define(DEFAULT_CRANKER_MAX_QUEUE, 10000).
-define(DEFAULT_RETRY_MAX_ATTEMPTS, 8).
-define(DEFAULT_DURABLE_HORIZON_HOURS, 24).
-define(DEFAULT_TARGET_MISS_MAX, 100000).
-define(OUTBOX_NON_ENTRY_KEYS,
    ?AO_CORE_KEYS ++ [<<"commitments">>, <<"ao-types">>, <<"device">>]
).

%% @doc Push either a message or an assigned slot number. If a `Process' is
%% provided in the `body' of the request, it will be scheduled (initializing
%% it if it does not exist). Otherwise, the message specified by the given
%% `slot' key will be pushed.
%%
%% Optional parameters:
%% `/result-depth':  The depth to which the full contents of the result
%%                    will be included in the response. Default: 1, returning
%%                    the full result of the first message, but only the 'tree'
%%                    of downstream messages.
%% `/async':         Boolean. When `true', the push runs in a spawned
%%                    process and the call returns immediately. When
%%                    `false' or absent, the push runs synchronously.
%% `/max-depth':     Bounds the recursive `/push' fan-out from one source
%%                    slot's outbox. The source slot's outbox is ALWAYS
%%                    scheduled on each target (the target's next slot
%%                    advances) -- `max-depth' only controls whether we
%%                    then drive that next slot's `/push' synchronously
%%                    to fan further downstream:
%%                       omitted - unbounded recursion.
%%                       `0'     - schedule on each target then stop.
%%                                 The target's compute is NOT invoked
%%                                 here; its own scheduler-driven `/push'
%%                                 (cron tick or explicit caller) picks
%%                                 up the new slot. The response carries
%%                                 a `resulted-in: <<"skipped">>' marker.
%%                       `N > 0' - recurse, with the inner `/push'
%%                                 inheriting `max-depth = N - 1'.
%%                                 Unwinds at most `N' levels deep.
push(Base, Req, Opts) ->
    Process = lib_process:as_process(Base, Opts),
    ?event(push, {push_base, {base, Process}, {req, Req}}, Opts),
    case hb_ao:get(<<"slot">>, {as, <<"message@1.0">>, Req}, no_slot, Opts) of
        no_slot ->
            case schedule_initial_message(Process, Req, Opts) of
                {ok, Assignment} ->
                    case find_type(hb_ao:get(<<"body">>, Assignment, Opts), Opts) of
                        <<"Process">> ->
                            ?event(push,
                                {initializing_process,
                                    {base, Process},
                                    {assignment, Assignment}},
                                Opts
                            ),
                            {ok, Assignment};
                        _ ->
                            ?event(push,
                                {pushing_message,
                                    {base, Process},
                                    {assignment, Assignment}
                                },
                                Opts
                            ),
                            push_with_mode(Process, Assignment, Opts)
                    end;
                {error, Res} -> {error, Res}
            end;
        _ -> push_with_mode(Process, Req, Opts)
    end.

%% @doc Select between a fire-and-forget push and one whose result is
%% returned to the caller. Both run the delivery outside the calling
%% process.
push_with_mode(Process, Req, Opts) ->
    case is_async(Process, Req, Opts) of
        true -> spawn(fun() -> do_push(Process, Req, Opts) end);
        false -> detached_push(Process, Req, Opts)
    end.

%% @doc Run `do_push' in a process that is monitored but not linked, then
%% block on its result. The caller of a `/push' served over HTTP is the
%% Cowboy request process, which is killed the moment the client
%% disconnects; the hop-by-hop delivery must not live there, or every
%% outbox entry the recursion has not reached yet is silently dropped.
%% The caller observes the same result a direct call yields, and an exception
%% is re-raised here with the worker's stacktrace -- the frames above `do_push'
%% are the worker's, not the caller's.
%% The first clause is the in-line case: a recursive `/push' raised by
%% `push_downstream_local' is already running inside a worker, so it must not
%% start another one.
detached_push(Process, Req, Opts) ->
    case hb_opts:get(push_in_worker, false, Opts) of
        true -> do_push(Process, Req, Opts);
        false ->
            case admit_push_worker(Opts) of
                false ->
                    % At the bound, deliver in-line. That is the behaviour of a
                    % node without detachment -- the push dies with its client
                    % -- so refusing to detach is never worse than not having
                    % this function at all.
                    ?event(push, {push_worker_refused, {process, Process}}),
                    do_push(Process, Req, Opts);
                true ->
                    Caller = self(),
                    ServerID = erlang:get(server_id),
                    WorkerOpts = detached_opts(Req, Opts),
                    {Worker, Monitor} =
                        spawn_monitor(
                            fun() ->
                                hb_http_server:set_proc_server_id(ServerID),
                                Caller !
                                    {push_result,
                                        self(),
                                        run_push(Process, Req, WorkerOpts)
                                    }
                            end
                        ),
                    ets:insert(?PUSH_WORKERS, {Worker}),
                    await_detached_push(Worker, Monitor)
            end
    end.

%% @doc The options a detached worker runs under. Detaching removes the only
%% cancellation the push path has: an in-line push stops when its client goes
%% away, and a detached one has nothing that can stop it. A cyclic push graph --
%% A pushes to B, B pushes back to A -- therefore recurses until the node runs
%% out of memory. Give the worker a depth bound when the caller did not set one.
%% The bound is applied here rather than in `do_push/3' because an in-line push
%% is still cancellable and does not need it.
%% The bound is on memory, not on delivery: it is marked `push-depth-implicit',
%% and a hop that reaches it is handed to the cranker rather than skipped (see
%% `push_downstream/4'). A `max-depth' the caller or the operator chose is an
%% instruction and keeps its documented skip semantics.
detached_opts(Req, Opts) ->
    Base = Opts#{ <<"push-in-worker">> => true },
    CallerDepth =
        case is_map(Req) of
            true -> hb_maps:get(<<"max-depth">>, Req, undefined, Opts);
            false -> undefined
        end,
    case {CallerDepth, hb_opts:get(push_max_depth, undefined, Opts)} of
        {undefined, undefined} ->
            Base#{
                <<"push-max-depth">> =>
                    hb_opts:get(
                        push_detached_max_depth,
                        ?DEFAULT_DETACHED_MAX_DEPTH,
                        Opts
                    ),
                <<"push-depth-implicit">> => true
            };
        _Set -> Base
    end.

%% @doc Admit a detached push while the node is below its worker bound. A row
%% is left behind by a worker that has exited, so the table is pruned before it
%% is counted and the bound tracks live deliveries rather than history. Two
%% callers can admit concurrently and overshoot by the number of callers racing;
%% that is bounded and harmless, where an unbounded count is not.
admit_push_worker(Opts) ->
    Max =
        hb_util:int(
            hb_opts:get(push_max_workers, ?DEFAULT_MAX_PUSH_WORKERS, Opts)
        ),
    ensure_push_worker_table(),
    try
        Dead =
            [ W || {W} <- ets:tab2list(?PUSH_WORKERS), not is_process_alive(W) ],
        lists:foreach(fun(W) -> ets:delete(?PUSH_WORKERS, W) end, Dead),
        Live = ets:info(?PUSH_WORKERS, size),
        ?event(push, {push_workers, {live, Live}, {max, Max}}),
        Live < Max
    catch error:badarg -> false
    end.

ensure_push_worker_table() -> ensure_table(?PUSH_WORKERS).

%% @doc Execute a push, tagging the outcome so that a detached worker can
%% hand back either a result or the exception it hit.
run_push(Process, Req, Opts) ->
    try {ok, do_push(Process, Req, Opts)}
    catch Class:Reason:Stacktrace -> {raise, Class, Reason, Stacktrace}
    end.

%% @doc Wait for the outcome of a detached push. A worker that exits before
%% reporting has taken an exit signal of its own, so we surface it as a
%% push failure rather than blocking forever.
await_detached_push(Worker, Monitor) ->
    receive
        {push_result, Worker, {ok, Res}} ->
            erlang:demonitor(Monitor, [flush]),
            Res;
        {push_result, Worker, {raise, Class, Reason, Stacktrace}} ->
            erlang:demonitor(Monitor, [flush]),
            erlang:raise(Class, Reason, Stacktrace);
        {'DOWN', Monitor, process, Worker, Reason} ->
            ?event(push, {push_worker_died, {reason, Reason}}),
            {error,
                #{
                    <<"body">> => <<"The push worker exited before finishing.">>,
                    <<"reason">> => hb_util:bin(hb_format:term(Reason))
                }
            }
    end.

%% @doc Determine if the push is asynchronous. The boolean `async' key
%% on either the request or the process selects between sync (default,
%% `false') and async (`true').
is_async(Process, Req, Opts) ->
    hb_util:bin(
        hb_maps:get_first(
            [{Req, <<"async">>}, {Process, <<"async">>}],
            false,
            Opts
        )
    ) =:= <<"true">>.

%% @doc Push a message or slot number, including its downstream results.
do_push(PrimaryProcess, Assignment, Opts) ->
    Slot = hb_ao:get(<<"slot">>, Assignment, Opts),
    ID = lib_process:process_id(PrimaryProcess, #{}, Opts),
    UncommittedID =
        lib_process:process_id(
            PrimaryProcess,
            #{ <<"commitments">> => <<"none">> },
            Opts
        ),
    BaseID = calculate_base_id(PrimaryProcess, Opts),
    ?event(debug,
        {push_computing_outbox,
            {process_id, ID},
            {base_id, BaseID},
            {process_uncommitted_id, UncommittedID},
            {slot, Slot}
        }
    ),
    ?event(push, {push_computing_outbox, {process_id, ID}, {slot, Slot}}),
    {Status, Result} =
        try
            hb_ao:resolve(
                {as, <<"process@1.0">>, PrimaryProcess},
                    #{ <<"path">> => <<"compute/results">>, <<"slot">> => Slot },
                    compute_opts(Opts)
                )
        catch
            Class:Reason:Trace ->
                ?event(
                    push,
                    {push_compute_failed,
                        {process, PrimaryProcess},
                        {slot, Slot},
                        {class, Class},
                        {reason, Reason},
                        {stack, {trace, Trace}}
                    },
                    Opts
                ),
                {error,
                    #{
                        <<"body">> =>
                                <<
                                    "Pushing slot ",
                                    (hb_util:bin(Slot))/binary,
                                    " failed on process `",
                                    (hb_util:bin(ID))/binary,
                                    "` with error: ",
                                    (hb_util:bin(hb_format:term(Reason, Opts, 0)))
                                        /binary
                                >>,
                        <<"class">> => Class,
                        <<"reason">> => Reason
                    }
                }
        end,
    % Determine if we should include the full compute result in our response.
    IncludeDepth = hb_ao:get(<<"result-depth">>, Assignment, 1, Opts),
    AdditionalRes =
        case IncludeDepth of
            X when X > 0 -> Result;
            _ -> #{}
        end,
    % Read the optional `max-depth' bound on recursive fan-out. `undefined'
    % means unbounded; a non-negative integer decrements at each downstream
    % `/push' and skips the recursion (target still scheduled) when it
    % reaches `0'.
    MaxDepth =
        parse_max_depth(
            hb_maps:get(
                <<"max-depth">>,
                Assignment,
                hb_opts:get(push_max_depth, undefined, Opts),
                Opts
            )
        ),
    ?event(push_depth, {depth, IncludeDepth, {assignment, Assignment}}),
    ?event(push,
        {push_compute_result,
            {process, ID},
            {slot, Slot},
            {status, Status}
        }
    ),
    ?event(debug,
        {push_computed,
            {status, Status},
            {assignment, Assignment},
            {request, hb_maps:get(<<"body">>, Assignment, Assignment, Opts)},
            {result,
                if is_list(Result) ->
                    hb_ao:normalize_keys(Result);
                true -> Result
                end
            }
        }),
    case {Status, hb_ao:get(<<"outbox">>, Result, #{}, Opts)} of
        {ok, NoResults} when ?IS_EMPTY_MESSAGE(NoResults) ->
            ?event(push_short, {done, {process, {string, ID}}, {slot, Slot}}),
            mark_slot_done(ID, Slot, Opts),
            {ok, AdditionalRes#{ <<"slot">> => Slot, <<"process">> => ID }};
        {ok, Outbox} ->
            ?event(push, {push_found_outbox, {outbox, Outbox}}),
            Entries = outbox_entries(Outbox, Opts),
            record_slot_entries(ID, Slot, hb_maps:keys(Entries, Opts), Opts),
            Origin =
                #{
                    <<"process">> => ID,
                    <<"slot">> => Slot,
                    <<"result-depth">> => IncludeDepth,
                    <<"max-depth">> => MaxDepth,
                    <<"from-base">> => BaseID,
                    <<"from-uncommitted">> => UncommittedID,
                    <<"from-scheduler">> =>
                        hb_ao:get(<<"scheduler">>, PrimaryProcess, Opts),
                    <<"from-authority">> =>
                        hb_ao:get(<<"authority">>, PrimaryProcess, Opts)
                },
            Downstream =
                hb_maps:map(
                    fun(Key, Msg) ->
                        push_entry(
                            Key,
                            Msg,
                            Origin#{ <<"outbox-key">> => Key },
                            Opts
                        )
                    end,
                    Entries,
                    Opts
                ),
            maybe_mark_slot_done(ID, Slot, Downstream, Opts),
            {ok, maps:merge(Downstream, AdditionalRes#{
                <<"slot">> => Slot,
                <<"process">> => ID
            })};
        {Err, Error} when Err == error; Err == failure ->
            ?event(push, {push_failed_to_find_outbox, {error, Error}}, Opts),
            {error, Error}
    end.

%% @doc Push one outbox entry: find its target, schedule it there, and follow
%% the target's new slot downstream. Under `push-durable' the entry is first
%% looked up in its delivery record, so that a retried or resumed push of the
%% same slot neither schedules it twice nor races a delivery still in flight.
push_entry(Key, RawMsgToPush = #{ <<"target">> := Target }, Origin, Opts) ->
    case claim_entry(Origin, Opts) of
        untracked -> deliver_entry(Key, RawMsgToPush, Target, Origin, Opts);
        {recorded, {scheduled, TargetID, TargetSlot, PushedMsgID}} ->
            ?event(push_short,
                {push_already_delivered,
                    {target, TargetID},
                    {slot, TargetSlot}
                }
            ),
            #{
                <<"id">> => PushedMsgID,
                <<"target">> => TargetID,
                <<"slot">> => TargetSlot,
                <<"recorded">> => true,
                <<"resulted-in">> =>
                    case continue_recorded(TargetID, TargetSlot, Origin, Opts) of
                        {ok, Downstream} -> Downstream;
                        {error, Error} -> #{ <<"response">> => <<"error">>,
                                             <<"reason">> => Error }
                    end
            };
        {recorded, rejected} ->
            (target_process_not_found(Target))#{ <<"recorded">> => true };
        in_flight ->
            #{
                <<"status">> => 202,
                <<"target">> => Target,
                <<"in-flight">> => true,
                <<"outbox-index">> => Key
            };
        {claimed, Claim} ->
            try deliver_entry(Key, RawMsgToPush, Target, Origin, Opts)
            after release_claim(Claim)
            end
    end;
push_entry(Key, Msg, _Origin, _Opts) ->
    #{
        <<"response">> => <<"error">>,
        <<"status">> => 404,
        <<"outbox-index">> => Key,
        <<"reason">> => <<"Target process not available.">>,
        <<"message">> => Msg
    }.

%% @doc Deliver an entry whose delivery has not been recorded. A target that
%% cannot be read right now is not the same as a target that does not exist:
%% only a fresh, definitive miss rejects the entry. A remembered miss or a
%% failed read defers it to the cranker, which retries it once the miss has
%% expired.
deliver_entry(Key, RawMsgToPush, Target, Origin, Opts) ->
    ID = maps:get(<<"process">>, Origin),
    MsgToPush =
        case maybe_evaluate_message(RawMsgToPush, Opts) of
            {ok, R} -> R;
            Err ->
                #{
                    <<"resolve">> => <<"error">>,
                    <<"target">> => ID,
                    <<"status">> => 400,
                    <<"outbox-index">> => Key,
                    <<"reason">> => Err,
                    <<"source">> => RawMsgToPush
                }
        end,
    case read_target(Target, Opts) of
        {ok, DownstreamProcess} ->
            push_result_message(DownstreamProcess, MsgToPush, Origin, Opts);
        {error, not_found} ->
            record_entry(Origin, rejected, Opts),
            target_process_not_found(Target);
        Unavailable ->
            defer_entry(Target, MsgToPush, Origin, Unavailable, 1, Opts)
    end.

%% @doc Continue a delivery that its record says was already scheduled. The
%% target slot's own `done' marker ends the walk: everything below it has been
%% pushed, so re-walking it would only recompute what is already delivered.
continue_recorded(TargetID, TargetSlot, Origin, Opts) ->
    case is_slot_done(TargetID, TargetSlot, Opts) of
        true -> {ok, <<"already-pushed">>};
        false -> push_downstream(TargetID, TargetSlot, Origin, Opts)
    end.

%% @doc Return the outbox entries that should be pushed downstream, discarding
%% the result metadata that shares the map with them. The outbox arrives as part
%% of a computed result, so it carries that result's `commitments' and `status'
%% alongside the messages the process actually emitted.
outbox_entries(Outbox, Opts) ->
    Normalized = hb_ao:normalize_keys(hb_private:reset(Outbox)),
    LowerPayload = not is_true(hb_opts:get(push_preserve_key_case, false, Opts)),
    Entries =
        hb_maps:fold(
            fun(Key, Msg, Acc) ->
                maps:put(
                    hb_util:to_lower(Key),
                    case LowerPayload of
                        true -> lower_case_entry(Msg);
                        false -> normalize_entry_target(Msg, Opts)
                    end,
                    Acc
                )
            end,
            #{},
            Normalized,
            Opts
        ),
    hb_maps:without(?OUTBOX_NON_ENTRY_KEYS, Entries, Opts).

%% @doc Lower-case the keys of an outbox entry, as the reference push device
%% does with `hb_util:lower_case_keys/2': a recipient reads `action', not
%% `Action', whichever node delivered it. Two things are left as they were
%% emitted, because their case is part of what they say: a `commitments' map,
%% whose keys are case-sensitive base64url commitment IDs, and a signed
%% sub-message (one that carries `commitments'), whose keys are covered by the
%% signature. Fold with `hb_util_string:lowercase/1' rather than
%% `hb_util:to_lower/1': a process names its own fields, `to_lower' throws on a
%% name that is not valid UTF-8, and one such name would abort delivery of the
%% whole outbox.
lower_case_entry(Msg) when is_map(Msg) ->
    maps:fold(
        fun(K, V, Acc) ->
            LowerK =
                case is_binary(K) of
                    true -> hb_util_string:lowercase(K);
                    false -> K
                end,
            maps:put(LowerK, lower_case_value(LowerK, V), Acc)
        end,
        #{},
        Msg
    );
lower_case_entry(Msg) -> Msg.

lower_case_value(<<"commitments">>, V) -> V;
lower_case_value(_, V = #{ <<"commitments">> := _ }) -> V;
lower_case_value(_, V) when is_map(V) -> lower_case_entry(V);
lower_case_value(_, V) -> V.

%% @doc Give an entry the lower-case `target' that the push path dispatches and
%% reads on, when the node is configured to preserve the case of payload keys
%% (`push-preserve-key-case'). A legacy AO process names the key `Target', and
%% `hb_ao:get/4' lowers the key it is asked for but not the keys it searches,
%% so such an entry matches no clause of the push walk and is answered `Target
%% process not available.' without ever being delivered. Rename only that key.
normalize_entry_target(Msg, Opts) when is_map(Msg) ->
    maybe
        false ?= hb_maps:is_key(<<"target">>, Msg, Opts),
        [Key] ?=
            [
                K
            ||
                K <- hb_maps:keys(Msg, Opts),
                is_binary(K),
                hb_util_string:lowercase(K) == <<"target">>
            ],
        maps:put(<<"target">>, maps:get(Key, Msg), maps:remove(Key, Msg))
    else
        _ -> Msg
    end;
normalize_entry_target(Msg, _Opts) -> Msg.

%% @doc Find the process an outbox entry targets. Most targets of a token,
%% vault or pair are wallets (Credit-Notice, Debit-Notice), which are not
%% messages at all: a full store read misses locally and then walks every
%% remote store (three GraphQL gateways here, 0.7-3.6 s) before failing, once
%% per entry, on every push. Local stores are read first -- every process this
%% node runs is there. A node may also remember remote misses for a bounded time
%% (`push-target-miss-ttl' seconds; default 0, off, as in the reference device),
%% so a wallet costs one remote walk per TTL rather than one per message.
%%
%% The gateway store reports a failed request as `not_found', so a remembered
%% miss cannot be told apart from a gateway outage. It is therefore never a
%% reason to reject an entry: it is returned as `{cached_miss, At}' and the
%% entry is deferred until a fresh read decides it. A failed read is
%% `{transient, Reason}' and is never remembered. Returns `{ok, Msg}',
%% `{error, not_found}' (a fresh, definitive miss), `{cached_miss, At}' or
%% `{transient, Reason}'.
read_target(Target, Opts) ->
    case hb_cache:read(Target, hb_store:scope(Opts, local)) of
        {ok, Msg} -> {ok, Msg};
        _ ->
            case recent_miss(Target, Opts) of
                {true, At} -> {cached_miss, At};
                false -> read_target_fresh(Target, Opts)
            end
    end.

%% @doc Read a target through every store, ignoring remembered misses.
read_target_fresh(Target, Opts) ->
    try hb_cache:read(Target, Opts) of
        {ok, Msg} -> {ok, Msg};
        {error, not_found} ->
            remember_miss(Target, Opts),
            {error, not_found};
        not_found ->
            remember_miss(Target, Opts),
            {error, not_found};
        Other -> {transient, Other}
    catch Class:Reason -> {transient, {Class, Reason}}
    end.

-define(TARGET_MISSES, dev_push_target_misses).

miss_ttl_ms(Opts) ->
    hb_util:int(hb_opts:get(<<"push-target-miss-ttl">>, 0, Opts)) * 1000.

recent_miss(Target, Opts) ->
    case miss_ttl_ms(Opts) of
        TTL when TTL =< 0 -> false;
        TTL ->
            case miss_time(Target) of
                {ok, At} ->
                    case now_ms() - At < TTL of
                        true -> {true, At};
                        false -> false
                    end;
                none -> false
            end
    end.

%% @doc When `Target' last missed on a fresh read, in monotonic milliseconds.
miss_time(Target) ->
    ensure_table(?TARGET_MISSES),
    try ets:lookup(?TARGET_MISSES, Target) of
        [{Target, At}] -> {ok, At};
        [] -> none
    catch error:badarg -> none
    end.

%% @doc Remember a fresh miss, and keep the table bounded: entries older than
%% the TTL are swept at most once per TTL, and a table still over
%% `push-target-miss-max' after a sweep is cleared. Clearing only costs the
%% wallets a remote walk each; it never loses a message.
remember_miss(Target, Opts) ->
    case miss_ttl_ms(Opts) of
        TTL when TTL =< 0 -> ok;
        TTL ->
            ensure_table(?TARGET_MISSES),
            Now = now_ms(),
            try
                ets:insert(?TARGET_MISSES, {Target, Now}),
                prune_misses(Now, TTL, Opts)
            catch error:badarg -> ok
            end,
            ok
    end.

prune_misses(Now, TTL, Opts) ->
    Max =
        hb_util:int(
            hb_opts:get(<<"push-target-miss-max">>, ?DEFAULT_TARGET_MISS_MAX, Opts)
        ),
    LastSweep =
        case ets:lookup(?TARGET_MISSES, '$sweep') of
            [{_, L}] -> L;
            [] -> undefined
        end,
    Size = ets:info(?TARGET_MISSES, size),
    Due = LastSweep == undefined orelse Now - LastSweep >= TTL,
    case Due orelse Size > Max of
        false -> ok;
        true ->
            ets:insert(?TARGET_MISSES, {'$sweep', Now}),
            ets:select_delete(
                ?TARGET_MISSES,
                [{{'$1', '$2'}, [{is_binary, '$1'}, {'<', '$2', Now - TTL}], [true]}]
            ),
            case ets:info(?TARGET_MISSES, size) > Max of
                true -> ets:delete_all_objects(?TARGET_MISSES);
                false -> ok
            end
    end.

now_ms() -> erlang:monotonic_time(millisecond).

%% @doc A named public table owned by a process that never exits: a table dies
%% with its owner, and the first caller is usually a short-lived request.
ensure_table(Name) ->
    case ets:whereis(Name) of
        undefined ->
            Parent = self(),
            Ref = make_ref(),
            {Owner, Mon} =
                spawn_monitor(
                    fun() ->
                        try ets:new(Name, [named_table, public, set]) of
                            _ ->
                                Parent ! {Ref, created},
                                receive after infinity -> ok end
                        catch error:badarg -> Parent ! {Ref, exists}
                        end
                    end
                ),
            receive
                {Ref, _} -> ok;
                {'DOWN', Mon, process, Owner, _} -> ok
            after 5000 -> ok
            end,
            erlang:demonitor(Mon, [flush]),
            ok;
        _ -> ok
    end.

target_process_not_found(Target) ->
    #{
        <<"response">> => <<"error">>,
        <<"status">> => 404,
        <<"target">> => Target,
        <<"reason">> => <<"Could not access target process!">>
    }.


%% @doc If the outbox message has a path we interpret it as a request to perform
%% AO-Core eval and schedule the result. Additionally, we  remove the `target` 
%% from the base message before execution and re-add it to the result, such that
%% the target to schedule the execution result upon is not confused with
%% functional components of the evaluation.
maybe_evaluate_message(Message, Opts) ->
    case hb_ao:get(<<"resolve">>, Message, Opts) of
        not_found -> 
            {ok, Message};
        ResolvePath ->
            ReqMsg =
                maps:without(
                    [<<"target">>],
                    Message
                ),
            ResolveOpts = Opts#{ <<"force-message">> => true },
            case hb_ao:resolve(ReqMsg#{ <<"path">> => ResolvePath }, ResolveOpts) of
                {ok, EvalRes} ->
                    {
                        ok,
                        EvalRes#{
                            <<"target">> =>
                                hb_ao:get(
                                    <<"target">>,
                                    Message,
                                    Opts
                                )
                        }
                    };
                Err -> Err
            end
    end.

%% @doc Push a downstream message result. The `Origin' map contains information
%% about the origin of the message: The process that originated the message,
%% the slot number from which it was sent, and the outbox key of the message,
%% and the depth to which downstream results should be included in the message.
push_result_message(TargetProcess, MsgToPush, Origin, Opts) ->
    NormMsgToPush = hb_ao:normalize_keys(MsgToPush, Opts),
    case hb_ao:get(<<"target">>, NormMsgToPush, undefined, Opts) of
        undefined ->
            ?event(push,
                {skip_no_target, {msg, MsgToPush}, {origin, Origin}},
                Opts
            ),
            #{};
        TargetID ->
            ?event(push,
                {pushing_child,
                    {target, TargetID},
                    {msg, MsgToPush},
                    {origin, Origin}
                },
                Opts
            ),
            case schedule_result(TargetProcess, MsgToPush, Origin, Opts) of
                {ok, Assignment} ->
                    % Analyze the result of the message push.
                    NextSlotOnProc = hb_ao:get(<<"slot">>, Assignment, Opts),
                    PushedMsg = hb_ao:get(<<"body">>, Assignment, Opts),
                    % Get the ID of the message that was pushed. We already have
                    % the 'origin' message, but we need the signed ID.
                    PushedMsgID = hb_message:id(PushedMsg, all, Opts),
                    % Record the delivery as soon as the target holds it, and
                    % journal the target's new slot so that its own push is
                    % resumed if the node stops before it runs.
                    record_entry(
                        Origin,
                        {scheduled, TargetID, NextSlotOnProc, PushedMsgID},
                        Opts
                    ),
                    journal_slot(TargetID, NextSlotOnProc, Opts),
                    ?event(push_short,
                        {pushed_message_to,
                            {process, TargetID},
                            {slot, NextSlotOnProc}
                        }
                    ),
                    case push_downstream(TargetID, NextSlotOnProc, Origin, Opts) of
                        {ok, Downstream} ->
                            #{
                                <<"id">> => PushedMsgID,
                                <<"target">> => TargetID,
                                <<"slot">> => NextSlotOnProc,
                                <<"resulted-in">> => Downstream
                            };
                        {error, Error} ->
                            ?event(push, {push_failed, {error, Error}}, Opts),
                            #{
                                <<"response">> => <<"error">>,
                                <<"target">> => TargetID,
                                <<"reason">> => Error
                            }
                    end;
                {error, Error} ->
                    ?event(push, {push_failed, {error, Error}}, Opts),
                    #{
                        <<"response">> => <<"error">>,
                        <<"target">> => TargetID,
                        <<"reason">> => Error
                    }
            end
    end.

%% @doc Push a downstream resultant message that has already been scheduled.
%% We determine whether to push the message locally or remotely based on the
%% `push_route_downstream' option. When the inherited `max-depth' has reached
%% `0' we skip the recursive `/push' entirely (returning the binary marker
%% `<<"skipped">>'): the message is already in the target's schedule queue
%% from the `schedule_result' call above, so the target's own `/push'
%% invocation will pick it up on its next cron tick or explicit caller.
%% A depth of `0' that is the detached worker's own memory bound rather than
%% the caller's choice (`push-depth-implicit') is not a reason to stop: the
%% hop is handed to the cranker, which continues it under a fresh bound, and
%% the response carries `<<"deferred">>'.
push_downstream(TargetID, NextSlotOnProc, Origin, Opts) ->
    Depth = parse_max_depth(hb_maps:get(<<"max-depth">>, Origin, undefined, Opts)),
    case {Depth, map_get_true(<<"push-depth-implicit">>, Opts)} of
        {0, true} ->
            ?event(push_short,
                {push_depth_bound_deferred,
                    {target, TargetID},
                    {slot, NextSlotOnProc}
                }
            ),
            case enqueue({continue, TargetID, NextSlotOnProc}, 0, Opts) of
                ok -> {ok, <<"deferred">>};
                full -> {ok, <<"skipped">>}
            end;
        {0, false} ->
            ?event(push_short,
                {push_max_depth_reached,
                    {target, TargetID},
                    {slot, NextSlotOnProc}
                }
            ),
            {ok, <<"skipped">>};
        _ ->
            case hb_opts:get(push_route_downstream, true, Opts) of
                true -> push_downstream_remote(TargetID, NextSlotOnProc, Origin, Opts);
                false -> push_downstream_local(TargetID, NextSlotOnProc, Origin, Opts)
            end
    end.

%% @doc Push a downstream message on a remote node if a route can be found to
%% perform the action. If no route is found, we execute the action locally.
push_downstream_remote(TargetID, NextSlotOnProc, Origin, RawOpts) ->
    Path =
        <<
            "/",
            TargetID/binary,
            "/push&slot=",
            (hb_util:bin(NextSlotOnProc))/binary
        >>,
    RouteReq =
        #{
            <<"path">> => <<"route">>,
            <<"route-path">> => Path
        },
    Opts =
        case hb_ao:resolve(
            #{ <<"device">> => <<"whois@1.0">> },
            #{ <<"path">> => <<"node">> },
            RawOpts
        ) of
            {ok, Host} -> RawOpts#{ <<"node-host">> => Host };
            _ -> RawOpts
        end,
    Self = hb_opts:get(node_host, host_not_specified, Opts),
    ?event(remote_push,
        {push_downstream_remote,
            {target, TargetID},
            {slot, NextSlotOnProc},
            {origin, Origin},
            {opts, Opts}
        }
    ),
    case hb_ao:resolve(#{ <<"device">> => <<"router@1.0">> }, RouteReq, Opts) of
        {error, no_matches} ->
            ?event(push,
                {no_push_route_found,
                    {target, TargetID},
                    {slot, NextSlotOnProc},
                    {continuing, locally}
                },
                Opts
            ),
            push_downstream_local(TargetID, NextSlotOnProc, Origin, Opts);
        {ok, Self} ->
            % If we matched ourselves as the route, we can just push locally.
            ?event(push,
                {routing_matched_self,
                    {target, TargetID},
                    {slot, NextSlotOnProc},
                    {continuing, locally}
                },
                Opts
            ),
            push_downstream_local(TargetID, NextSlotOnProc, Origin, Opts);
        {ok, Node} ->
            ?event(push,
                {routing_matched_remote,
                    {target, TargetID},
                    {slot, NextSlotOnProc},
                    {node, Node}
                },
                Opts
            ),
            hb_http:post(Node, Path, Opts)
    end.

%% @doc Push a resulting message recursively, executing the action on this node.
%% We decrement `result-depth' (and, when set, `max-depth') so the recursion
%% naturally winds down.
push_downstream_local(TargetID, NextSlotOnProc, Origin, Opts) ->
    ?event(push,
        {push_downstream_local,
            {target, TargetID},
            {slot, NextSlotOnProc},
            {origin, Origin}
        }
    ),
    BaseReq =
        #{
            <<"path">> => <<"push">>,
            <<"slot">> => NextSlotOnProc,
            <<"result-depth">> =>
                hb_maps:get(<<"result-depth">>, Origin, 1, Opts) - 1
        },
    Req =
        case parse_max_depth(hb_maps:get(<<"max-depth">>, Origin, undefined, Opts)) of
            undefined -> BaseReq;
            N when is_integer(N), N > 0 ->
                BaseReq#{ <<"max-depth">> => N - 1 }
        end,
    hb_ao:resolve(
        {as, <<"process@1.0">>, TargetID},
        Req,
        Opts#{ <<"cache-control">> => <<"always">> }
    ).

%% @doc Options for computing the pushed slot. A push computes its process
%% from inside another resolution, and `hb_ao' strips `spawn-worker' from
%% nested resolutions, so the computed state was never handed to a persistent
%% worker: it was discarded, and the next push restored the process from its
%% last VM snapshot and replayed every slot since. When the node runs process
%% workers, let this compute leave one behind. Inside a worker
%% `process-workers' is false, so a worker never spawns another.
compute_opts(Opts) ->
    Base = Opts#{ <<"hashpath">> => ignore },
    case hb_opts:get(<<"process-workers">>, false, Opts) of
        true -> Base#{ <<"spawn-worker">> => true };
        _ -> Base
    end.

%% @doc Normalise the `max-depth' value supplied by the caller. Accepts a
%% non-negative integer (verbatim or as a binary), returns `undefined' when
%% the value is absent or unparseable.
parse_max_depth(undefined) -> undefined;
parse_max_depth(N) when is_integer(N), N >= 0 -> N;
parse_max_depth(Bin) when is_binary(Bin) ->
    try hb_util:int(Bin) of
        N when is_integer(N), N >= 0 -> N;
        _ -> undefined
    catch
        _:_ -> undefined
    end;
parse_max_depth(_) -> undefined.

%% @doc Augment the message with from-* keys, if it doesn't already have them.
normalize_message(MsgToPush, Opts) ->
    hb_ao:set(
        MsgToPush,
        #{
            <<"target">> => target_process(MsgToPush, Opts)
        },
        Opts#{ <<"hashpath">> => ignore }
    ).

%% @doc Find the target process ID for a message to push.
target_process(MsgToPush, Opts) ->
    case hb_ao:get(<<"target">>, MsgToPush, Opts) of
        not_found -> undefined;
        RawTarget -> extract(target, RawTarget)
    end.

%% @doc Return either the `target' or the `hint'.
extract(hint, Raw) ->
    {_, Hint} = split_target(Raw),
    Hint;
extract(target, Raw) ->
    {Target, _} = split_target(Raw),
    Target.

%% @doc Split the target into the process ID and the optional query string.
split_target(RawTarget) ->
    case binary:split(RawTarget, [<<"?">>, <<"&">>]) of
        [Target, QStr] -> {Target, QStr};
        _ -> {RawTarget, <<>>}
    end.

%% @doc Calculate the base ID for a process. The base ID is not just the 
%% uncommitted process ID. It also excludes the `authority' and `scheduler'
%% keys.
calculate_base_id(GivenProcess, Opts) ->
    Process =
        case
            hb_ao:get(
                <<"process">>,
                GivenProcess,
                Opts#{ <<"hashpath">> => ignore }
            )
        of
            not_found -> GivenProcess;
            Proc -> Proc
        end,
    BaseProcess =
        hb_ao:set(
            Process,
            #{ <<"authority">> => unset, <<"scheduler">> => unset },
            Opts#{ <<"hashpath">> => ignore }
        ),
    {ok, BaseID} =
        hb_ao:resolve(
            BaseProcess,
            #{ <<"path">> => <<"id">>, <<"committers">> => <<"none">> },
            Opts
        ),
    ?event(debug_base, {push_generated_base, {id, BaseID}, {base, BaseProcess}}),
    BaseID.

%% @doc Add the necessary keys to the message to be scheduled, then schedule it.
%% If the remote scheduler does not support the given codec, it will be
%% downgraded and re-signed.
schedule_result(TargetProcess, MsgToPush, Origin, Opts) ->
    schedule_result(
        TargetProcess,
        MsgToPush,
        hb_opts:get(
            scheduler_default_commitment_spec,
            <<"httpsig@1.0">>,
            Opts
        ),
        Origin,
        Opts
    ).
schedule_result(TargetProcess, MsgToPush, Codec, Origin, Opts) ->
    Target = hb_ao:get(<<"target">>, MsgToPush, Opts),
    ?event(push,
        {push_scheduling_result,
            {target, {string, Target}},
            {target_process, TargetProcess},
            {msg, MsgToPush},
            {codec, Codec},
            {origin, Origin}
        },
        Opts
    ),
    AugmentedMsg = augment_message(Origin, MsgToPush, Opts),
    ?event(push, {prepared_msg, {msg, AugmentedMsg}}, Opts),
    % Load the `accept-id`'d wallet into the `Opts` map, if requested.
    SignedMsg = apply_security(AugmentedMsg, TargetProcess, Codec, Opts),
    % Verify the signed message before writing to cache
    true = hb_message:verify(SignedMsg, signers, Opts),
    % Write the signed message to cache before including it in the schedule request
    {ok, _} = hb_cache:write(SignedMsg, Opts),
    ScheduleReq = #{
        <<"path">> => <<"schedule">>,
        <<"method">> => <<"POST">>,
        <<"body">> => SignedMsg
    },
    ?event(push, {schedule_req, {req, ScheduleReq}}, Opts),
    ?event(debug,
        {push_scheduling_result,
            {signed_req, SignedMsg}
        }
    ),
    {ErlStatus, Res} =
        case hb_message:signers(SignedMsg, Opts) of
            [] ->
                {error,
                    <<
                        "Application of security policy failed: ",
                        "No identities matching authority were found."
                    >>
                };
            _Committers ->
                hb_ao:resolve(
                    {as, <<"process@1.0">>, TargetProcess},
                    ScheduleReq,
                    Opts#{ <<"cache-control">> => <<"always">> }
                )
        end,
    ?event(push, {push_sched_result, {status, ErlStatus}, {response, Res}}, Opts),
    case {ErlStatus, hb_ao:get(<<"status">>, Res, 200, Opts)} of
        {ok, 200} ->
            {ok, Res};
        {ok, 307} ->
            Location = hb_ao:get(<<"location">>, Res, Opts),
            ?event(push, {redirect, {location, {explicit, Location}}}),
            % Strip the now-resolved hint from the target and re-sign the
            % already-augmented message to the target's policy -- preserving the
            % `from-*' provenance and honoring the policy, rather than
            % re-committing the raw message with the default wallet.
            NormMsg = normalize_message(AugmentedMsg, Opts),
            SignedNormMsg = apply_security(NormMsg, TargetProcess, Codec, Opts),
            remote_schedule_result(Location, SignedNormMsg, Opts);
        {error, 422} ->
            ?event(push, {wrong_format, {422, Res}, {codec, Codec}}, Opts),
            case Codec of
                <<"ans104@1.0">> ->
                    {error, Res};
                <<"httpsig@1.0">> ->
                    ?event(push,
                        {downgrading_to_ans104,
                            {422, Res},
                            {codec, Codec},
                            {origin, Origin}
                        },
                        Opts
                    ),
                    schedule_result(
                        TargetProcess,
                        MsgToPush,
                        <<"ans104@1.0">>,
                        Origin,
                        Opts
                    )
            end;
        {error, _} ->
            {error, Res}
    end.

%% @doc Set the necessary keys in order for the recipient to know where the
%% message came from.
augment_message(Origin, ToSched, Opts) ->
    ?event(push, {adding_keys, {origin, Origin}, {to, ToSched}}, Opts),
    hb_message:uncommitted(
        hb_ao:set(
            ToSched,
            #{
                <<"data-protocol">> => <<"ao">>,
                <<"variant">> => <<"ao.N.1">>,
                <<"type">> => <<"Message">>,
                <<"from-process">> => maps:get(<<"process">>, Origin),
                <<"from-uncommitted">> => maps:get(<<"from-uncommitted">>, Origin),
                <<"from-base">> => maps:get(<<"from-base">>, Origin),
                <<"from-scheduler">> => maps:get(<<"from-scheduler">>, Origin),
                <<"from-authority">> => maps:get(<<"from-authority">>, Origin)
            },
            Opts#{ <<"hashpath">> => ignore }
        )
    ).

%% @doc Apply the recipient's security policy to the message. Observes the 
%% following parameters in order to calculate the appropriate security policy:
%% - `policy': A message that generates a security policy message.
%% - `authority': A single committer, or list of comma separated committers.
%% - (Default: Signs with default wallet)
apply_security(Msg, TargetProcess, Codec, Opts) ->
    % Verify the result before it is signed for POSTing, if paranoid mode
    % enables push_result. `commit_result' signs `uncommitted(Msg)', so we
    % verify exactly that content here, before any signature is applied.
    hb_message:paranoid_verify(push_result, hb_message:uncommitted(Msg), Opts),
    apply_security(policy, Msg, TargetProcess, Codec, Opts).
apply_security(policy, Msg, TargetProcess, Codec, Opts) ->
    case hb_ao:get(<<"policy">>, TargetProcess, not_found, Opts) of
        not_found -> apply_security(authority, Msg, TargetProcess, Codec, Opts);
        Policy ->
            case hb_ao:resolve(Policy, Opts) of
                {ok, PolicyOpts} ->
                    case hb_ao:get(<<"accept-committers">>, PolicyOpts, Opts) of
                        not_found ->
                            apply_security(
                                authority,
                                Msg,
                                TargetProcess,
                                Codec,
                                Opts
                            );
                        Committers ->
                            commit_result(Msg, Committers, Codec, Opts)
                    end;
                {error, Error} ->
                    ?event(push, {policy_error, {error, Error}}, Opts),
                    apply_security(authority, Msg, TargetProcess, Codec, Opts)
            end
    end;
apply_security(authority, Msg, TargetProcess, Codec, Opts) ->
    case hb_ao:get(<<"authority">>, TargetProcess, Opts) of
        not_found -> apply_security(default, Msg, TargetProcess, Codec, Opts);
    	Authorities when is_list(Authorities) ->
            % The `authority` key has already been parsed into a list of
            % committers. Sign with all local valid keys.
            commit_result(Msg, Authorities, Codec, Opts);
        Authority ->
            % Parse the authority string into a list of committers. Sign with
            % all local valid keys.
            ?event(push, {found_authority, {authority, Authority}}, Opts),
            commit_result(
                Msg,
                hb_util:binary_to_strings(Authority),
                Codec,
                Opts
            )
    end;
apply_security(default, Msg, TargetProcess, Codec, Opts) ->
    ?event(push, {default_policy, {target, TargetProcess}}, Opts),
    commit_result(
        Msg,
        [hb_util:human_id(hb_opts:get(priv_wallet, no_viable_wallet, Opts))],
        Codec,
        Opts
    ).

% @doc Attempt to sign a result message with the given committers.
commit_result(Msg, [], Codec, Opts) ->
    case hb_opts:get(push_always_sign, true, Opts) of
        true -> hb_message:commit(hb_message:uncommitted(Msg), Opts, Codec);
        false -> Msg
    end;
commit_result(Msg, Committers, Codec, Opts) ->
    Signed = lists:foldl(
        fun(Committer, Acc) ->
            case hb_opts:as(Committer, Opts) of
                {ok, CommitterOpts} ->
                    ?event(debug_commit, {signing_with_identity, Committer}),
                    hb_message:commit(Acc, CommitterOpts, Codec);
                {error, not_found} ->
                    ?event(debug_commit, desired_signer_not_available_on_node),
                    ?event(push,
                        {policy_warning,
                            {
                                unknown_committer,
                                Committer
                            }
                        },
                        Opts
                    ),
                    Acc
            end
        end,
        hb_message:uncommitted(Msg),
        Committers
    ),
    ?event(debug_commit,
        {signed_message_as, {explicit, hb_message:signers(Signed, Opts)}}
    ),
    case hb_message:signers(Signed, Opts) of
        [] ->
            ?event(debug_commit, signing_with_default_identity),
            commit_result(Msg, [], Codec, Opts);
        _FoundSigners ->
            Signed
    end.

%% @doc Push a message or a process, prior to pushing the resulting slot number.
schedule_initial_message(Base, Req, Opts) ->
    ModReq = Req#{ <<"path">> => <<"schedule">>, <<"method">> => <<"POST">> },
    ?event(push, {initial_push, {base, Base}, {req, ModReq}}, Opts),
    case hb_ao:resolve(Base, ModReq, Opts) of
        {ok, Res} ->
            case hb_ao:get(<<"status">>, Res, 200, Opts) of
                200 -> {ok, Res};
                307 ->
                    Location = hb_ao:get(<<"location">>, Res, Opts),
                    remote_schedule_result(Location, Req, Opts)
            end;
        {error, Res = #{ <<"status">> := 422 }} ->
            ?event(push, {initial_push_wrong_format, {error, Res}}, Opts),
            {error, Res};
        {error, Res} ->
            ?event(push, {initial_push_error, {error, Res}}, Opts),
            {error, Res}
    end.

remote_schedule_result(Location, SignedReq, Opts) ->
    ?event(push, {remote_schedule_result, {location, Location}, {req, SignedReq}}, Opts),
    {Node, RedirectPath} = parse_redirect(Location, Opts),
    Path =
        case find_type(SignedReq, Opts) of
            <<"Process">> -> <<"/schedule">>;
            <<"Message">> -> RedirectPath
        end,
    % Store a copy of the message for ourselves.
    {ok, _} = hb_cache:write(SignedReq, Opts),
    ?event(push, {remote_schedule_result, {path, Path}}, Opts),
    case hb_http:post(Node, Path, hb_maps:without([<<"path">>], SignedReq, Opts), Opts) of
        {ok, Res} ->
            ?event(push, {remote_schedule_result, {res, Res}}, Opts),
            case hb_ao:get(<<"status">>, Res, 200, Opts) of
                200 -> {ok, Res};
                307 ->
                    NewLocation = hb_ao:get(<<"location">>, Res, Opts),
                    remote_schedule_result(NewLocation, SignedReq, Opts)
            end;
        {error, Res} ->
            {error, Res}
    end.

find_type(Req, Opts) ->
    hb_ao:get_first(
        [
            {Req, <<"type">>},
            {Req, <<"body/type">>}
        ],
        Opts
    ).

parse_redirect(Location, Opts) ->
    Parsed = uri_string:parse(Location),
    Node =
        uri_string:recompose(
            (hb_maps:remove(query, Parsed, Opts))#{
                path => <<"/schedule">>
            }
        ),
    {Node, hb_maps:get(path, Parsed, undefined, Opts)}.

%%% Delivery records, deferral and the cranker.
%%%
%%% Under `push-durable' (default `false') every outbox entry a push delivers
%%% is recorded in the node's local store, keyed by the source process, the
%%% source slot and the entry's outbox key -- the entry's stable identity, as
%%% the schedule path has no dedup by message ID and a re-signed entry gets a
%%% new one:
%%%   push-delivery/<process>/<slot>/<key> -> pending | retrying | rejected |
%%%                                           {scheduled, Target, Slot, ID}
%%%   push-slot/<process>/<slot>           -> the slot's outbox keys
%%%   push-done/<process>/<slot>           -> every entry is resolved
%%%   push-journal/<hour>/<process>@<slot> -> a slot whose push must run
%%% A slot is journaled when its outbox is found, and every target slot when
%%% its message is scheduled; on the first durable push after a start the
%%% journal of the last `push-durable-horizon' hours is replayed, and every
%%% slot without a `done' marker is pushed again. A re-push consults the
%%% records, so it schedules only what was not scheduled before. An entry whose
%%% record is `pending' is claimed in memory by the live process delivering it,
%%% so a client that re-POSTs `/push' while the first detached worker runs
%%% does not deliver it twice. The record is written after the target's
%%% scheduler returns, so a node that stops between the two can still deliver
%%% that one entry twice on resume: at-least-once, not exactly-once.
%%%
%%% The cranker is a node-wide queue with a bounded worker pool
%%% (`push-cranker-workers', default 4) and a bounded length
%%% (`push-cranker-max-queue', default 10000). It drives two kinds of work,
%%% with or without `push-durable': hops past the detached depth bound
%%% (`{continue, Process, Slot}') and entries whose target could not be read
%%% (`{retry, ...}').

is_true(true) -> true;
is_true(<<"true">>) -> true;
is_true(_) -> false.

map_get_true(Key, Map) -> is_true(maps:get(Key, Map, false)).

durable(Opts) -> is_true(hb_opts:get(push_durable, false, Opts)).

-define(CLAIMS, dev_push_delivery_claims).

%% @doc Find, or claim, the delivery of one outbox entry. Returns `untracked'
%% when the node keeps no records, `{recorded, State}' for an entry that a
%% previous push already resolved, `in_flight' while a live process holds it,
%% or `{claimed, Claim}' when this process is now the one delivering it.
claim_entry(Origin, Opts) ->
    case durable(Opts) of
        false -> untracked;
        true ->
            Path = entry_path(Origin),
            case resolved_record(Path, Opts) of
                {recorded, _} = Recorded -> Recorded;
                unresolved ->
                    ensure_table(?CLAIMS),
                    case take_claim(Path, self()) of
                        false -> in_flight;
                        true ->
                            % A delivery may have finished between the read
                            % and the claim; it is the claim that orders them.
                            case resolved_record(Path, Opts) of
                                {recorded, _} = Recorded ->
                                    release_claim(Path),
                                    Recorded;
                                unresolved ->
                                    write_record(Path, pending, Opts),
                                    {claimed, Path}
                            end
                    end
            end
    end.

resolved_record(Path, Opts) ->
    case read_record(Path, Opts) of
        {ok, Scheduled = {scheduled, _, _, _}} -> {recorded, Scheduled};
        {ok, rejected} -> {recorded, rejected};
        _ -> unresolved
    end.

take_claim(Path, Self) ->
    case ets:insert_new(?CLAIMS, {Path, Self}) of
        true -> true;
        false ->
            case ets:lookup(?CLAIMS, Path) of
                [{_, Self}] -> true;
                [{_, Owner}] ->
                    case is_process_alive(Owner) of
                        true -> false;
                        false ->
                            ets:delete_object(?CLAIMS, {Path, Owner}),
                            take_claim(Path, Self)
                    end;
                [] -> take_claim(Path, Self)
            end
    end.

release_claim(Path) ->
    try ets:delete_object(?CLAIMS, {Path, self()})
    catch error:badarg -> ok
    end,
    ok.

%% @doc Record the state of an entry's delivery, when the node keeps records.
record_entry(Origin, State, Opts) ->
    case durable(Opts) andalso maps:is_key(<<"outbox-key">>, Origin) of
        true -> write_record(entry_path(Origin), State, Opts);
        false -> ok
    end.

%% @doc Note a slot's outbox keys and journal the slot, so that a push cut
%% short before every entry is resolved is resumed.
record_slot_entries(ID, Slot, Keys, Opts) ->
    case durable(Opts) of
        false -> ok;
        true ->
            maybe_resume_journal(Opts),
            write_record(slot_path(<<"push-slot">>, ID, Slot), Keys, Opts),
            journal_slot(ID, Slot, Opts)
    end.

%% @doc Journal a slot whose push must run, under the current hour.
journal_slot(ID, Slot, Opts) ->
    case durable(Opts) of
        false -> ok;
        true ->
            Bucket = hb_util:bin(erlang:system_time(second) div 3600),
            Group = <<"push-journal/", Bucket/binary>>,
            ensure_table(?CRANKER_STATE),
            GroupKey = {journal_group, local_store_hash(Opts), Group},
            case ets:insert_new(?CRANKER_STATE, {GroupKey, true}) of
                true -> store_call(group, Group, Opts);
                false -> ok
            end,
            store_call(
                write,
                #{ <<Group/binary, "/", ID/binary, "@",
                        (hb_util:bin(Slot))/binary>> => <<"1">> },
                Opts
            )
    end.

mark_slot_done(ID, Slot, Opts) ->
    case durable(Opts) of
        false -> ok;
        true ->
            store_call(write, #{ slot_path(<<"push-done">>, ID, Slot) => <<"1">> }, Opts)
    end.

is_slot_done(ID, Slot, Opts) ->
    durable(Opts) andalso
        store_call(read, slot_path(<<"push-done">>, ID, Slot), Opts) =/= not_found.

%% @doc Mark a slot done when none of its entries is left to deliver: a
%% deferred or in-flight entry, or one whose schedule request failed, keeps
%% the slot in the journal.
maybe_mark_slot_done(ID, Slot, Downstream, Opts) ->
    Unresolved =
        [
            K
        ||
            {K, R} <- maps:to_list(Downstream),
            is_map(R),
            maps:get(<<"deferred">>, R, false) == true
                orelse maps:is_key(<<"in-flight">>, R)
                orelse (maps:get(<<"response">>, R, undefined) == <<"error">>
                    andalso maps:get(<<"status">>, R, undefined) =/= 404)
        ],
    case Unresolved of
        [] -> mark_slot_done(ID, Slot, Opts);
        _ -> ok
    end.

%% @doc After a deferred entry is resolved, mark its slot done if it was the
%% last entry of the slot still open.
maybe_complete_slot(Origin, Opts) ->
    case durable(Opts) of
        false -> ok;
        true ->
            ID = maps:get(<<"process">>, Origin),
            Slot = maps:get(<<"slot">>, Origin),
            case read_record(slot_path(<<"push-slot">>, ID, Slot), Opts) of
                {ok, Keys} when is_list(Keys) ->
                    Open =
                        [
                            K
                        ||
                            K <- Keys,
                            resolved_record(
                                entry_path(Origin#{ <<"outbox-key">> => K }),
                                Opts
                            ) == unresolved
                        ],
                    case Open of
                        [] -> mark_slot_done(ID, Slot, Opts);
                        _ -> ok
                    end;
                _ -> ok
            end
    end.

entry_path(#{ <<"process">> := ID, <<"slot">> := Slot, <<"outbox-key">> := Key }) ->
    <<
        (slot_path(<<"push-delivery">>, ID, Slot))/binary,
        "/",
        (hb_util:encode(hb_util:bin(Key)))/binary
    >>.

slot_path(Prefix, ID, Slot) ->
    <<Prefix/binary, "/", (hb_util:bin(ID))/binary, "/", (hb_util:bin(Slot))/binary>>.

write_record(Path, Term, Opts) ->
    store_call(write, #{ Path => term_to_binary(Term) }, Opts).

read_record(Path, Opts) ->
    case store_call(read, Path, Opts) of
        {ok, Bin} when is_binary(Bin) ->
            try {ok, binary_to_term(Bin, [safe])}
            catch _:_ -> not_found
            end;
        _ -> not_found
    end.

%% @doc Call the node's local store. Records are bookkeeping: a store that
%% refuses one is logged and delivery carries on, at the old guarantee.
store_call(Function, Arg, Opts) ->
    LocalOpts = hb_store:scope(Opts, local),
    Store = hb_opts:get(store, [], LocalOpts),
    try hb_store:Function(Store, Arg, LocalOpts) of
        {ok, Res} -> {ok, Res};
        ok -> ok;
        Other when Function == read; Function == list ->
            ?event(push_durable, {store_miss, {function, Function}, {res, Other}}),
            not_found;
        Other ->
            ?event(push, {push_record_failed, {function, Function}, {res, Other}}),
            Other
    catch Class:Reason ->
        ?event(push, {push_record_failed, {function, Function}, {Class, Reason}}),
        not_found
    end.

%% @doc Replay the journal once per store per node start: push every journaled
%% slot of the last `push-durable-horizon' hours that has no `done' marker.
maybe_resume_journal(Opts) ->
    ensure_table(?CRANKER_STATE),
    Key = {resumed, local_store_hash(Opts)},
    case ets:insert_new(?CRANKER_STATE, {Key, true}) of
        true ->
            ServerID = erlang:get(server_id),
            spawn(fun() ->
                hb_http_server:set_proc_server_id(ServerID),
                resume_journal(Opts)
            end),
            ok;
        false -> ok
    end.

local_store_hash(Opts) ->
    erlang:phash2(hb_opts:get(store, [], hb_store:scope(Opts, local))).

resume_journal(Opts) ->
    Hours =
        hb_util:int(
            hb_opts:get(
                push_durable_horizon,
                ?DEFAULT_DURABLE_HORIZON_HOURS,
                Opts
            )
        ),
    Now = erlang:system_time(second) div 3600,
    Resumed =
        lists:sum(
            [
                resume_bucket(hb_util:bin(Bucket), Opts)
            ||
                Bucket <- lists:seq(Now - Hours, Now)
            ]
        ),
    ?event(push, {push_journal_resumed, {slots, Resumed}}),
    Resumed.

resume_bucket(Bucket, Opts) ->
    case store_call(list, <<"push-journal/", Bucket/binary>>, Opts) of
        {ok, Children} ->
            length(
                [
                    ok
                ||
                    Child <- Children,
                    [ID, SlotBin] <- [binary:split(Child, <<"@">>)],
                    not is_slot_done(ID, SlotBin, Opts),
                    enqueue({continue, ID, hb_util:int(SlotBin)}, 0, Opts) == ok
                ]
            );
        _ -> 0
    end.

%% @doc Defer an entry whose target could not be read. `Why' is the read's
%% outcome: a remembered miss is retried once it has expired, a failed read
%% with exponential backoff (1 s doubling, at most 300 s) for
%% `push-retry-max-attempts' attempts. With no room on the queue the entry is
%% decided now, with a fresh read, rather than dropped.
defer_entry(Target, MsgToPush, Origin, Why, Attempt, Opts) ->
    Max =
        hb_util:int(
            hb_opts:get(
                push_retry_max_attempts,
                ?DEFAULT_RETRY_MAX_ATTEMPTS,
                Opts
            )
        ),
    case Attempt > Max of
        true ->
            ?event(push,
                {push_retry_abandoned,
                    {target, Target},
                    {origin, Origin},
                    {why, Why}
                }
            ),
            unavailable_target(Target, Why);
        false ->
            Delay = retry_delay(Why, Attempt, Opts),
            record_entry(Origin, retrying, Opts),
            Item = {retry, Target, MsgToPush, Origin, now_ms(), Attempt},
            case enqueue(Item, Delay, Opts) of
                ok ->
                    ?event(push_short,
                        {push_entry_deferred,
                            {target, Target},
                            {why, Why},
                            {delay_ms, Delay}
                        }
                    ),
                    #{
                        <<"status">> => 202,
                        <<"target">> => Target,
                        <<"deferred">> => true,
                        <<"retry-in-ms">> => Delay,
                        <<"reason">> => why_bin(Why)
                    };
                full ->
                    case read_target_fresh(Target, Opts) of
                        {ok, DownstreamProcess} ->
                            push_result_message(
                                DownstreamProcess,
                                MsgToPush,
                                Origin,
                                Opts
                            );
                        {error, not_found} ->
                            record_entry(Origin, rejected, Opts),
                            target_process_not_found(Target);
                        Unavailable ->
                            ?event(push,
                                {push_retry_overflow,
                                    {target, Target},
                                    {why, Unavailable}
                                }
                            ),
                            unavailable_target(Target, Unavailable)
                    end
            end
    end.

retry_delay({cached_miss, At}, _Attempt, Opts) ->
    max(0, At + miss_ttl_ms(Opts) - now_ms()) + 50;
retry_delay(_, Attempt, _Opts) ->
    min(1000 bsl min(Attempt - 1, 16), 300000).

why_bin({cached_miss, _}) -> <<"target-recently-missing">>;
why_bin(_) -> <<"target-read-failed">>.

unavailable_target(Target, Why) ->
    #{
        <<"response">> => <<"error">>,
        <<"status">> => 503,
        <<"target">> => Target,
        <<"reason">> => why_bin(Why)
    }.

%% @doc Retry a deferred entry. A miss remembered after the entry was deferred
%% came from a fresh read, so it decides the entry; a miss remembered before
%% that is waited out; otherwise the target is read through every store.
retry_entry(Target, MsgToPush, Origin, DeferredAt, Attempt, Opts) ->
    Read =
        case hb_cache:read(Target, hb_store:scope(Opts, local)) of
            {ok, Local} -> {ok, Local};
            _ ->
                case miss_time(Target) of
                    {ok, At} when At >= DeferredAt -> {error, not_found};
                    _ ->
                        case recent_miss(Target, Opts) of
                            {true, At} -> {cached_miss, At};
                            false -> read_target_fresh(Target, Opts)
                        end
                end
        end,
    Res =
        case Read of
            {ok, DownstreamProcess} ->
                push_result_message(DownstreamProcess, MsgToPush, Origin, Opts);
            {error, not_found} ->
                ?event(push, {push_retry_rejected, {target, Target}}),
                record_entry(Origin, rejected, Opts),
                target_process_not_found(Target);
            Why ->
                defer_entry(Target, MsgToPush, Origin, Why, Attempt + 1, Opts)
        end,
    maybe_complete_slot(Origin, Opts),
    Res.

%% @doc Queue work for the cranker, after `Delay' milliseconds. Returns `full'
%% when the queue is at its bound.
enqueue(Item, Delay, Opts) ->
    Max =
        hb_util:int(
            hb_opts:get(
                push_cranker_max_queue,
                ?DEFAULT_CRANKER_MAX_QUEUE,
                Opts
            )
        ),
    ensure_cranker(),
    Size =
        try ets:lookup(?CRANKER_STATE, size) of
            [{size, N}] -> N;
            [] -> 0
        catch error:badarg -> 0
        end,
    case Size < Max of
        false ->
            ?event(push, {push_cranker_full, {size, Size}, {item, element(1, Item)}}),
            full;
        true ->
            ets:update_counter(?CRANKER_STATE, size, 1, {size, 0}),
            Msg = {enqueue, Item, crank_opts(Opts), erlang:get(server_id)},
            case Delay > 0 of
                true -> erlang:send_after(Delay, ?CRANKER, Msg);
                false -> ?CRANKER ! Msg
            end,
            ok
    end.

%% @doc The options the cranker runs an item under: those of a detached worker,
%% with a fresh implicit depth bound when the enqueuer's bound was implicit.
crank_opts(Opts) ->
    Clean =
        case map_get_true(<<"push-depth-implicit">>, Opts) of
            true ->
                maps:without(
                    [
                        <<"push-in-worker">>,
                        <<"push-depth-implicit">>,
                        <<"push-max-depth">>
                    ],
                    Opts
                );
            false -> maps:without([<<"push-in-worker">>], Opts)
        end,
    detached_opts(#{}, Clean).

ensure_cranker() ->
    ensure_table(?CRANKER_STATE),
    case whereis(?CRANKER) of
        undefined ->
            Pid =
                spawn(
                    fun() ->
                        receive registered -> ok end,
                        ets:insert(?CRANKER_STATE, {size, 0}),
                        cranker_loop(
                            #{
                                queue => queue:new(),
                                keys => #{},
                                running => #{},
                                opts => #{},
                                workers => ?DEFAULT_CRANKER_WORKERS
                            }
                        )
                    end
                ),
            try register(?CRANKER, Pid) of
                true -> Pid ! registered
            catch error:badarg -> exit(Pid, kill)
            end,
            ok;
        _ -> ok
    end.

cranker_loop(State = #{ queue := Q, keys := Keys, running := Running }) ->
    receive
        {enqueue, Item, Opts, ServerID} ->
            ItemKey = item_key(Item),
            case maps:is_key(ItemKey, Keys) of
                true ->
                    ets:update_counter(?CRANKER_STATE, size, -1, {size, 1}),
                    cranker_loop(State);
                false ->
                    OptsKey = erlang:phash2(Opts),
                    Workers =
                        hb_util:int(
                            hb_opts:get(
                                push_cranker_workers,
                                ?DEFAULT_CRANKER_WORKERS,
                                Opts
                            )
                        ),
                    cranker_loop(
                        dispatch(
                            State#{
                                queue => queue:in({ItemKey, Item, OptsKey, ServerID}, Q),
                                keys => Keys#{ ItemKey => true },
                                opts => (maps:get(opts, State))#{ OptsKey => Opts },
                                workers => Workers
                            }
                        )
                    )
            end;
        {'DOWN', Ref, process, _, _} ->
            case maps:take(Ref, Running) of
                {ItemKey, Rest} ->
                    ets:update_counter(?CRANKER_STATE, size, -1, {size, 1}),
                    cranker_loop(
                        dispatch(
                            prune_crank_opts(
                                State#{
                                    running => Rest,
                                    keys => maps:remove(ItemKey, Keys)
                                }
                            )
                        )
                    );
                error -> cranker_loop(State)
            end
    end.

dispatch(State = #{ queue := Q, running := Running, workers := Workers, opts := AllOpts }) ->
    case map_size(Running) < Workers andalso queue:out(Q) of
        {{value, {ItemKey, Item, OptsKey, ServerID}}, Rest} ->
            Opts = maps:get(OptsKey, AllOpts),
            {_, Ref} =
                spawn_monitor(
                    fun() ->
                        hb_http_server:set_proc_server_id(ServerID),
                        run_item(Item, Opts)
                    end
                ),
            dispatch(
                State#{
                    queue => Rest,
                    running => Running#{ Ref => {ItemKey, OptsKey} }
                }
            );
        _ -> State
    end;
dispatch(State) -> State.

%% @doc Keep only the option maps that a queued or running item still needs.
prune_crank_opts(State = #{ opts := AllOpts }) when map_size(AllOpts) =< 16 ->
    State;
prune_crank_opts(State = #{ queue := Q, running := Running, opts := AllOpts }) ->
    Live =
        [OK || {_, _, OK, _} <- queue:to_list(Q)] ++
            [OK || {_, OK} <- maps:values(Running)],
    State#{ opts => maps:with(Live, AllOpts) }.

item_key({continue, ID, Slot}) -> {continue, ID, Slot};
item_key({retry, _, _, _, _, _}) -> {retry, make_ref()}.

run_item(Item, Opts) ->
    try do_run_item(Item, Opts)
    catch Class:Reason:Stacktrace ->
        ?event(push,
            {push_cranker_item_failed,
                {item, element(1, Item)},
                {class, Class},
                {reason, Reason},
                {stack, {trace, Stacktrace}}
            }
        )
    end.

do_run_item({continue, ID, Slot}, Opts) ->
    case is_slot_done(ID, Slot, Opts) of
        true -> ok;
        false -> push_downstream(ID, Slot, #{ <<"result-depth">> => 0 }, Opts)
    end;
do_run_item({retry, Target, MsgToPush, Origin, DeferredAt, Attempt}, Opts) ->
    retry_entry(Target, MsgToPush, Origin, DeferredAt, Attempt, Opts).

%%% Tests

%% The test groups run sequentially with respect to each other but each
%% group is internally parallel. Splitting keeps the WASM-heavy core
%% push tests off the same BEAM as the max-depth + post-compute hook
%% tests; merging them into one `{inparallel, ...}' batch oversubscribed
%% the scheduler enough to slow every individual test by ~50%.
dev_push_test_() ->
    [
        {inparallel, core_push_test_cases() ++ genesis_wasm_tests()},
        {inparallel, max_depth_test_cases()}
    ].

core_push_test_cases() ->
    [
        {timeout, 30, fun test_full_push/0},
        {timeout, 90, fun test_push_as_identity/0},
        {timeout, 30, fun test_multi_process_push/0},
        {timeout, 30, fun test_push_prompts_encoding_change/0},
        {timeout, 60, fun test_remote_routed_push/0},
        {timeout, 30, fun test_oracle_push/0}
    ].

max_depth_test_cases() ->
    [
        {timeout, 30, fun test_max_depth_zero_schedules_only/0},
        {timeout, 30, fun test_max_depth_one_walks_one_hop/0},
        {timeout, 30, fun test_compute_push_hook_idempotent/0},
        fun test_paranoid_push_result/0,
        fun test_parse_max_depth/0
    ].

cron_depth_zero_push_test_() ->
    {timeout, 120, fun test_cron_depth_zero_push/0}.

%% @doc Delivery of a slot's outbox does not depend on the process that asked
%% for the push. Runs on its own because it kills a process mid-resolution.
push_detachment_test_() ->
    {timeout, 120, fun test_push_survives_caller_death/0}.

test_paranoid_push_result() ->
    % The `push_result' topic verifies the result before it is signed for
    % POSTing (at the `apply_security' entry): corrupting a committed
    % sub-message is caught before any signature is applied.
    Opts =
        #{
            <<"priv-wallet">> => hb:wallet(),
            <<"paranoid-verify">> => [push_result],
            <<"debug-print">> => []
        },
    Nested = hb_message:commit(#{ <<"body">> => <<"ok">> }, Opts),
    ?assertThrow(
        {paranoid_verification_failure, push_result, _, _, _},
        apply_security(
            #{ <<"body">> => Nested#{ <<"body">> => <<"mangled">> } },
            #{},
            <<"httpsig@1.0">>,
            Opts
        )
    ).

-ifdef(ENABLE_GENESIS_WASM).
genesis_wasm_tests() -> [{timeout, 30, fun test_nested_push_prompts_encoding_change/0}].
-else.
genesis_wasm_tests() -> [].
-endif.

test_full_push() ->
    hb_process_test_vectors:init(),
    Opts = #{
        <<"priv-wallet">> => hb:wallet(),
        <<"cache-control">> => <<"always">>,
        <<"store">> => [hb_test_utils:test_store(hb_store_lmdb)],
        % Exercise the `push_result' paranoid check against a real signed push.
        <<"paranoid-verify">> => [push_result]
    },
    Base = hb_process_test_vectors:aos_process(Opts),
    hb_cache:write(Base, Opts),
    {ok, SchedInit} =
        hb_ao:resolve(Base, #{
            <<"method">> => <<"POST">>,
            <<"path">> => <<"schedule">>,
            <<"body">> => Base
        },
        Opts
    ),
    ?event({test_setup, {base, Base}, {sched_init, SchedInit}}),
    Script = ping_pong_script(2),
    ?event({script, Script}),
    {ok, Req} = hb_process_test_vectors:schedule_aos_call(Base, Script, Opts),
    ?event({msg_sched_result, Req}),
    {ok, StartingMsgSlot} =
        hb_ao:resolve(Req, #{ <<"path">> => <<"slot">> }, Opts),
    ?event({starting_msg_slot, StartingMsgSlot}),
    Res =
        #{
            <<"path">> => <<"push">>,
            <<"slot">> => StartingMsgSlot
        },
    {ok, _} = hb_ao:resolve(Base, Res, Opts),
    ?assertEqual(
        {ok, <<"Done.">>},
        hb_ao:resolve(Base, <<"now/results/data">>, Opts)
    ).

test_push_as_identity() ->
    hb_process_test_vectors:init(),
    % Create a new identity for the scheduler.
    DefaultWallet = hb:wallet(),
    SchedulingWallet = ar_wallet:new(),
    SchedulingID = hb_util:human_id(SchedulingWallet),
    ComputeWallet = ar_wallet:new(),
    ComputeID = hb_util:human_id(ComputeWallet),
    TestStore = [hb_test_utils:test_store(hb_store_lmdb)],
    Opts = #{
        <<"priv-wallet">> => DefaultWallet,
        <<"cache-control">> => <<"always">>,
        <<"store">> => TestStore,
        <<"identities">> => #{
            SchedulingID => #{
                <<"priv-wallet">> => SchedulingWallet,
                <<"store">> => [hb_test_utils:test_store(hb_store_lmdb)]
            },
            ComputeID => #{
                <<"priv-wallet">> => ComputeWallet
            }
        }
    },
    % Create a new test AOS process, which will use the given identities as
    % its authority and scheduler.
    Base =
        hb_process_test_vectors:aos_process(
            Opts#{
                <<"authority">> => ComputeID,
                <<"scheduler">> => [SchedulingID, ComputeID]
            }
        ),
    ?event({base, Base}),
    % Perform the remainder of the test as with `full_push_test_/0'.
    hb_cache:write(Base, Opts),
    {ok, SchedInit} =
        hb_ao:resolve(Base, #{
            <<"method">> => <<"POST">>,
            <<"path">> => <<"schedule">>,
            <<"body">> => Base
        },
        Opts
    ),
    ?event({test_setup, {base, Base}, {sched_init, SchedInit}}),
    Script = ping_pong_script(2),
    ?event({script, Script}),
    {ok, Req} = hb_process_test_vectors:schedule_aos_call(Base, Script, Opts),
    ?event(push, {msg_sched_result, Req}),
    {ok, StartingMsgSlot} =
        hb_ao:resolve(Req, #{ <<"path">> => <<"slot">> }, Opts),
    ?event({starting_msg_slot, StartingMsgSlot}),
    Res =
        #{
            <<"path">> => <<"push">>,
            <<"slot">> => StartingMsgSlot
        },
    {ok, _} = hb_ao:resolve(Base, Res, Opts),
    ?assertEqual(
        {ok, <<"Done.">>},
        hb_ao:resolve(Base, <<"now/results/data">>, Opts)
    ),
    % Validate that the scheduler's wallet was used to sign the message.
    Assignment =
        hb_ao:get(
            <<"schedule/assignments/2">>,
            Base,
            Opts
        ),
    Committers = hb_ao:get(
        <<"committers">>,
        hb_cache:read_all_commitments(Assignment, Opts),
        Opts
    ),
    ?assert(lists:member(SchedulingID, Committers)),
    ?assert(lists:member(ComputeID, Committers)),
    % Validate that the compute wallet was used to sign the message.
    ?assertEqual(
        [ComputeID],
        hb_ao:get(<<"schedule/assignments/2/body/committers">>, Base, Opts)
    ).

test_multi_process_push() ->
    {Sender, _Receiver, MsgSlot, Opts} = setup_two_process_message(),
    %% Install a catch-all `Pong' handler on the Sender so the Receiver's
    %% reply (the helper's `reply_script' fires on `Action = "Ping"' and
    %% sends back `Action = "Reply"') is observable as `GOT PONG' in the
    %% Sender's `now/results/data'.
    {ok, _} =
        hb_process_test_vectors:schedule_aos_call(
            Sender,
            <<
                "Handlers.add(\"Pong\",\n"
                "   function (test) return true end,\n"
                "   function(m)\n"
                "       print(\"GOT PONG\")\n"
                "   end\n"
                ")"
            >>,
            Opts
        ),
    {ok, PushResult} =
        hb_ao:resolve(
            Sender,
            #{
                <<"path">> => <<"push">>,
                <<"slot">> => MsgSlot,
                <<"result-depth">> => 1
            },
            Opts
        ),
    ?event(push, {push_result, PushResult}),
    ?assertEqual(
        {ok, <<"GOT PONG">>},
        hb_ao:resolve(Sender, <<"now/results/data">>, Opts)
    ).

push_with_redirect_hint_test_disabled() ->
    {timeout, 30, fun() ->
        hb_process_test_vectors:init(),
        Stores =
            [
                #{
                    <<"store-module">> => hb_store_fs,
                    <<"name">> => <<"cache-TEST">>
                }
            ],
        ExtOpts = #{ <<"priv-wallet">> => ar_wallet:new(), <<"store">> => Stores },
        LocalOpts = #{ <<"priv-wallet">> => hb:wallet(), <<"store">> => Stores },
        ExtScheduler = hb_http_server:start_node(ExtOpts),
        ?event(push, {external_scheduler, {location, ExtScheduler}}),
        % Create the Pong server and client
        Client = hb_process_test_vectors:aos_process(),
        PongServer = hb_process_test_vectors:aos_process(ExtOpts),
        % Push the new process that runs on the external scheduler
        {ok, ServerSchedResp} =
            hb_http:post(
                ExtScheduler,
                <<"/push">>,
                PongServer,
                ExtOpts
            ),
        ?event(push, {pong_server_sched_resp, ServerSchedResp}),
        % Get the IDs of the server process
        PongServerID =
            hb_ao:get(
                <<"process/id">>,
                lib_process:ensure_process_key(PongServer, LocalOpts),
                LocalOpts
            ),
        {ok, ServerScriptSchedResp} =
            hb_http:post(
                ExtScheduler,
                <<PongServerID/binary, "/push">>,
                #{
                    <<"body">> =>
                        hb_message:commit(
                            #{
                                <<"target">> => PongServerID,
                                <<"action">> => <<"Eval">>,
                                <<"type">> => <<"Message">>,
                                <<"data">> => reply_script()
                            },
                            ExtOpts
                        )
                },
                ExtOpts
            ),
        ?event(push, {pong_server_script_sched_resp, ServerScriptSchedResp}),
        {ok, ToPush} =
            hb_process_test_vectors:schedule_aos_call(
                Client,
                <<
                    "Handlers.add(\"Pong\",\n"
                    "   function (test) return true end,\n"
                    "   function(m)\n"
                    "       print(\"GOT PONG\")\n"
                    "   end\n"
                    ")\n"
                    "Send({ Target = \"",
                        (PongServerID)/binary, "?hint=",
                        (ExtScheduler)/binary,
                    "\", Action = \"Ping\" })\n"
                >>,
                LocalOpts
            ),
        SlotToPush = hb_ao:get(<<"slot">>, ToPush, LocalOpts),
        ?event(push, {slot_to_push_client, SlotToPush}),
        Res = #{ <<"path">> => <<"push">>, <<"slot">> => SlotToPush },
        {ok, PushResult} = hb_ao:resolve(Client, Res, LocalOpts),
        ?event(push, {push_result_client, PushResult}),
        AfterPush = hb_ao:resolve(Client, <<"now/results/data">>, LocalOpts),
        ?event(push, {after_push, AfterPush}),
        % Note: This test currently only gets a reply that the message was not
        % trusted by the process. To fix this, we would have to add another 
        % trusted authority to the `test_aos_process' call. For now, this is 
        % enough to validate that redirects are pushed through correctly.
        ?assertEqual({ok, <<"GOT PONG">>}, AfterPush)
    end}.

test_push_prompts_encoding_change() ->
    hb_process_test_vectors:init(),
    Opts = #{
        <<"priv-wallet">> => hb:wallet(),
        <<"cache-control">> => <<"always">>,
        <<"store">> =>
            [
                #{ <<"store-module">> => hb_store_fs, <<"name">> => <<"cache-TEST">> },
                % Include a gateway store so that we can get the legacynet 
                % process when needed.
                #{ <<"store-module">> => hb_store_gateway,
                    <<"store">> => #{
                        <<"store-module">> => hb_store_fs,
                        <<"name">> => <<"cache-TEST">>
                    }
                }
            ]
    },
    Msg = hb_message:commit(#{
        <<"path">> => <<"push">>,
        <<"method">> => <<"POST">>,
        <<"target">> => <<"QQiMcAge5ZtxcUV7ruxpi16KYRE8UBP0GAAqCIJPXz0">>,
        <<"action">> => <<"Eval">>,
        <<"data">> => <<"print(\"Please ignore!\")">>
    }, Opts),
    ?event(push, {base, Msg}),
    Res =
        hb_ao:resolve_many(
            [
                <<"QQiMcAge5ZtxcUV7ruxpi16KYRE8UBP0GAAqCIJPXz0">>,
                {as, <<"process@1.0">>, <<>>},
                Msg
            ],
            Opts
        ),
    ?assertMatch({error, #{ <<"status">> := 422 }}, Res).

test_remote_routed_push() ->
    % Creates a network of nodes and processes with the following structure:
    % Node 1:
    %   - Schedules for process 1.
    %   - Routes requests for process 2 to Node 2.
    % Node 2:
    %   - Schedules for process 2.
    %
    % Process 1:
    %   - Has an `owner` of Node 1's wallet.
    %   - Has both node 1 and node 2 as authorities.
    %   - Pushes a `pong` message to process 2 on recipient of an `action: ping`
    %     message.
    % 
    % Process 2:
    %   - Has an `owner` of Node 2's wallet.
    %   - Has both node 1 and node 2 as authorities.
    %   - Pushes a `pong` message to process 1 on recipient of a message.
    % 
    % After establishing the network, we ensure that a message can be correctly
    % pushed from user to process 1, to process 2, then back to process 1.
    % 
    % We start by generating the isolated wallets and stores for each node.
    N1Wallet = ar_wallet:new(),
    N1Store = [hb_test_utils:test_store(hb_store_lmdb)],
    N2Wallet = ar_wallet:new(),
    N2Store = [hb_test_utils:test_store(hb_store_lmdb)],
    % Next, create the second node and process. We do this before node 1 such 
    % that the routes of node 1 and the target of process 1's message are known
    % when we create them.
    N2Opts =
        #{
            <<"store">> => N2Store,
            <<"priv-wallet">> => N2Wallet
        },
    N2 = hb_http_server:start_node(N2Opts),
    % Create the second process on the second node.
    Proc2 = hb_process_test_vectors:aos_process(N2Opts),
    LoadedProc2 = hb_cache:ensure_all_loaded(Proc2, N2Opts),
    Proc2ID = hb_message:id(Proc2, signed, N2Opts),
    % Next, create the first node and process.
    N1Opts =
        #{
            <<"store">> => N1Store,
            <<"priv-wallet">> => N1Wallet,
            <<"routes">> =>
                [
                    #{
                        <<"template">> => <<Proc2ID/binary, ".*">>,
                        <<"node">> => N2
                    }
                ]
        },
    N1 = hb_http_server:start_node(N1Opts),
    % Sanity check that routing resolves the Proc2ID path to N2 on the first node.
    ?assertMatch(
        {ok, N2},
        hb_http:get(
            N1,
            <<"/~router@1.0/route?route-path=", Proc2ID/binary, "/push&slot=1">>,
            N1Opts
        )
    ),
    % Create the first process on the first node.
    Proc1 = hb_process_test_vectors:aos_process(N1Opts),
    LoadedProc1 = hb_cache:ensure_all_loaded(Proc1, N1Opts),
    Proc1ID = hb_message:id(LoadedProc1, all, N1Opts),
    % Write both processes to each of the nodes' caches, such that both are
    % 'globally' available to each other.
    hb_cache:write(LoadedProc1, N1Opts),
    hb_cache:write(LoadedProc1, N2Opts),
    hb_cache:write(LoadedProc2, N1Opts),
    hb_cache:write(LoadedProc2, N2Opts),
    ?event(debug_test,
        {network_setup, 
            {proc1ID, Proc1ID},
            {proc2ID, Proc2ID},
            {n1, N1},
            {n2, N2},
            {wallet1, ar_wallet:to_address(N1Wallet)},
            {wallet2, ar_wallet:to_address(N2Wallet)}
        }
    ),
    % Set the authorities of the processes to include both wallets.
    SetAuthoritiesCommand =
        <<
            "ao.authorities = { ",
                "\"", (hb_util:human_id(N1Wallet))/binary, "\",",
                "\"", (hb_util:human_id(N2Wallet))/binary, "\"",
            " }; ",
            "ao.addAssignable('foobar', function (msg) return true end); "
            "ao.isAssignable = function(m) return true end"
        >>,
    {ok, SetAuthProc1} =
        hb_process_test_vectors:schedule_aos_call(LoadedProc1, SetAuthoritiesCommand, N1Opts),
    {ok, SetAuthProc2} =
        hb_process_test_vectors:schedule_aos_call(LoadedProc2, SetAuthoritiesCommand, N2Opts),
    ?event(debug_test,
        {set_authorities, 
            {command, {string, SetAuthoritiesCommand}},
            {proc1_result, SetAuthProc1},
            {proc2_result, SetAuthProc2}
        }
    ),
    % Load the scripts into each process. The second process has the base
    % reply script, and the first process has reply script with a trigger to
    % send a message to the second process.
    {ok, P2ScriptLoadRes} =
        hb_process_test_vectors:schedule_aos_call(
            LoadedProc2,
            reply_script(),
            N2Opts
        ),
    {ok, P1ScriptLoadRes} =
        hb_process_test_vectors:schedule_aos_call(
            LoadedProc1,
            reply_script(Proc2ID),
            N1Opts
        ),
    ?event(debug_test,
        {script_load, 
            {proc2_result, P2ScriptLoadRes},
            {proc1_result, P1ScriptLoadRes}
        }
    ),
    % Get the slot of the message to push on process 1.
    SlotP1 = hb_ao:get(<<"slot">>, P1ScriptLoadRes, N1Opts),
    ?event(debug_test, {slot_p1, SlotP1}),
    PushRes =
        hb_http:post(
            N1,
            #{ 
                <<"path">> => <<Proc1ID/binary, "/push">>,
                <<"slot">> => SlotP1
            },
            N1Opts
        ),
    ?event(debug_test, {push_res, PushRes}),
    {ok, SchedResP1} = hb_ao:resolve(LoadedProc1, <<"schedule">>, N1Opts),
    ?event(debug_test, {sched_res_p1, SchedResP1}),
    {ok, SchedResP2} = hb_ao:resolve(LoadedProc2, <<"schedule">>, N2Opts),
    ?event(debug_test, {sched_res_p2, SchedResP2}),
    ?assertEqual(
        {error, not_found},
        hb_ao:resolve_many(
            [
                LoadedProc2,
                #{ <<"path">> => <<"compute">>, <<"init">> => <<"stop">> }
            ],
            N1Opts
        )
    ),
    ?assertMatch(
        {ok, Slot} when Slot > 0,
        hb_ao:resolve(LoadedProc2, <<"now/at-slot">>, N2Opts)
    ).

%% @doc Play the same game one hop per node-local cron tick with `push = 0'.
test_cron_depth_zero_push() ->
    hb_process_test_vectors:init(),
    SharedStore = hb_test_utils:test_store(hb_store_fs),
    SharedOpts = #{ <<"store">> => [SharedStore] },
    ReadOnlyShared = SharedStore#{ <<"access">> => [<<"read">>] },
    Wallet1 = ar_wallet:new(),
    Wallet2 = ar_wallet:new(),
    Address1 = hb_util:human_id(Wallet1),
    Address2 = hb_util:human_id(Wallet2),
    UserWallet = ar_wallet:new(),
    Opts = #{
        <<"priv-wallet">> => UserWallet,
        <<"store">> =>
            [hb_test_utils:test_store(hb_store_lmdb), ReadOnlyShared]
    },
    Proc1 =
        hb_process_test_vectors:aos_process(
            SharedOpts#{
                <<"priv-wallet">> => UserWallet,
                <<"scheduler">> => Address1
            }
        ),
    Proc2 =
        hb_process_test_vectors:aos_process(
            SharedOpts#{
                <<"priv-wallet">> => UserWallet,
                <<"scheduler">> => Address2
            }
        ),
    {ok, _} = hb_cache:write(Proc1, SharedOpts),
    {ok, _} = hb_cache:write(Proc2, SharedOpts),
    Proc1ID = hb_message:id(Proc1, all, SharedOpts),
    Proc2ID = hb_message:id(Proc2, all, SharedOpts),
    Store1 = [hb_test_utils:test_store(hb_store_lmdb), ReadOnlyShared],
    Store2 = [hb_test_utils:test_store(hb_store_lmdb), ReadOnlyShared],
    Node1 = hb_http_server:start_node(#{
        <<"priv-wallet">> => Wallet1,
        <<"store">> => Store1
    }),
    Node2 = hb_http_server:start_node(#{
        <<"priv-wallet">> => Wallet2,
        <<"store">> => Store2
    }),
    share_location_record(Wallet1, Node1, Node2),
    share_location_record(Wallet2, Node2, Node1),
    {ok, _} = hb_http:post(Node1, <<"/schedule">>, Proc1, Opts),
    {ok, _} = hb_http:post(Node2, <<"/schedule">>, Proc2, Opts),
    Trust = <<
        "ao.authorities = { \"", Address1/binary, "\",\"", Address2/binary,
        "\" }; ao.addAssignable('all', function (msg) return true end); ",
        "ao.isAssignable = function(m) return true end"
    >>,
    {ok, _} =
        hb_http:post(
            Node1,
            <<Proc1ID/binary, "/schedule">>,
            eval_message(Proc1ID, Trust, Opts),
            Opts
        ),
    {ok, _} =
        hb_http:post(
            Node2,
            <<Proc2ID/binary, "/schedule">>,
            eval_message(
                Proc2ID,
                <<Trust/binary, "\n", (reply_script())/binary>>,
                Opts
            ),
            Opts
        ),
    {ok, _} =
        hb_http:post(
            Node1,
            <<Proc1ID/binary, "/schedule">>,
            eval_message(
                Proc1ID,
                <<"Send({ Target = \"", Proc2ID/binary, "\", Action = \"Ping\" })">>,
                Opts
            ),
            Opts
        ),
    Cron1 = start_now_push_cron(Node1, Proc1ID),
    Cron2 = start_now_push_cron(Node2, Proc2ID),
    GameSettled =
        wait_until(
            fun() -> remote_slot_current(Node1, Proc1ID) >= 3 end,
            60000
        ),
    {ok, _} = hb_http:get(Node1, <<"/~cron@1.0/stop=", Cron1/binary>>, #{}),
    {ok, _} = hb_http:get(Node2, <<"/~cron@1.0/stop=", Cron2/binary>>, #{}),
    ?assert(GameSettled),
    ?assertEqual(2, remote_slot_current(Node2, Proc2ID)),
    ?assertEqual(
        {error, not_found},
        hb_cache:read(
            <<"computed/", Proc2ID/binary, "/slot/2">>,
            #{ <<"store">> => Store1 }
        )
    ),
    ?assertEqual(
        {error, not_found},
        hb_cache:read(
            <<"computed/", Proc1ID/binary, "/slot/3">>,
            #{ <<"store">> => Store2 }
        )
    ),
    ?assertEqual(
        {ok, <<"Replying to...\n", Proc1ID/binary, "\nDone.">>},
        hb_http:get(
            Node2,
            <<Proc2ID/binary, "/compute&slot=2/results/data">>,
            #{}
        )
    ).

%% @doc Post an operator-signed location record to the target node.
share_location_record(Wallet, NodeURL, Target) ->
    Record =
        hb_message:commit(
            #{
                <<"type">> => <<"location">>,
                <<"url">> => NodeURL,
                <<"nonce">> => 1,
                <<"time-to-live">> => 60 * 60 * 1000
            },
            #{ <<"priv-wallet">> => Wallet }
        ),
    {ok, _} =
        hb_http:post(
            Target,
            <<"/~location@1.0/known">>,
            Record,
            #{}
        ).

%% @doc Read the current slot of a process from a node over HTTP.
remote_slot_current(Node, ProcID) ->
    {ok, Slot} = hb_http:get(Node, <<ProcID/binary, "/slot/current">>, #{}),
    hb_util:int(Slot).

%% @doc Establish a cron on the given node that drives `/<proc>/now' with
%% `push = 0' on every tick, returning the cron task ID.
start_now_push_cron(Node, ProcID) ->
    {ok, #{ <<"body">> := TaskID }} =
        hb_http:get(
            Node,
            <<
                "/~cron@1.0/every?interval=250-milliseconds",
                "&cron-path=/", ProcID/binary, "/now",
                "&push=0"
            >>,
            #{}
        ),
    TaskID.

%% @doc Commit an `Eval' message from the caller to the given process.
eval_message(ProcID, Code, Opts) ->
    hb_message:commit(
        #{
            <<"type">> => <<"Message">>,
            <<"action">> => <<"Eval">>,
            <<"target">> => ProcID,
            <<"data">> => Code
        },
        Opts
    ).

test_oracle_push() ->
    hb_process_test_vectors:init(),
    TestStore = [hb_test_utils:test_store(hb_store_lmdb)],
    Opts = #{ <<"priv-wallet">> => hb:wallet(), <<"store">> => TestStore },
    Client = hb_process_test_vectors:aos_process(Opts),
    {ok, _} = hb_cache:write(Client, Opts),
    {ok, _} = hb_process_test_vectors:schedule_aos_call(Client, oracle_script(), Opts),
    Res =
        #{
            <<"path">> => <<"push">>,
            <<"slot">> => 0
        },
    {ok, PushResult} = hb_ao:resolve(Client, Res, Opts),
    ?event({result, PushResult}),
    ComputeRes =
        hb_ao:resolve(
            Client,
            <<"now/results/data">>,
            Opts
        ),
    ?event({compute_res, ComputeRes}),
    ?assertMatch({ok, _}, ComputeRes).

%% @doc `parse_max_depth/1' contract: accept non-negative integers verbatim
%% or as binaries; reject everything else as `undefined' (unbounded).
test_parse_max_depth() ->
    ?assertEqual(undefined, parse_max_depth(undefined)),
    ?assertEqual(0, parse_max_depth(0)),
    ?assertEqual(7, parse_max_depth(7)),
    ?assertEqual(0, parse_max_depth(<<"0">>)),
    ?assertEqual(42, parse_max_depth(<<"42">>)),
    ?assertEqual(undefined, parse_max_depth(<<"not-a-number">>)),
    ?assertEqual(undefined, parse_max_depth(-1)),
    ?assertEqual(undefined, parse_max_depth(<<>>)),
    ?assertEqual(undefined, parse_max_depth(false)),
    ?assertEqual(undefined, parse_max_depth(true)).

%% @doc `max-depth = 0' on the outer push: the outbox of the source slot is
%% still scheduled on each target (target's `slot/current' advances), but the
%% recursive `/push' is skipped, so the response carries an explicit
%% `resulted-in: <<"skipped">>' marker and the target's compute is not invoked.
test_max_depth_zero_schedules_only() ->
    {Sender, Receiver, MsgSlot, Opts} = setup_two_process_message(),
    {ok, ReceiverSlotBefore} =
        hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
    {ok, PushResult} =
        hb_ao:resolve(
            Sender,
            #{
                <<"path">> => <<"push">>,
                <<"slot">> => MsgSlot,
                <<"max-depth">> => 0
            },
            Opts
        ),
    ?event({push_result_max_depth_zero, PushResult}),
    %% The outbox entry exists in the response with `resulted-in' set to the
    %% binary `<<"skipped">>' -- the explicit signal that we deliberately stopped.
    ?assertMatch(
        #{ <<"1">> := #{ <<"resulted-in">> := <<"skipped">> }},
        PushResult
    ),
    %% The receiver's scheduler nevertheless has the new message: schedule_result
    %% always runs, even when push_downstream is short-circuited.
    {ok, ReceiverSlotAfter} =
        hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
    ?assert(ReceiverSlotAfter > ReceiverSlotBefore).

%% @doc `max-depth = 1' on the outer push: the outbox is scheduled and the
%% target's `/push' runs once (depth 1 -> depth 0). When that target's own
%% outbox would fan out further, the recursion is skipped. Verifies the
%% decrement plumbing by observing both the recursion and the skip.
test_max_depth_one_walks_one_hop() ->
    {Sender, _Receiver, MsgSlot, Opts} = setup_two_process_message(),
    {ok, PushResult} =
        hb_ao:resolve(
            Sender,
            #{
                <<"path">> => <<"push">>,
                <<"slot">> => MsgSlot,
                <<"max-depth">> => 1
            },
            Opts
        ),
    ?event({push_result_max_depth_one, PushResult}),
    %% The receiver was actually pushed (recursion happened, depth 1 was
    %% spent here). For a `Reply' handler the receiver replies back to the
    %% sender, producing one further outbox entry whose own push would
    %% decrement to depth 0 and skip.
    #{ <<"1">> := #{ <<"resulted-in">> := Inner }} = PushResult,
    %% Either the receiver had a downstream message that skipped, or it had
    %% no outbox -- both prove that we did NOT short-circuit at depth 1.
    case hb_maps:get(<<"1">>, Inner, undefined) of
        undefined ->
            %% No downstream. Receiver's slot fired but produced no outbox.
            ?assert(hb_maps:is_key(<<"slot">>, Inner));
        Next ->
            %% Downstream existed; it must have skipped at depth 0.
            ?assertMatch(
                #{ <<"resulted-in">> := <<"skipped">> },
                Next
            )
    end.

%% @doc `~process@1.0/compute' called with `push = true' fires an async
%% `~push@1.0/push' for the freshly-computed slot. Calling
%% `~process@1.0/compute' again for the same slot is a cache hit, which
%% does NOT re-fire the hook -- so the push is naturally idempotent across
%% repeated polls (e.g. the cron tick pattern).
test_compute_push_hook_idempotent() ->
    {Sender, Receiver, MsgSlot, Opts} = setup_two_process_message(),
    {ok, ReceiverSlot0} =
        hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
    %% Drive a fresh compute for the message slot WITH `push = 0', so the
    %% hook fires and schedules on the receiver but the recursion halts.
    {ok, _} =
        hb_ao:resolve(
            Sender,
            #{
                <<"path">> => <<"compute">>,
                <<"slot">> => MsgSlot,
                <<"push">> => 0
            },
            Opts
        ),
    %% The hook spawns the push in a fresh process; wait for the receiver's
    %% schedule to advance.
    true = wait_until(
        fun() ->
            {ok, S} =
                hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
            S > ReceiverSlot0
        end,
        10000
    ),
    {ok, ReceiverSlot1} =
        hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
    %% Second call is a cache hit -> compute_slot is NOT entered ->
    %% maybe_trigger_push is NOT called. The receiver's slot must not advance
    %% any further.
    {ok, _} =
        hb_ao:resolve(
            Sender,
            #{
                <<"path">> => <<"compute">>,
                <<"slot">> => MsgSlot,
                <<"push">> => 0
            },
            Opts
        ),
    timer:sleep(500),
    {ok, ReceiverSlot2} =
        hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
    ?assertEqual(ReceiverSlot1, ReceiverSlot2).

%% @doc A `/push' whose caller vanishes mid-flight still delivers the source
%% slot's outbox to its target. The caller is a throwaway process killed with
%% an untrapped `shutdown' exit while the source slot is still computing --
%% the teardown Cowboy applies to a request process when its client
%% disconnects. Only a delivery that runs outside the caller can reach the
%% receiver's scheduler afterwards.
test_push_survives_caller_death() ->
    {Sender, Receiver, MsgSlot, Opts} = setup_lua_push_pair(),
    {ok, ReceiverSlot0} =
        hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
    Test = self(),
    Caller =
        spawn(
            fun() ->
                Test ! {pushing, self()},
                hb_ao:resolve(
                    Sender,
                    #{ <<"path">> => <<"push">>, <<"slot">> => MsgSlot },
                    Opts
                )
            end
        ),
    receive {pushing, Caller} -> ok
    after 5000 -> erlang:error(caller_never_started)
    end,
    % Every slot of the sender sleeps for seconds inside `compute', so this
    % kill lands long before the outbox reaches the receiver's scheduler.
    timer:sleep(1000),
    Monitor = erlang:monitor(process, Caller),
    exit(Caller, shutdown),
    receive {'DOWN', Monitor, process, Caller, shutdown} -> ok
    after 5000 -> erlang:error(caller_survived)
    end,
    ?assert(
        wait_until(
            fun() ->
                {ok, Slot} =
                    hb_ao:resolve(
                        Receiver,
                        #{ <<"path">> => <<"slot/current">> },
                        Opts
                    ),
                Slot > ReceiverSlot0
            end,
            60000
        )
    ).

%% @doc Stage a sender and a receiver `lua@5.3a' process and schedule -- without
%% pushing -- one message on the sender. Every sender slot sleeps for seconds
%% before emitting its single outbox entry, so a push against that slot is
%% reliably still in flight for as long as the caller lives. Returns
%% `{Sender, Receiver, MsgSlot, Opts}'.
setup_lua_push_pair() ->
    hb_process_test_vectors:init(),
    Opts = #{
        <<"priv-wallet">> => ar_wallet:new(),
        <<"cache-control">> => <<"always">>,
        <<"store">> => [hb_test_utils:test_store(hb_store_lmdb)]
    },
    Receiver = lua_push_process(receiver_module(), Opts),
    {ok, _} = hb_cache:write(Receiver, Opts),
    {ok, _} = schedule_body(Receiver, Receiver, Opts),
    Sender =
        lua_push_process(
            sender_module(hb_message:id(Receiver, all, Opts)),
            Opts
        ),
    {ok, _} = hb_cache:write(Sender, Opts),
    {ok, _} = schedule_body(Sender, Sender, Opts),
    {ok, MsgSched} =
        schedule_body(
            Sender,
            hb_message:commit(
                #{
                    <<"target">> => hb_message:id(Sender, all, Opts),
                    <<"type">> => <<"Message">>,
                    <<"action">> => <<"Fire">>
                },
                Opts
            ),
            Opts
        ),
    {ok, MsgSlot} = hb_ao:resolve(MsgSched, #{ <<"path">> => <<"slot">> }, Opts),
    {Sender, Receiver, MsgSlot, Opts}.

%% @doc POST a body onto a process's schedule.
schedule_body(Process, Body, Opts) ->
    hb_ao:resolve(
        Process,
        #{
            <<"method">> => <<"POST">>,
            <<"path">> => <<"schedule">>,
            <<"body">> => Body
        },
        Opts
    ).

%% @doc Build a signed `lua@5.3a' process that runs the given module source.
lua_push_process(Module, Opts) ->
    Address =
        hb_util:human_id(
            ar_wallet:to_address(hb_opts:get(priv_wallet, hb:wallet(), Opts))
        ),
    hb_message:commit(
        #{
            <<"device">> => <<"process@1.0">>,
            <<"type">> => <<"Process">>,
            <<"scheduler-device">> => <<"scheduler@1.0">>,
            <<"execution-device">> => <<"lua@5.3a">>,
            <<"module">> =>
                #{
                    <<"content-type">> => <<"application/lua">>,
                    <<"body">> => Module
                },
            <<"scheduler">> => Address,
            <<"scheduler-location">> => Address,
            <<"authority">> => Address,
            <<"test-random-seed">> => rand:uniform(1337)
        },
        Opts
    ).

%% @doc A Lua module whose `compute' sleeps for three seconds -- four turns of
%% `test-device@1.0/delay', which sleeps 750ms per call -- then emits a single
%% outbox entry addressed to `TargetID'. The entry names its target with a
%% lower-case `target', the shape `dev_push' dispatches on and the one the Lua
%% processes this node runs emit.
sender_module(TargetID) ->
    <<
        "function compute(process, message, opts)\n"
        "  for i = 1, 4 do\n"
        "    ao.resolve({ path = \"/~test-device@1.0/delay\" })\n"
        "  end\n"
        "  process.results = {\n"
        "    outbox = {\n"
        "      [\"1\"] = {\n"
        "        target = \"", TargetID/binary, "\",\n"
        "        action = \"Ping\"\n"
        "      }\n"
        "    }\n"
        "  }\n"
        "  return process\n"
        "end\n"
    >>.

%% @doc A Lua module whose `compute' emits no outbox, ending the push chain.
receiver_module() ->
    <<
        "function compute(process, message, opts)\n"
        "  process.results = { output = { data = \"pong\" } }\n"
        "  return process\n"
        "end\n"
    >>.

%% @doc Spin up two AOS processes -- a Sender and a Receiver with a `Reply'
%% handler for `Action = "Ping"' -- and schedule (without pushing) a single
%% Ping message on the Sender that targets the Receiver. Returns
%% `{Sender, Receiver, MsgSlot, Opts}' so individual tests can drive
%% `/push' (or `/compute&push') against the staged message and observe the
%% resulting downstream behaviour.
setup_two_process_message() ->
    hb_process_test_vectors:init(),
    Opts = #{
        <<"priv-wallet">> => ar_wallet:new(),
        <<"cache-control">> => <<"always">>,
        <<"store">> => [hb_test_utils:test_store(hb_store_lmdb)]
    },
    Sender = hb_process_test_vectors:aos_process(Opts),
    {ok, _} = hb_cache:write(Sender, Opts),
    {ok, _} =
        hb_ao:resolve(Sender, #{
            <<"method">> => <<"POST">>,
            <<"path">> => <<"schedule">>,
            <<"body">> => Sender
        }, Opts),
    Receiver = hb_process_test_vectors:aos_process(Opts),
    {ok, _} = hb_cache:write(Receiver, Opts),
    {ok, _} =
        hb_ao:resolve(Receiver, #{
            <<"method">> => <<"POST">>,
            <<"path">> => <<"schedule">>,
            <<"body">> => Receiver
        }, Opts),
    %% Install the Reply handler on the Receiver.
    {ok, _} = hb_process_test_vectors:schedule_aos_call(Receiver, reply_script(), Opts),
    %% Stage the Ping that the Sender will fire at the Receiver.
    ReceiverID = hb_message:id(Receiver, all, Opts),
    {ok, MsgSched} =
        hb_process_test_vectors:schedule_aos_call(
            Sender,
            <<
                "Send({ Target = \"", (ReceiverID)/binary,
                "\", Action = \"Ping\" })\n"
            >>,
            Opts
        ),
    {ok, MsgSlot} =
        hb_ao:resolve(MsgSched, #{ <<"path">> => <<"slot">> }, Opts),
    {Sender, Receiver, MsgSlot, Opts}.

%% @doc Poll `Pred' every 50ms until it returns `true' or `TimeoutMs'
%% elapses. Returns `true' on success, `false' on timeout.
wait_until(Pred, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    wait_until_loop(Pred, Deadline).

wait_until_loop(Pred, Deadline) ->
    case (catch Pred()) of
        true -> true;
        _ ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true -> false;
                false ->
                    timer:sleep(50),
                    wait_until_loop(Pred, Deadline)
            end
    end.

-ifdef(ENABLE_GENESIS_WASM).
%% @doc Test that a message that generates another message which resides on an
%% ANS-104 scheduler leads to `~push@1.0` re-signing the message correctly.
%% Requires `ENABLE_GENESIS_WASM' to be enabled.
test_nested_push_prompts_encoding_change() ->
    hb_process_test_vectors:init(),
    Opts = #{
        <<"priv-wallet">> => hb:wallet(),
        <<"cache-control">> => <<"always">>,
        <<"store">> => hb_opts:get(store)
    },
    ?event(debug_push, {opts, Opts}),
    Base = hb_process_test_vectors:aos_process(Opts),
    hb_cache:write(Base, Opts),
    {ok, SchedInit} =
        hb_ao:resolve(Base, #{
            <<"method">> => <<"POST">>,
            <<"path">> => <<"schedule">>,
            <<"body">> => Base
        },
        Opts
    ),
    ?event({test_setup, {base, Base}, {sched_init, SchedInit}}),
    Script = message_to_legacynet_scheduler_script(),
    ?event({script, Script}),
    {ok, Req} = hb_process_test_vectors:schedule_aos_call(Base, Script, Opts),
    ?event(push, {msg_sched_result, Req}),
    {ok, StartingMsgSlot} =
        hb_ao:resolve(Req, #{ <<"path">> => <<"slot">> }, Opts),
    ?event({starting_msg_slot, StartingMsgSlot}),
    Req2 =
        #{
            <<"path">> => <<"push">>,
            <<"slot">> => StartingMsgSlot
        },
    {ok, Res} = hb_ao:resolve(Base, Req2, Opts),
    ?event(push, {res, Res}),
    Msg = hb_message:commit(#{
        <<"path">> => <<"push">>,
        <<"method">> => <<"POST">>,
        <<"body">> =>
            hb_message:commit(
                #{
                    <<"target">> => hb_message:id(Base, all, Opts),
                    <<"action">> => <<"Ping">>
                },
                Opts
            )
    }, Opts),
    ?event(push, {base, Msg}),
    Res2 =
        hb_ao:resolve_many(
            [
                hb_message:id(Base, all, Opts),
                {as, <<"process@1.0">>, <<>>},
                Msg
            ],
            Opts
        ),
    ?assertMatch({ok, #{ <<"1">> := #{ <<"resulted-in">> := _ }}}, Res2).
-endif.
%%% Test helpers

ping_pong_script(Limit) ->
    <<
        "Handlers.add(\"Ping\",\n"
        "   function (test) return true end,\n"
        "   function(m)\n"
        "       C = tonumber(m.Count)\n"
        "       if C <= ", (integer_to_binary(Limit))/binary, " then\n"
        "           Send({ Target = ao.id, Action = \"Ping\", Count = C + 1 })\n"
        "           print(\"Ping\", C + 1)\n"
        "       else\n"
        "           print(\"Done.\")\n"
        "       end\n"
        "   end\n"
        ")\n"
        "Send({ Target = ao.id, Action = \"Ping\", Count = 1 })\n"
    >>.

reply_script() ->
    <<
        """
        Handlers.add("Reply",
           { Action = "Ping" },
           function(m)
               print("Replying to...")
               print(m.From)
               Send({ Target = m.From, Action = "Reply", Message = "Pong!" })
               print("Done.")
           end
        )
        """
    >>.
reply_script(OtherProcessID) ->
    <<
        (reply_script())/binary, "\n",
        "Send({ Target = \"", (OtherProcessID)/binary, "\", Action = \"Ping\" })\n"
    >>.

message_to_legacynet_scheduler_script() ->
    <<
        """
        Handlers.add("Ping",
           { Action = "Ping" },
           function(m)
               print("Pinging...")
               print(m.From)
               Send({
                    Target = "QQiMcAge5ZtxcUV7ruxpi16KYRE8UBP0GAAqCIJPXz0",
                    Action = "Ping"
                })
               print("Done.")
           end
        )
        """
    >>.

oracle_script() ->
    <<
        """
        Handlers.add("Oracle",
            function(m)
                return true
            end,
            function(m)
                print(m.Body)
            end
        )
        Send({
            target = ao.id,
            resolve = "/~relay@1.0/call",
            ["relay-path"] = "https://arweave.net"
        })
        
        """
    >>.

%% @doc Outbox targets are looked up locally first, and a target that no store
%% has -- a wallet -- is remembered, so it costs one remote walk per TTL rather
%% than one per message. A process that exists only remotely is still found.
read_target_skips_remote_for_known_misses_test() ->
    application:ensure_all_started(hb),
    RemoteDir =
        <<"cache-TEST/push-remote-",
            (hb_util:encode(crypto:strong_rand_bytes(8)))/binary>>,
    Remote = #{
        <<"store-module">> => hb_store_fs,
        <<"name">> => RemoteDir,
        <<"scope">> => remote
    },
    Local = hb_test_utils:test_store(hb_store_lmdb),
    Opts = #{
        <<"store">> => [Local, Remote],
        <<"priv-wallet">> => ar_wallet:new(),
        <<"push-target-miss-ttl">> => 300
    },
    LocalMsg = #{ <<"type">> => <<"Process">>, <<"n">> => <<"local">> },
    {ok, LocalID} = hb_cache:write(LocalMsg, Opts#{ <<"store">> => [Local] }),
    RemoteMsg = #{ <<"type">> => <<"Process">>, <<"n">> => <<"remote">> },
    {ok, RemoteID} = hb_cache:write(RemoteMsg, Opts#{ <<"store">> => [Remote] }),
    Wallet = hb_util:human_id(crypto:strong_rand_bytes(32)),
    RemoteReads =
        fun(Fun) ->
            erlang:trace_pattern({hb_store_fs, '_', '_'}, true, [call_count]),
            Fun(),
            Count =
                lists:sum(
                    [
                        case erlang:trace_info({hb_store_fs, F, A}, call_count) of
                            {call_count, C} when is_integer(C) -> C;
                            _ -> 0
                        end
                    ||
                        {F, A} <- [{read, 3}, {type, 3}, {resolve, 3}, {list, 3}]
                    ]
                ),
            erlang:trace_pattern({hb_store_fs, '_', '_'}, false, [call_count]),
            Count
        end,
    % A process this node has is found without touching a remote store.
    ?assertEqual(0,
        RemoteReads(fun() -> {ok, _} = read_target(LocalID, Opts) end)),
    % A wallet walks the remote stores once...
    ?assert(
        RemoteReads(fun() -> {error, not_found} = read_target(Wallet, Opts) end)
            > 0
    ),
    % ...and then not again within the TTL. The remembered miss is reported as
    % such, never as a definitive `not_found': the gateway store cannot tell a
    % missing ID from a failed request, so it may not be what drops a message.
    ?assertEqual(0,
        RemoteReads(
            fun() -> {cached_miss, _} = read_target(Wallet, Opts) end
        )
    ),
    % After the TTL, a miss is retried.
    ?assert(
        RemoteReads(
            fun() ->
                {error, not_found} =
                    read_target(
                        Wallet,
                        Opts#{ <<"push-target-miss-ttl">> => 0 }
                    )
            end
        ) > 0
    ),
    % A process held only remotely is still delivered to.
    ?assertMatch({ok, #{}}, read_target(RemoteID, Opts)).

%% @doc A computed result's outbox shares its map with the result's own
%% metadata. Only the messages the process emitted may be pushed: iterating the
%% metadata delivers messages that were never sent, and hands the push path a
%% commitment map in place of a message.
outbox_entries_excludes_result_metadata_test() ->
    Outbox =
        #{
            <<"mint">> =>
                #{
                    <<"target">> => <<"target-process-id">>,
                    <<"action">> => <<"Mint">>
                },
            <<"commitments">> =>
                #{
                    <<"commitment-id">> =>
                        #{ <<"commitment-device">> => <<"httpsig@1.0">> }
                },
            <<"ao-types">> => <<"quantity=\"integer\"">>,
            <<"device">> => <<"message@1.0">>,
            <<"hashpath">> => <<"a-hashpath">>
        },
    ?assertEqual(
        [<<"mint">>],
        lists:sort(maps:keys(outbox_entries(Outbox, #{})))
    ).

%% @doc The names a process gives its outbox entries are normalized, but the
%% entries themselves must be delivered as they were emitted. A signed entry
%% carries its own `commitments', keyed by case-sensitive base64url commitment
%% IDs; rewriting their case leaves the entry carrying signatures that no longer
%% match the IDs that name them.
outbox_entries_preserve_entry_commitment_ids_test() ->
    CommitmentID = <<"aXnLbjnJtgIsjZXS3hSqNLaj2okwHy3N7A1ZpogrnVI">>,
    Outbox =
        #{
            <<"Mint">> =>
                #{
                    <<"target">> => <<"target-process-id">>,
                    <<"commitments">> =>
                        #{
                            CommitmentID =>
                                #{ <<"type">> => <<"rsa-pss-sha512">> }
                        }
                }
        },
    Entries = outbox_entries(Outbox, #{}),
    ?assertEqual([<<"mint">>], maps:keys(Entries)),
    Entry = maps:get(<<"mint">>, Entries),
    ?assertEqual(
        [CommitmentID],
        maps:keys(maps:get(<<"commitments">>, Entry))
    ).

%% @doc A legacy AO process names its outbox entry's target `Target'. The push
%% walk dispatches on a lower-case `target' and `hb_ao:get/4' lowers only the
%% key it is given, so an entry that keeps the capitalised spelling is answered
%% with a 404 and never delivered. By default an entry's keys are lower-cased
%% as the reference device does; with `push-preserve-key-case' the entry keeps
%% its payload's case and only `target' is renamed. Either way its
%% `commitments' keep their case-sensitive base64url IDs, and a name that is
%% not valid UTF-8 does not abort the outbox.
outbox_entries_normalizes_legacy_target_key_test() ->
    CommitmentID = <<"aXnLbjnJtgIsjZXS3hSqNLaj2okwHy3N7A1ZpogrnVI">>,
    Outbox =
        #{
            <<"1">> =>
                #{
                    <<"Target">> => <<"target-process-id">>,
                    <<"Action">> => <<"Ping">>,
                    <<"Ta", 16#FF, "g">> => <<"raw">>,
                    <<"commitments">> =>
                        #{
                            CommitmentID =>
                                #{ <<"type">> => <<"rsa-pss-sha512">> }
                        }
                }
        },
    Entry = maps:get(<<"1">>, outbox_entries(Outbox, #{})),
    ?assertMatch(#{ <<"target">> := <<"target-process-id">> }, Entry),
    ?assertNot(maps:is_key(<<"Target">>, Entry)),
    ?assertEqual(<<"Ping">>, maps:get(<<"action">>, Entry)),
    ?assertEqual(
        [CommitmentID],
        maps:keys(maps:get(<<"commitments">>, Entry))
    ),
    ?assertEqual(4, map_size(Entry)),
    outbox_entries_preserve_key_case().

outbox_entries_preserve_key_case() ->
    CommitmentID = <<"aXnLbjnJtgIsjZXS3hSqNLaj2okwHy3N7A1ZpogrnVI">>,
    Outbox =
        #{
            <<"1">> =>
                #{
                    <<"Target">> => <<"target-process-id">>,
                    <<"Action">> => <<"Ping">>,
                    <<"Ta", 16#FF, "g">> => <<"raw">>,
                    <<"commitments">> =>
                        #{
                            CommitmentID =>
                                #{ <<"type">> => <<"rsa-pss-sha512">> }
                        }
                }
        },
    Entry =
        maps:get(
            <<"1">>,
            outbox_entries(Outbox, #{ <<"push-preserve-key-case">> => true })
        ),
    ?assertMatch(#{ <<"target">> := <<"target-process-id">> }, Entry),
    ?assertNot(maps:is_key(<<"Target">>, Entry)),
    ?assertEqual(<<"Ping">>, maps:get(<<"Action">>, Entry)),
    ?assertEqual(<<"raw">>, maps:get(<<"Ta", 16#FF, "g">>, Entry)),
    ?assertEqual(
        [CommitmentID],
        maps:keys(maps:get(<<"commitments">>, Entry))
    ).

%%% Regression tests for the push-delivery fixes. They use only the module's
%%% long-standing internals, so the same file runs against the base revision.

%% @doc A pushed entry reaches its recipient with lower-cased keys, as with the
%% reference push device: the recipient reads `action' and `quantity', not
%% `Action' and `Quantity'.
push_lowercases_payload_keys_test_() ->
    {timeout, 180, fun test_push_lowercases_payload_keys/0}.

test_push_lowercases_payload_keys() ->
    Opts = regress_opts(#{}),
    {Sender, Receiver, MsgSlot} =
        regress_pair(
            fun(RecvID) ->
                <<
                    "function compute(process, message, opts)\n"
                    "  process.results = { outbox = { [\"1\"] = {\n"
                    "    target = \"", RecvID/binary, "\",\n"
                    "    Action = \"Ping\", Quantity = \"5\", lower = \"x\" } } }\n"
                    "  return process\n"
                    "end\n"
                >>
            end,
            Opts
        ),
    {ok, _} =
        hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => MsgSlot }, Opts),
    RecvID = hb_message:id(Receiver, all, Opts),
    {ok, A} =
        hb_cache:read(
            hb_path:to_binary(
                [<<"~scheduler@1.0">>, <<"assignments">>, RecvID, <<"1">>]
            ),
            Opts
        ),
    Body =
        hb_cache:ensure_all_loaded(
            hb_ao:get(<<"body">>, hb_cache:ensure_all_loaded(A, Opts), Opts),
            Opts
        ),
    Keys = maps:keys(Body),
    ?assert(lists:member(<<"action">>, Keys)),
    ?assert(lists:member(<<"quantity">>, Keys)),
    ?assertNot(lists:member(<<"Action">>, Keys)),
    ?assertNot(lists:member(<<"Quantity">>, Keys)).

%% @doc Nested payload maps are lower-cased too, but a signed sub-message --
%% one that carries `commitments' -- is delivered exactly as it was signed.
outbox_entries_lowercase_nested_but_not_signed_test() ->
    Signed =
        hb_message:commit(
            #{ <<"Inner">> => <<"v">> },
            #{ <<"priv-wallet">> => ar_wallet:new() }
        ),
    Outbox =
        #{
            <<"1">> =>
                #{
                    <<"target">> => <<"t">>,
                    <<"Nested">> => #{ <<"Deep-Key">> => <<"1">> },
                    <<"Signed">> => Signed
                }
        },
    Entry = maps:get(<<"1">>, outbox_entries(Outbox, #{})),
    ?assertEqual(#{ <<"deep-key">> => <<"1">> }, maps:get(<<"nested">>, Entry)),
    ?assertEqual(Signed, maps:get(<<"signed">>, Entry)).

%% @doc A chain longer than the detached depth bound is followed to its end.
%% The process pings itself 40 times; a detached push bounded at 16 hops used
%% to stop near slot 18 and nothing ever drove the rest.
push_follows_chain_past_depth_bound_test_() ->
    {timeout, 300, fun test_push_follows_chain_past_depth_bound/0}.

test_push_follows_chain_past_depth_bound() ->
    Opts = regress_opts(#{}),
    SelfMod =
        <<
            "function compute(process, message, opts)\n"
            "  local b = message.body or message\n"
            "  local t = message.target or b.target\n"
            "  local n = tonumber(b.n or 0) + 1\n"
            "  if n <= 40 then\n"
            "    process.results = { outbox = { [\"1\"] = {\n"
            "      target = t, action = \"Ping\", n = tostring(n) } } }\n"
            "  else\n"
            "    process.results = { output = { data = \"done\" } }\n"
            "  end\n"
            "  return process\n"
            "end\n"
        >>,
    P = lua_push_process(SelfMod, Opts),
    {ok, _} = hb_cache:write(P, Opts),
    {ok, _} = schedule_body(P, P, Opts),
    PID = hb_message:id(P, all, Opts),
    {ok, MsgSched} =
        schedule_body(P,
            hb_message:commit(#{ <<"target">> => PID,
                <<"type">> => <<"Message">>, <<"action">> => <<"Fire">> }, Opts),
            Opts),
    {ok, MsgSlot} = hb_ao:resolve(MsgSched, #{ <<"path">> => <<"slot">> }, Opts),
    {ok, _} = hb_ao:resolve(P, #{ <<"path">> => <<"push">>, <<"slot">> => MsgSlot }, Opts),
    Reached =
        wait_until(
            fun() ->
                {ok, Cur} = hb_ao:resolve(P, #{ <<"path">> => <<"slot/current">> }, Opts),
                Cur >= MsgSlot + 40
            end,
            240000
        ),
    {ok, Final} = hb_ao:resolve(P, #{ <<"path">> => <<"slot/current">> }, Opts),
    ?event(debug_push, {chain_reached, Final}),
    ?assert(Reached),
    % ...and stops where the program stops: nothing is delivered twice.
    timer:sleep(2000),
    {ok, After} = hb_ao:resolve(P, #{ <<"path">> => <<"slot/current">> }, Opts),
    ?assertEqual(MsgSlot + 40, After).

%% @doc Under `push-durable', pushing a slot again -- a client retrying, or a
%% resumed push -- does not schedule its entries a second time, whether the
%% first push has finished or is still in flight.
push_durable_retry_does_not_duplicate_test_() ->
    {timeout, 240, fun test_push_durable_retry_does_not_duplicate/0}.

test_push_durable_retry_does_not_duplicate() ->
    Opts = regress_opts(#{ <<"push-durable">> => true }),
    {Sender, Receiver, MsgSlot} =
        regress_pair(fun simple_sender_module/1, Opts),
    {ok, R0} = hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
    Push =
        fun() ->
            hb_ao:resolve(
                Sender,
                #{ <<"path">> => <<"push">>, <<"slot">> => MsgSlot },
                Opts
            )
        end,
    {ok, _} = Push(),
    {ok, _} = Push(),
    {ok, R1} = hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
    ?assertEqual(R0 + 1, R1),
    % Two concurrent pushes of one slot whose compute takes seconds.
    {SlowSender, SlowReceiver, SlowSlot, SlowOpts} =
        setup_lua_push_pair_with(Opts),
    {ok, S0} =
        hb_ao:resolve(SlowReceiver, #{ <<"path">> => <<"slot/current">> }, SlowOpts),
    Self = self(),
    [
        spawn(fun() ->
            Self ! {pushed,
                hb_ao:resolve(
                    SlowSender,
                    #{ <<"path">> => <<"push">>, <<"slot">> => SlowSlot },
                    SlowOpts
                )}
        end)
    ||
        _ <- [1, 2]
    ],
    [ receive {pushed, _} -> ok after 120000 -> erlang:error(push_timeout) end
    || _ <- [1, 2] ],
    timer:sleep(500),
    {ok, S1} =
        hb_ao:resolve(SlowReceiver, #{ <<"path">> => <<"slot/current">> }, SlowOpts),
    ?assertEqual(S0 + 1, S1).

%% @doc A target that is remembered as missing is not dropped: once the miss
%% expires the entry is retried and delivered. The receiver does not exist
%% when it is missed, nor when the push runs; it appears a moment later.
push_cached_miss_is_retried_test_() ->
    {timeout, 180, fun test_push_cached_miss_is_retried/0}.

test_push_cached_miss_is_retried() ->
    Opts = regress_opts(#{ <<"push-target-miss-ttl">> => 5 }),
    Receiver = lua_push_process(receiver_module(), Opts),
    RecvID = hb_message:id(Receiver, all, Opts),
    Sender = lua_push_process(simple_sender_module(RecvID), Opts),
    {ok, _} = hb_cache:write(Sender, Opts),
    {ok, _} = schedule_body(Sender, Sender, Opts),
    {ok, MsgSched} =
        schedule_body(Sender,
            hb_message:commit(#{ <<"target">> => hb_message:id(Sender, all, Opts),
                <<"type">> => <<"Message">>, <<"action">> => <<"Fire">> }, Opts),
            Opts),
    {ok, MsgSlot} = hb_ao:resolve(MsgSched, #{ <<"path">> => <<"slot">> }, Opts),
    {ok, _} =
        hb_ao:resolve(Sender, #{ <<"path">> => <<"compute">>, <<"slot">> => MsgSlot }, Opts),
    % The receiver is missed while it does not exist...
    _ = read_target(RecvID, Opts),
    {ok, _} =
        hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => MsgSlot }, Opts),
    % ...and is spawned after the push has run.
    {ok, _} = hb_cache:write(Receiver, Opts),
    {ok, _} = schedule_body(Receiver, Receiver, Opts),
    ?assert(
        wait_until(
            fun() ->
                {ok, Cur} =
                    hb_ao:resolve(
                        Receiver,
                        #{ <<"path">> => <<"slot/current">> },
                        Opts
                    ),
                Cur >= 1
            end,
            30000
        )
    ).

%% @doc Push cost of a slot with `N' outbox entries, scheduled only
%% (`max-depth' 0), so that the figure is delivery bookkeeping and not the
%% receiver's compute. Prints the median of `Runs' pushes.
push_outbox_bench_test_() ->
    {timeout, 600, fun() ->
        [
            push_outbox_bench(Label, Extra, 50, 7)
        ||
            {Label, Extra} <-
                [
                    {plain, #{}},
                    {durable, #{ <<"push-durable">> => true }}
                ]
        ]
    end}.

push_outbox_bench(Label, Extra, N, Runs) ->
    Opts = regress_opts(Extra),
    Fan =
        fun(RecvID) ->
            <<
                "function compute(process, message, opts)\n"
                "  local out = {}\n"
                "  for i = 1, ", (integer_to_binary(N))/binary, " do\n"
                "    out[tostring(i)] = { target = \"", RecvID/binary, "\",\n"
                "      action = \"Ping\", i = tostring(i) }\n"
                "  end\n"
                "  process.results = { outbox = out }\n"
                "  return process\n"
                "end\n"
            >>
        end,
    {Sender, _Receiver, _} = regress_pair(Fan, Opts),
    SenderID = hb_message:id(Sender, all, Opts),
    Times =
        [
            begin
                {ok, Sched} =
                    schedule_body(Sender,
                        hb_message:commit(#{ <<"target">> => SenderID,
                            <<"type">> => <<"Message">>,
                            <<"action">> => <<"Fire">> }, Opts),
                        Opts),
                {ok, Slot} = hb_ao:resolve(Sched, #{ <<"path">> => <<"slot">> }, Opts),
                % Compute first, so that only the delivery is timed.
                {ok, _} =
                    hb_ao:resolve(
                        Sender,
                        #{ <<"path">> => <<"compute">>, <<"slot">> => Slot },
                        Opts
                    ),
                {T, {ok, _}} =
                    timer:tc(fun() ->
                        hb_ao:resolve(
                            Sender,
                            #{
                                <<"path">> => <<"push">>,
                                <<"slot">> => Slot,
                                <<"max-depth">> => 0
                            },
                            Opts
                        )
                    end),
                T div 1000
            end
        ||
            _ <- lists:seq(1, Runs)
        ],
    Sorted = lists:sort(Times),
    io:format(standard_error,
        "~nPUSH-BENCH ~p entries=~p runs=~p median_ms=~p all_ms=~p~n",
        [Label, N, Runs, lists:nth((Runs + 1) div 2, Sorted), Times]).

regress_opts(Extra) ->
    hb_process_test_vectors:init(),
    maps:merge(
        #{
            <<"priv-wallet">> => ar_wallet:new(),
            <<"cache-control">> => <<"always">>,
            <<"store">> => [hb_test_utils:test_store(hb_store_lmdb)]
        },
        Extra
    ).

%% @doc A receiver and a sender built from `SenderModFun(ReceiverID)', with one
%% message staged -- not pushed -- on the sender.
regress_pair(SenderModFun, Opts) ->
    Receiver = lua_push_process(receiver_module(), Opts),
    {ok, _} = hb_cache:write(Receiver, Opts),
    {ok, _} = schedule_body(Receiver, Receiver, Opts),
    RecvID = hb_message:id(Receiver, all, Opts),
    Sender = lua_push_process(SenderModFun(RecvID), Opts),
    {ok, _} = hb_cache:write(Sender, Opts),
    {ok, _} = schedule_body(Sender, Sender, Opts),
    {ok, MsgSched} =
        schedule_body(Sender,
            hb_message:commit(#{ <<"target">> => hb_message:id(Sender, all, Opts),
                <<"type">> => <<"Message">>, <<"action">> => <<"Fire">> }, Opts),
            Opts),
    {ok, MsgSlot} = hb_ao:resolve(MsgSched, #{ <<"path">> => <<"slot">> }, Opts),
    {Sender, Receiver, MsgSlot}.

setup_lua_push_pair_with(Opts) ->
    {Sender, Receiver, MsgSlot} = regress_pair(fun sender_module/1, Opts),
    {Sender, Receiver, MsgSlot, Opts}.

simple_sender_module(RecvID) ->
    <<
        "function compute(process, message, opts)\n"
        "  process.results = { outbox = { [\"1\"] = {\n"
        "    target = \"", RecvID/binary, "\", action = \"Ping\" } } }\n"
        "  return process\n"
        "end\n"
    >>.

%% @doc Under `push-durable' a slot whose push never completed -- the node
%% stopped mid-delivery -- is found in the journal and pushed on resume, and a
%% slot marked done is not pushed again.
push_durable_resume_from_journal_test_() ->
    {timeout, 120, fun test_push_durable_resume_from_journal/0}.

test_push_durable_resume_from_journal() ->
    Opts = regress_opts(#{ <<"push-durable">> => true }),
    {Sender, Receiver, MsgSlot} =
        regress_pair(fun simple_sender_module/1, Opts),
    SenderID = hb_message:id(Sender, all, Opts),
    {ok, R0} = hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
    % What a push leaves behind when the node stops right after it found the
    % slot's outbox: the slot is journaled, nothing is delivered.
    ok = journal_slot(SenderID, MsgSlot, Opts),
    ?assertEqual(1, resume_journal(Opts)),
    ?assert(
        wait_until(
            fun() ->
                {ok, R} =
                    hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
                R == R0 + 1
            end,
            30000
        )
    ),
    ?assert(wait_until(fun() -> is_slot_done(SenderID, MsgSlot, Opts) end, 30000)),
    % Both the source slot and the receiver's new slot are now done, so a
    % second resume pushes nothing, and nothing is delivered twice.
    ?assert(
        wait_until(fun() -> resume_journal(Opts) == 0 end, 30000)
    ),
    {ok, R1} = hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
    ?assertEqual(R0 + 1, R1).

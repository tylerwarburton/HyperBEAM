
%%% @doc A long-lived process worker that keeps state in memory between
%%% calls. Implements the interface of `hb_ao' to receive and respond 
%%% to computation requests regarding a process as a singleton.
-module(dev_process_worker).
-export([server/3, stop/1, group/3, await/5, notify_compute/4]).
-export([compute_on_schedule/4]).
-export([checkpoint_taken/0]).
-include_lib("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Forced collections of a worker's heap, at quiet points only: after the
%% request that handed off a checkpoint has been answered, and once when the
%% worker has then been idle for `process-worker-gc-idle-ms'. See `force_gc/2'.
-define(DEFAULT_GC_POLICY, <<"checkpoint+idle">>).
-define(DEFAULT_GC_IDLE_MS, 5000).
-define(GC_CHECKPOINT_DUE, {?MODULE, gc_checkpoint_due}).
%% A compute-on-schedule catch-up that has itself computed (as the leader for
%% a process with no worker) holds a dead copy of the state; above this many
%% words of heap it is collected before it waits for the next target.
-define(CATCH_UP_GC_WORDS, 1_000_000).
%% How long a catch-up waits on a live worker before it lets the wake go (a
%% later assignment wakes it again).
-define(CATCH_UP_WAIT_MS, 300_000).

%% Request keys a delegated `compute' carries to the worker's resolution.
-define(CARRIED_KEYS,
    [<<"push">>, <<"max-depth">>, <<"result-depth">>, <<"async">>, <<"init">>]
).

%% @doc Return a group name for a request. Cached compute reads run
%% ungrouped; uncached compute work groups by process ID; everything
%% else uses the default grouper, except a `now/<key>' key read
%% (`dev_process:now_key_read/2'): it is cheap, and its result holds only its
%% own key, so it must not be shared with a reader of another key -- which the
%% default grouper, blind to the private key it is named by, would do under
%% `await-inprogress: true'.
group(Base, undefined, Opts) ->
    hb_persistent:default_grouper(Base, undefined, Opts);
group(Base, Req, Opts) ->
    ProcessWorkers = hb_opts:get(<<"process-workers">>, false, Opts),
    IsCompute = hb_path:matches(<<"compute">>, hb_path:hd(Req, Opts)),
    case ProcessWorkers andalso IsCompute of
        true ->
            compute_group(Base, Req, Opts);
        false ->
            case dev_process:now_key_read(Req, Opts) of
                {true, _} -> ungrouped_exec;
                false -> hb_persistent:default_grouper(Base, Req, Opts)
            end
    end.

%% @doc Decide which group to enrol a `compute' request into. Cache-hit
%% reads bypass the per-process queue via `ungrouped_exec'; everything
%% else is serialised through the worker keyed on the process ID.
compute_group(Base, Req, Opts) ->
    case dev_process:request_slot(Req, slot_read_opts(Opts)) of
        {invalid, _} ->
            % A slot that cannot name an assignment is answered with an error
            % by `dev_process:compute/3' in the requester itself. It never
            % joins the process's queue, so it can never reach -- or harm --
            % the worker holding the live state.
            ungrouped_exec;
        TargetSlot ->
            compute_group(Base, Req, TargetSlot, Opts)
    end.

compute_group(Base, Req, TargetSlot, Opts) ->
    ProcID = process_to_group_name(Base, Opts),
    case compute_cached(ProcID, TargetSlot, Opts) of
        true ->
            ?event(worker,
                {compute_cache_hit_bypassing_queue,
                    {proc_id, ProcID},
                    {req, Req}
                },
                Opts
            ),
            ungrouped_exec;
        false ->
            ProcID
    end.

%% @doc Return `true' if the requested compute result is already cached.
compute_cached(ProcID, not_found, Opts) ->
    case dev_process_cache:latest(ProcID, Opts) of
        {ok, _Slot, _Msg} -> true;
        _ -> false
    end;
compute_cached(ProcID, Slot, Opts) ->
    % `dev_process:request_slot/2' has already unwrapped and validated the
    % slot, so it is an integer of at least -1 here. The `catch' stays as a
    % backstop: a cache read that fails means "not cached", and an uncomputed
    % slot is a queue, not a fault. (local patch over upstream edge.)
    case (catch dev_process_cache:read(ProcID, Slot, Opts)) of
        {ok, _Msg} -> true;
        _ -> false
    end.

process_to_group_name(Base, Opts) ->
    Initialized = lib_process:ensure_process_key(Base, Opts),
    ProcMsg =
        hb_ao:get(<<"process">>, Initialized, Opts#{ <<"hashpath">> => ignore }),
    ID = hb_message:id(ProcMsg, all),
    ?event({process_to_group_name, {id, ID}, {base, Base}}),
    hb_util:human_id(ID).

%% @doc Spawn a new worker process. This is called after the end of the first
%% execution of `hb_ao:resolve/3', so the state we are given is the
%% already current.
%%
%% The state arrives as the spawn's copy, compact, so no idle collection is
%% due until the worker has computed something (`gc_policy/1').
server(GroupName, RawBase, Opts) ->
    server(GroupName, RawBase, Opts, false).

server(GroupName, RawBase, Opts, IdleGCDue) ->
    Base = live_base(RawBase),
    ServerOpts = Opts#{
        <<"await-inprogress">> => false,
        <<"spawn-worker">> => false,
        <<"process-workers">> => false,
        % The worker computes slots; `dev_process_cache' already stores each
        % one durably (a delta, or a checkpoint). A listener's options -- an
        % HTTP request's `cache-control: always' -- would otherwise make
        % result caching serialize, hash and write the whole process state
        % (and read its hashpath back) on every slot, inside the one process
        % that every write to this process queues behind.
        <<"cache-control">> => [<<"no-store">>, <<"no-cache">>]
    },
    % The maximum amount of time the worker will wait for a request before
    % checking the cache for a snapshot. Default: 5 minutes.
    Timeout = hb_opts:get(process_worker_max_idle, 300_000, Opts),
    ?event(worker, {waiting_for_req, {group, GroupName}}),
    case next_request(GroupName, Timeout, IdleGCDue, Opts) of
        {resolve, Listener, GroupName, Req, ListenerOpts} ->
            TargetSlot = read_slot(Req, not_found, Opts),
            ?event(worker,
                {work_received,
                    {group, GroupName},
                    {slot, TargetSlot},
                    {listener, Listener}
                }
            ),
            Res =
                case catch_up_covered(Req, TargetSlot, Base) of
                    true -> {ok, Base};
                    false ->
                        execute(
                            Base,
                            Req,
                            TargetSlot,
                            hb_maps:merge(ListenerOpts, ServerOpts, Opts)
                        )
                end,
            ?event(worker, {work_done, {group, GroupName}, {req, Req}, {res, Res}}),
            send_notification(Listener, GroupName, TargetSlot, Res, Req),
            % The listener has its answer: a quiet point. A forced collection
            % after a checkpoint leaves nothing for the idle one to do until
            % the worker computes again.
            Collected = checkpoint_gc(Opts),
            server(GroupName, next_base(Base, Res), Opts, not Collected);
        stop ->
            ?event(worker, {stopping, {group, GroupName}, {base, Base}}),
            exit(normal);
        timeout ->
            % We have hit the in-memory persistence timeout. Generate a snapshot
            % of the current process state and ensure it is cached.
            hb_ao:resolve(
                Base,
                <<"snapshot">>,
                ServerOpts#{ <<"cache-control">> => [<<"store">>] }
            ),
            % Return the current process state.
            {ok, Base}
    end.

%% @doc Wait up to `Timeout' for the next request or `stop', and return it, or
%% `timeout'. With an idle collection due, a wait that outlasts
%% `process-worker-gc-idle-ms' runs it once and waits out the rest of
%% `Timeout' without it: a worker that stays idle is collected once, not once
%% per interval.
next_request(GroupName, Timeout, IdleGCDue, Opts) ->
    IdleMs =
        case IdleGCDue andalso idle_gc_ms(Opts) of
            Ms when is_integer(Ms), Ms < Timeout -> Ms;
            _ -> none
        end,
    receive
        {resolve, _Listener, GroupName, _Req, _ListenerOpts} = Msg -> Msg;
        stop -> stop
    after wait_ms(IdleMs, Timeout) ->
        case IdleMs of
            none -> timeout;
            _ ->
                force_gc(idle, Opts),
                next_request(GroupName, remaining(Timeout, IdleMs), false, Opts)
        end
    end.

wait_ms(none, Timeout) -> Timeout;
wait_ms(IdleMs, _Timeout) -> IdleMs.

remaining(infinity, _) -> infinity;
remaining(Timeout, Elapsed) -> max(0, Timeout - Elapsed).

%%% Forced collections of the worker's heap (`process-worker-gc').

%% @doc Note that the process running this call has just handed a checkpoint
%% to the store, or to its asynchronous writer. Called from `dev_process' on
%% the compute path, so it only sets a flag: the collection itself waits for
%% the worker's next quiet point (`checkpoint_gc/1'). In any process other
%% than a worker the flag is never read.
checkpoint_taken() ->
    put(?GC_CHECKPOINT_DUE, true),
    ok.

%% @doc After a request has been answered: run the checkpoint collection if
%% the request handed off a checkpoint and the policy asks for one. A request
%% that crosses several checkpoints (a long catch-up) is collected once.
%% Returns whether a collection ran.
checkpoint_gc(Opts) ->
    case erase(?GC_CHECKPOINT_DUE) of
        true ->
            case gc_policy(Opts) of
                #{ checkpoint := true } ->
                    force_gc(checkpoint, Opts),
                    true;
                _ -> false
            end;
        _ -> false
    end.

%% @doc The idle delay, or `false' when the policy has no idle collection.
idle_gc_ms(Opts) ->
    case gc_policy(Opts) of
        #{ idle := true } ->
            case hb_opts:get(<<"process-worker-gc-idle-ms">>, ?DEFAULT_GC_IDLE_MS, Opts) of
                Ms when is_integer(Ms), Ms >= 0 -> Ms;
                Bin when is_binary(Bin) ->
                    try binary_to_integer(Bin)
                    catch _:_ -> ?DEFAULT_GC_IDLE_MS
                    end;
                _ -> ?DEFAULT_GC_IDLE_MS
            end;
        _ -> false
    end.

%% @doc Parse `process-worker-gc': `off', `checkpoint', `idle' or
%% `checkpoint+idle' (an atom or binary), or a list of `checkpoint' and
%% `idle'. Anything else is the default.
gc_policy(Opts) ->
    parse_gc_policy(hb_opts:get(<<"process-worker-gc">>, ?DEFAULT_GC_POLICY, Opts)).

parse_gc_policy(L) when is_list(L), L =/= [] ->
    case lists:all(fun(X) -> is_binary(X) orelse is_atom(X) end, L) of
        true ->
            lists:foldl(
                fun(Mode, Acc) ->
                    maps:merge_with(
                        fun(_, A, B) -> A orelse B end,
                        Acc,
                        parse_gc_policy(Mode)
                    )
                end,
                #{ checkpoint => false, idle => false },
                L
            );
        false -> parse_gc_policy(list_to_binary(L))
    end;
parse_gc_policy(A) when is_atom(A) -> parse_gc_policy(atom_to_binary(A));
parse_gc_policy(<<"off">>) -> #{ checkpoint => false, idle => false };
parse_gc_policy(<<"false">>) -> #{ checkpoint => false, idle => false };
parse_gc_policy(<<"checkpoint">>) -> #{ checkpoint => true, idle => false };
parse_gc_policy(<<"idle">>) -> #{ checkpoint => false, idle => true };
parse_gc_policy(<<"checkpoint+idle">>) -> #{ checkpoint => true, idle => true };
parse_gc_policy(_) -> parse_gc_policy(?DEFAULT_GC_POLICY).

%% @doc One full collection of the calling worker, then one minor collection.
%% The full sweep leaves every live word in a young heap sized to fit it, with
%% the high-water mark at its top, so the next minor collection -- otherwise
%% the first one inside the next request -- would copy the whole state again
%% to promote it. Running that minor here moves the state to the old heap at
%% the quiet point and leaves a small young heap behind.
%%
%% Not `erlang:hibernate/3': it compacts no further than this for a worker
%% whose state is nearly all live, but the request that wakes it pays to grow
%% the heap back and promote the state, on the latency path; and the worker
%% loop would have to be restarted from a continuation.
force_gc(Reason, Opts) ->
    case hb_opts:get(<<"process-worker-gc-hook">>, undefined, Opts) of
        Hook when is_function(Hook, 1) -> Hook(Reason);
        _ -> ok
    end,
    {memory, Before} = erlang:process_info(self(), memory),
    Start = erlang:monotonic_time(microsecond),
    erlang:garbage_collect(),
    erlang:garbage_collect(self(), [{type, minor}]),
    Done = erlang:monotonic_time(microsecond),
    {memory, After} = erlang:process_info(self(), memory),
    ?event(worker_gc,
        {forced_gc,
            {reason, Reason},
            {ms, (Done - Start) div 1000},
            {memory_before, Before},
            {memory_after, After}
        }
    ),
    ok.

%% @doc Execute one request against the worker's live state.
%%
%% The request is rebuilt around the slot `read_slot/3' found, so the worker
%% computes the slot its waiter is matching on, whichever key named it. The
%% keys that steer `dev_process' beyond the slot -- `push' and the options the
%% push it triggers inherits, and `init' -- are carried across, so a delegated
%% compute behaves as one run in the requester would.
%%
%% Nothing a request does may take the worker down: its death loses the live
%% state, and the next request pays a snapshot restore plus a replay of every
%% slot since. A request that raises is answered with an error and the worker
%% keeps the state it had before the request (`next_base/2'), as it already
%% does for a request that returns one.
execute(Base, Req, TargetSlot, ExecOpts) ->
    Carried = hb_maps:with(?CARRIED_KEYS, Req, ExecOpts),
    WorkReq =
        case TargetSlot of
            not_found -> Carried#{ <<"path">> => <<"compute">> };
            {invalid, Raw} ->
                Carried#{ <<"path">> => <<"compute">>, <<"slot">> => Raw };
            Slot -> Carried#{ <<"path">> => <<"compute">>, <<"slot">> => Slot }
        end,
    try hb_ao:resolve(Base, WorkReq, ExecOpts)
    catch
        throw:{error, Reason} ->
            ?event(worker, {request_failed, {slot, TargetSlot}, {error, Reason}}),
            {error, Reason};
        Class:Reason:Stacktrace ->
            ?event(worker,
                {request_crashed,
                    {slot, TargetSlot},
                    {class, Class},
                    {reason, Reason},
                    {stacktrace, Stacktrace}
                }
            ),
            {error,
                #{
                    <<"status">> => 500,
                    <<"body">> => <<"Process worker failed to compute request.">>
                }
            }
    end.

%% @doc Choose the state the worker continues from after a request. A request
%% for a slot that is already cached is answered from the process cache, and
%% that answer is public state only: continuing from it would execute the next
%% slot against a freshly initialized VM, silently resetting every value the
%% execution device keeps outside the published message.
next_base(Base, {ok, NewBase}) when is_map(NewBase) ->
    case dev_process:is_cached_state(NewBase) of
        true -> Base;
        false -> NewBase
    end;
next_base(Base, _) -> Base.

%% @doc A worker may be started with the result of a cache hit. It then has no
%% live execution state, so it starts from the process definition instead and
%% restores from the last full snapshot on its first compute.
live_base(Base) ->
    case dev_process:is_cached_state(Base) of
        true -> maps:get(<<"process">>, Base, Base);
        false -> Base
    end.

%% @doc Read the slot a request is asking for, as `dev_process:request_slot/2'
%% does for `compute/3' and for the grouper: an integer of at least -1, `Default'
%% when the request names no slot, or `{invalid, Raw}'. One reading for the
%% worker, its waiters and the grouper is what makes them agree: a request
%% naming its slot as `compute=N' was grouped by N but served -- and matched
%% by its waiter -- as though it named none, answering with the latest state.
read_slot(Req, Default, Opts) ->
    case dev_process:request_slot(Req, slot_read_opts(Opts)) of
        not_found -> Default;
        Slot -> Slot
    end.

%% @doc The options to read a slot out of a request under.
%%
%% A worker inherits the options of the HTTP request that spawned it, and every
%% response a node serves carries `force-message' (`http-extra-opts'). `hb_ao'
%% honours it on the way out of `get/4' and `resolve/3' alike, so a literal
%% comes back WRAPPED as `#{ <<"ao-result">> => <<"body">>, <<"body">> =>
%% <<"9">> }'. A slot is always a literal, so this is never what a slot read
%% wants -- and both places this module reads one hand the value straight to
%% `hb_util:int/1', which has no clause for a map.
%%
%% The consequences were the whole of the per-process worker. `server/3' died
%% of a `function_clause' on the FIRST request it was ever given, so a worker
%% was spawned, sent one job, and killed by it -- never reused, while its
%% waiter took a `DOWN' and re-ran the resolution itself. And once a worker did
%% survive, `compute_group/3' died the same way whenever it was called with
%% caller options that `find_or_register/3' had not stripped, turning every
%% read that found a live worker into a 500.
slot_read_opts(Opts) ->
    Opts#{ <<"force-message">> => false }.

%% @doc Await a resolution from a worker executing the `process@1.0' device.
await(Worker, GroupName, Base, Req, Opts) ->
    case hb_path:matches(<<"compute">>, hb_path:hd(Req, Opts)) of
        false -> 
            hb_persistent:default_await(Worker, GroupName, Base, Req, Opts);
        true ->
            TargetSlot = read_slot(Req, any, Opts),
            ?event({awaiting_compute, 
                {worker, Worker},
                {group, GroupName},
                {target_slot, TargetSlot}
            }),
            receive
                {resolved, _, GroupName, {slot, RecvdSlot}, Res}
                        when RecvdSlot == TargetSlot orelse TargetSlot == any ->
                    ?event(debug_compute, {notified_of_resolution,
                        {target, TargetSlot},
                        {group, GroupName}
                    }),
                    Res;
                {resolved, _, GroupName, {slot, RecvdSlot}, _Res} ->
                    ?event(debug_compute, {waiting_again,
                        {target, TargetSlot},
                        {recvd, RecvdSlot},
                        {worker, Worker},
                        {group, GroupName}
                    }),
                    await(Worker, GroupName, Base, Req, Opts);
                {'DOWN', _R, process, Worker, _Reason} ->
                    ?event(debug_compute,
                        {leader_died,
                            {group, GroupName},
                            {leader, Worker},
                            {target, TargetSlot}
                        }
                    ),
                    {error, leader_died}
            end
    end.

%% @doc Notify any waiters for a specific slot of the computed results.
notify_compute(GroupName, SlotToNotify, Res, Opts) ->
    notify_compute(GroupName, SlotToNotify, Res, Opts, 0).
notify_compute(GroupName, SlotToNotify, Res, Opts, Count) ->
    ?event({notifying_of_computed_slot, {group, GroupName}, {slot, SlotToNotify}}),
    % A listener's request carries the slot in the spelling it arrived from
    % HTTP in -- a binary -- while the slot just computed is an integer, and a
    % receive pattern cannot coerce. Both spellings are bound before the
    % receive so the selective match covers each. A request for any OTHER slot
    % has to stay in the mailbox, which is why this is a guard on a selective
    % receive and not a test after the fact.
    %
    % `compute' names the slot ahead of `slot' (`dev_process:request_slot/2'),
    % so a request is matched on `slot' only when it has no `compute', and is
    % a request for the latest state only when it has neither.
    BinSlotToNotify = hb_util:bin(SlotToNotify),
    receive
        {resolve, Listener, GroupName, Msg = #{ <<"compute">> := Slot }, _ListenerOpts}
                when Slot =:= SlotToNotify; Slot =:= BinSlotToNotify ->
            send_notification(Listener, GroupName, SlotToNotify, Res, Msg),
            notify_compute(GroupName, SlotToNotify, Res, Opts, Count + 1);
        {resolve, Listener, GroupName, Msg = #{ <<"slot">> := Slot }, _ListenerOpts}
                when (Slot =:= SlotToNotify orelse Slot =:= BinSlotToNotify)
                    andalso not is_map_key(<<"compute">>, Msg) ->
            send_notification(Listener, GroupName, SlotToNotify, Res, Msg),
            notify_compute(GroupName, SlotToNotify, Res, Opts, Count + 1);
        {resolve, Listener, GroupName, Msg, _ListenerOpts}
                when is_map(Msg)
                    andalso not is_map_key(<<"slot">>, Msg)
                    andalso not is_map_key(<<"compute">>, Msg) ->
            send_notification(Listener, GroupName, SlotToNotify, Res, Msg),
            notify_compute(GroupName, SlotToNotify, Res, Opts, Count + 1)
    after 0 ->
        ?event(worker_short,
            {finished_notifying,
                {group, GroupName},
                {slot, SlotToNotify},
                {listeners, Count}
            }
        )
    end.

send_notification(Listener, GroupName, SlotToNotify, Res) ->
    send_notification(Listener, GroupName, SlotToNotify, Res, undefined).
send_notification(Listener, GroupName, SlotToNotify, Res, Req) ->
    ?event({sending_notification, {group, GroupName}, {slot, SlotToNotify}}),
    Listener ! {
        resolved,
        self(),
        GroupName,
        {slot, SlotToNotify},
        case is_catch_up(Req) of
            true -> catch_up_result(Res);
            false -> public_result(Res)
        end
    }.

%% @doc A compute-on-schedule catch-up only waits for its slot to be done, so
%% it is told whether it was, never sent the state. `public_result/1' is the
%% whole public state (tens of MB for a large game), and sending it copies it
%% into the catch-up on every wake, inside the worker every write to the
%% process queues behind.
catch_up_result({ok, _}) -> {ok, #{ <<"process-catch-up">> => <<"computed">> }};
catch_up_result(_) -> {error, #{ <<"process-catch-up">> => <<"failed">> }}.

is_catch_up(Req) when is_map(Req) ->
    maps:get(<<"process-catch-up">>, Req, false) =:= true;
is_catch_up(_) -> false.

%% @doc A catch-up for a slot the worker's state has already reached needs no
%% compute: computing an older slot reads it back out of the process cache.
catch_up_covered(Req, Slot, Base) when is_integer(Slot) ->
    is_catch_up(Req) andalso
        case maps:get(<<"at-slot">>, Base, undefined) of
            At when is_integer(At) -> At >= Slot;
            _ -> false
        end;
catch_up_covered(_, _, _) -> false.

%% @doc The result a listener receives. The worker's state carries the whole
%% execution-device state in `priv' (for Lua, the VM: hundreds of MB for a
%% large process), and a message send copies all of it into every listener's
%% heap, after every slot. A listener only needs the public result, so send
%% that -- keeping the hashpath, which later resolution steps use -- and mark
%% it as a cached state, so that it is never computed onward as though it had
%% a VM (`dev_process:ensure_loaded/3', `live_base/1', `next_base/2').
public_result({ok, Msg}) when is_map(Msg) ->
    Priv = hb_private:from_message(Msg),
    {ok,
        Msg#{
            <<"priv">> =>
                (maps:with([<<"hashpath">>], Priv))#{
                    <<"process-cached-state">> => true
                }
        }
    };
public_result(Res) ->
    Res.

%% @doc Stop a worker process.
stop(Worker) ->
    exit(Worker, normal).

%%% Compute on schedule (`process-compute-on-schedule').

%% @doc Wake the background catch-up of a process this node has just assigned
%% `Assignment' for, if `process-compute-on-schedule' covers the process.
%%
%% With `process-now-from-cache', `now' serves the newest cached state and never
%% computes, so a process whose clients only schedule (no `push') stayed
%% uncomputed until something asked for a slot: on prod a game process sat at
%% computed slot 4 with 2,004 assignments, then took the whole backlog in one
%% burst. A node that is both scheduler and compute unit can compute as it
%% schedules instead.
%%
%% The schedule request pays a name lookup and a send (or a spawn): it never
%% waits for compute, and nothing here raises into it. Per process at most one
%% catch-up is in flight, registered with `hb_name' as the checkpoint writer is.
%% A wake that finds it running only raises its target, so a burst of N
%% assignments costs a few compute requests, not N. The catch-up computes with
%% an ordinary `compute&slot=N' request, so the slot is computed by the
%% process's worker exactly as a read would compute it -- same checkpoints,
%% same cache -- and nothing is pushed: the request carries no `push'.
compute_on_schedule(ProcID, SchedPID, Assignment, Opts) ->
    try
        HumanID = hb_util:human_id(ProcID),
        case compute_on_schedule_enabled(HumanID, Opts) of
            false -> ok;
            true ->
                Slot =
                    hb_util:int(
                        hb_maps:get(<<"slot">>, Assignment, undefined, Opts)
                    ),
                wake_catch_up(HumanID, SchedPID, Slot, Opts, 3)
        end
    catch
        Class:Reason ->
            ?event(compute_on_schedule,
                {wake_failed,
                    {proc_id, ProcID},
                    {class, Class},
                    {reason, Reason}
                },
                Opts
            ),
            ok
    end.

%% @doc `true' (or `<<"true">>') for every process, or a list of the process
%% IDs to compute on schedule.
compute_on_schedule_enabled(ProcID, Opts) ->
    case hb_opts:get(<<"process-compute-on-schedule">>, false, Opts) of
        true -> true;
        <<"true">> -> true;
        IDs when is_list(IDs) -> lists:member(ProcID, IDs);
        _ -> false
    end.

catch_up_name(ProcID) -> {?MODULE, compute_on_schedule, ProcID}.

%% @doc Raise the running catch-up's target to `Slot', or start one. The
%% catch-up is registered before it does anything, so the check and the claim
%% are atomic; a wake that loses the race raises the winner's target instead.
wake_catch_up(ProcID, _SchedPID, Slot, Opts, 0) ->
    ?event(compute_on_schedule,
        {wake_dropped, {proc_id, ProcID}, {slot, Slot}},
        Opts
    ),
    ok;
wake_catch_up(ProcID, SchedPID, Slot, Opts, Attempts) ->
    Name = catch_up_name(ProcID),
    case hb_name:lookup(Name) of
        Pid when is_pid(Pid) ->
            Pid ! {catch_up, Slot},
            ok;
        undefined ->
            CatchUp =
                spawn(
                    fun() ->
                        receive {go, Name} -> ok end,
                        catch_up(
                            Name, ProcID, SchedPID, Slot, -1, undefined, Opts
                        )
                    end
                ),
            case hb_name:register(Name, CatchUp) of
                ok ->
                    CatchUp ! {go, Name},
                    ok;
                error ->
                    exit(CatchUp, kill),
                    wake_catch_up(ProcID, SchedPID, Slot, Opts, Attempts - 1)
            end
    end.

%% @doc The catch-up loop: compute to the highest slot it has been woken for,
%% then to any slot it was woken for meanwhile, until it is level. Every target
%% is attempted once: a compute that fails or is skipped is logged and not
%% retried until a later assignment wakes the catch-up again.
catch_up(Name, ProcID, SchedPID, Target, Done, Process, Opts) ->
    case drain_catch_up(Target) of
        Next when Next =< Done ->
            finish_catch_up(Name, ProcID, SchedPID, Done, Opts);
        Next ->
            NextProcess = catch_up_to(ProcID, Process, Next, Opts),
            shed_dead_state(),
            catch_up(Name, ProcID, SchedPID, Next, Next, NextProcess, Opts)
    end.

%% @doc A catch-up that found no worker became the leader of its compute: it
%% computed the state itself and handed it to the worker it spawned, keeping
%% a dead copy (hundreds of MB for a large process). Under steady scheduling a
%% catch-up never runs dry, so that copy would otherwise wait for a collection
%% its small live set rarely triggers. A full sweep costs only the live data,
%% which is small here.
shed_dead_state() ->
    erase(?GC_CHECKPOINT_DUE),
    case erlang:process_info(self(), total_heap_size) of
        {total_heap_size, Words} when Words > ?CATCH_UP_GC_WORDS ->
            erlang:garbage_collect();
        _ -> ok
    end.

%% @doc The highest target among `Slot' and the wakes waiting in the mailbox.
drain_catch_up(Slot) ->
    receive {catch_up, Next} -> drain_catch_up(max(Slot, Next))
    after 0 -> Slot
    end.

%% @doc Leave the registry, then look once more. A wake that found this
%% catch-up just before it unregistered may have sent its slot after the last
%% drain; its assignment was made before that, so the scheduler's current slot
%% covers it, and a newer one starts a fresh catch-up.
finish_catch_up(Name, ProcID, SchedPID, Done, Opts) ->
    hb_name:unregister(Name),
    Late = max(drain_catch_up(Done), scheduler_current(SchedPID)),
    case Late > Done of
        true -> wake_catch_up(ProcID, SchedPID, Late, Opts, 3);
        false -> ok
    end.

%% The scheduling server's current slot, by its `current' message protocol
%% (`dev_scheduler_server:current/2'). That module is packaged in
%% `scheduler@1.0' and loaded under a package-specific name, so naming it from
%% here raised `undef' -- after the catch-up had unregistered, so every catch-up
%% ended in a crash report (thousands an hour on prod) and a late wake was lost.
scheduler_current(SchedPID) when is_pid(SchedPID) ->
    Mon = erlang:monitor(process, SchedPID, [{alias, reply_demonitor}]),
    SchedPID ! {current, Mon, Mon},
    receive
        {current, Mon, Current} when is_integer(Current) -> Current;
        {current, Mon, _} -> -1;
        {'DOWN', Mon, process, _, _} -> -1
    after 5000 ->
        erlang:demonitor(Mon, [flush]),
        -1
    end;
scheduler_current(_) -> -1.

%% @doc Compute `Target' with an ordinary `compute' request, which joins the
%% process's worker queue (or, with no worker, becomes its leader), unless the
%% process is cold and reaching `Target' would replay more than
%% `process-historical-replay-limit' slots from its newest checkpoint. That
%% replay is skipped quietly: it is left to the first read that asks for it.
%% Returns the process message, read on first use.
catch_up_to(ProcID, Process, Target, Opts) ->
    case hb_name:lookup(ProcID) of
        Pid when is_pid(Pid) ->
            catch_up_live(Pid, ProcID, Target, Opts),
            Process;
        undefined ->
            catch_up_cold(ProcID, Process, Target, Opts)
    end.

%% @doc A process with a live worker (or a leader about to hand off to one) is
%% caught up by queueing a `compute' for `Target' on it, as `hb_persistent'
%% would for a request, and waiting for the answer, which is small
%% (`catch_up_result/1'). Nothing passes through `hb_ao' in the catch-up: no
%% process read, no cached-slot read, no result caching. Under the swarm that
%% path did all three on every wake, copying and writing the whole public state,
%% and per-slot store time on every process went from 7-40 ms to 117-183 ms.
catch_up_live(Pid, ProcID, Target, Opts) ->
    try
        cos_run_hook(ProcID, Target, Opts),
        Mon = erlang:monitor(process, Pid),
        Req =
            #{
                <<"path">> => <<"compute">>,
                <<"slot">> => Target,
                <<"process-catch-up">> => true
            },
        Pid ! {resolve, self(), ProcID, Req, catch_up_opts(Opts)},
        receive
            {resolved, _, ProcID, _, Res} ->
                erlang:demonitor(Mon, [flush]),
                ?event(compute_on_schedule,
                    {computed_live, {proc_id, ProcID}, {slot, Target}, {res, Res}},
                    Opts
                );
            {'DOWN', Mon, process, Pid, Reason} ->
                ?event(compute_on_schedule,
                    {worker_down, {proc_id, ProcID}, {slot, Target}, {reason, Reason}},
                    Opts
                )
        after ?CATCH_UP_WAIT_MS ->
            erlang:demonitor(Mon, [flush]),
            ?event(compute_on_schedule,
                {wait_timeout, {proc_id, ProcID}, {slot, Target}},
                Opts
            )
        end
    catch
        Class:Reason2 ->
            ?event(compute_on_schedule,
                {compute_crashed,
                    {proc_id, ProcID},
                    {slot, Target},
                    {class, Class},
                    {reason, Reason2}
                },
                Opts
            )
    end.

cos_run_hook(ProcID, Target, Opts) ->
    case hb_opts:get(<<"process-compute-on-schedule-hook">>, undefined, Opts) of
        Hook when is_function(Hook, 2) -> Hook(ProcID, Target);
        _ -> ok
    end.

%% @doc No worker: compute `Target' with an ordinary request, which becomes the
%% group's leader and spawns the worker every later wake finds.
catch_up_cold(ProcID, Process, Target, Opts) ->
    try
        case catch_up_allowed(ProcID, Target, Opts) of
            false -> Process;
            true ->
                Proc =
                    case Process of
                        undefined -> hb_util:ok(hb_cache:read(ProcID, Opts));
                        _ -> Process
                    end,
                cos_run_hook(ProcID, Target, Opts),
                Res =
                    hb_ao:resolve(
                        Proc,
                        #{ <<"path">> => <<"compute">>, <<"slot">> => Target },
                        catch_up_opts(Opts)
                    ),
                case Res of
                    {ok, _} ->
                        ?event(compute_on_schedule,
                            {computed, {proc_id, ProcID}, {slot, Target}},
                            Opts
                        );
                    Error ->
                        ?event(compute_on_schedule,
                            {compute_failed,
                                {proc_id, ProcID},
                                {slot, Target},
                                {error, Error}
                            },
                            Opts
                        )
                end,
                Proc
        end
    catch
        Class:Reason ->
            ?event(compute_on_schedule,
                {compute_crashed,
                    {proc_id, ProcID},
                    {slot, Target},
                    {class, Class},
                    {reason, Reason}
                },
                Opts
            ),
            Process
    end.

%% @doc The options a catch-up computes under. The catch-up is woken from
%% inside the scheduler device, and `hb_ao' strips `spawn-worker' from the
%% options every device call receives (it is one of its temporary options).
%% Resolved under them, a catch-up that found no worker became the group's
%% leader, computed from the newest checkpoint in its own process, and then
%% unregistered without handing its state to a worker. No worker ever held the
%% live state, so every catch-up was cold: on prod each hunt process (no
%% checkpoint but slot 0) replayed every slot from 1 on every wake, ~400 slots
%% per few new assignments. With process workers on, the catch-up delegates to
%% a worker as a request does, and every later catch-up finds that worker live.
%%
%% The options are the ones an HTTP request to this node resolves under
%% (`request_opts/1'), never the scheduler call's own. A worker computes every
%% slot of its process for as long as it lives (`process-worker-max-idle', a
%% day on prod) under the options it was spawned with, and they override each
%% request's (`server/4'). The scheduler call's options are whatever woke the
%% catch-up: the HTTP request with `hb_ao''s temporary options stripped, or a
%% push's (`push-in-worker', `push-max-depth', ...), or -- from inside another
%% worker -- `process-workers' and `await-inprogress' off, which made the
%% catch-up compute in its own process instead of delegating. A worker the
%% catch-up spawns must be the worker a request would have spawned.
catch_up_opts(Opts) ->
    ReqOpts = request_opts(Opts),
    case hb_opts:get(<<"process-workers">>, false, ReqOpts) of
        false -> ReqOpts;
        _ -> ReqOpts#{ <<"spawn-worker">> => true }
    end.

%% @doc The options an HTTP request to this node resolves under: the node
%% message, as `hb_http_server' holds it now, with `http-extra-opts' and
%% `force-message' applied as `dev_meta' applies them. Without an HTTP server
%% (a node used as a library, or most tests), `Opts' unchanged.
request_opts(Opts) ->
    case hb_opts:get(<<"http-server">>, no_server_ref, Opts) of
        no_server_ref -> Opts;
        _ ->
            case hb_http_server:get_opts(Opts) of
                NodeMsg when is_map(NodeMsg) ->
                    Extra = hb_opts:get(<<"http-extra-opts">>, #{}, NodeMsg),
                    (hb_maps:merge(NodeMsg, Extra, NodeMsg))#{
                        <<"force-message">> => true
                    };
                _ -> Opts
            end
    end.

%% @doc A live worker computes forward from the state it holds. Without one,
%% the compute restores the newest checkpoint at or below `Target' and replays
%% from there, which is allowed only within `catch_up_replay_limit/1'.
catch_up_allowed(ProcID, Target, Opts) ->
    case hb_name:lookup(ProcID) of
        Pid when is_pid(Pid) -> true;
        undefined ->
            Limit = catch_up_replay_limit(Opts),
            case dev_process:cold_replay_bound(ProcID, Target, Limit, Opts) of
                {true, _} -> true;
                {false, From} ->
                    ?event(compute_on_schedule,
                        {skipped_cold_replay,
                            {proc_id, ProcID},
                            {slot, Target},
                            {checkpoint, From},
                            {limit, Limit}
                        },
                        Opts
                    ),
                    false
            end
    end.

%% @doc The most slots a cold catch-up may replay from the newest checkpoint:
%% `process-compute-on-schedule-replay-limit', by default twice the snapshot
%% cadence (`process-snapshot-slots', when it is an integer), else 2000.
%%
%% It is not `process-historical-replay-limit': that bounds reads of old slots,
%% and is set well below the snapshot cadence (64 against 1000 on prod), so a
%% catch-up bounded by it was skipped almost every time the worker was gone.
%% Replaying from the newest checkpoint to the head is what any read or push of
%% a cold process does anyway; the bound only has to stop a replay from slot 0,
%% or from far below the retained checkpoints, that nobody asked for.
catch_up_replay_limit(Opts) ->
    LimitOpt = <<"process-compute-on-schedule-replay-limit">>,
    case hb_opts:get(LimitOpt, undefined, Opts) of
        undefined ->
            case hb_opts:get(<<"process-snapshot-slots">>, undefined, Opts) of
                Slots when is_integer(Slots), Slots > 0 -> 2 * Slots;
                _ -> 2000
            end;
        Limit -> Limit
    end.

%%% Tests

test_init() ->
    application:ensure_all_started(hb),
    ok.

info_test() ->
    test_init(),
    M1 = hb_process_test_vectors:wasm_process(<<"test/aos-2-pure-xs.wasm">>),
    Res = hb_device:info(M1, #{}),
    Grouper = hb_maps:get(grouper, Res, undefined, #{}),
    ?assert(is_function(Grouper, 3)),
    {module, Mod} = erlang:fun_info(Grouper, module),
    ?assertMatch(<<"_hb_device_", _/binary>>, atom_to_binary(Mod, utf8)).

%% @doc Listeners receive the public result, marked as a cached state, with
%% its hashpath -- never the execution-device state in `priv'.
worker_notification_is_public_test() ->
    Group = make_ref(),
    VM = #{ <<"state">> => lists:seq(1, 100000) },
    Full = #{
        <<"counter">> => <<"1">>,
        <<"priv">> => VM#{ <<"hashpath">> => <<"hp">> }
    },
    send_notification(self(), Group, 1, {ok, Full}),
    receive
        {resolved, _, Group, {slot, 1}, {ok, Public}} ->
            ?assertEqual(<<"1">>, maps:get(<<"counter">>, Public)),
            ?assertEqual(
                #{ <<"hashpath">> => <<"hp">>, <<"process-cached-state">> => true },
                maps:get(<<"priv">>, Public)
            ),
            ?assert(dev_process:is_cached_state(Public))
    after 1000 -> erlang:error(no_notification)
    end,
    send_notification(self(), Group, 2, {error, boom}),
    receive {resolved, _, Group, {slot, 2}, Err} -> ?assertEqual({error, boom}, Err)
    after 1000 -> erlang:error(no_notification)
    end.

grouper_test() ->
    test_init(),
    M1 = hb_process_test_vectors:aos_process(),
    M2 = #{ <<"path">> => <<"compute">>, <<"v">> => 1 },
    M3 = #{ <<"path">> => <<"compute">>, <<"v">> => 2 },
    M4 = #{ <<"path">> => <<"not-compute">>, <<"v">> => 3 },
    G1 = hb_persistent:group(M1, M2, #{ <<"process-workers">> => true }),
    G2 = hb_persistent:group(M1, M3, #{ <<"process-workers">> => true }),
    G3 = hb_persistent:group(M1, M4, #{ <<"process-workers">> => true }),
    ?event({group_samples, {g1, G1}, {g2, G2}, {g3, G3}}),
    ?assertEqual(G1, G2),
    ?assertNotEqual(G1, G3).

%% @doc `compute' requests whose result is already in the local cache
%% should bypass the per-process worker queue (returning the
%% `ungrouped_exec' sentinel that `hb_persistent:find_or_register/3'
%% short-circuits). Requests that still need work, and requests for a
%% slot beyond what is cached, must continue to serialise through the
%% process group.
grouper_skips_when_slot_cached_test() ->
    test_init(),
    Opts =
        #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new()
        },
    M1 = hb_process_test_vectors:aos_process(Opts),
    POpts = Opts#{ <<"process-workers">> => true },
    % With the cache empty, every compute request must group by
    % process so that the worker can do the actual work.
    Uncached = #{ <<"path">> => <<"compute">>, <<"slot">> => 5 },
    ProcessGroup = hb_persistent:group(M1, Uncached, POpts),
    ?assertNotEqual(ungrouped_exec, ProcessGroup),
    % Write slot 5 into the cache. The same request now has a result
    % available and the grouper should step out of the queue.
    {ok, _} =
        dev_process_cache:write(
            ProcessGroup,
            5,
            #{ <<"hello">> => <<"cached">> },
            Opts
        ),
    ?assertEqual(
        ungrouped_exec,
        hb_persistent:group(M1, Uncached, POpts)
    ),
    % Cache slots are not assumed to be gap-free. A lower slot that
    % has not actually been written must still go through the worker.
    MissingLower = #{ <<"path">> => <<"compute">>, <<"slot">> => 4 },
    ?assertEqual(ProcessGroup, hb_persistent:group(M1, MissingLower, POpts)),
    % A request for a slot beyond what we cached must still be
    % serialised through the worker.
    Beyond = #{ <<"path">> => <<"compute">>, <<"slot">> => 999 },
    ?assertEqual(ProcessGroup, hb_persistent:group(M1, Beyond, POpts)),
    % A `compute' request without a slot resolves via the cache-only
    % branch of `now/3' once any slot exists, so it also bypasses the
    % queue.
    NoSlot = #{ <<"path">> => <<"compute">> },
    ?assertEqual(
        ungrouped_exec,
        hb_persistent:group(M1, NoSlot, POpts)
    ).

%% @doc Regression: every slot this module reads comes back through `hb_ao',
%% which honours the caller's `force-message' -- and a worker inherits the
%% options of the HTTP request that spawned it, where the node's
%% `http-extra-opts' set exactly that. The slot arrived as
%% `#{ <<"ao-result">> => <<"body">>, <<"body">> => <<"9">> }' and was handed
%% to `hb_util:int/1', which has no clause for a map.
slot_read_survives_forced_message_test() ->
    Forced = #{ <<"force-message">> => true },
    Req = #{ <<"path">> => <<"compute">>, <<"slot">> => <<"9">> },
    % The integer, not the envelope -- and not the binary either: `dev_process'
    % counts slots in integers, and a waiter matching `RecvdSlot == 9' would
    % never see a notification carrying `<<"9">>'.
    ?assertEqual(9, read_slot(Req, not_found, Forced)),
    ?assert(is_integer(read_slot(Req, not_found, Forced))),
    % Each caller's default survives a request that names no slot: `compute'
    % without a slot is a real request shape (it serves the latest state).
    ?assertEqual(any, read_slot(#{ <<"path">> => <<"compute">> }, any, Forced)),
    ?assertEqual(
        not_found,
        read_slot(#{ <<"path">> => <<"compute">> }, not_found, Forced)
    ),
    % And an envelope that reaches us already built is unwrapped, however it
    % got there.
    ?assertEqual(
        9,
        read_slot(
            #{ <<"slot">> =>
                #{ <<"ao-result">> => <<"body">>, <<"body">> => <<"9">> } },
            not_found,
            #{}
        )
    ),
    ?assertEqual(9, read_slot(#{ <<"slot">> => 9 }, not_found, #{})).

%% @doc Regression: a grouper called with the FULL caller options -- rather
%% than the ones `find_or_register/3' strips -- saw `force-message' where the
%% register path never did, so `compute_group/3' died there and every read that
%% found a live worker was answered with a 500 instead of waiting on it. That
%% 500 was served in 4 ms, so the client simply retried, and the round trip
%% became the client's backoff rather than the node's work.
grouper_survives_forced_message_test() ->
    test_init(),
    Opts =
        #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new()
        },
    M1 = hb_process_test_vectors:aos_process(Opts),
    % The options a waiter arrives with: `process-workers' on, and
    % `force-message' as every HTTP response sets it.
    POpts = Opts#{ <<"process-workers">> => true, <<"force-message">> => true },
    Uncached = #{ <<"path">> => <<"compute">>, <<"slot">> => <<"5">> },
    ProcessGroup = hb_persistent:group(M1, Uncached, POpts),
    ?assert(is_binary(ProcessGroup)),
    ?assertNotEqual(ungrouped_exec, ProcessGroup),
    % And the group name must not depend on how the slot was spelled: a
    % waiter and the leader that registered the group have to agree.
    ?assertEqual(
        ProcessGroup,
        hb_persistent:group(
            M1,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 5 },
            Opts#{ <<"process-workers">> => true }
        )
    ),
    % A cached slot still steps out of the queue, with the slot spelled either
    % way and `force-message' set.
    {ok, _} =
        dev_process_cache:write(
            ProcessGroup,
            5,
            #{ <<"hello">> => <<"cached">> },
            Opts
        ),
    ?assertEqual(ungrouped_exec, hb_persistent:group(M1, Uncached, POpts)).

%% @doc Regression, end to end through the worker loop: a worker holding the
%% options an HTTP request hands it must survive a `compute' request whose slot
%% is spelled the way HTTP spells it -- a binary -- and must notify its
%% listener with a slot that the listener's own `await/5' can match.
worker_survives_forced_message_slot_test_() ->
    {timeout, 60, fun() ->
        test_init(),
        Opts =
            #{
                <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
                <<"priv-wallet">> => ar_wallet:new()
            },
        Base = hb_process_test_vectors:aos_process(Opts),
        hb_process_test_vectors:schedule_aos_call(Base, <<"return 1+1">>, Opts),
        WorkerOpts =
            Opts#{
                <<"force-message">> => true,
                <<"spawn-worker">> => false,
                <<"process-workers">> => false
            },
        Group = <<"forced-message-slot-worker">>,
        Self = self(),
        Worker = spawn(fun() -> server(Group, Base, WorkerOpts) end),
        MRef = erlang:monitor(process, Worker),
        Worker !
            {resolve,
                Self,
                Group,
                #{ <<"path">> => <<"compute">>, <<"slot">> => <<"0">> },
                WorkerOpts
            },
        receive
            {resolved, _, Group, {slot, NotifiedSlot}, Res} ->
                ?assertEqual(0, NotifiedSlot),
                ?assertMatch({ok, _}, Res);
            {'DOWN', MRef, process, Worker, Reason} ->
                ?assertEqual(worker_stayed_alive, {worker_died, Reason})
        after 30000 ->
            ?assertEqual(worker_answered, timed_out)
        end,
        % And it is still there for the next request, which is the entire point
        % of a persistent worker.
        ?assert(is_process_alive(Worker)),
        exit(Worker, normal)
    end}.

%%% Regression tests: requests that reach a live worker over HTTP. Every test
%%% bounds its own waits so a regression fails instead of hanging the suite.

counter_script() ->
    <<
        "Count = Count or 0\n"
        "function compute(first, second)\n"
        "  Count = Count + 1\n"
        "  local value = tostring(Count)\n"
        "  if second ~= nil then\n"
        "    first.count = value\n"
        "    first.results = { output = { data = value } }\n"
        "    return first\n"
        "  end\n"
        "  return {\n"
        "    patches = { { path = '/count', value = value } },\n"
        "    results = { output = { data = value } }\n"
        "  }\n"
        "end\n"
    >>.

%% @doc A Lua counter process with four scheduled messages, computed to slot 2
%% so that a worker holds its live state. Slot N leaves `count' at N + 1.
worker_setup() ->
    test_init(),
    Wallet = ar_wallet:new(),
    Opts =
        #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => Wallet,
            <<"spawn-worker">> => true,
            <<"process-workers">> => true,
            <<"await-inprogress">> => named
        },
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    Process =
        hb_message:commit(
            #{
                <<"device">> => <<"process@1.0">>,
                <<"type">> => <<"Process">>,
                <<"scheduler-device">> => <<"scheduler@1.0">>,
                <<"execution-device">> => <<"lua@5.3b">>,
                <<"module">> =>
                    #{
                        <<"content-type">> => <<"application/lua">>,
                        <<"body">> => counter_script()
                    },
                <<"authority">> => [Address],
                <<"scheduler-location">> => Address,
                <<"test-random-seed">> => rand:uniform(1000000)
            },
            Opts
        ),
    {ok, _} = hb_cache:write(Process, Opts),
    ProcID = hb_message:id(Process, all, Opts),
    lists:foreach(
        fun(N) ->
            {ok, _} =
                hb_ao:resolve(
                    Process,
                    hb_message:commit(
                        #{
                            <<"path">> => <<"schedule">>,
                            <<"method">> => <<"POST">>,
                            <<"body">> =>
                                hb_message:commit(
                                    #{
                                        <<"target">> => ProcID,
                                        <<"type">> => <<"Message">>,
                                        <<"number">> => N
                                    },
                                    Opts
                                )
                        },
                        Opts
                    ),
                    Opts
                )
        end,
        lists:seq(1, 4)
    ),
    {ok, S2} =
        hb_ao:resolve(
            Process,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 2 },
            Opts
        ),
    ?assertEqual(<<"3">>, hb_ao:get(<<"count">>, S2, Opts)),
    Group = hb_util:human_id(ProcID),
    Worker = await_worker(Group, 50),
    ?assert(is_pid(Worker)),
    {Process, Worker, Group, Opts}.

await_worker(_Group, 0) -> undefined;
await_worker(Group, N) ->
    case hb_name:lookup(Group) of
        Pid when is_pid(Pid) -> Pid;
        _ -> timer:sleep(100), await_worker(Group, N - 1)
    end.

%% @doc Resolve in a separate process, failing rather than hanging if no
%% answer arrives in time.
bounded_resolve(Base, Req, Opts) ->
    Self = self(),
    Ref = make_ref(),
    Pid = spawn(fun() -> Self ! {Ref, catch hb_ao:resolve(Base, Req, Opts)} end),
    receive {Ref, Res} -> Res
    after 15000 ->
        exit(Pid, kill),
        erlang:error({request_did_not_return, Req})
    end.

%% @doc Regression: `compute=N' names slot N to the grouper, so the worker must
%% compute slot N and its waiter must accept only slot N. The worker read only
%% `slot' and its waiter took the first notification it saw, so with a worker
%% alive the request was answered with the latest state (slot 2) instead.
compute_key_names_slot_on_worker_test_() ->
    {timeout, 120, fun() ->
        {Process, Worker, _Group, Opts} = worker_setup(),
        {ok, S3} =
            bounded_resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"compute">> => 3 },
                Opts
            ),
        ?assertEqual(3, hb_ao:get(<<"at-slot">>, S3, Opts)),
        ?assertEqual(<<"4">>, hb_ao:get(<<"count">>, S3, Opts)),
        ?assert(is_process_alive(Worker))
    end}.

%% @doc Regression: a malformed `slot' beside a valid `compute' was grouped by
%% `compute' but read by the worker from `slot', killing it in `hb_util:int/1'
%% and losing the live state.
malformed_slot_keeps_worker_test_() ->
    {timeout, 120, fun() ->
        {Process, Worker, _Group, Opts} = worker_setup(),
        Res =
            bounded_resolve(
                Process,
                #{
                    <<"path">> => <<"compute">>,
                    <<"compute">> => 3,
                    <<"slot">> => <<"abc">>
                },
                Opts
            ),
        ?assertMatch({ok, _}, Res),
        {ok, S3} = Res,
        ?assertEqual(<<"4">>, hb_ao:get(<<"count">>, S3, Opts)),
        ?assert(is_process_alive(Worker))
    end}.

%% @doc Regression: a slot that cannot name an assignment -- negative, or not
%% an integer -- is a 400 in the requester. A negative slot reached the worker,
%% whose rewind found nothing and threw; each waiter then re-elected a leader,
%% spawned a new worker and resent the request, forever.
invalid_slot_is_rejected_test_() ->
    {timeout, 120, fun() ->
        {Process, Worker, Group, Opts} = worker_setup(),
        lists:foreach(
            fun(Slot) ->
                Res =
                    bounded_resolve(
                        Process,
                        #{ <<"path">> => <<"compute">>, <<"slot">> => Slot },
                        Opts
                    ),
                ?assertMatch({error, #{ <<"status">> := 400 }}, Res)
            end,
            [<<"-2">>, -2, <<"abc">>]
        ),
        ?assert(is_process_alive(Worker)),
        ?assertEqual(Worker, hb_name:lookup(Group))
    end}.

%% @doc Regression: slot -1 is the initialized state before the first
%% assignment, not an invalid slot. A worker live at slot 2 answers it by
%% rewinding, survives, and still computes onward afterwards.
initial_slot_on_live_worker_test_() ->
    {timeout, 120, fun() ->
        {Process, Worker, Group, Opts} = worker_setup(),
        lists:foreach(
            fun(Slot) ->
                {ok, Init} =
                    bounded_resolve(
                        Process,
                        #{ <<"path">> => <<"compute">>, <<"slot">> => Slot },
                        Opts
                    ),
                ?assertEqual(-1, hb_ao:get(<<"at-slot">>, Init, Opts))
            end,
            [-1, <<"-1">>]
        ),
        {ok, S3} =
            bounded_resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => 3 },
                Opts
            ),
        ?assertEqual(<<"4">>, hb_ao:get(<<"count">>, S3, Opts)),
        ?assert(is_process_alive(Worker)),
        ?assertEqual(Worker, hb_name:lookup(Group))
    end}.

%% @doc Regression: `now' on a process with no assignments asks `compute' for
%% the scheduler's current slot, -1, and must be answered with the initialized
%% state. Rejecting -1 as invalid made it a 400, so every read of a fresh
%% process's state (a new ledger's balances, say) fell back to its default.
now_on_unscheduled_process_test_() ->
    {timeout, 60, fun() ->
        test_init(),
        Wallet = ar_wallet:new(),
        Opts =
            #{
                <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
                <<"priv-wallet">> => Wallet
            },
        Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
        Process =
            hb_message:commit(
                #{
                    <<"device">> => <<"process@1.0">>,
                    <<"type">> => <<"Process">>,
                    <<"scheduler-device">> => <<"scheduler@1.0">>,
                    <<"execution-device">> => <<"lua@5.3b">>,
                    <<"module">> =>
                        #{
                            <<"content-type">> => <<"application/lua">>,
                            <<"body">> => counter_script()
                        },
                    <<"authority">> => [Address],
                    <<"scheduler-location">> => Address,
                    <<"initial">> => <<"value">>,
                    <<"test-random-seed">> => rand:uniform(1000000)
                },
                Opts
            ),
        {ok, _} = hb_cache:write(Process, Opts),
        {ok, Now} = bounded_resolve(Process, <<"now">>, Opts),
        ?assertEqual(-1, hb_ao:get(<<"at-slot">>, Now, Opts)),
        ?assertEqual(
            <<"value">>,
            hb_ao:get(<<"process/initial">>, Now, Opts)
        )
    end}.

%% @doc Regression: the worker survives any request delivered to it, whether
%% or not the grouper would have admitted it, and answers it with an error.
worker_survives_bad_requests_test_() ->
    {timeout, 120, fun() ->
        {_Process, Worker, Group, Opts} = worker_setup(),
        lists:foreach(
            fun(Req) ->
                MRef = erlang:monitor(process, Worker),
                Worker ! {resolve, self(), Group, Req, Opts},
                receive
                    {resolved, _, Group, _, Res} ->
                        ?assertMatch({error, _}, Res);
                    {'DOWN', MRef, process, Worker, Reason} ->
                        ?assertEqual(worker_alive, {worker_died, Reason})
                after 15000 ->
                    ?assertEqual(worker_answered, timed_out)
                end,
                erlang:demonitor(MRef, [flush])
            end,
            [
                #{ <<"path">> => <<"compute">>, <<"slot">> => <<"-2">> },
                #{ <<"path">> => <<"compute">>, <<"slot">> => <<"abc">> },
                #{ <<"path">> => <<"compute">>, <<"slot">> => -5 }
            ]
        ),
        ?assert(is_process_alive(Worker)),
        % And it still serves the next slot from its live state.
        Worker ! {resolve, self(), Group,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 3 }, Opts},
        receive
            {resolved, _, Group, {slot, 3}, {ok, S3}} ->
                ?assertEqual(<<"4">>, hb_ao:get(<<"count">>, S3, Opts))
        after 15000 ->
            ?assertEqual(worker_answered, timed_out)
        end
    end}.

%% @doc Regression: the keys that steer a compute beyond its slot reach the
%% worker's resolution. `init' is the one with an observable answer: a
%% slot-less compute with `init' other than `now' is `not_found', which the
%% worker answered with the latest state instead, having rebuilt the request
%% from its slot alone. `push' was dropped the same way, so a delegated
%% compute never triggered its push.
worker_carries_request_keys_test_() ->
    {timeout, 120, fun() ->
        {_Process, Worker, Group, Opts} = worker_setup(),
        Worker ! {resolve, self(), Group,
            #{ <<"path">> => <<"compute">>, <<"init">> => <<"none">> }, Opts},
        receive
            {resolved, _, Group, _, Res} ->
                ?assertEqual({error, not_found}, Res)
        after 15000 ->
            ?assertEqual(worker_answered, timed_out)
        end
    end}.

%% @doc Regression: a request naming its slot with `compute' is notified only
%% of that slot. It has no `slot' key, and was taken for a request for the
%% latest state and answered with whichever slot completed first.
notify_compute_honours_compute_key_test() ->
    Group = make_ref(),
    Self = self(),
    Self ! {resolve, Self, Group, #{ <<"path">> => <<"compute">>, <<"compute">> => 3 }, #{}},
    notify_compute(Group, 5, {ok, #{}}, #{}),
    Notified =
        receive {resolved, _, Group, {slot, S}, _} -> {notified, S}
        after 0 -> none
        end,
    Pending =
        receive {resolve, Self, Group, #{ <<"compute">> := 3 }, _} -> pending
        after 0 -> consumed
        end,
    ?assertEqual(none, Notified),
    ?assertEqual(pending, Pending),
    Self ! {resolve, Self, Group, #{ <<"path">> => <<"compute">>, <<"compute">> => 5 }, #{}},
    notify_compute(Group, 5, {ok, #{}}, #{}),
    receive {resolved, _, Group, {slot, 5}, _} -> ok
    after 0 -> ?assertEqual(notified, none)
    end.

%%% Regression tests: historical reads must not regress the live state.

%% @doc A Lua counter process with `Count' scheduled messages, checkpointed
%% every 10 slots. Slot N leaves `count' at N + 1.
counter_process(Count, ExtraOpts) ->
    test_init(),
    Wallet = ar_wallet:new(),
    Opts =
        ExtraOpts#{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => Wallet,
            <<"process-snapshot-slots">> => 10,
            <<"process-delta-checkpoint-slots">> => 10
        },
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    Process =
        hb_message:commit(
            #{
                <<"device">> => <<"process@1.0">>,
                <<"type">> => <<"Process">>,
                <<"scheduler-device">> => <<"scheduler@1.0">>,
                <<"execution-device">> => <<"lua@5.3b">>,
                <<"module">> =>
                    #{
                        <<"content-type">> => <<"application/lua">>,
                        <<"body">> => counter_script()
                    },
                <<"authority">> => [Address],
                <<"scheduler-location">> => Address,
                <<"test-random-seed">> => rand:uniform(1000000)
            },
            Opts
        ),
    {ok, _} = hb_cache:write(Process, Opts),
    schedule_counter(Process, lists:seq(1, Count), Opts),
    {Process, Opts}.

schedule_counter(Process, Ns, Opts) ->
    ProcID = hb_message:id(Process, all, Opts),
    lists:foreach(
        fun(N) ->
            {ok, _} =
                hb_ao:resolve(
                    Process,
                    hb_message:commit(
                        #{
                            <<"path">> => <<"schedule">>,
                            <<"method">> => <<"POST">>,
                            <<"body">> =>
                                hb_message:commit(
                                    #{
                                        <<"target">> => ProcID,
                                        <<"type">> => <<"Message">>,
                                        <<"number">> => N
                                    },
                                    Opts
                                )
                        },
                        Opts
                    ),
                    Opts
                )
        end,
        Ns
    ).

%% @doc Run `Fun' and return its result with the number of slots it executed.
count_executed_slots(Fun) ->
    MFA = {dev_process, compute_slot, 6},
    erlang:trace_pattern(MFA, true, [call_count]),
    try
        Res = Fun(),
        {call_count, N} = erlang:trace_info(MFA, call_count),
        {Res, N}
    after
        erlang:trace_pattern(MFA, false, [call_count])
    end.

%% @doc Regression: a historical read must not replace the worker's live state.
%% `compute&slot=-1' rewound the worker to the initialized state and the worker
%% kept it, so the next head request replayed every slot from 0 (6,601 slots,
%% 215s, on stage). The worker is live at slot 50 here; after reads of -1 and 3
%% the next slot must cost exactly one executed slot.
historical_read_keeps_live_state_test_() ->
    {timeout, 300, fun() ->
        {Process, Opts} =
            counter_process(
                51,
                #{
                    <<"spawn-worker">> => true,
                    <<"process-workers">> => true,
                    <<"await-inprogress">> => named
                }
            ),
        {ok, S50} =
            hb_ao:resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => 50 },
                Opts
            ),
        ?assertEqual(<<"51">>, hb_ao:get(<<"count">>, S50, Opts)),
        Group = hb_util:human_id(hb_message:id(Process, all, Opts)),
        Worker = await_worker(Group, 50),
        ?assert(is_pid(Worker)),
        {ok, Init} =
            bounded_resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => <<"-1">> },
                Opts
            ),
        ?assertEqual(-1, hb_ao:get(<<"at-slot">>, Init, Opts)),
        ?assertEqual(not_found, hb_ao:get(<<"count">>, Init, Opts)),
        {ok, S3} =
            bounded_resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => 3 },
                Opts
            ),
        ?assertEqual(3, hb_ao:get(<<"at-slot">>, S3, Opts)),
        ?assertEqual(<<"4">>, hb_ao:get(<<"count">>, S3, Opts)),
        schedule_counter(Process, [52], Opts),
        {{ok, S51}, Executed} =
            count_executed_slots(
                fun() ->
                    bounded_resolve(
                        Process,
                        #{ <<"path">> => <<"compute">>, <<"slot">> => 51 },
                        Opts
                    )
                end
            ),
        ?assertEqual(<<"52">>, hb_ao:get(<<"count">>, S51, Opts)),
        ?assertEqual(1, Executed),
        ?assertEqual(Worker, hb_name:lookup(Group))
    end}.

%% @doc Without a worker: a historical read from a live state is an answer
%% only, and a head computation from a live state far behind the newest
%% checkpoint resumes from that checkpoint instead of replaying the gap.
historical_read_without_worker_test_() ->
    {timeout, 300, fun() ->
        {Process, Opts} = counter_process(61, #{}),
        {ok, S5} =
            hb_ao:resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => 5 },
                Opts
            ),
        ?assertNot(dev_process:is_cached_state(S5)),
        {ok, S59} =
            hb_ao:resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => 59 },
                Opts
            ),
        ?assertEqual(<<"60">>, hb_ao:get(<<"count">>, S59, Opts)),
        % A historical read from the live state is marked as an answer.
        {ok, Init} =
            hb_ao:resolve(
                S59,
                #{ <<"path">> => <<"compute">>, <<"slot">> => -1 },
                Opts
            ),
        ?assertEqual(-1, hb_ao:get(<<"at-slot">>, Init, Opts)),
        ?assert(dev_process:is_cached_state(Init)),
        % Slot 60 from the state at slot 5 resumes from the checkpoint at 50.
        {{ok, S60}, Executed} =
            count_executed_slots(
                fun() ->
                    hb_ao:resolve(
                        S5,
                        #{ <<"path">> => <<"compute">>, <<"slot">> => 60 },
                        Opts
                    )
                end
            ),
        ?assertEqual(<<"61">>, hb_ao:get(<<"count">>, S60, Opts)),
        ?assert(Executed =< 10)
    end}.

%%% Compute on schedule (`process-compute-on-schedule').

cos_opts(Extra) ->
    test_init(),
    maps:merge(
        #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"spawn-worker">> => true,
            <<"process-workers">> => true,
            <<"await-inprogress">> => named
        },
        Extra
    ).

%% A counter, and a Lua table that grows and is summed every slot: a state
%% that did not continue exactly from the previous slot reports another sum.
cos_script() ->
    <<
        "Count = Count or 0\n"
        "Big = Big or {}\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  Big[Count] = Count\n"
        "  local sum = 0\n"
        "  for i = 1, #Big do sum = sum + Big[i] end\n"
        "  return {\n"
        "    patches = {\n"
        "      { path = '/count', value = tostring(Count) },\n"
        "      { path = '/sum', value = tostring(sum) }\n"
        "    },\n"
        "    results = { output = { data = tostring(Count) } }\n"
        "  }\n"
        "end\n"
    >>.

cos_process(Opts) ->
    Address =
        hb_util:human_id(
            ar_wallet:to_address(hb_opts:get(<<"priv-wallet">>, undefined, Opts))
        ),
    Process =
        hb_message:commit(
            #{
                <<"device">> => <<"process@1.0">>,
                <<"type">> => <<"Process">>,
                <<"scheduler-device">> => <<"scheduler@1.0">>,
                <<"execution-device">> => <<"lua@5.3b">>,
                <<"module">> =>
                    #{
                        <<"content-type">> => <<"application/lua">>,
                        <<"body">> => cos_script()
                    },
                <<"authority">> => [Address],
                <<"scheduler-location">> => Address,
                <<"test-random-seed">> => rand:uniform(1000000)
            },
            Opts
        ),
    {ok, _} = hb_cache:write(Process, Opts),
    {Process, hb_util:human_id(hb_message:id(Process, all, Opts))}.

cos_schedule(Process, ProcID, Ns, Opts) ->
    lists:foreach(
        fun(N) ->
            {ok, _} =
                hb_ao:resolve(
                    Process,
                    hb_message:commit(
                        #{
                            <<"path">> => <<"schedule">>,
                            <<"method">> => <<"POST">>,
                            <<"body">> =>
                                hb_message:commit(
                                    #{
                                        <<"target">> => ProcID,
                                        <<"type">> => <<"Message">>,
                                        <<"number">> => N
                                    },
                                    Opts
                                )
                        },
                        Opts
                    ),
                    Opts
                )
        end,
        Ns
    ).

%% The newest slot in the process cache, or -1.
cos_latest(ProcID, Opts) ->
    case dev_process_cache:latest(ProcID, Opts) of
        {ok, Slot, _} -> Slot;
        _ -> -1
    end.

cos_await_latest(ProcID, Slot, Opts) ->
    ?assert(
        hb_util:wait_until(fun() -> cos_latest(ProcID, Opts) == Slot end, 60000)
    ).

cos_public(ProcID, Slot, Opts) ->
    {ok, State} = dev_process_cache:read(ProcID, Slot, Opts),
    {
        hb_ao:get(<<"at-slot">>, State, Opts),
        hb_ao:get(<<"count">>, State, Opts),
        hb_ao:get(<<"sum">>, State, Opts),
        hb_ao:get(<<"results/output/data">>, State, Opts)
    }.

%% A hook that reports every compute the catch-up starts, and may stall it.
cos_hook(Test, Ref, StallMs) ->
    fun(ProcID, Slot) ->
        Test ! {cos_compute, Ref, ProcID, Slot},
        timer:sleep(StallMs)
    end.

cos_computes(Ref) ->
    receive {cos_compute, Ref, _, Slot} -> [Slot | cos_computes(Ref)]
    after 0 -> []
    end.

cos_catch_up_idle(ProcID) ->
    ?assert(
        hb_util:wait_until(
            fun() -> hb_name:lookup(catch_up_name(ProcID)) == undefined end,
            30000
        )
    ).

%% @doc With the option on, scheduling alone brings the process cache to the
%% newest assignment: no `compute' or `now' request is made.
compute_on_schedule_computes_test_() ->
    {timeout, 120, fun() ->
        Opts = cos_opts(#{ <<"process-compute-on-schedule">> => true }),
        {Process, ProcID} = cos_process(Opts),
        cos_schedule(Process, ProcID, lists:seq(1, 5), Opts),
        cos_await_latest(ProcID, 4, Opts),
        ?assertEqual({4, <<"5">>, <<"15">>, <<"5">>}, cos_public(ProcID, 4, Opts)),
        cos_catch_up_idle(ProcID)
    end}.

%% @doc With the option off -- or a list that does not name the process --
%% scheduling computes nothing, as before.
compute_on_schedule_off_test_() ->
    {timeout, 120, fun() ->
        lists:foreach(
            fun(Setting) ->
                Opts = cos_opts(Setting),
                {Process, ProcID} = cos_process(Opts),
                cos_schedule(Process, ProcID, lists:seq(1, 3), Opts),
                timer:sleep(1500),
                ?assertEqual(-1, cos_latest(ProcID, Opts)),
                ?assertEqual(undefined, hb_name:lookup(catch_up_name(ProcID)))
            end,
            [
                #{},
                #{ <<"process-compute-on-schedule">> => false },
                #{ <<"process-compute-on-schedule">> =>
                    [hb_util:human_id(crypto:strong_rand_bytes(32))] }
            ]
        ),
        % A list that names the process enables it for that process.
        Opts = cos_opts(#{}),
        {Process, ProcID} = cos_process(Opts),
        Listed = Opts#{ <<"process-compute-on-schedule">> => [ProcID] },
        cos_schedule(Process, ProcID, lists:seq(1, 3), Listed),
        cos_await_latest(ProcID, 2, Opts)
    end}.

%% @doc A burst of schedules is coalesced: while the catch-up computes, later
%% wakes only raise its target, so 50 assignments cost a few computes.
compute_on_schedule_coalesces_test_() ->
    {timeout, 180, fun() ->
        Ref = make_ref(),
        Opts =
            cos_opts(#{
                <<"process-compute-on-schedule">> => true,
                <<"process-compute-on-schedule-hook">> =>
                    cos_hook(self(), Ref, 250)
            }),
        {Process, ProcID} = cos_process(Opts),
        cos_schedule(Process, ProcID, lists:seq(1, 50), Opts),
        cos_await_latest(ProcID, 49, Opts),
        cos_catch_up_idle(ProcID),
        Computes = cos_computes(Ref),
        ?event(debug_test, {computes, Computes}),
        ?assert(length(Computes) >= 1),
        ?assert(length(Computes) =< 10),
        % Targets only ever rise, and the last one is the newest assignment.
        ?assertEqual(Computes, lists:usort(Computes)),
        ?assertEqual(49, lists:last(Computes)),
        ?assertEqual({49, <<"50">>, <<"1275">>, <<"50">>}, cos_public(ProcID, 49, Opts))
    end}.

%% @doc The state computed on schedule (async checkpoints on, a checkpoint
%% every 4 slots) is the state a lazy `compute' of the same schedule yields.
compute_on_schedule_matches_lazy_test_() ->
    {timeout, 180, fun() ->
        Base =
            #{
                <<"process-async-checkpoints">> => true,
                <<"process-delta-checkpoint-slots">> => 4
            },
        Eager = cos_opts(Base#{ <<"process-compute-on-schedule">> => true }),
        {EagerProcess, EagerID} = cos_process(Eager),
        cos_schedule(EagerProcess, EagerID, lists:seq(1, 12), Eager),
        cos_await_latest(EagerID, 11, Eager),
        Lazy = cos_opts(Base),
        {LazyProcess, LazyID} = cos_process(Lazy),
        cos_schedule(LazyProcess, LazyID, lists:seq(1, 12), Lazy),
        ?assertEqual(-1, cos_latest(LazyID, Lazy)),
        {ok, _} =
            hb_ao:resolve(
                LazyProcess,
                #{ <<"path">> => <<"compute">>, <<"slot">> => 11 },
                Lazy
            ),
        lists:foreach(
            fun(Slot) ->
                ?assertEqual(
                    cos_public(LazyID, Slot, Lazy),
                    cos_public(EagerID, Slot, Eager)
                )
            end,
            lists:seq(0, 11)
        ),
        ?assertEqual({11, <<"12">>, <<"78">>, <<"12">>}, cos_public(EagerID, 11, Eager))
    end}.

%% @doc With no live worker, a wake whose target is further from the newest
%% checkpoint than `process-compute-on-schedule-replay-limit' is skipped without
%% computing; within the limit, the same wake computes. The historical read
%% limit plays no part.
compute_on_schedule_respects_cold_bound_test_() ->
    {timeout, 120, fun() ->
        Ref = make_ref(),
        Opts =
            cos_opts(#{
                <<"spawn-worker">> => false,
                <<"process-workers">> => false,
                <<"process-compute-on-schedule-hook">> => cos_hook(self(), Ref, 0)
            }),
        {Process, ProcID} = cos_process(Opts),
        % Slots 0-4 are assigned and never computed: no checkpoint exists.
        cos_schedule(Process, ProcID, lists:seq(1, 5), Opts),
        Bounded =
            Opts#{
                <<"process-compute-on-schedule">> => true,
                <<"process-compute-on-schedule-replay-limit">> => 2,
                <<"process-historical-replay-limit">> => infinity
            },
        cos_schedule(Process, ProcID, [6], Bounded),
        cos_catch_up_idle(ProcID),
        timer:sleep(500),
        ?assertEqual([], cos_computes(Ref)),
        ?assertEqual(-1, cos_latest(ProcID, Opts)),
        cos_schedule(
            Process,
            ProcID,
            [7],
            Bounded#{
                <<"process-compute-on-schedule-replay-limit">> => infinity,
                <<"process-historical-replay-limit">> => 2
            }
        ),
        cos_await_latest(ProcID, 6, Opts),
        ?assertEqual([6], cos_computes(Ref))
    end}.

%% @doc With a live worker, a catch-up asks the worker directly: each new
%% assignment costs exactly one executed slot (no replay), every slot comes out
%% as the lazy path computes it, and the catch-up is answered without the state.
compute_on_schedule_live_worker_test_() ->
    {timeout, 180, fun() ->
        Ref = make_ref(),
        Opts =
            cos_opts(#{
                <<"process-compute-on-schedule">> => true,
                <<"process-compute-on-schedule-hook">> => cos_hook(self(), Ref, 0)
            }),
        {Process, ProcID} = cos_process(Opts),
        cos_schedule(Process, ProcID, lists:seq(1, 3), Opts),
        cos_await_latest(ProcID, 2, Opts),
        cos_catch_up_idle(ProcID),
        ?assert(is_pid(await_worker(ProcID, 50))),
        _ = cos_computes(Ref),
        {_, Executed} =
            count_executed_slots(
                fun() ->
                    lists:foreach(
                        fun(N) ->
                            cos_schedule(Process, ProcID, [N], Opts),
                            % The acceptance test: computed within 5 s with no
                            % compute or now request.
                            ?assert(
                                hb_util:wait_until(
                                    fun() -> cos_latest(ProcID, Opts) == N - 1 end,
                                    5000
                                )
                            )
                        end,
                        lists:seq(4, 8)
                    ),
                    cos_catch_up_idle(ProcID)
                end
            ),
        ?assertEqual(5, Executed),
        ?assertEqual({7, <<"8">>, <<"36">>, <<"8">>}, cos_public(ProcID, 7, Opts)),
        ?assertEqual([3, 4, 5, 6, 7], lists:usort(cos_computes(Ref)))
    end}.

%% @doc A catch-up's answer never carries the state, and a catch-up for a slot
%% the worker already holds computes nothing.
catch_up_reply_is_small_test() ->
    Big = #{ <<"at-slot">> => 9, <<"data">> => binary:copy(<<"x">>, 100000) },
    CatchUp = #{ <<"path">> => <<"compute">>, <<"slot">> => 9, <<"process-catch-up">> => true },
    send_notification(self(), g, 9, {ok, Big}, CatchUp),
    receive {resolved, _, g, {slot, 9}, {ok, Small}} ->
        ?assertEqual(#{ <<"process-catch-up">> => <<"computed">> }, Small)
    after 1000 -> ?assert(false)
    end,
    send_notification(self(), g, 9, {ok, Big}, maps:remove(<<"process-catch-up">>, CatchUp)),
    receive {resolved, _, g, {slot, 9}, {ok, Full}} ->
        ?assertEqual(maps:get(<<"data">>, Big), maps:get(<<"data">>, Full))
    after 1000 -> ?assert(false)
    end,
    ?assert(catch_up_covered(CatchUp, 9, Big)),
    ?assert(catch_up_covered(CatchUp, 3, Big)),
    ?assertNot(catch_up_covered(CatchUp, 10, Big)),
    ?assertNot(catch_up_covered(maps:remove(<<"process-catch-up">>, CatchUp), 3, Big)),
    ?assertNot(catch_up_covered(CatchUp, 3, #{})).

%% @doc The catch-up's replay limit defaults to twice the snapshot cadence, or
%% 2000 without an integer one, and an explicit setting wins.
compute_on_schedule_replay_limit_default_test() ->
    ?assertEqual(2000, catch_up_replay_limit(#{})),
    ?assertEqual(
        2000,
        catch_up_replay_limit(#{ <<"process-snapshot-slots">> => <<"x">> })
    ),
    ?assertEqual(
        2000,
        catch_up_replay_limit(#{ <<"process-historical-replay-limit">> => 64 })
    ),
    ?assertEqual(
        2000,
        catch_up_replay_limit(#{ <<"process-snapshot-slots">> => 1000 })
    ),
    ?assertEqual(
        100,
        catch_up_replay_limit(#{ <<"process-snapshot-slots">> => 50 })
    ),
    ?assertEqual(
        infinity,
        catch_up_replay_limit(#{
            <<"process-snapshot-slots">> => 1000,
            <<"process-compute-on-schedule-replay-limit">> => infinity
        })
    ),
    ?assertEqual(
        10,
        catch_up_replay_limit(#{
            <<"process-snapshot-slots">> => 1000,
            <<"process-compute-on-schedule-replay-limit">> => 10
        })
    ).

%% @doc Regression: a live worker must compute only the slots it has not yet
%% computed, whatever traffic reaches the process between schedules. Prod
%% options: compute on schedule, async checkpoints, `now' from the cache, no
%% checkpoint but the initial one.
worker_keeps_live_state_under_traffic_test_() ->
    {timeout, 300, fun() ->
        Opts =
            cos_opts(#{
                <<"process-compute-on-schedule">> => true,
                <<"process-async-checkpoints">> => true,
                <<"process-now-from-cache">> => true,
                <<"process-historical-replay-limit">> => 64,
                <<"process-snapshot-slots">> => 1000,
                <<"process-delta-checkpoint-slots">> => 1000
            }),
        {Process, ProcID} = cos_process(Opts),
        cos_schedule(Process, ProcID, lists:seq(1, 20), Opts),
        cos_await_latest(ProcID, 19, Opts),
        cos_catch_up_idle(ProcID),
        Worker = await_worker(ProcID, 50),
        ?assert(is_pid(Worker)),
        Step =
            fun(Label, Traffic, Next) ->
                {_, Executed} =
                    count_executed_slots(
                        fun() ->
                            Traffic(),
                            cos_schedule(Process, ProcID, [Next], Opts),
                            cos_await_latest(ProcID, Next - 1, Opts),
                            cos_catch_up_idle(ProcID)
                        end
                    ),
                ?event(debug_test, {step, Label, {executed, Executed}}),
                {Label, Executed}
            end,
        Results =
            [
                Step(catch_up, fun() -> ok end, 21),
                Step(
                    now,
                    fun() ->
                        {ok, _} =
                            hb_ao:resolve(
                                Process,
                                #{ <<"path">> => <<"now">> },
                                Opts
                            )
                    end,
                    22
                ),
                Step(
                    now_key,
                    fun() ->
                        hb_ao:resolve(
                            Process,
                            #{ <<"path">> => <<"now/at-slot">> },
                            Opts
                        )
                    end,
                    23
                ),
                Step(
                    now_missing_key,
                    fun() ->
                        hb_ao:resolve(
                            Process,
                            #{ <<"path">> => <<"now/no-such-key">> },
                            Opts
                        )
                    end,
                    24
                ),
                Step(
                    old_slot,
                    fun() ->
                        {ok, _} =
                            hb_ao:resolve(
                                Process,
                                #{ <<"path">> => <<"compute">>, <<"slot">> => 5 },
                                Opts
                            )
                    end,
                    25
                ),
                Step(
                    head_slot,
                    fun() ->
                        {ok, _} =
                            hb_ao:resolve(
                                Process,
                                #{ <<"path">> => <<"compute">>, <<"slot">> => 23 },
                                Opts
                            )
                    end,
                    26
                )
            ],
        ?assertEqual(
            [
                {catch_up, 1},
                {now, 1},
                {now_key, 1},
                {now_missing_key, 1},
                {old_slot, 1},
                {head_slot, 1}
            ],
            Results
        ),
        ?assertEqual(Worker, hb_name:lookup(ProcID))
    end}.

%% @doc Regression: a worker the compute-on-schedule catch-up spawns is the
%% worker an HTTP request would have spawned. The catch-up resolved under the
%% scheduler call's options -- `hb_ao''s temporary options stripped, plus the
%% keys of whatever woke it -- and a worker keeps the options it was spawned
%% with for every slot it computes, for up to `process-worker-max-idle'. Over a
%% real HTTP node, process A is computed on schedule (its worker spawned by the
%% catch-up) and process B by HTTP `compute' requests (its worker spawned by
%% the first one). The two spawns' options must be the same, and so must each
%% worker's store traffic per slot: writes, bytes written, links and reads.
catch_up_spawns_request_worker_test_() ->
    {timeout, 300, fun() ->
        test_init(),
        Store = hb_test_utils:test_store(hb_store_lmdb),
        Base =
            #{
                <<"store">> => Store,
                <<"priv-wallet">> => ar_wallet:new(),
                <<"spawn-worker">> => true,
                <<"process-workers">> => true,
                <<"await-inprogress">> => named,
                <<"process-async-checkpoints">> => true,
                <<"process-now-from-cache">> => true,
                <<"process-historical-replay-limit">> => 64,
                <<"process-snapshot-slots">> => 1000,
                <<"process-delta-checkpoint-slots">> => 1000
            },
        {ProcA, IDA} = cos_process(Base),
        {ProcB, IDB} = cos_process(Base),
        Node =
            hb_http_server:start_node(
                Base#{ <<"process-compute-on-schedule">> => [IDA] }
            ),
        Client = #{ <<"priv-wallet">> => ar_wallet:new(), <<"store">> => Store },
        Tracer = spawn(fun() -> trace_collector([]) end),
        StartWorker = {hb_persistent, start_worker, 3},
        Spawns =
            try
                erlang:trace_pattern(StartWorker, true, [global]),
                erlang:trace(new_processes, true, [call, {tracer, Tracer}]),
                {ok, _} = hb_http:post(Node, <<"/schedule">>, ProcA, Client),
                {ok, _} = hb_http:post(Node, <<"/schedule">>, ProcB, Client),
                lists:foreach(
                    fun(N) ->
                        http_schedule(Node, IDA, N, Client),
                        http_schedule(Node, IDB, N, Client)
                    end,
                    lists:seq(1, 3)
                ),
                cos_await_latest(IDA, 3, Base),
                cos_catch_up_idle(IDA),
                {ok, _} = http_compute(Node, IDB, 3, Client),
                trace_collected(Tracer)
            after
                erlang:trace(new_processes, false, [call]),
                erlang:trace(all, false, [call]),
                erlang:trace_pattern(StartWorker, false, [global]),
                exit(Tracer, kill)
            end,
        SpawnOpts =
            fun(ID) ->
                [Opts] =
                    [ O || {_, {hb_persistent, start_worker, [G, _, O]}} <- Spawns,
                           G == ID ],
                Opts
            end,
        OptsA = SpawnOpts(IDA),
        OptsB = SpawnOpts(IDB),
        ?assertEqual(<<"true">>, hb_util:bin(maps:get(<<"spawn-worker">>, OptsA))),
        WorkerA = await_worker(IDA, 50),
        WorkerB = await_worker(IDB, 50),
        ?assert(is_pid(WorkerA)),
        ?assert(is_pid(WorkerB)),
        % Store traffic of each worker, one slot at a time: A's slot is
        % computed by the catch-up its schedule wakes, B's by an HTTP compute.
        PerSlot =
            [
                {
                    Slot,
                    worker_store_traffic(
                        WorkerA,
                        fun() ->
                            http_schedule(Node, IDA, Slot, Client),
                            cos_await_latest(IDA, Slot, Base),
                            cos_catch_up_idle(IDA)
                        end
                    ),
                    worker_store_traffic(
                        WorkerB,
                        fun() ->
                            http_schedule(Node, IDB, Slot, Client),
                            {ok, _} = http_compute(Node, IDB, Slot, Client)
                        end
                    )
                }
            ||
                Slot <- lists:seq(4, 8)
            ],
        ?event(debug_test, {per_slot_store_traffic, PerSlot}),
        lists:foreach(
            fun({Slot, A, B}) ->
                ?assertMatch(#{ write := {W, _} } when W > 0, A),
                ?assertEqual({Slot, B}, {Slot, A})
            end,
            PerSlot
        ),
        ?assertEqual(
            [],
            [ {K, maps:get(K, OptsA, absent), maps:get(K, OptsB, absent)}
            ||
                K <- lists:usort(maps:keys(OptsA) ++ maps:keys(OptsB)),
                maps:get(K, OptsA, absent) =/= maps:get(K, OptsB, absent)
            ]
        ),
        % Each worker kept its live state throughout.
        ?assertEqual(WorkerA, hb_name:lookup(IDA)),
        ?assertEqual(WorkerB, hb_name:lookup(IDB)),
        ?assertEqual({8, <<"9">>, <<"45">>, <<"9">>}, cos_public(IDA, 8, Base)),
        ?assertEqual(cos_public(IDA, 8, Base), cos_public(IDB, 8, Base))
    end}.

%% @doc Without an HTTP server the catch-up keeps the options it was given,
%% adding `spawn-worker' only when process workers are on.
catch_up_opts_without_http_server_test() ->
    Off = #{ <<"process-workers">> => false },
    ?assertEqual(Off, catch_up_opts(Off)),
    On = #{ <<"process-workers">> => true },
    ?assertEqual(On#{ <<"spawn-worker">> => true }, catch_up_opts(On)).

http_schedule(Node, ProcID, N, Client) ->
    {ok, _} =
        hb_http:post(
            Node,
            <<ProcID/binary, "~process@1.0/schedule">>,
            hb_message:commit(
                #{
                    <<"target">> => ProcID,
                    <<"type">> => <<"Message">>,
                    <<"number">> => N
                },
                Client
            ),
            Client
        ).

http_compute(Node, ProcID, Slot, Client) ->
    hb_http:get(
        Node,
        <<ProcID/binary, "~process@1.0/compute&slot=",
            (hb_util:bin(Slot))/binary>>,
        Client
    ).

%% A trace receiver that keeps every call it is sent, in order.
trace_collector(Acc) ->
    receive
        {trace, _Pid, call, MFA} -> trace_collector([MFA | Acc]);
        {collect, From, Ref} ->
            From ! {Ref, lists:reverse(Acc)},
            trace_collector([])
    end.

%% The calls collected so far, once every trace message sent before this
%% call has been delivered.
trace_collected(Tracer) ->
    Ref = erlang:trace_delivered(all),
    receive {trace_delivered, all, Ref} -> ok after 10000 -> ok end,
    Tracer ! {collect, self(), CRef = make_ref()},
    receive {CRef, Calls} -> [ {Tracer, C} || C <- Calls ]
    after 10000 -> []
    end.

%% The store calls `Worker' makes while `Fun' runs: for each of `write',
%% `link' and `read', the number of calls and the bytes of binary values
%% written.
worker_store_traffic(Worker, Fun) ->
    Tracer = spawn(fun() -> trace_collector([]) end),
    % With `process-state-blobs' a slot is one exact-key row, written past
    % `hb_store:write/3': count it as a write too.
    MFAs = [ {hb_store, F, 3} || F <- [write, link, read] ]
        ++ [ {hb_store_lmdb, exact_put, 3} ],
    Calls =
        try
            [ erlang:trace_pattern(MFA, true, [global]) || MFA <- MFAs ],
            erlang:trace(Worker, true, [call, {tracer, Tracer}]),
            Fun(),
            erlang:trace(Worker, false, [call]),
            trace_collected(Tracer)
        after
            [ erlang:trace_pattern(MFA, false, [global]) || MFA <- MFAs ],
            exit(Tracer, kill)
        end,
    lists:foldl(
        fun({_, {hb_store_lmdb, exact_put, [_, _Key, Value]}}, Acc) ->
            Bytes = byte_size(Value),
            maps:update_with(write, fun({N, B}) -> {N + 1, B + Bytes} end, {1, Bytes}, Acc);
           ({_, {hb_store, F, [_, Req, _]}}, Acc) ->
            Bytes =
                case {F, Req} of
                    {write, #{}} ->
                        lists:sum(
                            [ byte_size(V) || V <- maps:values(Req), is_binary(V) ]
                        );
                    _ -> 0
                end,
            maps:update_with(
                F,
                fun({N, B}) -> {N + 1, B + Bytes} end,
                {1, Bytes},
                Acc
            )
        end,
        #{},
        Calls
    ).

%% @doc Regression: the catch-up's last look at the scheduler named
%% `dev_scheduler_server', which is loaded under a package-specific name, and
%% raised `undef' at the end of every catch-up.
scheduler_current_test() ->
    Server =
        spawn(fun() ->
            receive {current, Ref, Reply} -> Reply ! {current, Ref, 41} end
        end),
    ?assertEqual(41, scheduler_current(Server)),
    Dead = spawn(fun() -> ok end),
    timer:sleep(50),
    ?assertEqual(-1, scheduler_current(Dead)),
    ?assertEqual(-1, scheduler_current(undefined)).

%%% Forced worker collections (`process-worker-gc').

%% @doc Every spelling of the policy, and the fallbacks.
gc_policy_parse_test() ->
    Both = #{ checkpoint => true, idle => true },
    None = #{ checkpoint => false, idle => false },
    ?assertEqual(Both, gc_policy(#{})),
    ?assertEqual(None, gc_policy(#{ <<"process-worker-gc">> => <<"off">> })),
    ?assertEqual(None, gc_policy(#{ <<"process-worker-gc">> => false })),
    ?assertEqual(
        #{ checkpoint => true, idle => false },
        gc_policy(#{ <<"process-worker-gc">> => checkpoint })
    ),
    ?assertEqual(
        #{ checkpoint => false, idle => true },
        gc_policy(#{ <<"process-worker-gc">> => <<"idle">> })
    ),
    ?assertEqual(
        Both,
        gc_policy(#{ <<"process-worker-gc">> => [<<"idle">>, <<"checkpoint">>] })
    ),
    ?assertEqual(Both, gc_policy(#{ <<"process-worker-gc">> => "checkpoint+idle" })),
    ?assertEqual(Both, gc_policy(#{ <<"process-worker-gc">> => <<"bogus">> })),
    ?assertEqual(5000, idle_gc_ms(#{})),
    ?assertEqual(
        250,
        idle_gc_ms(#{ <<"process-worker-gc-idle-ms">> => <<"250">> })
    ),
    ?assertEqual(
        false,
        idle_gc_ms(#{
            <<"process-worker-gc">> => <<"checkpoint">>,
            <<"process-worker-gc-idle-ms">> => 250
        })
    ).

%% A counter process (checkpoints every 10 slots) whose worker reports every
%% forced collection to the test. Slot 0 is computed by the requester, which
%% then hands the state to a new worker; every later slot is the worker's.
gc_counter(Count, Extra) ->
    Test = self(),
    Ref = make_ref(),
    {Process, Opts} =
        counter_process(
            Count,
            Extra#{
                <<"spawn-worker">> => true,
                <<"process-workers">> => true,
                <<"await-inprogress">> => named,
                <<"process-worker-gc-hook">> =>
                    fun(Reason) -> Test ! {worker_gc, Ref, Reason} end
            }
        ),
    {ok, _} = gc_compute(Process, 0, Opts),
    Group = hb_util:human_id(hb_message:id(Process, all, Opts)),
    ?assert(is_pid(await_worker(Group, 50))),
    % The new worker may also be handed the request that elected it, which
    % re-stores slot 0 (a checkpoint); let it settle, then start counting.
    timer:sleep(1000),
    gc_events(Ref),
    {Process, Ref, Opts}.

gc_compute(Process, Slot, Opts) ->
    bounded_resolve(
        Process,
        #{ <<"path">> => <<"compute">>, <<"slot">> => Slot },
        Opts
    ).

%% Slots `From'..`To' one request each, as the swarm drives a game process,
%% returning each slot's public state.
gc_compute_each(Process, Slots, Opts) ->
    [
        begin
            {ok, S} = gc_compute(Process, Slot, Opts),
            {
                hb_ao:get(<<"at-slot">>, S, Opts),
                hb_ao:get(<<"count">>, S, Opts),
                hb_ao:get(<<"results/output/data">>, S, Opts)
            }
        end
    ||
        Slot <- Slots
    ].

%% The forced collections reported so far, in order. A collection runs just
%% after the worker answers, so give the last one a moment to report.
gc_events(Ref) ->
    timer:sleep(300),
    gc_reported(Ref).

gc_reported(Ref) ->
    receive {worker_gc, Ref, Reason} -> [Reason | gc_reported(Ref)]
    after 0 -> []
    end.

%% @doc Off: no forced collection, however many checkpoints or idle waits.
worker_gc_off_test_() ->
    {timeout, 180, fun() ->
        {Process, Ref, Opts} =
            gc_counter(26, #{
                <<"process-worker-gc">> => <<"off">>,
                <<"process-worker-gc-idle-ms">> => 0
            }),
        gc_compute_each(Process, lists:seq(1, 25), Opts),
        timer:sleep(500),
        ?assertEqual([], gc_events(Ref))
    end}.

%% @doc Checkpoint mode: exactly one collection per checkpoint the worker
%% hands off (slots 10 and 20), synchronous or asynchronous.
worker_gc_per_checkpoint_test_() ->
    {timeout, 300, fun() ->
        lists:foreach(
            fun(Async) ->
                {Process, Ref, Opts} =
                    gc_counter(26, #{
                        <<"process-worker-gc">> => <<"checkpoint">>,
                        <<"process-async-checkpoints">> => Async
                    }),
                gc_compute_each(Process, lists:seq(1, 9), Opts),
                ?assertEqual([], gc_events(Ref)),
                gc_compute_each(Process, [10], Opts),
                ?assertEqual([checkpoint], gc_events(Ref)),
                gc_compute_each(Process, lists:seq(11, 25), Opts),
                ?assertEqual([checkpoint], gc_events(Ref))
            end,
            [false, true]
        )
    end}.

%% @doc Idle mode: one collection once the worker has been idle, none more
%% while it stays idle, and one again after it next works and goes idle.
worker_gc_once_per_idle_period_test_() ->
    {timeout, 180, fun() ->
        {Process, Ref, Opts} =
            gc_counter(26, #{
                <<"process-worker-gc">> => <<"idle">>,
                <<"process-worker-gc-idle-ms">> => 200
            }),
        gc_compute_each(Process, [1], Opts),
        timer:sleep(1000),
        ?assertEqual([idle], gc_events(Ref)),
        timer:sleep(1000),
        ?assertEqual([], gc_events(Ref)),
        % Checkpoints (slot 10) collect nothing in idle mode.
        gc_compute_each(Process, lists:seq(2, 12), Opts),
        timer:sleep(1000),
        ?assertEqual([idle], gc_events(Ref))
    end}.

%% @doc Both: a checkpoint's collection leaves nothing for the idle one until
%% the worker computes again.
worker_gc_checkpoint_then_idle_test_() ->
    {timeout, 180, fun() ->
        {Process, Ref, Opts} =
            gc_counter(26, #{
                <<"process-worker-gc">> => <<"checkpoint+idle">>,
                <<"process-worker-gc-idle-ms">> => 200
            }),
        gc_compute_each(Process, lists:seq(1, 10), Opts),
        timer:sleep(1000),
        ?assertEqual([checkpoint], gc_events(Ref)),
        gc_compute_each(Process, [11], Opts),
        timer:sleep(1000),
        ?assertEqual([idle], gc_events(Ref))
    end}.

%% @doc The policy never changes a result: the same slots give the same public
%% states with forced collections off and as often as they can run.
worker_gc_results_unchanged_test_() ->
    {timeout, 300, fun() ->
        Run =
            fun(Policy) ->
                {Process, _Ref, Opts} =
                    gc_counter(26, #{
                        <<"process-worker-gc">> => Policy,
                        <<"process-worker-gc-idle-ms">> => 0,
                        <<"process-async-checkpoints">> => true
                    }),
                gc_compute_each(Process, lists:seq(1, 25), Opts)
            end,
        Off = Run(<<"off">>),
        ?assertEqual(Off, Run(<<"checkpoint+idle">>)),
        ?assertEqual({25, <<"26">>, <<"26">>}, lists:last(Off))
    end}.

%%% End to end, through the HTTP server, as clients drive the production
%%% node: each slot is scheduled over HTTP, its `results' read back over HTTP
%%% (`compute&slot=N/results': the reply is signed and encoded), and a `now'
%%% key read. The chain goes to its own `essentials-store', as on that node.
%%% With `process-state-blobs', no slot's `results' is left in the main store
%%% outside the process state rows, and those stop growing. Slots per run:
%%% HB_E2E_PLATEAU_SLOTS (default 1500). The total main store is printed too:
%%% each HTTP request still writes some request messages of its own there.

plateau_script() ->
    <<
        "Count = Count or 0\n"
        "Seen = Seen or {}\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  local k = 'p' .. (Count % 500)\n"
        "  Seen[k] = Count\n"
        "  if Count % 50 == 0 then collectgarbage('collect') end\n"
        "  return {\n"
        "    patches = {\n"
        "      { path = '/count', value = tostring(Count) },\n"
        "      { path = '/' .. k, value = tostring(Count) }\n"
        "    },\n"
        "    results = {\n"
        "      output = { data = tostring(Count) },\n"
        "      outbox = { ['1'] = { target = 'peer', action = 'Note',\n"
        "        data = string.rep('m', 2000) .. tostring(Count) } }\n"
        "    }\n"
        "  }\n"
        "end\n"
    >>.

%% The main store: {all rows, all bytes, rows outside `pstate/' holding a
%% slot's outbox payload, `pstate/' rows, `pstate/' bytes}.
plateau_stats(Store) ->
    #{ <<"db">> := DB } = hb_store:find(Store),
    {ok, Acc} = elmdb:fold(DB,
        fun(K, V, {R, B, Res, PR, PB}) ->
            Sz = byte_size(K) + byte_size(V),
            case K of
                <<"pstate/", _/binary>> -> {R + 1, B + Sz, Res, PR + 1, PB + Sz};
                _ ->
                    Payload = binary:match(V, binary:copy(<<"m">>, 64)) =/= nomatch,
                    {R + 1, B + Sz, Res + case Payload of true -> 1; false -> 0 end, PR, PB}
            end
        end,
        {0, 0, 0, 0, 0}),
    Acc.

e2e_plateau_run(Blobs, N) ->
    Main = hb_test_utils:test_store(hb_store_lmdb),
    Ess = hb_test_utils:test_store(hb_store_lmdb),
    Base =
        #{
            <<"store">> => Main,
            <<"essentials-store">> => Ess,
            <<"priv-wallet">> => ar_wallet:new(),
            <<"spawn-worker">> => true,
            <<"process-workers">> => true,
            <<"await-inprogress">> => named,
            <<"process-async-checkpoints">> => true,
            <<"process-compute-on-schedule">> => true,
            <<"process-now-from-cache">> => true,
            <<"process-delta-checkpoint-slots">> => 100,
            <<"store-retention-checkpoints">> => 2,
            <<"process-state-blobs">> => Blobs,
            % As on the production node.
            <<"store-all-signed">> => false,
            % One client drives every request: keep it under the limiter.
            <<"rate-limit-requests">> => 1000000,
            <<"rate-limit-max">> => 1000000
        },
    Address = hb_util:human_id(ar_wallet:to_address(maps:get(<<"priv-wallet">>, Base))),
    % The client is separate from the node: its own wallet and store, so its
    % writes are not counted in the node's.
    Client = #{ <<"priv-wallet">> => ar_wallet:new(),
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb) },
    Proc =
        hb_message:commit(
            #{
                <<"device">> => <<"process@1.0">>,
                <<"type">> => <<"Process">>,
                <<"scheduler-device">> => <<"scheduler@1.0">>,
                <<"execution-device">> => <<"lua@5.3b">>,
                <<"module">> => #{ <<"content-type">> => <<"application/lua">>,
                    <<"body">> => plateau_script() },
                <<"authority">> => [Address],
                <<"scheduler-location">> => Address,
                <<"test-random-seed">> => rand:uniform(1000000)
            },
            Client
        ),
    ID = hb_util:human_id(hb_message:id(Proc, all, Client)),
    Node = hb_http_server:start_node(Base),
    {ok, _} = hb_http:post(Node, <<"/schedule">>, Proc, Client),
    lists:filtermap(
        fun(Slot) ->
            http_schedule(Node, ID, Slot, Client),
            {ok, Res} =
                hb_http:get(Node,
                    <<ID/binary, "~process@1.0/compute&slot=",
                        (integer_to_binary(Slot))/binary, "/results">>, Client),
            ?assertEqual(integer_to_binary(Slot + 1),
                hb_ao:get(<<"output/data">>, Res, Client)),
            {ok, _} = hb_http:get(Node, <<ID/binary, "~process@1.0/now/count">>, Client),
            case Slot rem 250 of
                249 ->
                    % Let an in-flight checkpoint writer land first.
                    timer:sleep(500),
                    Stats = {R, B, ResRows, PR, PB} = plateau_stats(Main),
                    {ER, EB, _, _, _} = plateau_stats(Ess),
                    io:format(standard_error,
                        "E2E blobs=~p slot=~p main rows=~p bytes=~p results-rows=~p "
                        "pstate rows=~p bytes=~p | essentials rows=~p bytes=~p~n",
                        [Blobs, Slot, R, B, ResRows, PR, PB, ER, EB]),
                    {true, {Slot, Stats}};
                _ -> false
            end
        end,
        lists:seq(1, N)
    ).

e2e_results_and_state_bounded_http_test_() ->
    {timeout, 3600, fun() ->
        test_init(),
        N =
            case os:getenv("HB_E2E_PLATEAU_SLOTS") of
                false -> 1500;
                S -> list_to_integer(S)
            end,
        Samples = e2e_plateau_run(true, N),
        case os:getenv("HB_E2E_PLATEAU_LEGACY") of
            false -> ok;
            _ -> e2e_plateau_run(false, N)
        end,
        % No slot's results is left behind outside the state rows.
        ?assertEqual([], [ X || X = {_, {_, _, ResRows, _, _}} <- Samples, ResRows > 0 ]),
        % The state rows stop growing: the second half of the run reaches no
        % higher peak than the first. (Samples fall at different points of the
        % checkpoint cycle, so compare peaks; bytes get 5% because the script's
        % state fills its 500 keys at slot 500.)
        {First, Second} = lists:split(length(Samples) div 2, Samples),
        Peak = fun(L) -> {lists:max([ PR || {_, {_, _, _, PR, _}} <- L ]),
                          lists:max([ PB || {_, {_, _, _, _, PB}} <- L ])} end,
        {R1, B1} = Peak(First),
        {R2, B2} = Peak(Second),
        ?assert(R2 =< R1),
        ?assert(B2 =< B1 * 1.05)
    end}.


%%% @doc A long-lived process worker that keeps state in memory between
%%% calls. Implements the interface of `hb_ao' to receive and respond 
%%% to computation requests regarding a process as a singleton.
-module(dev_process_worker).
-export([server/3, stop/1, group/3, await/5, notify_compute/4]).
-include_lib("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Return a group name for a request. Cached compute reads run
%% ungrouped; uncached compute work groups by process ID; everything
%% else uses the default grouper.
group(Base, undefined, Opts) ->
    hb_persistent:default_grouper(Base, undefined, Opts);
group(Base, Req, Opts) ->
    ProcessWorkers = hb_opts:get(<<"process-workers">>, false, Opts),
    IsCompute = hb_path:matches(<<"compute">>, hb_path:hd(Req, Opts)),
    case ProcessWorkers andalso IsCompute of
        true ->
            compute_group(Base, Req, Opts);
        false ->
            hb_persistent:default_grouper(Base, Req, Opts)
    end.

%% @doc Decide which group to enrol a `compute' request into. Cache-hit
%% reads bypass the per-process queue via `ungrouped_exec'; everything
%% else is serialised through the worker keyed on the process ID.
compute_group(Base, Req, Opts) ->
    ProcID = process_to_group_name(Base, Opts),
    TargetSlot =
        slot_number(dev_process:target_slot(Req, slot_read_opts(Opts))),
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
compute_cached(ProcID, RawSlot, Opts) ->
    % `dev_process:target_slot/2' has already unwrapped any HTTP-wrapped
    % typed-result map, so RawSlot is a scalar by the time it reaches here and
    % this function needs no unwrapper of its own -- the same shape being
    % handled in one place and not the other IS the defect this patch fixes,
    % and a second local implementation is how it survived the first attempt.
    % The `catch' stays as a backstop: a value that still won't coerce means
    % "not cached", and an uncomputed slot is a queue, not a fault.
    % (local patch over upstream edge.)
    case (catch dev_process_cache:read(ProcID, hb_util:int(RawSlot), Opts)) of
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
server(GroupName, Base, Opts) ->
    ServerOpts = Opts#{
        <<"await-inprogress">> => false,
        <<"spawn-worker">> => false,
        <<"process-workers">> => false
    },
    % The maximum amount of time the worker will wait for a request before
    % checking the cache for a snapshot. Default: 5 minutes.
    Timeout = hb_opts:get(process_worker_max_idle, 300_000, Opts),
    ?event(worker, {waiting_for_req, {group, GroupName}}),
    receive
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
                hb_ao:resolve(
                    Base,
                    #{ <<"path">> => <<"compute">>, <<"slot">> => TargetSlot },
                    hb_maps:merge(ListenerOpts, ServerOpts, Opts)
                ),
            ?event(worker, {work_done, {group, GroupName}, {req, Req}, {res, Res}}),
            send_notification(Listener, GroupName, TargetSlot, Res),
            server(
                GroupName,
                case Res of
                    {ok, NewBase} when is_map(NewBase) -> NewBase;
                    _ -> Base
                end,
                Opts
            );
        stop ->
            ?event(worker, {stopping, {group, GroupName}, {base, Base}}),
            exit(normal)
    after Timeout ->
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

%% @doc Read the slot a request is asking for, as an INTEGER.
read_slot(Req, Default, Opts) ->
    slot_number(hb_ao:get(<<"slot">>, Req, Default, slot_read_opts(Opts))).

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
%% survive, `compute_group/3' -- reached from `hb_persistent:await/4', which
%% unlike `find_or_register/3' does NOT strip the temporary options before
%% calling the grouper -- died the same way, turning every read that found a
%% live worker into a 500.
slot_read_opts(Opts) ->
    Opts#{ <<"force-message">> => false }.

%% @doc Narrow a slot to the integer `dev_process' stores and compares it as.
%%
%% The type matters as much as the unwrapping: a slot arrives from HTTP as a
%% binary, `dev_process' counts in integers, and `<<"9">> == 9' is false -- so
%% a worker that notified `{slot, <<"9">>}' would be ignored by a waiter
%% matching `RecvdSlot == 9', which would then block until the worker died.
%%
%% `not_found' and `any' are the two callers' defaults and pass through: they
%% mean "no slot was asked for", which is a real request shape (`compute' with
%% no slot serves the latest state).
%%
%% The map clause is kept even though this fork's `dev_process:target_slot/2'
%% already unwraps one: `read_slot/3' reads a request directly through
%% `hb_ao:get/4' and never passes through that unwrapper at all.
slot_number(not_found) -> not_found;
slot_number(any) -> any;
slot_number(#{ <<"ao-result">> := <<"body">>, <<"body">> := Literal }) ->
    slot_number(Literal);
slot_number(Slot) -> hb_util:int(Slot).

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
                    resolve_notification(Res, GroupName, RecvdSlot, Opts);
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
    BinSlotToNotify = hb_util:bin(SlotToNotify),
    receive
        {resolve, Listener, GroupName, #{ <<"slot">> := Slot }, _ListenerOpts}
                when Slot =:= SlotToNotify; Slot =:= BinSlotToNotify ->
            send_notification(Listener, GroupName, SlotToNotify, Res),
            notify_compute(GroupName, SlotToNotify, Res, Opts, Count + 1);
        {resolve, Listener, GroupName, Msg, _ListenerOpts}
                when is_map(Msg) andalso not is_map_key(<<"slot">>, Msg) ->
            send_notification(Listener, GroupName, SlotToNotify, Res),
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
    ?event({sending_notification, {group, GroupName}, {slot, SlotToNotify}}),
    Listener ! {
        resolved,
        self(),
        GroupName,
        {slot, SlotToNotify},
        notification_result(Res)
    }.

%% @doc A process worker keeps the complete result in its recursive loop and
%% has already made the public result durable before it notifies listeners.
%% Passing the result itself copies the resident VM -- or, after stripping
%% `priv', still copies the complete public process map -- into every listener's
%% heap. Send a cache marker instead, so the worker can start its next slot while
%% each listener reads the public result from the process cache.
notification_result({ok, Msg}) when is_map(Msg) ->
    cached;
notification_result(Res) ->
    Res.

resolve_notification(cached, GroupName, Slot, Opts) ->
    dev_process_cache:read(GroupName, Slot, Opts);
resolve_notification(Res, _GroupName, _Slot, _Opts) ->
    Res.

%% @doc Stop a worker process.
stop(Worker) ->
    exit(Worker, normal).

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

worker_notification_is_public_test() ->
    Opts = #{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"priv-wallet">> => ar_wallet:new()
    },
    Group = hb_util:encode(crypto:strong_rand_bytes(32)),
    Secret = #{ <<"vm">> => lists:seq(1, 1000) },
    Full = #{
        <<"counter">> => <<"1">>,
        <<"priv">> => Secret
    },
    {ok, _} = dev_process_cache:write(Group, 1, Full, Opts),
    send_notification(self(), Group, 1, {ok, Full}),
    receive
        {resolved, _, Group, {slot, 1}, Notification} ->
            {ok, Public} = resolve_notification(Notification, Group, 1, Opts),
            ?assertEqual(<<"1">>, maps:get(<<"counter">>, Public)),
            ?assertNot(maps:is_key(<<"priv">>, Public))
    after 1000 ->
        ?assert(false)
    end,
    % Sanitising the listener response must not alter the state retained by
    % the worker for its next request.
    ?assertEqual(Secret, maps:get(<<"priv">>, Full)).

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
        slot_number(#{ <<"ao-result">> => <<"body">>, <<"body">> => <<"9">> })
    ),
    ?assertEqual(9, slot_number(<<"9">>)),
    ?assertEqual(9, slot_number(9)),
    ?assertEqual(any, slot_number(any)),
    ?assertEqual(not_found, slot_number(not_found)).

%% @doc Regression: `hb_persistent:await/4' calls the grouper with the FULL
%% caller options -- unlike `find_or_register/3', which strips the temporary
%% ones first. So `compute_group/3' saw `force-message' where the register path
%% never did, and every read that found a live worker died there instead of
%% waiting on it. The 500 was served in 4 ms, so the client simply retried, and
%% the round trip became the client's backoff rather than the node's work.
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
            {resolved, _, Group, {slot, NotifiedSlot}, Notification} ->
                ?assertEqual(0, NotifiedSlot),
                Res = resolve_notification(
                    Notification,
                    Group,
                    NotifiedSlot,
                    WorkerOpts
                ),
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

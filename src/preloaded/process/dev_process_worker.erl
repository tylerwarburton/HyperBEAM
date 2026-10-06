
%%% @doc A long-lived process worker that keeps state in memory between
%%% calls. Implements the interface of `hb_ao' to receive and respond 
%%% to computation requests regarding a process as a singleton.
-module(dev_process_worker).
-export([server/3, stop/1, group/3, await/5, notify_compute/4]).
-include_lib("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Request keys a delegated `compute' carries to the worker's resolution.
-define(CARRIED_KEYS,
    [<<"push">>, <<"max-depth">>, <<"result-depth">>, <<"async">>, <<"init">>]
).

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
    % slot, so it is a non-negative integer here. The `catch' stays as a
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
server(GroupName, RawBase, Opts) ->
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
                execute(
                    Base,
                    Req,
                    TargetSlot,
                    hb_maps:merge(ListenerOpts, ServerOpts, Opts)
                ),
            ?event(worker, {work_done, {group, GroupName}, {req, Req}, {res, Res}}),
            send_notification(Listener, GroupName, TargetSlot, Res),
            server(GroupName, next_base(Base, Res), Opts);
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
%% does for `compute/3' and for the grouper: a non-negative integer, `Default'
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
        {resolve, Listener, GroupName, #{ <<"compute">> := Slot }, _ListenerOpts}
                when Slot =:= SlotToNotify; Slot =:= BinSlotToNotify ->
            send_notification(Listener, GroupName, SlotToNotify, Res),
            notify_compute(GroupName, SlotToNotify, Res, Opts, Count + 1);
        {resolve, Listener, GroupName, Msg = #{ <<"slot">> := Slot }, _ListenerOpts}
                when (Slot =:= SlotToNotify orelse Slot =:= BinSlotToNotify)
                    andalso not is_map_key(<<"compute">>, Msg) ->
            send_notification(Listener, GroupName, SlotToNotify, Res),
            notify_compute(GroupName, SlotToNotify, Res, Opts, Count + 1);
        {resolve, Listener, GroupName, Msg, _ListenerOpts}
                when is_map(Msg)
                    andalso not is_map_key(<<"slot">>, Msg)
                    andalso not is_map_key(<<"compute">>, Msg) ->
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
        public_result(Res)
    }.

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
            [<<"-2">>, -1, <<"abc">>]
        ),
        ?assert(is_process_alive(Worker)),
        ?assertEqual(Worker, hb_name:lookup(Group))
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

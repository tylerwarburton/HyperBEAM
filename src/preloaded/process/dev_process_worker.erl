
%%% @doc A long-lived process worker that keeps state in memory between
%%% calls. Implements the interface of `hb_ao' to receive and respond 
%%% to computation requests regarding a process as a singleton.
-module(dev_process_worker).
-export([server/3, stop/1, group/3, await/5, notify_compute/4, delegate/6]).
-export([waiter_rows/1, notify_result/3]).
-include_lib("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Request keys a delegated `compute' carries to the worker's resolution.
-define(CARRIED_KEYS, [<<"init">>]).

%%% Who waits for which slot of which process.
%%%
%%% A waiter for a slot the worker has not computed yet registers itself in
%%% one ETS table, `{{Group, Slot | latest, Ref}, Pid}', and sends the worker
%%% a two-tuple `{need, Group}'. Nothing else goes through the worker's
%%% mailbox: no request, no options, no state. The worker drains every
%%% `{need, _}' into one pass computing to the highest slot any live waiter
%%% asked for, and after each slot it stores (`notify_compute/4') it answers
%%% the rows at or below that slot, walking only those rows in key order --
%%% never a scan of its mailbox. A row whose waiter has died (an HTTP client left) is
%%% deleted when its slot resolves or when the worker next looks; it never
%%% makes the worker compute anything on its own account.
%%%
%%% The worker's queue is `off_heap', so a burst of waiters costs the worker
%%% neither heap nor garbage collection: on prod (2026-11-05) 6,620 queued
%%% `{resolve, Listener, Group, Req, Opts}' tuples, each a copy of the whole
%%% options map, and a full-mailbox selective receive after every slot left
%%% the vault's worker with a 108M-word heap, collecting garbage instead of
%%% computing.
-define(WAITERS, dev_process_worker_waiters).

%% @doc Return a group name for a request. A compute of a slot beyond the
%% newest stored one (the head) groups by process ID; any other compute runs
%% ungrouped; everything else uses the default grouper.
group(Base, undefined, Opts) ->
    hb_persistent:default_grouper(Base, undefined, Opts);
group(Base, Req, Opts) ->
    ProcessWorkers = hb_opts:get(process_workers, false, Opts),
    IsCompute = hb_path:matches(<<"compute">>, hb_path:hd(Req, Opts)),
    case ProcessWorkers andalso IsCompute of
        true ->
            compute_group(Base, Req, Opts);
        false ->
            hb_persistent:default_grouper(Base, Req, Opts)
    end.

%% @doc Decide which group to enrol a `compute' request into. Only work that
%% moves the state forward is serialised through the worker keyed on the
%% process ID; everything else bypasses the per-process queue via
%% `ungrouped_exec'.
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

%% A slot at or below the head is either stored, and read in the requester,
%% or deleted by retention, and refused there with a 410
%% (`dev_process:compute/3'). Neither needs the worker, and neither is worth
%% materializing here to find out which: the requester reads it once.
compute_group(Base, Req, not_found, Opts) ->
    ProcID = process_to_group_name(Base, Opts),
    case lib_process_cache:latest(ProcID, Opts) of
        {ok, _Slot, _Msg} ->
            ?event(worker,
                {compute_cache_hit_bypassing_queue,
                    {proc_id, ProcID},
                    {req, Req}
                },
                Opts
            ),
            ungrouped_exec;
        _ -> ProcID
    end;
compute_group(Base, Req, TargetSlot, Opts) ->
    ProcID = process_to_group_name(Base, Opts),
    case (catch lib_process_cache:latest_slot(ProcID, lib_process:scoped_opts(Opts))) of
        {ok, Head} when is_integer(Head), TargetSlot =< Head ->
            ?event(worker,
                {compute_at_or_below_head_bypassing_queue,
                    {proc_id, ProcID},
                    {slot, TargetSlot},
                    {head, Head},
                    {req, Req}
                },
                Opts
            ),
            ungrouped_exec;
        _ -> ProcID
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
    process_flag(message_queue_data, off_heap),
    ensure_waiters(),
    % The name a compute that reached the process ungrouped hands its slot
    % to (`dev_process:compute/3'); only a worker ever holds it.
    % (A lookup first drops the name of a worker that died.)
    _ = hb_name:lookup({process_worker, GroupName}),
    _ = hb_name:register({process_worker, GroupName}),
    hb_phase_timer:set_process(GroupName),
    % Waiters that registered while the request that started this worker was
    % computing inline sent their `need' to that request, not here.
    loop(GroupName, serve_needs(GroupName, live_base(RawBase), Opts), Opts).

loop(GroupName, Base, Opts) ->
    ServerOpts = server_opts(Opts),
    % The maximum amount of time the worker will wait for a request before
    % checking the cache for a snapshot. Default: 5 minutes.
    Timeout = hb_opts:get(process_worker_max_idle, 300_000, Opts),
    receive
        {need, GroupName} ->
            drain_needs(GroupName),
            Base2 = serve_needs(GroupName, Base, Opts),
            loop(GroupName, Base2, Opts);
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

drain_needs(GroupName) ->
    receive {need, GroupName} -> drain_needs(GroupName)
    after 0 -> ok
    end.

%% The worker computes slots; `lib_process_cache' already stores each one
%% durably (a delta, or a checkpoint), so result caching -- serializing,
%% hashing and writing the whole state on every slot, inside the one process
%% every write to this process queues behind -- is off.
server_opts(Opts) ->
    Opts#{
        <<"await-inprogress">> => false,
        <<"spawn-worker">> => false,
        <<"process-workers">> => false,
        <<"cache-control">> => [<<"no-store">>, <<"no-cache">>]
    }.

%% @doc One pass over the registered waiters of the process: answer those
%% whose slot is behind the live state from the store, compute to the highest
%% slot the live ones asked for, and answer every row at or below it. A row
%% whose slot failed is answered with the failure, so rows never pile up.
serve_needs(GroupName, Base, Opts) ->
    Rows = live_rows(GroupName),
    Current = held_slot(Base),
    {Behind, Ahead} =
        lists:partition(
            fun({S, _, _}) -> is_integer(S) andalso S =< Current end,
            [ R || R = {S, _, _} <- Rows, is_integer(S) ]
        ),
    answer(GroupName, Behind, stored),
    Latest = [ R || R = {latest, _, _} <- Rows ],
    Base2 =
        case Ahead of
            [] -> Base;
            _ ->
                Target = lists:max([ S || {S, _, _} <- Ahead ]),
                {Res, NextBase} = run(GroupName, Base, Target, Opts),
                settle(GroupName, Target, Res, stored_head(GroupName, NextBase, Opts)),
                NextBase
        end,
    case Latest of
        [] -> Base2;
        _ ->
            {LRes, LBase} = run(GroupName, Base2, not_found, Opts),
            answer(GroupName, Latest, LRes),
            LBase
    end.

run(GroupName, Base, Target, Opts) ->
    ?event(worker, {work_received, {group, GroupName}, {slot, Target}}),
    {ExecMicros, Res} =
        timer:tc(fun() ->
            execute(Base, #{}, Target, slot_opts(server_opts(Opts)))
        end),
    % The worker's busy time per request: what every message to this
    % process queues behind (`bench/game_slot_bench.erl').
    hb_phase_timer:record(worker_execute, ExecMicros),
    {Res, next_base(Base, Res)}.

%% After computing to `Target': the rows for it get its result; rows below
%% it that the slots' own notifications did not reach are read from the
%% store; rows above the newest stored slot (a failure stopped the run short)
%% get the failure. A failed run keeps the state it started from, but the
%% slots it stored before failing are stored: the store, not the state, says
%% how far it got.
stored_head(GroupName, Base, Opts) ->
    case catch lib_process_cache:latest_slot(GroupName, lib_process:scoped_opts(Opts)) of
        {ok, Head} when is_integer(Head) -> max(Head, held_slot(Base));
        _ -> held_slot(Base)
    end.

settle(GroupName, Target, Res, Reached) ->
    Rows = [ R || R = {S, _, _} <- rows(GroupName), is_integer(S), S =< Target ],
    lists:foreach(
        fun(Row = {S, _, _}) ->
            Answer =
                if S == Target -> Res;
                   S =< Reached -> stored;
                   true -> failed(Res)
                end,
            answer(GroupName, [Row], Answer)
        end,
        Rows
    ).

failed({ok, _}) -> {error, #{ <<"status">> => 500, <<"body">> => <<"Slot not reached.">> }};
failed(Res) -> Res.

held_slot(Base) ->
    case hb_maps:get(<<"at-slot">>, Base, -1, #{}) of
        S when is_integer(S) -> S;
        _ -> -1
    end.

%% @doc Send each row its answer and delete it.
answer(GroupName, Rows, Answer) ->
    Msg =
        case Answer of
            stored -> stored;
            _ -> public_result(Answer)
        end,
    lists:foreach(
        fun({S, Ref, Pid}) ->
            Pid ! {ready, Ref, Msg},
            ets:delete(?WAITERS, {GroupName, S, Ref})
        end,
        Rows
    ).

%% @doc The rows of a process for slots at or below `Slot', walked in key
%% order from the lowest: every integer sorts before `latest' and before any
%% reference, so the walk visits exactly the rows it returns and one more.
rows_upto(GroupName, Slot) ->
    try rows_upto(GroupName, Slot, ets:next(?WAITERS, {GroupName, -2, 0}))
    catch error:badarg -> []
    end.

rows_upto(GroupName, Slot, Key = {GroupName, S, Ref}) when is_integer(S), S =< Slot ->
    Next = ets:next(?WAITERS, Key),
    case ets:lookup(?WAITERS, Key) of
        [{_, Pid}] -> [{S, Ref, Pid} | rows_upto(GroupName, Slot, Next)];
        [] -> rows_upto(GroupName, Slot, Next)
    end;
rows_upto(_GroupName, _Slot, _) ->
    [].

%% @doc Every row of a process, as `{Slot | latest, Ref, Pid}'.
rows(GroupName) ->
    try
        ets:select(?WAITERS,
            [{{{GroupName, '$1', '$2'}, '$3'}, [], [{{'$1', '$2', '$3'}}]}])
    catch error:badarg -> []
    end.

%% @doc The rows of a process whose waiter is alive; the others are deleted.
live_rows(GroupName) ->
    lists:filter(
        fun({S, Ref, Pid}) ->
            case is_process_alive(Pid) of
                true -> true;
                false -> ets:delete(?WAITERS, {GroupName, S, Ref}), false
            end
        end,
        rows(GroupName)
    ).

%% @doc The number of registered waiters of a process.
waiter_rows(GroupName) -> length(rows(GroupName)).

%% The table is owned by a process that never exits: a table dies with its
%% owner, and the first caller is usually short-lived.
ensure_waiters() ->
    case ets:whereis(?WAITERS) of
        undefined ->
            Alias = erlang:alias([reply]),
            {Owner, Mon} =
                spawn_monitor(fun() ->
                    try ets:new(?WAITERS, [named_table, public, ordered_set,
                            {write_concurrency, true}]) of
                        _ ->
                            Alias ! {Alias, created},
                            receive after infinity -> ok end
                    catch error:badarg -> Alias ! {Alias, exists}
                    end
                end),
            receive
                {Alias, _} -> ok;
                {'DOWN', Mon, process, Owner, _} -> ok
            end,
            erlang:unalias(Alias),
            erlang:demonitor(Mon, [flush]),
            ok;
        _ -> ok
    end.

%% @doc The options a slot computes under. A computed slot is stored and read
%% by slot number (`lib_process_cache'), never by ID or hashpath, so the
%% resolutions that compute it do not content-address the process state:
%% with `hashpath => update' every nested resolution (`set', `next',
%% `execution', `at-slot') committed the whole state with httpsig to look up
%% a result that is never stored, to normalize its inputs and its result, and
%% to generate a hashpath. Both options survive nesting, unlike
%% `cache-control'. Measured on the test-net game (1 MB, 300 players) under a
%% swarm, 2026-10-27: 59% of the worker's time, one commit 18-39 ms.
slot_opts(Opts) ->
    Opts#{
        <<"hashpath">> => ignore,
        <<"result-cache-lookup">> => false
    }.

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
%% `hb_persistent' leaves the request to this function (`await_sends' in
%% `dev_process:device_info/1'): a `compute' registers its waiter and sends
%% the worker `{need, Group}'. Nothing else ever reaches the worker -- no
%% request and no options: only a `compute' is grouped by process, so
%% anything else found here is resolved by its caller.
await(Worker, GroupName, Base, Req, Opts) ->
    case hb_path:matches(<<"compute">>, hb_path:hd(Req, Opts))
            andalso read_slot(Req, latest, Opts) of
        false -> resolve_here(Base, Req, Opts);
        {invalid, _} -> resolve_here(Base, Req, Opts);
        latest ->
            % A slot-less compute asks for the latest state only with
            % `init' unset or `now'; any other is answered without work.
            case hb_maps:get(<<"init">>, Req, <<"now">>, Opts) of
                <<"now">> -> await_slot(Worker, GroupName, Base, Req, latest, Opts);
                _ -> resolve_here(Base, Req, Opts)
            end;
        TargetSlot -> await_slot(Worker, GroupName, Base, Req, TargetSlot, Opts)
    end.

resolve_here(Base, Req, Opts) ->
    hb_ao:resolve(Base, Req, Opts#{ <<"await-inprogress">> => false }).

await_slot(Worker, GroupName, Base, Req, TargetSlot, Opts) ->
                    % A waiter's whole wait: the worker's queue ahead of it
                    % plus its own `worker_execute'.
    hb_phase_timer:time(worker_await, GroupName, fun() ->
        await_compute(Worker, GroupName, Base, Req, TargetSlot, Opts)
    end).

%% @doc Wait for the worker to compute a slot for a compute that did not come
%% through the grouper (`dev_process:compute/3').
delegate(Worker, GroupName, Base, Req, Slot, Opts) ->
    Mon = erlang:monitor(process, Worker),
    try await_compute(Worker, hb_util:human_id(GroupName), Base, Req, Slot, Opts)
    after erlang:demonitor(Mon, [flush])
    end.

await_compute(Worker, GroupName, Base, Req, TargetSlot, Opts) ->
    ensure_waiters(),
    Ref = make_ref(),
    Key = {GroupName, TargetSlot, Ref},
    ets:insert(?WAITERS, {Key, self()}),
    try
        Worker ! {need, GroupName},
        receive
            {ready, Ref, stored} ->
                % Behind the worker's live state: the slot is stored, or it
                % is gone (a 410). Read in the waiter, never computed there.
                dev_process:compute(Base, Req,
                    Opts#{ <<"await-inprogress">> => false, <<"process-stored-only">> => true });
            {ready, Ref, Res} ->
                Res;
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
    after
        ets:delete(?WAITERS, Key)
    end.

%% @doc Answer the waiters of a process for every slot up to the one just
%% stored: the waiters for that slot get its result, earlier ones read theirs
%% from the store. One select bound on the process, so its cost is the rows it
%% answers, however many other messages and waiters there are.
notify_compute(GroupName, SlotToNotify, Res, _Opts) when is_integer(SlotToNotify) ->
    Rows = rows_upto(GroupName, SlotToNotify),
    {Exact, Earlier} = lists:partition(fun({S, _, _}) -> S == SlotToNotify end, Rows),
    answer(GroupName, Exact, Res),
    answer(GroupName, Earlier, stored),
    ?event(worker_short,
        {finished_notifying,
            {group, GroupName},
            {slot, SlotToNotify},
            {listeners, length(Rows)}
        }
    );
notify_compute(_GroupName, _SlotToNotify, _Res, _Opts) ->
    ok.

%% @doc Answer the waiters registered for exactly `Slot' with `Res', success
%% or not.
notify_result(GroupName, Slot, Res) when is_integer(Slot) ->
    answer(GroupName, [ R || R = {S, _, _} <- rows_upto(GroupName, Slot), S == Slot ], Res);
notify_result(_GroupName, _Slot, _Res) ->
    ok.

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

device_info_test() ->
    test_init(),
    M1 = hb_process_test_vectors:wasm_process(<<"test/aos-2-pure-xs.wasm">>),
    Res = hb_device:device_info(M1, #{}),
    Grouper = hb_maps:get(grouper, Res, undefined, #{}),
    ?assert(is_function(Grouper, 3)),
    {module, Mod} = erlang:fun_info(Grouper, module),
    ?assertMatch(<<"_hb_device_", _/binary>>, atom_to_binary(Mod, utf8)).

%% @doc Listeners receive the public result, marked as a cached state, with
%% its hashpath -- never the execution-device state in `priv'.
worker_notification_is_public_test() ->
    VM = #{ <<"state">> => lists:seq(1, 100000) },
    Full = #{
        <<"counter">> => <<"1">>,
        <<"priv">> => VM#{ <<"hashpath">> => <<"hp">> }
    },
    {ok, Public} = public_result({ok, Full}),
    ?assertEqual(<<"1">>, maps:get(<<"counter">>, Public)),
    ?assertEqual(
        #{ <<"hashpath">> => <<"hp">>, <<"process-cached-state">> => true },
        maps:get(<<"priv">>, Public)
    ),
    ?assert(dev_process:is_cached_state(Public)),
    ?assertEqual({error, boom}, public_result({error, boom})).

grouper_test() ->
    test_init(),
    M1 = hb_process_test_vectors:aos_process(),
    M2 = #{ <<"path">> => <<"compute">>, <<"slot">> => 1 },
    M3 = #{ <<"path">> => <<"compute">>, <<"slot">> => 2 },
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
        lib_process_cache:write(
            ProcessGroup,
            5,
            #{ <<"hello">> => <<"cached">> },
            Opts
        ),
    ?assertEqual(
        ungrouped_exec,
        hb_persistent:group(M1, Uncached, POpts)
    ),
    % A lower slot that is not cached (retention deleted it) never joins the
    % worker's queue, and the requester refuses it: a state only moves
    % forward, so it is gone (410), not recomputed.
    MissingLower = #{ <<"path">> => <<"compute">>, <<"slot">> => 4 },
    ?assertEqual(ungrouped_exec, hb_persistent:group(M1, MissingLower, POpts)),
    ?assertMatch(
        {error, #{ <<"status">> := 410 }},
        hb_ao:resolve(M1, MissingLower, Opts)
    ),
    % The head itself is a cached read; the slot after it groups.
    ?assertEqual(
        ProcessGroup,
        hb_persistent:group(M1, #{ <<"path">> => <<"compute">>, <<"slot">> => 6 }, POpts)
    ),
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
        lib_process_cache:write(
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
        _ = Self,
        Res = ask(Worker, Group, Base,
            #{ <<"path">> => <<"compute">>, <<"slot">> => <<"0">> }, WorkerOpts),
        ?assertMatch({ok, _}, Res),
        ?assertEqual(0, hb_ao:get(<<"at-slot">>, element(2, Res), Opts)),
        % And it is still there for the next request, which is the entire point
        % of a persistent worker.
        ?assert(is_process_alive(Worker)),
        % The slot's phases, timed deep inside the worker, count against its
        % process.
        Attributed =
            [ P || {P, G, _, _, _} <- hb_phase_timer:read_processes(), G == Group ],
        ?assert(lists:member(worker_execute, Attributed)),
        ?assert(lists:member(slot_execution, Attributed)),
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
%% A waiter's whole request to a worker: `await/5', watching the worker as
%% `hb_persistent' does.
ask(Worker, Group, Base, Req, Opts) ->
    Mon = erlang:monitor(process, Worker),
    try await(Worker, Group, Base, Req, Opts)
    after erlang:demonitor(Mon, [flush])
    end.

worker_setup() ->
    worker_setup(#{}).
worker_setup(Extra) ->
    test_init(),
    Wallet = ar_wallet:new(),
    Opts =
        Extra#{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => Wallet,
            <<"spawn-worker">> => true,
            <<"process-workers">> => true,
            <<"await-inprogress">> => named,
            % These tests drive the worker by hand; the scheduler's own
            % follow-up computes would answer their requests first.
            <<"process-compute-on-schedule">> => false
        },
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    Process =
        hb_message:commit(
            #{
                <<"device">> => <<"process@1.0">>,
                <<"type">> => <<"Process">>,
                <<"scheduler-device">> => <<"scheduler@1.0">>,
                <<"execution-device">> => <<"lua-patch@1.0">>,
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
%% and losing the live state. v0.11 checks request types against the
%% device's spec and refuses the request with a 400 before it is grouped; the
%% worker must survive it and keep computing.
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
        ?assertMatch({error, #{ <<"status">> := 400 }}, Res),
        ?assert(is_process_alive(Worker)),
        {ok, S3} =
            bounded_resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"compute">> => 3 },
                Opts
            ),
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
                    <<"execution-device">> => <<"lua-patch@1.0">>,
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
        {Process, Worker, Group, Opts} = worker_setup(),
        lists:foreach(
            fun(Req) ->
                ?assertMatch({error, _}, ask(Worker, Group, Process, Req, Opts))
            end,
            [
                #{ <<"path">> => <<"compute">>, <<"slot">> => <<"-2">> },
                #{ <<"path">> => <<"compute">>, <<"slot">> => <<"abc">> },
                #{ <<"path">> => <<"compute">>, <<"slot">> => -5 }
            ]
        ),
        ?assert(is_process_alive(Worker)),
        % And it still serves the next slot from its live state.
        {ok, S3} = ask(Worker, Group, Process,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 3 }, Opts),
        ?assertEqual(<<"4">>, hb_ao:get(<<"count">>, S3, Opts))
    end}.

%% @doc Regression: the keys that steer a compute beyond its slot reach the
%% worker's resolution. `init' is the one with an observable answer: a
%% slot-less compute with `init' other than `now' is `not_found', which the
%% worker answered with the latest state instead, having rebuilt the request
%% from its slot alone. `push' was dropped the same way, so a delegated
%% compute never triggered its push.
worker_carries_request_keys_test_() ->
    {timeout, 120, fun() ->
        {Process, Worker, Group, Opts} = worker_setup(),
        ?assertEqual({error, not_found},
            ask(Worker, Group, Process,
                #{ <<"path">> => <<"compute">>, <<"init">> => <<"none">> }, Opts))
    end}.

%% @doc `notify_compute' answers the registered waiters of a process at or
%% below the slot just stored -- that slot with its result, earlier ones from
%% the store -- and leaves later slots, `latest' waiters, other processes and
%% the caller's mailbox alone.
notify_compute_answers_registry_test() ->
    ensure_waiters(),
    Group = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Other = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Row = fun(G, S) -> R = make_ref(), ets:insert(?WAITERS, {{G, S, R}, self()}), R end,
    R3 = Row(Group, 3),
    R5 = Row(Group, 5),
    R7 = Row(Group, 7),
    RL = Row(Group, latest),
    RO = Row(Other, 5),
    self() ! {resolve, self(), Group, #{ <<"compute">> => 5 }, #{}},
    notify_compute(Group, 5, {ok, #{ <<"n">> => <<"5">> }}, #{}),
    receive {ready, R5, {ok, #{ <<"n">> := <<"5">> }}} -> ok
    after 0 -> erlang:error(exact_slot_not_answered)
    end,
    receive {ready, R3, stored} -> ok
    after 0 -> erlang:error(earlier_slot_not_answered)
    end,
    receive
        {ready, R7, _} -> erlang:error(later_slot_answered);
        {ready, RL, _} -> erlang:error(latest_answered);
        {ready, RO, _} -> erlang:error(other_process_answered)
    after 0 -> ok
    end,
    ?assertEqual(2, waiter_rows(Group)),
    receive {resolve, _, Group, _, _} -> ok
    after 0 -> erlang:error(mailbox_consumed)
    end,
    [ ets:delete(?WAITERS, K) || K <- [{Group, 7, R7}, {Group, latest, RL}, {Other, 5, RO}] ].

%% @doc The cost of answering a slot is the rows it answers: 5,000 waiters
%% for later slots of the same process do not make each slot's notification
%% slower (prod, 2026-11-05: a full-mailbox selective receive per slot).
notify_cost_flat_in_waiters_test() ->
    ensure_waiters(),
    Group = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Time = fun() ->
        {Micros, _} =
            timer:tc(fun() ->
                [ notify_compute(Group, S, {ok, #{}}, #{}) || S <- lists:seq(1, 2000) ]
            end),
        Micros
    end,
    Empty = Time(),
    Refs = [ make_ref() || _ <- lists:seq(1, 5000) ],
    ets:insert(?WAITERS, [ {{Group, 1000000, R}, self()} || R <- Refs ]),
    Loaded = Time(),
    ?assertEqual(5000, waiter_rows(Group)),
    [ ets:delete(?WAITERS, {Group, 1000000, R}) || R <- Refs ],
    ?event({notify_cost, {empty_us, Empty}, {loaded_us, Loaded}}),
    ?assert(Loaded < 5 * max(Empty, 2000)).

%% @doc A waiter sends the worker a two-tuple and nothing else: no request,
%% no options. Waiters that give up (an HTTP client that left) are dropped
%% without the worker computing anything for them, and leave no rows and no
%% messages behind; the ones still waiting get their answer.
waiters_carry_no_opts_and_leave_nothing_test_() ->
    {timeout, 120, fun() ->
        {Process, Worker, Group, Opts} = worker_setup(),
        erlang:suspend_process(Worker),
        Self = self(),
        Waiter =
            fun() ->
                spawn(fun() ->
                    Self ! {answer, self(),
                        catch hb_ao:resolve(Process,
                            #{ <<"path">> => <<"compute">>, <<"slot">> => 3 }, Opts)}
                end)
            end,
        Waiters = [ Waiter() || _ <- lists:seq(1, 200) ],
        ?assert(wait(fun() -> waiter_rows(Group) == 200 end, 15000)),
        {messages, Queued} = erlang:process_info(Worker, messages),
        ?assertEqual(200, length(Queued)),
        ?assert(lists:all(fun(M) -> M == {need, Group} end, Queued)),
        {Abandoned, Kept} = lists:split(150, Waiters),
        [ exit(P, kill) || P <- Abandoned ],
        erlang:resume_process(Worker),
        Answers = [ receive {answer, P, A} -> A after 30000 -> timeout end || P <- Kept ],
        ?assert(lists:all(fun(A) -> element(1, A) == ok end, Answers)),
        ?assertEqual(<<"4">>, hb_ao:get(<<"count">>, element(2, hd(Answers)), Opts)),
        ?assert(wait(fun() -> waiter_rows(Group) == 0 end, 15000)),
        ?assert(wait(fun() ->
            element(2, erlang:process_info(Worker, message_queue_len)) == 0 end, 15000)),
        ?assert(is_process_alive(Worker))
    end}.

wait(Pred, TimeoutMs) when TimeoutMs =< 0 -> Pred();
wait(Pred, TimeoutMs) ->
    case Pred() of
        true -> true;
        false -> timer:sleep(50), wait(Pred, TimeoutMs - 50)
    end.

%% @doc The worker never rewinds. A `{resolve}' for a slot behind its live
%% state that is no longer stored -- one the grouper would never have sent it,
%% a stale request it dequeues after retention ran, or one whose store has
%% lost the slot -- is answered with a 410, and the worker carries on from
%% the state it holds: it used to rebuild the process from its nearest
%% checkpoint, or from the start, while every message to it waited.
worker_never_rewinds_test_() ->
    {timeout, 120, fun() ->
        {Process, Worker, Group, Opts} =
            worker_setup(#{
                <<"process-state-blobs">> => true,
                <<"process-delta-cache">> => true,
                <<"process-hot-cache-slots">> => 0
            }),
        % The store loses every slot and the head, so the requests below miss
        % the cache, pass the head gate in `dev_process:compute/3', and reach
        % `compute_to_slot' on the worker's live state at slot 2.
        Store = hb_opts:get(<<"store">>, undefined, Opts),
        #{ <<"db">> := DB } = hb_store:find(Store),
        {ok, _} =
            elmdb:delete_batch(
                DB,
                [ <<"pstate/", Group/binary, "/", K/binary, "/",
                    (iolist_to_binary(io_lib:format("~12..0B", [S])))/binary>>
                || K <- [<<"d">>, <<"f">>, <<"s">>], S <- [0, 1, 2] ] ++
                [ <<"computed/", Group/binary, "/latest">> ]
            ),
        ?assertEqual({error, not_found}, lib_process_cache:latest_slot(Group, Opts)),
        ?assertEqual({error, not_found}, lib_process_cache:read(Group, 1, Opts)),
        Before = slot_executions(),
        lists:foreach(
            fun(Slot) ->
                ?assertMatch({error, #{ <<"status">> := 410 }},
                    ask(Worker, Group, Process,
                        #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, Opts))
            end,
            [1, 0]
        ),
        ?assertEqual(Before, slot_executions()),
        ?assert(is_process_alive(Worker)),
        ?assertEqual(Worker, hb_name:lookup(Group)),
        % The live state is where it was: the next slot computes from it.
        {ok, S3} = ask(Worker, Group, Process,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 3 }, Opts),
        ?assertEqual(<<"4">>, hb_ao:get(<<"count">>, S3, Opts)),
        ?assertEqual(Before + 1, slot_executions())
    end}.

slot_executions() ->
    lists:sum([ N || {slot_execution, _, N, _} <- hb_phase_timer:read() ]).

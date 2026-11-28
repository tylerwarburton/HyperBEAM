%%% @doc This module contains the device implementation of AO processes
%%% in AO-Core. The core functionality of the module is in 'routing' requests
%%% for different functionality (scheduling, computing, and pushing messages)
%%% to the appropriate device. This is achieved by swapping out the device 
%%% of the process message with the necessary component in order to run the 
%%% execution, then swapping it back before returning. Computation is supported
%%% as a stack of devices, customizable by the user, while the scheduling
%%% device is (by default) a single device.
%%% 
%%% This allows the devices to share state as needed. Additionally, after each
%%% computation step the device caches the result at a path relative to the
%%% process definition itself, such that the process message's ID can act as an
%%% immutable reference to the process's growing list of interactions. See 
%%% `lib_process_cache' for details.
%%% 
%%% The external API of the device is as follows:
%%% <pre>
%%% GET /ID/Schedule:                Returns the messages in the schedule
%%% POST /ID/Schedule:               Adds a message to the schedule
%%% 
%%% GET /ID/Compute/[IDorSlotNum]:   Returns the state of the process after 
%%%                                  applying a message
%%% GET /ID/Now:                     Returns the `/Results' key of the latest 
%%%                                  computed message
%%% </pre>
%%% 
%%% An example process definition will look like this:
%%% <pre>
%%%     Device: Process/1.0
%%%     Scheduler-Device: Scheduler/1.0
%%%     Execution-Device: Stack/1.0
%%%     Execution-Stack: "Scheduler/1.0", "Cron/1.0", "WASM/1.0"
%%%     Cron-Frequency: 10-Minutes
%%%     WASM-Image: WASMImageID
%%% </pre>
%%%
%%% Runtime options:
%%%     Cache-Frequency: The number of assignments that will be computed 
%%%                      before the full (restorable) state should be cached.
%%%     Cache-Keys:      A list of the keys that should be cached for all 
%%%                      assignments, in addition to `/Results'.
-module(dev_process).
-device_libraries([lib_process, lib_process_cache, lib_process_delivery]).
%%% Public API
-export([device_info/1]).
-export([as/3, compute/3, schedule/3, slot/3, now/3, push/3, snapshot/3]).
-export([deliveries/3]).
-export([target_slot/2, request_slot/2, is_cached_state/1, is_live_state/1]).
-export([default_device/3]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("include/hb.hrl").

%% The frequency at which the process state should be cached. Can be overridden
%% with the `process_snapshot_slots' or `process_snapshot_time' options.
-if(TEST == true).
-define(DEFAULT_SNAPSHOT_SLOTS, 1).
-define(DEFAULT_SNAPSHOT_TIME, undefined).
-else.
-define(DEFAULT_SNAPSHOT_SLOTS, undefined).
-define(DEFAULT_SNAPSHOT_TIME, 60).
-endif.
%% Compute time since the last snapshot after which the next slot snapshots
%% (`process-snapshot-replay-ms'): what a restart replays at most.
-define(DEFAULT_SNAPSHOT_REPLAY_MS, 60000).

%% @doc Return the options of the device: the `worker', `grouper' and `await'
%% functions of its executions, and the keys that it exports.
device_info(_Base) ->
    #{
        worker => fun dev_process_worker:server/3,
        grouper => fun dev_process_worker:group/3,
        await => fun dev_process_worker:await/5,
        % The await function sends its own request: a `compute' waiter
        % registers itself and sends the worker a two-tuple, never a copy of
        % its request and options (`dev_process_worker').
        await_sends => true,
        exports =>
            [
                <<"as">>,
                <<"compute">>,
                <<"now">>,
                <<"schedule">>,
                <<"slot">>,
                <<"snapshot">>,
                <<"push">>,
                <<"deliveries">>
            ]
    }.

%% @doc Return the process state with the device swapped out for the device
%% of the given key.
-spec as(
    #{ 'input-prefix' => binary(), _ => _ },
    #{ as => binary(), 'as-device' => binary(), _ => _ },
    #{ _ => _ }
) -> {ok, #{ device := binary(), _ => _ }}.
as(RawBase, Req, Opts) ->
    {ok, Base} = ensure_loaded(RawBase, Req, Opts),
    Key = 
        hb_ao:get_first(
            [
                {{as, <<"message@1.0">>, Req}, <<"as">>},
                {{as, <<"message@1.0">>, Req}, <<"as-device">>}
            ],
            <<"execution">>,
            Opts
        ),
    {ok,
        hb_util:deep_merge(
            lib_process:ensure_process_key(Base, Opts),
            #{
                <<"device">> =>
                    hb_maps:get(
                        << Key/binary, "-device">>,
                        Base,
                        default_device(Base, Key, Opts),
                        Opts
                    ),
                % Configure input prefix for proper message routing within the
                % device
                <<"input-prefix">> =>
                    case hb_maps:get(<<"input-prefix">>, Base, not_found, Opts) of
                        not_found -> <<"process">>;
                        Prefix -> Prefix
                    end,
                % Configure output prefixes for result organization
                <<"output-prefixes">> =>
                    hb_maps:get(
                        <<Key/binary, "-output-prefixes">>,
                        Base,
                        undefined, % Undefined in set will be ignored.
                        Opts
                    )
            },
            Opts
        )
    }.

%% @doc Returns the default device for a given piece of functionality. Expects
%% the `process/variant' key to be set in the message. The `execution-device'
%% _must_ be set in all processes aside those marked with `ao.TN.1' variant.
%% This is in order to ensure that post-mainnet processes do not default to
%% using infrastructure that should not be present on nodes in the future.
-spec default_device(
    #{ 'process/variant' => binary(), _ => _ },
    binary(),
    #{ _ => _ }
) -> binary().
default_device(Base, Key, Opts) ->
    lib_process:default_device(Base, Key, Opts).

%% @doc Wraps functions in the Scheduler device.
-spec schedule(#{ scheduler => _, _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, _} | {error, _}.
schedule(Base, Req, Opts) ->
    hb_phase_timer:time(schedule, fun() ->
        lib_process:run_as(<<"scheduler">>, Base, Req, Opts)
    end).

-spec slot(#{ scheduler => _, _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, _} | {error, _}.
slot(Base, Req, Opts) ->
    ?event({slot_called, {base, Base}, {req, Req}}),
    lib_process:run_as(<<"scheduler">>, Base, Req, Opts).

next(Base, _Req, Opts) ->
    lib_process:run_as(<<"scheduler">>, Base, next, normalized(Base, Opts)).

%% @doc The options to run a device on a slot's state with. Every state this
%% device computes or loads is normalised as the resolver gives a device its
%% base -- a load by resolving it, a slot by `lib_process:normalize_changed/3'
%% -- so the state is given to the resolver as its `normalized-base' and is
%% not normalised again: each normalisation rebuilds every published key.
normalized(State, Opts) ->
    Opts#{ <<"normalized-base">> => State }.

-spec snapshot(#{ execution => _, _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ _ => _ }}.
snapshot(RawBase, _Req, Opts) ->
    Base = lib_process:ensure_process_key(RawBase, Opts),
    {ok, SnapshotMsg} =
        lib_process:run_as(
            <<"execution">>,
            Base,
            #{ <<"path">> => <<"snapshot">>, <<"mode">> => <<"Map">> },
            Opts#{
                <<"cache-control">> => [<<"no-cache">>, <<"no-store">>]
            }
        ),
    {ok, SnapshotMsg}.

%% @doc Before computation begins, a boot phase is required. This phase
%% allows devices on the execution stack to initialize themselves. We set the
%% private `initialized' key to the ID of the state that the process device
%% returns, so a state that a later step changed is not taken as live. The key
%% is never stored, so a state read from the cache is not initialized.
init(Base, Req, Opts) ->
    ?event({init_called, {base, Base}, {req, Req}}),
    % If the process does not set `requires-determinism', set it to `true',
    % such that its execution devices give the same result on every node.
    {ok, Initialized} =
        lib_process:run_as(
            <<"execution">>,
            hb_maps:merge(#{ <<"requires-determinism">> => true }, Base, Opts),
            #{ <<"path">> => <<"init">> },
            Opts
        ),
    {
        ok,
        set_initialized(
            hb_ao:set(Initialized, #{ <<"at-slot">> => -1 }, Opts),
            Opts
        )
    }.

%% @doc Compute the result of an assignment applied to the process state.
%% This function serves as the main entry point for compute operations and routes
%% between two distinct execution paths:
%% 
%% - GET method: Normal compute execution that applies messages to process state
%%   and advances the state permanently. Used for regular process execution.
%% 
%% - POST method: Dryrun compute execution that simulates message processing
%%   without permanently modifying process state. Used for testing message 
%%   handlers and previewing results. The POST method is the key entry point
%%   for the dryrun functionality that allows external clients to test
%%   message processing without side effects.
-spec compute(
    #{ 'at-slot' => integer(), _ => _ },
    #{
        compute => integer(),
        slot => integer(),
        init => binary(),
        push => _,
        'result-depth' => _,
        async => _,
        'max-depth' => _,
        'max-age' => infinity | integer()
    },
    #{ _ => _ }
) -> {ok, #{ _ => _ }} | {error, _} | {failure, _}.
compute(Base, Req, Opts) ->
    ProcBase = lib_process:ensure_process_key(Base, Opts),
    ProcID = lib_process:process_id(ProcBase, #{}, Opts),
    TargetSlot = request_slot(Req, Opts),
    case TargetSlot of
        {invalid, RawSlot} ->
            case slot_of_message(ProcID, RawSlot, Opts) of
                {ok, Slot} -> compute(Base, with_slot(Req, Slot), Opts);
                not_found ->
                    {error, #{
                        <<"status">> => 404,
                        <<"body">> => <<"No assignment here carries that message ID.">>
                    }};
                invalid ->
                    ?event(compute, {invalid_slot_requested, {slot, RawSlot}}, Opts),
                    {error, invalid_slot_error()}
            end;
        not_found ->
            % The slot is not set, so we need to serve the latest known state
            % unless the `init' key is set to a value aside from `now'.
            % We do this by setting the `process-now-from-cache' option to `true'.
            case hb_maps:get(<<"init">>, Req, <<"now">>, Opts) of
                <<"now">> ->
                    now(Base, Req, Opts#{ <<"process-now-from-cache">> => true });
                _ ->
                    {error, not_found}
            end;
        Slot ->
            CacheRead =
                case worker_ahead(ProcID, Base, Slot, Opts) of
                    true -> ahead;
                    false ->
                        hb_phase_timer:time(compute_cache_read, fun() ->
                            lib_process_cache:read(ProcID, Slot, Opts)
                        end)
                end,
            case CacheRead of
                ahead ->
                    % The worker's own state is the newest stored slot and the
                    % slot is past it: nothing can be stored for it, and the
                    % worker computes it (`compute_uncached/5' ends here too).
                    compute_here(ProcID, ProcBase, Req, Slot, Opts);
                {ok, Result} ->
                    % The result is already cached, so we can return it.
                    ?event(
                        {compute_result_cached,
                            {proc_id, ProcID},
                            {slot, Slot},
                            {result, Result}
                        }
                    ),
                    {ok, mark_cached_state(without_snapshot(Result, Opts), Opts)};
                {error, not_found} ->
                    compute_uncached(ProcID, ProcBase, Req, Slot, Opts)
            end
    end.

%% @doc Whether the calling process worker holds the live state at the
%% newest slot stored (the hot cache's head, in memory) and the slot is past
%% it. Only the worker stores slots beyond the head, so such a slot is not
%% stored, and neither the cache read nor the head lookup that would say so
%% is needed. Anything else (no hot head yet, after a restart; a state that
%% is not the head) reads as before.
worker_ahead(ProcID, Base, Slot, Opts) ->
    hb_opts:get(<<"is-worker">>, false, Opts) == true andalso
        case maps:get(<<"at-slot">>, Base, undefined) of
            Held when is_integer(Held), Held < Slot ->
                lib_process_cache:hot_head(ProcID, Opts) == {ok, Held};
            _ -> false
        end.

%% @doc Compute a slot that is not stored. A process's state only moves
%% forward: a slot at or below the newest stored slot (the head) that is not
%% stored -- retention deleted it -- is never computed into the process's
%% cache again. It is rebuilt for the one request by `history/5' (a replay
%% from the nearest kept checkpoint below it that stores and delivers
%% nothing), one such replay per node at a time; with
%% `process-history-replay' false it is refused with a 410. An unbounded
%% recompute in whoever asked is what this guards against: on prod
%% (2026-11-05) ~30 detached pushes of the order-book vault, each ~85k slots
%% behind its head, replayed it at once, the node fell from 27 to 9 slots/s,
%% every process's slot took 3-7x as long, and the vault fell further
%% behind. Every route to a slot's state ends here or in the worker's
%% `compute_to_slot/5', which refuses the same slots. A slot's outbox
%% outlives its state in its `o' row (`lib_process_cache:read_results/3'),
%% which is what a push reads.
%%
%% Slot -1 is the initialized process before its first assignment: it is
%% always `init/3', never a replay, so it is not refused.
compute_uncached(ProcID, ProcBase, Req, Slot, Opts) ->
    case Slot >= 0 andalso
            lib_process_cache:latest_slot(ProcID, lib_process:scoped_opts(Opts)) of
        {ok, Head} when is_integer(Head), Slot =< Head ->
            history(ProcID, ProcBase, Req, Slot, Head, Opts);
        _ when Slot >= 0 ->
            % The worker said the slot is behind its live state, so it was
            % stored; it is not now, so it is gone (`dev_process_worker').
            case hb_opts:get(<<"process-stored-only">>, false, Opts) of
                true -> history(ProcID, ProcBase, Req, Slot, Slot, Opts);
                false -> compute_ahead(ProcID, ProcBase, Req, Slot, Opts)
            end;
        _ -> compute_here(ProcID, ProcBase, Req, Slot, Opts)
    end.

%% @doc A slot at or below the head that is no longer stored, rebuilt: from
%% the newest snapshot at or below it (held here, or put back from the cold
%% tier; or the definition when there is none), then every slot after that
%% recomputed from the schedule, with nothing written to the process's
%% cache, no snapshot taken, no worker involved and nothing delivered. Old
%% reads get slower, never wrong: a slot's state is a function of the
%% definition and the messages before it. The rebuild is a job off the
%% request (`hb_history'): the request is answered 202 with its progress and
%% ETA, repeats join it, and the rebuilt state is kept for later reads.
history(ProcID, ProcBase, Req, Slot, Head, Opts) ->
    case hb_opts:get(<<"process-history-replay">>, true, Opts) of
        false ->
            ?event(compute, {refused_pruned_slot, ProcID, Slot, Head}, Opts),
            {error, pruned_slot_error(ProcID, Slot, Head)};
        _ ->
            Run = fun(Job) ->
                history_replay(ProcID, ProcBase, Req, Slot, Opts#{ <<"history-job">> => Job })
            end,
            case hb_history:request(hb_util:human_id(ProcID), Slot, Run, Opts) of
                {pending, Info} ->
                    {error, #{ <<"status">> => 202,
                               <<"retry-after">> => retry_after(Info),
                               <<"body">> => rebuilding(ProcID, Slot, Info) }};
                {busy, #{ running := N }} ->
                    ?event(compute, {history_replay_busy, ProcID, Slot}, Opts),
                    {error, #{ <<"status">> => 503, <<"retry-after">> => 30,
                               <<"body">> => <<"Busy rebuilding ", (integer_to_binary(N))/binary,
                                               " other old slots. Retry.">> }};
                Result -> Result
            end
    end.

retry_after(#{ eta_seconds := ETA }) when is_integer(ETA) -> max(1, min(60, ETA));
retry_after(_) -> 5.

rebuilding(ProcID, Slot, #{ done := Done, total := Total, eta_seconds := ETA }) ->
    iolist_to_binary(io_lib:format(
        "Rebuilding slot ~b of ~s by replay: ~b of ~p slots done, ETA ~s. Ask again.",
        [Slot, hb_util:human_id(ProcID), Done, Total,
         case ETA of unknown -> "unknown"; _ -> integer_to_list(ETA) ++ " s" end])).

history_replay(ProcID, ProcBase, Req, Slot, Opts) ->
    HOpts = (slot_opts(Opts))#{
        <<"process-cache-write">> => false,
        <<"process-delivery">> => false,
        <<"process-workers">> => false,
        <<"spawn-worker">> => false,
        <<"process-worker-spawn">> => false,
        <<"scheduler-lookahead">> => false,
        <<"process-snapshot-target">> => false,
        <<"process-snapshot-slots">> => false,
        <<"process-snapshot-time">> => false,
        <<"process-snapshot-replay-ms">> => false,
        <<"process-replay-from-start">> => true
    },
    T0 = erlang:monotonic_time(millisecond),
    % A shipped checkpoint nearer the slot than any held here is put back
    % first (`hb_store_cold'), so the replay is the shorter one.
    _ = hb_store_cold:restore_checkpoint(ProcID, Slot, Opts),
    Definition = hb_maps:get(<<"process">>, ProcBase, ProcBase, Opts),
    case ensure_loaded(Definition, Req#{ <<"slot">> => Slot }, HOpts) of
        {ok, Loaded} ->
            From = lib_process:get_key(<<"at-slot">>, Loaded, HOpts),
            hb_history:started(HOpts, From, Slot),
            Res = compute_to_slot(ProcID, Loaded, Req, Slot, HOpts),
            ?event(compute,
                {history_replayed, {proc_id, ProcID}, {slot, Slot}, {from, From},
                 {ms, erlang:monotonic_time(millisecond) - T0}},
                Opts),
            case Res of
                {ok, State} -> {ok, mark_cached_state(without_snapshot(State, Opts), Opts)};
                Other -> Other
            end;
        Error -> Error
    end.

%% A slot beyond the head is the worker's to compute, even when its request
%% reached here ungrouped (a nested call runs with `await-inprogress' off):
%% it waits for the worker. Only with no worker is it computed here.
compute_ahead(ProcID, ProcBase, Req, Slot, Opts) ->
    case live_worker(ProcID, Opts) of
        {ok, Worker} ->
            case dev_process_worker:delegate(Worker, ProcID, ProcBase, Req, Slot, Opts) of
                {error, leader_died} ->
                    compute_here(ProcID, ProcBase, Req, Slot, Opts);
                Res -> Res
            end;
        none -> compute_here(ProcID, ProcBase, Req, Slot, Opts)
    end.

%% @doc The process's worker, when one runs and this is not it.
live_worker(ProcID, Opts) ->
    case hb_opts:get(<<"is-worker">>, false, Opts) =/= true andalso
            hb_opts:get(process_workers, false, Opts) == true andalso
            hb_name:lookup({process_worker, hb_util:human_id(ProcID)}) of
        Pid when is_pid(Pid), Pid =/= self() -> {ok, Pid};
        _ -> none
    end.

compute_here(ProcID, ProcBase, Req, Slot, Opts) ->
    SlotOpts = slot_opts(Opts),
    case hb_phase_timer:time(ensure_loaded, fun() ->
            ensure_loaded(ProcBase, Req, SlotOpts)
        end) of
        {ok, Loaded} ->
            ?event(compute,
                {computing, {process_id, ProcID},
                {to_slot, Slot}},
                Opts
            ),
            Res = compute_to_slot(ProcID, Loaded, Req, Slot, SlotOpts),
            % A compute outside the worker (the leader of a group
            % before a worker exists) answers the waiters registered
            % for its own slot, whatever its result: a failure stores
            % nothing, so no slot notification would reach them.
            case hb_opts:get(<<"is-worker">>, false, Opts) of
                true -> ok;
                _ -> dev_process_worker:notify_result(ProcID, Slot, Res)
            end,
            Res;
        {error, _} = Error -> Error
    end.

%% @doc The options every slot is computed under, whoever computes it: the
%% process worker, or the first caller after a restart (an HTTP request, a
%% push, the compute-on-schedule follower) computing inline before a worker
%% exists. A computed slot is stored and read by slot number
%% (`lib_process_cache'), never by ID or hashpath, so the nested resolutions
%% (`set', `next', `execution', `at-slot') must not content-address the
%% process state: with `hashpath => update' each commits the whole state with
%% httpsig. That was fixed for the worker alone (2026-10-27), so a process
%% that came back behind after a restart caught up at the slow rate and never
%% reached its worker: the prod vault (1.2 MB state) computed 1.8 slots/s for
%% hours with 13,731 requests queued behind it, 2026-11-04.
%% Nor does any nested resolution group: the worker already computes with
%% `await-inprogress => false', but an inline leader inherited its caller's
%% (a push's schedule asks for `true'), and `hb_persistent:default_grouper'
%% then named every `hb_ao:get' of the slot by a SHA-256 of the whole state's
%% `term_to_binary': 55% of the node's samples, prod 2026-11-04.
slot_opts(Opts) ->
    Opts#{
        <<"hashpath">> => ignore,
        <<"result-cache-lookup">> => false,
        <<"await-inprogress">> => false
    }.

%% @doc Return the slot requested by a `compute' request, or `not_found'.
target_slot(Req, Opts) ->
    hb_ao:get_first(
        [
            {{as, <<"message@1.0">>, Req}, <<"compute">>},
            {{as, <<"message@1.0">>, Req}, <<"slot">>}
        ],
        Opts
    ).

%% @doc Return the slot a `compute' request asks for as an integer of at least
%% -1, `not_found' when it names none, or `{invalid, Raw}'.
%%
%% This is the one reading of a request's slot: `compute/3', the process
%% worker, its grouper and its waiters all use it, so they cannot disagree on
%% which slot a request means -- `compute' takes precedence over `slot', as in
%% `target_slot/2'. A slot that is not an integer, or is below -1, can never
%% name a state; it is reported here instead of crashing `hb_util:int/1' or
%% reaching `compute_to_slot/6', where such a target rewinds to nothing and
%% throws.
%%
%% -1 is a real state: the initialized process before its first assignment
%% (`init/3' sets `at-slot' to -1), and the `current' slot the scheduler
%% reports for a process with no assignments yet. `now/3' asks for exactly
%% that slot, so rejecting it answered every `now' on a fresh process with a
%% 400 instead of its initial state.
%%
%% A request that carries its slot as a plain literal -- every HTTP `compute'
%% does -- is read directly: resolving the keys through `message@1.0' costs
%% ~125us, and this runs in the grouper, the worker and each waiter on every
%% request. Any other shape (a link, a typed or wrapped value, no literal at
%% all) takes the full `target_slot/2' path.
request_slot(Req, Opts) when is_map(Req) ->
    case literal_slot(Req) of
        {ok, Raw} -> parse_slot(Raw);
        not_literal -> request_slot_resolved(Req, Opts)
    end;
request_slot(Req, Opts) ->
    request_slot_resolved(Req, Opts).

request_slot_resolved(Req, Opts) ->
    case target_slot(Req, Opts) of
        not_found -> not_found;
        Raw -> parse_slot(unwrap_slot(Raw))
    end.

%% @doc The slot a request carries as a literal value, in `target_slot/2''s
%% precedence order, if it carries one that way.
literal_slot(#{ <<"compute">> := Slot }) when is_integer(Slot); is_binary(Slot) ->
    {ok, Slot};
literal_slot(Req = #{ <<"slot">> := Slot })
        when is_integer(Slot) orelse is_binary(Slot),
            not is_map_key(<<"compute">>, Req),
            not is_map_key(<<"compute+link">>, Req) ->
    {ok, Slot};
literal_slot(_) ->
    not_literal.

parse_slot(Slot) when is_integer(Slot), Slot >= -1 -> Slot;
parse_slot(Slot) when is_binary(Slot); is_list(Slot) ->
    try hb_util:int(Slot) of
        Int when Int >= -1 -> Int;
        _ -> {invalid, Slot}
    catch error:badarg -> {invalid, Slot}
    end;
parse_slot(Slot) -> {invalid, Slot}.

%% @doc The error returned for a request whose slot cannot name an assignment.
invalid_slot_error() ->
    #{
        <<"status">> => 400,
        <<"body">> => <<"Invalid slot: expected an integer of at least -1.">>
    }.

%% @doc The slot whose assignment carries message `ID', from the scheduler's
%% message index (`dev_scheduler_cache:find_assigned/4'), which also checks
%% that the assignment at that slot names the ID, and only up to the newest
%% durable slot. `compute&slot=<message id>' is read this way, once, here:
%% the worker, grouper and waiters keep their integer slots. Only the node
%% that schedules the process holds the index, so any other answers 404.
slot_of_message(ProcID, ID, Opts) when is_binary(ID) ->
    case hb_util:is_human_id(ID) of
        false -> invalid;
        true ->
            case dev_scheduler_cache:find_assigned(
                    ProcID, [ID], dev_scheduler_cache:durable(ProcID), Opts) of
                {ok, Slot, _} -> {ok, Slot};
                _ -> not_found
            end
    end;
slot_of_message(_ProcID, _ID, _Opts) -> invalid.

%% @doc `Req' with its slot replaced by `Slot', under the key `literal_slot/1'
%% reads first.
with_slot(Req = #{ <<"compute">> := _ }, Slot) -> Req#{ <<"compute">> => Slot };
with_slot(Req, Slot) -> (maps:remove(<<"compute+link">>, Req))#{ <<"slot">> => Slot }.

%% @doc The error returned for a slot at or below the head that is no longer
%% stored. It is gone, not missing: no later request will find it either.
pruned_slot_error(ProcID, Slot, Head) ->
    #{
        <<"status">> => 410,
        <<"body">> =>
            <<
                "Slot ", (hb_util:bin(Slot))/binary,
                " of ", (hb_util:bin(ProcID))/binary,
                " is below retained history (head ", (hb_util:bin(Head))/binary,
                ")."
            >>
    }.

%% @doc Unwrap an HTTP typed-result map to the scalar it carries. Any other
%% term -- including `not_found', which both callers depend on -- is returned
%% untouched.
unwrap_slot(Slot) when is_map(Slot) ->
    maps:get(maps:get(<<"ao-result">>, Slot, <<"body">>), Slot, Slot);
unwrap_slot(Slot) ->
    Slot.

%% @doc Continually get and apply the next assignment from the scheduler until
%% we reach the target slot that the user has requested.
compute_to_slot(ProcID, Base, Req, TargetSlot, Opts) ->
    case lib_process:get_key(<<"at-slot">>, Base, Opts#{ <<"hashpath">> => ignore }) of
        CurrentSlot when CurrentSlot == TargetSlot ->
            % The target slot's state is written to the cache when it is
            % computed, so we only return it here. A slot result is never
            % written from a state the device did not itself compute.
            ?event(compute_short,
                {reached_target_slot_returning_state,
                    {proc_id, ProcID},
                    {slot, TargetSlot}
                },
                Opts
            ),
            % The state here is one this device computed or loaded, so it is
            % already normalised (`lib_process:run_as/4'): its bookkeeping
            % keys are set without resolving it again.
            {ok,
                set_initialized(
                    lib_process:set_keys(
                        Base,
                        #{
                            <<"device">> => <<"process@1.0">>,
                            <<"snapshot">> => unset
                        },
                        Opts
                    ),
                    Opts
                )};
        CurrentSlot when CurrentSlot < TargetSlot ->
            % Compute the next state transition.
            NextSlot = CurrentSlot + 1,
            % Get the next input message from the scheduler device. The
            % message trace's row for the slot starts here (`hb_msg_trace').
            hb_msg_trace:open(ProcID, NextSlot),
            case hb_phase_timer:time(slot_next, ProcID, fun() -> next(Base, Req, Opts) end) of
                {error, Res} ->
                    hb_msg_trace:abandon(),
                    % If the scheduler device cannot provide a next message,
                    % we return its error details, along with the current slot.
                    ?event(compute_short,
                        {error_getting_assignment,
                            {proc_id, ProcID},
                            {attempted_slot, NextSlot},
                            {target_slot, TargetSlot},
                            {error, Res}
                        }
                    ),
                    {error,
                        Res#{
                            <<"phase">> => <<"get-schedule">>,
                            <<"attempted-slot">> => NextSlot,
                            <<"process-id">> => ProcID
                        }
                    };
                {ok, #{ <<"body">> := SlotMsg, <<"state">> := State }} ->
                    % Compute the next single state transition.
                    case compute_slot(ProcID, State, SlotMsg, Req, TargetSlot, Opts) of
                        {ok, NewState} ->
                            % Continue computing to the target slot.
                            compute_to_slot(
                                ProcID,
                                NewState,
                                Req,
                                TargetSlot,
                                Opts
                            );
                        {error, Error} ->
                            % Forward error details back to the caller.
                            {error, Error};
                        {failure, Failure} ->
                            % Forward failure details back to the caller.
                            {failure, Failure}
                    end
            end;
        CurrentSlot when CurrentSlot > TargetSlot ->
            % The slot is behind the state held here and not in the cache:
            % retention deleted its row. A state never moves backwards, so it
            % is refused, not replayed (`compute_uncached/5').
            ?event(
                compute,
                {refused_pruned_slot, ProcID, TargetSlot, CurrentSlot},
                Opts
            ),
            {error, pruned_slot_error(ProcID, TargetSlot, CurrentSlot)}
    end.

%% @doc Compute a single slot for a process, given an initialized state.
compute_slot(ProcID, State, RawInputMsg, InitReq, TargetSlot, Opts) ->
    hb_msg_trace:opened(),
    {PrepTimeMicroSecs, {ok, Slot, PreparedState, Req}} =
        timer:tc(
            fun() ->
                prepare_next_slot(ProcID, State, RawInputMsg, Opts)
            end
        ),
    ?event(
        compute,
        {prepared_slot,
            {proc_id, ProcID},
            {slot, Slot},
            {prep_time_microsecs, PrepTimeMicroSecs}
        },
        Opts
    ),
    % The Lua phases of the execution are noted against this slot, and only
    % this one (`hb_slot_log:run/1').
    {RuntimeMicroSecs, Res, Notes} =
        hb_slot_log:run(
            fun() ->
                lib_process:run_as(
                    <<"execution">>,
                    PreparedState,
                    Req,
                    normalized(PreparedState, lib_process:execution_opts(Opts))
                )
            end
        ),
    Action = slot_action(Req, Opts),
    ?event(
        compute,
        {computed_slot,
            {proc_id, ProcID},
            {slot, Slot},
            {runtime_microsecs, RuntimeMicroSecs}
        },
        Opts
    ),
    case Res of
        {ok, NewProcStateMsg} when is_map(NewProcStateMsg) ->
            % We have now transformed slot n -> n + 1. Increment the current slot.
            NewProcStateMsgWithSlot =
                with_replay_time(
                    lib_process:normalize_changed(
                        NewProcStateMsg,
                        #{ <<"device">> => <<"process@1.0">>, <<"at-slot">> => Slot },
                        Opts
                    ),
                    State,
                    PrepTimeMicroSecs + RuntimeMicroSecs,
                    Opts
                ),
            % Force a snapshot of the target slot so the terminal state is
            % restorable; intermediate slots snapshot per the configured interval.
            % See `snapshot_target/3' for turning the forced snapshot off.
            % The store's LMDB phases are noted apart from the execution's.
            {StoreTimeMicroSecs, ProcStateWithSnapshot, StoreNotes} =
                hb_slot_log:run(
                    fun() ->
                        store_result(
                            snapshot_target(Slot, TargetSlot, Opts),
                            ProcID,
                            Slot,
                            NewProcStateMsgWithSlot,
                            InitReq,
                            with_assignment_time(Req, Opts)
                        )
                    end
                ),
            hb_slot_log:end_slot(ProcID, Slot, Action, ok, PrepTimeMicroSecs,
                RuntimeMicroSecs, StoreTimeMicroSecs, merge_notes(Notes, StoreNotes)),
            % The slot is readable from here: its trace row ends now.
            hb_msg_trace:close(ProcID, Slot, Req, Action, ok, PrepTimeMicroSecs,
                RuntimeMicroSecs, StoreTimeMicroSecs, Notes, StoreNotes),
            ?event(compute_short,
                {computed_slot,
                    {proc_id, ProcID},
                    {slot, Slot},
                    {target_slot, TargetSlot},
                    {prep_ms, PrepTimeMicroSecs div 1000},
                    {execution_ms, RuntimeMicroSecs div 1000},
                    {store_ms, StoreTimeMicroSecs div 1000},
                    {action, Action}
                }
            ),
            % A rebuild of old slots counts the slot, and keeps it if a
            % request is waiting for it (`hb_history').
            case maps:is_key(<<"history-job">>, Opts) of
                true ->
                    hb_history:note_slot(Opts, hb_util:human_id(ProcID), Slot,
                        mark_cached_state(without_snapshot(ProcStateWithSnapshot, Opts), Opts));
                false -> ok
            end,
            % Notify waiters only after the slot is readable from the process
            % cache. Waiters may immediately re-enter via `/compute' or `/push',
            % and those paths treat the cache as the completion boundary.
            dev_process_worker:notify_compute(
                ProcID,
                Slot,
                {ok, ProcStateWithSnapshot},
                Opts
            ),
            % A slot with an outbox is delivered by the process's deliverer,
            % whoever computed it and whether or not anybody asked for a push:
            % its `o' row is already written, before its state row
            % (`lib_process_delivery'). A cast; it never waits.
            case lib_process_cache:has_outbox(ProcID, NewProcStateMsgWithSlot, Opts) of
                true -> lib_process_delivery:notify(ProcID, Slot, Opts);
                false -> ok
            end,
            {ok, ProcStateWithSnapshot};
        {Status, Error} when Status == ok; Status == error ->
            % An error occurred while computing the slot, or its result is
            % not a message, so it cannot be the process state. The slot's
            % message leaves the state as it found it: the error becomes
            % the slot's results over the inbound state, the slot is
            % stored as usual, and computing carries on to the next slot
            % from it.
            ErrMsg =
                (if is_map(Error) -> Error;
                    true -> #{ <<"error">> => Error }
                end)#{
                    <<"phase">> => <<"compute">>,
                    <<"attempted-slot">> => Slot
                },
            ?event(compute_short,
                {error_computing_slot,
                    {proc_id, ProcID},
                    {attempted_slot, Slot},
                    {target_slot, TargetSlot},
                    {prep_ms, PrepTimeMicroSecs div 1000},
                    {execution_ms, RuntimeMicroSecs div 1000},
                    {error, ErrMsg}
                }
            ),
            % The inbound state's results, and the delta it carries, are
            % slot N-1's. Neither may survive into this slot: its outbox
            % would be stored and delivered a second time as this slot's,
            % and its patches stored again as this slot's change. This
            % slot changes nothing; its results are its error.
            ErrResults = #{ <<"type">> => <<"error">>, <<"error">> => ErrMsg },
            Inbound = maps:without([<<"results">>, <<"results+link">>], State),
            Unchanged =
                case hb_private:get(<<"process-cache-delta">>, Inbound, not_found, Opts) of
                    not_found -> Inbound;
                    _ ->
                        hb_private:set(Inbound, <<"process-cache-delta">>,
                            #{ <<"patches">> => [], <<"results">> => ErrResults }, Opts)
                end,
            ErrState =
                hb_ao:set(
                    Unchanged,
                    #{
                        <<"device">> => <<"process@1.0">>,
                        <<"at-slot">> => Slot,
                        <<"results">> => ErrResults
                    },
                    Opts
                ),

            {StoreTimeMicroSecs, _, StoreNotes} =
                hb_slot_log:run(
                    fun() ->
                        store_result(
                            snapshot_target(Slot, TargetSlot, Opts),
                            ProcID,
                            Slot,
                            ErrState,
                            InitReq,
                            Opts
                        )
                    end
                ),
            hb_slot_log:end_slot(ProcID, Slot, Action, error, PrepTimeMicroSecs,
                RuntimeMicroSecs, StoreTimeMicroSecs, merge_notes(Notes, StoreNotes)),
            hb_msg_trace:close(ProcID, Slot, Req, Action, error, PrepTimeMicroSecs,
                RuntimeMicroSecs, StoreTimeMicroSecs, Notes, StoreNotes),
            {ok, ErrState};
        {failure, Failure} ->
            % A failure of this node -- a failed read or fetch -- is not a
            % result of the computation, so nothing of it is stored. The
            % process-failure hook sees it, and the failure is answered as
            % it arrived.
            hb_slot_log:end_slot(ProcID, Slot, Action, failure, PrepTimeMicroSecs,
                RuntimeMicroSecs, 0, Notes),
            hb_msg_trace:close(ProcID, Slot, Req, Action, failure, PrepTimeMicroSecs,
                RuntimeMicroSecs, 0, Notes, #{}),
            hb_hook:on(
                <<"process-failure">>,
                #{
                    <<"process-id">> => ProcID,
                    <<"slot">> => Slot,
                    <<"body">> => Failure
                },
                Opts
            ),
            {failure, Failure}
    end.

%% A slot's notes from its execution and its store, summed per phase.
merge_notes(A, B) -> maps:merge_with(fun(_, X, Y) -> X + Y end, A, B).

%% The slot message's `action', for the event log and `hb_slot_log'. Reading
%% it never fails a slot.
slot_action(Req, Opts) ->
    try hb_maps:get(<<"action">>, hb_maps:get(<<"body">>, Req, #{}, Opts), no_action_set, Opts)
    catch _:_ -> no_action_set
    end.

%% @doc Options for a compute this device issues itself, such as the one in
%% `now/3'. `spawn-worker' is a temporary option that `hb_ao' strips from
%% every nested execution, and a process compute is always nested (inside
%% `now', a push, the scheduler), so without this a process worker never
%% starts and every request rebuilds the process state from the cache.
%% `process-worker-spawn => true' asks for a worker on these computes only;
%% it is off by default, keeping v0.11's behaviour.
worker_spawn_opts(Opts) ->
    case hb_opts:get(<<"process-worker-spawn">>, false, Opts) of
        true -> Opts#{ <<"spawn-worker">> => true };
        _ -> Opts
    end.

%% @doc Prepare the process state message for computing the next slot.
prepare_next_slot(ProcID, State, RawReq, Opts) ->
    Slot = hb_util:int(hb_ao:get(<<"slot">>, RawReq, Opts)),
    ?event(compute, {next_slot, Slot}),
    % A slot computes its assignment on the process's state, whatever path the
    % assignment gives; the path of the message in its `body' is the execution
    % device's.
    Req = RawReq#{ <<"path">> => <<"compute">> },
    ?event(compute, {input_msg, Req}),
    ?event(compute, {executing, {proc_id, ProcID}, {slot, Slot}}, Opts),
    % Unset the previous results, and the previous slot's delta: a device
    % that returns the state without a delta of its own must not hand slot
    % N-1's on as this slot's (its outbox would be stored and delivered again).
    Unset = lib_process:set_keys(State, #{ <<"results">> => unset }, Opts),
    PreparedState =
        hb_private:set_priv(Unset,
            maps:remove(<<"process-cache-delta">>, hb_private:from_message(Unset))),
    {ok, Slot, PreparedState, Req}.

%% @doc Store the resulting state in the cache, potentially with the snapshot
%% key. The write is synchronous: callers may notify waiters or run push hooks
%% as soon as this returns, so the slot must already be cache-visible.
%% @doc Whether the target slot of a computation is snapshotted whatever the
%% configured interval. A request computes one new slot at a time under live
%% traffic, so the forced snapshot is a whole Lua VM image written for every
%% message. `process-snapshot-target => false' leaves snapshots to
%% `process-snapshot-slots' and `process-snapshot-time': less storage and a
%% cheaper slot, against a cold start that replays the slots since the last
%% interval snapshot.
snapshot_target(Slot, TargetSlot, Opts) ->
    Slot == TargetSlot
        andalso hb_opts:get(<<"process-snapshot-target">>, true, Opts) =/= false.

%% @doc The options to store a slot with: its assignment's timestamp in
%% milliseconds, which a checkpoint's `s' marker keeps so that retention can
%% date it (`lib_process_cache', `hb_store_retention'). An Arweave-scheduled
%% assignment's timestamp is in seconds; `assignment_ms/2' normalises it, and
%% a time that is not plausible is left out (the checkpoint is then kept).
with_assignment_time(Assignment, Opts) ->
    case hb_store_retention:assignment_ms(Assignment, Opts) of
        {ok, Ms} -> Opts#{ <<"process-assignment-timestamp">> => Ms };
        _ -> maps:remove(<<"process-assignment-timestamp">>, Opts)
    end.

store_result(ForceSnapshot, ProcID, Slot, Res, Req, RawOpts) ->
    Opts = lib_process:execution_opts(RawOpts),
    % Cache the `Snapshot' key as frequently as the node is configured to.
    ResMaybeWithSnapshot =
        case ForceSnapshot orelse should_snapshot(Slot, Res, Opts) of
            false -> Res;
            true ->
                ?event(
                    debug_compute,
                    {snapshotting, {proc_id, ProcID}, {slot, Slot}},
                    Opts
                ),
                TS = erlang:monotonic_time(microsecond),
                {ok, Snapshot} =
                    snapshot(Res, Req,
                        case lib_process_cache:async_checkpoints(Opts) of
                            true -> Opts#{ <<"snapshot-deferred-compression">> => true };
                            false -> Opts
                        end),
                hb_slot_log:note(process_snapshot, erlang:monotonic_time(microsecond) - TS),
				?event(snapshot,
					{got_snapshot,
						{storing_as_slot, Slot},
						{snapshot, Snapshot}
					}
				),
                ?event(snapshot,
                    {snapshot_generated,
                        {proc_id, ProcID},
                        {slot, Slot},
                        {snapshot, Snapshot}
                    },
                    Opts
                ),
                WithSnapshot =
                    hb_ao:set(
                        Res,
                        <<"snapshot">>,
                        Snapshot,
                        Opts
                    ),
				WithLastSnapshot =
                    hb_private:set(
                        WithSnapshot,
                        #{ <<"last-snapshot">> => os:system_time(second),
                           <<"replay-us">> => 0 },
                        Opts
                    ),
                ?event(debug_interval,
                    {snapshot_with_last_snapshot,
                        {proc_id, ProcID},
                        {slot, Slot},
                        {snapshot, WithLastSnapshot}
                    }
                ),
                WithLastSnapshot
    end,
    ?event(compute, {caching_result, {proc_id, ProcID}, {slot, Slot}}, Opts),
    lib_process_cache:write(ProcID, Slot, ResMaybeWithSnapshot, Opts),
    ?event(compute, {caching_completed, {proc_id, ProcID}, {slot, Slot}}, Opts),
    hb_maps:without([<<"snapshot">>], ResMaybeWithSnapshot, Opts).

%% @doc Should we snapshot a new full state result? First, we check if the 
%% `process_snapshot_time' option is set. If it is, we check if the elapsed time
%% since the last snapshot is greater than the value. We also check the
%% `process_snapshot_slots' option. If it is set, we check if the slot is
%% a multiple of the interval. If either are true, we must snapshot.
should_snapshot(Slot, Res, Opts) ->
    should_snapshot_slots(Slot, Opts) orelse should_snapshot_time(Res, Opts)
        orelse should_snapshot_replay(Res, Opts).

%% @doc Snapshot once the slots since the last snapshot took
%% `process-snapshot-replay-ms' (default 60,000) to compute: a restart then
%% replays at most that long, however slow the process's slots are. A slot
%% count alone left a full-state token at ~2 s a slot ~30 minutes of replay
%% behind its newest snapshot (1,000 slots), and a 15 s/slot one hours.
%% `false' turns it off.
should_snapshot_replay(Res, Opts) ->
    case hb_util:safe_int(
            hb_opts:get(<<"process-snapshot-replay-ms">>, ?DEFAULT_SNAPSHOT_REPLAY_MS, Opts)) of
        {ok, Ms} when Ms > 0 ->
            hb_private:get(<<"replay-us">>, Res, 0, Opts) >= Ms * 1000;
        _ -> false
    end.

%% The compute time since the last snapshot, carried in the state's private
%% keys from slot to slot (never stored).
with_replay_time(Res, Prev, Micros, Opts) ->
    hb_private:set(Res, <<"replay-us">>,
        hb_private:get(<<"replay-us">>, Prev, 0, Opts) + Micros, Opts).

%% @doc Calculate if we should snapshot based on the number of slots. A value
%% that is not a positive integer, such as `false' or `0', turns it off.
should_snapshot_slots(Slot, Opts) ->
    case hb_util:safe_int(
            hb_opts:get(process_snapshot_slots, ?DEFAULT_SNAPSHOT_SLOTS, Opts)) of
        {ok, SnapshotSlots} when SnapshotSlots > 0 ->
            Slot rem SnapshotSlots == 0;
        _ ->
            false
    end.

%% @doc Calculate if we should snapshot based on the elapsed time since the last
%% snapshot. A value that is not an integer, such as `false', turns it off.
should_snapshot_time(Res, Opts) ->
    case hb_util:safe_int(
            hb_opts:get(process_snapshot_time, ?DEFAULT_SNAPSHOT_TIME, Opts)) of
        {error, invalid} ->
            false;
        {ok, Secs} ->
            case hb_private:get(<<"last-snapshot">>, Res, undefined, Opts) of
                undefined ->
                    ?event(
                        debug_interval,
                        {no_last_snapshot,
                            {interval, Secs},
                            {msg, Res}
                        }
                    ),
                    true;
                OldTimestamp ->
                    ?event(
                        debug_interval,
                        {calculating,
                            {secs, Secs},
                            {timestamp, OldTimestamp},
                            {now, os:system_time(second)}
                        }
                    ),
                    os:system_time(second) > OldTimestamp + hb_util:int(Secs)
            end
    end.

%% @doc Returns the known state of the process at either the current slot, or
%% the latest slot in the cache depending on the `process-now-from-cache' option.
-spec now(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ _ => _ }} | {failure, _} | {error, _}.
now(RawBase, Req, Opts) ->
    Base = lib_process:ensure_process_key(RawBase, Opts),
    ProcessID = lib_process:process_id(Base, #{}, Opts),
    case hb_opts:get(process_now_from_cache, false, Opts) of
        false ->
            maybe
                {ok, CurrentSlot} ?=
                    hb_phase_timer:time(now_slot_current, fun() ->
                        hb_ao:resolve(
                            Base,
                            #{ <<"path">> => <<"slot/current">> },
                            Opts
                        )
                    end),
                ?event({now_called, {process, ProcessID}, {slot, CurrentSlot}}),
                {ok, _} = Computed ?=
                    hb_ao:resolve(
                        Base,
                        #{
                            <<"path">> => <<"compute">>,
                            <<"slot">> => CurrentSlot
                        },
                        worker_spawn_opts(Opts)
                    ),
                % Refreshed only once the slot is stored: the refreshed slot
                % is the head, and a slot the scheduler has assigned but
                % nobody has computed must never be named as one -- every
                % slot up to the head is refused as pruned.
                lib_process_cache:refresh(
                    ProcessID,
                    hb_util:int(CurrentSlot),
                    Opts
                ),
                Computed
            end;
        CacheParam ->
            % We are serving the latest known state from the cache, rather
            % than computing it.
            LatestKnown =
                hb_phase_timer:time(now_cache_latest, fun() ->
                    lib_process_cache:latest(ProcessID, [], Opts)
                end),
            case LatestKnown of
                {ok, LatestSlot, RawLatestMsg} ->
                    case lib_process_cache:fresh(ProcessID, LatestSlot, Req, Opts) of
                        {error, _} = Error ->
                            Error;
                        true ->
                            % Marked as a cache hit: it is public state only
                            % (never a base to compute from), and the store
                            % already holds it, so result caching must not
                            % write it back on every read.
                            LatestMsg =
                                mark_cached_state(
                                    without_snapshot(RawLatestMsg, Opts),
                                    Opts
                                ),
                            ?event(compute_cache,
                                {serving_latest_cached_state,
                                    {proc_id, ProcessID},
                                    {slot, LatestSlot}
                                },
                                Opts
                            ),
                            dev_process_worker:notify_compute(
                                ProcessID,
                                LatestSlot,
                                {ok, LatestMsg},
                                Opts
                            ),
                            {ok, LatestMsg};
                        false ->
                            ?event(compute_cache,
                                {latest_cached_state_stale,
                                    {proc_id, ProcessID},
                                    {slot, LatestSlot}
                                },
                                Opts
                            ),
                            uncached_now(
                                CacheParam,
                                Base,
                                Req,
                                Opts,
                                <<"No fresh cached state available.">>
                            )
                    end;
                _ ->
                    uncached_now(CacheParam, Base, Req, Opts)
            end
    end.

uncached_now(CacheParam, Base, Req, Opts) ->
    uncached_now(CacheParam, Base, Req, Opts, <<"No cached state available.">>).

uncached_now(always, _Base, _Req, _Opts, Failure) ->
    {failure, Failure};
uncached_now(<<"always">>, _Base, _Req, _Opts, Failure) ->
    {failure, Failure};
uncached_now(_CacheParam, Base, Req, Opts, _Failure) ->
    % The node is configured to use the cache if possible, but forcing
    % computation is also admissible.
    now(Base, Req, Opts#{ <<"process-now-from-cache">> => false }).

%% @doc The slots of a process whose outbox is not delivered yet, after making
%% sure its deliverer runs (`lib_process_delivery'). At boot the node calls it
%% with `process-delivery-sweep' set, and then it starts a deliverer for
%% every process this node schedules that is owed a delivery: a slot stored
%% before a restart is still delivered, whether or not its process is ever
%% computed again. With `process-delivery-metrics' set (the metrics endpoint
%% does), it gives the node's delivery backlog instead
%% (`lib_process_delivery:metrics/0').
deliveries(Base, _Req, Opts) ->
    case {hb_opts:get(<<"process-delivery-metrics">>, false, Opts),
            hb_opts:get(<<"process-delivery-sweep">>, false, Opts)} of
        {true, _} -> {ok, lib_process_delivery:metrics()};
        {_, true} ->
            {ok, #{ <<"started">> => lib_process_delivery:sweep(Opts) }};
        _ ->
            ProcBase = lib_process:ensure_process_key(Base, Opts),
            ProcID = lib_process:process_id(ProcBase, #{}, Opts),
            Pending = lib_process_delivery:pending(ProcID, Opts),
            case Pending of
                [] -> ok;
                _ -> _ = lib_process_delivery:ensure(ProcID, Opts)
            end,
            {ok, #{ <<"pending">> => Pending }}
    end.

%% @doc Recursively push messages to the scheduler until we find a message
%% that does not lead to any further messages being scheduled.
-spec push(#{ push => _, _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, _} | {error, _}.
push(Base, Req, Opts) ->
    hb_phase_timer:time(push, fun() ->
        lib_process:run_as(
            <<"push">>,
            lib_process:ensure_process_key(Base, Opts),
            Req,
            Opts
        )
    end).

%% @doc Set the private `initialized' key of a state that the process device
%% returns: the public part of the state itself. Its ID is what tells a
%% changed state apart, but computing an ID hashes every published key, and
%% the worker did it twice a slot (here and in `is_initialized/2'). The public
%% part is kept instead, sharing the state's memory, and compared by term
%% equality first, which takes microseconds for the unchanged term.
set_initialized(State, _Opts) ->
    hb_private:set_priv(
        State,
        (hb_private:from_message(State))#{ <<"initialized">> => public(State) }
    ).

%% @doc Return whether the state is the one that the process device returned:
%% its public part is the one `set_initialized/2' kept, or has its ID.
is_initialized(State, Opts) ->
    case hb_private:get(<<"initialized">>, State, Opts) of
        Public when is_map(Public) ->
            Now = public(State),
            Now =:= Public orelse
                hb_message:id(Now, uncommitted, Opts) =:=
                    hb_message:id(Public, uncommitted, Opts);
        StateID when is_binary(StateID) ->
            StateID =:= hb_message:id(State, uncommitted, Opts);
        _ ->
            false
    end.

%% @doc Whether a state is, term for term, the one the process device
%% returned: `is_initialized/2' without its ID comparison.
is_live_state(State) when is_map(State) ->
    case hb_private:from_message(State) of
        #{ <<"initialized">> := Public } when is_map(Public) ->
            public(State) =:= Public;
        _ -> false
    end;
is_live_state(_State) ->
    false.

public(State) -> maps:remove(<<"priv">>, State).

%% @doc Ensure that the process message we have in memory is live and
%% up-to-date.
ensure_loaded(Base, Req, Opts) ->
    case is_cached_state(Base) of
        true ->
            % A state read back from the cache (or sent to a listener) is
            % public only: it has no execution-device state, so it must never
            % be computed onward. Restore from the process definition instead.
            ensure_loaded_state(
                hb_maps:get(<<"process">>, Base, Base, Opts),
                Req,
                Opts
            );
        false ->
            ensure_loaded_state(Base, Req, Opts)
    end.

ensure_loaded_state(Base, Req, Opts) ->
    ensure_loaded_state_inner(Base, Req, Opts).

%% @doc `ok' when a process may be initialized from its definition: retention
%% never deleted any of its state, and nothing of it is stored or its lowest
%% stored slot is slot 0. Retention keeps every snapshot, slot 0's among
%% them, so a lowest slot of 0 alone does not say nothing was pruned: the
%% marker does (`lib_process_cache:pruned/2').
restorable_from_start(ProcID, Opts) ->
    case hb_opts:get(<<"process-replay-from-start">>, false, Opts) == true orelse
            {lib_process_cache:pruned(ProcID, Opts), lib_process_cache:stored_floor(ProcID, Opts)} of
        true -> ok;
        {false, none} -> ok;
        {false, {ok, 0}} -> ok;
        {_, Lowest} ->
            Floor = case Lowest of {ok, F} -> F; none -> 0 end,
            ?event(compute, {refused_restore_from_start, ProcID, {lowest_stored, Floor}}, Opts),
            Failure =
                #{
                    <<"status">> => 500,
                    <<"body">> =>
                        <<"No checkpoint of ", (hb_util:bin(ProcID))/binary,
                          " to restore from, and slots of it are pruned (lowest stored ",
                          (hb_util:bin(Floor))/binary,
                          "): it is never replayed from the start.">>
                },
            hb_hook:on(
                <<"process-failure">>,
                #{ <<"process-id">> => ProcID, <<"slot">> => Floor, <<"body">> => Failure },
                Opts
            ),
            {error, Failure}
    end.

ensure_loaded_state_inner(Base, Req, Opts) ->
    % Get the nonce we are currently on and the inbound nonce.
    TargetSlot = hb_ao:get(<<"slot">>, Req, undefined, Opts),
    ColdTried = is_map(Req) andalso maps:get(<<"cold-tried">>, Req, false) =:= true,
    ProcID = lib_process:process_id(Base, #{}, Opts),
    ?event({ensure_loaded, {base, Base}, {req, Req}}),
    % A state is live only while it is the state that the process device
    % returned; a step that changes the state changes its ID.
    case is_initialized(Base, Opts) of
        true ->
            ?event(already_initialized),
            % A state equal to the one the device returned is normalised
            % (`normalized/2'); one that only has its ID is normalised here.
            case is_live_state(Base) of
                true -> {ok, Base};
                false -> {ok, without_snapshot(Base, Opts)}
            end;
        false ->
            ?event(not_initialized),
            % Try to load the latest complete state from disk.
            LoadRes =
                lib_process_cache:latest(
                    ProcID,
                    [<<"snapshot">>],
                    TargetSlot,
                    Opts
                ),
            ?event(compute,
                {snapshot_load_res,
                    {proc_id, ProcID},
                    {res, LoadRes},
                    {target, TargetSlot}
                },
                Opts
            ),
            case LoadRes of
                {ok, MaybeLoadedSlot, SnapshotMsg} ->
                    % Restore the devices in the executor stack with the
                    % loaded state. This allows the devices to load any
                    % necessary 'shadow' state (state not represented in
                    % the public component of a message) into memory.
                    % Do not update the hashpath while we do this, and remove
                    % the snapshot key after we have normalized the message.
                    SnapshotReq =
                        hb_private:set(
                            SnapshotMsg,
                            <<"initialized">>,
                            <<"true">>,
                            Opts
                        ),
                    LoadedSlot =
                        hb_cache:ensure_all_loaded(MaybeLoadedSlot, Opts),
                    ?event(compute,
                        {found_state_checkpoint,
                            {proc_id, ProcID},
                            {slot, LoadedSlot}
                        },
                        Opts
                    ),
                    {ok, Normalized} =
                        lib_process:run_as(
                            <<"execution">>,
                            SnapshotReq,
                            normalize,
                            Opts#{ <<"hashpath">> => ignore }
                        ),
                    NormalizedWithoutSnapshot =
                        without_snapshot(Normalized, Opts),
                    ?event(snapshot,
                        {loaded_state_checkpoint_result,
                            {proc_id, ProcID},
                            {slot, LoadedSlot},
                            {after_normalization, NormalizedWithoutSnapshot}
                        }
                    ),
                    {ok, set_initialized(NormalizedWithoutSnapshot, Opts)};
                {error, not_found} when not ColdTried ->
                    % None held here: one shipped to the cold tier is put
                    % back and loaded (`hb_store_cold'); else, as below.
                    case hb_store_cold:restore_checkpoint(ProcID,
                            case is_integer(TargetSlot) of true -> TargetSlot; false -> 1 bsl 60 end,
                            Opts) of
                        {ok, _} -> ensure_loaded_state_inner(Base, Req, Opts);
                        not_found ->
                            ensure_loaded_state_inner(Base, Req#{ <<"cold-tried">> => true }, Opts)
                    end;
                {error, not_found} ->
                    % If we do not have a checkpoint, initialize the
                    % process from scratch -- but only when nothing of it was
                    % ever pruned. A process whose lowest stored slot is above
                    % 0 replayed from its definition would recompute (and
                    % re-emit) every slot retention deleted: on prod a replay
                    % from slot 0 of the vault ran for seven hours
                    % (2026-11-05). It fails closed instead.
                    ?event(
                        {no_checkpoint_found,
                            {process, ProcID},
                            {slot, TargetSlot}
                        }
                    ),
                    case restorable_from_start(ProcID, Opts) of
                        {error, _} = Refused -> Refused;
                        ok ->
                            % A process runs from its committed keys only,
                            % whether the request gives the process itself or
                            % carries it.
                            {ok, Process} =
                                hb_message:with_only_committed(
                                    hb_maps:get(<<"process">>, Base, Base, Opts),
                                    Opts
                                ),
                            {ok, _} =
                                hb_cache:write(Process, lib_process:cache_opts(Opts)),
                            init(
                                lib_process:ensure_process_key(Process, Opts),
                                Req,
                                Opts
                            )
                    end
            end
    end.

%% @doc Mark a state that was read back from the process cache. Cached states
%% are public only: they carry no execution-device state (for Lua, no VM), so
%% they are answers, never a base to compute the next slot from.
mark_cached_state(Msg, Opts) ->
    hb_private:set(Msg, #{ <<"process-cached-state">> => true }, Opts).

%% @doc Return `true' if a state came from the process cache rather than from
%% executing the process.
is_cached_state(Msg) ->
    maps:get(<<"process-cached-state">>, hb_private:from_message(Msg), false)
        =:= true.

%% @doc Remove the `snapshot' key from a message and return it.
without_snapshot(Msg, Opts) ->
    hb_ao:set(Msg, <<"snapshot">>, unset, Opts).

%% @doc A process definition is verified once, not on every request; a
%% different (tampered) definition is still verified and rejected.
process_id_verifies_once_test() ->
    application:ensure_all_started(hb),
    Opts = #{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"priv-wallet">> => ar_wallet:new()
    },
    Process = hb_process_test_vectors:aos_process(Opts),
    Base = #{ <<"process">> => Process },
    Verifies =
        fun(Fun) ->
            erlang:trace_pattern({hb_message, verify, 3}, true, [call_count]),
            Res = Fun(),
            {call_count, N} = erlang:trace_info({hb_message, verify, 3}, call_count),
            erlang:trace_pattern({hb_message, verify, 3}, false, [call_count]),
            {Res, N}
        end,
    {ID, _} = Verifies(fun() -> lib_process:process_id(Base, #{}, Opts) end),
    ?assertEqual(hb_message:id(Process, signed, Opts), ID),
    ?assertEqual(
        {ID, 0},
        Verifies(fun() -> lib_process:process_id(Base, #{}, Opts) end)
    ),
    Tampered = Process#{ <<"scheduler-location">> => <<"someone-else">> },
    ?assertThrow(
        {process_not_verified, _},
        lib_process:process_id(#{ <<"process">> => Tampered }, #{}, Opts)
    ).


%% @doc The target slot is snapshotted by default; with
%% `process-snapshot-target => false' only the configured interval snapshots.
snapshot_target_option_test() ->
    application:ensure_all_started(hb),
    lists:foreach(
        fun({Setting, Expected}) ->
            Opts = #{
                <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
                <<"priv-wallet">> => ar_wallet:new(),
                <<"process-snapshot-slots">> => 1000,
                <<"process-snapshot-time">> => false,
                <<"process-snapshot-target">> => Setting
            },
            Base = hb_process_test_vectors:aos_process(Opts),
            ProcID = hb_message:id(Base, all),
            hb_process_test_vectors:schedule_aos_call(Base, <<"return 1">>, Opts),
            hb_process_test_vectors:schedule_aos_call(Base, <<"return 2">>, Opts),
            {ok, _} =
                hb_ao:resolve(
                    Base,
                    #{ <<"path">> => <<"compute">>, <<"slot">> => 1 },
                    Opts
                ),
            {ok, Stored} = lib_process_cache:read(ProcID, 1, Opts),
            ?assertEqual(
                {Setting, Expected},
                {Setting, hb_maps:is_key(<<"snapshot">>, Stored, Opts)}
            )
        end,
        [{true, true}, {false, false}]
    ).

%% @doc `compute&slot=<message id>' computes the slot whose assignment carries
%% that message, the same as asking for the slot by number; an ID nothing here
%% assigned is a 404, and anything else that is not a slot is still a 400.
compute_slot_by_message_id_test() ->
    application:ensure_all_started(hb),
    Opts = #{ <<"store">> => hb_test_utils:test_store(hb_store_lmdb) },
    Base = hb_process_test_vectors:test_process(),
    Wallet = hb_opts:get(priv_wallet, hb:wallet(), Opts),
    WOpts = Opts#{ <<"priv-wallet">> => Wallet },
    Schedule = fun(Text) ->
        Body = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"test-label">> => Text }, WOpts),
        {ok, _} = hb_ao:resolve(Base,
            hb_message:commit(#{ <<"path">> => <<"schedule">>, <<"method">> => <<"POST">>,
                <<"body">> => Body }, WOpts), WOpts),
        hb_message:id(Body, signed, Opts)
    end,
    _One = Schedule(<<"one">>),
    Two = Schedule(<<"two">>),
    Compute = fun(Slot) ->
        hb_ao:resolve(Base, #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, Opts)
    end,
    {ok, BySlot} = Compute(2),
    {ok, ById} = Compute(Two),
    ?assertEqual(2, hb_ao:get(<<"at-slot">>, ById, Opts)),
    ?assertEqual(
        hb_ao:get(<<"results/assignment-slot">>, BySlot, Opts),
        hb_ao:get(<<"results/assignment-slot">>, ById, Opts)),
    Unknown = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assertMatch({error, #{ <<"status">> := 404 }}, Compute(Unknown)),
    ?assertMatch({error, #{ <<"status">> := 400 }}, Compute(<<"not-a-slot">>)).

%% @doc A device that returns the state without a delta of its own (here the
%% test device) never carries the previous slot's delta, with its outbox, into
%% the next slot: `prepare_next_slot/4' drops it.
prepare_drops_previous_delta_test() ->
    application:ensure_all_started(hb),
    Opts = #{ <<"store">> => hb_test_utils:test_store(hb_store_lmdb) },
    Base = hb_process_test_vectors:test_process(),
    ProcID = hb_message:id(Base, all, Opts),
    hb_process_test_vectors:schedule_test_message(Base, <<"one">>, #{}, Opts),
    hb_process_test_vectors:schedule_test_message(Base, <<"two">>, #{}, Opts),
    {ok, State1} = hb_ao:resolve(Base, #{ <<"path">> => <<"compute">>, <<"slot">> => 1 }, Opts),
    Outbox = #{ <<"1">> => #{ <<"target">> => ProcID, <<"data">> => <<"again">> } },
    Stale = hb_private:set(State1, <<"process-cache-delta">>,
        #{ <<"patches">> => [], <<"results">> => #{ <<"outbox">> => Outbox } }, Opts),
    {ok, 2, Prepared, Req} = prepare_next_slot(ProcID, Stale, #{ <<"slot">> => 2 }, Opts),
    {ok, State2} = lib_process:run_as(<<"execution">>, Prepared, Req,
        lib_process:execution_opts(Opts)),
    ?assertEqual(2, hb_ao:get(<<"results/assignment-slot">>, State2, Opts)),
    ?assertEqual(not_found, hb_private:get(<<"process-cache-delta">>, State2, not_found, Opts)).

%% @doc A `lua-patch@1.0' process whose slots publish nested messages and
%% nested results, rewriting some keys and adding others.
nested_patch_script() ->
    <<
        "Count = Count or 0\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  return {\n"
        "    patches = {\n"
        "      { path = '/count', value = tostring(Count) },\n"
        "      { path = '/player-' .. (Count % 3), value = { level = Count,\n"
        "          name = 'p' .. Count, inner = { wins = Count * 2, tags = { 'a', 'b' } } } },\n"
        "      { path = '/nested/deep', value = 'd' .. Count }\n"
        "    },\n"
        "    results = { output = { data = tostring(Count), extra = { n = Count } } }\n"
        "  }\n"
        "end\n"
    >>.

%% @doc Every state a slot leaves is already in the form the resolver gives a
%% device -- decoded by `structured@1.0', stripped of unsigned commitments --
%% so normalising the whole of it changes nothing. A slot normalises only the
%% keys it changed (`lib_process:normalize_changed/3') and hands the state on
%% as a `normalized-base'; this is what makes that the state that normalising
%% every key after every slot left.
slot_states_are_normalized_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        lists:foreach(
            fun(Workers) ->
                Opts = #{
                    <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
                    <<"priv-wallet">> => ar_wallet:new(),
                    <<"hashpath">> => ignore,
                    <<"process-workers">> => Workers,
                    <<"spawn-worker">> => Workers,
                    <<"process-worker-spawn">> => Workers
                },
                Process = hb_process_test_vectors:lua_patch_process(nested_patch_script(), Opts),
                {ok, _} = hb_cache:write(Process, Opts),
                [ hb_process_test_vectors:schedule_test_message(
                      Process, <<"inc">>, #{ <<"n">> => N }, Opts)
                || N <- lists:seq(1, 5) ],
                lists:foreach(
                    fun(Slot) ->
                        {ok, State} =
                            hb_ao:resolve(
                                Process,
                                #{ <<"path">> => <<"compute">>, <<"slot">> => Slot },
                                Opts
                            ),
                        Public = maps:remove(<<"priv">>, State),
                        {ok, Decoded} =
                            hb_ao:raw(<<"structured@1.0">>, <<"to">>, Public, #{}, Opts),
                        ?assertEqual(Public, hb_ao:execution_input(Decoded, Opts)),
                        ?assertEqual(
                            integer_to_binary(Slot + 1),
                            hb_ao:get(<<"count">>, State, Opts)
                        ),
                        ?assertEqual(
                            Slot + 1,
                            hb_ao:get(<<"results/output/extra/n">>, State, Opts)
                        )
                    end,
                    lists:seq(0, 4)
                )
            end,
            [false, true]
        )
    end}.

%% @doc `lib_process:set_keys/3' and `get_key/3' give what `hb_ao:set/3' and
%% `hb_ao:get/3' give on a normalised state, with or without commitments.
set_and_get_keys_match_resolution_test() ->
    application:ensure_all_started(hb),
    Opts = #{ <<"hashpath">> => ignore },
    State = #{
        <<"device">> => <<"process@1.0">>,
        <<"at-slot">> => 4,
        <<"results">> => #{ <<"output">> => #{ <<"data">> => <<"x">> } },
        <<"player-1">> => #{ <<"level">> => 2 },
        <<"count">> => <<"7">>
    },
    Committed = hb_message:commit(State, #{ <<"priv-wallet">> => ar_wallet:new() }),
    Values = #{
        <<"device">> => <<"scheduler@1.0">>,
        <<"at-slot">> => 5,
        <<"results">> => unset,
        <<"snapshot">> => unset,
        <<"output-prefixes">> => undefined
    },
    lists:foreach(
        fun(S) ->
            ?assertEqual(hb_ao:set(S, Values, Opts), lib_process:set_keys(S, Values, Opts)),
            ?assertEqual(
                hb_ao:get(<<"at-slot">>, S, Opts),
                lib_process:get_key(<<"at-slot">>, S, Opts)
            )
        end,
        [State, Committed]
    ).

%% @doc The state the device returned is live by term equality, without an
%% ID; a state with a changed key is not, and one whose public part was
%% rebuilt with the same content still is.
live_state_test() ->
    State = #{ <<"a">> => <<"1">>, <<"b">> => #{ <<"c">> => 2 } },
    Live = set_initialized(State#{ <<"priv">> => #{ <<"state">> => vm } }, #{}),
    ?assert(is_live_state(Live)),
    ?assert(is_initialized(Live, #{})),
    Changed = Live#{ <<"a">> => <<"2">> },
    ?assertNot(is_live_state(Changed)),
    ?assertNot(is_initialized(Changed, #{})),
    Rebuilt = maps:map(fun(_, V) -> V end, Live),
    ?assert(is_live_state(Rebuilt)),
    ?assertNot(is_live_state(State)).

%%% A process's state only moves forward.

%% @doc A Lua counter run by `lua-patch@1.0' (a delta process): slot N leaves
%% `count' at N + 1.
pruned_counter_script() ->
    <<
        "Count = Count or 0\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  local value = tostring(Count)\n"
        "  return {\n"
        "    patches = { { path = '/count', value = value } },\n"
        "    results = { output = { data = value } }\n"
        "  }\n"
        "end\n"
    >>.

%% @doc A delta process stored as `pstate/' rows, `Slots' messages scheduled
%% and computed, a VM snapshot every 5 slots, and retention run over it: with
%% the newest two snapshot checkpoints kept, its floor is `Slots - 1 - 5'.
pruned_setup(Slots) ->
    application:ensure_all_started(hb),
    Opts =
        #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"process-state-blobs">> => true,
            <<"process-delta-cache">> => true,
            <<"process-hot-cache-slots">> => 0,
            <<"process-snapshot-slots">> => 5,
            <<"process-snapshot-time">> => false,
            <<"process-snapshot-target">> => false,
            <<"process-compute-on-schedule">> => false,
            <<"spawn-worker">> => true,
            <<"process-workers">> => true,
            <<"process-worker-spawn">> => true,
            <<"await-inprogress">> => named
        },
    Process = hb_process_test_vectors:lua_patch_process(pruned_counter_script(), Opts),
    {ok, _} = hb_cache:write(Process, Opts),
    ProcID = hb_message:id(Process, all, Opts),
    [ hb_process_test_vectors:schedule_test_message(
          Process, <<"inc">>, #{ <<"n">> => N }, Opts)
    || N <- lists:seq(1, Slots) ],
    Head = Slots - 1,
    {ok, HeadState} =
        hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => Head }, Opts),
    ?assertEqual(integer_to_binary(Slots), hb_ao:get(<<"count">>, HeadState, Opts)),
    % No age floor and no slot window: only the newest checkpoints, and the
    % hourly ones below them, are kept, as the tests of reads below the
    % floor need.
    #{ deleted := Deleted } =
        hb_store_retention:retain(Opts#{ <<"store-retention-keep-seconds">> => 0,
            <<"store-retention-keep-slots">> => 0,
            <<"store-retention-checkpoint-tiers">> => [[3600, 24]] }),
    ?assert(Deleted > 0),
    {Process, ProcID, Head, Opts}.

%% @doc Retention's default policy (the newest two snapshots, and every slot
%% of the window below the head) over a delta process: every slot of the
%% window reads back as computed, without replay; nothing below the floor
%% is left; and a worker started afresh resumes from the kept head and
%% computes on.
window_prune_keeps_reads_test_() ->
    {timeout, 180, fun() ->
        application:ensure_all_started(hb),
        Opts =
            #{
                <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
                <<"priv-wallet">> => ar_wallet:new(),
                <<"process-state-blobs">> => true,
                <<"process-delta-cache">> => true,
                <<"process-hot-cache-slots">> => 0,
                <<"process-snapshot-slots">> => 3,
                <<"process-snapshot-time">> => false,
                <<"process-snapshot-target">> => false,
                <<"process-compute-on-schedule">> => false,
                <<"spawn-worker">> => true,
                <<"process-workers">> => true,
                <<"process-worker-spawn">> => true,
                <<"await-inprogress">> => named,
                <<"store-retention-keep-slots">> => 6
            },
        Process = hb_process_test_vectors:lua_patch_process(pruned_counter_script(), Opts),
        {ok, _} = hb_cache:write(Process, Opts),
        ProcID = hb_message:id(Process, all, Opts),
        Schedule = fun(N) ->
            hb_process_test_vectors:schedule_test_message(
                Process, <<"inc">>, #{ <<"n">> => N }, Opts)
        end,
        [ Schedule(N) || N <- lists:seq(1, 20) ],
        Count = fun(S) ->
            {ok, St} = hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => S }, Opts),
            hb_ao:get(<<"count">>, St, Opts)
        end,
        ?assertEqual(<<"20">>, Count(19)),
        #{ deleted := Deleted } = hb_store_retention:retain(Opts),
        ?assert(Deleted > 0),
        % The floor is the newest snapshot at least 6 below the head (19): 12.
        ?assertEqual([15, 18], hb_process_test_vectors:pstate_slots(ProcID, <<"s">>, Opts) -- [12]),
        ?assertEqual(12, hd(stored_slots(ProcID, Opts))),
        Execs = slot_executions(),
        [ ?assertEqual(integer_to_binary(S + 1), Count(S)) || S <- lists:seq(13, 19) ],
        ?assertEqual(Execs, slot_executions()),
        % Without a worker, the state is loaded from what is kept, and the
        % process computes on from it.
        Schedule(21),
        NoWorker = Opts#{ <<"process-workers">> => false, <<"spawn-worker">> => false },
        {ok, St20} = hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => 20 }, NoWorker),
        ?assertEqual(<<"21">>, hb_ao:get(<<"count">>, St20, NoWorker))
    end}.

slot_executions() ->
    lists:sum([ N || {slot_execution, _, N, _} <- hb_phase_timer:read() ]).

stored_slots(ProcID, Opts) ->
    lists:usort(
        lists:append(
            [ hb_process_test_vectors:pstate_slots(ProcID, K, Opts)
            || K <- [<<"d">>, <<"f">>, <<"s">>] ])).

%% @doc Each checkpoint a computed slot writes is dated by its assignment in
%% its `s' marker, and with the default age floor (three days) retention
%% deletes nothing of a process computed just now.
snapshot_markers_dated_by_assignment_test_() ->
    {timeout, 180, fun() ->
        T0 = erlang:system_time(millisecond),
        {_Process, ProcID, _Head, Opts} = pruned_setup(16),
        Store = hb_opts:get(<<"store">>, undefined, Opts),
        Snapshots = hb_process_test_vectors:pstate_slots(ProcID, <<"s">>, Opts),
        ?assertNotEqual([], Snapshots),
        lists:foreach(
            fun(S) ->
                {ok, Marker} =
                    hb_store:read(Store, lib_process_cache:blob_key(ProcID, <<"s">>, S), Opts),
                {ok, Ms} = hb_store_retention:marker_time(Marker),
                ?assert(Ms >= T0 - 1000 andalso Ms =< erlang:system_time(millisecond)),
                % The stored assignment's own time, which retention reads
                % for an empty marker.
                ?assertEqual({ok, Ms}, hb_store_retention:assignment_time(ProcID, S, Opts))
            end,
            Snapshots),
        Kept = stored_slots(ProcID, Opts),
        ?assertMatch(#{ deleted := 0 }, hb_store_retention:retain(Opts)),
        ?assertEqual(Kept, stored_slots(ProcID, Opts))
    end}.

%% @doc Prod, 2026-11-05: detached pushes of the order-book vault asked for
%% slots ~85k below its head after retention had deleted them, and each one
%% replayed the vault from the start in the requester, into its cache -- ~30
%% at once, for hours, with no way to cancel them. A slot at or below the
%% head that is not stored is rebuilt for the request alone: replayed from
%% the nearest kept snapshot below it, one replay at a time, with nothing
%% written; the process carries on forward. With `process-history-replay'
%% false it is a 410 and nothing is executed.
pruned_slot_is_replayed_not_stored_test_() ->
    {timeout, 180, fun() ->
        {Process, ProcID, Head, Opts} = pruned_setup(31),
        Kept = stored_slots(ProcID, Opts),
        % The oldest snapshot of the hour (slot 0) and the newest two are
        % kept; the snapshots between them and every other slot below the
        % floor are not.
        ?assert(lists:member(0, Kept)),
        ?assertNot(lists:member(5, Kept)),
        Pruned = [ S || S <- lists:seq(0, Head), not lists:member(S, Kept) ],
        ?assert(lists:member(1, Pruned)),
        ?assertEqual({ok, Head}, lib_process_cache:latest_slot(ProcID, Opts)),
        Compute =
            fun(Slot, O) ->
                hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, O)
            end,
        Last = lists:last(Pruned),
        Old = [Last, 1, 6],
        Before0 = slot_executions(),
        [ ?assertMatch({error, #{ <<"status">> := 410 }},
              Compute(Slot, Opts#{ <<"process-history-replay">> => false }))
        || Slot <- Old ],
        ?assertEqual(Before0, slot_executions()),
        % The first request starts a rebuild and is answered at once with
        % 202; the others join it (slots it passes on the way).
        [ ?assertMatch({error, #{ <<"status">> := 202 }}, Compute(Slot, Opts)) || Slot <- Old ],
        Await =
            fun Await(Slot, N) ->
                case Compute(Slot, Opts) of
                    {ok, State} -> State;
                    {error, #{ <<"status">> := 202 }} when N > 0 ->
                        timer:sleep(100), Await(Slot, N - 1)
                end
            end,
        [ ?assertEqual(integer_to_binary(Slot + 1), hb_ao:get(<<"count">>, Await(Slot, 300), Opts))
        || Slot <- Old ],
        % One replay, from slot 0 (the only snapshot below them) to the
        % newest asked for: the others were kept on its way.
        ?assertEqual(Last, slot_executions() - Before0),
        % Asked again, it is instant: from the cache, nothing replayed.
        ?assertMatch({ok, _}, Compute(1, Opts)),
        ?assertEqual(Last, slot_executions() - Before0),
        ?assertEqual(Kept, stored_slots(ProcID, Opts)),
        Before = slot_executions(),
        % A kept slot still reads back, and the next one computes forward.
        {ok, Floor} =
            hb_ao:resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => lists:min(Kept) },
                Opts
            ),
        ?assertEqual(
            integer_to_binary(lists:min(Kept) + 1),
            hb_ao:get(<<"count">>, Floor, Opts)
        ),
        hb_process_test_vectors:schedule_test_message(
            Process, <<"inc">>, #{ <<"n">> => Head + 2 }, Opts),
        {ok, Next} =
            hb_ao:resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => Head + 1 },
                Opts
            ),
        ?assertEqual(integer_to_binary(Head + 2), hb_ao:get(<<"count">>, Next, Opts)),
        ?assertEqual(Before + 1, slot_executions())
    end}.

%% @doc `now' names the scheduler's current slot as the head only once that
%% slot is stored. It used to refresh the head before computing it, so a
%% compute that failed left an uncomputed slot named as the head -- and every
%% slot up to the head is refused as pruned.
now_refreshes_only_stored_head_test_() ->
    {timeout, 60, fun() ->
        application:ensure_all_started(hb),
        Opts = #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"process-compute-on-schedule">> => false
        },
        Base =
            hb_process_test_vectors:lua_process(
                <<"function inc(base) base.count = (base.count or 0) + 1 return base end\n"
                  "function throw(base) error('The process failed.') end\n"
                  "function compute(base, req) "
                  "if req.body.path == 'throw' then return throw(base) end "
                  "return inc(base) end">>,
                Opts
            ),
        ProcID = hb_message:id(Base, all, Opts),
        hb_process_test_vectors:schedule_test_message(
            Base, <<"inc">>, #{ <<"path">> => <<"inc">> }, Opts),
        hb_process_test_vectors:schedule_test_message(
            Base, <<"throw">>, #{ <<"path">> => <<"throw">> }, Opts),
        {ok, _} =
            hb_ao:resolve(Base, #{ <<"path">> => <<"compute">>, <<"slot">> => 0 }, Opts),
        ?assertEqual({ok, 0}, lib_process_cache:latest_slot(ProcID, Opts)),
        ?assertMatch({failure, _}, now(Base, #{ <<"max-age">> => 0 }, Opts)),
        ?assertEqual({ok, 0}, lib_process_cache:latest_slot(ProcID, Opts))
    end}.

%% @doc A process with no checkpoint left to restore from, whose low slots
%% retention deleted, is never initialized from its definition and replayed:
%% that replay re-runs (and re-emits) every pruned slot. It fails closed with
%% a 500, executing nothing (prod, 2026-11-05: a replay of the vault from
%% slot 0 ran for seven hours).
restore_from_zero_refused_when_pruned_test_() ->
    {timeout, 180, fun() ->
        {Process, ProcID, Head, Opts} = pruned_setup(31),
        Store = hb_opts:get(<<"store">>, undefined, Opts),
        #{ <<"db">> := DB } = hb_store:find(Store),
        Snapshots = hb_process_test_vectors:pstate_slots(ProcID, <<"s">>, Opts),
        ?assertNotEqual([], Snapshots),
        {ok, _} =
            elmdb:delete_batch(
                DB,
                [ <<"pstate/", ProcID/binary, "/s/",
                    (iolist_to_binary(io_lib:format("~12..0B", [S])))/binary>>
                || S <- Snapshots ]
            ),
        % Retention kept the snapshot checkpoints' states, slot 0's among
        % them, so only the marker says the process was pruned.
        ?assert(lib_process_cache:pruned(ProcID, Opts)),
        Before = slot_executions(),
        Inline =
            Opts#{
                <<"process-workers">> => false,
                <<"spawn-worker">> => false,
                <<"process-worker-spawn">> => false
            },
        ?assertMatch(
            {error, #{ <<"status">> := 500 }},
            hb_ao:resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => Head + 1 },
                Inline
            )
        ),
        ?assertEqual(Before, slot_executions())
    end}.

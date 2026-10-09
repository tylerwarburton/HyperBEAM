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
%%% `dev_process_cache' for details.
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
%%%     process-historical-replay-limit:
%%%                      The most slots a read of a slot behind the state it
%%%                      is computed from may replay from the newest
%%%                      checkpoint at or below it (default 1000; `infinity'
%%%                      for no bound). Beyond it the read is a 503.
%%%     process-checkpoint-resume-gap:
%%%                      The gap, in slots, from the loaded state to the target
%%%                      at which computation resumes from a newer checkpoint
%%%                      when there is one (default 32).
-module(dev_process).
-device_libraries([lib_process]).
%%% Public API
-export([info/1, as/3, compute/3, schedule/3, slot/3, now/3, push/3, snapshot/3]).
-export([target_slot/2, request_slot/2, is_cached_state/1]).
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

%% The most slots a historical read (a slot behind the state being computed
%% from) may replay from its checkpoint before it is refused. Overridden with
%% the `process-historical-replay-limit' option; `infinity' removes the bound.
-define(DEFAULT_HISTORICAL_REPLAY_LIMIT, 1000).
%% The smallest gap between the loaded state and the target slot at which the
%% newest checkpoint is looked up to resume from instead. Overridden with the
%% `process-checkpoint-resume-gap' option.
-define(DEFAULT_CHECKPOINT_RESUME_GAP, 32).

%% @doc When the info key is called, we should return the process exports.
info(_Base) ->
    #{
        worker => fun dev_process_worker:server/3,
        grouper => fun dev_process_worker:group/3,
        await => fun dev_process_worker:await/5,
        exports =>
            [
                <<"info">>,
                <<"as">>,
                <<"compute">>,
                <<"now">>,
                <<"schedule">>,
                <<"slot">>,
                <<"snapshot">>,
                <<"push">>
            ]
    }.

%% @doc Return the process state with the device swapped out for the device
%% of the given key.
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
default_device(Base, Key, Opts) ->
    lib_process:default_device(Base, Key, Opts).

%% @doc Wraps functions in the Scheduler device.
schedule(Base, Req, Opts) ->
    lib_process:run_as(<<"scheduler">>, Base, Req, Opts).

slot(Base, Req, Opts) ->
    ?event({slot_called, {base, Base}, {req, Req}}),
    lib_process:run_as(<<"scheduler">>, Base, Req, Opts).

next(Base, _Req, Opts) ->
    lib_process:run_as(<<"scheduler">>, Base, next, Opts).

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
%% `Initialized' key to `True' to indicate that the process has been
%% initialized.
init(Base, Req, Opts) ->
    ?event({init_called, {base, Base}, {req, Req}}),
    {ok, Initialized} =
        lib_process:run_as(
            <<"execution">>,
            Base,
            #{ <<"path">> => <<"init">> },
            Opts
        ),
    {
        ok,
        hb_ao:set(
            Initialized,
            #{
                <<"initialized">> => <<"true">>,
                <<"at-slot">> => -1
            },
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
compute(Base, Req, Opts) ->
    ProcBase = lib_process:ensure_process_key(Base, Opts),
    ProcID = lib_process:process_id(ProcBase, #{}, Opts),
    TargetSlot = request_slot(Req, Opts),
    case TargetSlot of
        {invalid, RawSlot} ->
            ?event(compute, {invalid_slot_requested, {slot, RawSlot}}, Opts),
            {error, invalid_slot_error()};
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
            case dev_process_cache:read(ProcID, Slot, Opts) of
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
                  Res ->
                      % A plain missing slot already arrives as
                      % `{error, not_found}' and was handled, so this is not the
                      % common path. It exists for the shapes that were NOT
                      % handled and reached `case_clause': notably
                      % `{error, {invalid_process_delta_chain, _, _}}', which
                      % `dev_process_cache:materialize/3' returns when a delta's
                      % recorded base slot is not its predecessor. Collection
                      % makes that reachable.
                      %
                      % Such a miss is recoverable: the assignments and the
                      % process definition are the signed ledger and are never
                      % collected, so the slot can be rebuilt by replaying
                      % forward from the nearest retained snapshot, which is
                      % what `compute_to_slot/5' does.
                      %
                      % Anything that is not a recognised miss -- a store
                      % failure, say -- is returned untouched rather than
                      % answered with an expensive replay that would mask it.
                      case recoverable_miss(Res) of
                          false -> Res;
                          true ->
                              compute_miss(ProcID, ProcBase, Req, Slot, Res, Opts)
                      end
            end
    end.

%% @doc Compute a slot that is not in the cache.
%%
%% With a live state in hand, `compute_from/5' decides where to start. Without
%% one -- a cold worker after a restart, or a state read back from the cache --
%% `ensure_loaded/3' would restore the newest checkpoint at or below the target
%% or, with none, initialize the process. For a target below every retained
%% checkpoint of a process that has run past it, that is a replay from slot 0:
%% `process-historical-replay-limit' only guarded a worker already ahead of the
%% target. In production a cold worker asked for slot 43,827 of a process at
%% 50,737 (retention kept checkpoints near 49,000 and 50,000) replayed from 0 at
%% ~400 slots/min, with every live request for the process queued behind it.
%%
%% Such a target is a historical read, so it goes to `compute_historical/5',
%% which answers it off to the side within the limit or with a 503 without
%% doing any work, and leaves the worker cold: its next live request restores
%% the newest checkpoint. A process with no checkpoint above the target is not
%% past it, so its first compute, or a catch-up from its newest checkpoint, is
%% still computed from there, however far.
compute_miss(ProcID, ProcBase, Req, Slot, Miss, Opts) ->
    case is_live_state(ProcBase, Opts) orelse
            not checkpoint_after(ProcID, Slot, Opts) of
        false ->
            compute_historical(ProcID, ProcBase, Req, Slot, Opts);
        true ->
            {ok, Loaded} = ensure_loaded(ProcBase, Req, Opts),
            ?event(compute,
                {recomputing_uncached_slot,
                    {process_id, ProcID},
                    {to_slot, Slot},
                    {cache_said, Miss}},
                Opts
            ),
            compute_from(ProcID, Loaded, Req, Slot, Opts)
    end.

%% @doc Whether `ensure_loaded/3' would use `Base' as it is, rather than
%% restoring a state from the cache.
is_live_state(Base, Opts) ->
    not is_cached_state(Base) andalso
        hb_ao:get(<<"initialized">>, Base, Opts) == <<"true">>.

%% @doc Whether the process has a checkpoint above `Slot'.
checkpoint_after(ProcID, Slot, Opts) ->
    case catch dev_process_cache:newest_slot_after(
            ProcID, [<<"snapshot+link">>], Slot, Opts) of
        {ok, _} -> true;
        _ -> false
    end.

%% @doc Compute `Slot' from the loaded state, choosing where to start.
%%
%% A target behind the loaded state is a historical read. It is computed off to
%% the side by `compute_historical/5' and never becomes the caller's state:
%% previously it went to `compute_to_slot/5' with the live state, whose rewind
%% returned the old state as the result, and the process worker kept it as its
%% live base. A single `compute&slot=-1' (~40ms) thus put a worker live at slot
%% N back at the initialized state, and the next head request replayed every
%% slot from 0, ignoring all checkpoints (6,601 slots, 215s, on stage).
%%
%% A target well ahead of the loaded state resumes from the newest checkpoint
%% at or below it when that is newer than the loaded state. A loaded state that
%% is already initialized short-circuits `ensure_loaded_state/3', so it was
%% replayed forward however far behind the checkpoints it was. The check costs
%% a listing of the process's slots, so it is made only when the gap is at
%% least `process-checkpoint-resume-gap' slots: the steady head path (a gap of
%% a few slots) does no extra work, and a smaller gap replays no more than that.
compute_from(ProcID, Loaded, Req, Slot, Opts) ->
    case loaded_slot(Loaded, Opts) of
        Current when is_integer(Current), Current > Slot ->
            compute_historical(ProcID, Loaded, Req, Slot, Opts);
        Current when is_integer(Current) ->
            Gap = hb_util:int(
                hb_opts:get(
                    <<"process-checkpoint-resume-gap">>,
                    ?DEFAULT_CHECKPOINT_RESUME_GAP,
                    Opts
                )
            ),
            Start =
                case Slot - Current >= Gap of
                    true -> newer_checkpoint(ProcID, Loaded, Current, Req, Slot, Opts);
                    false -> Loaded
                end,
            compute_to_slot(ProcID, Start, Req, Slot, Opts);
        _ ->
            compute_to_slot(ProcID, Loaded, Req, Slot, Opts)
    end.

%% @doc The slot a loaded state is at, or `undefined'.
loaded_slot(State, Opts) ->
    hb_ao:get(<<"at-slot">>, State, undefined, Opts#{ <<"hashpath">> => ignore }).

%% @doc The newest checkpoint at or below `Slot', loaded, if it is ahead of the
%% `Current' slot of `Loaded'; otherwise `Loaded' itself.
newer_checkpoint(ProcID, Loaded, Current, Req, Slot, Opts) ->
    case checkpoint_slot(ProcID, Slot, Opts) of
        Checkpoint when Checkpoint > Current ->
            case rewind(Loaded, Req, Slot, Opts) of
                {ok, Restored} ->
                    case loaded_slot(Restored, Opts) of
                        Restart when is_integer(Restart), Restart > Current ->
                            ?event(compute,
                                {resuming_from_newer_checkpoint,
                                    {proc_id, ProcID},
                                    {loaded, Current},
                                    {checkpoint, Restart},
                                    {target, Slot}
                                },
                                Opts
                            ),
                            Restored;
                        _ -> Loaded
                    end;
                not_found -> Loaded
            end;
        _ -> Loaded
    end.

%% @doc The newest slot at or below `Slot' that holds a full checkpoint, or -1
%% (the initialized state) when there is none.
checkpoint_slot(ProcID, Slot, Opts) ->
    case catch dev_process_cache:latest_slot(ProcID, [<<"snapshot+link">>], Slot, Opts) of
        {ok, Found} -> Found;
        _ -> -1
    end.

%% @doc Compute a slot behind the loaded state, without touching that state.
%%
%% The slot is rebuilt by a throwaway executor started from the newest
%% checkpoint at or below it (or from `init' when there is none, as for -1),
%% and the result is marked as a cached state: it is an answer, never a base
%% to compute onward from, so the process worker keeps its live state
%% (`dev_process_worker:next_base/2'). Push is never triggered for a rebuilt
%% slot; it already ran when the slot was first computed.
%%
%% The rebuild is bounded by `process-historical-replay-limit' (default
%% ?DEFAULT_HISTORICAL_REPLAY_LIMIT slots, `infinity' for none): a read that
%% would replay more slots than that from its checkpoint is answered with a 503
%% before any work is done, since a historical read is cheap to request and
%% the replay runs in the worker every request for the process queues behind.
compute_historical(ProcID, Loaded, Req, Slot, Opts) ->
    From = checkpoint_slot(ProcID, Slot, Opts),
    Limit =
        hb_opts:get(
            <<"process-historical-replay-limit">>,
            ?DEFAULT_HISTORICAL_REPLAY_LIMIT,
            Opts
        ),
    ?event(compute,
        {computing_historical_slot,
            {proc_id, ProcID},
            {target, Slot},
            {loaded, loaded_slot(Loaded, Opts)},
            {checkpoint, From},
            {limit, Limit}
        },
        Opts
    ),
    case within_replay_limit(Slot - From, Limit) of
        false ->
            {error,
                #{
                    <<"status">> => 503,
                    <<"body">> =>
                        <<"Historical slot is too far from a checkpoint to "
                            "recompute.">>,
                    <<"slot">> => Slot,
                    <<"checkpoint">> => From,
                    <<"replay-limit">> => hb_util:bin(Limit)
                }
            };
        true ->
            HistReq = hb_maps:without([<<"push">>], Req, Opts),
            case rewind(Loaded, HistReq, Slot, Opts) of
                {ok, Start} ->
                    case compute_to_slot(ProcID, Start, HistReq, Slot, Opts) of
                        {ok, State} -> {ok, mark_cached_state(State, Opts)};
                        Error -> Error
                    end;
                not_found ->
                    {error,
                        #{
                            <<"status">> => 404,
                            <<"body">> => <<"No state to recompute slot from.">>,
                            <<"slot">> => Slot
                        }
                    }
            end
    end.

within_replay_limit(_Distance, Limit)
        when Limit == infinity; Limit == <<"infinity">>; Limit == false ->
    true;
within_replay_limit(Distance, Limit) ->
    Distance =< hb_util:int(Limit).

%% @doc Whether a `dev_process_cache:read/3' result means "not in the cache",
%% in which case the slot can be rebuilt from the ledger, as opposed to a real
%% failure that must not be hidden behind a replay.
recoverable_miss(not_found) -> true;
recoverable_miss({error, not_found}) -> true;
%% The delta chain is backward-linked, so if a base slot has been collected --
%% or was never written -- materialization reports the break, not the slot.
recoverable_miss({error, {invalid_process_delta_chain, _, _}}) -> true;
recoverable_miss(_) -> false.

%% @doc Return the slot requested by a `compute' request, or `not_found'.
%%
%% A slot reference can arrive HTTP-wrapped as a typed-result map (e.g.
%% `#{<<"ao-result">> => <<"body">>, <<"body">> => <<"243">>}') rather than as
%% the bare scalar, because a path segment such as `compute&slot=243' is parsed
%% into a sub-message. Unwrap it here, at the single point that produces the
%% value, so every caller receives a scalar and no caller needs an unwrapper of
%% its own. Callers still coerce with `hb_util:int/1' as before.
%%
%% Previously `compute/3' passed the map straight into `hb_util:int/1', raising
%% a `function_clause' that killed the process worker. The next request then
%% cold-resumed from the last snapshot: one full re-execution of the preceding
%% slot in the steady state, and up to `process-snapshot-slots' of replay after
%% a gap. Measured on this node before the fix: 171 worker deaths in 26 minutes,
%% an execution factor of 1.99, and read latency of 474ms per replayed slot
%% (r=0.982) reaching 77 seconds at depth 141. (local patch over upstream edge.)
target_slot(Req, Opts) ->
    unwrap_slot(
        hb_ao:get_first(
            [
                {{as, <<"message@1.0">>, Req}, <<"compute">>},
                {{as, <<"message@1.0">>, Req}, <<"slot">>}
            ],
            Opts
        )
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
        Raw -> parse_slot(Raw)
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
    compute_to_slot(ProcID, Base, Req, TargetSlot, Opts, false).
compute_to_slot(ProcID, Base, Req, TargetSlot, Opts, Stored) ->
    case hb_ao:get(<<"at-slot">>, Base, Opts#{ <<"hashpath">> => ignore }) of
        CurrentSlot when CurrentSlot == TargetSlot ->
            % We reached the target height and return. The snapshot here is
            % gated on the configured cadence (`process_snapshot_slots' /
            % `process_snapshot_time') instead of being forced every slot:
            % forcing a full-state snapshot on every latest slot dumps the
            % entire process heap (100s of MB for large Luerl/WASM processes)
            % on every action, blocking the node for seconds per slot and
            % filling disk. Cadence-gating leaves a resume point (first compute
            % always snapshots; then every interval) while cold-resume replays
            % only the slots since the last snapshot. (local override of the
            % upstream force-snapshot behavior.)
            ?event(compute_short,
                {reached_target_slot_returning_state,
                    {proc_id, ProcID},
                    {slot, TargetSlot}
                },
                Opts
            ),
            case Stored of
                false -> store_result(false, ProcID, TargetSlot, Base, Req, Opts);
                true -> ok
            end,
            {ok, without_snapshot(lib_process:as_process(Base, Opts), Opts)};
        CurrentSlot when CurrentSlot < TargetSlot ->
            % Compute the next state transition.
            NextSlot = CurrentSlot + 1,
            % Get the next input message from the scheduler device.
            case next(Base, Req, Opts) of
                {error, Res} ->
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
                                Opts,
                                true
                            );
                        {error, Error} ->
                            % Forward error details back to the caller.
                            {error, Error}
                    end
            end;
        CurrentSlot when CurrentSlot > TargetSlot ->
            % We are being asked for a slot behind the state we hold. The
            % original code threw here, on the assumption that "the cache should
            % already have the result" -- true only while every computed slot is
            % kept forever. Collecting old states breaks exactly that invariant:
            % a worker live at the head, asked for a collected historical slot,
            % would land here and fail.
            %
            % Rewinding is what makes that a latency cost instead of lost
            % history. Every assignment and the process definition are the
            % signed ledger and are never collected, so the slot can always be
            % rebuilt: restart from the newest snapshot at or below the target
            % and replay forward.
            ?event(
                compute,
                {rewinding_to_earlier_slot,
                    {target, TargetSlot},
                    {current, CurrentSlot}
                },
                Opts
            ),
            case rewind(Base, Req, TargetSlot, Opts) of
                {ok, Rewound} ->
                    compute_to_slot(
                        ProcID, Rewound, Req, TargetSlot, Opts, Stored
                    );
                not_found ->
                    % No snapshot at or below the target, so there is nothing to
                    % replay forward from. Preserve the original error rather
                    % than inventing a state.
                    ?event(
                        compute,
                        {error_already_calculated_slot,
                            {target, TargetSlot},
                            {current, CurrentSlot}
                        },
                        Opts
                    ),
                    throw(
                        {error,
                            {already_calculated_slot,
                                {target, TargetSlot},
                                {current, CurrentSlot}
                            }
                        }
                    )
            end
    end.

%% @doc Restart computation from the newest snapshot at or below `TargetSlot'.
%%
%% `ensure_loaded_state/3' already finds that snapshot -- it asks
%% `dev_process_cache:latest/4' for the latest state carrying `snapshot+link'
%% up to a limit -- but it short-circuits on an already-initialized state and
%% would hand back the very state we need to rewind from. So it is given the
%% process definition, which is never initialized, forcing the load.
rewind(Base, Req, TargetSlot, Opts) ->
    Definition = hb_maps:get(<<"process">>, Base, Base, Opts),
    RewindReq = hb_maps:put(<<"slot">>, TargetSlot, Req, Opts),
    case catch ensure_loaded_state(Definition, RewindReq, Opts) of
        {ok, Loaded} ->
            % Only accept a state at or below the target. A state still ahead of
            % it would re-enter this same clause and recurse forever, so treat
            % that as no usable snapshot.
            case hb_ao:get(
                    <<"at-slot">>, Loaded, Opts#{ <<"hashpath">> => ignore }) of
                Slot when is_integer(Slot), Slot =< TargetSlot ->
                    {ok, Loaded};
                Other ->
                    ?event(
                        compute,
                        {rewind_landed_above_target,
                            {target, TargetSlot},
                            {landed, Other}
                        },
                        Opts
                    ),
                    not_found
            end;
        _ ->
            not_found
    end.

%% @doc Compute a single slot for a process, given an initialized state.
compute_slot(ProcID, State, RawInputMsg, InitReq, TargetSlot, Opts) ->
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
    {RuntimeMicroSecs, Res} =
        timer:tc(
            fun() ->
                lib_process:run_as(<<"execution">>, PreparedState, Req, Opts)
            end
        ),
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
        {ok, NewProcStateMsg} ->
            % We have now transformed slot n -> n + 1. Increment the current slot.
            NewProcStateMsgWithSlot =
                hb_ao:set(
                    NewProcStateMsg,
                    #{ <<"device">> => <<"process@1.0">>, <<"at-slot">> => Slot },
                    Opts
                ),
            {StoreTimeMicroSecs, ProcStateWithSnapshot} =
                timer:tc(
                    fun() ->
                        store_result(
                            false,
                            ProcID,
                            Slot,
                            NewProcStateMsgWithSlot,
                            InitReq,
                            Opts
                        )
                    end
                ),
            ?event(compute_short,
                {computed_slot,
                    {proc_id, ProcID},
                    {slot, Slot},
                    {target_slot, TargetSlot},
                    {prep_ms, PrepTimeMicroSecs div 1000},
                    {execution_ms, RuntimeMicroSecs div 1000},
                    {store_ms, StoreTimeMicroSecs div 1000},
                    {action,
                        hb_ao:get(
                            <<"body/action">>,
                            Req,
                            no_action_set,
                            Opts#{ <<"hashpath">> => ignore }
                        )
                    }
                }
            ),
            % Notify waiters only after the slot is readable from the process
            % cache. Waiters may immediately re-enter via `/compute' or `/push',
            % and those paths treat the cache as the completion boundary.
            dev_process_worker:notify_compute(
                ProcID,
                Slot,
                {ok, ProcStateWithSnapshot},
                Opts
            ),
            % Optionally fire an async `/push' for the slot we just cached.
            % Only fresh computes reach this branch; cache hits in `compute/3'
            % short-circuit before we get here, so each slot is push-triggered
            % at most once per node lifetime regardless of how many times the
            % caller polls `/now' or `/compute'.
            maybe_trigger_push(State, Slot, InitReq, Opts),
            {ok, ProcStateWithSnapshot};
        {error, Error} ->
            % An error occurred while computing the slot. Return the details.
            ErrMsg =
                if is_map(Error) -> Error;
                true -> #{ <<"error">> => Error }
                end,
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
            {error,
                ErrMsg#{
                    <<"phase">> => <<"compute">>,
                    <<"attempted-slot">> => Slot
                }
            }
    end.

%% @doc Prepare the process state message for computing the next slot.
prepare_next_slot(ProcID, State, RawReq, Opts) ->
    Slot = hb_util:int(hb_ao:get(<<"slot">>, RawReq, Opts)),
    ?event(compute, {next_slot, Slot}),
    % If the input message does not have a path, set it to `compute'.
    Req =
        case hb_path:from_message(request, RawReq, Opts) of
            undefined -> RawReq#{ <<"path">> => <<"compute">> };
            _ -> RawReq
        end,
    ?event(compute, {input_msg, Req}),
    ?event(compute, {executing, {proc_id, ProcID}, {slot, Slot}}, Opts),
    % Unset the previous results.
    PreparedState = hb_ao:set(State, #{ <<"results">> => unset }, Opts),
    {ok, Slot, PreparedState, Req}.

%% @doc Fire a `~push@1.0/push' for the slot we just computed, iff the
%% originating request carries a truthy `push' key. The push is invoked
%% from a freshly-spawned process so a slow downstream chain cannot stall
%% the compute path that produced this slot.
%%
%% `push' values:
%%   `true' / `<<"true">>'  - push, no `max-depth' set (unbounded recursion).
%%   non-negative integer N - push with `max-depth = N', so the fan-out
%%                            unwinds at most N levels deep. See `dev_push'
%%                            for `max-depth = 0' semantics: each outbox
%%                            entry is still scheduled on its target, but
%%                            the recursive `/push' is skipped.
%%   anything else (or absent) - silent no-op.
maybe_trigger_push(Process, Slot, Req, Opts) ->
    case hb_maps:get(<<"push">>, Req, undefined, Opts) of
        true        -> dispatch_push(Process, Slot, undefined, Req, Opts);
        <<"true">>  -> dispatch_push(Process, Slot, undefined, Req, Opts);
        N when is_integer(N), N >= 0 ->
            dispatch_push(Process, Slot, N, Req, Opts);
        Bin when is_binary(Bin) ->
            try hb_util:int(Bin) of
                N when is_integer(N), N >= 0 ->
                    dispatch_push(Process, Slot, N, Req, Opts);
                _ -> ok
            catch _:_ -> ok
            end;
        _ -> ok
    end.

%% @doc Build the inner `~push@1.0/push' request for `Slot' and invoke it from
%% a freshly-spawned process. Inherits the
%% originating request's payload keys (e.g. `result-depth', `async')
%% so the caller's preference flows through, replaces `path'/`slot',
%% and -- when bounded -- sets `max-depth'. The default sync mode
%% propagates back-pressure to the compute path: a slow downstream
%% chain throttles further hook fires rather than queueing unbounded
%% spawns under load.
dispatch_push(Process, Slot, MaxDepth, Req, Opts) ->
    BaseReq =
        (hb_maps:without([<<"push">>, <<"path">>, <<"slot">>], Req, Opts))#{
            <<"path">> => <<"push">>,
            <<"slot">> => Slot
        },
    PushReq =
        case MaxDepth of
            undefined -> BaseReq;
            N -> BaseReq#{ <<"max-depth">> => N }
        end,
    % Extract the canonical process spec from the live state so push ID
    % computation lands on the same cache key that `store_result' just
    %% wrote under -- passing the live state directly hashes to a different
    %% key and sends the downstream read into a re-compute loop.
    Spec = hb_maps:get(<<"process">>, Process, Process, Opts),
    ?event(push,
        {triggered_by_compute,
            {slot, Slot},
            {max_depth, MaxDepth}
        },
        Opts
    ),
    spawn(fun() -> hb_ao:raw(<<"push@1.0">>, Spec, PushReq, Opts) end),
    ok.

%% @doc Store the resulting state in the cache, potentially with the snapshot
%% key. The write is synchronous: callers may notify waiters or run push hooks
%% as soon as this returns, so the slot must already be cache-visible.
%%
%% With `process-async-checkpoints' (default false), a delta (`lua@5.3b')
%% process stores every slot as a delta and hands its checkpoints to a
%% background writer instead: see `store_result_async/5'.
store_result(false, ProcID, Slot, Res, Req, Opts) ->
    case async_checkpoints(Res, Opts) of
        true -> store_result_async(ProcID, Slot, Res, Req, Opts);
        false -> store_result_sync(false, ProcID, Slot, Res, Req, Opts)
    end;
store_result(ForceSnapshot, ProcID, Slot, Res, Req, Opts) ->
    store_result_sync(ForceSnapshot, ProcID, Slot, Res, Req, Opts).

store_result_sync(ForceSnapshot, ProcID, Slot, Res, Req, Opts) ->
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
                {ok, Snapshot} = snapshot(Res, Req, Opts),
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
                        <<"last-snapshot">>,
                        os:system_time(second),
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
    dev_process_cache:write(ProcID, Slot, ResMaybeWithSnapshot, Opts),
    ?event(compute, {caching_completed, {proc_id, ProcID}, {slot, Slot}}, Opts),
    hb_maps:without([<<"snapshot">>], ResMaybeWithSnapshot, Opts).

%% @doc Asynchronous checkpoints: the compute path never waits for one.
%%
%% A checkpoint of a large process (the VM snapshot: `luerl:externalize',
%% `term_to_binary' and compression; then the full public state and snapshot
%% written to the store) took seconds of the slot it fell on (measured on prod:
%% `store_ms' 5.8-11.4 s on the game authority), and every compute and push
%% behind it queued. Here the slot is stored as an ordinary delta, so the delta
%% chain stays gapless and the slot is readable before waiters are notified,
%% and the state of exactly that slot -- an immutable term, which the next slots
%% never mutate -- is copied to a writer process that builds the snapshot and
%% publishes the checkpoint with `dev_process_cache:write_checkpoint/5' (data
%% durable first, the slot alias last). Until it lands, readers, cold restores
%% and retention see the previous checkpoint and the deltas after it.
%%
%% At most one writer per process is in flight. A checkpoint that falls due
%% while one is still writing is not queued: the state is marked overdue and
%% the next slot after the writer finishes is checkpointed instead (a
%% checkpoint is found by its content, never by its slot number). The only
%% cost left on the compute path is copying the state to the writer.
%%
%% Only delta processes take this path; a full-state process (`lua@5.3a',
%% WASM) keeps the synchronous checkpoint, whose slot is a full state anyway.
async_checkpoints(Res, Opts) ->
    is_map(maps:get(<<"process-cache-delta">>, hb_private:from_message(Res), undefined))
        andalso hb_util:atom(
            hb_opts:get(<<"process-async-checkpoints">>, false, Opts)
        ) == true.

store_result_async(ProcID, Slot, Res, Req, Opts) ->
    Due = checkpoint_overdue(Res) orelse should_snapshot(Slot, Res, Opts),
    dev_process_cache:write(
        ProcID,
        Slot,
        Res,
        Opts#{ <<"process-cache-defer-checkpoint">> => true }
    ),
    Next =
        case Due of
            false -> Res;
            true ->
                case start_checkpoint_writer(ProcID, Slot, Res, Req, Opts) of
                    started -> set_checkpoint_overdue(Res, false);
                    busy ->
                        ?event(compute_short,
                            {checkpoint_deferred,
                                {proc_id, ProcID},
                                {slot, Slot},
                                {reason, writer_busy}
                            }
                        ),
                        set_checkpoint_overdue(Res, true)
                end
        end,
    hb_maps:without([<<"snapshot">>], Next, Opts).

checkpoint_overdue(Res) ->
    maps:get(<<"checkpoint-overdue">>, hb_private:from_message(Res), false)
        =:= true.

set_checkpoint_overdue(Res, Overdue) ->
    Priv = hb_private:from_message(Res),
    case {Overdue, maps:is_key(<<"checkpoint-overdue">>, Priv)} of
        {false, false} -> Res;
        {false, true} ->
            hb_private:set_priv(Res, maps:remove(<<"checkpoint-overdue">>, Priv));
        {true, _} ->
            hb_private:set_priv(Res, Priv#{ <<"checkpoint-overdue">> => true })
    end.

%% @doc Start the checkpoint writer for `Slot', unless one is already in flight
%% for the process. The writer is registered under the process's name before
%% it does anything, so the check and the claim are atomic (`hb_name' inserts
%% with `insert_new'); a writer that dies is dropped from the registry by the
%% next lookup. Spawning copies `Res' (the VM included) into the writer: that
%% copy is all the compute path pays. The writer is not linked to the caller:
%% a process worker that stops lets it finish, and a node that stops abandons
%% it, which is safe, as nothing is visible until its final link.
start_checkpoint_writer(ProcID, Slot, Res, Req, Opts) ->
    Name = checkpoint_writer_name(ProcID, Opts),
    case hb_name:lookup(Name) of
        Pid when is_pid(Pid) -> busy;
        undefined ->
            % Low priority: the writer's compression must not take
            % schedulers from the compute and push paths it is moved off.
            Writer =
                spawn_opt(
                    fun() ->
                        receive {go, Name} -> ok end,
                        write_checkpoint(Name, ProcID, Slot, Res, Req, Opts)
                    end,
                    [{priority, low}]
                ),
            case hb_name:register(Name, Writer) of
                ok ->
                    Writer ! {go, Name},
                    started;
                error ->
                    exit(Writer, kill),
                    busy
            end
    end.

checkpoint_writer_name(ProcID, Opts) ->
    {
        ?MODULE,
        checkpoint_writer,
        ProcID,
        hb_opts:get(<<"process-cache-scope">>, local, Opts)
    }.

%% @doc The body of a checkpoint writer: snapshot, write, publish. Failure is
%% logged and leaves no checkpoint, which costs a longer replay on the next
%% cold restore, never correctness.
write_checkpoint(Name, ProcID, Slot, Res, Req, Opts) ->
    Hook =
        case hb_opts:get(<<"process-async-checkpoint-hook">>, undefined, Opts) of
            Fun when is_function(Fun, 2) -> Fun;
            _ -> fun(_, _) -> ok end
        end,
    Start = erlang:monotonic_time(microsecond),
    try
        Hook(started, Slot),
        {ok, Snapshot} = snapshot(Res, Req, Opts),
        Snapped = erlang:monotonic_time(microsecond),
        Hook(snapshot_taken, Slot),
        {ok, _} =
            dev_process_cache:write_checkpoint(
                ProcID,
                Slot,
                hb_ao:set(Res, <<"snapshot">>, Snapshot, Opts),
                fun() -> Hook(data_written, Slot) end,
                Opts
            ),
        Done = erlang:monotonic_time(microsecond),
        Hook(linked, Slot),
        ?event(compute_short,
            {checkpoint_written,
                {proc_id, ProcID},
                {slot, Slot},
                {snapshot_ms, (Snapped - Start) div 1000},
                {write_ms, (Done - Snapped) div 1000}
            }
        )
    catch
        Class:Reason:Stack ->
            ?event(error,
                {checkpoint_write_failed,
                    {proc_id, ProcID},
                    {slot, Slot},
                    {class, Class},
                    {reason, Reason},
                    {stacktrace, {trace, Stack}}
                }
            )
    after
        hb_name:unregister(Name)
    end.

%% @doc Should we snapshot a new full state result? First, we check if the 
%% `process_snapshot_time' option is set. If it is, we check if the elapsed time
%% since the last snapshot is greater than the value. We also check the
%% `process_snapshot_slots' option. If it is set, we check if the slot is
%% a multiple of the interval. If either are true, we must snapshot.
should_snapshot(Slot, Res, Opts) ->
    case hb_private:get(
        <<"process-cache-delta">>,
        Res,
        not_found,
        Opts#{ <<"hashpath">> => ignore }
    ) of
        Delta when is_map(Delta) ->
            should_snapshot_delta_slots(Slot, Opts);
        _ ->
            should_snapshot_slots(Slot, Opts) orelse
                should_snapshot_time(Res, Opts)
    end.

%% @doc `lua@5.3b' public checkpoints and VM snapshots use one cadence. The
%% production `lua@5.3a' cadence remains untouched.
should_snapshot_delta_slots(Slot, Opts) ->
    RawInterval = hb_opts:get(
        <<"process-delta-checkpoint-slots">>,
        1000,
        Opts
    ),
    case hb_util:int(RawInterval) of
        Interval when Interval > 0 -> Slot rem Interval == 0;
        _ -> erlang:error({invalid_process_delta_checkpoint_slots, RawInterval})
    end.

%% @doc Calculate if we should snapshot based on the number of slots.
should_snapshot_slots(Slot, Opts) ->
    case hb_opts:get(<<"process-snapshot-slots">>, ?DEFAULT_SNAPSHOT_SLOTS, Opts) of
        Undef when (Undef == undefined) or (Undef == <<"false">>) ->
            false;
        RawSnapshotSlots ->
            SnapshotSlots = hb_util:int(RawSnapshotSlots),
            Slot rem SnapshotSlots == 0
    end.

%% @doc Calculate if we should snapshot based on the elapsed time since the last
%% snapshot.
should_snapshot_time(Res, Opts) ->
    case hb_opts:get(<<"process-snapshot-time">>, ?DEFAULT_SNAPSHOT_TIME, Opts) of
        Undef when (Undef == undefined) or (Undef == <<"false">>) ->
            false;
        RawSecs ->
            Secs = hb_util:int(RawSecs),
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
now(RawBase, Req, Opts) ->
    Base = lib_process:ensure_process_key(RawBase, Opts),
    ProcessID = lib_process:process_id(Base, #{}, Opts),
    case hb_opts:get(process_now_from_cache, false, Opts) of
        false ->
            {ok, CurrentSlot} =
                hb_ao:resolve(
                    Base,
                    #{ <<"path">> => <<"slot/current">> },
                    Opts
                ),
            ?event({now_called, {process, ProcessID}, {slot, CurrentSlot}}),
            hb_ao:resolve(
                Base,
                (hb_maps:with([<<"push">>], Req, Opts))#{
                    <<"path">> => <<"compute">>,
                    <<"slot">> => CurrentSlot
                },
                Opts
            );
        CacheParam ->
            % We are serving the latest known state from the cache, rather
            % than computing it.
            LatestKnown = dev_process_cache:latest(ProcessID, [], Opts),
            case LatestKnown of
                {ok, LatestSlot, RawLatestMsg} ->
                    % Marked as a cache hit: it is public state only (never a
                    % base to compute from), and the store already holds it,
                    % so result caching must not write it back on every read.
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
                _ ->
                    if CacheParam =/= always ->
                        % The node is configured to use the cache if possible,
                        % but forcing computation is also admissible. Subsequently,
                        % as no other option is available, we compute the state.
                        now(Base, Req, Opts#{ <<"process-now-from-cache">> => false });
                    true ->
                        % The node is configured to only serve the latest known
                        % state from the cache, so we return the latest slot.
                        {failure, <<"No cached state available.">>}
                    end
            end
    end.

%% @doc Recursively push messages to the scheduler until we find a message
%% that does not lead to any further messages being scheduled.
push(Base, Req, Opts) ->
    lib_process:run_as(
        <<"push">>,
        lib_process:ensure_process_key(Base, Opts),
        Req,
        Opts
    ).

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
    % Get the nonce we are currently on and the inbound nonce.
    TargetSlot = hb_ao:get(<<"slot">>, Req, undefined, Opts),
    ProcID = lib_process:process_id(Base, #{}, Opts),
    ?event({ensure_loaded, {base, Base}, {req, Req}}),
    case hb_ao:get(<<"initialized">>, Base, Opts) of
        <<"true">> ->
            ?event(already_initialized),
            {ok, Base};
        _ ->
            ?event(not_initialized),
            % Try to load the latest complete state from disk.
            LoadRes =
                dev_process_cache:latest(
                    ProcID,
                    [<<"snapshot+link">>],
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
                    Process = 
                        hb_maps:get(
                            <<"process">>,
                            SnapshotMsg,
                            undefined,
                            Opts
                        ),
                    #{ <<"commitments">> := HmacCommits} =
                        hb_message:with_commitments(
                            #{ <<"type">> => <<"hmac-sha256">>},
                            Process,
                            Opts
                        ),
                    #{ <<"commitments">> := SignCommits } =
                        hb_message:with_commitments(ProcID, Process, Opts),
                    UpdateProcess =
                        hb_maps:put(
                            <<"commitments">>,
                            hb_maps:merge(HmacCommits, SignCommits),
                            Process,
                            Opts
                        ),
                    SnapshotReq =
                        SnapshotMsg#{
                            <<"process">> => UpdateProcess,
                            <<"initialized">> => <<"true">>
                        },
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
                    {ok, NormalizedWithoutSnapshot};
                {error, not_found} ->
                    % If we do not have a checkpoint, initialize the
                    % process from scratch.
                    ?event(
                        {no_checkpoint_found,
                            {process, ProcID},
                            {slot, TargetSlot}
                        }
                    ),
                    init(Base, Req, Opts)
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

%% @doc A 5.3b process uses the delta checkpoint cadence rather than the
%% production 5.3a slot/time cadence.
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

delta_snapshot_cadence_test() ->
    Opts = #{
        <<"process-snapshot-slots">> => 50,
        <<"process-snapshot-time">> => 1,
        <<"process-delta-checkpoint-slots">> => 1000
    },
    DeltaState = hb_private:set(
        #{},
        <<"process-cache-delta">>,
        #{ <<"patches">> => [], <<"results">> => #{} },
        Opts
    ),
    ?assert(should_snapshot(0, DeltaState, Opts)),
    ?assertNot(should_snapshot(50, DeltaState, Opts)),
    ?assertNot(should_snapshot(999, DeltaState, Opts)),
    ?assert(should_snapshot(1000, DeltaState, Opts)).

%% @doc A cache miss must route to re-execution, and a real failure must not.
%% `dev_process_cache:read/3' returns a bare `not_found' (it passes
%% `hb_cache:read/2''s result through untouched), and a delta whose base slot is
%% gone returns `{error, {invalid_process_delta_chain, _, _}}'. Before this,
%% `compute/3' matched only `{error, not_found}', so both crashed the request
%% with `case_clause' instead of rebuilding the slot from the ledger.
recoverable_miss_accepts_every_cache_miss_shape_test() ->
    ?assert(recoverable_miss(not_found)),
    ?assert(recoverable_miss({error, not_found})),
    ?assert(recoverable_miss({error, {invalid_process_delta_chain, 41, 43}})).

%% @doc A store failure answered with a full replay would hide the fault and
%% cost a replay per request, so anything unrecognised is passed back.
recoverable_miss_rejects_real_failures_test() ->
    ?assertNot(recoverable_miss({error, no_viable_store})),
    ?assertNot(recoverable_miss({error, timeout})),
    ?assertNot(recoverable_miss({ok, #{}})),
    ?assertNot(recoverable_miss({error, {some_other_reason, 1}})).

%% @doc When there is no snapshot at or below the target, `rewind/4' must report
%% `not_found' so the caller preserves the original `already_calculated_slot'
%% error. It must not crash, and it must not fabricate a state: returning one
%% above the target would re-enter the same clause and recurse forever.
rewind_without_snapshot_reports_not_found_test() ->
    Opts = #{ <<"store">> => [] },
    ?assertEqual(not_found, rewind(#{}, #{}, 100, Opts)),
    ?assertEqual(
        not_found,
        rewind(#{ <<"process">> => #{} }, #{ <<"slot">> => 100 }, 100, Opts)
    ).

%% @doc A historical read further from its checkpoint than the replay limit is
%% refused before any work is done: with no store there is no checkpoint, so
%% slot 7 is 8 slots from the initialized state.
historical_read_is_bounded_test() ->
    Opts = #{ <<"store">> => [], <<"process-historical-replay-limit">> => 2 },
    ?assertMatch(
        {error, #{ <<"status">> := 503, <<"checkpoint">> := -1 }},
        compute_historical(<<"pid">>, #{ <<"at-slot">> => 11 }, #{}, 7, Opts)
    ),
    ?assert(within_replay_limit(8, infinity)),
    ?assert(within_replay_limit(8, <<"8">>)),
    ?assertNot(within_replay_limit(9, 8)).

%%% Asynchronous checkpoints (`process-async-checkpoints').

ckpt_opts(Extra) ->
    hb:init(),
    application:ensure_all_started(hb),
    Store = hb_test_utils:test_store(hb_store_lmdb),
    ok = hb_store:start([Store], #{}, #{}),
    maps:merge(
        #{
            <<"store">> => [Store],
            <<"priv-wallet">> => ar_wallet:new(),
            <<"hashpath">> => ignore,
            <<"spawn-worker">> => false,
            <<"process-workers">> => false,
            <<"match-index">> => false,
            <<"process-delta-checkpoint-slots">> => 4,
            <<"process-async-checkpoints">> => true
        },
        Extra
    ).

%% A counter, and a Lua table that grows every slot and is summed every slot:
%% a VM that did not continue exactly from the right state reports another sum.
ckpt_script() ->
    <<
        "Count = Count or 0\n"
        "Big = Big or {}\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  Big[Count] = { n = Count, s = string.rep('x', Count % 5) }\n"
        "  local sum = 0\n"
        "  for i = 1, #Big do sum = sum + Big[i].n end\n"
        "  return {\n"
        "    patches = {\n"
        "      { path = '/count', value = tostring(Count) },\n"
        "      { path = '/sum', value = tostring(sum) }\n"
        "    },\n"
        "    results = { output = { data = tostring(Count) } }\n"
        "  }\n"
        "end\n"
    >>.

ckpt_process(Script, Opts) ->
    Wallet = hb_opts:get(<<"priv-wallet">>, hb:wallet(), Opts),
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    Process =
        hb_message:commit(
            #{
                <<"device">> => <<"process@1.0">>,
                <<"type">> => <<"Process">>,
                <<"scheduler-device">> => <<"scheduler@1.0">>,
                <<"execution-device">> => <<"lua@5.3b">>,
                <<"module">> => #{
                    <<"content-type">> => <<"application/lua">>,
                    <<"body">> => Script
                },
                <<"authority">> => [Address],
                <<"scheduler-location">> => Address,
                <<"test-random-seed">> => rand:uniform(1000000)
            },
            Opts
        ),
    {ok, _} = hb_cache:write(Process, Opts),
    Process.

ckpt_schedule(Process, Slots, Opts) ->
    ProcID = hb_message:id(Process, all, Opts),
    lists:foreach(
        fun(N) ->
            Req =
                hb_message:commit(
                    #{
                        <<"path">> => <<"schedule">>,
                        <<"method">> => <<"POST">>,
                        <<"body">> =>
                            hb_message:commit(
                                #{
                                    <<"target">> => ProcID,
                                    <<"type">> => <<"Message">>,
                                    <<"action">> => <<"Increment">>,
                                    <<"number">> => N
                                },
                                Opts
                            )
                    },
                    Opts
                ),
            {ok, _} = hb_ao:resolve(Process, Req, Opts)
        end,
        Slots
    ).

%% The live state a process worker would hold before its first slot.
ckpt_live(Process, Opts) ->
    Base = lib_process:ensure_process_key(Process, Opts),
    ProcID = lib_process:process_id(Base, #{}, Opts),
    {ok, Loaded} = ensure_loaded(Base, #{ <<"slot">> => 0 }, Opts),
    {ProcID, Loaded}.

%% Compute `Slots' one at a time from a live state, as the worker does.
ckpt_run(ProcID, State, Slots, Opts) ->
    lists:foldl(
        fun(Slot, S) ->
            {ok, Next} =
                compute_to_slot(
                    ProcID,
                    S,
                    #{ <<"path">> => <<"compute">>, <<"slot">> => Slot },
                    Slot,
                    Opts
                ),
            Next
        end,
        State,
        Slots
    ).

%% Restore `Slot' cold: newest visible checkpoint at or below it, then replay.
ckpt_cold(Process, Slot, Opts) ->
    Base = lib_process:ensure_process_key(Process, Opts),
    ProcID = lib_process:process_id(Base, #{}, Opts),
    Req = #{ <<"path">> => <<"compute">>, <<"slot">> => Slot },
    {ok, Restored} = rewind(Base, Req, Slot, Opts),
    From = loaded_slot(Restored, Opts),
    {ok, State} = compute_to_slot(ProcID, Restored, Req, Slot, Opts),
    {From, State}.

%% A hook that reports every writer phase to `Test' and, where `Block' says
%% so, waits for `go' or `crash' (a kill: no `after', as a node crash).
%% Messages carry the test's `Ref': eunit may run several tests in one
%% process, and a writer of an earlier test may still be reporting.
ckpt_hook(Test, Ref, Block) ->
    fun(Phase, Slot) ->
        Test ! {ckpt, Ref, Phase, Slot, self()},
        case Block(Phase, Slot) of
            false -> ok;
            true ->
                receive
                    {ckpt_cmd, Slot, go} -> ok;
                    {ckpt_cmd, Slot, crash} -> exit(self(), kill)
                end
        end
    end.

ckpt_wait(Ref, Phase, Slot) ->
    receive {ckpt, Ref, Phase, Slot, Writer} -> Writer
    after 60000 -> erlang:error({checkpoint_phase_not_reached, Phase, Slot})
    end.

ckpt_vm(State) ->
    term_to_binary(
        luerl:externalize(maps:get(<<"state">>, hb_private:from_message(State)))
    ).

ckpt_public(State, Opts) ->
    {
        hb_ao:get(<<"at-slot">>, State, Opts),
        hb_ao:get(<<"count">>, State, Opts),
        hb_ao:get(<<"sum">>, State, Opts),
        hb_ao:get(<<"results/output/data">>, State, Opts)
    }.

ckpt_writer_idle(ProcID, Opts) ->
    hb_util:wait_until(
        fun() -> hb_name:lookup(checkpoint_writer_name(ProcID, Opts)) == undefined end,
        30000
    ).

%% @doc A checkpoint never blocks the slots after it: slots 5-7 compute while
%% slot 4's writer is held before it starts, nothing is visible until it is
%% done, and a cold restore from the async checkpoint equals the live state,
%% VM included.
async_checkpoint_does_not_block_compute_test_() ->
    {timeout, 180, fun() ->
        Test = self(),
        Ref = make_ref(),
        Opts =
            ckpt_opts(#{
                <<"process-async-checkpoint-hook">> =>
                    ckpt_hook(Test, Ref, fun(P, S) -> P == started andalso S == 4 end)
            }),
        Process = ckpt_process(ckpt_script(), Opts),
        ckpt_schedule(Process, lists:seq(0, 9), Opts),
        {ProcID, L0} = ckpt_live(Process, Opts),
        L3 = ckpt_run(ProcID, L0, lists:seq(0, 3), Opts),
        _ = ckpt_wait(Ref, linked, 0),
        L4 = ckpt_run(ProcID, L3, [4], Opts),
        Writer = ckpt_wait(Ref, started, 4),
        L7 = ckpt_run(ProcID, L4, [5, 6, 7], Opts),
        % Three slots computed while the writer is still held.
        ?assert(is_process_alive(Writer)),
        ?assertEqual(0, checkpoint_slot(ProcID, 7, Opts)),
        % Slot 4 is readable, as a delta, the whole time.
        ?assertEqual(
            {4, <<"5">>, <<"15">>, <<"5">>},
            ckpt_public(hb_util:ok(dev_process_cache:read(ProcID, 4, Opts)), Opts)
        ),
        Writer ! {ckpt_cmd, 4, go},
        _ = ckpt_wait(Ref, linked, 4),
        ?assertEqual(4, checkpoint_slot(ProcID, 7, Opts)),
        ?assertEqual({7, <<"8">>, <<"36">>, <<"8">>}, ckpt_public(L7, Opts)),
        dev_process_cache_clear(ProcID, Opts),
        {From, Cold7} = ckpt_cold(Process, 7, Opts),
        ?assertEqual(4, From),
        ?assertEqual(ckpt_public(L7, Opts), ckpt_public(Cold7, Opts)),
        ?assertEqual(ckpt_vm(L7), ckpt_vm(Cold7))
    end}.

%% @doc The same slots give the same states with checkpoints sync and async.
async_checkpoint_is_deterministic_test_() ->
    {timeout, 180, fun() ->
        Run =
            fun(Async) ->
                Opts = ckpt_opts(#{ <<"process-async-checkpoints">> => Async }),
                Process = ckpt_process(ckpt_script(), Opts),
                ckpt_schedule(Process, lists:seq(0, 9), Opts),
                {ProcID, L0} = ckpt_live(Process, Opts),
                L9 = ckpt_run(ProcID, L0, lists:seq(0, 9), Opts),
                ckpt_writer_idle(ProcID, Opts),
                {_, Cold9} = ckpt_cold(Process, 9, Opts),
                {ckpt_public(L9, Opts), ckpt_public(Cold9, Opts),
                    checkpoint_slot(ProcID, 9, Opts)}
            end,
        {Live, Cold, Ckpt} = Run(true),
        ?assertEqual(Live, Cold),
        ?assertEqual(8, Ckpt),
        ?assertEqual({Live, Cold, Ckpt}, Run(false))
    end}.

%% @doc A writer killed after its data is durable but before its link leaves
%% no visible checkpoint: the slot still reads as its delta, a cold restore
%% resumes from the previous checkpoint and replays to the live state, and the
%% next checkpoint is written normally.
async_checkpoint_writer_crash_leaves_previous_test_() ->
    {timeout, 180, fun() ->
        Test = self(),
        Ref = make_ref(),
        Opts =
            ckpt_opts(#{
                <<"process-async-checkpoint-hook">> =>
                    ckpt_hook(Test, Ref, fun(P, S) -> P == data_written andalso S == 4 end)
            }),
        Process = ckpt_process(ckpt_script(), Opts),
        ckpt_schedule(Process, lists:seq(0, 9), Opts),
        {ProcID, L0} = ckpt_live(Process, Opts),
        L4 = ckpt_run(ProcID, L0, lists:seq(0, 4), Opts),
        Writer = ckpt_wait(Ref, data_written, 4),
        Mon = erlang:monitor(process, Writer),
        Writer ! {ckpt_cmd, 4, crash},
        receive {'DOWN', Mon, process, Writer, killed} -> ok
        after 30000 -> erlang:error(writer_not_killed)
        end,
        L6 = ckpt_run(ProcID, L4, [5, 6], Opts),
        ?assertEqual(0, checkpoint_slot(ProcID, 6, Opts)),
        {ok, Raw4} =
            hb_cache:read(dev_process_cache_path(ProcID, 4), Opts),
        ?assertEqual(<<"process-delta@1.0">>, hb_ao:get(<<"cache-format">>, Raw4, Opts)),
        dev_process_cache_clear(ProcID, Opts),
        % The cold replay passes slot 4 again, which is due: let that writer go.
        Self = self(),
        Releaser =
            spawn(fun() ->
                receive {ckpt, Ref, data_written, 4, W} -> W ! {ckpt_cmd, 4, go} end,
                Self ! released
            end),
        {From, Cold6} =
            ckpt_cold(Process, 6, Opts#{
                <<"process-async-checkpoint-hook">> =>
                    ckpt_hook(Releaser, Ref, fun(P, S) -> P == data_written andalso S == 4 end)
            }),
        ?assertEqual(0, From),
        ?assertEqual(ckpt_public(L6, Opts), ckpt_public(Cold6, Opts)),
        ?assertEqual(ckpt_vm(L6), ckpt_vm(Cold6)),
        receive released -> ok after 30000 -> erlang:error(replay_writer_missing) end,
        ckpt_writer_idle(ProcID, Opts),
        % The registry is free again: the next checkpoint is written.
        _ = ckpt_run(ProcID, L6, [7, 8], Opts),
        _ = ckpt_wait(Ref, linked, 8),
        ?assertEqual(8, checkpoint_slot(ProcID, 9, Opts))
    end}.

%% @doc Retention running while a checkpoint is in flight (its data durable,
%% its link not yet written) deletes nothing a restore needs: the previous
%% checkpoint and the deltas after it stay, and the in-flight checkpoint is
%% whole once it lands.
async_checkpoint_survives_retention_in_flight_test_() ->
    {timeout, 240, fun() ->
        Test = self(),
        Ref = make_ref(),
        Opts =
            ckpt_opts(#{
                <<"process-async-checkpoint-hook">> =>
                    ckpt_hook(Test, Ref, fun(P, S) -> P == data_written andalso S == 8 end),
                <<"process-hot-cache-slots">> => 1,
                <<"store-retention">> => true,
                <<"store-retention-recent-slots">> => 1,
                <<"store-retention-checkpoints">> => 1,
                <<"store-retention-grace-ms">> => 0,
                <<"store-retention-max-deletes-per-sec">> => 0,
                <<"store-retention-scan-rows">> => 5000
            }),
        Process = ckpt_process(ckpt_script(), Opts),
        ckpt_schedule(Process, lists:seq(0, 11), Opts),
        {ProcID, L0} = ckpt_live(Process, Opts),
        L9 = ckpt_run(ProcID, L0, lists:seq(0, 9), Opts),
        _ = ckpt_wait(Ref, linked, 4),
        Writer = ckpt_wait(Ref, data_written, 8),
        Report = hb_store_gc:retain(Opts),
        ?assert(maps:get(dropped_slots, Report) > 0),
        ?assert(is_process_alive(Writer)),
        ?assertEqual(4, checkpoint_slot(ProcID, 9, Opts)),
        dev_process_cache_clear(ProcID, Opts),
        {From4, Cold9} = ckpt_cold(Process, 9, Opts#{
            % This replay must not start a second writer for slot 8.
            <<"process-async-checkpoint-hook">> => fun(_, _) -> ok end
        }),
        ?assertEqual(4, From4),
        ?assertEqual(ckpt_public(L9, Opts), ckpt_public(Cold9, Opts)),
        Writer ! {ckpt_cmd, 8, go},
        _ = ckpt_wait(Ref, linked, 8),
        ?assertEqual(8, checkpoint_slot(ProcID, 9, Opts)),
        dev_process_cache_clear(ProcID, Opts),
        {From8, Cold9b} = ckpt_cold(Process, 9, Opts),
        ?assertEqual(8, From8),
        ?assertEqual(ckpt_public(L9, Opts), ckpt_public(Cold9b, Opts)),
        ?assertEqual(ckpt_vm(L9), ckpt_vm(Cold9b))
    end}.

%% @doc One writer per process: a checkpoint that falls due while one is in
%% flight is not queued but coalesced into the first slot after it finishes.
async_checkpoint_coalesces_when_writer_busy_test_() ->
    {timeout, 180, fun() ->
        Test = self(),
        Ref = make_ref(),
        Opts =
            ckpt_opts(#{
                <<"process-async-checkpoint-hook">> =>
                    ckpt_hook(Test, Ref, fun(P, S) -> P == started andalso S == 4 end)
            }),
        Process = ckpt_process(ckpt_script(), Opts),
        ckpt_schedule(Process, lists:seq(0, 12), Opts),
        {ProcID, L0} = ckpt_live(Process, Opts),
        L4 = ckpt_run(ProcID, L0, lists:seq(0, 4), Opts),
        Writer = ckpt_wait(Ref, started, 4),
        L9 = ckpt_run(ProcID, L4, lists:seq(5, 9), Opts),
        % Slot 8 fell due while slot 4 was writing: no second writer.
        receive {ckpt, Ref, started, Other, _} when Other > 4 ->
            erlang:error({second_writer, Other})
        after 0 -> ok
        end,
        ?assert(checkpoint_overdue(L9)),
        Writer ! {ckpt_cmd, 4, go},
        _ = ckpt_wait(Ref, linked, 4),
        ckpt_writer_idle(ProcID, Opts),
        L10 = ckpt_run(ProcID, L9, [10], Opts),
        _ = ckpt_wait(Ref, linked, 10),
        ?assertNot(checkpoint_overdue(L10)),
        _ = ckpt_run(ProcID, L10, [11], Opts),
        ckpt_writer_idle(ProcID, Opts),
        receive {ckpt, Ref, started, 11, _} -> erlang:error(not_coalesced)
        after 0 -> ok
        end,
        ?assertEqual(10, checkpoint_slot(ProcID, 11, Opts)),
        ?assertEqual(4, checkpoint_slot(ProcID, 9, Opts)),
        dev_process_cache_clear(ProcID, Opts),
        {From, Cold11} = ckpt_cold(Process, 11, Opts),
        ?assertEqual(10, From),
        ?assertEqual({11, <<"12">>, <<"78">>, <<"12">>}, ckpt_public(Cold11, Opts))
    end}.

%% @doc A process worker that stops while its checkpoint is in flight does not
%% take the writer with it: the checkpoint is completed, and is restorable.
async_checkpoint_outlives_worker_stop_test_() ->
    {timeout, 180, fun() ->
        Test = self(),
        Ref = make_ref(),
        Opts =
            ckpt_opts(#{
                <<"spawn-worker">> => true,
                <<"process-workers">> => true,
                <<"await-inprogress">> => named,
                <<"process-async-checkpoint-hook">> =>
                    ckpt_hook(Test, Ref, fun(P, S) -> P == started andalso S == 4 end)
            }),
        Process = ckpt_process(ckpt_script(), Opts),
        ckpt_schedule(Process, lists:seq(0, 6), Opts),
        {ok, State5} =
            hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => 5 }, Opts),
        ?assertEqual(<<"6">>, hb_ao:get(<<"count">>, State5, Opts)),
        Writer = ckpt_wait(Ref, started, 4),
        Group = hb_util:human_id(hb_message:id(Process, all, Opts)),
        Worker = hb_name:lookup(Group),
        ?assert(is_pid(Worker)),
        Mon = erlang:monitor(process, Worker),
        Worker ! stop,
        receive {'DOWN', Mon, process, Worker, _} -> ok
        after 30000 -> erlang:error(worker_did_not_stop)
        end,
        ?assert(is_process_alive(Writer)),
        Writer ! {ckpt_cmd, 4, go},
        _ = ckpt_wait(Ref, linked, 4),
        dev_process_cache_clear(Group, Opts),
        {From, Cold6} = ckpt_cold(Process, 6, Opts#{ <<"spawn-worker">> => false }),
        ?assertEqual(4, From),
        ?assertEqual({6, <<"7">>, <<"28">>, <<"7">>}, ckpt_public(Cold6, Opts))
    end}.

dev_process_cache_path(ProcID, Slot) ->
    <<"computed/", (hb_util:human_id(ProcID))/binary, "/slot/",
        (integer_to_binary(Slot))/binary>>.

%% Drop a process's in-memory cache entries, so reads come from the store.
dev_process_cache_clear(_ProcID, _Opts) ->
    [ catch ets:delete_all_objects(T)
    || T <- [dev_process_delta_hot_cache, dev_process_delta_recent_cache,
             dev_process_delta_replay_cache] ],
    ok.

%% @doc Manual checkpoint benchmark and profile, on a large Lua state. Example:
%% `HB_ASYNC_CKPT_BENCH=200000:3000:600:200 rebar3 device test --module dev_process'
%% (Lua table entries : public-state keys : slots : checkpoint cadence).
async_checkpoint_benchmark_report_test_() ->
    {timeout, 3600, fun() ->
        case os:getenv("HB_ASYNC_CKPT_BENCH") of
            false -> ok;
            Spec ->
                [BigN, PubN, Slots, Interval] =
                    [ list_to_integer(X) || X <- string:tokens(Spec, ":") ],
                Sync = ckpt_bench(false, BigN, PubN, Slots, Interval),
                io:format(user, "ASYNC_CKPT_BENCH sync ~p~n", [Sync]),
                Async = ckpt_bench(true, BigN, PubN, Slots, Interval),
                io:format(user, "ASYNC_CKPT_BENCH async ~p~n", [Async])
        end
    end}.

ckpt_bench_script(BigN, PubN) ->
    iolist_to_binary([
        "Count = Count or 0\n"
        "BIG_N = ", integer_to_list(BigN), "\n"
        "PUB_N = ", integer_to_list(PubN), "\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  local patches = {}\n"
        "  if not Big then\n"
        "    Big = {}\n"
        "    for i = 1, BIG_N do\n"
        "      Big[i] = { id = i, name = 'player-' .. i, hp = i % 100,"
        "        inv = { i, i + 1, 'sword' } }\n"
        "    end\n"
        "    for i = 1, PUB_N do\n"
        "      patches[#patches + 1] = { path = '/world/k' .. i, value = 'v' .. i }\n"
        "    end\n"
        "  end\n"
        "  local k = (Count * 7919) % BIG_N + 1\n"
        "  Big[k].hp = Count\n"
        "  Big[k].name = 'p' .. Count\n"
        "  patches[#patches + 1] = { path = '/count', value = tostring(Count) }\n"
        "  patches[#patches + 1] ="
        "    { path = '/world/k' .. ((Count % PUB_N) + 1), value = 'c' .. Count }\n"
        "  return { patches = patches, results = { output = { data = tostring(Count) } } }\n"
        "end\n"
    ]).

ckpt_bench(Async, BigN, PubN, Slots, Interval) ->
    Test = self(),
    Hook =
        fun(Phase, Slot) ->
            Mem =
                case Phase of
                    data_written ->
                        {memory, M} = process_info(self(), memory),
                        {binary, Bs} = process_info(self(), binary),
                        M + lists:sum([ Sz || {_, Sz, _} <- Bs ]);
                    _ -> 0
                end,
            Test ! {bench, Phase, Slot, erlang:monotonic_time(microsecond), Mem}
        end,
    Opts =
        ckpt_opts(#{
            <<"process-async-checkpoints">> => Async,
            <<"process-delta-checkpoint-slots">> => Interval,
            <<"process-async-checkpoint-hook">> => Hook
        }),
    Process = ckpt_process(ckpt_bench_script(BigN, PubN), Opts),
    ckpt_schedule(Process, lists:seq(0, Slots), Opts),
    {ProcID, L0} = ckpt_live(Process, Opts),
    {Slot0Us, L1} = timer:tc(fun() -> ckpt_run(ProcID, L0, [0], Opts) end),
    ckpt_writer_idle(ProcID, Opts),
    Start = erlang:monotonic_time(microsecond),
    {Times, Last} =
        lists:foldl(
            fun(Slot, {Acc, S}) ->
                {Us, Next} = timer:tc(fun() -> ckpt_run(ProcID, S, [Slot], Opts) end),
                {[{Slot, Us} | Acc], Next}
            end,
            {[], L1},
            lists:seq(1, Slots)
        ),
    Elapsed = erlang:monotonic_time(microsecond) - Start,
    ckpt_writer_idle(ProcID, Opts),
    Phases = ckpt_bench_drain(#{}),
    Ckpt = [ Us || {S, Us} <- Times, S rem Interval == 0 ],
    Normal = lists:sort([ Us || {S, Us} <- Times, S rem Interval =/= 0 ]),
    Writes =
        [ {S, (L - St) div 1000, maps:get({data_written, S}, Phases, 0) div 1}
        || {{started, S}, St} <- maps:to_list(Phases),
           L <- [maps:get({linked, S}, Phases, St)] ],
    Mems = [ M || {{mem, _}, M} <- maps:to_list(Phases) ],
    #{
        mode => case Async of true -> async; false -> sync end,
        slot0_ms => Slot0Us div 1000,
        checkpoint_slot_ms => [ U div 1000 || U <- Ckpt ],
        normal_slot_p50_ms => ckpt_pct(Normal, 50) / 1000,
        normal_slot_p99_ms => ckpt_pct(Normal, 99) / 1000,
        normal_slot_max_ms => lists:last(Normal) / 1000,
        slots => Slots,
        elapsed_ms => Elapsed div 1000,
        slots_per_sec => Slots * 1000000 / Elapsed,
        writer_ms => lists:sort([ {S, Ms} || {S, Ms, _} <- Writes ]),
        writer_peak_bytes => lists:max([0 | Mems]),
        checkpoint => checkpoint_slot(ProcID, Slots, Opts),
        profile => ckpt_profile(ProcID, Last, Opts)
    }.

ckpt_bench_drain(Acc) ->
    receive
        {bench, data_written, S, T, Mem} ->
            ckpt_bench_drain(Acc#{ {data_written, S} => T, {mem, S} => Mem });
        {bench, Phase, S, T, _} -> ckpt_bench_drain(Acc#{ {Phase, S} => T })
    after 0 -> Acc
    end.

ckpt_pct(Sorted, P) -> lists:nth(max(1, (length(Sorted) * P + 99) div 100), Sorted).

%% Where a synchronous checkpoint's time goes, step by step, on `State'.
ckpt_profile(_ProcID, State, Opts) ->
    T = fun(F) -> {Us, R} = timer:tc(F), {Us div 1000, R} end,
    VM = maps:get(<<"state">>, hb_private:from_message(State)),
    {CopyMs, Pid} =
        T(fun() -> spawn(fun() -> receive go -> erlang:phash2(State) end end) end),
    exit(Pid, kill),
    {ExtMs, Ext} = T(fun() -> luerl:externalize(VM) end),
    {EncMs, Raw} = T(fun() -> term_to_binary(Ext) end),
    {ZipMs, Zipped} = T(fun() -> term_to_binary(Ext, [compressed]) end),
    {SnapMs, {ok, Snap}} = T(fun() -> snapshot(State, #{}, Opts) end),
    WithSnap = hb_ao:set(State, <<"snapshot">>, Snap, Opts),
    StoreOpts = Opts#{ <<"match-index">> => false },
    {PubMs, _} = T(fun() -> hb_cache:write(hb_private:reset(State), StoreOpts) end),
    {FullMs, _} = T(fun() -> hb_cache:write(hb_private:reset(WithSnap), StoreOpts) end),
    {SyncMs, ok} =
        T(fun() -> hb_store:sync(hb_opts:get(<<"store">>, [], Opts), Opts) end),
    #{
        state_heap_mb => erts_debug:flat_size(State) * 8 div 1048576,
        copy_to_writer_ms => CopyMs,
        externalize_ms => ExtMs,
        term_to_binary_ms => EncMs,
        raw_snapshot_mb => byte_size(Raw) div 1048576,
        term_to_binary_compressed_ms => ZipMs,
        snapshot_kb => byte_size(Zipped) div 1024,
        device_snapshot_ms => SnapMs,
        public_state_write_ms => PubMs,
        full_state_with_snapshot_write_ms => FullMs,
        store_sync_ms => SyncMs
    }.

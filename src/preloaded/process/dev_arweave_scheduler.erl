%%% @doc An AO scheduler whose ordering is the canonical Arweave base layer.
%%% One global synchronizer indexes each confirmed block once, routing every
%%% data-free transaction header to its native recipient and the processes in
%%% its `Assign-To' instruction. Process schedules then materialize only their
%%% own routed entries as contiguous AO slots.
-module(dev_arweave_scheduler).
-implements(<<"arweave-scheduler@1.0">>).
-device_libraries([lib_process, lib_arweave_scheduler_blocks]).
-export([device_info/0, router/4]).
-export([schedule/3, next/3, slot/3, status/3, sync/3, follow/3, diagnose/3]).
-export([checkpoint/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MAX_ASSIGNMENT_QUERY_LEN, 1000).

device_info() ->
    #{
        exports =>
            [
                <<"status">>,
                <<"diagnose">>,
                <<"sync">>,
                <<"follow">>,
                <<"next">>,
                <<"schedule">>,
                <<"slot">>,
                <<"init">>,
                <<"checkpoint">>
            ],
        excludes => [set, keys],
        default => fun router/4
    }.

router(_, Base, Req, Opts) -> schedule(Base, Req, Opts).

%% @doc Return the next dense assignment after the process's current slot.
next(Base, Req, Opts) ->
    ProcessID = hb_util:human_id(lib_process:process_id(Base, Req, Opts)),
    LastProcessed =
        hb_util:int(
            hb_maps:get(
                <<"at-slot">>,
                Base,
                -1,
                Opts#{ <<"hashpath">> => ignore }
            )
        ),
    maybe
        {ok, Assignment} ?= assignment(ProcessID, LastProcessed + 1, Opts),
        {ok, #{ <<"body">> => Assignment, <<"state">> => Base }}
    end.

%% @doc The assignment at `Slot', synchronizing only when it is not yet
%% materialized. A materialized slot is final -- it was ordered from
%% confirmed blocks, and materializing again writes the same assignment
%% (`dev_arweave_scheduler_sync' `sparse_materialization_test') -- so a
%% compute replaying slots this node already holds reads them from the cache
%% and never queues behind the node-wide sync and the process's materializer.
%% Every slot used to ask both: replaying PUSD's history cost ~720 ms a slot
%% in that queue (p99 10 s) on the Paralith node, 2026-11-23, for slots it
%% had held for hours.
assignment(ProcessID, Slot, Opts) ->
    case dev_arweave_scheduler_cache:read_assignment(ProcessID, Slot, Opts) of
        {ok, Assignment} -> {ok, Assignment};
        not_found ->
            maybe
                {ok, _} ?= dev_arweave_scheduler_sync:process(ProcessID, Opts),
                find_assignment(ProcessID, Slot, Opts)
            end
    end.

find_assignment(ProcessID, Slot, Opts) ->
    case dev_arweave_scheduler_cache:read_assignment(ProcessID, Slot, Opts) of
        {ok, Assignment} -> {ok, Assignment};
        not_found ->
            {error,
                #{
                    <<"status">> => 404,
                    <<"reason">> =>
                        <<"Requested slot not yet available in schedule.">>
                }
            }
    end.

%% @doc Read a schedule or relay a presigned data-free L1 transaction.
schedule(Base, Req, Opts) ->
    case hb_util:key_to_atom(hb_maps:get(<<"method">>, Req, <<"GET">>, Opts)) of
        post -> post_schedule(Base, Req, Opts);
        get -> get_schedule(Base, Req, Opts)
    end.

get_schedule(Base, Req, Opts) ->
    ProcessID = find_process_id(Base, Req, Opts),
    {From, RequestedTo} = slot_range(Req, Opts),
    maybe
        {ok, #{ <<"next-slot">> := NextSlot }} ?=
            dev_arweave_scheduler_sync:process(ProcessID, Opts),
        Latest = NextSlot - 1,
        To =
            case RequestedTo of
                undefined -> Latest;
                _ -> min(RequestedTo, Latest)
            end,
        CappedTo = min(To, From + ?MAX_ASSIGNMENT_QUERY_LEN),
        Assignments = read_assignments(ProcessID, From, CappedTo, Opts),
        dev_arweave_scheduler_cache:assignments_to_bundle(
            ProcessID,
            Assignments,
            CappedTo < To,
            Opts
        )
    end.

post_schedule(Base, Req, Opts) ->
    ProcessID = find_process_id(Base, Req, Opts),
    maybe
        {ok, ToSchedule} ?= load_message(Base, Req, Opts),
        {ok, Committed} ?= committed(ToSchedule, Opts),
        {ok, TX} ?= valid_transaction(Committed, Opts),
        Targets = dev_arweave_scheduler_sync:targets(TX),
        true ?= lists:member(ProcessID, Targets),
        {ok, _} ?=
            hb_ao:raw(
                <<"arweave@2.9">>,
                <<"tx">>,
                hb_message:with_commitments(
                    #{ <<"commitment-device">> => <<"tx@1.0">> },
                    Committed,
                    Opts
                ),
                #{
                    <<"method">> => <<"POST">>,
                    <<"arweave-subject">> => <<"base">>
                },
                Opts
            ),
        {ok, _} = dev_arweave_scheduler_cache:write_header(Committed, Opts),
        {ok,
            #{
                <<"status">> => 202,
                <<"txid">> => hb_util:human_id(TX#tx.id),
                <<"targets">> => Targets,
                <<"body">> =>
                    <<"Transaction dispatched to Arweave. It will receive ",
                        "a slot after confirmation.">>
            }
        }
    else
        false ->
            {error,
                #{
                    <<"status">> => 422,
                    <<"reason">> =>
                        <<"Transaction does not target the requested process.">>,
                    <<"process">> => ProcessID
                }
            };
        Error -> Error
    end.

%% @doc Return the current contiguous slot after synchronizing this process.
slot(Base, Req, Opts) ->
    ProcessID = find_process_id(Base, Req, Opts),
    maybe
        {ok, #{ <<"next-slot">> := NextSlot }} ?=
            dev_arweave_scheduler_sync:process(ProcessID, Opts),
        {ok,
            #{
                <<"process">> => ProcessID,
                <<"current">> => NextSlot - 1,
                <<"cache-control">> => <<"no-store">>
            }
        }
    end.

%% @doc Proactively advance the shared global target index without touching a
%% particular process schedule.
sync(_Base, _Req, Opts) ->
    dev_arweave_scheduler_sync:sync(Opts).

%% @doc Advance the global index, then bring every process the node follows
%% (`arweave-scheduler-follow-processes') to its head, computed. The node's
%% background loop calls it (`hb_http_server').
follow(_Base, _Req, Opts) ->
    dev_arweave_scheduler_sync:follow(Opts).

%% @doc Report the global covered interval without scanning every process.
status(_Base, _Req, Opts) ->
    Address = hb_opts:address(hb:wallet(), Opts),
    case dev_arweave_scheduler_cache:read_global(Opts) of
        {ok, Global} ->
            {ok,
                #{
                    <<"address">> => Address,
                    <<"sync">> => Global,
                    <<"cache-control">> => <<"no-store">>
                }
            };
        _ ->
            {ok,
                #{
                    <<"address">> => Address,
                    <<"cache-control">> => <<"no-store">>
                }
            }
    end.

%% @doc Return process-materialization failures as data for operators. Normal
%% schedule/slot paths keep their existing status semantics; this key is
%% read-only and never changes the scheduler algorithm.
diagnose(Base, Req, Opts) ->
    try
        ProcessID = find_process_id(Base, Req, Opts),
        dev_arweave_scheduler_sync:process(ProcessID, Opts)
    of
        {ok, State} ->
            {ok, #{ <<"status">> => <<"ok">>, <<"state">> => State }};
        {error, Reason} ->
            {ok, #{ <<"status">> => <<"error">>, <<"error">> => Reason }};
        Other ->
            {ok, #{ <<"status">> => <<"unexpected">>,
                <<"detail">> => hb_util:bin(Other) }}
    catch
        Class:Reason:Stack ->
            Detail = iolist_to_binary(io_lib:format("~0p:~0p", [Class, Reason])),
            Frames = iolist_to_binary(io_lib:format("~0p", [lists:sublist(Stack, 5)])),
            {ok, #{ <<"status">> => <<"exception">>,
                <<"detail">> => Detail, <<"stack">> => Frames }}
    end.

checkpoint(State) -> {ok, State}.

find_process_id(Base, Req, Opts) ->
    case hb_maps:get(<<"target">>, Req, not_found, Opts) of
        Target when Target =/= not_found -> hb_util:human_id(Target);
        not_found ->
            hb_util:human_id(lib_process:process_id(Base, Req, Opts))
    end.

slot_range(Req, Opts) ->
    From =
        case hb_maps:get(<<"from">>, Req, not_found, Opts) of
            not_found -> 0;
            FromValue -> max(0, hb_util:int(FromValue))
        end,
    To =
        case hb_maps:get(<<"to">>, Req, not_found, Opts) of
            not_found -> undefined;
            ToValue -> hb_util:int(ToValue)
        end,
    {From, To}.

read_assignments(_ProcessID, From, To, _Opts) when From > To -> [];
read_assignments(ProcessID, Slot, To, Opts) ->
    case dev_arweave_scheduler_cache:read_assignment(ProcessID, Slot, Opts) of
        {ok, Assignment} ->
            [Assignment | read_assignments(ProcessID, Slot + 1, To, Opts)];
        not_found -> []
    end.

load_message(Base, Req, Opts) ->
    Subject = hb_maps:get(<<"subject">>, Req, not_found, Opts),
    Raw =
        case Subject of
            <<"base">> -> Base;
            <<"self">> -> Req;
            not_found -> hb_maps:get(<<"body">>, Req, Req, Opts);
            Key -> hb_maps:get(Key, Req, not_found, Opts)
        end,
    case Raw of
        not_found -> missing_message();
        _ ->
            try {ok, hb_cache:ensure_all_loaded(Raw, Opts)}
            catch
                error:{necessary_message_not_found, _, _} -> missing_message()
            end
    end.

missing_message() ->
    {error,
        #{
            <<"status">> => 404,
            <<"reason">> => <<"Cannot fully load message to schedule.">>
        }
    }.

committed(Message, Opts) ->
    case hb_message:with_only_committed(Message, Opts) of
        {ok, Committed} -> {ok, Committed};
        {error, Reason} ->
            {error,
                #{
                    <<"status">> => 400,
                    <<"reason">> =>
                        <<"Committed components cannot be validated.">>,
                    <<"detail">> => Reason
                }
            }
    end.

valid_transaction(Message, Opts) ->
    case {
        lists:member(
            <<"tx@1.0">>,
            hb_message:commitment_devices(Message, Opts)
        ),
        hb_message:signers(Message, Opts),
        hb_message:verify(Message, signers, Opts)
    } of
        {true, [_ | _], true} ->
            try hb_message:convert(Message, <<"tx@1.0">>, Opts) of
                TX = #tx{ data_size = 0 } -> {ok, TX};
                #tx{} ->
                    {error,
                        #{
                            <<"status">> => 422,
                            <<"reason">> =>
                                <<"The Arweave scheduler accepts data-free ",
                                    "transaction headers only.">>
                        }
                    }
            catch
                _:_ -> invalid_transaction()
            end;
        _ -> invalid_transaction()
    end.

invalid_transaction() ->
    {error,
        #{
            <<"status">> => 422,
            <<"reason">> =>
                <<"Message must have a valid signed tx@1.0 commitment.">>
        }
    }.

%%% Tests

%% A slot already materialized is read from the cache: neither the node-wide
%% sync nor the process's materializer is asked (there is no global record
%% and no gateway here, so asking either could not return this slot).
materialized_slot_skips_the_sync_test() ->
    Store = hb_test_utils:test_store(hb_store_volatile, <<"ar-sched-next">>),
    ok = hb_store:start(Store),
    Opts = #{ <<"store">> => [Store], <<"priv-wallet">> => ar_wallet:new() },
    ProcessID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok =
        dev_arweave_scheduler_cache:write_assignment(
            #{
                <<"path">> => <<"compute">>,
                <<"process">> => ProcessID,
                <<"slot">> => 1,
                <<"block-height">> => 100,
                <<"block-index">> => 0,
                <<"body">> => <<"message">>,
                <<"type">> => <<"Assignment">>
            },
            Opts
        ),
    {ok, Assignment} = assignment(ProcessID, 1, Opts),
    ?assertEqual(1, hb_util:int(hb_maps:get(<<"slot">>, Assignment, 0, Opts))),
    SchedulerStore =
        hb_opts:get(store, no_viable_store, dev_arweave_scheduler_cache:opts(Opts)),
    ?assertEqual(undefined,
        hb_name:lookup({dev_arweave_scheduler_sync, sync, SchedulerStore})),
    ?assertEqual(undefined,
        hb_name:lookup(
            {dev_arweave_scheduler_sync, process, SchedulerStore, ProcessID})),
    ok = hb_store:stop(Store).

slot_range_test() ->
    ?assertEqual({0, undefined}, slot_range(#{}, #{})),
    ?assertEqual(
        {0, 42},
        slot_range(#{ <<"from">> => -5, <<"to">> => 42 }, #{})
    ).

%% @doc A followed process's `/now' advances when the index learns a new slot
%% for it, with nobody computing it: the node's own background loop
%% (`hb_http_server', every `arweave_scheduler_follow_ms') materializes the
%% slot and computes it. The test only writes the index rows a sync would
%% have written and reads `/now' from the cache, never computing.
followed_process_now_advances_test_() ->
    {timeout, 60, fun followed_process_now_advances/0}.

followed_process_now_advances() ->
    hb:init(),
    Store = hb_test_utils:test_store(),
    Tip = atomics:new(1, []),
    atomics:put(Tip, 1, 20),
    Hook = fun(_, Request, _) ->
        Height = integer_to_binary(atomics:get(Tip, 1)),
        {ok, Request#{<<"body">> => #{<<"status">> => 200,
            <<"content-type">> => <<"application/json">>,
            <<"body">> => <<"{\"height\":", Height/binary, "}">>}}}
    end,
    Wallet = ar_wallet:new(),
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    Script = <<"function compute(req)\n"
        " return {patches={{path='/n', value=req.body.n or 'spawn'}}, results={}} end">>,
    Base = #{
        <<"store">> => [Store],
        <<"scheduler-store">> => [Store],
        <<"arweave-scheduler-store">> => [Store],
        <<"priv-wallet">> => Wallet,
        <<"hashpath">> => ignore,
        <<"arweave-scheduler-from">> => 10,
        <<"arweave-scheduler-from-hash">> => <<"anchor">>,
        <<"process-delta-cache">> => false,
        <<"scheduler-lookahead">> => false,
        <<"await-inprogress">> => false
    },
    Process = hb_message:commit(#{
        <<"device">> => <<"process@1.0">>,
        <<"type">> => <<"Process">>,
        <<"scheduler-device">> => <<"arweave-scheduler@1.0">>,
        <<"execution-device">> => <<"lua-patch@1.0">>,
        <<"module">> => #{<<"content-type">> => <<"application/lua">>, <<"body">> => Script},
        <<"authority">> => [Address],
        <<"scheduler-location">> => Address,
        <<"test-random-seed">> => rand:uniform(1000000)
    }, Base),
    {ok, _} = hb_cache:write(Process, Base),
    ProcID = hb_util:human_id(hb_message:id(Process, all, Base)),
    Put = fun(Path, Value) ->
        {ok, ID} = hb_cache:write(Value, Base),
        ok = hb_cache:link(ID, Path, Base)
    end,
    % The index as a sync leaves it: blocks to 18, the process spawned at
    % 10-0 and materialized to 18 (slot 0 only).
    Put(<<"~arweave-scheduler@1.0/sync/global">>,
        #{<<"from">> => 10, <<"to">> => 18, <<"from-hash">> => <<"anchor">>}),
    Put(<<"~arweave-scheduler@1.0/sync/process/", ProcID/binary>>,
        #{<<"process">> => ProcID, <<"spawn-ordinate">> => <<"10-0">>,
            <<"synced-to">> => 18, <<"next-slot">> => 1}),
    Put(<<"~arweave-scheduler@1.0/assignments/", ProcID/binary, "/0">>,
        #{<<"path">> => <<"compute">>, <<"data-protocol">> => <<"ao">>,
            <<"variant">> => <<"ao.N.1">>, <<"process">> => ProcID,
            <<"epoch">> => <<"0">>, <<"slot">> => 0,
            <<"block-height">> => 10, <<"block-index">> => 0,
            <<"block-hash">> => hb_util:encode(crypto:strong_rand_bytes(48)),
            <<"timestamp">> => 1700000000, <<"body">> => Process,
            <<"type">> => <<"Assignment">>}),
    % The gateway: a node that answers every request with the tip height.
    Gateway = hb_http_server:start_node(#{<<"port">> => 0,
        <<"on">> => #{<<"response">> => #{<<"device">> => #{<<"response">> => Hook}}}}),
    Opts = Base#{ <<"gateway">> => hb_util:bin(Gateway) },
    _Node = hb_http_server:start_node(Opts#{
        <<"port">> => 0,
        <<"arweave-scheduler-follow-ms">> => 200,
        <<"arweave-scheduler-follow-processes">> => [ProcID]
    }),
    Now = fun() ->
        case hb_ao:resolve(Process, #{ <<"path">> => <<"now">> },
                Opts#{ <<"process-now-from-cache">> => <<"always">> }) of
            {ok, State} ->
                {hb_util:int(hb_ao:get(<<"at-slot">>, State, Opts)),
                    hb_ao:get(<<"n">>, State, Opts)};
            _ -> none
        end
    end,
    WaitFor = fun Wait(Want, 0) -> Now() == Want;
                  Wait(Want, N) ->
                      Now() == Want orelse (ok == timer:sleep(200) andalso Wait(Want, N - 1))
              end,
    % Following computes the head it finds, so the spawn alone is computed.
    ?assert(WaitFor({0, <<"spawn">>}, 100)),
    % Block 19 carries a data-free transaction to the process: the index
    % learns it, and the tip moves past it.
    TX = ar_tx:sign(#tx{format = 2, target = hb_util:native_id(ProcID),
        tags = [{<<"N">>, <<"1">>}]}, Wallet),
    Header = hb_message:convert(
        ar_tx:json_struct_to_tx(hb_json:decode(hb_json:encode(ar_tx:tx_to_json_struct(TX)))),
        <<"structured@1.0">>, <<"tx@1.0">>, Opts),
    {ok, _} = dev_arweave_scheduler_cache:write_header(Header, Opts),
    {ok, _} = hb_cache:write(Header, Opts),
    {ok, _} = dev_arweave_scheduler_cache:write_block(19,
        #{ <<"height">> => 19, <<"indep-hash">> => crypto:strong_rand_bytes(48),
            <<"timestamp">> => 1700000120 }, Opts),
    ok = dev_arweave_scheduler_cache:write_target(ProcID, <<"19-0">>,
        hb_util:human_id(TX#tx.id), Opts),
    atomics:put(Tip, 1, 21),
    Put(<<"~arweave-scheduler@1.0/sync/global">>,
        #{<<"from">> => 10, <<"to">> => 19, <<"from-hash">> => <<"anchor">>}),
    ?assert(WaitFor({1, <<"1">>}, 100)).

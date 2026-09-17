
%%% @doc A wrapper around the hb_cache module that provides a more
%%% convenient interface for reading the result of a process at a given slot or
%%% message ID.
-module(dev_process_cache).
-export([latest/2, latest/3, latest/4, read/2, read/3, write/4]).
-include_lib("eunit/include/eunit.hrl").
-include("include/hb.hrl").

-define(DELTA_FORMAT, <<"process-delta@1.0">>).
-define(DELTA_META, <<"process-cache-delta">>).
-define(HOT_CACHE, dev_process_delta_hot_cache).
-define(DEFAULT_DELTA_CHECKPOINT_SLOTS, 1000).

%% @doc Read the result of a process at a given slot.
read(ProcID, Opts) ->
    hb_util:ok(latest(ProcID, Opts)).
read(ProcID, SlotRef, Opts) ->
    ?event({reading_computed_result, ProcID, SlotRef}),
    case hot_read(ProcID, SlotRef, Opts) of
        {ok, Msg} -> {ok, Msg};
        not_found ->
            Path = path(ProcID, SlotRef, Opts),
            case hb_cache:read(Path, Opts) of
                {ok, Stored} -> materialize(ProcID, Stored, Opts);
                Other -> Other
            end
    end.

%% @doc Write a process computation result to the cache.
write(ProcID, Slot, Msg, Opts) ->
    case delta_metadata(Msg, Opts) of
        {ok, Delta} -> write_delta(ProcID, Slot, Msg, Delta, Opts);
        not_found -> write_full(ProcID, Slot, Msg, Opts)
    end.

%% @doc Preserve the original full-message cache behavior.
write_full(ProcID, Slot, Msg, Opts) ->
    % Write the item to the cache in the root of the store.
    {ok, Root} = hb_cache:write(hb_private:reset(Msg), Opts),
    ok = link_result(ProcID, Slot, Root, Root, Opts),
    {ok, path(ProcID, Slot, Opts)}.

%% @doc Store a full public checkpoint or a small ordered delta. The Lua VM
%% snapshot and public-state checkpoint share a cadence so a cold restore never
%% has to cross more than one configured delta window.
write_delta(ProcID, Slot, Msg, Delta, Opts) ->
    Patches = hb_ao:get(<<"patches">>, Delta, not_found, Opts),
    Results = hb_ao:get(<<"results">>, Delta, not_found, Opts),
    case hb_process_delta:validate(Patches, Opts) of
        ok when is_map(Results) -> ok;
        ok -> erlang:error({invalid_process_delta_results, Results});
        Error -> erlang:error({invalid_process_delta, Error})
    end,
    PublicMsg = hb_private:reset(Msg),
    case should_checkpoint(ProcID, Slot, Msg, Opts) of
        true ->
            {ok, StoredRoot} = hb_cache:write(PublicMsg, Opts),
            ok = link_result(ProcID, Slot, StoredRoot, StoredRoot, Opts),
            hot_put(ProcID, Slot, PublicMsg, Opts),
            {ok, path(ProcID, Slot, Opts)};
        false ->
            Envelope = #{
                <<"cache-format">> => ?DELTA_FORMAT,
                <<"slot">> => Slot,
                <<"base-slot">> => Slot - 1,
                <<"patches">> => Patches,
                <<"results">> => Results
            },
            {ok, StoredRoot} = hb_cache:write(Envelope, Opts),
            ok = link_result(ProcID, Slot, StoredRoot, StoredRoot, Opts),
            hot_put(ProcID, Slot, PublicMsg, Opts),
            {ok, path(ProcID, Slot, Opts)}
    end.

%% @doc Atomically publish both cache aliases after their target is durable.
link_result(ProcID, Slot, LogicalRoot, StoredRoot, Opts) ->
    % Link the item to the path in the store by slot number.
    SlotNumPath = path(ProcID, Slot, Opts),
    % Link the item to the message ID path in the store.
    MsgIDPath =
        path(
            ProcID,
            LogicalRoot,
            Opts
        ),
    ?event(
        {linking_id,
            {proc_id, ProcID},
            {slot, Slot},
            {id, LogicalRoot},
            {path, MsgIDPath}
        }
    ),
    ok = hb_store:link(
        hb_opts:get(<<"store">>, no_viable_store, Opts),
        #{
            SlotNumPath => StoredRoot,
            MsgIDPath => StoredRoot
        },
        Opts
    ),
    ok.

delta_metadata(Msg, Opts) ->
    case hb_opts:get(<<"process-delta-cache">>, true, Opts) of
        false -> not_found;
        <<"false">> -> not_found;
        _ ->
            case hb_private:get(?DELTA_META, Msg, not_found, Opts) of
                Delta when is_map(Delta) -> {ok, Delta};
                _ -> not_found
            end
    end.

should_checkpoint(ProcID, Slot, Msg, Opts) ->
    HasSnapshot = hb_ao:get(<<"snapshot">>, Msg, not_found, Opts) =/= not_found,
    Interval = delta_checkpoint_slots(Opts),
    MissingBase =
        Slot > 0 andalso
            hb_store:read(
                hb_opts:get(<<"store">>, no_viable_store, Opts),
                path(ProcID, Slot - 1, Opts),
                Opts
            ) =:= {error, not_found},
    Slot =< 0 orelse HasSnapshot orelse Slot rem Interval =:= 0 orelse MissingBase.

delta_checkpoint_slots(Opts) ->
    Raw = hb_opts:get(
        <<"process-delta-checkpoint-slots">>,
        ?DEFAULT_DELTA_CHECKPOINT_SLOTS,
        Opts
    ),
    case hb_util:int(Raw) of
        Interval when Interval > 0 -> Interval;
        _ -> erlang:error({invalid_process_delta_checkpoint_slots, Raw})
    end.

materialize(ProcID, #{ <<"cache-format">> := ?DELTA_FORMAT } = Delta, Opts) ->
    Slot = hb_util:int(maps:get(<<"slot">>, Delta)),
    BaseSlot = hb_util:int(maps:get(<<"base-slot">>, Delta)),
    case BaseSlot =:= Slot - 1 of
        false -> {error, {invalid_process_delta_chain, BaseSlot, Slot}};
        true ->
            case read(ProcID, BaseSlot, Opts) of
                {ok, Base} ->
                    StoredPatches = hb_cache:ensure_all_loaded(
                        maps:get(<<"patches">>, Delta),
                        Opts
                    ),
                    StoredResults = hb_cache:ensure_all_loaded(
                        maps:get(<<"results">>, Delta),
                        Opts
                    ),
                    hb_process_delta:apply(
                        Base,
                        patch_list(StoredPatches, Opts),
                        StoredResults,
                        Slot,
                        Opts
                    );
                Error -> Error
            end
    end;
materialize(_ProcID, Msg, _Opts) -> {ok, Msg}.

patch_list(Patches, _Opts) when is_list(Patches) -> Patches;
patch_list(Patches, Opts) when is_map(Patches) ->
    case hb_util:is_ordered_list(Patches, Opts) of
        true -> hb_util:message_to_ordered_list(Patches);
        false -> Patches
    end;
patch_list(Patches, _Opts) -> Patches.

hot_read(ProcID, SlotRef, Opts) when is_integer(SlotRef) ->
    ensure_hot_cache(),
    Key = hot_key(ProcID, Opts),
    try ets:lookup(?HOT_CACHE, Key) of
        [{Key, SlotRef, Msg}] -> {ok, Msg};
        _ -> not_found
    catch error:badarg -> not_found
    end;
hot_read(_ProcID, _SlotRef, _Opts) -> not_found.

hot_put(ProcID, Slot, Msg, Opts) ->
    ensure_hot_cache(),
    Entry = {hot_key(ProcID, Opts), Slot, Msg},
    try ets:insert(?HOT_CACHE, Entry) of
        true -> ok
    catch error:badarg ->
        % The short-lived process that first created the table may have exited
        % between `ensure' and `insert'. Recreate once; the durable delta was
        % already written, so losing this acceleration never loses state.
        ensure_hot_cache(),
        try ets:insert(?HOT_CACHE, Entry)
        catch error:badarg -> false
        end,
        ok
    end,
    ok.

hot_key(ProcID, Opts) ->
    {
        ProcID,
        hb_opts:get(<<"process-cache-scope">>, local, Opts)
    }.

ensure_hot_cache() ->
    case ets:whereis(?HOT_CACHE) of
        undefined ->
            try ets:new(?HOT_CACHE, [named_table, public, set]) of
                _ -> ok
            catch error:badarg -> ok
            end;
        _ -> ok
    end.

%% @doc Calculate the path of a result, given a process ID and a slot.
path(ProcID, Ref, Opts) ->
    path(ProcID, Ref, [], Opts).
path(ProcID, Ref, PathSuffix, _Opts) ->
    hb_path:to_binary(
        [
            <<"computed">>,
            hb_util:human_id(ProcID)
        ] ++
        case Ref of
            Int when is_integer(Int) -> ["slot", integer_to_binary(Int)];
            root -> [];
            slot_root -> ["slot"];
            _ -> [Ref]
        end ++ PathSuffix
    ).

%% @doc Retrieve the latest slot for a given process. Optionally state a limit
%% on the slot number to search for, as well as a required path that the slot
%% must have.
latest(ProcID, Opts) -> latest(ProcID, [], Opts).
latest(ProcID, RequiredPath, Opts) ->
    latest(ProcID, RequiredPath, undefined, Opts).
latest(ProcID, RawRequiredPath, Limit, RawOpts) ->
    Scope = hb_opts:get(<<"process-cache-scope">>, local, RawOpts),
    % Normalize the store descriptor to a list of stores.
    UnscopedStore =
        case hb_opts:get(<<"store">>, no_viable_store, RawOpts) of
            StoreMsg when is_map(StoreMsg) -> [StoreMsg];
            Other -> Other
        end,
    % Apply the scope to the store and update the options message.
    ScopedStore = hb_store:scope(UnscopedStore, Scope),
    Opts = RawOpts#{ <<"store">> => ScopedStore },
    % Convert the required path to a list of _binary_ keys.
    RequiredPath =
        case RawRequiredPath of
            undefined -> [];
            [] -> [];
            _ ->
                hb_path:term_to_path_parts(
                    RawRequiredPath,
                    Opts
                )
        end,
    ?event({required_path_converted, {proc_id, ProcID}, {required_path, RequiredPath}}),
    Path = path(ProcID, slot_root, Opts),
    AllSlots = hb_cache:list_numbered(Path, Opts),
    ?event({all_slots, {proc_id, ProcID}, {slots, AllSlots}}),
    CappedSlots =
        case Limit of
            undefined -> AllSlots;
            _ -> lists:filter(fun(Slot) -> Slot =< Limit end, AllSlots)
        end,
    ?event(
        {finding_latest_slot,
            {proc_id, hb_util:human_id(ProcID)},
            {limit, Limit},
            {path, Path},
            {slots_in_range, CappedSlots}
        }
    ),
    % Find the highest slot that has the necessary path.
    BestSlot =
        first_with_path(
            ProcID,
            RequiredPath,
            lists:reverse(lists:sort(CappedSlots)),
            Opts
        ),
    case BestSlot of
        {failure, _} = Failure ->
            Failure;
        {error, _} = Error ->
            Error;
        not_found ->
            % No slot found with the necessary path was found.
            {error, not_found};
        SlotNum ->
            % Found. Return the slot number and the message at that slot.
            {ok, Msg} = read(ProcID, SlotNum, Opts),
            {ok, SlotNum, Msg}
    end.

%% @doc Find the latest assignment with the requested path suffix.
first_with_path(ProcID, RequiredPath, Slots, Opts) ->
    first_with_path(
        ProcID,
        RequiredPath,
        Slots,
        Opts,
        hb_opts:get(<<"store">>, no_viable_store, Opts)
    ).
first_with_path(_ProcID, _Required, [], _Opts, _Store) ->
    not_found;
first_with_path(ProcID, RequiredPath, [Slot | Rest], Opts, Store) ->
    RawPath = path(ProcID, Slot, RequiredPath, Opts),
    ?event({trying_slot, {slot, Slot}, {path, RawPath}}),
    case hb_store:read(Store, RawPath, Opts) of
        {error, not_found} ->
            first_with_path(ProcID, RequiredPath, Rest, Opts, Store);
        {failure, _} = Failure ->
            Failure;
        {error, _} = Error ->
            Error;
        _ ->
            Slot
    end.

%%% Tests

process_cache_suite_test_() ->
    hb_store:generate_test_suite(
        [
            {"write and read process outputs", fun test_write_and_read_output/1},
            {"find latest output (with path)", fun find_latest_outputs/1},
            {"delta roundtrip and checkpoint", fun delta_roundtrip/1}
        ],
        [
            {Name, Opts}
        ||
            {Name, Opts} <- hb_store:test_stores()
        ]
    ).

delta_roundtrip_test_() ->
    {timeout, 60, fun() ->
        application:ensure_all_started(hb),
        delta_roundtrip(#{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new()
        })
    end}.

%% @doc Manual storage benchmark. Example:
%% `HB_PROCESS_DELTA_BENCH=25:0,1000,4000 rebar3 device test -d dev_process'.
delta_cache_benchmark_report_test() ->
    case os:getenv("HB_PROCESS_DELTA_BENCH") of
        false -> ok;
        Spec ->
            [IterationsRaw, EntriesRaw] = string:split(Spec, ":"),
            Iterations = list_to_integer(IterationsRaw),
            Results = [
                delta_cache_benchmark(Iterations, list_to_integer(Entries))
                || Entries <- string:tokens(EntriesRaw, ",")
            ],
            io:format(user, "PROCESS_DELTA_BENCH ~p~n", [Results])
    end.

delta_replay_benchmark_report_test() ->
    case os:getenv("HB_PROCESS_DELTA_REPLAY") of
        false -> ok;
        Spec ->
            [SlotsRaw, EntriesRaw] = string:tokens(Spec, ":"),
            Result = delta_replay_benchmark(
                list_to_integer(SlotsRaw),
                list_to_integer(EntriesRaw)
            ),
            io:format(user, "PROCESS_DELTA_REPLAY ~p~n", [Result])
    end.

delta_cache_benchmark(Iterations, Entries) ->
    Opts = #{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"priv-wallet">> => ar_wallet:new(),
        <<"process-delta-checkpoint-slots">> => 1000
    },
    Ledger = maps:from_list([
        {
            <<"account-", (integer_to_binary(Number))/binary>>,
            <<"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef">>
        }
        || Number <- lists:seq(1, Entries)
    ]),
    Results0 = #{ <<"output">> => #{ <<"data">> => <<"0">> } },
    State0 = #{
        <<"at-slot">> => 0,
        <<"count">> => <<"0">>,
        <<"ledger">> => Ledger,
        <<"results">> => Results0
    },
    FullProc = hb_util:encode(crypto:strong_rand_bytes(32)),
    DeltaProc = hb_util:encode(crypto:strong_rand_bytes(32)),
    {ok, _} = write_full(FullProc, 0, State0, Opts),
    {ok, _} = write(
        DeltaProc,
        0,
        with_delta(State0, [], Results0, Opts),
        Opts
    ),
    {FullState, FullTimes} = benchmark_writes(
        full,
        FullProc,
        State0,
        Iterations,
        Opts
    ),
    {DeltaState, DeltaTimes} = benchmark_writes(
        delta,
        DeltaProc,
        State0,
        Iterations,
        Opts
    ),
    {ok, StoredDelta} = read(DeltaProc, Iterations, Opts),
    ?assertEqual(
        hb_ao:get(<<"count">>, FullState, Opts),
        hb_ao:get(<<"count">>, StoredDelta, Opts)
    ),
    ?assertEqual(
        hb_ao:get(<<"ledger">>, FullState, Opts),
        hb_ao:get(<<"ledger">>, DeltaState, Opts)
    ),
    FullSummary = cache_latency_summary(FullTimes),
    DeltaSummary = cache_latency_summary(DeltaTimes),
    #{
        <<"iterations">> => Iterations,
        <<"state-entries">> => Entries,
        <<"full">> => FullSummary,
        <<"delta">> => DeltaSummary,
        <<"p50-speedup">> =>
            maps:get(<<"p50-us">>, FullSummary) /
                maps:get(<<"p50-us">>, DeltaSummary)
    }.

benchmark_writes(Mode, ProcID, State0, Iterations, Opts) ->
    {State, RevTimes} = lists:foldl(
        fun(Slot, {Previous, Times}) ->
            Count = integer_to_binary(Slot),
            Patches = [#{ <<"path">> => <<"/count">>, <<"value">> => Count }],
            Results = #{ <<"output">> => #{ <<"data">> => Count } },
            {ok, Next} = hb_process_delta:apply(
                Previous,
                Patches,
                Results,
                Slot,
                Opts
            ),
            ToStore =
                case Mode of
                    full -> Next;
                    delta -> with_delta(Next, Patches, Results, Opts)
                end,
            {Micros, {ok, _}} = timer:tc(fun() ->
                case Mode of
                    full -> write_full(ProcID, Slot, ToStore, Opts);
                    delta -> write(ProcID, Slot, ToStore, Opts)
                end
            end),
            {Next, [Micros | Times]}
        end,
        {State0, []},
        lists:seq(1, Iterations)
    ),
    {State, lists:reverse(RevTimes)}.

cache_latency_summary(Times) ->
    Sorted = lists:sort(Times),
    #{
        <<"min-us">> => hd(Sorted),
        <<"mean-us">> => lists:sum(Sorted) div length(Sorted),
        <<"p50-us">> => cache_percentile(Sorted, 50),
        <<"p95-us">> => cache_percentile(Sorted, 95),
        <<"max-us">> => lists:last(Sorted)
    }.

cache_percentile(Sorted, Percent) ->
    Index = max(1, (length(Sorted) * Percent + 99) div 100),
    lists:nth(Index, Sorted).

delta_replay_benchmark(CheckpointSlots, Entries)
        when CheckpointSlots > 1, Entries >= 0 ->
    Opts = #{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"priv-wallet">> => ar_wallet:new(),
        <<"process-delta-checkpoint-slots">> => CheckpointSlots
    },
    Ledger = maps:from_list([
        {
            <<"account-", (integer_to_binary(Number))/binary>>,
            integer_to_binary(Number)
        }
        || Number <- lists:seq(1, Entries)
    ]),
    Results0 = #{ <<"output">> => #{ <<"data">> => <<"0">> } },
    State0 = #{
        <<"at-slot">> => 0,
        <<"count">> => <<"0">>,
        <<"ledger">> => Ledger,
        <<"results">> => Results0
    },
    ProcID = hb_util:encode(crypto:strong_rand_bytes(32)),
    {ok, _} = write(
        ProcID,
        0,
        with_delta(State0, [], Results0, Opts),
        Opts
    ),
    {StateAtCheckpoint, _} = benchmark_writes(
        delta,
        ProcID,
        State0,
        CheckpointSlots,
        Opts
    ),
    Target = CheckpointSlots - 1,
    {Micros, {ok, Historical}} = timer:tc(
        fun() -> read(ProcID, Target, Opts) end
    ),
    ?assertEqual(
        integer_to_binary(Target),
        hb_ao:get(<<"count">>, Historical, Opts)
    ),
    ?assertEqual(
        integer_to_binary(CheckpointSlots),
        hb_ao:get(<<"count">>, StateAtCheckpoint, Opts)
    ),
    #{
        <<"checkpoint-slots">> => CheckpointSlots,
        <<"state-entries">> => Entries,
        <<"replayed-deltas">> => Target,
        <<"historical-read-us">> => Micros
    }.

%% @doc Test for writing multiple computed outputs, then getting them by
%% their slot number and by their signed and unsigned IDs.
test_write_and_read_output(Opts) ->
    Proc = hb_cache:test_signed(
        #{ <<"test-item">> => hb_cache:test_unsigned(<<"test-body-data">>) }),
    ProcID = hb_util:human_id(hb_ao:get(id, Proc)),
    Item1 = hb_cache:test_signed(<<"Simple signed output #1">>),
    Item2 = hb_cache:test_unsigned(<<"Simple unsigned output #2">>),
    {ok, Path0} = write(ProcID, 0, Item1, Opts),
    {ok, Path1} = write(ProcID, 1, Item2, Opts),
    {ok, DirectReadItem1} = hb_cache:read(Path0, Opts),
    ?assert(hb_message:match(Item1, DirectReadItem1)),
    {ok, DirectReadItem2} = hb_cache:read(Path1, Opts),
    ?assert(hb_message:match(Item2, DirectReadItem2)),
    {ok, ReadItem1BySlotNum} = read(ProcID, 0, Opts),
    ?assert(hb_message:match(Item1, ReadItem1BySlotNum)),
    {ok, ReadItem2BySlotNum} = read(ProcID, 1, Opts),
    ?assert(hb_message:match(Item2, ReadItem2BySlotNum)),
    {ok, ReadItem1ByID} =
        read(ProcID, hb_util:human_id(hb_ao:get(id, Item1)), Opts),
    ?assert(hb_message:match(Item1, ReadItem1ByID)),
    {ok, ReadItem2ByID} =
        read(ProcID, hb_util:human_id(hb_message:id(Item2, all)), Opts),
    ?assert(hb_message:match(Item2, ReadItem2ByID)).

%% @doc Test for retrieving the latest computed output for a process.
find_latest_outputs(Opts) ->
    % Create test environment.
    Store = hb_opts:get(<<"store">>, no_viable_store, Opts),
    ResetRes = hb_store:reset(Store),
    ?event({reset_store, {result, ResetRes}, {store, Store}}),
    Proc1 = hb_process_test_vectors:aos_process(),
    ProcID = hb_util:human_id(hb_ao:get(id, Proc1, Opts)),
    % Create messages for the slots, with only the middle slot having a
    % `/Process' field, while the top slot has a `/Deep/Process' field.
    Msg0 = #{ <<"Results">> => #{ <<"Result-Number">> => 0 } },
    Base =
        #{ 
            <<"Results">> => #{ <<"Result-Number">> => 1 }, 
            <<"Process">> => Proc1 
        },
    Req =
        #{ 
            <<"Results">> => #{ <<"Result-Number">> => 2 }, 
            <<"Deep">> => #{ <<"Process">> => Proc1 } 
        },
    % Write the messages to the cache.
    {ok, _} = write(ProcID, 0, Msg0, Opts),
    {ok, _} = write(ProcID, 1, Base, Opts),
    {ok, _} = write(ProcID, 2, Req, Opts),
    ?event(wrote_items),
    % Read the messages with various qualifiers.
    {ok, 2, ReadReq} = latest(ProcID, Opts),
    ?event({read_latest, ReadReq}),
    ?assert(hb_message:match(Req, ReadReq)),
    ?event(read_latest_slot_without_qualifiers),
    {ok, 1, ReadBaseRequired} = latest(ProcID, <<"Process">>, Opts),
    ?event({read_latest_with_process, ReadBaseRequired}),
    ?assert(hb_message:match(Base, ReadBaseRequired)),
    ?event(read_latest_slot_with_shallow_key),
    {ok, 2, ReadReqRequired} = latest(ProcID, <<"Deep/Process">>, Opts),
    ?assert(hb_message:match(Req, ReadReqRequired)),
    ?event(read_latest_slot_with_deep_key),
    {ok, 1, ReadBase} = latest(ProcID, [], 1, Opts),
    ?assert(hb_message:match(Base, ReadBase)).

%% @doc Deltas remain addressable by slot and logical state ID, reconstruct
%% historical state in order, and become full checkpoints at the configured
%% interval.
delta_roundtrip(RawOpts) ->
    Opts = RawOpts#{ <<"process-delta-checkpoint-slots">> => 2 },
    ProcID = hb_util:encode(crypto:strong_rand_bytes(32)),
    Results0 = #{ <<"output">> => #{ <<"data">> => <<"0">> } },
    State0 = #{
        <<"at-slot">> => 0,
        <<"count">> => <<"0">>,
        <<"old">> => <<"remove-me">>,
        <<"ledger">> => #{ <<"alice">> => <<"10">> },
        <<"results">> => Results0
    },
    {ok, _} = write(ProcID, 0, with_delta(State0, [], Results0, Opts), Opts),
    Patches1 = [
        #{ <<"path">> => <<"/count">>, <<"value">> => <<"1">> },
        #{ <<"path">> => <<"/ledger/alice">>, <<"value">> => <<"11">> },
        #{ <<"path">> => <<"/old">>, <<"delete">> => true }
    ],
    Results1 = #{ <<"output">> => #{ <<"data">> => <<"1">> } },
    {ok, State1} = hb_process_delta:apply(State0, Patches1, Results1, 1, Opts),
    {ok, _} = write(
        ProcID,
        1,
        with_delta(State1, Patches1, Results1, Opts),
        Opts
    ),
    {ok, RawSlot1} = hb_cache:read(path(ProcID, 1, Opts), Opts),
    ?assertEqual(?DELTA_FORMAT, maps:get(<<"cache-format">>, RawSlot1)),
    {ok, DeltaID1} = hb_cache:write(RawSlot1, Opts),
    {ok, ReadByID1} = read(ProcID, DeltaID1, Opts),
    ?assertEqual(<<"1">>, hb_ao:get(<<"count">>, ReadByID1, Opts)),
    ?assertEqual(<<"11">>, hb_ao:get(<<"ledger/alice">>, ReadByID1, Opts)),
    ?assertEqual(not_found, hb_ao:get(<<"old">>, ReadByID1, not_found, Opts)),
    ?assertEqual(
        <<"1">>,
        hb_ao:get(<<"results/output/data">>, ReadByID1, Opts)
    ),
    Patches2 = [
        #{ <<"path">> => <<"/count">>, <<"value">> => <<"2">> }
    ],
    Results2 = #{ <<"output">> => #{ <<"data">> => <<"2">> } },
    {ok, State2} = hb_process_delta:apply(State1, Patches2, Results2, 2, Opts),
    {ok, _} = write(
        ProcID,
        2,
        with_delta(State2, Patches2, Results2, Opts),
        Opts
    ),
    % Slot 2 is a full checkpoint. Writing it also evicts slot 1 from the
    % one-state hot cache, forcing the historical read through its delta.
    {ok, RawSlot2} = hb_cache:read(path(ProcID, 2, Opts), Opts),
    ?assertEqual(
        not_found,
        hb_ao:get(<<"cache-format">>, RawSlot2, not_found, Opts)
    ),
    {ok, Historical1} = read(ProcID, 1, Opts),
    ?assertEqual(<<"1">>, hb_ao:get(<<"count">>, Historical1, Opts)),
    {ok, 2, Latest} = latest(ProcID, Opts),
    ?assertEqual(<<"2">>, hb_ao:get(<<"count">>, Latest, Opts)).

with_delta(State, Patches, Results, Opts) ->
    hb_private:set(
        State,
        ?DELTA_META,
        #{ <<"patches">> => Patches, <<"results">> => Results },
        Opts
    ).

%%% @doc Persistent cache for `~arweave-scheduler@1.0'. The cache contains one
%%% global chain synchronization record, one materialization record per
%%% process, the inverted target index, and contiguous process assignments.
%%% State records are content-addressed messages whose stable paths link to
%%% their latest value.
-module(dev_arweave_scheduler_cache).
-export([opts/1]).
-export([read_global/1, write_global/2]).
-export([read_process/2, write_process/3, list_processes/1]).
-export([write_header/2, write_execution_header/2, read_header/2]).
-export([write_block/3, read_block/2]).
-export([ensure_target_root/2]).
-export([write_target/4, write_targets/2, read_target/3, list_targets/2]).
-export([write_assignment/2, read_assignment/3]).
-export([assignments_to_bundle/4]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CACHE_PREFIX, <<"~arweave-scheduler@1.0">>).

%% @doc The scheduler's store, and its verified blocks: lib_arweave_scheduler_blocks.
opts(Opts) -> lib_arweave_scheduler_blocks:opts(Opts).

%% @doc Read and write the linked global synchronization record.
read_global(Opts) -> read_state(global_path(), Opts).
write_global(State, Opts) -> write_state(global_path(), State, Opts).

%% @doc Read and write a process's linked materialization record.
read_process(ProcessID, Opts) ->
    read_state(process_path(ProcessID), Opts).
write_process(ProcessID, State, Opts) ->
    write_state(process_path(ProcessID), State, Opts).

%% @doc List processes for which a materialization record exists.
list_processes(RawOpts) ->
    Opts = opts(RawOpts),
    Store = hb_opts:get(store, no_viable_store, Opts),
    case hb_store:list(Store, process_root(), Opts) of
        {ok, ProcessIDs} -> ProcessIDs;
        _ -> []
    end.

%% @doc Cache a signed transaction header under its TXID.
write_header(Header, RawOpts) ->
    hb_cache:write(Header, opts(RawOpts)).

%% @doc Publish a verified L1 process/message header to the ordinary local
%% store chain so process execution can resolve an assignment's body. Scheduler
%% indexes and assignments remain isolated in `arweave-scheduler-store'.
write_execution_header(Header, RawOpts) ->
    ExecutionOpts = RawOpts#{
        <<"store">> => hb_opts:get(
            store,
            no_viable_store,
            hb_store:scope(RawOpts, local)
        )
    },
    hb_cache:write(Header, ExecutionOpts).

%% @doc Read a cached signed transaction header by TXID.
read_header(TXID, RawOpts) ->
    hb_cache:read(TXID, opts(RawOpts)).

write_block(Height, Block, Opts) ->
    lib_arweave_scheduler_blocks:write_block(Height, Block, Opts).

read_block(Height, Opts) ->
    lib_arweave_scheduler_blocks:read_block(Height, Opts).

%% @doc Link an address and ordinate to the cached transaction header's TXID.
write_target(Address, Ordinate, TXID, RawOpts) ->
    write_targets([{Address, Ordinate, TXID}], RawOpts).

%% @doc Link all targets found in one block in a single store operation.
write_targets(Targets, RawOpts) ->
    write_targets_map(
        maps:from_list(
            [
                {
                    target_path(Address, Ordinate),
                    <<"ao:", (hb_util:human_id(TXID))/binary>>
                }
            ||
                {Address, Ordinate, TXID} <- Targets
            ]
        ),
        RawOpts
    ).

write_targets_map(Targets, _RawOpts) when map_size(Targets) =:= 0 -> ok;
write_targets_map(Targets, RawOpts) ->
    Opts = opts(RawOpts),
    Store = hb_opts:get(store, no_viable_store, Opts),
    hb_store:write(Store, Targets, Opts).

%% @doc Read an indexed target's signed TXID and its cached header.
read_target(Address, Ordinate, RawOpts) ->
    Opts = opts(RawOpts),
    Store = hb_opts:get(store, no_viable_store, Opts),
    case hb_store:read(Store, target_path(Address, Ordinate), Opts) of
        {ok, <<"ao:", TXID/binary>>} ->
            case hb_cache:read(TXID, Opts) of
                {ok, Header} -> {ok, TXID, Header};
                {error, not_found} -> not_found
            end;
        _ -> not_found
    end.

%% @doc Ensure that listing an address can distinguish no targets from failure.
%% We write an empty message at the address's path.
ensure_target_root(Address, RawOpts) ->
    Opts = opts(RawOpts),
    hb_store:write(
        hb_opts:get(store, no_viable_store, Opts),
        #{ hb_path:to_binary([target_root(Address), <<"ao-types">>]) => <<>> },
        Opts
    ).

%% @doc List the ordinates that target an address. Callers parse and sort the
%% numeric components; no store ordering guarantee is assumed.
list_targets(Address, RawOpts) ->
    Opts = opts(RawOpts),
    Store = hb_opts:get(store, no_viable_store, Opts),
    hb_store:list(Store, target_root(Address), Opts).

%% @doc Write a deterministic assignment and link its process slot to it.
write_assignment(Assignment, RawOpts) ->
    Opts = opts(RawOpts),
    ProcessID = hb_maps:get(<<"process">>, Assignment, not_found, Opts),
    Slot = hb_maps:get(<<"slot">>, Assignment, not_found, Opts),
    case hb_cache:write(Assignment, Opts) of
        {ok, _} ->
            hb_cache:link(
                hb_message:id(Assignment, signed, Opts),
                assignment_path(ProcessID, Slot),
                Opts
            );
        Error -> Error
    end.

%% @doc Read an assignment by its contiguous process slot.
read_assignment(ProcessID, Slot, RawOpts) ->
    Opts = opts(RawOpts),
    case hb_cache:read(assignment_path(ProcessID, Slot), Opts) of
        {ok, RawAssignment} ->
            Assignment =
                hb_message:normalize_commitments(RawAssignment, Opts),
            % The signed header is available locally. Its tag values may
            % name unavailable objects; loading them is an execution policy,
            % not a prerequisite for reading the canonical assignment.
            {ok, Assignment#{<<"body">> => hb_maps:get(
                <<"body">>, Assignment, not_found,
                lib_process:execution_opts(Opts))}};
        {error, not_found} -> not_found
    end.

assignment_read_does_not_fetch_payload_links_test() ->
    Opts = #{<<"store">> => [hb_test_utils:test_store()], <<"priv-wallet">> => ar_wallet:new()},
    ProcessID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Missing = <<"ao:", (hb_util:human_id(crypto:strong_rand_bytes(32)))/binary>>,
    Header = hb_message:commit(#{<<"target">> => ProcessID,
        <<"unused">> => {link, Missing, #{}}}, Opts, <<"tx@1.0">>),
    {ok, _} = hb_cache:write(Header, Opts),
    ok = write_assignment(#{<<"process">> => ProcessID, <<"slot">> => 1,
        <<"type">> => <<"Assignment">>, <<"body">> => Header}, Opts),
    {ok, Assignment} = read_assignment(ProcessID, 1, Opts),
    Body = hb_cache:ensure_loaded(maps:get(<<"body">>, Assignment), Opts),
    ?assertMatch({link, Missing, _}, maps:get(<<"unused">>, Body)).

%% @doc Format a schedule response without claiming that one tip hash or
%% timestamp sequenced every historical assignment in the response.
assignments_to_bundle(ProcessID, Assignments, More, RawOpts) ->
    Opts = RawOpts#{
        <<"hashpath">> => ignore,
        <<"cache-control">> => [<<"no-cache">>, <<"no-store">>],
        <<"await-inprogress">> => false
    },
    {ok,
        #{
            <<"type">> => <<"schedule">>,
            <<"process">> => hb_util:human_id(ProcessID),
            <<"continues">> => hb_util:atom(More),
            <<"assignments">> =>
                hb_message:normalize_commitments(
                    hb_maps:from_list(
                        [
                            {
                                hb_maps:get(
                                    <<"slot">>, Assignment, not_found, Opts
                                ),
                                Assignment
                            }
                        ||
                            Assignment <- Assignments
                        ]
                    ),
                    Opts
                )
        }
    }.

read_state(Path, RawOpts) ->
    Opts = opts(RawOpts),
    case hb_cache:read(Path, Opts) of
        {ok, State} -> {ok, hb_cache:ensure_all_loaded(State, Opts)};
        {error, not_found} -> not_found;
        Error -> Error
    end.

write_state(Path, State, RawOpts) ->
    Opts = opts(RawOpts),
    case hb_cache:write(State, Opts) of
        {ok, StateID} ->
            case hb_cache:link(StateID, Path, Opts) of
                ok -> {ok, StateID};
                Error -> Error
            end;
        Error -> Error
    end.

global_path() ->
    hb_path:to_binary([?CACHE_PREFIX, <<"sync">>, <<"global">>]).

process_root() ->
    hb_path:to_binary([?CACHE_PREFIX, <<"sync">>, <<"process">>]).

process_path(ProcessID) ->
    hb_path:to_binary([process_root(), hb_util:human_id(ProcessID)]).

target_root(Address) ->
    hb_path:to_binary(
        [?CACHE_PREFIX, <<"targets">>, hb_util:human_id(Address)]
    ).

target_path(Address, Ordinate) ->
    hb_path:to_binary([target_root(Address), Ordinate]).

assignment_path(ProcessID, Slot) ->
    hb_path:to_binary(
        [
            ?CACHE_PREFIX,
            <<"assignments">>,
            hb_util:human_id(ProcessID),
            hb_ao:normalize_key(Slot)
        ]
    ).

%%% Tests

opts_prefer_arweave_scheduler_store_test() ->
    Main = hb_test_utils:test_store(hb_store_volatile, <<"main">>),
    Generic = hb_test_utils:test_store(hb_store_volatile, <<"generic">>),
    Arweave = hb_test_utils:test_store(hb_store_volatile, <<"arweave">>),
    Selected = opts(#{
        <<"store">> => [Main],
        <<"scheduler-store">> => [Generic],
        <<"arweave-scheduler-store">> => [Arweave]
    }),
    ?assertEqual([Arweave], maps:get(<<"store">>, Selected)),
    ?assertEqual([Generic], maps:get(<<"store">>, opts(#{
        <<"store">> => [Main], <<"scheduler-store">> => [Generic]
    }))).

linked_spawn_uses_process_store_test() ->
    Source = hb_test_utils:test_store(hb_store_volatile, <<"linked-source">>),
    Target =
        (hb_test_utils:test_store(hb_store_volatile, <<"linked-target">>))#{
            <<"scope">> => remote
        },
    Scheduler = hb_test_utils:test_store(hb_store_volatile, <<"linked-scheduler">>),
    Process = hb_test_utils:test_store(hb_store_volatile, <<"linked-process">>),
    lists:foreach(fun hb_store:start/1, [Source, Target, Scheduler, Process]),
    Opts =
        #{
            <<"store">> => [Source, Target],
            <<"scheduler-store">> => [Scheduler],
            <<"process-store">> => [Process]
        },
    CacheOpts = lib_process:cache_opts(Opts),
    Balances = #{ <<"alice">> => <<"100">> },
    {ok, BalancesID} = hb_cache:write(Balances, Opts#{ <<"store">> => [Target] }),
    Spawn = #{ <<"balances">> => {link, <<"ao:", BalancesID/binary>>, #{}} },
    {ok, ProcessID} = hb_cache:write(Spawn, Opts#{ <<"store">> => [Source] }),
    {ok, LazySpawn} = hb_cache:read(ProcessID, Opts#{ <<"store">> => [Source] }),
    ?assertEqual(
        ProcessID,
        hb_message:id(LazySpawn, all, Opts#{ <<"store">> => [Target] })
    ),
    {ok, ProcessID} = hb_cache:write(LazySpawn, CacheOpts),
    ok =
        write_assignment(
            #{
                <<"process">> => ProcessID,
                <<"slot">> => 0,
                <<"body">> => {link, <<"ao:", ProcessID/binary>>, #{}}
            },
            Opts
        ),
    hb_store:stop(Source),
    {ok, Assignment} = read_assignment(ProcessID, 0, Opts),
    ReadSpawn = maps:get(<<"body">>, Assignment),
    ReadBalances =
        hb_maps:get(
            <<"balances">>,
            ReadSpawn,
            not_found,
            lib_process:execution_opts(Opts)
        ),
    ?assertEqual(
        <<"100">>,
        hb_maps:get(
            <<"alice">>,
            ReadBalances,
            not_found,
            lib_process:execution_opts(Opts)
        )
    ),
    lists:foreach(fun hb_store:stop/1, [Target, Scheduler, Process]).

linked_state_and_target_test() ->
    Store = hb_test_utils:test_store(hb_store_volatile, <<"ar-sched-cache">>),
    ok = hb_store:start(Store),
    Opts = #{ <<"store">> => [Store], <<"priv-wallet">> => ar_wallet:new() },
    ProcessID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Header =
        hb_message:commit(
            #{ <<"target">> => ProcessID },
            Opts,
            #{ <<"commitment-device">> => <<"tx@1.0">> }
        ),
    TXID = hb_util:human_id(hb_message:id(Header, signed, Opts)),
    Global =
        #{ <<"from">> => 100, <<"to">> => 101, <<"block-hash">> => TXID },
    {ok, _} = write_global(Global, Opts),
    ?assertEqual({ok, Global}, read_global(Opts)),
    Process =
        #{
            <<"process">> => ProcessID,
            <<"spawn-ordinate">> => <<"100-2">>,
            <<"synced-to">> => 101,
            <<"next-slot">> => 1
        },
    {ok, _} = write_process(ProcessID, Process, Opts),
    ?assertEqual({ok, Process}, read_process(ProcessID, Opts)),
    ?assertEqual([ProcessID], list_processes(Opts)),
    {ok, _} = write_header(Header, Opts),
    ok = write_target(ProcessID, <<"101-3">>, TXID, Opts),
    ?assertEqual({ok, [<<"101-3">>]}, list_targets(ProcessID, Opts)),
    EmptyStore = hb_test_utils:test_store(hb_store_volatile, <<"empty">>),
    ok = hb_store:start(EmptyStore),
    % With no scheduler store, a header that is only in a remote store is
    % not read.
    ?assertEqual(
        {error, not_found},
        read_header(
            TXID,
            #{ <<"store">> => [EmptyStore, Store#{ <<"scope">> => remote }] }
        )
    ),
    ReadOpts = Opts#{ <<"scheduler-store">> => [EmptyStore, Store] },
    {ok, ReadTXID, ReadHeader} = read_target(ProcessID, <<"101-3">>, ReadOpts),
    ?assertEqual(TXID, ReadTXID),
    ?assertEqual(
        TXID,
        hb_util:human_id(hb_message:id(ReadHeader, signed, Opts))
    ),
    ok = hb_store:stop(EmptyStore),
    ok = hb_store:stop(Store).

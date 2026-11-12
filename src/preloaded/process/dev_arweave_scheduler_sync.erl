%%% @doc Global Arweave synchronization for `~arweave-scheduler@1.0'. A
%%% single serialized pass walks each confirmed block once and builds the
%%% inverted `targets/<address>/<ordinate> -> TXID' index used by every
%%% process schedule.
%%%
%%% The gateway is transport; nothing it says is taken on trust. A block is
%%% indexed only when its fields recompute its `indep_hash' (which commits its
%%% height, its previous block and its transaction list in order), it extends
%%% the last indexed block, `arweave-scheduler-confirmation-depth' blocks that
%%% pass the same checks extend it in turn, and the signed headers of its
%%% transactions recompute its `tx_root'. Anything else stops the sync.
-module(dev_arweave_scheduler_sync).
-export([sync/1, process/2, from_height/1, confirmed_tip/1]).
-export([fetch_block/2, fetch_header/2, spawn_ordinate/2]).
-export([targets/1, ordinate/2, parse_ordinate/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ARWEAVE_DEVICE, <<"~arweave@2.9">>).
-define(DEFAULT_CONFIRMATION_DEPTH, 2).
-define(DEFAULT_FROM_HEIGHT, 1968888).
-define(DEFAULT_BLOCK_BATCH, 8).
-define(DEFAULT_BLOCK_WORKERS, 4).
-define(DEFAULT_HEADER_WORKERS, 32).
-define(DEFAULT_FETCH_ATTEMPTS, 20).
-define(DEFAULT_FETCH_RETRY_MS, 250).
-define(DEFAULT_SYNC_INTERVAL_MS, 1000).
-define(PROCESS_IDLE_MS, 1000).

%% @doc Bring the global target index to the confirmed chain frontier. The
%% pass is serialized per scheduler store, and repeated callers within
%% the short check interval reuse the completed global record without another
%% network-tip request.
sync(Opts) ->
    Store = scheduler_store(Opts),
    exclusive(
        {?MODULE, sync, Store},
        fun(Cached) -> sync_request(Cached, Opts) end
    ).

%% @doc Synchronize the global index and materialize one process's relevant
%% entries into contiguous AO slots. Each process is serialized independently;
%% the completed global record bounds entries visible during materialization.
process(ProcessID, Opts) ->
    case canonical_process_id(ProcessID) of
        {ok, HumanProcessID} ->
            Store = scheduler_store(Opts),
            maybe
                {ok, Global} ?= sync(Opts),
                exclusive(
                    {?MODULE, process, Store, HumanProcessID},
                    fun(_State) ->
                        {
                            materialize_process(HumanProcessID, Global, Opts),
                            undefined
                        }
                    end,
                    ?PROCESS_IDLE_MS
                )
            end;
        error ->
            {error,
                #{
                    <<"status">> => 422,
                    <<"reason">> =>
                        <<"Arweave process ID must be a canonical TXID.">>
                }
            }
    end.

sync_request(Cached, Opts) ->
    case recent_state(Cached, Opts) of
        {ok, _} = Recent -> {Recent, Cached};
        stale ->
            Result =
                do_sync(
                    dev_arweave_scheduler_cache:read_global(Opts),
                    Opts
                ),
            case Result of
                {ok, State} ->
                    {
                        Result,
                        {erlang:monotonic_time(millisecond), State}
                    };
                _ -> {Result, undefined}
            end
    end.

recent_state(Cached, Opts) ->
    Interval =
        hb_util:int(
            hb_opts:get(
                arweave_scheduler_sync_interval,
                ?DEFAULT_SYNC_INTERVAL_MS,
                Opts
            )
        ),
    Now = erlang:monotonic_time(millisecond),
    ConfiguredFrom = from_height(Opts),
    case Cached of
        {CheckedAt, State}
                when Interval > 0, Now - CheckedAt < Interval ->
            case hb_util:int(hb_maps:get(<<"from">>, State, -1, #{})) of
                ConfiguredFrom -> {ok, State};
                _ -> stale
            end;
        _ -> stale
    end.

do_sync(StateResult, Opts) ->
    From = from_height(Opts),
    maybe
        {ok, Upper} ?= confirmed_tip(Opts),
        {ok, State} ?= initial_state(StateResult, From),
        sync_blocks(State, Upper, Opts)
    end.

initial_state(not_found, From) ->
    {ok, #{ <<"from">> => From, <<"to">> => From - 1 }};
initial_state({ok, State}, From) ->
    case hb_util:int(hb_maps:get(<<"from">>, State, -1, #{})) of
        From -> {ok, State};
        StoredFrom ->
            {error,
                #{
                    <<"status">> => 409,
                    <<"reason">> =>
                        <<"Configured scheduler rollout height does not match ",
                            "the existing global synchronization record.">>,
                    <<"configured-from">> => From,
                    <<"stored-from">> => StoredFrom
                }
            }
    end;
initial_state({error, not_found}, From) -> initial_state(not_found, From);
initial_state(Error, _From) -> Error.

sync_blocks(State = #{ <<"to">> := To }, Upper, _Opts) when To >= Upper ->
    {ok, State};
sync_blocks(State = #{ <<"to">> := To }, Upper, Opts) ->
    BatchEnd =
        min(
            Upper,
            To +
                max(
                    1,
                    hb_util:int(
                        hb_opts:get(
                            arweave_scheduler_block_batch,
                            ?DEFAULT_BLOCK_BATCH,
                            Opts
                        )
                    )
                )
        ),
    % The batch and the descendants that confirm it, verified as one chain;
    % only the batch is indexed.
    Heights = lists:seq(To + 1, BatchEnd + confirmation_depth(Opts)),
    maybe
        {ok, Chain} ?= fetch_blocks(Heights, Opts),
        {ok, _} ?= validate_blocks(Chain, State, Opts),
        {Blocks, _Descendants} = lists:split(BatchEnd - To, Chain),
        {ok, NewState} ?= validate_blocks(Blocks, State, Opts),
        ok ?= index_blocks(Blocks, Opts),
        ok ?= write_blocks(Blocks, Opts),
        {ok, _} ?=
            dev_arweave_scheduler_cache:write_global(NewState, Opts),
        sync_blocks(NewState, Upper, Opts)
    end.

%% @doc Fetch one bounded batch of canonical blocks concurrently.
fetch_blocks(Heights, Opts) ->
    Results =
        hb_pmap:parallel_map(
            Heights,
            fun(Height) -> fetch_block(Height, Opts) end,
            max(
                1,
                hb_util:int(
                    hb_opts:get(
                        arweave_scheduler_block_workers,
                        ?DEFAULT_BLOCK_WORKERS,
                        Opts
                    )
                )
            )
        ),
    collect_blocks(Heights, Results, []).

%% @doc Pair fetched blocks with their requested heights or surface an error.
collect_blocks([], [], Blocks) ->
    {ok, lists:reverse(Blocks)};
collect_blocks([Height | Heights], [{ok, Block} | Results], Blocks) ->
    collect_blocks(Heights, Results, [{Height, Block} | Blocks]);
collect_blocks(_Heights, [Error | _], _Blocks) ->
    Error.

%% @doc Validate an entire batch's height and previous-hash chain in order.
validate_blocks([], State, _Opts) ->
    {ok, State};
validate_blocks([{Height, Block} | Blocks], State, Opts) ->
    maybe
        ok ?= validate_block(Height, State, Block, Opts),
        BlockHash = hb_maps:get(<<"indep-hash">>, Block, not_found, Opts),
        true ?= is_binary(BlockHash),
        validate_blocks(
            Blocks,
            State#{
                <<"to">> => Height,
                <<"block-hash">> => BlockHash
            },
            Opts
        )
    else
        false ->
            {error,
                #{
                    <<"status">> => 502,
                    <<"reason">> => <<"Arweave block has no canonical hash.">>,
                    <<"block-height">> => Height
                }
            };
        Error -> Error
    end.

%% @doc Cache each validated block after its transaction headers are indexed.
write_blocks([], _Opts) ->
    ok;
write_blocks([{Height, Block} | Blocks], Opts) ->
    case dev_arweave_scheduler_cache:write_block(Height, Block, Opts) of
        {ok, _} -> write_blocks(Blocks, Opts);
        Error -> Error
    end.

%% @doc Index a batch through one global header-worker limit before one
%% frontier commit makes the complete batch visible to materialization.
index_blocks(Blocks, Opts) ->
    Transactions =
        lists:append(
            [
                block_transactions(Height, Block, Opts)
            ||
                {Height, Block} <- Blocks
            ]
        ),
    Indexed =
        hb_pmap:parallel_map(
            Transactions,
            fun({Height, Index, TXID}) ->
                index_transaction(Height, Index, TXID, Opts)
            end,
            max(
                1,
                hb_util:int(
                    hb_opts:get(
                        arweave_scheduler_header_workers,
                        ?DEFAULT_HEADER_WORKERS,
                        Opts
                    )
                )
            )
        ),
    maybe
        {ok, Entries} ?= collect_indexed(Indexed, []),
        ok ?= verify_tx_roots(Blocks, Entries, Opts),
        dev_arweave_scheduler_cache:write_targets(
            lists:append([Targets || {_Height, _TX, Targets} <- Entries]),
            Opts
        )
    end.

%% @doc Each block's transactions, as their signed headers say, recompute the
%% block's `tx_root'.
verify_tx_roots(Blocks, Entries, Opts) ->
    Mismatched =
        [
            Height
        ||
            {Height, Block} <- Blocks,
            ar_block:tx_root(
                [TX || {TXHeight, TX, _} <- Entries, TXHeight =:= Height],
                Height
            ) =/= hb_util:decode(hb_maps:get(<<"tx-root">>, Block, <<>>, Opts))
        ],
    case Mismatched of
        [] -> ok;
        [Height | _] ->
            {error,
                #{
                    <<"status">> => 502,
                    <<"reason">> =>
                        <<"Arweave block transactions do not recompute its ",
                            "tx_root.">>,
                    <<"block-height">> => Height
                }
            }
    end.

%% @doc Enumerate one block's transactions with their lossless ordinates.
block_transactions(Height, Block, Opts) ->
    TXIDs =
        [
            hb_util:human_id(TXID)
        ||
            TXID <- hb_maps:get(<<"txs">>, Block, [], Opts)
        ],
    [
        {Height, Index, TXID}
    ||
        {Index, TXID} <-
            lists:zip(lists:seq(0, length(TXIDs) - 1), TXIDs)
    ].

materialize_process(ProcessID, Global, Opts) ->
    maybe
        {ok, State} ?= ensure_process(ProcessID, Global, Opts),
        materialize_targets(ProcessID, State, Global, Opts)
    end.

ensure_process(ProcessID, Global, Opts) ->
    case dev_arweave_scheduler_cache:read_process(ProcessID, Opts) of
        {ok, State} -> {ok, State};
        not_found -> initialize_process(ProcessID, Global, Opts);
        {error, not_found} -> initialize_process(ProcessID, Global, Opts);
        Error -> Error
    end.

initialize_process(
        ProcessID,
        Global = #{ <<"from">> := From, <<"to">> := To },
        Opts
    ) ->
    maybe
        {ok, Height, Index, SpawnOrdinate} ?= spawn_ordinate(ProcessID, Opts),
        ok ?= require_covered_spawn(ProcessID, Height, From, To),
        {ok, Header, #tx{}} ?= fetch_header(ProcessID, Opts),
        {ok, _} ?=
            dev_arweave_scheduler_cache:write_header(Header, Opts),
        {ok, _} ?=
            dev_arweave_scheduler_cache:write_execution_header(Header, Opts),
        ok ?=
            write_assignment(
                ProcessID,
                0,
                Height,
                Index,
                ProcessID,
                Opts
            ),
        State =
            #{
                <<"process">> => ProcessID,
                <<"spawn-ordinate">> => SpawnOrdinate,
                <<"synced-to">> => Height - 1,
                <<"next-slot">> => 1
            },
        {ok, _} ?=
            dev_arweave_scheduler_cache:write_process(ProcessID, State, Opts),
        {ok, State}
    else
        Error -> Error
    end.

require_covered_spawn(_ProcessID, Height, From, _To) when Height < From ->
    {error,
        #{
            <<"status">> => 409,
            <<"reason">> =>
                <<"Process predates the Arweave scheduler rollout height.">>,
            <<"spawn-height">> => Height,
            <<"indexed-from">> => From
        }
    };
require_covered_spawn(ProcessID, Height, _From, To) when Height > To ->
    {error,
        #{
            <<"status">> => 425,
            <<"reason">> =>
                <<"Process spawn block is not yet in the confirmed index.">>,
            <<"process">> => ProcessID,
            <<"spawn-height">> => Height,
            <<"indexed-to">> => To
        }
    };
require_covered_spawn(_ProcessID, _Height, _From, _To) -> ok.

materialize_targets(
        _ProcessID,
        State = #{ <<"synced-to">> := SyncedTo },
        #{ <<"to">> := GlobalTo },
        _Opts
    ) when SyncedTo >= GlobalTo ->
    {ok, State};
materialize_targets(ProcessID, State, Global, Opts) ->
    maybe
        ok ?=
            dev_arweave_scheduler_cache:ensure_target_root(ProcessID, Opts),
        {ok, Ordinates} ?=
            dev_arweave_scheduler_cache:list_targets(ProcessID, Opts),
        TargetOrdinates =
            lists:filtermap(
                fun(Ordinate) ->
                    case parse_ordinate(Ordinate) of
                        {ok, Position} -> {true, {Position, Ordinate}};
                        error -> false
                    end
                end,
                Ordinates
            ),
        Selected = select_targets(lists:sort(TargetOrdinates), State, Global),
        write_process_assignments(ProcessID, Selected, State, Global, Opts)
    end.

select_targets(
        Targets,
        #{
            <<"spawn-ordinate">> := SpawnOrdinate,
            <<"synced-to">> := SyncedTo
        },
        #{ <<"to">> := GlobalTo }
    ) ->
    {ok, SpawnPosition} = parse_ordinate(SpawnOrdinate),
    [
        {Position, Ordinate}
    ||
        {Position = {Height, _Index}, Ordinate} <- Targets,
        Position > SpawnPosition,
        Height > SyncedTo,
        Height =< GlobalTo
    ].

write_process_assignments(
        ProcessID,
        Targets,
        State = #{ <<"next-slot">> := NextSlot },
        Global,
        Opts
    ) ->
    case write_process_assignments(ProcessID, Targets, NextSlot, Opts) of
        {ok, NewNextSlot} ->
            compute_on_schedule(ProcessID, NextSlot, NewNextSlot, Opts),
            NewState =
                State#{
                    <<"synced-to">> =>
                        hb_maps:get(<<"to">>, Global, not_found, Opts),
                    <<"next-slot">> => NewNextSlot
                },
            case dev_arweave_scheduler_cache:write_process(
                ProcessID,
                NewState,
                Opts
            ) of
                {ok, _} -> {ok, NewState};
                Error -> Error
            end;
        Error -> Error
    end.

%% @doc A slot this node learns is computed promptly, as `scheduler@1.0'
%% computes the slots it assigns (`dev_scheduler_server:compute_on_schedule').
%% An Arweave schedule is pulled, not pushed: a sync writes new slots when
%% somebody asks, and nothing then computed them, so `/now' stayed at the old
%% slot. The PUSD bridge waited on exactly that: every attestor had signed a
%% deposit, PUSD held the three messages at slots 17-19, and the credit never
%% appeared until a compute was forced by hand (2026-11-04). One compute to
%% the newest slot, through the process's worker; it discards the state, so
%% it neither names nor stores it. `process-compute-on-schedule' false turns
%% it off, as for `scheduler@1.0'.
compute_on_schedule(_ProcessID, From, To, _Opts) when To =< From -> ok;
compute_on_schedule(ProcessID, _From, To, Opts) ->
    case hb_util:atom(hb_opts:get(<<"process-compute-on-schedule">>, true, Opts)) of
        false -> ok;
        _ ->
            spawn(fun() ->
                try
                    {ok, Process} = hb_cache:read(ProcessID, Opts),
                    hb_ao:resolve(
                        Process,
                        #{ <<"path">> => <<"compute">>, <<"slot">> => To - 1 },
                        Opts#{
                            <<"spawn-worker">> => true,
                            <<"hashpath">> => ignore,
                            <<"cache-control">> => [<<"no-store">>]
                        }
                    )
                catch Class:Reason ->
                    ?event(warning, {arweave_compute_on_schedule_failed,
                        {process, ProcessID}, {slot, To - 1},
                        {class, Class}, {reason, Reason}})
                end
            end),
            ok
    end.

write_process_assignments(_ProcessID, [], Slot, _Opts) -> {ok, Slot};
write_process_assignments(
        ProcessID,
        [{{Height, Index}, Ordinate} | Rest],
        Slot,
        Opts
    ) ->
    case dev_arweave_scheduler_cache:read_target(ProcessID, Ordinate, Opts) of
        {ok, TXID, _Header} ->
            case write_assignment(ProcessID, Slot, Height, Index, TXID, Opts) of
                ok ->
                    write_process_assignments(
                        ProcessID,
                        Rest,
                        Slot + 1,
                        Opts
                    );
                Error -> Error
            end;
        not_found ->
            {error,
                #{
                    <<"status">> => 500,
                    <<"reason">> => <<"Indexed scheduler target is missing.">>,
                    <<"ordinate">> => Ordinate
                }
            }
    end.

write_assignment(ProcessID, Slot, Height, Index, TXID, Opts) ->
    maybe
        {ok, BlockHash, Timestamp} ?= assignment_provenance(Height, Opts),
        Body = {link, <<"ao:", TXID/binary>>, #{}},
        Assignment =
            #{
                <<"path">> => <<"compute">>,
                <<"data-protocol">> => <<"ao">>,
                <<"variant">> => <<"ao.N.1">>,
                <<"process">> => ProcessID,
                <<"epoch">> => <<"0">>,
                <<"slot">> => Slot,
                <<"block-height">> => Height,
                <<"block-index">> => Index,
                <<"block-hash">> => BlockHash,
                <<"timestamp">> => Timestamp,
                <<"body">> => Body,
                <<"type">> => <<"Assignment">>
            },
        dev_arweave_scheduler_cache:write_assignment(Assignment, Opts)
    end.

%% @doc Bind every assignment to the exact cached canonical block used to
%% order it. Missing or malformed provenance fails closed rather than
%% materializing a slot that independent nodes cannot later audit.
assignment_provenance(Height, Opts) ->
    case dev_arweave_scheduler_cache:read_block(Height, Opts) of
        {ok, Block} ->
            try
                NativeHash = hb_maps:get(<<"indep-hash">>, Block, not_found, Opts),
                Timestamp = hb_util:int(
                    hb_maps:get(<<"timestamp">>, Block, not_found, Opts)
                ),
                true = is_binary(NativeHash),
                true = Timestamp >= 0,
                {ok, canonical_block_hash(NativeHash), Timestamp}
            catch
                _:_ -> assignment_provenance_error(Height)
            end;
        _ -> assignment_provenance_error(Height)
    end.

assignment_provenance_error(Height) ->
    {error,
        #{
            <<"status">> => 500,
            <<"reason">> =>
                <<"Canonical block provenance is unavailable for assignment.">>,
            <<"block-height">> => Height
        }
    }.

canonical_block_hash(NativeHash) when byte_size(NativeHash) =:= 48 ->
    hb_util:encode(NativeHash);
canonical_block_hash(EncodedHash) when byte_size(EncodedHash) =:= 64 ->
    NativeHash = hb_util:decode(EncodedHash),
    48 = byte_size(NativeHash),
    EncodedHash = hb_util:encode(NativeHash),
    EncodedHash.

assignment_provenance_missing_test() ->
    Store = hb_test_utils:test_store(hb_store_volatile, <<"ar-sched-provenance">>),
    ok = hb_store:start(Store),
    Opts = #{ <<"store">> => [Store], <<"priv-wallet">> => ar_wallet:new() },
    ?assertMatch(
        {error, #{ <<"block-height">> := 42 }},
        assignment_provenance(42, Opts)
    ),
    {ok, _} = dev_arweave_scheduler_cache:write_block(
        42,
        #{ <<"indep-hash">> => crypto:strong_rand_bytes(48) },
        Opts
    ),
    ?assertMatch(
        {error, #{ <<"block-height">> := 42 }},
        assignment_provenance(42, Opts)
    ),
    ok = hb_store:stop(Store).

%% @doc Fetch and route one transaction at its canonical block ordinate. Every
%% transaction's header is needed: the block's `tx_root' is recomputed from
%% all of them.
index_transaction(Height, Index, TXID, Opts) ->
    case fetch_header(TXID, Opts) of
        {ok, Header, TX} ->
            Ordinate = ordinate(Height, Index),
            Targets = transaction_targets(TX),
            case Targets of
                [] -> {ok, Height, TX, []};
                _ ->
                    case dev_arweave_scheduler_cache:write_execution_header(
                            Header, Opts) of
                        {ok, _} ->
                            {ok, Height, TX,
                                [
                                    {Address, Ordinate, TXID}
                                ||
                                    Address <- Targets
                                ]
                            };
                        Error -> Error
                    end
            end;
        Error -> Error
    end.

transaction_targets(TX = #tx{ data_size = 0 }) -> targets(TX);
transaction_targets(#tx{}) -> [].

collect_indexed([], Entries) -> {ok, lists:reverse(Entries)};
collect_indexed([{ok, Height, TX, Targets} | Rest], Entries) ->
    collect_indexed(Rest, [{Height, TX, Targets} | Entries]);
collect_indexed([Error | _], _Entries) -> Error.

%% @doc The fixed protocol rollout height. Nodes may override it explicitly,
%% but a persisted index refuses to continue if that value later changes.
from_height(Opts) ->
    hb_util:int(
        hb_opts:get(arweave_scheduler_from, ?DEFAULT_FROM_HEIGHT, Opts)
    ).

%% @doc The highest block safe to index. A configured maximum pins the chain
%% frontier and is also useful for deterministic isolated tests.
confirmed_tip(Opts) ->
    maybe
        {ok, Height} ?=
            hb_ao:resolve(
                #{
                    <<"path">> => <<?ARWEAVE_DEVICE/binary, "/current/height">>,
                    <<"include-proofs">> => false
                },
                no_result_cache(fetch_opts(Opts))
            ),
        Confirmed = hb_util:int(Height) - confirmation_depth(Opts),
        case hb_opts:get(arweave_scheduler_max_height, undefined, Opts) of
            undefined -> {ok, Confirmed};
            Max -> {ok, min(Confirmed, hb_util:int(Max))}
        end
    end.

%% @doc The number of verified blocks that must extend a block before it is
%% indexed: 2 is three confirmations.
confirmation_depth(Opts) ->
    hb_util:int(
        hb_opts:get(
            arweave_scheduler_confirmation_depth,
            ?DEFAULT_CONFIRMATION_DEPTH,
            Opts
        )
    ).

%% @doc Read a block this node verified when possible, otherwise fetch and
%% verify it.
fetch_block(Height, Opts) ->
    case dev_arweave_scheduler_cache:read_block(Height, Opts) of
        {ok, Block} -> {ok, Block};
        _ -> fetch_block_remote(Height, Opts)
    end.

fetch_block_remote(Height, Opts) ->
    fetch_block_remote(Height, fetch_attempts(Opts), Opts).

%% @doc Retry transient block transport failures within the worker that owns
%% the fetch, preserving completed work elsewhere in the batch.
fetch_block_remote(Height, Attempts, Opts) ->
    FetchOpts = no_result_cache(fetch_opts(Opts)),
    case hb_ao:resolve(
        #{
            <<"path">> => <<?ARWEAVE_DEVICE/binary, "/block">>,
            <<"block">> => Height,
            <<"include-proofs">> => true,
            <<"cache-control">> => [<<"no-cache">>]
        },
        FetchOpts
    ) of
        {ok, Block} ->
            try verified_block(hb_cache:ensure_all_loaded(Block, FetchOpts), Opts)
            catch
                throw:{necessary_message_not_found, _, _} ->
                    retry_block(Height, Attempts, Opts);
                throw:{could_not_read_lazy_link, _, _, _} ->
                    retry_block(Height, Attempts, Opts)
            end;
        _ when Attempts > 1 ->
            retry_block(Height, Attempts, Opts);
        _ ->
            {error,
                #{
                    <<"status">> => 503,
                    <<"reason">> => <<"Arweave block is not retrievable.">>,
                    <<"block-height">> => Height
                }
            }
    end.

%% @doc A fetched block, kept only if its fields recompute its `indep-hash'.
%% Its proofs are fields of that hash; they are dropped once it is checked.
verified_block(Block, Opts) ->
    Valid =
        try
            ar_block:indep_hash(Block) =:=
                hb_util:decode(maps:get(<<"indep-hash">>, Block))
        catch _:_ -> false
        end,
    case Valid of
        true ->
            {ok,
                hb_maps:without(
                    [<<"poa">>, <<"poa2">>],
                    hb_message:uncommitted(Block, Opts),
                    Opts
                )
            };
        false ->
            {error,
                #{
                    <<"status">> => 502,
                    <<"reason">> =>
                        <<"Arweave block fields do not recompute its ",
                            "indep_hash.">>,
                    <<"block-height">> =>
                        hb_maps:get(<<"height">>, Block, not_found, Opts)
                }
            }
    end.

%% @doc Retry an incomplete or unavailable block after a bounded delay.
retry_block(Height, Attempts, Opts) when Attempts > 1 ->
    timer:sleep(fetch_retry_delay(Opts)),
    fetch_block_remote(Height, Attempts - 1, Opts);
retry_block(Height, _Attempts, _Opts) ->
    {error,
        #{
            <<"status">> => 503,
            <<"reason">> => <<"Arweave block is not retrievable.">>,
            <<"block-height">> => Height
        }
    }.

%% @doc Read a signed transaction header locally when possible, otherwise
%% fetch it without data. The signed commitment must reproduce the requested
%% TXID before it is admitted to the scheduler store.
fetch_header(TXID, Opts) ->
    case dev_arweave_scheduler_cache:read_header(TXID, Opts) of
        {ok, Header} ->
            case validate_header(TXID, Header, Opts) of
                {ok, _, #tx{ data = <<>> }} = Valid -> Valid;
                _ -> fetch_header_remote(TXID, Opts)
            end;
        _ -> fetch_header_remote(TXID, Opts)
    end.

fetch_header_remote(TXID, Opts) ->
    fetch_header_remote(TXID, fetch_attempts(Opts), Opts).

%% @doc Retry transient header transport failures before failing the batch.
fetch_header_remote(TXID, Attempts, Opts) ->
    case hb_ao:resolve(
        <<
            ?ARWEAVE_DEVICE/binary, "/tx&tx=", TXID/binary,
            "&exclude-data=true"
        >>,
        no_result_cache(fetch_opts(Opts))
    ) of
        {ok, Header} ->
            case cache_header(TXID, Header, Opts) of
                {error, _} = Error ->
                    case needs_format_one_data(Header, Opts) of
                        true -> fetch_full_header(TXID, Attempts, Opts);
                        false -> Error
                    end;
                Result -> Result
            end;
        {error, #{ <<"status">> := 422 }} = Error -> Error;
        _ when Attempts > 1 ->
            timer:sleep(fetch_retry_delay(Opts)),
            fetch_header_remote(TXID, Attempts - 1, Opts);
        _ ->
            {error,
                #{
                    <<"status">> => 503,
                    <<"reason">> =>
                        <<"Arweave transaction header is not retrievable.">>,
                    <<"tx">> => TXID
                }
            }
    end.

%% @doc A format-one signature commits the complete data payload, so retrieve
%% it only when a data-bearing format-one header cannot verify without it.
fetch_full_header(TXID, Attempts, Opts) ->
    case hb_ao:resolve(
        <<?ARWEAVE_DEVICE/binary, "/tx&tx=", TXID/binary>>,
        no_result_cache(fetch_opts(Opts))
    ) of
        {ok, Header} -> cache_header(TXID, Header, Opts);
        {error, #{ <<"status">> := 422 }} = Error -> Error;
        _ when Attempts > 1 ->
            timer:sleep(fetch_retry_delay(Opts)),
            fetch_full_header(TXID, Attempts - 1, Opts);
        _ ->
            {error,
                #{
                    <<"status">> => 503,
                    <<"reason">> =>
                        <<"Arweave format-one transaction is not retrievable.">>,
                    <<"tx">> => TXID
                }
            }
    end.

%% @doc Identify headers whose format-one signatures require omitted data.
needs_format_one_data(Header, Opts) ->
    try hb_message:convert(Header, <<"tx@1.0">>, Opts) of
        #tx{ format = 1, data = <<>> } -> true;
        #tx{} -> false
    catch
        _:_ -> false
    end.

%% @doc Return the positive number of attempts allowed per network object.
fetch_attempts(Opts) ->
    max(
        1,
        hb_util:int(
            hb_opts:get(
                arweave_scheduler_fetch_attempts,
                ?DEFAULT_FETCH_ATTEMPTS,
                Opts
            )
        )
    ).

%% @doc Return the non-negative delay between network fetch attempts.
fetch_retry_delay(Opts) ->
    max(
        0,
        hb_util:int(
            hb_opts:get(
                arweave_scheduler_fetch_retry,
                ?DEFAULT_FETCH_RETRY_MS,
                Opts
            )
        )
    ).

%% @doc Every Arweave read of the scheduler goes to the node's `gateway'
%% (HTTPS in our nodes' config). What it returns is verified, not trusted.
fetch_opts(Opts) ->
    Gateway = hb_opts:get(gateway, <<"https://arweave.net">>, Opts),
    Opts#{
        <<"arweave-index-blocks">> => false,
        <<"routes">> =>
            [
                #{
                    <<"template">> => <<"^/arweave">>,
                    <<"node">> =>
                        #{
                            <<"match">> => <<"^/arweave">>,
                            <<"with">> => Gateway
                        }
                }
            ]
    }.

cache_header(TXID, Header, Opts) ->
    case validate_header(TXID, Header, Opts) of
        {ok, Header, TX = #tx{ data_size = 0 }} ->
            case dev_arweave_scheduler_cache:write_header(Header, Opts) of
                {ok, _} -> {ok, Header, TX};
                Error -> Error
            end;
        {ok, Header, TX} -> {ok, Header, TX};
        Error -> Error
    end.

validate_header(TXID, Header, Opts) ->
    try
        TX = hb_message:convert(Header, <<"tx@1.0">>, Opts),
        HeaderTXID = hb_util:human_id(TX#tx.id),
        case
            HeaderTXID =:= TXID andalso
                ar_tx:verify_tx_id(hb_util:native_id(TXID), TX)
        of
            true -> {ok, Header, TX};
            _ ->
                {error,
                    #{
                        <<"status">> => 502,
                        <<"reason">> =>
                            <<"Arweave header signature or TXID is invalid.">>,
                        <<"expected">> => TXID,
                        <<"actual">> => HeaderTXID
                    }
                }
        end
    catch
        _:_ ->
            {error,
                #{
                    <<"status">> => 502,
                    <<"reason">> => <<"Invalid Arweave transaction header.">>,
                    <<"tx">> => TXID
                }
            }
    end.

%% @doc Return the native recipient plus a comma-separated `Assign-To' list.
%% Invalid members and duplicate tag instances contribute no instructed
%% targets; the complete result is deduplicated.
targets(TX) ->
    Native =
        case TX#tx.target of
            Target when is_binary(Target), byte_size(Target) =:= 32 ->
                [hb_util:human_id(Target)];
            _ -> []
        end,
    AssignValues =
        [
            Value
        ||
            {Key, Value} <- TX#tx.tags,
            hb_util_string:lowercase(Key) =:= <<"assign-to">>
        ],
    Assigned =
        case AssignValues of
            [Value] when is_binary(Value) ->
                lists:filtermap(
                    fun canonical_address/1,
                    binary:split(Value, <<",">>, [global])
                );
            _ -> []
        end,
    lists:usort(Native ++ Assigned).

canonical_address(Raw) ->
    Address = trim_ascii(Raw),
    try
        Native = hb_util:native_id(Address),
        case byte_size(Native) =:= 32 andalso hb_util:human_id(Native) =:= Address of
            true -> {true, Address};
            false -> false
        end
    catch
        _:_ -> false
    end.

canonical_process_id(ProcessID) ->
    try canonical_address(hb_util:human_id(ProcessID)) of
        {true, HumanProcessID} -> {ok, HumanProcessID};
        false -> error
    catch
        _:_ -> error
    end.

trim_ascii(<<C, Rest/binary>>)
        when C =:= $\s; C =:= $\t; C =:= $\n; C =:= 11;
             C =:= 12; C =:= $\r ->
    trim_ascii(Rest);
trim_ascii(Bin) -> trim_ascii_right(Bin, byte_size(Bin)).

trim_ascii_right(_, 0) -> <<>>;
trim_ascii_right(Bin, Len) ->
    case binary:at(Bin, Len - 1) of
        C when C =:= $\s; C =:= $\t; C =:= $\n; C =:= 11;
               C =:= 12; C =:= $\r ->
            trim_ascii_right(Bin, Len - 1);
        _ -> binary:part(Bin, 0, Len)
    end.

%% @doc Locate the process transaction's exact canonical block position.
spawn_ordinate(ProcessID, Opts) ->
    TXID = hb_util:human_id(ProcessID),
    maybe
        {ok, Height} ?= transaction_height(TXID, Opts),
        {ok, Block} ?= fetch_block(Height, Opts),
        ok ?= validate_spawn_block_height(Height, Block, Opts),
        {ok, Index} ?=
            find_index(TXID, hb_maps:get(<<"txs">>, Block, [], Opts), 0),
        {ok, Height, Index, ordinate(Height, Index)}
    end.

validate_spawn_block_height(Height, Block, Opts) ->
    case hb_util:int(hb_maps:get(<<"height">>, Block, -1, Opts)) of
        Height -> ok;
        ActualHeight ->
            {error,
                #{
                    <<"status">> => 502,
                    <<"reason">> =>
                        <<"Process status returned the wrong Arweave block.">>,
                    <<"expected">> => Height,
                    <<"actual">> => ActualHeight
                }
            }
    end.

transaction_height(TXID, Opts) ->
    Result =
        hb_http:request(
            #{
                <<"method">> => <<"GET">>,
                <<"path">> => <<"/arweave/tx/", TXID/binary, "/status">>
            },
            no_result_cache(fetch_opts(Opts))
        ),
    case best_response(Result) of
        {ok, Response} ->
            Status =
                hb_json:decode(
                    hb_maps:get(<<"body">>, Response, <<"{}">>, Opts)
                ),
            case hb_maps:get(<<"block_height">>, Status, undefined, Opts) of
                undefined -> unconfirmed_process(TXID);
                Height -> {ok, hb_util:int(Height)}
            end;
        _ -> unconfirmed_process(TXID)
    end.

find_index(_TXID, [], _Index) ->
    {error,
        #{
            <<"status">> => 502,
            <<"reason">> =>
                <<"Process TXID is absent from its reported Arweave block.">>
        }
    };
find_index(TXID, [Candidate | Rest], Index) ->
    case hb_util:human_id(Candidate) of
        TXID -> {ok, Index};
        _ -> find_index(TXID, Rest, Index + 1)
    end.

unconfirmed_process(TXID) ->
    {error,
        #{
            <<"status">> => 404,
            <<"reason">> => <<"Process is not yet confirmed on Arweave.">>,
            <<"process">> => TXID
        }
    }.

best_response({error, {no_viable_responses, Responses}}) ->
    best_response(Responses);
best_response([]) -> {error, no_viable_responses};
best_response(Responses) when is_list(Responses) ->
    hd(
        lists:sort(
            fun({_, A}, {_, B}) ->
                response_status(A) =< response_status(B)
            end,
            Responses
        )
    );
best_response(Response) -> Response.

response_status(Response) when is_map(Response) ->
    maps:get(<<"status">>, Response, 999);
response_status(_) -> 999.

validate_block(Height, State, Block, Opts) ->
    ActualHeight = hb_util:int(hb_maps:get(<<"height">>, Block, -1, Opts)),
    Previous = hb_maps:get(<<"previous-block">>, Block, not_found, Opts),
    ExpectedPrevious = hb_maps:get(<<"block-hash">>, State, not_found, Opts),
    case {
        ActualHeight =:= Height,
        ExpectedPrevious =:= not_found orelse Previous =:= ExpectedPrevious
    } of
        {true, true} -> ok;
        {false, _} ->
            {error,
                #{
                    <<"status">> => 502,
                    <<"reason">> => <<"Arweave block height mismatch.">>,
                    <<"expected">> => Height,
                    <<"actual">> => ActualHeight
                }
            };
        {true, false} ->
            {error,
                #{
                    <<"status">> => 409,
                    <<"reason">> =>
                        <<"Arweave block does not extend the indexed chain.">>,
                    <<"block-height">> => Height
                }
            }
    end.

%% @doc Canonical, lossless transaction position within the weave.
ordinate(Height, Index) ->
    <<(integer_to_binary(Height))/binary, "-", (integer_to_binary(Index))/binary>>.

parse_ordinate(Ordinate) ->
    case binary:split(Ordinate, <<"-">>, [global]) of
        [Height, Index] ->
            try
                Position = {binary_to_integer(Height), binary_to_integer(Index)},
                case Position of
                    {H, I} when H >= 0, I >= 0 -> {ok, Position};
                    _ -> error
                end
            catch _:_ -> error
            end;
        _ -> error
    end.

no_result_cache(Opts) ->
    Opts#{
        <<"hashpath">> => ignore,
        <<"cache-control">> => [<<"no-cache">>, <<"no-store">>]
    }.

%% @doc Run one scheduler-store task at a time through a node-local worker.
%% Global synchronization and each process have separate names, so one slow
%% process initialization does not block unrelated schedules. Process workers
%% retire when idle, while the global worker retains its recent sync state.
%% Unexpected task failures terminate the worker and surface to queued callers.
exclusive(Name, Fun) ->
    exclusive(Name, Fun, infinity).

exclusive(Name, Fun, IdleTimeout) ->
    Runner =
        hb_name:singleton(
            Name,
            fun() ->
                try runner(undefined, IdleTimeout)
                after hb_name:unregister(Name)
                end
            end
        ),
    Monitor = erlang:monitor(process, Runner),
    Runner ! {run, self(), Monitor, Fun},
    receive
        {ran, Monitor, Result} ->
            erlang:demonitor(Monitor, [flush]),
            Result;
        {'DOWN', Monitor, process, Runner, Reason}
                when Reason =:= normal; Reason =:= noproc ->
            exclusive(Name, Fun, IdleTimeout);
        {'DOWN', Monitor, process, Runner, Reason} ->
            exit({arweave_scheduler_sync_runner_down, Reason})
    end.

runner(State, IdleTimeout) ->
    receive
        {run, Caller, Ref, Fun} ->
            {Result, NewState} = Fun(State),
            Caller ! {ran, Ref, Result},
            runner(NewState, IdleTimeout)
    after IdleTimeout ->
        ok
    end.

scheduler_store(Opts) ->
    hb_opts:get(
        store,
        no_viable_store,
        dev_arweave_scheduler_cache:opts(Opts)
    ).

%%% Tests

targets_test() ->
    Native = crypto:strong_rand_bytes(32),
    NativeAddress = hb_util:human_id(Native),
    A = hb_util:human_id(crypto:strong_rand_bytes(32)),
    B = hb_util:human_id(crypto:strong_rand_bytes(32)),
    TX =
        #tx{
            target = Native,
            tags =
                [
                    {<<"Assign-To">>,
                        <<11, 12, " ", A/binary, ",", B/binary, ",,",
                            A/binary, ",", NativeAddress/binary,
                            ",invalid \t\r\n", 11, 12>>}
                ]
        },
    ?assertEqual(
        lists:usort([NativeAddress, A, B]),
        targets(TX)
    ),
    ?assertEqual(
        [NativeAddress],
        targets(
            TX#tx{
                tags =
                    [
                        {<<"Assign-To">>, A},
                        {<<"assign-to">>, B}
                    ]
            }
        )
    ),
    ?assertEqual([], transaction_targets(TX#tx{ data_size = 1 })),
    ?assertEqual({ok, A}, canonical_process_id(A)),
    ?assertEqual(error, canonical_process_id(<<"not-a-process">>)).

binary_tag_names_test() ->
    Native = crypto:strong_rand_bytes(32),
    NativeAddress = hb_util:human_id(Native),
    Assigned = hb_util:human_id(crypto:strong_rand_bytes(32)),
    lists:foreach(
        fun(Key) ->
            TX = #tx{
                target = Native,
                tags = [{Key, Assigned}, {<<"AsSiGn-To">>, Assigned}]
            },
            ?assertEqual(lists:usort([NativeAddress, Assigned]), targets(TX))
        end,
        [<<224, 17>>, <<"assign-to", 0>>, <<"a", 16#C5, 16#BF, "sign-to">> |
            [<<Byte>> || Byte <- lists:seq(0, 255)]]
    ).

ordinate_test() ->
    ?assertEqual(<<"1966084-23">>, ordinate(1966084, 23)),
    ?assertEqual({ok, {1966084, 23}}, parse_ordinate(<<"1966084-23">>)),
    ?assertEqual(error, parse_ordinate(<<"1966084">>)),
    ?assertEqual(error, parse_ordinate(<<"-1-2">>)).

singleton_serialization_test() ->
    Name = {?MODULE, test, make_ref()},
    Parent = self(),
    Run =
        fun() ->
            Result =
                exclusive(
                    Name,
                    fun(State) ->
                        Count = case State of undefined -> 0; _ -> State end,
                        {{ok, Count}, Count + 1}
                    end
                ),
            Parent ! {complete, Result}
        end,
    _ = [spawn(Run) || _ <- lists:seq(1, 12)],
    Results = [receive {complete, {ok, N}} -> N end || _ <- lists:seq(1, 12)],
    ?assertEqual(lists:seq(0, 11), lists:sort(Results)),
    stop_test_runner(Name).

idle_worker_retirement_test() ->
    Name = {?MODULE, test, make_ref()},
    ?assertEqual(
        {ok, complete},
        exclusive(Name, fun(_State) -> {{ok, complete}, undefined} end, 1)
    ),
    ?assert(
        hb_util:wait_until(fun() -> hb_name:lookup(Name) =:= undefined end, 100)
    ).

public_process_path_test() ->
    Store =
        hb_test_utils:test_store(hb_store_volatile, <<"ar-sched-public">>),
    ok = hb_store:start(Store),
    Opts =
        #{
            <<"store">> => [Store],
            <<"arweave-scheduler-from">> => 100,
            <<"arweave-scheduler-sync-interval">> => 10000,
            <<"priv-wallet">> => ar_wallet:new()
        },
    ProcessID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Global = #{ <<"from">> => 100, <<"to">> => 101 },
    ProcessState =
        #{
            <<"process">> => ProcessID,
            <<"spawn-ordinate">> => <<"100-0">>,
            <<"synced-to">> => 101,
            <<"next-slot">> => 1
        },
    {ok, _} = dev_arweave_scheduler_cache:write_global(Global, Opts),
    {ok, _} =
        dev_arweave_scheduler_cache:write_process(
            ProcessID, ProcessState, Opts
        ),
    SyncName = {?MODULE, sync, scheduler_store(Opts)},
    ?assertEqual(
        {ok, seeded},
        exclusive(
            SyncName,
            fun(_Cached) ->
                {
                    {ok, seeded},
                    {erlang:monotonic_time(millisecond), Global}
                }
            end
        )
    ),
    ?assertEqual({ok, ProcessState}, process(ProcessID, Opts)),
    stop_test_runner(SyncName),
    stop_test_runner(
        {?MODULE, process, scheduler_store(Opts), ProcessID}
    ),
    ok = hb_store:stop(Store).

stop_test_runner(Name) ->
    case hb_name:lookup(Name) of
        undefined -> ok;
        Runner ->
            exit(Runner, kill),
            hb_name:unregister(Name)
    end.

block_validation_test() ->
    State = #{ <<"block-hash">> => <<"previous">> },
    Block =
        #{
            <<"height">> => 101,
            <<"previous-block">> => <<"previous">>,
            <<"indep-hash">> => <<"one">>
        },
    ?assertEqual(ok, validate_block(101, State, Block, #{})),
    ?assertMatch({error, _}, validate_block(100, State, Block, #{})),
    ?assertMatch(
        {error, _},
        validate_block(
            101,
            State,
            Block#{ <<"previous-block">> => <<"other">> },
            #{}
        )
    ),
    NextBlock =
        #{
            <<"height">> => 102,
            <<"previous-block">> => <<"one">>,
            <<"indep-hash">> => <<"two">>
        },
    ?assertEqual(
        {ok,
            #{
                <<"to">> => 102,
                <<"block-hash">> => <<"two">>
            }
        },
        validate_blocks([{101, Block}, {102, NextBlock}], State, #{})
    ),
    ?assertMatch(
        {error, _},
        validate_blocks(
            [
                {101, Block},
                {102, NextBlock#{ <<"previous-block">> => <<"wrong">> }}
            ],
            State,
            #{}
        )
    ).

block_cache_refresh_test_() ->
    {timeout, 60,
        fun() ->
            Stores = [hb_test_utils:test_store(hb_store_volatile)
                || _ <- lists:seq(1, 3)],
            lists:foreach(fun hb_store:start/1, Stores),
            [Ambient, Blocks, Scheduler] = Stores,
            Opts = no_result_cache(#{
                <<"store">> => [Ambient],
                <<"arweave-block-store">> => [Blocks],
                <<"scheduler-store">> => [Scheduler],
                <<"gateway">> => <<"http://chain-3.arweave.xyz:1984">>,
                <<"arweave-scheduler-fetch-attempts">> => 1
            }),
            Height = 2003806,
            Req = #{ <<"path">> => <<"~arweave@2.9/block">>,
                <<"block">> => Height, <<"include-proofs">> => false },
            try
                {ok, Canonical} = fetch_block(Height, Opts),
                ?assertNot(hb_maps:is_key(<<"poa">>, Canonical, Opts)),
                ?assertNot(hb_maps:is_key(<<"poa2">>, Canonical, Opts)),
                Orphan = Canonical#{ <<"indep-hash">> =>
                    <<"kOJgXHI4xd7gTjHGg5j9OimYygCIGyGrenmcGimIeR3jxBLZKXA9V2TKxP7J1V2v">> },
                ?assertNotEqual(Orphan, Canonical),
                BlockOpts = Opts#{ <<"store">> => [Blocks] },
                {ok, OrphanID} = hb_cache:write(Orphan, BlockOpts),
                ok = hb_cache:link(OrphanID,
                    <<"~arweave@2.9/block/height/2003806">>, BlockOpts),
                {ok, Cached} = hb_ao:resolve(Req, Opts),
                ?assertEqual(Orphan, hb_cache:ensure_all_loaded(Cached, Opts)),
                ?assertEqual({ok, Canonical}, fetch_block(Height, Opts)),
                {ok, _} = dev_arweave_scheduler_cache:write_block(
                    Height, Canonical, Opts
                ),
                Offline = Opts#{ <<"gateway">> => <<"http://127.0.0.1:1">>,
                    <<"routes">> => [] },
                ?assertEqual({ok, Canonical}, fetch_block(Height, Offline)),
                {ok, Refreshed} = hb_ao:resolve(
                    Req#{ <<"cache-control">> => [<<"no-cache">>] },
                    (fetch_opts(Opts))#{ <<"arweave-index-blocks">> => true }
                ),
                ?assertEqual(Canonical,
                    hb_cache:ensure_all_loaded(Refreshed, Opts)),
                {ok, Stored} = hb_ao:resolve(
                    Req#{ <<"cache-control">> => [<<"only-if-cached">>] },
                    Offline
                ),
                ?assertEqual(Canonical,
                    hb_cache:ensure_all_loaded(Stored, Opts)),
                ?assertEqual({error, not_found}, hb_ao:resolve(
                    Req#{ <<"cache-control">> =>
                        [<<"no-cache">>, <<"only-if-cached">>] }, Offline
                )),
                {ok, Confirmed} = confirmed_tip(Opts),
                ?assert(Confirmed > Height)
            after
                lists:foreach(fun hb_store:stop/1, Stores)
            end
        end}.

unparseable_types_header_indexing_test_() ->
    {timeout, 60,
        fun() ->
            Store = hb_test_utils:test_store(hb_store_volatile),
            ok = hb_store:start(Store),
            Opts = #{
                <<"store">> => [Store],
                <<"scheduler-store">> => [Store],
                <<"gateway">> => <<"https://arweave.net">>,
                <<"arweave-scheduler-fetch-attempts">> => 1
            },
            try
                % Each header's `ao-types' is `from-process="integer"', which
                % does not parse. The header is read with no types applied, and
                % its target is indexed.
                lists:foreach(
                    fun(TXID) ->
                        ?assertMatch(
                            {ok, 1, #tx{}, [_ | _]},
                            index_transaction(1, 0, TXID, Opts)
                        )
                    end,
                    [
                        <<"gbNWU2YAM9N8sxx4CGtOcmBcwgk7h_o7zgHthnP9GW0">>,
                        <<"lZhsB-RXx5IP04sX17LlsfbGAX-FZJ19q-iKv41gbQg">>,
                        <<"dZ048pvZ1Osx8Kqi90uah-z1TF7GI_1I-UGt_ujpHow">>
                    ]
                ),
                ?assertEqual(
                    {error, not_found},
                    collect_indexed([{ok, 1, #tx{}, []}, {error, not_found}], [])
                )
            after
                hb_store:stop(Store)
            end
        end}.

format_one_data_verification_test_() ->
    {timeout, 60,
        fun() ->
            Store =
                hb_test_utils:test_store(
                    hb_store_volatile,
                    <<"ar-sched-format-one">>
                ),
            ok = hb_store:start(Store),
            Opts =
                #{
                    <<"store">> => [Store],
                    <<"scheduler-store">> => [Store],
                    <<"gateway">> => <<"https://arweave.net">>,
                    <<"arweave-scheduler-fetch-attempts">> => 1,
                    <<"priv-wallet">> => ar_wallet:new()
                },
            TXID = <<"bxjwGJjTYxOrZbrB5IhniCMwRkK_v0H3ddbz035LRn8">>,
            {ok, _Header, TX} = fetch_header(TXID, Opts),
            ?assertEqual(1, TX#tx.format),
            ?assertEqual(304801, TX#tx.data_size),
            ?assertEqual(TX#tx.data_size, byte_size(TX#tx.data)),
            ?assert(ar_tx:verify_tx_id(hb_util:native_id(TXID), TX)),
            ok = hb_store:stop(Store)
        end}.

target_listing_failure_does_not_advance_test() ->
    Store = hb_test_utils:test_store(hb_store_volatile, <<"ar-sched-list">>),
    ok = hb_store:start(Store),
    ProcessID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    State =
        #{
            <<"process">> => ProcessID,
            <<"spawn-ordinate">> => <<"100-0">>,
            <<"synced-to">> => 100,
            <<"next-slot">> => 1
        },
    Restricted = Store#{ <<"access">> => [<<"write">>] },
    ?assertEqual(
        {error, not_found},
        materialize_targets(
            ProcessID,
            State,
            #{ <<"to">> => 101 },
            #{ <<"store">> => [Restricted] }
        )
    ),
    ok = hb_store:stop(Store).

%% @doc Materialization filters at the exact spawn ordinate, sorts numeric
%% positions, writes dense slots, and is deterministic when replayed.
sparse_materialization_test() ->
    Store = hb_test_utils:test_store(hb_store_volatile, <<"ar-sched-sync">>),
    ok = hb_store:start(Store),
    Opts = #{ <<"store">> => [Store], <<"priv-wallet">> => ar_wallet:new() },
    ProcessID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    HeaderA = hb_message:commit(#{ <<"message">> => <<"a">> }, Opts),
    HeaderB = hb_message:commit(#{ <<"message">> => <<"b">> }, Opts),
    TXIDA = hb_util:human_id(hb_message:id(HeaderA, signed, Opts)),
    TXIDB = hb_util:human_id(hb_message:id(HeaderB, signed, Opts)),
    {ok, _} = dev_arweave_scheduler_cache:write_header(HeaderA, Opts),
    {ok, _} = dev_arweave_scheduler_cache:write_header(HeaderB, Opts),
    Hash100 = crypto:strong_rand_bytes(48),
    Hash101 = hb_util:encode(crypto:strong_rand_bytes(48)),
    {ok, _} = dev_arweave_scheduler_cache:write_block(
        100,
        #{ <<"height">> => 100, <<"indep-hash">> => Hash100,
            <<"timestamp">> => 1700000000 },
        Opts
    ),
    {ok, _} = dev_arweave_scheduler_cache:write_block(
        101,
        #{ <<"height">> => 101, <<"indep-hash">> => Hash101,
            <<"timestamp">> => 1700000120 },
        Opts
    ),
    ok =
        dev_arweave_scheduler_cache:write_target(
            ProcessID,
            <<"101-0">>,
            TXIDB,
            Opts
        ),
    ok =
        dev_arweave_scheduler_cache:write_target(
            ProcessID,
            <<"100-0">>,
            TXIDA,
            Opts
        ),
    ok =
        dev_arweave_scheduler_cache:write_target(
            ProcessID,
            <<"100-10">>,
            TXIDB,
            Opts
        ),
    ok =
        dev_arweave_scheduler_cache:write_target(
            ProcessID,
            <<"100-2">>,
            TXIDA,
            Opts
        ),
    State =
        #{
            <<"process">> => ProcessID,
            <<"spawn-ordinate">> => <<"100-1">>,
            <<"synced-to">> => 99,
            <<"next-slot">> => 1
        },
    Global =
        #{ <<"from">> => 100, <<"to">> => 101, <<"block-hash">> => TXIDB },
    {ok, NewState} = materialize_targets(ProcessID, State, Global, Opts),
    ?assertEqual(4, hb_maps:get(<<"next-slot">>, NewState, Opts)),
    ?assertEqual(101, hb_maps:get(<<"synced-to">>, NewState, Opts)),
    {ok, Assignment1} =
        dev_arweave_scheduler_cache:read_assignment(ProcessID, 1, Opts),
    {ok, Assignment2} =
        dev_arweave_scheduler_cache:read_assignment(ProcessID, 2, Opts),
    {ok, Assignment3} =
        dev_arweave_scheduler_cache:read_assignment(ProcessID, 3, Opts),
    ?assertEqual(100, hb_maps:get(<<"block-height">>, Assignment1, Opts)),
    ?assertEqual(2, hb_maps:get(<<"block-index">>, Assignment1, Opts)),
    ?assertEqual(
        hb_util:encode(Hash100),
        hb_maps:get(<<"block-hash">>, Assignment1, Opts)
    ),
    ?assertEqual(1700000000, hb_maps:get(<<"timestamp">>, Assignment1, Opts)),
    ?assertEqual(100, hb_maps:get(<<"block-height">>, Assignment2, Opts)),
    ?assertEqual(10, hb_maps:get(<<"block-index">>, Assignment2, Opts)),
    ?assertEqual(
        hb_util:encode(Hash100),
        hb_maps:get(<<"block-hash">>, Assignment2, Opts)
    ),
    ?assertEqual(1700000000, hb_maps:get(<<"timestamp">>, Assignment2, Opts)),
    ?assertEqual(101, hb_maps:get(<<"block-height">>, Assignment3, Opts)),
    ?assertEqual(0, hb_maps:get(<<"block-index">>, Assignment3, Opts)),
    ?assertEqual(
        Hash101,
        hb_maps:get(<<"block-hash">>, Assignment3, Opts)
    ),
    ?assertEqual(1700000120, hb_maps:get(<<"timestamp">>, Assignment3, Opts)),
    AssignmentIDs =
        [
            hb_message:id(Assignment1, signed, Opts),
            hb_message:id(Assignment2, signed, Opts),
            hb_message:id(Assignment3, signed, Opts)
        ],
    {ok, ReplayState} = materialize_targets(ProcessID, State, Global, Opts),
    ?assertEqual(NewState, ReplayState),
    {ok, Replay1} =
        dev_arweave_scheduler_cache:read_assignment(ProcessID, 1, Opts),
    {ok, Replay2} =
        dev_arweave_scheduler_cache:read_assignment(ProcessID, 2, Opts),
    {ok, Replay3} =
        dev_arweave_scheduler_cache:read_assignment(ProcessID, 3, Opts),
    ?assertEqual(
        AssignmentIDs,
        [
            hb_message:id(Replay1, signed, Opts),
            hb_message:id(Replay2, signed, Opts),
            hb_message:id(Replay3, signed, Opts)
        ]
    ),
    ?assertEqual(
        {ok, NewState},
        materialize_targets(ProcessID, NewState, Global, Opts)
    ),
    ?assertEqual(
        {ok, NewState},
        materialize_targets(
            ProcessID,
            NewState,
            Global#{ <<"to">> => 100 },
            Opts
        )
    ),
    ok = hb_store:stop(Store).

%%% Verified ordering. The vectors are real blocks 2002970-2002972 from
%%% arweave.net, with the `chunk' and `unpacked_chunk' of their proofs removed
%%% (neither is a field of the hash), and the signed headers of the 78
%%% transactions of block 2002970.

fixture(Name) ->
    {ok, Body} = file:read_file(<<"test/arweave-blocks/", Name/binary, ".json">>),
    Body.

fixture_block(Height) ->
    dev_arweave:block_message(fixture(integer_to_binary(Height)), #{}).

fixture_headers() -> hb_json:decode(fixture(<<"2002970-txs">>)).

%% A changed block with its `indep-hash' recomputed: what a dishonest gateway
%% can mint, since the miner's proof of work is not checked here.
rehashed(Block) ->
    Block#{ <<"indep-hash">> => hb_util:encode(ar_block:indep_hash(Block)) }.

rehashed_json(Body, Changes) ->
    JSON = maps:merge(hb_json:decode(Body), Changes),
    Hash = ar_block:indep_hash(dev_arweave:block_message(hb_json:encode(JSON), #{})),
    hb_json:encode(JSON#{ <<"indep_hash">> => hb_util:encode(Hash) }).

real_block_indep_hash_test() ->
    Heights = [2002970, 2002971, 2002972],
    Chain =
        [
            begin
                Block = fixture_block(Height),
                ?assertEqual(
                    hb_util:decode(maps:get(<<"indep-hash">>, Block)),
                    ar_block:indep_hash(Block)
                ),
                {ok, Verified} = verified_block(Block, #{}),
                ?assertNot(maps:is_key(<<"poa">>, Verified)),
                {Height, Verified}
            end
        ||
            Height <- Heights
        ],
    ?assertMatch(
        {ok, #{ <<"to">> := 2002972 }},
        validate_blocks(Chain, #{}, #{})
    ),
    % Out of order, the same blocks do not form a chain.
    [First, Second, Third] = Chain,
    ?assertMatch({error, _}, validate_blocks([First, Third, Second], #{}, #{})).

wrong_indep_hash_test() ->
    Block = fixture_block(2002970),
    [TX1, TX2 | TXs] = maps:get(<<"txs">>, Block),
    lists:foreach(
        fun(Tampered) ->
            ?assertMatch(
                {error, #{ <<"status">> := 502 }},
                verified_block(Tampered, #{})
            )
        end,
        [
            Block#{ <<"timestamp">> => maps:get(<<"timestamp">>, Block) + 1 },
            Block#{ <<"txs">> => [TX2, TX1 | TXs] },
            Block#{ <<"txs">> => [TX2 | TXs] },
            Block#{ <<"indep-hash">> =>
                maps:get(<<"indep-hash">>, fixture_block(2002971)) },
            maps:remove(<<"poa">>, Block)
        ]
    ).

real_block_tx_root_test() ->
    Block = fixture_block(2002970),
    TXs = [ar_tx:json_struct_to_tx(Header) || Header <- fixture_headers()],
    ?assertEqual(78, length(TXs)),
    ?assert(lists:all(fun(TX) -> ar_tx:verify_tx_id(TX#tx.id, TX) end, TXs)),
    ?assertEqual(
        maps:get(<<"txs">>, Block),
        [hb_util:encode(TX#tx.id) || TX <- TXs]
    ),
    Entries = fun(List) -> [{2002970, TX, []} || TX <- List] end,
    ?assertEqual(ok, verify_tx_roots([{2002970, Block}], Entries(TXs), #{})),
    [Dropped | Kept] = TXs,
    Foreign = Dropped#tx{ data_root = crypto:strong_rand_bytes(32) },
    % A transaction missing from the block, or one the block's tx_root does
    % not commit to.
    ?assertMatch(
        {error, #{ <<"block-height">> := 2002970 }},
        verify_tx_roots([{2002970, Block}], Entries(Kept), #{})
    ),
    ?assertMatch(
        {error, #{ <<"block-height">> := 2002970 }},
        verify_tx_roots([{2002970, Block}], Entries([Foreign | Kept]), #{})
    ),
    % A minted block whose hash holds but whose tx_root is not its
    % transactions'.
    Forged = rehashed(Block#{ <<"tx-root">> =>
        hb_util:encode(crypto:strong_rand_bytes(32)) }),
    {ok, ForgedVerified} = verified_block(Forged, #{}),
    ?assertMatch(
        {error, _},
        verify_tx_roots([{2002970, ForgedVerified}], Entries(TXs), #{})
    ).

%% A gateway answering `/block/current', `/block/height/<n>' and `/tx/<id>'
%% from `Files', and 404 for anything else.
fixture_gateway(Files) ->
    {ok, URL, Handle} =
        hb_mock_server:start(
            [
                {"/[...]", gateway,
                    fun(#{ <<"path">> := <<"/", Path/binary>> }) ->
                        case maps:find(Path, Files) of
                            {ok, Body} -> {200, Body};
                            error -> {404, <<>>}
                        end
                    end}
            ]
        ),
    {Handle, URL}.

honest_files() ->
    Blocks =
        maps:from_list(
            [
                {<<"block/height/", (integer_to_binary(H))/binary>>,
                    fixture(integer_to_binary(H))}
            ||
                H <- [2002970, 2002971, 2002972]
            ]
        ),
    Headers =
        maps:from_list(
            [
                {<<"tx/", (maps:get(<<"id">>, Header))/binary>>,
                    hb_json:encode(Header)}
            ||
                Header <- fixture_headers()
            ]
        ),
    maps:merge(
        Blocks#{ <<"block/current">> => fixture(<<"2002972">>) },
        Headers
    ).

%% Sync from the gateway into a fresh store: `{Result, Global, Indexed}'.
sync_from(Files) ->
    {Handle, URL} = fixture_gateway(Files),
    Store = hb_test_utils:test_store(hb_store_volatile, <<"ar-sched-verified">>),
    ok = hb_store:start(Store),
    Opts = #{
        <<"store">> => [Store],
        <<"gateway">> => URL,
        <<"arweave-scheduler-from">> => 2002970,
        <<"arweave-scheduler-block-batch">> => 1,
        <<"arweave-scheduler-fetch-attempts">> => 1
    },
    try
        Result = do_sync(not_found, Opts),
        {
            Result,
            dev_arweave_scheduler_cache:read_global(Opts),
            [
                H
            ||
                H <- [2002970, 2002971, 2002972],
                {ok, _} <- [dev_arweave_scheduler_cache:read_block(H, Opts)]
            ]
        }
    after
        hb_store:stop(Store),
        hb_mock_server:stop(Handle)
    end.

verified_sync_test_() ->
    {timeout, 120, fun verified_sync/0}.

verified_sync() ->
    Honest = honest_files(),
    Hash70 = maps:get(<<"indep-hash">>, fixture_block(2002970)),
    % The tip is 2002972, so with depth 2 only 2002970 is confirmed, and its
    % two descendants are verified but not indexed.
    ?assertMatch(
        {{ok, #{ <<"to">> := 2002970, <<"block-hash">> := Hash70 }},
            {ok, _}, [2002970]},
        sync_from(Honest)
    ),
    Refused = fun(Files) ->
        {Result, Global, Indexed} = sync_from(Files),
        ?assertMatch({error, _}, Result),
        ?assertNotMatch({ok, _}, Global),
        ?assertEqual([], Indexed)
    end,
    % Depth not met: a confirming block is missing.
    Refused(maps:remove(<<"block/height/2002972">>, Honest)),
    % A confirming block, rehashed, that does not extend the chain.
    Refused(Honest#{ <<"block/height/2002972">> =>
        rehashed_json(fixture(<<"2002972">>),
            #{ <<"previous_block">> => Hash70 }) }),
    % A block whose fields do not recompute its indep_hash.
    Refused(Honest#{ <<"block/height/2002971">> =>
        hb_json:encode((hb_json:decode(fixture(<<"2002971">>)))#{
            <<"timestamp">> => 1 }) }),
    % A transaction whose header is unavailable: tx_root cannot be checked.
    [Header | _] = fixture_headers(),
    Refused(maps:remove(<<"tx/", (maps:get(<<"id">>, Header))/binary>>, Honest)).

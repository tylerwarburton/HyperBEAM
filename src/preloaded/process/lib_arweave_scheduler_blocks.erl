%%% @doc The blocks `~arweave-scheduler@1.0' has verified, by height, in its own
%%% store. A library, not a helper of that device, because the bridge replica
%%% reads the same record from another package: an Arweave assignment is only
%%% believed when it names the block this node verified at that height.
-module(lib_arweave_scheduler_blocks).
-export([opts/1, write_block/3, read_block/2]).
-include("include/hb.hrl").

-define(CACHE_PREFIX, <<"~arweave-scheduler@1.0">>).

%% @doc Use the Arweave scheduler's dedicated store when configured, then the
%% generic scheduler store for compatibility. Otherwise use the local-scoped
%% stores from the node's main `store' list. Keeping the L1 index separate
%% allows a coordinated rollout boundary without disturbing local schedules.
opts(Opts) ->
    Opts#{
        <<"store">> => hb_opts:get(
            arweave_scheduler_store,
            hb_opts:get(
                scheduler_store,
                hb_opts:get(store, no_viable_store, hb_store:scope(Opts, local)),
                Opts
            ),
            Opts
        )
    }.

%% @doc Cache and read canonical block messages under scheduler-owned height
%% paths. This avoids coupling the scheduler package to the Arweave device's
%% private block-cache module.
write_block(Height, Block, RawOpts) ->
    Opts = opts(RawOpts),
    case hb_cache:write(Block, Opts) of
        {ok, BlockID} ->
            case hb_cache:link(BlockID, block_path(Height), Opts) of
                ok -> {ok, BlockID};
                Error -> Error
            end;
        Error -> Error
    end.

read_block(Height, RawOpts) ->
    Opts = opts(RawOpts),
    try hb_cache:read(block_path(Height), Opts) of
        {ok, Block} -> {ok, hb_cache:ensure_all_loaded(Block, Opts)};
        {error, not_found} -> not_found;
        Error -> Error
    catch
        throw:{necessary_message_not_found, _, _} -> not_found;
        throw:{could_not_read_lazy_link, _, _, _} -> not_found
    end.

block_path(Height) ->
    hb_path:to_binary([?CACHE_PREFIX, <<"blocks">>, hb_util:bin(Height)]).

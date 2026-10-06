%%% @doc A module that performs caching operations for the Arweave device, 
%%% focused on ensuring that block metadata is queriable via pseudo-paths.
-module(dev_arweave_block_cache).
-export([latest/1, heights/1, read/2, write/2]).
-export([path/2]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Every entry point reads and writes through `hb_store_essentials:opts/1': with
%% an `essentials-store' configured, these records and the messages they name
%% are essentials and are written there, self-contained; otherwise the options
%% are unchanged.
latest(Opts) -> ess_latest(hb_store_essentials:opts(Opts)).
heights(Opts) -> ess_heights(hb_store_essentials:opts(Opts)).
read(A1, Opts) -> ess_read(A1, hb_store_essentials:opts(Opts)).
write(A1, Opts) -> ess_write(A1, hb_store_essentials:opts(Opts)).
path(A1, Opts) -> ess_path(A1, hb_store_essentials:opts(Opts)).

%% @doc The pseudo-path prefix which the Arweave block cache should use.
-define(ARWEAVE_BLOCK_CACHE_PREFIX, <<"~arweave@2.9">>).

%% @doc Get the latest block from the cache.
ess_latest(Opts) ->
    case ess_heights(Opts) of
        {ok, []} ->
            ?event(arweave_cache, no_blocks_in_cache),
            not_found;
        {ok, Blocks} ->
            Latest = lists:max(Blocks),
            ?event(arweave_cache, {latest_block_from_cache, {latest, Latest}}),
            {ok, Latest}
    end.

%% @doc Get the list of blocks from the cache.
ess_heights(Opts) ->
    AllBlocks =
        hb_cache:list_numbered(
            hb_path:to_binary([
                ?ARWEAVE_BLOCK_CACHE_PREFIX,
                <<"block">>,
                <<"height">>
            ]),
            Opts
        ),
    ?event(arweave_cache, {listed_blocks, length(AllBlocks)}),
    {ok, AllBlocks}.

%% @doc Read a block from the cache.
ess_read(Block, Opts) ->
    Res = hb_cache:read(ess_path(Block, Opts), Opts),
    ?event(arweave_cache, {read_block, {reference, Block}, {result, Res}}),
    Res.

%% @doc Return the path of a block that will be used in the cache.
ess_path(Block, _Opts) when is_integer(Block) ->
    hb_path:to_binary([
        ?ARWEAVE_BLOCK_CACHE_PREFIX,
        <<"block">>,
        <<"height">>,
        hb_util:bin(Block)
    ]).

%% @doc Write a block to the cache and create pseudo-paths for it.
ess_write(Block, Opts) ->
    {ok, Height} = hb_maps:find(<<"height">>, Block, Opts),
    {ok, BlockID} = hb_maps:find(<<"indep_hash">>, Block, Opts),
    {ok, BlockHash} = hb_maps:find(<<"hash">>, Block, Opts),
    {ok, MsgID} = hb_cache:write(Block, Opts),
    % Link the independent hash and the dependent hash to the written AO-Core
    % message ID.
    hb_cache:link(MsgID, BlockID, Opts),
    hb_cache:link(MsgID, BlockHash, Opts),
    % Link the block height pseudo-path to the message.
    hb_cache:link(MsgID, ess_path(Height, Opts), Opts),
    ?event(arweave_cache, {wrote_block, {height, Height}, {message_id, MsgID}}),
    {ok, MsgID}.

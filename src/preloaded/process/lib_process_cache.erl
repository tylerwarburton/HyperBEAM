
%%% @doc A wrapper around the hb_cache module that provides a more
%%% convenient interface for reading the result of a process at a given slot or
%%% message ID.
-module(lib_process_cache).
-export([fresh/3, fresh/4]).
-export([latest/2, latest/3, latest/4, latest_slot/2, hot_head/2]).
-export([read/2, read/3, refresh/3, write/4]).
-export([read_results/3, stored_floor/2, pruned/2]).
-export([delivery_store/1, blob_key/3, blob_put/4, blob_get/3, blob_slots/3]).
-export([blob_commit/3, has_outbox/3]).
-export([blob_encode/1, blob_decode/1, outbox_entries/2]).
-export([async_checkpoints/1, await_checkpoint/1]).
-include_lib("eunit/include/eunit.hrl").
-include("include/hb.hrl").

%% Children per listing of a kind, and slots per page of `latest_paged/6'.
-define(LIST_PAGE, 128).
-define(LATEST_PAGE, 100).

%% @doc Read the result of a process at a given slot.
read(ProcID, Opts) ->
    hb_util:ok(latest(ProcID, Opts)).
read(ProcID, SlotRef, RawOpts) ->
    Opts = lib_process:cache_opts(RawOpts),
    ?event({reading_computed_result, ProcID, SlotRef}),
    case is_integer(SlotRef) andalso hot_get(ProcID, SlotRef, Opts) of
        {ok, Hot} -> {ok, Hot};
        _ ->
            case is_integer(SlotRef) andalso blob_read(ProcID, SlotRef, Opts) of
                {ok, Blob} -> materialize(ProcID, Blob, Opts);
                _ ->
                    Path = path(ProcID, SlotRef, Opts),
                    case hb_cache:read(Path, Opts) of
                        {ok, Stored} -> materialize(ProcID, Stored, Opts);
                        Other -> Other
                    end
            end
    end.

%% @doc Write a process computation result to the cache. With
%% `process-cache-write' false nothing is written: a slot rebuilt for one
%% read (`dev_process:history/6') is never stored again.
write(ProcID, Slot, Msg, RawOpts) ->
    case hb_opts:get(<<"process-cache-write">>, true, RawOpts) of
        false -> {ok, not_stored};
        _ -> write_stored(ProcID, Slot, Msg, RawOpts)
    end.

write_stored(ProcID, Slot, Msg, RawOpts) ->
    Opts = lib_process:cache_opts(RawOpts),
    Public = public_state(ProcID, Slot, Msg, Opts),
    % The outbox row first, in the same buffered batch as the state row and
    % before it: a stored slot never exists without its outbox row, so a
    % slot the process worker will not compute again still has what it owes
    % its targets (`lib_process_delivery').
    _ = put_outbox(ProcID, Slot, Msg, Opts),
    Result =
        case {delta(Msg, Opts), blob_store(Opts)} of
            {{ok, Patches, Results}, Store} when Store =/= none, is_integer(Slot),
                                                 Slot >= 0 ->
                write_blob(ProcID, Slot, Msg, Patches, Results, Store, Opts);
            {{ok, Patches, Results}, _} ->
                case checkpoint_due(ProcID, Slot, Msg, Opts) of
                    true -> write_full(ProcID, Slot, Msg, Opts);
                    false ->
                        write_delta(ProcID, Slot, Patches, Results, Opts)
                end;
            {not_found, Store} when Store =/= none, is_integer(Slot), Slot >= 0 ->
                write_full_blob(ProcID, Slot, Msg, Store, Opts);
            {not_found, _} -> write_full(ProcID, Slot, Msg, Opts)
        end,
    % A VM snapshot is only ever restored from its stored row: the hot cache
    % does not keep one (left uncompressed for a checkpoint written off the
    % slot, it is the whole instance's memory). The rest of the state is kept
    % whole (`hot_put/4').
    hb_phase_timer:time(slot_hot_put, fun() ->
        hot_put(ProcID, Slot, maps:remove(<<"snapshot">>, Public), Opts)
    end),
    % The published-key view `now' reads are answered from.
    hb_process_view:update(ProcID, Slot, Public,
        case delta(Msg, Opts) of
            {ok, SlotPatches, _} -> SlotPatches;
            _ -> full
        end,
        Opts),
    Result.

%% @doc The public state of a slot (`hb_private:reset/1' of it), built from
%% the slot before's when the slot is a delta of it: that one with the keys
%% the slot's patches name, `results' and `at-slot', which every slot sets
%% (`hb_state_patch:apply/5'), and `snapshot', which a snapshot slot carries
%% (`dev_process:store_result/6'), reset again. Resetting the whole state
%% was a deep walk of every published key every slot: at 10,000 keys ~10,000
%% `reset' calls a slot and most of the store step. The slot before's public
%% state is the calling process's own (the process worker computes in
%% order); any other slot -- the first, a gap, a full state, a patch list
%% that does not parse -- is reset whole. `process-cache-public-check' (tests)
%% resets every slot whole as well and refuses a difference.
public_state(ProcID, Slot, Msg, Opts) ->
    Key = {?MODULE, public, ProcID},
    Public =
        case {is_integer(Slot) andalso get(Key), delta(Msg, Opts)} of
            {{Prev, PrevPublic}, {ok, Patches, _}} when Prev == Slot - 1 ->
                case hb_state_patch:roots(Patches, Opts) of
                    {ok, Roots} ->
                        lists:foldl(
                            fun(K, Acc) ->
                                case not hb_private:is_private(K) andalso maps:find(K, Msg) of
                                    {ok, V} -> Acc#{ K => hb_private:reset(V) };
                                    _ -> maps:remove(K, Acc)
                                end
                            end,
                            PrevPublic,
                            Roots ++ [<<"results">>, <<"at-slot">>, <<"snapshot">>]
                        );
                    error -> hb_private:reset(Msg)
                end;
            _ -> hb_private:reset(Msg)
        end,
    case hb_opts:get(<<"process-cache-public-check">>, false, Opts) of
        true ->
            case hb_private:reset(Msg) of
                Public -> ok;
                Full -> erlang:error({public_state_diverged, ProcID, Slot,
                    [ K || K <- lists:usort(maps:keys(Full) ++ maps:keys(Public)),
                           maps:get(K, Full, absent) =/= maps:get(K, Public, absent) ]})
            end;
        _ -> ok
    end,
    case is_integer(Slot) of
        true -> put(Key, {Slot, Public});
        false -> ok
    end,
    Public.

%% @doc Write the whole public state of a slot, as RC7 does for every slot.
write_full(ProcID, Slot, Msg, Opts) ->
    % Write the item to the cache in the root of the store.
    {ok, Root} = hb_cache:write(hb_private:reset(Msg), Opts),
    % Link the item to the path in the store by slot number.
    SlotNumPath = path(ProcID, Slot, Opts),
    hb_cache:link(Root, SlotNumPath, Opts),
    % Link the item to the message ID path in the store.
    MsgIDPath =
        path(
            ProcID,
            ID = hb_message:id(Msg, uncommitted, Opts),
            Opts
        ),
    ?event(
        {linking_id,
            {proc_id, ProcID},
            {slot, Slot},
            {id, ID},
            {path, MsgIDPath}
        }
    ),
    hb_cache:link(Root, MsgIDPath, Opts),
    ok = refresh(ProcID, Slot, Opts),
    % Return the slot number path.
    {ok, SlotNumPath}.

%% @doc Mark the process as refreshed at the current clock time. The marker
%% is the head: every slot at or below it that is not stored is refused with
%% a 410 (`dev_process:compute/3'). It is written to the store that holds the
%% slot rows, after them: LMDB commits a store's writes in order, so the
%% marker on disk is never ahead of the rows it names -- it is a lower bound,
%% and a stale one only sends a request to a stored read or to the worker.
refresh(ProcID, Slot, RawOpts) ->
    Opts = lib_process:cache_opts(RawOpts),
    CachedSlot = latest_slot(ProcID, lib_process:scoped_opts(Opts)),
    case CachedSlot of
        {ok, LatestSlot} when Slot < LatestSlot ->
            ok;
        _ ->
            hb_store:write(
                marker_store(Opts),
                #{ refreshed_path(ProcID) => refreshed_at(Slot, clock(Opts)) },
                Opts
            )
    end.

%% @doc The store the head marker lives in: the one holding the slot rows,
%% whether or not the computation delivers.
marker_store(Opts) ->
    case first_lmdb_store(Opts) of
        none -> hb_opts:get(store, no_viable_store, Opts);
        Store -> [Store]
    end.

refreshed_at(Slot, Timestamp) ->
    iolist_to_binary([
        integer_to_binary(Slot),
        <<":">>,
        integer_to_binary(Timestamp)
    ]).

refreshed_path(ProcID) ->
    path(ProcID, <<"latest">>, #{}).

%% @doc Return whether the latest cached process output is fresh enough for
%% `/now' to serve from cache under the effective `max-age'. A `max-age' that
%% is neither `infinity' nor an integer gives a 400 error.
fresh(ProcID, Req, RawOpts) ->
    Opts = lib_process:scoped_opts(RawOpts),
    case effective_max_age(Req, Opts) of
        infinity ->
            true;
        {error, _} = Error ->
            Error;
        _MaxAge ->
            case latest_slot(ProcID, Opts) of
                {ok, Slot} -> fresh(ProcID, Slot, Req, Opts);
                {error, not_found} -> false
            end
    end.
fresh(ProcID, Slot, Req, RawOpts) ->
    Opts = lib_process:scoped_opts(RawOpts),
    case effective_max_age(Req, Opts) of
        infinity ->
            true;
        {error, _} = Error ->
            Error;
        MaxAge ->
            case read_refreshed_at(ProcID, Opts) of
                {ok, Slot, RefreshedAt} -> clock(Opts) =< RefreshedAt + MaxAge;
                _ -> false
            end
    end.

%% @doc Read the timestamp of the last refresh of a process.
read_refreshed_at(ProcID, Opts) ->
    case hb_store:read(marker_store(Opts), refreshed_path(ProcID), Opts) of
        {ok, RefreshedAt} -> parse_refreshed_at(RefreshedAt);
        _ -> undefined
    end.

parse_refreshed_at(RefreshedAt) ->
    case binary:split(RefreshedAt, <<":">>) of
        [Slot, Timestamp] ->
            {ok, hb_util:int(Slot), hb_util:int(Timestamp)};
        _ ->
            undefined
    end.

%% @doc Calculate the effective maximum age of a process cache entry.
effective_max_age(Req, Opts) ->
    case lib_process:only_if_cached(Req, Opts) of
        true ->
            infinity;
        false ->
            case max_age_from_request(Req, Opts) of
                {ok, MaxAge} ->
                    normalize_max_age(MaxAge);
                error ->
                    normalize_max_age(
                        hb_opts:get(process_now_max_age, infinity, Opts)
                    )
            end
    end.

max_age_from_request(Req, Opts) when is_map(Req) ->
    hb_maps:find(<<"max-age">>, Req, Opts);
max_age_from_request(_Req, _Opts) ->
    error.

normalize_max_age(infinity) -> infinity;
normalize_max_age(<<"infinity">>) -> infinity;
normalize_max_age(RawMaxAge) ->
    case hb_util:safe_int(RawMaxAge) of
        {ok, MaxAge} -> MaxAge;
        {error, _} ->
            {error,
                #{
                    <<"status">> => 400,
                    <<"body">> => <<"Invalid max-age.">>
                }
            }
    end.

%% @doc Return the current clock time. Allows the option to override the clock
%% time with a custom value for test use.
clock(Opts) ->
    case hb_opts:get(process_clock, undefined, Opts) of
        undefined -> erlang:system_time(second);
        Time -> hb_util:int(Time)
    end.

%%% Delta storage and the hot cache.
%%%
%%% RC7 writes the whole public state of every slot: on the game, ~50 ms a
%%% slot and most of a slot's storage. With `process-delta-cache => true', a
%%% slot computed by `lua-patch@1.0' between checkpoints is stored as its
%%% patches and results only. A checkpoint (the whole state) is written every
%%% `process-delta-checkpoint-slots' (default 1000), whenever the state carries
%%% a VM snapshot, and whenever the previous slot cannot be found, so a chain
%%% never crosses a gap. Reading a delta slot applies its patches to the
%%% previous slot, which comes from the hot cache when it is there and is
%%% otherwise read the same way, back to the checkpoint.
%%%
%%% The hot cache keeps the newest `process-hot-cache-slots' (default 8)
%%% public states of each process in memory, so the steady state never reads
%%% a chain, and `latest' does not list every slot of the process.

-define(DELTA_FORMAT, <<"lua-patch-delta@1">>).
-define(HOT, lib_process_cache_hot).

delta(Msg, Opts) ->
    case hb_opts:get(<<"process-delta-cache">>, false, Opts) of
        true ->
            case hb_private:get(<<"process-cache-delta">>, Msg, not_found, Opts) of
                #{ <<"patches">> := Patches, <<"results">> := Results }
                        when is_list(Patches), is_map(Results) ->
                    {ok, Patches, Results};
                _ -> not_found
            end;
        _ -> not_found
    end.

checkpoint_due(ProcID, Slot, Msg, Opts) ->
    Slot =< 0
        orelse maps:is_key(<<"snapshot">>, Msg)
        orelse Slot rem checkpoint_slots(Opts) =:= 0
        orelse not base_exists(ProcID, Slot - 1, Opts).

checkpoint_slots(Opts) ->
    case hb_util:int(hb_opts:get(<<"process-delta-checkpoint-slots">>, 1000, Opts)) of
        N when N > 0 -> N;
        _ -> 1000
    end.

base_exists(ProcID, Slot, Opts) ->
    % Whether the hot cache holds it, without copying the state out.
    case hot_member(ProcID, Slot, Opts) of
        true -> true;
        false when Slot >= 0 ->
            blob_exists(ProcID, Slot, Opts) orelse
                case hb_cache:read(path(ProcID, Slot, Opts), Opts) of
                    {ok, _} -> true;
                    _ -> false
                end;
        false ->
            case hb_cache:read(path(ProcID, Slot, Opts), Opts) of
                {ok, _} -> true;
                _ -> false
            end
    end.

write_delta(ProcID, Slot, Patches, Results, Opts) ->
    Envelope = #{
        <<"cache-format">> => ?DELTA_FORMAT,
        <<"slot">> => Slot,
        <<"base-slot">> => Slot - 1,
        <<"patches">> => Patches,
        <<"results">> => Results
    },
    {ok, Root} = hb_cache:write(Envelope, Opts),
    SlotNumPath = path(ProcID, Slot, Opts),
    hb_cache:link(Root, SlotNumPath, Opts),
    ok = refresh(ProcID, Slot, Opts),
    {ok, SlotNumPath}.

%% @doc A stored message as the public state of its slot: a delta envelope is
%% applied to the state of the slot before it; anything else is that state.
materialize(ProcID, Stored = #{ <<"cache-format">> := ?DELTA_FORMAT }, Opts) ->
    Slot = hb_util:int(hb_maps:get(<<"slot">>, Stored, Opts)),
    BaseSlot = hb_util:int(hb_maps:get(<<"base-slot">>, Stored, Opts)),
    case read(ProcID, BaseSlot, Opts) of
        {ok, Base} ->
            Patches =
                patch_list(
                    hb_cache:ensure_all_loaded(
                        hb_maps:get(<<"patches">>, Stored, Opts),
                        Opts
                    ),
                    Opts
                ),
            Results =
                hb_cache:ensure_all_loaded(
                    hb_maps:get(<<"results">>, Stored, Opts),
                    Opts
                ),
            hb_state_patch:apply(
                maps:remove(<<"snapshot">>, Base),
                Patches,
                Results,
                Slot,
                Opts
            );
        Error -> Error
    end;
materialize(_ProcID, Msg, _Opts) ->
    {ok, Msg}.

patch_list(Patches, _Opts) when is_list(Patches) -> Patches;
patch_list(Patches, Opts) when is_map(Patches) ->
    case hb_util:is_ordered_list(Patches, Opts) of
        true -> hb_util:message_to_ordered_list(Patches);
        false -> []
    end.

%%% A stored slot's hot entry is its whole public state, copied into the
%%% table by the process that stored it (the process worker). 1c17e7325 kept
%%% only the writer's pid there, and the state in the writer's dictionary, so
%%% that a process nobody else reads never paid for the copy (17.4 ms a slot
%%% at 100,000 published keys of 500 bytes). Every other reader of a recent
%%% slot then paid instead: reading the writer's dictionary
%%% (`erlang:process_info/2') made the worker copy its state for each reader
%%% and crashed the emulator under load; reading the store rebuilt the slot
%%% from its checkpoint, 147 ms a read on a lab order book, and on Paralith
%%% a composed push went from 0.44 to 5.3 s. The slots pushes and clients read
%%% are the newest ones, which only this table holds, so it holds them whole.
-define(CHECKPOINTER, lib_process_cache_checkpointer).

hot_put(ProcID, Slot, Msg, Opts) when is_integer(Slot) ->
    case hot_slots(Opts) of
        0 -> ok;
        Keep ->
            ensure_hot_table(),
            try
                ets:insert(?HOT, {{ProcID, Slot}, Msg}),
                hot_trim(ProcID, Slot, Keep)
            catch error:badarg -> ok
            end,
            ok
    end;
hot_put(_ProcID, _Slot, _Msg, _Opts) ->
    ok.

%% Drop the slots of a process at or below `Slot - Keep', walking back from
%% there: what each insert pushed out, not a count of every slot held.
hot_trim(ProcID, Slot, Keep) ->
    case ets:prev(?HOT, {ProcID, Slot - Keep + 1}) of
        {ProcID, Old} = Key when Old =< Slot - Keep ->
            ets:delete(?HOT, Key),
            hot_trim(ProcID, Slot, Keep);
        _ -> ok
    end.

hot_member(ProcID, Slot, Opts) ->
    hot_slots(Opts) > 0 andalso
        (try ets:member(?HOT, {ProcID, Slot}) catch error:badarg -> false end).

%% The hot cache is node-wide and keyed by process ID alone, so only a
%% computation that writes it (`process-delta-cache') reads it: one with its
%% own stores never gets a state computed from another's.
hot_get(ProcID, Slot, Opts) ->
    case hot_slots(Opts) of
        0 -> not_found;
        _ -> hot_get(ProcID, Slot)
    end.

hot_get(ProcID, Slot) ->
    try ets:lookup(?HOT, {ProcID, Slot}) of
        [{_, Msg}] -> {ok, Msg};
        [] -> not_found
    catch error:badarg -> not_found
    end.

%% @doc The newest slot of a process the hot cache holds, from memory only:
%% `{ok, Slot}' or `not_found' (no hot cache, or nothing in it yet). Every
%% stored slot goes through it (`refresh/3'), so while it holds any slot of
%% a process it holds the newest one stored since the node started.
hot_head(ProcID, RawOpts) ->
    % The key alone: the state is not copied out.
    case hot_slots(lib_process:cache_opts(RawOpts)) > 0 andalso
            (try ets:prev(?HOT, {ProcID, infinity}) catch error:badarg -> none end) of
        {ProcID, Slot} -> {ok, Slot};
        _ -> not_found
    end.

%% @doc The newest hot slot of a process, at or below `Limit'.
hot_latest(ProcID, Limit, Opts) ->
    case hot_slots(Opts) of
        0 -> not_found;
        _ -> hot_latest(ProcID, Limit)
    end.

hot_latest(ProcID, Limit) ->
    Upper = case Limit of undefined -> infinity; _ -> Limit + 1 end,
    try ets:prev(?HOT, {ProcID, Upper}) of
        {ProcID, Slot} = Key ->
            case ets:lookup(?HOT, Key) of
                [{_, Msg}] -> {ok, Slot, Msg};
                [] -> not_found
            end;
        _ -> not_found
    catch error:badarg -> not_found
    end.

hot_slots(Opts) ->
    case hb_opts:get(<<"process-delta-cache">>, false, Opts) of
        true ->
            case hb_util:int(hb_opts:get(<<"process-hot-cache-slots">>, 8, Opts)) of
                N when N >= 0 -> N;
                _ -> 8
            end;
        _ -> 0
    end.

%% The table is owned by a process that never exits: a table dies with its
%% owner, and the first writer is usually a short-lived request.
ensure_hot_table() ->
    case ets:whereis(?HOT) of
        undefined ->
            Parent = self(),
            Ref = make_ref(),
            {Owner, Mon} =
                spawn_monitor(
                    fun() ->
                        try ets:new(?HOT, [named_table, public, ordered_set,
                                {read_concurrency, true}]) of
                            _ ->
                                Parent ! {Ref, created},
                                receive after infinity -> ok end
                        catch error:badarg -> Parent ! {Ref, exists}
                        end
                    end
                ),
            receive
                {Ref, _} -> ok;
                {'DOWN', Mon, process, Owner, _} -> ok
            after 5000 -> ok
            end,
            erlang:demonitor(Mon, [flush]),
            ok;
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
    Opts = lib_process:scoped_opts(RawOpts),
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
    case hot_latest(ProcID, Limit, Opts) of
        {ok, HotSlot, HotMsg} ->
            case path_exists(RequiredPath, HotMsg, Opts) of
                true -> {ok, HotSlot, HotMsg};
                false -> latest_stored(ProcID, RequiredPath, Limit, Opts)
            end;
        not_found when RequiredPath == [], Limit == undefined ->
            latest_filled(ProcID, Opts);
        not_found -> latest_stored(ProcID, RequiredPath, Limit, Opts)
    end.

%% @doc The newest stored state, kept hot once read. Reading it materializes
%% the slot -- a delta slot replays every slot back to its checkpoint -- and
%% nothing but a computed slot used to fill the hot cache, so a process that
%% was not computing since a restart paid that whole replay on every `/now'
%% read. One reader per process materializes it; the others, queued behind
%% it, find it hot. A state with a limit or a required path is not the newest
%% one, so it is never put in the hot cache, whose newest slot is the head.
latest_filled(ProcID, Opts) ->
    case hot_slots(Opts) of
        0 -> latest_stored(ProcID, [], undefined, Opts);
        _ ->
            hb_name:atomic(
                {?MODULE, latest, ProcID},
                fun() ->
                    case hot_latest(ProcID, undefined) of
                        {ok, _, _} = Hot -> Hot;
                        not_found ->
                            case latest_stored(ProcID, [], undefined, Opts) of
                                {ok, Slot, Msg} = Found ->
                                    hot_put(ProcID, Slot, Msg, Opts),
                                    Found;
                                Other -> Other
                            end
                    end
                end
            )
    end.

%% @doc The latest stored slot at or below `Limit' that has the path. Only a
%% checkpoint with a VM snapshot can satisfy `snapshot', so for it a page of
%% the newest `s' markers at or below `Limit' is listed (retention keeps every
%% one, so they are never listed whole); any other path pages down the stored
%% slots from `Limit', so a process that keeps days of slots is never listed
%% whole.
latest_stored(ProcID, RequiredPath, Limit, Opts) ->
    case {RequiredPath, blob_store(Opts)} of
        {_, none} ->
            latest_in(ProcID, RequiredPath, Limit, legacy_slots(ProcID, Opts), Opts);
        {[<<"snapshot">>], Store} ->
            latest_in(ProcID, RequiredPath, Limit,
                legacy_slots(ProcID, Opts) ++
                    blob_rows(ProcID, <<"s">>, Store, Limit, desc, ?LATEST_PAGE), Opts);
        {_, Store} ->
            latest_paged(ProcID, RequiredPath, Limit, legacy_slots(ProcID, Opts), Store, Opts)
    end.

%% Pages of the newest `?LATEST_PAGE' `d' and `f' slots at or below `Hi', down
%% to the highest slot below which a page may hold more: a slot of any kind
%% above that cut is in one of the pages.
latest_paged(ProcID, RequiredPath, Hi, Legacy, Store, Opts) ->
    Pages = [ blob_rows(ProcID, K, Store, Hi, desc, ?LATEST_PAGE) || K <- [<<"d">>, <<"f">>] ],
    Cut =
        case [ lists:last(Pg) || Pg <- Pages, length(Pg) == ?LATEST_PAGE ] of
            [] -> none;
            Lows -> lists:max(Lows)
        end,
    Candidates =
        lists:reverse(lists:usort(
            [ S || S <- lists:append(Pages) ++ Legacy,
                   Hi == undefined orelse S =< Hi,
                   Cut == none orelse S >= Cut ])),
    case first_with_path(ProcID, RequiredPath, Candidates, Opts) of
        not_found when Cut =/= none ->
            latest_paged(ProcID, RequiredPath, Cut - 1, Legacy, Store, Opts);
        not_found -> {error, not_found};
        Found -> Found
    end.

latest_in(ProcID, RequiredPath, Limit, AllSlots, Opts) ->
    Path = path(ProcID, slot_root, Opts),
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
        {ok, SlotNum, Msg} ->
            {ok, SlotNum, Msg}
    end.

%% @doc Find the latest assignment with the requested path suffix.
first_with_path(_ProcID, _Required, [], _Opts) ->
    not_found;
first_with_path(ProcID, [], [Slot | Rest], Opts) ->
    read_candidate(ProcID, [], Slot, Rest, Opts);
first_with_path(ProcID, RequiredPath, [Slot | Rest], Opts) ->
    read_candidate(ProcID, RequiredPath, Slot, Rest, Opts).

read_candidate(ProcID, RequiredPath, Slot, Rest, Opts) ->
    RawPath = path(ProcID, Slot, Opts),
    ?event({trying_slot, {slot, Slot}, {path, RawPath}}),
    Read =
        case blob_read(ProcID, Slot, Opts) of
            {ok, Blob} -> {ok, Blob};
            _ -> hb_cache:read(RawPath, Opts)
        end,
    case Read of
        {error, not_found} ->
            first_with_path(ProcID, RequiredPath, Rest, Opts);
        {failure, _} = Failure ->
            Failure;
        {error, _} = Error ->
            Error;
        {ok, Stored} ->
            case materialize(ProcID, Stored, Opts) of
                {ok, Msg} ->
                    case path_exists(RequiredPath, Msg, Opts) of
                        true -> {ok, Slot, Msg};
                        false ->
                            first_with_path(ProcID, RequiredPath, Rest, Opts)
                    end;
                Error -> Error
            end
    end.

latest_slot(ProcID, Opts) ->
    case read_refreshed_at(ProcID, Opts) of
        {ok, Slot, _} -> {ok, Slot};
        _ ->
            case edge_slots(ProcID, desc, Opts) of
                [] -> {error, not_found};
                Slots -> {ok, lists:max(Slots)}
            end
    end.

%% The highest (`desc') or lowest (`asc') stored slot of each layout: the
%% legacy one, and the first `d' and `f' row from that end.
edge_slots(ProcID, Direction, Opts) ->
    legacy_slots(ProcID, Opts) ++
        case blob_store(Opts) of
            none -> [];
            Store ->
                lists:append(
                    [ blob_rows(ProcID, K, Store, undefined, Direction, 1)
                    || K <- [<<"d">>, <<"f">>] ])
        end.

legacy_slots(ProcID, Opts) ->
    hb_cache:list_numbered(path(ProcID, slot_root, Opts), Opts).

%%% Process state blobs (`process-state-blobs').
%%%
%%% RC7 writes a computed slot through `hb_cache:write': content-addressed,
%%% every field its own row, shared with any other message holding the same
%%% value (a checkpoint and the process definition share the module), so no
%%% row of it can be deleted without proving nothing else uses it. With
%%% `process-state-blobs => true', a slot of a delta process is one row in a
%%% namespace owned by that process and slot, shared with nothing:
%%%
%%%   pstate/<proc>/d/<slot>   the delta envelope (patches + results)
%%%   pstate/<proc>/f/<slot>   a full public state (a checkpoint)
%%%   pstate/<proc>/s/<slot>   marker, written after `f': that state carries
%%%                            a VM snapshot, so a worker can resume from it.
%%%                            It holds the slot's assignment timestamp (ms,
%%%                            when known), the build that wrote it and the
%%%                            SHA-256 of the `f' row, so retention dates a
%%%                            checkpoint without reading its assignment.
%%%                            Markers written before carry nothing (empty).
%%%
%%% Slots are zero-padded to 12 digits. Values are `term_to_binary' of fully
%%% loaded terms, compressed. `hb_store_retention' deletes rows of this
%%% namespace, and only of it, below the newest checkpoints it keeps.
%%% A full-state process (no delta: `lua@5.3a', WASM, native devices) writes
%%% every slot as an `f' row. Negative slots keep RC7's layout.
%%%
%%% A value longer than `?BLOB_CHUNK' bytes is written as chunks of at most
%%% that size, `<key>.<n>' for n = 1.., and then `<key>' itself holding
%%% `?CHUNKED' and the chunk count. LMDB stores a value above half a page on
%%% its own run of contiguous pages and can reuse a freed run only for a value
%%% that fits it: rewriting a 60 KB state every slot and deleting old ones left
%%% the file growing with most of its pages free. A chunk fits one page, and
%%% any free page takes it.

-define(BLOB_NS, <<"pstate">>).
-define(BLOB_VSN, 1).
-define(BLOB_CHUNK, 4000).
%% Chunks of a value written together (~256 KB), each write a separate hold
%% of the store's write lock, with a pause after it (`blob_put/4'): an 81 MB
%% row is ~320 writes and ~0.3 s of pauses.
-define(CHUNKS_PER_WRITE, 64).
-define(CHUNK_PAUSE_MS, 1).
-define(CHUNKED, "pstate-chunks:").

blob_store(Opts) ->
    case hb_util:atom(hb_opts:get(<<"process-state-blobs">>, false, Opts)) of
        true ->
            Stores =
                hb_store:normalize_stores(
                    case hb_opts:get(<<"store">>, [], Opts) of
                        M when is_map(M) -> [M];
                        L when is_list(L) -> L;
                        _ -> []
                    end),
            case [ S || S = #{ <<"store-module">> := hb_store_lmdb } <- Stores,
                        not is_map_key(<<"access">>, S),
                        maps:get(<<"read-only">>, S, false) =/= true ] of
                [Store | _] -> Store;
                [] -> none
            end;
        _ -> none
    end.

blob_key(ProcID, Kind, Slot) ->
    <<(blob_prefix(ProcID, Kind))/binary, "/", (pad_slot(Slot))/binary>>.

blob_prefix(ProcID, Kind) ->
    <<?BLOB_NS/binary, "/", (hb_util:human_id(ProcID))/binary, "/", Kind/binary>>.

pad_slot(Slot) ->
    iolist_to_binary(io_lib:format("~12..0B", [Slot])).

blob_put(Store, Key, Value, Opts) when byte_size(Value) =< ?BLOB_CHUNK ->
    blob_write(Store, #{ Key => Value }, Key, Opts);
blob_put(Store, Key, Value, Opts) ->
    Chunks = chunks(Value),
    % The chunks first, so a key that names chunks always has them; a few
    % hundred to a write, as one write holds the store's write lock for all
    % of its rows: an 81 MB checkpoint (20,000 chunks) in one write held it
    % for seconds (checkpoint_put p90 4.2 s on the game's 8 shards at
    % 100,000 players), and every slot of every process waited behind it.
    % A checkpoint writer (low priority) pauses after each write, leaving the
    % lock to the slots waiting on it: nothing else stops it taking the lock
    % straight back. A slot writing its own row does not.
    Pause = erlang:process_info(self(), priority) == {priority, low},
    lists:foreach(
        fun(Group) ->
            ok = blob_write(Store, maps:from_list(Group), Key, Opts),
            Pause andalso timer:sleep(?CHUNK_PAUSE_MS)
        end,
        groups(
            [ {chunk_key(Key, N), C}
            || {N, C} <- lists:zip(lists:seq(1, length(Chunks)), Chunks) ],
            ?CHUNKS_PER_WRITE)
    ),
    blob_write(
        Store,
        #{ Key => <<?CHUNKED, (integer_to_binary(length(Chunks)))/binary>> },
        Key,
        Opts
    ).

blob_write(Store, Rows, Key, Opts) ->
    case hb_store:write([Store], Rows, Opts) of
        ok -> ok;
        Error -> erlang:error({process_state_blob_write_failed, Key, Error})
    end.

chunks(<<C:?BLOB_CHUNK/binary, Rest/binary>>) when Rest =/= <<>> -> [C | chunks(Rest)];
chunks(Last) -> [Last].

groups([], _N) -> [];
groups(List, N) when length(List) =< N -> [List];
groups(List, N) ->
    {Group, Rest} = lists:split(N, List),
    [Group | groups(Rest, N)].

chunk_key(Key, N) -> <<Key/binary, ".", (integer_to_binary(N))/binary>>.

blob_get(Store, Key, Opts) ->
    case blob_raw(Store, Key, Opts) of
        {ok, <<?CHUNKED, Count/binary>>} ->
            Parts =
                [ blob_raw(Store, chunk_key(Key, N), Opts)
                || N <- lists:seq(1, binary_to_integer(Count)) ],
            case lists:all(fun(P) -> P =/= not_found end, Parts) of
                true -> {ok, iolist_to_binary([ B || {ok, B} <- Parts ])};
                false -> not_found
            end;
        Other -> Other
    end.

blob_raw(Store, Key, Opts) ->
    case hb_store:read([Store], #{ <<"read">> => Key }, Opts) of
        {ok, Bin} when is_binary(Bin) -> {ok, Bin};
        _ -> not_found
    end.

%% @doc Commit every buffered write of the store and delete rows of the blob
%% namespace, each with its chunks, in one LMDB transaction: a delete is never
%% durable without the writes made before it. Only `pstate/' rows, ever.
blob_commit(Store, Keys, Opts) ->
    Doomed =
        lists:flatmap(
            fun(Key = <<"pstate/", _/binary>>) ->
                case blob_raw(Store, Key, Opts) of
                    {ok, <<?CHUNKED, Count/binary>>} ->
                        [Key | [ chunk_key(Key, N)
                               || N <- lists:seq(1, binary_to_integer(Count)) ]];
                    {ok, _} -> [Key];
                    not_found -> []
                end
            end,
            Keys
        ),
    #{ <<"db">> := DB } = hb_store:find(Store),
    {ok, _} = elmdb:delete_batch(DB, Doomed),
    ok.

%% @doc The stored form of a slot from the blob namespace: its full state when
%% there is one, else its delta envelope.
blob_read(ProcID, Slot, Opts) when is_integer(Slot), Slot >= 0 ->
    case blob_store(Opts) of
        none -> not_found;
        Store ->
            case blob_get(Store, blob_key(ProcID, <<"f">>, Slot), Opts) of
                {ok, Full} -> {ok, blob_decode(Full)};
                not_found ->
                    case blob_get(Store, blob_key(ProcID, <<"d">>, Slot), Opts) of
                        {ok, Delta} -> {ok, blob_decode(Delta)};
                        not_found -> not_found
                    end
            end
    end;
blob_read(_ProcID, _Slot, _Opts) ->
    not_found.

blob_exists(ProcID, Slot, Opts) ->
    case blob_store(Opts) of
        none -> false;
        Store ->
            blob_raw(Store, blob_key(ProcID, <<"d">>, Slot), Opts) =/= not_found
                orelse blob_raw(Store, blob_key(ProcID, <<"f">>, Slot), Opts) =/= not_found
    end.

%% @doc Up to `N' row slots of `Kind' a process holds, from `From' (a slot,
%% inclusive, or `undefined' for the end) in `Direction', skipping the
%% chunks of rows: a bounded cursor listing, never every row.
blob_rows(_ProcID, _Kind, _Store, From, desc, _N) when is_integer(From), From < 0 -> [];
blob_rows(ProcID, Kind, Store, From, Direction, N) ->
    blob_rows(ProcID, Kind, Store,
        case From of undefined -> none; _ -> pad_slot(max(0, From)) end, none, Direction, N, []).
%% `Seen' is the name a page continues from: the last of the page before.
blob_rows(ProcID, Kind, Store, From, Seen, Direction, N, Acc) ->
    Req = #{ <<"list">> => blob_prefix(ProcID, Kind), <<"limit">> => ?LIST_PAGE,
             <<"direction">> => Direction },
    Page =
        case hb_store:list([Store], case From of none -> Req; _ -> Req#{ <<"from">> => From } end, #{}) of
            {ok, Names} -> Names;
            _ -> []
        end,
    Rows = [ I || Name <- Page, Name =/= Seen, I <- [catch binary_to_integer(Name)], is_integer(I) ],
    Acc1 = Acc ++ lists:sublist(Rows, N - length(Acc)),
    case length(Acc1) >= N orelse length(Page) < ?LIST_PAGE of
        true -> Acc1;
        false ->
            Last = lists:last(Page),
            blob_rows(ProcID, Kind, Store, Last, Last, Direction, N, Acc1)
    end.

%% @doc The slots of `Kind' a process holds, ascending. Every one of them:
%% only for the kinds a process holds few of (`s', `o').
blob_slots(ProcID, Kind, Store) ->
    case hb_store:list([Store], blob_prefix(ProcID, Kind), #{}) of
        {ok, Names} ->
            lists:sort([ binary_to_integer(N) || N <- Names,
                         is_integer(catch binary_to_integer(N)) ]);
        _ -> []
    end.

blob_encode(Term) ->
    T0 = erlang:monotonic_time(microsecond),
    Bin = term_to_binary({?BLOB_VSN, Term}, [{compressed, 1}]),
    % A slot's store time, split: encoding a row (`store_encode'), and a
    % whole checkpoint row (`store_checkpoint', `full_blob/5').
    hb_slot_log:note(store_encode, erlang:monotonic_time(microsecond) - T0),
    Bin.
blob_decode(Bin) ->
    T0 = erlang:monotonic_time(microsecond),
    {?BLOB_VSN, Term} = binary_to_term(Bin),
    hb_slot_log:note(store_decode, erlang:monotonic_time(microsecond) - T0),
    Term.

%% @doc Write a delta process's slot as one row. A checkpoint is the whole
%% public state; one carrying a VM snapshot also gets its `s' marker, written
%% after the state so a marker never names a state that is not there.
write_blob(ProcID, Slot, Msg, Patches, Results, Store, Opts) ->
    LoadedResults = hb_cache:ensure_all_loaded(Results, Opts),
    Key =
        case checkpoint_due(ProcID, Slot, Msg, Opts) of
            false -> write_delta_blob(ProcID, Slot, Patches, LoadedResults, Store, Opts);
            true ->
                case Slot > 0 andalso async_ok(Opts)
                        andalso base_exists(ProcID, Slot - 1, Opts) of
                    true ->
                        D = write_delta_blob(ProcID, Slot, Patches, LoadedResults, Store, Opts),
                        checkpoint_async(ProcID, Slot, Msg, Store, Opts),
                        D;
                    false -> full_blob(ProcID, Slot, Msg, Store, Opts)
                end
        end,
    ok = refresh(ProcID, Slot, Opts),
    {ok, Key}.

write_delta_blob(ProcID, Slot, Patches, LoadedResults, Store, Opts) ->
    Envelope = #{
        <<"cache-format">> => ?DELTA_FORMAT,
        <<"slot">> => Slot,
        <<"base-slot">> => Slot - 1,
        <<"patches">> => hb_cache:ensure_all_loaded(Patches, Opts),
        <<"results">> => LoadedResults
    },
    K = blob_key(ProcID, <<"d">>, Slot),
    ok = blob_put(Store, K, blob_encode(Envelope), Opts),
    K.

%%% Checkpoints off the slot.
%%%
%%% A checkpoint slot of a delta process with its previous slot stored is
%%% written as a delta like any other, so the chain is whole the moment the
%%% slot returns, and its full state (`f', and `s' with a VM snapshot) is
%%% written by a process of its own: the next slots no longer wait for it.
%%% On a dedicated 16-core box that write was 3-4 s of a wasm-patch pair's
%%% 64 MB image (Hetzner, 2026-11-24), with the pair's next ~100 messages
%%% queued behind it. Until the full row is on disk the slot reads as its
%%% delta, back to the previous checkpoint; a crash before it is written
%%% loses only the shortcut, as a restart then restores from the previous
%%% snapshot and replays. Writers run at low priority, a bounded number at
%%% once, stalest first (`checkpointer/0'): large-state processes made every
%%% overlapping writer a multi-GB encode at once (8 game shards of 100,000
%%% players: the round trip p50 of every shard rose 33 -> 418 ms).
%%% `process-async-checkpoints' false writes them in the slot.

%% @doc Whether checkpoints of a process run with `RawOpts' are written off
%% the slot (`dev_process' then leaves a VM snapshot uncompressed).
async_checkpoints(RawOpts) -> async_ok(lib_process:cache_opts(RawOpts)).

async_ok(Opts) ->
    blob_store(Opts) =/= none andalso
        hb_util:atom(hb_opts:get(<<"process-async-checkpoints">>, true, Opts)) =/= false.

checkpoint_async(ProcID, Slot, Msg, Store, Opts) ->
    Checkpointer = checkpointer(),
    % The writer is spawned here, with the slot's state, and waits for its
    % turn (`checkpointer/0'): told to go, it writes the row; superseded by a
    % newer checkpoint of its process before its turn came, it exits.
    Writer = spawn(fun() ->
        Mon = erlang:monitor(process, Checkpointer),
        receive
            go ->
                process_flag(priority, low),
                Result =
                    try full_blob(ProcID, Slot, Msg, Store, Opts), ok
                    catch Class:Reason ->
                        ?event(warning, {checkpoint_failed, {proc_id, ProcID}, {slot, Slot},
                            {class, Class}, {reason, Reason}}),
                        failed
                    end,
                exit({checkpoint, Result});
            skip -> ok;
            {'DOWN', Mon, process, _, _} -> ok
        end
    end),
    Checkpointer ! {due, ProcID, Slot, Writer, checkpoint_slots(Opts), checkpoint_writers(Opts)},
    ok.

%% Writers that may run at once: `process-checkpoint-writers', default 1. Two
%% on the game's 8 shards at 100,000 players stored every checkpoint but
%% contended for the store's write lock: round trip p95 37 -> 624 ms,
%% lmdb_commit 3.9 -> 12.4 ms.
checkpoint_writers(Opts) ->
    case hb_util:int(hb_opts:get(<<"process-checkpoint-writers">>, 1, Opts)) of
        N when N >= 1 -> N;
        _ -> 1
    end.

%% @doc Wait until a process has no checkpoint waiting or being written.
await_checkpoint(ProcID) ->
    case whereis(?CHECKPOINTER) of
        undefined -> ok;
        Pid ->
            Pid ! {await, ProcID, self()},
            receive {checkpoint_idle, ProcID} -> ok
            after 120000 -> {error, timeout}
            end
    end.

%%% The checkpointer: one process on the node that decides which waiting
%%% checkpoint is written next. Every process keeps at most its newest
%%% checkpoint waiting (a newer one supersedes it: only the newest state is
%%% worth writing). At most `process-checkpoint-writers' (1) are written at once,
%%% one per process, the stalest first: the process with the most slots since
%%% its last stored checkpoint -- what a restart would replay -- and before
%%% all of those, any past three cadences without one. Its last stored slot
%%% is what it wrote since the node started (unknown: none, so a process it
%%% has not seen is taken as stale). On the game's 8 shards at 100,000
%%% players one writer skipping what came due while it was busy stored 10 of
%%% 23 snapshots in 150 s, and a skipped process could go without one for
%%% any length of time.
checkpointer() ->
    case whereis(?CHECKPOINTER) of
        undefined ->
            Pid = spawn(fun() ->
                try register(?CHECKPOINTER, self()) of
                    true ->
                        checkpointer_loop(#{ pending => #{}, running => #{},
                            busy => #{}, last => #{}, writers => 1, waiters => [] })
                catch error:badarg -> ok
                end
            end),
            Ref = erlang:monitor(process, Pid),
            receive {'DOWN', Ref, process, Pid, _} -> ok
            after 50 -> erlang:demonitor(Ref, [flush])
            end,
            checkpointer();
        Pid -> Pid
    end.

checkpointer_loop(S = #{ pending := Pending, running := Running, busy := Busy,
        last := Last, waiters := Waiters }) ->
    receive
        {due, ProcID, Slot, Writer, Cadence, Writers} ->
            case maps:find(ProcID, Pending) of
                {ok, {Older, _, _}} -> Older ! skip;
                error -> ok
            end,
            checkpointer_loop(start_checkpoints(S#{
                pending := Pending#{ ProcID => {Writer, Slot, Cadence} },
                writers := Writers }));
        {'DOWN', Ref, process, _, Reason} ->
            case maps:take(Ref, Running) of
                {{ProcID, Slot}, Running1} ->
                    Last1 =
                        case Reason of
                            {checkpoint, ok} ->
                                Last#{ ProcID => max(Slot, maps:get(ProcID, Last, -1)) };
                            _ -> Last
                        end,
                    S1 = S#{ running := Running1, busy := maps:remove(ProcID, Busy), last := Last1 },
                    checkpointer_loop(start_checkpoints(answer_waiters(S1)));
                error -> checkpointer_loop(S)
            end;
        {await, ProcID, From} ->
            checkpointer_loop(answer_waiters(S#{ waiters := [{ProcID, From} | Waiters] }));
        {status, From} ->
            From ! {checkpoint_status, maps:without([waiters], S)},
            checkpointer_loop(S)
    end.

%% Start the stalest waiting checkpoints while there are free writers.
start_checkpoints(S = #{ pending := Pending, running := Running, busy := Busy,
        last := Last, writers := Writers }) ->
    Free = Writers - map_size(Running),
    Ready =
        lists:reverse(lists:sort(
            [ {{Stale > 3 * Cadence, Stale}, ProcID}
            || {ProcID, {_Writer, Slot, Cadence}} <- maps:to_list(Pending),
               not is_map_key(ProcID, Busy),
               Stale <- [Slot - maps:get(ProcID, Last, -1)] ])),
    lists:foldl(
        fun({{_, Stale}, ProcID}, Acc = #{ pending := P, running := R, busy := B }) ->
            {Writer, Slot, _} = maps:get(ProcID, P),
            hb_phase_timer:record(checkpoint_staleness, Stale),
            Ref = erlang:monitor(process, Writer),
            Writer ! go,
            Acc#{ pending := maps:remove(ProcID, P), running := R#{ Ref => {ProcID, Slot} },
                busy := B#{ ProcID => true } }
        end,
        S,
        lists:sublist(Ready, max(0, Free))
    ).

answer_waiters(S = #{ pending := Pending, busy := Busy, waiters := Waiters }) ->
    {Done, Waiting} =
        lists:partition(
            fun({ProcID, _}) ->
                not is_map_key(ProcID, Pending) andalso not is_map_key(ProcID, Busy)
            end,
            Waiters),
    [ From ! {checkpoint_idle, ProcID} || {ProcID, From} <- Done ],
    S#{ waiters := Waiting }.

%% @doc Write a slot of a full-state process (one without patches: `lua@5.3a',
%% WASM, native devices) as one full-state row. Each row stands alone, so
%% retention can drop any of them but the ones it keeps.
write_full_blob(ProcID, Slot, Msg, Store, Opts) ->
    Key = full_blob(ProcID, Slot, Msg, Store, Opts),
    ok = refresh(ProcID, Slot, Opts),
    {ok, Key}.

full_blob(ProcID, Slot, Msg, Store, Opts) ->
    T0 = erlang:monotonic_time(microsecond),
    K = full_blob_row(ProcID, Slot, Msg, Store, Opts),
    hb_slot_log:note(store_checkpoint, erlang:monotonic_time(microsecond) - T0),
    K.

full_blob_row(ProcID, Slot, Msg, Store, Opts) ->
    Loaded = hb_cache:ensure_all_loaded(hb_private:reset(Msg), Opts),
    K = blob_key(ProcID, <<"f">>, Slot),
    Encoded = blob_encode(Loaded),
    hb_slot_log:note(checkpoint_bytes, byte_size(Encoded)),
    TP = erlang:monotonic_time(microsecond),
    ok = blob_put(Store, K, Encoded, Opts),
    hb_slot_log:note(checkpoint_put, erlang:monotonic_time(microsecond) - TP),
    case is_map_key(<<"snapshot">>, Loaded) of
        true -> ok = blob_put(Store, blob_key(ProcID, <<"s">>, Slot), marker(Encoded, Opts), Opts);
        false -> ok
    end,
    K.

%% The value of a slot's `s' marker. The assignment timestamp is the one
%% `dev_process' hands the write in `process-assignment-timestamp'; it is
%% read back by `hb_store_retention:marker_time/1'.
marker(Encoded, Opts) ->
    blob_encode(maps:from_list(
        [ {<<"timestamp">>, Ms}
        || Ms <- [maps:get(<<"process-assignment-timestamp">>, Opts, undefined)],
           is_integer(Ms) ] ++
        [ {<<"build">>, build()}, {<<"state-digest">>, crypto:hash(sha256, Encoded)} ])).

%% The build's short source hash, from the build info `dev_meta' reports.
build() ->
    case persistent_term:get({?MODULE, build}, undefined) of
        undefined ->
            Build =
                try file:consult(filename:join(code:priv_dir(hb), "hb_buildinfo")) of
                    {ok, [#{ <<"source-short">> := Source }]} -> hb_util:bin(Source);
                    _ -> <<"unknown">>
                catch _:_ -> <<"unknown">>
                end,
            persistent_term:put({?MODULE, build}, Build),
            Build;
        Build -> Build
    end.

%%% A slot's outbox, kept apart from its state (the `o' row).
%%%
%%% A slot whose results carry an outbox writes it, unlowered, as
%%%
%%%   pstate/<proc>/o/<slot>   #{ lower => boolean(), outbox => Raw }
%%%
%%% in the delivery store (`delivery_store/1'), in the same buffered batch as
%%% the slot's state row and before it. That costs the worker one map get and,
%%% only when there is an outbox, one buffered put: lowering, signing and
%%% posting are the deliverer's (`lib_process_delivery'), which owns the
%%% slot's `q' and `x' rows and deletes `o' once `x' exists. Retention never
%%% deletes an `o' row without its `x' (`hb_store_retention').

%% @doc Write a slot's outbox to its `o' row when it has one. Returns whether
%% a row was written. A delta state's results are raw (`lua-patch@1.0' hands
%% them over unlowered; `hb_state_patch' lowers them when it applies the
%% slot), so the row records whether the deliverer has to lower them.
put_outbox(ProcID, Slot, Msg, Opts) when is_integer(Slot), Slot >= 0 ->
    {Lower, Results} = slot_results(Msg, Opts),
    % Only the node that delivers the process's outbox keeps it: on any
    % other a row would never be delivered, and never deleted.
    case outbox_of(Results, Opts) =/= none andalso
            lib_process_delivery:delivers(ProcID, Opts) andalso
            outbox_of(Results, Opts) of
        false -> false;
        Raw ->
            case delivery_store(Opts) of
                none ->
                    ?event(warning, {no_delivery_store, {process, ProcID}, {slot, Slot}}),
                    false;
                Store ->
                    ok =
                        blob_put(
                            Store,
                            blob_key(ProcID, <<"o">>, Slot),
                            blob_encode(#{
                                <<"lower">> => Lower,
                                <<"outbox">> => hb_cache:ensure_all_loaded(Raw, Opts)
                            }),
                            Opts
                        ),
                    true
            end
    end;
put_outbox(_ProcID, _Slot, _Msg, _Opts) ->
    false.

%% @doc Whether a computed state has an outbox that `write/4' stored in an
%% `o' row: the one test the worker makes before it casts the slot to the
%% process's deliverer (`lib_process_delivery:notify/3').
has_outbox(ProcID, Msg, RawOpts) ->
    Opts = lib_process:cache_opts(RawOpts),
    {_Lower, Results} = slot_results(Msg, Opts),
    outbox_of(Results, Opts) =/= none andalso delivery_store(Opts) =/= none
        andalso lib_process_delivery:delivers(ProcID, Opts).

%% @doc A state's results as `write/4' stores them: raw from its delta (to be
%% lowered), or as its public state holds them.
slot_results(Msg, Opts) ->
    case delta(Msg, Opts) of
        {ok, _Patches, DeltaResults} -> {true, DeltaResults};
        not_found ->
            {false, hb_maps:get(<<"results">>, hb_private:reset(Msg), #{}, Opts)}
    end.

%% @doc The outbox of a slot's results, or `none' when it has none.
outbox_of(Results, Opts) when is_map(Results) ->
    case hb_maps:get(<<"outbox">>, Results, none, Opts) of
        none ->
            % A delta's raw results keep the key as the script spelled it
            % (`Outbox'); lowering normalizes it.
            case [ V || {K, V} <- maps:to_list(Results), is_binary(K),
                        K =/= <<"outbox">>, string:lowercase(K) == <<"outbox">> ] of
                [Raw | _] -> outbox_of(#{ <<"outbox">> => Raw }, Opts);
                [] -> none
            end;
        [] -> none;
        Outbox when is_map(Outbox) ->
            case ?IS_EMPTY_MESSAGE(hb_private:reset(Outbox)) of
                true -> none;
                false -> Outbox
            end;
        Outbox when is_list(Outbox) -> Outbox;
        _ -> none
    end;
outbox_of(_Results, _Opts) ->
    none.

%% @doc The entries of a decoded `o' row, as the slot's state holds its
%% outbox: lowered when the row says so, private keys dropped, keys
%% normalized and every value loaded.
outbox_entries(#{ <<"lower">> := Lower, <<"outbox">> := Raw }, Opts) ->
    Lowered =
        case Lower of
            true ->
                case hb_state_patch:apply(#{}, [], #{ <<"outbox">> => Raw }, Opts) of
                    {ok, #{ <<"results">> := #{ <<"outbox">> := L } }} -> L;
                    _ -> Raw
                end;
            false -> Raw
        end,
    case Lowered of
        List when is_list(List) ->
            hb_cache:ensure_all_loaded(hb_ao:normalize_keys(List), Opts);
        Map when is_map(Map) ->
            hb_cache:ensure_all_loaded(
                hb_ao:normalize_keys(hb_private:reset(Map)), Opts);
        _ -> #{}
    end.

%% @doc The store the delivery rows (`o', `q', `x') and the head marker live
%% in: the node's first writable LMDB store, whether or not slots are stored
%% as blobs. One store, so one flush makes a slot's rows durable together
%% and LMDB's ordered commits keep them in write order. A node with no
%% writable LMDB store has none, and delivers nothing (`lib_process_delivery'),
%% and so does a computation whose options say `process-delivery' false: a
%% bridge replica's, in its own isolated stores (`lib_bridge_replica').
delivery_store(Opts) ->
    case hb_opts:get(<<"process-delivery">>, true, Opts) of
        false -> none;
        _ -> first_lmdb_store(Opts)
    end.

first_lmdb_store(Opts) ->
    Stores =
        hb_store:normalize_stores(
            case hb_opts:get(<<"store">>, [], Opts) of
                M when is_map(M) -> [M];
                L when is_list(L) -> L;
                _ -> []
            end),
    case [ S || S = #{ <<"store-module">> := hb_store_lmdb } <- Stores,
                not is_map_key(<<"access">>, S),
                maps:get(<<"read-only">>, S, false) =/= true ] of
        [Store | _] -> Store;
        [] -> none
    end.

%% @doc A delta envelope's results as the state of their slot holds them:
%% `hb_state_patch' lowers them when it applies the slot.
delta_results(Results, Opts) when is_map(Results) ->
    case hb_state_patch:apply(#{}, [], Results, Opts) of
        {ok, #{ <<"results">> := Lowered }} -> Lowered;
        _ -> Results
    end;
delta_results(Results, _Opts) ->
    Results.

%% @doc The results of a stored slot, read without materializing its state
%% where the store allows it: from the hot cache, the slot's delta envelope
%% or its full row. Never computes: `{error, not_found}' when nothing holds
%% the slot.
read_results(ProcID, Slot, RawOpts) when is_integer(Slot), Slot >= 0 ->
    Opts = lib_process:cache_opts(RawOpts),
    case hot_get(ProcID, Slot, Opts) of
        {ok, #{ <<"results">> := Results }} ->
            {ok, hb_cache:ensure_all_loaded(Results, Opts)};
        _ ->
            case blob_store(Opts) of
                none ->
                    case read(ProcID, Slot, Opts) of
                        {ok, State} ->
                            {ok,
                                hb_cache:ensure_all_loaded(
                                    hb_maps:get(<<"results">>, State, #{}, Opts),
                                    Opts
                                )};
                        _ -> {error, not_found}
                    end;
                Store -> stored_results(ProcID, Slot, Store, Opts)
            end
    end;
read_results(_ProcID, _Slot, _Opts) ->
    {error, not_found}.

stored_results(ProcID, Slot, Store, Opts) ->
    Get = fun(Kind) -> blob_get(Store, blob_key(ProcID, Kind, Slot), Opts) end,
    case Get(<<"d">>) of
        {ok, Delta} ->
            Envelope = blob_decode(Delta),
            {ok, delta_results(maps:get(<<"results">>, Envelope, #{}), Opts)};
        not_found ->
            case Get(<<"f">>) of
                {ok, Full} -> {ok, maps:get(<<"results">>, blob_decode(Full), #{})};
                not_found -> {error, not_found}
            end
    end.

%% @doc The lowest slot whose state a process has stored, or `none'. A
%% process whose lowest stored slot is above 0 had slots pruned, so it is
%% never initialized from its definition and replayed
%% (`dev_process:ensure_loaded_state/3').
%% @doc Whether retention has deleted any state row of the process
%% (`hb_store_retention:pruned_key/1').
pruned(ProcID, RawOpts) ->
    Opts = lib_process:scoped_opts(RawOpts),
    case blob_store(Opts) of
        none -> false;
        Store ->
            blob_raw(Store, hb_store_retention:pruned_key(hb_util:human_id(ProcID)), Opts)
                =/= not_found
    end.

stored_floor(ProcID, RawOpts) ->
    Opts = lib_process:scoped_opts(RawOpts),
    case [ S || S <- edge_slots(ProcID, asc, Opts), S >= 0 ] of
        [] -> none;
        Slots -> {ok, lists:min(Slots)}
    end.

path_exists([], _Msg, _Opts) ->
    true;
path_exists([Key], Msg, _Opts) when is_map(Msg) ->
    % For the last key, we only check that it is present. Its value does not
    % need to be loaded.
    is_map_key(Key, Msg);
path_exists([Key | Rest], Msg, Opts) ->
    case hb_maps:find(Key, Msg, Opts) of
        {ok, Next} -> path_exists(Rest, Next, Opts);
        error -> false
    end.

%%% Tests

%% @doc A slot stored as a delta reads back as the state that was computed,
%% from the hot cache and, with the hot cache empty, from the store; a
%% checkpoint is written at slot 0 and at the cadence.
delta_slots_read_back_test() ->
    Opts = #{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"process-delta-cache">> => true,
        <<"process-delta-checkpoint-slots">> => 3
    },
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    State0 = #{ <<"count">> => <<"0">>, <<"keep">> => <<"k">>, <<"at-slot">> => 0 },
    Step =
        fun(N, Prev) ->
            Patches = [#{ <<"path">> => <<"/count">>, <<"value">> => integer_to_binary(N) }],
            Results = #{ <<"output">> => #{ <<"data">> => integer_to_binary(N) } },
            {ok, Next} = hb_state_patch:apply(Prev, Patches, Results, N, Opts),
            Msg = Next#{ <<"priv">> => #{ <<"process-cache-delta">> =>
                #{ <<"patches">> => Patches, <<"results">> => Results } } },
            {ok, _} = write(ProcID, N, Msg, Opts),
            Next
        end,
    {ok, _} = write(ProcID, 0, State0, Opts),
    States = lists:foldl(
        fun(N, Acc = [Prev | _]) -> [Step(N, Prev) | Acc] end,
        [State0],
        lists:seq(1, 4)
    ),
    Expected = lists:reverse(States),
    Raw = fun(N) -> {ok, M} = hb_cache:read(path(ProcID, N, Opts), Opts), M end,
    % Slots 1, 2 and 4 are deltas; 0 and 3 (the cadence) are checkpoints.
    ?assertEqual(
        [false, true, true, false, true],
        [ maps:is_key(<<"cache-format">>, Raw(N)) || N <- lists:seq(0, 4) ]
    ),
    Read = fun(N) ->
        {ok, M} = read(ProcID, N, Opts),
        hb_cache:ensure_all_loaded(M, Opts)
    end,
    Same = fun(N) ->
        E = lists:nth(N + 1, Expected),
        maps:with([<<"count">>, <<"keep">>], E) =:=
            maps:with([<<"count">>, <<"keep">>], Read(N))
    end,
    ?assert(lists:all(Same, lists:seq(0, 4))),
    ets:delete_all_objects(?HOT),
    ?assert(lists:all(Same, lists:seq(0, 4))),
    {ok, 4, Latest} = latest(ProcID, Opts),
    ?assertEqual(<<"4">>, hb_maps:get(<<"count">>, hb_cache:ensure_all_loaded(Latest, Opts), Opts)).

%% @doc A stored slot's hot entry is its whole public state: another process
%% reads a recent slot, or its results, from the table, never from the
%% store, and so does any reader after its writer has gone.
hot_state_test() ->
    Opts = #{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"process-delta-cache">> => true,
        <<"process-state-blobs">> => true,
        <<"process-delta-checkpoint-slots">> => 100,
        <<"process-hot-cache-slots">> => 2
    },
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Self = self(),
    Writer = spawn(fun() ->
        State0 = #{ <<"count">> => <<"0">>, <<"at-slot">> => 0 },
        {ok, _} = write(ProcID, 0, State0, Opts),
        Loop = fun Loop(N, Prev) ->
            receive
                {step, From} ->
                    Patches = [#{ <<"path">> => <<"/count">>, <<"value">> => integer_to_binary(N) }],
                    Results = #{ <<"output">> => #{ <<"data">> => integer_to_binary(N) } },
                    {ok, Next} = hb_state_patch:apply(Prev, Patches, Results, N, Opts),
                    {ok, _} = write(ProcID, N,
                        Next#{ <<"priv">> => #{ <<"process-cache-delta">> =>
                            #{ <<"patches">> => Patches, <<"results">> => Results } } }, Opts),
                    From ! {stepped, N},
                    Loop(N + 1, Next);
                stop -> ok
            end
        end,
        Self ! ready,
        Loop(1, State0)
    end),
    receive ready -> ok end,
    Step = fun() -> Writer ! {step, Self}, receive {stepped, N} -> N end end,
    Data = fun(N) -> {ok, R} = read_results(ProcID, N, Opts), hb_maps:get(<<"data">>, hb_maps:get(<<"output">>, R, Opts), Opts) end,
    1 = Step(), 2 = Step(),
    ?assertMatch({ok, #{ <<"count">> := <<"1">> }}, hot_get(ProcID, 1)),
    ?assertMatch({ok, #{ <<"count">> := <<"2">> }}, hot_get(ProcID, 2)),
    ?assertEqual(<<"1">>, Data(1)),
    ?assertMatch({ok, 2, #{ <<"count">> := <<"2">> }}, latest(ProcID, Opts)),
    Writer ! stop,
    timer:sleep(50),
    ?assertMatch({ok, #{ <<"count">> := <<"2">> }}, hot_get(ProcID, 2)),
    % Past `process-hot-cache-slots', the store answers.
    ?assertEqual(not_found, hot_get(ProcID, 0)),
    {ok, Zero} = read(ProcID, 0, Opts),
    ?assertEqual(<<"0">>, hb_maps:get(<<"count">>, Zero, Opts)).

%% @doc With the hot cache empty (a restart, then no slot computed), the newest
%% stored state is materialized once and kept hot: the next `latest' -- every
%% `/now' read served from the cache -- does not replay the delta chain again.
%% Concurrent cold readers all get the head. A read below the head (a limit)
%% never puts its older state in the hot cache.
latest_fills_hot_cache_test() ->
    Opts = #{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"process-delta-cache">> => true,
        <<"process-delta-checkpoint-slots">> => 100
    },
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    State0 = #{ <<"count">> => <<"0">>, <<"at-slot">> => 0 },
    {ok, _} = write(ProcID, 0, State0, Opts),
    lists:foldl(
        fun(N, Prev) ->
            Patches = [#{ <<"path">> => <<"/count">>, <<"value">> => integer_to_binary(N) }],
            Results = #{ <<"output">> => #{ <<"data">> => integer_to_binary(N) } },
            {ok, Next} = hb_state_patch:apply(Prev, Patches, Results, N, Opts),
            {ok, _} = write(ProcID, N, Next#{ <<"priv">> => #{ <<"process-cache-delta">> =>
                #{ <<"patches">> => Patches, <<"results">> => Results } } }, Opts),
            Next
        end,
        State0,
        lists:seq(1, 4)
    ),
    Count = fun(Msg) ->
        hb_maps:get(<<"count">>, hb_cache:ensure_all_loaded(Msg, Opts), Opts)
    end,
    ets:delete_all_objects(?HOT),
    ?assertMatch({ok, 2, _}, latest(ProcID, [], 2, Opts)),
    ?assertEqual(not_found, hot_latest(ProcID, undefined)),
    {ok, 4, Latest} = latest(ProcID, Opts),
    ?assertEqual(<<"4">>, Count(Latest)),
    {ok, 4, Hot} = hot_latest(ProcID, undefined),
    ?assertEqual(<<"4">>, Count(Hot)),
    ets:delete_all_objects(?HOT),
    Concurrent =
        hb_pmap:parallel_map(lists:seq(1, 20), fun(_) -> latest(ProcID, Opts) end, 20),
    ?assert(lists:all(fun({ok, 4, M}) -> Count(M) =:= <<"4">>; (_) -> false end, Concurrent)),
    ?assertMatch({ok, 4, _}, hot_latest(ProcID, undefined)).


process_cache_suite_test_() ->
    hb_store:generate_test_suite(
        [
            {
                "write and read process outputs",
                fun(Store) ->
                    test_write_and_read_output(#{ <<"store">> => [Store] })
                end
            },
            {
                "find latest output (with path)",
                fun(Store) ->
                    find_latest_outputs(#{ <<"store">> => [Store] })
                end
            },
            {
                "honor max-age when checking process cache freshness",
                fun(Store) ->
                    freshness_max_age(#{ <<"store">> => [Store] })
                end
            }
        ],
        hb_store:test_stores()
    ).

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
    Store = hb_opts:get(store, no_viable_store, Opts),
    ResetRes = hb_store:reset(Store),
    ?event({reset_store, {result, ResetRes}, {store, Store}}),
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ProcessRef = <<"test-process-ref">>,
    % Create messages for the slots, with only the middle slot having a
    % `/Process' field, while the top slot has a `/Deep/Process' field.
    Msg0 = #{ <<"Results">> => #{ <<"Result-Number">> => 0 } },
    Base =
        #{ 
            <<"Results">> => #{ <<"Result-Number">> => 1 }, 
            <<"Process">> => ProcessRef
        },
    Req =
        #{ 
            <<"Results">> => #{ <<"Result-Number">> => 2 }, 
            <<"Deep">> => #{ <<"Process">> => ProcessRef }
        },
    % Write the messages to the cache.
    {ok, _} = write(ProcID, 0, Msg0, Opts),
    {ok, _} = write(ProcID, 1, Base, Opts),
    {ok, _} = write(ProcID, 2, Req, Opts),
    ?event(wrote_items),
    % Read the messages with various qualifiers.
    {ok, 2, RawReadReq} = latest(ProcID, Opts),
    ReadReq = hb_cache:ensure_all_loaded(RawReadReq, Opts),
    ?event({read_latest, ReadReq}),
    ?assertEqual(2, maps:get(<<"Result-Number">>, maps:get(<<"Results">>, ReadReq))),
    ?assertEqual(ProcessRef, maps:get(<<"Process">>, maps:get(<<"Deep">>, ReadReq))),
    ?event(read_latest_slot_without_qualifiers),
    {ok, 1, RawReadBaseRequired} = latest(ProcID, <<"Process">>, Opts),
    ReadBaseRequired = hb_cache:ensure_all_loaded(RawReadBaseRequired, Opts),
    ?event({read_latest_with_process, ReadBaseRequired}),
    ?assertEqual(
        1,
        maps:get(<<"Result-Number">>, maps:get(<<"Results">>, ReadBaseRequired))
    ),
    ?assertEqual(ProcessRef, maps:get(<<"Process">>, ReadBaseRequired)),
    ?event(read_latest_slot_with_shallow_key),
    {ok, 2, RawReadReqRequired} = latest(ProcID, <<"Deep/Process">>, Opts),
    ReadReqRequired = hb_cache:ensure_all_loaded(RawReadReqRequired, Opts),
    ?assertEqual(
        2,
        maps:get(<<"Result-Number">>, maps:get(<<"Results">>, ReadReqRequired))
    ),
    ?assertEqual(ProcessRef, maps:get(<<"Process">>, maps:get(<<"Deep">>, ReadReqRequired))),
    ?event(read_latest_slot_with_deep_key),
    {ok, 2, _} = latest(ProcID, <<"Deep">>, Opts),
    ?event(read_latest_slot_with_link_key),
    {ok, 1, RawReadBase} = latest(ProcID, [], 1, Opts),
    ReadBase = hb_cache:ensure_all_loaded(RawReadBase, Opts),
    ?assertEqual(1, maps:get(<<"Result-Number">>, maps:get(<<"Results">>, ReadBase))),
    ?assertEqual(ProcessRef, maps:get(<<"Process">>, ReadBase)).

%% @doc Test for serving `/now' from cache only while the cache is fresh enough.
freshness_max_age(Opts) ->
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Slot = 1,
    SlotResult = #{
        <<"device">> => <<"process@1.0">>,
        <<"at-slot">> => Slot,
        <<"results">> => #{ <<"number">> => 1 }
    },
    % Assert that the process is not fresh by default.
    ?assertEqual(
        false,
        fresh(
            ProcID,
            #{ <<"max-age">> => 60 },
            Opts#{ <<"process-clock">> => 100 }
        )
    ),
    % Write the slot result to the cache at clock time 100.
    {ok, _} = write(ProcID, Slot, SlotResult, Opts#{ <<"process-clock">> => 100 }),
    {ok, 1, RawReadSlotResult} = latest(ProcID, Opts),
    ReadSlotResult = hb_cache:ensure_all_loaded(RawReadSlotResult, Opts),
    ?assertEqual(<<"process@1.0">>, maps:get(<<"device">>, ReadSlotResult)),
    ?assertEqual(1, maps:get(<<"number">>, maps:get(<<"results">>, ReadSlotResult))),
    ?assertEqual({ok, Slot, 100}, read_refreshed_at(ProcID, Opts)),
    % Assert that writing an older slot does not refresh the latest cached slot.
    OlderSlot = 0,
    OlderSlotResult = SlotResult#{
        <<"at-slot">> => OlderSlot,
        <<"results">> => #{ <<"number">> => 0 }
    },
    {ok, _} =
        write(
            ProcID,
            OlderSlot,
            OlderSlotResult,
            Opts#{ <<"process-clock">> => 200 }
        ),
    ?assertEqual({ok, Slot, 100}, read_refreshed_at(ProcID, Opts)),
    ?assertEqual(
        false,
        fresh(
            ProcID,
            #{ <<"max-age">> => 60 },
            Opts#{ <<"process-clock">> => 250 }
        )
    ),
    % Assert that the process is fresh exactly at the max-age.
    ?assertEqual(
        true,
        fresh(
            ProcID,
            #{ <<"max-age">> => 60 },
            Opts#{ <<"process-clock">> => 160 }
        )
    ),
    % Assert that the process is not fresh after the max-age.
    ?assertEqual(
        false,
        fresh(
            ProcID,
            #{ <<"max-age">> => 60 },
            Opts#{ <<"process-clock">> => 161 }
        )
    ),
    % Assert that the process is fresh if the max-age is infinity.
    ?assertEqual(
        true,
        fresh(
            ProcID,
            #{ <<"max-age">> => <<"infinity">> },
            Opts#{ <<"process-clock">> => 1000 }
        )
    ),
    % Assert that the process is fresh if the only-if-cached flag is set.
    ?assertEqual(
        true,
        fresh(
            ProcID,
            #{
                <<"cache-control">> => [<<"only-if-cached">>],
                <<"max-age">> => 0
            },
            Opts#{ <<"process-clock">> => 1000 }
        )
    ),
    % Assert that the max age is read as a fallback from the node opts
    ?assertEqual(
        true,
        fresh(
            ProcID,
            #{},
            Opts#{
                <<"process-clock">> => 160,
                <<"process-now-max-age">> => 60
            }
        )
    ),
    ?assertEqual(
        false,
        fresh(
            ProcID,
            #{},
            Opts#{
                <<"process-clock">> => 161,
                <<"process-now-max-age">> => 60
            }
        )
    ),
    % A scheduler check of an unchanged slot refreshes its cache age.
    ok = refresh(ProcID, Slot, Opts#{ <<"process-clock">> => 250 }),
    ?assertEqual({ok, Slot, 250}, read_refreshed_at(ProcID, Opts)).

%% @doc Process cache writes go only to `process-store' when configured.
isolated_process_store_test() ->
    MainStore = hb_test_utils:test_store(hb_store_fs, <<"process-main">>),
    ProcessStore = hb_test_utils:test_store(hb_store_fs, <<"process-isolated">>),
    Opts = #{
        <<"store">> => [MainStore],
        <<"process-store">> => [ProcessStore]
    },
    hb_store:start(MainStore),
    hb_store:start(ProcessStore),
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Msg = #{ <<"results">> => #{ <<"ok">> => <<"stored">> } },
    {ok, Path} = write(ProcID, 1, Msg, Opts),
    MainOpts = #{ <<"store">> => [MainStore] },
    ?assertMatch({error, not_found}, hb_cache:read(Path, MainOpts)),
    ?assertMatch({ok, _}, read(ProcID, 1, Opts)),
    ?assertMatch({ok, 1, _}, latest(ProcID, Opts)),
    hb_store:reset(MainStore),
    ?assertMatch({ok, _}, read(ProcID, 1, Opts)),
    ?assertMatch({ok, 1, _}, latest(ProcID, Opts)),
    hb_store:reset(ProcessStore),
    ?assertMatch({error, not_found}, read(ProcID, 1, Opts)),
    ?assertMatch({error, not_found}, latest(ProcID, Opts)).

linked_trie_process_store_test() ->
    MainStore = hb_test_utils:test_store(hb_store_fs, <<"trie-main">>),
    ProcessStore = hb_test_utils:test_store(hb_store_fs, <<"trie-process">>),
    Opts = #{
        <<"store">> => [MainStore],
        <<"process-store">> => [ProcessStore]
    },
    hb_store:start(MainStore),
    hb_store:start(ProcessStore),
    ExecOpts = lib_process:execution_opts(Opts),
    {ok, Trie1} =
        hb_ao:resolve(
            #{ <<"device">> => <<"trie@1.0">> },
            #{ <<"path">> => <<"set">>,
                <<"w0MPTVR4CoZvasQxZi6bjVlgYNWTONwvQUyxTT5aA-8">> => 3 },
            ExecOpts
        ),
    {ok, Trie2} =
        hb_ao:resolve(
            Trie1,
            #{ <<"path">> => <<"set">>,
                <<"wReY9V0F2PYI1bZgTpFjORh-_8hw_SUCEFjyqrFLpxM">> => 12 },
            ExecOpts
        ),
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    {ok, _} = write(ProcID, 12, #{ <<"dedup">> => Trie2 }, Opts),
    {ok, Cached} = read(ProcID, 12, Opts),
    ?assertEqual(
        12,
        hb_ao:get(
            <<"dedup/wReY9V0F2PYI1bZgTpFjORh-_8hw_SUCEFjyqrFLpxM">>,
            Cached,
            lib_process:cache_opts(Opts)
        )
    ).

%% @doc With `process-state-blobs', a delta process's slots are one row each
%% under `pstate/', read back as computed (from the store, hot cache empty),
%% and a checkpoint with a snapshot gets its marker and is found by `latest'.
state_blobs_layout_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb),
    Opts = #{
        <<"store">> => Store,
        <<"process-delta-cache">> => true,
        <<"process-state-blobs">> => true,
        <<"process-delta-checkpoint-slots">> => 3,
        <<"process-hot-cache-slots">> => 0
    },
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    State0 = #{ <<"count">> => <<"0">>, <<"at-slot">> => 0 },
    Delta = fun(Msg, P, R) ->
        Msg#{ <<"priv">> => #{ <<"process-cache-delta">> =>
            #{ <<"patches">> => P, <<"results">> => R } } }
    end,
    {ok, _} = write(ProcID, 0, Delta(State0#{ <<"snapshot">> => <<"vm0">> }, [], #{}), Opts),
    States =
        lists:foldl(
            fun(N, Acc = [Prev | _]) ->
                Bin = integer_to_binary(N),
                Patches = [#{ <<"path">> => <<"/count">>, <<"value">> => Bin }],
                Results = #{ <<"output">> => #{ <<"data">> => Bin } },
                {ok, Next} = hb_state_patch:apply(Prev, Patches, Results, N, Opts),
                Msg = case N of 3 -> Next#{ <<"snapshot">> => <<"vm3">> }; _ -> Next end,
                {ok, _} = write(ProcID, N, Delta(Msg, Patches, Results), Opts),
                ok = await_checkpoint(ProcID),
                [Next | Acc]
            end,
            [State0],
            lists:seq(1, 5)),
    % Slot 3's full state is written off the slot; slot 3 is also a delta,
    % so the chain is whole before it lands.
    ok = await_checkpoint(ProcID),
    Keys =
        lists:sort(
            [ K || K <- element(2, hb_store:list([Store], <<"pstate/", ProcID/binary, "/d">>, #{})) ]),
    ?assertEqual([pad_slot(N) || N <- [1, 2, 3, 4, 5]], Keys),
    ?assertEqual([0, 3], blob_slots(ProcID, <<"s">>, Store)),
    ?assertEqual([0, 3], blob_slots(ProcID, <<"f">>, Store)),
    % Nothing of it went through the content-addressed layout.
    ?assertEqual([], legacy_slots(ProcID, Opts)),
    lists:foreach(
        fun({N, Expected}) ->
            {ok, Read} = read(ProcID, N, Opts),
            ?assertEqual(hb_maps:get(<<"count">>, Expected), hb_maps:get(<<"count">>, Read))
        end,
        lists:zip(lists:seq(0, 5), lists:reverse(States))),
    ?assertMatch({ok, 3, #{ <<"snapshot">> := <<"vm3">> }},
                 latest(ProcID, [<<"snapshot">>], undefined, Opts)),
    ?assertMatch({ok, 0, _}, latest(ProcID, [<<"snapshot">>], 2, Opts)),
    ?assertEqual({ok, 5}, latest_slot(ProcID, Opts)).

%% @doc A checkpoint written off the slot: before it lands, every slot reads
%% back through the deltas; a checkpoint due while one is being written is
%% left to the next; with `process-async-checkpoints' false it is written in
%% the slot, with no delta row.
async_checkpoint_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb),
    Base = #{
        <<"store">> => Store,
        <<"process-delta-cache">> => true,
        <<"process-state-blobs">> => true,
        <<"process-delta-checkpoint-slots">> => 2,
        <<"process-hot-cache-slots">> => 0
    },
    Delta = fun(Msg, P, R) ->
        Msg#{ <<"priv">> => #{ <<"process-cache-delta">> =>
            #{ <<"patches">> => P, <<"results">> => R } } }
    end,
    Run = fun(Opts) ->
        ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
        State0 = #{ <<"count">> => <<"0">>, <<"at-slot">> => 0 },
        {ok, _} = write(ProcID, 0, Delta(State0, [], #{}), Opts),
        lists:foldl(
            fun(N, Prev) ->
                Bin = integer_to_binary(N),
                Patches = [#{ <<"path">> => <<"/count">>, <<"value">> => Bin }],
                Results = #{ <<"output">> => #{ <<"data">> => Bin } },
                {ok, Next} = hb_state_patch:apply(Prev, Patches, Results, N, Opts),
                {ok, _} = write(ProcID, N, Delta(Next, Patches, Results), Opts),
                {ok, Read} = read(ProcID, N, Opts),
                ok = await_checkpoint(ProcID),
                ?assertEqual(Bin, hb_maps:get(<<"count">>, Read)),
                Next
            end,
            State0,
            lists:seq(1, 4)),
        ok = await_checkpoint(ProcID),
        {ProcID, blob_slots(ProcID, <<"f">>, Store), blob_slots(ProcID, <<"d">>, Store)}
    end,
    {P1, F1, D1} = Run(Base),
    % Each checkpoint lands, and its slot is a delta as well.
    ?assertEqual([0, 2, 4], F1),
    ?assertEqual([1, 2, 3, 4], D1),
    {ok, Read4} = read(P1, 4, Base),
    ?assertEqual(<<"4">>, hb_maps:get(<<"count">>, Read4)),
    {_, F2, D2} = Run(Base#{ <<"process-async-checkpoints">> => false }),
    ?assertEqual([0, 2, 4], F2),
    ?assertEqual([1, 3], D2).

%% @doc The checkpointer starts the stalest waiting checkpoints first, those
%% past three cadences before any other, never more than its writers at once
%% and never two of one process; a newer checkpoint of a process supersedes
%% its waiting one.
checkpointer_order_test() ->
    Writer = fun() -> spawn(fun() -> receive go -> receive stop -> ok end; skip -> ok end end) end,
    [W1, W2, W3] = [Writer(), Writer(), Writer()],
    S0 = #{ pending => #{
                <<"a">> => {W1, 120, 100},   % 120 slots since its last: 1.2 cadences
                <<"b">> => {W2, 350, 100},   % 350 since none stored: past 3 cadences
                <<"c">> => {W3, 260, 100} },
            running => #{}, busy => #{},
            last => #{ <<"a">> => 0, <<"b">> => -1, <<"c">> => 0 },
            writers => 1, waiters => [] },
    S1 = start_checkpoints(S0),
    ?assertEqual([<<"a">>, <<"c">>], lists:sort(maps:keys(maps:get(pending, S1)))),
    ?assertEqual([<<"b">>], maps:keys(maps:get(busy, S1))),
    S2 = start_checkpoints(S1#{ writers := 2 }),
    ?assertEqual([<<"a">>], maps:keys(maps:get(pending, S2))),
    ?assertEqual([<<"b">>, <<"c">>], lists:sort(maps:keys(maps:get(busy, S2)))),
    % A busy process's next checkpoint waits for it.
    S3 = start_checkpoints(S2#{ writers := 3,
        pending := (maps:get(pending, S2))#{ <<"c">> => {Writer(), 400, 100} } }),
    ?assertEqual([<<"c">>], maps:keys(maps:get(pending, S3))),
    [ P ! stop || P <- [W1, W2, W3] ],
    % Through the process: a newer checkpoint of a waiting process supersedes it.
    Checkpointer = checkpointer(),
    Self = self(),
    Blocker = spawn(fun() -> receive go -> Self ! started, receive stop -> exit({checkpoint, ok}) end end end),
    Old = spawn(fun() -> receive go -> Self ! {old, go}; skip -> Self ! {old, skip} end end),
    New = spawn(fun() -> receive go -> Self ! {new, go}, exit({checkpoint, ok}); skip -> Self ! {new, skip} end end),
    P1 = hb_util:human_id(crypto:strong_rand_bytes(32)),
    P2 = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Checkpointer ! {due, P1, 10, Blocker, 100, 1},
    receive started -> ok after 5000 -> ?assert(false) end,
    Checkpointer ! {due, P2, 5, Old, 100, 1},
    Checkpointer ! {due, P2, 6, New, 100, 1},
    receive {old, R} -> ?assertEqual(skip, R) after 5000 -> ?assert(false) end,
    Blocker ! stop,
    receive {new, R2} -> ?assertEqual(go, R2) after 5000 -> ?assert(false) end,
    ok = await_checkpoint(P2),
    Checkpointer ! {status, self()},
    receive {checkpoint_status, #{ last := LastSlots }} ->
        ?assertEqual(6, maps:get(P2, LastSlots)),
        ?assertEqual(10, maps:get(P1, LastSlots))
    after 5000 -> ?assert(false)
    end,
    % The node-wide writer count returns to its default.
    Checkpointer ! {due, P1, 11, spawn(fun() -> receive _ -> ok end end), 100, 1},
    ok = await_checkpoint(P1).

%% @doc With `process-state-blobs', a full-state process (no patches) writes
%% each slot as one `f' row too, never RC7's content-addressed layout.
state_blobs_full_state_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb),
    Opts = #{ <<"store">> => Store, <<"process-state-blobs">> => true,
              <<"process-delta-cache">> => true, <<"process-hot-cache-slots">> => 0 },
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    lists:foreach(
        fun(N) ->
            Msg = #{ <<"count">> => integer_to_binary(N), <<"at-slot">> => N },
            Msg1 = case N of 2 -> Msg#{ <<"snapshot">> => <<"vm">> }; _ -> Msg end,
            {ok, _} = write(ProcID, N, Msg1, Opts)
        end,
        lists:seq(0, 3)),
    ?assertEqual([0, 1, 2, 3], blob_slots(ProcID, <<"f">>, Store)),
    ?assertEqual([2], blob_slots(ProcID, <<"s">>, Store)),
    ?assertEqual([], legacy_slots(ProcID, Opts)),
    {ok, R} = read(ProcID, 1, Opts),
    ?assertEqual(<<"1">>, hb_maps:get(<<"count">>, R)),
    ?assertMatch({ok, 2, _}, latest(ProcID, [<<"snapshot">>], undefined, Opts)).

%% @doc A state longer than a page is stored as page-sized chunks and reads
%% back whole; a row with a chunk missing reads as absent, not truncated.
state_blobs_chunked_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb),
    Opts = #{ <<"store">> => Store, <<"process-state-blobs">> => true,
              <<"process-hot-cache-slots">> => 0 },
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Big = hb_util:encode(crypto:strong_rand_bytes(50000)),
    {ok, _} = write(ProcID, 0, #{ <<"big">> => Big, <<"snapshot">> => <<"vm">> }, Opts),
    {ok, R} = read(ProcID, 0, Opts),
    ?assertEqual(Big, hb_maps:get(<<"big">>, R)),
    Key = blob_key(ProcID, <<"f">>, 0),
    {ok, Names} = hb_store:list([Store], blob_prefix(ProcID, <<"f">>), #{}),
    Chunks = [ N || N <- Names, binary:match(N, <<".">>) =/= nomatch ],
    ?assert(length(Chunks) >= 10),
    [ ?assertMatch({ok, B} when byte_size(B) =< ?BLOB_CHUNK,
          blob_raw(Store, <<(blob_prefix(ProcID, <<"f">>))/binary, "/", N/binary>>, Opts))
    || N <- Chunks ],
    ?assertEqual([0], blob_slots(ProcID, <<"f">>, Store)),
    #{ <<"db">> := DB } = hb_store:find(Store),
    {ok, 1} = elmdb:delete_batch(DB, [chunk_key(Key, 2)]),
    ?assertEqual(not_found, blob_get(Store, Key, Opts)).

%% @doc A slot whose results carry an outbox writes it, unlowered, to its `o'
%% row -- a full state's slot as well as a delta's -- and one without an
%% outbox writes none. The worker never lowers it: the write path applies no
%% patch for it, with or without an outbox. The deliverer lowers it when it
%% reads the row (`outbox_entries/2'). `read_results' answers a stored slot
%% from the hot cache, its delta envelope (lowering only its results) or its
%% full row; once the state is gone it is not found -- the `o' row is the
%% deliverer's, not a reader's.
outbox_rows_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb),
    Opts = #{
        <<"store">> => Store,
        <<"process-delta-cache">> => true,
        <<"process-state-blobs">> => true,
        <<"process-delta-checkpoint-slots">> => 1000,
        <<"process-hot-cache-slots">> => 0,
        <<"priv-wallet">> => ar_wallet:new()
    },
    % A process whose definition names `Scheduler' as its scheduler.
    Define =
        fun(Scheduler) ->
            P = hb_message:commit(#{ <<"type">> => <<"Process">>,
                <<"scheduler-location">> => Scheduler,
                <<"nonce">> => hb_util:human_id(crypto:strong_rand_bytes(32)) }, Opts),
            {ok, _} = hb_cache:write(P, Opts),
            hb_util:human_id(hb_message:id(P, all, Opts))
        end,
    Here = hb_opts:address(Opts),
    ProcID = Define(Here),
    Outbox = #{ <<"1">> => #{ <<"target">> => <<"t">>, <<"data">> => <<"hi">> } },
    % Not scheduled here, or not known here: nothing is kept for a delivery
    % that never happens.
    lists:foreach(
        fun(Elsewhere) ->
            {ok, _} = write(Elsewhere, 0, #{ <<"at-slot">> => 0,
                <<"results">> => #{ <<"outbox">> => Outbox } }, Opts),
            ?assertEqual([], blob_slots(Elsewhere, <<"o">>, Store)),
            ?assertNot(has_outbox(Elsewhere,
                #{ <<"results">> => #{ <<"outbox">> => Outbox } }, Opts))
        end,
        [Define(hb_util:human_id(crypto:strong_rand_bytes(32))),
            hb_util:human_id(crypto:strong_rand_bytes(32))]
    ),
    Delta = fun(Msg, P, R) ->
        Msg#{ <<"priv">> => #{ <<"process-cache-delta">> =>
            #{ <<"patches">> => P, <<"results">> => R } } }
    end,
    State0 = #{ <<"count">> => <<"0">>, <<"at-slot">> => 0,
                <<"results">> => #{ <<"outbox">> => Outbox } },
    {ok, _} = write(ProcID, 0, Delta(State0, [], #{ <<"outbox">> => Outbox }), Opts),
    erlang:trace_pattern({hb_state_patch, apply, 4}, true, [call_count]),
    lists:foldl(
        fun(N, Prev) ->
            Bin = integer_to_binary(N),
            Patches = [#{ <<"path">> => <<"/count">>, <<"value">> => Bin }],
            Results =
                case N rem 2 of
                    1 -> #{ <<"Outbox">> => Outbox, <<"output">> => Bin };
                    0 -> #{ <<"output">> => Bin }
                end,
            {ok, Next} = hb_state_patch:apply(Prev, Patches, Results, N, Opts),
            Before = trace_count({hb_state_patch, apply, 4}),
            {ok, _} = write(ProcID, N, Delta(Next, Patches, Results), Opts),
            ?assertEqual(Before, trace_count({hb_state_patch, apply, 4})),
            Next
        end,
        State0,
        lists:seq(1, 4)
    ),
    erlang:trace_pattern({hb_state_patch, apply, 4}, false, [call_count]),
    % Only the slots with an outbox: the checkpoint at 0 and deltas 1 and 3.
    % The raw spelling (`Outbox') is kept; the deliverer lowers it.
    ?assertEqual([0, 1, 3], blob_slots(ProcID, <<"o">>, Store)),
    ?assertEqual([1, 2, 3, 4], blob_slots(ProcID, <<"d">>, Store)),
    {ok, Row} = blob_get(Store, blob_key(ProcID, <<"o">>, 1), Opts),
    ?assertMatch(#{ <<"lower">> := true }, blob_decode(Row)),
    ?assertEqual(Outbox, outbox_entries(blob_decode(Row), Opts)),
    {ok, Row0} = blob_get(Store, blob_key(ProcID, <<"o">>, 0), Opts),
    ?assertEqual(Outbox, outbox_entries(blob_decode(Row0), Opts)),
    ?assert(has_outbox(ProcID, Delta(#{}, [], #{ <<"outbox">> => Outbox }), Opts)),
    ?assertNot(has_outbox(ProcID, Delta(#{}, [], #{ <<"output">> => <<"x">> }), Opts)),
    % From the delta envelope, lowered as the slot's state holds them, with
    % no patch applied.
    erlang:trace_pattern({hb_state_patch, apply_patches, 3}, true, [call_count]),
    {ok, R3} = read_results(ProcID, 3, Opts),
    Applied = trace_count({hb_state_patch, apply_patches, 3}),
    erlang:trace_pattern({hb_state_patch, apply_patches, 3}, false, [call_count]),
    ?assertEqual(1, Applied),
    ?assertEqual(Outbox, maps:get(<<"outbox">>, R3)),
    {ok, Read3} = read(ProcID, 3, Opts),
    ?assertEqual(maps:get(<<"results">>, Read3), R3),
    ?assertEqual({ok, #{ <<"output">> => <<"4">> }}, read_results(ProcID, 4, Opts)),
    % From a full row.
    ?assertMatch({ok, #{ <<"outbox">> := Outbox }}, read_results(ProcID, 0, Opts)),
    % From the hot cache.
    HotOpts = Opts#{ <<"process-hot-cache-slots">> => 8 },
    hot_put(ProcID, 9, #{ <<"results">> => #{ <<"output">> => <<"hot">> } }, HotOpts),
    ?assertEqual({ok, #{ <<"output">> => <<"hot">> }}, read_results(ProcID, 9, HotOpts)),
    % Once the state is gone the results are not found, `o' row or not.
    #{ <<"db">> := DB } = hb_store:find(Store),
    {ok, _} = elmdb:delete_batch(DB, [blob_key(ProcID, <<"d">>, 1)]),
    ?assertEqual({error, not_found}, read_results(ProcID, 1, Opts)),
    ?assertEqual({error, not_found}, read_results(ProcID, 7, Opts)),
    % The lowest stored slot.
    ?assertEqual({ok, 0}, stored_floor(ProcID, Opts)),
    ?assertEqual(none, stored_floor(hb_util:human_id(crypto:strong_rand_bytes(32)), Opts)),
    % A full-state process writes its `o' row too, not lowered.
    Full = Define(Here),
    {ok, _} = write(Full, 0, #{ <<"results">> => #{ <<"outbox">> => Outbox } }, Opts),
    {ok, _} = write(Full, 1, #{ <<"results">> => #{ <<"outbox">> => #{} } }, Opts),
    ?assertEqual([0], blob_slots(Full, <<"o">>, Store)),
    {ok, FullRow} = blob_get(Store, blob_key(Full, <<"o">>, 0), Opts),
    ?assertMatch(#{ <<"lower">> := false }, blob_decode(FullRow)),
    % Delivery rows go with their chunks.
    Big = blob_key(Full, <<"q">>, 3),
    ok = blob_put(Store, Big, crypto:strong_rand_bytes(3 * 4000), Opts),
    ok = blob_commit(Store, [Big, blob_key(Full, <<"q">>, 4)], Opts),
    ?assertEqual(not_found, blob_get(Store, Big, Opts)),
    % And the commit carries the writes before it, durable at once.
    ok = blob_put(Store, blob_key(Full, <<"x">>, 5), <<"x">>, Opts),
    ok = blob_commit(Store, [], Opts),
    ?assertEqual(0, elmdb:overlay_count(DB)),
    ?assertEqual([], blob_slots(Full, <<"q">>, Store)),
    ?assertNotMatch({ok, [_ | _]}, hb_store:list([Store], blob_prefix(Full, <<"q">>), #{})).

%% @doc The head marker lives with the slot rows: it reads back from the
%% store that holds them.
head_marker_with_rows_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb),
    Opts = #{
        <<"store">> => [Store],
        <<"process-delta-cache">> => true,
        <<"process-state-blobs">> => true,
        <<"process-hot-cache-slots">> => 0
    },
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    {ok, _} = write(ProcID, 0, #{ <<"count">> => <<"0">> }, Opts),
    {ok, _} = write(ProcID, 1, #{ <<"count">> => <<"1">> }, Opts),
    ?assertEqual({ok, 1}, latest_slot(ProcID, Opts)),
    {ok, Raw} = hb_store:read([Store], refreshed_path(ProcID), Opts),
    ?assertMatch({ok, 1, _}, parse_refreshed_at(Raw)).

%% @doc A checkpoint's `s' marker carries its assignment's timestamp (when
%% the write was handed one), the build that wrote it and the digest of its
%% state row. A marker with no timestamp, or an empty one from before
%% markers had a value, reads as `legacy'.
snapshot_marker_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb),
    Opts = #{ <<"store">> => Store, <<"process-state-blobs">> => true,
              <<"process-hot-cache-slots">> => 0 },
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    {ok, _} = write(ProcID, 0, #{ <<"at-slot">> => 0, <<"snapshot">> => <<"vm">> },
                    Opts#{ <<"process-assignment-timestamp">> => 1700000000123 }),
    {ok, Marker} = blob_raw(Store, blob_key(ProcID, <<"s">>, 0), Opts),
    ?assertEqual({ok, 1700000000123}, hb_store_retention:marker_time(Marker)),
    {ok, Full} = blob_raw(Store, blob_key(ProcID, <<"f">>, 0), Opts),
    ?assertMatch(#{ <<"build">> := B } when is_binary(B), blob_decode(Marker)),
    ?assertEqual(crypto:hash(sha256, Full), maps:get(<<"state-digest">>, blob_decode(Marker))),
    ?assert(byte_size(Marker) < 200),
    {ok, _} = write(ProcID, 1, #{ <<"at-slot">> => 1, <<"snapshot">> => <<"vm">> }, Opts),
    {ok, Undated} = blob_raw(Store, blob_key(ProcID, <<"s">>, 1), Opts),
    ?assertEqual(legacy, hb_store_retention:marker_time(Undated)),
    ?assertEqual(legacy, hb_store_retention:marker_time(<<>>)).

%% @doc `latest' with a path pages down the stored slots: it finds the newest
%% slot with the path however far below the head it is, at or below a limit.
latest_pages_down_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb),
    Opts = #{ <<"store">> => Store, <<"process-state-blobs">> => true,
              <<"process-hot-cache-slots">> => 0 },
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    lists:foreach(
        fun(N) ->
            Msg = #{ <<"at-slot">> => N },
            {ok, _} = write(ProcID, N, case N of 7 -> Msg#{ <<"marked">> => <<"yes">> }; _ -> Msg end, Opts)
        end,
        lists:seq(0, 3 * ?LATEST_PAGE + 10)),
    ?assertMatch({ok, 7, _}, latest(ProcID, <<"marked">>, Opts)),
    ?assertMatch({ok, 7, _}, latest(ProcID, <<"marked">>, 2 * ?LATEST_PAGE, Opts)),
    ?assertEqual({error, not_found}, latest(ProcID, <<"marked">>, 6, Opts)),
    ?assertMatch({ok, 5, _}, latest(ProcID, [], 5, Opts)),
    Top = 3 * ?LATEST_PAGE + 10,
    ?assertMatch({ok, Top, _}, latest(ProcID, Opts)),
    ?assertEqual({ok, 0}, stored_floor(ProcID, Opts)).

%% @doc A process holding a million delta rows: a retention run
%% (`hb_store_retention'), the lowest and newest stored slot and the latest
%% state each stay within a 16 MB heap and finish promptly. Listing every row would take ~6M words of heap for the
%% names alone.
million_rows_bounded_test_() ->
    {timeout, 900, fun() ->
        Store = hb_test_utils:test_store(hb_store_lmdb, <<"ret-million">>),
        ok = hb_store:start([Store], #{}, #{}),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        Head = 1002000,
        Now = erlang:system_time(millisecond),
        Day = 24 * 3600 * 1000,
        Checkpoints = [{0, 10}, {1000, 9}, {2000, 8}, {Head, 0}],
        ok = hb_store:write([Store], maps:from_list(
            [ {blob_key(P, <<"f">>, S), blob_encode(#{ <<"at-slot">> => S })}
            || {S, _} <- Checkpoints ] ++
            [ {blob_key(P, <<"s">>, S), blob_encode(#{ <<"timestamp">> => Now - D * Day })}
            || {S, D} <- Checkpoints ]), #{}),
        Deltas = [ S || S <- lists:seq(1, Head - 1), not lists:keymember(S, 1, Checkpoints) ],
        lists:foreach(
            fun(Page) ->
                ok = hb_store:write([Store],
                    maps:from_list([ {blob_key(P, <<"d">>, S), <<"d">>} || S <- Page ]), #{})
            end,
            pages(Deltas, 50000)),
        ok = hb_store:flush([Store], #{}),
        ?assert(length(Deltas) > 1000000),
        Bounded =
            fun(F) ->
                Parent = self(),
                Ref = make_ref(),
                {Pid, Mon} =
                    spawn_opt(fun() -> Parent ! {Ref, timer:tc(F)} end,
                        [monitor, {max_heap_size,
                            #{ size => 2000000, kill => true, error_logger => false }}]),
                receive
                    {Ref, {Micros, R}} ->
                        erlang:demonitor(Mon, [flush]),
                        io:format(user, "~nbounded call: ~p ms~n", [Micros div 1000]),
                        ?assert(Micros < 30000000),
                        R;
                    {'DOWN', Mon, process, Pid, Reason} -> erlang:error({over_heap_bound, Reason})
                end
            end,
        % The bound holds what listing every row would take.
        ?assertError({over_heap_bound, _}, Bounded(fun() -> blob_slots(P, <<"d">>, Store) end)),
        Report =
            Bounded(fun() ->
                hb_store_retention:retain(#{ <<"store">> => [Store],
                    <<"store-retention-max-cpu-pct">> => 100 })
            end),
        % The 1,998 deltas below the floor at 2000; the snapshot checkpoints
        % at 0 and 1000 stay.
        ?assertMatch(#{ deleted := 1998 }, Report),
        Read = fun(K, S) -> hb_store:read([Store], blob_key(P, K, S), #{}) end,
        ?assertMatch({error, _}, Read(<<"d">>, 1999)),
        ?assertMatch({ok, _}, Read(<<"f">>, 1000)),
        ?assertMatch({ok, _}, Read(<<"s">>, 0)),
        ?assertEqual({ok, <<"d">>}, Read(<<"d">>, 2001)),
        ?assertEqual({ok, <<"d">>}, Read(<<"d">>, Head - 1)),
        Opts = #{ <<"store">> => [Store], <<"process-state-blobs">> => true,
                  <<"process-delta-cache">> => true, <<"process-hot-cache-slots">> => 0 },
        ?assertEqual({ok, 0}, Bounded(fun() -> stored_floor(P, Opts) end)),
        ?assertEqual({ok, Head}, Bounded(fun() -> latest_slot(P, Opts) end)),
        ?assertMatch({ok, Head, #{ <<"at-slot">> := Head }},
            Bounded(fun() -> latest(P, Opts) end)),
        % And a second run finds nothing more to do, as cheaply.
        ?assertMatch(#{ deleted := 0 },
            Bounded(fun() ->
                hb_store_retention:retain(#{ <<"store">> => [Store],
                    <<"store-retention-max-cpu-pct">> => 100 })
            end))
    end}.

pages([], _N) -> [];
pages(L, N) when length(L) =< N -> [L];
pages(L, N) -> {Page, Rest} = lists:split(N, L), [Page | pages(Rest, N)].

trace_count(MFA) ->
    case erlang:trace_info(MFA, call_count) of
        {call_count, N} when is_integer(N) -> N;
        _ -> 0
    end.

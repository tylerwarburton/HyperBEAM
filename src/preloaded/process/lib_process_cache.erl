
%%% @doc A wrapper around the hb_cache module that provides a more
%%% convenient interface for reading the result of a process at a given slot or
%%% message ID.
-module(lib_process_cache).
-export([fresh/3, fresh/4]).
-export([latest/2, latest/3, latest/4]).
-export([read/2, read/3, refresh/3, write/4]).
-include_lib("eunit/include/eunit.hrl").
-include("include/hb.hrl").

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

%% @doc Write a process computation result to the cache.
write(ProcID, Slot, Msg, RawOpts) ->
    Opts = lib_process:cache_opts(RawOpts),
    Public = hb_private:reset(Msg),
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
    hot_put(ProcID, Slot, Public, Opts),
    Result.

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

%% @doc Mark the process as refreshed at the current clock time.
refresh(ProcID, Slot, RawOpts) ->
    Opts = lib_process:cache_opts(RawOpts),
    CachedSlot = latest_slot(ProcID, lib_process:scoped_opts(Opts)),
    case CachedSlot of
        {ok, LatestSlot} when Slot < LatestSlot ->
            ok;
        _ ->
            Store = hb_opts:get(store, no_viable_store, Opts),
            hb_store:write(
                Store,
                #{ refreshed_path(ProcID) => refreshed_at(Slot, clock(Opts)) },
                Opts
            )
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
    Store = hb_opts:get(store, no_viable_store, Opts),
    case hb_store:read(Store, refreshed_path(ProcID), Opts) of
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
    case hot_get(ProcID, Slot, Opts) of
        {ok, _} -> true;
        not_found when Slot >= 0 ->
            blob_exists(ProcID, Slot, Opts) orelse
                case hb_cache:read(path(ProcID, Slot, Opts), Opts) of
                    {ok, _} -> true;
                    _ -> false
                end;
        not_found ->
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

hot_put(ProcID, Slot, Msg, Opts) when is_integer(Slot) ->
    case hot_slots(Opts) of
        0 -> ok;
        Keep ->
            ensure_hot_table(),
            try
                ets:insert(?HOT, {{ProcID, Slot}, Msg}),
                hot_trim(ProcID, Keep)
            catch error:badarg -> ok
            end,
            ok
    end;
hot_put(_ProcID, _Slot, _Msg, _Opts) ->
    ok.

hot_trim(ProcID, Keep) ->
    Held = ets:select_count(?HOT, [{{{ProcID, '_'}, '_'}, [], [true]}]),
    case Held > Keep of
        true ->
            case ets:next(?HOT, {ProcID, -2}) of
                {ProcID, Oldest} ->
                    ets:delete(?HOT, {ProcID, Oldest}),
                    hot_trim(ProcID, Keep);
                _ -> ok
            end;
        false -> ok
    end.

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
        not_found -> latest_stored(ProcID, RequiredPath, Limit, Opts)
    end.

%% @doc The latest stored slot, listing every slot of the process.
latest_stored(ProcID, RequiredPath, Limit, Opts) ->
    Path = path(ProcID, slot_root, Opts),
    AllSlots =
        case {RequiredPath, blob_store(Opts)} of
            {[<<"snapshot">>], Store} when Store =/= none ->
                % Only a checkpoint with a VM snapshot can satisfy it.
                legacy_slots(ProcID, Opts) ++ blob_slots(ProcID, <<"s">>, Store);
            _ -> slots(ProcID, Opts)
        end,
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
            case lists:sort(slots(ProcID, Opts)) of
                [] -> {error, not_found};
                Slots -> {ok, lists:last(Slots)}
            end
    end.

slots(ProcID, Opts) ->
    case blob_store(Opts) of
        none -> legacy_slots(ProcID, Opts);
        Store ->
            lists:usort(
                legacy_slots(ProcID, Opts) ++
                blob_slots(ProcID, <<"d">>, Store) ++
                blob_slots(ProcID, <<"f">>, Store))
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
%%%   pstate/<proc>/s/<slot>   empty marker, written after `f': that state
%%%                            carries a VM snapshot, so a worker can resume
%%%                            from it
%%%
%%% Slots are zero-padded to 12 digits. Values are `term_to_binary' of fully
%%% loaded terms, compressed. `hb_store_retention' deletes rows of this
%%% namespace, and only of it, below the newest checkpoints it keeps.
%%% A full-state process (no delta: `lua@5.3a', WASM, native devices) writes
%%% every slot as an `f' row. Negative slots keep RC7's layout.

-define(BLOB_NS, <<"pstate">>).
-define(BLOB_VSN, 1).

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

blob_put(Store, Key, Value, Opts) ->
    case hb_store:write([Store], #{ Key => Value }, Opts) of
        ok -> ok;
        Error -> erlang:error({process_state_blob_write_failed, Key, Error})
    end.

blob_get(Store, Key, Opts) ->
    case hb_store:read([Store], #{ <<"read">> => Key }, Opts) of
        {ok, Bin} when is_binary(Bin) -> {ok, Bin};
        _ -> not_found
    end.

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
            blob_get(Store, blob_key(ProcID, <<"d">>, Slot), Opts) =/= not_found
                orelse blob_get(Store, blob_key(ProcID, <<"f">>, Slot), Opts) =/= not_found
    end.

%% @doc The slots of `Kind' (`d', `f' or `s') a process holds, ascending.
blob_slots(ProcID, Kind, Store) ->
    case hb_store:list([Store], blob_prefix(ProcID, Kind), #{}) of
        {ok, Names} ->
            lists:sort([ binary_to_integer(N) || N <- Names,
                         is_integer(catch binary_to_integer(N)) ]);
        _ -> []
    end.

blob_encode(Term) -> term_to_binary({?BLOB_VSN, Term}, [{compressed, 1}]).
blob_decode(Bin) -> {?BLOB_VSN, Term} = binary_to_term(Bin), Term.

%% @doc Write a delta process's slot as one row. A checkpoint is the whole
%% public state; one carrying a VM snapshot also gets its `s' marker, written
%% after the state so a marker never names a state that is not there.
write_blob(ProcID, Slot, Msg, Patches, Results, Store, Opts) ->
    Key =
        case checkpoint_due(ProcID, Slot, Msg, Opts) of
            false ->
                Envelope = #{
                    <<"cache-format">> => ?DELTA_FORMAT,
                    <<"slot">> => Slot,
                    <<"base-slot">> => Slot - 1,
                    <<"patches">> => hb_cache:ensure_all_loaded(Patches, Opts),
                    <<"results">> => hb_cache:ensure_all_loaded(Results, Opts)
                },
                K = blob_key(ProcID, <<"d">>, Slot),
                ok = blob_put(Store, K, blob_encode(Envelope), Opts),
                K;
            true -> full_blob(ProcID, Slot, Msg, Store, Opts)
        end,
    ok = refresh(ProcID, Slot, Opts),
    {ok, Key}.

%% @doc Write a slot of a full-state process (one without patches: `lua@5.3a',
%% WASM, native devices) as one full-state row. Each row stands alone, so
%% retention can drop any of them but the ones it keeps.
write_full_blob(ProcID, Slot, Msg, Store, Opts) ->
    Key = full_blob(ProcID, Slot, Msg, Store, Opts),
    ok = refresh(ProcID, Slot, Opts),
    {ok, Key}.

full_blob(ProcID, Slot, Msg, Store, Opts) ->
    Loaded = hb_cache:ensure_all_loaded(hb_private:reset(Msg), Opts),
    K = blob_key(ProcID, <<"f">>, Slot),
    ok = blob_put(Store, K, blob_encode(Loaded), Opts),
    case is_map_key(<<"snapshot">>, Loaded) of
        true -> ok = blob_put(Store, blob_key(ProcID, <<"s">>, Slot), <<>>, Opts);
        false -> ok
    end,
    K.

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
                [Next | Acc]
            end,
            [State0],
            lists:seq(1, 5)),
    Keys =
        lists:sort(
            [ K || K <- element(2, hb_store:list([Store], <<"pstate/", ProcID/binary, "/d">>, #{})) ]),
    ?assertEqual([pad_slot(N) || N <- [1, 2, 4, 5]], Keys),
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

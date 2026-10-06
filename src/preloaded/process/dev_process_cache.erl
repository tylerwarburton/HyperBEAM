
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
-define(RECENT_CACHE, dev_process_delta_recent_cache).
-define(REPLAY_CACHE, dev_process_delta_replay_cache).
%% Recent-window eviction order: process -> last write sequence, and the
%% sequence-ordered set of processes. See `touch_recent/1'.
-define(RECENT_SEQ, dev_process_delta_recent_seq).
-define(RECENT_LRU, dev_process_delta_recent_lru).
-define(HOT_CACHE_OWNER, dev_process_delta_cache_owner).
-define(DEFAULT_DELTA_CHECKPOINT_SLOTS, 1000).
-define(DEFAULT_HOT_CACHE_SLOTS, 32).
%% Byte ceiling for the recent-slot window. Never reached at the default
%% 32-slot window (measured: 915 entries, 124 MB, ~139 KB/entry on a live node),
%% so this changes nothing until an operator raises the window. That is the
%% point: it makes raising it safe.
-define(DEFAULT_HOT_CACHE_MB, 1024).
%% Entries the process being written keeps when the byte bound trims it.
-define(RECENT_FLOOR_SLOTS, 4).
-define(DEFAULT_REPLAY_CACHE_SLOTS, 128).
-define(DEFAULT_REPLAY_CACHE_MB, 1536).

%% @doc Read the result of a process at a given slot.
read(ProcID, Opts) ->
    hb_util:ok(latest(ProcID, Opts)).
read(ProcID, SlotRef, Opts) ->
    ?event({reading_computed_result, ProcID, SlotRef}),
    case hot_read(ProcID, SlotRef, Opts) of
        {ok, Msg} -> {ok, Msg};
        not_found -> read_stored(ProcID, SlotRef, Opts)
    end.

%% @doc Read a slot from the store, bypassing the in-memory caches. Those hold
%% states without their VM `snapshot' (see `cacheable/1'), so a caller that
%% needs the snapshot must come here.
read_stored(ProcID, SlotRef, Opts) ->
    Path = path(ProcID, SlotRef, Opts),
    case hb_cache:read(Path, Opts) of
        {ok, Stored} ->
            case retention_loaded(Stored, materialize(ProcID, Stored, Opts), Opts) of
                {ok, Msg} when is_integer(SlotRef) ->
                    % Keep the rebuilt state, so the next read of the
                    % latest slot does not replay the delta chain
                    % again. `hot_put' never replaces a newer slot.
                    hot_put(ProcID, SlotRef, Msg, Opts),
                    {ok, Msg};
                Res -> Res
            end;
        Other -> Other
    end.

%% @doc With store retention on, a state read from the store is loaded whole
%% (all but its VM `snapshot', which a cold restore loads at once) before it
%% reaches the in-memory caches or a process worker. A lazily loaded state
%% holds links into the stored rows of the checkpoint it came from, and
%% retention deletes old checkpoints' rows once newer ones exist: a worker that
%% kept computing on such a state for hours would otherwise find a field gone
%% the first time it touched it. Without retention, nothing changes.
%% A delta is materialized onto a base that came through here already and
%% patches that are loaded whole, so only a stored full state needs it: a
%% replay of a long delta chain pays the load once, at its checkpoint.
retention_loaded(#{ <<"cache-format">> := ?DELTA_FORMAT }, Result, _Opts) ->
    Result;
retention_loaded(_Stored, {ok, Msg}, Opts) when is_map(Msg) ->
    case hb_util:atom(hb_opts:get(<<"store-retention">>, false, Opts)) of
        true ->
            {ok,
                maps:map(
                    fun(<<"snapshot">>, V) -> V;
                       (_K, V) -> hb_cache:ensure_all_loaded(V, Opts)
                    end,
                    Msg
                )
            };
        _ -> {ok, Msg}
    end;
retention_loaded(_Stored, Other, _Opts) -> Other.

%% @doc Write a process computation result to the cache.
write(ProcID, Slot, Msg, Opts) ->
    case delta_metadata(Msg, Opts) of
        {ok, Delta} -> write_delta(ProcID, Slot, Msg, Delta, Opts);
        not_found -> write_full(ProcID, Slot, Msg, Opts)
    end.

%% @doc Preserve the original full-message cache behavior.
write_full(ProcID, Slot, Msg, Opts) ->
    % Write the item to the cache in the root of the store.
    PublicMsg = hb_private:reset(Msg),
    {ok, Root} = hb_cache:write(PublicMsg, storage_opts(Opts)),
    ok = link_result(ProcID, Slot, Root, Root, Opts),
    % Keep the hot cache's newest entry current for every process: `latest'
    % answers from it.
    hot_put(ProcID, Slot, PublicMsg, Opts),
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
            {ok, StoredRoot} = hb_cache:write(PublicMsg, storage_opts(Opts)),
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
            {ok, StoredRoot} = hb_cache:write(Envelope, storage_opts(Opts)),
            ok = link_result(ProcID, Slot, StoredRoot, StoredRoot, Opts),
            hot_put(ProcID, Slot, PublicMsg, Opts),
            {ok, path(ProcID, Slot, Opts)}
    end.

%% @doc Computed state is addressed by process/slot and state-ID aliases, not
%% reverse queries. Skip that derived index while preserving all durable data.
storage_opts(Opts) ->
    Opts#{ <<"match-index">> => false }.

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
    % `snapshot' is also an exported process function. Resolving an absent key
    % here would execute that function and build the full VM snapshot merely
    % to test for its presence.
    HasSnapshot = maps:is_key(<<"snapshot">>, Msg),
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
                    % A delta slot has no VM snapshot of its own: one carried
                    % over from a checkpoint base would be a stale VM state.
                    hb_process_delta:apply(
                        cacheable(Base),
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
    % Every lookup sits inside the `try': the cache is an accelerator, so a
    % table that vanished with its owner is a miss, never a failed read. The
    % `select' copies the head state out only when it is the slot asked for.
    try
        case ets:select(?HOT_CACHE, [{{Key, SlotRef, '$1'}, [], ['$1']}]) of
            [Msg] -> {ok, Msg};
            [] ->
                case ets:lookup(?RECENT_CACHE, {Key, SlotRef}) of
                    [{_, Msg}] -> {ok, Msg};
                    [] ->
                        case ets:lookup(?REPLAY_CACHE, {Key, SlotRef}) of
                            [{_, Msg}] -> {ok, Msg};
                            [] -> not_found
                        end
                end
        end
    catch _:_ -> not_found
    end;
hot_read(_ProcID, _SlotRef, _Opts) -> not_found.

%% @doc Hold the newest known state of a process. An older slot never replaces
%% a newer one: a replay after a restart writes old slots while readers want
%% the latest, and letting the replay evict it sends every read back down the
%% delta chain.
%%
%% Best effort: every caller has already made the state durable (or read it
%% from the store), so a cache failure -- a table gone with its owner, a bad
%% window option -- is logged and dropped, never raised into the write or read
%% it accelerates.
hot_put(ProcID, Slot, Msg, Opts) ->
    ensure_hot_cache(),
    Key = hot_key(ProcID, Opts),
    try
        Cached = cacheable(Msg),
        Newest = hot_advance(Key, Slot, Cached),
        recent_put(Key, Slot, Newest, Cached, Opts)
    catch Class:Reason ->
        ?event(compute_cache,
            {hot_cache_put_failed,
                {proc_id, ProcID},
                {slot, Slot},
                {class, Class},
                {reason, Reason}
            }
        ),
        ok
    end.

%% @doc Offer `Slot' as the newest state of `Key' and return the newest slot
%% held afterwards. The move is a conditional replace, so it is atomic against
%% a concurrent put: a reader rebuilding slot S and the writer of S+1 used to
%% both see the old entry, and whichever inserted last won -- leaving S as
%% `latest' for as long as the process stayed idle. Retries only when another
%% put changed the entry between the two steps.
hot_advance(Key, Slot, Msg) ->
    case ets:lookup_element(?HOT_CACHE, Key, 2, absent) of
        absent ->
            case ets:insert_new(?HOT_CACHE, {Key, Slot, Msg}) of
                true -> Slot;
                false -> hot_advance(Key, Slot, Msg)
            end;
        Newer when Newer > Slot -> Newer;
        _ ->
            Replace =
                [{
                    {Key, '$1', '_'},
                    [{'=<', '$1', Slot}],
                    [{{{const, Key}, Slot, {const, Msg}}}]
                }],
            case ets:select_replace(?HOT_CACHE, Replace) of
                1 -> Slot;
                0 -> hot_advance(Key, Slot, Msg)
            end
    end.

%% @doc The form of a state the in-memory caches hold: without its VM
%% `snapshot'. A checkpoint's snapshot is a full VM image (~19 MB on a live Lua
%% process) that no cache reader uses: `compute' and `now' strip it from what
%% they return, `compute_cached' only tests for presence, and a delta replay
%% patches public state. The one reader that needs it -- a cold restore asking
%% `latest/4' for `snapshot+link' -- reads the store (`read_stored/3').
cacheable(Msg) when is_map(Msg) -> maps:remove(<<"snapshot">>, Msg);
cacheable(Msg) -> Msg.

%% @doc Also keep the last `process-hot-cache-slots' (default 32) states of a
%% process. A `compute&slot=N' read for a recent slot that is no longer the
%% newest -- every client settling its own write while others advance the
%% process -- otherwise rebuilt slot N by replaying every delta back to the
%% last checkpoint (up to ~1,000 full-state patch applications, seconds of CPU
%% per read). With the window, it is a lookup, or a replay of a few deltas
%% from the nearest kept slot.
recent_put(Key, Slot, Newest, Msg, Opts) ->
    Window =
        hb_util:int(
            hb_opts:get(<<"process-hot-cache-slots">>,
                ?DEFAULT_HOT_CACHE_SLOTS, Opts)
        ),
    Oldest = Newest - Window + 1,
    case Window > 0 andalso Slot >= Oldest of
        false -> replay_put(Key, Slot, Msg, Opts);
        true ->
            ets:insert(?RECENT_CACHE, {{Key, Slot}, Msg}),
            touch_recent(Key),
            expire_recent(Key, Oldest),
            enforce_recent_bytes(Key, Opts),
            ok
    end.

%% @doc Expire only slots below the window, without scanning retained history.
expire_recent(Key, Oldest) ->
    case ets:prev(?RECENT_CACHE, {Key, Oldest}) of
        {Key, Slot} ->
            ets:delete(?RECENT_CACHE, {Key, Slot}),
            expire_recent(Key, Oldest);
        _ -> ok
    end.

%% @doc Mark `Key' as the most recently written process in the recent window.
%% `?RECENT_SEQ' maps a process to its last write sequence and `?RECENT_LRU'
%% orders processes by it, so the least recently written one is `ets:first/1'.
%% Both hold one row per process with a window, a few words each. Two puts to
%% one process racing here can leave a superseded `?RECENT_LRU' row behind;
%% eviction drops such a row when it reaches it, and `sweep_recent_lru/0'
%% bounds how many can collect before that.
touch_recent(Key) ->
    Seq = erlang:unique_integer([monotonic]),
    case ets:lookup_element(?RECENT_SEQ, Key, 2, absent) of
        absent -> ok;
        Old -> ets:delete(?RECENT_LRU, {Old, Key})
    end,
    ets:insert(?RECENT_SEQ, {Key, Seq}),
    ets:insert(?RECENT_LRU, {{Seq, Key}}),
    case ets:info(?RECENT_LRU, size) > 2 * ets:info(?RECENT_SEQ, size) + 64 of
        true -> sweep_recent_lru();
        false -> ok
    end.

%% @doc Drop every superseded `?RECENT_LRU' row. A full pass, but it runs only
%% once superseded rows outnumber live ones, so it is amortized over at least
%% as many racing puts as it visits rows.
sweep_recent_lru() ->
    ets:foldl(
        fun({{Seq, Key}} = Row, ok) ->
            case ets:lookup_element(?RECENT_SEQ, Key, 2, absent) of
                Seq -> ok;
                _ -> ets:delete_object(?RECENT_LRU, Row)
            end,
            ok
        end,
        ok,
        ?RECENT_LRU
    ).

%% @doc Bound the recent window by bytes as well as by slot count.
%%
%% The window is a slot count, but an entry is a materialized process state, and
%% state size varies by orders of magnitude between processes -- `replay_put'
%% documents the same hazard: "a count bounds memory only by accident". At the
%% default 32 that does not matter. It matters a great deal once the window is
%% raised to cover a busy process, which is the whole reason to raise it: a
%% shard serving a thousand concurrent clients advances hundreds of slots
%% between a client's write and that client's read of its own reply, so the
%% window has to span that gap or every such read falls back to a delta replay.
%%
%% When over budget, evict from the LEAST RECENTLY WRITTEN process first, its
%% oldest slot first, one entry at a time until the table is back under budget,
%% rather than flushing the table the way `replay_put' does. A global flush is
%% safe there because losing a replay window costs a replay. Here it would cost
%% the opposite of what the cache is for: every in-flight client settling its
%% own write would miss at once and fall into the replay storm the window
%% exists to prevent. An idle process's window is the one no client is
%% settling against (its newest state stays in `?HOT_CACHE'), so it goes
%% first; the process being written is trimmed last, oldest first, and keeps
%% at least `?RECENT_FLOOR_SLOTS' entries -- the reads this serves cluster just
%% behind the head. Each put evicts about what it inserted, so the cost is
%% amortized: a few ordered lookups and deletes per put, never a scan.
enforce_recent_bytes(Key, Opts) ->
    LimitMB =
        hb_util:int(
            hb_opts:get(<<"process-hot-cache-mb">>,
                ?DEFAULT_HOT_CACHE_MB, Opts)
        ),
    case LimitMB > 0 of
        false -> ok;
        true -> evict_recent(Key, LimitMB * 1048576)
    end.

evict_recent(Active, Limit) ->
    case recent_bytes() >= Limit of
        false -> ok;
        true ->
            case ets:first(?RECENT_LRU) of
                '$end_of_table' -> ok;
                {Seq, Key} = Row ->
                    Live =
                        ets:lookup_element(?RECENT_SEQ, Key, 2, absent) =:= Seq,
                    case Live of
                        false ->
                            ets:delete(?RECENT_LRU, Row),
                            evict_recent(Active, Limit);
                        true when Key =:= Active ->
                            trim_active(Active, Limit);
                        true ->
                            case recent_slots(Key, 1) of
                                [Slot] ->
                                    ets:delete(?RECENT_CACHE, {Key, Slot});
                                [] ->
                                    ets:delete(?RECENT_LRU, Row),
                                    ets:delete_object(?RECENT_SEQ, {Key, Seq})
                            end,
                            evict_recent(Active, Limit)
                    end
            end
    end.

%% @doc Every other process's window is gone and the table is still over
%% budget: trim the process being written, oldest first, down to the floor.
trim_active(Active, Limit) ->
    case recent_bytes() >= Limit of
        false -> ok;
        true ->
            case recent_slots(Active, ?RECENT_FLOOR_SLOTS + 1) of
                [Oldest | Rest] when length(Rest) >= ?RECENT_FLOOR_SLOTS ->
                    ets:delete(?RECENT_CACHE, {Active, Oldest}),
                    trim_active(Active, Limit);
                _ ->
                    % At the floor. Trimming further would evict the states
                    % just written, which would make the put pointless.
                    ok
            end
    end.

%% @doc The oldest `N' slots `Key' holds in the recent window. The key prefix
%% is bound, so this is an ordered range read, not a table scan.
recent_slots(Key, N) ->
    case ets:select(?RECENT_CACHE, [{{{Key, '$1'}, '_'}, [], ['$1']}], N) of
        {Slots, _} -> Slots;
        '$end_of_table' -> []
    end.

%% @doc Held bytes, as ETS accounts them.
%%
%% `ets:info/2' `memory' counts the table's own words and NOT the payload of
%% refc binaries (over 64 bytes), which live off-heap and are only pointed at
%% from the table. That is accurate for what this cache holds -- a process
%% state is a map of many small fields, which is why a live node reports
%% ~138 KB/entry across 916 entries. The one large binary in play, the VM
%% `snapshot' a checkpoint carries, is removed by `cacheable/1' in `hot_put'
%% before a state reaches any of these tables. A state carrying some other
%% large binary would still be under-counted. `replay_put' bounds itself the
%% same way and inherits the same caveat.
recent_bytes() ->
    case ets:info(?RECENT_CACHE, memory) of
        Words when is_integer(Words) -> Words * erlang:system_info(wordsize);
        _ -> 0
    end.

%% @doc Keep the states a replay rebuilds on its way to a historical slot.
%% `recent_put' holds a window at the head of a process, so a read of a slot far
%% behind the head -- a client walking back through history -- rebuilt every
%% intermediate state and then discarded all of them, and the next read of a
%% neighbouring slot replayed the same chain again from the last checkpoint.
%% The window here follows the slot being rebuilt rather than the head, which is
%% where a replay's working set actually is.
%%
%% Bounded twice: per process by `process-replay-cache-slots', and overall by
%% `process-replay-cache-mb'. The global bound counts bytes rather than entries
%% because entry size tracks process state size -- a game authority holds around
%% 330 KB per entry where a small token process holds a fraction of that -- so a
%% count bounds memory only by accident. The byte bound is set well above what
%% concurrent replays hold -- eight processes were observed holding 713 entries
%% in 593 MB -- because reaching it drops every process's window at once.
%% Losing the table costs a replay, never state: every entry is derived from a
%% durable delta or checkpoint.
replay_put(Key, Slot, Msg, Opts) ->
    Window = hb_util:int(
        hb_opts:get(<<"process-replay-cache-slots">>,
            ?DEFAULT_REPLAY_CACHE_SLOTS, Opts)
    ),
    LimitMB = hb_util:int(
        hb_opts:get(<<"process-replay-cache-mb">>,
            ?DEFAULT_REPLAY_CACHE_MB, Opts)
    ),
    case Window > 0 of
        false -> ok;
        true ->
            % A flush drops every process's window at once, so the bound is
            % set where it is reached rarely. It costs replays, never state.
            Held =
                case ets:info(?REPLAY_CACHE, memory) of
                    Words when is_integer(Words) ->
                        Words * erlang:system_info(wordsize);
                    _ -> 0
                end,
            case Held >= LimitMB * 1048576 of
                true -> ets:delete_all_objects(?REPLAY_CACHE);
                false -> ok
            end,
            ets:insert(?REPLAY_CACHE, {{Key, Slot}, Msg}),
            Oldest = Slot - Window + 1,
            ets:select_delete(
                ?REPLAY_CACHE,
                [{{{Key, '$1'}, '_'}, [{'<', '$1', Oldest}], [true]}]
            ),
            ok
    end.

%% @doc The cache key of a process: its ID and the `process-cache-scope'.
%% `hb_store_gc:forget_process_cache/3' builds the same key and drops entries
%% from these tables by name (core code cannot call a preloaded device module):
%% a change to the key or the table layout must be made there too. The
%% store is deliberately not part of it. A node runs one store stack per scope,
%% and the store descriptor is not a stable identity: `latest_from_store/4'
%% re-scopes it before reading, so keying on it would split one process's
%% entries across descriptors and miss on every such read. Two nodes sharing a
%% VM (tests) with different stores must use distinct process IDs or scopes.
hot_key(ProcID, Opts) ->
    {
        ProcID,
        hb_opts:get(<<"process-cache-scope">>, local, Opts)
    }.

%% @doc Make sure the hot cache tables exist. They are owned by a dedicated
%% process that never exits: an ETS table dies with its owner, and the first
%% caller is often a short-lived HTTP request, which used to take the whole
%% hot cache with it when it finished -- leaving every `now' read to rebuild
%% the latest state from the delta chain.
ensure_hot_cache() ->
    case ets:whereis(?HOT_CACHE) of
        undefined ->
            Parent = self(),
            Ref = make_ref(),
            {Owner, Mon} =
                spawn_monitor(fun() -> hot_cache_owner(Parent, Ref) end),
            receive
                {Ref, _} -> ok;
                {'DOWN', Mon, process, Owner, _} -> ok
            after 5000 -> ok
            end,
            erlang:demonitor(Mon, [flush]),
            ok;
        _ -> ok
    end.

%% @doc Become the one owner of the cache tables, or report that one exists.
%% Ownership is claimed by registering a name, which is atomic, rather than by
%% creating the first table: when an owner dies its tables are deleted one at a
%% time, so a successor could create the first table while an older one still
%% held a later name, and crash on it.
hot_cache_owner(Parent, Ref) ->
    case catch register(?HOT_CACHE_OWNER, self()) of
        true ->
            lists:foreach(
                fun new_cache_table/1,
                [
                    {?RECENT_CACHE, ordered_set},
                    {?REPLAY_CACHE, ordered_set},
                    {?RECENT_SEQ, set},
                    {?RECENT_LRU, ordered_set},
                    % Last: callers test for this one to skip creation.
                    {?HOT_CACHE, set}
                ]
            ),
            Parent ! {Ref, created},
            receive after infinity -> ok end;
        _ ->
            Parent ! {Ref, exists}
    end.

%% @doc Create a named table, waiting out a dead predecessor's table of the
%% same name while the runtime deletes it.
new_cache_table(Spec) -> new_cache_table(Spec, 1000).
new_cache_table({Name, Type} = Spec, Tries) ->
    try ets:new(Name, [named_table, public, Type])
    catch error:badarg when Tries > 0 ->
        timer:sleep(1),
        new_cache_table(Spec, Tries - 1)
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
latest(ProcID, RawRequiredPath, undefined, RawOpts)
        when RawRequiredPath == undefined; RawRequiredPath == [] ->
    % The newest state this node has written for the process is held in the
    % hot cache (every delta write and every rebuilt read puts it there, and an
    % older slot never replaces a newer one). Answer from it, instead of
    % listing every slot the process has ever computed -- thousands of store
    % entries, parsed and sorted, on every `now' read.
    case hot_newest(ProcID, RawOpts) of
        {ok, Slot, Msg} -> {ok, Slot, Msg};
        not_found -> latest_from_store(ProcID, RawRequiredPath, undefined, RawOpts)
    end;
latest(ProcID, RawRequiredPath, Limit, RawOpts) ->
    latest_from_store(ProcID, RawRequiredPath, Limit, RawOpts).

hot_newest(ProcID, Opts) ->
    ensure_hot_cache(),
    Key = hot_key(ProcID, Opts),
    try ets:lookup(?HOT_CACHE, Key) of
        [{Key, Slot, Msg}] -> {ok, Slot, Msg};
        [] -> not_found
    catch error:badarg -> not_found
    end.

latest_from_store(ProcID, RawRequiredPath, Limit, RawOpts) ->
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
            % Found. Return the slot number and the message at that slot. A
            % required path (in practice `snapshot+link', for a cold restore)
            % may name a key the in-memory caches strip, so read the store.
            {ok, Msg} =
                case RequiredPath of
                    [] -> read(ProcID, SlotNum, Opts);
                    _ -> read_stored(ProcID, SlotNum, Opts)
                end,
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

%% @doc A read of a slot below the head window keeps the states its replay
%% rebuilt, so a read of a neighbouring historical slot does not walk the delta
%% chain back to the checkpoint a second time.
replay_cache_serves_neighbouring_slots_test_() ->
    {timeout, 60, fun() ->
        application:ensure_all_started(hb),
        Store = hb_test_utils:test_store(hb_store_lmdb),
        Opts = #{
            <<"store">> => [Store],
            <<"priv-wallet">> => ar_wallet:new(),
            % Slot 0 is the only checkpoint, so every later slot is a delta.
            <<"process-delta-checkpoint-slots">> => 1000,
            % A head window of two leaves slot 10 well below it.
            <<"process-hot-cache-slots">> => 2
        },
        ProcID = hb_util:encode(crypto:strong_rand_bytes(32)),
        Results0 = #{ <<"output">> => #{ <<"data">> => <<"0">> } },
        State0 = #{
            <<"at-slot">> => 0,
            <<"count">> => <<"0">>,
            <<"results">> => Results0
        },
        {ok, _} =
            write(ProcID, 0, with_delta(State0, [], Results0, Opts), Opts),
        lists:foldl(
            fun(Slot, State) ->
                Bin = integer_to_binary(Slot),
                Patches =
                    [#{ <<"path">> => <<"/count">>, <<"value">> => Bin }],
                Results = #{ <<"output">> => #{ <<"data">> => Bin } },
                {ok, Next} =
                    hb_process_delta:apply(State, Patches, Results, Slot, Opts),
                {ok, _} =
                    write(
                        ProcID,
                        Slot,
                        with_delta(Next, Patches, Results, Opts),
                        Opts
                    ),
                Next
            end,
            State0,
            lists:seq(1, 12)
        ),
        % Drop every in-memory trace of the process, so the first read is cold.
        HotKey = hot_key(ProcID, Opts),
        ets:delete(?HOT_CACHE, HotKey),
        ets:match_delete(?RECENT_CACHE, {{HotKey, '_'}, '_'}),
        ets:match_delete(?REPLAY_CACHE, {{HotKey, '_'}, '_'}),
        erlang:trace_pattern({hb_process_delta, apply, 5}, true, [call_count]),
        Applies =
            fun() ->
                {call_count, N} =
                    erlang:trace_info(
                        {hb_process_delta, apply, 5},
                        call_count
                    ),
                N
            end,
        Start = Applies(),
        {ok, Slot10} = read(ProcID, 10, Opts),
        Cold = Applies() - Start,
        ?assertEqual(<<"10">>, hb_ao:get(<<"count">>, Slot10, Opts)),
        % Cold read rebuilt the chain from the checkpoint.
        ?assert(Cold >= 10),
        {ok, Slot9} = read(ProcID, 9, Opts),
        Warm = Applies() - Start - Cold,
        ?assertEqual(<<"9">>, hb_ao:get(<<"count">>, Slot9, Opts)),
        % Its base was rebuilt on the way to slot 10 and kept.
        ?assertEqual(0, Warm),
        erlang:trace_pattern({hb_process_delta, apply, 5}, false, [call_count])
    end}.

delta_roundtrip_test_() ->
    {timeout, 60, fun() ->
        application:ensure_all_started(hb),
        delta_roundtrip(#{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new()
        })
    end}.

%% @doc Full checkpoints and deltas remain directly readable and replayable
%% from cold storage without reverse-index entries. Ordinary cache data remains
%% queryable through the match index.
computed_writes_skip_match_index_and_replay_test_() ->
    {timeout, 60, fun() ->
        application:ensure_all_started(hb),
        Store = hb_test_utils:test_store(hb_store_lmdb),
        Opts = #{
            <<"store">> => [Store],
            <<"match-index">> => [Store],
            <<"priv-wallet">> => ar_wallet:new(),
            <<"process-delta-checkpoint-slots">> => 2
        },
        ProcID = hb_util:encode(crypto:strong_rand_bytes(32)),
        Marker = hb_util:encode(crypto:strong_rand_bytes(32)),
        Results0 = #{ <<"output">> => #{ <<"data">> => <<"0">> } },
        FullProcID = hb_util:encode(crypto:strong_rand_bytes(32)),
        FullMarker = <<"full-", Marker/binary>>,
        FullState = #{
            <<"at-slot">> => 0,
            <<"full-marker">> => FullMarker,
            <<"results">> => Results0
        },
        {ok, _} = write_full(FullProcID, 0, FullState, Opts),
        {ok, ReadFull} = read(FullProcID, 0, Opts),
        ?assertEqual(FullMarker, hb_ao:get(<<"full-marker">>, ReadFull, Opts)),
        ?assertEqual(
            {error, not_found},
            hb_cache:match(#{ <<"full-marker">> => FullMarker }, Opts)
        ),
        State0 = #{
            <<"at-slot">> => 0,
            <<"count">> => <<"0">>,
            <<"computed-marker">> => Marker,
            <<"results">> => Results0
        },
        {ok, _} = write(
            ProcID,
            0,
            with_delta(State0, [], Results0, Opts),
            Opts
        ),
        {ok, Full0} = read(ProcID, 0, Opts),
        ?assertEqual(Marker, hb_ao:get(<<"computed-marker">>, Full0, Opts)),
        ?assertEqual(
            {error, not_found},
            hb_cache:match(#{ <<"computed-marker">> => Marker }, Opts)
        ),
        Patches1 = [#{ <<"path">> => <<"/count">>, <<"value">> => <<"1">> }],
        Results1 = #{ <<"output">> => #{ <<"data">> => <<"1">> } },
        {ok, State1} = hb_process_delta:apply(State0, Patches1, Results1, 1, Opts),
        {ok, _} = write(
            ProcID,
            1,
            with_delta(State1, Patches1, Results1, Opts),
            Opts
        ),
        Patches2 = [#{ <<"path">> => <<"/count">>, <<"value">> => <<"2">> }],
        Results2 = #{ <<"output">> => #{ <<"data">> => <<"2">> } },
        {ok, State2} = hb_process_delta:apply(State1, Patches2, Results2, 2, Opts),
        {ok, _} = write(
            ProcID,
            2,
            with_delta(State2, Patches2, Results2, Opts),
            Opts
        ),
        {ok, Full2} = read(ProcID, 2, Opts),
        ?assertEqual(<<"2">>, hb_ao:get(<<"count">>, Full2, Opts)),
        HotKey = hot_key(ProcID, Opts),
        ets:delete(?HOT_CACHE, HotKey),
        ets:delete(?RECENT_CACHE, {HotKey, 0}),
        ets:delete(?RECENT_CACHE, {HotKey, 1}),
        ets:delete(?RECENT_CACHE, {HotKey, 2}),
        erlang:trace_pattern({hb_process_delta, apply, 5}, true, [call_count]),
        {Cold1, Replayed} =
            try
                {ok, ColdRead} = read(ProcID, 1, Opts),
                {call_count, ReplayCount} =
                    erlang:trace_info({hb_process_delta, apply, 5}, call_count),
                {ColdRead, ReplayCount}
            after
                erlang:trace_pattern(
                    {hb_process_delta, apply, 5},
                    false,
                    [call_count]
                )
            end,
        ?assert(Replayed > 0),
        ?assertEqual(<<"1">>, hb_ao:get(<<"count">>, Cold1, Opts)),
        ?assertEqual(
            {error, not_found},
            hb_cache:match(#{ <<"count">> => <<"2">> }, Opts)
        ),
        % `~match@1.0' only indexes signed IDs, so the ordinary write is a
        % signed message.
        NormalMarker = <<"normal-", Marker/binary>>,
        {ok, _} = hb_cache:write(
            hb_message:commit(#{ <<"normal-marker">> => NormalMarker }, Opts),
            Opts
        ),
        ?assertMatch(
            {ok, [_ | _]},
            hb_cache:match(#{ <<"normal-marker">> => NormalMarker }, Opts)
        )
    end}.

%% @doc The hot cache must outlive whichever process happened to create it (in
%% production, usually a short-lived HTTP request), must never let an older
%% slot evict a newer one, and must keep a state rebuilt from the delta chain
%% so the next read of that slot does not rebuild it again.
hot_cache_survives_creator_and_keeps_newest_test() ->
    application:ensure_all_started(hb),
    Opts = #{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"priv-wallet">> => ar_wallet:new(),
        <<"process-delta-checkpoint-slots">> => 1000
    },
    ProcID = hb_util:encode(crypto:strong_rand_bytes(32)),
    Results = #{ <<"output">> => #{ <<"data">> => <<"0">> } },
    State0 = #{ <<"at-slot">> => 0, <<"count">> => <<"0">>, <<"results">> => Results },
    {ok, _} = write(ProcID, 0, with_delta(State0, [], Results, Opts), Opts),
    Patches = [#{ <<"path">> => <<"/count">>, <<"value">> => <<"1">> }],
    {ok, State1} = hb_process_delta:apply(State0, Patches, Results, 1, Opts),
    % A short-lived process writes slot 1 and exits.
    {Writer, Mon} =
        spawn_monitor(fun() ->
            {ok, _} = write(ProcID, 1, with_delta(State1, Patches, Results, Opts), Opts)
        end),
    receive {'DOWN', Mon, process, Writer, normal} -> ok end,
    ?assertMatch({ok, _}, hot_read(ProcID, 1, Opts)),
    % An older slot never replaces the newest.
    hot_put(ProcID, 0, State0, Opts),
    ?assertMatch({ok, _}, hot_read(ProcID, 1, Opts)),
    ?assertMatch({ok, 1, _}, hot_newest(ProcID, Opts)),
    % Drop the entry: the next read rebuilds slot 1 from its delta once, and
    % keeps it.
    ets:delete(?HOT_CACHE, hot_key(ProcID, Opts)),
    {ok, Rebuilt} = read(ProcID, 1, Opts),
    ?assertEqual(<<"1">>, hb_ao:get(<<"count">>, Rebuilt, Opts)),
    ?assertMatch({ok, _}, hot_read(ProcID, 1, Opts)).

%% @doc `latest' answers from the hot cache without listing every slot, and a
%% read of a recent slot that is no longer the newest does not replay the delta
%% chain back to the checkpoint.
recent_slots_are_served_without_replay_test() ->
    application:ensure_all_started(hb),
    Opts = #{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"priv-wallet">> => ar_wallet:new(),
        <<"process-delta-checkpoint-slots">> => 1000,
        <<"process-hot-cache-slots">> => 8
    },
    ProcID = hb_util:encode(crypto:strong_rand_bytes(32)),
    Results = #{ <<"output">> => #{ <<"data">> => <<"0">> } },
    State0 = #{ <<"at-slot">> => 0, <<"count">> => <<"0">>, <<"results">> => Results },
    {ok, _} = write(ProcID, 0, with_delta(State0, [], Results, Opts), Opts),
    lists:foldl(
        fun(Slot, Prev) ->
            Count = integer_to_binary(Slot),
            Patches = [#{ <<"path">> => <<"/count">>, <<"value">> => Count }],
            {ok, Next} = hb_process_delta:apply(Prev, Patches, Results, Slot, Opts),
            {ok, _} = write(ProcID, Slot, with_delta(Next, Patches, Results, Opts), Opts),
            Next
        end,
        State0,
        lists:seq(1, 20)
    ),
    Calls =
        fun(MFA, Fun) ->
            erlang:trace_pattern(MFA, true, [call_count]),
            Res = Fun(),
            {call_count, N} = erlang:trace_info(MFA, call_count),
            erlang:trace_pattern(MFA, false, [call_count]),
            {Res, N}
        end,
    {{ok, 20, Latest}, 0} =
        Calls({hb_cache, list_numbered, 2}, fun() -> latest(ProcID, Opts) end),
    ?assertEqual(<<"20">>, hb_ao:get(<<"count">>, Latest, Opts)),
    % Slot 15 is inside the 8-slot window: no delta is replayed.
    {{ok, Recent}, 0} =
        Calls({hb_process_delta, apply, 5}, fun() -> read(ProcID, 15, Opts) end),
    ?assertEqual(<<"15">>, hb_ao:get(<<"count">>, Recent, Opts)),
    % Slot 5 is outside it: rebuilt from the checkpoint, and still correct.
    {{ok, Old}, Replayed} =
        Calls({hb_process_delta, apply, 5}, fun() -> read(ProcID, 5, Opts) end),
    ?assertEqual(<<"5">>, hb_ao:get(<<"count">>, Old, Opts)),
    ?assert(Replayed > 0),
    % A historical read never replaces the newest state.
    ?assertMatch({ok, 20, _}, latest(ProcID, Opts)).

snapshot_presence_is_structural_test() ->
    application:ensure_all_started(hb),
    Opts = #{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"priv-wallet">> => ar_wallet:new(),
        <<"process-delta-checkpoint-slots">> => 1000
    },
    ProcID = hb_util:encode(crypto:strong_rand_bytes(32)),
    Results = #{ <<"output">> => #{ <<"data">> => <<"0">> } },
    State = #{
        <<"device">> => <<"test-device@1.0">>,
        <<"at-slot">> => 0,
        <<"results">> => Results
    },
    {ok, _} = write(
        ProcID,
        0,
        with_delta(State, [], Results, Opts),
        Opts
    ),
    ?assertNot(should_checkpoint(ProcID, 1, State, Opts)),
    ?assert(should_checkpoint(
        ProcID,
        1,
        State#{ <<"snapshot">> => #{} },
        Opts
    )).

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

%% @doc The byte bound must not fire at the default window. If it did, every
%% node would silently lose its recent window on upgrade.
hot_cache_byte_bound_inert_at_default_test() ->
    ensure_hot_cache(),
    Key = {<<"inert-", (integer_to_binary(erlang:unique_integer([positive])))/binary>>, local},
    Opts = #{},
    lists:foreach(
        fun(S) -> recent_put(Key, S, 31, #{ <<"n">> => S }, Opts) end,
        lists:seq(0, 31)
    ),
    Held = ets:select_count(?RECENT_CACHE, [{{{Key, '$1'}, '_'}, [], [true]}]),
    ?assertEqual(32, Held),
    ets:match_delete(?RECENT_CACHE, {{Key, '_'}, '_'}).

%% @doc Over budget with no other process to evict, the process being written
%% trims its own window from the oldest end and keeps the newest run.
hot_cache_byte_bound_trims_newest_half_test() ->
    ensure_hot_cache(),
    Key = {<<"trim-", (integer_to_binary(erlang:unique_integer([positive])))/binary>>, local},
    %% Bulk must come from heap terms, not one big binary: a >64-byte binary is
    %% refc and stored off-heap, so `ets:info/2' `memory' would not see it and
    %% the bound would never fire. Real process states are maps of many small
    %% fields, so this is also the shape the bound is tuned for.
    Opts = #{ <<"process-hot-cache-slots">> => 64, <<"process-hot-cache-mb">> => 1 },
    Fat = maps:from_list([ {integer_to_binary(I), I} || I <- lists:seq(1, 3000) ]),
    lists:foreach(
        fun(S) -> recent_put(Key, S, 63, Fat#{ <<"n">> => S }, Opts) end,
        lists:seq(0, 63)
    ),
    Slots = lists:sort(ets:select(?RECENT_CACHE, [{{{Key, '$1'}, '_'}, [], ['$1']}])),
    ?assert(length(Slots) < 64),
    ?assert(length(Slots) > 0),
    %% What survives is the newest run: the reads this serves sit just behind
    %% the head, so trimming from the old end is the useful direction.
    ?assertEqual(63, lists:max(Slots)),
    ets:match_delete(?RECENT_CACHE, {{Key, '_'}, '_'}).

%% @doc Over budget, the least recently written process loses its window
%% first, oldest slot first, and the process being written keeps its window.
%% Trimming the writer instead -- the old policy -- collapsed a busy process to
%% one entry while idle processes kept theirs and memory stayed at the bound.
hot_cache_byte_bound_evicts_idle_processes_first_test() ->
    ensure_hot_cache(),
    ets:delete_all_objects(?RECENT_CACHE),
    U = integer_to_binary(erlang:unique_integer([positive])),
    [IdleA, IdleB, IdleC] =
        [{<<N/binary, "-", U/binary>>, local} || N <- [<<"a">>, <<"b">>, <<"c">>]],
    Active = {<<"active-", U/binary>>, local},
    Opts = #{ <<"process-hot-cache-slots">> => 64, <<"process-hot-cache-mb">> => 1 },
    Fat = maps:from_list([ {integer_to_binary(I), I} || I <- lists:seq(1, 1000) ]),
    Count = fun(K) -> ets:select_count(?RECENT_CACHE, [{{{K, '$1'}, '_'}, [], [true]}]) end,
    % Three idle processes, each holding four entries, written a, b, c.
    lists:foreach(
        fun(K) ->
            [ recent_put(K, S, 3, Fat#{ <<"n">> => S }, Opts) || S <- lists:seq(0, 3) ]
        end,
        [IdleA, IdleB, IdleC]
    ),
    ?assertEqual([4, 4, 4], [Count(K) || K <- [IdleA, IdleB, IdleC]]),
    Trace =
        [
            begin
                recent_put(Active, S, S, Fat#{ <<"n">> => S }, Opts),
                {S, Count(IdleA), Count(IdleC), Count(Active)}
            end
        ||
            S <- lists:seq(0, 39)
        ],
    % The first eviction falls on `a', the least recently written, while `c'
    % is still whole.
    ?assertMatch([_ | _], [T || {_, A, 4, _} = T <- Trace, A < 4]),
    % At the end the idle windows are gone and the writer keeps a real window,
    % its newest slots, under the byte bound.
    ?assertEqual([0, 0, 0], [Count(K) || K <- [IdleA, IdleB, IdleC]]),
    ActiveSlots = ets:select(?RECENT_CACHE, [{{{Active, '$1'}, '_'}, [], ['$1']}]),
    ?assert(length(ActiveSlots) >= 8),
    ?assertEqual(39, lists:max(ActiveSlots)),
    ?assert(recent_bytes() < 1048576),
    ets:match_delete(?RECENT_CACHE, {{Active, '_'}, '_'}).

%% @doc A zero byte limit disables the bound rather than trimming everything.
hot_cache_byte_bound_zero_disables_test() ->
    ensure_hot_cache(),
    Key = {<<"zero-", (integer_to_binary(erlang:unique_integer([positive])))/binary>>, local},
    Opts = #{ <<"process-hot-cache-slots">> => 16, <<"process-hot-cache-mb">> => 0 },
    Fat = maps:from_list([ {integer_to_binary(I), I} || I <- lists:seq(1, 3000) ]),
    lists:foreach(
        fun(S) -> recent_put(Key, S, 15, Fat#{ <<"n">> => S }, Opts) end,
        lists:seq(0, 15)
    ),
    ?assertEqual(16, ets:select_count(?RECENT_CACHE, [{{{Key, '$1'}, '_'}, [], [true]}])),
    ets:match_delete(?RECENT_CACHE, {{Key, '_'}, '_'}).

%% @doc Budget eviction preserves latest reads and durable historical results.
recent_cache_budget_test_() ->
    {timeout, 60, fun() ->
        application:ensure_all_started(hb),
        ets:delete_all_objects(?RECENT_CACHE),
        Store = hb_test_utils:test_store(hb_store_lmdb),
        Opts = #{
            <<"store">> => [Store],
            <<"priv-wallet">> => ar_wallet:new(),
            <<"process-hot-cache-slots">> => 2048,
            <<"process-hot-cache-mb">> => 1,
            <<"process-delta-checkpoint-slots">> => 8
        },
        ProcID = hb_util:encode(crypto:strong_rand_bytes(32)),
        Results = #{ <<"output">> => #{ <<"data">> => <<"ok">> } },
        State0 = #{
            <<"at-slot">> => 0, <<"count">> => 0,
            <<"payload">> => lists:seq(1, 12000), <<"results">> => Results
        },
        {ok, _} = write(ProcID, 0, with_delta(State0, [], Results, Opts), Opts),
        lists:foldl(fun(Slot, Prev) ->
            Patches = [#{ <<"path">> => <<"/count">>, <<"value">> => Slot }],
            {ok, Next} = hb_process_delta:apply(Prev, Patches, Results, Slot, Opts),
            {ok, _} = write(ProcID, Slot, with_delta(Next, Patches, Results, Opts), Opts),
            ?assert(ets:info(?RECENT_CACHE, memory) * erlang:system_info(wordsize)
                =< 1048576),
            Next
        end, State0, lists:seq(1, 12)),
        ?assertEqual([], ets:lookup(?RECENT_CACHE, {hot_key(ProcID, Opts), 0})),
        ?assertMatch({ok, 12, _}, hot_newest(ProcID, Opts)),
        {ok, Historical} = read(ProcID, 3, Opts),
        ?assertEqual(3, hb_ao:get(<<"count">>, Historical, Opts)),
        ?assertMatch({ok, 12, _}, hot_newest(ProcID, Opts)),
        ok
    end}.

%% @doc Expiry removes gaps and negative initialization slots, without touching
%% neighbouring processes or copying retained public state out of ETS.
recent_expiry_boundaries_test() ->
    ensure_hot_cache(),
    U = integer_to_binary(erlang:unique_integer([positive])),
    Key = {<<"expiry-", U/binary>>, local},
    Other = {<<"expiry-other-", U/binary>>, local},
    lists:foreach(fun(Slot) ->
        ets:insert(?RECENT_CACHE, {{Key, Slot}, #{ <<"slot">> => Slot }})
    end, [-1, 0, 3, 9, 10, 11]),
    ets:insert(?RECENT_CACHE, {{Other, 0}, #{}}),
    expire_recent(Key, 10),
    ?assertEqual([10, 11], ets:select(?RECENT_CACHE,
        [{{{Key, '$1'}, '_'}, [], ['$1']}])),
    ?assertMatch([_], ets:lookup(?RECENT_CACHE, {Other, 0})),
    expire_recent(Key, 10),
    ets:match_delete(?RECENT_CACHE, {{Key, '_'}, '_'}),
    ets:delete(?RECENT_CACHE, {Other, 0}).

%% @doc A reader putting a rebuilt slot S races the writer of S+1. The head must
%% end at S+1 every time: a lost race used to leave S as `latest' for as long
%% as the process stayed idle. The stale head is a large state so that copying
%% it out -- what the old lookup-then-insert did between its two steps -- holds
%% the race window open long enough to lose it reliably.
hot_put_race_keeps_newest_test_() ->
    {timeout, 60, fun() ->
        ensure_hot_cache(),
        ProcID = <<"race-", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
        Opts = #{ <<"process-hot-cache-slots">> => 4 },
        Key = hot_key(ProcID, Opts),
        Stale = maps:from_list([ {integer_to_binary(I), I} || I <- lists:seq(1, 3000) ]),
        Schedulers = erlang:system_info(schedulers_online),
        Round =
            fun() ->
                ets:insert(?HOT_CACHE, {Key, 0, Stale}),
                Self = self(),
                Go = make_ref(),
                Put =
                    fun(Slot, Scheduler) ->
                        spawn_opt(
                            fun() ->
                                receive Go -> ok end,
                                hot_put(ProcID, Slot, #{ <<"n">> => Slot }, Opts),
                                Self ! {Go, Slot}
                            end,
                            [{scheduler, Scheduler}]
                        )
                    end,
                % On separate schedulers, so the two puts really overlap.
                Pids = [Put(10, 1), Put(11, min(2, Schedulers))],
                [ P ! Go || P <- Pids ],
                [ receive {Go, S} -> ok end || S <- [10, 11] ],
                latest(ProcID, Opts)
            end,
        Lost = [ R || R <- [ Round() || _ <- lists:seq(1, 200) ], element(2, R) =/= 11 ],
        ets:delete(?HOT_CACHE, Key),
        ets:match_delete(?RECENT_CACHE, {{Key, '_'}, '_'}),
        ?assertEqual(0, length(Lost))
    end}.

%% @doc A checkpoint's VM snapshot never reaches the in-memory caches, where
%% the byte bound cannot see it, and does not leak into the delta slots built
%% on that checkpoint. A cold restore asking for `snapshot+link' still gets it,
%% from the store.
snapshot_is_not_cached_in_memory_test_() ->
    {timeout, 60, fun() ->
        application:ensure_all_started(hb),
        Opts = #{
            <<"store">> => [hb_test_utils:test_store(hb_store_lmdb)],
            <<"priv-wallet">> => ar_wallet:new(),
            <<"process-delta-checkpoint-slots">> => 1000
        },
        ProcID = hb_util:encode(crypto:strong_rand_bytes(32)),
        Image = crypto:strong_rand_bytes(1024 * 1024),
        Results = #{ <<"output">> => #{ <<"data">> => <<"0">> } },
        State0 = #{
            <<"at-slot">> => 0,
            <<"count">> => <<"0">>,
            <<"results">> => Results,
            <<"snapshot">> => #{ <<"data">> => Image }
        },
        {ok, _} = write(ProcID, 0, with_delta(State0, [], Results, Opts), Opts),
        Patches = [#{ <<"path">> => <<"/count">>, <<"value">> => <<"1">> }],
        {ok, State1} =
            hb_process_delta:apply(
                maps:remove(<<"snapshot">>, State0), Patches, Results, 1, Opts),
        {ok, _} = write(ProcID, 1, with_delta(State1, Patches, Results, Opts), Opts),
        Key = hot_key(ProcID, Opts),
        [{_, Cached0}] = ets:lookup(?RECENT_CACHE, {Key, 0}),
        ?assertNot(maps:is_key(<<"snapshot">>, Cached0)),
        % Rebuild slot 1 from the store: the checkpoint base is read with its
        % snapshot, and the delta state must not inherit it.
        ets:delete(?HOT_CACHE, Key),
        ets:match_delete(?RECENT_CACHE, {{Key, '_'}, '_'}),
        {ok, Rebuilt} = read(ProcID, 1, Opts),
        ?assertEqual(<<"1">>, hb_ao:get(<<"count">>, Rebuilt, Opts)),
        ?assertNot(maps:is_key(<<"snapshot">>, Rebuilt)),
        {ok, 1, Head} = hot_newest(ProcID, Opts),
        ?assertNot(maps:is_key(<<"snapshot">>, Head)),
        % The cold-restore lookup finds the checkpoint and its snapshot.
        {ok, 0, Restore} = latest(ProcID, [<<"snapshot+link">>], undefined, Opts),
        Snapshot = hb_cache:ensure_all_loaded(maps:get(<<"snapshot">>, Restore), Opts),
        ?assertEqual(Image, hb_ao:get(<<"data">>, Snapshot, Opts)),
        ets:delete(?HOT_CACHE, Key),
        ets:match_delete(?RECENT_CACHE, {{Key, '_'}, '_'})
    end}.

%% @doc The caches are best effort: once a slot is durable, nothing the cache
%% layer raises may fail the write that made it so.
cache_failure_never_fails_a_durable_write_test_() ->
    {timeout, 60, fun() ->
        application:ensure_all_started(hb),
        Opts = #{
            <<"store">> => [hb_test_utils:test_store(hb_store_lmdb)],
            <<"priv-wallet">> => ar_wallet:new(),
            <<"process-hot-cache-slots">> => <<"not-a-number">>
        },
        ProcID = hb_util:encode(crypto:strong_rand_bytes(32)),
        State = #{ <<"at-slot">> => 0, <<"count">> => <<"0">> },
        ?assertMatch({ok, _}, write(ProcID, 0, State, Opts)),
        {ok, Read} = read(ProcID, 0, Opts#{ <<"process-hot-cache-slots">> => 4 }),
        ?assertEqual(<<"0">>, hb_ao:get(<<"count">>, Read, Opts))
    end}.

%% @doc Killing the table owner under concurrent puts and reads raises nothing
%% to the callers, and the next caller restores every table under one owner.
cache_owner_death_is_not_raised_test_() ->
    {timeout, 120, fun() ->
        ensure_hot_cache(),
        Opts = #{ <<"process-hot-cache-slots">> => 4 },
        Self = self(),
        Raised =
            lists:sum([
                begin
                    Owner = ets:info(?HOT_CACHE, owner),
                    Workers =
                        [
                            spawn(fun() ->
                                Res =
                                    [
                                        catch begin
                                            hot_put(<<"od">>, I * 1000 + J, #{}, Opts),
                                            hot_read(<<"od">>, I * 1000 + J, Opts),
                                            hot_newest(<<"od">>, Opts)
                                        end
                                    ||
                                        J <- lists:seq(1, 200)
                                    ],
                                Self ! {od, length([ x || {'EXIT', _} <- Res ])}
                            end)
                        ||
                            I <- lists:seq(1, 8)
                        ],
                    is_pid(Owner) andalso exit(Owner, kill),
                    lists:sum([ receive {od, N} -> N end || _ <- Workers ])
                end
            ||
                _ <- lists:seq(1, 30)
            ]),
        ?assertEqual(0, Raised),
        ensure_hot_cache(),
        Owner = ets:info(?HOT_CACHE, owner),
        ?assert(is_process_alive(Owner)),
        ?assertEqual(Owner, ets:info(?RECENT_CACHE, owner)),
        ?assertEqual(Owner, ets:info(?REPLAY_CACHE, owner)),
        ets:delete(?HOT_CACHE, {<<"od">>, local}),
        ets:match_delete(?RECENT_CACHE, {{{<<"od">>, local}, '_'}, '_'})
    end}.

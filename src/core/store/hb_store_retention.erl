%%% @doc Retention for the process cache: delete the computed states a node
%%% no longer needs, online, in the background, with bounded CPU.
%%%
%%% It deletes only rows of the `pstate/' namespace (`process-state-blobs' in
%%% `lib_process_cache'): one row per computed slot of a delta process, owned
%%% by that process and slot and referenced by nothing else. Nothing outside
%%% that namespace is ever a candidate -- not an assignment, a message, a
%%% process definition or anything content-addressed -- and the irreplaceable
%%% store is never even opened for writing. A build whose `elmdb' has no
%%% `delete_batch/2' (`patches/elmdb-delete.patch' not applied) never deletes:
%%% retention refuses to start and says so.
%%%
%%% Every process keeps one unbroken run of stored state from its floor up to
%%% its head, and every slot in it reads without recompute. The floor is the
%%% newest checkpoint carrying a VM snapshot (an `s' marker: the only states
%%% a worker can resume from) whose assignment is at least
%%% `store-retention-keep-seconds' old (default 259,200: three days), and
%%% never above the `store-retention-checkpoints'-th newest one (default 2),
%%% which stays the minimum kept. A slot's time is the assignment timestamp
%%% its `s' marker carries (`lib_process_cache'); a marker written before
%%% markers carried one is empty, and its assignment is read instead. A time
%%% that cannot be read counts as young, so the floor is never above it: what
%%% cannot be dated is kept. A timestamp in seconds (an Arweave-scheduled
%%% process's) is read as seconds, and one before 2020 cannot be dated.
%%% `store-retention-policy', the node operator's list of
%%% `#{ <<"process">> => <pid>, <<"keep-seconds">> => N }', sets
%%% `keep-seconds' per process; a run whose policy is invalid deletes
%%% nothing. It is never read from a process's tags: whoever spawns a process
%%% cannot make the node keep its history. `keep-seconds' 0 is the floor of
%%% the newest checkpoints alone.
%%%
%%% Every snapshot checkpoint is kept, at any age: its `s' marker and its
%%% state row are never deleted, so every process can still be resumed from
%%% any snapshot it ever took. Only the slots between them, which a worker
%%% recomputes from a snapshot, age out.
%%%
%%% A delta process (`lua-patch', `lua-atomic') loses every other row below
%%% its floor, top-down: the highest slot first, and within a slot the `s' marker
%%% before the state. So whatever a run (or a `kill -9' in the middle of one)
%%% leaves behind is still a readable chain: every remaining delta still
%%% reaches the checkpoint below it. A process with no more checkpoints than
%%% the minimum, none old enough, or whose floor checkpoint has no state row,
%%% is left alone. A full-state process (no delta rows: `lua@5.3a', WASM,
%%% native devices) keeps its snapshot checkpoints, its newest
%%% `store-retention-recent-slots' (default 32)
%%% states and every slot from its floor up; with `keep-seconds' set and no
%%% floor, all of them. A slot below the floor
%%% is answered 410: a process's state only moves forward, and nothing
%%% replays it (`dev_process:compute/3'). A slot's outbox is its `o' row, and
%%% its signed messages its `q' row, until the process's deliverer has
%%% delivered every entry and written the receipts as the slot's `x' row
%%% (`lib_process_delivery'). `o' and `q' go whenever `x' exists -- the
%%% deliverer deletes them itself, so these are leftovers of a crash -- and
%%% NEVER without it. `x' goes once the slot is below the floor (for a
%%% full-state process: once its state is not kept). `undelivered_outboxes'
%%% in a run's report counts, per process, the `o' rows with no `x'.
%%%
%%% A run lists in full only `s' markers and the `o' and `q' rows. The rows
%%% below the floor are walked downwards in windows of slots, each one found
%%% with a bounded listing, and the rows at and above it are never listed.
%%% The server remembers, per process, the floor of the last walk that
%%% finished: below it only snapshots are left, so the next walk stops there
%%% instead of stepping down through every kept snapshot. So a run's cost
%%% follows what it deletes, not what it keeps. A restarted server remembers
%%% nothing and walks each process down once.
%%%
%%% Each batch of `store-retention-batch-keys' (default 1,000) keys is one
%%% LMDB write transaction (`elmdb:delete_batch/2'), so a crash deletes a
%%% whole batch or none of it. After each batch the run sleeps long enough to
%%% keep its share of one scheduler at `store-retention-max-cpu-pct' (default
%%% 25). A run stops after `store-retention-max-candidates' keys (default
%%% 1,000,000); the next run continues.
%%%
%%% Configuration: `store-retention' (true to run), `store-retention-
%%% interval-ms' (600,000), `store-retention-first-ms' (60,000), and the keys
%%% above. The store is the first writable `hb_store_lmdb' of the node's
%%% `store' list that is not an irreplaceable store.
-module(hb_store_retention).
-export([maybe_start/1, start/1, stop/0, run_now/1, status/0, running/0]).
-export([retain/1, retain/2, plan/3, plan/4, store/1, pruned_key/1]).
-export([marker_time/1, assignment_time/3, assignment_ms/2, policy/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NS, <<"pstate">>).
%% States a full-state process keeps besides its snapshot checkpoints.
-define(RECENT, 32).
%% How long every slot is kept by default: three days.
-define(KEEP_SECONDS, 259200).
%% A timestamp below this is in seconds (an Arweave block's, which the
%% `arweave-scheduler@1.0' stamps its assignments with), not milliseconds:
%% 10^11 is 1973 in milliseconds and the year 5138 in seconds.
-define(SECONDS_BELOW, 100000000000).
%% A time before this (2020-01-01, in ms) is not a real assignment's: it
%% cannot be dated, so it is kept.
-define(EARLIEST_MS, 1577836800000).
%% Slots per window of the walk below the floor; names per listing of a
%% window, and per listing for the newest rows of a kind.
-define(WINDOW, 1000).
-define(PAGE, 1000).
-define(ROWS_PAGE, 128).
-define(ZONE_KINDS, [<<"s">>, <<"d">>, <<"f">>, <<"o">>, <<"q">>, <<"x">>]).
-define(SERVER, hb_store_retention_server).
-define(STATUS, {?MODULE, status}).
-define(RUNNING, {?MODULE, running}).

%%% The background server.

%% @doc Start the retention server when `store-retention' is true.
maybe_start(Opts) ->
    case hb_util:atom(hb_opts:get(<<"store-retention">>, false, Opts)) of
        true -> start(Opts);
        _ -> ok
    end.

start(Opts) ->
    case delete_supported() of
        false ->
            ?event(error, {store_retention_not_started, elmdb_has_no_delete_batch}),
            {error, no_delete_primitive};
        true ->
            case whereis(?SERVER) of
                undefined ->
                    Pid = spawn(fun() -> server_init(Opts) end),
                    try register(?SERVER, Pid), ok
                    catch error:badarg -> exit(Pid, kill), ok
                    end;
                _ -> ok
            end
    end.

stop() ->
    case whereis(?SERVER) of
        undefined -> ok;
        Pid -> exit(Pid, kill), ok
    end.

server_init(Opts) ->
    erlang:send_after(int(<<"store-retention-first-ms">>, 60000, Opts), self(), run),
    server(Opts, #{}).

%% `Swept': per process, the floor of its last finished walk.
server(Opts, Swept) ->
    receive
        run ->
            Report = safe_retain(Opts, Swept),
            erlang:send_after(int(<<"store-retention-interval-ms">>, 600000, Opts), self(), run),
            server(Opts, maps:get(swept, Report, Swept));
        {run_now, From, Ref} ->
            Report = safe_retain(Opts, Swept),
            From ! {Ref, maps:remove(swept, Report)},
            server(Opts, maps:get(swept, Report, Swept))
    end.

%% @doc Run once now: in the server when it runs, else inline.
run_now(Opts) ->
    case whereis(?SERVER) of
        undefined -> maps:remove(swept, safe_retain(Opts, #{}));
        Pid ->
            Ref = make_ref(),
            Pid ! {run_now, self(), Ref},
            receive {Ref, R} -> R end
    end.

status() ->
    (persistent_term:get(?STATUS, #{}))#{ running => running(),
                                          server => whereis(?SERVER) =/= undefined }.

running() -> persistent_term:get(?RUNNING, false).

safe_retain(Opts, Swept) ->
    persistent_term:put(?RUNNING, true),
    Report =
        try retain(Opts, Swept)
        catch C:R:St ->
            ?event(error, {store_retention_failed, C, R, {trace, St}}),
            #{ status => failed, error => {C, R} }
        after
            persistent_term:put(?RUNNING, false)
        end,
    Prev = persistent_term:get(?STATUS, #{}),
    persistent_term:put(?STATUS, Prev#{
        last_run => maps:remove(swept, Report),
        undelivered_outboxes => maps:get(undelivered_outboxes, Report, #{}),
        runs => maps:get(runs, Prev, 0) + 1,
        total_deleted => maps:get(total_deleted, Prev, 0) + maps:get(deleted, Report, 0)
    }),
    ?event(store_retention, {retention_run, maps:remove(swept, Report)}),
    Report.

%%% One run.

%% @doc One retention run over the node's process-cache store. `Swept' is
%% the floor of each process's last finished walk, and the report's `swept'
%% the same after this run.
retain(Opts) -> retain(Opts, #{}).
retain(Opts, Swept0) ->
    case {delete_supported(), policy(Opts)} of
        {false, _} -> #{ status => no_delete_primitive, deleted => 0 };
        {true, {error, Reason}} ->
            ?event(error, {store_retention_refused, {invalid_policy, Reason}}),
            #{ status => invalid_policy, error => Reason, deleted => 0 };
        {true, {ok, _}} ->
            Start = erlang:monotonic_time(millisecond),
            Store = store(Opts),
            ok = hb_store:start([Store], #{}, Opts),
            #{ <<"db">> := DB } = hb_store:find(Store),
            Cfg = (config(Opts))#{ swept => Swept0 },
            Max = int(<<"store-retention-max-candidates">>, 1000000, Opts),
            Batch = max(1, int(<<"store-retention-batch-keys">>, 1000, Opts)),
            Pct = min(100, max(1, int(<<"store-retention-max-cpu-pct">>, 25, Opts))),
            Procs = list(Store, ?NS),
            {Deleted, Slept, Planned, Unreadable, Swept} =
                lists:foldl(
                    fun(_P, Acc = {D, _, _, _, _}) when D >= Max -> Acc;
                       (P, {D, Sl, Pl, U, Sw}) ->
                            Budget = Max - D,
                            Delete =
                                fun(Keys, {N, S, K}) ->
                                    Now = lists:sublist(Keys, Budget - K),
                                    ok = mark_pruned(Store, P, Now, Opts),
                                    {N1, S1} = delete(DB, Now, Batch, Pct),
                                    Acc1 = {N + N1, S + S1, K + length(Now)},
                                    case K + length(Now) >= Budget of
                                        true -> {halt, Acc1};
                                        false -> {cont, Acc1}
                                    end
                                end,
                            {{D1, Sl1, Pl1}, U1, Done} = walk(Store, P, Cfg, Delete, {0, 0, 0}),
                            Sw1 = case Done of {ok, F} -> Sw#{ P => F }; halted -> Sw end,
                            {D + D1, Sl + Sl1, Pl + Pl1, U + U1, Sw1}
                    end,
                    {0, 0, 0, 0, Swept0},
                    Procs
                ),
            #{
                status => ok,
                processes => length(Procs),
                planned => Planned,
                deleted => Deleted,
                unreadable_markers => Unreadable,
                undelivered_outboxes => undelivered(Store, Procs),
                duration_ms => erlang:monotonic_time(millisecond) - Start,
                throttle_ms => Slept,
                store => maps:get(<<"name">>, Store),
                swept => maps:with(Procs, Swept)
            }
    end.

%% The run's settings.
config(Opts) ->
    {ok, Policy} = policy(Opts),
    #{
        keep => max(1, int(<<"store-retention-checkpoints">>, 2, Opts)),
        recent => max(1, int(<<"store-retention-recent-slots">>, ?RECENT, Opts)),
        keep_seconds => max(0, int(<<"store-retention-keep-seconds">>, ?KEEP_SECONDS, Opts)),
        policy => Policy,
        now => erlang:system_time(millisecond),
        opts => Opts
    }.

%% @doc The node operator's per-process overrides, `store-retention-policy':
%% a list of `#{ <<"process">> => PID, <<"keep-seconds">> => N }'. It is read
%% from the node's options only, never from a process's own tags, so whoever
%% spawns a process cannot make the node keep its history. The PID is a
%% value, not a key, because loading a JSON config lowercases a map's keys:
%% a PID key would never match its process again. Anything else (a map, an
%% entry with no valid PID or keep-seconds, a PID named twice) is an error,
%% and a run with an invalid policy deletes nothing: an override that cannot
%% be read must not quietly become the node's default.
policy(Opts) ->
    case hb_opts:get(<<"store-retention-policy">>, [], Opts) of
        Empty when Empty =:= []; Empty =:= #{} -> {ok, #{}};
        List when is_list(List) ->
            lists:foldl(
                fun(_, {error, _} = Err) -> Err;
                   (Entry, {ok, Acc}) ->
                        case policy_entry(Entry) of
                            {ok, P, _} when is_map_key(P, Acc) -> {error, {duplicate_process, P}};
                            {ok, P, N} -> {ok, Acc#{ P => N }};
                            error -> {error, {invalid_entry, Entry}}
                        end
                end,
                {ok, #{}},
                List);
        Other -> {error, {not_a_list, Other}}
    end.

policy_entry(#{ <<"process">> := P, <<"keep-seconds">> := N }) when ?IS_ID(P) ->
    try hb_util:int(N) of
        Secs when is_integer(Secs), Secs >= 0 -> {ok, hb_util:human_id(P), Secs};
        _ -> error
    catch _:_ -> error
    end;
policy_entry(_) -> error.

keep_seconds(P, #{ policy := Policy, keep_seconds := Default }) ->
    maps:get(P, Policy, Default).

%% @doc The keys of one process to delete, in the order to delete them. With
%% a `Keep' and `Recent' count it plans as a node with no age floor
%% (`store-retention-keep-seconds' 0) does; with a node options map, as
%% `retain/1' does (and fails on an invalid `store-retention-policy').
plan(Store, P, Opts) when is_map(Opts) ->
    {Keys, _, _} = walk(Store, P, config(Opts), fun(Ks, Acc) -> {cont, [Ks | Acc]} end, []),
    lists:append(lists:reverse(Keys));
plan(Store, P, Keep) -> plan(Store, P, Keep, ?RECENT).
plan(Store, P, Keep, Recent) ->
    plan(Store, P, #{ <<"store-retention-checkpoints">> => Keep,
                      <<"store-retention-recent-slots">> => Recent,
                      <<"store-retention-keep-seconds">> => 0 }).

%% Hand the keys of one process to `Fun(Keys, Acc)' in the order to delete
%% them, a page at a time, until it answers `{halt, Acc}'. First the `o' and
%% `q' rows of delivered slots (few: the deliverer deletes them itself), then
%% every row below the process's floor, top-down, a window of slots at a
%% time. The rows at and above the floor are never listed, so a run's cost
%% follows the rows it deletes, not the rows it keeps. The walk stops at
%% the floor of the process's last finished walk (`swept' in `Cfg'): below
%% it only snapshots are left. Returns the last `Acc', the number of
%% snapshot markers whose time could not be read, and `{ok, Floor}' when the
%% walk finished (`halted' when `Fun' stopped it).
walk(Store, P, Cfg0, Fun, Acc0) ->
    Cfg = Cfg0#{ keep_seconds := keep_seconds(P, Cfg0) },
    Snaps = lists:reverse(slots(Store, P, <<"s">>)),
    {Floor, Unreadable} = floor(Store, P, Snaps, Cfg),
    Rule =
        keep_snapshots(
            case list_page(Store, prefix(P, <<"d">>), none, asc, 1) of
                [] -> full_rule(Store, P, Snaps, Floor, Cfg);
                _ -> delta_rule(Store, P, Floor)
            end,
            Snaps,
            maps:get(P, maps:get(swept, Cfg, #{}), 0)),
    Newest = maps:from_list(
        [ {K, case rows_desc(Store, P, K, 1) of [N] -> N; [] -> -1 end}
        || K <- [<<"d">>, <<"f">>, <<"o">>] ]),
    case Fun(delivered(Store, P, Rule, Newest), Acc0) of
        {halt, Acc} -> {Acc, Unreadable, halted};
        {cont, Acc} ->
            case zone(Store, P, Rule, Newest, maps:get(start, Rule), Fun, Acc) of
                {done, Acc1} -> {Acc1, Unreadable, {ok, maps:get(start, Rule)}};
                {halted, Acc1} -> {Acc1, Unreadable, halted}
            end
    end.

%% Every snapshot checkpoint stays, at any age: its marker and its state.
%% Below `Bottom' (the floor of the last finished walk) nothing else is
%% left, so the walk ends there.
keep_snapshots(Rule = #{ keep := Keep }, Snaps, Bottom) ->
    Snap = maps:from_list([ {S, true} || S <- Snaps ]),
    Rule#{
        bottom => Bottom,
        keep =>
            fun(K, S) when K == <<"s">>; K == <<"f">> -> is_map_key(S, Snap) orelse Keep(K, S);
               (K, S) -> Keep(K, S)
            end
    }.

%% The floor: the newest snapshot checkpoint whose assignment is at least
%% `keep-seconds' old, and never above the `Keep'-th newest one, which stays
%% the minimum kept. With `keep-seconds' 0 it is the `Keep'-th newest. A
%% process with no more than `Keep' checkpoints, or none old enough, has no
%% floor. Slot times only grow, so the search is binary over the candidates;
%% a marker whose time cannot be read counts as young, so the floor is never
%% above it (fail closed: unreadable means kept).
floor(_Store, _P, Snaps, #{ keep := Keep }) when length(Snaps) =< Keep -> {none, 0};
floor(_Store, _P, Snaps, #{ keep := Keep, keep_seconds := 0 }) ->
    {{ok, lists:nth(Keep, Snaps)}, 0};
floor(Store, P, Snaps, #{ keep := Keep, keep_seconds := Secs, now := Now, opts := Opts }) ->
    Candidates = list_to_tuple(lists:reverse(lists:nthtail(Keep - 1, Snaps))),
    Old =
        fun(S) ->
            case snapshot_time(Store, P, S, Opts) of
                {ok, Ms} -> Ms =< Now - Secs * 1000;
                error -> unknown
            end
        end,
    search(Candidates, 1, tuple_size(Candidates), Old, none, 0).

search(_T, Lo, Hi, _Old, Best, Failed) when Lo > Hi -> {Best, Failed};
search(T, Lo, Hi, Old, Best, Failed) ->
    Mid = (Lo + Hi) div 2,
    S = element(Mid, T),
    case Old(S) of
        true -> search(T, Mid + 1, Hi, Old, {ok, S}, Failed);
        false -> search(T, Lo, Mid - 1, Old, Best, Failed);
        unknown -> search(T, Lo, Mid - 1, Old, Best, Failed + 1)
    end.

%% The time of a snapshot checkpoint: from its marker, or, for a marker
%% written before markers carried one (empty), its assignment's.
snapshot_time(Store, P, S, Opts) ->
    case read(Store, key(P, <<"s">>, S)) of
        {ok, Marker} ->
            case marker_time(Marker) of
                legacy -> assignment_time(P, S, Opts);
                Time -> Time
            end;
        _ -> error
    end.

%% @doc The assignment timestamp (ms) an `s' marker carries, `error' for one
%% that is not a plausible time (`to_ms/1'), or `legacy' for a marker that
%% carries none: an empty marker, written before markers had a value. A marker is a `pstate' blob (`lib_process_cache:blob_encode/1':
%% `{1, Map}' in the external term format).
marker_time(Marker) ->
    try binary_to_term(Marker, [safe]) of
        {1, #{ <<"timestamp">> := T }} when is_integer(T) -> to_ms(T);
        _ -> legacy
    catch _:_ -> legacy
    end.

%% @doc The timestamp (ms) of the assignment of slot `S' of `P', as the
%% scheduler cache keeps it here (`dev_scheduler_cache'), or `error'.
assignment_time(P, S, Opts) ->
    Store = hb_opts:get(scheduler_store, hb_opts:get(store, no_viable_store, Opts), Opts),
    Path = <<"~scheduler@1.0/assignments/", P/binary, "/", (integer_to_binary(S))/binary>>,
    try hb_cache:read(Path, Opts#{ <<"store">> => Store }) of
        {ok, Assignment} -> assignment_ms(Assignment, Opts);
        _ -> error
    catch _:_ -> error
    end.

%% @doc An assignment's timestamp in milliseconds, or `error'. The
%% `arweave-scheduler@1.0' stamps an assignment with its Arweave block's
%% timestamp, in seconds; `scheduler@1.0' with the time in milliseconds.
%% Both carry a `block-height', so the unit is told by size alone
%% (`to_ms/1'). `dev_process' writes this into a checkpoint's `s' marker.
assignment_ms(Assignment, Opts) ->
    case hb_util:safe_int(hb_maps:get(<<"timestamp">>, Assignment, undefined, Opts)) of
        {ok, T} -> to_ms(T);
        _ -> error
    end.

%% A timestamp in milliseconds, read as seconds when it is below
%% `?SECONDS_BELOW'. One before `?EARLIEST_MS' cannot be a real assignment's
%% time, so it is `error': a time in a wrong unit is kept, never read as old.
to_ms(T) when is_integer(T), T >= 0, T < ?SECONDS_BELOW -> check_ms(T * 1000);
to_ms(T) when is_integer(T) -> check_ms(T);
to_ms(_) -> error.

check_ms(Ms) when Ms >= ?EARLIEST_MS -> {ok, Ms};
check_ms(_) -> error.

%% A delta process keeps every slot from its floor up, so every kept delta
%% still reaches a full state. A floor whose checkpoint has no state row
%% leaves the process alone.
delta_rule(_Store, _P, none) -> keep_all();
delta_rule(Store, P, {ok, Floor}) ->
    case read(Store, key(P, <<"f">>, Floor)) of
        {ok, _} -> #{ start => Floor, keep => fun(_Kind, S) -> S >= Floor end };
        _ ->
            ?event(warning, {store_retention_skipped, {process, P},
                             {floor_without_state, Floor}}),
            keep_all()
    end.

%% A full-state process: every row stands alone. Keep its newest `Keep'
%% snapshot checkpoints, its newest `Recent' states and, with an age floor,
%% every slot from the floor up; with `keep-seconds' set and no floor, all
%% of it.
full_rule(Store, P, Snaps, Floor, #{ keep := Keep, recent := Recent, keep_seconds := Secs }) ->
    KeepS = lists:sublist(Snaps, Keep),
    RecentF = rows_desc(Store, P, <<"f">>, Recent),
    KeepF = KeepS ++ RecentF,
    % Every slot at or above `Top' is kept for its checkpoints and recency.
    STop = case length(Snaps) > Keep of true -> lists:nth(Keep + 1, Snaps) + 1; false -> 0 end,
    FTop = case length(RecentF) < Recent of true -> 0; false -> lists:last(RecentF) end,
    AgeFloor =
        case {Secs, Floor} of
            {0, _} -> infinity;
            {_, {ok, F}} -> F;
            {_, none} -> 0
        end,
    #{
        start => min(AgeFloor, max(STop, FTop)),
        keep =>
            fun(<<"s">>, S) -> S >= AgeFloor orelse lists:member(S, KeepS);
               (_, S) -> S >= AgeFloor orelse lists:member(S, KeepF)
            end
    }.

keep_all() -> #{ start => 0, keep => fun(_, _) -> true end }.

%% The `o' and `q' rows of delivered slots (with an `x') at or above the
%% rule's start or below the last finished walk's floor (an outbox delivered
%% late), each with its chunks, and the chunks of `o' rows that are gone
%% there; `zone/7' takes the ones between. Few: the deliverer deletes them
%% itself, so these are leftovers of a crash. An undelivered outbox is never
%% planned.
delivered(Store, P, #{ start := Start, bottom := Bottom }, Newest) ->
    Outside = fun(S) -> S >= Start orelse S < Bottom end,
    {ORows, OChunks} = split_names(names(Store, P, <<"o">>)),
    {QRows, QChunks} = split_names(names(Store, P, <<"q">>)),
    Delivered = fun(S) -> Outside(S) andalso exists(Store, key(P, <<"x">>, S)) end,
    Rows =
        lists:sort(fun(A, B) -> A >= B end,
            [ {S, <<"o">>} || S <- ORows, Delivered(S) ] ++
            [ {S, <<"q">>} || S <- QRows, Delivered(S) ]),
    Chunks = #{ <<"o">> => OChunks, <<"q">> => QChunks },
    lists:flatmap(
        fun({S, Kind}) ->
            [key(P, Kind, S) |
                [ chunk_key(P, Kind, S, N) || {CS, N} <- maps:get(Kind, Chunks), CS == S ]]
        end,
        Rows) ++
    [ chunk_key(P, <<"o">>, S, N) || {S, N} <- OChunks, Outside(S),
      S < maps:get(<<"o">>, Newest), not lists:member(S, ORows) ].

%% Every row below the rule's start, top-down, a window of slots at a time:
%% the highest slot first, and within a slot the `s' marker before the
%% state, the state before its outbox and the outbox before its receipts. So
%% whatever a run (or a `kill -9' in the middle of one) leaves behind is
%% still a readable chain. Each window is found by asking every kind for its
%% highest slot below the last one, so the walk stops as soon as nothing is
%% left below.
zone(Store, P, Rule = #{ bottom := Bottom }, Newest, Hi, Fun, Acc) ->
    Tops = [ T || K <- ?ZONE_KINDS, T <- [top_below(Store, P, K, Hi)], T =/= none, T >= Bottom ],
    case Tops of
        [] -> {done, Acc};
        _ ->
            Top = lists:max(Tops),
            Lo = max(Bottom, Top - ?WINDOW + 1),
            case Fun(window(Store, P, Rule, Newest, Lo, Top), Acc) of
                {halt, Acc1} -> {halted, Acc1};
                {cont, Acc1} -> zone(Store, P, Rule, Newest, Lo, Fun, Acc1)
            end
    end.

%% The keys to delete of the slots `Lo'..`Top', in order. A row's chunks
%% go right after it, so a reader finds a whole row or none. Chunks of `d',
%% `f' and `o' whose row is gone (a run stopped between the two) go too,
%% below the newest row of their kind: a row being written has its chunks
%% written first. `q' and `x' rows are written by the deliverer in no slot
%% order, so their chunks only ever go with their row.
window(Store, P, #{ keep := Keep }, Newest, Lo, Top) ->
    Names = [ {K, Parsed} || K <- ?ZONE_KINDS, Name <- range(Store, P, K, Lo, Top),
                             Parsed <- [parse(Name)], Parsed =/= error ],
    Rows = [ {S, K} || {K, {S, row}} <- Names ],
    IsRow = maps:from_list([ {{K, S}, true} || {S, K} <- Rows ]),
    Chunks =
        lists:foldl(
            fun({K, {S, {chunk, N}}}, M) -> maps:update_with({K, S}, fun(L) -> [N | L] end, [N], M);
               (_, M) -> M
            end,
            #{},
            Names),
    ChunkKeys = fun(K, S) -> lists:sort([ chunk_key(P, K, S, N) || N <- maps:get({K, S}, Chunks, []) ]) end,
    Doomed = [ {S, rank(K), K} || {S, K} <- Rows, doomed(K, S, Keep, IsRow) ],
    Ordered = lists:sort(fun({A, KA, _}, {B, KB, _}) -> {A, -KA} >= {B, -KB} end, Doomed),
    lists:flatmap(fun({S, _, K}) -> [key(P, K, S) | ChunkKeys(K, S)] end, Ordered) ++
    lists:sort([ Key || {K, S} <- maps:keys(Chunks),
                        lists:member(K, [<<"d">>, <<"f">>, <<"o">>]),
                        S < maps:get(K, Newest), not is_map_key({K, S}, IsRow),
                        Key <- ChunkKeys(K, S) ]).

%% `o' and `q' go whenever the slot's `x' exists, and never without it; `x'
%% goes once the slot's state does.
doomed(K, S, _Keep, IsRow) when K == <<"o">>; K == <<"q">> -> is_map_key({<<"x">>, S}, IsRow);
doomed(<<"x">>, S, Keep, _IsRow) -> not Keep(<<"f">>, S);
doomed(K, S, Keep, _IsRow) -> not Keep(K, S).

rank(<<"s">>) -> 0;
rank(<<"d">>) -> 1;
rank(<<"f">>) -> 2;
rank(<<"o">>) -> 3;
rank(<<"q">>) -> 3;
rank(<<"x">>) -> 4.

%% The highest slot of a kind below `Hi', or `none'.
top_below(_Store, _P, _Kind, Hi) when Hi =< 0 -> none;
top_below(Store, P, Kind, Hi) ->
    case [ S || Name <- list_page(Store, prefix(P, Kind), pad(Hi), desc, 2),
                {S, _} <- [parse(Name)], S < Hi ] of
        [S | _] -> S;
        [] -> none
    end.

%% The names (rows and chunks) of a kind in the slots `Lo'..`Top', listed
%% downwards a page at a time from just above `Top'.
range(Store, P, Kind, Lo, Top) ->
    range(Store, P, Kind, Lo, Top, pad(Top + 1), []).
range(Store, P, Kind, Lo, Top, From, Acc) ->
    Page = list_page(Store, prefix(P, Kind), From, desc, ?PAGE),
    In = [ N || N <- Page, N =/= From, {S, _} <- [parse(N)], S >= Lo, S =< Top ],
    Last = case Page of [] -> From; _ -> lists:last(Page) end,
    Below = case parse(Last) of {S, _} -> S < Lo; error -> true end,
    case length(Page) < ?PAGE orelse Below of
        true -> lists:append(lists:reverse([In | Acc]));
        false -> range(Store, P, Kind, Lo, Top, Last, [In | Acc])
    end.

%% The newest `N' row slots of a kind, descending, skipping chunk names.
rows_desc(Store, P, Kind, N) -> rows_desc(Store, P, Kind, none, N, []).
rows_desc(Store, P, Kind, From, N, Acc) ->
    Page = list_page(Store, prefix(P, Kind), From, desc, ?ROWS_PAGE),
    Rows = [ S || Name <- Page, Name =/= From, {S, row} <- [parse(Name)] ],
    Acc1 = Acc ++ lists:sublist(Rows, N - length(Acc)),
    case length(Acc1) >= N orelse length(Page) < ?ROWS_PAGE of
        true -> Acc1;
        false -> rows_desc(Store, P, Kind, lists:last(Page), N, Acc1)
    end.

%% A child name of a kind: `{Slot, row}' or `{Slot, {chunk, N}}'.
parse(Name) ->
    case binary:split(Name, <<".">>) of
        [S] ->
            case catch binary_to_integer(S) of
                I when is_integer(I) -> {I, row};
                _ -> error
            end;
        [S, N] ->
            case {catch binary_to_integer(S), catch binary_to_integer(N)} of
                {I, J} when is_integer(I), is_integer(J) -> {I, {chunk, N}};
                _ -> error
            end
    end.

%% Every name of a kind, as row slots and `{Slot, N}' chunks.
split_names(Names) ->
    Parsed = [ P || N <- Names, P <- [parse(N)], P =/= error ],
    {lists:sort([ S || {S, row} <- Parsed ]), [ {S, N} || {S, {chunk, N}} <- Parsed ]}.

%% @doc Per process, the number of `o' rows with no `x' marker: outboxes no
%% push has delivered yet. Only processes that have any are listed.
undelivered(Store, Procs) ->
    maps:from_list(
        [ {P, N}
        || P <- Procs,
           N <- [length([ S || S <- slots(Store, P, <<"o">>),
                               not exists(Store, key(P, <<"x">>, S)) ])],
           N > 0 ]
    ).

chunk_key(P, Kind, Slot, N) -> <<(key(P, Kind, Slot))/binary, ".", N/binary>>.

%% Delete in batches, top-down, throttled to `Pct' percent of one scheduler.
delete(_DB, [], _Batch, _Pct) -> {0, 0};
delete(DB, Keys, Batch, Pct) ->
    {Now, Rest} = split(Batch, Keys),
    % Only rows of the process-cache namespace, ever.
    true = lists:all(fun is_retention_key/1, Now),
    T0 = erlang:monotonic_time(microsecond),
    N = case elmdb:delete_batch(DB, Now) of
            {ok, Count} -> Count;
            Error ->
                % elmdb fails the store like a failed flush: it refuses
                % writes until the node restarts.
                ?event(error,
                    {store_retention_delete_failed,
                        {main_store_refuses_writes_until_restart, true},
                        {error, Error}}),
                persistent_term:put(?STATUS,
                    (persistent_term:get(?STATUS, #{}))#{ store_failed => Error }),
                erlang:error({store_retention_delete_failed, Error})
        end,
    Work = (erlang:monotonic_time(microsecond) - T0) div 1000,
    Sleep = Work * (100 - Pct) div Pct,
    timer:sleep(Sleep),
    {N2, S2} = delete(DB, Rest, Batch, Pct),
    {N + N2, Sleep + S2}.

split(N, L) when length(L) =< N -> {L, []};
split(N, L) -> lists:split(N, L).

is_retention_key(<<"pstate/", Rest/binary>>) ->
    case binary:split(Rest, <<"/">>, [global]) of
        [P, Kind, SlotPart] ->
            ?IS_ID(P) andalso lists:member(Kind, [<<"s">>, <<"d">>, <<"f">>, <<"o">>, <<"q">>, <<"x">>])
                andalso is_slot_part(SlotPart);
        _ -> false
    end;
is_retention_key(_) -> false.

%% Before a process first loses a state row (`s', `d' or `f'), mark it
%% pruned: its kept snapshots start at slot 0, so its lowest stored slot no
%% longer says whether it was, and a pruned process must never be replayed
%% from its definition (`dev_process'). The marker is a buffered write, and
%% `elmdb:delete_batch/2' commits buffered writes in the same transaction
%% before it deletes: the marker is durable no later than the first deletion.
mark_pruned(Store, P, Keys, Opts) ->
    State = fun(Key) ->
                case binary:split(Key, <<"/">>, [global]) of
                    [_, _, Kind, _] -> lists:member(Kind, [<<"s">>, <<"d">>, <<"f">>]);
                    _ -> false
                end
            end,
    case lists:any(State, Keys) andalso not exists(Store, pruned_key(P)) of
        true -> hb_store:write([Store], #{ pruned_key(P) => <<"pruned">> }, Opts);
        false -> ok
    end.

%% @doc The marker of a process that has lost state rows to retention.
pruned_key(P) -> prefix(P, <<"p">>).

%% A slot (12 digits), or a chunk of one (`<12 digits>.<n>').
is_slot_part(Part) ->
    {Slot, Chunk} =
        case binary:split(Part, <<".">>) of
            [S] -> {S, <<"1">>};
            [S, C] -> {S, C}
        end,
    byte_size(Slot) == 12 andalso is_integer(catch binary_to_integer(Slot))
        andalso is_integer(catch binary_to_integer(Chunk)).

key(P, Kind, Slot) -> <<(prefix(P, Kind))/binary, "/", (pad(Slot))/binary>>.

pad(Slot) -> iolist_to_binary(io_lib:format("~12..0B", [Slot])).

prefix(P, Kind) -> <<?NS/binary, "/", P/binary, "/", Kind/binary>>.

%% Every row slot of a kind, ascending. Only for the kinds a process holds
%% few of: `s' markers and undelivered `o' rows.
slots(Store, P, Kind) ->
    lists:sort([ binary_to_integer(N) || N <- names(Store, P, Kind),
                 is_integer(catch binary_to_integer(N)) ]).

names(Store, P, Kind) -> list(Store, prefix(P, Kind)).

list(Store, Path) ->
    case hb_store:list([Store], Path, #{}) of
        {ok, L} -> L;
        _ -> []
    end.

%% One page of a listing: at most `Limit' children of `Path', from the child
%% `From' (inclusive) or the first one, in `Direction'.
list_page(Store, Path, From, Direction, Limit) ->
    Req = #{ <<"list">> => Path, <<"limit">> => Limit, <<"direction">> => Direction },
    case hb_store:list([Store], case From of none -> Req; _ -> Req#{ <<"from">> => From } end, #{}) of
        {ok, L} -> L;
        _ -> []
    end.

read(Store, Key) ->
    hb_store:read([Store], #{ <<"read">> => Key, <<"exact">> => true }, #{}).

exists(Store, Key) ->
    case read(Store, Key) of
        {ok, _} -> true;
        _ -> false
    end.

%% @doc The store retention acts on: the first writable `hb_store_lmdb' of
%% the node's store list, never an irreplaceable store.
store(Opts) ->
    Stores =
        hb_store:normalize_stores(
            case hb_opts:get(<<"store">>, [], Opts) of
                L when is_list(L) -> L;
                M when is_map(M) -> [M]
            end),
    Irr = case hb_store_irreplaceable:store(Opts) of undefined -> []; I -> I end,
    IrrNames = lists:flatmap(
        fun(S) -> [maps:get(<<"name">>, S, undefined),
                   maps:get(<<"name">>, maps:get(<<"inner">>, S, #{}), undefined)] end, Irr),
    case [ S || S = #{ <<"store-module">> := hb_store_lmdb } <- Stores,
                not is_map_key(<<"access">>, S),
                maps:get(<<"read-only">>, S, false) =/= true,
                not lists:member(maps:get(<<"name">>, S, undefined), IrrNames) ] of
        [Store | _] -> Store;
        [] -> erlang:error(store_retention_needs_a_writable_lmdb_store)
    end.

delete_supported() ->
    _ = code:ensure_loaded(elmdb),
    erlang:function_exported(elmdb, delete_batch, 2).

int(Key, Default, Opts) -> hb_util:int(hb_opts:get(Key, Default, Opts)).

%%% Tests

delete_primitive_present_test() ->
    ?assert(delete_supported()).

%% @doc Only `pstate/' rows below the newest two snapshot checkpoints go;
%% the floor checkpoint, everything above it, every other namespace and the
%% irreplaceable store stay.
retain_deletes_only_below_floor_test() ->
    Main = hb_test_utils:test_store(hb_store_lmdb, <<"ret-main">>),
    Irr = hb_test_utils:test_store(hb_store_lmdb, <<"ret-irr">>),
    ok = hb_store:start([Main, Irr], #{}, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Q = hb_util:human_id(crypto:strong_rand_bytes(32)),
    % P: checkpoints with snapshots at 0, 10, 20, 30; deltas between.
    Rows =
        maps:from_list(
            [ {key(P, <<"f">>, S), <<"full">>} || S <- [0, 10, 20, 30] ] ++
            [ {key(P, <<"s">>, S), <<>>} || S <- [0, 10, 20, 30] ] ++
            [ {key(P, <<"d">>, S), <<"delta">>} || S <- lists:seq(1, 35), S rem 10 =/= 0 ] ++
            % Q has only two snapshot checkpoints: kept whole.
            [ {key(Q, <<"f">>, S), <<"full">>} || S <- [0, 5] ] ++
            [ {key(Q, <<"s">>, S), <<>>} || S <- [0, 5] ] ++
            [ {key(Q, <<"d">>, S), <<"delta">>} || S <- [1, 2, 3] ]),
    Other = #{
        <<"~scheduler@1.0/assignments/", P/binary, "/0">> => <<"ao:x">>,
        P => <<"definition">>,
        <<"computed/", P/binary, "/slot/0">> => <<"ao:y">>
    },
    ok = hb_store:write([Main], maps:merge(Rows, Other), #{}),
    ok = hb_store:write([Irr], Rows, #{}),
    ok = hb_store:flush([Main, Irr], #{}),
    Opts = #{ <<"store">> => [Main], <<"irreplaceable-store">> => Irr,
              <<"store-retention-batch-keys">> => 3,
              <<"store-retention-keep-seconds">> => 0 },
    #{ deleted := Deleted } = retain(Opts),
    Gone = [ K || K <- maps:keys(Rows), hb_store:read([Main], K, #{}) =/= {ok, maps:get(K, Rows)} ],
    % The snapshot checkpoints below the floor (0, 10) stay.
    Expected = lists:sort([ key(P, <<"d">>, S) || S <- lists:seq(1, 19), S rem 10 =/= 0 ]),
    ?assertEqual(Expected, lists:sort(Gone)),
    ?assertEqual(length(Expected), Deleted),
    [ ?assertEqual({ok, V}, hb_store:read([Main], K, #{})) || {K, V} <- maps:to_list(Other) ],
    [ ?assertEqual({ok, V}, hb_store:read([Irr], K, #{})) || {K, V} <- maps:to_list(Rows) ],
    % P is marked pruned, before its first deletion; Q, which lost nothing,
    % is not.
    ?assertMatch({ok, _}, hb_store:read([Main], pruned_key(P), #{})),
    ?assertMatch({error, _}, hb_store:read([Main], pruned_key(Q), #{})),
    % A second run finds nothing more.
    ?assertMatch(#{ deleted := 0 }, retain(Opts)).

%% @doc Deletion runs top-down: the highest doomed slot first. The snapshot
%% checkpoints below the floor stay.
plan_order_test() ->
    Main = hb_test_utils:test_store(hb_store_lmdb, <<"ret-order">>),
    ok = hb_store:start([Main], #{}, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok = hb_store:write([Main], maps:from_list(
        [ {key(P, K, S), <<"x">>} || K <- [<<"f">>, <<"s">>], S <- [0, 4, 8] ] ++
        [ {key(P, <<"d">>, S), <<"x">>} || S <- [1, 2, 3, 5] ]), #{}),
    ?assertEqual(
        [ key(P, <<"d">>, 5), key(P, <<"d">>, 3), key(P, <<"d">>, 2), key(P, <<"d">>, 1) ],
        plan(Main, P, 1)).

never_a_foreign_key_test() ->
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assert(is_retention_key(key(P, <<"d">>, 5))),
    ?assertNot(is_retention_key(<<"~scheduler@1.0/assignments/", P/binary, "/5">>)),
    ?assertNot(is_retention_key(<<"pstate/", P/binary, "/z/000000000005">>)),
    ?assertNot(is_retention_key(P)).

%% @doc A full-state process keeps every snapshot checkpoint and its newest
%% states; everything else goes, newest first.
plan_full_state_test() ->
    Main = hb_test_utils:test_store(hb_store_lmdb, <<"ret-full">>),
    ok = hb_store:start([Main], #{}, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok = hb_store:write([Main], maps:from_list(
        [ {key(P, <<"f">>, S), <<"x">>} || S <- lists:seq(0, 12) ] ++
        [ {key(P, <<"s">>, S), <<>>} || S <- [0, 5, 10] ]), #{}),
    ?assertEqual(
        [ key(P, <<"f">>, 9), key(P, <<"f">>, 8), key(P, <<"f">>, 7), key(P, <<"f">>, 6),
          key(P, <<"f">>, 4), key(P, <<"f">>, 3), key(P, <<"f">>, 2), key(P, <<"f">>, 1) ],
        plan(Main, P, 2, 3)),
    % Without any snapshot checkpoint, only the newest states stay.
    Q = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok = hb_store:write([Main], maps:from_list(
        [ {key(Q, <<"f">>, S), <<"x">>} || S <- lists:seq(0, 4) ]), #{}),
    ?assertEqual([ key(Q, <<"f">>, 1), key(Q, <<"f">>, 0) ], plan(Main, Q, 2, 3)).

%% @doc A chunked row's chunks go right after it; chunks left without their
%% row go too, unless they belong to the newest slot (a row being written).
plan_chunks_test() ->
    Main = hb_test_utils:test_store(hb_store_lmdb, <<"ret-chunks">>),
    ok = hb_store:start([Main], #{}, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    C = fun(S, N) -> <<(key(P, <<"f">>, S))/binary, ".", (integer_to_binary(N))/binary>> end,
    ok = hb_store:write([Main], maps:from_list(
        [ {key(P, <<"f">>, S), <<"pstate-chunks:2">>} || S <- [1, 2, 3] ] ++
        [ {C(S, N), <<"x">>} || S <- [0, 1, 2, 3, 4], N <- [1, 2] ]), #{}),
    ?assertEqual(
        [ key(P, <<"f">>, 2), C(2, 1), C(2, 2), key(P, <<"f">>, 1), C(1, 1), C(1, 2),
          C(0, 1), C(0, 2) ],
        plan(Main, P, 2, 1)),
    ?assert(lists:all(fun is_retention_key/1, plan(Main, P, 2, 1))),
    ?assertNot(is_retention_key(<<(key(P, <<"f">>, 1))/binary, ".x">>)).

%% @doc A slot's delivery rows: `o' (its outbox) and `q' (its signed
%% messages) are never deleted without the slot's `x' (its receipts) -- at any
%% slot, below the floor or not -- and are deleted whenever `x' exists. `x'
%% itself goes once the slot is below the floor. A run reports, per process,
%% the outboxes with no `x'.
outbox_rows_wait_for_delivery_test() ->
    Main = hb_test_utils:test_store(hb_store_lmdb, <<"ret-outbox">>),
    ok = hb_store:start([Main], #{}, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok = hb_store:write([Main], maps:from_list(
        [ {key(P, K, S), <<"x">>} || K <- [<<"f">>, <<"s">>], S <- [0, 10, 20] ] ++
        [ {key(P, <<"d">>, S), <<"x">>} || S <- lists:seq(1, 25), S rem 10 =/= 0 ] ++
        [ {key(P, <<"o">>, S), <<"x">>} || S <- [3, 5, 12, 22] ] ++
        [ {key(P, <<"q">>, S), <<"x">>} || S <- [3, 22] ] ++
        [ {key(P, <<"x">>, S), <<"r">>} || S <- [5, 7, 22] ]), #{}),
    Planned = plan(Main, P, 2),
    Has = fun(K, S) -> lists:member(key(P, K, S), Planned) end,
    % Delivered: `o' and `q' go, at any slot.
    ?assert(Has(<<"o">>, 5)),
    ?assert(Has(<<"o">>, 22)),
    ?assert(Has(<<"q">>, 22)),
    % `x' goes below the floor only.
    ?assert(Has(<<"x">>, 5)),
    ?assert(Has(<<"x">>, 7)),
    ?assertNot(Has(<<"x">>, 22)),
    % Undelivered: never, below the floor or not.
    ?assertNot(Has(<<"o">>, 3)),
    ?assertNot(Has(<<"q">>, 3)),
    ?assertNot(Has(<<"o">>, 12)),
    ?assert(lists:all(fun is_retention_key/1, Planned)),
    % Within a slot: the state, then the outbox, then its receipts.
    ?assertEqual(
        [key(P, <<"d">>, 5), key(P, <<"o">>, 5), key(P, <<"x">>, 5)],
        [ K || K <- Planned, lists:member(K,
            [key(P, <<"d">>, 5), key(P, <<"o">>, 5), key(P, <<"x">>, 5)]) ]),
    #{ undelivered_outboxes := Undelivered } =
        retain(#{ <<"store">> => [Main], <<"store-retention-keep-seconds">> => 0 }),
    ?assertEqual(#{ P => 2 }, maps:with([P], Undelivered)),
    ?assertEqual({ok, <<"x">>}, hb_store:read([Main], key(P, <<"o">>, 3), #{})),
    ?assertEqual({ok, <<"x">>}, hb_store:read([Main], key(P, <<"q">>, 3), #{})),
    ?assertEqual({ok, <<"x">>}, hb_store:read([Main], key(P, <<"o">>, 12), #{})),
    ?assertEqual({ok, <<"r">>}, hb_store:read([Main], key(P, <<"x">>, 22), #{})),
    ?assertNotEqual({ok, <<"x">>}, hb_store:read([Main], key(P, <<"o">>, 22), #{})),
    % Once delivered, the outbox goes at the next run.
    ok = hb_store:write([Main], #{ key(P, <<"x">>, 3) => <<"r">> }, #{}),
    retain(#{ <<"store">> => [Main], <<"store-retention-keep-seconds">> => 0 }),
    ?assertNotEqual({ok, <<"x">>}, hb_store:read([Main], key(P, <<"o">>, 3), #{})),
    ?assertNotEqual({ok, <<"x">>}, hb_store:read([Main], key(P, <<"q">>, 3), #{})),
    ?assertEqual({ok, <<"x">>}, hb_store:read([Main], key(P, <<"o">>, 12), #{})),
    % A full-state process: the same rule.
    Q = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok = hb_store:write([Main], maps:from_list(
        [ {key(Q, <<"f">>, S), <<"x">>} || S <- lists:seq(0, 6) ] ++
        [ {key(Q, <<"o">>, S), <<"x">>} || S <- [1, 2, 6] ] ++
        [ {key(Q, <<"x">>, S), <<"r">>} || S <- [1, 6] ]), #{}),
    QPlanned = plan(Main, Q, 2, 2),
    ?assert(lists:member(key(Q, <<"o">>, 1), QPlanned)),
    ?assert(lists:member(key(Q, <<"o">>, 6), QPlanned)),
    ?assert(lists:member(key(Q, <<"x">>, 1), QPlanned)),
    ?assertNot(lists:member(key(Q, <<"x">>, 6), QPlanned)),
    ?assertNot(lists:member(key(Q, <<"o">>, 2), QPlanned)).

%%% The age floor.

-define(HOUR, 3600).

%% A delta process: a checkpoint with a VM snapshot every 10 slots, its
%% marker dated `Ages' seconds ago (oldest first; `legacy' for an empty
%% marker), and deltas between, up to 5 slots above the newest. Returns the
%% head.
age_layout(Store, P, Ages) ->
    Now = erlang:system_time(millisecond),
    Points = lists:zip(lists:seq(0, 10 * (length(Ages) - 1), 10), Ages),
    Head = 10 * (length(Ages) - 1) + 5,
    ok = hb_store:write([Store], maps:from_list(
        [ {key(P, <<"f">>, S), <<"full">>} || {S, _} <- Points ] ++
        [ {key(P, <<"s">>, S), marker_at(Now, A)} || {S, A} <- Points ] ++
        [ {key(P, <<"d">>, S), <<"delta">>} || S <- lists:seq(1, Head), S rem 10 =/= 0 ]),
        #{}),
    Head.

marker_at(_Now, legacy) -> <<>>;
marker_at(Now, {seconds, Age}) -> blob(#{ <<"timestamp">> => Now div 1000 - Age });
marker_at(Now, Age) -> blob(#{ <<"timestamp">> => Now - Age * 1000 }).

%% A `pstate' blob, as `lib_process_cache' writes one.
blob(Term) -> term_to_binary({1, Term}, [{compressed, 1}]).

new_pid() -> hb_util:human_id(crypto:strong_rand_bytes(32)).

%% The floor a process was left with: the checkpoint below its lowest delta,
%% after checking that every snapshot checkpoint (marker and state) is still
%% there, at any age, and that it is one unbroken run from the floor to the
%% head -- every delta above the floor kept, none below it.
floor_of(Store, P, Head) ->
    Points = lists:seq(0, Head - 5, 10),
    ?assertEqual(Points, slots(Store, P, <<"s">>)),
    ?assertEqual(Points, slots(Store, P, <<"f">>)),
    [Lowest | _] = slots(Store, P, <<"d">>),
    Floor = Lowest - 1,
    ?assertEqual([ S || S <- lists:seq(Floor + 1, Head), S rem 10 =/= 0 ], slots(Store, P, <<"d">>)),
    Floor.

%% Run retention over a fresh process with markers aged `Ages', and return
%% its floor.
floor_after(Ages, Opts) ->
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"ret-age">>),
    ok = hb_store:start([Store], #{}, #{}),
    P = new_pid(),
    Head = age_layout(Store, P, Ages),
    retain(Opts#{ <<"store">> => [Store] }),
    floor_of(Store, P, Head).

%% @doc The floor is the newest checkpoint at least `keep-seconds' old: every
%% slot younger than that stays. The default is three days.
age_floor_test() ->
    Hours = [6, 5, 4, 3, 2, 1],
    Ages = [ H * ?HOUR || H <- Hours ],
    ?assertEqual(20, floor_after(Ages, #{ <<"store-retention-keep-seconds">> => 3 * ?HOUR + 1800 })),
    ?assertEqual(30, floor_after(Ages, #{ <<"store-retention-keep-seconds">> => 3 * ?HOUR })),
    % Nothing old enough: nothing goes.
    ?assertEqual(0, floor_after(Ages, #{ <<"store-retention-keep-seconds">> => 7 * ?HOUR })),
    Days = [ D * 24 * ?HOUR || D <- [5, 4, 3, 2, 1, 0] ],
    ?assertEqual(20, floor_after(Days, #{})),
    ?assertEqual(20, floor_after(Days ++ [0, 0], #{})).

%% @doc The newest `store-retention-checkpoints' checkpoints stay whatever
%% their age: the floor is never above the `Keep'-th newest.
checkpoints_stay_the_minimum_test() ->
    Ages = [ H * ?HOUR || H <- [6, 5, 4, 3, 2, 1] ],
    ?assertEqual(40, floor_after(Ages, #{ <<"store-retention-keep-seconds">> => 60 })),
    ?assertEqual(30, floor_after(Ages, #{ <<"store-retention-keep-seconds">> => 60,
                                          <<"store-retention-checkpoints">> => 3 })).

%% @doc `keep-seconds' 0 is the floor of the newest checkpoints alone, as
%% before there was an age floor: no marker is read, so neither an empty one
%% nor a young one keeps anything.
zero_keep_seconds_is_the_checkpoint_floor_test() ->
    Zero = #{ <<"store-retention-keep-seconds">> => 0 },
    ?assertEqual(40, floor_after([0, 0, 0, 0, 0, 0], Zero)),
    ?assertEqual(40, floor_after(lists:duplicate(6, legacy), Zero)),
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"ret-zero">>),
    ok = hb_store:start([Store], #{}, #{}),
    P = new_pid(),
    _ = age_layout(Store, P, lists:duplicate(6, legacy)),
    ?assertEqual(plan(Store, P, 2), plan(Store, P, Zero)),
    ?assertMatch(#{ unreadable_markers := 0 }, retain(Zero#{ <<"store">> => [Store] })).

%% @doc `store-retention-policy' sets `keep-seconds' per process, from the
%% node's options; every other process keeps the node's.
per_process_policy_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"ret-policy">>),
    ok = hb_store:start([Store], #{}, #{}),
    Ages = [ H * ?HOUR || H <- [6, 5, 4, 3, 2, 1] ],
    [P, Short, Long] = Procs = [ new_pid() || _ <- [1, 2, 3] ],
    Head = lists:last([ age_layout(Store, Q, Ages) || Q <- Procs ]),
    retain(#{
        <<"store">> => [Store],
        <<"store-retention-keep-seconds">> => 3 * ?HOUR + 1800,
        <<"store-retention-policy">> => [
            #{ <<"process">> => Short, <<"keep-seconds">> => 0 },
            #{ <<"process">> => Long, <<"keep-seconds">> => <<"86400">> }
        ]
    }),
    ?assertEqual(20, floor_of(Store, P, Head)),
    ?assertEqual(40, floor_of(Store, Short, Head)),
    ?assertEqual(0, floor_of(Store, Long, Head)).

%% @doc A policy in a node's JSON config file reaches its process: loading the
%% file lowercases a map's keys, so the PID is a value. A mixed-case PID
%% keeps its override, and the old map form (whose PID keys come back
%% lowercased) is refused: the run deletes nothing rather than give the
%% process the node's default.
policy_from_json_config_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"ret-policy-json">>),
    ok = hb_store:start([Store], #{}, #{}),
    Ages = [ H * ?HOUR || H <- [6, 5, 4, 3, 2, 1] ],
    Mixed = fun Mixed() ->
        Q = new_pid(),
        case string:lowercase(Q) =/= Q of true -> Q; false -> Mixed() end
    end,
    [Long, Other] = Procs = [Mixed(), new_pid()],
    Head = lists:last([ age_layout(Store, Q, Ages) || Q <- Procs ]),
    Dir = filename:join(["cache-TEST", "ret-policy-json"]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Load =
        fun(Name, Policy) ->
            Path = filename:join(Dir, Name),
            ok = file:write_file(Path, hb_json:encode(#{
                <<"store-retention-keep-seconds">> => 3 * ?HOUR + 1800,
                <<"store-retention-policy">> => Policy })),
            {ok, Conf} = hb_opts:load(Path),
            Conf
        end,
    AsMap = Load("map.json", #{ Long => #{ <<"keep-seconds">> => 86400 } }),
    ?assertMatch({error, _}, policy(AsMap)),
    ?assertMatch(#{ status := invalid_policy, deleted := 0 },
                 retain(AsMap#{ <<"store">> => [Store] })),
    ?assertEqual(0, floor_of(Store, Long, Head)),
    ?assertEqual(0, floor_of(Store, Other, Head)),
    AsList = Load("list.json", [ #{ <<"process">> => Long, <<"keep-seconds">> => 86400 } ]),
    ?assertEqual({ok, #{ Long => 86400 }}, policy(AsList)),
    retain(AsList#{ <<"store">> => [Store] }),
    ?assertEqual(0, floor_of(Store, Long, Head)),
    ?assertEqual(20, floor_of(Store, Other, Head)),
    % Entries that cannot be read are refused, not skipped.
    ?assertMatch({error, _}, policy(#{ <<"store-retention-policy">> =>
        [#{ <<"process">> => <<"not-an-id">>, <<"keep-seconds">> => 1 }] })),
    ?assertMatch({error, _}, policy(#{ <<"store-retention-policy">> =>
        [#{ <<"process">> => Long }] })),
    ?assertMatch({error, _}, policy(#{ <<"store-retention-policy">> =>
        [#{ <<"process">> => Long, <<"keep-seconds">> => 1 },
         #{ <<"process">> => Long, <<"keep-seconds">> => 2 }] })),
    ?assertEqual({ok, #{}}, policy(#{ <<"store-retention-policy">> => #{} })),
    ?assertEqual({ok, #{}}, policy(#{})).

%% @doc A checkpoint whose time cannot be read counts as young: the floor is
%% never above it, and a process none of whose markers can be dated keeps
%% every slot. Retention reports how many it could not read.
fail_closed_on_unreadable_time_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"ret-closed">>),
    ok = hb_store:start([Store], #{}, #{}),
    P = new_pid(),
    % Empty markers, and no assignment to read instead.
    Head = age_layout(Store, P, lists:duplicate(6, legacy)),
    #{ deleted := 0, unreadable_markers := Unreadable } =
        retain(#{ <<"store">> => [Store], <<"store-retention-keep-seconds">> => 60 }),
    ?assert(Unreadable > 0),
    ?assertEqual(0, floor_of(Store, P, Head)),
    % Slot 30's time is unknown; without it the floor would be 30.
    Ages = [ H * ?HOUR || H <- [6, 5, 4] ] ++ [legacy] ++ [ H * ?HOUR || H <- [2, 1] ],
    ?assertEqual(20, floor_after(Ages, #{ <<"store-retention-keep-seconds">> => 2 * ?HOUR + 1800 })).

%% @doc A marker written before markers carried a time is empty: its
%% checkpoint is dated by its assignment instead.
legacy_marker_reads_assignment_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"ret-legacy">>),
    ok = hb_store:start([Store], #{}, #{}),
    Opts = #{ <<"store">> => [Store] },
    P = new_pid(),
    % Empty markers below, dated ones above.
    Ages = [ H * ?HOUR || H <- [6, 5, 4, 3, 2, 1] ],
    Head = age_layout(Store, P, [legacy, legacy, legacy] ++ lists:nthtail(3, Ages)),
    Now = erlang:system_time(millisecond),
    % Where the scheduler cache keeps a slot's assignment.
    lists:foreach(
        fun({S, Age}) ->
            {ok, ID} = hb_cache:write(#{
                <<"type">> => <<"Assignment">>,
                <<"variant">> => <<"ao.N.1">>,
                <<"process">> => P,
                <<"slot">> => S,
                <<"timestamp">> => Now - Age * 1000
            }, Opts),
            ok = hb_cache:link(ID,
                <<"~scheduler@1.0/assignments/", P/binary, "/", (integer_to_binary(S))/binary>>,
                Opts)
        end,
        lists:zip([0, 10, 20], lists:sublist(Ages, 3))),
    retain(Opts#{ <<"store-retention-keep-seconds">> => 3 * ?HOUR + 1800 }),
    ?assertEqual(20, floor_of(Store, P, Head)).

%% @doc An Arweave-scheduled process's assignments carry their block's
%% timestamp, in seconds. It is read as seconds, in the marker and in a
%% legacy marker's assignment: a young slot read as milliseconds would look
%% decades old and be deleted. A timestamp before 2020 cannot be dated, so
%% its checkpoint is kept.
timestamp_in_seconds_test() ->
    Opts = #{},
    Arweave = #{ <<"timestamp">> => 1700000000, <<"block-height">> => 100 },
    ?assertEqual({ok, 1700000000000}, assignment_ms(Arweave, Opts)),
    ?assertEqual({ok, 1700000000123},
                 assignment_ms(#{ <<"timestamp">> => 1700000000123 }, Opts)),
    ?assertEqual({ok, 1700000000000},
                 assignment_ms(#{ <<"timestamp">> => <<"1700000000">> }, Opts)),
    ?assertEqual(error, assignment_ms(#{ <<"timestamp">> => 12345 }, Opts)),
    ?assertEqual(error, assignment_ms(#{ <<"timestamp">> => 1500000000000 }, Opts)),
    ?assertEqual(error, assignment_ms(#{}, Opts)),
    ?assertEqual({ok, 1700000000000},
                 marker_time(blob(#{ <<"timestamp">> => 1700000000 }))),
    ?assertEqual(error, marker_time(blob(#{ <<"timestamp">> => 0 }))),
    % Markers in seconds: the hours-old ones date as hours, not decades.
    Secs = [ {seconds, H * ?HOUR} || H <- [6, 5, 4, 3, 2, 1] ],
    ?assertEqual(0, floor_after(Secs, #{})),
    ?assertEqual(20, floor_after(Secs, #{ <<"store-retention-keep-seconds">> => 3 * ?HOUR + 1800 })),
    % Legacy markers dated by Arweave-shaped assignments, in seconds.
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"ret-arweave">>),
    ok = hb_store:start([Store], #{}, #{}),
    SOpts = #{ <<"store">> => [Store] },
    P = new_pid(),
    Head = age_layout(Store, P, lists:duplicate(6, legacy)),
    Now = erlang:system_time(second),
    lists:foreach(
        fun({S, Age}) ->
            {ok, ID} = hb_cache:write(#{
                <<"type">> => <<"Assignment">>,
                <<"variant">> => <<"ao.N.1">>,
                <<"process">> => P,
                <<"slot">> => S,
                <<"block-height">> => 100 + S,
                <<"timestamp">> => Now - Age
            }, SOpts),
            ok = hb_cache:link(ID,
                <<"~scheduler@1.0/assignments/", P/binary, "/", (integer_to_binary(S))/binary>>,
                SOpts)
        end,
        lists:zip(lists:seq(0, 50, 10), [ H * ?HOUR || H <- [6, 5, 4, 3, 2, 1] ])),
    ?assertMatch(#{ deleted := 0, unreadable_markers := 0 }, retain(SOpts)),
    ?assertEqual(0, floor_of(Store, P, Head)),
    retain(SOpts#{ <<"store-retention-keep-seconds">> => 3 * ?HOUR + 1800 }),
    ?assertEqual(20, floor_of(Store, P, Head)).

%% @doc Whatever prefix of a run's deletions lands (a `kill -9' at any
%% point), every remaining delta still chains down to a full state and every
%% `s' marker names a state that exists.
every_crash_prefix_leaves_a_chain_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"ret-prefix">>),
    ok = hb_store:start([Store], #{}, #{}),
    P = new_pid(),
    Head = age_layout(Store, P, [ H * ?HOUR || H <- [6, 5, 4, 3, 2, 1] ]),
    Rows = [ {K, S} || K <- [<<"s">>, <<"d">>, <<"f">>], S <- slots(Store, P, K) ],
    Keys = plan(Store, P, #{ <<"store-retention-keep-seconds">> => 3 * ?HOUR + 1800 }),
    ?assertEqual(length(Keys), length(lists:usort(Keys))),
    Doomed = [ {K, binary_to_integer(S)} || Key <- Keys,
               [_, _, K, S] <- [binary:split(Key, <<"/">>, [global])] ],
    Chains =
        fun(Left) ->
            Has = fun(K, S) -> lists:member({K, S}, Left) end,
            Reaches = fun Reaches(S) -> Has(<<"f">>, S) orelse (Has(<<"d">>, S) andalso Reaches(S - 1)) end,
            lists:all(fun({<<"d">>, S}) -> Reaches(S - 1);
                         ({<<"s">>, S}) -> Has(<<"f">>, S);
                         (_) -> true
                      end, Left)
        end,
    lists:foreach(
        fun(N) -> ?assert(Chains(Rows -- lists:sublist(Doomed, N))) end,
        lists:seq(0, length(Doomed))),
    % Only deltas go; the lowest one left is just above the floor (20).
    ?assertEqual([], [ R || R = {K, _} <- Doomed, K =/= <<"d">> ]),
    ?assertEqual(21, lists:min([ S || {<<"d">>, S} <- Rows -- Doomed ])),
    ?assert(Head > 20).

%% @doc A finished walk is remembered: the next one stops at its floor
%% instead of stepping down through the snapshots kept below it, and an
%% outbox delivered late below that floor is still cleaned up.
swept_floor_bounds_the_walk_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb, <<"ret-swept">>),
    ok = hb_store:start([Store], #{}, #{}),
    P = new_pid(),
    Head = age_layout(Store, P, lists:duplicate(6, 0)),
    ok = hb_store:write([Store], #{ key(P, <<"o">>, 3) => <<"x">> }, #{}),
    Opts = #{ <<"store">> => [Store], <<"store-retention-keep-seconds">> => 0 },
    #{ swept := Swept } = retain(Opts),
    ?assertEqual(#{ P => 40 }, Swept),
    ?assertEqual(40, floor_of(Store, P, Head)),
    % Below the swept floor the walk lists nothing, so a row planted there
    % (which no real slot below a floor ever gets) is left alone...
    ok = hb_store:write([Store], #{ key(P, <<"d">>, 25) => <<"delta">> }, #{}),
    ?assertMatch(#{ deleted := 0 }, retain(Opts, Swept)),
    ?assertEqual({ok, <<"delta">>}, hb_store:read([Store], key(P, <<"d">>, 25), #{})),
    % ...but a late delivery below it is not.
    ok = hb_store:write([Store], #{ key(P, <<"x">>, 3) => <<"r">> }, #{}),
    ?assertMatch(#{ deleted := 1 }, retain(Opts, Swept)),
    ?assertMatch({error, not_found}, hb_store:read([Store], key(P, <<"o">>, 3), #{})),
    % A server that remembers nothing walks the whole process again: the
    % planted delta goes, and so does the late receipt below the floor.
    ?assertMatch(#{ deleted := 2 }, retain(Opts)),
    ?assertEqual(40, floor_of(Store, P, Head)).


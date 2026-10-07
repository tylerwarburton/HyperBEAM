%%% @doc A store wrapper that continuously exports every write of its inner
%%% store to a (possibly slow, possibly remote) directory as an append-only,
%%% resumable log, with restore.
%%%
%%% Built for the essentials store (`hb_store_essentials'): the inner store is a
%%% LOCAL store that serves every read and takes every write synchronously, and
%%% the export is the off-box copy.
%%%
%%% <pre>
%%%   write/link/group --> inner store (local, synchronous)
%%%                    \-> cast {rec} --> writer --> local journal segments
%%%                                         |  (local disk only)
%%%                                         v  jobs, answered asynchronously
%%%                                   target worker --> Path/seg-N.log ...
%%%                                   (every target file operation)
%%% </pre>
%%%
%%% Three rules keep a slow, stalled or full target off the request path:
%%% <ol>
%%%   <li>A caller pays a counter update and a message send. Nothing a caller
%%%       does ever waits on the writer's start: the exporter is registered
%%%       before it initialises, and initialising touches only the local
%%%       journal. (The first version made the caller of the first write wait
%%%       up to 30 s, inside a `global:trans' every other writer queued
%%%       behind, while the writer listed and stat'ed every file on the
%%%       target -- ~350 ms each on the WireGuard NFS mount. That froze
%%%       POST /schedule for minutes after every restart on stage.)</li>
%%%   <li>The writer never touches the target. Every target operation --
%%%       listing, `df', writing, renaming, the manifest, pruning -- runs in
%%%       one long-lived target worker, one job at a time, so a stalled soft
%%%       mount (45 s per operation) holds at most one dirty I/O scheduler
%%%       thread, and the process-wide `target-ops' gate (default 2) bounds
%%%       how many any exporter or checkpoint archive may hold.</li>
%%%   <li>`sync/3' (called by the scheduler before it confirms a slot) waits
%%%       only for the writer to append the records already sent to it to the
%%%       LOCAL journal -- never for the target -- and gives up after 2 s.</li>
%%% </ol>
%%%
%%% Durability and gaps. Records sent to the writer after the local write are
%%% lost if the VM dies before they are appended; and past the backlog bound
%%% (`max-backlog-bytes', `max-pending') the writer stops journaling. Either
%%% way the log has a gap, which is closed WITHOUT a base image when the
%%% journal is intact: the writer keeps, per process, the highest assignment
%%% slot it has journaled (a watermark, persisted at every segment close and
%%% clean stop, and recovered from unshipped local segments), so on an unclean
%%% start (or when it resumes after dropping) it re-exports, from the local
%%% store, every assignment above the watermarks with its whole closure, plus
%%% the definitions of processes it has no watermark for and the small
%%% namespaces (`~location@1.0', `~bundler@1.0', `~meta@1.0', upload marks).
%%% A gap mark (`gap-N.mark') is shipped before any segment; the
%%% `{gap_filled, N}' record that follows the catch-up closes it, and
%%% `restore/3' refuses a gap that neither a fill nor a newer base covers. A
%%% full base image is written only on the first start of a non-empty store,
%%% or when asked (`resync/1', after a migration). A clean stop -- the node's
%%% application stop, which `docker stop' reaches through SIGTERM ->
%%% init:stop -- is recorded in the journal, and the next start needs neither.
%%%
%%% The target only ever sees whole files written once: `seg-N.log' (a few MB,
%%% gzip-compressed, `.tmp' + sync + rename), `base-N.log'/`.done' (frames of
%%% compressed raw rows), `gap-N.mark' and `manifest.log', an append-only list
%%% of every file shipped with its size and CRC (computed while writing, never
%%% by reading a file back), which `restore/3' checks. When a base completes,
%%% everything it supersedes is pruned: one complete chain remains.
%%%
%%% Export options (`essentials-export'):
%%% <ul>
%%%   <li>`path': the export directory (the remote mount). Required.</li>
%%%   <li>`journal': the local journal directory. Default: `<inner name>-journal'.</li>
%%%   <li>`segment-bytes' (8 MiB), `segment-ms' (60 s): roll a segment at this
%%%       size or age.</li>
%%%   <li>`compress' (true): gzip segments before shipping.</li>
%%%   <li>`max-backlog-bytes' (4 GiB), `max-pending' (100000): bounds past
%%%       which the writer stops journaling (and later catches up).</li>
%%%   <li>`ship-interval-ms' (1000). Failed target jobs are retried with
%%%       exponential backoff up to 5 minutes.</li>
%%%   <li>`max-fill-pct' (75), `max-bytes' (none): never take the target's
%%%       filesystem above this fill, or the export above this budget. At the
%%%       ceiling the export pauses (the backlog grows, nothing is lost),
%%%       emits `essentials_export_ceiling' events, and resumes by itself.</li>
%%%   <li>`target-ops' (2): process-wide bound on concurrent target jobs.</li>
%%% </ul>
-module(hb_store_export).
-export([wrap/2, status/1, restore/2, restore/3, sync_export/1, resync/1]).
-export([space_ok/3, put_file/2, append_manifest/2, dir_bytes/1, shutdown_all/0]).
-export([with_target/2, raw_is_file/1, raw_ensure_dir/1, raw_write_file/2]).
-export([verify_restorable/1, verify_restorable/2, exported_marks/1, prepare_prune/1]).
-define(PRUNED_MARK, "local-pruned").
-export([start/3, stop/3, reset/3, scope/1]).
-export([read/3, write/3, list/3, match/3, group/3, link/3, type/3,
         resolve/3, sync/3, delete/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-define(DEFAULT_SEGMENT_BYTES, 8 * 1024 * 1024).
-define(DEFAULT_SEGMENT_MS, 60000).
-define(DEFAULT_MAX_FILL_PCT, 75).
-define(MAX_BACKOFF_MS, 300000).
-define(DEFAULT_MAX_BACKLOG, 4 * 1024 * 1024 * 1024).
-define(DEFAULT_MAX_PENDING, 100000).
-define(DEFAULT_SHIP_MS, 1000).
-define(DEFAULT_TARGET_OPS, 2).
-define(SYNC_WAIT_MS, 2000).
-define(DEFAULT_AUDIT_MS, 3600000).
-define(SCAN_ROWS, 2000).
-define(REG, hb_store_export_registry).
%% Test fixtures write assignments for made-up process IDs with no
%% definitions, some starting above slot 0.
-define(LAX, #{ <<"require-definitions">> => false, <<"allow-partial-processes">> => true }).
-define(GATE, hb_store_export_target_gate).
-define(SCHED_ASSIGN, "~scheduler@1.0/assignments/").
-define(SMALL_NAMESPACES,
    [<<"~location@1.0">>, <<"~bundler@1.0">>, <<"~meta@1.0">>, <<"~arweave@2.9">>,
     <<"~scheduler@1.0/uploaded">>]).

%% @doc A store message that wraps `Inner' with an export configured by
%% `Export'. The wrapper carries the inner store's name, so it names one
%% exporter per inner store.
wrap(Inner = #{ <<"store-module">> := Mod }, Export) ->
    #{
        <<"store-module">> => ?MODULE,
        <<"name">> => maps:get(<<"name">>, Inner, hb_util:bin(Mod)),
        <<"inner">> => Inner,
        <<"export">> => Export
    }.

inner(#{ <<"inner">> := Inner }) -> Inner.
inner_mod(Store) -> maps:get(<<"store-module">>, inner(Store)).

%%% Store callbacks: the inner store answers; writes are also recorded.

start(Store, Req, Opts) ->
    ok = hb_store:start([inner(Store)], Req, Opts),
    _ = exporter(Store),
    ok.

%% Stopping closes the inner LMDB environment, which the target worker may be
%% reading (a base image) and the catch-up may be walking: both are stopped
%% first, and only then is the environment closed.
stop(Store, Req, Opts) ->
    catch call(Store, stop_export, 30000),
    catch call(Store, quiesce, 60000),
    hb_store:stop([inner(Store)], Req, Opts).

reset(Store, Req, Opts) ->
    hb_store:reset([inner(Store)], Req, Opts).

scope(_Store) -> local.

read(Store, Req, Opts) -> (inner_mod(Store)):read(inner(Store), Req, Opts).
list(Store, Req, Opts) -> (inner_mod(Store)):list(inner(Store), Req, Opts).
match(Store, Req, Opts) -> (inner_mod(Store)):match(inner(Store), Req, Opts).
type(Store, Req, Opts) -> (inner_mod(Store)):type(inner(Store), Req, Opts).
resolve(Store, Req, Opts) -> (inner_mod(Store)):resolve(inner(Store), Req, Opts).

%% @doc Make the inner store durable at the requested level, then wait --
%% bounded -- for the writer to have appended every record sent so far to the
%% local journal (and, at `fsync', synced it). The target is never waited for.
sync(Store, Req, Opts) ->
    Mod = inner_mod(Store),
    _ = code:ensure_loaded(Mod),
    Res =
        case erlang:function_exported(Mod, sync, 3) of
            true -> Mod:sync(inner(Store), Req, Opts);
            false -> ok
        end,
    Level = case maps:get(<<"level">>, Req, commit) of
                <<"fsync">> -> fsync; fsync -> fsync; _ -> commit
            end,
    case call(Store, {drain, Level}, ?SYNC_WAIT_MS) of
        ok -> ok;
        {error, timeout} ->
            ?event(warning, {essentials_export_sync_slow, maps:get(<<"name">>, Store)});
        _ -> ok
    end,
    Res.

%% The export is an archive of essentials, which are never deleted: a delete
%% is applied locally only when the inner store supports one, and is not
%% exported.
delete(Store, Req, Opts) ->
    Mod = inner_mod(Store),
    _ = code:ensure_loaded(Mod),
    case erlang:function_exported(Mod, delete, 3) of
        true -> Mod:delete(inner(Store), Req, Opts);
        false -> {error, not_supported}
    end.

write(Store, Req, Opts) ->
    recorded(Store, {write, Req}, (inner_mod(Store)):write(inner(Store), Req, Opts)).
link(Store, Req, Opts) ->
    recorded(Store, {link, Req}, (inner_mod(Store)):link(inner(Store), Req, Opts)).
group(Store, Req, Opts) ->
    recorded(Store, {group, Req}, (inner_mod(Store)):group(inner(Store), Req, Opts)).

%% Record only what the inner store accepted.
recorded(Store, Rec, ok) -> record(Store, Rec), ok;
recorded(Store, Rec, {ok, _} = R) -> record(Store, Rec), R;
recorded(_Store, _Rec, R) -> R.

%%% The write side: a counter and a message, nothing else.

record(Store, Rec) ->
    {Pid, Counters} = exporter(Store),
    Max = cfg(Store, <<"max-pending">>, ?DEFAULT_MAX_PENDING),
    case counters:get(Counters, 1) >= Max of
        true ->
            counters:add(Counters, 2, 1),
            Pid ! overflow;
        false ->
            counters:add(Counters, 1, 1),
            Pid ! {rec, Rec}
    end,
    ok.

%% @doc The exporter of a wrapped store: its writer pid and its counters
%% (1: records queued, 2: records dropped). Started on first use, without
%% waiting: the writer is registered before it initialises, and the records
%% sent meanwhile wait in its mailbox.
exporter(Store) ->
    Name = maps:get(<<"name">>, Store),
    ensure_registry(),
    case ets:lookup(?REG, Name) of
        [{_, Pid, Counters}] ->
            case is_process_alive(Pid) of
                true -> {Pid, Counters};
                false ->
                    ets:delete_object(?REG, {Name, Pid, Counters}),
                    exporter(Store)
            end;
        [] ->
            Counters = counters:new(2, [write_concurrency]),
            % Whether the inner store holds rows the journal never saw is
            % decided here, before the writer exists and anything can be
            % recorded: only on the very first start (no journal yet), and
            % from the local store only.
            First = first_start_needs_base(Store),
            Pid = spawn(fun() -> writer_start(Store, Counters, First) end),
            case ets:insert_new(?REG, {Name, Pid, Counters}) of
                true -> Pid ! go, {Pid, Counters};
                false -> exit(Pid, kill), exporter(Store)
            end
    end.

%% The registry is owned by a process that never exits.
ensure_registry() ->
    case ets:whereis(?REG) of
        undefined ->
            Parent = self(),
            Ref = make_ref(),
            {Owner, Mon} =
                spawn_monitor(fun() ->
                    try ets:new(?REG, [named_table, public, set, {read_concurrency, true}]) of
                        _ -> Parent ! {Ref, ok}, receive after infinity -> ok end
                    catch error:badarg -> Parent ! {Ref, exists}
                    end
                end),
            receive
                {Ref, _} -> ok;
                {'DOWN', Mon, process, Owner, _} -> ok
            after 5000 -> ok
            end,
            erlang:demonitor(Mon, [flush]),
            ok;
        _ -> ok
    end.

call(Store, Msg, Timeout) ->
    {Pid, _} = exporter(Store),
    call_pid(Pid, Msg, Timeout).

call_pid(Pid, Msg, Timeout) ->
    Ref = make_ref(),
    Mon = erlang:monitor(process, Pid),
    Pid ! {call, self(), Ref, Msg},
    receive
        {Ref, Reply} -> erlang:demonitor(Mon, [flush]), Reply;
        {'DOWN', Mon, process, Pid, Reason} -> {error, {exporter_down, Reason}}
    after Timeout -> erlang:demonitor(Mon, [flush]), {error, timeout}
    end.

%% @doc Per process, the highest assignment slot S such that every slot up to
%% S is in a segment already shipped to the target (as of the newest shipped
%% segment). A process absent from the map has nothing guaranteed exported.
exported_marks(Store) ->
    case call(Store, exported_marks, 10000) of
        M when is_map(M) -> M;
        _ -> #{}
    end.

%% @doc Ask the export to become the only copy of history, ahead of the local
%% store being pruned (`essentials-retention-days'). Returns `true' once the
%% target carries the `local-pruned' marker -- from then on a base never
%% supersedes (deletes) older files, and a restore replays the whole chain
%% from the start -- and `false' until then (the marker is written by the
%% target worker at its next opportunity).
prepare_prune(Store) ->
    case call(Store, prepare_prune, 10000) of
        true -> true;
        _ -> false
    end.

%% @doc Export status and lag metrics.
status(Store) -> call(Store, status, 10000).

%% @doc Schedule a base image: for rows written to the inner store without
%% passing through the wrapper (a migration copies raw rows into it).
resync(Store) -> call(Store, resync, 10000).

%% @doc Close the current segment and return once everything journaled so far
%% is shipped (or the target refused it: an error, the ceiling). Blocking; for
%% tests and for an operator before maintenance. The writer keeps serving
%% while it waits.
sync_export(Store) -> call(Store, sync_export, 600000).

%% @doc Stop every exporter in this VM cleanly: append whatever is queued,
%% close the segment, persist the watermarks, and record the clean stop.
%% Called from `hb_app:stop/1' once the listener is down.
shutdown_all() ->
    case ets:whereis(?REG) of
        undefined -> ok;
        _ ->
            [ catch call_pid(Pid, stop_export, 30000)
            || {_, Pid, _} <- ets:tab2list(?REG), is_process_alive(Pid) ],
            ok
    end.

cfg(Store, Key, Default) ->
    Export = maps:get(<<"export">>, Store, #{}),
    case maps:get(Key, Export, Default) of
        V when is_binary(V), is_integer(Default) -> binary_to_integer(V);
        V when is_binary(V), is_boolean(Default) -> V == <<"true">>;
        V -> V
    end.

%%% The writer: local journal only.

writer_start(Store, Counters, First) ->
    process_flag(trap_exit, true),
    receive go -> ok after 5000 -> ok end,
    writer_loop(init_state(Store, Counters, First)).

journal_dir(Store) ->
    Inner = inner(Store),
    hb_util:list(cfg(Store, <<"journal">>,
        <<(hb_util:bin(maps:get(<<"name">>, Inner, <<"essentials">>)))/binary, "-journal">>)).

first_start_needs_base(Store) ->
    case raw_is_file(filename:join(journal_dir(Store), "state")) of
        true -> false;
        false -> not inner_empty(inner(Store))
    end.

init_state(Store, Counters, FirstNeedsBase) ->
    Inner = inner(Store),
    Journal = journal_dir(Store),
    Path = hb_util:list(cfg(Store, <<"path">>, undefined)),
    ok = raw_ensure_dir(filename:join(Journal, "x")),
    Local = local_segments(Journal),
    StateFile = filename:join(Journal, "state"),
    PrevState =
        case prim_file:read_file(StateFile) of
            {error, enoent} -> first;
            {ok, <<"stopped">>} -> clean;
            {ok, _} -> unclean
        end,
    % A segment number is never reused: the next one is persisted locally, and
    % a fresh journal starts from the clock, above anything an older journal
    % shipped.
    Seq =
        lists:max([
            read_int(filename:join(Journal, "next-seq"), 0),
            1 + lists:max([0 | Local]),
            case PrevState of first -> os:system_time(second) * 10; _ -> 0 end
        ]),
    {WSeq, Marks0} = read_watermarks(Journal),
    % Unshipped local segments are durable: fold their assignments in, and cut
    % a torn tail off the one the crash interrupted.
    Marks = lists:foldl(
        fun(Q, M) when Q >= WSeq -> segment_watermarks(seg_file(Journal, Q), M);
           (_, M) -> M
        end, Marks0, Local),
    ok = raw_write_file(StateFile, <<"exporting">>),
    OpenGaps = read_gaps(Journal),
    Worker = spawn_link(fun() -> target_worker_start(Path) end),
    S0 = #{
        store => Store,
        inner => Inner,
        counters => Counters,
        journal => Journal,
        path => Path,
        worker => Worker,
        worker_busy => true,
        seq => Seq,
        fd => undefined,
        seg_bytes => 0,
        seg_opened => undefined,
        backlog => lists:sum([ raw_size(seg_file(Journal, Q)) || Q <- Local ]),
        dropping => false,
        marks => Marks,
        catchup => undefined,
        open_gaps => OpenGaps,
        resync => raw_is_file(filename:join(Journal, "resync-needed")),
        pruned_marker => raw_is_file(filename:join(Journal, ?PRUNED_MARK)),
        want_marker => false,
        target_bytes => 0,
        ceiling => false,
        next_try => 0,
        waiters => [],
        stats => #{ shipped_segments => 0, shipped_bytes => 0, raw_bytes => 0,
                    errors => 0, consecutive_errors => 0, ceiling_hits => 0,
                    resyncs => 0, catchups => 0, catchup_rows => 0, pruned_files => 0,
                    last_ship => undefined, last_error => undefined }
    },
    Worker ! {job, self(), target_info},
    S1 =
        case PrevState of
            clean -> maybe_catchup(S0);
            first ->
                % A non-empty store with no journal yet (a migration, or a node
                % that ran without the export) needs a base.
                case FirstNeedsBase of
                    true -> mark_resync(S0);
                    false -> S0
                end;
            unclean when Marks == #{}, WSeq == 0, OpenGaps == [] ->
                % A journal from before watermarks: nothing to catch up from.
                ?event(warning, {essentials_export_unclean_start, {journal, Journal}, base}),
                mark_resync(S0);
            unclean ->
                % Everything journaled durably is in the recovered watermarks;
                % anything after them may be lost. Open a gap from them, then
                % catch up on every open gap.
                ?event(warning, {essentials_export_unclean_start, {journal, Journal}, catchup}),
                maybe_catchup(open_gap(S0))
        end,
    erlang:send_after(cfg(Store, <<"ship-interval-ms">>, ?DEFAULT_SHIP_MS), self(), tick),
    S1.

read_int(File, Default) ->
    case prim_file:read_file(File) of
        {ok, B} -> try binary_to_integer(B) catch _:_ -> Default end;
        _ -> Default
    end.

inner_empty(#{ <<"store-module">> := hb_store_lmdb } = Inner) ->
    try
        #{ <<"db">> := DB } = hb_store:find(Inner),
        ok = elmdb:flush(DB),
        case elmdb:scan_rows(DB, <<>>, 1) of
            {ok, [], _, _} -> true;
            _ -> false
        end
    catch _:_ -> false
    end;
inner_empty(_) -> false.

writer_loop(S) ->
    receive
        {rec, Rec} ->
            {Recs, Next} = drain([Rec], 1000),
            counters:sub(maps:get(counters, S), 1, length(Recs)),
            S1 = append(S, Recs),
            writer_loop(case Next of overflow -> start_dropping(S1); none -> S1 end);
        overflow ->
            writer_loop(start_dropping(S));
        tick ->
            S1 = maybe_audit(maybe_catchup(maybe_ship(maybe_roll(S)))),
            erlang:send_after(
                cfg(maps:get(store, S), <<"ship-interval-ms">>, ?DEFAULT_SHIP_MS),
                self(), tick),
            writer_loop(answer_waiters(S1));
        {worker, Result} ->
            writer_loop(answer_waiters(maybe_ship(worker_done(S, Result))));
        {catchup, From, Rows0} ->
            % Catch-up rows are never dropped, whatever the live records are
            % doing; the catch-up sends the next chunk only after this ack.
            % A mutable key (outside the content-addressed units) journaled
            % live since the catch-up began has a newer value than the row
            % the catch-up read: the row is not journaled after it.
            Fresh = maps:get(live_mutable, S, #{}),
            Rows = [ R || R = {K, _} <- Rows0, not maps:is_key(K, Fresh) ],
            S1 = append(S, [{raw, Rows}], force),
            From ! catchup_ack,
            writer_loop(S1);
        {catchup_done, Gaps, N, Caught} ->
            writer_loop(catchup_done(S, Gaps, N, Caught));
        {'EXIT', Pid, Reason} ->
            case S of
                #{ worker := Pid } ->
                    % The worker never dies on a target error (it reports
                    % them); restart it all the same.
                    ?event(warning, {essentials_export_worker_down, Reason}),
                    W = spawn_link(fun() -> target_worker_start(maps:get(path, S)) end),
                    writer_loop(S#{ worker => W, worker_busy => false });
                #{ catchup := {Pid, _} } when Reason =/= normal ->
                    % Its gaps stay open; a base closes them.
                    ?event(warning, {essentials_export_catchup_failed, Reason}),
                    writer_loop(mark_resync(S#{ catchup => undefined }));
                _ -> writer_loop(S)
            end;
        {call, From, Ref, status} ->
            From ! {Ref, status_of(S)},
            writer_loop(S);
        {call, From, Ref, prepare_prune} ->
            From ! {Ref, maps:get(pruned_marker, S)},
            writer_loop(S#{ want_marker => true });
        {call, From, Ref, exported_marks} ->
            From ! {Ref, maps:get(exported, S, #{})},
            writer_loop(S);
        {call, From, Ref, inner} ->
            From ! {Ref, maps:get(inner, S)},
            writer_loop(S);
        {call, From, Ref, resync} ->
            From ! {Ref, ok},
            writer_loop(mark_resync(S));
        {call, From, Ref, {drain, Level}} ->
            S1 = drain_all(S),
            case {Level, maps:get(fd, S1)} of
                {fsync, Fd} when Fd =/= undefined -> _ = file:datasync(Fd);
                _ -> ok
            end,
            From ! {Ref, ok},
            writer_loop(S1);
        {call, From, Ref, flush} ->
            S1 = close_segment(drain_all(S)),
            From ! {Ref, ok},
            writer_loop(S1);
        {call, From, Ref, stop_export} ->
            S1 = close_segment(drain_all(S)),
            ok = write_watermarks(S1),
            % Open gaps are persisted: the next start resumes their catch-up
            % whether or not this one finished.
            ok = raw_write_file(filename:join(maps:get(journal, S1), "state"), <<"stopped">>),
            From ! {Ref, ok},
            writer_loop(S1#{ stopped => true });
        {call, From, Ref, quiesce} ->
            % Stop whatever reads the inner store: a base in flight in the
            % target worker, a catch-up. Their NIF calls complete before the
            % processes exit, so no read transaction outlives this reply.
            S1 = quiesce(S),
            From ! {Ref, ok},
            writer_loop(S1);
        {call, From, Ref, sync_export} ->
            S1 = maybe_ship(close_segment(drain_all(S#{ next_try => 0 }))),
            writer_loop(answer_waiters(S1#{ waiters => [{From, Ref} | maps:get(waiters, S1)] }))
    end.

quiesce(S = #{ worker := W, catchup := C, path := Path }) ->
    Stop =
        fun(undefined) -> ok;
           (Pid) ->
               unlink(Pid),
               Mon = erlang:monitor(process, Pid),
               exit(Pid, kill),
               receive {'DOWN', Mon, process, Pid, _} -> ok end
        end,
    Stop(W),
    case C of {CPid, _} -> Stop(CPid); _ -> ok end,
    receive {worker, _} -> ok after 0 -> ok end,
    W1 = spawn_link(fun() -> target_worker_start(Path) end),
    % A killed catch-up's gaps stay open (persisted) and are redone at the
    % next start.
    S#{ worker => W1, worker_busy => false, catchup => undefined }.

%% Take queued records in arrival order, stopping at an `overflow' message:
%% a record a sender queued after it dropped one must not be journaled before
%% the drop is seen (it would raise a watermark past the dropped record).
drain(Acc, 0) -> {lists:reverse(Acc), none};
drain(Acc, N) ->
    receive
        {rec, R} -> drain([R | Acc], N - 1);
        overflow -> {lists:reverse(Acc), overflow}
    after 0 -> {lists:reverse(Acc), none}
    end.

drain_all(S) ->
    receive
        {rec, R} ->
            counters:sub(maps:get(counters, S), 1, 1),
            drain_all(append(S, [R]));
        overflow ->
            drain_all(start_dropping(S))
    after 0 -> S
    end.

%% Append records to the current local segment, rolling it by size. Written
%% straight through (no write-behind buffer): a record appended is in the OS
%% when this returns, which is what `sync/3' promises.
append(S, Recs) -> append(S, Recs, live).

append(S = #{ dropping := true }, _Recs, live) -> S;
append(S = #{ stopped := true }, Recs, Mode) ->
    ok = raw_write_file(filename:join(maps:get(journal, S), "state"), <<"exporting">>),
    append(maps:remove(stopped, S), Recs, Mode);
append(S, Recs0, Mode) ->
    S1a = ensure_segment(S),
    {Recs, S1} = dedupe(Recs0, S1a),
    Bin = << <<(frame(R))/binary>> || R <- Recs >>,
    case file:write(maps:get(fd, S1), Bin) of
        ok ->
            S2 = S1#{ seg_bytes => maps:get(seg_bytes, S1) + byte_size(Bin),
                      backlog => maps:get(backlog, S1) + byte_size(Bin),
                      live_mutable =>
                          case {Mode, maps:get(catchup, S1)} of
                              % Before dedupe: a live write skipped as equal to
                              % the last journaled value is still newer than
                              % what the catch-up read.
                              {live, {_, _}} -> mutable_keys(Recs0, maps:get(live_mutable, S1, #{}));
                              _ -> maps:get(live_mutable, S1, #{})
                          end,
                      marks => lists:foldl(fun rec_watermarks/2, maps:get(marks, S1), Recs) },
            Store = maps:get(store, S),
            case Mode == live andalso maps:get(backlog, S2) >= cfg(Store, <<"max-backlog-bytes">>,
                                              ?DEFAULT_MAX_BACKLOG) of
                true -> start_dropping(S2);
                false ->
                    case maps:get(seg_bytes, S2) >= cfg(Store, <<"segment-bytes">>,
                                                        ?DEFAULT_SEGMENT_BYTES) of
                        true -> close_segment(S2);
                        false -> S2
                    end
            end;
        {error, Reason} when Mode == live ->
            ?event(error, {essentials_journal_write_failed, Reason}),
            start_dropping(bump_stat(S1, errors));
        {error, Reason} ->
            % A catch-up row that cannot be written fails the catch-up; its
            % gaps stay open.
            erlang:error({essentials_journal_write_failed, Reason})
    end.

frame(Rec) ->
    Bin = term_to_binary(Rec),
    <<(byte_size(Bin)):32, (erlang:crc32(Bin)):32, Bin/binary>>.

%% A frame whose payload is compressed (base images: thousands of rows each).
frame_compressed(Rec) ->
    Bin = term_to_binary(Rec, [{compressed, 6}]),
    <<(byte_size(Bin)):32, (erlang:crc32(Bin)):32, Bin/binary>>.

mutable_keys(Recs, Acc) ->
    lists:foldl(
        fun({Op, Req}, A) when (Op == write orelse Op == link) andalso is_map(Req) ->
                maps:fold(fun(K, _, A2) ->
                              KB = hb_util:bin(K),
                              case unit_key(KB) of
                                  true -> A2;
                                  false -> A2#{ KB => true }
                              end
                          end, A, Req);
           (_, A) -> A
        end, Acc, Recs).

%% Keys of content-addressed units are immutable; everything else may change.
unit_key(<<"data/", _/binary>>) -> true;
unit_key(K) -> byte_size(hd(binary:split(K, <<"/">>))) == 43.

%% The same row is written many times over: `hb_cache' writes a message's
%% nested messages while converting it (the offload) and again when it writes
%% the message, and every message re-writes the blobs and tag messages it
%% shares with the last. Within a segment a row is journaled only when its
%% value differs from the last value journaled for that key in the segment, so
%% replaying the segment in order gives every key its last written value
%% (V1, V2, V1 restores V1).
dedupe(Recs, S = #{ seen := Seen }) ->
    {Out, Seen1} =
        lists:foldl(
            fun(Rec, {Acc, Sn}) ->
                case Rec of
                    {Op, Req} when (Op == write orelse Op == link orelse Op == raw)
                                   andalso (is_map(Req) orelse is_list(Req)) ->
                        Pairs = case Req of M when is_map(M) -> maps:to_list(M); L -> L end,
                        {Keep, Sn1} =
                            lists:foldl(
                                fun({K, V}, {KAcc, X}) ->
                                    H = erlang:md5(term_to_binary({Op, V})),
                                    % Keyed by the row alone: a catch-up row
                                    % (`raw') between two equal live writes
                                    % must not let the second be skipped.
                                    case maps:get(K, X, undefined) of
                                        H -> {KAcc, X};
                                        _ -> {[{K, V} | KAcc], X#{ K => H }}
                                    end
                                end,
                                {[], Sn}, Pairs),
                        case {Keep, Op} of
                            {[], _} -> {Acc, Sn1};
                            {_, raw} -> {[{raw, lists:reverse(Keep)} | Acc], Sn1};
                            _ -> {[{Op, maps:from_list(Keep)} | Acc], Sn1}
                        end;
                    _ -> {[Rec | Acc], Sn}
                end
            end,
            {[], Seen}, Recs),
    {lists:reverse(Out), S#{ seen => Seen1 }};
dedupe(Recs, S) ->
    dedupe(Recs, S#{ seen => #{} }).

ensure_segment(S = #{ fd := undefined, journal := J, seq := Seq }) ->
    ok = raw_write_file(filename:join(J, "next-seq"), integer_to_binary(Seq + 1)),
    {ok, Fd} = file:open(seg_file(J, Seq), [append, raw, binary]),
    S#{ fd => Fd, seg_bytes => 0, seg_opened => erlang:monotonic_time(millisecond) };
ensure_segment(S) -> S.

close_segment(S = #{ fd := undefined }) -> S;
close_segment(S0 = #{ fd := Fd, seq := Seq }) ->
    _ = file:close(Fd),
    % What this segment completes, for `exported_marks/1' once it ships.
    % Only a segment closed with no gap open and nothing being dropped
    % completes its marks: with a gap open, rows below them may be missing.
    S = case {maps:get(open_gaps, S0), maps:get(dropping, S0), maps:get(catchup, S0)} of
            {[], false, undefined} ->
                S0#{ closed_marks => (maps:get(closed_marks, S0, #{}))#{
                         Seq => contiguous_marks(maps:get(marks, S0)) } };
            _ -> S0
        end,
    S1 = S#{ fd => undefined, seq => Seq + 1, seg_bytes => 0, seg_opened => undefined,
             seen => #{} },
    ok = write_watermarks(S1),
    S1.

maybe_roll(S = #{ seg_opened := undefined }) -> S;
maybe_roll(S = #{ seg_opened := Opened }) ->
    case erlang:monotonic_time(millisecond) - Opened >=
            cfg(maps:get(store, S), <<"segment-ms">>, ?DEFAULT_SEGMENT_MS) of
        true -> close_segment(S);
        false -> S
    end.

%%% Watermarks: per process, the highest assignment slot S such that every
%%% slot up to S is journaled -- `{Contiguous, PendingAbove}'. A plain maximum
%%% is not a lower bound when one process's assignments are journaled out of
%%% order (several writers of one process): slot 2001 journaled before slot
%%% 102 was dropped put a maximum-based gap above 102. Slots start at 0; a
%%% catch-up or a base also moves `Contiguous' up to what they exported.

rec_watermarks({link, Req}, Marks) when is_map(Req) ->
    maps:fold(fun(K, _V, M) -> key_watermark(hb_util:bin(K), M) end, Marks, Req);
%% Catch-up rows (`raw') do not move the watermarks: a catch-up interrupted
%% half way must be redone whole. Its completion moves them (`catchup_done').
rec_watermarks(_, Marks) -> Marks.

key_watermark(<<?SCHED_ASSIGN, Rest/binary>>, Marks) ->
    case binary:split(Rest, <<"/">>) of
        [P, SlotBin] ->
            try binary_to_integer(SlotBin) of
                Slot -> Marks#{ P => mark_add(maps:get(P, Marks, {-1, []}), Slot) }
            catch _:_ -> Marks
            end;
        _ -> Marks
    end;
key_watermark(_, Marks) -> Marks.

%% Add a journaled slot to a process's mark.
mark_add({C, Pending}, Slot) when Slot =< C -> {C, Pending};
mark_add({C, Pending}, Slot) when Slot == C + 1 -> mark_advance({Slot, Pending});
mark_add({C, Pending}, Slot) -> {C, ordsets:add_element(Slot, Pending)}.

mark_advance({C, [Next | Rest]}) when Next == C + 1 -> mark_advance({Next, Rest});
mark_advance({C, [Next | Rest]}) when Next =< C -> mark_advance({C, Rest});
mark_advance(M) -> M.

%% Everything up to `Upto' is journaled (a catch-up or base exported it).
mark_raise(Marks, P, Upto) ->
    {C, Pending} = maps:get(P, Marks, {-1, []}),
    Marks#{ P => mark_advance({max(C, Upto), Pending}) }.

%% The lower bounds a gap is opened from.
contiguous_marks(Marks) ->
    maps:map(fun(_, {C, _}) -> C; (_, C) when is_integer(C) -> C end, Marks).

%% The watermarks file holds them as of the start of segment `Seq': every
%% segment numbered `Seq' or above may hold more.
write_watermarks(#{ journal := J, seq := Seq, marks := Marks }) ->
    File = filename:join(J, "watermarks"),
    ok = raw_write_file(File ++ ".tmp", term_to_binary({Seq, Marks})),
    prim_file:rename(File ++ ".tmp", File).

read_watermarks(J) ->
    case prim_file:read_file(filename:join(J, "watermarks")) of
        {ok, Bin} ->
            try binary_to_term(Bin) of
                {Seq, Marks} when is_map(Marks) ->
                    {Seq, maps:map(fun(_, C) when is_integer(C) -> {C, []}; (_, M) -> M end, Marks)};
                _ -> {0, #{}}
            catch _:_ -> {0, #{}}
            end;
        _ -> {0, #{}}
    end.

%% Fold one local segment's records into the watermarks; cut a torn tail off.
segment_watermarks(File, Marks) ->
    case prim_file:read_file(File) of
        {ok, Bin} ->
            Good = valid_prefix(Bin, 0),
            case Good == byte_size(Bin) of
                true -> ok;
                false ->
                    ?event(warning, {essentials_journal_torn_tail, File, Good, byte_size(Bin)}),
                    ok = raw_write_file(File, binary:part(Bin, 0, Good))
            end,
            lists:foldl(fun rec_watermarks/2, Marks, records(binary:part(Bin, 0, Good)));
        _ -> Marks
    end.

records(<<Len:32, _Crc:32, Bin:Len/binary, Rest/binary>>) ->
    [binary_to_term(Bin) | records(Rest)];
records(_) -> [].

valid_prefix(Bin, Off) ->
    case Bin of
        <<_:Off/binary, Len:32, Crc:32, Frame:Len/binary, _/binary>> ->
            case erlang:crc32(Frame) of
                Crc -> valid_prefix(Bin, Off + 8 + Len);
                _ -> Off
            end;
        _ -> Off
    end.

%%% Gaps: dropping, unclean starts, catch-up.
%%%
%%% A gap is opened whenever records may have been lost: an unclean start, or
%%% the writer dropping live records past a bound. It is a pair
%%% `{Id, StartMarks}': every assignment of process P above `StartMarks[P]'
%%% (every assignment, and the definition, of a process not in it) may be
%%% missing from the journal. Open gaps are persisted (`gaps', synced) BEFORE
%%% anything else is journaled, and a gap mark `gap-<Id>.mark' is shipped
%%% ahead of every later segment. A catch-up covers the open gaps it starts
%%% with: it re-exports, from the local store, everything above the
%%% pointwise minimum of their start marks -- never from watermarks raised by
%%% live appends since -- and finishes with `{gaps_filled, Ids}' naming
%%% exactly those gaps. Only then are they removed from `gaps'. A gap opened
%%% while a catch-up runs is left for the next one. A completed base closes
%%% every gap opened before it began.

%% Past a bound: stop journaling live records, and open a gap from the
%% watermarks as they are now -- `drain/2' has stopped at the overflow, so they
%% are below every record dropped. Resumes once the backlog has shipped and the
%% queue has drained.
start_dropping(S = #{ dropping := true }) -> S;
start_dropping(S) ->
    ?event(warning, {essentials_export_backlog_full, status_of(S)}),
    open_gap(close_segment(S#{ dropping => true })).

mark_resync(S = #{ journal := J }) ->
    ok = raw_write_file(filename:join(J, "resync-needed"), <<>>),
    S#{ resync => true }.

%% Open a gap from the current watermarks; persist it before returning.
open_gap(S0 = #{ journal := J, marks := Marks }) ->
    S = close_segment(S0),
    Id = maps:get(seq, S),
    Gaps = maps:get(open_gaps, S) ++ [{Id, contiguous_marks(Marks)}],
    ok = write_gaps(J, Gaps),
    ok = raw_write_file(filename:join(J, "gap-" ++ seq_str(Id) ++ ".mark"), <<>>),
    % The id is used: the next segment is numbered after it.
    ok = raw_write_file(filename:join(J, "next-seq"), integer_to_binary(Id + 1)),
    S#{ seq => Id + 1, open_gaps => Gaps }.

write_gaps(J, Gaps) ->
    File = filename:join(J, "gaps"),
    {ok, Fd} = file:open(File ++ ".tmp", [write, raw, binary]),
    ok = file:write(Fd, term_to_binary(Gaps)),
    ok = file:sync(Fd),
    ok = file:close(Fd),
    prim_file:rename(File ++ ".tmp", File).

read_gaps(J) ->
    case prim_file:read_file(filename:join(J, "gaps")) of
        {ok, Bin} -> binary_to_term(Bin);
        _ -> []
    end.

%% Start a catch-up over every open gap, unless one is running or the writer
%% is dropping (the catch-up then waits for it to stop).
maybe_catchup(S = #{ open_gaps := [] }) -> S;
maybe_catchup(S = #{ catchup := {_, _} }) -> S;
maybe_catchup(S = #{ dropping := true }) -> S;
maybe_catchup(S = #{ inner := Inner, open_gaps := Gaps }) ->
    Ids = [ Id || {Id, _} <- Gaps ],
    Self = self(),
    Pid = spawn_link(fun() -> catchup(Self, Inner, Gaps) end),
    S#{ catchup => {Pid, Ids}, live_mutable => #{} }.

catchup_done(S, Ids, N, Caught) ->
    St = maps:get(stats, S),
    % Everything up to `Caught' is now journaled for each process.
    Marks = maps:fold(fun(P, M, Acc) -> mark_raise(Acc, P, M) end, maps:get(marks, S), Caught),
    S1 = append(S#{ marks => Marks }, [{gaps_filled, Ids}], force),
    case maps:get(fd, S1) of undefined -> ok; Fd -> ok = file:datasync(Fd) end,
    S2 = close_segment(S1),
    Open = [ G || G = {Id, _} <- maps:get(open_gaps, S2), not lists:member(Id, Ids) ],
    ok = write_gaps(maps:get(journal, S2), Open),
    maybe_catchup(S2#{ catchup => undefined, open_gaps => Open,
                       stats => St#{ catchups => maps:get(catchups, St) + 1,
                                     catchup_rows => maps:get(catchup_rows, St) + N } }).

catchup(Writer, Inner, Gaps) ->
    #{ <<"db">> := DB } = hb_store:find(Inner),
    ok = elmdb:flush(DB),
    Ids = [ Id || {Id, _} <- Gaps ],
    Start = fun(P) -> lists:min([ maps:get(P, M, -1) || {_, M} <- Gaps ]) end,
    Procs = children(DB, <<"~scheduler@1.0/assignments">>),
    Send =
        fun(Keys, N) ->
            Rows = hb_store_gc:closure_rows(DB, Keys),
            [ begin
                Writer ! {catchup, self(), Chunk},
                receive catchup_ack -> ok end
              end
            || Chunk <- chunk_rows(Rows, 500) ],
            N + length(Rows)
        end,
    {N1, Caught} =
        lists:foldl(
            fun(P, {N, C}) ->
                Mark = Start(P),
                All = [ Sl || Ch <- children(DB, <<?SCHED_ASSIGN, P/binary>>),
                              Sl <- [catch binary_to_integer(Ch)], is_integer(Sl) ],
                Slots = [ Sl || Sl <- All, Sl > Mark ],
                Keys = [ <<?SCHED_ASSIGN, P/binary, "/", (integer_to_binary(Sl))/binary>>
                       || Sl <- lists:sort(Slots) ]
                    ++ [ P || Mark == -1 ],
                C1 = case All of [] -> C; _ -> C#{ P => lists:max(All) } end,
                {Send(Keys, N), C1}
            end,
            {0, #{}},
            Procs),
    N2 = Send(?SMALL_NAMESPACES, N1),
    Writer ! {catchup_done, Ids, N2, Caught}.

children(DB, Prefix) ->
    case elmdb:list(DB, <<Prefix/binary, "/">>) of
        {ok, L} -> [ C || C <- L, is_binary(C) ];
        _ -> []
    end.

chunk_rows([], _N) -> [];
chunk_rows(L, N) when length(L) =< N -> [L];
chunk_rows(L, N) -> {A, B} = lists:split(N, L), [A | chunk_rows(B, N)].

%%% Shipping: the writer hands jobs to the target worker and never waits.

maybe_ship(S = #{ worker_busy := true }) -> S;
maybe_ship(S = #{ next_try := Next }) ->
    case erlang:system_time(millisecond) >= Next of
        false -> S;
        true -> do_maybe_ship(S)
    end.

%% The periodic audit (`audit-interval-ms', default 1 h): with the worker
%% idle, check the target as a restore would see it -- one listing and one
%% manifest read -- and keep the result in the status as `last_audit'.
maybe_audit(S = #{ worker_busy := true }) -> S;
maybe_audit(S = #{ worker := W, path := Path }) ->
    Every = cfg(maps:get(store, S), <<"audit-interval-ms">>, ?DEFAULT_AUDIT_MS),
    Now = erlang:system_time(millisecond),
    case Now - maps:get(last_audit_start, S, 0) >= Every of
        false -> S;
        true ->
            Open = [ Id || {Id, _} <- maps:get(open_gaps, S) ],
            W ! {job, self(), {audit, Path, Open}},
            S#{ worker_busy => true, last_audit_start => Now }
    end.

do_maybe_ship(S = #{ want_marker := true, pruned_marker := false, worker := W,
                      path := Path }) ->
    % Every base from here on has a higher id than the marker records.
    W ! {job, self(), {mark_pruned, Path, maps:get(seq, S)}},
    S#{ worker_busy => true };
do_maybe_ship(S = #{ worker := W, journal := J }) ->
    Segs = closed_segments(S),
    Gaps = segs_in(J, "gap-", ".mark"),
    Cfg = export_cfg(S),
    case {Segs, Gaps, maps:get(resync, S), maps:get(dropping, S)} of
        {[], [], false, false} -> S;
        {[], [], _, true} ->
            % The backlog is gone and the queue has drained: journal again,
            % and catch up on what was dropped meanwhile. A base waits for
            % this too: a gap opened by dropping that is still dropping after
            % the base's reservation would be closed by the base while its
            % newest records are in neither (the scan is not a snapshot).
            Max = cfg(maps:get(store, S), <<"max-pending">>, ?DEFAULT_MAX_PENDING),
            case counters:get(maps:get(counters, S), 1) < max(1, Max div 2) of
                true -> maybe_catchup(S#{ dropping => false });
                false -> S
            end;
        {[], [], true, false} ->
            % The base takes an id of its own; gaps opened before it are
            % closed by it, gaps opened after it have higher ids.
            S1 = close_segment(S),
            B = maps:get(seq, S1),
            ok = raw_write_file(filename:join(maps:get(journal, S1), "next-seq"),
                                integer_to_binary(B + 1)),
            W ! {job, self(), {base, maps:get(inner, S1), B, Cfg, maps:get(target_bytes, S1)}},
            S1#{ worker_busy => true, seq => B + 1, base_pending => B };
        _ ->
            W ! {job, self(), {ship, J, Segs, Cfg, maps:get(target_bytes, S)}},
            S#{ worker_busy => true, shipping_segs => Segs }
    end.

worker_done(S, Result) ->
    S0 = S#{ worker_busy => false },
    St0 = maps:get(stats, S0),
    Ok = fun(X) -> X#{ next_try => 0, stats => (maps:get(stats, X))#{ consecutive_errors => 0 } } end,
    case Result of
        {target_info, Bytes} -> S0#{ target_bytes => Bytes };
        marked_pruned ->
            ok = raw_write_file(filename:join(maps:get(journal, S0), ?PRUNED_MARK), <<>>),
            S0#{ pruned_marker => true };
        {audited, Audit} ->
            case maps:get(ok, Audit) of
                true -> ok;
                false -> ?event(warning, {essentials_export_audit_failed, Audit})
            end,
            S0#{ last_audit => Audit };
        {shipped, N, Raw, Bytes, Ceiling} ->
            Shipped = lists:sublist(maps:get(shipping_segs, S0, []), N),
            Closed = maps:get(closed_marks, S0, #{}),
            Exported =
                case [ maps:get(Q, Closed) || Q <- Shipped, maps:is_key(Q, Closed) ] of
                    [] -> maps:get(exported, S0, #{});
                    Ms -> lists:last(Ms)
                end,
            S0b = S0#{ exported => Exported, closed_marks => maps:without(Shipped, Closed) },
            S1a = Ok(at_ceiling(S0b, Ceiling)),
            % At the ceiling, look again after an interval, not at once.
            S1 = case Ceiling of
                     false -> S1a;
                     _ -> S1a#{ next_try => erlang:system_time(millisecond) +
                                    cfg(maps:get(store, S), <<"ship-interval-ms">>, ?DEFAULT_SHIP_MS) }
                 end,
            St = maps:get(stats, S1),
            S1#{ backlog => max(0, maps:get(backlog, S1) - Raw),
                 target_bytes => maps:get(target_bytes, S1) + Bytes,
                 stats => St#{
                     shipped_segments => maps:get(shipped_segments, St) + N,
                     shipped_bytes => maps:get(shipped_bytes, St) + Bytes,
                     raw_bytes => maps:get(raw_bytes, St) + Raw,
                     last_ship => case N of 0 -> maps:get(last_ship, St);
                                            _ -> os:system_time(millisecond) end } };
        {based, Bytes, Freed, Pruned, BaseMaxes} ->
            _ = prim_file:delete(filename:join(maps:get(journal, S0), "resync-needed")),
            B = maps:get(base_pending, S0, 0),
            Open = [ G || G = {Id, _} <- maps:get(open_gaps, S0), Id > B ],
            ok = write_gaps(maps:get(journal, S0), Open),
            Marks = maps:fold(fun(P, M, Acc) -> mark_raise(Acc, P, M) end,
                              maps:get(marks, S0), BaseMaxes),
            S1 = Ok(at_ceiling(S0#{ open_gaps => Open, marks => Marks }, false)),
            St = maps:get(stats, S1),
            S1#{ resync => false,
                 target_bytes => max(0, maps:get(target_bytes, S1) + Bytes - Freed),
                 stats => St#{ resyncs => maps:get(resyncs, St) + 1,
                               pruned_files => maps:get(pruned_files, St) + Pruned } };
        {ceiling, Why} -> at_ceiling(S0#{ next_try => erlang:system_time(millisecond) +
                                          cfg(maps:get(store, S), <<"ship-interval-ms">>,
                                              ?DEFAULT_SHIP_MS) }, Why);
        {error, Reason} ->
            ?event(warning, {essentials_export_ship_failed, Reason}),
            N = maps:get(consecutive_errors, St0) + 1,
            Base = cfg(maps:get(store, S), <<"ship-interval-ms">>, ?DEFAULT_SHIP_MS),
            S0#{ next_try => erlang:system_time(millisecond) +
                                 min(?MAX_BACKOFF_MS, Base bsl min(N, 20)),
                 stats => St0#{ errors => maps:get(errors, St0) + 1, last_error => Reason,
                                consecutive_errors => N } }
    end.

%% Enter or leave the fill ceiling, loudly on each transition.
at_ceiling(S = #{ ceiling := Was }, Why) ->
    Now = Why =/= false,
    case {Was, Now} of
        {false, true} ->
            ?event(warning, {essentials_export_ceiling, paused, Why}),
            St = maps:get(stats, S),
            S#{ ceiling => true, stats => St#{ ceiling_hits => maps:get(ceiling_hits, St) + 1 } };
        {true, false} ->
            ?event(warning, {essentials_export_ceiling, resumed}),
            S#{ ceiling => false };
        _ -> S
    end.

%% `sync_export' callers are answered once nothing is left to ship -- or once
%% shipping cannot proceed (an error, the ceiling).
answer_waiters(S = #{ waiters := [] }) -> S;
answer_waiters(S = #{ waiters := Ws }) ->
    Idle = not maps:get(worker_busy, S),
    Done = closed_segments(S) == [] andalso not maps:get(resync, S)
        andalso maps:get(catchup, S) == undefined
        andalso maps:get(open_gaps, S) == []
        andalso not maps:get(dropping, S)
        andalso segs_in(maps:get(journal, S), "gap-", ".mark") == [],
    Stuck = maps:get(ceiling, S) orelse maps:get(consecutive_errors, maps:get(stats, S)) > 0,
    case Idle andalso (Done orelse Stuck) of
        true ->
            St = status_of(S),
            [ From ! {Ref, St} || {From, Ref} <- Ws ],
            S#{ waiters => [] };
        false -> S
    end.

export_cfg(#{ store := Store }) -> maps:get(<<"export">>, Store, #{}).

bump_stat(S, K) ->
    St = maps:get(stats, S),
    S#{ stats => St#{ K => maps:get(K, St) + 1 } }.

status_of(S) ->
    Segs = closed_segments(S),
    J = maps:get(journal, S),
    Oldest =
        case Segs of
            [] -> 0;
            [First | _] ->
                case prim_file:read_file_info(seg_file(J, First), [{time, posix}]) of
                    {ok, I} -> max(0, os:system_time(second) - element(6, I)) * 1000;
                    _ -> 0
                end
        end,
    St = maps:get(stats, S),
    St#{
        queued_records => counters:get(maps:get(counters, S), 1),
        dropped_records => counters:get(maps:get(counters, S), 2),
        local_backlog_bytes => maps:get(backlog, S),
        local_segments => length(Segs),
        lag_ms => Oldest,
        dropping => maps:get(dropping, S),
        ceiling => maps:get(ceiling, S),
        target_bytes => maps:get(target_bytes, S),
        last_audit => maps:get(last_audit, S, undefined),
        resync_pending => maps:get(resync, S),
        catchup_running => maps:get(catchup, S) =/= undefined,
        open_gaps => [ Id || {Id, _} <- maps:get(open_gaps, S) ],
        next_segment => maps:get(seq, S),
        shipping => maps:get(worker_busy, S),
        compression_ratio =>
            case maps:get(shipped_bytes, St) of
                0 -> undefined;
                B -> maps:get(raw_bytes, St) / B
            end
    }.

%% Closed local segments, oldest first: every one but the segment being written.
closed_segments(#{ journal := J, seq := Seq, fd := Fd }) ->
    [ Q || Q <- local_segments(J), Fd == undefined orelse Q < Seq ].

local_segments(J) -> segs_in(J, "seg-", ".log").
remote_segments(undefined) -> [];
remote_segments(P) -> segs_in(P, "seg-", ".log").
remote_bases(undefined) -> [];
remote_bases(P) -> segs_in(P, "base-", ".done").
remote_gaps(undefined) -> [];
remote_gaps(P) -> segs_in(P, "gap-", ".mark").

segs_in(Dir, Prefix, Suffix) ->
    case prim_file:list_dir(Dir) of
        {ok, Names} ->
            lists:sort(
                [ list_to_integer(lists:sublist(N, length(Prefix) + 1, 12))
                || N <- Names,
                   lists:prefix(Prefix, N),
                   lists:suffix(Suffix, N),
                   length(N) == length(Prefix) + 12 + length(Suffix)
                ]);
        _ -> []
    end.

seq_str(Seq) -> lists:flatten(io_lib:format("~12..0B", [Seq])).
seg_name(Seq) -> "seg-" ++ seq_str(Seq) ++ ".log".
seg_file(J, Seq) -> filename:join(J, seg_name(Seq)).

%%% The target worker: every target file operation, one job at a time.

target_worker_start(Path) ->
    _ = (catch repair_manifest(Path)),
    target_worker(Path).

target_worker(Path) ->
    receive
        {job, Writer, Job} ->
            Result =
                try with_target(fun() -> job(Path, Job) end, wait)
                catch C:R -> {error, {C, R}}
                end,
            Writer ! {worker, Result},
            target_worker(Path)
    end.

%% The target's root is created only the first time it is used in this VM:
%% once seen, a missing root is an outage (an unmounted or renamed target),
%% and writing into a fresh directory in its place would ship files no restore
%% of the real target will see.
ensure_root(P) ->
    Key = {?MODULE, target_seen, P},
    case prim_file:read_file_info(P) of
        {ok, #file_info{ type = directory }} ->
            case persistent_term:get(Key, false) of
                true -> ok;
                false -> persistent_term:put(Key, true)
            end;
        {ok, _} -> {error, {target_not_a_directory, P}};
        {error, enoent} ->
            case persistent_term:get(Key, false) of
                true -> {error, {target_missing, P}};
                false ->
                    ok = raw_ensure_dir(filename:join(P, "x")),
                    persistent_term:put(Key, true)
            end;
        Err -> Err
    end.

job(Path, target_info) ->
    {target_info, dir_bytes(Path)};
job(_Path, {mark_pruned, P, Seq}) ->
    ok = ensure_root(P),
    case put_file(filename:join(P, ?PRUNED_MARK), integer_to_binary(Seq)) of
        ok -> marked_pruned;
        Err -> {error, Err}
    end;
job(_Path, {audit, P, OpenGaps}) ->
    {audited, audit(P, OpenGaps)};
job(P, {ship, J, Segs, Cfg, Used}) ->
    ok = ensure_root(P),
    ok = ship_gaps(J, P),
    ship_segments(J, P, Segs, Cfg, Used);
job(P, {base, Inner, Seq, Cfg, Used}) ->
    ok = ensure_root(P),
    case space_ok(P, Used + inner_bytes(Inner), Cfg) of
        ok ->
            {ok, Bytes, Maxes} = write_base(Inner, P, Seq),
            % Once the local store is pruned the export holds history the
            % base does not: it supersedes nothing.
            {Freed, Pruned} =
                case raw_is_file(filename:join(P, ?PRUNED_MARK)) of
                    true -> {0, 0};
                    false -> prune(P, Seq)
                end,
            {based, Bytes, Freed, Pruned, Maxes};
        Ceiling -> Ceiling
    end.

ship_gaps(J, P) ->
    lists:foldl(
        fun(Seq, ok) ->
                Name = "gap-" ++ seq_str(Seq) ++ ".mark",
                case put_file(filename:join(P, Name), <<>>) of
                    ok -> prim_file:delete(filename:join(J, Name));
                    Err -> Err
                end;
           (_, Err) -> Err
        end,
        ok,
        segs_in(J, "gap-", ".mark")).

%% Ship closed segments, oldest first, each compressed, while the target has
%% room. Stops at the first failure or the ceiling.
ship_segments(J, P, Segs, Cfg, Used) ->
    Compress = maps:get(<<"compress">>, Cfg, true) =/= false
        andalso maps:get(<<"compress">>, Cfg, true) =/= <<"false">>,
    Loop =
        fun L([], N, Raw, Bytes) -> {shipped, N, Raw, Bytes, false};
            L([Seq | Rest], N, Raw, Bytes) ->
                Src = seg_file(J, Seq),
                {ok, Bin} = prim_file:read_file(Src),
                Out = case Compress of true -> zlib:gzip(Bin); false -> Bin end,
                case space_ok(P, Used + Bytes + byte_size(Out), Cfg) of
                    ok ->
                        Name = seg_name(Seq),
                        case put_file(filename:join(P, Name), Out) of
                            ok ->
                                case append_manifest(P, {Name, byte_size(Out), erlang:crc32(Out)}) of
                                    ok ->
                                        ok = prim_file:delete(Src),
                                        L(Rest, N + 1, Raw + byte_size(Bin), Bytes + byte_size(Out));
                                    Err -> partial(N, Raw, Bytes, Err)
                                end;
                            Err -> partial(N, Raw, Bytes, Err)
                        end;
                    Why -> {shipped, N, Raw, Bytes, Why}
                end
        end,
    Loop(Segs, 0, 0, 0).

partial(0, _Raw, _Bytes, Err) -> Err;
partial(N, Raw, Bytes, {error, _} = Err) ->
    ?event(warning, {essentials_export_ship_failed_after, N, Err}),
    {shipped, N, Raw, Bytes, false}.

%% What a restore would refuse, found from one listing and the manifest:
%% a listed segment from the newest base on that is missing, a base without
%% its file, a manifest that cannot be read. Gaps still open are reported (a
%% restore made now would refuse them); gaps closed by the writer have their
%% fill journaled ahead of the close.
audit(P, OpenGaps) ->
    At = os:system_time(second),
    try
        {ok, Names} = prim_file:list_dir(P),
        Bases = lists:sort([ Q || N <- Names, {"base-", Q} <- [parse_seq(N)],
                                  lists:suffix(".done", N) ]),
        B = case prim_file:read_file(filename:join(P, ?PRUNED_MARK)) of
                {ok, MB} -> lists:max([0 | [ X || X <- Bases, X < binary_to_integer(MB) ]]);
                _ -> lists:max([0 | Bases])
            end,
        Manifest = manifest(P),
        Listed = [ Q || N <- maps:keys(Manifest), {"seg-", Q} <- [parse_seq(N)], Q >= B ],
        Present = [ Q || N <- Names, {"seg-", Q} <- [parse_seq(N)], lists:suffix(".log", N) ],
        Missing = Listed -- Present,
        BaseOk = B == 0 orelse lists:member("base-" ++ seq_str(B) ++ ".log", Names),
        RemoteGaps = [ Q || N <- Names, {"gap-", Q} <- [parse_seq(N)], Q > B ],
        Pending = [ G || G <- RemoteGaps, lists:member(G, OpenGaps) ],
        #{ at => At, ok => Missing == [] andalso BaseOk andalso Pending == [],
           base => B, segments => length(Listed), missing_segments => Missing,
           base_present => BaseOk, open_gaps_on_target => Pending }
    catch C:R -> #{ at => At, ok => false, error => {C, R} }
    end.

%% @doc The deep check, on demand: restore the export into a scratch store
%% and compare its assignments with the local essentials store's. Returns
%% `{ok, Counts}' or `{error, Reason}'. Reads the whole export: run it off
%% peak, or against a copy.
verify_restorable(Store) -> verify_restorable(Store, #{}).
verify_restorable(Store, RestoreOpts) ->
    Path = hb_util:list(cfg(Store, <<"path">>, undefined)),
    Scratch = hb_util:bin(journal_dir(Store) ++ "/verify-" ++
                          integer_to_list(erlang:unique_integer([positive]))),
    Target = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => Scratch },
    try restore(Path, Target, RestoreOpts) of
        {ok, R} ->
            #{ <<"db">> := DB } = hb_store:find(inner(Store)),
            ok = elmdb:flush(DB),
            Local = lists:sum([ length(children(DB, <<?SCHED_ASSIGN, P/binary>>))
                              || P <- children(DB, <<"~scheduler@1.0/assignments">>) ]),
            case maps:get(assignments, R) of
                Local -> {ok, #{ assignments => Local }};
                Got -> {error, {assignments, Got, local, Local}}
            end
    catch C:E -> {error, {C, E}}
    after
        catch hb_store:stop([Target], #{}, #{}),
        os:cmd("rm -rf " ++ hb_util:list(Scratch))
    end.

%% Write a file durably and atomically: temp file, sync, rename.
put_file(RawDst, Bin) ->
    Dst = hb_util:list(RawDst),
    Tmp = Dst ++ ".tmp",
    case file:open(Tmp, [write, raw, binary]) of
        {ok, Fd} ->
            Res = case file:write(Fd, Bin) of ok -> file:sync(Fd); E -> E end,
            _ = file:close(Fd),
            case Res of
                ok -> prim_file:rename(Tmp, Dst);
                Err -> Err
            end;
        Err -> Err
    end.

%% @doc Run `Fun' holding one of the process-wide target-operation slots
%% (`target-ops', default 2): the export's worker waits for one (`wait'); the
%% checkpoint archive takes one only if free (`try_once') and otherwise returns
%% `{error, target_busy}'. A stalled network mount can thus hold at most this
%% many dirty I/O scheduler threads, never starve the ones LMDB and every file
%% read need.
with_target(Fun, Mode) ->
    ensure_gate(),
    Acquire =
        fun A() ->
            case try_acquire() of
                ok -> ok;
                busy when Mode == wait -> timer:sleep(50), A();
                busy -> busy
            end
        end,
    case Acquire() of
        busy -> {error, target_busy};
        ok -> try Fun() after ets:delete(?GATE, self()) end
    end.

%% A slot is a row keyed by the holder's pid, so a holder killed mid-operation
%% (its writer crashed, a quiesce) cannot leak it: dead holders' rows are
%% reclaimed whenever the gate is full.
try_acquire() ->
    case ets:info(?GATE, size) < ?DEFAULT_TARGET_OPS of
        true ->
            ets:insert(?GATE, {self()}),
            case ets:info(?GATE, size) =< ?DEFAULT_TARGET_OPS of
                true -> ok;
                false -> ets:delete(?GATE, self()), busy
            end;
        false ->
            Dead = [ P || {P} <- ets:tab2list(?GATE), not is_process_alive(P) ],
            case Dead of
                [] -> busy;
                _ -> [ ets:delete(?GATE, P) || P <- Dead ], try_acquire()
            end
    end.

ensure_gate() ->
    case ets:whereis(?GATE) of
        undefined ->
            ensure_registry(),
            Parent = self(),
            Ref = make_ref(),
            {Owner, Mon} =
                spawn_monitor(fun() ->
                    try ets:new(?GATE, [named_table, public, set]) of
                        _ -> Parent ! {Ref, ok}, receive after infinity -> ok end
                    catch error:badarg -> Parent ! {Ref, exists}
                    end
                end),
            receive {Ref, _} -> ok; {'DOWN', Mon, process, Owner, _} -> ok after 5000 -> ok end,
            erlang:demonitor(Mon, [flush]),
            ok;
        _ -> ok
    end.

inner_bytes(#{ <<"name">> := Name }) ->
    max(0, raw_size(filename:join(hb_util:list(Name), "data.mdb")));
inner_bytes(_) -> 0.

%% @doc May `Path' take more data so that this exporter's files total `Bytes'?
%% Checks the byte budget (`max-bytes') against `Bytes', and the filesystem
%% fill (`max-fill-pct', default 75). `stat-fun' (a fun of the path returning
%% `{TotalBytes, AvailBytes}') replaces `df' -- for tests.
space_ok(Path, Bytes, Cfg) ->
    Budget = maps:get(<<"max-bytes">>, Cfg, undefined),
    MaxPct = maps:get(<<"max-fill-pct">>, Cfg, ?DEFAULT_MAX_FILL_PCT),
    case is_integer(Budget) andalso Bytes > Budget of
        true -> {ceiling, {budget, Bytes, Budget}};
        false ->
            case fs_stat(Path, Cfg) of
                {Total, Avail} when is_integer(Total), Total > 0 ->
                    Pct = ((Total - Avail) * 100) div Total,
                    case Pct >= MaxPct of
                        true -> {ceiling, {fill, Pct, MaxPct}};
                        false -> ok
                    end;
                _ -> ok
            end
    end.

fs_stat(Path, Cfg) ->
    case maps:get(<<"stat-fun">>, Cfg, undefined) of
        F when is_function(F, 1) -> F(Path);
        _ ->
            Dir = existing_parent(hb_util:list(Path)),
            try
                Out = os:cmd("df -Pk '" ++ Dir ++ "' 2>/dev/null"),
                case string:tokens(Out, "\n") of
                    [_Header, Line | _] ->
                        case string:tokens(Line, " ") of
                            [_FS, Total, _Used, Avail | _] ->
                                {list_to_integer(Total) * 1024, list_to_integer(Avail) * 1024};
                            _ -> unknown
                        end;
                    _ -> unknown
                end
            catch _:_ -> unknown
            end
    end.

existing_parent(Dir) ->
    case raw_is_dir(Dir) of
        true -> Dir;
        false ->
            case filename:dirname(Dir) of
                Dir -> Dir;
                Parent -> existing_parent(Parent)
            end
    end.

%% @doc Total size of the files directly in `Dir'. Run by the target worker
%% only.
dir_bytes(undefined) -> 0;
dir_bytes(Dir) ->
    case prim_file:list_dir(Dir) of
        {ok, Names} -> lists:sum([ max(0, raw_size(filename:join(Dir, N))) || N <- Names ]);
        _ -> 0
    end.

%% @doc Append one framed `{File, Bytes, Crc32}' record to `Dir/manifest.log'
%% and sync it.
append_manifest(Dir, Entry) ->
    File = filename:join(Dir, "manifest.log"),
    case file:open(File, [append, raw, binary]) of
        {ok, Fd} ->
            Res = case file:write(Fd, frame(Entry)) of ok -> file:sync(Fd); E -> E end,
            _ = file:close(Fd),
            Res;
        Err -> Err
    end.

%% A torn final frame of `manifest.log' (a soft mount gave up mid-append) is
%% cut off, so later appends follow a valid frame.
repair_manifest(undefined) -> ok;
repair_manifest(Path) ->
    File = filename:join(Path, "manifest.log"),
    case prim_file:read_file(File) of
        {ok, Bin} ->
            Good = valid_prefix(Bin, 0),
            case Good == byte_size(Bin) of
                true -> ok;
                false ->
                    ?event(warning, {essentials_export_manifest_repaired,
                                     {kept, Good}, {was, byte_size(Bin)}}),
                    put_file(File, binary:part(Bin, 0, Good))
            end;
        _ -> ok
    end.

%% @doc A full image of the inner store: frames of compressed raw rows,
%% streamed in bounded chunks to `.tmp', CRC'd as written -- never read back
%% -- then synced, renamed, listed in the manifest and marked `.done'.
write_base(Inner = #{ <<"store-module">> := hb_store_lmdb }, P, Seq) ->
    #{ <<"db">> := DB } = hb_store:find(Inner),
    ok = elmdb:flush(DB),
    % The base is a key-order scan over many short read transactions, not a
    % snapshot: a message written during it can sort before the cursor while
    % its assignment link (`~scheduler@1.0/...' sorts last) is still ahead,
    % so the image can hold a link without its message. The watermarks it may
    % raise are therefore the assignments present BEFORE the scan began --
    % whole closures, which the scan cannot miss -- never what it saw.
    PreMaxes =
        lists:foldl(
            fun(Pr, M) ->
                case [ Sl || C <- children(DB, <<?SCHED_ASSIGN, Pr/binary>>),
                             Sl <- [catch binary_to_integer(C)], is_integer(Sl) ] of
                    [] -> M;
                    Sls -> M#{ Pr => lists:max(Sls) }
                end
            end,
            #{},
            children(DB, <<"~scheduler@1.0/assignments">>)),
    Name = "base-" ++ seq_str(Seq) ++ ".log",
    File = filename:join(P, Name),
    {ok, Fd} = file:open(File ++ ".tmp", [write, raw, binary]),
    % Also the highest assignment slot per process in the image: everything up
    % to it is in the base, so the writer's watermarks can be raised to it.
    Loop =
        fun L(From, Crc, Size, Maxes) ->
            case elmdb:scan_rows(DB, From, ?SCAN_ROWS) of
                {ok, Rows, _N, Next} ->
                    {Crc1, Size1} =
                        case Rows of
                            [] -> {Crc, Size};
                            _ ->
                                F = frame_compressed({raw, Rows}),
                                ok = file:write(Fd, F),
                                {erlang:crc32(Crc, F), Size + byte_size(F)}
                        end,
                    Maxes2 = Maxes,
                    case Next of
                        done -> {Crc1, Size1, Maxes2};
                        _ -> L(Next, Crc1, Size1, Maxes2)
                    end;
                {error, T, D} -> throw({scan_failed, T, D})
            end
        end,
    {Crc, Size, BaseMaxes} =
        try
            R = Loop(<<>>, erlang:crc32(<<>>), 0, PreMaxes),
            ok = file:sync(Fd),
            R
        after file:close(Fd)
        end,
    ok = prim_file:rename(File ++ ".tmp", File),
    ok = append_manifest(P, {Name, Size, Crc}),
    ok = put_file(filename:join(P, "base-" ++ seq_str(Seq) ++ ".done"), <<>>),
    {ok, Size, BaseMaxes};
write_base(Inner, _P, _Seq) ->
    erlang:error({base_needs_lmdb_inner, Inner}).



%% @doc Once base `Seq' is complete -- renamed into place, its manifest entry
%% synced and its `.done' written, all before this runs -- delete what it
%% supersedes: older bases, every segment below it, and the gap marks it
%% covers. One complete chain remains: the newest base and the segments after
%% it. Returns `{BytesFreed, FilesDeleted}'.
prune(P, Seq) ->
    {ok, Names} = prim_file:list_dir(P),
    Old =
        [ N || N <- Names,
               case parse_seq(N) of
                   {"seg-", Q} -> Q < Seq;
                   {"base-", Q} -> Q < Seq;
                   {"gap-", Q} -> Q =< Seq;
                   _ -> false
               end ],
    lists:foldl(
        fun(N, {B, C}) ->
            F = filename:join(P, N),
            Size = max(0, raw_size(F)),
            case prim_file:delete(F) of ok -> {B + Size, C + 1}; _ -> {B, C} end
        end,
        {0, 0},
        Old).

parse_seq(Name) ->
    case [ Pre || Pre <- ["seg-", "base-", "gap-"], lists:prefix(Pre, Name) ] of
        [Pre] when length(Name) >= length(Pre) + 12 ->
            Digits = lists:sublist(Name, length(Pre) + 1, 12),
            case lists:all(fun(C) -> C >= $0 andalso C =< $9 end, Digits) of
                true -> {Pre, list_to_integer(Digits)};
                false -> none
            end;
        _ -> none
    end.

%%% Raw file operations.
%%%
%%% `file:read_file/1', `file:list_dir/1', `file:delete/1', `filelib:*' and
%%% the rest of the plain `file' API are served by the VM's ONE file server
%%% process (`file_server_2'). A single such call that hangs -- a read on a
%%% stalled soft NFS mount waits 45 s per operation -- blocks every file
%%% operation of every process in the node behind it: the `hb_store_fs'
%%% store a read falls through to, code loading, everything. That is what
%%% froze the stage node for 82-148 s while a base image was finalised (the
%%% export read the whole base back with `file:read_file/1' to CRC it). This
%%% module therefore uses only `prim_file' and raw file handles, which run in
%%% the calling process.

raw_write_file(File, Bin) ->
    case file:open(File, [write, raw, binary]) of
        {ok, Fd} ->
            R = file:write(Fd, Bin),
            _ = file:close(Fd),
            R;
        Err -> Err
    end.

raw_size(File) ->
    case prim_file:read_file_info(File) of
        {ok, #file_info{ size = Size }} -> Size;
        _ -> 0
    end.

raw_is_file(File) ->
    case prim_file:read_file_info(File) of
        {ok, #file_info{ type = regular }} -> true;
        _ -> false
    end.

raw_is_dir(Dir) ->
    case prim_file:read_file_info(Dir) of
        {ok, #file_info{ type = directory }} -> true;
        _ -> false
    end.

%% Like `filelib:ensure_dir/1': make every parent directory of `Path'.
raw_ensure_dir(Path) ->
    Dir = filename:dirname(Path),
    case raw_is_dir(Dir) of
        true -> ok;
        false ->
            ok = raw_ensure_dir(Dir),
            case prim_file:make_dir(Dir) of
                ok -> ok;
                {error, eexist} -> ok;
                Err -> Err
            end
    end.

%%% Restore.

%% @doc Rebuild a store from an export directory: the newest complete base
%% image, then every segment numbered above it, in order. Fails, loudly,
%% rather than return an incomplete store:
%% <ul>
%%   <li>every file applied must be in the manifest with its size and CRC;</li>
%%   <li>every segment the manifest lists from the base on must be present:
%%       no shipped segment can be missing;</li>
%%   <li>every gap opened after the base must be named by a
%%       `{gaps_filled, Ids}' record -- exactly, never by a later fill;</li>
%%   <li>afterwards, every process's assignment slots must be contiguous
%%       from slot 0 and every assignment must load whole;</li>
%%   <li>every process with assignments must have its definition.</li>
%% </ul>
%% A torn final record (a segment cut short by a crash) is ignored.
%%
%% Two options relax the last two checks, for exports that legitimately
%% hold partial processes -- one started from a migration of a node whose
%% local slots began above 0, or whose definitions live only elsewhere:
%% `allow-partial-processes' (true: contiguity is checked from each process's
%% lowest exported slot, not from 0) and `require-definitions' (false: a
%% missing definition is counted in `processes_without_definition' instead of
%% failing). Use them only when the gap is known and expected: they also hide
%% a lost prefix or a lost definition.
restore(Path, Target) -> restore(Path, Target, #{}).
restore(RawPath, Target, Opts) ->
    Path = hb_util:list(RawPath),
    ok = hb_store:start([Target], #{}, Opts),
    Bases = remote_bases(Path),
    Segs = remote_segments(Path),
    Gaps = remote_gaps(Path),
    Manifest = manifest(Path),
    % `local-pruned' holds the first id after which bases superseded nothing.
    History =
        case prim_file:read_file(filename:join(Path, ?PRUNED_MARK)) of
            {ok, MB} -> binary_to_integer(MB);
            _ -> false
        end,
    {From, BaseRows} =
        case Bases of
            [] -> {0, 0};
            % History kept: replay from the newest base before the marker
            % (it superseded the files before it), or from the start.
            _ when is_integer(History) ->
                {lists:max([0 | [ B || B <- Bases, B < History ]]), 0};
            _ ->
                B = lists:max(Bases),
                {ok, BaseN, _} =
                    apply_file(filename:join(Path, "base-" ++ seq_str(B) ++ ".log"),
                               Manifest, Target, Opts),
                {B, BaseN}
        end,
    From1 = From,
    Wanted = [ Q || Q <- Segs, Q >= From1 ],
    % A gap is closed by a fill recorded in a later segment, or by a base
    % with a higher id.
    LiveGaps = [ G || G <- Gaps, not lists:any(fun(B) -> B >= G end, Bases) ],
    % Every segment the manifest lists at or after the base must be here: a
    % shipped segment that went missing (or a torn manifest's lost tail, whose
    % files are then unlisted, which `apply_file' refuses) fails the restore.
    % Ids need not be contiguous: a writer killed between reserving an id and
    % using it (a base it never finished) leaves one unused.
    Listed = lists:usort([ Q || N <- maps:keys(Manifest),
                                {"seg-", Q} <- [parse_seq(N)], Q >= From1 ]),
    case Listed -- Wanted of
        [] -> ok;
        Lost -> erlang:error({export_segments_missing, lists:sublist(Lost, 10)})
    end,
    % With history kept (the local store was pruned) every base is replayed
    % in its place in the chain, between the segments around it.
    Files = lists:keysort(1, [ {Q, seg_name(Q)} || Q <- Wanted ] ++
                             [ {B, "base-" ++ seq_str(B) ++ ".log"}
                             || is_integer(History), B <- Bases, B >= From1 ]),
    {Records, Filled} =
        lists:foldl(
            fun({_, Name}, {N, F}) ->
                {ok, N1, F1} = apply_file(filename:join(Path, Name),
                                          Manifest, Target, Opts),
                {N + N1, F1 ++ F}
            end,
            {0, []},
            Files),
    case [ G || G <- LiveGaps, not lists:member(G, Filled) ] of
        [] -> ok;
        Uncovered -> erlang:error({export_gap_not_covered, Uncovered, From1})
    end,
    ok = hb_store:sync([Target], #{}, Opts),
    Assignments = verify_assignments(Target, Opts),
    NoDefinition = verify_definitions(Target, Opts),
    {ok, #{ base => From1, base_records => BaseRows,
            segments => length(Wanted), records => Records,
            gaps_filled => lists:usort(Filled), assignments => Assignments,
            processes_without_definition => NoDefinition }}.

%% Every process with assignments must have its definition (the message at
%% its ID), unless the caller allows it (`require-definitions' false), in which
%% case the processes without one are returned.
verify_definitions(Target, Opts) ->
    TOpts = Opts#{ <<"store">> => [Target] },
    #{ <<"db">> := DB } = hb_store:find(Target),
    Missing =
        [ P || P <- children(DB, <<"~scheduler@1.0/assignments">>),
               not is_map(catch hb_cache:ensure_all_loaded(hb_util:ok(hb_cache:read(P, TOpts)), TOpts)) ],
    case {Missing, maps:get(<<"require-definitions">>, Opts, true)} of
        {[], _} -> 0;
        {_, false} -> length(Missing);
        _ -> erlang:error({export_definitions_missing, lists:sublist(Missing, 10)})
    end.

%% Every process's assignment slots contiguous, every assignment loadable.
verify_assignments(Target, Opts) ->
    TOpts = Opts#{ <<"store">> => [Target] },
    #{ <<"db">> := DB } = hb_store:find(Target),
    Procs = children(DB, <<"~scheduler@1.0/assignments">>),
    lists:sum(
        [ begin
            Slots = lists:sort([ Sl || C <- children(DB, <<?SCHED_ASSIGN, P/binary>>),
                                       Sl <- [catch binary_to_integer(C)], is_integer(Sl) ]),
            % A scheduler numbers a process's assignments from 0. Only when the
            % caller says the export holds processes it never saw begin
            % (`allow-partial-processes') may they start later.
            First = case maps:get(<<"allow-partial-processes">>, Opts, false) of
                        true -> hd(Slots ++ [0]);
                        _ -> 0
                    end,
            case Slots of
                [] -> ok;
                _ ->
                    Expected = lists:seq(First, lists:last(Slots)),
                    case Slots == Expected of
                        true -> ok;
                        false -> erlang:error({export_assignments_not_contiguous, P,
                                               lists:sublist(Expected -- Slots, 10)})
                    end
            end,
            [ case catch hb_cache:ensure_all_loaded(
                    hb_util:ok(hb_cache:read(<<?SCHED_ASSIGN, P/binary, "/",
                                               (integer_to_binary(Sl))/binary>>, TOpts)), TOpts) of
                  M when is_map(M) -> ok;
                  Other -> erlang:error({export_assignment_unreadable, P, Sl, Other})
              end
            || Sl <- Slots ],
            length(Slots)
          end
        || P <- Procs ]).

manifest(Path) ->
    case prim_file:read_file(filename:join(Path, "manifest.log")) of
        {ok, M} ->
            lists:foldl(fun({N, Size, Crc}, Acc) -> Acc#{ N => {Size, Crc} } end,
                        #{}, records(binary:part(M, 0, valid_prefix(M, 0))));
        _ -> erlang:error({export_manifest_missing, Path})
    end.

apply_file(File, Manifest, Target, Opts) ->
    {ok, Bin0} = prim_file:read_file(File),
    Name = filename:basename(File),
    case maps:get(Name, Manifest, undefined) of
        undefined -> erlang:error({export_file_not_in_manifest, Name});
        {Size, Crc} when Size == byte_size(Bin0) ->
            case erlang:crc32(Bin0) of
                Crc -> ok;
                _ -> erlang:error({export_file_corrupt, Name})
            end;
        _ -> erlang:error({export_file_size_mismatch, Name})
    end,
    Bin = case Bin0 of <<31, 139, _/binary>> -> zlib:gunzip(Bin0); _ -> Bin0 end,
    apply_records(Bin, Target, Opts, 0, []).

apply_records(<<Len:32, Crc:32, Bin:Len/binary, Rest/binary>>, Target, Opts, N, Filled) ->
    case erlang:crc32(Bin) of
        Crc ->
            case binary_to_term(Bin) of
                {gaps_filled, Gs} -> apply_records(Rest, Target, Opts, N, Gs ++ Filled);
                Rec ->
                    apply_record(Rec, Target, Opts),
                    apply_records(Rest, Target, Opts, N + 1, Filled)
            end;
        _ -> erlang:error({corrupt_export_record, N})
    end;
apply_records(_Torn, _Target, _Opts, N, Filled) ->
    {ok, N, Filled}.

apply_record({write, Req}, T, Opts) -> ok = hb_store:write([T], Req, Opts);
apply_record({link, Req}, T, Opts) -> ok = hb_store:link([T], Req, Opts);
apply_record({group, Req}, T, Opts) -> ok = hb_store:group([T], Req, Opts);
apply_record({raw, Rows}, T, Opts) -> ok = hb_store:write([T], maps:from_list(Rows), Opts).

%%% Tests

export_test_dir(Name) ->
    Dir = "cache-TEST/export-" ++ Name ++ "-" ++ integer_to_list(erlang:unique_integer([positive])),
    os:cmd("rm -rf " ++ Dir),
    Dir.

export_store(Dir, ExportOpts) ->
    Inner = #{ <<"store-module">> => hb_store_lmdb,
               <<"name">> => hb_util:bin(Dir ++ "/lmdb") },
    wrap(Inner, ExportOpts#{ <<"path">> => hb_util:bin(Dir ++ "/remote"),
                              <<"journal">> => hb_util:bin(Dir ++ "/journal") }).

%% @doc Messages written through the wrapper are readable locally, shipped as
%% segments, and a store restored from the export reads them back identically.
export_and_restore_roundtrip_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("rt"),
        Store = export_store(Dir, #{ <<"segment-ms">> => 100, <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store], <<"priv-wallet">> => ar_wallet:new() },
        ok = hb_store:start([Store], #{}, Opts),
        Msgs = [ hb_message:commit(#{ <<"n">> => integer_to_binary(N),
                                       <<"data">> => crypto:strong_rand_bytes(200) }, Opts)
               || N <- lists:seq(1, 50) ],
        IDs = [ begin {ok, _} = hb_cache:write(M, Opts), hb_message:id(M, signed, Opts) end
              || M <- Msgs ],
        ok = hb_store:link([Store], #{ <<"~test@1.0/last">> => lists:last(IDs) }, Opts),
        St = sync_export(Store),
        ?assertEqual(0, maps:get(local_segments, St)),
        ?assert(maps:get(shipped_segments, St) >= 1),
        Target = #{ <<"store-module">> => hb_store_lmdb,
                    <<"name">> => hb_util:bin(Dir ++ "/restored") },
        {ok, R} = restore(Dir ++ "/remote", Target, maps:merge(Opts, ?LAX)),
        ?assert(maps:get(records, R) + maps:get(base_records, R) > 0),
        TOpts = Opts#{ <<"store">> => [Target] },
        lists:foreach(
            fun({ID, M}) ->
                {ok, A} = hb_cache:read(ID, Opts),
                {ok, B} = hb_cache:read(ID, TOpts),
                ?assertEqual(hb_cache:ensure_all_loaded(A, Opts),
                             hb_cache:ensure_all_loaded(B, TOpts)),
                ?assert(hb_message:match(M, hb_cache:ensure_all_loaded(B, TOpts), primary, TOpts))
            end,
            lists:zip(IDs, Msgs)
        ),
        {ok, Last} = hb_cache:read(<<"~test@1.0/last">>, TOpts),
        ?assert(is_map(Last))
    end}.

%% @doc At the fill ceiling (here a mocked filesystem at 80% against the
%% default 75%) nothing is shipped and nothing is lost: the backlog grows, the
%% status says so, and when space appears the export resumes by itself and a
%% restore is complete. A byte budget is enforced the same way.
export_pauses_at_fill_ceiling_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("ceiling"),
        Key = {?MODULE, fill, Dir},
        persistent_term:put(Key, {1000, 200}),
        Stat = fun(_Path) -> persistent_term:get(Key) end,
        Store = export_store(Dir, #{ <<"segment-ms">> => 50,
                                     <<"ship-interval-ms">> => 50,
                                     <<"stat-fun">> => Stat }),
        Opts = #{ <<"store">> => [Store], <<"priv-wallet">> => ar_wallet:new() },
        ok = hb_store:start([Store], #{}, Opts),
        IDs = [ begin {ok, ID} = hb_cache:write(#{ <<"n">> => integer_to_binary(N),
                    <<"data">> => crypto:strong_rand_bytes(300) }, Opts), ID end
              || N <- lists:seq(1, 100) ],
        timer:sleep(500),
        St1 = sync_export(Store),
        ?assert(maps:get(ceiling, St1)),
        ?assert(maps:get(ceiling_hits, St1) >= 1),
        ?assertEqual(0, maps:get(shipped_segments, St1)),
        ?assert(maps:get(local_backlog_bytes, St1) > 0),
        ?assertEqual(0, maps:get(dropped_records, St1)),
        ?assertNot(filelib:is_file(Dir ++ "/remote/manifest.log")),
        % Space appears: the export resumes without being asked.
        persistent_term:put(Key, {1000, 900}),
        ?assert(hb_util:wait_until(fun() -> not maps:get(ceiling, status(Store)) andalso
                                         maps:get(local_segments, status(Store)) == 0 end, 10000)),
        St2 = sync_export(Store),
        ?assert(maps:get(shipped_segments, St2) >= 1),
        Target = #{ <<"store-module">> => hb_store_lmdb,
                    <<"name">> => hb_util:bin(Dir ++ "/restored") },
        {ok, _} = restore(Dir ++ "/remote", Target, maps:merge(Opts, ?LAX)),
        TOpts = Opts#{ <<"store">> => [Target] },
        [ ?assertMatch({ok, _}, hb_cache:read(ID, TOpts)) || ID <- IDs ],
        % A byte budget already exceeded pauses it again.
        ?assertMatch({ceiling, {budget, _, 10}},
            space_ok(Dir ++ "/remote", 11, #{ <<"max-bytes">> => 10, <<"stat-fun">> => Stat })),
        ?assertMatch({ceiling, {fill, 80, 75}},
            space_ok("x", 0, #{ <<"stat-fun">> => fun(_) -> {100, 20} end })),
        ?assertEqual(ok, space_ok("x", 0, #{ <<"stat-fun">> => fun(_) -> {100, 26} end }))
    end}.


%% @doc Integration against a real (network) mount: `HB_EXPORT_REAL_DIR=<dir>'.
%% Writes `HB_EXPORT_REAL_N' messages, ships them as a few MB-sized segments,
%% restores from the mount, verifies every message, and prints the timings.
export_real_mount_test_() ->
    {timeout, 1800, fun() ->
        case os:getenv("HB_EXPORT_REAL_DIR") of
            false -> ok;
            Real ->
                application:ensure_all_started(hb),
                N = list_to_integer(os:getenv("HB_EXPORT_REAL_N", "3000")),
                Local = export_test_dir("real"),
                Inner = #{ <<"store-module">> => hb_store_lmdb,
                           <<"name">> => hb_util:bin(Local ++ "/lmdb") },
                Store = wrap(Inner, #{ <<"path">> => hb_util:bin(Real ++ "/essentials"),
                                       <<"journal">> => hb_util:bin(Local ++ "/journal"),
                                       <<"segment-bytes">> => 2 * 1024 * 1024,
                                       <<"segment-ms">> => 3600000 }),
                Opts = #{ <<"store">> => [Store], <<"priv-wallet">> => ar_wallet:new() },
                ok = hb_store:start([Store], #{}, Opts),
                {WriteUs, IDs} =
                    timer:tc(fun() ->
                        [ begin
                            {ok, ID} = hb_cache:write(#{ <<"n">> => integer_to_binary(I),
                                <<"data">> => hb_util:encode(crypto:strong_rand_bytes(1500)) }, Opts),
                            ID
                          end
                        || I <- lists:seq(1, N) ]
                    end),
                {SyncUs, St} = timer:tc(fun() -> sync_export(Store) end),
                Target = #{ <<"store-module">> => hb_store_lmdb,
                            <<"name">> => hb_util:bin(Local ++ "/restored") },
                {RestoreUs, {ok, R}} = timer:tc(fun() -> restore(Real ++ "/essentials", Target, maps:merge(Opts, ?LAX)) end),
                TOpts = Opts#{ <<"store">> => [Target] },
                Bad = [ ID || ID <- IDs,
                              hb_cache:ensure_all_loaded(element(2, hb_cache:read(ID, Opts)), Opts) =/=
                              hb_cache:ensure_all_loaded(element(2, hb_cache:read(ID, TOpts)), TOpts) ],
                ?assertEqual([], Bad),
                {ok, Files} = file:list_dir(Real ++ "/essentials"),
                io:format(user,
                    "~nEXPORT_REAL messages=~p write_ms=~p ship_ms=~p restore_ms=~p "
                    "shipped_bytes=~p segments=~p files=~p restore=~0p ceiling=~p errors=~p~n",
                    [N, WriteUs div 1000, SyncUs div 1000, RestoreUs div 1000,
                     maps:get(shipped_bytes, St), maps:get(shipped_segments, St),
                     length(Files), R, maps:get(ceiling, St), maps:get(errors, St)])
        end
    end}.

%% @doc A torn manifest tail is cut back on start; a shipped file that the
%% manifest does not list is a restore error, not silently trusted.
export_manifest_is_strict_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("manifest"),
        Remote = Dir ++ "/remote",
        Store = export_store(Dir, #{ <<"segment-ms">> => 100, <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        ok = hb_store:write([Store], #{ <<"a">> => <<"1">> }, Opts),
        _ = sync_export(Store),
        {ok, M0} = file:read_file(Remote ++ "/manifest.log"),
        ok = file:write_file(Remote ++ "/manifest.log", <<M0/binary, 0, 0, 1, 0, 7, 7>>),
        ok = repair_manifest(Remote),
        ?assertEqual({ok, M0}, file:read_file(Remote ++ "/manifest.log")),
        % An unlisted segment.
        ok = file:write_file(Remote ++ "/seg-" ++ seq_str(lists:max(remote_segments(Remote)) + 1) ++ ".log",
                             frame({write, #{ <<"b">> => <<"2">> }})),
        T = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => hb_util:bin(Dir ++ "/restored") },
        ?assertError({export_file_not_in_manifest, _}, restore(Remote, T, ?LAX))
    end}.

%% An assignment-shaped essential: a message, and the scheduler's link to it.
assign(Store, Opts, P, N) ->
    {ok, ID} = hb_cache:write(#{ <<"n">> => integer_to_binary(N),
                                 <<"data">> => crypto:strong_rand_bytes(300) }, Opts),
    ok = hb_store:link([Store],
        #{ <<?SCHED_ASSIGN, P/binary, "/", (integer_to_binary(N))/binary>> => ID }, Opts),
    ID.

assignment_bytes(Opts, P, N) ->
    Path = <<?SCHED_ASSIGN, P/binary, "/", (integer_to_binary(N))/binary>>,
    case hb_cache:read(Path, Opts) of
        {ok, M} -> term_to_binary(hb_cache:ensure_all_loaded(M, Opts));
        Other -> {missing, Other}
    end.

writer_pid(Store) ->
    [{_, Pid, _}] = ets:lookup(?REG, maps:get(<<"name">>, Store)),
    Pid.

restored(Dir, Name) ->
    #{ <<"store-module">> => hb_store_lmdb, <<"name">> => hb_util:bin(Dir ++ "/" ++ Name) }.

%% @doc A writer that dies with records in its mailbox (deterministic here:
%% suspended, written to, then killed) loses them. The restart finds the
%% unclean stop and closes the gap by catching up from its watermarks -- no
%% base image -- and a restore has every assignment. An uncovered gap is
%% refused.
export_unclean_stop_catches_up_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("unclean"),
        Store = export_store(Dir, #{ <<"segment-ms">> => 100000, <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        [ assign(Store, Opts, P, N) || N <- lists:seq(1, 50) ],
        _ = sync_export(Store),
        Pid = writer_pid(Store),
        erlang:suspend_process(Pid),
        [ assign(Store, Opts, P, N) || N <- lists:seq(51, 100) ],
        exit(Pid, kill),
        timer:sleep(20),
        [ assign(Store, Opts, P, N) || N <- lists:seq(101, 110) ],
        St = sync_export(Store),
        ?assertEqual(0, maps:get(resyncs, St)),
        ?assert(maps:get(catchups, St) >= 1),
        Remote = Dir ++ "/remote",
        ?assertMatch([_ | _], remote_gaps(Remote)),
        ?assertEqual([], remote_bases(Remote)),
        T = restored(Dir, "restored"),
        {ok, R} = restore(Remote, T, ?LAX),
        ?assertMatch([_ | _], maps:get(gaps_filled, R)),
        TOpts = #{ <<"store">> => [T] },
        Bad = [ N || N <- lists:seq(1, 110),
                     assignment_bytes(Opts, P, N) =/= assignment_bytes(TOpts, P, N) ],
        ?assertEqual([], Bad),
        % A gap newer than any fill or base: refused.
        ok = file:write_file(Remote ++ "/gap-" ++ seq_str(999999999999) ++ ".mark", <<>>),
        ?assertError(_, restore(Remote, restored(Dir, "r2"), ?LAX))
    end}.

%% @doc A clean stop (what the node's application stop does on `docker stop')
%% leaves nothing to repair: the next start writes no gap, no catch-up, no base.
export_clean_stop_needs_nothing_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("clean"),
        Store = export_store(Dir, #{ <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        [ assign(Store, Opts, P, N) || N <- lists:seq(1, 20) ],
        ok = shutdown_all(),
        Pid = writer_pid(Store),
        exit(Pid, kill),
        timer:sleep(20),
        [ assign(Store, Opts, P, N) || N <- lists:seq(21, 30) ],
        St = sync_export(Store),
        ?assertEqual(0, maps:get(catchups, St)),
        ?assertEqual(0, maps:get(resyncs, St)),
        ?assertEqual([], remote_gaps(Dir ++ "/remote")),
        {ok, _} = restore(Dir ++ "/remote", restored(Dir, "restored"), ?LAX),
        TOpts = #{ <<"store">> => [restored(Dir, "restored")] },
        ?assertEqual([], [ N || N <- lists:seq(1, 30),
                                assignment_bytes(Opts, P, N) =/= assignment_bytes(TOpts, P, N) ])
    end}.

%% @doc A completed base supersedes everything before it: exactly one complete
%% chain (the newest base and the segments after it) remains, and restores.
export_prunes_superseded_bases_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("prune"),
        Remote = Dir ++ "/remote",
        Store = export_store(Dir, #{ <<"segment-ms">> => 100, <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        lists:foreach(
            fun(Round) ->
                [ assign(Store, Opts, P, Round * 100 + N) || N <- lists:seq(1 - 80 * min(1, Round - 1), 20) ],
                _ = sync_export(Store),
                ok = resync(Store),
                St = sync_export(Store),
                ?assertEqual(Round, maps:get(resyncs, St))
            end,
            lists:seq(1, 3)),
        [ assign(Store, Opts, P, 320 + N) || N <- lists:seq(1, 5) ],
        _ = sync_export(Store),
        [B] = remote_bases(Remote),
        ?assertEqual([B], segs_in(Remote, "base-", ".log")),
        ?assert(lists:all(fun(Q) -> Q >= B end, remote_segments(Remote))),
        T = restored(Dir, "restored"),
        {ok, _} = restore(Remote, T, ?LAX),
        TOpts = #{ <<"store">> => [T] },
        All = lists:seq(101, 325),
        ?assertEqual([], [ N || N <- All, assignment_bytes(Opts, P, N) =/= assignment_bytes(TOpts, P, N) ])
    end}.

%% @doc A target that blocks indefinitely (its manifest is a FIFO with no
%% writer: the target worker's first read never returns, holding a dirty I/O
%% thread, as a stalled soft NFS mount does) never reaches a writer: writes and
%% `sync/3' stay as fast as with a healthy target. The first version blocked
%% the first writer for 30 s and every other writer behind it.
export_blocked_target_never_blocks_writers_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Measure =
            fun(Name, Prepare) ->
                Dir = export_test_dir(Name),
                ok = filelib:ensure_dir(Dir ++ "/remote/x"),
                Prepare(Dir ++ "/remote"),
                Store = export_store(Dir, #{ <<"ship-interval-ms">> => 50 }),
                Opts = #{ <<"store">> => [Store] },
                {StartUs, ok} = timer:tc(fun() -> hb_store:start([Store], #{}, Opts) end),
                P = hb_util:human_id(crypto:strong_rand_bytes(32)),
                Lats =
                    [ begin
                        {T, _} = timer:tc(fun() ->
                            assign(Store, Opts, P, N),
                            ok = hb_store:sync([Store], #{}, Opts)
                        end),
                        T
                      end
                    || N <- lists:seq(1, 300) ],
                Sorted = lists:sort(Lats),
                {StartUs, lists:nth(297, Sorted), lists:last(Sorted), Store}
            end,
        {S0, P99Ok, MaxOk, _} = Measure("healthy", fun(_) -> ok end),
        {S1, P99Blocked, MaxBlocked, Blocked} =
            Measure("blocked", fun(R) -> "" = os:cmd("mkfifo " ++ R ++ "/manifest.log") end),
        io:format(user, "~nBLOCKED_TARGET start_us healthy=~p blocked=~p p99_us healthy=~p blocked=~p "
                        "max_us healthy=~p blocked=~p status=~0p~n",
                  [S0, S1, P99Ok, P99Blocked, MaxOk, MaxBlocked, status(Blocked)]),
        % The VM's file server stays free: an unrelated plain `file' call (what
        % `hb_store_fs' and code loading use) is not queued behind the stalled
        % target, which held it for minutes in the first version.
        {FsUs, {ok, _}} = timer:tc(fun() -> file:read_file("rebar.config") end),
        io:format(user, "BLOCKED_TARGET unrelated_file_read_us=~p~n", [FsUs]),
        ?assert(FsUs < 1000000),
        % Release the stuck worker for good: move the FIFO aside, put a real
        % manifest in its place, and open the FIFO read-write, which wakes any
        % open blocked on it (its next write to it fails, and is retried
        % against the real file). Otherwise it holds a dirty I/O thread for
        % the rest of the VM's life, which hangs anything that traces every
        % process (eprof).
        Fifo = hb_util:list(cfg(Blocked, <<"path">>, undefined)) ++ "/manifest.log",
        ok = file:rename(Fifo, Fifo ++ ".fifo"),
        ok = file:write_file(Fifo, <<>>),
        {ok, F} = file:open(Fifo ++ ".fifo", [read, write, raw]),
        timer:sleep(200),
        ok = file:close(F),
        {ok, F2} = file:open(Fifo ++ ".fifo", [read, write, raw]),
        timer:sleep(200),
        ok = file:close(F2),
        ?assert(S1 < 1000000),
        ?assert(MaxBlocked < 1000000),
        ?assert(P99Blocked < max(3 * P99Ok, 20000))
    end}.

%% @doc The confirm-path `sync/3' does not wait for a catch-up: the catch-up
%% sends its rows a small chunk at a time and waits for each to be journaled,
%% so a sync is served between chunks.
export_sync_not_slowed_by_catchup_test_() ->
    {timeout, 300, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("synccu"),
        Store = export_store(Dir, #{ <<"segment-ms">> => 100000, <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        Procs = [ hb_util:human_id(crypto:strong_rand_bytes(32)) || _ <- lists:seq(1, 8) ],
        [ assign(Store, Opts, P, 0) || P <- Procs ],
        _ = sync_export(Store),
        % Lose all that follows with the writer's mailbox: a large catch-up.
        Pid = writer_pid(Store),
        erlang:suspend_process(Pid),
        [ assign(Store, Opts, P, N) || P <- Procs, N <- lists:seq(1, 600) ],
        exit(Pid, kill),
        timer:sleep(20),
        P0 = hb_util:human_id(crypto:strong_rand_bytes(32)),
        Lats =
            [ begin
                assign(Store, Opts, P0, N),
                {T, ok} = timer:tc(fun() -> hb_store:sync([Store], #{}, Opts) end),
                Running = maps:get(catchup_running, status(Store)),
                {T, Running}
              end
            || N <- lists:seq(1, 200) ],
        During = lists:sort([ T || {T, true} <- Lats ]),
        io:format(user, "~nSYNC_DURING_CATCHUP n=~p max_us=~p~n",
                  [length(During), lists:last([0 | During])]),
        ?assert(length(During) > 0),
        ?assert(lists:last(During) < 500000),
        St = sync_export(Store),
        ?assertEqual([], maps:get(open_gaps, St))
    end}.

%% @doc Within a segment, a key written V1, V2, V1 restores V1 (dedupe keeps
%% the last write of each key), and a catch-up re-exports `~arweave@2.9'.
export_dedupe_keeps_last_write_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("lastwrite"),
        Store = export_store(Dir, #{ <<"segment-ms">> => 100000, <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        [ ok = hb_store:write([Store], #{ <<"~test@1.0/mutable">> => V }, Opts)
        || V <- [<<"v1">>, <<"v2">>, <<"v1">>] ],
        _ = sync_export(Store),
        Pid = writer_pid(Store),
        erlang:suspend_process(Pid),
        ok = hb_store:write([Store], #{ <<"~arweave@2.9/block/height/7">> => <<"blk">> }, Opts),
        exit(Pid, kill),
        timer:sleep(20),
        _ = sync_export(Store),
        T = restored(Dir, "restored"),
        {ok, _} = restore(Dir ++ "/remote", T, ?LAX),
        ?assertEqual({ok, <<"v1">>}, hb_store:read([T], <<"~test@1.0/mutable">>, #{})),
        ?assertEqual({ok, <<"blk">>}, hb_store:read([T], <<"~arweave@2.9/block/height/7">>, #{}))
    end}.

%% @doc A target-operation slot held by a process that is killed is reclaimed:
%% killed holders (a crashed writer's worker) used to leak the gate shut, after
%% which no export job ever ran again.
export_target_gate_survives_killed_holders_test() ->
    Holders = [ spawn(fun() -> with_target(fun() -> receive after infinity -> ok end end, wait) end)
              || _ <- lists:seq(1, ?DEFAULT_TARGET_OPS) ],
    timer:sleep(100),
    ?assertEqual({error, target_busy}, with_target(fun() -> ok end, try_once)),
    [ exit(H, kill) || H <- Holders ],
    timer:sleep(50),
    ?assertEqual(ran, with_target(fun() -> ran end, try_once)).

%% @doc A shipped segment that disappears from the target fails the restore,
%% even when it is the last one.
export_missing_segment_fails_restore_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("missingseg"),
        Remote = Dir ++ "/remote",
        Store = export_store(Dir, #{ <<"segment-ms">> => 100000, <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        [ begin [ assign(Store, Opts, P, R * 10 + N) || N <- lists:seq(0, 9) ], _ = sync_export(Store) end
        || R <- [0, 1, 2] ],
        Last = lists:last(remote_segments(Remote)),
        ok = file:delete(Remote ++ "/" ++ seg_name(Last)),
        ?assertError({export_segments_missing, [Last]}, restore(Remote, restored(Dir, "r"), ?LAX))
    end}.

%% @doc The periodic audit reports a healthy export, and notices a listed
%% segment that went missing; the deep check restores and compares.
export_audit_notices_missing_segment_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("audit"),
        Remote = Dir ++ "/remote",
        Store = export_store(Dir, #{ <<"segment-ms">> => 100000, <<"ship-interval-ms">> => 50,
                                     <<"audit-interval-ms">> => 200 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        [ begin [ assign(Store, Opts, P, R * 10 + N) || N <- lists:seq(0, 9) ], _ = sync_export(Store) end
        || R <- [0, 1] ],
        ?assertMatch({ok, #{ assignments := 20 }}, verify_restorable(Store, ?LAX)),
        ?assert(hb_util:wait_until(fun() ->
            case maps:get(last_audit, status(Store)) of #{ ok := true } -> true; _ -> false end end, 5000)),
        ok = file:delete(Remote ++ "/" ++ seg_name(lists:last(remote_segments(Remote)))),
        ?assert(hb_util:wait_until(fun() ->
            case maps:get(last_audit, status(Store)) of #{ ok := false, missing_segments := [_] } -> true; _ -> false end end, 5000)),
        ?assertMatch({error, _}, verify_restorable(Store, ?LAX))
    end}.

%% @doc A catch-up row never overwrites a newer live write of a mutable key:
%% the catch-up reads ~location@1.0 as V1, the key is then written V2 live,
%% and the restore has V2.
export_catchup_never_overwrites_newer_live_value_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("stale"),
        Store = export_store(Dir, #{ <<"segment-ms">> => 100000, <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        Loc = <<"~location@1.0/", P/binary>>,
        [ assign(Store, Opts, P, N) || N <- lists:seq(0, 5) ],
        _ = sync_export(Store),
        W1 = writer_pid(Store),
        erlang:suspend_process(W1),
        ok = hb_store:write([Store], #{ Loc => <<"v1">> }, Opts),
        [ assign(Store, Opts, P, N) || N <- lists:seq(6, 3000) ],
        exit(W1, kill),
        timer:sleep(20),
        % The restarted writer's catch-up is running; write the newer value.
        ok = hb_store:write([Store], #{ Loc => <<"v2">> }, Opts),
        St = sync_export(Store),
        ?assert(maps:get(catchups, St) >= 1),
        T = restored(Dir, "restored"),
        {ok, _} = restore(Dir ++ "/remote", T, ?LAX),
        ?assertEqual({ok, <<"v2">>}, hb_store:read([T], Loc, #{}))
    end}.

%% @doc A catch-up row between two equal live writes of a key does not let
%% the second be skipped: replaying the segment must end on the live value
%% (the rev2 fuzz restored stale `~location@1.0' values from this).
export_dedupe_raw_between_equal_writes_test() ->
    K = <<"~location@1.0/p">>,
    {Out, _} = dedupe([{write, #{ K => <<"0">> }}, {raw, [{K, <<"1">>}]},
                       {write, #{ K => <<"0">> }}], #{}),
    ?assertEqual([{write, #{ K => <<"0">> }}, {raw, [{K, <<"1">>}]},
                  {write, #{ K => <<"0">> }}], Out).

%% @doc A target root that disappears after first use is an outage: the
%% worker does not recreate it (an empty directory in its place would receive
%% segments the real target never sees); shipping resumes when it is back.
export_missing_root_is_an_outage_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("rootgone"),
        Remote = Dir ++ "/remote",
        Store = export_store(Dir, #{ <<"segment-ms">> => 50, <<"ship-interval-ms">> => 20 }),
        Opts = #{ <<"store">> => [Store] },
        ok = hb_store:start([Store], #{}, Opts),
        P = hb_util:human_id(crypto:strong_rand_bytes(32)),
        [ assign(Store, Opts, P, N) || N <- lists:seq(0, 4) ],
        _ = sync_export(Store),
        ok = file:rename(Remote, Remote ++ ".off"),
        [ assign(Store, Opts, P, N) || N <- lists:seq(5, 9) ],
        timer:sleep(500),
        ?assertNot(filelib:is_dir(Remote)),
        ok = file:rename(Remote ++ ".off", Remote),
        _ = sync_export(Store),
        {ok, R} = restore(Remote, restored(Dir, "r"), ?LAX),
        ?assertEqual(10, maps:get(assignments, R))
    end}.

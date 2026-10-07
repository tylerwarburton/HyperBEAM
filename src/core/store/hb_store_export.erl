%%% @doc A store wrapper that continuously exports every write of its inner
%%% store to a (possibly slow, possibly remote) directory as an append-only,
%%% resumable log, with restore.
%%%
%%% Built for the essentials store (`hb_store_essentials'): the inner store is a
%%% LOCAL store that serves every read and takes every write synchronously, and
%%% the export is the off-box copy. Nothing on the write path waits for the
%%% export target:
%%%
%%% <pre>
%%%   write/link/group --> inner store (local, synchronous)
%%%                    \-> cast {record} --> writer --> local journal segments
%%%                                                         |
%%%                                         shipper --------+--> Path/seg-N.log
%%% </pre>
%%%
%%% A caller pays one counter update and one message send. The writer appends
%%% framed records (`<<Len:32, Crc:32, term_to_binary(Rec)>>') to the current
%%% local segment and rolls it by size or age; the shipper copies each closed
%%% segment to the export path (write to `.tmp', sync, rename) and then deletes
%%% the local copy. A slow or unavailable target only lets local segments
%%% accumulate. That backlog is bounded (`max-backlog-bytes', and
%%% `max-pending' records queued in memory): past the bound the writer stops
%%% journaling, records that a resync is needed, and once the shipper has
%%% drained the backlog it writes a full base image of the inner store
%%% (`base-N.log', then `base-N.done') from which, plus every segment numbered
%%% N or above, the store can be rebuilt. Exporting is therefore never allowed
%%% to slow or fail a write, and a gap in the log is always closed by a base.
%%% The local essentials are never deleted by the export.
%%%
%%% Restart is resumable: local segments not yet shipped are shipped on start
%%% (re-shipping one is an idempotent overwrite), and the segment counter
%%% continues from the highest number seen locally or at the target.
%%%
%%% Export options (`essentials-export'):
%%% <ul>
%%%   <li>`path': the export directory (the remote mount). Required.</li>
%%%   <li>`journal': the local journal directory. Default: `<inner name>-journal'
%%%       beside the inner store.</li>
%%%   <li>`segment-bytes' (8 MiB), `segment-ms' (5000): roll a segment at this
%%%       size or age, so the export lags by at most about this much under a
%%%       healthy target.</li>
%%%   <li>`max-backlog-bytes' (4 GiB): unshipped local journal above which the
%%%       writer stops journaling and schedules a resync.</li>
%%%   <li>`max-pending' (100000): records queued to the writer above which new
%%%       records are dropped (and a resync scheduled) rather than queued.</li>
%%%   <li>`ship-interval-ms' (1000): how often the shipper looks for work.
%%%       A failed ship (any error: `eio', `etimedout', `estale' from a soft
%%%       NFS mount, a full disk) is retried with exponential backoff up to 5
%%%       minutes while the local backlog grows.</li>
%%%   <li>`max-fill-pct' (75): never write to the target if that would take
%%%       its filesystem above this fill. Other tenants of a shared mount have
%%%       ceilings of their own (the AutoGrow backup push refuses above 85%).
%%%       At the ceiling the export pauses -- the local backlog grows, nothing
%%%       is lost -- emits `essentials_export_ceiling' events and reports
%%%       `ceiling => true', and resumes by itself when space appears.</li>
%%%   <li>`max-bytes' (none): a byte budget for everything this export keeps
%%%       at the target, enforced the same way.</li>
%%% </ul>
%%%
%%% The target only ever sees whole files written once: `seg-N.log' (a few MB,
%%% `.tmp' + sync + rename), `base-N.log'/`.done', and `manifest.log', an
%%% append-only list of every file shipped with its size and CRC, which
%%% `restore/3' checks. No per-key file is ever created on the target: a
%%% network mount pays a round trip per file operation.
%%%
%%% Metrics: `status/1' and the `essentials_export' event counters: queued and
%%% dropped records, local backlog bytes and segments, the age of the oldest
%%% unshipped segment (the export lag), shipped segments and bytes, errors,
%%% resyncs.
-module(hb_store_export).
-export([wrap/2, status/1, restore/2, restore/3, sync_export/1, resync/1]).
-export([space_ok/3, put_file/2, append_manifest/2, dir_bytes/1]).
-export([start/3, stop/3, reset/3, scope/1]).
-export([read/3, write/3, list/3, match/3, group/3, link/3, type/3,
         resolve/3, sync/3, delete/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_SEGMENT_BYTES, 8 * 1024 * 1024).
-define(DEFAULT_SEGMENT_MS, 60000).
-define(DEFAULT_MAX_FILL_PCT, 75).
-define(MAX_BACKOFF_MS, 300000).
-define(DEFAULT_MAX_BACKLOG, 4 * 1024 * 1024 * 1024).
-define(DEFAULT_MAX_PENDING, 100000).
-define(DEFAULT_SHIP_MS, 1000).
-define(SCAN_ROWS, 2000).

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

stop(Store, Req, Opts) ->
    catch call(Store, flush, 30000),
    hb_store:stop([inner(Store)], Req, Opts).

reset(Store, Req, Opts) ->
    hb_store:reset([inner(Store)], Req, Opts).

scope(_Store) -> local.

read(Store, Req, Opts) -> (inner_mod(Store)):read(inner(Store), Req, Opts).
list(Store, Req, Opts) -> (inner_mod(Store)):list(inner(Store), Req, Opts).
match(Store, Req, Opts) -> (inner_mod(Store)):match(inner(Store), Req, Opts).
type(Store, Req, Opts) -> (inner_mod(Store)):type(inner(Store), Req, Opts).
resolve(Store, Req, Opts) -> (inner_mod(Store)):resolve(inner(Store), Req, Opts).

sync(Store, Req, Opts) ->
    Mod = inner_mod(Store),
    _ = code:ensure_loaded(Mod),
    case erlang:function_exported(Mod, sync, 3) of
        true -> Mod:sync(inner(Store), Req, Opts);
        false -> ok
    end.

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
%% (1: records queued, 2: records dropped). Started on first use.
exporter(Store) ->
    Key = {?MODULE, maps:get(<<"name">>, Store)},
    case persistent_term:get(Key, undefined) of
        {Pid, _Counters} = E ->
            case is_process_alive(Pid) of
                true -> E;
                false -> start_exporter(Key, Store)
            end;
        undefined -> start_exporter(Key, Store)
    end.

start_exporter(Key, Store) ->
    global:trans({Key, self()},
        fun() ->
            case persistent_term:get(Key, undefined) of
                {Pid, _} = E when is_pid(Pid) ->
                    case is_process_alive(Pid) of
                        true -> E;
                        false -> do_start_exporter(Key, Store)
                    end;
                _ -> do_start_exporter(Key, Store)
            end
        end).

do_start_exporter(Key, Store) ->
    Counters = counters:new(2, [write_concurrency]),
    Parent = self(),
    Ref = make_ref(),
    Pid =
        spawn(fun() ->
            process_flag(trap_exit, true),
            State = init_state(Store, Counters),
            Parent ! {Ref, started},
            writer_loop(State)
        end),
    receive {Ref, started} -> ok after 30000 -> ok end,
    persistent_term:put(Key, {Pid, Counters}),
    {Pid, Counters}.

call(Store, Msg, Timeout) ->
    {Pid, _} = exporter(Store),
    Ref = make_ref(),
    Pid ! {call, self(), Ref, Msg},
    receive {Ref, Reply} -> Reply after Timeout -> {error, timeout} end.

%% @doc Export status and lag metrics.
status(Store) -> call(Store, status, 10000).

%% @doc Schedule a base image: for rows written to the inner store without
%% passing through the wrapper (a migration copies raw rows into it).
resync(Store) -> call(Store, resync, 10000).

%% @doc Close the current segment and ship everything now (blocking; for
%% tests and for an operator before maintenance).
sync_export(Store) -> call(Store, sync_export, 600000).

cfg(Store, Key, Default) ->
    Export = maps:get(<<"export">>, Store, #{}),
    case maps:get(Key, Export, Default) of
        V when is_binary(V), is_integer(Default) -> binary_to_integer(V);
        V -> V
    end.

%%% Writer / shipper process. One process does both, so they cannot race on
%%% the segment files; shipping runs in a linked helper so a slow target never
%%% blocks the journal.

init_state(Store, Counters) ->
    Inner = inner(Store),
    Journal =
        hb_util:list(cfg(Store, <<"journal">>,
            <<(hb_util:bin(maps:get(<<"name">>, Inner, <<"essentials">>)))/binary,
              "-journal">>)),
    Path = hb_util:list(cfg(Store, <<"path">>, undefined)),
    ok = filelib:ensure_dir(filename:join(Journal, "x")),
    Local = local_segments(Journal),
    Remote = remote_segments(Path),
    Next = 1 + lists:max([0 | Local ++ Remote ++ remote_bases(Path)]),
    FirstEver = not filelib:is_file(filename:join(Journal, "state")),
    ok = file:write_file(filename:join(Journal, "state"), <<"exporting">>),
    ResyncFlag = filelib:is_file(filename:join(Journal, "resync-needed")),
    S0 = #{
        store => Store,
        inner => Inner,
        counters => Counters,
        journal => Journal,
        path => Path,
        seq => Next,
        fd => undefined,
        seg_bytes => 0,
        seg_opened => undefined,
        backlog => lists:sum([ filelib:file_size(seg_file(Journal, Q)) || Q <- Local ]),
        dropping => false,
        resync => FirstEver orelse ResyncFlag,
        shipper => undefined,
        target_bytes => dir_bytes(Path),
        ceiling => false,
        next_try => 0,
        stats => #{ shipped_segments => 0, shipped_bytes => 0, errors => 0,
                    consecutive_errors => 0, ceiling_hits => 0,
                    resyncs => 0, last_ship => undefined, last_error => undefined }
    },
    case S0 of
        #{ resync := true } -> mark_resync(Journal);
        _ -> ok
    end,
    erlang:send_after(cfg(Store, <<"ship-interval-ms">>, ?DEFAULT_SHIP_MS), self(), tick),
    S0.

writer_loop(S) ->
    receive
        {rec, Rec} ->
            Recs = drain([Rec], 1000),
            counters:sub(maps:get(counters, S), 1, length(Recs)),
            writer_loop(append(S, Recs));
        overflow ->
            writer_loop(start_dropping(S));
        tick ->
            S1 = maybe_roll(S),
            S2 = maybe_ship(S1),
            erlang:send_after(
                cfg(maps:get(store, S), <<"ship-interval-ms">>, ?DEFAULT_SHIP_MS),
                self(), tick),
            writer_loop(S2);
        {shipped, Result} ->
            writer_loop(shipped(S, Result));
        {'EXIT', Pid, Reason} ->
            case maps:get(shipper, S) of
                Pid when Reason =/= normal ->
                    writer_loop(shipped(S#{ shipper => undefined }, {error, Reason}));
                _ -> writer_loop(S)
            end;
        {call, From, Ref, status} ->
            From ! {Ref, status_of(S)},
            writer_loop(S);
        {call, From, Ref, resync} ->
            mark_resync(maps:get(journal, S)),
            From ! {Ref, ok},
            writer_loop(S#{ resync => true });
        {call, From, Ref, flush} ->
            S1 = close_segment(drain_all(S)),
            From ! {Ref, ok},
            writer_loop(S1);
        {call, From, Ref, sync_export} ->
            % An explicit request does not wait out a backoff.
            S1 = wait_shipper(close_segment(drain_all(S#{ next_try => 0 }))),
            S2 = ship_now(S1),
            From ! {Ref, status_of(S2)},
            writer_loop(S2)
    end.

drain(Acc, 0) -> lists:reverse(Acc);
drain(Acc, N) ->
    receive {rec, R} -> drain([R | Acc], N - 1)
    after 0 -> lists:reverse(Acc)
    end.

drain_all(S) ->
    receive {rec, R} ->
        counters:sub(maps:get(counters, S), 1, 1),
        drain_all(append(S, [R]))
    after 0 -> S
    end.

%% Append records to the current local segment, rolling it by size.
append(S = #{ dropping := true }, _Recs) -> S;
append(S, Recs) ->
    S1 = ensure_segment(S),
    Bin = << <<(frame(R))/binary>> || R <- Recs >>,
    case file:write(maps:get(fd, S1), Bin) of
        ok ->
            S2 = S1#{ seg_bytes => maps:get(seg_bytes, S1) + byte_size(Bin),
                      backlog => maps:get(backlog, S1) + byte_size(Bin) },
            Store = maps:get(store, S),
            case maps:get(backlog, S2) >= cfg(Store, <<"max-backlog-bytes">>,
                                              ?DEFAULT_MAX_BACKLOG) of
                true -> start_dropping(S2);
                false ->
                    case maps:get(seg_bytes, S2) >= cfg(Store, <<"segment-bytes">>,
                                                        ?DEFAULT_SEGMENT_BYTES) of
                        true -> close_segment(S2);
                        false -> S2
                    end
            end;
        {error, Reason} ->
            ?event(error, {essentials_journal_write_failed, Reason}),
            start_dropping(bump_stat(S1, errors))
    end.

frame(Rec) ->
    Bin = term_to_binary(Rec),
    <<(byte_size(Bin)):32, (erlang:crc32(Bin)):32, Bin/binary>>.

ensure_segment(S = #{ fd := undefined, journal := J, seq := Seq }) ->
    {ok, Fd} = file:open(seg_file(J, Seq), [append, raw, binary, {delayed_write, 1048576, 200}]),
    S#{ fd => Fd, seg_bytes => 0, seg_opened => erlang:monotonic_time(millisecond) };
ensure_segment(S) -> S.

close_segment(S = #{ fd := undefined }) -> S;
close_segment(S = #{ fd := Fd, seq := Seq }) ->
    _ = file:close(Fd),
    S#{ fd => undefined, seq => Seq + 1, seg_bytes => 0, seg_opened => undefined }.

maybe_roll(S = #{ seg_opened := undefined }) -> S;
maybe_roll(S = #{ seg_opened := Opened }) ->
    case erlang:monotonic_time(millisecond) - Opened >=
            cfg(maps:get(store, S), <<"segment-ms">>, ?DEFAULT_SEGMENT_MS) of
        true -> close_segment(S);
        false -> S
    end.

%% Past a bound: stop journaling (the log now has a gap) and schedule a
%% resync, which writes a base image once the backlog has drained.
start_dropping(S = #{ dropping := true }) -> S;
start_dropping(S) ->
    ?event(warning, {essentials_export_backlog_full, status_of(S)}),
    mark_resync(maps:get(journal, S)),
    close_segment(S#{ dropping => true, resync => true }).

mark_resync(Journal) ->
    file:write_file(filename:join(Journal, "resync-needed"), <<>>).

%% Ship closed segments in a helper process; when none are left and a resync
%% is due, write the base image.
maybe_ship(S = #{ shipper := Pid }) when is_pid(Pid) -> S;
maybe_ship(S = #{ next_try := Next }) ->
    case erlang:system_time(millisecond) >= Next of
        false -> S;
        true -> do_maybe_ship(S)
    end.

do_maybe_ship(S = #{ dropping := true }) ->
    % Resume journaling once the backlog is gone, with a base to bridge the gap.
    case closed_segments(S) of
        [] -> start_ship(S#{ dropping => false }, base);
        _ -> start_ship(S, segments)
    end;
do_maybe_ship(S = #{ resync := true }) ->
    case closed_segments(S) of
        [] -> start_ship(S, base);
        _ -> start_ship(S, segments)
    end;
do_maybe_ship(S) ->
    case closed_segments(S) of
        [] -> S;
        _ -> start_ship(S, segments)
    end.

ship_now(S) ->
    Errors = maps:get(errors, maps:get(stats, S)),
    S1 = wait_shipper(maybe_ship(S#{ next_try => 0 })),
    Pending = closed_segments(S1) =/= [] orelse maps:get(resync, S1),
    case Pending andalso not maps:get(ceiling, S1)
            andalso maps:get(errors, maps:get(stats, S1)) == Errors of
        true -> ship_now(S1);
        false -> S1
    end.

wait_shipper(S = #{ shipper := undefined }) -> S;
wait_shipper(S = #{ shipper := Pid }) ->
    receive
        {shipped, Result} -> shipped(S, Result);
        {'EXIT', Pid, Reason} when Reason =/= normal ->
            shipped(S#{ shipper => undefined }, {error, Reason})
    end.

start_ship(S = #{ journal := J, path := P }, What) ->
    Segs0 = closed_segments(S),
    Inner = maps:get(inner, S),
    Need =
        case What of
            segments -> segment_bytes(J, Segs0);
            base -> inner_bytes(Inner)
        end,
    case space_ok(P, Need + maps:get(target_bytes, S), export_cfg(S)) of
        ok -> do_start_ship(at_ceiling(S, false, ok), What, Segs0, Inner);
        {ceiling, _} = Why when What == segments ->
            % Ship the oldest segments that fit, if any do.
            case fitting(J, P, Segs0, S) of
                [] -> at_ceiling(S, true, Why);
                Fit -> do_start_ship(at_ceiling(S, true, Why), What, Fit, Inner)
            end;
        Why -> at_ceiling(S, true, Why)
    end.

do_start_ship(S = #{ journal := J, path := P }, What, Segs, Inner) ->
    Self = self(),
    BaseSeq = maps:get(seq, S),
    % A base is numbered with the segment the writer opens next, so every
    % write it might miss is in a segment numbered at or above it.
    S1 = case What of base -> close_segment(S); _ -> S end,
    Pid =
        spawn_link(fun() ->
            Result =
                try
                    case What of
                        segments -> {segments, ship_segments(J, P, Segs)};
                        base -> {base, write_base(Inner, P, BaseSeq)}
                    end
                catch C:R -> {error, {C, R}}
                end,
            Self ! {shipped, Result}
        end),
    S1#{ shipper => Pid }.

shipped(S, Result) ->
    S0 = S#{ shipper => undefined },
    St0 = maps:get(stats, S0),
    S1 =
        case Result of
            {_, {error, _}} -> S0;
            {error, _} -> S0;
            _ -> S0#{ next_try => 0, stats => St0#{ consecutive_errors => 0 } }
        end,
    case Result of
        {segments, {ok, N, Bytes}} ->
            St = maps:get(stats, S1),
            S1#{ backlog => max(0, maps:get(backlog, S1) - Bytes),
                 target_bytes => maps:get(target_bytes, S1) + Bytes,
                 stats => St#{
                shipped_segments => maps:get(shipped_segments, St) + N,
                shipped_bytes => maps:get(shipped_bytes, St) + Bytes,
                last_ship => os:system_time(millisecond) } };
        {base, {ok, BaseBytes}} ->
            _ = file:delete(filename:join(maps:get(journal, S1), "resync-needed")),
            bump_stat(S1#{ resync => false,
                           target_bytes => maps:get(target_bytes, S1) + BaseBytes }, resyncs);
        {_, {error, Reason}} -> ship_error(S1, Reason);
        {error, Reason} -> ship_error(S1, Reason)
    end.

%% Any failure is retried, with exponential backoff, while the backlog grows.
ship_error(S, Reason) ->
    ?event(warning, {essentials_export_ship_failed, Reason}),
    St = maps:get(stats, S),
    N = maps:get(consecutive_errors, St) + 1,
    Base = cfg(maps:get(store, S), <<"ship-interval-ms">>, ?DEFAULT_SHIP_MS),
    Delay = min(?MAX_BACKOFF_MS, Base bsl min(N, 20)),
    S#{ next_try => erlang:system_time(millisecond) + Delay,
        stats => St#{ errors => maps:get(errors, St) + 1, last_error => Reason,
                      consecutive_errors => N } }.

%% Enter or leave the fill ceiling, loudly on each transition.
at_ceiling(S = #{ ceiling := Was }, Now, Why) ->
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

export_cfg(#{ store := Store }) -> maps:get(<<"export">>, Store, #{}).

segment_bytes(J, Segs) -> lists:sum([ filelib:file_size(seg_file(J, Q)) || Q <- Segs ]).

inner_bytes(#{ <<"name">> := Name }) ->
    filelib:file_size(filename:join(hb_util:list(Name), "data.mdb"));
inner_bytes(_) -> 0.

%% The oldest closed segments whose total still fits under the limits.
fitting(J, P, Segs, S) ->
    {Fit, _} =
        lists:foldl(
            fun(Q, {Acc, Used}) ->
                Size = filelib:file_size(seg_file(J, Q)),
                case Acc =/= stop andalso
                        space_ok(P, Used + Size, export_cfg(S)) == ok of
                    true -> {[Q | Acc], Used + Size};
                    false -> {stop, Used}
                end
            end,
            {[], maps:get(target_bytes, S)},
            Segs
        ),
    case Fit of stop -> []; _ -> lists:reverse(Fit) end.

%% @doc May `Path' take more data so that this exporter's files total `Bytes'?
%% Checks the byte budget (`max-bytes') against `Bytes', and the filesystem
%% fill (`max-fill-pct', default 75) after the new bytes would land.
%% `stat-fun' (a fun of the path returning `{TotalBytes, AvailBytes}')
%% replaces `df' -- for tests, and for filesystems `df' cannot read.
space_ok(Path, Bytes, Cfg) ->
    Budget = maps:get(<<"max-bytes">>, Cfg, undefined),
    MaxPct = maps:get(<<"max-fill-pct">>, Cfg, ?DEFAULT_MAX_FILL_PCT),
    case is_integer(Budget) andalso Bytes > Budget of
        true -> {ceiling, {budget, Bytes, Budget}};
        false ->
            case fs_stat(Path, Cfg) of
                {Total, Avail} when is_integer(Total), Total > 0 ->
                    Used = Total - Avail,
                    % The new bytes are the part of `Bytes' not yet written:
                    % callers pass totals, so charge the worst case of all.
                    Pct = (Used * 100) div Total,
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
    case filelib:is_dir(Dir) of
        true -> Dir;
        false ->
            case filename:dirname(Dir) of
                Dir -> Dir;
                Parent -> existing_parent(Parent)
            end
    end.

%% @doc Total size of the files directly in `Dir' (one listing, one stat each:
%% done once, at start, since every file operation on a network mount costs a
%% round trip).
dir_bytes(undefined) -> 0;
dir_bytes(Dir) ->
    case file:list_dir(Dir) of
        {ok, Names} -> lists:sum([ max(0, filelib:file_size(filename:join(Dir, N))) || N <- Names ]);
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

bump_stat(S, K) ->
    St = maps:get(stats, S),
    S#{ stats => St#{ K => maps:get(K, St) + 1 } }.

%% Copy each closed local segment to the target, in order, then delete it
%% locally. Stops at the first failure; the rest are retried next tick.
ship_segments(J, P, Segs) ->
    ok = filelib:ensure_dir(filename:join(P, "x")),
    lists:foldl(
        fun(Seq, {ok, N, B}) ->
                Src = seg_file(J, Seq),
                case file:read_file(Src) of
                    {ok, Bin} ->
                        Name = seg_name(Seq),
                        case put_file(filename:join(P, Name), Bin) of
                            ok ->
                                case append_manifest(P, {Name, byte_size(Bin), erlang:crc32(Bin)}) of
                                    ok ->
                                        ok = file:delete(Src),
                                        {ok, N + 1, B + byte_size(Bin)};
                                    Err -> Err
                                end;
                            Err -> Err
                        end;
                    Err -> Err
                end;
           (_Seq, Err) -> Err
        end,
        {ok, 0, 0},
        Segs
    ).

%% Write a file durably and atomically: temp file, sync, rename.
put_file(RawDst, Bin) ->
    Dst = hb_util:list(RawDst),
    Tmp = Dst ++ ".tmp",
    case file:open(Tmp, [write, raw, binary]) of
        {ok, Fd} ->
            Res = case file:write(Fd, Bin) of ok -> file:sync(Fd); E -> E end,
            _ = file:close(Fd),
            case Res of
                ok -> file:rename(Tmp, Dst);
                Err -> Err
            end;
        Err -> Err
    end.

%% A full image of the inner store as raw rows, streamed in bounded chunks.
write_base(Inner = #{ <<"store-module">> := hb_store_lmdb }, P, Seq) ->
    ok = filelib:ensure_dir(filename:join(P, "x")),
    #{ <<"db">> := DB } = hb_store:find(Inner),
    ok = elmdb:flush(DB),
    Name = filename:join(P, "base-" ++ seq_str(Seq) ++ ".log"),
    {ok, Fd} = file:open(Name ++ ".tmp", [write, raw, binary, {delayed_write, 4194304, 1000}]),
    Loop =
        fun L(From) ->
            case elmdb:scan_rows(DB, From, ?SCAN_ROWS) of
                {ok, Rows, _N, Next} ->
                    case Rows of
                        [] -> ok;
                        _ -> ok = file:write(Fd, frame({raw, Rows}))
                    end,
                    case Next of
                        done -> ok;
                        _ -> L(Next)
                    end;
                {error, T, D} -> throw({scan_failed, T, D})
            end
        end,
    try Loop(<<>>) after file:close(Fd) end,
    {ok, Fd2} = file:open(Name ++ ".tmp", [read, raw]),
    ok = file:sync(Fd2), ok = file:close(Fd2),
    ok = file:rename(Name ++ ".tmp", Name),
    {ok, Bin} = file:read_file(Name),
    ok = append_manifest(P, {filename:basename(Name), byte_size(Bin), erlang:crc32(Bin)}),
    ok = file:write_file(filename:join(P, "base-" ++ seq_str(Seq) ++ ".done"), <<>>),
    {ok, byte_size(Bin)};
write_base(Inner, _P, _Seq) ->
    {error, {base_needs_lmdb_inner, Inner}}.

status_of(S) ->
    Segs = closed_segments(S),
    J = maps:get(journal, S),
    Backlog = maps:get(backlog, S),
    Oldest =
        case Segs of
            [] -> 0;
            [First | _] ->
                case file:read_file_info(seg_file(J, First), [{time, posix}]) of
                    {ok, I} -> max(0, os:system_time(second) - element(6, I)) * 1000;
                    _ -> 0
                end
        end,
    St = maps:get(stats, S),
    St#{
        queued_records => counters:get(maps:get(counters, S), 1),
        dropped_records => counters:get(maps:get(counters, S), 2),
        local_backlog_bytes => Backlog,
        local_segments => length(Segs),
        lag_ms => Oldest,
        dropping => maps:get(dropping, S),
        ceiling => maps:get(ceiling, S),
        target_bytes => maps:get(target_bytes, S),
        resync_pending => maps:get(resync, S),
        next_segment => maps:get(seq, S),
        shipping => maps:get(shipper, S) =/= undefined
    }.

%% Closed local segments, oldest first: every one but the segment being written.
closed_segments(#{ journal := J, seq := Seq, fd := Fd }) ->
    [ Q || Q <- local_segments(J), Fd == undefined orelse Q < Seq ].

local_segments(J) -> segs_in(J, "seg-", ".log").
remote_segments(undefined) -> [];
remote_segments(P) -> segs_in(P, "seg-", ".log").
remote_bases(undefined) -> [];
remote_bases(P) -> segs_in(P, "base-", ".done").

segs_in(Dir, Prefix, Suffix) ->
    case file:list_dir(Dir) of
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

%%% Restore.

%% @doc Rebuild a store from an export directory: the newest complete base
%% image, then every segment numbered at or above it, in order. Without a base,
%% the segments must run unbroken from 1. A torn final record (a segment cut
%% short by a crash) is ignored; anything else malformed fails the restore.
restore(Path, Target) -> restore(Path, Target, #{}).
restore(RawPath, Target, Opts) ->
    Path = hb_util:list(RawPath),
    ok = hb_store:start([Target], #{}, Opts),
    Bases = remote_bases(Path),
    Segs = remote_segments(Path),
    {From, BaseRows} =
        case Bases of
            [] -> {1, 0};
            _ ->
                B = lists:max(Bases),
                {ok, BaseN} = apply_file(filename:join(Path, "base-" ++ seq_str(B) ++ ".log"),
                                     Target, Opts),
                {B, BaseN}
        end,
    Wanted = [ Q || Q <- Segs, Q >= From ],
    case contiguous(From, Wanted) of
        false -> {error, {gap_in_export, From, Wanted}};
        true ->
            Records =
                lists:sum(
                    [ element(2, apply_file(filename:join(Path, seg_name(Q)), Target, Opts))
                    || Q <- Wanted ]),
            ok = hb_store:sync([Target], #{}, Opts),
            {ok, #{ base => From, base_records => BaseRows,
                    segments => length(Wanted), records => Records }}
    end.

contiguous(_From, []) -> true;
contiguous(From, [From | Rest]) -> contiguous(From + 1, Rest);
contiguous(_, _) -> false.

apply_file(File, Target, Opts) ->
    {ok, Bin} = file:read_file(File),
    ok = check_manifest(File, Bin),
    apply_records(Bin, Target, Opts, 0).

%% A file listed in the manifest must have the size and CRC recorded there.
check_manifest(File, Bin) ->
    Dir = filename:dirname(File),
    Name = filename:basename(File),
    case file:read_file(filename:join(Dir, "manifest.log")) of
        {ok, M} ->
            Entries = [ E || E = {N, _, _} <- manifest_entries(M, []), N == Name ],
            case Entries of
                [] -> ok;
                _ ->
                    case lists:last(Entries) of
                        {_, Size, Crc} when Size == byte_size(Bin) ->
                            case erlang:crc32(Bin) of
                                Crc -> ok;
                                _ -> erlang:error({export_file_corrupt, Name})
                            end;
                        _ -> erlang:error({export_file_size_mismatch, Name})
                    end
            end;
        _ -> ok
    end.

manifest_entries(<<Len:32, Crc:32, Bin:Len/binary, Rest/binary>>, Acc) ->
    case erlang:crc32(Bin) of
        Crc -> manifest_entries(Rest, [binary_to_term(Bin) | Acc]);
        _ -> lists:reverse(Acc)
    end;
manifest_entries(_, Acc) -> lists:reverse(Acc).

apply_records(<<Len:32, Crc:32, Bin:Len/binary, Rest/binary>>, Target, Opts, N) ->
    case erlang:crc32(Bin) of
        Crc ->
            apply_record(binary_to_term(Bin), Target, Opts),
            apply_records(Rest, Target, Opts, N + 1);
        _ -> erlang:error({corrupt_export_record, N})
    end;
apply_records(_Torn, _Target, _Opts, N) ->
    {ok, N}.

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
        {ok, R} = restore(Dir ++ "/remote", Target, Opts),
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

%% @doc An unreachable target never fails or slows a write: records are
%% journaled locally, the backlog bound turns overflow into a resync, and when
%% the target returns a base image closes the gap so a restore is complete.
export_survives_unreachable_target_test_() ->
    {timeout, 120, fun() ->
        application:ensure_all_started(hb),
        Dir = export_test_dir("down"),
        % The target path is a file, so every ship fails until it is removed.
        ok = filelib:ensure_dir(Dir ++ "/x"),
        ok = file:write_file(Dir ++ "/remote", <<"not a directory">>),
        Store = export_store(Dir, #{ <<"segment-bytes">> => 4096,
                                     <<"max-backlog-bytes">> => 20000,
                                     <<"ship-interval-ms">> => 50 }),
        Opts = #{ <<"store">> => [Store], <<"priv-wallet">> => ar_wallet:new() },
        ok = hb_store:start([Store], #{}, Opts),
        Write =
            fun(N) ->
                M = #{ <<"n">> => integer_to_binary(N), <<"data">> => crypto:strong_rand_bytes(500) },
                {T, {ok, ID}} = timer:tc(fun() -> hb_cache:write(M, Opts) end),
                {T, ID}
            end,
        Res = [ Write(N) || N <- lists:seq(1, 300) ],
        timer:sleep(500),
        St1 = status(Store),
        ?assert(maps:get(errors, St1) > 0),
        ?assert(maps:get(resync_pending, St1)),
        % Writes were never slowed by the target: the slowest is a local write.
        ?assert(lists:max([ T || {T, _} <- Res ]) < 1000000),
        ok = file:delete(Dir ++ "/remote"),
        St2 = sync_export(Store),
        ?assertNot(maps:get(resync_pending, St2)),
        ?assert(maps:get(resyncs, St2) >= 1),
        Target = #{ <<"store-module">> => hb_store_lmdb,
                    <<"name">> => hb_util:bin(Dir ++ "/restored") },
        {ok, _} = restore(Dir ++ "/remote", Target, Opts),
        TOpts = Opts#{ <<"store">> => [Target] },
        lists:foreach(
            fun({_, ID}) ->
                {ok, A} = hb_cache:read(ID, Opts),
                {ok, B} = hb_cache:read(ID, TOpts),
                ?assertEqual(hb_cache:ensure_all_loaded(A, Opts),
                             hb_cache:ensure_all_loaded(B, TOpts))
            end,
            Res
        )
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
        {ok, _} = restore(Dir ++ "/remote", Target, Opts),
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
                {RestoreUs, {ok, R}} = timer:tc(fun() -> restore(Real ++ "/essentials", Target, Opts) end),
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

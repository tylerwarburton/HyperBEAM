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
%%%   <li>`ship-interval-ms' (1000): how often the shipper looks for work.</li>
%%% </ul>
%%%
%%% Metrics: `status/1' and the `essentials_export' event counters: queued and
%%% dropped records, local backlog bytes and segments, the age of the oldest
%%% unshipped segment (the export lag), shipped segments and bytes, errors,
%%% resyncs.
-module(hb_store_export).
-export([wrap/2, status/1, restore/2, restore/3, sync_export/1, resync/1]).
-export([start/3, stop/3, reset/3, scope/1]).
-export([read/3, write/3, list/3, match/3, group/3, link/3, type/3,
         resolve/3, sync/3, delete/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_SEGMENT_BYTES, 8 * 1024 * 1024).
-define(DEFAULT_SEGMENT_MS, 5000).
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
        stats => #{ shipped_segments => 0, shipped_bytes => 0, errors => 0,
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
            S1 = wait_shipper(close_segment(drain_all(S))),
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
maybe_ship(S = #{ dropping := true }) ->
    % Resume journaling once the backlog is gone, with a base to bridge the gap.
    case closed_segments(S) of
        [] -> start_ship(S#{ dropping => false }, base);
        _ -> start_ship(S, segments)
    end;
maybe_ship(S = #{ resync := true }) ->
    case closed_segments(S) of
        [] -> start_ship(S, base);
        _ -> start_ship(S, segments)
    end;
maybe_ship(S) ->
    case closed_segments(S) of
        [] -> S;
        _ -> start_ship(S, segments)
    end.

ship_now(S) ->
    Errors = maps:get(errors, maps:get(stats, S)),
    S1 = wait_shipper(maybe_ship(S)),
    Pending = closed_segments(S1) =/= [] orelse maps:get(resync, S1),
    case Pending andalso maps:get(errors, maps:get(stats, S1)) == Errors of
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
    Self = self(),
    Segs = closed_segments(S),
    Inner = maps:get(inner, S),
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
    S1 = S#{ shipper => undefined },
    case Result of
        {segments, {ok, N, Bytes}} ->
            St = maps:get(stats, S1),
            S1#{ backlog => max(0, maps:get(backlog, S1) - Bytes),
                 stats => St#{
                shipped_segments => maps:get(shipped_segments, St) + N,
                shipped_bytes => maps:get(shipped_bytes, St) + Bytes,
                last_ship => os:system_time(millisecond) } };
        {base, ok} ->
            _ = file:delete(filename:join(maps:get(journal, S1), "resync-needed")),
            bump_stat(S1#{ resync => false }, resyncs);
        {_, {error, Reason}} -> ship_error(S1, Reason);
        {error, Reason} -> ship_error(S1, Reason)
    end.

ship_error(S, Reason) ->
    ?event(warning, {essentials_export_ship_failed, Reason}),
    St = maps:get(stats, S),
    S#{ stats => St#{ errors => maps:get(errors, St) + 1, last_error => Reason } }.

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
                        case put_file(filename:join(P, seg_name(Seq)), Bin) of
                            ok ->
                                ok = file:delete(Src),
                                {ok, N + 1, B + byte_size(Bin)};
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
put_file(Dst, Bin) ->
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
    file:write_file(filename:join(P, "base-" ++ seq_str(Seq) ++ ".done"), <<>>);
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
    apply_records(Bin, Target, Opts, 0).

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

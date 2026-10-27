%%% @doc A store wrapper that journals every write of its inner store, append
%%% only, and ships the journal to a slow or remote directory (the offsite
%%% copy), with restore.
%%%
%%% Built for the irreplaceable store (`hb_store_irreplaceable'): the inner
%%% store is local, serves every read and takes every write; the journal is
%%% the offsite copy of everything written to it.
%%%
%%% <pre>
%%%   write --> writer: append to local journal, then write the inner store
%%%   flush --> writer: fdatasync the journal, then flush the inner store
%%%                     |
%%%                     v sealed segments
%%%                   shipper --> Path/seg-Token-N.log.gz, Path/manifest.log
%%% </pre>
%%%
%%% Write-ahead. The writer appends a write's rows to the journal before it
%%% writes them to the inner store, and both happen in the writer, so the
%%% journal holds every write in the order the inner store took them. A
%%% `kill -9' of the node loses nothing the inner store kept: the append is in
%%% the OS page cache before the inner write starts. `flush/3', which the
%%% scheduler calls before it confirms a slot, syncs the journal before it
%%% flushes the inner store, so a confirmed slot is on disk in both.
%%%
%%% Confirmation. A flush appends a `flushed' marker before it syncs, and
%%% only writes a `flushed' marker follows count: a restore drops what a run
%%% wrote and never confirmed (a `start' marker opens every run), and a
%%% `revoke' marker withdraws a confirmation whose inner flush failed. Any
%%% failed sync poisons the journal: it is never retried (on Linux a retry
%%% can report success for pages the kernel dropped), and every later write
%%% and flush of the run is refused.
%%%
%%% The gap left is a host crash (power loss) after the inner store committed
%%% a write on its own (its flusher, a list, a full batch) but before the
%%% journal was synced: the inner store then has a row the journal lost, from
%%% a slot that was never confirmed. An unclean start (no `clean' marker)
%%% closes it: the writer keeps, per process, the highest confirmed
%%% assignment slot, and journals again, with its whole closure, every
%%% assignment above it that the inner store holds (so a slot reassigned
%%% after a crash restores as the inner store has it), and the definition of
%%% every process it has never journaled.
%%%
%%% Frames carry a magic, so a torn tail (nothing intact after the damage:
%%% a frame cut short, the zero fill a host crash leaves) is told from damage
%%% in the middle (an intact frame follows). A torn tail of the open segment
%%% is cut on start; anything else stops the writer, and stops a restore,
%%% with the file and offset. A failed append (a full local disk) is
%%% truncated away and refused: the write fails and the inner store is not
%%% written.
%%%
%%% The writer never touches the target. A shipper process copies each sealed
%%% segment to it (`.tmp', sync, rename, directory sync), then appends the
%%% file's name, size, CRC and a hash chain over every shipped file to
%%% `manifest.log' (synced; a torn last line is cut), then deletes the local
%%% copy. A restore checks size, CRC and chain, and refuses a result whose
%%% assignments skip a slot. A slow,
%%% stalled or full target only grows the local backlog; nothing is dropped.
%%% It never takes the target's filesystem above `max-fill-pct': at the
%%% ceiling, shipping pauses (`offsite_journal_ceiling' events) and resumes
%%% by itself.
%%%
%%% Export options (`irreplaceable-export'):
%%% <ul>
%%%   <li>`path': the target directory. Required.</li>
%%%   <li>`journal': the local journal directory. Default `<inner>-journal'.</li>
%%%   <li>`segment-bytes' (8 MiB), `segment-ms' (60 s): seal a segment at this
%%%       size or age.</li>
%%%   <li>`compress' (true): gzip segments on the target.</li>
%%%   <li>`max-fill-pct' (75): never ship past this fill of the target.</li>
%%%   <li>`ship-interval-ms' (1 s); failures back off to 5 minutes.</li>
%%%   <li>`fsync-ms' (200): sync an unsynced journal at least this often.</li>
%%% </ul>
-module(hb_store_offsite_journal).
-export([wrap/2, status/1, restore/2, restore/3, ship_now/1, clear_poison/1]).
-export([start/3, stop/3, reset/3, flush/3, scope/0, scope/1]).
-export([read/3, write/3, list/3, match/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-define(SEGMENT_BYTES, 8 * 1024 * 1024).
-define(SEGMENT_MS, 60000).
-define(MAX_FILL_PCT, 75).
-define(SHIP_MS, 1000).
-define(FSYNC_MS, 200).
-define(MAX_BACKOFF_MS, 300000).
-define(CALL_MS, 120000).
-define(ASSIGNMENTS, <<"~scheduler@1.0/assignments">>).
-define(MAGIC, <<"HBJ1">>).

%% @doc A store message that wraps `Inner' with an offsite journal configured
%% by `Export'. It carries the inner store's name, so it names one writer per
%% inner store, and fails loudly: a write the journal refuses must not fall
%% through to the next store in a list.
wrap(Inner = #{ <<"store-module">> := Mod }, Export) ->
    #{
        <<"store-module">> => ?MODULE,
        <<"name">> => maps:get(<<"name">>, Inner, hb_util:bin(Mod)),
        <<"inner">> => Inner,
        <<"export">> => Export,
        <<"store-failure">> => throw,
        % A refusal is final for that call: `hb_store' must not stop and
        % restart the store to retry it (that restarted the writer every
        % second on a full disk).
        <<"max-retries">> => 0
    }.

inner(#{ <<"inner">> := Inner }) -> hb_store:normalize_store(Inner).
inner_mod(Store) -> maps:get(<<"store-module">>, inner(Store)).

%%% Store callbacks.

start(Store, Req, Opts) ->
    ok = hb_store:start([inner(Store)], Req, Opts),
    _ = writer(Store),
    ok.

stop(Store, Req, Opts) ->
    catch call(Store, stop, 30000),
    stop_shipper(Store),
    hb_store:stop([inner(Store)], Req, Opts).

%% @doc Reset the inner store and the local journal. The target is never
%% touched.
reset(Store, Req, Opts) ->
    catch call(Store, stop, 30000),
    stop_shipper(Store),
    os:cmd("rm -rf " ++ journal_dir(Store)),
    hb_store:reset([inner(Store)], Req, Opts).

scope() -> local.
scope(_) -> local.

read(Store, Req, Opts) -> (inner_mod(Store)):read(inner(Store), Req, Opts).
list(Store, Req, Opts) -> (inner_mod(Store)):list(inner(Store), Req, Opts).
match(Store, Req, Opts) -> (inner_mod(Store)):match(inner(Store), Req, Opts).

%% @doc Journal the rows, then write them to the inner store, in the writer.
write(Store, Req, _Opts) when is_map(Req) ->
    Pairs =
        lists:sort(
            [ {path_bin(P), V} || {P, V} <- maps:to_list(Req) ]
        ),
    case call(Store, {append, Pairs}, ?CALL_MS) of
        ok -> ok;
        {error, Reason} -> {failure, {offsite_journal, Reason}};
        Other -> Other
    end.

%% @doc Sync the journal, then flush the inner store.
flush(Store, _Req, _Opts) ->
    case call(Store, flush, ?CALL_MS) of
        ok -> ok;
        {error, Reason} -> {failure, {offsite_journal, Reason}};
        Other -> Other
    end.

path_bin(P) when is_binary(P) -> P;
path_bin(P) -> hb_path:to_binary(P).

%% @doc The writer's and shipper's state.
status(Store) ->
    W = case catch call(Store, status, 5000) of
            M when is_map(M) -> M;
            Other -> #{ writer => Other }
        end,
    S = case hb_name:lookup({?MODULE, shipper, maps:get(<<"name">>, Store)}) of
            undefined -> #{ shipper => not_running };
            Pid ->
                Ref = make_ref(),
                Pid ! {status, self(), Ref},
                receive {Ref, St} -> St after 5000 -> #{ shipper => timeout } end
        end,
    maps:merge(W, S).

%% @doc Ship every sealed segment now and wait for the round to finish.
ship_now(Store) ->
    _ = call(Store, seal, ?CALL_MS),
    Pid = shipper(Store),
    Ref = make_ref(),
    Pid ! {ship_now, self(), Ref},
    receive {Ref, R} -> R after ?CALL_MS -> {error, timeout} end.

%%% Calls into the writer.

call(Store, Msg, Timeout) ->
    Pid = writer(Store),
    Ref = erlang:monitor(process, Pid),
    Pid ! {call, self(), Ref, Msg},
    receive
        {Ref, Reply} ->
            erlang:demonitor(Ref, [flush]),
            Reply;
        {'DOWN', Ref, process, Pid, Reason} ->
            {error, {writer_down, Reason}}
    after Timeout ->
        erlang:demonitor(Ref, [flush]),
        {error, timeout}
    end.

%% A caller never waits for the writer to start: it is registered first and
%% initialises after, so the first calls queue in its mailbox.
writer(Store = #{ <<"name">> := Name }) ->
    hb_name:singleton({?MODULE, writer, Name}, fun() -> writer_init(Store) end).

shipper(Store = #{ <<"name">> := Name }) ->
    hb_name:singleton({?MODULE, shipper, Name}, fun() -> shipper_init(Store) end).

stop_shipper(#{ <<"name">> := Name }) ->
    case hb_name:lookup({?MODULE, shipper, Name}) of
        undefined -> ok;
        Pid ->
            Ref = erlang:monitor(process, Pid),
            Pid ! stop,
            receive {'DOWN', Ref, process, Pid, _} -> ok
            after 60000 -> exit(Pid, kill), ok
            end
    end.

%%% Configuration.

export(Store) -> maps:get(<<"export">>, Store, #{}).
cfg(Key, Default, Store) ->
    case maps:get(Key, export(Store), Default) of
        V when is_integer(Default), is_binary(V) -> hb_util:int(V);
        V when is_integer(Default), is_float(V) -> trunc(V);
        V -> V
    end.

journal_dir(Store) ->
    case maps:get(<<"journal">>, export(Store), undefined) of
        undefined ->
            hb_util:list(maps:get(<<"name">>, inner(Store))) ++ "-journal";
        J -> hb_util:list(J)
    end.

target_dir(Store) -> hb_util:list(maps:get(<<"path">>, export(Store))).

compress(Store) -> hb_util:atom(cfg(<<"compress">>, true, Store)) =/= false.

%%% The writer.
%%%
%%% It never crashes on a refusal. Three states:
%%% - running: appends and confirms;
%%% - not started: the start marker (or the reconcile, or a rebase) could not
%%%   be journaled, as on a full disk: every call is refused and tries the
%%%   start again, so the writer runs once space is back;
%%% - refusing: a local segment is damaged, or the journal is poisoned. A
%%%   poisoning is written to `poisoned' in the journal directory and holds
%%%   across restarts until `clear_poison/1', which journals the whole inner
%%%   store again (a rebase) before the next write is taken.

writer_init(Store) ->
    process_flag(trap_exit, true),
    Dir = journal_dir(Store),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    case read_poison(Dir) of
        {poisoned, Reason} ->
            ?event(error, {offsite_journal_refusing, {poisoned, Reason}}),
            writer_refusing(Store, {journal_poisoned, Reason});
        none ->
            Clean = filelib:is_regular(filename:join(Dir, "clean")),
            _ = file:delete(filename:join(Dir, "clean")),
            {Sealed, Marks0, Token} = read_marks(Dir),
            % Every local file newer than the last seal (or not sealed at
            % all) may hold assignments the marks do not: read them, cut
            % torn tails, seal. A damaged file refuses the journal.
            case catch recover_local(Dir, Sealed, Marks0) of
                {'EXIT', {{offsite_journal_corrupt, File, Off}, _}} ->
                    writer_refusing(Store, {journal_corrupt, File, Off});
                {MaxN, Marks1} ->
                    S0 = #{
                        store => Store,
                        dir => Dir,
                        token => Token,
                        seg => max(Sealed, MaxN) + 1,
                        marks => Marks1,
                        pending => #{},
                        unflushed => false,
                        sealed => max(Sealed, MaxN),
                        fd => undefined,
                        off => 0,
                        opened => erlang:monotonic_time(millisecond),
                        dirty => false,
                        poisoned => false,
                        started => false,
                        appended => 0,
                        reconciled => 0,
                        rebased => 0,
                        errors => 0,
                        clean_start => Clean
                    },
                    S1 = ensure_started(write_marks(open_segment(S0))),
                    _ = shipper(Store),
                    erlang:send_after(cfg(<<"fsync-ms">>, ?FSYNC_MS, Store), self(), fsync_tick),
                    erlang:send_after(1000, self(), roll_tick),
                    writer_loop(S1)
            end
    end.

%% @doc Open the run: the start marker (whatever the last run wrote and never
%% confirmed is dropped by a restore from here on), then, after an unclean
%% stop, the reconcile, and a rebase when one is pending. A failure leaves the
%% writer not started, to try again at the next call.
ensure_started(S = #{ started := true }) -> S;
ensure_started(S = #{ poisoned := P }) when P =/= false -> S;
ensure_started(S = #{ clean_start := Clean, dir := Dir }) ->
    try
        S1 = case append({start, Clean}, S) of
                 {ok, SA} -> SA;
                 {error, R1, _} -> throw({start_refused, R1})
             end,
        S2 = case Clean of
                 true -> S1;
                 false -> reconcile(S1)
             end,
        S3 = case filelib:is_regular(filename:join(Dir, "rebase")) of
                 true -> rebase(S2);
                 false -> S2
             end,
        S3#{ started => true }
    catch
        throw:Why -> not_started(S, Why);
        error:{offsite_journal_reconcile_failed, Why} -> not_started(S, {reconcile, Why})
    end.

%% The attempt may have sealed or broken the segment it was writing: drop
%% it, so the next attempt opens a new one.
not_started(S, Why) ->
    ?event(error, {offsite_journal_not_started, Why}),
    case maps:get(fd, S) of
        undefined -> ok;
        Fd -> catch file:close(Fd)
    end,
    S#{ fd => undefined, dirty => false, errors => maps:get(errors, S) + 1 }.

writer_loop(S = #{ store := Store }) ->
    receive
        {call, From, Ref, Msg} when Msg =/= status, Msg =/= stop ->
            case ensure_started(S) of
                S1 = #{ started := true } -> handle(From, Ref, Msg, S1);
                S1 ->
                    From ! {Ref, refusal(S1, {error, journal_not_started})},
                    writer_loop(S1)
            end;
        {call, From, Ref, status} ->
            Status = maps:with([dir, token, seg, sealed, off, appended, poisoned,
                                started, reconciled, rebased, errors, clean_start], S),
            From ! {Ref, Status#{ processes => map_size(maps:get(marks, S)) }},
            writer_loop(S);
        {call, From, Ref, stop} ->
            _ = stop_writer(S),
            From ! {Ref, ok};
        fsync_tick ->
            S1 = sync_journal(S),
            erlang:send_after(cfg(<<"fsync-ms">>, ?FSYNC_MS, Store), self(), fsync_tick),
            writer_loop(S1);
        roll_tick ->
            erlang:send_after(1000, self(), roll_tick),
            writer_loop(maybe_roll(S));
        {'EXIT', _, _} ->
            writer_loop(S);
        _ ->
            writer_loop(S)
    end.

handle(From, Ref, {append, Pairs}, S) ->
    {Reply, S1} = append_and_write(Pairs, S),
    From ! {Ref, Reply},
    writer_loop(maybe_roll(S1));
handle(From, Ref, flush, S) ->
    {Reply, S1} = do_flush(S),
    From ! {Ref, Reply},
    writer_loop(S1);
handle(From, Ref, seal, S) ->
    S1 = roll(S),
    From ! {Ref, refusal(S1, ok)},
    writer_loop(S1);
handle(From, Ref, _Other, S) ->
    From ! {Ref, {error, unknown_call}},
    writer_loop(S).

%% @doc A writer that refuses every call (a damaged segment, or a poisoning
%% from an earlier run), without crashing: answering keeps it registered, so
%% nothing restarts it in a loop.
writer_refusing(Store, Reason) ->
    receive
        {call, From, Ref, status} ->
            From ! {Ref, #{ refusing => Reason }},
            writer_refusing(Store, Reason);
        {call, From, Ref, stop} ->
            From ! {Ref, ok};
        {call, From, Ref, _} ->
            From ! {Ref, {error, Reason}},
            writer_refusing(Store, Reason);
        _ ->
            writer_refusing(Store, Reason)
    end.

%% A poisoned journal refuses everything.
refusal(#{ poisoned := false }, Reply) -> Reply;
refusal(#{ poisoned := Reason }, _Reply) -> {error, {journal_poisoned, Reason}}.

%% @doc Poison the journal: refuse everything from now on, and, through the
%% `poisoned' file, in every later run until `clear_poison/1'.
poison(S = #{ poisoned := false, dir := Dir }, Reason) ->
    ?event(error, {offsite_journal_poisoned, Reason}),
    case write_synced(filename:join(Dir, "poisoned"), term_to_binary({poisoned, Reason})) of
        ok -> sync_dir(Dir);
        Err -> ?event(error, {offsite_journal_poison_marker_failed, Err})
    end,
    S#{ poisoned => Reason, errors => maps:get(errors, S) + 1 };
poison(S, _Reason) -> S.

read_poison(Dir) ->
    case file:read_file(filename:join(Dir, "poisoned")) of
        {ok, Bin} ->
            case catch binary_to_term(Bin) of
                {poisoned, Reason} -> {poisoned, Reason};
                _ -> {poisoned, unreadable_marker}
            end;
        _ -> none
    end.

%% @doc Clear a poisoning, after the cause is fixed. What was written while
%% the journal was failing cannot be told apart, so the next run journals
%% every row of the inner store again (a rebase) before it takes a write.
clear_poison(Store) ->
    Dir = journal_dir(Store),
    _ = (catch call(Store, stop, 30000)),
    stop_writer_process(Store),
    ok = write_synced(filename:join(Dir, "rebase"), <<>>),
    sync_dir(Dir),
    ok = case file:delete(filename:join(Dir, "poisoned")) of
             ok -> ok;
             {error, enoent} -> ok;
             E -> E
         end,
    sync_dir(Dir),
    ok.

stop_writer_process(#{ <<"name">> := Name }) ->
    case hb_name:lookup({?MODULE, writer, Name}) of
        undefined -> ok;
        Pid ->
            Ref = erlang:monitor(process, Pid),
            exit(Pid, kill),
            receive {'DOWN', Ref, process, Pid, _} -> ok end,
            hb_name:unregister({?MODULE, writer, Name})
    end.

%% @doc Journal every row of the inner store, then confirm them.
rebase(S = #{ store := Store, dir := Dir }) ->
    Inner = inner(Store),
    case maps:get(<<"store-module">>, Inner) of
        hb_store_lmdb -> ok;
        Other -> throw({rebase_unsupported, Other})
    end,
    #{ <<"db">> := DB } = hb_store:find(Inner),
    ok = elmdb:flush(DB),
    {ok, {S1, Batch, N}} =
        elmdb:fold(DB,
            fun(K, V, {SA, B, C}) when length(B) >= 500 ->
                    {append_rebase(B, SA), [{K, V}], C + 1};
               (K, V, {SA, B, C}) ->
                    {SA, [{K, V} | B], C + 1}
            end,
            {S, [], 0}),
    S2 = append_rebase(Batch, S1),
    S3 = case append({flushed}, S2) of
             {ok, SF} -> sync_journal(SF);
             {error, R, _} -> throw({rebase_failed, R})
         end,
    case maps:get(poisoned, S3) of
        false -> ok;
        Why -> throw({rebase_failed, Why})
    end,
    ok = file:delete(filename:join(Dir, "rebase")),
    sync_dir(Dir),
    ?event(warning, {offsite_journal_rebased, {rows, N}}),
    S3#{ rebased => N,
         marks => merge_marks(maps:get(pending, S3), maps:get(marks, S3)),
         pending => #{} }.

append_rebase([], S) -> S;
append_rebase(Rows, S) ->
    Pairs = lists:reverse(Rows),
    case append({w, Pairs}, S) of
        {ok, S1} -> S1#{ pending => mark(Pairs, maps:get(pending, S1)) };
        {error, R, _} -> throw({rebase_failed, R})
    end.

%% @doc Append the rows, then write them to the inner store. A refused append
%% writes nothing. The rows count as confirmed at the next flush.
append_and_write(_Pairs, S = #{ poisoned := P }) when P =/= false ->
    {refusal(S, ok), S};
append_and_write(Pairs, S = #{ store := Store }) ->
    case append({w, Pairs}, S) of
        {ok, S1} ->
            Inner = inner(Store),
            Mod = maps:get(<<"store-module">>, Inner),
            Res =
                try Mod:write(Inner, maps:from_list(Pairs), #{})
                catch C:R -> {error, {C, R}}
                end,
            {Res, S1#{ pending => mark(Pairs, maps:get(pending, S1)),
                       unflushed => true,
                       appended => maps:get(appended, S1) + 1 }};
        {error, Reason, S1} ->
            {{error, Reason}, S1}
    end.

append(_Rec, S = #{ poisoned := P }) when P =/= false ->
    {error, {journal_poisoned, P}, S};
append(Rec, S = #{ fd := undefined }) ->
    % The previous segment broke; start a new one.
    case catch open_segment(S#{ seg => maps:get(seg, S) + 1 }) of
        S1 = #{ fd := Fd } when Fd =/= undefined -> append(Rec, S1);
        _ -> {error, journal_unavailable, S#{ errors => maps:get(errors, S) + 1 }}
    end;
append(Rec, S = #{ fd := Fd, off := Off }) ->
    Frame = frame(Rec),
    case write_frame(Fd, Frame) of
        ok -> {ok, S#{ off => Off + byte_size(Frame), dirty => true }};
        {error, Reason} ->
            ?event(error, {offsite_journal_append_failed, Reason}),
            % Cut any partial frame away, then seal this segment: nothing is
            % ever appended after a failure in the same file.
            _ = file:position(Fd, Off),
            _ = file:truncate(Fd),
            S1 = seal_current(S),
            {error, Reason, S1#{ fd => undefined,
                                 errors => maps:get(errors, S1) + 1 }}
    end.

write_frame(Fd, Frame) ->
    case persistent_term:get({?MODULE, fail_write}, false) of
        true -> {error, enospc};   % test hook
        false -> file:write(Fd, Frame)
    end.

frame(Rec) ->
    Bin = term_to_binary(Rec),
    <<?MAGIC/binary, (byte_size(Bin)):32, (erlang:crc32(Bin)):32, Bin/binary>>.

%% @doc Confirm what was written since the last flush: append a `flushed'
%% marker, sync the journal, then flush the inner store. If the inner flush
%% fails, a `revoke' marker withdraws the confirmation and the journal stops.
do_flush(S = #{ poisoned := P }) when P =/= false -> {refusal(S, ok), S};
do_flush(S = #{ store := Store }) ->
    Marked =
        case maps:get(unflushed, S) of
            false -> {ok, S};
            true -> append({flushed}, S)
        end,
    case Marked of
        {error, Reason, S1} -> {{error, Reason}, S1};
        {ok, S1} ->
            case sync_journal(S1) of
                S2 = #{ poisoned := false } ->
                    case catch hb_store:flush(inner(Store), #{}) of
                        ok ->
                            {ok, S2#{ marks => merge_marks(maps:get(pending, S2),
                                                           maps:get(marks, S2)),
                                      pending => #{}, unflushed => false }};
                        Error ->
                            S3 = case append({revoke}, S2) of
                                     {ok, SR} -> sync_journal(SR);
                                     {error, _, SR} -> SR
                                 end,
                            {{error, {inner_flush_failed, Error}},
                             poison(S3, {inner_flush_failed, Error})}
                    end;
                S2 -> {refusal(S2, ok), S2}
            end
    end.

merge_marks(New, Marks) ->
    maps:fold(fun(P, N, M) -> M#{ P => max(N, maps:get(P, M, -1)) } end, Marks, New).

%% @doc Sync the journal. A failed sync is never retried (on Linux a retry
%% can report success for pages the kernel already dropped): it poisons the
%% journal.
sync_journal(S = #{ poisoned := P }) when P =/= false -> S;
sync_journal(S = #{ dirty := false }) -> S;
sync_journal(S = #{ fd := undefined }) -> S#{ dirty := false };
sync_journal(S = #{ fd := Fd }) ->
    case datasync(Fd) of
        ok -> S#{ dirty := false };
        {error, Reason} -> poison(S, {sync_failed, Reason})
    end.

datasync(Fd) ->
    case persistent_term:get({?MODULE, fail_sync}, false) of
        true -> {error, eio};   % test hook
        false -> file:datasync(Fd)
    end.

maybe_roll(S = #{ fd := undefined }) -> S;
maybe_roll(S = #{ off := 0 }) -> S;
maybe_roll(S = #{ store := Store, off := Off, opened := Opened }) ->
    Age = erlang:monotonic_time(millisecond) - Opened,
    case Off >= cfg(<<"segment-bytes">>, ?SEGMENT_BYTES, Store)
            orelse Age >= cfg(<<"segment-ms">>, ?SEGMENT_MS, Store) of
        true -> roll(S);
        false -> S
    end.

%% @doc Seal the active segment (if it holds anything) and open the next.
roll(S = #{ poisoned := P }) when P =/= false -> S;
roll(S = #{ off := 0, fd := Fd }) when Fd =/= undefined -> S;
roll(S) ->
    case seal_current(S) of
        S1 = #{ poisoned := false } -> open_segment(S1#{ seg => maps:get(seg, S1) + 1 });
        S1 -> S1
    end.

%% Seal: sync (a failure poisons, and the file stays as it is), close, record
%% the marks as of this segment, rename it to its sealed name, sync the
%% directory. A crash between the steps leaves an `open-' file at or below the
%% sealed number, which the next start reads and seals again.
seal_current(S = #{ fd := undefined }) -> S;
seal_current(S = #{ fd := Fd, dir := Dir, seg := N, store := Store }) ->
    case S of
        #{ poisoned := false, dirty := true } ->
            case datasync(Fd) of
                ok -> seal_synced(S#{ dirty := false }, Fd, Dir, N, Store);
                {error, Reason} -> poison(S, {sync_failed, Reason})
            end;
        #{ poisoned := false } -> seal_synced(S, Fd, Dir, N, Store);
        _ -> S
    end.

seal_synced(S, Fd, Dir, N, Store) ->
    _ = file:close(Fd),
    Open = open_name(Dir, N),
    case maps:get(off, S) of
        0 ->
            _ = file:delete(Open),
            S#{ fd => undefined };
        _ ->
            % The marks only shorten the next start's scan, so a full disk
            % that refuses them does not stop the seal.
            S1 = write_marks(S#{ sealed => N }),
            case file:rename(Open, seg_name(Dir, N)) of
                ok -> sync_dir(Dir);
                {error, Reason} ->
                    ?event(error, {offsite_journal_seal_failed, Reason})
            end,
            case hb_name:lookup({?MODULE, shipper, maps:get(<<"name">>, Store)}) of
                undefined -> ok;
                Pid -> Pid ! wake
            end,
            S1#{ fd => undefined }
    end.

open_segment(S = #{ dir := Dir, seg := N }) ->
    case file:open(open_name(Dir, N), [read, write, raw, binary]) of
        {ok, Fd} ->
            {ok, _} = file:position(Fd, eof),
            sync_dir(Dir),
            S#{ fd => Fd, off => 0, opened => erlang:monotonic_time(millisecond) };
        {error, Reason} ->
            ?event(error, {offsite_journal_open_failed, Reason}),
            S#{ fd => undefined }
    end.

%% @doc Sync a directory, so a rename or a new file in it survives a host
%% crash. Erlang cannot open a directory, so this is `sync <dir>' (fsync).
sync_dir(Dir) ->
    _ = os:cmd("sync " ++ hb_util:list(Dir) ++ " 2>/dev/null"),
    ok.

stop_writer(S = #{ poisoned := P }) when P =/= false -> S;
stop_writer(S0) ->
    {_, S} = do_flush(S0),
    S1 =
        case append({stop}, S) of
            {ok, SA} -> SA;
            {error, _, SA} -> SA
        end,
    S2 = seal_current(sync_journal(S1)),
    _ = write_marks(S2),
    Dir = maps:get(dir, S2),
    case maps:get(poisoned, S2) of
        false ->
            case file:open(filename:join(Dir, "clean"), [write, raw]) of
                {ok, F} ->
                    _ = file:sync(F),
                    _ = file:close(F),
                    sync_dir(Dir);
                _ -> ok
            end;
        _ -> ok
    end,
    S2.

%%% Marks: per process, the highest assignment slot journaled and confirmed.

mark(Pairs, Marks) ->
    lists:foldl(
        fun({Path, _}, M) ->
            case assignment_key(Path) of
                {P, N} -> M#{ P => max(N, maps:get(P, M, -1)) };
                false -> M
            end
        end,
        Marks,
        Pairs
    ).

assignment_key(<<"~scheduler@1.0/assignments/", Rest/binary>>) ->
    case binary:split(Rest, <<"/">>, [global]) of
        [P, N] ->
            case catch binary_to_integer(N) of
                I when is_integer(I) -> {P, I};
                _ -> false
            end;
        _ -> false
    end;
assignment_key(_) -> false.

read_marks(Dir) ->
    case file:read_file(filename:join(Dir, "marks")) of
        {ok, Bin} ->
            case catch binary_to_term(Bin) of
                {marks, Sealed, Marks, Token} -> {Sealed, Marks, Token};
                _ -> {-1, #{}, new_token()}
            end;
        _ -> {-1, #{}, new_token()}
    end.

write_marks(S = #{ dir := Dir, sealed := Sealed, marks := Marks, token := Token }) ->
    File = filename:join(Dir, "marks"),
    Tmp = File ++ ".tmp",
    case write_synced(Tmp, term_to_binary({marks, Sealed, Marks, Token})) of
        ok -> ok = file:rename(Tmp, File), sync_dir(Dir);
        {error, Reason} -> ?event(error, {offsite_journal_marks_failed, Reason})
    end,
    S.

new_token() -> hb_util:to_hex(crypto:strong_rand_bytes(4)).

token_bin(T) -> hb_util:bin(T).

%% @doc Recover local journal files: every `open-' file, and `seg-' files
%% above the last seal. Cut a torn tail; refuse (raise) on damage anywhere
%% else. Fold the confirmed assignments into the marks, and seal the file.
%% Returns the highest segment number seen.
recover_local(Dir, Sealed, Marks) ->
    Files = local_files(Dir),
    {Max, Replay} =
        lists:foldl(
            fun({N, Kind, File}, {Max, R}) ->
                case Kind =:= open orelse N > Sealed of
                    false ->
                        % Sealed and not shipped yet: it must still be whole.
                        {ok, SBin} = file:read_file(File),
                        case classify_tail(SBin, valid_prefix(SBin, 0)) of
                            whole -> {max(Max, N), R};
                            _ ->
                                ?event(error, {offsite_journal_corrupt, {file, File}}),
                                erlang:error({offsite_journal_corrupt, File,
                                              valid_prefix(SBin, 0)})
                        end;
                    true ->
                        {ok, Bin} = file:read_file(File),
                        Valid = valid_prefix(Bin, 0),
                        case {classify_tail(Bin, Valid), Kind} of
                            {whole, _} -> ok;
                            {torn, open} ->
                                ?event(warning, {offsite_journal_torn_tail,
                                    {file, File}, {kept, Valid}, {size, byte_size(Bin)}}),
                                {ok, Fd} = file:open(File, [read, write, raw, binary]),
                                {ok, _} = file:position(Fd, Valid),
                                ok = file:truncate(Fd),
                                ok = file:sync(Fd),
                                ok = file:close(Fd);
                            % A sealed segment was synced whole: anything
                            % short of whole is damage.
                            _ ->
                                ?event(error, {offsite_journal_corrupt, {file, File},
                                               {offset, Valid}}),
                                erlang:error({offsite_journal_corrupt, File, Valid})
                        end,
                        R1 = replay(records(binary:part(Bin, 0, Valid)), R),
                        case {Kind, Valid} of
                            {open, 0} -> _ = file:delete(File);
                            {open, _} -> ok = file:rename(File, seg_name(Dir, N));
                            _ -> ok
                        end,
                        {max(Max, N), R1}
                end
            end,
            {-1, replay_init(fun(Pairs, M) -> mark(Pairs, M) end, Marks)},
            Files
        ),
    sync_dir(Dir),
    {Max, replay_finish(Replay)}.

%%% Replay: what a journal confirms. Rows count once a `flushed' marker
%%% follows them and no `revoke' follows that; rows written before a new run
%%% (`start'), a clean `stop' or the end of the journal without a `flushed'
%%% were never confirmed and are dropped.

replay_init(Apply, Acc) ->
    #{ apply => Apply, acc => Acc, pending => [], held => [], dropped => 0, records => 0 }.

replay(Recs, R) -> lists:foldl(fun replay_step/2, R, Recs).

replay_step(Rec, R = #{ records := C }) -> step(Rec, R#{ records => C + 1 }).

step({w, Pairs}, R = #{ pending := P }) -> R#{ pending => [Pairs | P] };
step({flushed}, R = #{ pending := P }) -> (apply_held(R))#{ held => P, pending => [] };
step({revoke}, R) -> R#{ held => [] };
step(_StartOrStop, R) -> drop_pending(apply_held(R)).

apply_held(R = #{ held := H, apply := F, acc := Acc }) ->
    R#{ acc => lists:foldl(F, Acc, lists:reverse(H)), held => [] }.

drop_pending(R = #{ pending := P, dropped := D }) ->
    R#{ pending => [], dropped => D + length(P) }.

replay_finish(R) -> maps:get(acc, drop_pending(apply_held(R))).

%% @doc The journal files in a directory, oldest first.
local_files(Dir) ->
    {ok, Names} = file:list_dir(Dir),
    lists:sort(
        lists:filtermap(
            fun(Name) ->
                case parse_local(Name) of
                    {N, Kind} -> {true, {N, Kind, filename:join(Dir, Name)}};
                    false -> false
                end
            end,
            Names
        )
    ).

parse_local("seg-" ++ Rest) -> parse_num(Rest, seg);
parse_local("open-" ++ Rest) -> parse_num(Rest, open);
parse_local(_) -> false.

parse_num(Rest, Kind) ->
    case string:split(Rest, ".log") of
        [Num, ""] ->
            case catch list_to_integer(Num) of
                N when is_integer(N) -> {N, Kind};
                _ -> false
            end;
        _ -> false
    end.

open_name(Dir, N) -> filename:join(Dir, "open-" ++ pad(N) ++ ".log").
seg_name(Dir, N) -> filename:join(Dir, "seg-" ++ pad(N) ++ ".log").
pad(N) -> lists:flatten(io_lib:format("~12..0B", [N])).

%%% Frames: `HBJ1', length, CRC32, term. The magic lets a reader tell a torn
%%% tail from damage in the middle: after the last whole frame, a torn tail
%%% holds no intact frame; damage in the middle is followed by one.

valid_prefix(Bin, Off) ->
    case Bin of
        <<_:Off/binary, "HBJ1", Len:32, Crc:32, Frame:Len/binary, _/binary>> when Len > 0 ->
            case erlang:crc32(Frame) of
                Crc -> valid_prefix(Bin, Off + 12 + Len);
                _ -> Off
            end;
        _ -> Off
    end.

%% @doc What follows the whole frames of a file: nothing (`whole'), a torn
%% tail (no intact frame anywhere after: zeros, a frame cut short, the zero
%% fill a host crash leaves), or damage in the middle (`corrupt': an intact
%% frame follows the first bad one).
classify_tail(Bin, Valid) when Valid == byte_size(Bin) -> whole;
classify_tail(Bin, Valid) ->
    <<_:Valid/binary, Rest/binary>> = Bin,
    Later = [ Pos || {Pos, _} <- binary:matches(Rest, ?MAGIC), Pos > 0 ],
    case lists:any(fun(Pos) -> valid_prefix(Rest, Pos) > Pos end, Later) of
        true -> corrupt;
        false -> torn
    end.

records(<<"HBJ1", Len:32, _Crc:32, Bin:Len/binary, Rest/binary>>) when Len > 0 ->
    [binary_to_term(Bin) | records(Rest)];
records(_) -> [].

%%% Reconcile: after an unclean start, journal every assignment the inner
%%% store holds above the confirmed marks, with its closure, and the
%%% definition of every process with no mark, then confirm them: they are
%%% what the inner store, which survived, holds. A slot that was journaled
%%% but never confirmed (the run before was killed while assigning it) is
%%% therefore journaled again as the inner store has it -- reassigned, or
%%% not there at all, in which case no restore brings it back.

reconcile(S = #{ store := Store, marks := Marks }) ->
    Inner = inner(Store),
    Procs =
        case hb_store:list([Inner], ?ASSIGNMENTS, #{}) of
            {ok, Ps} -> Ps;
            _ -> []
        end,
    {S1, Count} =
        lists:foldl(
            fun(P, {SAcc, C}) ->
                Mark = maps:get(P, Marks, undefined),
                Slots =
                    case hb_store:list([Inner], <<?ASSIGNMENTS/binary, "/", P/binary>>, #{}) of
                        {ok, L} ->
                            lists:sort([ binary_to_integer(X) || X <- L,
                                         is_integer(catch binary_to_integer(X)) ]);
                        _ -> []
                    end,
                Missing = [ N || N <- Slots, Mark =:= undefined orelse N > Mark ],
                Def =
                    case Mark of
                        undefined -> closure(Inner, P);
                        _ -> []
                    end,
                SDef = journal_rows(Def, SAcc),
                SNext =
                    lists:foldl(
                        fun(N, SS) ->
                            Key = <<?ASSIGNMENTS/binary, "/", P/binary, "/",
                                    (integer_to_binary(N))/binary>>,
                            journal_rows(closure(Inner, Key), SS)
                        end,
                        SDef,
                        Missing
                    ),
                {SNext, C + length(Missing)}
            end,
            {S, 0},
            Procs
        ),
    case Count of
        0 -> ok;
        _ -> ?event(warning, {offsite_journal_reconciled, {assignments, Count}})
    end,
    S2 = case append({flushed}, S1) of
             {ok, SF} -> sync_journal(SF);
             {error, Reason, _} -> erlang:error({offsite_journal_reconcile_failed, Reason})
         end,
    case maps:get(poisoned, S2) of
        false -> ok;
        Why -> erlang:error({offsite_journal_reconcile_failed, Why})
    end,
    S2#{ reconciled => Count,
         marks => merge_marks(maps:get(pending, S2), maps:get(marks, S2)),
         pending => #{} }.

journal_rows([], S) -> S;
journal_rows(Rows, S) ->
    case append({w, Rows}, S) of
        {ok, S1} -> S1#{ pending => mark(Rows, maps:get(pending, S1)) };
        {error, Reason, _} -> erlang:error({offsite_journal_reconcile_failed, Reason})
    end.

%% @doc Every raw row reachable from `Path' in a store: the row itself or the
%% rows below it, and, through each `ao:<ID>', `ao://with-commitments=' and
%% `ni:' value, the rows they name.
closure(Store, Path) ->
    {Rows, _} = closure(Store, Path, {[], #{}}),
    lists:reverse(Rows).
closure(_Store, Path, {Rows, Seen}) when is_map_key(Path, Seen) -> {Rows, Seen};
closure(Store, Path, {Rows, Seen0}) ->
    Seen = Seen0#{ Path => true },
    case hb_store:read([Store], #{ <<"read">> => Path }, #{}) of
        {ok, Value} ->
            follow(Store, Value, {[{Path, Value} | Rows], Seen});
        {composite, Children} ->
            lists:foldl(
                fun({Key, Value}, Acc = {R, Sn}) ->
                        Sub = <<Path/binary, "/", Key/binary>>,
                        case is_map_key(Sub, Sn) of
                            true -> Acc;
                            false -> follow(Store, Value, {[{Sub, Value} | R], Sn#{ Sub => true }})
                        end;
                   (Key, Acc) ->
                        closure(Store, <<Path/binary, "/", Key/binary>>, Acc)
                end,
                {Rows, Seen},
                Children
            );
        _ -> {Rows, Seen}
    end.

follow(Store, <<"ao://with-commitments=", IDs/binary>>, Acc) ->
    lists:foldl(
        fun(ID, A) when ?IS_ID(ID) -> closure(Store, ID, A);
           (_, A) -> A
        end,
        Acc,
        binary:split(IDs, <<",">>, [global]));
follow(Store, <<"ao:", ID/binary>>, Acc) when ?IS_ID(ID) -> closure(Store, ID, Acc);
follow(Store, Name = <<"ni:", _/binary>>, Acc) ->
    closure(Store, hb_escape:encode_path_component(Name), Acc);
follow(_Store, _Value, Acc) -> Acc.

%%% The shipper.

shipper_init(Store) ->
    erlang:send_after(0, self(), tick),
    shipper_loop(Store, #{ shipped => 0, backoff => 0, state => idle, chain => undefined }).

shipper_loop(Store, St) ->
    receive
        stop -> ok;
        {status, From, Ref} ->
            From ! {Ref, #{ shipper => maps:without([chain], St#{ backlog => backlog(Store) }) }},
            shipper_loop(Store, St);
        {ship_now, From, Ref} ->
            St1 = ship_round(Store, St),
            From ! {Ref, maps:get(state, St1)},
            shipper_loop(Store, St1);
        wake ->
            shipper_loop(Store, ship_round(Store, St));
        tick ->
            St1 = ship_round(Store, St),
            Delay =
                case maps:get(backoff, St1) of
                    0 -> cfg(<<"ship-interval-ms">>, ?SHIP_MS, Store);
                    B -> B
                end,
            erlang:send_after(Delay, self(), tick),
            shipper_loop(Store, St1)
    end.

backlog(Store) ->
    Files = try local_files(journal_dir(Store)) catch _:_ -> [] end,
    Segs = [ F || {_, seg, F} <- Files ],
    #{ segments => length(Segs),
       bytes => lists:sum([ filelib:file_size(F) || F <- Segs ]) }.

%% @doc Ship the sealed segments, oldest first, stopping at the first that
%% cannot go.
ship_round(Store, St) ->
    try
        Segs = [ {N, F} || {N, seg, F} <- local_files(journal_dir(Store)) ],
        Chain =
            case maps:get(chain, St) of
                undefined -> manifest_tip(target_dir(Store));
                Known -> Known
            end,
        ship_segs(Segs, Store, St#{ state => idle, chain => Chain })
    catch C:R ->
        ?event(error, {offsite_journal_ship_failed, C, R}),
        failed(St#{ chain => undefined }, {C, R})
    end.

ship_segs([], _Store, St) -> St#{ backoff => 0 };
ship_segs([{N, File} | Rest], Store, St) ->
    case ship_one(N, File, Store, maps:get(chain, St)) of
        {ok, Chain} ->
            ship_segs(Rest, Store, St#{ shipped => maps:get(shipped, St) + 1, chain => Chain });
        {ceiling, Info} ->
            ?event(warning, {offsite_journal_ceiling, Info}),
            St#{ state => {ceiling, Info}, backoff => ?MAX_BACKOFF_MS div 10 };
        {error, Reason} ->
            ?event(error, {offsite_journal_ship_failed, Reason}),
            % The manifest's tip is read again before the next attempt.
            failed(St#{ chain => undefined }, Reason)
    end.

failed(St, Reason) ->
    B = case maps:get(backoff, St) of 0 -> 1000; B0 -> min(B0 * 2, ?MAX_BACKOFF_MS) end,
    St#{ state => {error, Reason}, backoff => B }.

%% @doc The hash chain's tip: the last whole line of the target's manifest.
%% A torn last line (a crash in the middle of an append) is cut away here,
%% so the next line starts on a line of its own.
manifest_tip(Target) ->
    File = filename:join(Target, "manifest.log"),
    case file:read_file(File) of
        {ok, <<>>} -> <<>>;
        {ok, Bin} ->
            {Lines, Torn} = manifest_lines(Bin),
            case Torn of
                <<>> -> ok;
                _ ->
                    ?event(warning, {offsite_journal_torn_manifest_line, Torn}),
                    {ok, F} = file:open(File, [read, write, raw, binary]),
                    {ok, _} = file:position(F, byte_size(Bin) - byte_size(Torn)),
                    ok = file:truncate(F),
                    ok = file:sync(F),
                    ok = file:close(F)
            end,
            case Lines of
                [] -> <<>>;
                _ -> element(4, parse_line(lists:last(Lines)))
            end;
        {error, enoent} -> <<>>;
        {error, Reason} -> erlang:error({manifest_unreadable, Reason})
    end.

%% Whole lines, and the torn fragment after the last newline.
manifest_lines(Bin) ->
    Parts = binary:split(Bin, <<"\n">>, [global]),
    {lists:droplast(Parts) -- [<<>>], lists:last(Parts)}.

parse_line(Line) ->
    case binary:split(Line, <<" ">>, [global]) of
        [Name, Size, Crc, Chain] ->
            {Name, binary_to_integer(Size), binary_to_integer(Crc), Chain};
        _ -> erlang:error({manifest_line, Line})
    end.

chain(Prev, Body) ->
    hb_util:to_hex(crypto:hash(sha256, <<Prev/binary, (crypto:hash(sha256, Body))/binary>>)).

ship_one(N, File, Store, Prev) ->
    Target = target_dir(Store),
    {ok, Raw} = file:read_file(File),
    % Never ship a damaged segment: the offsite copy would carry the damage
    % under a matching CRC.
    case classify_tail(Raw, valid_prefix(Raw, 0)) of
        whole -> ok;
        _ -> erlang:error({offsite_journal_corrupt, File, valid_prefix(Raw, 0)})
    end,
    Body = case compress(Store) of true -> zlib:gzip(Raw); false -> Raw end,
    {_, _, Token} = read_marks(journal_dir(Store)),
    Name = "seg-" ++ hb_util:list(Token) ++ "-" ++ pad(N) ++ ".log" ++
        case compress(Store) of true -> ".gz"; false -> "" end,
    Chain = chain(Prev, Body),
    maybe
        ok ?= filelib:ensure_dir(filename:join(Target, "x")),
        ok ?= space_ok(Target, byte_size(Body),
                       cfg(<<"max-fill-pct">>, ?MAX_FILL_PCT, Store)),
        Final = filename:join(Target, Name),
        Tmp = Final ++ ".tmp",
        ok ?= write_synced(Tmp, Body),
        ok ?= file:rename(Tmp, Final),
        ok ?= sync_dir(Target),
        Line = io_lib:format("~s ~B ~B ~s~n",
                             [Name, byte_size(Body), erlang:crc32(Body), Chain]),
        ok ?= append_synced(filename:join(Target, "manifest.log"), Line),
        ok ?= sync_dir(Target),
        ok ?= file:delete(File),
        {ok, Chain}
    end.

%% @doc Whether `Bytes' more fit on the filesystem of `Dir' below `MaxPct'.
space_ok(Dir, Bytes, MaxPct) ->
    case df(Dir) of
        {ok, Used, Avail} ->
            Total = Used + Avail,
            After = Used + (Bytes div 1024) + 1,
            case Total > 0 andalso After * 100 =< MaxPct * Total of
                true -> ok;
                false ->
                    {ceiling, #{ dir => hb_util:bin(Dir), used_kb => Used,
                                 total_kb => Total, max_fill_pct => MaxPct }}
            end;
        Error -> Error
    end.

df(Dir) ->
    Out = os:cmd("df -Pk " ++ Dir ++ " 2>/dev/null"),
    case string:lexemes(Out, "\n") of
        [_Header, Line | _] ->
            case string:lexemes(Line, " ") of
                [_Fs, _Blocks, Used, Avail | _] ->
                    {ok, list_to_integer(Used), list_to_integer(Avail)};
                _ -> {error, {df, Out}}
            end;
        _ -> {error, {df, Out}}
    end.

write_synced(File, Data) ->
    maybe
        {ok, F} ?= file:open(File, [write, raw, binary]),
        R = maybe
                ok ?= file:write(F, Data),
                file:sync(F)
            end,
        _ = file:close(F),
        case R of
            ok -> ok;
            Err -> _ = file:delete(File), Err
        end
    end.

append_synced(File, Data) ->
    maybe
        {ok, F} ?= file:open(File, [append, raw, binary]),
        R = maybe
                ok ?= file:write(F, Data),
                file:sync(F)
            end,
        _ = file:close(F),
        R
    end.

%%% Restore.

%% @doc Replay an export into a store: every segment the target's manifest
%% lists, in order, each checked against its size, CRC and the manifest's
%% hash chain, then any local journal segments not yet shipped. Only
%% confirmed writes are applied (see Replay above). Stops with an error on
%% damage anywhere but the torn tail of a local segment, and after the
%% replay refuses a result whose assignments skip a slot for any process.
restore(Sources, TargetStore) -> restore(Sources, TargetStore, #{}).
restore(Source, TargetStore, Opts)
        when is_binary(Source); is_list(Source), is_integer(hd(Source)) ->
    restore([Source], TargetStore, Opts);
restore(Sources, TargetStore, Opts) when is_list(Sources) ->
    try
        Files = lists:append([ source_files(hb_util:list(Src)) || Src <- Sources ]),
        {_Seen, Ordered} =
            lists:foldl(
                fun(F = {Key, _, _}, {Sn, Acc}) ->
                    case is_map_key(Key, Sn) of
                        true -> {Sn, Acc};
                        false -> {Sn#{ Key => true }, [F | Acc]}
                    end
                end,
                {#{}, []},
                Files
            ),
        Apply =
            fun(Pairs, {Rows, Procs}) ->
                ok = hb_store:write(TargetStore, maps:from_list(Pairs), Opts),
                {Rows + length(Pairs), mark(Pairs, Procs)}
            end,
        R0 = replay_init(Apply, {0, #{}}),
        {R1, Counts} =
            lists:foldl(
                fun({_, File, Check}, {R, C}) -> restore_file(File, Check, R, C) end,
                {R0, #{ files => 0, torn => 0 }},
                lists:reverse(Ordered)
            ),
        R2 = drop_pending(apply_held(R1)),
        {Rows, Procs} = maps:get(acc, R2),
        _ = hb_store:flush(TargetStore, Opts),
        Report = Counts#{ rows => Rows, records => maps:get(records, R2),
                          unconfirmed => maps:get(dropped, R2) },
        case gaps(TargetStore, maps:keys(Procs)) of
            [] -> {ok, Report};
            Gaps -> {error, {gaps, Gaps}}
        end
    catch throw:{restore_error, Error} -> {error, Error}
    end.

%% @doc Processes whose assignments in a store skip a slot between their
%% lowest and highest, with the missing slots.
gaps(Store, Procs) ->
    lists:filtermap(
        fun(P) ->
            Slots =
                case hb_store:list([Store], <<?ASSIGNMENTS/binary, "/", P/binary>>, #{}) of
                    {ok, L} -> lists:sort([ binary_to_integer(X) || X <- L,
                                            is_integer(catch binary_to_integer(X)) ]);
                    _ -> []
                end,
            case Slots of
                [] -> false;
                _ ->
                    case lists:seq(hd(Slots), lists:last(Slots)) -- Slots of
                        [] -> false;
                        Missing -> {true, {P, Missing}}
                    end
            end
        end,
        Procs).

%% A source is an export directory (it has a manifest) or a local journal.
source_files(Dir) ->
    case file:read_file(filename:join(Dir, "manifest.log")) of
        {ok, Manifest} ->
            {Lines, _Torn} = manifest_lines(Manifest),
            {Files, _} =
                lists:mapfoldl(
                    fun(Line, Prev) ->
                        {Name, Size, Crc, Chain} =
                            try parse_line(Line)
                            catch _:_ -> throw({restore_error, {manifest_line, Line}})
                            end,
                        {{shipped_key(Name), filename:join(Dir, Name), {Size, Crc, Prev, Chain}},
                         Chain}
                    end,
                    <<>>,
                    Lines),
            Files;
        _ ->
            {_, _, Token} = read_marks(Dir),
            [ {{token_bin(Token), N}, F, {local, Kind}} || {N, Kind, F} <- local_files(Dir) ]
    end.

shipped_key(Name) ->
    [<<"seg">>, Token, Num | _] =
        binary:split(hb_util:bin(Name), [<<"-">>, <<".">>], [global]),
    {Token, binary_to_integer(Num)}.

restore_file(File, Check, R, C) ->
    Body =
        case file:read_file(File) of
            {ok, B} -> B;
            Error -> throw({restore_error, {File, Error}})
        end,
    case Check of
        {local, _} -> ok;
        {Size, _, _, _} when byte_size(Body) =/= Size -> throw({restore_error, {bad_size, File}});
        {_, Crc, Prev, Chain} ->
            case erlang:crc32(Body) of
                Crc -> ok;
                _ -> throw({restore_error, {bad_crc, File}})
            end,
            case chain(Prev, Body) of
                Chain -> ok;
                _ -> throw({restore_error, {manifest_chain, File}})
            end
    end,
    Plain = case Body of <<31, 139, _/binary>> -> zlib:gunzip(Body); _ -> Body end,
    Valid = valid_prefix(Plain, 0),
    Torn =
        case {classify_tail(Plain, Valid), Check} of
            {whole, _} -> 0;
            % Only the segment being written can be torn; a sealed one was
            % synced whole, and a shipped one checked against its manifest.
            {torn, {local, open}} -> 1;
            _ -> throw({restore_error, {corrupt, File, Valid}})
        end,
    {replay(records(binary:part(Plain, 0, Valid)), R),
     C#{ files => maps:get(files, C) + 1, torn => maps:get(torn, C) + Torn }}.

%%% Tests

test_setup(Tag, Export) ->
    Inner = hb_test_utils:test_store(hb_store_lmdb, Tag),
    Target = <<(maps:get(<<"name">>, Inner))/binary, "-export">>,
    Store = wrap(Inner, maps:merge(#{ <<"path">> => Target }, Export)),
    ok = hb_store:start([Store], #{}, #{}),
    {Store, Inner, Target}.

rows(Prefix, From, To) ->
    maps:from_list(
        [ {<<Prefix/binary, (integer_to_binary(I))/binary>>,
           <<"value-", (integer_to_binary(I))/binary>>}
        || I <- lists:seq(From, To) ]).

write_rows(Store, Rows) ->
    maps:foreach(fun(K, V) -> ok = hb_store:write([Store], #{ K => V }, #{}) end, Rows).

assert_rows(Store, Rows) ->
    maps:foreach(
        fun(K, V) -> ?assertEqual({ok, V}, hb_store:read([Store], K, #{})) end,
        Rows).

kill_writer(#{ <<"name">> := Name }) ->
    Pid = hb_name:lookup({?MODULE, writer, Name}),
    Ref = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, process, Pid, _} -> ok end,
    hb_name:unregister({?MODULE, writer, Name}).

fresh_store(Tag) ->
    S = hb_test_utils:test_store(hb_store_lmdb, Tag),
    ok = hb_store:start([S], #{}, #{}),
    S.

roundtrip_test() ->
    {Store, _Inner, Target} = test_setup(<<"oj-rt">>, #{}),
    Rows = rows(<<"k/">>, 1, 200),
    write_rows(Store, Rows),
    ok = flush(Store, #{}, #{}),
    ?assertEqual(idle, ship_now(Store)),
    Fresh = fresh_store(<<"oj-rt-restore">>),
    {ok, #{ rows := 200 }} = restore(Target, Fresh),
    assert_rows(Fresh, Rows),
    ok = hb_store:stop([Store], #{}, #{}),
    ?assert(filelib:is_regular(filename:join(journal_dir(Store), "clean"))).

%% @doc A writer killed mid-stream, with a torn and a zero-filled tail on its
%% open segment, keeps every whole frame, appends nothing after the damage,
%% and restores everything written before and after.
torn_tail_test() ->
    {Store, _Inner, Target} = test_setup(<<"oj-torn">>, #{}),
    Before = rows(<<"a/">>, 1, 50),
    write_rows(Store, Before),
    ok = flush(Store, #{}, #{}),
    kill_writer(Store),
    Dir = journal_dir(Store),
    [{_, open, Open}] = [ F || F = {_, open, _} <- local_files(Dir) ],
    {ok, Fd} = file:open(Open, [append, raw, binary]),
    % A frame cut short, then the zero fill a host crash leaves.
    ok = file:write(Fd, <<200:32, 12345:32, "partial">>),
    ok = file:write(Fd, <<0:4096>>),
    ok = file:close(Fd),
    After = rows(<<"b/">>, 1, 50),
    write_rows(Store, After),
    ok = flush(Store, #{}, #{}),
    ?assertEqual(idle, ship_now(Store)),
    Fresh = fresh_store(<<"oj-torn-restore">>),
    {ok, #{ rows := 100, torn := 0 }} = restore(Target, Fresh),
    assert_rows(Fresh, maps:merge(Before, After)).

%% @doc An unclean start journals what the inner store holds above the marks:
%% here an assignment written to the inner store behind the journal's back,
%% with the message and blob it links, and a process the journal never saw.
reconcile_test() ->
    {Store, Inner, Target} = test_setup(<<"oj-rec">>, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Q = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Msg = hb_util:human_id(crypto:strong_rand_bytes(32)),
    A = fun(Proc, N) -> <<?ASSIGNMENTS/binary, "/", Proc/binary, "/",
                          (integer_to_binary(N))/binary>> end,
    Journaled = #{ A(P, 0) => <<"slot-0">>, A(P, 1) => <<"slot-1">> },
    write_rows(Store, Journaled),
    ok = flush(Store, #{}, #{}),
    kill_writer(Store),
    Blob = crypto:strong_rand_bytes(100),
    BlobName = <<"ni:///sha-256;", (hb_util:encode(crypto:hash(sha256, Blob)))/binary>>,
    Gap = #{
        A(P, 2) => <<"ao:", Msg/binary>>,
        <<Msg/binary, "/action">> => <<"Ping">>,
        <<Msg/binary, "/data">> => BlobName,
        hb_escape:encode_path_component(BlobName) => Blob,
        Q => <<"q-definition">>,
        A(Q, 0) => <<"q-slot-0">>
    },
    ok = hb_store:write([Inner], Gap, #{}),
    ok = hb_store:flush([Inner], #{}),
    % The next call restarts the writer, uncleanly.
    ok = flush(Store, #{}, #{}),
    ?assertMatch(#{ reconciled := 2, clean_start := false }, status(Store)),
    ?assertEqual(idle, ship_now(Store)),
    Fresh = fresh_store(<<"oj-rec-restore">>),
    {ok, _} = restore(Target, Fresh),
    assert_rows(Fresh, maps:merge(Journaled, Gap)).

%% @doc Shipping never takes the target past `max-fill-pct': at the ceiling
%% nothing is shipped and the backlog stays local; under it, it all ships.
ceiling_test() ->
    {Store, _Inner, Target} = test_setup(<<"oj-ceil">>, #{ <<"max-fill-pct">> => 0 }),
    Rows = rows(<<"c/">>, 1, 20),
    write_rows(Store, Rows),
    ok = flush(Store, #{}, #{}),
    ?assertMatch({ceiling, _}, ship_now(Store)),
    ?assertNot(filelib:is_regular(filename:join(Target, "manifest.log"))),
    #{ shipper := #{ backlog := #{ segments := 1 } } } = status(Store),
    stop_shipper(Store),
    hb_name:unregister({?MODULE, shipper, maps:get(<<"name">>, Store)}),
    Store2 = Store#{ <<"export">> => (export(Store))#{ <<"max-fill-pct">> => 100 } },
    ?assertEqual(idle, ship_now(Store2)),
    Fresh = fresh_store(<<"oj-ceil-restore">>),
    {ok, #{ rows := 20 }} = restore(Target, Fresh),
    assert_rows(Fresh, Rows).

%% @doc A shipped segment that does not match its manifest is refused.
bad_crc_test() ->
    {Store, _Inner, Target} = test_setup(<<"oj-crc">>, #{ <<"compress">> => false }),
    write_rows(Store, rows(<<"d/">>, 1, 10)),
    ok = flush(Store, #{}, #{}),
    idle = ship_now(Store),
    [Seg] = [ F || F <- filelib:wildcard(hb_util:list(Target) ++ "/seg-*") ],
    {ok, Bin} = file:read_file(Seg),
    Last = binary:last(Bin),
    ok = file:write_file(Seg, <<(binary:part(Bin, 0, byte_size(Bin) - 1))/binary,
                                (Last bxor 1)>>),
    ?assertMatch({error, {bad_crc, _}}, restore(Target, fresh_store(<<"oj-crc-r">>))).

akey(P, N) -> <<?ASSIGNMENTS/binary, "/", P/binary, "/", (integer_to_binary(N))/binary>>.

inner_delete(Inner, Keys) ->
    #{ <<"db">> := DB } = hb_store:find(Inner),
    {ok, _} = elmdb:delete_batch(DB, Keys).

%% @doc A failed sync poisons the journal: the flush after it fails, even
%% when a roll came between, and every later write and flush fails too,
%% though syncing works again. Nothing is retried.
sync_error_poisons_test() ->
    {Store, _Inner, _Target} = test_setup(<<"oj-poison">>, #{}),
    write_rows(Store, rows(<<"p/">>, 1, 3)),
    persistent_term:put({?MODULE, fail_sync}, true),
    try
        _ = call(Store, seal, 5000),
        ?assertMatch({failure, _}, flush(Store, #{}, #{}))
    after
        persistent_term:erase({?MODULE, fail_sync})
    end,
    ?assertMatch({failure, _}, write(Store, #{ <<"p/4">> => <<"x">> }, #{})),
    ?assertMatch({failure, _}, flush(Store, #{}, #{})),
    kill_writer(Store).

%% @doc Only confirmed (flushed) writes are restored, and an unclean start
%% journals again every assignment the inner store holds above the last
%% confirmed one: a slot written but never confirmed, lost from the inner
%% store and reassigned with different content, restores as the inner store
%% has it, and one never reassigned is not resurrected.
unconfirmed_never_restored_test() ->
    {Store, Inner, Target} = test_setup(<<"oj-unconf">>, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Q = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok = write(Store, #{ akey(P, 0) => <<"p0">>, akey(Q, 0) => <<"q0">> }, #{}),
    ok = flush(Store, #{}, #{}),
    % In flight at the crash: journaled, never confirmed.
    ok = write(Store, #{ akey(P, 1) => <<"old">> }, #{}),
    ok = write(Store, #{ akey(Q, 1) => <<"never">> }, #{}),
    kill_writer(Store),
    % The inner store lost both; the scheduler then reassigned P's slot 1,
    % whose journal record a power loss took while LMDB kept it.
    inner_delete(Inner, [akey(P, 1), akey(Q, 1)]),
    ok = hb_store:write([Inner], #{ akey(P, 1) => <<"new">> }, #{}),
    ok = hb_store:flush([Inner], #{}),
    ok = flush(Store, #{}, #{}),
    ?assertEqual(idle, ship_now(Store)),
    Fresh = fresh_store(<<"oj-unconf-r">>),
    {ok, _} = restore(Target, Fresh),
    ?assertEqual({ok, <<"new">>}, hb_store:read([Fresh], akey(P, 1), #{})),
    ?assertMatch({error, _}, hb_store:read([Fresh], akey(Q, 1), #{})),
    ?assertEqual({ok, <<"q0">>}, hb_store:read([Fresh], akey(Q, 0), #{})).

%% @doc Damage in the middle of a journal segment is not a torn tail: the
%% writer refuses to start and restore stops with an error naming the file.
mid_file_corruption_halts_test() ->
    {Store, _Inner, _Target} = test_setup(<<"oj-mid">>, #{ <<"max-fill-pct">> => 0 }),
    write_rows(Store, rows(<<"m/">>, 1, 30)),
    ok = flush(Store, #{}, #{}),
    ok = call(Store, seal, 5000),
    kill_writer(Store),
    Dir = journal_dir(Store),
    [{_, seg, Seg} | _] = local_files(Dir),
    {ok, Bin} = file:read_file(Seg),
    Mid = byte_size(Bin) div 2,
    <<Head:Mid/binary, B, Tail/binary>> = Bin,
    ok = file:write_file(Seg, <<Head/binary, (B bxor 16#55), Tail/binary>>),
    ?assertMatch({error, {corrupt, _, _}}, restore(list_to_binary(Dir), fresh_store(<<"oj-mid-r">>))),
    ?assertMatch({failure, _}, write(Store, #{ <<"m/x">> => <<"x">> }, #{})),
    % The damaged file is left as it was.
    ?assertEqual({ok, <<Head/binary, (B bxor 16#55), Tail/binary>>}, file:read_file(Seg)).

%% @doc A torn last manifest line is ignored; a shipped file replaced with
%% different content, its manifest line rewritten to match, breaks the
%% manifest's hash chain and is refused.
manifest_chain_test() ->
    {Store, _Inner, Target} = test_setup(<<"oj-chain">>, #{ <<"compress">> => false }),
    Rows = rows(<<"c/">>, 1, 10),
    write_rows(Store, Rows),
    ok = flush(Store, #{}, #{}),
    idle = ship_now(Store),
    write_rows(Store, rows(<<"c2/">>, 1, 10)),
    ok = flush(Store, #{}, #{}),
    idle = ship_now(Store),
    Manifest = filename:join(hb_util:list(Target), "manifest.log"),
    {ok, M} = file:read_file(Manifest),
    ok = file:write_file(Manifest, <<M/binary, "seg-torn-0000">>),
    {ok, _} = restore(Target, fresh_store(<<"oj-chain-r1">>)),
    % Rewrite the first shipped file, and its line, consistently.
    [First | _] = binary:split(M, <<"\n">>),
    [Name, _, _ | Chain] = binary:split(First, <<" ">>, [global]),
    File = filename:join(hb_util:list(Target), Name),
    Forged = frame({w, [{<<"c/1">>, <<"forged">>}]}),
    ok = file:write_file(File, Forged),
    Line = iolist_to_binary([Name, " ", integer_to_binary(byte_size(Forged)), " ",
                             integer_to_binary(erlang:crc32(Forged)), [ [" ", C] || C <- Chain ]]),
    ok = file:write_file(Manifest, binary:replace(M, First, Line)),
    ?assertMatch({error, {manifest_chain, _}}, restore(Target, fresh_store(<<"oj-chain-r2">>))).

%% @doc A restore whose assignments for a process skip a slot is refused.
gap_refused_test() ->
    {Store, _Inner, Target} = test_setup(<<"oj-gap">>, #{}),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok = write(Store, #{ akey(P, 0) => <<"0">>, akey(P, 2) => <<"2">> }, #{}),
    ok = flush(Store, #{}, #{}),
    idle = ship_now(Store),
    ?assertMatch({error, {gaps, [{P, [1]}]}}, restore(Target, fresh_store(<<"oj-gap-r">>))).

%% @doc The closure of a message stored with several commitments follows the
%% `ao://with-commitments=' value to each commitment's ID.
closure_follows_commitments_path_test() ->
    Inner = fresh_store(<<"oj-clos">>),
    A = hb_util:human_id(crypto:strong_rand_bytes(32)),
    S1 = hb_util:human_id(crypto:strong_rand_bytes(32)),
    U = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ok = hb_store:write([Inner], #{
        <<"k">> => <<"ao:", A/binary>>,
        A => <<"ao://with-commitments=", S1/binary>>,
        S1 => <<"ao:", U/binary>>,
        <<U/binary, "/data">> => <<"x">>
    }, #{}),
    Keys = [ K || {K, _} <- closure(Inner, <<"k">>) ],
    ?assert(lists:member(<<U/binary, "/data">>, Keys)).

writer_pid(#{ <<"name">> := Name }) -> hb_name:lookup({?MODULE, writer, Name}).

%% @doc A refused write is final: `hb_store' does not stop and restart the
%% store to retry it, and a writer that cannot even journal its start marker
%% (a full disk) refuses calls without crashing, then runs once it can.
refusal_does_not_restart_writer_test() ->
    {Store, _Inner, _Target} = test_setup(<<"oj-noretry">>, #{}),
    ok = hb_store:write([Store], #{ <<"r/1">> => <<"1">> }, #{}),
    Pid = writer_pid(Store),
    persistent_term:put({?MODULE, fail_write}, true),
    R1 = (catch hb_store:write([Store], #{ <<"r/2">> => <<"2">> }, #{})),
    Same = writer_pid(Store),
    % A restart while the disk is still full: refused, not crashed.
    kill_writer(Store),
    R2 = write(Store, #{ <<"r/3">> => <<"3">> }, #{}),
    Pid2 = writer_pid(Store),
    R3 = write(Store, #{ <<"r/3">> => <<"3">> }, #{}),
    Pid3 = writer_pid(Store),
    persistent_term:erase({?MODULE, fail_write}),
    ?assertMatch({failure, _}, R1),
    ?assertEqual(Pid, Same),
    ?assertMatch({failure, _}, R2),
    ?assert(is_pid(Pid2)),
    ?assertMatch({failure, _}, R3),
    ?assertEqual(Pid2, Pid3),
    % Space is back: the same writer starts and takes writes.
    ?assertEqual(ok, write(Store, #{ <<"r/4">> => <<"4">> }, #{})),
    ?assertEqual(Pid2, writer_pid(Store)),
    kill_writer(Store).

%% @doc A poisoning holds across restarts until it is cleared, and clearing
%% it journals the whole inner store again: a row written in the failing
%% window, whose journal record was never synced, is restored.
poison_is_sticky_until_cleared_test() ->
    {Store, Inner, Target} = test_setup(<<"oj-sticky">>, #{}),
    ok = write(Store, #{ <<"s/1">> => <<"1">> }, #{}),
    ok = flush(Store, #{}, #{}),
    ok = write(Store, #{ <<"s/window">> => <<"w">> }, #{}),
    persistent_term:put({?MODULE, fail_sync}, true),
    try ?assertMatch({failure, _}, flush(Store, #{}, #{}))
    after persistent_term:erase({?MODULE, fail_sync})
    end,
    kill_writer(Store),
    % A new run is still poisoned.
    ?assertMatch({failure, _}, write(Store, #{ <<"s/2">> => <<"2">> }, #{})),
    ?assertMatch({failure, _}, flush(Store, #{}, #{})),
    ok = clear_poison(Store),
    ?assertEqual(ok, write(Store, #{ <<"s/3">> => <<"3">> }, #{})),
    ok = flush(Store, #{}, #{}),
    ?assertMatch(#{ rebased := N } when N >= 2, status(Store)),
    ?assertEqual({ok, <<"w">>}, hb_store:read([Inner], <<"s/window">>, #{})),
    idle = ship_now(Store),
    Fresh = fresh_store(<<"oj-sticky-r">>),
    {ok, _} = restore(Target, Fresh),
    assert_rows(Fresh, #{ <<"s/1">> => <<"1">>, <<"s/window">> => <<"w">>,
                          <<"s/3">> => <<"3">> }).

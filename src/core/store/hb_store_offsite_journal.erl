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
%%%   flush --> syncer: one round for every flush waiting (group commit):
%%%                     cut (writer), fdatasync the journal, flush the inner
%%%                     store, answer them all
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
%%% Group commit. Durability has its own process, the syncer, so a sync never
%%% holds up an append and no caller pays for a sync alone. Every flush that
%%% is waiting when a round starts shares it: the syncer asks the writer for a
%%% cut (the `flushed' marker; the writer goes on appending), syncs the
%%% journal through it on a descriptor of its own (a sync is per file, not
%%% per descriptor), flushes the inner store once, and answers every waiter.
%%% A round covers every append answered before any of its flushes was
%%% asked for, so N scheduling processes cost one sync, not N in a row.
%%%
%%% Confirmation. A flush appends a `flushed' marker before it syncs, and
%%% only writes a `flushed' marker follows count: a restore drops what a run
%%% wrote and never confirmed (a `start' marker opens every run), and a
%%% `revoke' marker withdraws a confirmation whose inner flush failed. Any
%%% failed sync of the local journal poisons it: it is never retried (on
%%% Linux a retry can report success for pages the kernel dropped), and every
%%% later write and flush of the run is refused.
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
%%% The local journal is one log, in segment order, and segments are sealed
%%% in that order. A segment closed for a successor while still `open-' ends
%%% with a `{closed, N}' frame; one sealed instead is synced, renamed `seg-'
%%% and the rename synced before the successor exists. An `open-' segment was
%%% never confirmed past its last sync (a confirmation follows a sync through
%%% its `flushed' marker, and a sync covers every lower segment first), so on
%%% start an `open-' file whose first bad frame comes before its end, or one
%%% followed by another file without its `{closed, N}' frame (it lost its
%%% tail on a frame boundary), ends the log: the `open-' files after it are
%%% deleted, it is cut at its first bad frame, and what is left is synced and
%%% sealed. A sealed (`seg-') segment was synced whole: damage in one, or one
%%% after the end of the log, stops the writer, and a restore, with the file
%%% and offset. A failed append (a full local disk) is truncated away and
%%% refused: the write fails and the inner store is not written.
%%%
%%% The writer never touches the target. A shipper process copies each sealed
%%% segment to it (`.tmp', sync, rename, directory sync), then appends the
%%% file's name, size, CRC and a hash chain over every shipped file to
%%% `manifest.log' (synced; a torn last line is cut), then deletes the local
%%% copy and syncs the journal directory. The local journal is the gate; the
%%% offsite copy only has to be correct. A failed sync on the target stops
%%% the shipper, never the writer: it is never retried, in this run or a
%%% later one (the `ship-poisoned' marker), until `clear_ship_poison/1', and
%%% `status/1' shows it. A failed sync of the journal directory after the
%%% local copy is deleted poisons the journal: that sync also covers the
%%% writer's own new and renamed segments, and the kernel reports its error
%%% once. A restore checks size, CRC and chain, and refuses a result whose
%%% assignments skip a slot. A slow, stalled, full or failed target only
%%% grows the local backlog; nothing is dropped. It never takes the target's
%%% filesystem above `max-fill-pct': at the ceiling, shipping pauses
%%% (`offsite_journal_ceiling' events) and resumes by itself.
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
-export([wrap/2, status/1, restore/2, restore/3, ship_now/1, clear_poison/1,
         clear_ship_poison/1]).
-export([start/3, stop/3, reset/3, flush/3, scope/0, scope/1]).
-export([read/3, write/3, list/3, match/3, batch/3, hold/3]).
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
    stop_helper(syncer, Store),
    catch call(Store, stop, 30000),
    stop_shipper(Store),
    hb_store:stop([inner(Store)], Req, Opts).

%% @doc Reset the inner store and the local journal. The target is never
%% touched.
reset(Store, Req, Opts) ->
    stop_helper(syncer, Store),
    catch call(Store, stop, 30000),
    stop_shipper(Store),
    os:cmd("rm -rf " ++ journal_dir(Store)),
    hb_store:reset([inner(Store)], Req, Opts).

scope() -> local.
scope(_) -> local.

read(Store, Req = #{ <<"read">> := Path }, Opts) ->
    held_read(Store, read, Path, (inner_mod(Store)):read(inner(Store), Req, Opts));
read(Store, Req, Opts) -> (inner_mod(Store)):read(inner(Store), Req, Opts).
list(Store, Req = #{ <<"list">> := Path }, Opts) ->
    held_read(Store, list, Path, (inner_mod(Store)):list(inner(Store), Req, Opts));
list(Store, Req, Opts) -> (inner_mod(Store)):list(inner(Store), Req, Opts).
match(Store, Req, Opts) -> (inner_mod(Store)):match(inner(Store), Req, Opts).

%% @doc Journal the rows, then write them to the inner store, in the writer.
%% Inside `batch/3' the rows are held instead, and written with the batch.
write(Store, Req, _Opts) when is_map(Req) ->
    Pairs =
        lists:sort(
            [ {path_bin(P), V} || {P, V} <- maps:to_list(Req) ]
        ),
    case get(held_key(Store)) of
        undefined -> write_groups(Store, [Pairs]);
        Held ->
            put(held_key(Store), [Pairs | Held]),
            ok
    end.

%% @doc Journal the groups (each one caller's write) as one frame and write
%% them to the inner store in order: one writer call.
write_groups(Store, Groups) ->
    case call(Store, {append, Groups}, ?CALL_MS) of
        ok -> ok;
        {error, Reason} -> {failure, {offsite_journal, Reason}};
        Other -> Other
    end.

%% @doc Run `Fun', holding every write this process makes to the store, then
%% journal and write them all in one writer call when it returns: one frame,
%% then each write to the inner store as it was made, in the order it was
%% made (`append_and_write/2'). Nothing is written if `Fun' raises,
%% and a refused write raises here, after `Fun'. A batch inside a batch joins
%% it. The rows are not in the inner store until the batch ends: inside it a
%% read sees the store as it was before the batch, which is right for a row
%% that was there already (a content-addressed row written again reads the
%% same), and a read that would miss or differ from a held row raises
%% instead (`held_read/4').
batch(Store, Fun, Opts) ->
    {Result, Commit} = hold(Store, Fun, Opts),
    case Commit() of
        ok -> Result;
        Failure -> throw(Failure)
    end.

%% @doc `batch/3' without the write: run `Fun' with this process's writes to
%% the store held, and return `{Result, Commit}'. `Commit()' journals and
%% writes them (one writer call; `ok' or the refusal), from any process, so
%% the caller need not wait for the writer itself. Nothing is written until
%% it is called, nor if `Fun' raises. Inside a batch or a hold, this joins it
%% and `Commit' is a no-op.
hold(Store, Fun, _Opts) ->
    Key = held_key(Store),
    case get(Key) of
        undefined ->
            put(Key, []),
            Result =
                try Fun()
                catch Class:Reason:Stack ->
                    erase(Key),
                    erlang:raise(Class, Reason, Stack)
                end,
            case lists:reverse(erase(Key)) of
                [] -> {Result, fun() -> ok end};
                Groups -> {Result, fun() -> write_groups(Store, Groups) end}
            end;
        _ -> {Fun(), fun() -> ok end}
    end.

held_key(#{ <<"name">> := Name }) -> {?MODULE, held, Name}.

%% @doc A read or list inside a batch, against the rows it holds: the inner
%% store's answer stands if no held row could change it, or if it already
%% has the held value of the key read; a held key it does not have, or has
%% with another value, and a held row under a path it finds nothing at,
%% raise rather than answer as if the write had not been made.
held_read(Store, Kind, Path, Result) ->
    case get(held_key(Store)) of
        undefined -> Result;
        [] -> Result;
        Held ->
            P = path_bin(Path),
            Prefix = <<P/binary, "/">>,
            Size = byte_size(Prefix),
            Pairs = lists:append(Held),   % newest write first
            Exact = [ V || Kind == read, {K, V} <- Pairs, K =:= P ],
            Under =
                lists:any(
                    fun({K, _}) ->
                        byte_size(K) > Size andalso binary:part(K, 0, Size) =:= Prefix
                    end,
                    Pairs),
            Missed = Result == not_found orelse Result == {error, not_found},
            case {Exact, Under} of
                {[V | _], _} when Result =:= {ok, V} -> Result;
                {[_ | _], _} -> error({read_of_held_write, P});
                {[], true} when Missed -> error({read_of_held_write, P});
                _ -> Result
            end
    end.

%% @doc Sync the journal, then flush the inner store: one syncer round, shared
%% with every other flush waiting for one.
flush(Store, _Req, _Opts) ->
    case call_helper(syncer, Store, flush, ?CALL_MS) of
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
    Y = case hb_name:lookup({?MODULE, syncer, maps:get(<<"name">>, Store)}) of
            undefined -> #{ syncer => not_running };
            SPid -> #{ syncer => call_pid(SPid, status, 5000) }
        end,
    maps:merge(maps:merge(W, S), Y).

%% @doc Ship every sealed segment now and wait for the round to finish.
ship_now(Store) ->
    _ = call(Store, seal, ?CALL_MS),
    Pid = shipper(Store),
    Ref = make_ref(),
    Pid ! {ship_now, self(), Ref},
    receive {Ref, R} -> R after ?CALL_MS -> {error, timeout} end.

%%% Calls into the writer.

call(Store, Msg, Timeout) -> call_helper(writer, Store, Msg, Timeout).

%% @doc Call the writer or the syncer through this process's cached pid
%% (`helper/2'). A pid that is gone (`noproc': the call never reached it) is
%% looked up again and called once more; any other exit only drops the cache.
call_helper(Kind, Store, Msg, Timeout) ->
    case call_pid(helper(Kind, Store), Msg, Timeout) of
        {error, {writer_down, noproc}} ->
            forget_helper(Kind, Store),
            call_pid(helper(Kind, Store), Msg, Timeout);
        Reply = {error, {writer_down, _}} ->
            forget_helper(Kind, Store),
            Reply;
        Reply -> Reply
    end.

call_pid(Pid, Msg, Timeout) ->
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

syncer(Store = #{ <<"name">> := Name }) ->
    hb_name:singleton({?MODULE, syncer, Name},
        fun() ->
            process_flag(priority, high),
            syncer_loop(Store, #{ path => undefined, fd => undefined,
                                      rounds => 0, flushes => 0 }) end).

%% The writer or syncer pid, cached in the caller's process dictionary: a
%% registry lookup on every store call was 6-10% of a busy scheduler.
helper(Kind, Store) ->
    Key = helper_key(Kind, Store),
    case get(Key) of
        Pid when is_pid(Pid) -> Pid;
        undefined ->
            Pid = case Kind of
                      writer -> writer(Store);
                      syncer -> syncer(Store)
                  end,
            put(Key, Pid),
            Pid
    end.

forget_helper(Kind, Store) -> erase(helper_key(Kind, Store)).

helper_key(Kind, #{ <<"name">> := Name }) -> {?MODULE, Kind, Name}.

stop_shipper(Store) -> stop_helper(shipper, Store).

%% The shipper and the syncer stop on `stop', after the round in hand.
stop_helper(Kind, #{ <<"name">> := Name }) ->
    case hb_name:lookup({?MODULE, Kind, Name}) of
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
                {'EXIT', {{offsite_journal_sync_failed, What, Reason}, _}} ->
                    % Never retried: a later start would sync it again and
                    % could be told it worked.
                    Why = {sync_failed, {recover, What, Reason}},
                    write_poison(Dir, Why),
                    writer_refusing(Store, {journal_poisoned, Why});
                {'EXIT', Reason} ->
                    % Anything else (a file that cannot be read, cut or
                    % renamed): refuse, rather than crash and have the next
                    % call respawn the writer to recover, and sync, again.
                    Why = {recovery_failed, Reason},
                    write_poison(Dir, Why),
                    writer_refusing(Store, {journal_poisoned, Why});
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
                        % Segments closed by a roll whose tail a round has
                        % not synced yet, and ones not yet renamed sealed:
                        % the syncer does both, off the append path. A new
                        % segment's directory entry is synced by the next
                        % round too (`dir_dirty').
                        unsynced => [],
                        unsealed => [],
                        dir_dirty => false,
                        poisoned => false,
                        started => false,
                        % The syncer's round in hand: where its cut was and
                        % the marks it confirms.
                        cut => undefined,
                        last_round => undefined,
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
                    % Every scheduler waits on the writer and the syncer for
                    % a few microseconds of work each: run them ahead of the
                    % node's request handlers, not behind them.
                    process_flag(priority, high),
                    writer_loop(S1)
            end
    end.

%% @doc Open the run: the start marker (whatever the last run wrote and never
%% confirmed is dropped by a restore from here on), then, after an unclean
%% stop, the reconcile, and a rebase when one is pending. A failure leaves the
%% writer not started, to try again at the next call, from the state the
%% attempt failed in (`{not_started, Why, State}'): what it appended, and a
%% poisoning, are kept.
ensure_started(S = #{ started := true }) -> S;
ensure_started(S = #{ poisoned := P }) when P =/= false -> S;
ensure_started(S = #{ clean_start := Clean, dir := Dir }) ->
    try
        S1 = case append({start, Clean}, S) of
                 {ok, SA} -> SA;
                 {error, R1, SA} -> throw({not_started, {start_refused, R1}, SA})
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
        throw:{not_started, Why, SF} -> not_started(SF, Why)
    end.

%% The attempt may have sealed or broken the segment it was writing: the next
%% attempt opens a new one. One it left open is closed as a roll closes one
%% (`close_segment/1'), so it is synced and sealed in order with the rest
%% (or, poisoned, never). Rows it journaled and never confirmed are not
%% marked.
not_started(S, Why) ->
    ?event(error, {offsite_journal_not_started, Why}),
    S1 = close_segment(S),
    S1#{ pending => #{}, errors => maps:get(errors, S1) + 1 }.

writer_loop(S = #{ store := Store }) ->
    receive
        {call, From, Ref, {poison, Why}} ->
            % The shipper's failed sync: refused from now on, started or not.
            S1 = poison(S, Why),
            From ! {Ref, ok},
            writer_loop(S1);
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
            Every = cfg(<<"fsync-ms">>, ?FSYNC_MS, Store),
            % Under load the syncer's rounds keep the journal synced: the
            % tick is for writes nobody flushes.
            Recent =
                case maps:get(last_round, S) of
                    undefined -> false;
                    T -> erlang:monotonic_time(millisecond) - T < Every
                end,
            S1 = case Recent of
                     true -> S;
                     false -> sync_journal(S)
                 end,
            erlang:send_after(Every, self(), fsync_tick),
            writer_loop(S1);
        roll_tick ->
            erlang:send_after(1000, self(), roll_tick),
            writer_loop(maybe_roll(S));
        {'EXIT', _, _} ->
            writer_loop(S);
        _ ->
            writer_loop(S)
    end.

handle(From, Ref, {append, Groups}, S) ->
    {Reply, S1} = append_and_write(Groups, S),
    From ! {Ref, Reply},
    writer_loop(maybe_roll(S1));
handle(From, Ref, cut, S) ->
    {Reply, S1} = cut(S),
    From ! {Ref, Reply},
    writer_loop(S1);
handle(From, Ref, round_done, S) ->
    S1 = round_done(S),
    From ! {Ref, refusal(S1, ok)},
    writer_loop(S1);
handle(From, Ref, {round_failed, Why}, S) ->
    {Reply, S1} = round_failed(Why, S),
    From ! {Ref, Reply},
    writer_loop(S1);
handle(From, Ref, seal, S) ->
    S1 = seal_now(S),
    From ! {Ref, refusal(S1, ok)},
    writer_loop(S1);
handle(From, Ref, {marks, Sealed}, S = #{ marks := Marks, token := Token }) ->
    From ! {Ref, refusal(S, {ok, {marks, Sealed, Marks, Token}})},
    writer_loop(S);
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
    write_poison(Dir, Reason),
    S#{ poisoned => Reason, errors => maps:get(errors, S) + 1 };
poison(S, _Reason) -> S.

write_poison(Dir, Reason) ->
    ?event(error, {offsite_journal_poisoned, Reason}),
    Written =
        maybe
            ok ?= write_synced(filename:join(Dir, "poisoned"), term_to_binary({poisoned, Reason})),
            sync_dir(Dir)
        end,
    case Written of
        ok -> ok;
        Err -> ?event(error, {offsite_journal_poison_marker_failed, Err})
    end.

read_poison(Dir) -> read_poison(Dir, "poisoned").

read_poison(Dir, Name) ->
    case file:read_file(filename:join(Dir, Name)) of
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
    ok = sync_dir(Dir),
    ok = case file:delete(filename:join(Dir, "poisoned")) of
             ok -> ok;
             {error, enoent} -> ok;
             E -> E
         end,
    ok = sync_dir(Dir),
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
        Other -> throw({not_started, {rebase_unsupported, Other}, S})
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
             {error, R, SE} -> throw({not_started, {rebase_failed, R}, SE})
         end,
    case maps:get(poisoned, S3) of
        false -> ok;
        Why -> throw({not_started, {rebase_failed, Why}, S3})
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
        {error, R, SE} -> throw({not_started, {rebase_failed, R}, SE})
    end.

%% @doc Append the groups' rows as one frame, then write them to the inner
%% store: each group (one caller's write) as its own inner write, in order,
%% stopping at the first that fails. One inner write for them all would not
%% do: the inner store can commit part of a write (elmdb's `put_batch' fills
%% the overlay a concurrent commit takes, in map order, not the caller's), so
%% a crash could keep a slot link and lose the rows it names, or keep a later
%% slot and lose an earlier one. One write per group, in order, keeps what
%% any commit takes a prefix of the caller's writes, as
%% `dev_scheduler_server:repair_index' and `dev_scheduler_cache:latest/2'
%% need. A refused append writes nothing. The rows count as confirmed at the
%% next flush. A row whose key already holds the same value earlier in the
%% frame is dropped from the frame and from the inner writes (`dedupe/1').
append_and_write(_Groups, S = #{ poisoned := P }) when P =/= false ->
    {refusal(S, ok), S};
append_and_write(Groups0, S = #{ store := Store }) ->
    Groups = dedupe(Groups0),
    Pairs = lists:append(Groups),
    case append({w, Pairs}, S) of
        {ok, S1} ->
            Inner = inner(Store),
            Res = write_inner(maps:get(<<"store-module">>, Inner), Inner, Groups),
            {Res, S1#{ pending => mark(Pairs, maps:get(pending, S1)),
                       unflushed => true,
                       appended => maps:get(appended, S1) + 1 }};
        {error, Reason, S1} ->
            {{error, Reason}, S1}
    end.

%% @doc Drop each row that repeats the value its key already has earlier in
%% the groups (a batch writes the same body once per caller). Only the latest
%% value counts: a key written `A', then `B', then `A' again keeps all three,
%% in order, so replaying the frame ends where writing every row would. A
%% group left with no rows is dropped; an earlier group wrote them, before it.
dedupe(Groups) ->
    {Kept, _} =
        lists:mapfoldl(
            fun(Group, Seen0) ->
                {Rev, Seen} =
                    lists:foldl(
                        fun({K, V}, {Acc, Sn}) ->
                            case Sn of
                                #{ K := V } -> {Acc, Sn};
                                _ -> {[{K, V} | Acc], Sn#{ K => V }}
                            end
                        end,
                        {[], Seen0},
                        Group
                    ),
                {lists:reverse(Rev), Seen}
            end,
            #{},
            Groups
        ),
    [ G || G <- Kept, G =/= [] ].

stored_content(Mod, Inner, Key = <<"ni:", _/binary>>) ->
    case Mod:read(Inner, #{ <<"read">> => Key, <<"exact">> => true }, #{}) of
        {ok, _} -> true;
        _ -> false
    end;
stored_content(_Mod, _Inner, _Key) -> false.

%% A content-addressed (`ni:') row the inner store already holds is not put
%% again: its key names its value, so the put would change nothing. It stays
%% in the frame, so a restore is still complete.
write_inner(_Mod, _Inner, []) -> ok;
write_inner(Mod, Inner, [Group | Groups]) ->
    Res =
        try
            case [ Row || Row = {K, _} <- Group, not stored_content(Mod, Inner, K) ] of
                [] -> ok;
                New -> Mod:write(Inner, maps:from_list(New), #{})
            end
        catch C:R -> {error, {C, R}}
        end,
    case Res of
        ok -> write_inner(Mod, Inner, Groups);
        Failure -> Failure
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
            % Cut any partial frame away, then seal in order, as `seal_now/1'
            % does: the closed segments, then this one, synced after the cut
            % (it is dirty again). Nothing is ever appended after a failure
            % in the same file. A cut that fails poisons: the file is left
            % `open-', for the next start to cut. Poisoned, the descriptor
            % is closed here; sealed, it is closed already.
            Cut = maybe
                      {ok, Off} ?= file:position(Fd, Off),
                      file:truncate(Fd)
                  end,
            S1 = case Cut of
                     ok -> seal_current(seal_closed(sync_journal(S#{ dirty => true })));
                     Failed -> poison(S, {truncate_failed, Failed})
                 end,
            _ = case S1 of
                    #{ fd := undefined } -> ok;
                    #{ fd := Left } -> file:close(Left)
                end,
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

%% @doc The writer's half of a syncer round: append the `flushed' marker that
%% confirms everything written so far, and tell the syncer what to sync. The
%% writer goes on taking appends; the marks wait for `round_done'.
cut(S = #{ poisoned := P }) when P =/= false -> {refusal(S, ok), S};
cut(S) ->
    Marked =
        case maps:get(unflushed, S) of
            false -> {ok, S};
            true -> append({flushed}, S)
        end,
    case Marked of
        {error, Reason, S1} -> {{error, Reason}, S1};
        {ok, S1} ->
            Info = cut_info(S1),
            {{ok, Info}, cut_taken(S1, Info)}
    end.

%% What the round must do before it may confirm the cut: sync every segment
%% that may hold unsynced appends (closed ones by name, then the open one),
%% rename the closed ones sealed, and sync the directory if a segment was
%% created or renamed. A cut whose round never finished (a killed syncer)
%% hands its work on to this one.
cut_info(S = #{ dir := Dir, seg := N, fd := Fd, dirty := Dirty }) ->
    {HSegs, HSeal, HDir} = handed(S),
    #{ dir => Dir,
       segs => lists:usort(HSegs ++ maps:get(unsynced, S) ++ [ N || Fd =/= undefined, Dirty ]),
       seal => lists:usort(HSeal ++ maps:get(unsealed, S)),
       dir_sync => HDir orelse maps:get(dir_dirty, S) }.

handed(#{ cut := {_, _, _, #{ segs := Segs, seal := Seal, dir_sync := DirSync }} }) ->
    {Segs, Seal, DirSync};
handed(_) -> {[], [], false}.

cut_taken(S = #{ seg := N, off := Off, pending := Pending }, Info) ->
    S#{ cut => {N, Off, Pending, Info}, pending => #{}, unflushed => false,
        unsynced => [], unsealed => [], dir_dirty => false }.

%% @doc The round synced the journal through the cut and flushed the inner
%% store: its marks are confirmed, and the journal is clean if nothing was
%% appended since. A round of an earlier writer is not ours.
round_done(S = #{ cut := undefined }) -> S;
round_done(S = #{ cut := {N, Off, Pending, _Info}, seg := Seg, off := Now }) ->
    S#{ cut => undefined,
        marks => merge_marks(Pending, maps:get(marks, S)),
        dirty => maps:get(dirty, S) andalso {N, Off} =/= {Seg, Now},
        last_round => erlang:monotonic_time(millisecond) }.

%% @doc The round failed. A failed sync poisons the journal (never retried);
%% a failed inner flush is withdrawn with a `revoke' marker, then poisons it.
round_failed(Why = {sync_failed, _}, S) ->
    S1 = poison(S#{ cut => undefined }, Why),
    {refusal(S1, ok), S1};
round_failed({inner_flush_failed, Error}, S) ->
    S1 = case append({revoke}, S#{ cut => undefined }) of
             {ok, SR} -> sync_journal(SR);
             {error, _, SR} -> SR
         end,
    {{error, {inner_flush_failed, Error}}, poison(S1, {inner_flush_failed, Error})}.

%%% The syncer: every flush waiting when a round starts shares it.

syncer_loop(Store, St) ->
    receive
        {call, From, Ref, flush} ->
            Waiters = [{From, Ref} | queued_flushes()],
            {Reply, St1, Sealed} = sync_round(Store, St),
            lists:foreach(fun({W, R}) -> W ! {R, Reply} end, Waiters),
            sealed(Store, Reply, Sealed),
            syncer_loop(Store,
                St1#{ rounds := maps:get(rounds, St1) + 1,
                      flushes := maps:get(flushes, St1) + length(Waiters) });
        {call, From, Ref, status} ->
            From ! {Ref, maps:with([rounds, flushes], St)},
            syncer_loop(Store, St);
        stop ->
            close_synced(St),
            ok;
        _ ->
            syncer_loop(Store, St)
    end.

queued_flushes() ->
    receive {call, From, Ref, flush} -> [{From, Ref} | queued_flushes()]
    after 0 -> []
    end.

%% @doc One round: the cut, one sync of the journal through it, one flush of
%% the inner store. Only then is the writer told (`round_done'), and its
%% answer is every waiter's: a writer poisoned since the cut (its own sync of
%% the segment failed, though ours passed) refuses, and nobody hears `ok'. A
%% failure is the writer's to record before anyone hears.
sync_round(Store, St) ->
    case call_helper(writer, Store, cut, ?CALL_MS) of
        {ok, Cut} ->
            % The writer that took the cut: `call_helper' keeps it cached.
            Writer = helper(writer, Store),
            case sync_cut(Cut, St) of
                {ok, St1} ->
                    case catch hb_store:flush(inner(Store), #{}) of
                        ok ->
                            {call_pid(Writer, round_done, ?CALL_MS), St1,
                                maps:get(seal, Cut)};
                        Error ->
                            {call_pid(Writer,
                                {round_failed, {inner_flush_failed, Error}}, ?CALL_MS), St1, []}
                    end;
                {error, Reason, St1} ->
                    {call_pid(Writer, {round_failed, {sync_failed, Reason}}, ?CALL_MS), St1, []}
            end;
        Refused -> {Refused, St, []}
    end.

%% @doc Sync each segment of the cut, on the syncer's own descriptor; then
%% rename the closed ones sealed (each is synced whole by now: this round
%% synced it, or an earlier one did and nothing was appended since), and sync
%% the directory if a segment was created or renamed. A segment is renamed
%% sealed only once it is synced, and deleted only once it is shipped, so one
%% found under neither name holds nothing left to sync.
sync_cut(#{ dir := Dir, segs := Segs, seal := Seal, dir_sync := DirSync }, St) ->
    case sync_segs(Dir, Segs, St) of
        {ok, St1} ->
            lists:foreach(fun(N) -> seal_rename(Dir, N) end, Seal),
            case DirSync orelse Seal =/= [] of
                true ->
                    case sync_dir(Dir) of
                        ok -> {ok, St1};
                        {error, Reason} -> {error, Reason, St1}
                    end;
                false -> {ok, St1}
            end;
        Error -> Error
    end.

sync_segs(_Dir, [], St) -> {ok, St};
sync_segs(Dir, [N | Segs], St) ->
    Synced =
        case segment_fd(open_name(Dir, N), St) of
            {error, enoent, St1} ->
                case segment_fd(seg_name(Dir, N), St1) of
                    {error, enoent, St2} -> {ok, St2};
                    Found -> sync_fd(Found)
                end;
            Found -> sync_fd(Found)
        end,
    case Synced of
        {ok, St3} -> sync_segs(Dir, Segs, St3);
        Error -> Error
    end.

seal_rename(Dir, N) ->
    case file:rename(open_name(Dir, N), seg_name(Dir, N)) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, Reason} -> ?event(error, {offsite_journal_seal_failed, {seg, N}, Reason})
    end.

%% @doc After a round that sealed segments: record the marks as of them (they
%% only shorten the next start's scan, so a failure is logged; the marks are
%% the writer's, read from it), then wake the shipper.
sealed(_Store, _Reply, []) -> ok;
sealed(Store, ok, Seal) ->
    case call_helper(writer, Store, {marks, lists:max(Seal)}, ?CALL_MS) of
        {ok, Marks} ->
            File = filename:join(journal_dir(Store), "marks"),
            Tmp = File ++ ".syncer.tmp",
            case write_synced(Tmp, term_to_binary(Marks)) of
                ok -> _ = file:rename(Tmp, File), ok;
                {error, Reason} -> ?event(error, {offsite_journal_marks_failed, Reason})
            end;
        _ -> ok
    end,
    case hb_name:lookup({?MODULE, shipper, maps:get(<<"name">>, Store)}) of
        undefined -> ok;
        Pid -> Pid ! wake
    end;
sealed(_Store, _Refused, _Seal) -> ok.

sync_fd({ok, St = #{ fd := Fd }}) ->
    case datasync(Fd) of
        ok -> {ok, St};
        {error, Reason} -> {error, Reason, St}
    end;
sync_fd(Error) -> Error.

segment_fd(Path, St = #{ path := Path, fd := Fd }) when Fd =/= undefined -> {ok, St};
segment_fd(Path, St) ->
    close_synced(St),
    case file:open(Path, [read, raw, binary]) of
        {ok, Fd} -> {ok, St#{ path => Path, fd => Fd }};
        {error, Reason} -> {error, Reason, St#{ path => undefined, fd => undefined }}
    end.

close_synced(#{ fd := undefined }) -> ok;
close_synced(#{ fd := Fd }) -> _ = file:close(Fd), ok.

%% @doc Sync the journal. A failed sync is never retried (on Linux a retry
%% can report success for pages the kernel already dropped): it poisons the
%% journal.
sync_journal(S = #{ poisoned := P }) when P =/= false -> S;
sync_journal(S) ->
    case sync_closed(S) of
        S1 = #{ poisoned := false } -> sync_current(S1);
        S1 -> S1
    end.

%% The closed segments a round has not synced (handed to a round that may
%% never finish, too), by name, and the directory if a segment was created.
%% The open one is `sync_current/1''s, on its descriptor; with none open,
%% `seg' is the last one closed, and is synced here by name.
sync_closed(S = #{ dir := Dir, seg := Seg, fd := Fd }) ->
    {HSegs, _, HDir} = handed(S),
    Segs = lists:usort(HSegs ++ maps:get(unsynced, S)) -- [ Seg || Fd =/= undefined ],
    Synced =
        maybe
            ok ?= sync_named(Dir, Segs),
            case HDir orelse maps:get(dir_dirty, S) of
                true -> sync_dir(Dir);
                false -> ok
            end
        end,
    case Synced of
        ok -> S#{ unsynced => [], dir_dirty => false };
        {error, Reason} -> poison(S, {sync_failed, Reason})
    end.

sync_named(_Dir, []) -> ok;
sync_named(Dir, [N | Segs]) ->
    Opened =
        case file:open(open_name(Dir, N), [read, raw, binary]) of
            {error, enoent} -> file:open(seg_name(Dir, N), [read, raw, binary]);
            Other -> Other
        end,
    case Opened of
        {error, enoent} -> sync_named(Dir, Segs);
        {error, Reason} -> {error, Reason};
        {ok, Fd} ->
            R = datasync(Fd),
            _ = file:close(Fd),
            case R of
                ok -> sync_named(Dir, Segs);
                Error -> Error
            end
    end.

sync_file(File) ->
    case file:open(File, [read, raw, binary]) of
        {ok, Fd} ->
            R = datasync(Fd),
            _ = file:close(Fd),
            R;
        Error -> Error
    end.

sync_current(S = #{ dirty := false }) -> S;
sync_current(S = #{ fd := undefined }) -> S#{ dirty := false };
sync_current(S = #{ fd := Fd }) ->
    case datasync(Fd) of
        ok -> S#{ dirty := false };
        {error, Reason} -> poison(S, {sync_failed, Reason})
    end.

datasync(Fd) ->
    % Test hooks: a slow disk, then a failing one.
    timer:sleep(persistent_term:get({?MODULE, sync_delay_ms}, 0)),
    case persistent_term:get({?MODULE, fail_sync}, false) of
        true -> {error, eio};
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

%% @doc Close the active segment (if it holds anything) and open the next,
%% with no sync: the next syncer round syncs the closed segment's tail and
%% the new directory entry before it confirms anything after them, and
%% renames the closed segment sealed (`sync_cut/2'). A roll was a datasync,
%% a synced marks file and three directory syncs in the writer, which every
%% scheduler's append queued behind. Until it is sealed, a closed segment is
%% an `open-' file below the active one, which a start recovers and seals
%% like any other.
roll(S = #{ poisoned := P }) when P =/= false -> S;
roll(S = #{ off := 0, fd := Fd }) when Fd =/= undefined -> S;
roll(S) ->
    case close_segment(S) of
        S1 = #{ poisoned := false, seg := N } -> open_next(S1#{ seg => N + 1 });
        S1 -> S1
    end.

%% @doc Close the open segment for a successor. Its last frame is
%% `{closed, N}': an `open-' file followed by another without it lost its
%% tail, and ends the log (`recover_local/3'). The next round (or any sync
%% of the journal) syncs it, by name, and renames it sealed. An append of the
%% frame that fails seals the segment as any failed append does, or poisons;
%% a poisoned journal appends nothing more, and only closes the descriptor.
%% `seg' stays the closed segment's, with no descriptor: the next append
%% opens the one after it.
close_segment(S = #{ fd := undefined }) -> S;
close_segment(S = #{ poisoned := P, fd := Fd }) when P =/= false ->
    _ = file:close(Fd),
    S#{ fd => undefined, dirty => false };
close_segment(S = #{ seg := N }) ->
    case append({closed, N}, S) of
        {ok, S1 = #{ fd := Fd }} ->
            _ = file:close(Fd),
            S1#{ fd => undefined, dirty => false, sealed => N,
                 unsynced => [N | maps:get(unsynced, S1)],
                 unsealed => [N | maps:get(unsealed, S1)] };
        {error, _, S1} -> S1
    end.

open_next(S = #{ dir := Dir, seg := N }) ->
    case file:open(open_name(Dir, N), [read, write, raw, binary]) of
        {ok, Fd} ->
            {ok, _} = file:position(Fd, eof),
            S#{ fd => Fd, off => 0, opened => erlang:monotonic_time(millisecond),
                dir_dirty => true };
        {error, Reason} ->
            ?event(error, {offsite_journal_open_failed, Reason}),
            S#{ fd => undefined }
    end.

%% @doc Seal everything now (`ship_now/1'): sync, rename the closed segments
%% sealed, then seal the active one, if it holds anything, and open the next.
seal_now(S = #{ poisoned := P }) when P =/= false -> S;
seal_now(S) ->
    case seal_closed(sync_journal(S)) of
        S1 = #{ poisoned := P } when P =/= false -> S1;
        S1 = #{ off := 0, fd := Fd } when Fd =/= undefined -> S1;
        S1 ->
            case seal_current(S1) of
                S2 = #{ poisoned := false } ->
                    open_segment(S2#{ seg => maps:get(seg, S2) + 1 });
                S2 -> S2
            end
    end.

%% Rename the closed segments sealed: they must be synced already
%% (`sync_journal/1').
seal_closed(S = #{ poisoned := P }) when P =/= false -> S;
seal_closed(S = #{ dir := Dir, store := Store }) ->
    {_, HSeal, _} = handed(S),
    case lists:usort(HSeal ++ maps:get(unsealed, S)) of
        [] -> S;
        Seal ->
            lists:foreach(fun(N) -> seal_rename(Dir, N) end, Seal),
            case sync_dir(Dir) of
                ok ->
                    case hb_name:lookup({?MODULE, shipper, maps:get(<<"name">>, Store)}) of
                        undefined -> ok;
                        Pid -> Pid ! wake
                    end,
                    S#{ unsealed => [] };
                {error, Reason} -> poison(S, {sync_failed, Reason})
            end
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

%% A segment sealed here has no `{closed, N}' frame, and a successor may
%% follow it: it must not stay an `open-' file, so a failed rename (or a
%% failed delete of an empty one) poisons.
seal_synced(S, Fd, Dir, N, Store) ->
    _ = file:close(Fd),
    Open = open_name(Dir, N),
    case maps:get(off, S) of
        0 ->
            case file:delete(Open) of
                ok -> S#{ fd => undefined };
                {error, enoent} -> S#{ fd => undefined };
                {error, DelReason} ->
                    (poison(S, {seal_failed, DelReason}))#{ fd => undefined }
            end;
        _ ->
            % The marks only shorten the next start's scan, so a full disk
            % that refuses them does not stop the seal.
            S1 = write_marks(S#{ sealed => N }),
            S2 = case file:rename(Open, seg_name(Dir, N)) of
                     ok ->
                         case sync_dir(Dir) of
                             ok -> S1;
                             {error, SyncReason} -> poison(S1, {sync_failed, SyncReason})
                         end;
                     {error, Reason} ->
                         ?event(error, {offsite_journal_seal_failed, Reason}),
                         poison(S1, {seal_failed, Reason})
                 end,
            case hb_name:lookup({?MODULE, shipper, maps:get(<<"name">>, Store)}) of
                undefined -> ok;
                Pid -> Pid ! wake
            end,
            S2#{ fd => undefined }
    end.

open_segment(S = #{ dir := Dir, seg := N }) ->
    case file:open(open_name(Dir, N), [read, write, raw, binary]) of
        {ok, Fd} ->
            {ok, _} = file:position(Fd, eof),
            S1 = S#{ fd => Fd, off => 0, opened => erlang:monotonic_time(millisecond) },
            case sync_dir(Dir) of
                ok -> S1;
                {error, Reason} -> poison(S1, {sync_failed, Reason})
            end;
        {error, Reason} ->
            ?event(error, {offsite_journal_open_failed, Reason}),
            S#{ fd => undefined }
    end.

%% @doc Sync a directory, so a rename or a new file in it survives a host
%% crash. Erlang cannot open a directory, so this runs `sync <dir>' (fsync),
%% with no shell between (any path goes as one argument), and its exit
%% status is the answer.
sync_dir(Dir) ->
    case os:find_executable("sync") of
        false -> {error, {sync_dir, hb_util:bin(Dir), no_sync_executable}};
        Exe ->
            Port = open_port({spawn_executable, Exe},
                             [{args, [hb_util:list(Dir)]}, exit_status,
                              stderr_to_stdout, binary]),
            sync_dir_wait(Port, Dir, <<>>)
    end.

sync_dir_wait(Port, Dir, Out) ->
    receive
        {Port, {data, Data}} -> sync_dir_wait(Port, Dir, <<Out/binary, Data/binary>>);
        {Port, {exit_status, 0}} -> ok;
        {Port, {exit_status, Status}} ->
            {error, {sync_dir, hb_util:bin(Dir), Status, Out}}
    end.

stop_writer(S = #{ poisoned := P }) when P =/= false -> S;
stop_writer(S0) ->
    {_, S} = do_flush(S0),
    S1 =
        case append({stop}, S) of
            {ok, SA} -> SA;
            {error, _, SA} -> SA
        end,
    S2 = seal_current(seal_closed(sync_journal(S1))),
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
    Written =
        maybe
            ok ?= write_synced(Tmp, term_to_binary({marks, Sealed, Marks, Token})),
            ok = file:rename(Tmp, File),
            sync_dir(Dir)
        end,
    case Written of
        ok -> ok;
        {error, Reason} -> ?event(error, {offsite_journal_marks_failed, Reason})
    end,
    S.

new_token() -> hb_util:to_hex(crypto:strong_rand_bytes(4)).

token_bin(T) -> hb_util:bin(T).

%% @doc Recover the local journal, one log in segment order: every `open-'
%% file, and `seg-' files above the last seal, are read and their confirmed
%% assignments folded into the marks; every `open-' file is sealed. An
%% `open-' file was never confirmed past its last sync, so one that does not
%% end in `{closed, N}', or is not whole frames, ends the log (`ends_log/4'):
%% it is cut at its first bad frame, and the `open-' files after it are
%% dropped. A `seg-' file was synced whole: damage in one, or one after the
%% end of the log, refuses (raises), before anything is changed. A failed
%% sync raises too, and is never retried. Returns the highest segment number
%% seen.
recover_local(Dir, Sealed, Marks) ->
    {Max, Replay, Seals, Drop, _End} =
        lists:foldl(
            fun({N, open, File}, {Max, R, Seals, Drop, End}) when End =/= false ->
                    {max(Max, N), R, Seals, [File | Drop], End};
               ({_N, seg, File}, {_, _, _, _, End}) when End =/= false ->
                    corrupt(File, {after_end_of_log, End});
               ({N, Kind, File}, {Max, R, Seals, Drop, false}) ->
                    {ok, Bin} = file:read_file(File),
                    Valid = valid_prefix(Bin, 0),
                    Whole = Valid == byte_size(Bin),
                    case {Kind, Whole} of
                        {seg, false} -> corrupt(File, Valid);
                        _ -> ok
                    end,
                    Recs = case Kind =:= open orelse N > Sealed of
                               true -> records(binary:part(Bin, 0, Valid));
                               false -> []
                           end,
                    Seals1 = case Kind of
                                 open -> [{N, File, Valid, byte_size(Bin)} | Seals];
                                 seg -> Seals
                             end,
                    {max(Max, N), replay(Recs, R), Seals1, Drop,
                     case ends_log(Kind, N, Whole, Recs) of
                         true -> File;
                         false -> false
                     end}
            end,
            {-1, replay_init(fun(Pairs, M) -> mark(Pairs, M) end, Marks), [], [], false},
            local_files(Dir)
        ),
    % What follows the end goes first, durably: a later start must not find
    % the cut file whole with them still after it.
    case Drop of
        [] -> ok;
        _ ->
            ?event(warning, {offsite_journal_dropped_after_end, {files, Drop}}),
            lists:foreach(fun(F) -> ok = file:delete(F) end, Drop),
            recovery_sync(Dir, sync_dir(Dir))
    end,
    lists:foreach(fun(Seal) -> recover_seal(Dir, Seal) end, lists:reverse(Seals)),
    recovery_sync(Dir, sync_dir(Dir)),
    {Max, replay_finish(Replay)}.

%% Whether a file ends the local log if anything follows it: an `open-' one
%% that is not whole frames ending in its own `{closed, N}' (the last file
%% of the log never has one). A `seg-' file was synced whole before it was
%% renamed, and needs none.
ends_log(seg, _N, _Whole, _Recs) -> false;
ends_log(open, N, Whole, Recs) ->
    not (Whole andalso Recs =/= [] andalso lists:last(Recs) =:= {closed, N}).

corrupt(File, Off) ->
    ?event(error, {offsite_journal_corrupt, {file, File}, {offset, Off}}),
    erlang:error({offsite_journal_corrupt, File, Off}).

%% Seal an `open-' file: cut it at its last whole frame, sync it (the last
%% run's tail may be in the page cache only: a segment is renamed sealed
%% only once synced), rename it. One with nothing whole in it is deleted.
recover_seal(_Dir, {_N, File, 0, _Size}) ->
    ok = file:delete(File);
recover_seal(Dir, {N, File, Valid, Size}) ->
    case Valid < Size of
        true ->
            ?event(warning, {offsite_journal_torn_tail,
                {file, File}, {kept, Valid}, {size, Size}}),
            {ok, Fd} = file:open(File, [read, write, raw, binary]),
            {ok, _} = file:position(Fd, Valid),
            ok = file:truncate(Fd),
            Synced = datasync(Fd),
            _ = file:close(Fd),
            recovery_sync(File, Synced);
        false ->
            recovery_sync(File, sync_file(File))
    end,
    ok = file:rename(File, seg_name(Dir, N)).

recovery_sync(_What, ok) -> ok;
recovery_sync(What, {error, Reason}) ->
    ?event(error, {offsite_journal_recovery_sync_failed, What, Reason}),
    erlang:error({offsite_journal_sync_failed, What, Reason}).

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
step({closed, _N}, R) -> R;    % the log goes on in the next segment
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

%%% Frames: `HBJ1', length, CRC32, term. What follows the last whole frame
%%% is never searched for more: a payload can carry a whole frame of its own.

valid_prefix(Bin, Off) ->
    case Bin of
        <<_:Off/binary, "HBJ1", Len:32, Crc:32, Frame:Len/binary, _/binary>> when Len > 0 ->
            case erlang:crc32(Frame) of
                Crc -> valid_prefix(Bin, Off + 12 + Len);
                _ -> Off
            end;
        _ -> Off
    end.

%% Whether a file is whole frames, end to end.
whole(Bin) -> valid_prefix(Bin, 0) == byte_size(Bin).

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
             {error, Reason, SE} -> throw({not_started, {reconcile, Reason}, SE})
         end,
    case maps:get(poisoned, S2) of
        false -> ok;
        Why -> throw({not_started, {reconcile, Why}, S2})
    end,
    S2#{ reconciled => Count,
         marks => merge_marks(maps:get(pending, S2), maps:get(marks, S2)),
         pending => #{} }.

journal_rows([], S) -> S;
journal_rows(Rows, S) ->
    case append({w, Rows}, S) of
        {ok, S1} -> S1#{ pending => mark(Rows, maps:get(pending, S1)) };
        {error, Reason, SE} -> throw({not_started, {reconcile, Reason}, SE})
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
%% cannot go. A poisoned journal ships nothing until `clear_poison/1', and a
%% poisoned shipper nothing until `clear_ship_poison/1'.
ship_round(_Store, St = #{ ship_poisoned := Why }) ->
    St#{ state => {ship_poisoned, Why} };
ship_round(Store, St) ->
    Dir = journal_dir(Store),
    case {read_poison(Dir), read_poison(Dir, "ship-poisoned")} of
        {{poisoned, Why}, _} -> St#{ state => {poisoned, Why} };
        {none, {poisoned, Why}} -> St#{ state => {ship_poisoned, Why}, ship_poisoned => Why };
        {none, none} ->
            try
                Segs = [ {N, F} || {N, seg, F} <- local_files(journal_dir(Store)) ],
                Chain =
                    case maps:get(chain, St) of
                        undefined -> manifest_tip(target_dir(Store));
                        Known -> Known
                    end,
                ship_segs(Segs, Store, St#{ state => idle, chain => Chain })
            catch
                error:{sync_failed, Reason} -> ship_failed(Store, St, {sync_failed, Reason});
                C:R ->
                    ?event(error, {offsite_journal_ship_failed, C, R}),
                    failed(St#{ chain => undefined }, {C, R})
            end
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
            ship_failed(Store, St, Reason)
    end.

%% @doc A failed sync on the ship path is never retried (a retry can report
%% success for pages the kernel dropped). One on the target stops the
%% shipper, in this run and later ones (the `ship-poisoned' marker), until
%% `clear_ship_poison/1'; the writer goes on, and the backlog stays local.
%% One of the journal directory, after a shipped segment's local copy is
%% deleted, poisons the journal: that sync covers the writer's own new and
%% renamed segments too, and its error is reported once, so the writer's
%% next directory sync could pass with one of its entries lost. Anything
%% else backs off, and the manifest's tip is read again before the next
%% attempt.
ship_failed(Store, St, {journal_sync_failed, Reason}) ->
    Why = {sync_failed, {ship, Reason}},
    poison_journal(Store, Why),
    St#{ state => {poisoned, Why}, chain => undefined };
ship_failed(Store, St, Why = {sync_failed, _}) ->
    Dir = journal_dir(Store),
    ?event(error, {offsite_journal_ship_poisoned, Why}),
    case write_synced(filename:join(Dir, "ship-poisoned"), term_to_binary({poisoned, Why})) of
        ok ->
            case sync_dir(Dir) of
                ok -> ok;
                {error, Reason} -> poison_journal(Store, {sync_failed, {ship, Reason}})
            end;
        Err -> ?event(error, {offsite_journal_ship_poison_marker_failed, Err})
    end,
    St#{ state => {ship_poisoned, Why}, ship_poisoned => Why, chain => undefined };
ship_failed(_Store, St, Reason) ->
    failed(St#{ chain => undefined }, Reason).

poison_journal(Store, Why) ->
    write_poison(journal_dir(Store), Why),
    _ = (catch call(Store, {poison, Why}, 5000)),
    ok.

%% @doc Let the shipper ship again after a failed sync on the target, once
%% the target is checked: a `.tmp' is written again from the local copy, and
%% the manifest's tip is read again.
clear_ship_poison(Store) ->
    Dir = journal_dir(Store),
    stop_shipper(Store),
    hb_name:unregister({?MODULE, shipper, maps:get(<<"name">>, Store)}),
    ok = case file:delete(filename:join(Dir, "ship-poisoned")) of
             ok -> ok;
             {error, enoent} -> ok;
             E -> E
         end,
    ok = sync_dir(Dir),
    _ = shipper(Store),
    ok.

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
                    case file:sync(F) of
                        ok -> ok;
                        {error, Reason} -> erlang:error({sync_failed, Reason})
                    end,
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
    case whole(Raw) of
        true -> ok;
        false -> erlang:error({offsite_journal_corrupt, File, valid_prefix(Raw, 0)})
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
        ok ?= synced(sync_dir(Target)),
        Line = io_lib:format("~s ~B ~B ~s~n",
                             [Name, byte_size(Body), erlang:crc32(Body), Chain]),
        ok ?= append_synced(filename:join(Target, "manifest.log"), Line),
        ok ?= synced(sync_dir(Target)),
        ok ?= file:delete(File),
        % Or a host crash brings it back, to be shipped again.
        ok ?= case sync_dir(journal_dir(Store)) of
                  ok -> ok;
                  {error, DirReason} -> {error, {journal_sync_failed, DirReason}}
              end,
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
                synced(file_sync(File, F))
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
                synced(file_sync(File, F))
            end,
        _ = file:close(F),
        R
    end.

file_sync(File, F) ->
    % Test hook: a failing disk under one directory (the target).
    case persistent_term:get({?MODULE, fail_sync_under}, undefined) of
        Dir when is_list(Dir) ->
            case lists:prefix(Dir, hb_util:list(File)) of
                true -> {error, eio};
                false -> file:sync(F)
            end;
        _ -> file:sync(F)
    end.

%% A failed sync, told apart from a failed write: the shipper never retries
%% one (`ship_failed/3').
synced(ok) -> ok;
synced({error, Reason}) -> {error, {sync_failed, Reason}}.

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
        {R1, Counts, _End} =
            lists:foldl(
                % One log, as `recover_local/3' reads it: a local `open-'
                % file that is torn, or lacks its `{closed, N}', ends it,
                % and only `open-' files may follow it, never confirmed
                % (dropped).
                fun({_, _File, {local, open}}, {R, C, End}) when End =/= false ->
                        {R, C#{ dropped_files => maps:get(dropped_files, C) + 1 }, End};
                   ({_, File, _}, {_, _, End}) when End =/= false ->
                        throw({restore_error, {after_end_of_log, File}});
                   ({{_, N}, File, Check}, {R, C, false}) ->
                        restore_file(N, File, Check, R, C)
                end,
                {R0, #{ files => 0, torn => 0, dropped_files => 0 }, false},
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

restore_file(N, File, Check, R, C) ->
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
    Whole = Valid == byte_size(Plain),
    Torn =
        case {Whole, Check} of
            {true, _} -> 0;
            % Only a segment being written can be torn, at its first bad
            % frame; a sealed one was synced whole, and a shipped one
            % checked against its manifest.
            {false, {local, open}} -> 1;
            _ -> throw({restore_error, {corrupt, File, Valid}})
        end,
    Recs = records(binary:part(Plain, 0, Valid)),
    Kind = case Check of {local, open} -> open; _ -> seg end,
    {replay(Recs, R),
     C#{ files => maps:get(files, C) + 1, torn => maps:get(torn, C) + Torn },
     case ends_log(Kind, N, Whole, Recs) of
         true -> File;
         false -> false
     end}.

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

%% @doc A start that finds the last run's open segment whole syncs it before
%% renaming it sealed: its tail may be in the page cache only (a kill before
%% the round that would have synced it). Renamed unsynced, a host crash then
%% leaves a sealed segment whose tail is torn, and every later start refuses
%% the journal (`offsite_journal_corrupt').
recovered_open_segment_synced_before_sealed_test() ->
    {Store, _Inner, _Target} = test_setup(<<"oj-recover-sync">>, #{ <<"fsync-ms">> => 600000 }),
    ok = write(Store, #{ <<"s/1">> => <<"1">> }, #{}),
    ok = flush(Store, #{}, #{}),
    % Appended, never synced: the writer dies before any round.
    ok = write(Store, #{ <<"s/2">> => <<"2">> }, #{}),
    kill_writer(Store),
    Dir = journal_dir(Store),
    [{N, open, Open}] = [ F || F = {_, open, _} <- local_files(Dir) ],
    {Sealed, Marks, _} = read_marks(Dir),
    Self = self(),
    Worker = spawn(fun() -> receive go -> Self ! {recovered, recover_local(Dir, Sealed, Marks)} end end),
    Patterns = [{?MODULE, datasync, 1}, {file, rename, 2}],
    [ erlang:trace_pattern(P, true, [local]) || P <- Patterns ],
    erlang:trace(Worker, true, [call]),
    Worker ! go,
    receive {recovered, _} -> ok after 30000 -> error(recover_timeout) end,
    [ erlang:trace_pattern(P, false, [local]) || P <- Patterns ],
    Ref = erlang:trace_delivered(Worker),
    receive {trace_delivered, _, Ref} -> ok end,
    Calls = collect_calls(Open),
    ?assert(filelib:is_regular(seg_name(Dir, N))),
    ?assertMatch([synced, {renamed, Open} | _], Calls).

collect_calls(Open) ->
    receive
        {trace, _, call, {?MODULE, datasync, _}} -> [synced | collect_calls(Open)];
        {trace, _, call, {file, rename, [From, _]}} ->
            case filename:absname(From) == filename:absname(Open) of
                true -> [{renamed, Open} | collect_calls(Open)];
                false -> collect_calls(Open)
            end
    after 0 -> []
    end.

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

%% @doc Sixteen callers flushing at once share one round: one sync of the
%% journal, one flush of the inner store, and every one of them is answered.
concurrent_flushes_share_one_round_test() ->
    {Store, _Inner, Target} = test_setup(<<"oj-group">>, #{ <<"fsync-ms">> => 600000 }),
    ok = write(Store, #{ <<"g/0">> => <<"0">> }, #{}),
    ok = flush(Store, #{}, #{}),
    Syncer = hb_name:lookup({?MODULE, syncer, maps:get(<<"name">>, Store)}),
    #{ syncer := #{ rounds := R0 } } = status(Store),
    erlang:suspend_process(Syncer),
    Parent = self(),
    N = 16,
    [ spawn(fun() ->
          ok = write(Store, #{ <<"g/", (integer_to_binary(I))/binary>> => <<"v">> }, #{}),
          Parent ! {flushed, I, flush(Store, #{}, #{})}
      end) || I <- lists:seq(1, N) ],
    ok = wait_until(fun() -> element(2, process_info(Syncer, message_queue_len)) == N end),
    Pattern = [{?MODULE, datasync, 1}, {hb_store_lmdb, flush, 3}],
    [ erlang:trace_pattern(MFA, true, [local, call_count]) || MFA <- Pattern ],
    erlang:trace(Syncer, true, [call]),
    erlang:trace(writer_pid(Store), true, [call]),
    erlang:resume_process(Syncer),
    Replies = [ receive {flushed, I, R} -> R after 10000 -> timeout end
              || I <- lists:seq(1, N) ],
    [Syncs, InnerFlushes] =
        [ element(2, erlang:trace_info(MFA, call_count)) || MFA <- Pattern ],
    [ erlang:trace_pattern(MFA, false, [local, call_count]) || MFA <- Pattern ],
    ?assertEqual(lists:duplicate(N, ok), Replies),
    ?assertEqual(1, Syncs),
    ?assertEqual(1, InnerFlushes),
    ?assertMatch(#{ syncer := #{ rounds := R } } when R == R0 + 1, status(Store)),
    % What they wrote is confirmed: it restores.
    idle = ship_now(Store),
    Fresh = fresh_store(<<"oj-group-r">>),
    {ok, _} = restore(Target, Fresh),
    [ ?assertEqual({ok, <<"v">>},
          hb_store:read([Fresh], <<"g/", (integer_to_binary(I))/binary>>, #{}))
    || I <- lists:seq(1, N) ].

%% @doc A sync in progress does not hold up appends: with every sync of the
%% journal taking a second, a write made while a flush syncs is answered at
%% once, and the flush still waits for its sync.
appends_do_not_wait_for_a_sync_test() ->
    {Store, _Inner, _Target} = test_setup(<<"oj-overlap">>, #{ <<"fsync-ms">> => 600000 }),
    ok = write(Store, #{ <<"o/1">> => <<"1">> }, #{}),
    persistent_term:put({?MODULE, sync_delay_ms}, 1000),
    try
        Parent = self(),
        spawn(fun() -> Parent ! {flushed, timer:tc(fun() -> flush(Store, #{}, #{}) end)} end),
        timer:sleep(200),
        {WriteUs, ok} = timer:tc(fun() -> write(Store, #{ <<"o/2">> => <<"2">> }, #{}) end),
        {FlushUs, ok} = receive {flushed, F} -> F after 10000 -> timeout end,
        ?assert(WriteUs < 300000),
        ?assert(FlushUs >= 1000000)
    after
        persistent_term:erase({?MODULE, sync_delay_ms})
    end.

wait_until(F) -> wait_until(F, 500).
wait_until(_F, 0) -> timeout;
wait_until(F, N) ->
    case F() of
        true -> ok;
        false -> timer:sleep(10), wait_until(F, N - 1)
    end.

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

%% @doc The journal is poisoned after a round's cut, by a failed sync of the
%% very segment the round is syncing (the writer's own sync; the syncer's,
%% on its own descriptor, passes). The round must not answer `ok': the writer
%% refuses `round_done', and every waiter of the round hears the refusal.
poison_after_cut_fails_round_test() ->
    {Store, _Inner, _Target} = test_setup(<<"oj-poison-cut">>, #{ <<"fsync-ms">> => 600000 }),
    ok = write(Store, #{ <<"a/1">> => <<"1">> }, #{}),
    Parent = self(),
    persistent_term:put({?MODULE, sync_delay_ms}, 500),
    try
        [ spawn(fun() -> Parent ! {flushed, I, flush(Store, #{}, #{})} end)
        || I <- [1, 2] ],
        timer:sleep(100),          % the syncer has its cut and sleeps in datasync
        persistent_term:put({?MODULE, sync_delay_ms}, 0),
        persistent_term:put({?MODULE, fail_sync}, true),
        SealReply = call(Store, seal, 5000),   % the writer's own sync fails
        persistent_term:erase({?MODULE, fail_sync}),
        ?assertMatch({error, {journal_poisoned, _}}, SealReply),
        [ ?assertMatch({failure, {offsite_journal, {journal_poisoned, _}}},
              receive {flushed, I, F} -> F after 10000 -> timeout end)
        || I <- [1, 2] ]
    after
        persistent_term:erase({?MODULE, sync_delay_ms}),
        persistent_term:erase({?MODULE, fail_sync})
    end.

%% @doc A batch is one writer call: every write in it is held, then journaled
%% as one frame and written to the inner store write by write, in order (a
%% later write of a key wins). A read that reaches a held row inside the batch
%% raises rather than missing it; a batch whose function raises writes
%% nothing. The rows restore like any others.
batch_is_one_writer_call_test() ->
    {Store, Inner, Target} = test_setup(<<"oj-batch">>, #{}),
    ok = write(Store, #{ <<"b/0">> => <<"0">> }, #{}),
    #{ appended := A0 } = status(Store),
    done =
        batch(Store,
            fun() ->
                ok = hb_store:write([Store], #{ <<"b/1">> => <<"1">> }, #{}),
                ok = hb_store:write([Store], #{ <<"b/2">> => <<"old">>,
                                                <<"b/3">> => <<"3">> }, #{}),
                ok = hb_store:write([Store], #{ <<"b/2">> => <<"2">> }, #{}),
                % Nested: joins this batch.
                ok = batch(Store,
                         fun() -> hb_store:write([Store], #{ <<"c/1">> => <<"c">> }, #{}) end,
                         #{}),
                ?assertError({read_of_held_write, <<"b/1">>},
                    read(Store, #{ <<"read">> => <<"b/1">> }, #{})),
                ?assertError({read_of_held_write, <<"c">>},
                    list(Store, #{ <<"list">> => <<"c">> }, #{})),
                % A row written before the batch reads as usual, also when
                % the batch writes it again with the same value; with another
                % value it would read stale, so it raises.
                ?assertEqual({ok, <<"0">>}, hb_store:read([Store], <<"b/0">>, #{})),
                ok = hb_store:write([Store], #{ <<"b/0">> => <<"0">> }, #{}),
                ?assertEqual({ok, <<"0">>}, read(Store, #{ <<"read">> => <<"b/0">> }, #{})),
                ok = hb_store:write([Store], #{ <<"b/00">> => <<"x">> }, #{}),
                ok = hb_store:write([Store], #{ <<"b/0">> => <<"changed">> }, #{}),
                ?assertError({read_of_held_write, <<"b/0">>},
                    read(Store, #{ <<"read">> => <<"b/0">> }, #{})),
                ok = hb_store:write([Store], #{ <<"b/0">> => <<"0">> }, #{}),
                done
            end,
            #{}),
    ?assertMatch(#{ appended := A } when A == A0 + 1, status(Store)),
    Rows = #{ <<"b/0">> => <<"0">>, <<"b/00">> => <<"x">>, <<"b/1">> => <<"1">>,
              <<"b/2">> => <<"2">>, <<"b/3">> => <<"3">>, <<"c/1">> => <<"c">> },
    assert_rows(Inner, Rows),
    ?assertError(boom,
        batch(Store,
            fun() ->
                ok = hb_store:write([Store], #{ <<"d/1">> => <<"1">> }, #{}),
                error(boom)
            end,
            #{})),
    ?assertEqual({error, not_found}, hb_store:read([Inner], <<"d/1">>, #{})),
    ?assertMatch(#{ appended := A } when A == A0 + 1, status(Store)),
    ok = flush(Store, #{}, #{}),
    idle = ship_now(Store),
    Fresh = fresh_store(<<"oj-batch-r">>),
    {ok, _} = restore(Target, Fresh),
    assert_rows(Fresh, Rows),
    ?assertEqual({error, not_found}, hb_store:read([Fresh], <<"d/1">>, #{})).

%% @doc A row is dropped only when its key holds the same value earlier in
%% the frame; `A', `B', `A' keeps all three, in order.
dedupe_test() ->
    ?assertEqual(
        [[{a, 1}, {b, 2}], [{c, 3}], [{a, 2}], [{a, 1}]],
        dedupe([[{a, 1}, {b, 2}], [{a, 1}, {c, 3}], [{b, 2}], [{a, 2}], [{a, 1}]])).

%% @doc A batch that writes the same rows twice journals them once, and
%% restores to the same store.
batch_journals_repeated_rows_once_test() ->
    {Store, Inner, Target} = test_setup(<<"oj-dedupe">>, #{}),
    Body = rows(<<"body/">>, 1, 3),
    ok = batch(Store,
            fun() ->
                ok = hb_store:write([Store], Body, #{}),
                ok = hb_store:write([Store], Body#{ <<"slot/1">> => <<"a">> }, #{}),
                ok = hb_store:write([Store], Body#{ <<"slot/2">> => <<"b">> }, #{})
            end,
            #{}),
    Rows = Body#{ <<"slot/1">> => <<"a">>, <<"slot/2">> => <<"b">> },
    assert_rows(Inner, Rows),
    ok = flush(Store, #{}, #{}),
    idle = ship_now(Store),
    Fresh = fresh_store(<<"oj-dedupe-r">>),
    ?assertMatch({ok, #{ rows := 5 }}, restore(Target, Fresh)),
    assert_rows(Fresh, Rows),
    ok = hb_store:stop([Store, Fresh], #{}, #{}).

%% @doc A content-addressed row the inner store holds is not put again, but
%% is still journaled. (Writing another value under one is impossible in use;
%% here it shows which copy got the write.)
stored_content_is_not_put_again_test() ->
    {Store, Inner, Target} = test_setup(<<"oj-ni">>, #{}),
    Key = <<"ni:%2f%2f%2fsha-256;test">>,
    ok = write(Store, #{ Key => <<"first">> }, #{}),
    % Only a committed row is seen: an uncommitted one is simply put again.
    ok = flush(Store, #{}, #{}),
    ok = write(Store, #{ Key => <<"second">>, <<"other">> => <<"o">> }, #{}),
    assert_rows(Inner, #{ Key => <<"first">>, <<"other">> => <<"o">> }),
    ok = flush(Store, #{}, #{}),
    idle = ship_now(Store),
    Fresh = fresh_store(<<"oj-ni-r">>),
    ?assertMatch({ok, #{ rows := 3 }}, restore(Target, Fresh)),
    assert_rows(Fresh, #{ Key => <<"second">>, <<"other">> => <<"o">> }),
    ok = hb_store:stop([Store, Fresh], #{}, #{}).

%% @doc A refused batch raises after its function, and writes nothing.
batch_refused_raises_test() ->
    {Store, Inner, _Target} = test_setup(<<"oj-batch-refused">>, #{}),
    ok = write(Store, #{ <<"e/0">> => <<"0">> }, #{}),
    persistent_term:put({?MODULE, fail_write}, true),
    try
        ?assertThrow({failure, {offsite_journal, _}},
            batch(Store,
                fun() -> hb_store:write([Store], #{ <<"e/1">> => <<"1">> }, #{}) end,
                #{}))
    after
        persistent_term:erase({?MODULE, fail_write})
    end,
    ?assertEqual({error, not_found}, hb_store:read([Inner], <<"e/1">>, #{})).

%% @doc What any commit of the inner store takes from a batch is a prefix of
%% the batch's writes: a commit that races the batch (another process's
%% syncer round, the inner store's own flusher) never keeps a slot link
%% without the rows written before it, nor a later link without an earlier
%% one. The batch is 32 assignments, each 40 body rows, then the slot link,
%% then the index row, in that order. A commit fires once the batch has begun
%% writing; the data file is copied before anything else commits, which is
%% what a `kill -9' at that moment leaves. Fails when the batch is one inner
%% write (one `put_batch', in map order).
batch_commit_is_a_prefix_test_() ->
    {timeout, 300, fun batch_commit_is_a_prefix/0}.

batch_commit_is_a_prefix() ->
    Body = fun(S) -> [ {<<"m", (integer_to_binary(S))/binary, "/", (integer_to_binary(J))/binary>>,
                        <<"v">>} || J <- lists:seq(1, 40) ] end,
    Link = fun(S) -> <<"~scheduler@1.0/assignments/P/", (integer_to_binary(S))/binary>> end,
    Idx = fun(S) -> <<"~scheduler@1.0/message-slots/P/id", (integer_to_binary(S))/binary>> end,
    Torn =
        lists:filtermap(
            fun(T) ->
                Tag = <<"oj-prefix-", (integer_to_binary(T))/binary>>,
                Inner = (hb_test_utils:test_store(hb_store_lmdb, Tag))#{
                    <<"flush-every">> => 0, <<"batch-size">> => 1000000000 },
                Store = wrap(Inner, #{ <<"path">> => <<(maps:get(<<"name">>, Inner))/binary, "-export">>,
                                       <<"fsync-ms">> => 600000 }),
                ok = hb_store:start([Store], #{}, #{}),
                #{ <<"db">> := DB } = hb_store:find(Inner),
                ok = elmdb:flush(DB),
                Parent = self(),
                Threshold = 50 + 50 * (T rem 20),
                Racer =
                    fun Spin(Deadline) ->
                        case elmdb:overlay_count(DB) of
                            C when C > Threshold -> elmdb:flush(DB);
                            _ ->
                                case erlang:monotonic_time(millisecond) > Deadline of
                                    true -> no_race;
                                    false -> Spin(Deadline)
                                end
                        end
                    end,
                spawn(fun() -> Parent ! {raced, Racer(erlang:monotonic_time(millisecond) + 30000)} end),
                timer:sleep(5),
                ok = batch(Store,
                        fun() ->
                            lists:foreach(
                                fun(S) ->
                                    [ ok = hb_store:write([Store], #{ K => V }, #{}) || {K, V} <- Body(S) ],
                                    ok = hb_store:write([Store], #{ Link(S) => <<"ao:m">> }, #{}),
                                    ok = hb_store:write([Store], #{ Idx(S) => integer_to_binary(S) }, #{})
                                end,
                                lists:seq(0, 31))
                        end,
                        #{}),
                receive {raced, _} -> ok end,
                Snap = committed_snapshot(Inner, Tag),
                Has = fun(K) -> case elmdb:get(Snap, K) of {ok, _} -> true; _ -> false end end,
                Links = [ S || S <- lists:seq(0, 31), Has(Link(S)) ],
                Orphans = [ S || S <- Links, not lists:all(fun({K, _}) -> Has(K) end, Body(S)) ],
                Holes = [ S || S <- lists:seq(0, 31), not lists:member(S, Links),
                               lists:any(fun(L) -> L > S end, Links) ],
                case {Orphans, Holes} of
                    {[], []} -> false;
                    _ -> {true, {T, {orphan_links, Orphans}, {holes, Holes}}}
                end
            end,
            lists:seq(1, 12)),
    ?assertEqual([], Torn).

%% The inner store's committed state: a copy of its data file, opened on its
%% own.
committed_snapshot(Inner, Tag) ->
    Dir = hb_util:list(maps:get(<<"name">>, Inner)),
    Copy = hb_util:list(<<Tag/binary, "-committed">>),
    os:cmd("rm -rf " ++ Copy),
    ok = filelib:ensure_dir(filename:join(Copy, "x")),
    {ok, _} = file:copy(filename:join(Dir, "data.mdb"), filename:join(Copy, "data.mdb")),
    {ok, Env} = elmdb:env_open(Copy, [{map_size, 1 bsl 30}]),
    {ok, DB} = elmdb:db_open(Env, [create]),
    DB.

%% @doc A segment roll does not hold up appends: with every sync taking 200
%% ms and a roll after every append, appends still take a few milliseconds
%% (a roll synced, marked and renamed the segment in the writer, and the next
%% append waited for it). The next round syncs every closed segment's tail
%% before it confirms, and renames each sealed; what it confirmed restores.
roll_does_not_hold_up_appends_test() ->
    {Store, _Inner, Target} = test_setup(<<"oj-roll">>, #{ <<"segment-bytes">> => 1,
                                                            <<"fsync-ms">> => 600000 }),
    ok = write(Store, #{ <<"r/0">> => <<"0">> }, #{}),
    ok = flush(Store, #{}, #{}),
    persistent_term:put({?MODULE, sync_delay_ms}, 200),
    Rows = rows(<<"r/">>, 1, 5),
    try
        Times =
            [ element(1, timer:tc(fun() -> ok = write(Store, #{ K => V }, #{}) end)) div 1000
            || {K, V} <- lists:sort(maps:to_list(Rows)) ],
        ?assert(lists:max(Times) < 100),
        #{ seg := Before } = status(Store),
        erlang:trace_pattern({?MODULE, datasync, 1}, true, [local, call_count]),
        Syncer = hb_name:lookup({?MODULE, syncer, maps:get(<<"name">>, Store)}),
        erlang:trace(if is_pid(Syncer) -> Syncer; true -> all end, true, [call]),
        ok = flush(Store, #{}, #{}),
        {call_count, Syncs} = erlang:trace_info({?MODULE, datasync, 1}, call_count),
        erlang:trace_pattern({?MODULE, datasync, 1}, false, [local, call_count]),
        % Five closed segments, each with an unsynced append.
        ?assert(Syncs >= 5),
        % ...each renamed sealed by the round (a roll since may have closed
        % the one the round's cut was in).
        ?assertEqual([], [ N || {N, open, _} <- local_files(journal_dir(Store)), N < Before ])
    after
        persistent_term:erase({?MODULE, sync_delay_ms})
    end,
    idle = ship_now(Store),
    Fresh = fresh_store(<<"oj-roll-r">>),
    {ok, _} = restore(Target, Fresh),
    assert_rows(Fresh, Rows#{ <<"r/0">> => <<"0">> }).

test_dir(Tag) ->
    Dir = hb_util:list(maps:get(<<"name">>, hb_test_utils:test_store(hb_store_fs, Tag))),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

%% @doc A torn tail is cut at its first bad frame, whatever follows it: here
%% the frame torn is a write whose value carries whole frames of its own (a
%% message's data can), which a search past the damage took for an intact
%% frame after it, refusing the journal for good. A restore of the torn file
%% takes everything before the cut; the writer starts, cuts it and runs.
embedded_frame_in_torn_tail_test() ->
    {Store, _Inner, Target} = test_setup(<<"oj-embed">>, #{}),
    Forged = frame({w, [{<<"x/1">>, <<"forged">>}]}),
    Evil = <<Forged/binary, Forged/binary>>,
    Before = #{ <<"e/1">> => Evil, <<"e/2">> => <<"2">> },
    write_rows(Store, Before),
    ok = flush(Store, #{}, #{}),
    kill_writer(Store),
    Dir = journal_dir(Store),
    [{_, open, Open}] = [ F || F = {_, open, _} <- local_files(Dir) ],
    % The same value written again, cut one byte short: every frame inside
    % it is whole.
    Torn = frame({w, [{<<"e/3">>, Evil}]}),
    {ok, Fd} = file:open(Open, [append, raw, binary]),
    ok = file:write(Fd, binary:part(Torn, 0, byte_size(Torn) - 1)),
    ok = file:close(Fd),
    Local = fresh_store(<<"oj-embed-local">>),
    ?assertMatch({ok, #{ torn := 1 }}, restore(list_to_binary(Dir), Local)),
    assert_rows(Local, Before),
    After = #{ <<"e/4">> => <<"4">> },
    write_rows(Store, After),
    ok = flush(Store, #{}, #{}),
    ?assertEqual(idle, ship_now(Store)),
    Fresh = fresh_store(<<"oj-embed-r">>),
    {ok, _} = restore(Target, Fresh),
    assert_rows(Fresh, maps:merge(Before, After)),
    ?assertMatch({error, _}, hb_store:read([Fresh], <<"e/3">>, #{})),
    ?assertMatch({error, _}, hb_store:read([Fresh], <<"x/1">>, #{})).

%% @doc The journal is one log: a torn `open-' segment ends it. A later
%% `open-' one, here holding a `flushed' marker, was never confirmed (a sync
%% covers every lower segment first, and the torn one was never synced
%% through its tail), so it is dropped. Replayed, its marker confirmed the
%% torn file's unconfirmed write and its own, and the marks rose above a
%% slot only the lost tail held: a hole no restore gets past.
torn_open_segment_ends_the_log_test() ->
    Dir = test_dir(<<"oj-onelog">>),
    P = <<"P">>,
    Kept = <<(frame({start, false}))/binary,
             (frame({w, [{akey(P, 0), <<"0">>}]}))/binary,
             (frame({flushed}))/binary,
             (frame({w, [{akey(P, 1), <<"1">>}]}))/binary>>,
    ok = file:write_file(open_name(Dir, 1),
                         <<Kept/binary, (binary:part(frame({flushed}), 0, 7))/binary>>),
    ok = file:write_file(open_name(Dir, 2),
                         <<(frame({w, [{akey(P, 2), <<"2">>}]}))/binary,
                           (frame({flushed}))/binary>>),
    % A restore reads the log the same way.
    Fresh = fresh_store(<<"oj-onelog-r">>),
    ?assertMatch({ok, #{ torn := 1, dropped_files := 1, rows := 1 }},
                 restore(list_to_binary(Dir), Fresh)),
    ?assertEqual({ok, <<"0">>}, hb_store:read([Fresh], akey(P, 0), #{})),
    ?assertEqual({2, #{ P => 0 }}, recover_local(Dir, -1, #{})),
    ?assertEqual([{1, seg, seg_name(Dir, 1)}], local_files(Dir)),
    ?assertEqual({ok, Kept}, file:read_file(seg_name(Dir, 1))).

%% @doc A sealed segment was synced whole: one cut short is refused, not
%% cut, at or above the last seal, and so is a sealed segment after the end
%% of the log. A refusal changes nothing.
damaged_sealed_segment_refused_test() ->
    Dir = test_dir(<<"oj-sealed-torn">>),
    Whole = <<(frame({w, [{<<"a">>, <<"1">>}]}))/binary, (frame({flushed}))/binary>>,
    Short = binary:part(Whole, 0, byte_size(Whole) - 3),
    ok = file:write_file(seg_name(Dir, 1), Short),
    ?assertError({offsite_journal_corrupt, _, _}, recover_local(Dir, -1, #{})),
    ?assertError({offsite_journal_corrupt, _, _}, recover_local(Dir, 1, #{})),
    ?assertEqual({ok, Short}, file:read_file(seg_name(Dir, 1))),
    ok = file:write_file(seg_name(Dir, 1), Whole),
    Torn = <<Whole/binary, "HBJ1", 0>>,
    ok = file:write_file(open_name(Dir, 2), Torn),
    ok = file:write_file(seg_name(Dir, 3), Whole),
    ok = file:write_file(open_name(Dir, 4), Whole),
    ?assertError({offsite_journal_corrupt, _, {after_end_of_log, _}},
                 recover_local(Dir, -1, #{})),
    ?assertEqual({ok, Torn}, file:read_file(open_name(Dir, 2))),
    ?assert(filelib:is_regular(open_name(Dir, 4))),
    ?assertMatch({error, {after_end_of_log, _}},
                 restore(list_to_binary(Dir), fresh_store(<<"oj-sealed-torn-r">>))).

%% @doc A sync that fails while a start recovers the journal poisons it: the
%% writer refuses, the segment is not renamed sealed, and the next start
%% refuses too, though syncing works again. It is never synced a second
%% time (on Linux that can report success for pages the kernel dropped).
recovery_sync_failure_poisons_test() ->
    {Store, _Inner, _Target} = test_setup(<<"oj-recover-eio">>, #{ <<"fsync-ms">> => 600000 }),
    ok = write(Store, #{ <<"v/1">> => <<"1">> }, #{}),
    ok = flush(Store, #{}, #{}),
    kill_writer(Store),
    Dir = journal_dir(Store),
    [{N, open, Open}] = [ F || F = {_, open, _} <- local_files(Dir) ],
    persistent_term:put({?MODULE, fail_sync}, true),
    R1 = try write(Store, #{ <<"v/2">> => <<"2">> }, #{})
         after persistent_term:erase({?MODULE, fail_sync})
         end,
    ?assertMatch({failure, {offsite_journal, {journal_poisoned, _}}}, R1),
    ?assert(filelib:is_regular(Open)),
    ?assertNot(filelib:is_regular(seg_name(Dir, N))),
    kill_writer(Store),
    ?assertMatch({failure, {offsite_journal, {journal_poisoned, _}}},
                 write(Store, #{ <<"v/3">> => <<"3">> }, #{})),
    ?assertMatch(#{ refusing := {journal_poisoned, _} }, status(Store)),
    ?assert(filelib:is_regular(Open)),
    kill_writer(Store).

%% @doc A sync that fails while an unclean start reconciles poisons the
%% journal in this run: the attempt's segment is never synced again nor
%% renamed sealed, and nothing is confirmed after it, though syncing works
%% again.
failed_start_keeps_its_poison_test() ->
    {Store, _Inner, _Target} = test_setup(<<"oj-start-eio">>, #{ <<"fsync-ms">> => 600000 }),
    ok = write(Store, #{ <<"u/1">> => <<"1">> }, #{}),
    ok = flush(Store, #{}, #{}),
    % Sealed, so the start recovers no file and syncs nothing before the
    % reconcile does.
    _ = call(Store, seal, 5000),
    kill_writer(Store),
    Dir = journal_dir(Store),
    persistent_term:put({?MODULE, fail_sync}, true),
    R1 = try write(Store, #{ <<"u/2">> => <<"2">> }, #{})
         after persistent_term:erase({?MODULE, fail_sync})
         end,
    ?assertMatch({failure, {offsite_journal, {journal_poisoned, {sync_failed, _}}}}, R1),
    [{N, open, Open}] = [ F || F = {_, open, _} <- local_files(Dir) ],
    ?assertMatch({failure, {offsite_journal, {journal_poisoned, _}}},
                 write(Store, #{ <<"u/3">> => <<"3">> }, #{})),
    ?assertMatch({failure, _}, flush(Store, #{}, #{})),
    _ = call(Store, seal, 5000),
    ?assert(filelib:is_regular(Open)),
    ?assertNot(filelib:is_regular(seg_name(Dir, N))),
    kill_writer(Store).

%% @doc A directory sync reports failure, and any path is one argument.
sync_dir_reports_failure_test() ->
    Dir = test_dir(<<"oj sync dir">>),
    ?assertEqual(ok, sync_dir(Dir)),
    ?assertMatch({error, {sync_dir, _, _, _}}, sync_dir(filename:join(Dir, "missing"))).

%% @doc A segment closed for a successor ends in its `{closed, N}' frame, and
%% the log goes on past it. An `open-' file followed by another without that
%% frame lost its tail, here exactly on a frame boundary (or all of it, a
%% file of size 0, as delayed allocation can leave one whose successor
%% reached the disk): it ends the log, and the later file's `flushed' marker,
%% never confirmed, is dropped. Replayed, it confirmed rows only the lost
%% tail held, and the marks rose over slots that are gone.
unterminated_open_segment_ends_the_log_test() ->
    P = <<"P">>,
    Head = <<(frame({start, false}))/binary,
             (frame({w, [{akey(P, 0), <<"0">>}]}))/binary,
             (frame({flushed}))/binary,
             (frame({w, [{akey(P, 1), <<"1">>}]}))/binary>>,
    Next = <<(frame({w, [{akey(P, 2), <<"2">>}]}))/binary, (frame({flushed}))/binary>>,
    Case =
        fun(Tag, First) ->
            Dir = test_dir(<<"oj-closed-", Tag/binary>>),
            ok = file:write_file(open_name(Dir, 1), First),
            ok = file:write_file(open_name(Dir, 2), Next),
            Restored = restore(list_to_binary(Dir),
                               fresh_store(<<"oj-closed-r-", Tag/binary>>)),
            {Restored, recover_local(Dir, -1, #{}), local_files(Dir)}
        end,
    % Closed: open-2's marker confirms the write before the frame, and its own.
    {Closed, ClosedMarks, ClosedFiles} = Case(<<"whole">>, <<Head/binary, (frame({closed, 1}))/binary>>),
    ?assertMatch({ok, #{ rows := 3, dropped_files := 0 }}, Closed),
    ?assertEqual({2, #{ P => 2 }}, ClosedMarks),
    ?assertMatch([{1, seg, _}, {2, seg, _}], ClosedFiles),
    % Its `closed' frame lost: open-2 is dropped, and nothing past slot 0 is
    % marked.
    {Lost, LostMarks, LostFiles} = Case(<<"lost">>, Head),
    ?assertMatch({ok, #{ rows := 1, dropped_files := 1 }}, Lost),
    ?assertEqual({2, #{ P => 0 }}, LostMarks),
    ?assertMatch([{1, seg, _}], LostFiles),
    % All of it lost.
    {Empty, EmptyMarks, EmptyFiles} = Case(<<"empty">>, <<>>),
    ?assertMatch({ok, #{ rows := 0, dropped_files := 1 }}, Empty),
    ?assertEqual({2, #{}}, EmptyMarks),
    ?assertEqual([], EmptyFiles),
    % The writer's roll writes the frame.
    {Store, _Inner, _Target} =
        test_setup(<<"oj-closed-roll">>, #{ <<"segment-bytes">> => 1, <<"fsync-ms">> => 600000 }),
    ok = write(Store, #{ <<"k">> => <<"v">> }, #{}),
    kill_writer(Store),
    [{N, open, Rolled} | _] = local_files(journal_dir(Store)),
    {ok, Bin} = file:read_file(Rolled),
    ?assertEqual({closed, N}, lists:last(records(Bin))).

%% @doc A failed sync on the target stops the shipper, never the writer: the
%% writer goes on confirming, the backlog stays local, and the sync is never
%% tried again, by this shipper or the next, until `clear_ship_poison/1'.
%% Then everything ships and restores.
target_sync_failure_stops_only_the_shipper_test() ->
    {Store, _Inner, Target} = test_setup(<<"oj-ship-eio">>, #{}),
    Rows = rows(<<"t/">>, 1, 10),
    write_rows(Store, Rows),
    ok = flush(Store, #{}, #{}),
    persistent_term:put({?MODULE, fail_sync_under}, hb_util:list(Target) ++ "/"),
    R1 = try ship_now(Store)
         after persistent_term:erase({?MODULE, fail_sync_under})
         end,
    ?assertMatch({ship_poisoned, {sync_failed, _}}, R1),
    More = rows(<<"u/">>, 1, 10),
    write_rows(Store, More),
    ?assertEqual(ok, flush(Store, #{}, #{})),
    ?assertMatch({ship_poisoned, _}, ship_now(Store)),
    stop_shipper(Store),
    hb_name:unregister({?MODULE, shipper, maps:get(<<"name">>, Store)}),
    ?assertMatch({ship_poisoned, _}, ship_now(Store)),
    ?assertMatch(#{ started := true, poisoned := false,
                    shipper := #{ state := {ship_poisoned, _},
                                  backlog := #{ segments := Segs } } } when Segs >= 2,
                 status(Store)),
    ?assertNot(filelib:is_regular(filename:join(Target, "manifest.log"))),
    ok = clear_ship_poison(Store),
    ?assertEqual(idle, ship_now(Store)),
    Fresh = fresh_store(<<"oj-ship-eio-r">>),
    {ok, _} = restore(Target, Fresh),
    assert_rows(Fresh, maps:merge(Rows, More)).

%% @doc A recovery that fails in an unexpected way (here a journal file that
%% cannot be read) refuses, poisoned, and the writer stays up: it does not
%% crash, to be spawned again by the next call and recover, and sync, again.
unexpected_recovery_failure_refuses_test() ->
    {Store, _Inner, _Target} = test_setup(<<"oj-recover-exit">>, #{}),
    ok = write(Store, #{ <<"x/1">> => <<"1">> }, #{}),
    ok = flush(Store, #{}, #{}),
    kill_writer(Store),
    Dir = journal_dir(Store),
    ok = file:make_dir(open_name(Dir, 999999)),
    ?assertMatch({failure, {offsite_journal, {journal_poisoned, {recovery_failed, _}}}},
                 write(Store, #{ <<"x/2">> => <<"2">> }, #{})),
    Pid = writer_pid(Store),
    ?assert(is_pid(Pid) andalso is_process_alive(Pid)),
    ?assertMatch({poisoned, {recovery_failed, _}}, read_poison(Dir)),
    kill_writer(Store).

%% @doc A segment a failed start leaves behind is synced by name before it is
%% sealed. The writer has no segment open after it, and `seg' still names
%% it: a sync of the journal that took `seg' for the open segment skipped
%% it, and a seal renamed it `seg-' unsynced (and, its sync failing, the
%% journal went on).
not_started_segment_is_synced_test() ->
    Dir = test_dir(<<"oj-not-started">>),
    N = 5,
    {ok, Fd} = file:open(open_name(Dir, N), [read, write, raw, binary]),
    S0 = #{ store => #{ <<"name">> => <<"oj-not-started">> }, dir => Dir, seg => N,
            fd => Fd, off => 0, dirty => false, unsynced => [], unsealed => [],
            dir_dirty => false, poisoned => false, cut => undefined, pending => #{},
            errors => 0, sealed => -1 },
    {ok, S1} = append({start, true}, S0),
    S2 = not_started(S1, test),
    persistent_term:put({?MODULE, fail_sync}, true),
    S3 = try sync_journal(S2)
         after persistent_term:erase({?MODULE, fail_sync})
         end,
    ?assertMatch({sync_failed, eio}, maps:get(poisoned, S3)),
    _ = seal_closed(S3),
    ?assert(filelib:is_regular(open_name(Dir, N))),
    ?assertNot(filelib:is_regular(seg_name(Dir, N))).

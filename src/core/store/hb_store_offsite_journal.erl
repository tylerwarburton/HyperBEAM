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
%%% The one gap left is a host crash (power loss) after the inner store
%%% committed a write on its own (its flusher, a list, a full batch) but
%%% before the journal was synced: the inner store then has a row the journal
%%% lost. That row belongs to a slot that was never confirmed. An unclean
%%% start (no `clean' marker) closes such gaps: the writer keeps, per process,
%%% the highest assignment slot it has journaled, and journals again, with its
%%% whole closure, every assignment above it that the inner store holds, and
%%% the definition of every process it has never journaled.
%%%
%%% A torn journal tail (a frame cut short, or the zero-filled tail a host
%%% crash leaves on ext4) is cut back to its last whole frame on start, and
%%% the writer starts a new segment, so nothing is ever appended after damage.
%%% A failed append (a full local disk) is truncated away and refused: the
%%% write fails, the inner store is not written, and the slot is not
%%% confirmed.
%%%
%%% The writer never touches the target. A shipper process copies each sealed
%%% segment to it (`.tmp', sync, rename), then appends the file's name, size
%%% and CRC to `manifest.log' (synced), then deletes the local copy. A slow,
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
-export([wrap/2, status/1, restore/2, restore/3, ship_now/1]).
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
        <<"store-failure">> => throw
    }.

inner(#{ <<"inner">> := Inner }) -> Inner.
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
    ok = call(Store, seal, ?CALL_MS),
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

writer_init(Store) ->
    process_flag(trap_exit, true),
    Dir = journal_dir(Store),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Clean = filelib:is_regular(filename:join(Dir, "clean")),
    _ = file:delete(filename:join(Dir, "clean")),
    {Sealed, Marks0, Token} = read_marks(Dir),
    % Every local file newer than the last seal (or not sealed at all) may
    % hold assignments the marks do not: read them, cut torn tails, seal.
    {MaxN, Marks1} = recover_local(Dir, Sealed, Marks0),
    S0 = #{
        store => Store,
        dir => Dir,
        token => Token,
        seg => max(Sealed, MaxN) + 1,
        marks => Marks1,
        sealed => max(Sealed, MaxN),
        fd => undefined,
        off => 0,
        opened => erlang:monotonic_time(millisecond),
        dirty => false,
        broken => false,
        appended => 0,
        reconciled => 0,
        errors => 0,
        clean_start => Clean
    },
    S1 = write_marks(open_segment(S0)),
    S2 =
        case Clean of
            true -> S1;
            false -> reconcile(S1)
        end,
    _ = shipper(Store),
    erlang:send_after(cfg(<<"fsync-ms">>, ?FSYNC_MS, Store), self(), fsync_tick),
    erlang:send_after(1000, self(), roll_tick),
    writer_loop(S2).

writer_loop(S = #{ store := Store }) ->
    receive
        {call, From, Ref, {append, Pairs}} ->
            {Reply, S1} = append_and_write(Pairs, S),
            From ! {Ref, Reply},
            writer_loop(maybe_roll(S1));
        {call, From, Ref, flush} ->
            {Reply, S1} = do_flush(S),
            From ! {Ref, Reply},
            writer_loop(S1);
        {call, From, Ref, seal} ->
            S1 = roll(S),
            From ! {Ref, ok},
            writer_loop(S1);
        {call, From, Ref, status} ->
            Status = maps:with([dir, token, seg, sealed, off, appended,
                                reconciled, errors, clean_start, broken], S),
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

%% @doc Append the rows, then write them to the inner store. A refused append
%% writes nothing.
append_and_write(Pairs, S = #{ store := Store }) ->
    case append({w, Pairs}, S) of
        {ok, S1} ->
            Inner = inner(Store),
            Mod = maps:get(<<"store-module">>, Inner),
            Res =
                try Mod:write(Inner, maps:from_list(Pairs), #{})
                catch C:R -> {error, {C, R}}
                end,
            {Res, S1#{ marks => mark(Pairs, maps:get(marks, S1)),
                       appended => maps:get(appended, S1) + 1 }};
        {error, Reason, S1} ->
            {{error, Reason}, S1}
    end.

append(_Rec, S = #{ fd := undefined }) ->
    % The previous segment broke; start a new one.
    case catch open_segment(S#{ seg => maps:get(seg, S) + 1 }) of
        S1 = #{ fd := Fd } when Fd =/= undefined -> append(_Rec, S1);
        _ -> {error, journal_unavailable, S#{ errors => maps:get(errors, S) + 1 }}
    end;
append(Rec, S = #{ fd := Fd, off := Off }) ->
    Bin = term_to_binary(Rec),
    Frame = <<(byte_size(Bin)):32, (erlang:crc32(Bin)):32, Bin/binary>>,
    case file:write(Fd, Frame) of
        ok -> {ok, S#{ off => Off + byte_size(Frame), dirty => true }};
        {error, Reason} ->
            ?event(error, {offsite_journal_append_failed, Reason}),
            % Cut any partial frame away, then seal this segment: nothing is
            % ever appended after a failure in the same file.
            _ = file:position(Fd, Off),
            _ = file:truncate(Fd),
            S1 = seal_current(S#{ broken => true }),
            {error, Reason, S1#{ fd => undefined,
                                 errors => maps:get(errors, S) + 1 }}
    end.

do_flush(S = #{ store := Store }) ->
    case sync_journal(S) of
        S1 = #{ dirty := false } ->
            Inner = inner(Store),
            {catch hb_store:flush(Inner, #{}), S1};
        S1 -> {{error, journal_sync_failed}, S1}
    end.

sync_journal(S = #{ dirty := false }) -> S;
sync_journal(S = #{ fd := undefined }) -> S#{ dirty := false };
sync_journal(S = #{ fd := Fd }) ->
    case file:datasync(Fd) of
        ok -> S#{ dirty := false };
        {error, Reason} ->
            ?event(error, {offsite_journal_sync_failed, Reason}),
            S#{ errors := maps:get(errors, S) + 1 }
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
roll(S = #{ off := 0, fd := Fd }) when Fd =/= undefined -> S;
roll(S) ->
    S1 = seal_current(S),
    open_segment(S1#{ seg => maps:get(seg, S1) + 1 }).

%% Seal: sync, close, record the marks as of this segment, then rename it to
%% its sealed name. A crash between the two leaves an `open-' file at or below
%% the sealed number, which the next start reads and seals again.
seal_current(S = #{ fd := undefined }) -> S;
seal_current(S = #{ fd := Fd, dir := Dir, seg := N, store := Store }) ->
    _ = file:datasync(Fd),
    _ = file:close(Fd),
    Open = open_name(Dir, N),
    case maps:get(off, S) of
        0 ->
            _ = file:delete(Open),
            S#{ fd => undefined, dirty => false };
        _ ->
            S1 = write_marks(S#{ sealed => N }),
            ok = file:rename(Open, seg_name(Dir, N)),
            case hb_name:lookup({?MODULE, shipper, maps:get(<<"name">>, Store)}) of
                undefined -> ok;
                Pid -> Pid ! wake
            end,
            S1#{ fd => undefined, dirty => false }
    end.

open_segment(S = #{ dir := Dir, seg := N }) ->
    case file:open(open_name(Dir, N), [read, write, raw, binary]) of
        {ok, Fd} ->
            {ok, _} = file:position(Fd, eof),
            S#{ fd => Fd, off => 0, opened => erlang:monotonic_time(millisecond) };
        {error, Reason} ->
            ?event(error, {offsite_journal_open_failed, Reason}),
            S#{ fd => undefined }
    end.

stop_writer(S) ->
    S1 =
        case append({stop}, S) of
            {ok, SA} -> SA;
            {error, _, SA} -> SA
        end,
    S2 = seal_current(sync_journal(S1)),
    _ = write_marks(S2),
    Dir = maps:get(dir, S2),
    {ok, F} = file:open(filename:join(Dir, "clean"), [write, raw]),
    _ = file:sync(F),
    _ = file:close(F),
    S2.

%%% Marks: per process, the highest assignment slot journaled.

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
    ok = write_synced(Tmp, term_to_binary({marks, Sealed, Marks, Token})),
    ok = file:rename(Tmp, File),
    S.

new_token() -> hb_util:to_hex(crypto:strong_rand_bytes(4)).

token_bin(T) -> hb_util:bin(T).

%% @doc Recover local journal files: every `open-' file, and `seg-' files
%% above the last seal. Cut each to its whole frames, fold its assignments
%% into the marks, and seal it. Returns the highest segment number seen.
recover_local(Dir, Sealed, Marks) ->
    Files = local_files(Dir),
    lists:foldl(
        fun({N, Kind, File}, {Max, M}) ->
            case Kind =:= open orelse N > Sealed of
                false -> {max(Max, N), M};
                true ->
                    {ok, Bin} = file:read_file(File),
                    Valid = valid_prefix(Bin, 0),
                    case Valid < byte_size(Bin) of
                        true ->
                            ?event(warning, {offsite_journal_torn_tail,
                                {file, File}, {kept, Valid}, {size, byte_size(Bin)}}),
                            {ok, Fd} = file:open(File, [read, write, raw, binary]),
                            {ok, _} = file:position(Fd, Valid),
                            ok = file:truncate(Fd),
                            ok = file:sync(Fd),
                            ok = file:close(Fd);
                        false -> ok
                    end,
                    M1 = lists:foldl(
                        fun({w, Pairs}, Acc) -> mark(Pairs, Acc); (_, Acc) -> Acc end,
                        M,
                        records(binary:part(Bin, 0, Valid))
                    ),
                    case {Kind, Valid} of
                        {open, 0} -> _ = file:delete(File);
                        {open, _} -> ok = file:rename(File, seg_name(Dir, N));
                        _ -> ok
                    end,
                    {max(Max, N), M1}
            end
        end,
        {-1, Marks},
        Files
    ).

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

%%% Frames.

%% A zero-length frame is never written: it is the zero-filled tail a host
%% crash leaves, and its CRC (crc32(<<>>) = 0) would match.
valid_prefix(Bin, Off) ->
    case Bin of
        <<_:Off/binary, Len:32, Crc:32, Frame:Len/binary, _/binary>> when Len > 0 ->
            case erlang:crc32(Frame) of
                Crc -> valid_prefix(Bin, Off + 8 + Len);
                _ -> Off
            end;
        _ -> Off
    end.

records(<<Len:32, _Crc:32, Bin:Len/binary, Rest/binary>>) when Len > 0 ->
    [binary_to_term(Bin) | records(Rest)];
records(_) -> [].

%%% Reconcile: after an unclean start, journal every assignment the inner
%%% store holds above the marks, with its closure, and the definition of every
%%% process with no mark.

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
    sync_journal(S1#{ reconciled => Count }).

journal_rows([], S) -> S;
journal_rows(Rows, S) ->
    case append({w, Rows}, S) of
        {ok, S1} -> S1#{ marks => mark(Rows, maps:get(marks, S1)) };
        {error, Reason, _} -> erlang:error({offsite_journal_reconcile_failed, Reason})
    end.

%% @doc Every raw row reachable from `Path' in a store: the row itself or the
%% rows below it, and, through each `ao:<ID>' and `ni:' value, the rows they
%% name.
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

follow(Store, <<"ao:", ID/binary>>, Acc) when ?IS_ID(ID) -> closure(Store, ID, Acc);
follow(Store, Name = <<"ni:", _/binary>>, Acc) ->
    closure(Store, hb_escape:encode_path_component(Name), Acc);
follow(_Store, _Value, Acc) -> Acc.

%%% The shipper.

shipper_init(Store) ->
    erlang:send_after(0, self(), tick),
    shipper_loop(Store, #{ shipped => 0, backoff => 0, state => idle }).

shipper_loop(Store, St) ->
    receive
        stop -> ok;
        {status, From, Ref} ->
            From ! {Ref, #{ shipper => St#{ backlog => backlog(Store) } }},
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
        ship_segs(Segs, Store, St#{ state => idle })
    catch C:R ->
        ?event(error, {offsite_journal_ship_failed, C, R}),
        failed(St, {C, R})
    end.

ship_segs([], _Store, St) -> St#{ backoff => 0 };
ship_segs([{N, File} | Rest], Store, St) ->
    case ship_one(N, File, Store) of
        ok -> ship_segs(Rest, Store, St#{ shipped => maps:get(shipped, St) + 1 });
        {ceiling, Info} ->
            ?event(warning, {offsite_journal_ceiling, Info}),
            St#{ state => {ceiling, Info}, backoff => ?MAX_BACKOFF_MS div 10 };
        {error, Reason} ->
            ?event(error, {offsite_journal_ship_failed, Reason}),
            failed(St, Reason)
    end.

failed(St, Reason) ->
    B = case maps:get(backoff, St) of 0 -> 1000; B0 -> min(B0 * 2, ?MAX_BACKOFF_MS) end,
    St#{ state => {error, Reason}, backoff => B }.

ship_one(N, File, Store) ->
    Target = target_dir(Store),
    {ok, Raw} = file:read_file(File),
    Body = case compress(Store) of true -> zlib:gzip(Raw); false -> Raw end,
    {_, _, Token} = read_marks(journal_dir(Store)),
    Name = "seg-" ++ hb_util:list(Token) ++ "-" ++ pad(N) ++ ".log" ++
        case compress(Store) of true -> ".gz"; false -> "" end,
    maybe
        ok ?= filelib:ensure_dir(filename:join(Target, "x")),
        ok ?= space_ok(Target, byte_size(Body),
                       cfg(<<"max-fill-pct">>, ?MAX_FILL_PCT, Store)),
        Final = filename:join(Target, Name),
        Tmp = Final ++ ".tmp",
        ok ?= write_synced(Tmp, Body),
        ok ?= file:rename(Tmp, Final),
        Line = io_lib:format("~s ~B ~B~n", [Name, byte_size(Body), erlang:crc32(Body)]),
        ok ?= append_synced(filename:join(Target, "manifest.log"), Line),
        ok ?= file:delete(File)
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
%% lists, in order, each checked against its size and CRC, then any local
%% journal segments not yet shipped. Returns counts, or the first error.
restore(Sources, TargetStore) -> restore(Sources, TargetStore, #{}).
restore(Source, TargetStore, Opts)
        when is_binary(Source); is_list(Source), is_integer(hd(Source)) ->
    restore([Source], TargetStore, Opts);
restore(Sources, TargetStore, Opts) when is_list(Sources) ->
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
    Result =
        lists:foldl(
            fun(_, {error, _} = E) -> E;
               ({_, File, Check}, Acc) -> restore_file(File, Check, TargetStore, Opts, Acc)
            end,
            #{ files => 0, records => 0, rows => 0, torn => 0 },
            lists:reverse(Ordered)
        ),
    case Result of
        {error, _} -> Result;
        _ ->
            _ = hb_store:flush(TargetStore, Opts),
            {ok, Result}
    end.

%% A source is an export directory (it has a manifest) or a local journal.
source_files(Dir) ->
    case file:read_file(filename:join(Dir, "manifest.log")) of
        {ok, Manifest} ->
            [ begin
                  [Name, Size, Crc] = string:lexemes(hb_util:list(Line), " "),
                  {shipped_key(Name), filename:join(Dir, Name),
                   {list_to_integer(Size), list_to_integer(Crc)}}
              end
            || Line <- binary:split(Manifest, <<"\n">>, [global, trim_all]) ];
        _ ->
            {_, _, Token} = read_marks(Dir),
            [ {{token_bin(Token), N}, F, local} || {N, _, F} <- local_files(Dir) ]
    end.

shipped_key(Name) ->
    [<<"seg">>, Token, Num | _] =
        binary:split(hb_util:bin(Name), [<<"-">>, <<".">>], [global]),
    {Token, binary_to_integer(Num)}.

restore_file(File, Check, Target, Opts, Acc) ->
    case file:read_file(File) of
        {ok, Body} ->
            case Check of
                local -> apply_body(Body, Target, Opts, Acc);
                {Size, _} when byte_size(Body) =/= Size -> {error, {bad_size, File}};
                {_, Crc} ->
                    case erlang:crc32(Body) of
                        Crc -> apply_body(Body, Target, Opts, Acc);
                        _ -> {error, {bad_crc, File}}
                    end
            end;
        Error -> {error, {File, Error}}
    end.

apply_body(Body0, Target, Opts, Acc) ->
    Body = case Body0 of <<31, 139, _/binary>> -> zlib:gunzip(Body0); _ -> Body0 end,
    Valid = valid_prefix(Body, 0),
    Recs = records(binary:part(Body, 0, Valid)),
    Rows =
        lists:foldl(
            fun({w, Pairs}, N) ->
                    ok = hb_store:write(Target, maps:from_list(Pairs), Opts),
                    N + length(Pairs);
               (_, N) -> N
            end,
            0,
            Recs
        ),
    Acc#{
        files => maps:get(files, Acc) + 1,
        records => maps:get(records, Acc) + length(Recs),
        rows => maps:get(rows, Acc) + Rows,
        torn => maps:get(torn, Acc) + case Valid < byte_size(Body) of true -> 1; false -> 0 end
    }.

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
    ok = file:write(Fd, <<0:32, 0:32, 0:4096>>),
    ok = file:write(Fd, <<200:32, 12345:32, "partial">>),
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

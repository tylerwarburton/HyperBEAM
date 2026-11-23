%%% @doc File operations made in the calling process, never through the
%%% node's one file server.
%%%
%%% Every `file:' call without `raw' (`read_file/1', `list_dir/1',
%%% `rename/2', `delete/1', `make_dir/1', every `filelib:' call) is a
%%% `gen_server:call' to `file_server_2', one process for the whole node, which
%%% makes the calls one at a time. One slow call holds every other caller. On
%%% the NFS share (the cold tier, the offsite journal's target) one call costs
%%% a round trip at least and can hang for a minute; every compute that read
%%% the local `hb_store_fs' store then waited behind it (2026-11-23: 56-76%
%%% of worker time, 23 of 23 stack samples in `file:call').
%%%
%%% These do the same system calls through `prim_file', in the caller: a hung
%%% call holds only its caller (and one dirty IO scheduler thread). The
%%% results are those of the `file' and `filelib' functions of the same name,
%%% which make these same `prim_file' calls inside the file server.
%%%
%%% `bounded/2' runs a call that may hang (one on the share) in a process of
%%% its own, abandoned after a timeout, and holds at most `?MAX_SLOW' of them
%%% in flight at once across the node. An abandoned call cannot be cancelled:
%%% it holds a dirty IO thread until the kernel gives up, and those threads
%%% also serve every local file and LMDB call. The bound keeps a hung share
%%% from taking them all.
-module(hb_file).
-export([read_file/1, write_file/2, write_file/3, list_dir/1, list_dir_all/1]).
-export([rename/2, delete/1, make_dir/1, ensure_dir/1]).
-export([read_file_info/1, read_link_info/1, is_file/1, is_regular/1, file_size/1]).
-export([bounded/2, in_flight/0]).
-ifdef(TEST).
-export([without_file_server/2]).
-endif.
-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").

-on_load(init/0).

-define(SLOW, {?MODULE, slow_calls}).
%% Bounded calls in flight, at most. The VM has 10 dirty IO threads unless
%% `+SDio' says otherwise; a hung share may hold this many of them.
-define(MAX_SLOW, 4).
-define(WAIT_STEP_MS, 5).

init() ->
    persistent_term:put(?SLOW, atomics:new(1, [{signed, true}])),
    ok.

%%% Plain calls, in the caller.

read_file(Name) -> prim_file:read_file(name(Name)).

write_file(Name, Data) -> write_file(Name, Data, []).
write_file(Name, Data, Modes) -> file:write_file(name(Name), Data, [raw | Modes -- [raw]]).

list_dir(Name) -> prim_file:list_dir(name(Name)).

list_dir_all(Name) -> prim_file:list_dir_all(name(Name)).

rename(From, To) -> prim_file:rename(name(From), name(To)).

delete(Name) -> prim_file:delete(name(Name)).

make_dir(Name) -> prim_file:make_dir(name(Name)).

read_file_info(Name) -> prim_file:read_file_info(name(Name)).

read_link_info(Name) -> prim_file:read_link_info(name(Name)).

%% @doc As `filelib:ensure_dir/1': every parent directory of `Name' exists,
%% or `{error, Reason}'.
ensure_dir(Name) ->
    N = name(Name),
    case N of
        "/" -> ok;
        _ -> ensure_path(filename:dirname(N))
    end.

ensure_path(Path) ->
    case is_dir(Path) of
        true -> ok;
        false ->
            case filename:dirname(Path) of
                Path -> {error, einval};
                Parent ->
                    _ = ensure_path(Parent),
                    case prim_file:make_dir(Path) of
                        {error, eexist} = Exists ->
                            case is_dir(Path) of
                                true -> ok;
                                false -> Exists
                            end;
                        Other -> Other
                    end
            end
    end.

is_dir(Name) ->
    case prim_file:read_file_info(Name) of
        {ok, #file_info{ type = directory }} -> true;
        _ -> false
    end.

%% @doc As `filelib:is_file/1': a regular file or a directory.
is_file(Name) ->
    case prim_file:read_file_info(name(Name)) of
        {ok, #file_info{ type = T }} -> T =:= regular orelse T =:= directory;
        _ -> false
    end.

%% @doc As `filelib:is_regular/1'.
is_regular(Name) ->
    case prim_file:read_file_info(name(Name)) of
        {ok, #file_info{ type = regular }} -> true;
        _ -> false
    end.

%% @doc As `filelib:file_size/1': 0 when it cannot be read.
file_size(Name) ->
    case prim_file:read_file_info(name(Name)) of
        {ok, #file_info{ size = Size }} -> Size;
        _ -> 0
    end.

%% A name as `file' takes it: a binary, or a possibly deep list.
name(N) when is_binary(N) -> N;
name(N) when is_atom(N) -> atom_to_list(N);
name(N) when is_list(N) -> filename:flatten(N).

%%% Calls that may hang.

%% @doc Run `Fun' in a process of its own: `{ok, catch Fun()}', `{error,
%% {'EXIT', Why}}' if it raised or died, or `{error, timeout}' after `TimeoutMs'
%% (waiting for a free place counts). A call still running at the timeout is
%% killed; it ends when its system call returns, and only then frees its
%% place.
bounded(Fun, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    case acquire(Deadline) of
        false -> {error, timeout};
        true -> run(Fun, Deadline)
    end.

%% @doc Bounded calls in flight now, abandoned ones included.
in_flight() -> atomics:get(persistent_term:get(?SLOW), 1).

acquire(Deadline) ->
    Ref = persistent_term:get(?SLOW),
    case atomics:add_get(Ref, 1, 1) =< ?MAX_SLOW of
        true -> true;
        false ->
            atomics:sub(Ref, 1, 1),
            case Deadline - erlang:monotonic_time(millisecond) of
                Left when Left > 0 ->
                    timer:sleep(min(?WAIT_STEP_MS, Left)),
                    acquire(Deadline);
                _ -> false
            end
    end.

%% The place is freed by a watcher when the worker is gone, however it ends.
run(Fun, Deadline) ->
    Slow = persistent_term:get(?SLOW),
    Parent = self(),
    Ref = make_ref(),
    Worker =
        spawn(fun() ->
            receive {Ref, go} -> Parent ! {Ref, catch Fun()} end
        end),
    Mon = erlang:monitor(process, Worker),
    spawn(fun() ->
        WMon = erlang:monitor(process, Worker),
        Worker ! {Ref, go},
        receive {'DOWN', WMon, process, _, _} -> atomics:sub(Slow, 1, 1) end
    end),
    Left = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {Ref, {'EXIT', Why}} -> erlang:demonitor(Mon, [flush]), {error, {'EXIT', Why}};
        {Ref, Res} -> erlang:demonitor(Mon, [flush]), {ok, Res};
        {'DOWN', Mon, process, Worker, Why} -> {error, {'EXIT', Why}}
    after Left ->
        erlang:demonitor(Mon, [flush]),
        exit(Worker, kill),
        % A reply sent just before the kill is dropped.
        receive {Ref, _} -> ok after 0 -> ok end,
        {error, timeout}
    end.

%%% Tests

%% @doc The calls answer as the `file' and `filelib' ones do.
same_results_as_file_test() ->
    Dir = "cache-TEST/hb_file-" ++ integer_to_list(erlang:unique_integer([positive])),
    File = Dir ++ "/a/b/c",
    ?assertEqual(filelib:ensure_dir(File), ensure_dir(File)),
    ?assert(is_file(Dir ++ "/a/b")),
    ?assertNot(is_regular(Dir ++ "/a/b")),
    ?assertEqual(ok, write_file(list_to_binary(File), <<"v">>)),
    ?assertEqual(file:read_file(File), read_file(File)),
    ?assertEqual(1, file_size(File)),
    ?assertEqual(0, file_size(File ++ "x")),
    ?assert(is_regular([Dir, "/a/b", "/c"])),
    ?assertEqual(file:list_dir(Dir ++ "/a/b"), list_dir(Dir ++ "/a/b")),
    ?assertEqual(file:list_dir_all(Dir ++ "/a/b"), list_dir_all(list_to_binary(Dir ++ "/a/b"))),
    ?assertEqual(file:list_dir_all(Dir ++ "/none"), list_dir_all(Dir ++ "/none")),
    ?assertEqual(file:read_file(Dir ++ "/none"), read_file(Dir ++ "/none")),
    ?assertEqual(ok, rename(File, File ++ "2")),
    ?assertEqual({error, enoent}, rename(File, File ++ "3")),
    ?assertEqual({ok, <<"v">>}, read_file(File ++ "2")),
    ?assertEqual(ok, delete(File ++ "2")),
    ?assertEqual(file:delete(File ++ "2"), delete(File ++ "2")),
    ?assertEqual(file:make_dir(Dir), make_dir(Dir)),
    ?assertEqual(ok, ensure_dir(Dir ++ "/a/b/c")),
    os:cmd("rm -rf " ++ Dir).

-ifdef(TEST).
%% @doc Run `Fun' in a process of its own with `file_server_2' suspended, as a
%% hung NFS call holds it: `{ok, catch Fun()}', or `blocked' if it has not
%% answered in `TimeoutMs'. The server is resumed either way.
without_file_server(Fun, TimeoutMs) ->
    FS = whereis(file_server_2),
    Self = self(),
    Ref = make_ref(),
    true = erlang:suspend_process(FS),
    try
        spawn(fun() -> Self ! {Ref, catch Fun()} end),
        receive {Ref, R} -> {ok, R} after TimeoutMs -> blocked end
    after
        true = erlang:resume_process(FS)
    end.
-endif.

%% @doc None of them waits for the file server: with `file_server_2'
%% suspended, each still answers; the `file' calls do not.
file_server_not_used_test() ->
    Dir = "cache-TEST/hb_file-fs-" ++ integer_to_list(erlang:unique_integer([positive])),
    File = Dir ++ "/x/y",
    ok = filelib:ensure_dir(File),
    ?assertEqual(blocked, without_file_server(fun() -> file:list_dir_all(Dir) end, 200)),
    ?assertEqual({ok, ok}, without_file_server(fun() ->
        ok = ensure_dir(Dir ++ "/x/z/w"),
        ok = write_file(File, <<"v">>),
        {ok, <<"v">>} = read_file(File),
        {ok, [_ | _]} = list_dir_all(Dir ++ "/x"),
        {ok, _} = list_dir(Dir ++ "/x"),
        true = is_regular(File),
        ok = rename(File, File ++ ".2"),
        ok = delete(File ++ ".2"),
        ok
    end, 5000)),
    os:cmd("rm -rf " ++ Dir).

%% @doc A bounded call answers, or times out; a hung call keeps its place
%% until it ends, and no more than `?MAX_SLOW' are in flight at once.
bounded_test() ->
    ?assertEqual({ok, 3}, bounded(fun() -> 1 + 2 end, 1000)),
    ?assertMatch({error, {'EXIT', _}}, bounded(fun() -> error(boom) end, 1000)),
    % A place is freed once its worker is gone, just after the answer.
    ?assertEqual(ok, wait_in_flight(0, 100)),
    Self = self(),
    Hang = fun() -> Self ! {hung, self()}, receive release -> ok end end,
    [ spawn(fun() -> Self ! {res, bounded(Hang, 60000)} end) || _ <- lists:seq(1, ?MAX_SLOW) ],
    Hung = [ receive {hung, P} -> P after 5000 -> error(not_hung) end
           || _ <- lists:seq(1, ?MAX_SLOW) ],
    try
        ?assertEqual(?MAX_SLOW, in_flight()),
        % Every place is taken: the next waits for one, and gives up.
        ?assertEqual({error, timeout}, bounded(fun() -> ok end, 100))
    after
        [ P ! release || P <- Hung ]
    end,
    [ receive {res, R} -> ?assertEqual({ok, ok}, R) after 5000 -> error(no_result) end
    || _ <- Hung ],
    ?assertEqual(ok, wait_in_flight(0, 100)),
    % A timed-out call is killed, and its place freed.
    ?assertEqual({error, timeout}, bounded(fun() -> timer:sleep(10000) end, 50)),
    ?assertEqual(ok, wait_in_flight(0, 100)).

wait_in_flight(N, 0) -> {in_flight, in_flight(), N};
wait_in_flight(N, Tries) ->
    case in_flight() of
        N -> ok;
        _ -> timer:sleep(10), wait_in_flight(N, Tries - 1)
    end.

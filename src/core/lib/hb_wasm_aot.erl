%%% @doc Ahead-of-time compilation for `wasm-patch@1.0', by the node itself.
%%%
%%% A node never loads AOT bytes it did not compile: `get/2' takes a `.wasm',
%%% runs the pinned `wamrc' on it and keeps the result in a node-local cache
%%% that nothing replicates:
%%%
%%%   `<wasm-patch-aot-dir>/<sha256(wasm)>/<wamr-tag>/<sha256(flags)>.aot'
%%%
%%% The module is the instrumented one, which embeds the pass version, so a
%%% new pass never reuses old code.
%%% One compile runs at a time on a node. When there is no usable `wamrc'
%%% (missing, or not the runtime's WAMR version) or a compile fails, the
%%% module runs under the interpreter: bit-identical results, only slower.
-module(hb_wasm_aot).
-export([get/2, key/2, flags/1, wamrc/1, wamr_tag/0]).
-export([instrument/3, instrument_version/1, prewarm/2, prewarm_cli/1, approved/2]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(WAMR_TAG, <<"WAMR-2.2.0">>).
-define(WAMRC_VERSION, "2.2.0").
-define(FLAGS, [
    "--bounds-checks=1",
    "--stack-bounds-checks=1",
    "--disable-simd",
    "--opt-level=3",
    "--target=x86_64",
    "--cpu=x86-64-v2"
]).
-define(COMPILE_TIMEOUT_MS, 300000).

%% @doc The WAMR release the runtime and `wamrc' are built from.
wamr_tag() -> ?WAMR_TAG.

%% @doc The `wamrc' flags.
flags(_Opts) -> ?FLAGS.

%% @doc The cache key of a module under the node's flags.
key(Wasm, Opts) ->
    Flags = iolist_to_binary(lists:join(" ", flags(Opts))),
    <<(hex(crypto:hash(sha256, Wasm)))/binary, "/", ?WAMR_TAG/binary, "/",
        (hex(crypto:hash(sha256, Flags)))/binary>>.

%% @doc `{aot, Bytes, Key}' or `{interp, Reason}'. `wasm-patch-aot' set to
%% `false' always gives the interpreter.
get(Wasm, Opts) ->
    case hb_opts:get(<<"wasm-patch-aot">>, true, Opts) of
        false -> {interp, disabled};
        <<"false">> -> {interp, disabled};
        _ ->
            Key = key(Wasm, Opts),
            Path = filename:join(dir(Opts), hb_util:list(<<Key/binary, ".aot">>)),
            case file:read_file(Path) of
                {ok, Aot} -> {aot, Aot, Key};
                {error, _} -> compile(Wasm, Key, Path, Opts)
            end
    end.

%% The cache directory: `wasm-patch-aot-dir', else `priv/wasm-aot' of the
%% build, which an image pre-warms (`prewarm/2') and ships with.
dir(Opts) ->
    case hb_opts:get(<<"wasm-patch-aot-dir">>, not_found, Opts) of
        not_found -> filename:join(code:priv_dir(hb), "wasm-aot");
        Dir -> hb_util:list(Dir)
    end.

%% @doc Compile the approved modules ahead of time, as the node would at a
%% process's first init, so a node starts with them cached. `File' is JSON:
%% `{"modules": [{"sha256": <hex of the .wasm>, "source": <path or URL>}]}';
%% a path is relative to the working directory. Each module is fetched,
%% checked against its hash, instrumented under the node's memory cap and
%% compiled. Returns `[{Sha256, Result}]'; nothing here is ever loaded from
%% outside the node's own compiler. `make wasm-patch-prewarm' runs it.
prewarm(File, Opts) ->
    {ok, Json} = file:read_file(File),
    #{ <<"modules">> := Modules } = json:decode(Json),
    Cap = hb_util:int(hb_opts:get(<<"wasm-patch-max-pages">>, 65536, Opts)),
    [ {Sha, prewarm_one(Sha, Source, Cap, Opts)}
    || #{ <<"sha256">> := Sha, <<"source">> := Source } <- Modules ].

prewarm_one(Sha, Source, Cap, Opts) ->
    case fetch(Source) of
        {ok, Wasm} ->
            case hex(crypto:hash(sha256, Wasm)) of
                Sha ->
                    ok = keep_approved(Sha, Wasm, Opts),
                    case instrument(Wasm, Cap, Opts) of
                        {ok, Instrumented, _Version} ->
                            case get(Instrumented, Opts#{ <<"wasm-patch-aot">> => true }) of
                                {aot, _, Key} -> {ok, Key};
                                {interp, Reason} -> {error, Reason}
                            end;
                        Other -> Other
                    end;
                Got -> {error, {sha256_mismatch, Got}}
            end;
        Error -> Error
    end.

%% @doc `make wasm-patch-prewarm': print the results, halt 0 only when every
%% module compiled.
prewarm_cli(File) ->
    Results = prewarm(hb_util:bin(File), #{}),
    io:format("~p~n", [Results]),
    halt(case lists:all(fun({_, {ok, _}}) -> true; (_) -> false end, Results) of
        true -> 0;
        false -> 1
    end).

%% @doc An approved image this node holds (`prewarm/2' keeps one copy of each
%% module it compiles, under `approved/<sha256>.wasm' in the AOT directory):
%% `{ok, Bytes}' when present and still of that hash, else `not_found'. A
%% process pinning its image by `image-sha256' boots from this copy without
%% fetching the image.
approved(Sha, Opts) when is_binary(Sha), byte_size(Sha) =:= 64 ->
    case lists:all(fun(C) -> (C >= $0 andalso C =< $9) orelse (C >= $a andalso C =< $f) end,
            binary_to_list(Sha)) of
        false -> not_found;
        true ->
            case file:read_file(approved_path(Sha, Opts)) of
                {ok, Bytes} ->
                    case hex(crypto:hash(sha256, Bytes)) of
                        Sha -> {ok, Bytes};
                        _ -> not_found
                    end;
                _ -> not_found
            end
    end;
approved(_Sha, _Opts) -> not_found.

approved_path(Sha, Opts) ->
    filename:join([dir(Opts), "approved", hb_util:list(Sha) ++ ".wasm"]).

keep_approved(Sha, Wasm, Opts) ->
    Path = approved_path(Sha, Opts),
    ok = filelib:ensure_dir(Path),
    file:write_file(Path, Wasm).

fetch(<<"http://", _/binary>> = URL) -> http_get(URL);
fetch(<<"https://", _/binary>> = URL) -> http_get(URL);
fetch(Path) -> file:read_file(Path).

http_get(URL) ->
    {ok, _} = application:ensure_all_started([inets, ssl]),
    case httpc:request(get, {hb_util:list(URL), []}, [{timeout, 60000}],
            [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} -> {ok, Body};
        Other -> {error, {fetch_failed, URL, Other}}
    end.

%% One compile at a time on this node; a second caller for the same module
%% finds the first one's output when it gets the lock.
compile(Wasm, Key, Path, Opts) ->
    case wamrc(Opts) of
        {error, Reason} ->
            ?event(warning, {wasm_patch_aot_unavailable, Reason}),
            {interp, Reason};
        {ok, Wamrc} ->
            global:trans(
                {?MODULE, compile},
                fun() ->
                    case file:read_file(Path) of
                        {ok, Aot} -> {aot, Aot, Key};
                        {error, _} -> run_wamrc(Wamrc, Wasm, Key, Path, Opts)
                    end
                end,
                [node()]
            )
    end.

run_wamrc(Wamrc, Wasm, Key, Path, Opts) ->
    ok = filelib:ensure_dir(Path),
    Tmp = Path ++ ".tmp-" ++ integer_to_list(erlang:unique_integer([positive])),
    In = Tmp ++ ".wasm",
    ok = file:write_file(In, Wasm),
    Args = ?FLAGS ++ ["-o", Tmp, In],
    T0 = erlang:monotonic_time(millisecond),
    Res = exec(Wamrc, Args),
    file:delete(In),
    case {Res, file:read_file(Tmp)} of
        {{0, _}, {ok, Aot}} ->
            ok = file:rename(Tmp, Path),
            ?event(wasm_patch,
                {wasm_patch_aot_compiled, {key, Key},
                    {ms, erlang:monotonic_time(millisecond) - T0},
                    {bytes, byte_size(Aot)}},
                Opts),
            {aot, Aot, Key};
        {{Status, Output}, _} ->
            file:delete(Tmp),
            ?event(warning, {wasm_patch_aot_failed, {key, Key}, {status, Status}, {output, Output}}),
            {interp, {wamrc_failed, Status}}
    end.

%% @doc Validate and instrument a module with the pinned pass
%% (`native/wasm_patch_instrument', `make wasm-patch-instrument'):
%% `{ok, Instrumented, PassVersion}', `{refused, Reason}' (the module may not
%% run) or `{error, Reason}' (no usable pass on this node). The output runs
%% under both the interpreter and AOT, so fuel and depth count the same.
instrument(Wasm, CapPages, Opts) ->
    case instrument_version(Opts) of
        {error, _} = Error -> Error;
        {ok, Pass, Version} ->
            Dir = dir(Opts),
            Base = filename:join(Dir, "instrument-" ++
                integer_to_list(erlang:unique_integer([positive]))),
            In = Base ++ ".in.wasm",
            Out = Base ++ ".out.wasm",
            ok = filelib:ensure_dir(In),
            ok = file:write_file(In, Wasm),
            Res = exec(Pass, ["--memory-cap-pages", integer_to_list(CapPages), In, Out]),
            file:delete(In),
            Read = file:read_file(Out),
            file:delete(Out),
            case {Res, Read} of
                {{0, _}, {ok, Instrumented}} -> {ok, Instrumented, Version};
                {{1, Reason}, _} -> {refused, string:trim(Reason)};
                {{Status, Output}, _} -> {error, {instrument_failed, Status, Output}}
            end
    end.

%% @doc The pass binary and its version: `wasm-patch-instrument' option, else
%% `HB_WASM_PATCH_INSTRUMENT', else the build's `priv/wasm-patch-instrument'.
instrument_version(Opts) ->
    Pass =
        case hb_opts:get(<<"wasm-patch-instrument">>, not_found, Opts) of
            not_found ->
                case os:getenv("HB_WASM_PATCH_INSTRUMENT") of
                    false -> filename:join(code:priv_dir(hb), "wasm-patch-instrument");
                    Env -> Env
                end;
            Configured -> hb_util:list(Configured)
        end,
    case filelib:is_regular(Pass) of
        false -> {error, {no_instrument_pass, Pass}};
        true ->
            Cached = {?MODULE, pass, Pass},
            case persistent_term:get(Cached, undefined) of
                undefined ->
                    Res =
                        case exec(Pass, ["--version"]) of
                            {0, Out} -> {ok, Pass, string:trim(Out)};
                            Other -> {error, {instrument_version, Other}}
                        end,
                    persistent_term:put(Cached, Res),
                    Res;
                Res -> Res
            end
    end.

%% @doc The `wamrc' to use: `wasm-patch-wamrc', else `HB_WAMRC', else the
%% build's `priv/wamrc' (`make wamrc'), and only if it reports the runtime's
%% WAMR version.
wamrc(Opts) ->
    Candidate =
        case hb_opts:get(<<"wasm-patch-wamrc">>, not_found, Opts) of
            not_found ->
                case os:getenv("HB_WAMRC") of
                    false -> filename:join(code:priv_dir(hb), "wamrc");
                    Env -> Env
                end;
            Configured -> hb_util:list(Configured)
        end,
    case Candidate of
        false -> {error, no_wamrc};
        _ ->
            case filelib:is_regular(Candidate) of
                false -> {error, {no_wamrc, Candidate}};
                true -> check_version(Candidate)
            end
    end.

check_version(Wamrc) ->
    Cached = {?MODULE, version, Wamrc},
    case persistent_term:get(Cached, undefined) of
        undefined ->
            Res =
                case exec(Wamrc, ["--version"]) of
                    {_, Out} ->
                        case string:find(Out, ?WAMRC_VERSION) of
                            nomatch -> {error, {wamrc_version, Out}};
                            _ -> {ok, Wamrc}
                        end
                end,
            persistent_term:put(Cached, Res),
            Res;
        Res -> Res
    end.

exec(Exe, Args) ->
    Port = open_port({spawn_executable, Exe},
        [{args, Args}, exit_status, stderr_to_stdout, binary, hide]),
    collect(Port, []).

collect(Port, Acc) ->
    receive
        {Port, {data, D}} -> collect(Port, [D | Acc]);
        {Port, {exit_status, S}} -> {S, iolist_to_binary(lists:reverse(Acc))}
    after ?COMPILE_TIMEOUT_MS ->
        catch port_close(Port),
        {timeout, iolist_to_binary(lists:reverse(Acc))}
    end.

hex(Bin) -> binary:encode_hex(Bin, lowercase).

%%% Tests

key_test() ->
    K1 = key(<<"a">>, #{}),
    ?assertEqual(K1, key(<<"a">>, #{})),
    ?assertNotEqual(K1, key(<<"b">>, #{})),
    [Hash, Tag, _] = binary:split(K1, <<"/">>, [global]),
    ?assertEqual(64, byte_size(Hash)),
    ?assertEqual(?WAMR_TAG, Tag).

prewarm_test_() ->
    {timeout, 120, fun() ->
        Dir = "_build/test-wasm-aot-prewarm",
        {ok, Wasm} = file:read_file("test/wasm-patch/wp-test.wasm"),
        Sha = hex(crypto:hash(sha256, Wasm)),
        File = Dir ++ "/prewarm.json",
        ok = filelib:ensure_dir(File),
        ok = file:write_file(File, json:encode(#{ <<"modules">> => [
            #{ <<"sha256">> => Sha, <<"source">> => <<"test/wasm-patch/wp-test.wasm">> },
            #{ <<"sha256">> => binary:copy(<<"0">>, 64), <<"source">> => <<"test/wasm-patch/wp-test.wasm">> }
        ]})),
        Opts = #{ <<"wasm-patch-aot-dir">> => Dir },
        [{Sha, First}, {_, Bad}] = prewarm(File, Opts),
        ?assertMatch({error, {sha256_mismatch, _}}, Bad),
        % The approved copy is kept, and only under its own hash.
        ?assertEqual({ok, Wasm}, approved(Sha, Opts)),
        ?assertEqual(not_found, approved(binary:copy(<<"0">>, 64), Opts)),
        ?assertEqual(not_found, approved(<<"../../etc/passwd">>, Opts)),
        case wamrc(Opts) of
            {ok, _} ->
                {ok, Key} = First,
                {ok, Instrumented, _} = instrument(Wasm, 65536, Opts),
                ?assertMatch({aot, _, Key}, get(Instrumented, Opts));
            {error, _} ->
                ?assertMatch({error, _}, First)
        end
    end}.

disabled_test() ->
    ?assertEqual({interp, disabled}, get(<<"x">>, #{ <<"wasm-patch-aot">> => false })),
    ?assertEqual({interp, no_wamrc_here},
        case wamrc(#{ <<"wasm-patch-wamrc">> => <<"/nonexistent/wamrc">> }) of
            {error, {no_wamrc, _}} -> {interp, no_wamrc_here};
            Other -> Other
        end).

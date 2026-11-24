%%% @doc Ahead-of-time compilation for `wasm-patch@1.0', by the node itself.
%%%
%%% A node never loads AOT bytes it did not compile: `get/2' takes a `.wasm',
%%% runs the pinned `wamrc' on it and keeps the result in a node-local cache
%%% that nothing replicates:
%%%
%%%   `<wasm-patch-aot-dir>/<sha256(wasm)>/<wamr-tag>/<sha256(flags)>.aot'
%%%
%%% The flags include the instrumentation pass version
%%% (`wasm-patch-instrument-version'), so a new pass never reuses old code.
%%% One compile runs at a time on a node. When there is no usable `wamrc'
%%% (missing, or not the runtime's WAMR version) or a compile fails, the
%%% module runs under the interpreter: bit-identical results, only slower.
-module(hb_wasm_aot).
-export([get/2, key/2, flags/1, wamrc/1, wamr_tag/0]).
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

%% @doc The `wamrc' flags, the instrumentation pass version last.
flags(Opts) ->
    ?FLAGS ++ ["instrument=" ++ hb_util:list(hb_util:bin(
        hb_opts:get(<<"wasm-patch-instrument-version">>, <<"none">>, Opts)))].

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
            Path = filename:join(dir(Opts), <<Key/binary, ".aot">>),
            case file:read_file(Path) of
                {ok, Aot} -> {aot, Aot, Key};
                {error, _} -> compile(Wasm, Key, Path, Opts)
            end
    end.

dir(Opts) ->
    hb_util:list(hb_opts:get(<<"wasm-patch-aot-dir">>, <<"cache-wasm-aot">>, Opts)).

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

%% @doc The `wamrc' to use: `wasm-patch-wamrc', else `HB_WAMRC', else `wamrc'
%% on the path, and only if it reports the runtime's WAMR version.
wamrc(Opts) ->
    Candidate =
        case hb_opts:get(<<"wasm-patch-wamrc">>, not_found, Opts) of
            not_found ->
                case os:getenv("HB_WAMRC") of
                    false -> os:find_executable("wamrc");
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
    ?assertNotEqual(K1, key(<<"a">>, #{ <<"wasm-patch-instrument-version">> => <<"1">> })),
    [Hash, Tag, _] = binary:split(K1, <<"/">>, [global]),
    ?assertEqual(64, byte_size(Hash)),
    ?assertEqual(?WAMR_TAG, Tag).

disabled_test() ->
    ?assertEqual({interp, disabled}, get(<<"x">>, #{ <<"wasm-patch-aot">> => false })),
    ?assertEqual({interp, no_wamrc_here},
        case wamrc(#{ <<"wasm-patch-wamrc">> => <<"/nonexistent/wamrc">> }) of
            {error, {no_wamrc, _}} -> {interp, no_wamrc_here};
            Other -> Other
        end).

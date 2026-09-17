%%% @doc Request-only Lua execution with explicit deterministic state patches.
%%%
%%% `lua@5.3a' passes the full public process message into Lua and expects the
%%% full message back. This version keeps application state in the resident
%%% Luerl VM, passes only the request by default, and accepts a small result:
%%%
%%% ```lua
%%% return {
%%%   patches = {
%%%     { path = "/counter", value = "1" },
%%%     { path = "/old-key", delete = true }
%%%   },
%%%   results = { output = { data = "1" } }
%%% }
%%% ```
%%%
%%% Patches are applied in order to the AO-Core message held by the process
%%% worker. The device deliberately does not change `lua@5.3a' semantics.
-module(dev_lua_53b).
-implements(<<"lua@5.3b">>).
-export([info/1, init/3, compute/4, snapshot/3, normalize/3, functions/3]).
-export([head_to_head_benchmark/2]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(LEGACY_LUA, <<"lua@5.3a">>).
-define(RESERVED_PATCH_KEYS, [
    <<"at-slot">>,
    <<"commitments">>,
    <<"device">>,
    <<"initialized">>,
    <<"priv">>,
    <<"process">>,
    <<"results">>,
    <<"snapshot">>
]).

%% @doc Expose Lua functions as device keys, using this module's compute path.
info(Base) ->
    #{
        default => fun compute/4,
        excludes =>
            [
                <<"id">>,
                <<"commitments">>,
                <<"committers">>,
                <<"keys">>,
                <<"path">>,
                <<"set">>,
                <<"remove">>,
                <<"verify">>,
                <<"encode">>,
                <<"decode">>
            ] ++ maps:keys(Base)
    }.

%% @doc Reuse the stable `lua@5.3a' VM initialization and sandbox.
init(Base, Req, Opts) ->
    hb_ao:raw(?LEGACY_LUA, <<"init">>, Base, Req, Opts).

%% @doc Reuse the stable `lua@5.3a' snapshot representation.
snapshot(Base, Req, Opts) ->
    hb_ao:raw(?LEGACY_LUA, <<"snapshot">>, Base, Req, Opts).

%% @doc Reuse the stable `lua@5.3a' snapshot restore path.
normalize(Base, Req, Opts) ->
    hb_ao:raw(?LEGACY_LUA, <<"normalize">>, Base, Req, Opts).

%% @doc Reuse the stable `lua@5.3a' function inventory.
functions(Base, Req, Opts) ->
    hb_ao:raw(?LEGACY_LUA, <<"functions">>, Base, Req, Opts).

%% @doc Invoke Lua with request-only parameters and apply its explicit patches.
compute(Key, RawBase, RawReq, Opts) ->
    Req = hb_cache:read_all_commitments(RawReq, Opts),
    {ok, Base} = init(RawBase, Req, Opts),
    OldPriv = #{ <<"state">> := State } = hb_private:from_message(Base),
    Function =
        hb_ao:get_first(
            [
                {Req, <<"body/function">>},
                {Req, <<"function">>},
                {{as, <<"message@1.0">>, Base}, <<"function">>}
            ],
            Key,
            Opts#{ <<"hashpath">> => ignore }
        ),
    Params =
        hb_ao:get_first(
            [
                {Req, <<"body/parameters">>},
                {Req, <<"parameters">>}
            ],
            [Req],
            Opts#{ <<"hashpath">> => ignore }
        ),
    Response =
        try luerl:call_function_dec(
            [Function],
            encode(Params, Opts),
            State
        )
        catch
            _:Reason:Stacktrace -> {error, Reason, Stacktrace}
        end,
    process_response(Response, Base, OldPriv, Opts).

%% @doc Normalize the two supported Lua return shapes.
process_response({ok, [Result], NewState}, Base, Priv, Opts) ->
    process_response({ok, [<<"ok">>, Result], NewState}, Base, Priv, Opts);
process_response({ok, [Status, Encoded], NewState}, Base, Priv, Opts) ->
    case decode(Encoded, Opts) of
        Result when is_map(Result) ->
            case apply_result(Base, Result, Opts) of
                {ok, Patched} ->
                    {
                        hb_util:atom(Status),
                        Patched#{
                            <<"priv">> => Priv#{ <<"state">> => NewState }
                        }
                    };
                Error -> Error
            end;
        Other ->
            {error, #{
                <<"status">> => 422,
                <<"body">> => <<"lua@5.3b result must be a message.">>,
                <<"result">> => hb_util:bin(Other)
            }}
    end;
process_response({lua_error, RawError, State}, _Base, _Priv, Opts) ->
    Error =
        try decode(luerl:decode(RawError, State), Opts)
        catch _:_ -> RawError
        end,
    {error, #{
        <<"status">> => 500,
        <<"body">> => Error
    }};
process_response({error, Reason, Trace}, _Base, _Priv, _Opts) ->
    {error, #{
        <<"status">> => 500,
        <<"body">> =>
            iolist_to_binary(io_lib:format("Erlang error while running Lua: ~p", [Reason])),
        <<"trace">> => iolist_to_binary(hb_format:trace(Trace))
    }}.

%% @doc Apply ordered patches, then replace the per-slot result message.
apply_result(Base, Result, Opts) ->
    Patches = hb_ao:get(<<"patches">>, Result, [], Opts),
    Results = hb_ao:get(<<"results">>, Result, #{}, Opts),
    case {is_list(Patches), is_map(Results)} of
        {true, true} ->
            case apply_patches(Base, Patches, Opts) of
                {ok, Patched} ->
                    {ok, hb_ao:set(Patched, <<"results">>, Results, Opts)};
                Error -> Error
            end;
        _ ->
            {error, #{
                <<"status">> => 422,
                <<"body">> =>
                    <<"lua@5.3b requires list `patches' and message `results'.">>
            }}
    end.

%% @doc Apply a list of `{path, value|delete}' messages in order.
apply_patches(Base, [], _Opts) ->
    {ok, Base};
apply_patches(Base, [Patch | Rest], Opts) when is_map(Patch) ->
    Path = hb_ao:get(<<"path">>, Patch, not_found, Opts),
    case patch_path(Path) of
        {ok, NormalizedPath} ->
            Delete = hb_util:atom(hb_ao:get(<<"delete">>, Patch, false, Opts)),
            case {Delete, hb_ao:get(<<"value">>, Patch, not_found, Opts)} of
                {true, _} ->
                    apply_patches(
                        hb_ao:set(Base, NormalizedPath, unset, Opts),
                        Rest,
                        Opts
                    );
                {false, not_found} ->
                    patch_error(<<"Patch is missing `value'.">>);
                {false, Value} ->
                    apply_patches(
                        hb_ao:set(Base, NormalizedPath, Value, Opts),
                        Rest,
                        Opts
                    )
            end;
        Error -> Error
    end;
apply_patches(_Base, _Patches, _Opts) ->
    patch_error(<<"Every patch must be a message.">>).

%% @doc Validate a patch path and reject process/runtime-owned keys.
patch_path(Path) when is_binary(Path); is_list(Path) ->
    case hb_path:term_to_path_parts(Path) of
        [First | _] ->
            Normalized = hb_ao:normalize_key(First),
            case lists:member(Normalized, ?RESERVED_PATCH_KEYS) of
                true -> patch_error(<<"Patch targets a runtime-owned key.">>);
                false -> {ok, Path}
            end;
        [] -> patch_error(<<"Patch path cannot target the message root.">>)
    end;
patch_path(_) ->
    patch_error(<<"Patch `path' must be a path string.">>).

%% @doc Return a client-readable invalid-patch error.
patch_error(Body) ->
    {error, #{ <<"status">> => 422, <<"body">> => Body }}.

%% @doc Decode a Lua value and normalize any message commitments it carries.
decode(Value, Opts) ->
    hb_message:normalize_commitments(decode_value(Value, Opts), Opts, verify).

decode_value([], _Opts) -> #{};
decode_value(Value = [{_K, _V} | _], Opts) ->
    decode_value(decode_table(Value, Opts, #{}), Opts);
decode_value(Value, Opts) when is_map(Value) ->
    case hb_util:is_ordered_list(Value, Opts) of
        true ->
            lists:map(
                fun(Item) -> decode_value(Item, Opts) end,
                hb_util:message_to_ordered_list(Value)
            );
        false -> Value
    end;
decode_value(Value, _Opts) -> Value.

decode_table([], _Opts, Acc) -> Acc;
decode_table([{Key, Value} | Rest], Opts, Acc) ->
    decode_table(Rest, Opts, Acc#{ Key => decode_value(Value, Opts) }).

%% @doc Encode AO-Core maps as Lua tables without eagerly loading whole trees.
encode(Map, Opts) when is_map(Map) ->
    case hb_util:is_ordered_list(Map, Opts) of
        true -> encode(hb_util:message_to_ordered_list(Map), Opts);
        false -> maps:to_list(maps:map(fun(_, Value) -> encode(Value, Opts) end, Map))
    end;
encode(List, Opts) when is_list(List) ->
    lists:map(fun(Value) -> encode(Value, Opts) end, List);
encode(Link, Opts) when ?IS_LINK(Link) ->
    encode(hb_cache:ensure_all_loaded(Link, Opts), Opts);
encode(Atom, _Opts) when is_atom(Atom) andalso Atom =/= false andalso Atom =/= true ->
    hb_util:bin(Atom);
encode(Value, _Opts) -> Value.

%%% Tests

%% @doc Compare execution latency at a fixed public-state size. This is kept
%% out of EUnit so correctness tests do not acquire timing assertions.
head_to_head_benchmark(Iterations, Entries)
        when is_integer(Iterations), Iterations > 0,
             is_integer(Entries), Entries >= 0 ->
    hb:init(),
    Script = equivalent_script(),
    Opts = #{ <<"hashpath">> => ignore },
    Ledger = maps:from_list([
        {
            <<"account-", (integer_to_binary(Number))/binary>>,
            <<"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef">>
        }
        || Number <- lists:seq(1, Entries)
    ]),
    {ok, StateA0} = hb_ao:resolve(
        (base(<<"lua@5.3a">>, Script))#{ <<"ledger">> => Ledger },
        <<"init">>,
        Opts
    ),
    {ok, StateB0} = hb_ao:resolve(
        (base(<<"lua@5.3b">>, Script))#{ <<"ledger">> => Ledger },
        <<"init">>,
        Opts
    ),
    {StateA, TimesA} = benchmark_iterations(StateA0, Iterations, Opts),
    {StateB, TimesB} = benchmark_iterations(StateB0, Iterations, Opts),
    PublicA = public_benchmark_state(StateA),
    PublicB = public_benchmark_state(StateB),
    case PublicA =:= PublicB of
        true -> ok;
        false -> erlang:error({benchmark_state_mismatch, PublicA, PublicB})
    end,
    SummaryA = latency_summary(TimesA),
    SummaryB = latency_summary(TimesB),
    #{
        <<"iterations">> => Iterations,
        <<"state-entries">> => Entries,
        <<"lua@5.3a">> => SummaryA,
        <<"lua@5.3b">> => SummaryB,
        <<"p50-speedup">> =>
            maps:get(<<"p50-us">>, SummaryA) /
                maps:get(<<"p50-us">>, SummaryB)
    }.

benchmark_iterations(State0, Iterations, Opts) ->
    {State, RevTimes} =
        lists:foldl(
            fun(Number, {Prev, Times}) ->
                {Micros, {ok, Next}} = timer:tc(
                    fun() -> hb_ao:resolve(Prev, request(Number), Opts) end
                ),
                {Next, [Micros | Times]}
            end,
            {State0, []},
            lists:seq(1, Iterations)
        ),
    {State, lists:reverse(RevTimes)}.

latency_summary(Times) ->
    Sorted = lists:sort(Times),
    #{
        <<"min-us">> => hd(Sorted),
        <<"mean-us">> => lists:sum(Sorted) div length(Sorted),
        <<"p50-us">> => percentile(Sorted, 50),
        <<"p95-us">> => percentile(Sorted, 95),
        <<"max-us">> => lists:last(Sorted)
    }.

percentile(Sorted, Percent) ->
    Index = max(1, (length(Sorted) * Percent + 99) div 100),
    lists:nth(Index, Sorted).

public_benchmark_state(State) ->
    Results = maps:get(<<"results">>, State),
    Output = maps:get(<<"output">>, Results),
    #{
        <<"count">> => maps:get(<<"count">>, State),
        <<"last-action">> => maps:get(<<"last-action">>, State),
        <<"ledger">> => strip_runtime_metadata(maps:get(<<"ledger">>, State)),
        <<"output-data">> => maps:get(<<"data">>, Output)
    }.

strip_runtime_metadata(Map) when is_map(Map) ->
    maps:from_list([
        {Key, strip_runtime_metadata(Value)}
        || {Key, Value} <- maps:to_list(Map),
           Key =/= <<"commitments">>,
           Key =/= <<"priv">>
    ]);
strip_runtime_metadata(List) when is_list(List) ->
    lists:map(fun strip_runtime_metadata/1, List);
strip_runtime_metadata(Value) -> Value.

%% @doc Run the manual benchmark only when explicitly requested. Example:
%% `HB_LUA_53B_BENCH=20:0,1000,4000 rebar3 device test -d dev_lua_53b'.
head_to_head_benchmark_report_test() ->
    case os:getenv("HB_LUA_53B_BENCH") of
        false -> ok;
        Spec ->
            [IterationsRaw, EntriesRaw] = string:split(Spec, ":"),
            Iterations = list_to_integer(IterationsRaw),
            Results = [
                head_to_head_benchmark(
                    Iterations,
                    list_to_integer(Entries)
                )
                || Entries <- string:tokens(EntriesRaw, ",")
            ],
            io:format(user, "LUA53B_BENCH ~p~n", [Results])
    end.

%% @doc The same Lua source produces equivalent public state and replies under
%% full-message `5.3a' and request-only patch `5.3b' calling conventions.
head_to_head_equivalence_test() ->
    hb:init(),
    Script = equivalent_script(),
    Opts = #{ <<"hashpath">> => ignore },
    {ok, StateA0} = hb_ao:resolve(base(<<"lua@5.3a">>, Script), <<"init">>, Opts),
    {ok, StateB0} = hb_ao:resolve(base(<<"lua@5.3b">>, Script), <<"init">>, Opts),
    {StateA, StateB} =
        lists:foldl(
            fun(Number, {PrevA, PrevB}) ->
                Req = request(Number),
                {ok, NextA} = hb_ao:resolve(PrevA, Req, Opts),
                {ok, NextB} = hb_ao:resolve(PrevB, Req, Opts),
                ?assertEqual(
                    hb_ao:get(<<"count">>, NextA, Opts),
                    hb_ao:get(<<"count">>, NextB, Opts)
                ),
                ?assertEqual(
                    hb_ao:get(<<"last-action">>, NextA, Opts),
                    hb_ao:get(<<"last-action">>, NextB, Opts)
                ),
                ?assertEqual(
                    hb_ao:get(<<"results/output/data">>, NextA, Opts),
                    hb_ao:get(<<"results/output/data">>, NextB, Opts)
                ),
                {NextA, NextB}
            end,
            {StateA0, StateB0},
            lists:seq(1, 25)
        ),
    ?assertEqual(<<"25">>, hb_ao:get(<<"count">>, StateA, Opts)),
    ?assertEqual(<<"25">>, hb_ao:get(<<"count">>, StateB, Opts)).

%% @doc A `5.3b' snapshot restores its resident globals and continues.
snapshot_restore_continuity_test() ->
    hb:init(),
    Script = equivalent_script(),
    Opts = #{ <<"hashpath">> => ignore },
    {ok, State0} = hb_ao:resolve(base(<<"lua@5.3b">>, Script), <<"init">>, Opts),
    State5 =
        lists:foldl(
            fun(Number, Prev) ->
                {ok, Next} = hb_ao:resolve(Prev, request(Number), Opts),
                Next
            end,
            State0,
            lists:seq(1, 5)
        ),
    {ok, Snapshot} = hb_ao:resolve(State5, <<"snapshot">>, Opts),
    Cold =
        hb_ao:set(
            hb_private:reset(State5),
            <<"snapshot">>,
            Snapshot,
            Opts
        ),
    {ok, Restored} = hb_ao:resolve(Cold, <<"normalize">>, Opts),
    {ok, State6} = hb_ao:resolve(Restored, request(6), Opts),
    ?assertEqual(<<"6">>, hb_ao:get(<<"count">>, State6, Opts)),
    ?assertEqual(<<"6">>, hb_ao:get(<<"results/output/data">>, State6, Opts)).

%% @doc Deletes work, while runtime-owned paths are refused.
patch_validation_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    DeleteScript =
        <<
            "function compute(req) "
            "return { patches = {{ path = '/old', delete = true }}, "
            "results = { output = { data = 'deleted' } } } end"
        >>,
    DeleteBase = (base(<<"lua@5.3b">>, DeleteScript))#{ <<"old">> => <<"value">> },
    {ok, Deleted} = hb_ao:resolve(DeleteBase, request(1), Opts),
    ?assertEqual(not_found, hb_ao:get(<<"old">>, Deleted, not_found, Opts)),
    ReservedScript =
        <<
            "function compute(req) "
            "return { patches = {{ path = '/device', value = 'broken' }}, "
            "results = {} } end"
        >>,
    ?assertMatch(
        {error, #{ <<"status">> := 422 }},
        hb_ao:resolve(base(<<"lua@5.3b">>, ReservedScript), request(1), Opts)
    ).

%% @doc One source supports both execution conventions for differential tests.
equivalent_script() ->
    <<
        "Count = Count or 0\n"
        "function compute(first, second)\n"
        "  Count = Count + 1\n"
        "  local value = tostring(Count)\n"
        "  local req = second or first\n"
        "  local body = req.body or req\n"
        "  local action = tostring(body.action or '')\n"
        "  if second ~= nil then\n"
        "    first.count = value\n"
        "    first['last-action'] = action\n"
        "    first.results = { output = { data = value } }\n"
        "    return first\n"
        "  end\n"
        "  return {\n"
        "    patches = {\n"
        "      { path = '/count', value = value },\n"
        "      { path = '/last-action', value = action }\n"
        "    },\n"
        "    results = { output = { data = value } }\n"
        "  }\n"
        "end\n"
    >>.

base(Device, Script) ->
    #{
        <<"device">> => Device,
        <<"module">> => #{
            <<"content-type">> => <<"application/lua">>,
            <<"body">> => Script
        }
    }.

request(Number) ->
    #{
        <<"path">> => <<"compute">>,
        <<"body">> => #{
            <<"action">> => <<"Increment">>,
            <<"number">> => Number
        }
    }.

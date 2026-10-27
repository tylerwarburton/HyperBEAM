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
-module(dev_lua_patch).
-implements(<<"lua-patch@1.0">>).
-export([device_info/1, init/3, compute/4, snapshot/3, normalize/3, functions/3]).
-export([head_to_head_benchmark/2, incremental_gc_benchmark/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(LEGACY_LUA, <<"lua@5.3a">>).
%% @doc Expose Lua functions as device keys, using this module's compute path.
device_info(_Base) ->
    #{
        default => fun compute/4,
        direct_message_keys => true,
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
            ]
    }.

%% @doc Reuse the stable `lua@5.3a' VM initialization and sandbox, with the
%% node-dependent functions always removed.
init(Base, Req, Opts) ->
    hb_ao:raw(?LEGACY_LUA, <<"init">>, Base, Req, deterministic(Opts)).

%% @doc The node options with `math.random' and the clock removed before any
%% module loads, whatever the process sets `requires-determinism' to: their
%% results depend on the node, so two nodes would compute different states.
%% Ahead of any device's own entries (`lua-atomic@1.0' then removes `os').
deterministic(Opts) ->
    Device = hb_opts:get(<<"lua-device-sandbox">>, [], Opts),
    Opts#{
        <<"lua-device-sandbox">> =>
            [
                {[math, random], <<"sandboxed">>},
                {[math, randomseed], <<"sandboxed">>},
                {[os, time], <<"sandboxed">>},
                {[os, clock], <<"sandboxed">>},
                {[os, date], <<"sandboxed">>}
            ] ++ Device
    }.

%% @doc Reuse the stable `lua@5.3a' snapshot representation.
snapshot(Base, Req, Opts) ->
    hb_ao:raw(?LEGACY_LUA, <<"snapshot">>, Base, Req, Opts).

%% @doc Reuse the stable `lua@5.3a' snapshot restore path. It builds the host
%% functions a snapshot may keep from a new state, so it takes the same
%% sandbox as `init/3': the removed functions cannot return from a snapshot.
normalize(Base, Req, Opts) ->
    hb_ao:raw(?LEGACY_LUA, <<"normalize">>, Base, Req, deterministic(Opts)).

%% @doc Reuse the stable `lua@5.3a' function inventory.
functions(Base, Req, Opts) ->
    hb_ao:raw(?LEGACY_LUA, <<"functions">>, Base, Req, Opts).

%% @doc Invoke Lua with request-only parameters and apply its explicit patches.
compute(Key, RawBase, RawReq, Opts) ->
    T0 = erlang:monotonic_time(microsecond),
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
    T1 = erlang:monotonic_time(microsecond),
    InSlot = in_slot(OldPriv),
    % RC7's `ao' library reads the node message from here, as in `dev_lua';
    % `snapshot/3' keeps it out of the snapshot. Outside a process slot, a
    % library call that crashes is a Lua error the contract can catch
    % (`dev_lua_lib:guarded/3').
    CallState =
        luerl:put_private(
            <<"host-errors">>,
            case InSlot of true -> raise; false -> lua end,
            luerl:put_private(<<"node-message">>, Opts, State)
        ),
    Response =
        try call_lua(Function, encode(Params, Opts), CallState, deadline(InSlot, Opts))
        catch
            _:Reason:Stacktrace -> {error, Reason, Stacktrace}
        end,
    T2 = erlang:monotonic_time(microsecond),
    % Where a slot's execution time goes, printed with `compute_short'.
    % `lua_us' includes Luerl encoding the request and decoding the result.
    ?event(compute_short,
        {lua_patch_timing, {prep_us, T1 - T0}, {lua_us, T2 - T1}},
        Opts
    ),
    hb_phase_timer:record(lua_patch_prep, T1 - T0),
    hb_phase_timer:record(lua_patch_lua, T2 - T1),
    process_response(Response, Base, OldPriv, Opts).

%% @doc Whether the call computes a slot of a `process@1.0' process, which
%% marks the state it initializes (`dev_process:init/3') privately, out of
%% reach of a request.
in_slot(Priv) ->
    is_map_key(<<"initialized">>, Priv).

%% @doc How long a call may run. A slot has no deadline: it must compute the
%% same on every node, and a deadline would make the result depend on how
%% fast and how busy each node is. A direct call (`POST /~lua-patch@1.0/...')
%% keeps nothing, so it is stopped after `lua-compute-deadline' ms (default
%% two minutes; `infinity' or `false' turns it off).
deadline(true, _Opts) -> infinity;
deadline(false, Opts) ->
    case hb_opts:get(<<"lua-compute-deadline">>, 120000, Opts) of
        Ms when is_integer(Ms), Ms > 0 -> Ms;
        false -> infinity;
        infinity -> infinity;
        <<"infinity">> -> infinity;
        Other ->
            case catch hb_util:int(Other) of
                Ms when is_integer(Ms), Ms > 0 -> Ms;
                _ -> infinity
            end
    end.

%% @doc Call Lua, within a deadline when there is one. The call runs in its own
%% process, which is killed when the deadline passes, so Luerl stops working
%% instead of running on after the client has gone.
call_lua(Function, Args, State, infinity) ->
    luerl:call_function_dec([Function], Args, State);
call_lua(Function, Args, State, Ms) ->
    Self = self(),
    Ref = make_ref(),
    {Pid, Monitor} =
        spawn_monitor(
            fun() ->
                Self !
                    {Ref,
                        try {done, luerl:call_function_dec([Function], Args, State)}
                        catch Class:Reason:Stack -> {raised, Class, Reason, Stack}
                        end}
            end
        ),
    receive
        {Ref, {done, Result}} ->
            erlang:demonitor(Monitor, [flush]),
            Result;
        {Ref, {raised, Class, Reason, Stack}} ->
            erlang:demonitor(Monitor, [flush]),
            erlang:raise(Class, Reason, Stack);
        {'DOWN', Monitor, process, Pid, Reason} ->
            erlang:error({lua_call_exited, Reason})
    after Ms ->
        exit(Pid, kill),
        erlang:demonitor(Monitor, [flush]),
        {deadline, Ms}
    end.

%% @doc Normalize the two supported Lua return shapes.
process_response({ok, [Result], NewState}, Base, Priv, Opts) ->
    process_response({ok, [<<"ok">>, Result], NewState}, Base, Priv, Opts);
process_response({ok, [Status, Encoded], NewState}, Base, Priv, Opts) ->
    {DecodeMicros, Decoded} = timer:tc(fun() -> decode(Encoded, Opts) end),
    case Decoded of
        Result when is_map(Result) ->
            {ApplyMicros, Applied} =
                timer:tc(fun() -> apply_result(Base, Result, Opts) end),
            ?event(compute_short,
                {lua_patch_timing,
                    {decode_us, DecodeMicros},
                    {apply_us, ApplyMicros}
                },
                Opts
            ),
            hb_phase_timer:record(lua_patch_decode, DecodeMicros),
            hb_phase_timer:record(lua_patch_apply, ApplyMicros),
            case Applied of
                {ok, Patched, Delta} ->
                    {
                        hb_util:atom(Status),
                        Patched#{
                            <<"priv">> => Priv#{
                                <<"state">> => NewState,
                                <<"process-cache-delta">> => Delta
                            }
                        }
                    };
                Error -> Error
            end;
        Other ->
            {error, #{
                <<"status">> => 422,
                <<"body">> => <<"lua-patch@1.0 result must be a message.">>,
                <<"result">> => hb_util:bin(Other)
            }}
    end;
process_response({deadline, Ms}, _Base, _Priv, _Opts) ->
    {error, #{
        <<"status">> => 504,
        <<"body">> =>
            <<"Lua compute stopped at the node's lua-compute-deadline of ",
                (integer_to_binary(Ms))/binary, " ms.">>
    }};
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
%% Lua cannot tell an empty array from an empty table, and `decode/2' turns
%% both into `#{}', so an empty `patches' message is the empty patch list. A
%% `results' array becomes the equivalent ordered message.
apply_result(Base, Result, Opts) ->
    Patches =
        case hb_ao:get(<<"patches">>, Result, [], Opts) of
            Empty when Empty =:= #{} -> [];
            DecodedPatches -> filter_patches(DecodedPatches, Opts)
        end,
    Results =
        case hb_ao:get(<<"results">>, Result, #{}, Opts) of
            List when is_list(List) -> hb_ao:normalize_keys(List, Opts);
            DecodedResults -> DecodedResults
        end,
    case {is_list(Patches), is_map(Results)} of
        {true, true} ->
            case hb_state_patch:apply(Base, Patches, Results, Opts) of
                {ok, Patched} ->
                    {ok, Patched, #{
                        <<"patches">> => Patches,
                        <<"results">> => Results
                    }};
                Error -> Error
            end;
        _ ->
            {error, #{
                <<"status">> => 422,
                <<"body">> =>
                    <<"lua-patch@1.0 requires list `patches' and message `results'.">>
            }}
    end.

%% @doc Apply the node's invalid-patch policy. By default an invalid patch
%% fails the slot with a 422. With `lua-patch-invalid-patches' set to `drop',
%% invalid patches are removed before they are applied or stored, so a
%% contract that publishes a key built from untrusted input cannot halt the
%% process. Replay reads the stored, already filtered, patch list.
filter_patches(Patches, Opts) when is_list(Patches) ->
    case hb_opts:get(<<"lua-patch-invalid-patches">>, <<"reject">>, Opts) of
        <<"drop">> ->
            case hb_state_patch:partition(Patches, Opts) of
                {Valid, []} -> Valid;
                {Valid, Dropped} ->
                    ?event(warning, {lua_patch_dropped_patches, {dropped, Dropped}}),
                    Valid
            end;
        _ -> Patches
    end;
filter_patches(Patches, _Opts) ->
    Patches.

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
        false ->
            maps:to_list(
                maps:map(fun(Key, Value) -> encode_value(Key, Value, Opts) end, Map)
            )
    end;
encode(List, Opts) when is_list(List) ->
    lists:map(fun(Value) -> encode(Value, Opts) end, List);
encode(Link, Opts) when ?IS_LINK(Link) ->
    encode(hb_cache:ensure_all_loaded(Link, Opts), Opts);
encode(Atom, _Opts) when is_atom(Atom) andalso Atom =/= false andalso Atom =/= true ->
    hb_util:bin(Atom);
encode(Value, _Opts) -> Value.

%% @doc Lua sees only the signed commitments of a message, each with its
%% committer as an address, as `lua@5.3a' gives them. v0.11 holds a committer
%% as a URI (`uri-did-ar-...'); a contract that took that for an account id
%% would give every player a second, empty account. Unsigned commitments are
%% hidden because each node makes its own, so a script reading them could
%% compute differently on two nodes.
encode_value(<<"commitments">>, Commitments, Opts) when is_map(Commitments) ->
    Signed =
        maps:fold(
            fun(ID, Commitment = #{ <<"committer">> := Committer }, Acc) ->
                    Acc#{
                        ID => Commitment#{
                            <<"committer">> => hb_util:committer_address(Committer)
                        }
                    };
               (_ID, _Unsigned, Acc) ->
                    Acc
            end,
            #{},
            Commitments
        ),
    encode(Signed, Opts);
encode_value(_Key, Value, Opts) ->
    encode(Value, Opts).

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
        (base(<<"lua-patch@1.0">>, Script))#{ <<"ledger">> => Ledger },
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
        <<"lua-patch@1.0">> => SummaryB,
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
%% `HB_LUA_PATCH_BENCH=20:0,1000,4000 rebar3 device test -d dev_lua_patch'.
head_to_head_benchmark_report_test() ->
    case os:getenv("HB_LUA_PATCH_BENCH") of
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
            io:format(user, "LUA_PATCH_BENCH ~p~n", [Results])
    end.

%% @doc Compare one full collection per call with a bounded incremental mark
%% step over the same live heap and transient allocation stream.
incremental_gc_benchmark(Iterations, Records, GarbagePerCall)
        when Iterations > 0, Records >= 0, GarbagePerCall >= 0 ->
    Script = iolist_to_binary(io_lib:format(
        "Live = {}\n"
        "for i = 1, ~B do Live[i] = { a=i, b={i,i+1}, c=tostring(i) } end\n"
        "Counter = 0\n"
        "local function allocate()\n"
        "  local garbage = {}\n"
        "  for i = 1, ~B do garbage[i] = { i, {i+1, i+2} } end\n"
        "  garbage = nil\n"
        "  Counter = Counter + 1\n"
        "end\n"
        "function run_full() allocate(); collectgarbage('collect'); return Counter end\n"
        "function run_step() allocate(); return Counter, collectgarbage('step', 500) end\n"
        "function check() collectgarbage('collect'); return Counter, #Live end\n",
        [Records, GarbagePerCall]
    )),
    {ok, [], Full0} = luerl:do_dec(Script, luerl:init()),
    {ok, [], Step0} = luerl:do_dec(Script, luerl:init()),
    {FullTimes, FullState} = timed_luerl_calls(<<"run_full">>, Iterations, Full0),
    {StepTimes, StepState} = timed_luerl_calls(<<"run_step">>, Iterations, Step0),
    {ok, [Iterations, Records], _} =
        luerl:call_function_dec([<<"check">>], [], FullState),
    {ok, [Iterations, Records], _} =
        luerl:call_function_dec([<<"check">>], [], StepState),
    #{
        <<"iterations">> => Iterations,
        <<"live-records">> => Records,
        <<"garbage-per-call">> => GarbagePerCall,
        <<"full">> => latency_summary(FullTimes),
        <<"incremental">> => latency_summary(StepTimes)
    }.

timed_luerl_calls(Function, Iterations, State0) ->
    {State, RevTimes} = lists:foldl(
        fun(_, {Prev, Times}) ->
            {Micros, {ok, _, Next}} = timer:tc(
                fun() -> luerl:call_function_dec([Function], [], Prev) end
            ),
            {Next, [Micros | Times]}
        end,
        {State0, []},
        lists:seq(1, Iterations)
    ),
    {lists:reverse(RevTimes), State}.

incremental_gc_benchmark_report_test() ->
    case os:getenv("HB_LUERL_GC_BENCH") of
        false -> ok;
        Spec ->
            [Iterations, Records, Garbage] = [
                list_to_integer(Value)
                || Value <- string:tokens(Spec, ":")
            ],
            Result = incremental_gc_benchmark(Iterations, Records, Garbage),
            io:format(user, "LUERL_GC_BENCH ~p~n", [Result])
    end.

%% @doc The same Lua source produces equivalent public state and replies under
%% full-message `lua@5.3a' and request-only patch `lua-patch' calling conventions.
head_to_head_equivalence_test() ->
    hb:init(),
    Script = equivalent_script(),
    Opts = #{ <<"hashpath">> => ignore },
    {ok, StateA0} = hb_ao:resolve(base(<<"lua@5.3a">>, Script), <<"init">>, Opts),
    {ok, StateB0} = hb_ao:resolve(base(<<"lua-patch@1.0">>, Script), <<"init">>, Opts),
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

%% @doc A `lua-patch' snapshot restores its resident globals and continues.
snapshot_restore_continuity_test() ->
    hb:init(),
    Script = equivalent_script(),
    Opts = #{ <<"hashpath">> => ignore },
    {ok, State0} = hb_ao:resolve(base(<<"lua-patch@1.0">>, Script), <<"init">>, Opts),
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

%% @doc Snapshots keep closures, aliases and cycles. They do not collect: a
%% restored VM must hold exactly the live heap, or it diverges from the live
%% VM in table ids and table-keyed iteration order.
snapshot_restores_live_heap_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Script = <<
        "local kept = { count = 0 }; kept.self = kept\n"
        "Alias = kept\n"
        "function compute(req)\n"
        "  assert(kept == Alias and kept.self == kept)\n"
        "  kept.count = kept.count + 1\n"
        "  local garbage = {}\n"
        "  for i = 1, 1000 do\n"
        "    garbage[i] = { value = string.rep('discard', 100) .. i }\n"
        "  end\n"
        "  garbage = nil\n"
        "  collectgarbage('step', 1)\n"
        "  return { patches = {{ path = '/count', value = tostring(kept.count) }} }\n"
        "end"
    >>,
    {ok, Live} = hb_ao:resolve(base(<<"lua-patch@1.0">>, Script), request(1), Opts),
    {ok, Snapshot} = hb_ao:resolve(Live, <<"snapshot">>, Opts),
    Cold = hb_ao:set(hb_private:reset(Live), <<"snapshot">>, Snapshot, Opts),
    {ok, Restored} = hb_ao:resolve(Cold, <<"normalize">>, Opts),
    {ok, Next} = hb_ao:resolve(Restored, request(2), Opts),
    ?assertEqual(<<"2">>, hb_ao:get(<<"count">>, Next, Opts)),
    % Taking a snapshot must not replace or mutate the live VM's heap.
    {ok, LiveNext} = hb_ao:resolve(Live, request(2), Opts),
    ?assertEqual(<<"2">>, hb_ao:get(<<"count">>, LiveNext, Opts)).

%% @doc The `ao' host library works from a `lua-patch@1.0' contract.
ao_library_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Script =
        <<
            "function compute(req) "
            "return { patches = {{ path = '/got', "
            "value = ao.get('hello', { hello = 'world' }) }}, results = {} } end"
        >>,
    {ok, Out} = hb_ao:resolve(base(<<"lua-patch@1.0">>, Script), request(1), Opts),
    ?assertEqual(<<"world">>, hb_ao:get(<<"got">>, Out, Opts)).

%% @doc A direct call that runs past `lua-compute-deadline' answers 504 and
%% its Luerl work stops: no process is left running Lua.
deadline_stops_direct_compute_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{ <<"hashpath">> => ignore, <<"lua-compute-deadline">> => 300 },
        Script = <<"function compute() while true do end end">>,
        Self = self(),
        Caller =
            spawn(fun() ->
                Self ! {result, hb_ao:resolve(base(<<"lua-patch@1.0">>, Script), request(1), Opts)}
            end),
        Result =
            receive {result, R} -> R
            after 10000 -> exit(Caller, kill), no_answer
            end,
        ?assertMatch({error, #{ <<"status">> := 504 }}, Result),
        timer:sleep(100),
        InLuerl =
            [
                P
            ||
                P <- erlang:processes(),
                case erlang:process_info(P, current_function) of
                    {current_function, {M, _, _}} ->
                        lists:prefix("luerl", atom_to_list(M));
                    _ -> false
                end
            ],
        ?assertEqual([], InLuerl)
    end}.

%% @doc A process slot has no deadline, however small the setting: every node
%% must compute the same slot, whatever its speed.
slot_has_no_deadline_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"spawn-worker">> => false,
            <<"process-workers">> => false,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false,
            <<"lua-compute-deadline">> => 1
        },
        Script =
            <<
                "function compute(req)\n"
                "  local n = 0\n"
                "  for i = 1, 200000 do n = n + 1 end\n"
                "  return { patches = {{ path = '/n', value = tostring(n) }}, results = {} }\n"
                "end\n"
            >>,
        Process = delta_process(Opts, Script),
        {ok, _} = hb_cache:write(Process, Opts),
        {ok, _} = hb_ao:resolve(Process, schedule_request(Process, 1, Opts), Opts),
        {ok, State} =
            hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => 0 }, Opts),
        ?assertEqual(<<"200000">>, hb_ao:get(<<"n">>, State, Opts))
    end}.

%% @doc Outside a process slot, a library call that crashes on the node is a
%% Lua error with a fixed message, which the contract's `pcall' catches.
host_crash_is_catchable_test() ->
    hb:init(),
    Script =
        <<
            "function compute()\n"
            "  local ok, err = pcall(ao.resolve, 5)\n"
            "  return { patches = {{ path = '/caught', value = tostring(ok) .. ' ' .. tostring(err) }} }\n"
            "end\n"
        >>,
    {ok, Out} =
        hb_ao:resolve(base(<<"lua-patch@1.0">>, Script), request(1), #{ <<"hashpath">> => ignore }),
    ?assertEqual(<<"false ao.resolve failed on this node">>, hb_ao:get(<<"caught">>, Out, #{})).

%% @doc Inside a process slot it still fails the slot: another node may not
%% crash, and a caught error would let the two states differ.
host_crash_fails_slot_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"spawn-worker">> => false,
            <<"process-workers">> => false,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false
        },
        Script =
            <<
                "function compute(req)\n"
                "  local ok = pcall(ao.resolve, 5)\n"
                "  return { patches = {{ path = '/caught', value = tostring(ok) }}, results = {} }\n"
                "end\n"
            >>,
        Process = delta_process(Opts, Script),
        {ok, _} = hb_cache:write(Process, Opts),
        {ok, _} = hb_ao:resolve(Process, schedule_request(Process, 1, Opts), Opts),
        % The slot's Lua never runs past the crash: whatever the process
        % records for a failed slot, the contract did not catch it.
        Caught =
            case catch hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => 0 }, Opts) of
                {ok, State} -> hb_ao:get(<<"caught">>, State, not_found, Opts);
                _ -> not_found
            end,
        ?assertEqual(not_found, Caught)
    end}.

%% @doc An id of 43 characters that is not in canonical form names nothing:
%% resolving it is `not_found', not a crash in the Arweave store.
non_canonical_id_not_found_test() ->
    hb:init(),
    Script =
        <<
            "function compute()\n"
            "  local ok, status = pcall(ao.resolve, '/PAIRbrokennnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn/pair-vault')\n"
            "  return { patches = {{ path = '/r', value = tostring(ok) .. ' ' .. tostring(status) }} }\n"
            "end\n"
        >>,
    {ok, Out} =
        hb_ao:resolve(base(<<"lua-patch@1.0">>, Script), request(1), #{ <<"hashpath">> => ignore }),
    ?assertEqual(<<"true error">>, hb_ao:get(<<"r">>, Out, #{})).

%% @doc The node's random stream and clock are unavailable even when the base
%% does not ask for determinism: at module load, at compute, and after a
%% restore.
no_node_randomness_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Script =
        <<
            "local function kinds() "
            "return table.concat({ type(math.random), type(math.randomseed), "
            "type(os.time), type(os.clock), type(os.date) }, ' ') end "
            "local early = kinds() "
            "function compute(req) "
            "return { patches = {{ path = '/kinds', value = early .. ' ' .. kinds() }}, "
            "results = {} } end"
        >>,
    Base = (base(<<"lua-patch@1.0">>, Script))#{ <<"requires-determinism">> => false },
    Expected = iolist_to_binary(lists:join(<<" ">>, lists:duplicate(10, <<"string">>))),
    {ok, First} = hb_ao:resolve(Base, request(1), Opts),
    ?assertEqual(Expected, hb_ao:get(<<"kinds">>, First, Opts)),
    {ok, Snapshot} = hb_ao:resolve(First, <<"snapshot">>, Opts),
    Cold = hb_ao:set(hb_private:reset(First), <<"snapshot">>, Snapshot, Opts),
    {ok, Restored} = hb_ao:resolve(Cold, <<"normalize">>, Opts),
    {ok, Second} = hb_ao:resolve(Restored, request(2), Opts),
    ?assertEqual(Expected, hb_ao:get(<<"kinds">>, Second, Opts)).

%% @doc Deletes work, while runtime-owned paths are refused.
%% @doc Lua identifies a signer by its address, not by v0.11's committer URI,
%% and never sees an unsigned commitment.
committer_is_address_test() ->
    hb:init(),
    Wallet = ar_wallet:new(),
    Opts = #{ <<"hashpath">> => ignore, <<"priv-wallet">> => Wallet },
    Script =
        <<
            "function compute(req) "
            "local seen = {} "
            "for _, c in pairs(req.body.commitments or {}) do "
            "seen[#seen + 1] = tostring(c.committer) end "
            "return { patches = {{ path = '/who', value = table.concat(seen, ',') }}, "
            "results = {} } end"
        >>,
    Body = hb_message:commit(#{ <<"action">> => <<"Ping">> }, Opts),
    Req = #{ <<"path">> => <<"compute">>, <<"body">> => Body },
    {ok, Out} = hb_ao:resolve(base(<<"lua-patch@1.0">>, Script), Req, Opts),
    ?assertEqual(
        hb_util:human_id(ar_wallet:to_address(Wallet)),
        hb_ao:get(<<"who">>, Out, Opts)
    ).

patch_validation_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    DeleteScript =
        <<
            "function compute(req) "
            "return { patches = {{ path = '/old', delete = true }}, "
            "results = { output = { data = 'deleted' } } } end"
        >>,
    DeleteBase = (base(<<"lua-patch@1.0">>, DeleteScript))#{ <<"old">> => <<"value">> },
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
        hb_ao:resolve(base(<<"lua-patch@1.0">>, ReservedScript), request(1), Opts)
    ).

%% @doc An empty Lua `patches' table is an empty list, a `results' array is an
%% ordered message, and a `+link' key fails the slot unless the node drops it.
lua_result_shapes_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Run =
        fun(Body, RunOpts) ->
            Script = <<"function compute(req) return ", Body/binary, " end">>,
            hb_ao:resolve(base(<<"lua-patch@1.0">>, Script), request(1), RunOpts)
        end,
    ?assertMatch({ok, _}, Run(<<"{ patches = {}, results = {} }">>, Opts)),
    {ok, Listed} = Run(<<"{ patches = {}, results = { 'a', 'b' } }">>, Opts),
    ?assertEqual(<<"b">>, hb_ao:get(<<"results/2">>, Listed, Opts)),
    Linked =
        <<
            "{ patches = { { path = '/player-x+link', value = 'null' }, "
            "{ path = '/kept', value = 'yes' } }, results = {} }"
        >>,
    ?assertMatch({error, #{ <<"status">> := 422 }}, Run(Linked, Opts)),
    {ok, Dropped} =
        Run(Linked, Opts#{ <<"lua-patch-invalid-patches">> => <<"drop">> }),
    ?assertEqual(<<"yes">>, hb_ao:get(<<"kept">>, Dropped, Opts)),
    ?assertNot(maps:is_key(<<"player-x+link">>, Dropped)),
    #{ <<"process-cache-delta">> := #{ <<"patches">> := StoredPatches } } =
        hb_private:from_message(Dropped),
    ?assertEqual(1, length(StoredPatches)).

%% @doc Exercise the lua-patch device through process@1.0: slot 0 is a checkpoint,
%% slot 1 is a delta, and a cold request for slot 2 restores then replays.
process_delta_restore_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"hashpath">> => ignore,
            <<"spawn-worker">> => false,
            <<"process-workers">> => false,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false,
            <<"process-delta-checkpoint-slots">> => 2
        },
        Process = delta_process(Opts),
        {ok, _} = hb_cache:write(Process, Opts),
        {ok, _} = hb_ao:resolve(Process, schedule_request(Process, 1, Opts), Opts),
        {ok, _} = hb_ao:resolve(Process, schedule_request(Process, 2, Opts), Opts),
        {ok, State1} = hb_ao:resolve(
            Process,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 1 },
            Opts
        ),
        ?assertEqual(<<"2">>, hb_ao:get(<<"count">>, State1, Opts)),
        {ok, _} = hb_ao:resolve(Process, schedule_request(Process, 3, Opts), Opts),
        % `Process' has no private VM state. Reaching slot 2 must restore the
        % slot-0 VM snapshot, replay slot 1, then execute slot 2.
        {ok, State2} = hb_ao:resolve(
            Process,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 2 },
            Opts
        ),
        ?assertEqual(<<"3">>, hb_ao:get(<<"count">>, State2, Opts)),
        ?assertEqual(
            <<"3">>,
            hb_ao:get(<<"results/output/data">>, State2, Opts)
        ),
        {ok, Historical1} = hb_ao:resolve(
            Process,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 1 },
            Opts
        ),
        ?assertEqual(<<"2">>, hb_ao:get(<<"count">>, Historical1, Opts)),
        ?assertEqual(
            <<"2">>,
            hb_ao:get(<<"results/output/data">>, Historical1, Opts)
        )
    end}.

%% @doc A key written with capitals is one key through every slot: written in
%% one slot and again in later ones, through delta and checkpoint slots, a
%% cold read of an old slot, a VM restore, and an HTTP read in both forms.
%% v0.11 stores and reads every message key lowercased, so the key is
%% published lowercased and read by any spelling. Every `player-<address>'
%% and `balance-<address>' key is mixed-case; before this, a second write in a
%% later slot made the key a list.
mixed_case_keys_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"port">> => 10000 + rand:uniform(20000),
            <<"spawn-worker">> => false,
            <<"process-workers">> => false,
            <<"process-now-from-cache">> => true,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false,
            <<"process-delta-checkpoint-slots">> => 3
        },
        Node = hb_http_server:start_node(Opts),
        Script =
            <<
                "N = N or 0\n"
                "function compute(req)\n"
                "  N = N + 1\n"
                "  return { patches = {\n"
                "    { path = '/Mixed-Key', value = 'm-' .. N },\n"
                "    { path = '/once', value = 'v-' .. N }\n"
                "  }, results = {} }\n"
                "end\n"
            >>,
        Process = delta_process(Opts, Script),
        {ok, _} = hb_cache:write(Process, Opts),
        ProcID = hb_util:human_id(hb_message:id(Process, all, Opts)),
        lists:foreach(
            fun(N) ->
                {ok, _} = hb_ao:resolve(Process, schedule_request(Process, N, Opts), Opts)
            end,
            lists:seq(1, 6)
        ),
        Compute =
            fun(Base, Slot) ->
                hb_ao:resolve(
                    Base,
                    #{ <<"path">> => <<"compute">>, <<"slot">> => Slot },
                    Opts
                )
            end,
        Exact =
            fun(State, Value) ->
                Public = hb_private:reset(hb_cache:ensure_all_loaded(State, Opts)),
                ?assertEqual(
                    {Value, false, Value},
                    {
                        maps:get(<<"mixed-key">>, Public, missing),
                        maps:is_key(<<"Mixed-Key">>, Public),
                        hb_ao:get(<<"Mixed-Key">>, Public, Opts)
                    }
                )
            end,
        % Slots 1, 2, 4 are deltas and 3 a checkpoint.
        {ok, State4} = Compute(Process, 4),
        Exact(State4, <<"m-5">>),
        % Cold reads of a delta slot and a checkpoint, from the cache.
        {ok, Cached2} = Compute(Process, 2),
        Exact(Cached2, <<"m-3">>),
        {ok, Cached3} = Compute(Process, 3),
        Exact(Cached3, <<"m-4">>),
        % The next slot restores the VM from the cached public state.
        {ok, Cached4} = Compute(Process, 4),
        {ok, State5} = Compute(Cached4, 5),
        Exact(State5, <<"m-6">>),
        % Read as a client does: plain HTTP, the key spelt as it was written.
        {ok, _} = application:ensure_all_started(inets),
        Get =
            fun(Path, Accept) ->
                {ok, {{_, Status, _}, _, Body}} =
                    httpc:request(
                        get,
                        {
                            hb_util:list(Node) ++ hb_util:list(ProcID)
                                ++ "~process@1.0/" ++ Path,
                            [{"accept", Accept}]
                        },
                        [{timeout, 30000}],
                        [{body_format, binary}]
                    ),
                {Status, Body}
            end,
        ?assertEqual({200, <<"m-6">>}, Get("now/Mixed-Key", "text/plain")),
        ?assertEqual({200, <<"m-6">>}, Get("now/mixed-key", "text/plain")),
        {200, JSON} = Get("now", "application/json"),
        Decoded = hb_json:decode(JSON),
        ?assertEqual(
            {<<"m-6">>, false},
            {
                maps:get(<<"mixed-key">>, Decoded, missing),
                maps:is_key(<<"Mixed-Key">>, Decoded)
            }
        )
    end}.

%% @doc A persistent process worker that is asked for a slot it has already
%% computed answers from the process cache. That cached state is public only:
%% it carries no Luerl VM. The worker must keep its own live state rather than
%% adopt the cached one, or its next slot runs against a freshly initialized VM
%% and every Lua global silently resets while the slot counter continues.
worker_keeps_live_vm_after_cached_read_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"spawn-worker">> => true,
            <<"process-worker-spawn">> => true,
            <<"process-workers">> => true,
            <<"await-inprogress">> => named,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false,
            <<"process-delta-checkpoint-slots">> => 1000
        },
        Process = delta_process(Opts),
        {ok, _} = hb_cache:write(Process, Opts),
        [
            {ok, _} = hb_ao:resolve(Process, schedule_request(Process, N, Opts), Opts)
        ||
            N <- lists:seq(1, 3)
        ],
        {ok, State2} = hb_ao:resolve(
            Process,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 2 },
            Opts
        ),
        ?assertEqual(<<"3">>, hb_ao:get(<<"count">>, State2, Opts)),
        Group = hb_util:human_id(hb_message:id(Process, all, Opts)),
        Worker = wait_for_worker(Group, 50),
        % Ask the live worker for a slot it has already computed, exactly as
        % `hb_persistent:await/5' does for a request grouped before the slot
        % reached the cache.
        Worker ! {
            resolve,
            self(),
            Group,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 1 },
            Opts
        },
        receive
            {resolved, _, Group, {slot, 1}, {ok, State1}} ->
                ?assertEqual(<<"2">>, hb_ao:get(<<"count">>, State1, Opts))
        after 10000 -> erlang:error(worker_did_not_answer)
        end,
        {ok, _} = hb_ao:resolve(Process, schedule_request(Process, 4, Opts), Opts),
        {ok, State3} = hb_ao:resolve(
            Process,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 3 },
            Opts
        ),
        % Slot 3 is the fourth execution. A reset VM would report `2' here.
        ?assertEqual(<<"4">>, hb_ao:get(<<"count">>, State3, Opts))
    end}.

%% @doc A worker can be spawned with the result of a request that was served
%% from the process cache. It must restore a real VM before computing, rather
%% than continue from the public-only cached state.
worker_started_from_cached_state_restores_vm_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"spawn-worker">> => false,
            <<"process-workers">> => false,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false,
            <<"process-delta-checkpoint-slots">> => 1000
        },
        Process = delta_process(Opts),
        {ok, _} = hb_cache:write(Process, Opts),
        [
            {ok, _} = hb_ao:resolve(Process, schedule_request(Process, N, Opts), Opts)
        ||
            N <- lists:seq(1, 4)
        ],
        {ok, _} = hb_ao:resolve(
            Process,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 2 },
            Opts
        ),
        % A second request for slot 2 is a cache hit: public state, no VM.
        {ok, Cached} = hb_ao:resolve(
            Process,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 2 },
            Opts
        ),
        Group = hb_util:human_id(hb_message:id(Process, all, Opts)),
        Worker = hb_persistent:start_worker(Group, Cached, Opts),
        Worker ! {
            resolve,
            self(),
            Group,
            #{ <<"path">> => <<"compute">>, <<"slot">> => 3 },
            Opts
        },
        receive
            {resolved, _, Group, {slot, 3}, {ok, State3}} ->
                ?assertEqual(<<"4">>, hb_ao:get(<<"count">>, State3, Opts))
        after 20000 -> erlang:error(worker_did_not_answer)
        end,
        exit(Worker, kill)
    end}.

%% @doc A `now' read over HTTP is served from the process cache and must not
%% write the state back to the store. HTTP requests run under the node's
%% default `cache-control: always', which made every `now' read, and every key
%% read from its result, re-serialize, re-hash and write the whole process
%% state -- although the delta cache already holds it durably.
now_http_read_does_not_store_state_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"port">> => 10000 + rand:uniform(20000),
            <<"process-now-from-cache">> => true,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false,
            <<"process-delta-checkpoint-slots">> => 1000
        },
        Node = hb_http_server:start_node(Opts),
        Process = delta_process(Opts),
        {ok, _} = hb_cache:write(Process, Opts),
        Schedule =
            fun(N) ->
                {ok, _} =
                    hb_ao:resolve(Process, schedule_request(Process, N, Opts), Opts)
            end,
        Compute =
            fun(Slot) ->
                {ok, _} =
                    hb_ao:resolve(
                        Process,
                        #{ <<"path">> => <<"compute">>, <<"slot">> => Slot },
                        Opts
                    )
            end,
        lists:foreach(Schedule, [1, 2, 3]),
        Compute(2),
        ProcID = hb_util:human_id(hb_message:id(Process, all, Opts)),
        Get =
            fun(Path) ->
                hb_http:get(
                    Node,
                    <<"/", ProcID/binary, "~process@1.0/", Path/binary>>,
                    Opts
                )
            end,
        {Reads, StateWrites} =
            count_state_writes(
                fun() ->
                    [
                        Get(<<"now/count">>),
                        Get(<<"now/last-action">>),
                        Get(<<"compute/count?slot=1">>)
                    ]
                end
            ),
        ?assertMatch(
            [{ok, <<"3">>}, {ok, <<"Increment">>}, {ok, <<"2">>}],
            Reads
        ),
        ?assertEqual(0, StateWrites),
        % `now' still follows newly computed slots.
        Schedule(4),
        Compute(3),
        ?assertMatch({ok, <<"4">>}, Get(<<"now/count">>))
    end}.

%% @doc Run `Fun' and count the `hb_cache:write/2' calls, in any process, whose
%% message is a process state (it carries `last-action').
count_state_writes(Fun) ->
    MatchSpec =
        [{['$1', '_'],
            [{is_map, '$1'}, {is_map_key, <<"last-action">>, '$1'}],
            []}],
    erlang:trace_pattern({hb_cache, write, 2}, MatchSpec, [global]),
    erlang:trace(all, true, [call, {tracer, self()}]),
    Res =
        try Fun()
        after
            erlang:trace(all, false, [call]),
            erlang:trace_pattern({hb_cache, write, 2}, false, [global])
        end,
    {Res, count_trace_messages(0)}.

count_trace_messages(N) ->
    receive
        {trace, _, call, {hb_cache, write, _}} -> count_trace_messages(N + 1)
    after 100 -> N
    end.

%% @doc A push computes its process inside another resolution. That compute
%% must leave a persistent worker behind, so the next push computes one slot
%% from the live VM instead of restoring the last snapshot and replaying every
%% slot since -- which after a restart made every write replay ~1,000 slots.
push_leaves_worker_for_next_push_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"spawn-worker">> => true,
            <<"process-worker-spawn">> => true,
            <<"process-workers">> => true,
            <<"await-inprogress">> => named,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false,
            <<"process-delta-checkpoint-slots">> => 1000,
            <<"port">> => 10000 + rand:uniform(20000)
        },
        Node = hb_http_server:start_node(Opts),
        Process = delta_process(Opts),
        {ok, _} = hb_cache:write(Process, Opts),
        Group = hb_util:human_id(hb_message:id(Process, all, Opts)),
        Schedule =
            fun(N) ->
                {ok, _} =
                    hb_ao:resolve(Process, schedule_request(Process, N, Opts), Opts)
            end,
        % Exactly what clients send after a write: `GET .../push&slot=N'.
        Push =
            fun(Slot) ->
                {ok, _} =
                    hb_http:get(
                        Node,
                        <<"/", Group/binary, "~process@1.0/push&slot=",
                            (integer_to_binary(Slot))/binary>>,
                        Opts
                    )
            end,
        lists:foreach(Schedule, [1, 2, 3, 4]),
        % The first push catches up from the start: slots 1-3 as deltas.
        ?assertEqual(3, count_slot_computes(fun() -> Push(3) end)),
        Worker = wait_for_worker(Group, 50),
        Schedule(5),
        % The next push is served by that worker: exactly one slot.
        ?assertEqual(1, count_slot_computes(fun() -> Push(4) end)),
        ?assertEqual(Worker, hb_name:lookup(Group)),
        {ok, State} =
            hb_ao:resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => 4 },
                Opts
            ),
        ?assertEqual(<<"5">>, hb_ao:get(<<"count">>, State, Opts)),
        exit(Worker, kill)
    end}.

%% @doc Run `Fun' and count, in any process, the slots it computes and stores
%% (each slot that is not a checkpoint is stored as one delta envelope).
count_slot_computes(Fun) ->
    MatchSpec =
        [{['$1', '_'],
            [{is_map, '$1'}, {is_map_key, <<"cache-format">>, '$1'}],
            []}],
    erlang:trace_pattern({hb_cache, write, 2}, MatchSpec, [global]),
    % With `process-state-blobs' a delta slot is one `d' row instead.
    erlang:trace_pattern({hb_store_lmdb, exact_put, 3}, true, [global]),
    erlang:trace(all, true, [call, {tracer, self()}]),
    try Fun()
    after
        erlang:trace(all, false, [call]),
        erlang:trace_pattern({hb_cache, write, 2}, false, [global]),
        erlang:trace_pattern({hb_store_lmdb, exact_put, 3}, false, [global])
    end,
    count_slot_trace_messages(0).

count_slot_trace_messages(N) ->
    receive
        {trace, _, call, {hb_cache, write, _}} ->
            count_slot_trace_messages(N + 1);
        {trace, _, call, {hb_store_lmdb, exact_put, [_, Key, _]}} ->
            case binary:match(Key, <<"/d/">>) of
                nomatch -> count_slot_trace_messages(N);
                _ -> count_slot_trace_messages(N + 1)
            end
    after 100 -> N
    end.

%% @doc A worker computing a slot for a listener that runs under
%% `cache-control: always' (every HTTP request) stores only what the process
%% cache stores -- a small delta -- and never writes the whole state.
worker_compute_does_not_store_whole_state_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"spawn-worker">> => true,
            <<"process-worker-spawn">> => true,
            <<"process-workers">> => true,
            <<"await-inprogress">> => named,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false,
            <<"process-delta-checkpoint-slots">> => 1000
        },
        Process = delta_process(Opts),
        {ok, _} = hb_cache:write(Process, Opts),
        Group = hb_util:human_id(hb_message:id(Process, all, Opts)),
        [
            {ok, _} = hb_ao:resolve(Process, schedule_request(Process, N, Opts), Opts)
        ||
            N <- lists:seq(1, 3)
        ],
        {ok, _} =
            hb_ao:resolve(
                Process,
                #{ <<"path">> => <<"compute">>, <<"slot">> => 1 },
                Opts
            ),
        Worker = wait_for_worker(Group, 50),
        ListenerOpts = Opts#{ <<"cache-control">> => [<<"always">>] },
        {Res, Writes} =
            count_state_writes(
                fun() ->
                    Worker !
                        {
                            resolve,
                            self(),
                            Group,
                            #{ <<"path">> => <<"compute">>, <<"slot">> => 2 },
                            ListenerOpts
                        },
                    receive
                        {resolved, _, Group, {slot, 2}, R} -> R
                    after 20000 -> erlang:error(worker_did_not_answer)
                    end
                end
            ),
        {ok, State} = Res,
        ?assertEqual(<<"3">>, hb_ao:get(<<"count">>, State, Opts)),
        ?assertEqual(0, Writes),
        exit(Worker, kill)
    end}.

%% @doc A public state -- a cache hit, or what a worker now sends its
%% listeners -- carries no VM. Computing onward from one must restore the VM
%% from the process, not run the next slot against a fresh one.
compute_from_public_state_restores_vm_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"spawn-worker">> => false,
            <<"process-workers">> => false,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false,
            <<"process-delta-checkpoint-slots">> => 1000
        },
        Process = delta_process(Opts),
        {ok, _} = hb_cache:write(Process, Opts),
        [
            {ok, _} = hb_ao:resolve(Process, schedule_request(Process, N, Opts), Opts)
        ||
            N <- lists:seq(1, 3)
        ],
        Compute =
            fun(Base, Slot) ->
                hb_ao:resolve(
                    Base,
                    #{ <<"path">> => <<"compute">>, <<"slot">> => Slot },
                    Opts
                )
            end,
        {ok, _} = Compute(Process, 1),
        % A second read of slot 1 is a cache hit: public state, no VM.
        {ok, Public} = Compute(Process, 1),
        ?assertEqual(
            true,
            maps:get(<<"process-cached-state">>, hb_private:from_message(Public))
        ),
        {ok, State2} = Compute(Public, 2),
        % Slot 2 is the third execution. A fresh VM would report `1'.
        ?assertEqual(<<"3">>, hb_ao:get(<<"count">>, State2, Opts))
    end}.

wait_for_worker(_Group, 0) -> erlang:error(no_process_worker);
wait_for_worker(Group, Tries) ->
    case hb_name:lookup(Group) of
        Pid when is_pid(Pid) -> Pid;
        _ -> timer:sleep(100), wait_for_worker(Group, Tries - 1)
    end.

%% @doc Luerl's `step' collector advances a real multi-call mark cycle while
%% allocations and reachable mutations continue between steps. Externalizing
%% a pending VM keeps its cycle, so a restored VM steps exactly like the live one.
incremental_gc_step_test() ->
    Script = <<
        "Kept = {}\n"
        "function stepper(budget)\n"
        "  local n = #Kept + 1\n"
        "  Kept[n] = { value = n }\n"
        "  local complete = collectgarbage('step', budget)\n"
        "  return complete, n, Kept[n].value\n"
        "end\n"
        "function kept_count() return #Kept end\n"
    >>,
    {ok, [], State0} = luerl:do_dec(Script, luerl:init()),
    {ok, [false, 1, 1], State1} =
        luerl:call_function_dec([<<"stepper">>], [1], State0),
    {CompletedAt, State2} = finish_gc_cycle(State1, 2, 1000),
    {ok, [CompletedAt], State3} =
        luerl:call_function_dec([<<"kept_count">>], [], State2),
    {ok, [false, Next, Next], Pending} =
        luerl:call_function_dec([<<"stepper">>], [1], State3),
    External = luerl:externalize(Pending),
    Restored = luerl:internalize(External),
    {ok, [true, AfterRestore, AfterRestore], _} =
        luerl:call_function_dec([<<"stepper">>], [1000000], Restored),
    ?assertEqual(Next + 1, AfterRestore).

finish_gc_cycle(_State, Number, Limit) when Number > Limit ->
    erlang:error(incremental_gc_did_not_complete);
finish_gc_cycle(State, Number, Limit) ->
    case luerl:call_function_dec([<<"stepper">>], [25], State) of
        {ok, [true, Number, Number], Next} -> {Number, Next};
        {ok, [false, Number, Number], Next} ->
            finish_gc_cycle(Next, Number + 1, Limit)
    end.

delta_process(Opts) ->
    delta_process(Opts, equivalent_script()).
delta_process(Opts, Script) ->
    Wallet = hb_opts:get(<<"priv-wallet">>, hb:wallet(), Opts),
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    hb_message:commit(
        #{
            <<"device">> => <<"process@1.0">>,
            <<"type">> => <<"Process">>,
            <<"scheduler-device">> => <<"scheduler@1.0">>,
            <<"execution-device">> => <<"lua-patch@1.0">>,
            <<"module">> => #{
                <<"content-type">> => <<"application/lua">>,
                <<"body">> => Script
            },
            <<"authority">> => [Address],
            <<"scheduler-location">> => Address,
            <<"test-random-seed">> => 53
        },
        Opts
    ).

schedule_request(Process, Number, Opts) ->
    ProcID = hb_message:id(Process, all, Opts),
    hb_message:commit(
        #{
            <<"path">> => <<"schedule">>,
            <<"method">> => <<"POST">>,
            <<"body">> => hb_message:commit(
                #{
                    <<"target">> => ProcID,
                    <<"type">> => <<"Message">>,
                    <<"action">> => <<"Increment">>,
                    <<"number">> => Number
                },
                Opts
            )
        },
        Opts
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

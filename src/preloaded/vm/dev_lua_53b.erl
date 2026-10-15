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
-export([head_to_head_benchmark/2, incremental_gc_benchmark/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(LEGACY_LUA, <<"lua@5.3a">>).
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
            case hb_process_delta:apply(Base, Patches, Results, Opts) of
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
                    <<"lua@5.3b requires list `patches' and message `results'.">>
            }}
    end.

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

%% @doc Exercise the 5.3b device through process@1.0: slot 0 is a checkpoint,
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
            <<"process-workers">> => true,
            <<"await-inprogress">> => named,
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
        % `hb_persistent:await/4' does for a request grouped before the slot
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
            <<"process-workers">> => true,
            <<"await-inprogress">> => named,
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
    erlang:trace(all, true, [call, {tracer, self()}]),
    try Fun()
    after
        erlang:trace(all, false, [call]),
        erlang:trace_pattern({hb_cache, write, 2}, false, [global])
    end,
    count_trace_messages(0).

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
            <<"process-workers">> => true,
            <<"await-inprogress">> => named,
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
%% a pending VM safely cancels its heap snapshot instead of serializing it.
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
    ?assertError(
        {badkey, luerl_gc_step_cycle},
        luerl:get_private(luerl_gc_step_cycle, External)
    ),
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
    Wallet = hb_opts:get(<<"priv-wallet">>, hb:wallet(), Opts),
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    hb_message:commit(
        #{
            <<"device">> => <<"process@1.0">>,
            <<"type">> => <<"Process">>,
            <<"scheduler-device">> => <<"scheduler@1.0">>,
            <<"execution-device">> => <<"lua@5.3b">>,
            <<"module">> => #{
                <<"content-type">> => <<"application/lua">>,
                <<"body">> => equivalent_script()
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

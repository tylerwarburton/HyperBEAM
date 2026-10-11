-module(luerl_gc_bench).
%% Luerl worker GC benchmark: a large live Lua table set, an allocation-heavy
%% action per message, and optionally a full collectgarbage("collect") per
%% message (as the game contract does). Reports per-message time, Erlang major
%% and minor collections (counted by tracing the worker) and heap size of the
%% worker process under different spawn options / forced-GC policies. Not a
%% test: run it by hand, against the luerl in the image, e.g.
%%
%%   erlc scripts/luerl_gc_bench.erl
%%   erl -noshell -pa _build/default/lib/luerl/ebin \
%%       -eval 'luerl_gc_bench:main(["300000", "30", "1", "default", "fs0",
%%              "young256", "every10"]), halt().'
%%
%% Arguments: players (3 tables each; 300000 is ~280 MB live), messages,
%% collect per message (1/0), then variants: `default', `fs0'
%% (fullsweep_after 0), `young<MB>' (min_heap_size), `vheap<MB>'
%% (min_bin_vheap_size), `every<K>' (a forced major+minor collection every K
%% messages, as `process-worker-gc' does at checkpoints). Keep it to one CPU
%% and a few GB on a shared host.
-export([build/1, run/4, main/1]).

script() -> <<"
Players = {}
function setup(n)
  for i = 1, n do
    Players[i] = { id = i, name = 'player-' .. i, hp = i % 100,
                   inv = { i, i + 1, i + 2 }, pos = { x = i, y = -i } }
  end
end
function handle(k, collect)
  local out = {}
  local n = #Players
  for j = 1, 300 do
    local p = Players[1 + (k * 7919 + j * 104729) % n]
    p.hp = (p.hp + 1) % 100
    p.inv[(k + j) % 3 + 1] = k
    p.pos = { x = p.pos.x + 1, y = p.pos.y }
    out[#out + 1] = { id = p.id, hp = p.hp, msg = 'updated ' .. p.name }
  end
  if collect then collectgarbage('collect') end
  return #out
end
">>.

build(N) ->
    St0 = luerl:init(),
    {ok, _, St1} = luerl:do(script(), St0),
    {ok, _, St2} = luerl:call_function([<<"setup">>], [N], St1),
    luerl:gc(St2).

%% Variant = {Name, SpawnOpts, Policy}; Policy = none | {every, K} (major+minor
%% after every K messages, as the checkpoint GC does).
run(St, Msgs, {Name, SpawnOpts, Policy}, Collect) ->
    Self = self(),
    Tracer = spawn(fun() -> tracer([]) end),
    {Pid, Ref} = spawn_opt(
        fun() ->
            receive go -> ok end,
            put(collect, Collect),
            Self ! {self(), worker(St, Msgs, Policy)}
        end, [monitor, {priority, normal} | SpawnOpts]),
    erlang:trace(Pid, true, [garbage_collection, monotonic_timestamp, {tracer, Tracer}]),
    Pid ! go,
    R = receive {Pid, X} -> X end,
    receive {'DOWN', Ref, _, _, _} -> ok end,
    Tracer ! {done, self()},
    Events = receive {events, E} -> E end,
    report(Name, R, Events, Msgs).

tracer(Acc) ->
    receive
        {trace_ts, _, Ev, Info, TS} -> tracer([{Ev, TS, Info} | Acc]);
        {done, P} -> P ! {events, lists:reverse(Acc)}
    end.

worker(St0, Msgs, Policy) ->
    erlang:garbage_collect(),
    {memory, M0} = process_info(self(), memory),
    {Ts, Ms, Gs, St} = loop(1, Msgs, Policy, St0, [], [], []),
    put(keep, St),
    {memory, MEnd} = process_info(self(), memory),
    erlang:trace(self(), false, [garbage_collection]),
    erlang:garbage_collect(),
    erlang:garbage_collect(self(), [{type, minor}]),
    {memory, MIdle} = process_info(self(), memory),
    #{start_mb => M0 div 1048576, end_mb => MEnd div 1048576,
      idle_mb => MIdle div 1048576, times => lists:reverse(Ts),
      mems => lists:reverse(Ms), forced => lists:reverse(Gs)}.

loop(K, Msgs, _P, St, Ts, Ms, Gs) when K > Msgs -> {Ts, Ms, Gs, St};
loop(K, Msgs, Policy, St0, Ts, Ms, Gs) ->
    T0 = erlang:monotonic_time(microsecond),
    {ok, _, St1} = luerl:call_function([<<"handle">>], [K, get(collect)], St0),
    T1 = erlang:monotonic_time(microsecond),
    {memory, M} = process_info(self(), memory),
    Gs1 =
        case Policy of
            {every, E} when K rem E == 0 ->
                G0 = erlang:monotonic_time(microsecond),
                erlang:garbage_collect(),
                erlang:garbage_collect(self(), [{type, minor}]),
                [erlang:monotonic_time(microsecond) - G0 | Gs];
            _ -> Gs
        end,
    loop(K + 1, Msgs, Policy, St1, [T1 - T0 | Ts], [M | Ms], Gs1).

report(Name, #{start_mb := S, end_mb := ME, idle_mb := MI, times := Ts, mems := Ms, forced := Gs}, Events, Msgs) ->
    Pauses = fun(Start, End) -> pauses(Events, Start, End, undefined, []) end,
    Major = Pauses(gc_major_start, gc_major_end),
    Minor = Pauses(gc_minor_start, gc_minor_end),
    Sorted = lists:sort(Ts),
    N = length(Ts),
    Steady = lists:nthtail(N div 4, Ms),
    io:format(
        "~-22s msg_ms p50 ~6.1f p90 ~7.1f max ~7.1f mean ~6.1f | "
        "majors ~4w (~.2f/msg) pause mean ~6.1f max ~6.1f ms | "
        "minors ~5w total ~7.1f ms | heap MB start ~w mean ~w max ~w end ~w after-idle-gc ~w | forced ~w x ~.1f ms~n",
        [Name, pct(Sorted, 50), pct(Sorted, 90), lists:last(Sorted) / 1000,
         lists:sum(Ts) / N / 1000,
         length(Major), length(Major) / Msgs, mean(Major), max0(Major),
         length(Minor), float(lists:sum(Minor)),
         S, lists:sum(Steady) div length(Steady) div 1048576,
         lists:max(Steady) div 1048576, ME, MI, length(Gs), mean([G / 1000 || G <- Gs])]).

pauses([], _, _, _, Acc) -> Acc;
pauses([{S, TS, _} | R], S, E, _, Acc) -> pauses(R, S, E, TS, Acc);
pauses([{E, TS, _} | R], S, E, T0, Acc) when T0 =/= undefined ->
    pauses(R, S, E, undefined,
        [erlang:convert_time_unit(TS - T0, native, microsecond) / 1000 | Acc]);
pauses([_ | R], S, E, T0, Acc) -> pauses(R, S, E, T0, Acc).

pct(Sorted, P) -> lists:nth(max(1, length(Sorted) * P div 100), Sorted) / 1000.
mean([]) -> 0.0;
mean(L) -> lists:sum(L) / length(L).
max0([]) -> 0.0;
max0(L) -> lists:max(L).

%% main([N, Msgs, VariantNames...])
main([NA, MA, CA | Names]) ->
    N = list_to_integer(NA), Msgs = list_to_integer(MA),
    Collect = CA =:= "1",
    T0 = erlang:monotonic_time(millisecond),
    St = build(N),
    Live = erts_debug:flat_size(St) * 8 div 1048576,
    io:format("state: ~w players, live ~w MB, built in ~w ms, luerl_heap ~s~n",
        [N, Live, erlang:monotonic_time(millisecond) - T0,
         case lists:member({seen_new, 1}, luerl_heap:module_info(functions)) of
             true -> "low-garbage";
             false -> "pre-2026-10-11"
         end]),
    io:format("luerl collect per message: ~p~n", [Collect]),
    lists:foreach(fun(Name) -> run(St, Msgs, variant(Name), Collect) end, Names).

variant("default") -> {"default", [], none};
variant("fs0") -> {"fullsweep_after=0", [{fullsweep_after, 0}], none};
variant("young" ++ MB) ->
    W = list_to_integer(MB) * 1048576 div 8,
    {"min_heap " ++ MB ++ "MB", [{min_heap_size, W}], none};
variant("vheap" ++ MB) ->
    W = list_to_integer(MB) * 1048576 div 8,
    {"min_bin_vheap " ++ MB ++ "MB", [{min_bin_vheap_size, W}], none};
variant("every" ++ K) -> {"forced gc every " ++ K, [], {every, list_to_integer(K)}}.

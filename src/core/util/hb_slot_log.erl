%%% @doc What each process's slots cost and what they ran, kept in ETS for
%%% `/~hyperbuddy@1.0/slots'.
%%%
%%% Per process (keyed by the process id that `compute_slot' computes, never
%%% the calling Erlang process's label): cumulative counters -- slots, error
%%% slots and the microseconds of every slot phase -- cumulative counters per
%%% action, and a ring of the last `slot-log-size' slots, which is only used
%%% for percentiles. Rates and means come from the counters.
%%%
%%% Lua phases are reported by the VM inside a slot's execution (`note/2').
%%% `run/1' opens a fresh set of notes for the execution and closes it on the
%%% way out, crash or not, restoring whatever was open around it, so a nested
%%% compute neither loses nor steals its caller's notes. `end_slot/8' adds the
%%% notes to the slot's process exactly once.
%%%
%%% Options, read by `hb_phase_timer:configure/1': `slot-log' (default true),
%%% `slot-log-size' (ring rows per process, 16-256, default 128) and
%%% `slot-log-processes' (processes with their own counters, default 512;
%%% slots of later ones count only towards the node's `unassigned' totals).
%%% At most 65,536 ring rows in all. The log is off whenever phase timers
%%% are. The table is created by `hb_phase_timer''s owner, which also runs
%%% `sweep/0' every minute: a process with no slot for 15 minutes and no
%%% process worker is forgotten.
%%%
%%% Keys and actions are binaries, never atoms.
-module(hb_slot_log).
-export([configure/1, new_table/0, run/1, note/2, end_slot/8]).
-export([report/1, sweep/0, sweep/1, reset/0, action/1, memo/3]).
-include_lib("eunit/include/eunit.hrl").

-define(T, hb_slot_log).
-define(OPEN, {?MODULE, open}).
-define(CONFIG, {?MODULE, config}).
-define(DEFAULT_CONFIG, {true, 128, 512}).
-define(MAX_ROWS, 65536).
-define(MAX_ACTIONS, 32).
-define(OTHER, <<"other">>).
-define(IDLE_MS, 15 * 60 * 1000).

%% Counter row: {{Proc, c}, Slots, ErrorSlots, PrepUs, ExecUs, StoreUs,
%% LuaPrepUs, LuaUs, DecodeUs, ApplyUs, GcUs, Seq, Actions, LastSlot, LastMs}.
-define(SEQ, 12).
-define(NACTIONS, 13).
-define(LAST_SLOT, 14).
-define(LAST_MS, 15).
-define(LUA_PHASES,
    [lua_patch_prep, lua_patch_lua, lua_patch_decode, lua_patch_apply, lua_patch_gc]).

%% @doc Read the slot-log options. Called by `hb_phase_timer:configure/1'.
configure(Opts) ->
    On =
        case hb_opts:get(<<"slot-log">>, true, Opts) of
            false -> false;
            <<"false">> -> false;
            _ -> true
        end,
    Size = clamp(int(hb_opts:get(<<"slot-log-size">>, 128, Opts), 128), 16, 256),
    Procs =
        clamp(int(hb_opts:get(<<"slot-log-processes">>, 512, Opts), 512),
            1, ?MAX_ROWS div Size),
    Config = {On, Size, Procs},
    % A persistent_term write is a global GC; only make one when it changes.
    case config() of
        Config -> ok;
        _ -> persistent_term:put(?CONFIG, Config)
    end,
    ok.

config() -> persistent_term:get(?CONFIG, ?DEFAULT_CONFIG).

int(V, Default) ->
    case catch hb_util:int(V) of
        N when is_integer(N) -> N;
        _ -> Default
    end.

clamp(N, Lo, Hi) -> max(Lo, min(Hi, N)).

%% @doc Create the table. Only `hb_phase_timer''s owner calls this.
new_table() ->
    ets:new(?T, [named_table, public, set, {write_concurrency, true}]).

%% @doc Run a slot's execution with its own notes open. Returns
%% `{Micros, Result, Notes}'. Whatever was open before is restored on the way
%% out, whether `Fun' returns or crashes.
run(Fun) ->
    Prev = put(?OPEN, #{}),
    try timer:tc(Fun) of
        {Micros, Res} -> {Micros, Res, notes()}
    after
        case Prev of
            undefined -> erase(?OPEN);
            _ -> put(?OPEN, Prev)
        end
    end.

notes() ->
    case get(?OPEN) of
        Notes when is_map(Notes) -> Notes;
        _ -> #{}
    end.

%% @doc Record a phase of the running slot. Inside `run/1' it counts node-wide
%% now and against the slot's process at `end_slot/8'; outside one it is
%% `hb_phase_timer:record/2', as before.
note(Phase, Micros) ->
    case get(?OPEN) of
        Notes when is_map(Notes) ->
            hb_phase_timer:record_node(Phase, Micros),
            put(?OPEN, Notes#{ Phase => maps:get(Phase, Notes, 0) + Micros }),
            ok;
        _ -> hb_phase_timer:record(Phase, Micros)
    end.

%% @doc Account one computed slot of `ProcID'. `Status' is `ok', `error' or
%% `failure'; `Notes' is what `run/1' returned.
end_slot(ProcID, Slot, Action, Status, PrepUs, ExecUs, StoreUs, Notes) ->
    case hb_phase_timer:enabled() of
        false -> ok;
        true ->
            hb_phase_timer:record_node(slot_prep, PrepUs),
            hb_phase_timer:record_node(slot_execution, ExecUs),
            hb_phase_timer:record_node(slot_store, StoreUs),
            Label = hb_phase_timer:label(ProcID),
            hb_phase_timer:record_label(slot_prep, Label, PrepUs),
            hb_phase_timer:record_label(slot_execution, Label, ExecUs),
            hb_phase_timer:record_label(slot_store, Label, StoreUs),
            maps:foreach(
                fun(Phase, Us) -> hb_phase_timer:record_label(Phase, Label, Us) end,
                Notes
            ),
            case config() of
                {true, Size, Cap} ->
                    try log(ProcID, Slot, Action, Status, PrepUs, ExecUs, StoreUs,
                            Notes, Size, Cap)
                    catch error:badarg -> ok
                    end;
                _ -> ok
            end
    end,
    ok.

%% A `failure' computed and stored nothing, and the same slot is tried again,
%% so it only adds to `error_slots': `slots', `last_slot', the phase counters,
%% the ring and the actions describe slots that were computed.
log(ProcID, _Slot, _Action, failure, _PrepUs, _ExecUs, _StoreUs, _Notes, _Size, Cap) ->
    case count(ProcID, [{3, 1}], Cap) of
        unassigned -> ok;
        _ ->
            ets:update_element(?T, {ProcID, c},
                {?LAST_MS, erlang:system_time(millisecond)})
    end;
log(ProcID, Slot, Action, Status, PrepUs, ExecUs, StoreUs, Notes, Size, Cap) ->
    [LuaPrep, Lua, Decode, Apply, Gc] = [ maps:get(P, Notes, 0) || P <- ?LUA_PHASES ],
    Err = case Status of ok -> 0; _ -> 1 end,
    Incr =
        [{2, 1}, {3, Err}, {4, PrepUs}, {5, ExecUs}, {6, StoreUs}, {7, LuaPrep},
            {8, Lua}, {9, Decode}, {10, Apply}, {11, Gc}, {?SEQ, 1}],
    case count(ProcID, Incr, Cap) of
        unassigned ->
            ets:update_counter(?T, unassigned, [{2, 1}, {3, ExecUs}], {unassigned, 0, 0});
        Counts ->
            Now = erlang:system_time(millisecond),
            ets:update_element(?T, {ProcID, c}, [{?LAST_SLOT, Slot}, {?LAST_MS, Now}]),
            Name = action(Action),
            ets:insert(?T,
                {{ProcID, lists:last(Counts) rem Size}, Slot, Now, Status,
                    PrepUs, ExecUs, StoreUs, LuaPrep, Lua, Decode, Apply, Gc, Name}),
            count_action(ProcID, Name, ExecUs)
    end.

%% Add to `ProcID''s counters, admitting it if there is room.
count(ProcID, Incr, Cap) ->
    try ets:update_counter(?T, {ProcID, c}, Incr)
    catch error:badarg ->
        case admit(ProcID, Cap) of
            true -> ets:update_counter(?T, {ProcID, c}, Incr);
            false -> unassigned
        end
    end.

admit(ProcID, Cap) ->
    case ets:lookup(?T, meta) of
        [{meta, Used, _}] when Used >= Cap -> false;
        _ ->
            case ets:update_counter(?T, meta, {2, 1}, {meta, 0, 0}) =< Cap of
                false ->
                    ets:update_counter(?T, meta, {2, -1}),
                    false;
                true ->
                    Zero = {{ProcID, c}, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    case ets:insert_new(?T, Zero) of
                        true -> ok;
                        false -> ets:update_counter(?T, meta, {2, -1}) % a racer won
                    end,
                    true
            end
    end.

count_action(ProcID, Name, ExecUs) ->
    try ets:update_counter(?T, {ProcID, a, Name}, [{2, 1}, {3, ExecUs}])
    catch error:badarg ->
        Key =
            case ets:update_counter(?T, {ProcID, c}, {?NACTIONS, 1}) =< ?MAX_ACTIONS of
                true -> Name;
                false -> ?OTHER
            end,
        ets:update_counter(?T, {ProcID, a, Key}, [{2, 1}, {3, ExecUs}],
            {{ProcID, a, Key}, 0, 0})
    end.

%% @doc An action as it is stored: printable ASCII only (any other byte is
%% `?'), at most 64 bytes, a fresh binary; `none' for anything else.
action(Action) when is_binary(Action) ->
    << <<(if C >= 16#20, C =< 16#7E -> C; true -> $? end)>>
    || <<C>> <= binary:part(Action, 0, min(64, byte_size(Action))) >>;
action(_) -> <<"none">>.

%% @doc The log as a map: every process with counters (only `pid' when it is
%% given), whatever its last slot, so a client diffing the cumulative counters
%% keeps its baseline across idle gaps. `window' (seconds) bounds only the
%% ring rows used for percentiles. Each process carries its raw counters
%% (`counters', `actions', `last_slot') and ring statistics in ms (`n',
%% `span_s', `ms'). Only counter and action rows are copied out of the table,
%% and ring rows only of processes with a slot inside the window.
report(#{ window := WindowS } = Args) ->
    Key = maps:get(pid, Args, undefined),
    P = case Key of undefined -> '_'; _ -> Key end,
    {_, Size, Cap} = config(),
    Now = erlang:system_time(millisecond),
    From = Now - WindowS * 1000,
    {Used, Evicted} =
        case lookup(meta) of
            [{meta, U, E}] -> {U, E};
            [] -> {0, 0}
        end,
    {UCount, UUs} =
        case lookup(unassigned) of
            [{unassigned, C, Us}] -> {C, Us};
            [] -> {0, 0}
        end,
    Actions =
        lists:foldl(
            fun({{Proc, a, Name}, N, Us}, Acc) ->
                Acc#{ Proc => [#{ <<"action">> => Name, <<"count">> => N,
                    <<"exec_us">> => Us } | maps:get(Proc, Acc, [])] }
            end,
            #{},
            match({{P, a, '_'}, '_', '_'})
        ),
    Procs =
        [ process(Counter, Size, Now, From, WindowS, maps:get(Proc, Actions, []))
        || Counter = {{Proc, c}, _, _, _, _, _, _, _, _, _, _, _, _, _, _} <- counters(P) ],
    #{
        ring => Size,
        slot_log => #{ <<"used">> => Used, <<"cap">> => Cap, <<"evicted">> => Evicted },
        unassigned => #{ <<"count">> => UCount, <<"us">> => UUs },
        processes => Procs
    }.

process({{P, c}, Slots, Err, Prep, Exec, Store, LuaPrep, Lua, Decode, Apply, Gc,
        Seq, _, LastSlot, LastMs}, Size, Now, From, WindowS, Actions) ->
    Ring =
        [ Row || LastMs >= From,
                 Pos <- lists:seq(0, Size - 1),
                 Row <- lookup({P, Pos}),
                 element(3, Row) >= From ],
    N = length(Ring),
    % Once the ring has wrapped, its oldest row inside the window marks the
    % span the rows cover; before that they cover the whole window.
    SpanS =
        case Seq > Size andalso N > 0 of
            true -> min(WindowS, (Now - lists:min([ element(3, R) || R <- Ring ])) / 1000);
            false -> WindowS
        end,
    Col = fun(I) -> [ element(I, R) || R <- Ring ] end,
    NodeUs =
        [ max(0, E - LP - L - D - A - G)
        || {_, _, _, _, _, E, _, LP, L, D, A, G, _} <- Ring ],
    #{
        <<"pid">> => P,
        <<"last_slot">> => LastSlot,
        <<"n">> => N,
        <<"span_s">> => round(SpanS * 10) / 10,
        <<"ms">> => #{
            <<"exec">> => stat(Col(6)),
            <<"lua">> => stat(Col(9)),
            <<"store">> => stat(Col(7)),
            <<"node">> => stat(NodeUs)
        },
        <<"counters">> => #{
            <<"slots">> => Slots, <<"error_slots">> => Err,
            <<"prep_us">> => Prep, <<"exec_us">> => Exec, <<"store_us">> => Store,
            <<"lua_prep_us">> => LuaPrep, <<"lua_us">> => Lua,
            <<"decode_us">> => Decode, <<"apply_us">> => Apply, <<"gc_us">> => Gc
        },
        <<"actions">> =>
            lists:sort(
                fun(#{ <<"count">> := A }, #{ <<"count">> := B }) -> A >= B end,
                Actions
            )
    }.

%% Mean, p50, p95 and max in ms of a list of microsecond samples.
stat([]) -> #{ <<"mean">> => 0, <<"p50">> => 0, <<"p95">> => 0, <<"max">> => 0 };
stat(Us) ->
    Sorted = lists:sort(Us),
    N = length(Sorted),
    Nth = fun(Q) -> lists:nth(max(1, ceil(Q * N)), Sorted) / 1000 end,
    #{
        <<"mean">> => round(lists:sum(Sorted) / N) / 1000,
        <<"p50">> => Nth(0.5),
        <<"p95">> => Nth(0.95),
        <<"max">> => lists:last(Sorted) / 1000
    }.

%% The counter rows of `P' (`'_'' for every process).
counters(P) -> match(setelement(1, erlang:make_tuple(?LAST_MS, '_'), {P, c})).

match(Pattern) ->
    try ets:match_object(?T, Pattern) catch error:badarg -> [] end.

lookup(Key) ->
    try ets:lookup(?T, Key) catch error:badarg -> [] end.

%% @doc Forget every process idle for 15 minutes that has no process worker.
sweep() -> sweep(erlang:system_time(millisecond)).
sweep(NowMs) ->
    try
        Idle =
            [ P || {{P, c}, _, _, _, _, _, _, _, _, _, _, _, _, _, LastMs} <- counters('_'),
                   LastMs < NowMs - ?IDLE_MS,
                   (catch hb_name:lookup({process_worker, P})) =:= undefined ],
        lists:foreach(fun evict/1, Idle)
    catch error:badarg -> ok
    end,
    ok.

evict(P) ->
    ets:select_delete(?T, [{'$1', [{'=:=', {element, 1, {element, 1, '$1'}}, P}], [true]}]),
    ets:update_counter(?T, meta, [{2, -1}, {3, 1}], {meta, 0, 0}).

%% @doc `Fun()''s result for `Key', computed at most once per `TtlMs': a
%% burst of readers shares one report. One row holds the last result, so the
%% memo never holds more than one body whatever keys callers send.
memo(Key, TtlMs, Fun) ->
    Now = erlang:monotonic_time(millisecond),
    case lookup(memo) of
        [{memo, Key, At, Value}] when Now - At < TtlMs -> Value;
        _ ->
            Value = Fun(),
            try ets:insert(?T, {memo, Key, Now, Value}) catch error:badarg -> true end,
            Value
    end.

%% @doc Forget everything. Called by `hb_phase_timer:reset/0'.
reset() ->
    try ets:delete_all_objects(?T) catch error:badarg -> true end,
    erase(?OPEN),
    ok.

%%% Tests

-define(P(I), <<"proc-", (integer_to_binary(I))/binary>>).

setup(Opts) ->
    hb_phase_timer:configure(Opts),
    hb_phase_timer:reset().

teardown() ->
    hb_phase_timer:configure(#{}),
    hb_phase_timer:reset().

with(Opts, Fun) ->
    setup(Opts),
    try Fun() after teardown() end.

slot(P, Slot) -> slot(P, Slot, <<"Act">>, ok).
slot(P, Slot, Action, Status) ->
    {_, _, Notes} = run(fun() -> note(lua_patch_lua, 3), ok end),
    end_slot(P, Slot, Action, Status, 1, 10, 2, Notes).

report() -> report(#{ window => 600 }).

proc(P) ->
    [Proc] = [ X || X = #{ <<"pid">> := Q } <- maps:get(processes, report(#{ window => 600, pid => P })), Q =:= P ],
    Proc.

ring_rows(P) ->
    length([ R || R <- ets:tab2list(?T), tuple_size(R) == 13,
        element(1, element(1, R)) =:= P ]).

ring_bound_and_cap_test() ->
    with(#{ <<"slot-log-processes">> => 2 }, fun() ->
        [ slot(?P(1), S) || S <- lists:seq(1, 1000) ],
        ?assertEqual(128, ring_rows(?P(1))),
        #{ <<"n">> := 128, <<"last_slot">> := 1000,
            <<"counters">> := #{ <<"slots">> := 1000, <<"exec_us">> := 10000 } } = proc(?P(1)),
        slot(?P(2), 1),
        slot(?P(3), 1),
        slot(?P(3), 2),
        #{ slot_log := #{ <<"used">> := 2, <<"cap">> := 2 },
            unassigned := #{ <<"count">> := 2, <<"us">> := 20 },
            processes := Procs } = report(),
        ?assertEqual([?P(1), ?P(2)], lists:sort([ P || #{ <<"pid">> := P } <- Procs ]))
    end).

exactly_once_test() ->
    with(#{}, fun() ->
        {_, ok, Notes} =
            run(fun() ->
                note(lua_patch_lua, 40), note(lua_patch_lua, 2),
                note(lua_patch_decode, 5), ok end),
        ?assertEqual(#{ lua_patch_lua => 42, lua_patch_decode => 5 }, Notes),
        end_slot(?P(1), 1, <<"A">>, ok, 1, 100, 1, Notes),
        {_, ok, Notes2} = run(fun() -> ok end),
        ?assertEqual(#{}, Notes2),
        end_slot(?P(1), 2, <<"A">>, ok, 1, 100, 1, Notes2),
        ?assertEqual(undefined, get(?OPEN)),
        Per = [ {Ph, T, N} || {Ph, P, T, N, _} <- hb_phase_timer:read_processes(), P =:= ?P(1) ],
        ?assert(lists:member({lua_patch_lua, 42, 1}, Per)),
        ?assert(lists:member({lua_patch_decode, 5, 1}, Per)),
        ?assert(lists:member({slot_execution, 200, 2}, Per)),
        ?assertMatch([{lua_patch_lua, 42, 2, _}],
            [ R || R = {lua_patch_lua, _, _, _} <- hb_phase_timer:read() ]),
        #{ <<"counters">> := #{ <<"lua_us">> := 42, <<"decode_us">> := 5 } } = proc(?P(1)),
        % Outside a slot a note is a plain record, as before.
        note(lua_patch_lua, 7),
        ?assertMatch([{lua_patch_lua, 49, 3, _}],
            [ R || R = {lua_patch_lua, _, _, _} <- hb_phase_timer:read() ])
    end).

nesting_and_crash_test() ->
    with(#{}, fun() ->
        {_, Inner, Outer} =
            run(fun() ->
                note(lua_patch_lua, 1),
                {_, ok, In} = run(fun() -> note(lua_patch_lua, 50), ok end),
                note(lua_patch_apply, 2),
                In
            end),
        ?assertEqual(#{ lua_patch_lua => 50 }, Inner),
        ?assertEqual(#{ lua_patch_lua => 1, lua_patch_apply => 2 }, Outer),
        {_, ok, AfterCrash} =
            run(fun() ->
                note(lua_patch_lua, 4),
                catch run(fun() -> note(lua_patch_lua, 9), error(boom) end),
                ok
            end),
        ?assertEqual(#{ lua_patch_lua => 4 }, AfterCrash),
        ?assertEqual(undefined, get(?OPEN))
    end).

labelling_test() ->
    with(#{}, fun() ->
        hb_phase_timer:set_process(<<"worker-label">>),
        slot(?P(7), 1),
        Labels = lists:usort([ P || {slot_execution, P, _, _, _} <- hb_phase_timer:read_processes() ]),
        ?assertEqual([?P(7)], Labels)
    end).

error_and_failure_test() ->
    with(#{}, fun() ->
        slot(?P(1), 1, <<"A">>, ok),
        slot(?P(1), 2, <<"A">>, error),
        % A failure stores nothing and is retried: it is not a computed slot.
        slot(?P(1), 3, <<"A">>, failure),
        slot(?P(1), 3, <<"A">>, failure),
        #{ <<"last_slot">> := 2, <<"n">> := 2,
            <<"counters">> := #{ <<"slots">> := 2, <<"error_slots">> := 3,
                <<"exec_us">> := 20 },
            <<"actions">> := [#{ <<"count">> := 2 }] } = proc(?P(1)),
        Statuses = lists:sort([ element(4, R) || R <- ets:tab2list(?T), tuple_size(R) == 13 ]),
        ?assertEqual([error, ok], Statuses),
        % A process that has only failed has computed nothing.
        slot(?P(2), 9, <<"A">>, failure),
        #{ <<"last_slot">> := 0, <<"n">> := 0,
            <<"counters">> := #{ <<"slots">> := 0, <<"error_slots">> := 1 } } = proc(?P(2))
    end).

idle_processes_keep_their_counters_test() ->
    with(#{}, fun() ->
        slot(?P(1), 1),
        slot(?P(1), 2),
        slot(?P(2), 5),
        % P1's last slot falls outside a 60 s window; P2's does not.
        Old = erlang:system_time(millisecond) - 120000,
        ets:update_element(?T, {?P(1), c}, {?LAST_MS, Old}),
        [ ets:update_element(?T, {?P(1), Pos}, {3, Old}) || Pos <- [1, 2] ],
        #{ processes := Procs } = report(#{ window => 60 }),
        ?assertMatch(
            [#{ <<"pid">> := <<"proc-1">>, <<"last_slot">> := 2, <<"n">> := 0,
                    <<"counters">> := #{ <<"slots">> := 2 },
                    <<"actions">> := [#{ <<"count">> := 2 }] },
                #{ <<"pid">> := <<"proc-2">>, <<"last_slot">> := 5, <<"n">> := 1 }],
            lists:sort(fun(#{ <<"pid">> := A }, #{ <<"pid">> := B }) -> A =< B end, Procs)),
        % A named process is reported alone, with only its own actions.
        ?assertMatch(
            #{ processes := [#{ <<"pid">> := <<"proc-2">>,
                <<"actions">> := [#{ <<"count">> := 1 }] }] },
            report(#{ window => 60, pid => ?P(2) }))
    end).

action_sanitising_test() ->
    Long = binary:copy(<<"ab">>, 40),
    Split = <<(binary:copy(<<"x">>, 63))/binary, 16#E2, 16#82, 16#AC>>,
    ?assertEqual(binary:part(Long, 0, 64), action(Long)),
    ?assertEqual(<<(binary:copy(<<"x">>, 63))/binary, "?">>, action(Split)),
    ?assertEqual(<<"a?b?c">>, action(<<"a", 0, "b", 16#FF, "c">>)),
    ?assertEqual(<<"??">>, action(<<"\n\t">>)),
    ?assertEqual(<<"none">>, action(no_action_set)),
    ?assertEqual(<<"none">>, action(42)),
    with(#{}, fun() ->
        [ slot(?P(1), I, A, ok) || {I, A} <- lists:zip([1, 2, 3], [Long, Split, <<1, 2, 200>>]) ],
        ?assert(is_binary(hb_json:encode(report())))
    end).

actions_cap_test() ->
    with(#{}, fun() ->
        [ slot(?P(1), I, ?P(I), ok) || I <- lists:seq(1, 40) ],
        slot(?P(1), 41, ?P(1), ok),
        #{ <<"actions">> := Actions } = proc(?P(1)),
        ?assertEqual(33, length(Actions)),
        ?assertMatch([#{ <<"count">> := 2 }],
            [ A || A = #{ <<"action">> := <<"proc-1">> } <- Actions ]),
        ?assertMatch([#{ <<"count">> := 8 }],
            [ A || A = #{ <<"action">> := ?OTHER } <- Actions ])
    end).

atom_safety_test() ->
    with(#{}, fun() ->
        slot(?P(1), 0, <<"warm">>, ok),
        _ = hb_json:encode(report()),
        Before = erlang:system_info(atom_count),
        [ slot(?P(I rem 50), I, <<"Act-", (integer_to_binary(I))/binary>>, ok)
        || I <- lists:seq(1, 10000) ],
        _ = hb_json:encode(report()),
        ?assert(erlang:system_info(atom_count) - Before < 10)
    end).

switches_test() ->
    with(#{ <<"slot-log">> => false }, fun() ->
        slot(?P(1), 1),
        ?assertEqual([], ets:tab2list(?T)),
        ?assertMatch([_ | _], hb_phase_timer:read()),
        ?assertMatch([_ | _], hb_phase_timer:read_processes())
    end),
    with(#{ <<"phase-timers">> => false }, fun() ->
        slot(?P(1), 1),
        ?assertEqual([], ets:tab2list(?T)),
        ?assertEqual([], hb_phase_timer:read())
    end).

memo_test() ->
    with(#{}, fun() ->
        Calls = counters:new(1, []),
        F = fun() -> counters:add(Calls, 1, 1), counters:get(Calls, 1) end,
        ?assertEqual(1, memo(a, 60000, F)),
        ?assertEqual(1, memo(a, 60000, F)),   % fresh: served from the memo
        ?assertEqual(2, memo(b, 60000, F)),   % another key replaces the one row
        ?assertEqual(3, memo(a, 60000, F)),
        ?assertEqual(4, memo(a, 0, F)),       % stale at once
        ?assertEqual(1, length(ets:lookup(?T, memo)))
    end).

sweep_and_reset_test() ->
    with(#{}, fun() ->
        slot(?P(1), 1),
        slot(?P(2), 1),
        Now = erlang:system_time(millisecond),
        sweep(Now + ?IDLE_MS - 1000),
        ?assertMatch(#{ slot_log := #{ <<"used">> := 2, <<"evicted">> := 0 } }, report()),
        ets:update_element(?T, {?P(2), c}, {?LAST_MS, Now + ?IDLE_MS}),
        sweep(Now + ?IDLE_MS + 1000),
        #{ slot_log := #{ <<"used">> := 1, <<"evicted">> := 1 } } = report(),
        ?assertEqual([], [ R || R <- ets:tab2list(?T), is_tuple(element(1, R)),
            element(1, element(1, R)) =:= ?P(1) ]),
        slot(?P(1), 2),
        ?assertMatch(#{ <<"counters">> := #{ <<"slots">> := 1 } }, proc(?P(1))),
        hb_phase_timer:reset(),
        ?assertEqual([], ets:tab2list(?T))
    end).

concurrency_test_() ->
    {timeout, 60, fun() ->
        with(#{}, fun() ->
            Parent = self(),
            Pids =
                [ spawn_link(fun() ->
                    [ slot(?P((W * 500 + S) rem 600), S) || S <- lists:seq(1, 500) ],
                    Parent ! {done, self()}
                  end)
                || W <- lists:seq(1, 64) ],
            [ receive {done, P} -> ok end || P <- Pids ],
            #{ slot_log := #{ <<"used">> := Used }, unassigned := #{ <<"count">> := U },
                processes := Procs } = report(),
            ?assertEqual(512, Used),
            ?assertEqual(512, length(Procs)),
            Counted = lists:sum([ S || #{ <<"counters">> := #{ <<"slots">> := S } } <- Procs ]),
            ?assertEqual(64 * 500, Counted + U),
            Rows = length([ R || R <- ets:tab2list(?T), tuple_size(R) == 13 ]),
            ?assert(Rows =< 512 * 128),
            % Rows, counters and action rows, at a few hundred bytes each.
            ?assert(ets:info(?T, memory) * erlang:system_info(wordsize) < 32 * 1024 * 1024)
        end)
    end}.

microbench_test_() ->
    {timeout, 60, fun() ->
        with(#{}, fun() ->
            N = 100000,
            {Us, _} =
                timer:tc(fun() ->
                    [ end_slot(?P(I rem 47), I, <<"Order.Place">>, ok, 100, 5000, 300,
                        #{ lua_patch_lua => 2000, lua_patch_decode => 50 })
                    || I <- lists:seq(1, N) ]
                end),
            PerCall = Us / N,
            ?debugFmt("end_slot: ~.2f us per call over ~p calls", [PerCall, N]),
            ?assert(PerCall < 10)
        end)
    end}.

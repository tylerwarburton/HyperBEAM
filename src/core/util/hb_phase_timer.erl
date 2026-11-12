%%% @doc Cumulative time per named phase of a slot, kept in an ETS table so it
%%% can be read from a running node (`hb eval "hb_phase_timer:read()."').
%%%
%%% Tracing is the wrong tool for this: a traced call's arguments are copied
%%% into each trace message, and the phases of a slot take the whole process
%%% state as an argument, so the measurement becomes the cost. Logging is not
%%% much better, and a process worker does not print its events. Recording is
%%% two `ets:update_counter' calls.
%%%
%%% The node option `phase-timers' (default `true') turns recording on or off;
%%% `configure/1' reads it when the HTTP server starts. The totals are served
%%% on `/~hyperbuddy@1.0/metrics' as `hb_phase_seconds_total{phase}' and
%%% `hb_phase_count_total{phase}' (`hb_metrics_collector').
%%%
%%% A sample is also counted against a process when the recorder names one. A
%%% process worker names its process once (`set_process/1'), so every phase it
%%% records -- slot, store, Lua -- is attributed with no argument passing; a
%%% waiter names it per call (`time/3'). Those totals are served as
%%% `hb_process_phase_seconds_total{phase,process}' and
%%% `hb_process_phase_count_total{phase,process}'. Only the first
%%% `phase-timers-processes' (default 128) processes get their own label;
%%% later ones share `other', so throwaway spawns cannot grow the series.
-module(hb_phase_timer).
-export([configure/1, enabled/0, record/2, record/3, time/2, time/3]).
-export([set_process/1, read/0, read_processes/0, reset/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TABLE, hb_phase_timer).
-define(PROCS, hb_phase_timer_processes).
-define(ENABLED_KEY, {?MODULE, enabled}).
-define(MAX_KEY, {?MODULE, max_processes}).
-define(DEFAULT_MAX, 128).
-define(LABEL_KEY, {?MODULE, process}).
-define(OTHER, <<"other">>).

%% @doc Set recording on or off from the node option `phase-timers', and the
%% number of processes that get their own label from `phase-timers-processes'.
configure(Opts) ->
    On =
        case hb_opts:get(<<"phase-timers">>, true, Opts) of
            false -> false;
            <<"false">> -> false;
            _ -> true
        end,
    % A persistent_term write is a global GC; only make one when it changes.
    case enabled() of
        On -> ok;
        _ -> persistent_term:put(?ENABLED_KEY, On)
    end,
    Max = hb_util:int(hb_opts:get(<<"phase-timers-processes">>, ?DEFAULT_MAX, Opts)),
    case max_processes() of
        Max -> ok;
        _ -> persistent_term:put(?MAX_KEY, Max)
    end,
    ok.

%% @doc Whether recording is on.
enabled() -> persistent_term:get(?ENABLED_KEY, true).

max_processes() -> persistent_term:get(?MAX_KEY, ?DEFAULT_MAX).

%% @doc Name the process every later sample from the calling Erlang process
%% belongs to. A process worker calls this; it is a no-op after the first.
set_process(Process) ->
    case get(?LABEL_KEY) of
        undefined ->
            case enabled() of
                true -> put(?LABEL_KEY, label(Process));
                false -> ok
            end;
        _ -> ok
    end,
    ok.

%% @doc Add `Micros' to the total of `Phase' and count one more sample; also
%% to the calling Erlang process's named process, if it has one.
record(Phase, Micros) when is_integer(Micros) ->
    case enabled() of
        true ->
            add(Phase, Micros),
            case get(?LABEL_KEY) of
                undefined -> ok;
                Label -> add({Phase, Label}, Micros)
            end;
        false -> ok
    end,
    ok.

%% @doc As `record/2', attributed to `Process'.
record(Phase, Process, Micros) when is_integer(Micros) ->
    case enabled() of
        true ->
            add(Phase, Micros),
            add({Phase, label(Process)}, Micros);
        false -> ok
    end,
    ok.

%% @doc Run `Fun', record its wall time under `Phase', and return its result.
time(Phase, Fun) ->
    case enabled() of
        true ->
            {Micros, Res} = timer:tc(Fun),
            record(Phase, Micros),
            Res;
        false -> Fun()
    end.

%% @doc As `time/2', attributed to `Process'.
time(Phase, Process, Fun) ->
    case enabled() of
        true ->
            {Micros, Res} = timer:tc(Fun),
            record(Phase, Process, Micros),
            Res;
        false -> Fun()
    end.

add(Key, Micros) ->
    ensure_table(),
    try ets:update_counter(?TABLE, Key, [{2, Micros}, {3, 1}], {Key, 0, 0})
    catch error:badarg -> ok
    end.

%% A process keeps the label it was given; a new one gets its own while fewer
%% than the cap have one, `other' after. A racing admission can overshoot the
%% counter, which only means one fewer label.
label(Process) ->
    ensure_table(),
    try ets:lookup(?PROCS, Process) of
        [_] -> Process;
        [] ->
            Admitted = ets:update_counter(?PROCS, '$count', 1, {'$count', 0}),
            case Admitted =< max_processes() of
                true ->
                    ets:insert(?PROCS, {Process}),
                    Process;
                false -> ?OTHER
            end
    catch error:badarg -> ?OTHER
    end.

%% @doc Every phase as `{Phase, TotalMicros, Count, MeanMicros}'.
read() ->
    try
        lists:sort(
            [ {P, T, N, T div max(N, 1)}
            || {P, T, N} <- ets:tab2list(?TABLE), is_atom(P) ]
        )
    catch error:badarg -> []
    end.

%% @doc Every phase of every named process as
%% `{Phase, Process, TotalMicros, Count, MeanMicros}'.
read_processes() ->
    try
        lists:sort(
            [ {P, Proc, T, N, T div max(N, 1)}
            || {{P, Proc}, T, N} <- ets:tab2list(?TABLE) ]
        )
    catch error:badarg -> []
    end.

%% @doc Forget every total and every process label.
reset() ->
    try ets:delete_all_objects(?TABLE) catch error:badarg -> true end,
    try ets:delete_all_objects(?PROCS) catch error:badarg -> true end,
    erase(?LABEL_KEY),
    ok.

%% The tables are owned by a process that never exits: a table dies with its
%% owner, and the first recorder is usually a short-lived request.
ensure_table() ->
    case ets:whereis(?TABLE) of
        undefined ->
            Parent = self(),
            Ref = make_ref(),
            {Owner, Mon} =
                spawn_monitor(
                    fun() ->
                        try ets:new(?PROCS, [named_table, public, set,
                                {read_concurrency, true}]) of
                            _ ->
                                ets:new(?TABLE, [named_table, public, set,
                                    {write_concurrency, true}]),
                                Parent ! {Ref, created},
                                receive after infinity -> ok end
                        catch error:badarg -> Parent ! {Ref, exists}
                        end
                    end
                ),
            receive
                {Ref, _} -> ok;
                {'DOWN', Mon, process, Owner, _} -> ok
            after 5000 -> ok
            end,
            erlang:demonitor(Mon, [flush]),
            ok;
        _ -> ok
    end.

record_and_read_test() ->
    reset(),
    record(test_phase, 10),
    record(test_phase, 30),
    ?assertMatch([{test_phase, 40, 2, 20} | _],
        [ R || R = {test_phase, _, _, _} <- read() ]).

time_returns_result_and_counts_test() ->
    reset(),
    ?assertEqual(42, time(timed_phase, fun() -> 42 end)),
    ?assertMatch([{timed_phase, _, 1, _}],
        [ R || R = {timed_phase, _, _, _} <- read() ]).

worker_label_attributes_every_sample_test() ->
    reset(),
    set_process(<<"proc-a">>),
    record(slot_execution, 10),
    record(slot_execution, 20),
    set_process(<<"proc-b">>), % ignored: a worker names its process once
    record(slot_store, 5),
    erase(?LABEL_KEY),
    record(slot_store, 7),
    ?assertEqual([{slot_execution, 30, 2, 15}, {slot_store, 12, 2, 6}], read()),
    ?assertEqual(
        [{slot_execution, <<"proc-a">>, 30, 2, 15},
            {slot_store, <<"proc-a">>, 5, 1, 5}],
        read_processes()).

explicit_process_test() ->
    reset(),
    ?assertEqual(ok, time(worker_await, <<"proc-c">>, fun() -> ok end)),
    ?assertMatch([{worker_await, <<"proc-c">>, _, 1, _}], read_processes()),
    ?assertMatch([{worker_await, _, 1, _}], read()).

processes_over_the_cap_share_other_test() ->
    reset(),
    try
        configure(#{ <<"phase-timers-processes">> => 2 }),
        [ record(p, <<"proc-", (integer_to_binary(I))/binary>>, 1)
        || I <- lists:seq(1, 5) ],
        record(p, <<"proc-1">>, 1),
        ?assertEqual(
            [{p, <<"other">>, 3, 3, 1},
                {p, <<"proc-1">>, 2, 2, 1},
                {p, <<"proc-2">>, 1, 1, 1}],
            read_processes())
    after
        configure(#{})
    end.

disabled_records_nothing_test() ->
    reset(),
    try
        configure(#{ <<"phase-timers">> => false }),
        ?assertEqual(ok, time(off_phase, fun() -> ok end)),
        record(off_phase, 5),
        record(off_phase, <<"proc">>, 5),
        set_process(<<"proc">>),
        record(off_phase, 5),
        ?assertEqual([], read()),
        ?assertEqual([], read_processes())
    after
        configure(#{ <<"phase-timers">> => true })
    end.

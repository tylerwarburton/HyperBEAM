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
-module(hb_phase_timer).
-export([configure/1, enabled/0, record/2, time/2, read/0, reset/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TABLE, hb_phase_timer).
-define(ENABLED_KEY, {?MODULE, enabled}).

%% @doc Set recording on or off from the node option `phase-timers'.
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
    ok.

%% @doc Whether recording is on.
enabled() -> persistent_term:get(?ENABLED_KEY, true).

%% @doc Add `Micros' to the total of `Phase' and count one more sample.
record(Phase, Micros) when is_integer(Micros) ->
    case enabled() of
        true ->
            ensure_table(),
            try ets:update_counter(?TABLE, Phase, [{2, Micros}, {3, 1}], {Phase, 0, 0})
            catch error:badarg -> ok
            end;
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

%% @doc Every phase as `{Phase, TotalMicros, Count, MeanMicros}'.
read() ->
    try
        lists:sort(
            [ {P, T, N, T div max(N, 1)} || {P, T, N} <- ets:tab2list(?TABLE) ]
        )
    catch error:badarg -> []
    end.

%% @doc Forget every total.
reset() ->
    try ets:delete_all_objects(?TABLE) catch error:badarg -> true end,
    ok.

%% The table is owned by a process that never exits: a table dies with its
%% owner, and the first recorder is usually a short-lived request.
ensure_table() ->
    case ets:whereis(?TABLE) of
        undefined ->
            Parent = self(),
            Ref = make_ref(),
            {Owner, Mon} =
                spawn_monitor(
                    fun() ->
                        try ets:new(?TABLE, [named_table, public, set,
                                {write_concurrency, true}]) of
                            _ ->
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

disabled_records_nothing_test() ->
    reset(),
    try
        configure(#{ <<"phase-timers">> => false }),
        ?assertEqual(ok, time(off_phase, fun() -> ok end)),
        record(off_phase, 5),
        ?assertEqual([], [ R || R = {off_phase, _, _, _} <- read() ])
    after
        configure(#{ <<"phase-timers">> => true })
    end.

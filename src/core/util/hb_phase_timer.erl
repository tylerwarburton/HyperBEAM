%%% @doc Cumulative time per named phase of a slot, kept in an ETS table so it
%%% can be read from a running node (`hb eval "hb_phase_timer:read()."').
%%%
%%% Tracing is the wrong tool for this: a traced call's arguments are copied
%%% into each trace message, and the phases of a slot take the whole process
%%% state as an argument, so the measurement becomes the cost. Logging is not
%%% much better, and a process worker does not print its events. Recording is
%%% two `ets:update_counter' calls.
-module(hb_phase_timer).
-export([record/2, read/0, reset/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TABLE, hb_phase_timer).

%% @doc Add `Micros' to the total of `Phase' and count one more sample.
record(Phase, Micros) when is_integer(Micros) ->
    ensure_table(),
    try ets:update_counter(?TABLE, Phase, [{2, Micros}, {3, 1}], {Phase, 0, 0})
    catch error:badarg -> ok
    end,
    ok.

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

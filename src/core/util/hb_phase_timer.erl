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
%%%
%%% A computed slot is recorded by `hb_slot_log:end_slot/8', against the
%%% process it computes rather than the caller's label, and the same owner
%%% process holds that module's table (`slot-log' options, `configure/1').
-module(hb_phase_timer).
-export([configure/1, enabled/0, record/2, record/3, time/2, time/3]).
-export([set_process/1, read/0, read_processes/0, reset/0]).
-export([label/1, record_node/2, record_label/3]).
-export([read_hist/0, percentiles/2, bucket/1]).
-include_lib("eunit/include/eunit.hrl").

-define(TABLE, hb_phase_timer).
-define(PROCS, hb_phase_timer_processes).
-define(ENABLED_KEY, {?MODULE, enabled}).
-define(MAX_KEY, {?MODULE, max_processes}).
-define(DEFAULT_MAX, 128).
-define(LABEL_KEY, {?MODULE, process}).
-define(OTHER, <<"other">>).
%% Node-wide rows also carry a log2 histogram of their samples: bucket `B'
%% counts samples of `2^B' to `2^(B+1)-1' microseconds (bucket 0 also holds
%% 0), the last one everything from `2^(?BUCKETS-1)' up (~33 s).
-define(BUCKETS, 26).

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
    hb_slot_log:configure(Opts),
    hb_msg_trace:configure(Opts).

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

%% @doc Add to the node-wide total of `Phase' only.
record_node(Phase, Micros) when is_integer(Micros) ->
    case enabled() of
        true -> add(Phase, Micros);
        false -> ok
    end,
    ok.

%% @doc Add to `Label''s total of `Phase' only; `Label' comes from `label/1'.
record_label(Phase, Label, Micros) when is_integer(Micros) ->
    case enabled() of
        true -> add({Phase, Label}, Micros);
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

%% A node-wide row is `{Key, Total, Count, Bucket0, ...}', updated by one
%% `ets:update_counter' (its histogram bucket is a third increment); a
%% process's row `{{Phase, Label}, Total, Count}' has no histogram.
add(Key = {_, _}, Micros) ->
    ensure_table(),
    try ets:update_counter(?TABLE, Key, [{2, Micros}, {3, 1}], {Key, 0, 0})
    catch error:badarg -> ok
    end;
add(Key, Micros) ->
    ensure_table(),
    try
        ets:update_counter(?TABLE, Key,
            [{2, Micros}, {3, 1}, {4 + bucket(Micros), 1}],
            setelement(1, erlang:make_tuple(3 + ?BUCKETS, 0), Key))
    catch error:badarg -> ok
    end.

%% @doc The histogram bucket of a sample: the log2 of its microseconds, at
%% most `?BUCKETS - 1'.
bucket(Micros) -> bucket(Micros bsr 1, 0).
bucket(M, B) when M =< 0; B >= ?BUCKETS - 1 -> B;
bucket(M, B) -> bucket(M bsr 1, B + 1).

%% A process keeps the label it was given; a new one gets its own while fewer
%% than the cap have one, `other' after. A racing admission can overshoot the
%% counter, which only means one fewer label. Once the cap is reached a miss
%% is a read, never a write: every slot of an unlabelled process asks.
%% @doc The label samples for `Process' are counted under.
label(Process) ->
    ensure_table(),
    Max = max_processes(),
    try ets:lookup(?PROCS, Process) of
        [_] -> Process;
        [] ->
            Full =
                case ets:lookup(?PROCS, '$count') of
                    [{_, Count}] -> Count >= Max;
                    [] -> false
                end,
            case Full orelse ets:update_counter(?PROCS, '$count', 1, {'$count', 0}) > Max of
                false ->
                    ets:insert(?PROCS, {Process}),
                    Process;
                true -> ?OTHER
            end
    catch error:badarg -> ?OTHER
    end.

%% @doc Every phase as `{Phase, TotalMicros, Count, MeanMicros}'. A phase of
%% an HTTP request (`{http, Class, Phase}') is named `<<"http_Class_Phase">>'.
read() ->
    [ {P, T, N, T div max(N, 1)} || {P, T, N, _} <- read_hist() ].

%% @doc Every node-wide phase as `{Phase, TotalMicros, Count, Buckets}', with
%% `Buckets' the histogram's counts, lowest first (`bucket/1').
read_hist() ->
    try
        lists:sort(
            [ {name(element(1, Row)), element(2, Row), element(3, Row),
                    lists:nthtail(3, tuple_to_list(Row))}
            || Row <- ets:tab2list(?TABLE), tuple_size(Row) =:= 3 + ?BUCKETS ]
        )
    catch error:badarg -> []
    end.

name({http, Class, Phase}) ->
    <<"http_", (atom_to_binary(Class))/binary, "_", (atom_to_binary(Phase))/binary>>;
name(P) -> P.

%% @doc The upper bound in microseconds of the bucket holding each quantile
%% in `Qs' (0-1) of a histogram: exact to a factor of two.
percentiles(Buckets, Qs) ->
    N = lists:sum(Buckets),
    [ case N of
          0 -> 0;
          _ -> upper(Buckets, max(1, ceil(Q * N)), 0, 0)
      end
    || Q <- Qs ].

upper([C | Rest], Rank, B, Seen) when Seen + C < Rank ->
    upper(Rest, Rank, B + 1, Seen + C);
upper(_, _, B, _) -> 1 bsl (B + 1).

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
    hb_slot_log:reset(),
    hb_msg_trace:reset().

%% The tables are owned by a process that never exits: a table dies with its
%% owner, and the first recorder is usually a short-lived request. The slot
%% log's table is created first, so finding this module's tables means it
%% exists too; the owner that created it sweeps it every minute. The message
%% trace's tables (`hb_msg_trace') are created by the same owner.
ensure_table() ->
    case ets:whereis(?TABLE) =/= undefined andalso
            ets:whereis(hb_slot_log) =/= undefined andalso
            ets:whereis(hb_msg_trace) =/= undefined of
        false ->
            Parent = self(),
            Ref = make_ref(),
            {Owner, Mon} =
                spawn_monitor(
                    fun() ->
                        SlotLog = create(fun hb_slot_log:new_table/0),
                        Trace = create(fun hb_msg_trace:new_tables/0),
                        Timers =
                            create(fun() ->
                                ets:new(?PROCS, [named_table, public, set,
                                    {read_concurrency, true}]),
                                ets:new(?TABLE, [named_table, public, set,
                                    {write_concurrency, true}])
                            end),
                        Parent ! {Ref, done},
                        case SlotLog orelse Timers orelse Trace of
                            false -> ok;
                            true -> sweep_loop()
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
        true -> ok
    end.

create(Fun) ->
    try Fun(), true
    catch error:badarg -> false
    end.

sweep_loop() ->
    receive after 60000 -> ok end,
    catch hb_slot_log:sweep(),
    catch hb_msg_trace:sweep(),
    sweep_loop().

record_and_read_test() ->
    reset(),
    record(test_phase, 10),
    record(test_phase, 30),
    ?assertMatch([{test_phase, 40, 2, 20} | _],
        [ R || R = {test_phase, _, _, _} <- read() ]).

histogram_test() ->
    reset(),
    [ record(hist_phase, U) || U <- [0, 1, 3, 3, 100, 1000] ],
    [{hist_phase, 1107, 6, Buckets}] = [ R || R = {hist_phase, _, _, _} <- read_hist() ],
    ?assertEqual(?BUCKETS, length(Buckets)),
    ?assertEqual([2, 2, 0, 0, 0, 0, 1, 0, 0, 1], lists:sublist(Buckets, 10)),
    ?assertEqual([4, 128, 1024], percentiles(Buckets, [0.5, 0.8, 1.0])),
    ?assertEqual(?BUCKETS - 1, bucket(1 bsl 40)),
    record_node({http, now, resolve}, 10),
    ?assertMatch([{<<"http_now_resolve">>, 10, 1, 10}],
        [ R || R = {<<"http_now_resolve">>, _, _, _} <- read() ]).

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
            read_processes()),
        % Past the cap a miss is a read: the admission counter stays put.
        [ label(<<"late-", (integer_to_binary(I))/binary>>) || I <- lists:seq(1, 100) ],
        ?assertEqual([{'$count', 2}], ets:lookup(?PROCS, '$count'))
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

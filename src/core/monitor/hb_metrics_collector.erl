-module(hb_metrics_collector).

-export(
    [
        deregister_cleanup/1,
        collect_mf/2,
        collect_metrics/2
    ]
).
-behaviour(prometheus_collector).
-include("include/hb_http_client.hrl").
-include_lib("eunit/include/eunit.hrl").
%%====================================================================
%% Collector API
%%====================================================================
deregister_cleanup(_) -> ok.

collect_mf(_Registry, Callback) ->
    {Uptime, _} = erlang:statistics(wall_clock),
    Callback(
        create_gauge(
            process_uptime_seconds,
            "The number of seconds the Erlang process has been up.",
            Uptime
        )
    ),

    SystemLoad = safe_avg5(),

    Callback(
        create_gauge(
            system_load,
            "The load values are proportional to how long"
            " time a runnable Unix process has to spend in the run queue"
            " before it is scheduled. Accordingly, higher values mean"
            " more system load",
            SystemLoad
        )
    ),

    {InUse, Free, Queue} = hackney_pool_stats(),
    Callback(
        create_gauge(
            hackney_pool_in_use,
            "Hackney connections currently in use",
            InUse
        )
    ),
    Callback(
        create_gauge(
            hackney_pool_free,
            "Idle hackney connections available in the pool",
            Free
        )
    ),
    Callback(
        create_gauge(
            hackney_pool_queue,
            "Requests waiting for a hackney connection",
            Queue
        )
    ),

    Phases = hb_phase_timer:read(),
    Callback(
        prometheus_model_helpers:create_mf(
            hb_phase_seconds_total,
            "Wall time spent in each named phase of the message path "
            "(hb_phase_timer; node option phase-timers).",
            counter,
            ?MODULE,
            Phases
        )
    ),
    Callback(
        prometheus_model_helpers:create_mf(
            hb_phase_count_total,
            "Number of timed samples of each named phase (hb_phase_timer).",
            counter,
            ?MODULE,
            Phases
        )
    ),
    % The node's delivery backlog: sums and maxima over every process, never
    % a series per process (`lib_process_delivery:metrics/0').
    lists:foreach(
        fun({Name, Help, Value}) -> Callback(create_gauge(Name, Help, Value)) end,
        try
            {ok, Gauges} =
                hb_ao:raw(<<"process@1.0">>, #{}, #{ <<"path">> => <<"deliveries">> },
                    #{ <<"process-delivery-metrics">> => true }),
            Gauges
        catch _:_ -> []
        end
    ),
    ProcessPhases = hb_phase_timer:read_processes(),
    Callback(
        prometheus_model_helpers:create_mf(
            hb_process_phase_seconds_total,
            "hb_phase_seconds_total per process (phase-timers-processes "
            "processes get a label; the rest share `other').",
            counter,
            ?MODULE,
            ProcessPhases
        )
    ),
    Callback(
        prometheus_model_helpers:create_mf(
            hb_process_phase_count_total,
            "hb_phase_count_total per process.",
            counter,
            ?MODULE,
            ProcessPhases
        )
    ),

    ok.
collect_metrics(hb_phase_seconds_total, Phases) ->
    prometheus_model_helpers:counter_metrics(
        [ {[{phase, P}], T / 1_000_000} || {P, T, _N, _Mean} <- Phases ]
    );
collect_metrics(hb_phase_count_total, Phases) ->
    prometheus_model_helpers:counter_metrics(
        [ {[{phase, P}], N} || {P, _T, N, _Mean} <- Phases ]
    );
collect_metrics(hb_process_phase_seconds_total, Phases) ->
    prometheus_model_helpers:counter_metrics(
        [ {[{phase, P}, {process, Proc}], T / 1_000_000}
        || {P, Proc, T, _N, _Mean} <- Phases ]
    );
collect_metrics(hb_process_phase_count_total, Phases) ->
    prometheus_model_helpers:counter_metrics(
        [ {[{phase, P}, {process, Proc}], N} || {P, Proc, _T, N, _Mean} <- Phases ]
    );
collect_metrics(system_load, SystemLoad) ->
    %% Return the gauge metric with no labels
    prometheus_model_helpers:gauge_metrics(
        [
            {[], SystemLoad}
        ]
    );
collect_metrics(process_uptime_seconds, Uptime) ->
    UptimeSeconds = Uptime / 1000,
    prometheus_model_helpers:gauge_metrics([{[], UptimeSeconds}]);
collect_metrics(hackney_pool_in_use, Value) ->
    prometheus_model_helpers:gauge_metrics([{[], Value}]);
collect_metrics(hackney_pool_free, Value) ->
    prometheus_model_helpers:gauge_metrics([{[], Value}]);
collect_metrics(hackney_pool_queue, Value) ->
    prometheus_model_helpers:gauge_metrics([{[], Value}]);
collect_metrics(_Gauge, Value) when is_number(Value) ->
    prometheus_model_helpers:gauge_metrics([{[], Value}]).

%%====================================================================
%% Private Functions
%%====================================================================

%% @doc Wrapper around cpu_sup:avg5/0 with a 2-second timeout.
%% cpu_sup:avg5/0 uses an infinity timeout to os_mon internally;
%% if the port program stalls, it blocks the Prometheus scrape indefinitely.
%% On timeout, the worker is killed to avoid leaking blocked processes.
safe_avg5() ->
    Ref = make_ref(),
    Self = self(),
    {Pid, MonRef} = spawn_monitor(fun() -> Self ! {Ref, catch cpu_sup:avg5()} end),
    receive
        {Ref, Load} when is_integer(Load) ->
            erlang:demonitor(MonRef, [flush]),
            Load;
        {Ref, _} ->
            erlang:demonitor(MonRef, [flush]),
            0;
        {'DOWN', MonRef, process, Pid, _} ->
            0
    after 2000 ->
        exit(Pid, kill),
        erlang:demonitor(MonRef, [flush]),
        receive {Ref, _} -> ok after 0 -> ok end,
        0
    end.

%% @doc Read the stats of every peer's hackney pool at scrape time, summed.
hackney_pool_stats() ->
    lists:foldl(
        fun(Pool, {InUse, Free, Queue}) ->
            try hackney_pool:get_stats(Pool) of
                Stats ->
                    {InUse + proplists:get_value(in_use_count, Stats, 0),
                     Free + proplists:get_value(free_count, Stats, 0),
                     Queue + proplists:get_value(queue_count, Stats, 0)}
            catch _:_ -> {InUse, Free, Queue}
            end
        end,
        {0, 0, 0},
        [
            Pool
        ||
            {Pool = {?HACKNEY_POOL, _, _, _}, _, _, _} <-
                supervisor:which_children(hackney_sup)
        ]
    ).

create_gauge(Name, Help, Data) ->
    prometheus_model_helpers:create_mf(Name, Help, gauge, ?MODULE, Data).
%%====================================================================
%% Tests
%%====================================================================

process_phases_render_with_process_label_test() ->
    application:ensure_all_started(prometheus),
    hb_phase_timer:reset(),
    hb_phase_timer:record(slot_execution, <<"proc-a">>, 1_500_000),
    Registry = hb_metrics_collector_test,
    prometheus_registry:register_collector(Registry, ?MODULE),
    try
        Text = prometheus_text_format:format(Registry),
        ?assertNotEqual(nomatch, binary:match(Text,
            <<"hb_process_phase_seconds_total{phase=\"slot_execution\",process=\"proc-a\"} 1.5">>)),
        ?assertNotEqual(nomatch, binary:match(Text,
            <<"hb_process_phase_count_total{phase=\"slot_execution\",process=\"proc-a\"} 1">>)),
        ?assertNotEqual(nomatch, binary:match(Text,
            <<"hb_phase_seconds_total{phase=\"slot_execution\"} 1.5">>))
    after
        prometheus_registry:clear(Registry)
    end.

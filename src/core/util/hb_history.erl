%%% @doc Rebuilds of old slots, off the request path.
%%%
%%% A slot whose state retention deleted is rebuilt by replay from the
%%% nearest snapshot below it (`dev_process:history/6'). That can take
%%% minutes, so it never runs in the request: the first request starts a job
%%% and is answered at once with 202 and the job's progress and ETA, and
%%% later requests are answered from it.
%%%
%%%   - One job per process and target slot. A request for a slot a running
%%%     job of the same process will pass through (between the snapshot it
%%%     started from and its target) joins that job instead of starting one:
%%%     the job keeps that slot's state when it gets there.
%%%   - At most `process-history-jobs' (default 2) jobs run per node, each at
%%%     low priority; past that a request is answered 503 with `retry-after'.
%%%   - Every rebuilt state is kept in memory for `process-history-ttl-ms'
%%%     (default 30 minutes), at most `process-history-cache' (default 32)
%%%     of them, oldest dropped first, so a follow-up read is instant.
%%%
%%% `status/0' lists the jobs (slots done, slots a second, ETA) and the cache;
%%% the gauges `hb_history_jobs' and `hb_history_cached' and the counter
%%% `hb_history_replayed_slots_total' follow them.
-module(hb_history).
-export([request/4, note_slot/4, started/3, status/0]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(JOBS, hb_history_jobs).
-define(CACHE, hb_history_cache).
-define(OWNER, hb_history_owner).
-define(DEFAULT_JOBS, 2).
-define(DEFAULT_TTL_MS, 1800000).
-define(DEFAULT_CACHE, 32).

%% @doc The state of slot `Slot' of `P': `{ok, State}' from the cache, or
%% else `{pending, Info}' (a job is rebuilding it: started now, or joined),
%% `{busy, Info}' (the node runs as many jobs as it may) or the job's
%% `{error, _}' when it failed. `Run(JobKey)' rebuilds the slot; it must
%% pass `JobKey' as the `history-job' option of the compute.
request(P, Slot, Run, Opts) ->
    ensure_tables(),
    case cached(P, Slot) of
        {ok, Result} -> Result;
        miss ->
            case joinable(P, Slot) of
                {ok, Job} -> {pending, info(Job)};
                none -> start(P, Slot, Run, Opts)
            end
    end.

cached(P, Slot) ->
    Now = now_ms(),
    case ets:lookup(?CACHE, {P, Slot}) of
        [{_, Result, Expires, _}] when Expires > Now -> {ok, Result};
        [_] -> ets:delete(?CACHE, {P, Slot}), miss;
        [] -> miss
    end.

%% A running job of `P' that will pass `Slot': it is added to the job's
%% wanted slots, so its state is kept when the job gets there.
joinable(P, Slot) ->
    Jobs = [ J || J = #{ process := JP, target := T, from := F } <- jobs(),
                  JP =:= P, Slot =< T, (F =:= unknown orelse Slot > F) ],
    case Jobs of
        [Job = #{ key := Key } | _] ->
            ets:insert(?JOBS, {{wanted, Key, Slot}, true}),
            {ok, Job};
        [] -> none
    end.

start(P, Slot, Run, Opts) ->
    Max = hb_util:int(hb_opts:get(<<"process-history-jobs">>, ?DEFAULT_JOBS, Opts)),
    case length(jobs()) >= Max of
        true -> {busy, #{ running => length(jobs()), max => Max }};
        false ->
            Key = {P, Slot, make_ref()},
            Job = #{ key => Key, process => P, target => Slot, from => unknown,
                     done => 0, started => now_ms() },
            ets:insert(?JOBS, {{job, Key}, Job}),
            ets:insert(?JOBS, {{wanted, Key, Slot}, true}),
            Settings = settings(Opts),
            _ = spawn(fun() ->
                process_flag(priority, low),
                Result =
                    try Run(Key)
                    catch C:E -> {error, #{ <<"status">> => 500, <<"body">> =>
                        iolist_to_binary(io_lib:format("Rebuild failed: ~p", [{C, E}])) }}
                    end,
                % Only a success is kept; a failure is answered once, to
                % the next request, and then retried.
                put_cache(P, Slot, Result,
                    case Result of {ok, _} -> maps:get(ttl, Settings); _ -> 5000 end, Settings),
                ets:match_delete(?JOBS, {{wanted, Key, '_'}, '_'}),
                ets:delete(?JOBS, {job, Key}),
                gauges()
            end),
            gauges(),
            {pending, info(Job)}
    end.

settings(Opts) ->
    #{ ttl => hb_util:int(hb_opts:get(<<"process-history-ttl-ms">>, ?DEFAULT_TTL_MS, Opts)),
       max => hb_util:int(hb_opts:get(<<"process-history-cache">>, ?DEFAULT_CACHE, Opts)) }.

put_cache(P, Slot, Result, TTL, #{ max := Max }) ->
    Now = now_ms(),
    ets:insert(?CACHE, {{P, Slot}, Result, Now + TTL, Now}),
    case ets:info(?CACHE, size) > Max of
        true ->
            Oldest = lists:sort([ {At, K} || {K, _, _, At} <- ets:tab2list(?CACHE) ]),
            [ ets:delete(?CACHE, K) || {_, K} <- lists:sublist(Oldest, ets:info(?CACHE, size) - Max) ],
            ok;
        false -> ok
    end.

%% @doc The job rebuilding with `Opts' (its `history-job') starts from
%% `From': called once the snapshot it replays from is loaded.
started(Opts, From, _Target) ->
    case hb_opts:get(<<"history-job">>, undefined, Opts) of
        undefined -> ok;
        Key ->
            case ets:lookup(?JOBS, {job, Key}) of
                [{_, Job}] -> ets:insert(?JOBS, {{job, Key}, Job#{ from => From }}), ok;
                [] -> ok
            end
    end.

%% @doc A slot a job computed: counted, and kept if a request wanted it.
note_slot(Opts, P, Slot, State) ->
    case hb_opts:get(<<"history-job">>, undefined, Opts) of
        undefined -> ok;
        Key ->
            catch prometheus_counter:inc(hb_history_replayed_slots_total),
            case ets:lookup(?JOBS, {job, Key}) of
                [{_, Job = #{ done := D }}] ->
                    ets:insert(?JOBS, {{job, Key}, Job#{ done => D + 1 }});
                [] -> ok
            end,
            case ets:member(?JOBS, {wanted, Key, Slot}) of
                true -> put_cache(P, Slot, {ok, State}, ?DEFAULT_TTL_MS, #{ max => ?DEFAULT_CACHE });
                false -> ok
            end,
            ok
    end.

%% @doc The jobs running, with their progress, and the cache.
status() ->
    ensure_tables(),
    #{ jobs => [ info(J) || J <- jobs() ],
       cached => [ #{ process => P, slot => S, expires_in_ms => E - now_ms() }
                 || {{P, S}, _, E, _} <- ets:tab2list(?CACHE) ] }.

jobs() -> [ J || {{job, _}, J} <- ets:tab2list(?JOBS) ].

%% A job's progress: slots done of the slots to replay, the rate, the ETA.
info(#{ process := P, target := T, from := F, done := D, started := S }) ->
    Secs = max(1, now_ms() - S) / 1000,
    Rate = D / Secs,
    Total = case F of unknown -> unknown; _ -> T - F end,
    ETA =
        case {Total, Rate > 0} of
            {unknown, _} -> unknown;
            {_, false} -> unknown;
            {_, true} -> round(max(0, Total - D) / Rate)
        end,
    #{ process => P, slot => T, from => F, done => D, total => Total,
       slots_per_second => round(Rate * 10) / 10, eta_seconds => ETA }.

gauges() ->
    catch prometheus_gauge:set(hb_history_jobs, length(jobs())),
    catch prometheus_gauge:set(hb_history_cached, ets:info(?CACHE, size)),
    ok.

%% The tables belong to a process that never exits.
ensure_tables() ->
    case ets:whereis(?JOBS) of
        undefined ->
            Parent = self(),
            Ref = make_ref(),
            spawn(fun() ->
                try
                    ets:new(?JOBS, [named_table, public, set]),
                    ets:new(?CACHE, [named_table, public, set]),
                    catch hb_prometheus:declare(gauge, [{name, hb_history_jobs}, {help, "Old-slot rebuilds running"}]),
                    catch hb_prometheus:declare(gauge, [{name, hb_history_cached}, {help, "Old-slot states held"}]),
                    catch hb_prometheus:declare(counter, [{name, hb_history_replayed_slots_total},
                        {help, "Slots replayed by old-slot rebuilds"}]),
                    Parent ! {Ref, ok},
                    receive after infinity -> ok end
                catch error:badarg -> Parent ! {Ref, ok}
                end
            end),
            receive {Ref, ok} -> ok after 5000 -> ok end;
        _ -> ok
    end.

now_ms() -> erlang:monotonic_time(millisecond).

%%% Tests

%% @doc One job per slot: the first request starts it, repeats join it, the
%% result is cached, and the node runs at most `process-history-jobs'.
coalesce_and_cache_test() ->
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Self = self(),
    Run = fun(Key) ->
        Self ! {ran, Key, self()},
        Opts = #{ <<"history-job">> => Key },
        started(Opts, 0, 10),
        receive go -> ok end,
        [ note_slot(Opts, P, S, {state, S}) || S <- lists:seq(1, 10) ],
        {ok, {state, 10}}
    end,
    Opts = #{ <<"process-history-jobs">> => 1 },
    {pending, #{ slot := 10 }} = request(P, 10, Run, Opts),
    {pending, _} = request(P, 10, Run, Opts),
    % A slot the job passes joins it; another process waits its turn.
    {pending, _} = request(P, 5, Run, Opts),
    ?assertMatch({busy, _}, request(hb_util:human_id(crypto:strong_rand_bytes(32)), 1, Run, Opts)),
    JobPid = receive {ran, _, Pid} -> Pid after 1000 -> error(not_started) end,
    receive {ran, _, _} -> error(second_job) after 200 -> ok end,
    [#{ done := 0, total := 10 }] = maps:get(jobs, status()),
    % Release the job; the target and the slot that joined are both cached.
    JobPid ! go,
    ?assert(lists:any(fun(_) -> timer:sleep(20), cached(P, 10) =/= miss end, lists:seq(1, 100))),
    ?assertEqual({ok, {state, 10}}, request(P, 10, Run, Opts)),
    ?assertEqual({ok, {state, 5}}, request(P, 5, Run, Opts)),
    ?assertEqual([], maps:get(jobs, status())).

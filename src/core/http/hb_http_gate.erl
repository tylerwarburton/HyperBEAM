%%% @doc One gate per remote host and port, shared by every outbound request of the
%%% node. It keeps two promises:
%%%
%%% 1. A host that answered `429' is not asked again, by anyone on this node,
%%%    before its `retry-after' has passed. Until then a request to it is
%%%    answered here with a `429' of our own (header `hb-gate: local'), at
%%%    once and without touching the network, so every caller's existing
%%%    `429' handling applies unchanged and a gateway fallback list moves on
%%%    to its next host immediately. A rate limiter that counts refused
%%%    requests too (arweave.net's does, down to its floor) is otherwise kept
%%%    shut by the callers that each wait out only their own refusal.
%%% 2. A host listed in `http-gate-rates' (`#{Host => RequestsPerSecond}')
%%%    receives requests no faster than that, from all callers together. A
%%%    caller waits for its turn; a caller whose turn is further away than
%%%    `http-gate-max-wait-ms' is refused locally instead, as in (1).
%%%
%%% A `429' with no usable `retry-after' cools the host for
%%% `http-gate-default-cooldown-s'; any cooldown is capped at
%%% `http-gate-max-cooldown-s'. The state is one public ETS table, so the gate
%%% costs a lookup per request and serialises nobody.
-module(hb_http_gate).
-export([check/2, observe/3, refusal/1, httpc_request/5, host/1, reset/0]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_COOLDOWN_S, 30).
-define(MAX_COOLDOWN_S, 900).
-define(DEFAULT_MAX_WAIT_MS, 5000).

%% @doc Whether a request to `Peer' may go out now. Waits for the host's pace
%% if it has one. Returns `ok', or `{refuse, RetryAfterSeconds}'.
check(Peer, Opts) ->
    Key = authority(Peer),
    Now = now_us(),
    case ets:lookup(table(), {cool, Key}) of
        [{_, Until}] when Until > Now -> {refuse, ceil_s(Until - Now)};
        _ -> pace(Key, Now, Opts)
    end.

%% @doc Record a response from `Peer': a `429' cools the host for as long as it
%% asked. Accepts the `hb_http_client' and the `httpc' response shapes.
observe(Peer, Response, Opts) ->
    case status_headers(Response) of
        {429, Headers} ->
            Secs = min(
                max(1, retry_after(Headers, default_cooldown(Opts))),
                int_opt(http_gate_max_cooldown_s, ?MAX_COOLDOWN_S, Opts)
            ),
            Key = authority(Peer),
            ?event(warning, {http_gate_cooling, {peer, Key}, {seconds, Secs}}),
            extend({cool, Key}, now_us() + Secs * 1_000_000),
            ok;
        _ -> ok
    end.

%% @doc The local refusal, in `hb_http_client' response form.
refusal(Secs) ->
    {ok, 429,
        [
            {<<"retry-after">>, integer_to_binary(Secs)},
            {<<"hb-gate">>, <<"local">>}
        ],
        <<"Host is cooling down after a 429.">>}.

%% @doc `httpc:request/4' through the gate, for the callers that use `httpc'
%% directly. A local refusal has the shape of an `httpc' 429 reply.
httpc_request(Method, Request, HTTPOptions, Options, Opts) ->
    URL = element(1, Request),
    case check(URL, Opts) of
        {refuse, Secs} ->
            {ok,
                {{"HTTP/1.1", 429, "Too Many Requests"},
                    [{"retry-after", integer_to_list(Secs)}, {"hb-gate", "local"}],
                    <<>>}};
        ok ->
            Response = httpc:request(Method, Request, HTTPOptions, Options),
            observe(URL, Response, Opts),
            Response
    end.

%% @doc The lower-case host of a peer or URL, as binary or string.
host(Peer) -> element(1, authority(Peer)).

%% @doc What the gate keys on: `{Host, Port}'. Two services on one host (two
%% local nodes, say) are limited separately.
authority(Peer) ->
    Bin = hb_util:bin(Peer),
    case catch uri_string:parse(Bin) of
        URI = #{ host := Host } when Host =/= <<>> ->
            Port =
                case URI of
                    #{ port := P } when is_integer(P) -> P;
                    #{ scheme := <<"http">> } -> 80;
                    _ -> 443
                end,
            {hb_util:to_lower(hb_util:bin(Host)), Port};
        _ -> {hb_util:to_lower(Bin), 443}
    end.

%% @doc Forget every cooldown and pace (tests).
reset() -> ets:delete_all_objects(table()), ok.

%%% Pacing

%% Reserve the next free turn (`tat', the theoretical arrival time) with a
%% compare-and-swap, so concurrent callers each get their own turn.
pace(Key = {Host, _Port}, Now, Opts) ->
    case rate(Host, Opts) of
        undefined -> ok;
        Rate ->
            Interval = max(1, round(1_000_000 / Rate)),
            MaxWait = int_opt(http_gate_max_wait_ms, ?DEFAULT_MAX_WAIT_MS, Opts) * 1000,
            reserve({pace, Key}, Now, Interval, MaxWait)
    end.

reserve(Key, Now, Interval, MaxWait) ->
    {Old, Turn} =
        case ets:lookup(table(), Key) of
            [{_, Tat}] -> {Tat, max(Tat, Now)};
            [] -> {absent, Now}
        end,
    Wait = Turn - Now,
    case Wait > MaxWait of
        true -> {refuse, ceil_s(Wait)};
        false ->
            case swap(Key, Old, Turn + Interval) of
                true ->
                    Wait > 0 andalso timer:sleep(ceil(Wait / 1000)),
                    ok;
                false -> reserve(Key, now_us(), Interval, MaxWait)
            end
    end.

swap(Key, absent, New) -> ets:insert_new(table(), {Key, New});
swap(Key, Old, New) ->
    1 =:= ets:select_replace(table(), [{{Key, Old}, [], [{const, {Key, New}}]}]).

%% Move a deadline later, never earlier.
extend(Key, Until) ->
    case ets:lookup(table(), Key) of
        [{_, Current}] when Current >= Until -> ok;
        [] -> ets:insert_new(table(), {Key, Until}) orelse extend(Key, Until);
        [{_, Current}] -> swap(Key, Current, Until) orelse extend(Key, Until)
    end.

rate(Host, Opts) ->
    Rates = hb_opts:get(http_gate_rates, #{}, Opts),
    case is_map(Rates) andalso maps:get(Host, Rates, undefined) of
        R when is_number(R), R > 0 -> R;
        R when is_binary(R) ->
            case catch binary_to_integer(R) of N when is_integer(N), N > 0 -> N; _ -> undefined end;
        _ -> undefined
    end.

%%% Responses

status_headers({ok, Status, Headers, _Body}) -> {Status, Headers};
status_headers({ok, {{_, Status, _}, Headers, _Body}}) -> {Status, Headers};
status_headers(_) -> none.

retry_after(Headers, Default) ->
    Value =
        hd([V || {K, V} <- Headers, hb_util:to_lower(hb_util:bin(K)) =:= <<"retry-after">>]
            ++ [undefined]),
    case catch binary_to_integer(string:trim(hb_util:bin(Value))) of
        Secs when is_integer(Secs), Secs >= 0 -> Secs;
        _ -> Default
    end.

default_cooldown(Opts) -> int_opt(http_gate_default_cooldown_s, ?DEFAULT_COOLDOWN_S, Opts).

int_opt(Key, Default, Opts) ->
    case catch hb_util:int(hb_opts:get(Key, Default, Opts)) of
        N when is_integer(N), N >= 0 -> N;
        _ -> Default
    end.

now_us() -> erlang:monotonic_time(microsecond).

ceil_s(Us) -> max(1, ceil(Us / 1_000_000)).

%%% The table, owned by a process of its own so that it outlives its callers.

table() ->
    case ets:whereis(?MODULE) of
        undefined -> start_owner(), ?MODULE;
        _ -> ?MODULE
    end.

start_owner() ->
    Parent = self(),
    Ref = make_ref(),
    {_, Monitor} =
        spawn_monitor(
            fun() ->
                try
                    ets:new(?MODULE, [named_table, public, set,
                        {read_concurrency, true}, {write_concurrency, true}]),
                    Parent ! {Ref, ok},
                    timer:sleep(infinity)
                catch error:badarg -> Parent ! {Ref, exists}
                end
            end
        ),
    receive {Ref, _} -> ok; {'DOWN', Monitor, _, _, _} -> ok end,
    erlang:demonitor(Monitor, [flush]),
    ok.

%%% Tests

too_many(RetryAfter) -> {ok, 429, [{<<"Retry-After">>, RetryAfter}], <<>>}.

host_test() ->
    ?assertEqual(<<"arweave.net">>, host(<<"https://Arweave.net">>)),
    ?assertEqual(<<"arweave.net">>, host("https://arweave.net:443/tx/abc?x=1")),
    ?assertEqual({<<"localhost">>, 8734}, authority(<<"http://localhost:8734">>)),
    ?assertEqual({<<"arweave.net">>, 443}, authority(<<"https://arweave.net">>)).

cooldown_test() ->
    reset(),
    ?assertEqual(ok, check(<<"https://cool.test">>, #{})),
    ok = observe(<<"https://cool.test">>, too_many(<<"299">>), #{}),
    {refuse, Secs} = check(<<"https://cool.test/height">>, #{}),
    ?assert(Secs >= 298 andalso Secs =< 299),
    % Every caller sees it, and other hosts are not affected.
    Self = self(),
    spawn(fun() -> Self ! {other, check("https://cool.test:443/x", #{})} end),
    receive {other, Other} -> ?assertMatch({refuse, _}, Other) end,
    ?assertEqual(ok, check(<<"https://other.test">>, #{})).

cooldown_only_extends_test() ->
    reset(),
    ok = observe(<<"https://ext.test">>, too_many(<<"100">>), #{}),
    ok = observe(<<"https://ext.test">>, too_many(<<"5">>), #{}),
    {refuse, Secs} = check(<<"https://ext.test">>, #{}),
    ?assert(Secs >= 99).

cooldown_bounds_test() ->
    reset(),
    % No retry-after: the default.
    ok = observe(<<"https://none.test">>, {ok, 429, [], <<>>}, #{}),
    {refuse, D} = check(<<"https://none.test">>, #{}),
    ?assert(D =< ?DEFAULT_COOLDOWN_S andalso D >= ?DEFAULT_COOLDOWN_S - 1),
    % An absurd retry-after: the cap.
    Opts = #{ <<"http-gate-max-cooldown-s">> => 60 },
    ok = observe(<<"https://huge.test">>, too_many(<<"86400">>), Opts),
    {refuse, H} = check(<<"https://huge.test">>, Opts),
    ?assert(H =< 60),
    % retry-after 0 still cools for a second; other statuses never cool.
    ok = observe(<<"https://zero.test">>, too_many(<<"0">>), #{}),
    ?assertMatch({refuse, 1}, check(<<"https://zero.test">>, #{})),
    ok = observe(<<"https://ok.test">>, {ok, 503, [{<<"retry-after">>, <<"9">>}], <<>>}, #{}),
    ok = observe(<<"https://ok.test">>, {error, timeout}, #{}),
    ?assertEqual(ok, check(<<"https://ok.test">>, #{})).

cooldown_expires_test() ->
    reset(),
    ok = observe(<<"https://short.test">>, too_many(<<"1">>), #{}),
    ?assertMatch({refuse, 1}, check(<<"https://short.test">>, #{})),
    timer:sleep(1050),
    ?assertEqual(ok, check(<<"https://short.test">>, #{})).

pace_test() ->
    reset(),
    Opts = #{ <<"http-gate-rates">> => #{ <<"paced.test">> => 20 } },
    Self = self(),
    Start = erlang:monotonic_time(millisecond),
    [spawn(fun() -> Self ! {paced, check(<<"https://paced.test">>, Opts),
        erlang:monotonic_time(millisecond)} end) || _ <- lists:seq(1, 10)],
    Results = [receive {paced, R, T} -> {R, T} end || _ <- lists:seq(1, 10)],
    ?assert(lists:all(fun({R, _}) -> R =:= ok end, Results)),
    % Ten turns at 20/s: the last one is at least 9 x 50 ms after the first.
    Last = lists:max([T || {_, T} <- Results]),
    ?assert(Last - Start >= 440),
    % An unlisted host is not paced.
    T0 = erlang:monotonic_time(millisecond),
    [ok = check(<<"https://free.test">>, Opts) || _ <- lists:seq(1, 50)],
    ?assert(erlang:monotonic_time(millisecond) - T0 < 100).

pace_refuses_beyond_max_wait_test() ->
    reset(),
    Opts = #{ <<"http-gate-rates">> => #{ <<"slow.test">> => 1 },
        <<"http-gate-max-wait-ms">> => 500 },
    ?assertEqual(ok, check(<<"https://slow.test">>, Opts)),
    % The next turn is ~1 s away, past the 500 ms wait: refused locally, and
    % the turn is not taken, so the refusal does not push later callers back.
    ?assertMatch({refuse, 1}, check(<<"https://slow.test">>, Opts)),
    ?assertMatch({refuse, 1}, check(<<"https://slow.test">>, Opts)),
    timer:sleep(1000),
    ?assertEqual(ok, check(<<"https://slow.test">>, Opts)).

httpc_refusal_shape_test() ->
    reset(),
    ok = observe("https://shape.test/height", {ok, {{"HTTP/1.1", 429, "Too Many Requests"},
        [{"retry-after", "40"}], <<>>}}, #{}),
    {ok, {{_, 429, _}, Headers, <<>>}} =
        httpc_request(get, {"https://shape.test/height", []}, [], [], #{}),
    ?assertEqual("local", proplists:get_value("hb-gate", Headers)),
    ?assert(list_to_integer(proplists:get_value("retry-after", Headers)) >= 39),
    {ok, 429, BinHeaders, _} = refusal(7),
    ?assertEqual(<<"7">>, proplists:get_value(<<"retry-after">>, BinHeaders)).

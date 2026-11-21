-module(ar_timestamp).
-export([start/0, get/0]).
-include_lib("eunit/include/eunit.hrl").
-define(TIMEOUT, 1000 * 15).
-define(MAX_BACKOFF, 1000 * 120).
-include_lib("include/hb.hrl").

%%% A simple Erlang server that caches the current Arweave timestamp and
%%% refreshes it periodically.

%% @doc Check if the server is already running, and if not, start it.
start() ->
    ?event(starting_ar_timestamp_server),
    case whereis(?MODULE) of
        undefined -> spawn_server();
        PID ->
            case is_process_alive(PID) of
                true -> PID;
                false -> spawn_server()
            end
    end.

%% @doc Spawn a new server and its refresher.
spawn_server() ->
    TSServer =
        spawn(fun() ->
            ets:new(?MODULE, [named_table, protected, {read_concurrency, true}]),
            cache(
                case hb_client_remote:arweave_timestamp() of
                    {ok, Current} -> Current;
                    {error, _} -> {0, 0, hb_util:human_id(<<0:256>>)}
                end
            )
        end),
    spawn(fun() -> link(TSServer), refresher(TSServer) end),
    register(?MODULE, TSServer),
    TSServer.

%% @doc Get the current timestamp: read from the server's table, so that no
%% caller waits on the server. Every assignment the scheduler builds reads it,
%% and a request to the one server process queued the scheduler behind every
%% other caller's request. Until the table holds a value, ask the server,
%% starting it if it isn't already running.
get() ->
    try ets:lookup(?MODULE, current) of
        [{current, Timestamp}] -> Timestamp;
        [] -> get_from_server()
    catch error:badarg -> get_from_server()
    end.

get_from_server() ->
    ?event(getting_ar_timestamp),
    PID = start(),
    ?event({got_ar_timestamp_pid, PID}),
    PID ! {get, self()},
    ?event(waiting_for_ar_timestamp),
    receive
        {timestamp, Timestamp} ->
            ?event({got_ar_timestamp, Timestamp}),
            Timestamp
    end.

%% @doc Cache the current timestamp from Arweave.
cache(Current) ->
    ets:insert(?MODULE, {current, Current}),
    ?event(cache_waiting),
    receive
        {get, Pid} ->
            ?event({got_get_request, Pid}),
            Pid ! {timestamp, Current},
            ?event({sent_timestamp, Current}),
            cache(Current);
        {refresh, New} ->
            ?event({refreshed_ar_timestamp, New}),
            cache(New)
    end.

%% @doc Refresh the timestamp cache periodically. A failed read keeps the last
%% block: an assignment is stamped with a block a few seconds old, and with
%% block 0 only until the first read succeeds. A gateway that refuses is not
%% asked again before it says to, and each refusal in a row doubles the wait,
%% up to `?MAX_BACKOFF': asking every 15 s through a rate limit kept the node's
%% address limited.
refresher(TSServer) -> refresher(TSServer, ?TIMEOUT).
refresher(TSServer, Wait) ->
    timer:sleep(Wait),
    case hb_client_remote:arweave_timestamp() of
        {ok, New} ->
            TSServer ! {refresh, New},
            refresher(TSServer, ?TIMEOUT);
        {error, RetryAfter} ->
            refresher(TSServer, backoff(Wait, RetryAfter))
    end.

backoff(Wait, RetryAfter) when is_integer(RetryAfter) ->
    min(?MAX_BACKOFF, max(2 * Wait, RetryAfter * 1000));
backoff(Wait, _) ->
    min(?MAX_BACKOFF, 2 * Wait).

backoff_test() ->
    ?assertEqual(30000, backoff(?TIMEOUT, undefined)),
    ?assertEqual(60000, backoff(?TIMEOUT, 60)),
    ?assertEqual(30000, backoff(?TIMEOUT, 30)),
    ?assertEqual(?MAX_BACKOFF, backoff(90000, 30)),
    ?assertEqual(?MAX_BACKOFF, backoff(?TIMEOUT, 3600)).

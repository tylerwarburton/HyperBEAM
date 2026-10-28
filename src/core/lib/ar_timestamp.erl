-module(ar_timestamp).
-export([start/0, get/0]).
-define(TIMEOUT, 1000 * 15).
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
            cache(hb_client_remote:arweave_timestamp())
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

%% @doc Refresh the timestamp cache periodically.
refresher(TSServer) ->
    timer:sleep(?TIMEOUT),
    try TSServer ! {refresh, hb_client_remote:arweave_timestamp()}
    catch
        Class:Reason ->
            ?event(warning,
                {ar_timestamp_refresh_failed,
                    {class, Class},
                    {reason, Reason}
                }
            )
    end,
    refresher(TSServer).
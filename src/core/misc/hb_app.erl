%%%-------------------------------------------------------------------
%% @doc The main HyperBEAM application module.
%% @end
%%%-------------------------------------------------------------------

-module(hb_app).

-behaviour(application).

-export([start/2, stop/1]).

-include("include/hb.hrl").

start(_StartType, _StartArgs) ->
    hb:init(),
    {ok, Supervisor} = hb_sup:start_link(),
    ok = hb_name:start(),
    _TimestampServer = ar_timestamp:start(),
    {ok, _Listener, ServerID} = hb_http_server:start_application(),
    {ok, Supervisor, ServerID}.

stop(ServerID) ->
    Res = cowboy:stop_listener(ServerID),
    % No request can write any more: stop the essentials exporters cleanly, so
    % the next start does not take this stop for a crash and resync.
    catch hb_store_export:shutdown_all(),
    Res.

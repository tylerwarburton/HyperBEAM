%%%-------------------------------------------------------------------
%% @doc The main HyperBEAM application module.
%% @end
%%%-------------------------------------------------------------------

-module(hb_app).

-behaviour(application).

-export([start/2, stop/1]).

-include("include/hb.hrl").

start(_StartType, _StartArgs) ->
    ok = require_nifs(),
    hb:init(),
    {ok, Supervisor} = hb_sup:start_link(),
    ok = hb_name:start(),
    _TimestampServer = ar_timestamp:start(),
    {ok, _Listener, ServerID} = hb_http_server:start_application(),
    {ok, Supervisor, ServerID}.

stop(ServerID) ->
    cowboy:stop_listener(ServerID).

%% @doc Refuse to start without the NIFs that a computation may call: a node
%% that cannot run `timelock@1.0' must not compute a process that does.
require_nifs() ->
    case code:ensure_loaded(blst_nif) of
        {module, blst_nif} -> ok;
        Error -> {error, {blst_nif_not_loaded, Error}}
    end.

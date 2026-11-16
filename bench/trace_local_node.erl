%%% @doc A local node for a message-trace stress run: the game slot bench's
%%% node options (`game_slot_bench'), served over HTTP, with one game-sized
%%% `lua-patch@1.0' process seeded and its ID printed. Clients schedule
%%% `Play' messages to it and read `now/at-slot' back; the node's trace is
%%% `/~hyperbuddy@1.0/trace'.
%%%
%%%   TRACE_NODE=<port>:<players>:<dir> erl -noshell -pa _build/default/lib/*/ebin \
%%%     -eval 'trace_local_node:start()'
-module(trace_local_node).
-export([start/0]).

start() ->
    [Port, Players, Dir] = string:split(os:getenv("TRACE_NODE", "8734:300:/tmp/trace-node"), ":", all),
    os:cmd("rm -rf " ++ Dir),
    application:ensure_all_started([kernel, stdlib, inets, ssl, ranch, cowboy, gun, os_mon]),
    hb:init(),
    Wallet = ar_wallet:new(),
    Opts = #{
        <<"port">> => list_to_integer(Port),
        <<"priv-wallet">> => Wallet,
        <<"store">> => [#{ <<"store-module">> => hb_store_lmdb,
                           <<"name">> => list_to_binary(Dir ++ "/main") }],
        <<"spawn-worker">> => true,
        <<"process-worker-spawn">> => true,
        <<"process-workers">> => true,
        <<"await-inprogress">> => named,
        <<"scheduler-lookahead">> => false,
        <<"scheduler-publish-remote">> => false,
        <<"process-snapshot-target">> => false,
        <<"process-delta-cache">> => true,
        <<"process-delta-checkpoint-slots">> => 1000,
        <<"process-hot-cache-slots">> => 512,
        <<"process-state-blobs">> => true,
        <<"process-now-from-cache">> => true,
        <<"match-index">> => false,
        <<"rate-limit-requests">> => 1000000,
        <<"rate-limit-period">> => 60,
        <<"rate-limit-max">> => 1000000,
        <<"rate-limit-min">> => 0
    },
    Node = hb_http_server:start_node(Opts),
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    Script = script(list_to_integer(Players)),
    Process =
        hb_message:commit(
            #{
                <<"device">> => <<"process@1.0">>,
                <<"type">> => <<"Process">>,
                <<"scheduler-device">> => <<"scheduler@1.0">>,
                <<"execution-device">> => <<"lua-patch@1.0">>,
                <<"module">> => #{ <<"content-type">> => <<"application/lua">>,
                                   <<"body">> => Script },
                <<"authority">> => [Address],
                <<"scheduler-location">> => Address,
                <<"scheduler">> => Address,
                <<"test-random-seed">> => rand:uniform(1000000)
            },
            Opts),
    {ok, _} = hb_cache:write(Process, Opts),
    ProcID = hb_message:id(Process, all, Opts),
    io:format("TRACE_NODE url=~s address=~s process=~s~n", [Node, Address, ProcID]),
    receive after infinity -> ok end.

%% The game slot bench's contract: `Seed' publishes every player, any other
%% action rewrites one.
script(Players) ->
    iolist_to_binary([
        "NPlayers = ", integer_to_list(Players), "\n"
        "Players = Players or {}\n"
        "Count = Count or 0\n"
        "Seeded = Seeded or false\n"
        "local pad = string.rep('x', 2800)\n"
        "local function rec(p)\n"
        "  return '{\"level\":' .. p.level .. ',\"wins\":' .. p.wins ..\n"
        "    ',\"sprite\":' .. p.sprite .. ',\"pad\":\"' .. pad .. '\"}'\n"
        "end\n"
        "function compute(req)\n"
        "  local body = req.body or req\n"
        "  Count = Count + 1\n"
        "  local patches = {}\n"
        "  if not Seeded then\n"
        "    Seeded = true\n"
        "    for i = 1, NPlayers do\n"
        "      Players[i] = { level = 1, wins = 0, sprite = 0 }\n"
        "      patches[#patches + 1] = { path = '/player-' .. i, value = rec(Players[i]) }\n"
        "    end\n"
        "  else\n"
        "    local i = (Count % NPlayers) + 1\n"
        "    local p = Players[i]\n"
        "    p.sprite = p.sprite + 1\n"
        "    p.wins = p.wins + (Count % 3)\n"
        "    patches[1] = { path = '/player-' .. i, value = rec(p) }\n"
        "  end\n"
        "  patches[#patches + 1] = { path = '/count', value = tostring(Count) }\n"
        "  return { patches = patches, results = { output = { data = tostring(Count) } } }\n"
        "end\n"
    ]).

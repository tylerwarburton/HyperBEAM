%%% @doc Is local disk bounded under sustained load while the cold tier
%%% grows? A node (`storage_bench:opts/1': the live node config, journaled
%%% irreplaceable store) with retention and the cold tier on, `Procs'
%%% `lua-patch' counters, client-signed RSA inputs scheduled at `Rate' a
%%% second in total and computed every second, for `Minutes'. Every 10 s it
%%% writes one line to `<Dir>/bounded.csv' and prints it:
%%%
%%%   t_s, slots, writes_per_s,
%%%   main_live_mb, main_file_mb, irr_live_mb, irr_file_mb, journal_mb,
%%%   share_mb, ship_mb_per_s, lag_slots, pruned_slots, level
%%%
%%% A bounded node shows `main_live_mb' and `irr_live_mb' level off once each
%%% process is past its hot window (`hot-slots') while `share_mb' keeps
%%% growing, with `lag_slots' flat. The share is a local folder here, so
%%% `ship_mb_per_s' is the node's own ceiling, not the link's.
%%%
%%%   erl -noshell -pa _build/default/lib/*/ebin -pa <this ebin> \
%%%     -eval 'bounded_bench:run("<dir>", Minutes, Rate, Procs, HotSlots)'
-module(bounded_bench).
-export([run/5]).

-define(SAMPLE_MS, 10000).
-define(CLIENTS_PER_PROC, 4).

run(Dir, Minutes, Rate, NProcs, HotSlots) ->
    os:cmd("rm -rf " ++ Dir),
    hb:init(),
    Base = storage_bench:opts(Dir),
    Opts = Base#{
        <<"store-retention">> => true,
        <<"store-retention-first-ms">> => 10000,
        <<"store-retention-interval-ms">> => 30000,
        <<"cold-tier">> => #{
            <<"path">> => list_to_binary(Dir ++ "/share"),
            <<"hot-slots">> => HotSlots,
            <<"hot-slots-floor">> => min(HotSlots, 2000),
            <<"interval-ms">> => 5000,
            <<"arweave-gateway">> => false }
    },
    _ = hb_http_server:start_node(Opts),
    ok = hb_store_retention:maybe_start(Opts),
    ok = hb_store_cold:maybe_start(Opts),
    Counter = <<
        "Count = Count or 0\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  return { patches = {{ path = '/count', value = tostring(Count) }} }\n"
        "end\n">>,
    Procs = [ spawn_process(process(Counter, Opts), Opts) || _ <- lists:seq(1, NProcs) ],
    Counters = counters:new(1, [write_concurrency]),
    End = erlang:monotonic_time(millisecond) + Minutes * 60000,
    PerClient = Rate / (NProcs * ?CLIENTS_PER_PROC),
    Clients =
        [ spawn_link(fun() -> client(P, PerClient, End, Counters, Opts) end)
        || P <- Procs, _ <- lists:seq(1, ?CLIENTS_PER_PROC) ],
    Computers = [ spawn_link(fun() -> computer(P, End, Opts) end) || P <- Procs ],
    {ok, CSV} = file:open(Dir ++ "/bounded.csv", [write, raw, binary]),
    ok = file:write(CSV, <<"t_s,slots,writes_per_s,main_live_mb,main_file_mb,irr_live_mb,"
        "irr_file_mb,journal_mb,share_mb,ship_mb_per_s,lag_slots,pruned_slots,level\n">>),
    sample(CSV, Dir, Opts, Counters, erlang:monotonic_time(millisecond), End, {0, 0}),
    [ exit(Pid, kill) || Pid <- Clients ++ Computers ],
    ok = file:close(CSV),
    ok.

process(Script, Opts) ->
    Address = hb_util:human_id(ar_wallet:to_address(hb_opts:get(<<"priv-wallet">>, undefined, Opts))),
    hb_message:commit(#{
        <<"device">> => <<"process@1.0">>, <<"type">> => <<"Process">>,
        <<"scheduler-device">> => <<"scheduler@1.0">>,
        <<"execution-device">> => <<"lua-patch@1.0">>,
        <<"module">> => #{ <<"content-type">> => <<"application/lua">>, <<"body">> => Script },
        <<"authority">> => [Address], <<"scheduler-location">> => Address,
        <<"scheduler">> => Address,
        <<"test-random-seed">> => rand:uniform(1000000000)
    }, Opts).

spawn_process(Process, Opts) ->
    {ok, _} = hb_cache:write(Process, Opts#{ <<"store">> => hb_opts:get(<<"scheduler-store">>, [], Opts) }),
    Process.

pid(Process, Opts) -> hb_util:human_id(hb_message:id(Process, all, Opts)).

%% One client: its own RSA wallet, ~1.7 KB inputs, at `PerSecond'.
client(P, PerSecond, End, Counters, Opts) ->
    C = Opts#{ <<"priv-wallet">> => ar_wallet:new() },
    Gap = round(1000 / PerSecond),
    client_loop(P, Gap, End, Counters, C, Opts, 0).

client_loop(P, Gap, End, Counters, C, Opts, N) ->
    T0 = erlang:monotonic_time(millisecond),
    case T0 >= End of
        true -> ok;
        false ->
            Body = hb_message:commit(#{
                <<"target">> => pid(P, Opts), <<"type">> => <<"Message">>,
                <<"action">> => <<"Count">>, <<"number">> => integer_to_binary(N),
                <<"player-id">> => hb_util:human_id(crypto:strong_rand_bytes(32)),
                <<"data">> => base64:encode(crypto:strong_rand_bytes(225)) }, C, <<"ans104@1.0">>),
            case catch hb_ao:resolve(P, hb_message:commit(#{
                    <<"path">> => <<"schedule">>, <<"method">> => <<"POST">>,
                    <<"body">> => Body }, C), Opts) of
                {ok, _} -> counters:add(Counters, 1, 1);
                _ -> ok
            end,
            Spent = erlang:monotonic_time(millisecond) - T0,
            timer:sleep(max(0, Gap - Spent)),
            client_loop(P, Gap, End, Counters, C, Opts, N + 1)
    end.

%% Compute each process to its head, every second.
computer(P, End, Opts) ->
    case erlang:monotonic_time(millisecond) >= End of
        true -> ok;
        false ->
            _ = (catch hb_ao:resolve(P, #{ <<"path">> => <<"now">> }, Opts)),
            timer:sleep(1000),
            computer(P, End, Opts)
    end.

sample(CSV, Dir, Opts, Counters, Start, End, {LastSlots, LastShipped}) ->
    timer:sleep(?SAMPLE_MS),
    Now = erlang:monotonic_time(millisecond),
    Slots = counters:get(Counters, 1),
    Health = hb_store_lmdb:health(),
    Store = fun(Path) ->
        H = maps:get(list_to_binary(Path), Health, #{}),
        {mb(maps:get(live_bytes, H, 0)), mb(maps:get(file_bytes, H, 0))}
    end,
    {MainLive, MainFile} = Store(Dir ++ "/lmdb"),
    {IrrLive, IrrFile} = Store(Dir ++ "/irreplaceable"),
    Cold = hb_store_cold:status(),
    Shipped = maps:get(shipped_bytes, Cold, 0),
    Line = io_lib:format("~B,~B,~.1f,~.1f,~.1f,~.1f,~.1f,~.1f,~.1f,~.2f,~B,~B,~s~n",
        [(Now - Start) div 1000, Slots, (Slots - LastSlots) * 1000 / ?SAMPLE_MS,
         MainLive, MainFile, IrrLive, IrrFile,
         mb(du(Dir ++ "/journal") + du(Dir ++ "/export")), mb(du(Dir ++ "/share")),
         mb(Shipped - LastShipped) * 1000 / ?SAMPLE_MS,
         maps:get(lag_slots, Cold, 0), maps:get(pruned_slots, Cold, 0),
         hb_store_budget:level()]),
    ok = file:write(CSV, Line),
    io:format(user, "~s", [Line]),
    case Now >= End of
        true -> ok;
        false -> sample(CSV, Dir, Opts, Counters, Start, End, {Slots, Shipped})
    end.

mb(B) -> B / 1048576.

du(Path) ->
    case string:lexemes(os:cmd("du -sb " ++ Path ++ " 2>/dev/null"), "\t\n") of
        [B | _] -> list_to_integer(B);
        _ -> 0
    end.

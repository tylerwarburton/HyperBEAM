%%% @doc Bytes on disk per slot, by row family, for the three process shapes
%%% our nodes carry, under the node config they run (`infra/nodes'):
%%%
%%%   game    `lua-patch@1.0', 300 players of ~3 KB, one rewritten per slot
%%%   book    `lua-patch@1.0', 2,000 resting orders of ~200 B, one per slot
%%%   token   `lua@5.3a' (full state), `TOKEN_ENTRIES' balances (default
%%%           6,000, ~600 KB published), one transfer per slot
%%%
%%% Each input is a client-signed ANS-104 item (RSA-4096, five tags and a
%%% ~300 B body: ~1.7 KB, the size the audit measured on Arweave). The token
%%% is also followed by a bridge-style replica (`lib_bridge_replica:view/3'
%%% over HTTP, as the attestor runs it). The irreplaceable store is journaled
%%% (`irreplaceable-export'), as on the authority node.
%%%
%%% Run inside a build (copy to `src/core/test/', then `rebar3 device test
%%% --with-core --module=storage_bench') with STORAGE_BENCH set to:
%%%   load:<rounds>:<dir>   one message to each process per round. Prints
%%%       logical bytes per slot by row family and store, the physical growth
%%%       of each store, then runs retention once with the node's defaults
%%%       and once at `keep-seconds' 0 and prints what is left.
%%%
%%% The crash test runs outside eunit, in a VM that is `kill -9'ed:
%%%   erl -noshell -pa _build/default/lib/*/ebin -pa <bench ebin> \
%%%     -eval 'storage_bench:crash_load("<dir>")'      loads until killed
%%%   erl ... -eval 'storage_bench:crash_verify("<dir>")'  restarts on the
%%%       same stores: every scheduled slot is there in order, the head
%%%       recomputed from the irreplaceable store alone matches the stored
%%%       head, and a pruned slot is rebuilt by replay (timed).
-module(storage_bench).
-export([crash_load/1, crash_verify/1, opts/1, census/1]).
-include_lib("eunit/include/eunit.hrl").

bench_test_() ->
    case os:getenv("STORAGE_BENCH") of
        false -> [];
        Spec ->
            {timeout, 14400, fun() -> run(string:split(Spec, ":", all)) end}
    end.

lmdb(Dir, Sync) ->
    #{ <<"store-module">> => hb_store_lmdb, <<"name">> => list_to_binary(Dir),
       <<"sync">> => Sync }.

%% The live node config (`infra/nodes/node-config.v011.json'), minus the
%% remote stores, with the authority node's journal.
opts(Dir) ->
    hb_store_irreplaceable:node_message(#{
        <<"store">> => [lmdb(Dir ++ "/lmdb", false)],
        <<"irreplaceable-store">> => lmdb(Dir ++ "/irreplaceable", true),
        <<"irreplaceable-export">> =>
            #{ <<"path">> => list_to_binary(Dir ++ "/export"),
               <<"journal">> => list_to_binary(Dir ++ "/journal") },
        <<"priv-wallet">> => wallet(Dir),
        <<"port">> => 10000 + rand:uniform(20000),
        <<"spawn-worker">> => true,
        <<"process-worker-spawn">> => true,
        <<"process-workers">> => true,
        <<"await-inprogress">> => named,
        <<"scheduler-lookahead">> => false,
        <<"scheduler-publish-remote">> => false,
        <<"scheduler-default-commitment-spec">> => <<"ans104@1.0">>,
        <<"process-snapshot-time">> => 900,
        <<"process-snapshot-slots">> => 1000,
        <<"process-snapshot-target">> => false,
        <<"process-now-from-cache">> => true,
        <<"process-delta-cache">> => true,
        <<"process-delta-checkpoint-slots">> => 1000,
        <<"process-hot-cache-slots">> => 512,
        <<"process-state-blobs">> => true,
        <<"store-retention">> => false,
        <<"match-index">> => false,
        <<"store-all-signed">> => false,
        <<"lua-patch-invalid-patches">> => <<"drop">>,
        % The cold tier, shipping every 5 s and keeping 30 s locally, so a
        % run ships, prunes and reads back (`hb_store_cold').
        <<"cold-tier">> => #{ <<"path">> => list_to_binary(Dir ++ "/share"),
                              <<"keep-local-seconds">> => 30, <<"interval-ms">> => 5000,
                              <<"arweave-gateway">> => false }
    }).

%% The node's key, kept beside its stores so a restarted node is the same
%% scheduler.
wallet(Dir) ->
    File = Dir ++ "/wallet.json",
    case file:read_file(File) of
        {ok, Bin} -> ar_wallet:from_json(Bin);
        _ ->
            ok = filelib:ensure_dir(File),
            W = ar_wallet:new(),
            ok = file:write_file(File, ar_wallet:to_json(W)),
            W
    end.

%%% The processes.

%% Pseudo-random characters, so states compress as real ones do (a
%% repeated pad compressed the game's 1 MB state to 18 KB).
rnd() ->
    <<
        "local A = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'\n"
        "local function rnd(seed, n)\n"
        "  local t, x = {}, seed\n"
        "  for k = 1, n do\n"
        "    x = (x * 1103515245 + 12345) % 2147483648\n"
        "    local c = (x // 65536) % 64 + 1\n"
        "    t[k] = A:sub(c, c)\n"
        "  end\n"
        "  return table.concat(t)\n"
        "end\n"
    >>.

game_script() ->
    <<(rnd())/binary,
        "NPlayers = 300\n"
        "Players = Players or {}\n"
        "Count = Count or 0\n"
        "local function rec(i, p)\n"
        "  return '{\"level\":' .. p.level .. ',\"wins\":' .. p.wins ..\n"
        "    ',\"sprite\":' .. p.sprite .. ',\"pad\":\"' .. rnd(i * 7919 + p.sprite, 2800) .. '\"}'\n"
        "end\n"
        "function compute(req)\n"
        "  local body = req.body or req\n"
        "  Count = Count + 1\n"
        "  local patches = {}\n"
        "  if tostring(body.action) == 'Seed' then\n"
        "    for i = 1, NPlayers do\n"
        "      Players[i] = { level = 1, wins = 0, sprite = 0 }\n"
        "      patches[#patches + 1] = { path = '/player-' .. i, value = rec(i, Players[i]) }\n"
        "    end\n"
        "  else\n"
        "    local i = (Count % NPlayers) + 1\n"
        "    local p = Players[i]\n"
        "    p.sprite = p.sprite + 1\n"
        "    p.wins = p.wins + (Count % 3)\n"
        "    patches[1] = { path = '/player-' .. i, value = rec(i, p) }\n"
        "  end\n"
        "  patches[#patches + 1] = { path = '/count', value = tostring(Count) }\n"
        "  return { patches = patches, results = { output = { data = tostring(Count) } } }\n"
        "end\n"
    >>.

%% Resting orders live in the VM; the book publishes the 50 nearest the
%% touch as one key, as the venue does.
book_script() ->
    <<(rnd())/binary,
        "Orders = Orders or {}\n"
        "Count = Count or 0\n"
        "function compute(req)\n"
        "  local body = req.body or req\n"
        "  Count = Count + 1\n"
        "  if tostring(body.action) == 'Seed' then\n"
        "    for i = 1, 2000 do Orders[i] = { id = i, px = 1000 + (i % 97), qty = i % 13 + 1, m = rnd(i, 150) } end\n"
        "  else\n"
        "    local i = (Count % 2000) + 1\n"
        "    Orders[i] = { id = Count, px = 1000 + (Count % 97), qty = Count % 13 + 1, m = rnd(Count, 150) }\n"
        "  end\n"
        "  local top = {}\n"
        "  for i = 1, 50 do\n"
        "    local o = Orders[((Count + i) % 2000) + 1]\n"
        "    top[#top + 1] = '{\"id\":' .. o.id .. ',\"px\":' .. o.px .. ',\"qty\":' .. o.qty .. ',\"m\":\"' .. o.m .. '\"}'\n"
        "  end\n"
        "  return { patches = {\n"
        "      { path = '/book', value = '[' .. table.concat(top, ',') .. ']' },\n"
        "      { path = '/count', value = tostring(Count) } },\n"
        "    results = { output = { data = tostring(Count) } } }\n"
        "end\n"
    >>.

%% A full-state token: the whole state in and out every slot.
token_script(Entries) ->
    iolist_to_binary([rnd(),
        "function compute(base, req, opts)\n"
        "  local body = req.body or req\n"
        "  base.count = math.tointeger((base.count or 0) + 1)\n"
        "  base.balances = base.balances or {}\n"
        "  if tostring(body.action) == 'Seed' then\n"
        "    for i = 1, ", integer_to_list(Entries), " do\n"
        "      base.balances[string.lower(rnd(i, 43))] = tostring(1000000000 + i * 7919)\n"
        "    end\n"
        "  else\n"
        "    local a = string.lower(rnd((base.count % ", integer_to_list(Entries), ") + 1, 43))\n"
        "    base.balances[a] = tostring(math.tointeger(tonumber(base.balances[a]) - 1))\n"
        "  end\n"
        "  base.results = { output = { data = tostring(base.count) } }\n"
        "  return base\n"
        "end\n"
    ]).

process(Device, Script, Opts) ->
    Address = hb_util:human_id(ar_wallet:to_address(hb_opts:get(<<"priv-wallet">>, undefined, Opts))),
    hb_message:commit(#{
        <<"device">> => <<"process@1.0">>, <<"type">> => <<"Process">>,
        <<"scheduler-device">> => <<"scheduler@1.0">>,
        <<"execution-device">> => Device,
        <<"module">> => #{ <<"content-type">> => <<"application/lua">>, <<"body">> => Script },
        <<"authority">> => [Address], <<"scheduler-location">> => Address,
        <<"scheduler">> => Address,
        <<"test-random-seed">> => rand:uniform(1000000)
    }, Opts).

spawn_process(Process, Opts) ->
    {ok, _} = hb_cache:write(Process, Opts#{ <<"store">> => hb_opts:get(<<"scheduler-store">>, [], Opts) }),
    Process.

procs(Opts) ->
    Entries = list_to_integer(os:getenv("TOKEN_ENTRIES", "8000")),
    [ {Name, spawn_process(process(Dev, Script, Opts), Opts)}
    || {Name, Dev, Script} <-
        [{game, <<"lua-patch@1.0">>, game_script()},
         {book, <<"lua-patch@1.0">>, book_script()},
         {token, <<"lua@5.3a">>, token_script(Entries)}] ].

pid(Process, Opts) -> hb_util:human_id(hb_message:id(Process, all, Opts)).

%% A client-signed ANS-104 input of the size our clients send.
schedule(Process, Action, N, Opts) ->
    C = client_opts(Opts),
    Body = hb_message:commit(#{
        <<"target">> => pid(Process, Opts), <<"type">> => <<"Message">>,
        <<"action">> => Action, <<"number">> => integer_to_binary(N),
        <<"player-id">> => hb_util:human_id(crypto:strong_rand_bytes(32)),
        <<"run-id">> => hb_util:encode(crypto:strong_rand_bytes(16)),
        <<"data">> => base64:encode(crypto:strong_rand_bytes(225)) }, C, <<"ans104@1.0">>),
    {ok, _} = hb_ao:resolve(Process, hb_message:commit(#{
        <<"path">> => <<"schedule">>, <<"method">> => <<"POST">>,
        <<"body">> => Body }, C), Opts).

client_opts(Opts) ->
    case get(client) of
        undefined ->
            put(client, #{ <<"store">> => [hb_test_utils:test_store(hb_store_lmdb, <<"storage-bench-client">>)],
                           <<"priv-wallet">> => ar_wallet:new() });
        _ -> ok
    end,
    maps:merge(Opts, get(client)).

compute(Process, N, Opts) ->
    {ok, State} = hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => N }, Opts),
    State.

%%% Measuring.

%% Logical bytes (key + value) by row family, per store.
census(Store) ->
    #{ <<"db">> := DB } = hb_store:find(Store),
    _ = (catch elmdb:flush(DB)),
    {ok, Acc} = elmdb:fold(DB, fun(K, V, A) ->
        F = family(K, V),
        {N, B} = maps:get(F, A, {0, 0}),
        A#{ F => {N + 1, B + byte_size(K) + byte_size(V)} }
    end, #{}),
    Acc.

family(<<"pstate/", Rest/binary>>, _V) ->
    case binary:split(Rest, <<"/">>, [global]) of
        [Pid, Kind | _] -> {pstate, Pid, Kind};
        _ -> pstate_other
    end;
family(<<"~scheduler@1.0/", Rest/binary>>, _V) ->
    [Kind | _] = binary:split(Rest, <<"/">>), {scheduler, Kind};
family(<<"ni:", _/binary>>, _V) -> content_addressed;
family(<<"data/", _/binary>>, _V) -> data;
family(K, V) ->
    case binary:split(K, <<"/">>, [global]) of
        [_Id] -> {message, case V of <<"link:", _/binary>> -> link; <<"group">> -> group; _ -> value end};
        [_Id, <<"commitments">> | _] -> {message, commitments};
        [_Id | _] -> {message, fields}
    end.

store_list(Opts) ->
    [Main | _] = hb_opts:get(<<"store">>, [], Opts),
    [Irr0 | _] = hb_opts:get(<<"scheduler-store">>, [], Opts),
    Irr = case Irr0 of #{ <<"store-module">> := hb_store_offsite_journal } -> maps:get(<<"inner">>, Irr0); _ -> Irr0 end,
    [{main, Main}, {irreplaceable, maps:without([<<"store-failure">>], Irr)}].

du(Path) ->
    case string:lexemes(os:cmd("du -sb " ++ Path ++ " 2>/dev/null"), "\t\n") of
        [B | _] -> list_to_integer(B);
        _ -> 0
    end.

physical(Dir) ->
    #{ main => du(Dir ++ "/lmdb"), irreplaceable => du(Dir ++ "/irreplaceable"),
       journal => du(Dir ++ "/journal") + du(Dir ++ "/export"),
       replica => du(Dir ++ "/replica") }.

say(F, A) -> io:format(user, F ++ "~n", A).

diff(A, B) ->
    maps:filter(fun(_, {_, Bytes}) -> Bytes =/= 0 end,
        maps:map(fun(K, {N, Bytes}) ->
            {N0, B0} = maps:get(K, A, {0, 0}), {N - N0, Bytes - B0}
        end, B)).

%% Per-slot rows and bytes of each family, by store; pstate rows are
%% attributed to their process, everything else is shared by all slots.
report(Label, Before, After, PidNames, Slots, SlotsPer) ->
    lists:foreach(
        fun({Store, _}) ->
            D = diff(maps:get(Store, Before), maps:get(Store, After)),
            Rows =
                lists:sort(
                    [ {name(F, PidNames), N, B,
                       B / case F of {pstate, _, _} -> SlotsPer; _ -> Slots end}
                    || {F, {N, B}} <- maps:to_list(D) ]),
            Total = lists:sum([ B || {_, _, B, _} <- Rows ]),
            say("~s ~p: ~.1f KB/slot logical over ~p slots", [Label, Store, Total / Slots / 1024, Slots]),
            [ say("  ~-40s rows=~-8w bytes=~-11w ~w B/slot", [lists:flatten(io_lib:format("~p", [F])), N, B, round(PerSlot)])
            || {F, N, B, PerSlot} <- Rows ]
        end,
        maps:to_list(maps:remove(physical, Before))).

name({pstate, Pid, Kind}, PidNames) -> {maps:get(Pid, PidNames, other), Kind};
name(F, _) -> F.

snapshot(Opts, Dir) ->
    (maps:from_list([ {Name, census(S)} || {Name, S} <- store_list(Opts) ]))
        #{ physical => physical(Dir) }.

replica_view(Token, Url, Opts, Dir) ->
    ReplicaDir = list_to_binary(Dir ++ "/replica"),
    case (lib([{view, 3}, {slot_path, 3}])):view(pid(Token, Opts), Url,
            #{ opts => Opts, dir => ReplicaDir }) of
        {ok, _, _, Info} -> Info;
        Other -> Other
    end.

%% A packaged library is loaded under a generated name (`hb_device_rename'):
%% the first loaded module that exports every one of `Funs'.
lib(Funs) ->
    [M | _] =
        [ M || {M, _} <- code:all_loaded(),
               lists:prefix("_hb_device_", atom_to_list(M)),
               lists:all(fun({F, A}) -> erlang:function_exported(M, F, A) end, Funs) ],
    M.

replica_census(Token, Opts, Dir) ->
    Base = Dir ++ "/replica/" ++ binary_to_list(pid(Token, Opts)),
    Stores = [{verified, lmdb(Base, false)}, {computed, lmdb(Base ++ ".computed", false)}],
    maps:from_list([ {N, catch census(S)} || {N, S} <- Stores ]).

%%% Runs.

run(["load", Rounds0, Dir]) ->
    hb:init(),
    Rounds = list_to_integer(Rounds0),
    os:cmd("rm -rf " ++ Dir),
    Opts = opts(Dir),
    Url = hb_http_server:start_node(Opts),
    Procs = procs(Opts),
    PidNames = maps:from_list([ {pid(P, Opts), N} || {N, P} <- Procs ]),
    {token, Token} = lists:keyfind(token, 1, Procs),
    % Seed (slot 0) and warm up five slots each.
    [ begin schedule(P, <<"Seed">>, 0, Opts), compute(P, 0, Opts) end || {_, P} <- Procs ],
    [ begin schedule(P, <<"Play">>, I, Opts), compute(P, I, Opts) end
    || I <- lists:seq(1, 5), {_, P} <- Procs ],
    say("STORAGE replica warm-up: ~p", [replica_view(Token, Url, Opts, Dir)]),
    S0 = snapshot(Opts, Dir),
    R0 = replica_census(Token, Opts, Dir),
    Times =
        lists:foldl(
            fun(I, Acc) ->
                T = [ {N, element(1, timer:tc(fun() ->
                        schedule(P, <<"Play">>, I, Opts), compute(P, I, Opts) end))}
                    || {N, P} <- Procs ],
                % The attestor brings its replica to the head every pass.
                case I rem 10 of 0 -> replica_view(Token, Url, Opts, Dir); _ -> ok end,
                [T | Acc]
            end,
            [],
            lists:seq(6, 5 + Rounds)),
    Info = replica_view(Token, Url, Opts, Dir),
    S1 = snapshot(Opts, Dir),
    R1 = replica_census(Token, Opts, Dir),
    Slots = 3 * Rounds,
    Med = fun(L) -> lists:nth(length(L) div 2 + 1, lists:sort(L)) div 1000 end,
    say("STORAGE load rounds=~p slots=~p slot ms median: ~p", [Rounds, Slots,
        [ {N, Med([ T || Row <- Times, {N1, T} <- Row, N1 == N ])} || {N, _} <- Procs ]]),
    report("STORAGE written", S0, S1, PidNames, Slots, Rounds),
    P0 = maps:get(physical, S0), P1 = maps:get(physical, S1),
    say("STORAGE physical growth per slot (B): ~p",
        [maps:map(fun(K, V) -> (V - maps:get(K, P0)) div
            case K of replica -> Rounds; _ -> Slots end end, P1)]),
    say("STORAGE replica ~p", [Info]),
    [ say("STORAGE replica ~p: ~p", [K, diff(maps:get(K, R0), maps:get(K, R1))]) || K <- [verified, computed] ],
    % What retention keeps: with the node's defaults (everything here is
    % young), then at keep-seconds 0 (the floor of the newest checkpoints).
    lists:foreach(
        fun({Label, Extra}) ->
            Rep = hb_store_retention:retain(maps:merge(Opts, Extra)),
            Kept = census(proplists:get_value(main, store_list(Opts))),
            say("STORAGE retention ~s: deleted=~p, main store keeps ~.1f MB:",
                [Label, maps:get(deleted, Rep, Rep),
                 lists:sum([ B || {_, B} <- maps:values(Kept) ]) / 1048576]),
            [ say("  ~-40s rows=~-8w bytes=~w", [lists:flatten(io_lib:format("~p", [name(F, PidNames)])), N, B])
            || {F, {N, B}} <- lists:sort(maps:to_list(Kept)) ]
        end,
        [{"defaults", #{}}, {"keep0", #{ <<"store-retention-keep-seconds">> => 0 }}]),
    ok.

%%% The crash test.

crash_load(Dir) ->
    hb:init(),
    Opts = opts(Dir),
    _ = hb_http_server:start_node(Opts),
    Procs = procs(Opts),
    ok = file:write_file(Dir ++ "/procs.bin", term_to_binary([ {N, P} || {N, P} <- Procs ])),
    [ begin schedule(P, <<"Seed">>, 0, Opts), compute(P, 0, Opts) end || {_, P} <- Procs ],
    say("CRASH loading ~p", [[ {N, pid(P, Opts)} || {N, P} <- Procs ]]),
    crash_loop(Procs, 1, Opts, Dir).

%% Schedule a slot to every process, compute it, and record the slot as
%% acknowledged only after both returned: a slot in the log was confirmed.
crash_loop(Procs, I, Opts, Dir) ->
    [ begin schedule(P, <<"Play">>, I, Opts), compute(P, I, Opts) end || {_, P} <- Procs ],
    ok = file:write_file(Dir ++ "/acked", integer_to_binary(I)),
    crash_loop(Procs, I + 1, Opts, Dir).

crash_verify(Dir) ->
    hb:init(),
    Opts = opts(Dir),
    {ok, Acked} = file:read_file(Dir ++ "/acked"),
    Ack = binary_to_integer(Acked),
    {ok, Bin} = file:read_file(Dir ++ "/procs.bin"),
    Procs = binary_to_term(Bin),
    Results =
        [ crash_verify_one(Name, P, Ack, Opts, Dir) || {Name, P} <- Procs ],
    say("CRASH acked=~p ~p", [Ack, Results]),
    halt(case lists:all(fun(R) -> maps:get(ok, R) end, Results) of true -> 0; false -> 1 end).

crash_verify_one(Name, P, Ack, Opts, Dir) ->
    Pid = pid(P, Opts),
    % The scheduler's head (this also loads its device's modules).
    {ok, Current} = hb_ao:resolve(P, #{ <<"path">> => <<"slot/current">> }, Opts),
    % Every slot up to the head reads back, in order, each naming its slot:
    % held here, or (shipped and pruned) read back from the cold tier.
    Top = hb_util:int(Current),
    Local = length((sched_cache()):list(Pid, Opts)),
    Sorted = lists:seq(0, Top),
    InOrder = lists:all(
            fun(S) ->
                {ok, A} = (sched_cache()):read(Pid, S, Opts),
                hb_util:int(hb_ao:get(<<"slot">>, A, Opts)) =:= S
            end, Sorted),
    % The head, through the node (resumes from its newest checkpoint).
    {THead, Head} = timer:tc(fun() -> published(compute(P, Top, Opts), Opts) end),
    % The same head from the irreplaceable store alone.
    Fresh = (opts(Dir))#{ <<"store">> => [lmdb(Dir ++ "/fresh-" ++ atom_to_list(Name), false)
        | tl(hb_opts:get(<<"store">>, [], Opts))],
        <<"process-workers">> => false, <<"spawn-worker">> => false },
    forget(P, Opts),
    {TFresh, FromIrr} = timer:tc(fun() -> published(compute(P, Top, Fresh), Fresh) end),
    % An old slot after retention at keep-seconds 0: rebuilt by replay.
    _ = hb_store_retention:retain(Opts#{ <<"store-retention-keep-seconds">> => 0 }),
    Old = max(1, Top div 2),
    forget(P, Opts),
    {TOld, OldRes} = timer:tc(fun() -> catch published(await(P, Old, Opts), Opts) end),
    forget(P, Opts),
    {_, OldFresh} = timer:tc(fun() -> catch published(await(P, Old, Fresh), Fresh) end),
    #{ name => Name, top => Top, held_locally => Local, acked_present => Top >= Ack, in_order => InOrder,
       head_equal => Head =:= FromIrr, head_ms => THead div 1000,
       head_from_irreplaceable_ms => TFresh div 1000,
       old_slot => Old, old_slot_equal => OldRes =:= OldFresh, old_slot_replay_ms => TOld div 1000,
       ok => Top >= Ack andalso InOrder andalso Head =:= FromIrr andalso OldRes =:= OldFresh }.

%% A slot's state, waiting out a rebuild of an old slot (202, `hb_history').
await(P, N, Opts) ->
    case hb_ao:resolve(P, #{ <<"path">> => <<"compute">>, <<"slot">> => N }, Opts) of
        {ok, State} -> State;
        {error, #{ <<"status">> := 202 }} -> timer:sleep(200), await(P, N, Opts)
    end.

%% Drop the process's worker and the in-memory hot cache, so the next
%% compute starts from what the stores hold.
forget(P, Opts) ->
    lists:foreach(
        fun(Name) ->
            case hb_name:lookup(Name) of
                W when is_pid(W) ->
                    Ref = erlang:monitor(process, W), exit(W, kill),
                    receive {'DOWN', Ref, _, _, _} -> ok end;
                _ -> ok
            end
        end,
        [pid(P, Opts), {process_worker, pid(P, Opts)}]),
    catch ets:delete_all_objects(lib_process_cache_hot),
    ok.

%% A state's published keys, without what differs between two computes of
%% the same slot (the node-local bookkeeping).
published(State, Opts) ->
    L = hb_cache:ensure_all_loaded(hb_private:reset(State), Opts),
    maps:without([<<"snapshot">>, <<"process">>, <<"commitments">>], L).

sched_cache() -> lib([{write_spawn, 2}, {signature_ids, 2}, {find_assigned, 4}]).

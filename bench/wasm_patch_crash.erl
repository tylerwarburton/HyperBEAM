-module(wasm_patch_crash).
%% kill -9 test for a wasm-patch@1.0 process (Lua runtime, AOT): schedule N
%% slots once, then compute slot by slot while the driver kills the VM at random
%% points; a final run computes to the end and checks every slot's state.
%%   erl ... -eval 'wasm_patch_crash:schedule(Dir, N)'   then   wasm_patch_crash:compute(Dir)   then   wasm_patch_crash:check(Dir)
-export([schedule/2, compute/1, check/1]).

opts(Dir, Wallet) ->
    #{
        <<"hashpath">> => ignore,
        <<"debug-print">> => false,
        <<"store">> => #{ <<"store-module">> => hb_store_lmdb, <<"name">> => list_to_binary(Dir ++ "/store"), <<"capacity">> => 1024 * 1024 * 1024 },
        <<"priv-wallet">> => Wallet,
        <<"spawn-worker">> => false,
        <<"process-workers">> => false,
        <<"process-delta-cache">> => true,
        <<"process-snapshot-target">> => false,
        <<"process_snapshot_slots">> => 20,
        <<"process-delta-checkpoint-slots">> => 20,
        <<"wasm-patch-aot">> => true,
        <<"wasm-patch-aot-dir">> => "_build/crash-aot"
    }.

script() ->
    <<
        "Count = Count or 0\n"
        "Bal = Bal or {}\n"
        "function compute(req)\n"
        "  local body = req.body or req\n"
        "  Count = Count + 1\n"
        "  local who = 'acct-' .. tostring(Count % 37)\n"
        "  Bal[who] = (Bal[who] or 0) + Count\n"
        "  if body.action == 'Fail' then error('refused') end\n"
        "  local sum = 0\n"
        "  for _, v in pairs(Bal) do sum = sum + v end\n"
        "  return { patches = { { path = '/count', value = tostring(Count) },\n"
        "      { path = '/' .. who, value = tostring(Bal[who]) },\n"
        "      { path = '/sum', value = tostring(sum) } },\n"
        "    results = { output = { data = tostring(Count) } } }\n"
        "end\n"
    >>.

schedule(Dir, N) ->
    hb:init(),
    Wallet = ar_wallet:new(),
    ok = file:write_file(Dir ++ "/wallet", term_to_binary(Wallet)),
    Opts = opts(Dir, Wallet),
    {ok, Runtime} = file:read_file(os:getenv("HB_WASM_LUA")),
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    Process = hb_message:commit(#{
        <<"device">> => <<"process@1.0">>, <<"type">> => <<"Process">>,
        <<"scheduler-device">> => <<"scheduler@1.0">>,
        <<"execution-device">> => <<"wasm-patch@1.0">>,
        <<"image">> => Runtime,
        <<"module">> => #{ <<"content-type">> => <<"application/lua">>, <<"body">> => script() },
        <<"authority">> => [Address], <<"scheduler-location">> => Address
    }, Opts),
    {ok, _} = hb_cache:write(Process, Opts),
    ok = file:write_file(Dir ++ "/process", term_to_binary(Process)),
    ProcID = hb_message:id(Process, all, Opts),
    lists:foreach(fun(I) ->
        Action = case I rem 13 of 0 -> <<"Fail">>; _ -> <<"Increment">> end,
        {ok, _} = hb_ao:resolve(Process, hb_message:commit(#{
            <<"path">> => <<"schedule">>, <<"method">> => <<"POST">>,
            <<"body">> => hb_message:commit(#{ <<"target">> => ProcID,
                <<"type">> => <<"Message">>, <<"action">> => Action }, Opts)
        }, Opts), Opts)
    end, lists:seq(1, N)),
    ok = file:write_file(Dir ++ "/n", integer_to_binary(N)),
    io:format("scheduled ~p~n", [N]).

load(Dir) ->
    hb:init(),
    {ok, W} = file:read_file(Dir ++ "/wallet"),
    {ok, P} = file:read_file(Dir ++ "/process"),
    {ok, N} = file:read_file(Dir ++ "/n"),
    Wallet = binary_to_term(W),
    {binary_to_term(P), binary_to_integer(N), opts(Dir, Wallet)}.

compute(Dir) ->
    {Process, N, Opts} = load(Dir),
    lists:foreach(fun(Slot) ->
        {ok, _} = hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, Opts),
        io:format("slot ~p~n", [Slot])
    end, lists:seq(0, N - 1)),
    io:format("computed to ~p~n", [N - 1]).

%% Every slot's count and sum as a crash-free model predicts them: the module's
%% Count advances on every slot, failed or not, but a failed slot's writes are
%% rolled back, so Count only counts successful slots.
check(Dir) ->
    {Process, N, Opts} = load(Dir),
    {_, Bad} = lists:foldl(fun(Slot, {{Count, Bal}, Errs}) ->
        Fail = (Slot + 1) rem 13 == 0,
        Model =
            case Fail of
                true -> {Count, Bal};
                false ->
                    C = Count + 1,
                    Who = C rem 37,
                    {C, Bal#{ Who => maps:get(Who, Bal, 0) + C }}
            end,
        {ok, S} = hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, Opts),
        {MC, MB} = Model,
        Want = {integer_to_binary(MC), integer_to_binary(lists:sum(maps:values(MB)))},
        Got = {hb_ao:get(<<"count">>, S, Opts), hb_ao:get(<<"sum">>, S, Opts)},
        {Model, case Want == Got orelse MC == 0 of true -> Errs; false -> [{Slot, Want, Got} | Errs] end}
    end, {{0, #{}}, []}, lists:seq(0, N - 1)),
    io:format("CHECK ~p slots, ~p mismatches ~p~n", [N, length(Bad), lists:sublist(lists:reverse(Bad), 5)]).

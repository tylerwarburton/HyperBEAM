%%% @doc Luerl's mark-and-sweep collector (`luerl_heap:gc/1', Luerl 1.3.0) with
%%% its seen sets held in maps instead of `ordsets'. Adding to an ordset is
%%% linear in its size, so Luerl's own collector is quadratic in the number of
%%% live tables: 5-10 s for 20,000 of them, and ~300 ms a collection on the
%%% orderbook's network test. With no Lua running it visits the same objects
%%% in the same order and frees the same ones, so it returns the same state
%%% (`same_result_test'). From inside running Lua it also keeps what the
%%% suspended frames' instructions hold (an open generic `for''s iterator
%%% state), which Luerl's own collector freed.
-module(hb_luerl_gc).
-export([gc/1, collectgarbage/3]).
%% Luerl is built with full map keys, so its table store is maps.
-define(HAS_FULL_KEYS, true).
-include_lib("luerl/include/luerl.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(gct, {t, s}).

%% @doc `collectgarbage' for Lua: `collect' (or no option) collects, every
%% other option is ignored, as in `luerl_lib_basic:collectgarbage/3'.
collectgarbage(_, [], St) -> {[], gc(St)};
collectgarbage(_, [<<"collect">> | _], St) -> {[], gc(St)};
collectgarbage(_, _, St) -> {[], St}.

%% @doc Collect the garbage of a Luerl state.
gc(#luerl{tabs = #tstruct{data = Tt0, free = Tf0} = Tab0,
          envs = #tstruct{data = Et0, free = Ef0} = Env0,
          usds = #tstruct{data = Ut0, free = Uf0} = Usd0,
          fncs = #tstruct{data = Ft0, free = Ff0} = Fnc0,
          g = G, stk = Stk, cs = Cs, meta = Meta} = St) ->
    Root = [Meta#meta.nil, Meta#meta.boolean, Meta#meta.number,
            Meta#meta.string, G | Stk],
    {SeenT, SeenE, SeenU, SeenF} =
        mark(Root, [Cs], #gct{t = Tt0, s = #{}}, #gct{t = Et0, s = #{}},
             #gct{t = Ut0, s = #{}}, #gct{t = Ft0, s = #{}}),
    {Tf1, Tt1} = filter(SeenT, Tf0, Tt0),
    {Ef1, Et1} = filter(SeenE, Ef0, Et0),
    {Uf1, Ut1} = filter(SeenU, Uf0, Ut0),
    {Ff1, Ft1} = filter(SeenF, Ff0, Ft0),
    St#luerl{
        tabs = Tab0#tstruct{data = Tt1, free = Tf1},
        envs = Env0#tstruct{data = Et1, free = Ef1},
        usds = Usd0#tstruct{data = Ut1, free = Uf1},
        fncs = Fnc0#tstruct{data = Ft1, free = Ff1}
    }.

mark([{in_table, _} | Todo], More, GcT, GcE, GcU, GcF) ->
    mark(Todo, More, GcT, GcE, GcU, GcF);
mark([#tref{i = T} | Todo], More, #gct{t = Tt, s = Ts0} = GcT, GcE, GcU, GcF) ->
    case is_map_key(T, Ts0) of
        true -> mark(Todo, More, GcT, GcE, GcU, GcF);
        false ->
            #table{a = Arr, d = Dict, meta = Meta} = ?GET_TABLE(T, Tt),
            Aes = array:sparse_to_list(Arr),
            Des = ttdict:to_list(Dict),
            mark([Meta | Todo], [[{in_table, T}], Des, Aes, [{in_table, -T}] | More],
                 GcT#gct{s = Ts0#{T => true}}, GcE, GcU, GcF)
    end;
mark([#eref{i = F} | Todo], More, GcT, #gct{t = Et, s = Es0} = GcE, GcU, GcF) ->
    case is_map_key(F, Es0) of
        true -> mark(Todo, More, GcT, GcE, GcU, GcF);
        false ->
            Ses = tuple_to_list(?GET_TABLE(F, Et)),
            mark(Todo, [Ses | More], GcT, GcE#gct{s = Es0#{F => true}}, GcU, GcF)
    end;
mark([#usdref{i = U} | Todo], More, GcT, GcE, #gct{s = Us0} = GcU, GcF) ->
    case is_map_key(U, Us0) of
        true -> mark(Todo, More, GcT, GcE, GcU, GcF);
        false -> mark(Todo, More, GcT, GcE, GcU#gct{s = Us0#{U => true}}, GcF)
    end;
mark([#funref{i = F, env = Erefs} | Todo], More, GcT, GcE, GcU,
     #gct{t = Ft0, s = Fs0} = GcF) ->
    case is_map_key(F, Fs0) of
        true -> mark(Todo, [Erefs | More], GcT, GcE, GcU, GcF);
        false ->
            Fdef = ?GET_TABLE(F, Ft0),
            mark([Fdef | Todo], [Erefs | More], GcT, GcE, GcU,
                 GcF#gct{s = Fs0#{F => true}})
    end;
mark([#lua_func{funrefs = Funrefs} | Todo], More, GcT, GcE, GcU, GcF) ->
    mark(Todo, [Funrefs | More], GcT, GcE, GcU, GcF);
%% A suspended frame's instructions are roots too, which Luerl's collector
%% misses: an open generic `for' keeps its iterator function, state and
%% control value inside the loop instructions (`{gfor_call, ...}',
%% `{gfor_loop, ...}') in the frame's continuation, so a collection from the
%% loop body freed the table only the iterator referenced. A frame's
%% arguments are kept as well.
mark([#call_frame{func = Funref, args = Args, lvs = Lvs, env = Env, is = Is, cont = Cont} | Todo],
     More0, GcT, GcE, GcU, GcF) ->
    More1 =
        [tuple_to_list(Lv) || Lv <- Lvs, is_tuple(Lv)]
            ++ [Env, code_refs(Is, []), code_refs(Cont, []), list(Args) | More0],
    mark([Funref | Todo], More1, GcT, GcE, GcU, GcF);
mark([#loop_frame{lvs = Lvs, stk = Stk, env = Env, is = Is, cont = Cont} | Todo],
     More0, GcT, GcE, GcU, GcF) ->
    More1 =
        [tuple_to_list(Lv) || Lv <- Lvs, is_tuple(Lv)]
            ++ [Stk, Env, code_refs(Is, []), code_refs(Cont, []) | More0],
    mark(Todo, More1, GcT, GcE, GcU, GcF);
mark([#erl_func{} | Todo], More, GcT, GcE, GcU, GcF) ->
    mark(Todo, More, GcT, GcE, GcU, GcF);
mark([#erl_mfa{} | Todo], More, GcT, GcE, GcU, GcF) ->
    mark(Todo, More, GcT, GcE, GcU, GcF);
mark([#thread{} | Todo], More, GcT, GcE, GcU, GcF) ->
    mark(Todo, More, GcT, GcE, GcU, GcF);
mark([#userdata{meta = Meta} | Todo], More, GcT, GcE, GcU, GcF) ->
    mark([Meta | Todo], More, GcT, GcE, GcU, GcF);
mark([{K, V} | Todo], More, GcT, GcE, GcU, GcF) ->
    mark([K, V | Todo], More, GcT, GcE, GcU, GcF);
mark([_ | Todo], More, GcT, GcE, GcU, GcF) ->
    mark(Todo, More, GcT, GcE, GcU, GcF);
mark([], [M | More], GcT, GcE, GcU, GcF) ->
    mark(M, More, GcT, GcE, GcU, GcF);
mark([], [], #gct{s = St}, #gct{s = Se}, #gct{s = Su}, #gct{s = Sf}) ->
    {St, Se, Su, Sf}.

%% @doc The heap references held in instructions: anything Luerl allocates
%% that the emulator has put into the code it is running.
code_refs(#tref{} = Ref, Acc) -> [Ref | Acc];
code_refs(#funref{} = Ref, Acc) -> [Ref | Acc];
code_refs(#eref{} = Ref, Acc) -> [Ref | Acc];
code_refs(#usdref{} = Ref, Acc) -> [Ref | Acc];
code_refs([Head | Tail], Acc) -> code_refs(Tail, code_refs(Head, Acc));
code_refs(Tuple, Acc) when is_tuple(Tuple) ->
    lists:foldl(fun code_refs/2, Acc, tuple_to_list(Tuple));
code_refs(_, Acc) -> Acc.

list(List) when is_list(List) -> List;
list(_) -> [].

%% @doc Free what was not seen, adding it to the free list in the order
%% Luerl's own collector does.
filter(Seen, Free0, Data0) ->
    Free1 =
        ?FOLD_TABLES(
            fun(K, _, Free) ->
                case is_map_key(K, Seen) of
                    true -> Free;
                    false -> [K | Free]
                end
            end,
            Free0,
            Data0
        ),
    {Free1, ?FILTER_TABLES(fun(K, _) -> is_map_key(K, Seen) end, Data0)}.

%%% Tests

%% @doc The same state as Luerl's collector, on a heap with garbage, live
%% tables, closures with upvalues and metatables.
same_result_test() ->
    {ok, _, St0} =
        luerl:do_dec(
            <<
                "Live = {}\n"
                "for i = 1, 300 do Live[i] = { n = i, t = { i } } end\n"
                "for i = 1, 500 do local g = { i, { i } } end\n"
                "local up = { 'kept' }\n"
                "function Keep() return up[1] end\n"
                "setmetatable(Live, { __index = function() return 0 end })\n"
                "local f = function() return { 'dropped' } end\n"
                "f()\n"
            >>,
            luerl:init()
        ),
    ?assertEqual(luerl_heap:gc(St0), gc(St0)),
    {ok, [<<"kept">>], _} = luerl:call_function_dec([<<"Keep">>], [], gc(St0)).

%% @doc Linear rather than quadratic: 20,000 live tables in well under a
%% second (Luerl's own collector takes several).
large_heap_test() ->
    {ok, _, St} =
        luerl:do_dec(
            <<"Live = {} for i = 1, 20000 do Live[i] = { i } end">>,
            luerl:init()
        ),
    {Micros, _} = timer:tc(fun() -> gc(St) end),
    ?assert(Micros < 1000000).

%% @doc A collection from inside an open generic `for' keeps what only the
%% iterator references, whether the body collects directly or through a call.
collect_inside_generic_for_test() ->
    Install =
        fun(St) ->
            {ok, St1} =
                luerl:set_table_keys(
                    [<<"collectgarbage">>],
                    #erl_mfa{ m = ?MODULE, f = collectgarbage, a = nil },
                    St
                ),
            St1
        end,
    Script =
        <<
            "local function f() collectgarbage('collect') end\n"
            "local s = 0\n"
            "for i, v in ipairs({ 10, 20, 30 }) do f(); s = s + v end\n"
            "for k, v in pairs({ a = 1, b = 2 }) do collectgarbage('collect'); s = s + v end\n"
            "return s\n"
        >>,
    ?assertMatch({ok, [63], _}, luerl:do_dec(Script, Install(luerl:init()))).

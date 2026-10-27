%%% @doc Request-only Lua with pure in-VM atomic batches. Public patches and
%%% checkpoints use `lua-patch@1.0' semantics; the stricter sandbox is
%%% mandatory.
%%%
%%% `ao.atomic(fn)' runs `fn' against the VM as it stands. When `fn' returns
%%% `true, ...' the batch commits and those values are returned. Anything else
%%% (an error, `false, reason', or an invalid result) restores the VM to what it
%%% was before the call and returns `false, reason'. Host calls (`ao.get',
%%% `ao.resolve', `ao.set', `ao.event') are refused inside a batch, since their
%%% effects are outside the VM and cannot be rolled back.
%%%
%%% Formerly `lua@5.3c' on the old fork. On v0.11 a snapshot loses every Erlang
%%% function in the VM (`dev_lua:decode_snapshot/3'), so a restored VM has its
%%% atomic primitive and host guards installed again after the version witness
%%% is checked.
-module(dev_lua_atomic).
-implements(<<"lua-atomic@1.0">>).
-export([device_info/1, init/3, compute/4, snapshot/3, normalize/3, functions/3]).
%% Called by Luerl as `#erl_mfa{}' host functions, never as device keys.
-export([lua_atomic/3, lua_collect/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("luerl/include/luerl.hrl").

-define(PATCH_LUA, <<"lua-patch@1.0">>).
-define(VERSION, {lua_atomic, version}).
-define(DEPTH, {lua_atomic, depth}).
-define(GC_PENDING, {lua_atomic, gc_pending}).
-define(HOST_FUNCTIONS, [<<"get">>, <<"resolve">>, <<"set">>, <<"event">>]).
-define(ATOMIC_MFA, #erl_mfa{ m = ?MODULE, f = lua_atomic, a = nil }).
-define(COLLECT_MFA, #erl_mfa{ m = ?MODULE, f = lua_collect, a = nil }).

%% @doc Install raw closure callbacks without decoding the closure environment.
%% Every read and write is raw, so no metamethod a module set on `_G' or `ao'
%% runs while the host functions are in hand.
install_atomic(State) ->
    S0 = luerl:put_private(?VERSION, 1, ensure_ao(State)),
    S1 = lists:foldl(
        fun guard_host/2,
        luerl:put_private(?DEPTH, 0, S0),
        ?HOST_FUNCTIONS
    ),
    S2 = luerl_heap:raw_set_table_key(raw_ao(S1), <<"atomic">>, ?ATOMIC_MFA, S1),
    S3 = raw_set(raw_ao(S2), <<"atomic_version">>, 1, S2),
    preload(S3).

%% @doc A contract can replace the global `ao' with something that is not a
%% table; the VM is then armed with a new, empty one rather than refused.
ensure_ao(State) ->
    case raw_ao(State) of
        #tref{} -> State;
        _ -> raw_set(State#luerl.g, <<"ao">>, #{}, State)
    end.

%% @doc Replace `collectgarbage' (`lua-device-preload'). It runs before any
%% module code, so no module can keep Luerl's own. `ao.atomic' and this
%% `collectgarbage' are `#erl_mfa{}' values, and the preloaded state names
%% both, so restore (which allows the host functions of a new state and drops
%% every other) keeps them wherever a contract keeps them, and drops Luerl's
%% own `collectgarbage'.
preload(State) ->
    S1 = luerl:put_private({lua_atomic, host}, [?ATOMIC_MFA, ?COLLECT_MFA], State),
    luerl_heap:raw_set_table_key(S1#luerl.g, <<"collectgarbage">>, ?COLLECT_MFA, S1).

raw_ao(State) ->
    luerl_heap:raw_get_table_key(State#luerl.g, <<"ao">>, State).

raw_set(Table, Key, Value, State) ->
    {Encoded, S1} = luerl:encode(Value, State),
    luerl_heap:raw_set_table_key(Table, Key, Encoded, S1).

%% @doc Whether a batch is running.
in_batch(State) ->
    case private(?DEPTH, State) of
        Depth when is_integer(Depth) -> Depth > 0;
        _ -> false
    end.

%% @doc `collectgarbage' that only asks for a collection; `compute/4' runs one
%% when the call returns, with no Lua on the stack. Two reasons:
%% - Inside a batch (`luerl:call_function/3', a new call stack) the frames of
%%   the suspended Lua beneath it are not roots, so collecting there freed the
%%   caller's locals.
%% - A full collection costs time in the size of the whole heap, and the
%%   orderbook's network test asks for one after every simulated message,
%%   thousands in one compute (on Luerl's own quadratic collector, ~300 ms
%%   each; see `hb_luerl_gc'). One per compute is what a contract that
%%   collects at the end of its handler costs anyway.
%% Every other option is ignored, as Luerl's own `collectgarbage' ignores it.
lua_collect(_, Args, State) ->
    case Args of
        [] -> {[], luerl:put_private(?GC_PENDING, true, State)};
        [<<"collect">> | _] -> {[], luerl:put_private(?GC_PENDING, true, State)};
        _ -> {[], State}
    end.

%% @doc Run the collection Lua asked for during a compute, if it asked.
collect_pending({Status, Msg}, Opts) when is_map(Msg) ->
    case hb_private:get(<<"state">>, Msg, Opts) of
        not_found -> {Status, Msg};
        State ->
            case private(?GC_PENDING, State) of
                true ->
                    Collected = hb_luerl_gc:gc(luerl:delete_private(?GC_PENDING, State)),
                    {Status, hb_private:set(Msg, <<"state">>, Collected, Opts)};
                _ -> {Status, Msg}
            end
    end;
collect_pending(Other, _Opts) ->
    Other.

%% @doc Wrap host entries before the VM is exposed to a caller. The library is
%% installed after module loading with raw writes (`dev_lua_lib'), and no Lua
%% runs between that and this, so Lua never holds an unwrapped host function.
%% The mandatory sandbox removes other host-effect sources, and VMs from other
%% devices are rejected by `init/3'.
guard_host(Name, State) ->
    AO = raw_ao(State),
    case luerl_heap:raw_get_table_key(AO, Name, State) of
        nil -> State;
        Original -> guard_host(AO, Name, Original, State)
    end.

guard_host(AO, Name, Original, State) ->
    Wrapper = fun(Args, Current) ->
        case in_batch(Current) of
            true -> luerl_lib:lua_error(
                {assert_error, <<"Host calls are forbidden inside ao.atomic">>},
                Current
            );
            false ->
                case luerl:call_function(Original, Args, Current) of
                    {ok, Values, Next} -> {Values, Next};
                    {lua_error, Error, Next} -> luerl_lib:lua_error(Error, Next)
                end
        end
    end,
    raw_set(AO, Name, Wrapper, State).

%% @doc A version witness prevents importing an unrestricted VM into this device.
atomic_enabled(State) ->
    private(?VERSION, State) =:= 1.

%% @doc Read optional VM metadata without depending on a record layout.
private(Key, State) ->
    try luerl:get_private(Key, State)
    catch error:{badkey, Key} -> undefined end.

%% @doc Keep savepoints on the Erlang stack, never in serializable VM metadata.
lua_atomic(_, [Closure], Before) ->
    Depth = private(?DEPTH, Before),
    case Depth < 16 of
        true ->
            Working = luerl:put_private(?DEPTH, Depth + 1, Before),
            finish(luerl:call_function(Closure, [], Working), Before, Depth);
        false -> {[false, <<"Atomic nesting limit exceeded">>], Before}
    end;
lua_atomic(_, _, Before) ->
    {[false, <<"ao.atomic expects one function">>], Before}.

%% @doc Only committed return references belong to the returned VM. Failures
%% carry a bounded binary; Erlang resource accounting is outside the savepoint.
finish({ok, [true | Values], After}, _Before, Depth) ->
    {[true | Values], luerl:put_private(?DEPTH, Depth, After)};
finish({ok, [false, Reason], _After}, Before, _Depth)
        when is_binary(Reason), byte_size(Reason) =< 4096 ->
    {[false, Reason], Before};
finish({lua_error, _, _After}, Before, _Depth) ->
    {[false, <<"Atomic callback raised an error">>], Before};
finish(_, Before, _Depth) ->
    {[false, <<"Invalid atomic callback result">>], Before}.

%% @doc Dispatch Lua calls through the atomic-capable runtime. The `excludes'
%% list is constant, as in `lua-patch@1.0', so a slot does not walk every
%% published key.
device_info(_Base) ->
    #{
        default => fun compute/4,
        direct_message_keys => true,
        excludes =>
            [
                <<"id">>,
                <<"commitments">>,
                <<"committers">>,
                <<"keys">>,
                <<"path">>,
                <<"set">>,
                <<"remove">>,
                <<"verify">>,
                <<"encode">>,
                <<"decode">>,
                <<"lua_atomic">>,
                <<"lua_collect">>
            ]
    }.

%% @doc Initialize under the mandatory sandbox before any module can capture
%% host functions. Existing VMs must originate from this runtime.
init(Base, Req, Opts) ->
    case hb_private:get(<<"state">>, Base, Opts) of
        not_found ->
            case hb_ao:raw(?PATCH_LUA, <<"init">>, Base, Req, strict(Opts)) of
                {ok, Initialized} -> arm(Initialized, Opts);
                Error -> Error
            end;
        State ->
            case atomic_enabled(State) of
                true -> {ok, Base};
                false -> refuse()
            end
    end.

%% @doc Install the atomic primitive and host guards in a VM built by this
%% device: a new one, or one restored from a snapshot, whose Erlang functions
%% the restore drops.
arm(Base, Opts) ->
    State = hb_private:get(<<"state">>, Base, Opts),
    {ok, hb_private:set(Base, <<"state">>, install_atomic(State), Opts)}.

refuse() ->
    {error, <<"lua-atomic@1.0 requires a VM created by lua-atomic@1.0">>}.

%% @doc The node options with this device's sandbox added. `dev_lua' applies it
%% on top of the node's minimum sandbox, before any module loads.
strict(Opts) ->
    Opts#{
        <<"lua-device-sandbox">> => sandbox(),
        <<"lua-device-preload">> => fun preload/1
    }.

%% @doc Restrict both global names and package aliases before module loading.
%% `math.random' is seeded per node, so it is refused whatever the process sets
%% `requires-determinism' to (`os' is gone entirely).
sandbox() ->
    [
        {[math, random], <<"sandboxed">>},
        {[math, randomseed], <<"sandboxed">>},
        {[loadfile], <<"sandboxed">>},
        {[dofile], <<"sandboxed">>},
        {[print], <<"sandboxed">>},
        {[eprint], <<"sandboxed">>},
        {[package, searchers], <<"sandboxed">>},
        {[package, searchpath], <<"sandboxed">>},
        {[io], <<"sandboxed">>},
        {[os], <<"sandboxed">>},
        {[debug], <<"sandboxed">>},
        {[package, loaded, io], <<"sandboxed">>},
        {[package, loaded, os], <<"sandboxed">>},
        {[package, loaded, debug], <<"sandboxed">>}
    ].

%% @doc Execute with the patch protocol after checking the VM witness.
compute(Key, Base, Req, Opts) ->
    case init(Base, Req, Opts) of
        {ok, Ready} ->
            collect_pending(hb_ao:raw(?PATCH_LUA, Key, Ready, Req, Opts), Opts);
        Error -> Error
    end.

%% @doc Persist the ordinary Luerl VM; no savepoint roots are retained.
snapshot(Base, Req, Opts) ->
    hb_ao:raw(?PATCH_LUA, <<"snapshot">>, Base, Req, Opts).

%% @doc Restore, verify the sandbox witness, then arm the restored VM again.
normalize(Base, Req, Opts) ->
    case hb_private:get(<<"state">>, Base, Opts) of
        not_found ->
            case hb_ao:raw(?PATCH_LUA, <<"normalize">>, Base, Req, strict(Opts)) of
                {ok, Restored} ->
                    case hb_private:get(<<"state">>, Restored, Opts) of
                        not_found -> {ok, Restored};
                        State ->
                            case atomic_enabled(State) of
                                true -> arm(Restored, Opts);
                                false -> refuse()
                            end
                    end;
                Error -> Error
            end;
        _ -> init(Base, Req, Opts)
    end.

%% @doc Enumerate functions using the shared VM representation.
functions(Base, Req, Opts) ->
    hb_ao:raw(?PATCH_LUA, <<"functions">>, Base, Req, Opts).

%%% Tests

%% @doc Exercise savepoints through AO-Core, including aliases, nested calls,
%% garbage collection, resource bounds, publication and checkpoint recovery.
%% The second compute runs on a restored VM, so it also proves the primitive
%% and the host guards are installed again after a restore.
atomic_roundtrip_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Script = <<"""
        local count = 0
        local row = { balance = 17 }
        local alias = row
        function compute(req)
            local before = count
            local committed, result, outbox = ao.atomic(function()
                count = count + 1
                row.balance = 19
                return true, { value = count }, { amount = 2 }
            end)
            assert(committed and result.value == before + 1 and outbox.amount == 2, 'assert(committed and result.value == before + 1 and outbox.amount == 2)')
            local refused, reason = ao.atomic(function()
                row.balance = 23
                row = { balance = 29 }
                count = 900
                local inner = ao.atomic(function() count = 901; return true end)
                assert(inner, 'assert(inner)')
                collectgarbage('collect')
                return false, 'refused'
            end)
            assert(not refused and reason == 'refused', 'assert(not refused and reason == refused)')
            assert(row == alias and row.balance == 19 and count == before + 1, 'assert(row == alias and row.balance == 19 and count == before + 1)')
            local outer = ao.atomic(function()
                local inner = ao.atomic(function()
                    count = 999
                    error({ allocated = true })
                end)
                assert(not inner and count == before + 1, 'assert(not inner and count == before + 1)')
                return true
            end)
            assert(outer, 'assert(outer)')
            local invalid = ao.atomic(function() count = 999; return false, {} end)
            assert(not invalid and count == before + 1, 'assert(not invalid and count == before + 1)')
            local oversized = ao.atomic(function()
                count = 999; return false, string.rep('x', 4097)
            end)
            assert(not oversized and count == before + 1, 'assert(not oversized and count == before + 1)')
            local function nested(n)
                if n == 18 then return true end
                return ao.atomic(function() return nested(n + 1) end)
            end
            local deep = nested(1)
            assert(not deep, 'assert(not deep)')
            for _, fn in ipairs({ao.resolve, ao.get, ao.set, ao.event}) do
                local escaped = ao.atomic(function()
                    count = 999
                    fn('must never run')
                    return true
                end)
                assert(not escaped and count == before + 1, 'assert(not escaped and count == before + 1)')
            end
            assert(type(io) == 'string' and type(os) == 'string', 'assert(type(io) == string and type(os) == string)')
            assert(type(debug) == 'string' and type(print) == 'string', 'assert(type(debug) == string and type(print) == string)')
            assert(type(require('io')) == 'string', 'assert(type(require(io)) == string)')
            assert(type(require('os')) == 'string', 'assert(type(require(os)) == string)')
            assert(type(require('debug')) == 'string', 'assert(type(require(debug)) == string)')
            assert(type(loadfile) == 'string' and type(dofile) == 'string', 'assert(type(loadfile) == string and type(dofile) == string)')
            assert(ao.atomic_version == 1, 'assert(ao.atomic_version == 1)')
            local legal = ao.get('hello', {hello = 'world'})
            assert(legal ~= nil, 'assert(legal ~= nil)')
            return { patches = {{path='/count', value=tostring(count)}} }
        end
        """>>,
    Base = base(Script),
    {ok, First} = hb_ao:resolve(Base, <<"compute">>, Opts),
    ?assertEqual(<<"1">>, hb_ao:get(<<"count">>, First, Opts)),
    {ok, Snapshot} = hb_ao:resolve(First, <<"snapshot">>, Opts),
    Cold = hb_ao:set(hb_private:reset(First), <<"snapshot">>, Snapshot, Opts),
    {ok, Restored} = hb_ao:resolve(Cold, <<"normalize">>, Opts),
    {ok, Second} = hb_ao:resolve(Restored, <<"compute">>, Opts),
    ?assertEqual(<<"2">>, hb_ao:get(<<"count">>, Second, Opts)).

%% @doc Collection inside `ao.atomic' must not free the suspended caller's
%% locals, whether the batch commits or rolls back. Outside a batch it still
%% runs, and keeps the caller's locals.
atomic_gc_keeps_outer_locals_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Script = <<"""
        function compute(req)
            local mine = { owner = 'alice', balance = 100 }
            local committed = ao.atomic(function()
                collectgarbage('collect')
                return true
            end)
            local refused = ao.atomic(function()
                collectgarbage('collect')
                return false, 'refused'
            end)
            for i = 1, 400 do
                ao.atomic(function() collectgarbage('step', 5); return true end)
            end
            collectgarbage('collect')
            Stash = {}
            for i = 1, 60 do Stash[i] = { owner = 'mallory', balance = i } end
            local seen = table.concat({ tostring(committed), tostring(refused),
                tostring(mine.owner), tostring(mine.balance) }, ' ')
            return { patches = {{ path = '/seen', value = seen }} }
        end
        """>>,
    {ok, Result} = hb_ao:resolve(base(Script), <<"compute">>, Opts),
    ?assertEqual(<<"true false alice 100">>, hb_ao:get(<<"seen">>, Result, Opts)).

%% @doc Unrestricted `lua-patch@1.0' state cannot acquire the sandbox witness by
%% changing its device name, live or through a snapshot.
legacy_vm_rejected_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Base = #{
        <<"device">> => ?PATCH_LUA,
        <<"module">> => #{ <<"content-type">> => <<"application/lua">>, <<"body">> => <<"function compute() return {} end">> }
    },
    {ok, Legacy} = hb_ao:resolve(Base, <<"init">>, Opts),
    Switched = hb_ao:set(Legacy, <<"device">>, <<"lua-atomic@1.0">>, Opts),
    ?assertMatch({error, _}, hb_ao:resolve(Switched, <<"compute">>, Opts)),
    {ok, Snapshot} = hb_ao:resolve(Legacy, <<"snapshot">>, Opts),
    Cold = hb_ao:set(hb_private:reset(Switched), <<"snapshot">>, Snapshot, Opts),
    ?assertMatch({error, _}, hb_ao:resolve(Cold, <<"normalize">>, Opts)).

%% @doc Sandbox restrictions apply before module code and cannot be disabled.
module_sandbox_test() ->
    hb:init(),
    Base = #{
        <<"device">> => <<"lua-atomic@1.0">>,
        <<"sandbox">> => false,
        <<"module">> => #{ <<"content-type">> => <<"application/lua">>, <<"body">> => <<"""
            local alias = require('io')
            assert(type(alias) == 'string', 'assert(type(alias) == string)')
            assert(type(require('os')) == 'string', 'assert(type(require(os)) == string)')
            function compute() return { patches = {{path='/safe',value='yes'}} } end
            """>> }
    },
    Opts = #{ <<"hashpath">> => ignore, <<"lua-minimum-sandbox">> => false },
    {ok, Result} = hb_ao:resolve(Base, <<"compute">>, Opts),
    ?assertEqual(<<"yes">>, hb_ao:get(<<"safe">>, Result, Opts)).

%% @doc `collectgarbage' kept from module load and `ao.atomic' kept from a
%% compute work the same on a live VM and on a restored one.
host_aliases_survive_restore_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Script = <<"""
        local gc = collectgarbage
        function compute(req)
            Atomic = Atomic or ao.atomic
            local collected = pcall(gc, 'collect')
            local ran, committed = pcall(Atomic, function() return true end)
            return { patches = {{ path = '/seen',
                value = tostring(collected) .. ' ' .. tostring(ran and committed) }} }
        end
        """>>,
    {ok, First} = hb_ao:resolve(base(Script), <<"compute">>, Opts),
    ?assertEqual(<<"true true">>, hb_ao:get(<<"seen">>, First, Opts)),
    {ok, Snapshot} = hb_ao:resolve(First, <<"snapshot">>, Opts),
    Cold = hb_ao:set(hb_private:reset(First), <<"snapshot">>, Snapshot, Opts),
    {ok, Restored} = hb_ao:resolve(Cold, <<"normalize">>, Opts),
    {ok, Second} = hb_ao:resolve(Restored, <<"compute">>, Opts),
    ?assertEqual(<<"true true">>, hb_ao:get(<<"seen">>, Second, Opts)).

%% @doc A contract that replaced the global `ao' with a number still restores.
non_table_ao_restores_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Script = <<"""
        function compute(req)
            local kind = type(ao)
            ao = 5
            return { patches = {{ path = '/kind', value = kind }} }
        end
        """>>,
    {ok, First} = hb_ao:resolve(base(Script), <<"compute">>, Opts),
    {ok, Snapshot} = hb_ao:resolve(First, <<"snapshot">>, Opts),
    Cold = hb_ao:set(hb_private:reset(First), <<"snapshot">>, Snapshot, Opts),
    {ok, Restored} = hb_ao:resolve(Cold, <<"normalize">>, Opts),
    {ok, Second} = hb_ao:resolve(Restored, <<"compute">>, Opts),
    ?assertEqual(<<"table">>, hb_ao:get(<<"kind">>, Second, Opts)).

%% @doc `collectgarbage('collect')' costs one collection per compute, however
%% often Lua asks: the orderbook's test suite drives thousands of contract
%% calls, each ending in a collection, inside one compute, and on stock Luerl
%% each full collection of a large heap took up to ~300 ms (the suite ran past
%% nginx's 300 s). The garbage is still freed when the compute returns.
collect_once_per_compute_test_() ->
    {timeout, 120, fun() ->
        hb:init(),
        Opts = #{ <<"hashpath">> => ignore },
        Script = <<"""
            Live = Live or {}
            function compute(req)
                if #Live == 0 then for i = 1, 20000 do Live[i] = { i } end end
                for i = 1, 20000 do local t = { i } end
                for i = 1, 50 do collectgarbage('collect') end
                return { patches = {{ path = '/n', value = tostring(#Live) }} }
            end
            """>>,
        {Micros, {ok, Result}} =
            timer:tc(fun() -> hb_ao:resolve(base(Script), <<"compute">>, Opts) end),
        ?assertEqual(<<"20000">>, hb_ao:get(<<"n">>, Result, Opts)),
        ?assert(Micros < 5000000),
        #luerl{ tabs = #tstruct{ data = Tables } } =
            hb_private:get(<<"state">>, Result, Opts),
        ?assert(map_size(Tables) < 30000)
    end}.

%% @doc `math.random' is unavailable even to a base that does not ask for
%% determinism, from module load on.
no_random_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Script = <<"""
        local early = type(math.random)
        function compute(req)
            return { patches = {{ path = '/random',
                value = early .. ' ' .. type(math.random) .. ' ' .. type(math.randomseed) }} }
        end
        """>>,
    Base = (base(Script))#{ <<"requires-determinism">> => false },
    {ok, Result} = hb_ao:resolve(Base, <<"compute">>, Opts),
    ?assertEqual(<<"string string string">>, hb_ao:get(<<"random">>, Result, Opts)).

%% @doc A module that keeps its own `collectgarbage' while it loads gets the
%% batch-safe one: collecting through the alias inside a batch, committed or
%% rolled back, leaves the caller's locals alive.
module_gc_alias_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Script = <<"""
        local gc = collectgarbage
        function compute(req)
            local mine = { owner = 'alice', balance = 100 }
            ao.atomic(function() gc('collect'); return true end)
            ao.atomic(function() gc('collect'); return false, 'refused' end)
            local seen = tostring(mine.owner) .. ' ' .. tostring(mine.balance)
            return { patches = {{ path = '/seen', value = seen }} }
        end
        """>>,
    {ok, Result} = hb_ao:resolve(base(Script), <<"compute">>, Opts),
    ?assertEqual(<<"alice 100">>, hb_ao:get(<<"seen">>, Result, Opts)).

%% @doc A restored VM never holds Luerl's own `collectgarbage', wherever the
%% snapshot put it: the restore drops it like any other host function.
restore_drops_original_gc_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Script = <<"""
        function compute(req)
            return { patches = {{ path = '/orig', value = type(Orig) }} }
        end
        """>>,
    {ok, First} = hb_ao:resolve(base(Script), <<"compute">>, Opts),
    State = hb_private:get(<<"state">>, First, Opts),
    Fresh = luerl:init(),
    #erl_mfa{} = Orig =
        luerl_heap:raw_get_table_key(Fresh#luerl.g, <<"collectgarbage">>, Fresh),
    {ok, Planted} = luerl:set_table_keys([<<"Orig">>], Orig, State),
    Live = hb_private:set(First, <<"state">>, Planted, Opts),
    {ok, Snapshot} = hb_ao:resolve(Live, <<"snapshot">>, Opts),
    Cold = hb_ao:set(hb_private:reset(First), <<"snapshot">>, Snapshot, Opts),
    {ok, Restored} = hb_ao:resolve(Cold, <<"normalize">>, Opts),
    {ok, Second} = hb_ao:resolve(Restored, <<"compute">>, Opts),
    ?assertEqual(<<"nil">>, hb_ao:get(<<"orig">>, Second, Opts)).

%% @doc A module cannot catch the raw host functions with `__newindex' on `_G'
%% and on the `ao' table that is installed after it loads.
newindex_capture_at_install_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Script = <<"""
        Captured = {}
        setmetatable(_G, { __newindex = function(t, k, v)
            if k == 'ao' and type(v) == 'table' then
                setmetatable(v, { __newindex = function(t2, k2, v2)
                    Captured[k2] = v2
                    rawset(t2, k2, v2)
                end })
            end
            rawset(t, k, v)
        end })
        function compute(req)
            local ran = ao.atomic(function()
                if Captured.get then Captured.get('x', { x = 'y' }) end
                return true
            end)
            local caught = tostring(Captured.get ~= nil)
            return { patches = {
                { path = '/caught', value = caught },
                { path = '/ran', value = tostring(ran) }
            } }
        end
        """>>,
    {ok, Result} = hb_ao:resolve(base(Script), <<"compute">>, Opts),
    ?assertEqual(<<"false">>, hb_ao:get(<<"caught">>, Result, Opts)).

%% @doc Nor with `__newindex' on `ao' set at compute time, with `ao.get'
%% removed, which a restore would fire when it sets the host functions again.
newindex_capture_at_restore_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Script = <<"""
        Captured = Captured or {}
        function compute(req)
            if getmetatable(ao) == nil then
                setmetatable(ao, { __newindex = function(t, k, v)
                    Captured[k] = v
                    rawset(t, k, v)
                end })
                rawset(ao, 'get', nil)
            end
            local ran = ao.atomic(function()
                if Captured.get then Captured.get('x', { x = 'y' }) end
                return true
            end)
            return { patches = {
                { path = '/caught', value = tostring(Captured.get ~= nil) },
                { path = '/ran', value = tostring(ran) }
            } }
        end
        """>>,
    {ok, First} = hb_ao:resolve(base(Script), <<"compute">>, Opts),
    ?assertEqual(<<"false">>, hb_ao:get(<<"caught">>, First, Opts)),
    {ok, Snapshot} = hb_ao:resolve(First, <<"snapshot">>, Opts),
    Cold = hb_ao:set(hb_private:reset(First), <<"snapshot">>, Snapshot, Opts),
    {ok, Restored} = hb_ao:resolve(Cold, <<"normalize">>, Opts),
    {ok, Second} = hb_ao:resolve(Restored, <<"compute">>, Opts),
    ?assertEqual(<<"false">>, hb_ao:get(<<"caught">>, Second, Opts)).

%% @doc The monorepo's deploy tooling (`contract-device.mjs', `ATOMIC_PROBE')
%% refuses a custody deployment unless a plain HTTP POST of this script answers
%% 200 with the body `lua-atomic-v1-ready'. On v0.11 the reply to `compute'
%% carries the result's `results' as a link, so RC7's httpsig codec sends it as
%% multipart (the body is one part of it). `compute/body' answers the body
%% alone, which is the reply the tooling checks.
atomic_http_probe_test() ->
    hb:init(),
    Node = hb_http_server:start_node(#{ <<"priv-wallet">> => ar_wallet:new() }),
    Probe = <<"""
        function compute()
          assert(ao.atomic_version == 1 and type(ao.atomic) == 'function')
          local value = 0
          local ok = ao.atomic(function() value = 1; return false, 'probe' end)
          assert(not ok and value == 0)
          return {patches={{path='/body',value='lua-atomic-v1-ready'}}}
        end
        """>>,
    {ok, _} = application:ensure_all_started(inets),
    Post =
        fun(Path) ->
            {ok, {{_, Status, _}, _Headers, Body}} =
                httpc:request(
                    post,
                    {
                        hb_util:list(Node) ++ "~lua-atomic@1.0/" ++ Path,
                        [{"accept", "text/plain"}],
                        "application/lua",
                        Probe
                    },
                    [{timeout, 30000}],
                    [{body_format, binary}]
                ),
            {Status, string:trim(Body)}
        end,
    ?assertEqual({200, <<"lua-atomic-v1-ready">>}, Post("compute/body")),
    {200, Multipart} = Post("compute"),
    ?assertNotEqual(nomatch, binary:match(Multipart, <<"\r\n\r\nlua-atomic-v1-ready\r\n">>)).

%% @doc Through `process@1.0' with delta storage: a request served from the
%% process cache carries public state only, and the next compute restores the
%% VM. The restored VM keeps its count and its atomic primitive, and still
%% refuses host calls inside a batch.
process_restore_keeps_atomic_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"spawn-worker">> => false,
            <<"process-workers">> => false,
            <<"process-delta-cache">> => true,
            <<"process-snapshot-target">> => false,
            <<"process-delta-checkpoint-slots">> => 1000
        },
        Process = process(Opts),
        {ok, _} = hb_cache:write(Process, Opts),
        [
            {ok, _} = hb_ao:resolve(Process, schedule_request(Process, N, Opts), Opts)
        ||
            N <- lists:seq(1, 3)
        ],
        Compute =
            fun(Base, Slot) ->
                hb_ao:resolve(
                    Base,
                    #{ <<"path">> => <<"compute">>, <<"slot">> => Slot },
                    Opts
                )
            end,
        {ok, _} = Compute(Process, 1),
        {ok, Public} = Compute(Process, 1),
        ?assertEqual(
            true,
            maps:get(<<"process-cached-state">>, hb_private:from_message(Public))
        ),
        {ok, State2} = Compute(Public, 2),
        ?assertEqual(<<"3">>, hb_ao:get(<<"count">>, State2, Opts)),
        ?assertEqual(<<"guarded">>, hb_ao:get(<<"host">>, State2, Opts))
    end}.

base(Script) ->
    #{
        <<"device">> => <<"lua-atomic@1.0">>,
        <<"module">> => #{
            <<"content-type">> => <<"application/lua">>,
            <<"body">> => Script
        }
    }.

process(Opts) ->
    Wallet = hb_opts:get(<<"priv-wallet">>, hb:wallet(), Opts),
    Address = hb_util:human_id(ar_wallet:to_address(Wallet)),
    hb_message:commit(
        #{
            <<"device">> => <<"process@1.0">>,
            <<"type">> => <<"Process">>,
            <<"scheduler-device">> => <<"scheduler@1.0">>,
            <<"execution-device">> => <<"lua-atomic@1.0">>,
            <<"module">> => #{
                <<"content-type">> => <<"application/lua">>,
                <<"body">> => <<"""
                    Count = Count or 0
                    function compute(req)
                      local ok = ao.atomic(function()
                        Count = Count + 1
                        return true
                      end)
                      assert(ok, 'commit failed')
                      local escaped = ao.atomic(function()
                        Count = Count + 100
                        ao.event('must never run')
                        return true
                      end)
                      return {
                        patches = {
                          { path = '/count', value = tostring(Count) },
                          { path = '/host', value = escaped and 'escaped' or 'guarded' }
                        },
                        results = { output = { data = tostring(Count) } }
                      }
                    end
                    """>>
            },
            <<"authority">> => [Address],
            <<"scheduler-location">> => Address,
            <<"test-random-seed">> => 53
        },
        Opts
    ).

schedule_request(Process, Number, Opts) ->
    ProcID = hb_message:id(Process, all, Opts),
    hb_message:commit(
        #{
            <<"path">> => <<"schedule">>,
            <<"method">> => <<"POST">>,
            <<"body">> => hb_message:commit(
                #{
                    <<"target">> => ProcID,
                    <<"type">> => <<"Message">>,
                    <<"action">> => <<"Increment">>,
                    <<"number">> => Number
                },
                Opts
            )
        },
        Opts
    ).

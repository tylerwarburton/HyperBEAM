%%% @doc Request-only Lua with pure in-VM atomic batches. Public patches and
%%% checkpoints use lua@5.3b semantics; the stricter sandbox is mandatory.
-module(dev_lua_53c).
-implements(<<"lua@5.3c">>).
-export([info/1, init/3, compute/4, snapshot/3, normalize/3, functions/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(VERSION, {lua_atomic, version}).
-define(DEPTH, {lua_atomic, depth}).

%% @doc Install raw closure callbacks without decoding the closure environment.
install_atomic(State) ->
    S0 = luerl:put_private(?VERSION, 1, State),
    S1 = lists:foldl(
        fun guard_host/2,
        luerl:put_private(?DEPTH, 0, S0),
        [<<"get">>, <<"resolve">>, <<"set">>, <<"event">>]
    ),
    {ok, S2} = luerl:set_table_keys_dec([ao, atomic], fun atomic/2, S1),
    luerl:set_table_keys_dec([ao, atomic_version], 1, S2).

%% @doc Wrap host entries before the VM is exposed to a caller. The legacy
%% library is installed after module loading, so Lua cannot retain an original
%% host alias before these wrappers are installed. The mandatory sandbox removes
%% other host-effect sources and legacy VMs are rejected by init/3.
guard_host(Name, State) ->
    {ok, Original, S0} = luerl:get_table_keys([<<"ao">>, Name], State),
    Wrapper = fun(Args, Current) ->
        case private(?DEPTH, Current) > 0 of
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
    {ok, S1} = luerl:set_table_keys_dec([ao, Name], Wrapper, S0),
    S1.

%% @doc A version witness prevents importing an unrestricted VM into this device.
atomic_enabled(State) ->
    private(?VERSION, State) =:= 1.

%% @doc Read optional VM metadata without depending on a record layout.
private(Key, State) ->
    try luerl:get_private(Key, State)
    catch error:{badkey, Key} -> undefined end.

%% @doc Keep savepoints on the Erlang stack, never in serializable VM metadata.
atomic([Closure], Before) ->
    Depth = private(?DEPTH, Before),
    case Depth < 16 of
        true ->
            Working = luerl:put_private(?DEPTH, Depth + 1, Before),
            finish(luerl:call_function(Closure, [], Working), Before, Depth);
        false -> {[false, <<"Atomic nesting limit exceeded">>], Before}
    end;
atomic(_, Before) ->
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

%% @doc Dispatch Lua calls through the atomic-capable runtime.
info(_Base) ->
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
                <<"decode">>
            ]
    }.

%% @doc Initialize under the mandatory sandbox before any module can capture
%% host functions. Existing VMs must originate from this runtime.
init(Base, Req, Opts) ->
    case hb_private:get(<<"state">>, Base, Opts) of
        not_found ->
            Strict = Opts#{ <<"lua-minimum-sandbox">> => sandbox(Opts) },
            case hb_ao:raw(<<"lua@5.3b">>, <<"init">>, Base, Req, Strict) of
                {ok, Initialized} ->
                    State = hb_private:get(<<"state">>, Initialized, Opts),
                    {ok, Ready} = install_atomic(State),
                    {ok, hb_private:set(Initialized, <<"state">>, Ready, Opts)};
                Error -> Error
            end;
        State ->
            case atomic_enabled(State) of
                true -> {ok, Base};
                false -> {error, <<"lua@5.3c requires a native 5.3c snapshot">>}
            end
    end.

%% @doc Restrict both global names and package aliases before module loading.
sandbox(Opts) ->
    Extra = case hb_opts:get(<<"lua-minimum-sandbox">>, [], Opts) of
        false -> [];
        Spec -> Spec
    end,
    Extra ++ [
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

%% @doc Execute with the existing patch protocol after checking the VM witness.
compute(Key, Base, Req, Opts) ->
    case init(Base, Req, Opts) of
        {ok, Ready} -> hb_ao:raw(<<"lua@5.3b">>, Key, Ready, Req, Opts);
        Error -> Error
    end.

%% @doc Persist the ordinary Luerl VM; no savepoint roots are retained.
snapshot(Base, Req, Opts) ->
    hb_ao:raw(<<"lua@5.3b">>, <<"snapshot">>, Base, Req, Opts).

%% @doc Restore and verify the sandbox witness before accepting the VM.
normalize(Base, Req, Opts) ->
    case hb_ao:raw(<<"lua@5.3b">>, <<"normalize">>, Base, Req, Opts) of
        {ok, Restored} -> init(Restored, Req, Opts);
        Error -> Error
    end.

%% @doc Enumerate functions using the shared VM representation.
functions(Base, Req, Opts) ->
    hb_ao:raw(<<"lua@5.3b">>, <<"functions">>, Base, Req, Opts).

%% @doc Exercise savepoints through AO-Core, including aliases, nested calls,
%% garbage collection, resource bounds, publication and checkpoint recovery.
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
    Base = #{
        <<"device">> => <<"lua@5.3c">>,
        <<"module">> => #{ <<"content-type">> => <<"application/lua">>, <<"body">> => Script }
    },
    {ok, First} = hb_ao:resolve(Base, <<"compute">>, Opts),
    ?assertEqual(<<"1">>, hb_ao:get(<<"count">>, First, Opts)),
    {ok, Snapshot} = hb_ao:resolve(First, <<"snapshot">>, Opts),
    Cold = hb_ao:set(hb_private:reset(First), <<"snapshot">>, Snapshot, Opts),
    {ok, Restored} = hb_ao:resolve(Cold, <<"normalize">>, Opts),
    {ok, Second} = hb_ao:resolve(Restored, <<"compute">>, Opts),
    ?assertEqual(<<"2">>, hb_ao:get(<<"count">>, Second, Opts)).

%% @doc Unrestricted legacy state cannot acquire the sandbox witness by changing
%% its device name. A fresh deployment or explicitly designed migration is needed.
legacy_vm_rejected_test() ->
    hb:init(),
    Opts = #{ <<"hashpath">> => ignore },
    Base = #{
        <<"device">> => <<"lua@5.3b">>,
        <<"module">> => #{ <<"content-type">> => <<"application/lua">>, <<"body">> => <<"function compute() return {} end">> }
    },
    {ok, Legacy} = hb_ao:resolve(Base, <<"init">>, Opts),
    Switched = hb_ao:set(Legacy, <<"device">>, <<"lua@5.3c">>, Opts),
    ?assertMatch({error, _}, hb_ao:resolve(Switched, <<"compute">>, Opts)).

%% @doc Sandbox restrictions apply before module code and cannot be disabled.
module_sandbox_test() ->
    hb:init(),
    Base = #{
        <<"device">> => <<"lua@5.3c">>,
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

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
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PATCH_LUA, <<"lua-patch@1.0">>).
-define(VERSION, {lua_atomic, version}).
-define(DEPTH, {lua_atomic, depth}).
-define(HOST_FUNCTIONS, [<<"get">>, <<"resolve">>, <<"set">>, <<"event">>]).

%% @doc Install raw closure callbacks without decoding the closure environment.
install_atomic(State) ->
    S0 = luerl:put_private(?VERSION, 1, State),
    S1 = lists:foldl(
        fun guard_host/2,
        luerl:put_private(?DEPTH, 0, S0),
        ?HOST_FUNCTIONS
    ),
    {ok, S2} = luerl:set_table_keys_dec([ao, atomic], fun atomic/2, S1),
    {ok, S3} = luerl:set_table_keys_dec([ao, atomic_version], 1, S2),
    {ok, S4} = luerl:set_table_keys_dec([collectgarbage], fun collect/2, S3),
    S4.

%% @doc `collectgarbage' that does nothing inside a batch. A batch runs through
%% `luerl:call_function/3', which starts a new call stack, so the frames of the
%% suspended Lua beneath it are not roots: a collection there frees the
%% caller's locals. The old fork's Luerl patch deferred such a collection;
%% v0.11 runs stock Luerl, so the batch skips it and the next collection
%% outside a batch does the work. Outside a batch this is Luerl's own
%% `collectgarbage', called on the current state so its stack stays a root.
collect(Args, State) ->
    case private(?DEPTH, State) > 0 of
        true -> {[], State};
        false -> luerl_lib_basic:collectgarbage(nil, Args, State)
    end.

%% @doc Wrap host entries before the VM is exposed to a caller. The library is
%% installed after module loading, so Lua cannot retain an original host alias
%% before these wrappers are installed. The mandatory sandbox removes other
%% host-effect sources, and VMs from other devices are rejected by `init/3'.
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
                <<"decode">>
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
    Opts#{ <<"lua-device-sandbox">> => sandbox() }.

%% @doc Restrict both global names and package aliases before module loading.
sandbox() ->
    [
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
        {ok, Ready} -> hb_ao:raw(?PATCH_LUA, Key, Ready, Req, Opts);
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

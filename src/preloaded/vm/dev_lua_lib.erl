%%% @doc A module for providing AO library functions to the Lua environment.
%%% This module contains the implementation of the functions, each by the name
%%% that should be used in the `ao' table in the Lua environment. Every export
%%% is imported into the Lua environment.
%%% 
%%% Each function adheres closely to the Luerl calling convention, adding the 
%%% appropriate node message as a third argument:
%%% 
%%%     fun(Args, State, NodeMsg) -> {ResultTerms, NewState}
%%% 
%%% As Lua allows for multiple return values, each function returns a list of
%%% terms to grant to the caller. Matching the tuple convention used by AO-Core,
%%% the first term is typically the status, and the second term is the result.
-module(dev_lua_lib).
%%% Library functions. Each exported function is _automatically_ added to the
%%% Lua environment, except for the `install/3' and `reinstall/3' functions,
%%% which install the library in a new Lua state and in a restored one.
-export([get/3, resolve/3, set/3, event/3, install/3, reinstall/3]).
-include("include/hb.hrl").
-include_lib("luerl/include/luerl.hrl").

%%% The set of devices that must be included in the device sandbox for an
%%% execution that is able to perform AO-Core resolutions. Without the following
%%% devices, all resolutions will fail.
-define(MINIMAL_AO_CORE_DEVICES, [<<"structured@1.0">>]).
-define(LIBRARY_FUNCTIONS, [
    {get, fun get/3},
    {resolve, fun resolve/3},
    {set, fun set/3},
    {event, fun event/3}
]).

%% @doc Install the library into the given Lua environment. Every read and
%% write here is raw: a module can set `__index' or `__newindex' on `_G' or on
%% `ao', and a metamethod would run Lua with the host functions in hand.
install(Base, State, Opts) ->
    % Initialize the AO-Core resolver.
    BaseAOTable =
        case raw_ao(State) of
            nil ->
                ?event(no_ao_table),
                #{};
            Existing ->
                % The table keeps its functions, which `dev_lua:decode/2' drops.
                ExistingTable = luerl:decode(Existing, State),
                ?event({existing_ao_table, ExistingTable}),
                ExistingTable
        end,
    ?event({base_ao_table, BaseAOTable}),
    % A new table, so it carries no metatable that the module set.
    {AOTable, State1} = luerl:encode(BaseAOTable, State),
    State2 = luerl_heap:raw_set_table_key(State1#luerl.g, <<"ao">>, AOTable, State1),
    set_functions(Base, State2, Opts).

%% @doc Set the library functions again in the `ao' table of a Lua state
%% restored from a snapshot. The functions are closures of the module that
%% installed them, which is not loaded when another build of this device wrote
%% the snapshot. A state whose `ao' is not a table has no library to set.
reinstall(Base, State, Opts) ->
    case raw_ao(State) of
        #tref{} -> set_functions(Base, State, Opts);
        _ -> {ok, State}
    end.

%% @doc The global `ao', read without metamethods.
raw_ao(State) ->
    luerl_heap:raw_get_table_key(State#luerl.g, <<"ao">>, State).

%% @doc Set the library functions in the `ao' table of the given Lua state.
set_functions(Base, State, Opts) ->
    % Compute the device-name allowlist for the Lua sandbox. When the
    % caller provides a `device-sandbox' field, only those names plus
    % the minimal-AO-Core set are admissible; otherwise we leave the
    % allowlist undefined (no restriction).
    DevSandboxDef =
        hb_ao:get(
            <<"device-sandbox">>,
            {as, <<"message@1.0">>, Base},
            false,
            Opts
        ),
    AdmissibleNames =
        case DevSandboxDef of
            false -> all;
            DevNames ->
                hb_util:message_to_ordered_list(
                    hb_util:unique(DevNames ++ ?MINIMAL_AO_CORE_DEVICES)
                )
        end,
    ?event({adding_ao_core_resolver, {device_sandbox, AdmissibleNames}}),
    ExecOpts =
        #{
            <<"admissible-devices">> => AdmissibleNames,
            <<"hashpath">> => ignore
        },
    {
        ok,
        lists:foldl(
            fun({FuncName, Func}, StateIn) ->
                {Encoded, StateEnc} =
                    luerl:encode(
                        fun(RawArgs, ImportState) ->
                          guarded(FuncName, ImportState, fun() ->
                            ?event(lua_import, {calling_import, {func, FuncName}}),
                            % Get the node message of the invocation from the
                            % private data of the Lua state.
                            ImportOpts =
                                luerl:get_private(<<"node-message">>, ImportState),
                            % Decode the arguments from the Lua environment. A
                            % table key that `hb_ao:normalize_key/2' has no
                            % clause for, such as `1.5', is a bad argument,
                            % which `pcall' catches. Other errors can depend on
                            % this node, and are raised.
                            Args =
                                try
                                    lists:map(
                                        fun(Arg) ->
                                            dev_lua:decode(
                                                luerl:decode(Arg, ImportState),
                                                ImportOpts
                                            )
                                        end,
                                        RawArgs
                                    )
                                catch
                                    error:function_clause ->
                                        luerl_lib:badarg_error(
                                            FuncName,
                                            RawArgs,
                                            ImportState
                                        )
                                end,
                            % Call the function with the decoded arguments.
                            {Res, ResState} =
                                Func(
                                    Args,
                                    ImportState,
                                    maps:merge(ImportOpts, ExecOpts)
                                ),
                            % Encode the response for return to Lua
                            return(Res, ResState, ImportOpts)
                          end)
                        end,
                        StateIn
                    ),
                luerl_heap:raw_set_table_key(
                    raw_ao(StateEnc),
                    atom_to_binary(FuncName),
                    Encoded,
                    StateEnc
                )
            end,
            State,
            ?LIBRARY_FUNCTIONS
        )
    }.

%% @doc Run a library call. A Lua error passes through. Any other error is
%% raised, failing the call, unless the device set `host-errors' to `lua' in
%% the state's private data: then it is a Lua error with a fixed message, which
%% the contract's `pcall' catches. The message never carries the reason, which
%% can depend on the node. `lua-patch@1.0' asks for `lua' only outside a
%% process slot, where nothing is kept: inside one, catching an error that
%% another node does not have would let the two states differ.
guarded(FuncName, State, Call) ->
    try Call()
    catch
        error:{lua_error, _, _} = LuaError:Stack -> erlang:raise(error, LuaError, Stack);
        Class:Reason:Stack ->
            case host_errors(State) of
                lua ->
                    luerl_lib:lua_error(
                        {error_call,
                            [<<"ao.", (atom_to_binary(FuncName))/binary, " failed on this node">>]},
                        State
                    );
                raise -> erlang:raise(Class, Reason, Stack)
            end
    end.

host_errors(State) ->
    try luerl:get_private(<<"host-errors">>, State) of
        lua -> lua;
        _ -> raise
    catch error:{badkey, _} -> raise
    end.

%% @doc Helper function for returning a result from a Lua function.
return(Result, ExecState, Opts) ->
    ?event(lua_import, {import_returning, {result, Result}}),
    TableEncoded = dev_lua:encode(hb_cache:ensure_all_loaded(Result, Opts), Opts),
    {ReturnParams, ResultingState} =
        lists:foldr(
            fun(LuaEncoded, {Params, StateIn}) ->
                {NewParam, NewState} = luerl:encode(LuaEncoded, StateIn),
                {[NewParam | Params], NewState}
            end,
            {[], ExecState},
            TableEncoded
        ),
    ?event({lua_encoded, ReturnParams}),
    {ReturnParams, ResultingState}.

%% @doc A wrapper function for performing AO-Core resolutions. Offers both the 
%% single-message (using `hb_singleton:from/1' to parse) and multiple-message
%% (using `hb_ao:resolve_many/2') variants.
resolve([SingletonMsg], ExecState, ExecOpts) ->
    ?event({ao_core_resolver, {msg, SingletonMsg}}),
    ParsedMsgs = hb_singleton:from(SingletonMsg, ExecOpts),
    ?event({parsed_msgs_to_resolve, ParsedMsgs}),
    resolve({many, ParsedMsgs}, ExecState, ExecOpts);
resolve(Msgs, ExecState, ExecOpts) when is_list(Msgs) ->
    resolve({many, Msgs}, ExecState, ExecOpts);
resolve({many, Msgs}, ExecState, ExecOpts) ->
    % The messages are what the device executes; private keys in them are
    % caller input from Lua.
    MaybeAsMsgs = lists:map(fun convert_as/1, hb_private:reset(Msgs)),
    try hb_ao:resolve_many(MaybeAsMsgs, ExecOpts) of
        {Status, Res} ->
            ?event({resolved_msgs, {status, Status}, {res, Res}, {exec_opts, ExecOpts}}),
            Key = case Msgs of [] -> undefined; _ -> lists:last(Msgs) end,
            {[Status, committers(Key, Res)], ExecState}
    catch
        Error ->
            ?event(lua_error, {ao_core_resolver_error, Error}),
            {[<<"error">>, Error], ExecState}
    end.

%% @doc A wrapper for `hb_ao''s `get' functionality. A key that the message
%% does not have gives Lua's `nil'.
get([Key, Base], ExecState, ExecOpts) ->
    ?event({ao_core_get, {base, Base}, {key, Key}}),
    NewRes =
        committers(
            Key,
            hb_ao:get(convert_as(Key), convert_as(Base), nil, ExecOpts)
        ),
    ?event({ao_core_get_result, {result, NewRes}}),
    {[NewRes], ExecState}.

%% @doc Convert the `committers' of a message to the form that Lua reads. Each
%% Arweave committer is given as its address.
committers(<<"committers">>, Committers) when is_list(Committers) ->
    lists:map(fun hb_util:committer_address/1, Committers);
committers(_Key, Res) ->
    Res.

%% @doc Converts any `as' terms from Lua to their HyperBEAM equivalents.
convert_as([<<"as">>, Device, RawMsg]) ->
    {as, Device, RawMsg};
convert_as(Other) ->
    Other.

%% @doc Wrapper for `hb_ao''s `set' functionality. Setting a key to Lua's `nil'
%% removes it.
set([Base, Key, nil], ExecState, ExecOpts) ->
    set([Base, Key, unset], ExecState, ExecOpts);
set([Base, Key, Value], ExecState, ExecOpts) ->
    ?event({ao_core_set, {base, Base}, {key, Key}, {value, Value}}),
    NewRes = hb_ao:set(Base, Key, Value, ExecOpts),
    ?event({ao_core_set_result, {result, NewRes}}),
    {[NewRes], ExecState};
set([Base, NewValues], ExecState, ExecOpts) ->
    ?event({ao_core_set, {base, Base}, {new_values, NewValues}}),
    NewRes = hb_ao:set(Base, NewValues, ExecOpts),
    ?event({ao_core_set_result, {result, NewRes}}),
    {[NewRes], ExecState}.

%% @doc Allows Lua scripts to signal events using the HyperBEAM hosts internal
%% event system.
event([Event], ExecState, Opts) ->
    ?event({recalling_event, Event}),
    event([global, Event], ExecState, Opts);
event([Group, Event], State, Opts) when is_list(Event) ->
    event([Group, list_to_tuple(Event)], State, Opts);
event([Group, Event], ExecState, _Opts) ->
    ?event(
        lua_event,
        {event,
            {group, Group},
            {event, Event}
        }
    ),
    ?event(Group, Event),
    {[<<"ok">>], ExecState}.

%%% @doc The in-process WAMR embedding behind `wasm-patch@1.0'
%%% (`native/hb_wasm_nif/hb_wasm_nif.c'). Every call that runs WebAssembly is
%%% a dirty-CPU NIF; the instance is a resource held by whoever holds the
%%% process state, with its own lock.
%%%
%%% An instance counts the successful calls applied to its memory (`gen').
%%% `call/5' names the gen it expects and is refused (`{error, stale}') if the
%%% instance has moved on, so a state that is not the instance's newest can
%%% never compute on its memory. A trap leaves the instance dirty
%%% (`{error, dirty}') until `restore/4'.
-module(hb_wasm_nif).
-export([load/1, instantiate/3, call/5, snapshot/1, restore/4, gen/1, memory_size/1]).
-export([register_invoke/1, prepare/1, imports/1]).
-on_load(init/0).
-include_lib("eunit/include/eunit.hrl").

init() ->
    SoName = filename:join([code:priv_dir(hb), "hb_wasm_nif"]),
    erlang:load_nif(SoName, 0).

-define(NIF, erlang:nif_error(hb_wasm_nif_not_loaded)).

%% @doc Load a module: a `.wasm', or an `.aot' this node compiled. Returns
%% `{ok, Module, #{ aot, max_pages, init_pages }}' or `{error, Reason}'.
%% Refuses any import outside the allow-list and a module without the ABI
%% exports. Use `prepare/1' first, so the `invoke_*' natives it imports exist.
load(_Bytes) -> ?NIF.
%% @doc `{ok, Instance, #{ fuel, wide, initialize }}' or `{error, Reason}'.
instantiate(_Module, _StackBytes, _HeapBytes) -> ?NIF.
%% @doc `Which' 0 runs `_initialize' (when exported) then `wp_init'; 1 runs
%% `wp_handle'. `{ok, Reply, FuelLeft}', `{trap, Reason}' or
%% `{error, stale | dirty}'.
call(_Inst, _Which, _ExpectedGen, _Request, _Fuel) -> ?NIF.
%% @doc `{ok, Gen, Memory, [{Name, Kind, Bits}]}' or `{error, dirty}'.
snapshot(_Inst) -> ?NIF.
%% @doc Replace the instance's memory and exported mutable globals and set its
%% gen. `ok' or `{error, Reason}'.
restore(_Inst, _Gen, _Memory, _Globals) -> ?NIF.
gen(_Inst) -> ?NIF.
memory_size(_Inst) -> ?NIF.
register_invoke(_Name) -> ?NIF.

%% @doc Register the natives a `.wasm' imports (`env.invoke_<sig>'), so that
%% `load/1' can link them. Other names are left for `load/1' to refuse.
prepare(Wasm) ->
    case imports(Wasm) of
        {ok, Imports} ->
            lists:foldl(
                fun({<<"env">>, <<"invoke_", _/binary>> = Name, func}, ok) ->
                        case register_invoke(Name) of
                            ok -> ok;
                            {error, Reason} -> {error, {Name, Reason}}
                        end;
                   (_, Acc) -> Acc
                end,
                ok,
                Imports
            );
        Error -> Error
    end.

%% @doc The imports of a `.wasm' binary: `[{Module, Name, func | table |
%% memory | global | tag}]'.
imports(<<0, "asm", 1:32/little, Sections/binary>>) ->
    try {ok, find_imports(Sections)}
    catch _:_ -> {error, malformed_wasm}
    end;
imports(_) ->
    {error, not_wasm}.

find_imports(<<>>) -> [];
find_imports(<<Id, Rest/binary>>) ->
    {Size, Rest1} = leb(Rest),
    <<Body:Size/binary, Next/binary>> = Rest1,
    case Id of
        2 ->
            {N, Entries} = leb(Body),
            import_entries(N, Entries);
        _ -> find_imports(Next)
    end.

import_entries(0, _) -> [];
import_entries(N, Bin) ->
    {Module, R1} = name(Bin),
    {Name, <<Kind, R2/binary>>} = name(R1),
    R3 = skip_desc(Kind, R2),
    [{Module, Name, kind(Kind)} | import_entries(N - 1, R3)].

kind(0) -> func;
kind(1) -> table;
kind(2) -> memory;
kind(3) -> global;
kind(4) -> tag.

skip_desc(0, Bin) -> element(2, leb(Bin));
skip_desc(1, <<_RefType, Flags, Rest/binary>>) -> skip_limits(Flags, Rest);
skip_desc(2, <<Flags, Rest/binary>>) -> skip_limits(Flags, Rest);
skip_desc(3, <<_ValType, _Mut, Rest/binary>>) -> Rest;
skip_desc(4, <<_Attr, Rest/binary>>) -> element(2, leb(Rest)).

skip_limits(Flags, Bin) ->
    {_Min, R1} = leb(Bin),
    case Flags band 1 of
        1 -> element(2, leb(R1));
        0 -> R1
    end.

name(Bin) ->
    {Len, R} = leb(Bin),
    <<Name:Len/binary, Rest/binary>> = R,
    {Name, Rest}.

leb(Bin) -> leb(Bin, 0, 0).
leb(<<1:1, B:7, Rest/binary>>, Shift, Acc) -> leb(Rest, Shift + 7, Acc bor (B bsl Shift));
leb(<<0:1, B:7, Rest/binary>>, Shift, Acc) -> {Acc bor (B bsl Shift), Rest}.

%%% Tests

imports_test() ->
    % (module (import "env" "invoke_vii" (func (param i32 i32 i32)))
    %         (import "env" "throw_longjmp" (func)))
    Type = <<1, 2, 16#60, 3, 16#7f, 16#7f, 16#7f, 0, 16#60, 0, 0>>,
    Entries = iolist_to_binary([
        2,
        3, <<"env">>, 10, <<"invoke_vii">>, 0, 0,
        3, <<"env">>, 13, <<"throw_longjmp">>, 0, 1
    ]),
    Wasm = iolist_to_binary([
        <<0, "asm", 1:32/little>>,
        1, byte_size(Type), Type,
        2, byte_size(Entries), Entries
    ]),
    ?assertEqual(
        {ok, [{<<"env">>, <<"invoke_vii">>, func}, {<<"env">>, <<"throw_longjmp">>, func}]},
        imports(Wasm)
    ),
    ?assertEqual({error, not_wasm}, imports(<<0, "aot", 0, 0, 0, 0>>)).

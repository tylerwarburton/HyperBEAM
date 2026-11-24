%%% @doc The `wasm-patch@1.0' wire format (docs/WASM_PATCH_ABI.md): one
%%% binary request in, one binary result out, both little-endian.
%%%
%%% Values:
%%% ```
%%% 01 binary  u32 len, bytes        04 / 05  true / false
%%% 02 i64                           06 map   u32 n, n x (key, value)
%%% 03 f64                           07 list  u32 n, n x value
%%% '''
%%% A map key is a binary, an i64 or an f64.
%%%
%%% Result:
%%% ```
%%% u8 status    0 = ok, then the patches and the results
%%%              1 = error, then one value: the error
%%% u32 n        patch count
%%% n x (u8 op, path, value if op = 1)    op 1 = set, 2 = delete; path a binary value
%%% value        the results tree
%%% '''
%%%
%%% `wp_init' answers `[u8 status][value]': 0 and the init's result (unused),
%%% or 1 and the error.
%%%
%%% `decode_result/1' returns what `luerl:decode/2' returns for the same Lua
%%% value -- a table is a list of `{Key, Value}', an empty table `[]' -- so the
%%% one normalisation `lua-patch@1.0' applies (`lib_patch_exec:decode/2') turns
%%% both runtimes' results into the same terms.
-module(hb_wasm_patch_codec).
-export([encode/1, decode_result/1, decode_init/1, decode_value/1, decode_terms/1]).
-include_lib("eunit/include/eunit.hrl").

-define(BIN, 1).
-define(I64, 2).
-define(F64, 3).
-define(TRUE, 4).
-define(FALSE, 5).
-define(MAP, 6).
-define(LIST, 7).
-define(SET, 1).
-define(DELETE, 2).

%% @doc Encode an Erlang term: binaries, integers, floats, booleans, maps,
%% lists, and atoms (as binaries). Map keys are written in sorted order, so a
%% request is the same bytes on every node.
encode(Term) ->
    iolist_to_binary(enc(Term)).

enc(Bin) when is_binary(Bin) -> [?BIN, <<(byte_size(Bin)):32/little>>, Bin];
enc(true) -> <<?TRUE>>;
enc(false) -> <<?FALSE>>;
enc(Int) when is_integer(Int), Int >= -16#8000000000000000, Int =< 16#7fffffffffffffff ->
    <<?I64, Int:64/little-signed>>;
enc(Int) when is_integer(Int) -> enc(float(Int));
enc(Float) when is_float(Float) -> <<?F64, Float:64/little-float>>;
enc(Atom) when is_atom(Atom) -> enc(atom_to_binary(Atom, utf8));
enc(Map) when is_map(Map) ->
    Pairs = lists:sort(maps:to_list(Map)),
    [?MAP, <<(length(Pairs)):32/little>>, [[key(K), enc(V)] || {K, V} <- Pairs]];
enc(List) when is_list(List) ->
    [?LIST, <<(length(List)):32/little>>, [enc(V) || V <- List]].

key(K) when is_binary(K); is_integer(K); is_float(K) -> enc(K);
key(K) when is_atom(K) -> enc(atom_to_binary(K, utf8)).

%% @doc Decode a module's reply. `{ok, Result}' where `Result' is the
%% Luerl-shaped `[{<<"patches">>, ...}, {<<"results">>, ...}]', `{error, Value}'
%% for status 1, or `{malformed, Reason}'.
decode_result(<<0, N:32/little, Rest/binary>>) ->
    try
        {Patches, Rest1} = patches(N, 1, Rest, []),
        {Results, <<>>} = value(Rest1),
        {ok, [{<<"patches">>, Patches}, {<<"results">>, Results}]}
    catch _:_ -> {malformed, <<"wasm-patch result does not decode">>}
    end;
decode_result(<<1, Rest/binary>>) ->
    try
        {Error, <<>>} = value(Rest),
        {error, Error}
    catch _:_ -> {malformed, <<"wasm-patch error does not decode">>}
    end;
decode_result(<<>>) ->
    {malformed, <<"wasm-patch result is empty">>};
decode_result(_) ->
    {malformed, <<"wasm-patch result has an unknown status">>}.

patches(0, _I, Rest, Acc) -> {lists:reverse(Acc), Rest};
patches(N, I, <<?SET, ?BIN, Len:32/little, Path:Len/binary, Rest/binary>>, Acc) ->
    {Value, Rest1} = value(Rest),
    patches(N - 1, I + 1, Rest1,
        [{I, [{<<"path">>, Path}, {<<"value">>, Value}]} | Acc]);
patches(N, I, <<?DELETE, ?BIN, Len:32/little, Path:Len/binary, Rest/binary>>, Acc) ->
    patches(N - 1, I + 1, Rest,
        [{I, [{<<"path">>, Path}, {<<"delete">>, true}]} | Acc]).

%% @doc Decode an ok result straight into the terms `lua-patch@1.0' publishes,
%% in one walk: `{ok, Patches, Results}'. They are what `decode_result/1' and
%% `lib_patch_exec:decode/2' give, except that no unsigned commitment is added
%% to each nested message (`hb_message:normalize_commitments/3'), which the
%% process strips again before the state is used or stored
%% (`lib_process:normalize_changed/3'). A table is a map, or a list when its
%% keys are 1..n, with the same rule (`hb_util:is_ordered_list/2'); an empty
%% table is `#{}'. A result naming `commitments' or a private key anywhere,
%% or holding an empty table in a list, gives `fallback': the caller takes the
%% general path for it.
decode_terms(<<0, N:32/little, Rest/binary>>) ->
    try
        {Patches, Rest1} = tpatches(N, Rest, []),
        {Results, <<>>} = tvalue(Rest1),
        {ok, Patches, Results}
    catch
        throw:fallback -> fallback;
        _:_ -> {malformed, <<"wasm-patch result does not decode">>}
    end;
decode_terms(_) ->
    fallback.

tpatches(0, Rest, Acc) -> {lists:reverse(Acc), Rest};
tpatches(N, <<?SET, ?BIN, Len:32/little, Path:Len/binary, Rest/binary>>, Acc) ->
    {Value, Rest1} = tvalue(Rest),
    tpatches(N - 1, Rest1, [#{ <<"path">> => Path, <<"value">> => Value } | Acc]);
tpatches(N, <<?DELETE, ?BIN, Len:32/little, Path:Len/binary, Rest/binary>>, Acc) ->
    tpatches(N - 1, Rest, [#{ <<"path">> => Path, <<"delete">> => true } | Acc]).

tvalue(<<?MAP, 0:32/little, Rest/binary>>) -> {#{}, Rest};
tvalue(<<?MAP, N:32/little, Rest/binary>>) -> tpairs(N, Rest, #{}, plain);
tvalue(<<?LIST, 0:32/little, Rest/binary>>) -> {#{}, Rest};
tvalue(<<?LIST, N:32/little, Rest/binary>>) -> titems(N, Rest, []);
tvalue(Bin) -> value(Bin).

titems(0, Rest, Acc) -> {lists:reverse(Acc), Rest};
titems(N, Bin, Acc) ->
    {V, R} = tvalue(Bin),
    titems(N - 1, R, [in_list(V) | Acc]).

%% An empty table in a list leaves the general path's normalisation as `[]',
%% not `#{}' (the unsigned commitment's codec round trip); rare enough to send
%% the whole result there rather than reproduce it.
in_list(V) when V =:= #{} -> throw(fallback);
in_list(V) -> V.

tpairs(0, Rest, Map, plain) -> {Map, Rest};
tpairs(0, Rest, Map, check) ->
    case hb_util:is_ordered_list(Map, #{}) of
        true -> {[in_list(V) || V <- hb_util:message_to_ordered_list(Map, #{})], Rest};
        false -> {Map, Rest}
    end;
tpairs(N, Bin, Map, Mode) ->
    {K, R1} = value(Bin),
    Mode1 =
        case K of
            _ when is_binary(K) -> key_mode(K, Mode);
            _ when is_number(K) -> check
        end,
    {V, R2} = tvalue(R1),
    tpairs(N - 1, R2, Map#{ K => V }, Mode1).

%% A key that may make the table a list (`check'), or that the general path
%% treats apart (`fallback').
key_mode(<<"commitments">>, _Mode) -> throw(fallback);
key_mode(<<"ao-types">>, _Mode) -> check;
key_mode(<<C, _/binary>>, _Mode) when C >= $0, C =< $9 -> check;
key_mode(Key, Mode) ->
    case hb_private:is_private(Key) of
        true -> throw(fallback);
        false -> Mode
    end.

%% @doc Decode `wp_init''s answer: `ok', `{error, Value}' or
%% `{malformed, Reason}'.
decode_init(<<Status, Rest/binary>>) when Status == 0; Status == 1 ->
    try value(Rest) of
        {_, <<>>} when Status == 0 -> ok;
        {Error, <<>>} -> {error, Error};
        _ -> {malformed, <<"wp_init answer has trailing bytes">>}
    catch _:_ -> {malformed, <<"wp_init answer does not decode">>}
    end;
decode_init(_) ->
    {malformed, <<"wp_init answer has no status">>}.

%% @doc Decode one value: `{Term, Rest}'.
decode_value(Bin) -> value(Bin).

value(<<?BIN, Len:32/little, Bin:Len/binary, Rest/binary>>) -> {Bin, Rest};
value(<<?I64, Int:64/little-signed, Rest/binary>>) -> {Int, Rest};
value(<<?F64, Float:64/little-float, Rest/binary>>) -> {Float, Rest};
value(<<?TRUE, Rest/binary>>) -> {true, Rest};
value(<<?FALSE, Rest/binary>>) -> {false, Rest};
value(<<?MAP, N:32/little, Rest/binary>>) -> pairs(N, Rest, []);
value(<<?LIST, N:32/little, Rest/binary>>) -> items(N, 1, Rest, []).

pairs(0, Rest, Acc) -> {lists:reverse(Acc), Rest};
pairs(N, Bin, Acc) ->
    {K, R1} = value(Bin),
    true = is_binary(K) orelse is_number(K),
    {V, R2} = value(R1),
    pairs(N - 1, R2, [{K, V} | Acc]).

items(0, _I, Rest, Acc) -> {lists:reverse(Acc), Rest};
items(N, I, Bin, Acc) ->
    {V, R} = value(Bin),
    items(N - 1, I + 1, R, [{I, V} | Acc]).

%%% Tests

roundtrip_scalars_test() ->
    lists:foreach(
        fun(V) ->
            ?assertEqual({V, <<>>}, decode_value(encode(V)))
        end,
        [<<>>, <<"abc">>, 0, -1, 16#7fffffffffffffff, -16#8000000000000000, 1.5, -0.0, true, false]
    ),
    ?assertEqual({<<"x">>, <<>>}, decode_value(encode(x))),
    ?assertEqual({1.0e20, <<>>}, decode_value(encode(100000000000000000000))).

map_and_list_test() ->
    Bin = encode(#{ <<"b">> => [<<"p">>, 2], <<"a">> => #{} , 3 => false }),
    % Keys sorted: integers sort before binaries.
    ?assertEqual(
        {[{3, false}, {<<"a">>, []}, {<<"b">>, [{1, <<"p">>}, {2, 2}]}], <<>>},
        decode_value(Bin)
    ),
    ?assertEqual(encode(#{ <<"z">> => 1, <<"a">> => 2 }), encode(#{ <<"a">> => 2, <<"z">> => 1 })).

result_test() ->
    Ok = iolist_to_binary([
        0, <<2:32/little>>,
        ?SET, encode(<<"/a">>), encode(1),
        ?DELETE, encode(<<"/b">>),
        encode(#{ <<"output">> => #{ <<"data">> => <<"x">> } })
    ]),
    ?assertEqual(
        {ok, [
            {<<"patches">>, [
                {1, [{<<"path">>, <<"/a">>}, {<<"value">>, 1}]},
                {2, [{<<"path">>, <<"/b">>}, {<<"delete">>, true}]}
            ]},
            {<<"results">>, [{<<"output">>, [{<<"data">>, <<"x">>}]}]}
        ]},
        decode_result(Ok)
    ),
    ?assertEqual({error, <<"boom">>}, decode_result(<<1, (encode(<<"boom">>))/binary>>)),
    ?assertMatch({malformed, _}, decode_result(<<0, 1:32/little, 9>>)),
    ?assertMatch({malformed, _}, decode_result(<<0, 0:32/little, (encode(1))/binary, 0>>)),
    ?assertMatch({malformed, _}, decode_result(<<>>)),
    ?assertMatch({malformed, _}, decode_result(<<7>>)),
    ?assertEqual(ok, decode_init(<<0, (encode(#{}))/binary>>)),
    ?assertEqual({error, <<"no">>}, decode_init(<<1, (encode(<<"no">>))/binary>>)),
    ?assertMatch({malformed, _}, decode_init(<<0, 0:32/little>>)).

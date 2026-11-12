%%% @doc Deterministic state-patch support shared by `lua-patch@1.0' and the
%%% process cache. A patch list is an ordered list of messages shaped as
%%% `#{ <<"path">> => Path, <<"value">> => Value }' or
%%% `#{ <<"path">> => Path, <<"delete">> => true }'.
-module(hb_state_patch).
-export([apply/4, apply/5, validate/2, partition/2]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Top-level keys owned by the runtime, compared case-insensitively. Keys with
%% a `scheduler' prefix or a `-device'/`-output-prefixes' suffix are reserved
%% by rule in `reserved_root/1', since `process@1.0' derives them per device.
-define(RESERVED_PATCH_KEYS, [
    <<"ao-types">>,
    <<"at-slot">>,
    <<"authority">>,
    <<"commitments">>,
    <<"committers">>,
    <<"device">>,
    <<"function">>,
    <<"id">>,
    <<"initialized">>,
    <<"input-prefix">>,
    <<"keys">>,
    <<"module">>,
    <<"output-prefixes">>,
    <<"path">>,
    <<"priv">>,
    <<"process">>,
    <<"remove">>,
    <<"results">>,
    <<"set">>,
    <<"snapshot">>,
    <<"type">>,
    <<"verify">>
]).

%% Keys the cache codec interprets at any depth. A patch segment may never
%% name them; a patch value may carry `commitments' and private keys only as
%% the runtime-added messages that `dev_lua_patch' decoding produces.
-define(RESERVED_SEGMENT_KEYS, [<<"ao-types">>, <<"commitments">>, <<"device">>]).

%% @doc Apply application patches and replace the per-slot result. `Slot' is
%% optional so the Lua device can patch before process@1.0 assigns the slot.
apply(Base, Patches, Results, Opts) ->
    apply(Base, Patches, Results, undefined, Opts).
apply(Base, Patches, RawResults, Slot, Opts)
        when is_list(Patches), is_map(RawResults) ->
    Prepared = base_set(Base, <<"results">>, unset, Opts),
    case {apply_patches(Prepared, Patches, Opts), lower_value(RawResults)} of
        {_, {error, _} = Error} -> Error;
        {{ok, Patched}, {ok, Results}} ->
            WithResults = base_set(Patched, <<"results">>, Results, Opts),
            case Slot of
                undefined -> {ok, WithResults};
                _ -> {ok, base_set(WithResults, <<"at-slot">>, Slot, Opts)}
            end;
        {Error, _} -> Error
    end;
apply(_Base, _Patches, _Results, _Slot, _Opts) ->
    patch_error(<<"Delta requires list `patches' and message `results'.">>).

%% @doc Validate patches without mutating a message.
validate(Patches, Opts) when is_list(Patches) ->
    validate_patches(Patches, Opts);
validate(_Patches, _Opts) ->
    patch_error(<<"Delta `patches' must be a list.">>).

validate_patches([], _Opts) -> ok;
validate_patches([Patch | Rest], Opts) ->
    case parse_patch(Patch, Opts) of
        {ok, _, _} -> validate_patches(Rest, Opts);
        Error -> Error
    end.

%% @doc Split patches into those that validate and those that do not, keeping
%% order. Used by callers whose policy is to drop rather than reject.
partition(Patches, Opts) when is_list(Patches) ->
    lists:partition(
        fun(Patch) ->
            case parse_patch(Patch, Opts) of
                {ok, _, _} -> true;
                _ -> false
            end
        end,
        Patches
    ).

%% @doc Apply patches with plain `message@1.0' semantics. Intermediate path
%% segments are read as map keys, never resolved through the base's device,
%% so the live base (`lua-patch@1.0') and the replay base (`process@1.0') produce
%% the same state.
apply_patches(Base, [], _Opts) ->
    {ok, Base};
apply_patches(Base, [Patch | Rest], Opts) ->
    case parse_patch(Patch, Opts) of
        {ok, Parts, Op} ->
            case set_in(Base, Parts, Op, Opts) of
                {ok, Patched} -> apply_patches(Patched, Rest, Opts);
                Error -> Error
            end;
        Error -> Error
    end.

set_in(Msg, [Key], delete, Opts) ->
    case is_plain(Msg, Key) of
        true -> {ok, maps:remove(Key, Msg)};
        false -> message_set(Msg, Key, unset, <<"deep">>, Opts)
    end;
set_in(Msg, [Key], {set, Value}, Opts) ->
    case is_plain(Msg, Key) andalso not is_map(Value) andalso not is_list(Value) of
        true -> {ok, Msg#{ Key => Value }};
        false -> message_set(Msg, Key, Value, <<"deep">>, Opts)
    end;
set_in(Msg, [Key | Rest], Op, Opts) ->
    case child(Msg, Key, Opts) of
        not_found ->
            case set_in(#{}, Rest, Op, Opts) of
                {ok, NewChild} -> {ok, Msg#{ Key => NewChild }};
                Error -> Error
            end;
        Child when is_map(Child); is_list(Child) ->
            case set_in(hb_ao:normalize_keys(Child, Opts), Rest, Op, Opts) of
                {ok, NewChild} ->
                    case is_plain(Msg, Key) of
                        true -> {ok, Msg#{ Key => NewChild }};
                        false ->
                            message_set(Msg, Key, NewChild, <<"explicit">>, Opts)
                    end;
                Error -> Error
            end;
        _ ->
            patch_error(<<"Patch parent is not a message.">>)
    end.

%% @doc `message@1.0' lookup: exact key, then its lower-case form.
child(Msg, Key, Opts) ->
    case hb_maps:get(Key, Msg, not_found, Opts) of
        not_found -> hb_maps:get(hb_util:to_lower(Key), Msg, not_found, Opts);
        Value -> Value
    end.

%% @doc Whether `Key' of `Msg' can be set or removed as a plain map key.
%% `message@1.0''s `set' walks every key of the message and works out which keys
%% its commitments cover; on a process state, with one key per player, that was
%% most of a patch's cost (28 ms a slot for the game). A level with no
%% commitments and no stored link for the key has nothing for it to do beyond
%% the put or remove itself, so both give the same message
%% (`fast_set_matches_message_set_test'). Everything else still goes through
%% `message@1.0'.
is_plain(Msg, Key) ->
    not is_map_key(<<"commitments">>, Msg)
        andalso not is_map_key(<<Key/binary, "+link">>, Msg).

%% @doc `hb_ao:set/4' for the per-slot keys `apply/5' writes, plain when the
%% base allows it (`is_plain/2'). `results' is set only after it was unset, so
%% a deep merge has nothing to merge with.
base_set(Base, Key, unset, Opts) ->
    case is_plain(Base, Key) of
        true -> maps:remove(Key, Base);
        false -> hb_ao:set(Base, Key, unset, Opts)
    end;
base_set(Base, Key, Value, Opts) ->
    case is_plain(Base, Key)
            andalso (not is_map(Value) orelse not is_map_key(Key, Base)) of
        true -> Base#{ Key => Value };
        false -> hb_ao:set(Base, Key, Value, Opts)
    end.

message_set(Msg, Key, Value, Mode, Opts) ->
    hb_ao:raw(
        <<"message@1.0">>,
        <<"set">>,
        Msg,
        #{ Key => Value, <<"set-mode">> => Mode },
        Opts
    ).

parse_patch(Patch, Opts) when is_map(Patch) ->
    Path = field(<<"path">>, Patch, not_found, Opts),
    case {patch_path(Path), patch_operation(Patch, Opts)} of
        {{ok, Parts}, {ok, delete}} -> {ok, Parts, delete};
        {{ok, Parts}, {ok, {set, Value}}} ->
            case check_value(Value) of
                ok ->
                    case lower_value(Value) of
                        {ok, Lower} -> {ok, Parts, {set, Lower}};
                        Error -> Error
                    end;
                Error -> Error
            end;
        {Error = {error, _}, _} -> Error;
        {_, Error = {error, _}} -> Error
    end;
parse_patch(_Patch, _Opts) ->
    patch_error(<<"Every patch must be a message.">>).

%% @doc A patch field. A patch decoded from Lua is a plain map, so its fields
%% are read directly: resolving one through AO-Core first normalises the whole
%% value, which costs time on every patch and, on v0.11, raises on a value
%% whose `commitments' is malformed instead of letting `check_value/1' refuse
%% it. A patch read back from the store may hold the field as a link
%% (`value+link'), which only AO-Core resolves.
field(Key, Patch, Default, Opts) ->
    case maps:find(Key, Patch) of
        {ok, Value} -> Value;
        error ->
            case maps:is_key(<<Key/binary, "+link">>, Patch) of
                true -> hb_ao:get(Key, Patch, Default, Opts);
                false -> Default
            end
    end.

patch_operation(Patch, Opts) ->
    Delete = field(<<"delete">>, Patch, false, Opts),
    Value = field(<<"value">>, Patch, not_found, Opts),
    case {Delete, Value} of
        {true, _} -> {ok, delete};
        {<<"true">>, _} -> {ok, delete};
        {false, not_found} -> patch_error(<<"Patch is missing `value'.">>);
        {<<"false">>, not_found} -> patch_error(<<"Patch is missing `value'.">>);
        {false, _} -> {ok, {set, Value}};
        {<<"false">>, _} -> {ok, {set, Value}};
        _ -> patch_error(<<"Patch `delete' must be boolean.">>)
    end.

%% @doc Split a patch path into segments, checking every one of them. A
%% binary path may start with one `/'; any other empty segment is refused, as
%% is any segment the cache codec or the runtime would interpret.
patch_path(Path) when is_binary(Path) ->
    case binary:split(Path, <<"/">>, [global]) of
        [<<>> | Parts] -> patch_parts(Parts, true, []);
        Parts -> patch_parts(Parts, true, [])
    end;
patch_path(Path = [_ | _]) ->
    case lists:all(fun is_binary/1, Path) of
        true -> patch_parts(Path, true, []);
        false -> patch_error(<<"Patch `path' must be a path string.">>)
    end;
patch_path([]) ->
    patch_error(<<"Patch path cannot target the message root.">>);
patch_path(_) ->
    patch_error(<<"Patch `path' must be a path string.">>).

patch_parts([], true, []) ->
    patch_error(<<"Patch path cannot target the message root.">>);
patch_parts([], _Root, Acc) ->
    {ok, lists:reverse(Acc)};
patch_parts([Part | Rest], Root, Acc) ->
    case segment_error(Part, Root) of
        ok -> patch_parts(Rest, false, [hb_util:lowercase_key(Part) | Acc]);
        Error -> Error
    end.

%% @doc Lowercase every key of a patch value, as v0.11 stores and reads every
%% message key (`dev_structured:decode_keys/1', `dev_message:get/4'). A
%% mixed-case key would read back lowercased from the cache, and two writes to
%% it in different slots would then become two keys that the codec joins into
%% a list. Keys that differ only in case are refused rather than joined.
%% Runtime-added `commitments' and private messages are kept as they are.
lower_value(Value) when is_map(Value) ->
    lower_entries(maps:to_list(Value), #{});
lower_value(Value) when is_list(Value) ->
    lower_list(Value, []);
lower_value(Value) ->
    {ok, Value}.

lower_list([], Acc) -> {ok, lists:reverse(Acc)};
lower_list([Value | Rest], Acc) ->
    case lower_value(Value) of
        {ok, Lower} -> lower_list(Rest, [Lower | Acc]);
        Error -> Error
    end.

lower_entries([], Acc) -> {ok, Acc};
lower_entries([{Key, Value} | Rest], Acc) ->
    Kept =
        is_binary(Key) andalso
            (Key =:= <<"commitments">> orelse hb_private:is_private(Key)),
    LowerKey = hb_util:lowercase_key(Key),
    LowerValue =
        case Kept of
            true -> {ok, Value};
            false -> lower_value(Value)
        end,
    case {is_map_key(LowerKey, Acc), LowerValue} of
        {true, _} ->
            patch_error(<<"Patch value has keys that differ only in case.">>);
        {false, {ok, Lower}} -> lower_entries(Rest, Acc#{ LowerKey => Lower });
        {false, Error} -> Error
    end.

segment_error(<<>>, _Root) ->
    patch_error(<<"Patch path has an empty segment.">>);
segment_error(Part, Root) ->
    Lower = hb_util:to_lower(Part),
    case binary:match(Part, <<"/">>) =/= nomatch
            orelse is_link_key(Lower)
            orelse hb_private:is_private(Lower)
            orelse lists:member(Lower, ?RESERVED_SEGMENT_KEYS)
            orelse (Root andalso reserved_root(Lower)) of
        true -> patch_error(<<"Patch targets a runtime-owned key.">>);
        false -> ok
    end.

reserved_root(<<"scheduler", _/binary>>) -> true;
reserved_root(Key) ->
    lists:member(Key, ?RESERVED_PATCH_KEYS)
        orelse has_suffix(Key, <<"-device">>)
        orelse has_suffix(Key, <<"-output-prefixes">>).

has_suffix(Key, Suffix) ->
    KeySize = byte_size(Key),
    SuffixSize = byte_size(Suffix),
    KeySize >= SuffixSize
        andalso binary:part(Key, KeySize - SuffixSize, SuffixSize) =:= Suffix.

%% @doc Refuse value keys that do not survive a cache round-trip: a `+link'
%% suffix (read back as a link to load), `/' or empty (split into, or dropped
%% from, the stored path) and `ao-types'. Runtime-added `commitments' and
%% private messages are accepted as messages and not inspected.
check_value(Value) when is_map(Value) ->
    check_entries(maps:next(maps:iterator(Value)));
check_value(Value) when is_list(Value) ->
    check_list(Value);
check_value(_Value) ->
    ok.

check_list([]) -> ok;
check_list([Value | Rest]) ->
    case check_value(Value) of
        ok -> check_list(Rest);
        Error -> Error
    end.

check_entries(none) -> ok;
check_entries({Key, Value, Iter}) ->
    case check_entry(Key, Value) of
        ok -> check_entries(maps:next(Iter));
        Error -> Error
    end.

check_entry(<<"commitments">>, Value) when is_map(Value) -> ok;
check_entry(<<"priv", _/binary>>, Value) when is_map(Value) -> ok;
check_entry(Key, Value) when is_binary(Key) ->
    Lower = hb_util:to_lower(Key),
    case Key =:= <<>>
            orelse binary:match(Key, <<"/">>) =/= nomatch
            orelse is_link_key(Lower)
            orelse hb_private:is_private(Lower)
            orelse lists:member(Lower, [<<"ao-types">>, <<"commitments">>]) of
        true -> patch_error(<<"Patch value has a key the cache cannot store.">>);
        false -> check_value(Value)
    end;
check_entry(_Key, Value) ->
    check_value(Value).

patch_error(Body) ->
    {error, #{ <<"status">> => 422, <<"body">> => Body }}.

%% A `+link' key names a stored reference, not a value a patch may write.
is_link_key(Key) when byte_size(Key) >= 5 ->
    binary:part(Key, byte_size(Key) - 5, 5) =:= <<"+link">>;
is_link_key(_) -> false.

%%% Tests

%% @doc The plain-map fast path gives the message `message@1.0' gives.
fast_set_matches_message_set_test() ->
    Opts = #{ <<"hashpath">> => ignore },
    Base = #{
        <<"keep">> => <<"k">>,
        <<"player-a">> => <<"{\"gold\":1}">>,
        <<"count">> => 7,
        <<"nested">> => #{ <<"inner">> => <<"i">>, <<"other">> => <<"o">> }
    },
    Patches = [
        [set(<<"/player-a">>, <<"{\"gold\":2}">>)],
        [set(<<"/player-b">>, <<"{\"gold\":3}">>)],
        [#{ <<"path">> => <<"/player-a">>, <<"delete">> => true }],
        [#{ <<"path">> => <<"/absent">>, <<"delete">> => true }],
        [set(<<"/count">>, 8)],
        [set(<<"/nested/inner">>, <<"changed">>)],
        [#{ <<"path">> => <<"/nested/other">>, <<"delete">> => true }],
        [set(<<"/fresh/deep/key">>, <<"v">>)],
        [set(<<"/player-a">>, <<"x">>), set(<<"/player-a">>, <<"y">>),
            #{ <<"path">> => <<"/keep">>, <<"delete">> => true }]
    ],
    lists:foreach(
        fun(PatchList) ->
            {ok, Fast} = apply_patches(Base, PatchList, Opts),
            {ok, Slow} = reference_apply(Base, PatchList, Opts),
            ?assertEqual({PatchList, Slow}, {PatchList, Fast})
        end,
        Patches
    ).

%% The same patches applied through `message@1.0' alone, as before the fast
%% path existed.
reference_apply(Msg, [], _Opts) -> {ok, Msg};
reference_apply(Msg, [Patch | Rest], Opts) ->
    {ok, Parts, Op} = parse_patch(Patch, Opts),
    {ok, Next} = reference_set_in(Msg, Parts, Op, Opts),
    reference_apply(Next, Rest, Opts).

reference_set_in(Msg, [Key], delete, Opts) ->
    message_set(Msg, Key, unset, <<"deep">>, Opts);
reference_set_in(Msg, [Key], {set, Value}, Opts) ->
    message_set(Msg, Key, Value, <<"deep">>, Opts);
reference_set_in(Msg, [Key | Rest], Op, Opts) ->
    case child(Msg, Key, Opts) of
        not_found ->
            {ok, NewChild} = reference_set_in(#{}, Rest, Op, Opts),
            {ok, Msg#{ Key => NewChild }};
        Child ->
            {ok, NewChild} =
                reference_set_in(hb_ao:normalize_keys(Child, Opts), Rest, Op, Opts),
            message_set(Msg, Key, NewChild, <<"explicit">>, Opts)
    end.

store_opts() ->
    #{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"hashpath">> => ignore
    }.

set(Path, Value) -> #{ <<"path">> => Path, <<"value">> => Value }.

%% @doc Every segment is checked, so `+link' suffixes (read back from the cache
%% as links to load), reserved names in any case, nested codec keys and empty
%% segments are refused, while ordinary published keys are accepted.
patch_path_validation_test() ->
    Opts = #{ <<"hashpath">> => ignore },
    Refused =
        [
            <<"/results+link">>, <<"/device+link">>, <<"/commitments+link">>,
            <<"/at-slot+link">>, <<"/x+link">>, <<"/players/abc+link">>,
            <<"/Device">>, <<"/DEVICE">>, <<"/Results">>, <<"/ao-types">>,
            <<"/execution-device">>, <<"/scheduler">>, <<"/scheduler-location">>,
            <<"/authority">>, <<"/module">>, <<"/type">>, <<"/function">>,
            <<"/priv-x">>, <<"/a/commitments">>, <<"/a/device">>,
            <<"/a/ao-types">>, <<"/a//b">>, <<"/a/">>, <<"//">>, <<"/">>,
            [<<"device">>], [<<"a">>, <<"b+link">>]
        ],
    lists:foreach(
        fun(Path) ->
            ?assertMatch(
                {{error, #{ <<"status">> := 422 }}, _},
                {validate([set(Path, <<"v">>)], Opts), Path}
            )
        end,
        Refused
    ),
    Accepted =
        [
            <<"/player-AbC_-0123456789abcdefghijklmnopqrstuvwxy">>,
            <<"counter">>, <<"/a/b/c">>, <<"/x+LINKS">>, <<"/a/type">>,
            <<"/a/results">>, [<<"a">>, <<"b">>]
        ],
    lists:foreach(
        fun(Path) ->
            ?assertEqual({ok, Path}, {validate([set(Path, <<"v">>)], Opts), Path})
        end,
        Accepted
    ).

%% @doc Value keys the cache cannot store faithfully are refused.
patch_value_validation_test() ->
    Opts = #{ <<"hashpath">> => ignore },
    lists:foreach(
        fun(Key) ->
            ?assertMatch(
                {error, #{ <<"status">> := 422 }},
                validate([set(<<"/t">>, #{ <<"n">> => [#{ Key => <<"v">> }] })], Opts)
            )
        end,
        [<<"x+link">>, <<"a/b">>, <<>>, <<"ao-types">>, <<"commitments">>, <<"priv">>]
    ),
    ?assertEqual(
        ok,
        validate(
            [set(<<"/t">>, #{ 1 => <<"a">>, <<"Key">> => #{ <<"priv">> => #{} } })],
            Opts
        )
    ).

%% @doc A refused patch fails the whole delta, and an accepted state survives
%% a store round-trip unchanged.
link_patch_cannot_reach_store_test() ->
    Opts = store_opts(),
    Base = #{ <<"device">> => <<"process@1.0">>, <<"keep">> => <<"k">> },
    ?assertMatch(
        {error, #{ <<"status">> := 422 }},
        apply(Base, [set(<<"/player-x+link">>, <<"null">>)], #{}, 1, Opts)
    ),
    {ok, State} =
        apply(Base, [set(<<"/player-x">>, <<"null">>)], #{}, 1, Opts),
    {ok, ID} = hb_cache:write(State, Opts),
    {ok, Read} = hb_cache:read(ID, Opts),
    ?assertEqual(
        <<"null">>,
        hb_maps:get(<<"player-x">>, hb_cache:ensure_all_loaded(Read, Opts))
    ).

%% @doc Nested patches never resolve through the base's device: the live
%% `lua-patch@1.0' base and the replayed `process@1.0' base give the plain result.
patch_ignores_base_device_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = #{ <<"hashpath">> => ignore },
        Run =
            fun(Device, Path) ->
                Self = self(),
                Base = #{ <<"device">> => Device, <<"keep">> => <<"k">> },
                Pid =
                    spawn(fun() ->
                        Self ! {self(), catch apply(Base, [set(Path, <<"v">>)], #{}, Opts)}
                    end),
                receive {Pid, Res} -> Res
                after 10000 -> exit(Pid, kill), timeout
                end
            end,
        lists:foreach(
            fun(Key) ->
                Path = <<"/", Key/binary, "/x">>,
                Expected = #{ <<"x">> => <<"v">> },
                lists:foreach(
                    fun(Device) ->
                        ?assertMatch(
                            {{ok, #{ Key := Expected }}, _, _},
                            {Run(Device, Path), Device, Key}
                        )
                    end,
                    [<<"process@1.0">>, <<"lua-patch@1.0">>]
                )
            end,
            [<<"now">>, <<"schedule">>, <<"compute">>, <<"slot">>, <<"info">>,
                <<"functions">>, <<"init">>, <<"normalize">>, <<"plain">>]
        )
    end}.

%% @doc Ordinary patch semantics are unchanged: a nested set merges into an
%% existing message, a delete removes one key, a list parent becomes an
%% ordered message, and a scalar parent is a clean 422.
patch_semantics_test() ->
    Opts = #{ <<"hashpath">> => ignore },
    Base =
        #{
            <<"a">> => #{ <<"b">> => <<"1">>, <<"c">> => <<"2">> },
            <<"l">> => [<<"p">>, <<"q">>],
            <<"s">> => <<"str">>
        },
    {ok, Next} =
        apply(
            Base,
            [
                set(<<"/a/d">>, <<"3">>),
                #{ <<"path">> => <<"/a/b">>, <<"delete">> => true },
                set(<<"/l/2">>, <<"Q">>),
                set(<<"/n/m">>, <<"4">>)
            ],
            #{},
            Opts
        ),
    ?assertEqual(#{ <<"c">> => <<"2">>, <<"d">> => <<"3">> }, maps:get(<<"a">>, Next)),
    ?assertEqual(#{ <<"1">> => <<"p">>, <<"2">> => <<"Q">> }, maps:get(<<"l">>, Next)),
    ?assertEqual(#{ <<"m">> => <<"4">> }, maps:get(<<"n">>, Next)),
    ?assertMatch(
        {error, #{ <<"status">> := 422 }},
        apply(Base, [set(<<"/s/x">>, <<"1">>)], #{}, Opts)
    ).

%% @doc Keys are lowercased as the cache stores them: a mixed-case path
%% replaces the lowercase key a cached base holds, value and result keys are
%% lowercased at every depth, and keys that differ only in case are refused.
lowercase_keys_test() ->
    Opts = #{ <<"hashpath">> => ignore },
    Cached = #{ <<"mixed-key">> => <<"m-1">> },
    {ok, Next} =
        apply(
            Cached,
            [
                #{ <<"path">> => <<"/Mixed-Key">>, <<"value">> => <<"m-2">> },
                #{
                    <<"path">> => <<"/Player-A/Stats">>,
                    <<"value">> => #{ <<"Gold">> => <<"1">> }
                }
            ],
            #{ <<"Output">> => #{ <<"Data">> => <<"x">> } },
            Opts
        ),
    ?assertEqual(
        #{
            <<"mixed-key">> => <<"m-2">>,
            <<"player-a">> => #{ <<"stats">> => #{ <<"gold">> => <<"1">> } },
            <<"results">> => #{ <<"output">> => #{ <<"data">> => <<"x">> } }
        },
        Next
    ),
    ?assertMatch(
        {error, #{ <<"status">> := 422 }},
        apply(
            #{},
            [#{ <<"path">> => <<"/k">>, <<"value">> => #{ <<"A">> => <<"1">>, <<"a">> => <<"2">> } }],
            #{},
            Opts
        )
    ).

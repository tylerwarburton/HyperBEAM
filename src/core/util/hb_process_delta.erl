%%% @doc Deterministic state-patch support shared by `lua@5.3b' and the
%%% process cache. A patch list is an ordered list of messages shaped as
%%% `#{ <<"path">> => Path, <<"value">> => Value }' or
%%% `#{ <<"path">> => Path, <<"delete">> => true }'.
-module(hb_process_delta).
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
%% the runtime-added messages that `dev_lua_53b' decoding produces.
-define(RESERVED_SEGMENT_KEYS, [<<"ao-types">>, <<"commitments">>, <<"device">>]).

%% @doc Apply application patches and replace the per-slot result. `Slot' is
%% optional so the Lua device can patch before process@1.0 assigns the slot.
apply(Base, Patches, Results, Opts) ->
    apply(Base, Patches, Results, undefined, Opts).
apply(Base, Patches, Results, Slot, Opts)
        when is_list(Patches), is_map(Results) ->
    Prepared = hb_ao:set(Base, <<"results">>, unset, Opts),
    case apply_patches(Prepared, Patches, Opts) of
        {ok, Patched} ->
            WithResults = hb_ao:set(Patched, <<"results">>, Results, Opts),
            case Slot of
                undefined -> {ok, WithResults};
                _ ->
                    {ok,
                        hb_ao:set(
                            WithResults,
                            <<"at-slot">>,
                            Slot,
                            Opts
                        )}
            end;
        Error -> Error
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
%% so the live base (`lua@5.3b') and the replay base (`process@1.0') produce
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
    message_set(Msg, Key, unset, <<"deep">>, Opts);
set_in(Msg, [Key], {set, Value}, Opts) ->
    message_set(Msg, Key, Value, <<"deep">>, Opts);
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
                    message_set(Msg, Key, NewChild, <<"explicit">>, Opts);
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

message_set(Msg, Key, Value, Mode, Opts) ->
    hb_ao:raw(
        <<"message@1.0">>,
        <<"set">>,
        Msg,
        #{ Key => Value, <<"set-mode">> => Mode },
        Opts
    ).

parse_patch(Patch, Opts) when is_map(Patch) ->
    Path = hb_ao:get(<<"path">>, Patch, not_found, Opts),
    case {patch_path(Path), patch_operation(Patch, Opts)} of
        {{ok, Parts}, {ok, delete}} -> {ok, Parts, delete};
        {{ok, Parts}, {ok, {set, Value}}} ->
            case check_value(Value) of
                ok -> {ok, Parts, {set, Value}};
                Error -> Error
            end;
        {Error = {error, _}, _} -> Error;
        {_, Error = {error, _}} -> Error
    end;
parse_patch(_Patch, _Opts) ->
    patch_error(<<"Every patch must be a message.">>).

patch_operation(Patch, Opts) ->
    Delete = hb_ao:get(<<"delete">>, Patch, false, Opts),
    Value = hb_ao:get(<<"value">>, Patch, not_found, Opts),
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
        ok -> patch_parts(Rest, false, [Part | Acc]);
        Error -> Error
    end.

segment_error(<<>>, _Root) ->
    patch_error(<<"Patch path has an empty segment.">>);
segment_error(Part, Root) ->
    Lower = hb_util:to_lower(Part),
    case binary:match(Part, <<"/">>) =/= nomatch
            orelse hb_link:is_link_key(Lower)
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
            orelse hb_link:is_link_key(Lower)
            orelse hb_private:is_private(Lower)
            orelse lists:member(Lower, [<<"ao-types">>, <<"commitments">>]) of
        true -> patch_error(<<"Patch value has a key the cache cannot store.">>);
        false -> check_value(Value)
    end;
check_entry(_Key, Value) ->
    check_value(Value).

patch_error(Body) ->
    {error, #{ <<"status">> => 422, <<"body">> => Body }}.

%%% Tests

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
%% `lua@5.3b' base and the replayed `process@1.0' base give the plain result.
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
                    [<<"process@1.0">>, <<"lua@5.3b">>]
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

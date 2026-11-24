%%% @doc What `lua-patch@1.0' and `wasm-patch@1.0' share: how a slot's
%%% request is built from the scheduled message, and how a runtime's result is
%%% normalised, checked and applied as ordered patches. The runtimes differ only
%%% in how the request reaches the code and how the result comes back (Luerl
%%% terms, or `hb_wasm_patch_codec'); both hand `decode/2' the Luerl shape, so a
%%% contract publishes the same terms on either.
-module(lib_patch_exec).
-export([in_slot/1, slot/2, request/5, params/4, literal_input_links/2, encode/3]).
-export([decode/2, result/6, error_result/1, apply_result/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Request

%% @doc Whether the call computes a slot of a `process@1.0' process, which
%% marks the state it initializes (`dev_process:init/3') privately, out of
%% reach of a request.
in_slot(Priv) ->
    is_map_key(<<"initialized">>, Priv).

%% @doc The slot number of a request, or `undefined'.
slot(Req, Opts) ->
    case hb_maps:get(<<"slot">>, Req, not_found, Opts) of
        not_found -> undefined;
        Raw ->
            case catch hb_util:int(Raw) of
                N when is_integer(N) -> N;
                _ -> undefined
            end
    end.

%% @doc The entry point and the parameters of a call: `{Function, Params}'.
%% Scheduled messages are input to the contract, never instructions for
%% choosing another entry point or replacing its execution context.
request(Key, Base, Req, InSlot, Opts) ->
    FunctionSources = case InSlot of
        true -> [{{as, <<"message@1.0">>, Base}, <<"function">>}];
        false -> [
            {Req, <<"body/function">>},
            {Req, <<"function">>},
            {{as, <<"message@1.0">>, Base}, <<"function">>}
        ]
    end,
    Function =
        case InSlot andalso base_literal([<<"function">>], Base, Opts) of
            {ok, Literal} -> Literal;
            not_found -> Key;
            _ ->
                hb_ao:get_first(
                    FunctionSources,
                    Key,
                    Opts#{ <<"hashpath">> => ignore }
                )
        end,
    {Function, params(Req, InSlot, InSlot andalso literal_input_links(Base, Opts), Opts)}.

%% @doc The parameters of a call, for a runtime with one entry point. In a slot
%% they are the scheduled message alone, with literal links when the process
%% asks for them (`Literal', from `literal_input_links/2', which a caller may
%% work out once per process: it reads only the signed definition).
params(Req, true, true, Opts) -> [literal_request(Req, Opts)];
params(Req, true, false, _Opts) -> [Req];
params(Req, false, _Literal, Opts) ->
    hb_ao:get_first(
        [
            {Req, <<"body/parameters">>},
            {Req, <<"parameters">>}
        ],
        [Req],
        Opts#{ <<"hashpath">> => ignore }
    ).

%% @doc A process can consume tag values as literal URIs. This policy comes
%% from its signed definition, not the incoming message or node defaults.
literal_input_links(Base, Opts) ->
    Paths = [[<<"process">>, <<"lua-input-links">>], [<<"lua-input-links">>]],
    case first_literal(Paths, Base, Opts) of
        {ok, Policy} -> Policy =:= <<"literal">>;
        not_found -> false;
        resolve ->
            hb_ao:get_first([
                {{as, <<"message@1.0">>, Base}, <<"process/lua-input-links">>},
                {{as, <<"message@1.0">>, Base}, <<"lua-input-links">>}
            ], <<"resolve">>, Opts) =:= <<"literal">>
    end.

first_literal([], _Base, _Opts) -> not_found;
first_literal([Path | Rest], Base, Opts) ->
    case base_literal(Path, Base, Opts) of
        not_found -> first_literal(Rest, Base, Opts);
        Found -> Found
    end.

%% @doc A binary at `Path' of the base, read as `message@1.0' reads it: each
%% key lowercased, then matched exactly. The base a device function receives
%% is already normalised (`hb_ao' resolve stage 2), so this is the value a
%% resolution through `{as, <<"message@1.0">>, Base}' returns -- after
%% normalising every published key of the base twice more, once to set the
%% device and once to read. Anything but a map on the way and a binary at
%% the end is left to that resolution (`resolve').
base_literal([], Value, _Opts) when is_binary(Value) -> {ok, Value};
base_literal([], _Value, _Opts) -> resolve;
base_literal([Key | Rest], Msg, Opts) when is_map(Msg) ->
    Lower = hb_util:lowercase_key(Key),
    case maps:find(Lower, Msg) of
        {ok, Value} -> base_literal(Rest, Value, Opts);
        error ->
            case maps:is_key(<<Lower/binary, "+link">>, Msg) of
                true -> resolve;
                false -> not_found
            end
    end;
base_literal(_Path, _Value, _Opts) ->
    resolve.

%% @doc Load the scheduled header and its signature metadata, then preserve
%% payload links as strings. An unknown tag must not make execution depend on
%% retrieval of an arbitrary remote object before the contract can reject it.
%% The commitments are the ones the scheduled ID names, never every one the
%% store holds for the same content: two signers' identical messages share an
%% uncommitted ID, and merging them gives each the other's signature.
literal_request(Req, Opts) ->
    Body = hb_cache:ensure_loaded(hb_maps:get(<<"body">>, Req, #{}, Opts), Opts),
    Commitments = hb_cache:ensure_all_loaded(
        hb_maps:get(<<"commitments">>, Body, #{}, Opts), Opts),
    literal_links(Req#{<<"body">> => Body#{<<"commitments">> => Commitments}}, Opts).

%% A `lazy' link is the store's own handle on a value of this message: the
%% cache writes every value of 60 bytes or more (`?DIRECT_VALUE_LENGTH') as a
%% `ni:' name and reads it back as a lazy link. Load it, so a long tag reaches
%% the contract as its bytes and not as `ni:///sha-256;...'. Only a link
%% decoded from a tag's own text stays a literal URI.
literal_links(Link = {link, _, #{<<"lazy">> := true}}, Opts) ->
    literal_links(hb_cache:ensure_loaded(Link, Opts), Opts);
literal_links({link, URI, _}, _Opts) -> URI;
literal_links(Map, Opts) when is_map(Map) ->
    maps:map(fun(_, V) -> literal_links(V, Opts) end, Map);
literal_links(List, Opts) when is_list(List) -> [literal_links(V, Opts) || V <- List];
literal_links(Value, _Opts) -> Value.

%% @doc Encode AO-Core maps for a runtime without eagerly loading whole
%% trees. `luerl' gives the `{Key, Value}' lists Luerl encodes; `term' gives
%% plain maps and lists (`hb_wasm_patch_codec').
encode(Map, Shape, Opts) when is_map(Map) ->
    Normalized = assign_to_target(Map),
    case hb_util:is_ordered_list(Normalized, Opts) of
        true -> encode(hb_util:message_to_ordered_list(Normalized), Shape, Opts);
        false ->
            Encoded =
                maps:map(
                    fun(Key, Value) -> encode_value(Key, Value, Shape, Opts) end,
                    Normalized
                ),
            case Shape of
                luerl -> maps:to_list(Encoded);
                term -> Encoded
            end
    end;
encode(List, Shape, Opts) when is_list(List) ->
    lists:map(fun(Value) -> encode(Value, Shape, Opts) end, List);
encode(Link, Shape, Opts) when ?IS_LINK(Link) ->
    encode(hb_cache:ensure_all_loaded(Link, Opts), Shape, Opts);
encode(Atom, _Shape, _Opts) when is_atom(Atom) andalso Atom =/= false andalso Atom =/= true ->
    hb_util:bin(Atom);
encode(Value, _Shape, _Opts) -> Value.

%% @doc A contract sees only the signed commitments of a message, each with
%% its committer as an address, as `lua@5.3a' gives them. v0.11 holds a
%% committer as a URI (`uri-did-ar-...'); a contract that took that for an
%% account id would give every player a second, empty account. Unsigned
%% commitments are hidden because each node makes its own, so a script
%% reading them could compute differently on two nodes.
encode_value(<<"commitments">>, Commitments, Shape, Opts) when is_map(Commitments) ->
    Signed =
        maps:fold(
            fun(ID, Commitment = #{ <<"committer">> := Committer }, Acc) ->
                    Acc#{
                        ID => assign_to_commitment(Commitment#{
                            <<"committer">> => hb_util:committer_address(Committer)
                        })
                    };
               (_ID, _Unsigned, Acc) ->
                    Acc
            end,
            #{},
            Commitments
        ),
    encode(Signed, Shape, Opts);
encode_value(_Key, Value, Shape, Opts) ->
    encode(Value, Shape, Opts).

%% An L1 message uses the signed `Assign-To' tag because a process has no
%% private key and should not receive AR. The contract door speaks AO-Core and
%% requires `target'. Expose the single-process form as its equivalent target;
%% the verified commitment is translated below by the same rule.
assign_to_target(Map) ->
    case {maps:is_key(<<"target">>, Map), maps:get(<<"assign-to">>, Map, undefined)} of
        {false, Target} when is_binary(Target), byte_size(Target) =:= 43 ->
            Map#{ <<"target">> => Target };
        _ -> Map
    end.

assign_to_commitment(Commitment) ->
    case maps:get(<<"committed">>, Commitment, []) of
        Committed when is_list(Committed) ->
            case lists:member(<<"assign-to">>, Committed)
                    andalso not lists:member(<<"target">>, Committed) of
                true -> Commitment#{ <<"committed">> => [<<"target">> | Committed] };
                false -> Commitment
            end;
        _ -> Commitment
    end.

%%% Result

%% @doc Normalise a Luerl-shaped value (a table is a list of `{Key, Value}',
%% an empty table `[]') and any message commitments it carries.
decode(Value, Opts) ->
    hb_message:normalize_commitments(decode_value(Value, Opts), Opts, verify).

decode_value([], _Opts) -> #{};
decode_value(Value = [{_K, _V} | _], Opts) ->
    decode_value(decode_table(Value, Opts, #{}), Opts);
decode_value(Value, Opts) when is_map(Value) ->
    case hb_util:is_ordered_list(Value, Opts) of
        true ->
            lists:map(
                fun(Item) -> decode_value(Item, Opts) end,
                hb_util:message_to_ordered_list(Value)
            );
        false -> Value
    end;
decode_value(Value, _Opts) -> Value.

decode_table([], _Opts, Acc) -> Acc;
decode_table([{Key, Value} | Rest], Opts, Acc) ->
    decode_table(Rest, Opts, Acc#{ Key => decode_value(Value, Opts) }).

%% @doc Turn a decoded result into the next state. `Ctx' names the device
%% (`device'), the phases its decode and apply are noted as (`decode_phase',
%% `apply_phase'), and the private state the next state keeps (`priv'). The
%% applied delta goes into it as `process-cache-delta', which is what the
%% process cache stores for the slot.
result(Status, Decoded, DecodeMicros, Base, Ctx, Opts) ->
    #{ device := Device, priv := NewPriv } = Ctx,
    case Decoded of
        Result when is_map(Result) ->
            {ApplyMicros, Applied} =
                timer:tc(fun() -> apply_result(Base, Result, Device, Opts) end),
            ?event(compute_short,
                {maps:get(timing_event, Ctx, patch_timing),
                    {decode_us, DecodeMicros},
                    {apply_us, ApplyMicros}
                },
                Opts
            ),
            hb_slot_log:note(maps:get(decode_phase, Ctx), DecodeMicros),
            hb_slot_log:note(maps:get(apply_phase, Ctx), ApplyMicros),
            case Applied of
                {ok, Patched, Delta} ->
                    {
                        hb_util:atom(Status),
                        Patched#{
                            <<"priv">> => NewPriv#{
                                <<"process-cache-delta">> => Delta
                            }
                        }
                    };
                Error -> Error
            end;
        Other ->
            {error, #{
                <<"status">> => 422,
                <<"body">> => <<Device/binary, " result must be a message.">>,
                <<"result">> => hb_util:bin(Other)
            }}
    end.

%% @doc The failed-slot result for an error the contract raised.
error_result(Error) ->
    {error, #{
        <<"status">> => 500,
        <<"body">> => Error
    }}.

%% @doc Apply ordered patches, then replace the per-slot result message.
%% A runtime cannot tell an empty array from an empty table, and `decode/2'
%% turns both into `#{}', so an empty `patches' message is the empty patch
%% list. A `results' array becomes the equivalent ordered message.
apply_result(Base, Result, Opts) ->
    apply_result(Base, Result, <<"lua-patch@1.0">>, Opts).
apply_result(Base, Result, Device, Opts) ->
    Patches =
        case field(<<"patches">>, Result, [], Opts) of
            Empty when Empty =:= #{} -> [];
            DecodedPatches -> filter_patches(DecodedPatches, Opts)
        end,
    Results =
        case field(<<"results">>, Result, #{}, Opts) of
            List when is_list(List) -> hb_ao:normalize_keys(List, Opts);
            DecodedResults -> DecodedResults
        end,
    case {is_list(Patches), is_map(Results)} of
        {true, true} ->
            case hb_state_patch:apply(Base, Patches, Results, Opts) of
                {ok, Patched} ->
                    {ok, Patched, #{
                        <<"patches">> => Patches,
                        <<"results">> => Results
                    }};
                Error -> Error
            end;
        _ ->
            {error, #{
                <<"status">> => 422,
                <<"body">> =>
                    <<Device/binary, " requires list `patches' and message `results'.">>
            }}
    end.

%% A field of a decoded result: a plain map key, which is what `message@1.0'
%% resolves it to; anything else is left to AO-Core.
field(Key, Result, Default, Opts) ->
    case maps:find(Key, Result) of
        {ok, Value} when not ?IS_LINK(Value) -> Value;
        _ -> hb_ao:get(Key, Result, Default, Opts)
    end.

%% @doc Apply the node's invalid-patch policy. By default an invalid patch
%% fails the slot with a 422. With `lua-patch-invalid-patches' set to `drop',
%% invalid patches are removed before they are applied or stored, so a
%% contract that publishes a key built from untrusted input cannot halt the
%% process. Replay reads the stored, already filtered, patch list.
filter_patches(Patches, Opts) when is_list(Patches) ->
    case hb_opts:get(<<"lua-patch-invalid-patches">>, <<"reject">>, Opts) of
        <<"drop">> ->
            case hb_state_patch:partition(Patches, Opts) of
                {Valid, []} -> Valid;
                {Valid, Dropped} ->
                    ?event(warning, {lua_patch_dropped_patches, {dropped, Dropped}}),
                    Valid
            end;
        _ -> Patches
    end;
filter_patches(Patches, _Opts) ->
    Patches.

%%% Tests

assign_to_target_test() ->
    Target = <<"TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT">>,
    ?assertEqual(Target, maps:get(<<"target">>, assign_to_target(
        #{ <<"assign-to">> => Target }
    ))),
    ?assertEqual(
        <<"native">>,
        maps:get(<<"target">>, assign_to_target(
            #{ <<"assign-to">> => Target, <<"target">> => <<"native">> }
        ))
    ),
    Commitment = assign_to_commitment(
        #{ <<"committed">> => [<<"assign-to">>, <<"action">>] }
    ),
    ?assert(lists:member(<<"target">>, maps:get(<<"committed">>, Commitment))).

%% @doc Both encode shapes carry the same content.
encode_shapes_test() ->
    Msg = #{ <<"a">> => <<"1">>, <<"l">> => [<<"x">>, <<"y">>], <<"k">> => some_atom },
    ?assertEqual(
        #{ <<"a">> => <<"1">>, <<"l">> => [<<"x">>, <<"y">>], <<"k">> => <<"some_atom">> },
        encode(Msg, term, #{})
    ),
    ?assertEqual(
        lists:sort([{<<"a">>, <<"1">>}, {<<"l">>, [<<"x">>, <<"y">>]}, {<<"k">>, <<"some_atom">>}]),
        lists:sort(encode(Msg, luerl, #{}))
    ).

%%% @doc Deterministic state-patch support shared by `lua@5.3b' and the
%%% process cache. A patch list is an ordered list of messages shaped as
%%% `#{ <<"path">> => Path, <<"value">> => Value }' or
%%% `#{ <<"path">> => Path, <<"delete">> => true }'.
-module(hb_process_delta).
-export([apply/4, apply/5, validate/2]).

-define(RESERVED_PATCH_KEYS, [
    <<"at-slot">>,
    <<"commitments">>,
    <<"device">>,
    <<"initialized">>,
    <<"priv">>,
    <<"process">>,
    <<"results">>,
    <<"snapshot">>
]).

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
validate_patches([Patch | Rest], Opts) when is_map(Patch) ->
    Path = hb_ao:get(<<"path">>, Patch, not_found, Opts),
    case {patch_path(Path), patch_operation(Patch, Opts)} of
        {{ok, _}, {ok, _}} -> validate_patches(Rest, Opts);
        {Error = {error, _}, _} -> Error;
        {_, Error = {error, _}} -> Error
    end;
validate_patches(_Patches, _Opts) ->
    patch_error(<<"Every patch must be a message.">>).

apply_patches(Base, [], _Opts) ->
    {ok, Base};
apply_patches(Base, [Patch | Rest], Opts) when is_map(Patch) ->
    Path = hb_ao:get(<<"path">>, Patch, not_found, Opts),
    case {patch_path(Path), patch_operation(Patch, Opts)} of
        {{ok, NormalizedPath}, {ok, delete}} ->
            apply_patches(
                hb_ao:set(Base, NormalizedPath, unset, Opts),
                Rest,
                Opts
            );
        {{ok, NormalizedPath}, {ok, {set, Value}}} ->
            apply_patches(
                hb_ao:set(Base, NormalizedPath, Value, Opts),
                Rest,
                Opts
            );
        {Error = {error, _}, _} -> Error;
        {_, Error = {error, _}} -> Error
    end;
apply_patches(_Base, _Patches, _Opts) ->
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

patch_path(Path) when is_binary(Path); is_list(Path) ->
    case hb_path:term_to_path_parts(Path) of
        [First | _] ->
            Normalized = hb_ao:normalize_key(First),
            case lists:member(Normalized, ?RESERVED_PATCH_KEYS) of
                true -> patch_error(<<"Patch targets a runtime-owned key.">>);
                false -> {ok, Path}
            end;
        _ -> patch_error(<<"Patch path cannot target the message root.">>)
    end;
patch_path(_) ->
    patch_error(<<"Patch `path' must be a path string.">>).

patch_error(Body) ->
    {error, #{ <<"status">> => 422, <<"body">> => Body }}.


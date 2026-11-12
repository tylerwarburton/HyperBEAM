%%% @doc Pseudo-path cache for node location records.
%%% Writes location records to ~location@1.0/COMMITTER -> LocationRecord.
-module(dev_location_cache).
-export([write/2, read/2, read/3, read_any/2, read_any/3, accepts/3, list/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% The pseudo-path prefix which the location cache should use.
-define(LOCATION_CACHE_PREFIX, <<"~location@1.0">>).
-define(LOCATION_RECEIVED_PREFIX, <<"~location-cache@1.0">>).

%% @doc Merge the location store with the main store. Used before writing
%% to the cache.
opts(Opts) ->
    Opts#{
        <<"store">> =>
            hb_opts:get(
                scheduler_store,
                hb_opts:get(store, no_viable_store, Opts),
                Opts
            )
    }.

%% @doc Read the latest known scheduler location for a committer, given in any
%% form that `hb_committer:uri/2' reads. Its path links to the record's signed
%% ID, so the record is read with its commitments.
read_any(Address, RawOpts) ->
    Opts = opts(RawOpts),
    Raw =
        hb_cache:read(
            hb_path:to_binary([
                ?LOCATION_CACHE_PREFIX,
                hb_committer:uri(Address, Opts)
            ]),
            Opts
        ),
    Res = authenticated_record(Address, Raw, Opts),
    Event =
        case Res of
            {ok, _} -> found_in_store;
            {error, not_found} -> not_found_in_store;
            _ -> local_lookup_unexpected_result
        end,
    ?event(scheduler_location, {Event, {address, Address}, {res, Res}}),
    Res.

%% @doc An owner can advertise different scheduler protocols. Keep each
%% protocol's signed record and nonce floor even when another is the default.
read_any(Address, undefined, Opts) -> read_any(Address, Opts);
read_any(Address, Variant, RawOpts) ->
    Opts = opts(RawOpts),
    case hb_cache:read(received_path(Address, Variant, Opts), Opts) of
        {ok, #{<<"record-id">> := ID}} -> authenticated_record(Address, hb_cache:read(ID, Opts), Opts);
        _ ->
            case read_any(Address, RawOpts) of
                {ok, Location} ->
                    case variant(Location, Opts) =:= Variant of
                        true -> {ok, Location};
                        false -> {error, not_found}
                    end;
                Other -> Other
            end
    end.

%% @doc Only a signed routing record can establish a nonce floor. This also
%% prevents an invalid record cached by an older reader from pinning a nonce.
authenticated_record(Address, {ok, Record}, Opts) ->
    try
        {ok, Signed} = hb_message:with_only_committed(Record, Opts),
        true = lists:member(hb_committer:uri(Address, Opts), hb_message:signers(Signed, Opts)),
        true = hb_message:verify(Signed, all, Opts),
        _ = nonce(Signed, Opts),
        {Type, URL, TTL, Variant} = binding(Signed, Opts),
        true = lists:member(Type, [<<"location">>, <<"scheduler-location">>]),
        true = is_binary(URL) andalso is_binary(Variant) andalso TTL >= 0,
        {ok, Signed}
    catch _:_ -> {error, not_found}
    end;
authenticated_record(_Address, Other, _Opts) -> Other.

%% @doc TTL is a cache lifetime measured from receipt, not an expiration date
%% for the signed record. An expired record remains available to nonce checks.
read(Address, RawOpts) -> read(Address, undefined, RawOpts).
read(Address, Variant, RawOpts) ->
    Opts = opts(RawOpts),
    case read_any(Address, Variant, RawOpts) of
        {ok, Location} ->
            case hb_cache:read(received_path(Address, variant(Location, Opts), Opts), Opts) of
                {ok, Receipt} ->
                    case fresh(Location, Receipt, os:system_time(millisecond), Opts) of
                        true -> {ok, Location};
                        false -> {error, not_found}
                    end;
                _ -> {error, not_found}
            end;
        Other -> Other
    end.

%% @doc Receipt metadata is local and bound to the exact signed record. A
%% legacy entry without metadata, clock rollback or partial write is stale.
fresh(Location, Receipt, Now, Opts) ->
    try
        ID = hb_message:id(Location, signed, Opts),
        ID = hb_maps:get(<<"record-id">>, Receipt, not_found, Opts),
        Received = hb_maps:get(<<"received-at">>, Receipt, not_found, Opts),
        true = is_integer(Received),
        Lifetime = case hb_maps:get(<<"time-to-live">>, Location, not_found, Opts) of
            Value when is_integer(Value) -> Value;
            Value when is_binary(Value) -> binary_to_integer(Value)
        end,
        Lifetime > 0 andalso Now >= Received andalso Now - Received < Lifetime
    catch _:_ -> false
    end.

%% @doc Keep local receipt metadata outside the public list of location keys.
received_path(Address, Variant, Opts) ->
    Protocol = hb_util:human_id(crypto:hash(sha256, Variant)),
    hb_path:to_binary([?LOCATION_RECEIVED_PREFIX, Protocol, hb_committer:uri(Address, Opts)]).

%% @doc The protocol is signed data, never a filesystem path component.
variant(Location, Opts) -> hb_maps:get(<<"variant">>, Location, <<"ao.N.1">>, Opts).

%% @doc Persist the receipt before changing the location alias. Readers reject
%% mismatched IDs if the node stops between the two writes.
write_received(Signer, ID, Now, Variant, Opts) ->
    {ok, ReceiptID} = hb_cache:write(#{<<"record-id">> => ID, <<"received-at">> => Now}, Opts),
    hb_cache:link(ReceiptID, received_path(Signer, Variant, Opts), Opts).

%% @doc Write the latest known scheduler location for an address.
write(LocationMsg, RawOpts) ->
    hb_name:atomic({?MODULE, writes}, fun() -> write_locked(LocationMsg, RawOpts) end).

%% @doc Compare and replace under the same lock so a delayed gateway response
%% cannot overwrite a newer peer notification after passing an earlier check.
write_locked(LocationMsg, RawOpts) ->
    Opts = opts(RawOpts),
    Signers = hb_message:signers(LocationMsg, Opts),
    case Signers =/= [] andalso lists:all(fun(Signer) -> accepts(Signer, LocationMsg, RawOpts) end, Signers) of
        true -> store_record(LocationMsg, Signers, Opts);
        false -> {error, <<"Location record would roll back or conflict with the cached nonce.">>}
    end.

%% @doc Preserve the nonce floor even after expiry. Equivalent routing fields
%% at the same nonce may refresh the local TTL without changing authority.
accepts(Address, Location, RawOpts) ->
    Opts = opts(RawOpts),
    try
        New = nonce(Location, Opts),
        case read_any(Address, variant(Location, Opts), RawOpts) of
            {ok, Previous} ->
                Old = nonce(Previous, Opts),
                New > Old orelse (New =:= Old andalso binding(Location, Opts) =:= binding(Previous, Opts));
            {error, not_found} -> true;
            _ -> false
        end
    catch _:_ -> false
    end.

%% @doc The numeric nonce, independent of the record's transport codec.
nonce(Location, Opts) ->
    Value = hb_maps:get(<<"nonce">>, Location, not_found, Opts),
    Nonce = case Value of
        N when is_integer(N) -> N;
        N when is_binary(N) -> binary_to_integer(N)
    end,
    true = Nonce >= 0,
    Nonce.

%% @doc Compare routing fields while allowing integer/string codec round-trips.
binding(Location, Opts) ->
    TTL = hb_maps:get(<<"time-to-live">>, Location, not_found, Opts),
    Lifetime = case TTL of
        N when is_integer(N) -> N;
        N when is_binary(N) -> binary_to_integer(N)
    end,
    {hb_util:to_lower(hb_maps:get(<<"type">>, Location, not_found, Opts)),
        hb_maps:get(<<"url">>, Location, not_found, Opts), Lifetime,
        hb_maps:get(<<"variant">>, Location, <<"ao.N.1">>, Opts)}.

%% @doc Store the signed record and its separate local lifetime metadata.
store_record(LocationMsg, Signers, Opts) ->
    ?event(
        scheduler_location,
        {caching_locally,
            {signers, Signers},
            {location_msg, LocationMsg}
        }
    ),
    case hb_cache:write(LocationMsg, Opts) of
        {ok, _UnsignedID} ->
            ID = hb_message:id(LocationMsg, signed, Opts),
            Now = os:system_time(millisecond),
            lists:foreach(
                fun(Signer) ->
                    ok = write_received(Signer, ID, Now, variant(LocationMsg, Opts), Opts),
                    case read_any(Signer, Opts) of
                        {ok, Default} ->
                            NewNonce = nonce(LocationMsg, Opts),
                            OldNonce = nonce(Default, Opts),
                            case NewNonce > OldNonce orelse (NewNonce =:= OldNonce
                                    andalso variant(LocationMsg, Opts) =:= variant(Default, Opts)) of
                                true -> link_default(Signer, ID, Opts);
                                false -> ok
                            end;
                        {error, not_found} -> link_default(Signer, ID, Opts)
                    end
                end,
                Signers
            ),
            ok;
        false ->
            % The message is not valid, so we don't cache it.
            {error, <<"Invalid scheduler location message. Not caching.">>};
        {error, Reason} ->
            ?event(warning, {failed_to_cache_location_msg, {reason, Reason}}),
            {error, Reason}
    end.

%% @doc The unspecified-protocol lookup retains the highest known nonce.
link_default(Signer, ID, Opts) ->
    hb_cache:link(ID, hb_path:to_binary([?LOCATION_CACHE_PREFIX, Signer]), Opts).

%% @doc Return a list of all known location records.
list(RawOpts) ->
    Opts = opts(RawOpts),
    Store = hb_opts:get(store, no_viable_store, Opts),
    hb_store:list(Store, hb_path:to_binary([?LOCATION_CACHE_PREFIX]), Opts).

%% @doc Expiry and restart retain both the signature and the nonce floor.
cache_lifetime_test() ->
    Opts = #{<<"store">> => [hb_test_utils:test_store()], <<"priv-wallet">> => ar_wallet:new()},
    Address = hb_util:human_id(ar_wallet:to_address(maps:get(<<"priv-wallet">>, Opts))),
    Location = hb_message:commit(#{<<"type">> => <<"Scheduler-Location">>,
        <<"url">> => <<"https://node.example">>, <<"nonce">> => 7,
        <<"time-to-live">> => 600000}, Opts),
    ID = hb_message:id(Location, signed, Opts),
    ok = write(Location, Opts),
    {ok, Cached} = read(Address, Opts),
    ?assert(hb_message:verify(Cached, all, Opts)),
    ?assertEqual(ID, hb_message:id(Cached, signed, Opts)),
    Receipt = #{<<"record-id">> => ID, <<"received-at">> => 1000},
    ?assert(fresh(Cached, Receipt, 600999, Opts)),
    ?assertNot(fresh(Cached, Receipt, 601000, Opts)),
    ?assertNot(fresh(Cached, Receipt, 999, Opts)),
    ok = write_received(Address, ID, os:system_time(millisecond) - 600001, variant(Location, Opts), opts(Opts)),
    ?assertEqual({error, not_found}, read(Address, Opts)),
    {ok, Expired} = read_any(Address, Opts),
    ?assertEqual(7, hb_maps:get(<<"nonce">>, Expired, Opts)),
    ?assert(hb_message:verify(Expired, all, Opts)),
    % A new reader uses the persisted receipt, not a new in-memory first-seen time.
    ReaderOpts = #{<<"store">> => maps:get(<<"store">>, Opts)},
    ?assertEqual({error, not_found}, read(Address, ReaderOpts)),
    ok = write(Location, Opts),
    ?assertMatch({ok, _}, read(Address, ReaderOpts)),
    ?assertNot(fresh(Cached, Receipt#{<<"record-id">> := <<"wrong">>}, 1001, Opts)).

%% @doc Recheck the nonce at the write boundary, including concurrent updates.
cache_nonce_race_test() ->
    Opts = #{<<"store">> => [hb_test_utils:test_store()], <<"priv-wallet">> => ar_wallet:new()},
    Address = hb_util:human_id(ar_wallet:to_address(maps:get(<<"priv-wallet">>, Opts))),
    Fields = #{<<"type">> => <<"Scheduler-Location">>, <<"url">> => <<"https://node.example">>,
        <<"time-to-live">> => 600000},
    Record = fun(N) -> hb_message:commit(Fields#{<<"nonce">> => N}, Opts) end,
    Old = Record(7),
    ok = write(Old, Opts),
    Delayed = Record(8),
    ?assert(accepts(Address, Delayed, Opts)),
    ok = write(Record(9), Opts),
    ?assertMatch({error, _}, write(Delayed, Opts)),
    ?assertMatch({error, _}, write(hb_message:commit(Fields#{<<"nonce">> => 9,
        <<"url">> := <<"https://conflicting.example">>}, Opts), Opts)),
    Parent = self(), Ref = make_ref(),
    Records = [Record(N) || N <- lists:seq(10, 25)],
    [spawn(fun() -> Parent ! {Ref, write(Msg, Opts)} end) || Msg <- Records],
    [receive {Ref, _Result} -> ok after 10000 -> error(location_write_timeout) end || _ <- Records],
    {ok, Latest} = read(Address, Opts),
    ?assertEqual(25, nonce(Latest, Opts)),
    ?assert(hb_message:verify(Latest, all, Opts)).

%% @doc Different scheduler protocols keep independent TTLs and nonce floors.
cache_variant_test() ->
    Opts = #{<<"store">> => [hb_test_utils:test_store()], <<"priv-wallet">> => ar_wallet:new()},
    Address = hb_util:human_id(ar_wallet:to_address(maps:get(<<"priv-wallet">>, Opts))),
    Fields = #{<<"type">> => <<"Scheduler-Location">>, <<"time-to-live">> => 600000},
    Modern = hb_message:commit(Fields#{<<"url">> => <<"https://modern.example">>,
        <<"variant">> => <<"ao.N.1">>, <<"nonce">> => 9}, Opts),
    Legacy = hb_message:commit(Fields#{<<"url">> => <<"https://legacy.example">>,
        <<"variant">> => <<"ao.TN.1">>, <<"nonce">> => 7}, Opts),
    ok = write(Modern, Opts),
    ok = write(Legacy, Opts),
    {ok, Default} = read(Address, Opts),
    ?assertEqual(9, nonce(Default, Opts)),
    {ok, OldProtocol} = read(Address, <<"ao.TN.1">>, Opts),
    ?assertEqual(7, nonce(OldProtocol, Opts)),
    ?assertEqual(<<"https://legacy.example">>, hb_maps:get(<<"url">>, OldProtocol, Opts)),
    ok = write_received(Address, hb_message:id(Modern, signed, Opts),
        os:system_time(millisecond) - 600001, <<"ao.N.1">>, opts(Opts)),
    ?assertEqual({error, not_found}, read(Address, <<"ao.N.1">>, Opts)),
    ?assertMatch({ok, _}, read(Address, <<"ao.TN.1">>, Opts)),
    ?assertEqual({error, not_found}, read(Address, <<"unknown">>, Opts)).

%% @doc An invalid signature cannot pin a forged high nonce in a legacy cache.
cache_invalid_floor_test() ->
    Opts = #{<<"store">> => [hb_test_utils:test_store()], <<"priv-wallet">> => ar_wallet:new()},
    Address = hb_util:human_id(ar_wallet:to_address(maps:get(<<"priv-wallet">>, Opts))),
    Location = hb_message:commit(#{<<"type">> => <<"Scheduler-Location">>,
        <<"url">> => <<"https://node.example">>, <<"nonce">> => 7,
        <<"time-to-live">> => 600000}, Opts),
    Forged = Location#{<<"nonce">> := 9999},
    ?assertEqual({error, not_found}, authenticated_record(Address, {ok, Forged}, Opts)),
    {ok, _} = hb_cache:write(Forged, Opts),
    ok = link_default(hb_committer:uri(Address, Opts), hb_message:id(Forged, signed, Opts), Opts),
    ?assertEqual({error, not_found}, read_any(Address, Opts)),
    ok = write(Location, Opts),
    {ok, Recovered} = read(Address, Opts),
    ?assertEqual(7, nonce(Recovered, Opts)).

%%% @doc The signed bytes of ANS-104 items this node has taken in, kept for the
%%% schedule uploader (`dev_scheduler_uploader') so that it posts the item a
%%% client sent, and the assignment its scheduler signed, instead of
%%% rebuilding each from the store.
%%%
%%% An entry is keyed by the item's signed ID and its owner, both read from
%%% the bytes themselves, and the bytes go in only after their own signature
%%% verifies: an item is only ever found under the ID its bytes carry, and a
%%% second item with the same signature under another key (RSA permits one)
%%% cannot take the place of the first.
%%%
%%% The table is node-wide and bounded by entries and by bytes: an item that
%%% would take it past either is not kept, and the uploader then builds it
%%% from the store as before. An entry goes when the window carrying it is
%%% built, or after `?MAX_AGE_MS'. A miss (a restart, an `httpsig@1.0'
%%% push, a full table) costs only the rebuild.
-module(hb_item_bytes).
-export([enabled/1, put_verified/1, put_signed/1, lookup/2, forget/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TABLE, ?MODULE).
-define(MAX_ENTRIES, 100000).
-define(MAX_BYTES, 128 * 1024 * 1024).
-define(MAX_AGE_MS, 600000).
-define(SWEEP_MS, 1000).

%% @doc Whether this node keeps item bytes: it publishes schedules, and lists
%% processes to archive whole.
enabled(Opts) ->
    hb_opts:get(scheduler_publish_remote, false, Opts) =:= true andalso
        not lists:member(hb_opts:get(scheduler_upload_processes, [], Opts),
            [[], <<>>, #{}]).

%% @doc Keep the bytes of an item a client sent, if their signature verifies.
put_verified(Bytes) when is_binary(Bytes) ->
    try ar_bundles:deserialize_item_wrapper(Bytes) of
        TX = #tx{ signature = Sig } when Sig =/= ?DEFAULT_SIG ->
            case ar_bundles:verify_item(TX) of
                true -> insert({TX#tx.id, TX#tx.owner}, Bytes);
                false -> unverified
            end;
        _ -> unsigned
    catch _:_ -> undecodable
    end.

%% @doc Keep the bytes of an item this node signed.
put_signed(TX = #tx{ signature = Sig }) when Sig =/= ?DEFAULT_SIG ->
    insert({TX#tx.id, TX#tx.owner}, ar_bundles:serialize(TX));
put_signed(_) -> unsigned.

%% @doc The item that carries `Msg''s ANS-104 signature, as its bytes give
%% it, or `not_found'.
lookup(Msg, Opts) ->
    case ets:whereis(?TABLE) of
        undefined -> not_found;
        _ ->
            try key(Msg, Opts) of
                {ok, Key} ->
                    case ets:lookup(?TABLE, Key) of
                        [{Key, Bytes, _}] ->
                            {ok, ar_bundles:deserialize_item_wrapper(Bytes)};
                        [] -> not_found
                    end;
                not_found -> not_found
            catch _:_ -> not_found
            end
    end.

%% @doc Drop the entries of these items.
forget(Items) ->
    case ets:whereis(?TABLE) of
        undefined -> ok;
        _ ->
            lists:foreach(
                fun(#tx{ id = ID, owner = Owner }) -> delete({ID, Owner});
                   (_) -> ok
                end,
                Items
            )
    end.

%% The key of a message's one signed ANS-104 commitment.
key(Msg, Opts) ->
    Commitments = hb_maps:get(<<"commitments">>, Msg, #{}, Opts),
    Signed =
        [ {hb_util:native_id(ID), hb_util:decode(Owner)}
        || {ID, C} <- hb_maps:to_list(Commitments, Opts),
            is_map(C),
            hb_maps:get(<<"commitment-device">>, C, undefined, Opts)
                =:= <<"ans104@1.0">>,
            hb_maps:is_key(<<"signature">>, C, Opts),
            Owner <- [hb_maps:get(<<"owner">>, C, undefined, Opts)],
            is_binary(Owner)
        ],
    case Signed of
        [Key] -> {ok, Key};
        _ -> not_found
    end.

%% Reserve the room, then take the key; give the room back if the key was
%% there already.
insert(Key, Bytes) ->
    ok = ensure_table(),
    Size = byte_size(Bytes),
    Count = ets:update_counter(?TABLE, '$entries', 1),
    Total = ets:update_counter(?TABLE, '$bytes', Size),
    Release = fun() ->
        ets:update_counter(?TABLE, '$entries', -1),
        ets:update_counter(?TABLE, '$bytes', -Size)
    end,
    case Count > ?MAX_ENTRIES orelse Total > ?MAX_BYTES of
        true -> Release(), full;
        false ->
            case ets:insert_new(?TABLE, {Key, Bytes, now_ms()}) of
                true -> ok;
                false -> Release(), ok
            end
    end.

delete(Key) ->
    case ets:take(?TABLE, Key) of
        [{Key, Bytes, _}] ->
            ets:update_counter(?TABLE, '$entries', -1),
            ets:update_counter(?TABLE, '$bytes', -byte_size(Bytes)),
            ok;
        [] -> ok
    end.

now_ms() -> erlang:monotonic_time(millisecond).

%% The table is owned by a process that never exits, which also drops the
%% entries older than `?MAX_AGE_MS'.
ensure_table() ->
    case ets:whereis(?TABLE) of
        undefined ->
            Parent = self(),
            Ref = make_ref(),
            {Owner, Mon} =
                spawn_monitor(
                    fun() ->
                        try ets:new(?TABLE, [named_table, public, set,
                                {read_concurrency, true},
                                {write_concurrency, true}]) of
                            _ ->
                                ets:insert(?TABLE,
                                    [{'$entries', 0}, {'$bytes', 0}]),
                                Parent ! {Ref, created},
                                sweep()
                        catch error:badarg -> Parent ! {Ref, exists}
                        end
                    end
                ),
            receive
                {Ref, _} -> ok;
                {'DOWN', Mon, process, Owner, _} -> ok
            after 5000 -> ok
            end,
            erlang:demonitor(Mon, [flush]),
            ok;
        _ -> ok
    end.

sweep() ->
    receive after ?SWEEP_MS -> ok end,
    Cutoff = now_ms() - ?MAX_AGE_MS,
    Old = ets:select(?TABLE, [{{'$1', '_', '$2'}, [{'<', '$2', Cutoff}], ['$1']}]),
    lists:foreach(fun delete/1, Old),
    sweep().

%%% Tests

signed_item(Data) ->
    ar_bundles:sign_item(
        #tx{ format = ans104, data = Data, data_size = byte_size(Data),
            tags = [{<<"Action">>, <<"Probe">>}] },
        hb:wallet()
    ).

%% An item is found by the message decoded from it, as the bytes it came in.
round_trip_test() ->
    Opts = #{},
    TX = signed_item(crypto:strong_rand_bytes(16)),
    Bytes = ar_bundles:serialize(TX),
    ok = put_verified(Bytes),
    Msg = hb_http:decode(<<"ans104@1.0">>, Bytes, Opts),
    {ok, Found} = lookup(Msg, Opts),
    ?assertEqual(Bytes, ar_bundles:serialize(Found)),
    ok = forget([Found]),
    ?assertEqual(not_found, lookup(Msg, Opts)).

%% Bytes whose signature does not verify are not kept, nor unsigned ones.
refuses_unverified_test() ->
    TX = signed_item(<<"real">>),
    Bytes = ar_bundles:serialize(TX),
    Tampered = <<(binary:part(Bytes, 0, byte_size(Bytes) - 4))/binary, "fake">>,
    ?assertEqual(unverified, put_verified(Tampered)),
    ?assertEqual(unsigned, put_signed(TX#tx{ signature = ?DEFAULT_SIG })),
    ?assertEqual(undecodable, put_verified(<<"junk">>)).

%% A full table keeps nothing more, and gives the room it reserved back.
bounded_test() ->
    ok = ensure_table(),
    [{_, Bytes0}] = ets:lookup(?TABLE, '$bytes'),
    ets:update_counter(?TABLE, '$bytes', ?MAX_BYTES),
    try
        ?assertEqual(full, put_verified(ar_bundles:serialize(signed_item(<<"x">>))))
    after ets:update_counter(?TABLE, '$bytes', -?MAX_BYTES)
    end,
    ?assertEqual([{'$bytes', Bytes0}], ets:lookup(?TABLE, '$bytes')).

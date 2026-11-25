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
-export([archived/2, put_verified/3, put_signed/1, lookup/2, forget/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TABLE, ?MODULE).
-define(MAX_ENTRIES, 100000).
-define(MAX_BYTES, 128 * 1024 * 1024).
-define(MAX_AGE_MS, 600000).
-define(SWEEP_MS, 1000).

%% @doc Whether this node archives `ProcID''s schedule whole: it publishes
%% schedules (`scheduler-publish-remote', on unless set false) and lists the
%% process on `scheduler-upload-processes'.
archived(ProcID, Opts) ->
    hb_opts:get(scheduler_publish_remote, true, Opts) =/= false andalso
        lists:member(hb_util:human_id(ProcID),
            hb_util:list_option(
                hb_opts:get(scheduler_upload_processes, [], Opts), Opts)).

%% @doc Keep the bytes of an item a client sent to `Path', if this node
%% archives the process it is for and the bytes' signature verifies. The
%% process is the path's leading ID (`/<pid>~process@1.0/schedule'), or else
%% the item's `target' (`/~scheduler@1.0/schedule').
put_verified(Bytes, Path, Opts) when is_binary(Bytes) ->
    case hb_opts:get(scheduler_upload_processes, [], Opts) of
        Empty when Empty =:= []; Empty =:= <<>> -> not_archived;
        _ ->
            case path_process(Path) of
                undefined -> keep(Bytes, unknown, Opts);
                ProcID -> keep(Bytes, archived(ProcID, Opts), Opts)
            end
    end.

keep(_Bytes, false, _Opts) -> not_archived;
keep(Bytes, Archived, Opts) ->
    try ar_bundles:deserialize_item_wrapper(Bytes) of
        TX = #tx{ signature = Sig } when Sig =/= ?DEFAULT_SIG ->
            case Archived =:= true orelse target_archived(TX, Opts) of
                false -> not_archived;
                true ->
                    case ar_bundles:verify_item(TX) of
                        true -> insert({TX#tx.id, TX#tx.owner}, Bytes);
                        false -> unverified
                    end
            end;
        _ -> unsigned
    catch _:_ -> undecodable
    end.

path_process(<<"/", ID:43/binary, _/binary>>) -> ID;
path_process(_) -> undefined.

target_archived(#tx{ target = Target }, Opts) when byte_size(Target) == 32 ->
    archived(Target, Opts);
target_archived(_, _Opts) -> false.

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

-define(PROC, <<"pRoCeSsPrOcEsSpRoCeSsPrOcEsSpRoCeSsPrOcEsS0">>).
-define(PATH, <<"/", ?PROC/binary, "~process@1.0/schedule">>).

opts() -> #{ <<"scheduler-upload-processes">> => [?PROC] }.

signed_item(Data) -> signed_item(Data, <<>>).
signed_item(Data, Target) ->
    ar_bundles:sign_item(
        #tx{ format = ans104, data = Data, data_size = byte_size(Data),
            target = Target, tags = [{<<"Action">>, <<"Probe">>}] },
        hb:wallet()
    ).

%% An item is found by the message decoded from it, as the bytes it came in.
round_trip_test() ->
    Opts = opts(),
    TX = signed_item(crypto:strong_rand_bytes(16)),
    Bytes = ar_bundles:serialize(TX),
    ok = put_verified(Bytes, ?PATH, Opts),
    Msg = hb_http:decode(<<"ans104@1.0">>, Bytes, Opts),
    {ok, Found} = lookup(Msg, Opts),
    ?assertEqual(Bytes, ar_bundles:serialize(Found)),
    ok = forget([Found]),
    ?assertEqual(not_found, lookup(Msg, Opts)).

%% Only an item for an archived process is kept: the process in the path,
%% or else the item's target.
archived_only_test() ->
    Opts = opts(),
    Other = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Item = fun(Target) -> ar_bundles:serialize(signed_item(crypto:strong_rand_bytes(8), Target)) end,
    ?assertEqual(not_archived,
        put_verified(Item(<<>>), <<"/", Other/binary, "~process@1.0/schedule">>, Opts)),
    ?assertEqual(not_archived, put_verified(Item(<<>>), ?PATH, #{})),
    ?assertEqual(not_archived,
        put_verified(Item(<<>>), <<"/~scheduler@1.0/schedule">>, Opts)),
    ?assertEqual(not_archived,
        put_verified(Item(hb_util:native_id(Other)), <<"/~scheduler@1.0/schedule">>, Opts)),
    ?assertEqual(ok,
        put_verified(Item(hb_util:native_id(?PROC)), <<"/~scheduler@1.0/schedule">>, Opts)),
    ?assertEqual(not_archived,
        put_verified(Item(<<>>), ?PATH,
            Opts#{ <<"scheduler-publish-remote">> => false })).

%% Bytes whose signature does not verify are not kept, nor unsigned ones.
refuses_unverified_test() ->
    TX = signed_item(<<"real">>),
    Bytes = ar_bundles:serialize(TX),
    Tampered = <<(binary:part(Bytes, 0, byte_size(Bytes) - 4))/binary, "fake">>,
    ?assertEqual(unverified, put_verified(Tampered, ?PATH, opts())),
    ?assertEqual(unsigned, put_signed(TX#tx{ signature = ?DEFAULT_SIG })),
    ?assertEqual(undecodable, put_verified(<<"junk">>, ?PATH, opts())).

%% A full table keeps nothing more, and gives the room it reserved back.
bounded_test() ->
    ok = ensure_table(),
    [{_, Bytes0}] = ets:lookup(?TABLE, '$bytes'),
    ets:update_counter(?TABLE, '$bytes', ?MAX_BYTES),
    try
        ?assertEqual(full,
            put_verified(ar_bundles:serialize(signed_item(<<"x">>)), ?PATH, opts()))
    after ets:update_counter(?TABLE, '$bytes', -?MAX_BYTES)
    end,
    ?assertEqual([{'$bytes', Bytes0}], ets:lookup(?TABLE, '$bytes')).

%%% @doc `push@1.0' schedules a message (or takes a slot already assigned), and
%%% answers with the slot's results and the receipts of its outbox.
%%%
%%% A push no longer delivers anything. The node that schedules a process
%%% delivers every slot's outbox itself, whoever computed the slot and whether
%%% or not anybody asked: each outbox entry is signed once, stored, flushed,
%%% and posted until its target's scheduler assigns it (`lib_process_delivery').
%%% Each target computes what it is sent and its own deliverer sends on what
%%% that slot emits, so a chain of messages runs to its end with no client
%%% holding it up -- and a client that leaves loses nothing.
%%%
%%% So a push is a read: it waits for its slot (through the process worker
%%% when the slot is not computed yet), then for that slot's delivery, then --
%%% to `max-depth' -- for the slots its messages were assigned on the targets
%%% this node holds, the same way. It never signs or schedules an outbox entry
%%% and never detaches: it dies with its HTTP client, and delivery goes on
%%% without it. `async' and `push' are accepted and ignored.
-module(dev_push).
-specification("../../../specs/push@1.0.md").
-device_libraries([lib_process, lib_process_cache, lib_process_delivery]).
%%% Public API
-export([push/3, schedule/3, pending/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc List the outbox entries this node has computed and not delivered: the
%% deliverer's own `o' rows without an `x' receipt (`lib_process_delivery').
%% An optional `process' restricts the query to one source. Reading it
%% neither computes nor delivers messages. Each entry can be sent to
%% `schedule', or its source slot can be drained with `push'.
pending(_Base, Req, Opts) ->
    Pending =
        case hb_maps:get(<<"process">>, Req, undefined, Opts) of
            undefined -> lists:sort(lib_process_delivery:pending(Opts));
            ProcessID ->
                [{ProcessID, lib_process_delivery:pending(ProcessID, Opts)}]
        end,
    Entries = [
        #{
            <<"process">> => ProcessID,
            <<"slot">> => Slot,
            <<"outbox-key">> => Key
        }
    || {ProcessID, Slots} <- Pending,
        Slot <- Slots,
        Key <-
            lists:sort(
                fun(A, B) -> pending_order(A) =< pending_order(B) end,
                lib_process_delivery:pending_keys(ProcessID, Slot, Opts)
            )
    ],
    {ok, #{
        <<"count">> => length(Entries),
        <<"entries">> => hb_util:list_to_numbered_message(Entries),
        <<"cache-control">> => <<"no-store">>
    }}.

pending_order(Key) ->
    try {0, binary_to_integer(Key)} catch error:badarg -> {1, Key} end.

%% @doc Drain pending slots through the same durable delivery reader.
read_all(Process, Req, Opts) ->
    ID = lib_process:process_id(Process, #{}, Opts),
    {ok, Pending} = pending(Process, #{<<"process">> => ID}, Opts),
    Entries = hb_maps:values(hb_maps:get(<<"entries">>, Pending, Opts), Opts),
    Slots = lists:usort([hb_maps:get(<<"slot">>, E, Opts) || E <- Entries]),
    Results = maps:from_list([{hb_util:bin(Slot),
        case read(Process, Slot, depths(Req, Opts), Opts) of
            {ok, Result} -> Result;
            {error, Error} -> #{<<"response">> => <<"error">>, <<"reason">> => Error}
        end} || Slot <- Slots]),
    {ok, #{<<"process">> => ID, <<"count">> => length(Slots),
        <<"slots">> => Results, <<"cache-control">> => <<"no-store">>}}.

%% The table of recently missing targets (`lib_process_delivery'), named here
%% for the tests that read it.
-define(MISSING_TARGETS, dev_push_missing_targets).
-define(MISSING_TARGETS_MAX, 100000).

%% @doc Push either a message or an assigned slot number. If a message is
%% provided in the `body' of the request, it is scheduled (a process is
%% initialized). Otherwise the slot named by the `slot' key is read.
%%
%% The answer is the slot's results with, under each outbox key, the receipt
%% of that entry: the slot it was assigned on its target, its ID and its
%% target -- or `status: terminal' for an entry that could never be delivered
%% (its target does not exist). It is returned once every entry of the slot
%% is delivered; a slot with no outbox is returned at once.
%%
%% `max-depth' (default and ceiling: the node's `push_max_depth', 30) is how
%% many hops downstream the push follows: each receipt of a target this node
%% holds gets `resulted-in', the push of the slot its message was assigned,
%% with `max-depth' one less. So a push returns once its chain has run that
%% far -- computed by the targets' workers, delivered by their deliverers.
%% `result-depth' (default 1) is how many levels carry the slots' results.
-spec push(
    #{ _ => _ },
    #{ slot => integer(), body => #{ _ => _ }, _ => _ },
    #{ _ => _ }
) -> {ok, #{ _ => _ }} | {error, _}.
push(Base, Req, Opts) ->
    Process = lib_process:as_process(Base, Opts),
    ?event(push, {push_base, {base, Process}, {req, Req}}, Opts),
    case hb_ao:get(<<"slot">>, {as, <<"message@1.0">>, Req}, no_slot, Opts) of
        no_slot ->
            case schedule_initial_message(Process, Req, Opts) of
                {ok, Assignment} ->
                    case find_type(hb_ao:get(<<"body">>, Assignment, Opts), Opts) of
                        <<"Process">> -> {ok, Assignment};
                        _ ->
                            read(Process, hb_ao:get(<<"slot">>, Assignment, Opts),
                                depths(Req, Opts), Opts)
                    end;
                {error, Res} -> {error, Res}
            end;
        <<"all">> -> read_all(Process, Req, Opts);
        Slot -> read(Process, Slot, depths(Req, Opts), Opts)
    end.

%% @doc How far a push follows its chain, and how far it includes results.
depths(Req, Opts) ->
    Get = fun(Key, Default) -> hb_ao:get(Key, {as, <<"message@1.0">>, Req}, Default, Opts) end,
    {
        min(parse_depth(Get(<<"max-depth">>, infinity)),
            parse_depth(hb_opts:get(push_max_depth, 30, Opts))),
        hb_util:int(Get(<<"result-depth">>, 1))
    }.

parse_depth(N) when is_integer(N), N >= 0 -> N;
parse_depth(Bin) when is_binary(Bin) ->
    try hb_util:int(Bin) of
        N when N >= 0 -> N;
        _ -> infinity
    catch _:_ -> infinity
    end;
parse_depth(_) -> infinity.

%% @doc The results of a slot, and the receipts of its outbox once it is
%% delivered, each followed downstream to `MaxDepth'.
read(Process, RawSlot, {MaxDepth, ResultDepth}, RawOpts) ->
    Opts = RawOpts#{ <<"await-inprogress">> => false },
    ID = lib_process:process_id(Process, #{}, Opts),
    Slot = hb_util:int(RawSlot),
    case slot_results(Process, ID, Slot, RawOpts) of
        {ok, RawResults} ->
            Results = hb_cache:ensure_all_loaded(RawResults, Opts),
            Response =
                maps:merge(
                    case ResultDepth > 0 of
                        true -> results_map(Results);
                        false -> #{}
                    end,
                    #{ <<"slot">> => Slot, <<"process">> => ID }
                ),
            case has_outbox(Results, Opts) of
                false -> {ok, Response};
                true ->
                    case lib_process_delivery:await_delivered(ID, Slot, Opts) of
                        {ok, Receipts} ->
                            {ok,
                                maps:merge(
                                    Response,
                                    downstream(Receipts, MaxDepth, ResultDepth, RawOpts)
                                )};
                        {error, Reason} -> {error, delivery_error(ID, Slot, Reason)}
                    end
            end;
        Error -> Error
    end.

%% @doc Each receipt of a target this node holds, with the push of the slot
%% its message was assigned there. That push waits on the target's worker and
%% deliverer like this one; it never computes or delivers for them.
downstream(Receipts, 0, _ResultDepth, _Opts) -> Receipts;
downstream(Receipts, MaxDepth, ResultDepth, Opts) ->
    maps:map(
        fun(_Key, Receipt = #{ <<"target">> := Target, <<"slot">> := Slot })
                when Slot =/= undefined ->
            case hb_cache:read(Target, hb_store:scope(Opts, local)) of
                {ok, TargetProcess} ->
                    Next = {next_depth(MaxDepth), ResultDepth - 1},
                    case catch read(TargetProcess, Slot, Next, Opts) of
                        {ok, Res} -> Receipt#{ <<"resulted-in">> => Res };
                        {error, Error} -> Receipt#{ <<"resulted-in">> => error_map(Error) };
                        Other -> Receipt#{ <<"resulted-in">> => error_map(Other) }
                    end;
                _ -> Receipt
            end;
           (_Key, Receipt) -> Receipt
        end,
        Receipts
    ).

%% @doc Why a slot with an outbox has no receipts here. Never a 200: a push
%% answered without receipts would read as a slot that sent nothing.
delivery_error(ID, _Slot, not_delivering) ->
    #{
        <<"status">> => 421,
        <<"body">> => <<"This node does not deliver the outbox of ", ID/binary, ".">>,
        <<"reason">> => <<"not_delivering">>
    };
delivery_error(ID, Slot, no_delivery_record) ->
    #{
        <<"status">> => 404,
        <<"body">> => <<"No delivery record for slot ", (hb_util:bin(Slot))/binary,
            " of ", ID/binary, ".">>,
        <<"reason">> => <<"no_delivery_record">>
    };
delivery_error(ID, _Slot, Reason) ->
    #{
        <<"status">> => 503,
        <<"body">> => <<"The outbox of ", ID/binary, " cannot be delivered here now.">>,
        <<"reason">> => hb_util:bin(io_lib:format("~0p", [Reason]))
    }.

next_depth(infinity) -> infinity;
next_depth(N) -> N - 1.

error_map(Error) when is_map(Error) -> Error#{ <<"response">> => <<"error">> };
error_map(Error) ->
    #{ <<"response">> => <<"error">>, <<"reason">> => hb_util:bin(io_lib:format("~0p", [Error])) }.

%% The slot's results, with an outbox the process returned as a list keyed as
%% its receipts are (`1', `2', ...): a client finds each entry's receipt under
%% the entry's own key.
results_map(Results = #{ <<"outbox">> := Outbox }) when is_list(Outbox) ->
    Results#{ <<"outbox">> => hb_ao:normalize_keys(Outbox) };
results_map(Results) when is_map(Results) -> Results;
results_map(_) -> #{}.

has_outbox(Results, Opts) when is_map(Results) ->
    case hb_maps:get(<<"outbox">>, Results, #{}, Opts) of
        Outbox when is_map(Outbox) -> not ?IS_EMPTY_MESSAGE(hb_private:reset(Outbox));
        Outbox when is_list(Outbox) -> Outbox =/= [];
        _ -> false
    end;
has_outbox(_, _) -> false.

%% @doc The results of a slot of a process. A stored slot's are read from the
%% store as they are (`lib_process_cache:read_results/3'). A slot that has
%% never been stored is computed, and that compute groups: the process
%% worker's grouper names it by process id, which is how it reaches the live
%% worker. A slot at or below the head whose state retention deleted is
%% refused by the compute with a 410; it is never replayed.
slot_results(Process, ProcID, Slot, Opts) ->
    case lib_process_cache:read_results(ProcID, Slot, Opts) of
        {ok, Results} -> {ok, Results};
        {error, not_found} ->
            hb_ao:resolve(
                {as, <<"process@1.0">>, Process},
                #{ <<"path">> => <<"compute/results">>, <<"slot">> => Slot },
                (worker_spawn_opts(Opts))#{ <<"hashpath">> => ignore }
            )
    end.

%% @doc The push's compute is nested, so `hb_ao' has stripped `spawn-worker'
%% from its options. With `process-worker-spawn => true' it asks for the
%% process's worker again, as `dev_process:worker_spawn_opts/1' does for
%% `now'.
worker_spawn_opts(Opts) ->
    case hb_opts:get(<<"process-worker-spawn">>, false, Opts) of
        true -> Opts#{ <<"spawn-worker">> => true };
        _ -> Opts
    end.

%% @doc The receipt of one outbox entry of a slot: `slot' and `outbox-key'.
%% It waits for the slot's delivery like `push/3' -- it never signs or posts
%% the entry itself, so asking twice can never deliver it twice.
-spec schedule(
    #{ process := binary() },
    #{ slot := integer(), 'outbox-key' := binary() },
    #{ _ => _ }
) -> {ok, #{ _ => _ }} | {error, _}.
schedule(Base, Req, Opts) ->
    ProcessID = hb_maps:get(<<"process">>, Base, [], Opts),
    Key = hb_util:bin(hb_maps:get(<<"outbox-key">>, Req, [], Opts)),
    Slot = hb_util:int(hb_maps:get(<<"slot">>, Req, [], Opts)),
    case lib_process_delivery:await_delivered(ProcessID, Slot, Opts) of
        {ok, Receipts} when is_map(Receipts), is_map_key(Key, Receipts) ->
            {ok, maps:get(Key, Receipts)};
        {error, Reason} -> {error, delivery_error(ProcessID, Slot, Reason)};
        _ ->
            {error,
                #{
                    <<"status">> => 404,
                    <<"body">> => <<"No such outbox entry.">>
                }}
    end.

%% @doc Push a message or a process, prior to reading the resulting slot.
schedule_initial_message(Base, Req, Opts) ->
    ModReq = Req#{ <<"path">> => <<"schedule">>, <<"method">> => <<"POST">> },
    ?event(push, {initial_push, {base, Base}, {req, ModReq}}, Opts),
    case hb_ao:resolve(Base, ModReq, Opts) of
        {ok, Res} ->
            case hb_ao:get(<<"status">>, Res, 200, Opts) of
                200 -> {ok, Res};
                307 ->
                    Location = hb_ao:get(<<"location">>, Res, Opts),
                    lib_process_delivery:remote_schedule_result(Location, Req, Opts)
            end;
        {error, Res = #{ <<"status">> := 422 }} ->
            ?event(push, {initial_push_wrong_format, {error, Res}}, Opts),
            {error, Res};
        {error, Res} ->
            ?event(push, {initial_push_error, {error, Res}}, Opts),
            {error, Res}
    end.

find_type(Req, Opts) -> lib_process_delivery:find_type(Req, Opts).

%%% Signing and target helpers live in `lib_process_delivery'; these names
%%% are kept for the tests below.
read_target(Target, Opts) -> lib_process_delivery:read_target(Target, Opts).
recently_missing(Target, TTL) -> lib_process_delivery:recently_missing(Target, TTL).
remember_missing(Target, TTL) -> lib_process_delivery:remember_missing(Target, TTL).
augment_message(Origin, Msg, Opts) -> lib_process_delivery:augment_message(Origin, Msg, Opts).
apply_security(Msg, Target, Codec, Opts) ->
    lib_process_delivery:apply_security(Msg, Target, Codec, Opts).
normalize_message(Msg, Opts) -> lib_process_delivery:normalize_message(Msg, Opts).

%%% Tests

%% @doc `pending' lists the targeted entries of the deliverer's own `o' rows
%% that have no `x' receipt, and storing a slot writes no other index.
pending_reads_delivery_rows_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb),
    Opts = #{
        <<"store">> => Store,
        <<"process-state-blobs">> => true,
        <<"priv-wallet">> => ar_wallet:new()
    },
    Proc = hb_message:commit(#{ <<"type">> => <<"Process">>,
        <<"scheduler-location">> => hb_opts:address(Opts),
        <<"nonce">> => hb_util:human_id(crypto:strong_rand_bytes(32)) }, Opts),
    {ok, _} = hb_cache:write(Proc, Opts),
    ProcID = hb_util:human_id(hb_message:id(Proc, all, Opts)),
    Outbox = #{
        <<"2">> => #{ <<"target">> => ProcID, <<"data">> => <<"b">> },
        <<"1">> => #{ <<"target">> => ProcID, <<"data">> => <<"a">> },
        <<"3">> => #{ <<"data">> => <<"no target">> }
    },
    [ {ok, _} = lib_process_cache:write(ProcID, Slot,
        #{ <<"at-slot">> => Slot, <<"results">> => #{ <<"outbox">> => Outbox } }, Opts)
    || Slot <- [0, 1] ],
    ok = lib_process_cache:blob_put(Store, lib_process_cache:blob_key(ProcID, <<"x">>, 1),
        lib_process_cache:blob_encode(#{}), Opts),
    Entries = fun(Req) ->
        {ok, Res} = pending(#{}, Req, Opts),
        [ {hb_maps:get(<<"slot">>, E, Opts), hb_maps:get(<<"outbox-key">>, E, Opts)}
        || E <- hb_util:message_to_ordered_list(hb_maps:get(<<"entries">>, Res, Opts), Opts),
           hb_maps:get(<<"process">>, E, Opts) =:= ProcID ]
    end,
    ?assertEqual([{0, <<"1">>}, {0, <<"2">>}], Entries(#{ <<"process">> => ProcID })),
    ?assertEqual([{0, <<"1">>}, {0, <<"2">>}], Entries(#{})),
    ?assertEqual([], hb_cache:list(<<"~push@1.0/outbox">>, Opts)).

%% @doc A target that failed to resolve is skipped until its entry expires.
missing_target_memory_test() ->
    ID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assertNot(recently_missing(ID, 600)),
    remember_missing(ID, 600),
    ?assert(recently_missing(ID, 600)),
    ?assertNot(recently_missing(ID, 0)),
    ets:insert(?MISSING_TARGETS, {ID, erlang:monotonic_time(second) - 601}),
    ?assertNot(recently_missing(ID, 600)).

%% @doc `read_target' remembers a miss, and a TTL of 0 remembers nothing.
read_target_remembers_miss_test() ->
    Store = hb_test_utils:test_store(),
    hb_store:start(Store),
    Opts = #{ <<"store">> => [Store] },
    Missing = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assertEqual({error, not_found}, read_target(Missing, Opts)),
    ?assert(recently_missing(Missing, 600)),
    Off = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assertEqual(
        {error, not_found},
        read_target(Off, Opts#{ <<"push-missing-target-ttl">> => 0 })
    ),
    ?assertNot(recently_missing(Off, 600)),
    {ok, Found} = hb_cache:write(#{ <<"greeting">> => <<"hello">> }, Opts),
    ?assertMatch({ok, _}, read_target(Found, Opts)).

%% @doc A target this node stores right after a failed lookup is found at once,
%% not after the TTL, and its entry is cleared.
read_target_finds_local_write_after_miss_test() ->
    Store = hb_test_utils:test_store(),
    hb_store:start(Store),
    Opts = #{ <<"store">> => [Store], <<"priv-wallet">> => ar_wallet:new() },
    Msg = hb_message:commit(#{ <<"spawned">> => <<"late">> }, Opts),
    ID = hb_message:id(Msg, signed, Opts),
    ?assertEqual({error, not_found}, read_target(ID, Opts)),
    ?assert(recently_missing(ID, 60)),
    {ok, _} = hb_cache:write(Msg, Opts),
    ?assertMatch({ok, _}, read_target(ID, Opts)),
    ?assertNot(recently_missing(ID, 60)).

%% @doc A gateway that answered with no such item is a miss and is remembered;
%% a gateway that could not be reached is not, so it is asked again next time,
%% and it is never reported as a miss: the deliverer retries it, where a miss
%% is a terminal.
read_target_remembers_only_real_miss_test() ->
    Local = hb_test_utils:test_store(),
    hb_store:start(Local),
    Up = hb_util:bin(string:trim(hb_util:list(hb_http_server:start_node(#{})), trailing, "/")),
    Gateway =
        fun(Node) ->
            #{ <<"store">> => [Local,
                #{ <<"store-module">> => hb_store_gateway, <<"node">> => Node,
                    <<"node-type">> => <<"ao">> }] }
        end,
    Miss = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assertEqual({error, not_found}, read_target(Miss, Gateway(Up))),
    ?assert(recently_missing(Miss, 60)),
    Down = hb_util:human_id(crypto:strong_rand_bytes(32)),
    DownRes = read_target(Down, Gateway(<<"http://127.0.0.1:1">>)),
    ?assertNotMatch({ok, _}, DownRes),
    ?assertNotEqual({error, not_found}, DownRes),
    ?assertNot(recently_missing(Down, 60)),
    % An error status (the node has no `/graphql' of its own) is not a miss.
    Err = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ErrOpts =
        #{ <<"store">> => [Local,
            #{ <<"store-module">> => hb_store_gateway, <<"node">> => Up }] },
    ?assertNotMatch({ok, _}, read_target(Err, ErrOpts)),
    ?assertNot(recently_missing(Err, 60)).

%% @doc Only IDs are remembered: a target can be any binary.
non_id_target_not_remembered_test() ->
    Path = <<"not/an/id">>,
    remember_missing(Path, 60),
    ?assertNot(recently_missing(Path, 60)).

%% @doc A full table keeps its live entries: a new miss is not remembered
%% until entries expire, and then the expired ones are swept to make room.
full_table_keeps_live_entries_test() ->
    remember_missing(hb_util:human_id(crypto:strong_rand_bytes(32)), 60),
    ets:delete_all_objects(?MISSING_TARGETS),
    Now = erlang:monotonic_time(second),
    Live = [ hb_util:human_id(crypto:strong_rand_bytes(32))
        || _ <- lists:seq(1, ?MISSING_TARGETS_MAX) ],
    ets:insert(?MISSING_TARGETS, [ {ID, Now} || ID <- Live ]),
    New = hb_util:human_id(crypto:strong_rand_bytes(32)),
    remember_missing(New, 60),
    ?assertNot(recently_missing(New, 60)),
    ?assert(recently_missing(hd(Live), 60)),
    ets:insert(?MISSING_TARGETS, [ {ID, Now - 61} || ID <- Live ]),
    remember_missing(New, 60),
    ?assert(recently_missing(New, 60)),
    ?assertEqual(1, ets:info(?MISSING_TARGETS, size)),
    ets:delete_all_objects(?MISSING_TARGETS).

%% @doc A lookup of a remembered miss is refused with its own reason.
read_target_reports_remembered_miss_test() ->
    Store = hb_test_utils:test_store(),
    hb_store:start(Store),
    Opts = #{ <<"store">> => [Store] },
    ID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assertEqual({error, not_found}, read_target(ID, Opts)),
    ?assertEqual({error, recently_not_found}, read_target(ID, Opts)).

dev_push_test_() ->
    [
        {inparallel, core_push_test_cases() ++ genesis_wasm_tests()}
    ].

core_push_test_cases() ->
    [
        {timeout, 60, fun test_full_push/0},
        {timeout, 60, fun() -> test_push_stamps_provenance(<<"httpsig@1.0">>) end},
        {timeout, 60, fun() -> test_push_stamps_provenance(<<"ans104@1.0">>) end},
        fun test_redirect_resign_keeps_stamps/0,
        {timeout, 90, fun test_push_as_identity/0},
        {timeout, 60, fun test_multi_process_push/0},
        {timeout, 90, fun test_remote_routed_push/0},
        {timeout, 60, fun test_oracle_push/0},
        fun test_paranoid_push_result/0
    ].

cron_push_test_() ->
    {timeout, 120, fun test_cron_push/0}.

test_paranoid_push_result() ->
    % The `push_result' topic verifies the result before it is signed for
    % POSTing (at the `apply_security' entry): corrupting a committed
    % sub-message is caught before any signature is applied.
    Opts =
        #{
            <<"priv-wallet">> => hb:wallet(),
            <<"paranoid-verify">> => [push_result],
            <<"debug-print">> => []
        },
    Nested = hb_message:commit(#{ <<"body">> => <<"ok">> }, Opts),
    ?assertThrow(
        {paranoid_verification_failure, push_result, _, _, _},
        apply_security(
            #{ <<"body">> => Nested#{ <<"body">> => <<"mangled">> } },
            #{},
            <<"httpsig@1.0">>,
            Opts
        )
    ).

-ifdef(ENABLE_GENESIS_WASM).
genesis_wasm_tests() -> [{timeout, 30, fun test_nested_push_prompts_encoding_change/0}].
-else.
genesis_wasm_tests() -> [].
-endif.

%% @doc Wait until `now/results/data' of `Base' is `Expected'. A push returns
%% once its own slot is delivered; the rest of a chain runs on, each target
%% computing what it is sent (here: each `now' computes the newest slot) and
%% its deliverer sending on what that emits.
await_now_data(Base, Expected, Opts) ->
    wait_until(
        fun() -> hb_ao:resolve(Base, <<"now/results/data">>, Opts) == {ok, Expected} end,
        30000
    ).

test_full_push() ->
    hb_process_test_vectors:init(),
    Opts = #{
        <<"priv-wallet">> => hb:wallet(),
        <<"cache-control">> => <<"always">>,
        <<"store">> => [hb_test_utils:test_store(hb_store_lmdb)],
        % Exercise the `push_result' paranoid check against a real signed push.
        <<"paranoid-verify">> => [push_result]
    },
    Base = hb_process_test_vectors:aos_process(Opts),
    hb_cache:write(Base, Opts),
    {ok, _} =
        hb_ao:resolve(Base, #{
            <<"method">> => <<"POST">>,
            <<"path">> => <<"schedule">>,
            <<"body">> => Base
        },
        Opts
    ),
    {ok, Req} = hb_process_test_vectors:schedule_aos_call(Base, ping_pong_script(2), Opts),
    {ok, StartingMsgSlot} = hb_ao:resolve(Req, #{ <<"path">> => <<"slot">> }, Opts),
    {ok, Pushed} =
        hb_ao:resolve(Base, #{ <<"path">> => <<"push">>, <<"slot">> => StartingMsgSlot }, Opts),
    % The push answers with its slot's delivery: the first Ping, on the
    % process itself, at the next slot.
    ?assertEqual(
        hb_util:int(StartingMsgSlot) + 1,
        hb_util:int(hb_util:deep_get(<<"1/slot">>, Pushed, Opts))
    ),
    ?assert(await_now_data(Base, <<"Done.">>, Opts)).

%% @doc A pushed message names the slot and the outbox entry that emitted it,
%% under the pusher's signature, in either codec the push signs with. The
%% `ans104@1.0' run is the codec the 422 downgrade re-signs with: there
%% `from-slot' must still come back an integer.
test_push_stamps_provenance(Codec) ->
    hb_process_test_vectors:init(),
    Opts = #{
        <<"priv-wallet">> => hb:wallet(),
        <<"cache-control">> => <<"always">>,
        <<"store">> => [hb_test_utils:test_store(hb_store_lmdb)],
        <<"scheduler-default-commitment-spec">> => Codec
    },
    Base = hb_process_test_vectors:aos_process(Opts),
    hb_cache:write(Base, Opts),
    {ok, _} =
        hb_ao:resolve(
            Base,
            #{
                <<"method">> => <<"POST">>,
                <<"path">> => <<"schedule">>,
                <<"body">> => Base
            },
            Opts
        ),
    {ok, Req} =
        hb_process_test_vectors:schedule_aos_call(Base, ping_pong_script(2), Opts),
    {ok, Slot} = hb_ao:resolve(Req, #{ <<"path">> => <<"slot">> }, Opts),
    {ok, Pushed} =
        hb_ao:resolve(Base, #{ <<"path">> => <<"push">>, <<"slot">> => Slot }, Opts),
    {ok, RawPushedMsg} =
        hb_cache:read(hb_util:deep_get(<<"1/id">>, Pushed, Opts), Opts),
    PushedMsg =
        assert_stamped(
            hb_cache:ensure_all_loaded(RawPushedMsg, Opts),
            Base,
            Slot,
            <<"1">>,
            Opts
        ),
    ?assert(lists:member(Codec, hb_message:commitment_devices(PushedMsg, Opts))),
    % The stamps survive the codec's own wire form, `from-slot' as an integer.
    Wire = hb_message:convert(PushedMsg, Codec, <<"structured@1.0">>, Opts),
    Back = hb_message:convert(Wire, <<"structured@1.0">>, Codec, Opts),
    ?assertEqual(hb_util:int(Slot), hb_maps:get(<<"from-slot">>, Back, Opts)),
    ?assertEqual(<<"1">>, hb_maps:get(<<"from-outbox-key">>, Back, Opts)).

%% @doc Check that a pushed message's stamps name the sender `Proc', its
%% `Slot' and outbox `Key': the entry ID matches the sender's stored outbox,
%% `from-slot' is an integer, and every stamp is under the pusher's
%% signature, so altering any one of them breaks it. Returns the message.
assert_stamped(PushedMsg, Proc, Slot, Key, Opts) ->
    PushedID = hb_message:id(PushedMsg, all, Opts),
    ?assertEqual(
        lib_process:process_id(Proc, #{}, Opts),
        hb_maps:get(<<"from-process">>, PushedMsg, Opts)
    ),
    ?assertEqual(hb_util:int(Slot), hb_maps:get(<<"from-slot">>, PushedMsg, Opts)),
    ?assertEqual(Key, hb_maps:get(<<"from-outbox-key">>, PushedMsg, Opts)),
    {ok, Results} =
        hb_ao:resolve(
            Proc,
            #{ <<"path">> => <<"compute/results">>, <<"slot">> => Slot },
            Opts
        ),
    Outbox =
        hb_ao:normalize_keys(
            hb_private:reset(hb_ao:get(<<"outbox">>, Results, Opts))
        ),
    Entry = hb_cache:ensure_all_loaded(hb_maps:get(Key, Outbox, Opts), Opts),
    EntryID = hb_message:id(Entry, all, Opts),
    ?assertEqual(EntryID, hb_maps:get(<<"from-entry">>, PushedMsg, Opts)),
    ?assertNotEqual(EntryID, PushedID),
    assert_signed_stamps(PushedMsg, Opts).

%% @doc Every delivery stamp is committed by the message's signature, so
%% changing any one of them breaks verification. Returns the message.
assert_signed_stamps(Msg, Opts) ->
    ?assert(hb_message:verify(Msg, all, Opts)),
    Committed = hb_message:committed(Msg, all, Opts),
    lists:foreach(
        fun(Stamp) ->
            ?assert(lists:member(Stamp, Committed)),
            Other =
                case hb_maps:get(Stamp, Msg, Opts) of
                    N when is_integer(N) -> N + 1;
                    B -> <<B/binary, "0">>
                end,
            ?assertNot(hb_message:verify(Msg#{ Stamp => Other }, all, Opts))
        end,
        [<<"from-slot">>, <<"from-outbox-key">>, <<"from-entry">>]
    ),
    Msg.

%% @doc On a 307 the deliverer strips the hint from the target and re-signs
%% the already-augmented message to the target's policy, in either codec. The
%% re-signed message keeps every stamp, `from-slot' an integer, under the
%% pusher's signature.
test_redirect_resign_keeps_stamps() ->
    Opts = #{ <<"priv-wallet">> => hb:wallet() },
    Target = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Origin =
        #{
            <<"process">> => hb_util:human_id(crypto:strong_rand_bytes(32)),
            <<"from-slot">> => 7,
            <<"from-outbox-key">> => <<"2">>,
            <<"from-entry">> => hb_util:human_id(crypto:strong_rand_bytes(32)),
            <<"from-uncommitted">> => hb_util:human_id(crypto:strong_rand_bytes(32)),
            <<"from-base">> => hb_util:human_id(crypto:strong_rand_bytes(32)),
            <<"from-scheduler">> => hb:address(),
            <<"from-authority">> => hb:address()
        },
    Augmented =
        augment_message(
            Origin,
            #{ <<"target">> => <<Target/binary, "?hint=elsewhere">>, <<"action">> => <<"Ping">> },
            Opts
        ),
    lists:foreach(
        fun(Codec) ->
            Resigned =
                apply_security(
                    normalize_message(Augmented, Opts),
                    #{ <<"authority">> => hb:address() },
                    Codec,
                    Opts
                ),
            ?assertEqual(Target, hb_maps:get(<<"target">>, Resigned, Opts)),
            ?assertEqual(7, hb_maps:get(<<"from-slot">>, Resigned, Opts)),
            ?assertEqual(<<"2">>, hb_maps:get(<<"from-outbox-key">>, Resigned, Opts)),
            ?assertEqual(
                hb_maps:get(<<"from-entry">>, Origin, Opts),
                hb_maps:get(<<"from-entry">>, Resigned, Opts)
            ),
            assert_signed_stamps(Resigned, Opts)
        end,
        [<<"httpsig@1.0">>, <<"ans104@1.0">>]
    ).

test_push_as_identity() ->
    hb_process_test_vectors:init(),
    DefaultWallet = hb:wallet(),
    SchedulingWallet = ar_wallet:new(),
    SchedulingID = hb:address(SchedulingWallet),
    ComputeWallet = ar_wallet:new(),
    ComputeID = hb:address(ComputeWallet),
    TestStore = [hb_test_utils:test_store(hb_store_lmdb)],
    Opts = #{
        <<"priv-wallet">> => DefaultWallet,
        <<"cache-control">> => <<"always">>,
        <<"store">> => TestStore,
        <<"identities">> => #{
            SchedulingID => #{
                <<"priv-wallet">> => SchedulingWallet,
                <<"store">> => [hb_test_utils:test_store(hb_store_lmdb)]
            },
            ComputeID => #{
                <<"priv-wallet">> => ComputeWallet
            }
        }
    },
    % Its first scheduler is an Ethereum address, which is not a committer.
    EthAddress = <<"0xb9c5714089478a327f09197987f16f9e5d936e8a">>,
    Base =
        hb_process_test_vectors:aos_process(
            Opts#{
                <<"authority">> => ComputeID,
                <<"scheduler">> => [EthAddress, SchedulingID, ComputeID]
            }
        ),
    hb_cache:write(Base, Opts),
    {ok, _} =
        hb_ao:resolve(Base, #{
            <<"method">> => <<"POST">>,
            <<"path">> => <<"schedule">>,
            <<"body">> => Base
        },
        Opts
    ),
    {ok, Req} = hb_process_test_vectors:schedule_aos_call(Base, ping_pong_script(2), Opts),
    {ok, StartingMsgSlot} = hb_ao:resolve(Req, #{ <<"path">> => <<"slot">> }, Opts),
    {ok, _} =
        hb_ao:resolve(Base, #{ <<"path">> => <<"push">>, <<"slot">> => StartingMsgSlot }, Opts),
    ?assert(await_now_data(Base, <<"Done.">>, Opts)),
    % Validate that the scheduler's wallet was used to sign the assignment.
    Assignment = hb_ao:get(<<"schedule/assignments/2">>, Base, Opts),
    Committers =
        hb_ao:get(
            <<"committers">>,
            hb_cache:read_all_commitments(Assignment, Opts),
            Opts
        ),
    ?assert(lists:member(hb_test_utils:committer(SchedulingWallet), Committers)),
    ?assert(lists:member(hb_test_utils:committer(ComputeWallet), Committers)),
    % Validate that the compute wallet was used to sign the message.
    ?assertEqual(
        [hb_test_utils:committer(ComputeWallet)],
        hb_ao:get(<<"schedule/assignments/2/body/committers">>, Base, Opts)
    ),
    % A delivery skips an authority that is not a committer, and signs with
    % the process's other authorities.
    ?assertEqual(
        [hb_test_utils:committer(ComputeWallet)],
        hb_message:signers(
            apply_security(
                #{ <<"body">> => <<"pushed">> },
                Base#{ <<"authority">> => [EthAddress, ComputeID] },
                <<"httpsig@1.0">>,
                Opts
            ),
            Opts
        )
    ).

test_multi_process_push() ->
    {Sender, Receiver, MsgSlot, Opts} = setup_two_process_message(),
    {ok, _} =
        hb_process_test_vectors:schedule_aos_call(
            Sender,
            <<
                "Handlers.add(\"Pong\",\n"
                "   function (test) return true end,\n"
                "   function(m)\n"
                "       print(\"GOT PONG\")\n"
                "   end\n"
                ")"
            >>,
            Opts
        ),
    {ok, PushResult} =
        hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => MsgSlot }, Opts),
    % The pushed message contains the AOS `Tags' as keys, not as a `tags' list.
    {ok, Pushed} =
        hb_cache:read(hb_util:deep_get(<<"1/id">>, PushResult, Opts), Opts),
    ?assertEqual(not_found, hb_ao:get(<<"tags">>, Pushed, Opts)),
    % The receiver computes the Ping when it is asked for its newest state,
    % and its deliverer sends the reply on.
    ?assert(wait_until(
        fun() ->
            _ = hb_ao:resolve(Receiver, <<"now">>, Opts),
            hb_ao:resolve(Sender, <<"now/results/data">>, Opts) == {ok, <<"GOT PONG">>}
        end,
        30000
    )).

test_remote_routed_push() ->
    % Node 1 schedules process 1 and routes process 2 to node 2, which
    % schedules process 2. A message pushed to process 1 sends a Ping to
    % process 2: node 1's deliverer posts it, the scheduler redirects it to
    % node 2, and node 2 assigns it. Node 1 never computes process 2.
    N1Wallet = ar_wallet:new(),
    N1Store = [hb_test_utils:test_store(hb_store_lmdb)],
    N2Wallet = ar_wallet:new(),
    N2Store = [hb_test_utils:test_store(hb_store_lmdb)],
    N2Opts =
        #{
            <<"store">> => N2Store,
            <<"priv-wallet">> => N2Wallet,
            <<"process-compute-on-schedule">> => false
        },
    N2 = hb_http_server:start_node(N2Opts),
    Proc2 = hb_process_test_vectors:aos_process(N2Opts),
    LoadedProc2 = hb_cache:ensure_all_loaded(Proc2, N2Opts),
    Proc2ID = hb_message:id(Proc2, signed, N2Opts),
    N1Opts =
        #{
            <<"store">> => N1Store,
            <<"priv-wallet">> => N1Wallet,
            <<"routes">> =>
                [
                    #{
                        <<"template">> => <<Proc2ID/binary, ".*">>,
                        <<"node">> => N2
                    }
                ]
        },
    N1 = hb_http_server:start_node(N1Opts),
    Proc1 = hb_process_test_vectors:aos_process(N1Opts),
    LoadedProc1 = hb_cache:ensure_all_loaded(Proc1, N1Opts),
    Proc1ID = hb_message:id(LoadedProc1, all, N1Opts),
    hb_cache:write(LoadedProc1, N1Opts),
    hb_cache:write(LoadedProc1, N2Opts),
    hb_cache:write(LoadedProc2, N1Opts),
    hb_cache:write(LoadedProc2, N2Opts),
    SetAuthoritiesCommand =
        <<
            "ao.authorities = { ",
                "\"", (hb:address(N1Wallet))/binary, "\",",
                "\"", (hb:address(N2Wallet))/binary, "\"",
            " }; ",
            "ao.addAssignable('foobar', function (msg) return true end); "
            "ao.isAssignable = function(m) return true end"
        >>,
    {ok, _} =
        hb_process_test_vectors:schedule_aos_call(LoadedProc1, SetAuthoritiesCommand, N1Opts),
    {ok, _} =
        hb_process_test_vectors:schedule_aos_call(LoadedProc2, SetAuthoritiesCommand, N2Opts),
    {ok, _} =
        hb_process_test_vectors:schedule_aos_call(LoadedProc2, reply_script(), N2Opts),
    {ok, P1ScriptLoadRes} =
        hb_process_test_vectors:schedule_aos_call(LoadedProc1, reply_script(Proc2ID), N1Opts),
    SlotP1 = hb_ao:get(<<"slot">>, P1ScriptLoadRes, N1Opts),
    {ok, _} =
        hb_http:get(
            N1,
            <<Proc1ID/binary, "/compute&slot=", (hb_util:bin(SlotP1))/binary>>,
            N1Opts
        ),
    ?assertEqual(
        {error, not_found},
        hb_ao:resolve_many(
            [
                LoadedProc2,
                #{ <<"path">> => <<"compute">>, <<"init">> => <<"stop">> }
            ],
            N1Opts
        )
    ),
    ?assertMatch(
        {ok, Slot} when Slot > 0,
        hb_ao:resolve(LoadedProc2, <<"now/at-slot">>, N2Opts)
    ),
    % Node 1 cannot locate process 2's scheduler (no location record), so
    % the Ping is not delivered -- and not lost: it stays pending, retried.
    P1 = hb_util:human_id(Proc1ID),
    ?assert(wait_until(
        fun() -> lists:member(hb_util:int(SlotP1), lib_process_delivery:pending(P1, N1Opts)) end,
        10000
    )),
    timer:sleep(1500),
    ?assert(lists:member(hb_util:int(SlotP1), lib_process_delivery:pending(P1, N1Opts))),
    case hb_name:lookup({delivery, P1}) of
        Pid when is_pid(Pid) -> exit(Pid, kill);
        _ -> ok
    end.

%% @doc Two nodes, each scheduling one process: a Ping from process 1 reaches
%% process 2 on the other node, and its reply comes back. Each node only
%% computes its own process when a cron asks for its newest state; the
%% deliveries are the nodes' own.
test_cron_push() ->
    hb_process_test_vectors:init(),
    SharedStore = hb_test_utils:test_store(hb_store_fs),
    SharedOpts = #{ <<"store">> => [SharedStore] },
    ReadOnlyShared = SharedStore#{ <<"access">> => [<<"read">>] },
    Wallet1 = ar_wallet:new(),
    Wallet2 = ar_wallet:new(),
    Address1 = hb:address(Wallet1),
    Address2 = hb:address(Wallet2),
    UserWallet = ar_wallet:new(),
    Opts = #{
        <<"priv-wallet">> => UserWallet,
        <<"store">> =>
            [hb_test_utils:test_store(hb_store_lmdb), ReadOnlyShared]
    },
    Proc1 =
        hb_process_test_vectors:aos_process(
            SharedOpts#{
                <<"priv-wallet">> => UserWallet,
                <<"scheduler">> => Address1
            }
        ),
    Proc2 =
        hb_process_test_vectors:aos_process(
            SharedOpts#{
                <<"priv-wallet">> => UserWallet,
                <<"scheduler">> => Address2
            }
        ),
    {ok, _} = hb_cache:write(Proc1, SharedOpts),
    {ok, _} = hb_cache:write(Proc2, SharedOpts),
    Proc1ID = hb_message:id(Proc1, all, SharedOpts),
    Proc2ID = hb_message:id(Proc2, all, SharedOpts),
    Store1 = [hb_test_utils:test_store(hb_store_lmdb), ReadOnlyShared],
    Store2 = [hb_test_utils:test_store(hb_store_lmdb), ReadOnlyShared],
    Node1 = hb_http_server:start_node(#{
        <<"priv-wallet">> => Wallet1,
        <<"store">> => Store1,
        <<"hosts-block-internal">> => false
    }),
    Node2 = hb_http_server:start_node(#{
        <<"priv-wallet">> => Wallet2,
        <<"store">> => Store2,
        <<"hosts-block-internal">> => false
    }),
    share_location_record(Wallet1, Node1, Node2),
    share_location_record(Wallet2, Node2, Node1),
    {ok, _} = hb_http:post(Node1, <<"//schedule">>, Proc1, Opts),
    {ok, _} = hb_http:post(Node2, <<"//schedule">>, Proc2, Opts),
    Trust = <<
        "ao.authorities = { \"", Address1/binary, "\",\"", Address2/binary,
        "\" }; ao.addAssignable('all', function (msg) return true end); ",
        "ao.isAssignable = function(m) return true end"
    >>,
    {ok, _} =
        hb_http:post(
            Node1,
            <<Proc1ID/binary, "/schedule">>,
            eval_message(Proc1ID, Trust, Opts),
            Opts
        ),
    {ok, _} =
        hb_http:post(
            Node2,
            <<Proc2ID/binary, "/schedule">>,
            eval_message(
                Proc2ID,
                <<Trust/binary, "\n", (reply_script())/binary>>,
                Opts
            ),
            Opts
        ),
    {ok, _} =
        hb_http:post(
            Node1,
            <<Proc1ID/binary, "/schedule">>,
            eval_message(
                Proc1ID,
                <<"Send({ Target = \"", Proc2ID/binary, "\", Action = \"Ping\" })">>,
                Opts
            ),
            Opts
        ),
    Cron1 = start_now_cron(Node1, Proc1ID),
    Cron2 = start_now_cron(Node2, Proc2ID),
    GameSettled =
        wait_until(
            fun() -> remote_slot_current(Node1, Proc1ID) >= 3 end,
            60000
        ),
    {ok, _} = hb_http:get(Node1, <<"/~cron@1.0/stop=", Cron1/binary>>, #{}),
    {ok, _} = hb_http:get(Node2, <<"/~cron@1.0/stop=", Cron2/binary>>, #{}),
    ?assert(GameSettled),
    ?assertEqual(2, remote_slot_current(Node2, Proc2ID)),
    ?assertEqual(
        {error, not_found},
        hb_cache:read(
            <<"computed/", Proc2ID/binary, "/slot/2">>,
            #{ <<"store">> => Store1 }
        )
    ),
    ?assertEqual(
        {ok, <<"Replying to...\n", Proc1ID/binary, "\nDone.">>},
        hb_http:get(
            Node2,
            <<Proc2ID/binary, "/compute&slot=2/results/data">>,
            #{}
        )
    ).

%% @doc Post an operator-signed location record to the target node.
share_location_record(Wallet, NodeURL, Target) ->
    Record =
        hb_message:commit(
            #{
                <<"type">> => <<"location">>,
                <<"url">> => NodeURL,
                <<"nonce">> => 1,
                <<"time-to-live">> => 60 * 60 * 1000
            },
            #{ <<"priv-wallet">> => Wallet }
        ),
    {ok, _} =
        hb_http:post(
            Target,
            <<"/~location@1.0/known">>,
            Record,
            #{}
        ).

%% @doc Read the current slot of a process from a node over HTTP.
remote_slot_current(Node, ProcID) ->
    {ok, Slot} = hb_http:get(Node, <<ProcID/binary, "/slot/current">>, #{}),
    hb_util:int(Slot).

%% @doc Establish a cron on the given node that drives `/<proc>/now' on every
%% tick, returning the cron task ID.
start_now_cron(Node, ProcID) ->
    {ok, #{ <<"body">> := TaskID }} =
        hb_http:get(
            Node,
            <<
                "/~cron@1.0/every?interval=250-milliseconds",
                "&cron-path=/", ProcID/binary, "/now"
            >>,
            #{}
        ),
    TaskID.

%% @doc Commit an `Eval' message from the caller to the given process.
eval_message(ProcID, Code, Opts) ->
    hb_message:commit(
        #{
            <<"type">> => <<"Message">>,
            <<"action">> => <<"Eval">>,
            <<"target">> => ProcID,
            <<"data">> => Code
        },
        Opts
    ).

test_oracle_push() ->
    hb_process_test_vectors:init(),
    TestStore = [hb_test_utils:test_store(hb_store_lmdb)],
    Opts = #{ <<"priv-wallet">> => hb:wallet(), <<"store">> => TestStore },
    Client = hb_process_test_vectors:aos_process(Opts),
    {ok, _} = hb_cache:write(Client, Opts),
    {ok, _} = hb_process_test_vectors:schedule_aos_call(Client, oracle_script(), Opts),
    {ok, PushResult} =
        hb_ao:resolve(Client, #{ <<"path">> => <<"push">>, <<"slot">> => 0 }, Opts),
    ?event({result, PushResult}),
    ?assertMatch({ok, _}, hb_ao:resolve(Client, <<"now/results/data">>, Opts)).

%% @doc Spin up two AOS processes -- a Sender and a Receiver with a `Reply'
%% handler for `Action = "Ping"' -- and schedule a single Ping message on the
%% Sender that targets the Receiver. Returns `{Sender, Receiver, MsgSlot,
%% Opts}'. The Sender holds none of the keys in `SenderWithout'.
setup_two_process_message() ->
    setup_two_process_message([]).
setup_two_process_message(SenderWithout) ->
    hb_process_test_vectors:init(),
    Opts = #{
        <<"priv-wallet">> => ar_wallet:new(),
        <<"cache-control">> => [],
        <<"store">> => [hb_test_utils:test_store(hb_store_lmdb)]
    },
    Sender =
        hb_message:commit(
            hb_maps:without(
                SenderWithout,
                hb_message:uncommitted(
                    hb_process_test_vectors:aos_process(Opts),
                    Opts
                ),
                Opts
            ),
            Opts
        ),
    {ok, _} = hb_cache:write(Sender, Opts),
    {ok, _} =
        hb_ao:resolve(Sender, #{
            <<"method">> => <<"POST">>,
            <<"path">> => <<"schedule">>,
            <<"body">> => Sender
        }, Opts),
    Receiver = hb_process_test_vectors:aos_process(Opts),
    {ok, _} = hb_cache:write(Receiver, Opts),
    {ok, _} =
        hb_ao:resolve(Receiver, #{
            <<"method">> => <<"POST">>,
            <<"path">> => <<"schedule">>,
            <<"body">> => Receiver
        }, Opts),
    {ok, _} = hb_process_test_vectors:schedule_aos_call(Receiver, reply_script(), Opts),
    ReceiverID = hb_message:id(Receiver, all, Opts),
    {ok, MsgSched} =
        hb_process_test_vectors:schedule_aos_call(
            Sender,
            <<
                "Send({ Target = \"", (ReceiverID)/binary,
                "\", Action = \"Ping\" })\n"
            >>,
            Opts
        ),
    {ok, MsgSlot} =
        hb_ao:resolve(MsgSched, #{ <<"path">> => <<"slot">> }, Opts),
    {Sender, Receiver, MsgSlot, Opts}.

%% @doc A push delivers each outbox entry once, however often it is asked:
%% the receiver's schedule advances once, and a repeat push answers with the
%% same receipt. A Sender with no `authority' sends a message with no
%% `from-authority'.
push_delivers_once_test_() ->
    {timeout, 60, fun() ->
        {Sender, Receiver, MsgSlot, Opts} =
            setup_two_process_message([<<"authority">>]),
        Before = receiver_slot(Receiver, Opts),
        SID = hb_message:id(Sender, all, Opts),
        % The receiver replies, and the conversation goes on without anybody
        % pushing it; what must happen once is this slot's message.
        Copies = fun() ->
            length([ S || S <- lists:seq(Before + 1, receiver_slot(Receiver, Opts)),
                Body <- [assignment_body(Receiver, S, Opts)],
                hb_ao:get(<<"from-process">>, Body, undefined, Opts) == SID,
                hb_util:int(hb_ao:get(<<"from-slot">>, Body, -1, Opts)) == hb_util:int(MsgSlot) ])
        end,
        Push = #{ <<"path">> => <<"push">>, <<"slot">> => MsgSlot },
        {ok, First} = hb_ao:resolve(Sender, Push, Opts),
        #{ <<"1">> := Receipt } = First,
        ?assertEqual(Before + 1, hb_util:int(maps:get(<<"slot">>, Receipt))),
        ?assertEqual(1, Copies()),
        {ok, Second} = hb_ao:resolve(Sender, Push, Opts),
        ?assertEqual(
            maps:with([<<"id">>, <<"slot">>, <<"target">>], Receipt),
            maps:with([<<"id">>, <<"slot">>, <<"target">>], maps:get(<<"1">>, Second))
        ),
        ?assertEqual(1, Copies()),
        ?assertEqual(
            undefined,
            hb_ao:get(
                <<"schedule/assignments/2/body/from-authority">>,
                Receiver,
                undefined,
                Opts
            )
        ),
        % The `schedule' key reads the same receipt; it never posts again.
        {ok, Again} =
            hb_ao:resolve(
                #{ <<"device">> => <<"push@1.0">>,
                    <<"process">> => hb_message:id(Sender, all, Opts) },
                #{ <<"path">> => <<"schedule">>, <<"slot">> => MsgSlot,
                    <<"outbox-key">> => <<"1">> },
                Opts
            ),
        ?assertEqual(maps:get(<<"id">>, Receipt), maps:get(<<"id">>, Again)),
        ?assertEqual(1, Copies()),
        % A push of a slot that is not yet scheduled returns the compute's
        % error.
        ?assertMatch(
            {error, #{ <<"phase">> := <<"get-schedule">> }},
            hb_ao:resolve(
                Receiver,
                #{ <<"path">> => <<"push">>, <<"slot">> => Before + 50 },
                Opts
            )
        )
    end}.

%% @doc Poll `Pred' every 50ms until it returns `true' or `TimeoutMs'
%% elapses. Returns `true' on success, `false' on timeout.
wait_until(Pred, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    wait_until_loop(Pred, Deadline).

wait_until_loop(Pred, Deadline) ->
    case (catch Pred()) of
        true -> true;
        _ ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true -> false;
                false ->
                    timer:sleep(50),
                    wait_until_loop(Pred, Deadline)
            end
    end.

receiver_slot(Receiver, Opts) ->
    {ok, Slot} = hb_ao:resolve(Receiver, #{ <<"path">> => <<"slot/current">> }, Opts),
    hb_util:int(Slot).

-ifdef(ENABLE_GENESIS_WASM).
%% @doc A message that generates another message which resides on an ANS-104
%% scheduler leads the deliverer to re-sign the message correctly.
%% Requires `ENABLE_GENESIS_WASM' to be enabled.
test_nested_push_prompts_encoding_change() ->
    hb_process_test_vectors:init(),
    Opts = #{
        <<"priv-wallet">> => hb:wallet(),
        <<"cache-control">> => <<"always">>,
        <<"store">> => hb_opts:get(store)
    },
    Base = hb_process_test_vectors:aos_process(Opts),
    hb_cache:write(Base, Opts),
    {ok, _} =
        hb_ao:resolve(Base, #{
            <<"method">> => <<"POST">>,
            <<"path">> => <<"schedule">>,
            <<"body">> => Base
        },
        Opts
    ),
    {ok, Req} =
        hb_process_test_vectors:schedule_aos_call(
            Base, message_to_legacynet_scheduler_script(), Opts),
    {ok, StartingMsgSlot} = hb_ao:resolve(Req, #{ <<"path">> => <<"slot">> }, Opts),
    {ok, _} =
        hb_ao:resolve(Base, #{ <<"path">> => <<"push">>, <<"slot">> => StartingMsgSlot }, Opts),
    Msg = hb_message:commit(#{
        <<"path">> => <<"push">>,
        <<"method">> => <<"POST">>,
        <<"body">> =>
            hb_message:commit(
                #{
                    <<"target">> => hb_message:id(Base, all, Opts),
                    <<"action">> => <<"Ping">>
                },
                Opts
            )
    }, Opts),
    Res2 =
        hb_ao:resolve_many(
            [
                <<"ao:", (hb_message:id(Base, all, Opts))/binary>>,
                {as, <<"process@1.0">>, <<>>},
                Msg
            ],
            Opts
        ),
    ?assertMatch({ok, #{ <<"1">> := #{ <<"slot">> := _ }}}, Res2).
-endif.

%%% Test helpers

ping_pong_script(Limit) ->
    <<
        "Handlers.add(\"Ping\",\n"
        "   function (test) return true end,\n"
        "   function(m)\n"
        "       C = tonumber(m.Count)\n"
        "       if C <= ", (integer_to_binary(Limit))/binary, " then\n"
        "           Send({ Target = ao.id, Action = \"Ping\", Count = C + 1 })\n"
        "           print(\"Ping\", C + 1)\n"
        "       else\n"
        "           print(\"Done.\")\n"
        "       end\n"
        "   end\n"
        ")\n"
        "Send({ Target = ao.id, Action = \"Ping\", Count = 1 })\n"
    >>.

reply_script() ->
    <<
        """
        Handlers.add("Reply",
           { Action = "Ping" },
           function(m)
               print("Replying to...")
               print(m.From)
               Send({ Target = m.From, Action = "Reply", Message = "Pong!" })
               print("Done.")
           end
        )
        """
    >>.
reply_script(OtherProcessID) ->
    <<
        (reply_script())/binary, "\n",
        "Send({ Target = \"", (OtherProcessID)/binary, "\", Action = \"Ping\" })\n"
    >>.

message_to_legacynet_scheduler_script() ->
    <<
        """
        Handlers.add("Ping",
           { Action = "Ping" },
           function(m)
               print("Pinging...")
               print(m.From)
               Send({
                    Target = "QQiMcAge5ZtxcUV7ruxpi16KYRE8UBP0GAAqCIJPXz0",
                    Action = "Ping"
                })
               print("Done.")
           end
        )
        """
    >>.

oracle_script() ->
    <<
        """
        Handlers.add("Oracle",
            function(m)
                return true
            end,
            function(m)
                print(m.Body)
            end
        )
        Send({
            target = ao.id,
            resolve = "/~relay@1.0/call",
            ["relay-path"] = "https://arweave.net"
        })
        
        """
    >>.

%%% Delivery: the node delivers every stored slot's outbox, exactly once.

%% @doc A delta process whose message carrying `sendto' (and `sendto2') sends
%% one message to each of those targets, in that order.
sender_script() ->
    <<
        "Count = Count or 0\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  local value = tostring(Count)\n"
        "  local res = { output = { data = value } }\n"
        "  local body = req.body or {}\n"
        "  local out = {}\n"
        "  if body.sendto then\n"
        "    out[#out + 1] = { target = body.sendto, data = 'ping-' .. value }\n"
        "  end\n"
        "  if body.sendto2 then\n"
        "    out[#out + 1] = { target = body.sendto2, data = 'ping2-' .. value }\n"
        "  end\n"
        "  if #out > 0 then res.outbox = out end\n"
        "  return {\n"
        "    patches = { { path = '/count', value = value } },\n"
        "    results = res\n"
        "  }\n"
        "end\n"
    >>.

%% @doc A delta process that answers every delivered message (one carrying
%% `from-process', as the deliverer stamps it) with one back to its sender.
replier_script() ->
    <<
        "Count = Count or 0\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  local value = tostring(Count)\n"
        "  local res = { output = { data = value } }\n"
        "  local from = req.body and req.body['from-process']\n"
        "  if from then\n"
        "    res.outbox = { { target = from, data = 'pong-' .. value } }\n"
        "  end\n"
        "  return {\n"
        "    patches = { { path = '/count', value = value } },\n"
        "    results = res\n"
        "  }\n"
        "end\n"
    >>.

delivery_opts(Extra) ->
    hb_process_test_vectors:init(),
    maps:merge(
        #{
            <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(),
            <<"process-state-blobs">> => true,
            <<"process-delta-cache">> => true,
            <<"process-hot-cache-slots">> => 0,
            <<"process-snapshot-slots">> => 5,
            <<"process-snapshot-time">> => false,
            <<"process-snapshot-target">> => false,
            <<"process-compute-on-schedule">> => false
        },
        Extra
    ).

%% @doc A process running `Script', written and initialized on its scheduler.
spawn_process(Script, Opts) ->
    P = hb_process_test_vectors:lua_patch_process(Script, Opts),
    {ok, _} = hb_cache:write(P, Opts),
    {ok, _} =
        hb_ao:resolve(
            P,
            #{ <<"method">> => <<"POST">>, <<"path">> => <<"schedule">>, <<"body">> => P },
            Opts
        ),
    {P, hb_message:id(P, all, Opts)}.

%% @doc A sender and two silent targets.
delivery_setup(Extra) ->
    Opts = delivery_opts(Extra),
    {A, AID} = spawn_process(<<"function compute() end">>, Opts),
    {B, BID} = spawn_process(<<"function compute() end">>, Opts),
    Sender = hb_process_test_vectors:lua_patch_process(sender_script(), Opts),
    {ok, _} = hb_cache:write(Sender, Opts),
    {Sender, hb_message:id(Sender, all, Opts), {A, AID}, {B, BID}, Opts}.

%% @doc Schedule a message on the sender; `Body' names its targets.
send(Sender, N, Body, Opts) ->
    hb_process_test_vectors:schedule_test_message(
        Sender, <<"msg">>, Body#{ <<"n">> => N }, Opts).

compute(Process, Slot, Opts) ->
    hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, Opts).

rows(ProcID, Kind, Opts) -> hb_process_test_vectors:pstate_slots(ProcID, Kind, Opts).

%% Delivered: every slot has its `x', and no `o' or `q' is left without one
%% (those beside an `x' go with the next commit, or retention).
delivered(ProcID, Slots, Opts) ->
    X = rows(ProcID, <<"x">>, Opts),
    X == Slots
        andalso rows(ProcID, <<"o">>, Opts) -- X == []
        andalso rows(ProcID, <<"q">>, Opts) -- X == [].

deliverer(ProcID) -> hb_name:lookup({delivery, ProcID}).

counter(Name) ->
    lists:sum([ N || {P, _, N, _} <- hb_phase_timer:read(), P == Name ]).

%% @doc A post that records the ID of every signed message it is given, then
%% does what `Then' says.
recording_post(Self, Then) ->
    fun(TP, T, L, Signed, O, Real) ->
        ID = hb_message:id(Signed, all, O),
        Self ! {posted, T, ID, hb_maps:get(<<"from-slot">>, Signed, undefined, O)},
        Then(TP, T, L, Signed, O, Real)
    end.

posted_ids() ->
    receive {posted, _, ID, _} -> [ID | posted_ids()]
    after 0 -> []
    end.

assignment_body(Target, Slot, Opts) ->
    hb_ao:get(<<"schedule/assignments/", (hb_util:bin(Slot))/binary, "/body">>, Target, Opts).

%% @doc The 2026-11-05 incident in miniature: a slot computed by anybody --
%% here a plain `compute' with no `push' -- is delivered by the node. Before,
%% only a compute that carried `push' pushed, so a slot the scheduler's
%% follower computed first was never delivered (the vault's payout 1210).
worker_delivers_without_push_test_() ->
    {timeout, 60, fun() ->
        {Sender, SID, {A, AID}, _, Opts} = delivery_setup(#{}),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        Before = receiver_slot(A, Opts),
        {ok, _} = compute(Sender, 0, Opts),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        ?assertEqual(Before + 1, receiver_slot(A, Opts)),
        Body = assignment_body(A, Before + 1, Opts),
        ?assertEqual(SID, hb_ao:get(<<"from-process">>, Body, Opts)),
        ?assertEqual(0, hb_ao:get(<<"from-slot">>, Body, Opts)),
        % A push of the slot is a read: the same receipt, nothing posted.
        {ok, Pushed} = hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => 0 }, Opts),
        ?assertEqual(Before + 1, hb_util:int(hb_util:deep_get(<<"1/slot">>, Pushed, Opts))),
        ?assertEqual(Before + 1, receiver_slot(A, Opts))
    end}.

%% @doc A push is a read that dies with its caller: a push killed while it
%% waits leaves nothing behind, and delivery goes on without it.
push_is_read_test_() ->
    {timeout, 60, fun() ->
        {Sender, SID, {A, AID}, {B, BID}, Opts} = delivery_setup(#{}),
        send(Sender, 1, #{ <<"sendto">> => AID, <<"sendto2">> => BID }, Opts),
        {BeforeA, BeforeB} = {receiver_slot(A, Opts), receiver_slot(B, Opts)},
        Procs = length(erlang:processes()),
        Caller =
            spawn(fun() ->
                hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => 0 }, Opts)
            end),
        timer:sleep(50),
        exit(Caller, kill),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        ?assertEqual(BeforeA + 1, receiver_slot(A, Opts)),
        ?assertEqual(BeforeB + 1, receiver_slot(B, Opts)),
        % Asking again, however often, delivers nothing more.
        [ {ok, _} = hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => 0 }, Opts)
        || _ <- lists:seq(1, 3) ],
        ?assertEqual(BeforeA + 1, receiver_slot(A, Opts)),
        ?assertEqual(BeforeB + 1, receiver_slot(B, Opts)),
        % No detached push is left running: only the deliverer, the worker
        % and the stores' own processes may remain.
        ?assert(length(erlang:processes()) =< Procs + 10)
    end}.

%% @doc A target that fails is retried with the same signed message, and the
%% slot is not delivered (no `x') until the target assigns it. Nothing is
%% ever counted as delivered on an answer that is not proof: a 404 (a message
%% the scheduler could not load yet), a 429, a 5xx.
failing_downstream_retried_test_() ->
    {timeout, 90, fun() ->
        Counter = counters:new(1, []),
        Self = self(),
        Post =
            recording_post(Self,
                fun(TP, T, L, S, O, Real) ->
                    counters:add(Counter, 1, 1),
                    case counters:get(Counter, 1) of
                        1 -> {error, #{ <<"status">> => 404,
                                <<"body">> => <<"Cannot fully load message to schedule.">> }};
                        2 -> {error, #{ <<"status">> => 429 }};
                        3 -> {error, #{ <<"status">> => 503 }};
                        _ -> Real(TP, T, L, S, O)
                    end
                end),
        {Sender, SID, {A, AID}, _, Opts} =
            delivery_setup(#{ <<"delivery-test-post">> => Post }),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        Before = receiver_slot(A, Opts),
        RetriesBefore = counter(delivery_retry),
        {ok, _} = compute(Sender, 0, Opts),
        ?assert(wait_until(fun() -> counters:get(Counter, 1) >= 2 end, 15000)),
        ?assertEqual([], rows(SID, <<"x">>, Opts)),
        ?assertEqual([0], rows(SID, <<"o">>, Opts)),
        ?assertEqual([0], rows(SID, <<"q">>, Opts)),
        ?assertEqual(Before, receiver_slot(A, Opts)),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 30000)),
        ?assertEqual(Before + 1, receiver_slot(A, Opts)),
        ?assert(counter(delivery_retry) - RetriesBefore >= 3),
        IDs = posted_ids(),
        ?assert(length(IDs) >= 4),
        ?assertEqual(1, length(lists:usort(IDs)))
    end}.

%% @doc A target that does not answer holds back only its own entries: the
%% other target of the same slot is delivered, and the worker keeps computing.
slow_downstream_does_not_block_test_() ->
    {timeout, 90, fun() ->
        Self = self(),
        Gate = fun(AID) ->
            fun(TP, T, L, S, O, Real) ->
                case T of
                    AID ->
                        Self ! {blocked, self()},
                        receive release -> Real(TP, T, L, S, O) end;
                    _ -> Real(TP, T, L, S, O)
                end
            end
        end,
        Opts0 = delivery_opts(#{}),
        {A, AID} = spawn_process(<<"function compute() end">>, Opts0),
        {B, BID} = spawn_process(<<"function compute() end">>, Opts0),
        Opts = Opts0#{ <<"delivery-test-post">> => Gate(AID) },
        Sender = hb_process_test_vectors:lua_patch_process(sender_script(), Opts),
        {ok, _} = hb_cache:write(Sender, Opts),
        SID = hb_message:id(Sender, all, Opts),
        send(Sender, 1, #{ <<"sendto">> => AID, <<"sendto2">> => BID }, Opts),
        [ send(Sender, N, #{}, Opts) || N <- lists:seq(2, 4) ],
        {BeforeA, BeforeB} = {receiver_slot(A, Opts), receiver_slot(B, Opts)},
        {ok, _} = compute(Sender, 0, Opts),
        Helper = receive {blocked, H} -> H after 15000 -> erlang:error(never_posted) end,
        ?assert(wait_until(fun() -> receiver_slot(B, Opts) == BeforeB + 1 end, 15000)),
        ?assertEqual(BeforeA, receiver_slot(A, Opts)),
        ?assertEqual([], rows(SID, <<"x">>, Opts)),
        {Micros, {ok, _}} = timer:tc(fun() -> compute(Sender, 3, Opts) end),
        ?assert(Micros < 10000000),
        Helper ! release,
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        ?assertEqual(BeforeA + 1, receiver_slot(A, Opts))
    end}.

%% @doc A crash after a target assigned an entry but before the slot's `x':
%% the restarted deliverer posts the same signed message, the target answers
%% 409 with the slot it already has, and that becomes the receipt. One
%% assignment, never two.
crash_after_post_before_x_test_() ->
    {timeout, 90, fun() ->
        Self = self(),
        Post = recording_post(Self, fun(TP, T, L, S, O, Real) -> Real(TP, T, L, S, O) end),
        {Sender, SID, {A, AID}, _, Opts} =
            delivery_setup(#{
                <<"delivery-test-post">> => Post,
                <<"delivery-test-crash">> => after_post
            }),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        Before = receiver_slot(A, Opts),
        {ok, _} = compute(Sender, 0, Opts),
        ?assert(wait_until(fun() -> receiver_slot(A, Opts) == Before + 1 end, 15000)),
        ?assert(wait_until(fun() -> deliverer(SID) == undefined end, 15000)),
        ?assertEqual([0], rows(SID, <<"o">>, Opts)),
        ?assertEqual([0], rows(SID, <<"q">>, Opts)),
        ?assertEqual([], rows(SID, <<"x">>, Opts)),
        % The node comes back: its boot sweep finds the slot.
        Already = counter(delivery_already),
        ?assertEqual(1, lib_process_delivery:sweep(maps:remove(<<"delivery-test-crash">>, Opts))),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        ?assertEqual(Before + 1, receiver_slot(A, Opts)),
        ?assertEqual(Already + 1, counter(delivery_already)),
        IDs = posted_ids(),
        ?assertEqual(2, length(IDs)),
        ?assertEqual(1, length(lists:usort(IDs))),
        {ok, Pushed} = hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => 0 }, Opts),
        ?assertEqual(hd(IDs), hb_util:deep_get(<<"1/id">>, Pushed, Opts)),
        ?assertEqual(Before + 1, hb_util:int(hb_util:deep_get(<<"1/slot">>, Pushed, Opts)))
    end}.

%% @doc A crash after signing but before the flush: nothing was posted, so the
%% restarted deliverer signs again and the target gets exactly one message.
crash_before_flush_test_() ->
    {timeout, 90, fun() ->
        {Sender, SID, {A, AID}, _, Opts} =
            delivery_setup(#{ <<"delivery-test-crash">> => before_flush }),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        Before = receiver_slot(A, Opts),
        {ok, _} = compute(Sender, 0, Opts),
        ?assert(wait_until(fun() -> rows(SID, <<"o">>, Opts) == [0] end, 15000)),
        timer:sleep(1000),
        ?assertEqual(undefined, deliverer(SID)),
        ?assertEqual([], rows(SID, <<"q">>, Opts)),
        ?assertEqual(Before, receiver_slot(A, Opts)),
        {ok, _} = lib_process_delivery:ensure(SID, maps:remove(<<"delivery-test-crash">>, Opts)),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        ?assertEqual(Before + 1, receiver_slot(A, Opts))
    end}.

%% @doc Entries are independent: an entry whose post fails -- here its helper
%% crashes (a badmatch, as a failed read of its target would), then its target
%% answers 503 -- backs off alone. The same target's entry from a later slot is
%% delivered meanwhile, and the crash is counted where an operator sees it.
entries_are_independent_test_() ->
    {timeout, 90, fun() ->
        Failed = counters:new(1, []),
        Post =
            fun(TP, T, L, S, O, Real) ->
                case hb_maps:get(<<"from-slot">>, S, undefined, O) of
                    0 ->
                        counters:add(Failed, 1, 1),
                        case counters:get(Failed, 1) of
                            1 -> erlang:error({badmatch, poisoned});
                            2 -> {error, #{ <<"status">> => 503 }};
                            _ -> Real(TP, T, L, S, O)
                        end;
                    _ -> Real(TP, T, L, S, O)
                end
            end,
        {Sender, SID, {A, AID}, _, Opts} =
            delivery_setup(#{ <<"delivery-test-post">> => Post }),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        send(Sender, 2, #{ <<"sendto">> => AID }, Opts),
        Before = receiver_slot(A, Opts),
        Crashes = counter(delivery_post_crash),
        {ok, _} = compute(Sender, 1, Opts),
        ?assert(wait_until(fun() -> rows(SID, <<"x">>, Opts) == [1] end, 15000)),
        ?assert(lists:member(0, rows(SID, <<"o">>, Opts))),
        ?assert(wait_until(fun() -> delivered(SID, [0, 1], Opts) end, 30000)),
        ?assertEqual(3, counters:get(Failed, 1)),
        ?assertEqual(Crashes + 1, counter(delivery_post_crash)),
        ?assertEqual(
            [1, 0],
            [ hb_ao:get(<<"from-slot">>, assignment_body(A, S, Opts), Opts)
            || S <- [Before + 1, Before + 2] ]
        )
    end}.

%% @doc A target nobody has, with no gateway to ask: a proven terminal. The
%% slot is delivered with a terminal receipt; nothing waits on it forever.
missing_target_terminal_test_() ->
    {timeout, 60, fun() ->
        {Sender, SID, _, _, Opts} = delivery_setup(#{}),
        Nobody = hb_util:human_id(crypto:strong_rand_bytes(32)),
        send(Sender, 1, #{ <<"sendto">> => Nobody }, Opts),
        {ok, Pushed} = hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => 0 }, Opts),
        ?assertEqual(<<"terminal">>, hb_util:deep_get(<<"1/status">>, Pushed, Opts)),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 5000))
    end}.

%% @doc With the gateway unreachable, a target the node does not hold is
%% neither delivered nor given up: it is looked up again, with backoff, and
%% never remembered as missing.
gateway_unavailable_retried_test_() ->
    {timeout, 60, fun() ->
        Gateway = #{ <<"store-module">> => hb_store_gateway,
            <<"node">> => <<"http://127.0.0.1:1">>, <<"node-type">> => <<"ao">> },
        Opts0 = delivery_opts(#{}),
        Opts = Opts0#{ <<"store">> => [maps:get(<<"store">>, Opts0), Gateway] },
        Sender = hb_process_test_vectors:lua_patch_process(sender_script(), Opts),
        {ok, _} = hb_cache:write(Sender, Opts),
        SID = hb_message:id(Sender, all, Opts),
        Unknown = hb_util:human_id(crypto:strong_rand_bytes(32)),
        send(Sender, 1, #{ <<"sendto">> => Unknown }, Opts),
        Retries = counter(delivery_lookup_retry),
        {ok, _} = compute(Sender, 0, Opts),
        ?assert(wait_until(fun() -> counter(delivery_lookup_retry) >= Retries + 2 end, 20000)),
        ?assertEqual([], rows(SID, <<"x">>, Opts)),
        ?assertEqual([0], rows(SID, <<"o">>, Opts)),
        ?assertNot(recently_missing(Unknown, 3600)),
        % The node-wide gauges see it: one series each, no process label.
        {ok, Gauges} = hb_ao:raw(<<"process@1.0">>, #{}, #{ <<"path">> => <<"deliveries">> },
            Opts#{ <<"process-delivery-metrics">> => true }),
        Values = maps:from_list([ {N, V} || {N, _Help, V} <- Gauges ]),
        ?assert(maps:get(hb_delivery_pending_entries, Values) >= 1),
        ?assert(maps:get(hb_delivery_oldest_pending_seconds, Values) >= 0),
        ?assert(is_integer(maps:get(hb_worker_mailbox_len, Values))),
        ?assert(is_integer(maps:get(hb_worker_waiter_rows, Values))),
        exit(deliverer(SID), kill)
    end}.

%% @doc However many times a slot is cast and the node is swept, one deliverer
%% delivers it, once.
single_deliverer_test_() ->
    {timeout, 60, fun() ->
        {Sender, SID, {A, AID}, _, Opts} = delivery_setup(#{}),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        Before = receiver_slot(A, Opts),
        {ok, _} = compute(Sender, 0, Opts),
        Self = self(),
        [ spawn(fun() -> lib_process_delivery:notify(SID, 0, Opts), Self ! done end)
        || _ <- lists:seq(1, 20) ],
        [ spawn(fun() -> lib_process_delivery:sweep(Opts), Self ! done end)
        || _ <- lists:seq(1, 5) ],
        [ receive done -> ok end || _ <- lists:seq(1, 25) ],
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        timer:sleep(500),
        ?assertEqual(Before + 1, receiver_slot(A, Opts))
    end}.

%% @doc A chain with no client at all: the sender's message reaches the
%% replier, whose reply comes back and is computed -- every hop delivered by
%% the node, every slot computed by the scheduler's follower.
chain_without_client_test_() ->
    {timeout, 90, fun() ->
        Opts =
            delivery_opts(#{
                <<"process-compute-on-schedule">> => true,
                <<"process-workers">> => true,
                <<"process-worker-spawn">> => true
            }),
        {_Replier, RID} = spawn_process(replier_script(), Opts),
        Sender = hb_process_test_vectors:lua_patch_process(sender_script(), Opts),
        {ok, _} = hb_cache:write(Sender, Opts),
        SID = hb_message:id(Sender, all, Opts),
        send(Sender, 1, #{ <<"sendto">> => RID }, Opts),
        ?assert(wait_until(
            fun() -> lib_process_cache:latest_slot(SID, Opts) == {ok, 1} end,
            30000
        )),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        ?assert(wait_until(fun() -> rows(RID, <<"x">>, Opts) == [1] end, 15000)),
        {ok, Reply} = lib_process_cache:read(SID, 1, Opts),
        ?assertEqual(<<"2">>, hb_ao:get(<<"count">>, Reply, Opts))
    end}.

%%% A push never replays its process.

%% @doc A sender with 16 slots computed (slot 2 sends to a local target), a VM
%% snapshot every 5 slots and retention run: its floor is slot 10, so slot
%% 2's state is gone (the snapshot checkpoints at 0 and 5 stay). Its outbox
%% was delivered by the node, unasked.
late_push_setup() ->
    {Sender, SenderID, {Target, TargetID}, _, Opts} = delivery_setup(#{}),
    Before = receiver_slot(Target, Opts),
    lists:foreach(
        fun(N) ->
            Extra = case N of 3 -> #{ <<"sendto">> => TargetID }; _ -> #{} end,
            send(Sender, N, Extra, Opts)
        end,
        lists:seq(1, 16)
    ),
    {ok, _} = compute(Sender, 15, Opts),
    ?assert(wait_until(fun() -> delivered(SenderID, [2], Opts) end, 15000)),
    ?assertEqual(Before + 1, receiver_slot(Target, Opts)),
    % No age floor: only the newest checkpoints are kept, as the test needs.
    #{ deleted := Deleted } =
        hb_store_retention:retain(Opts#{ <<"store-retention-keep-seconds">> => 0 }),
    ?assert(Deleted > 0),
    Rows = fun(Kind) -> rows(SenderID, Kind, Opts) end,
    ?assertNot(lists:member(2, Rows(<<"d">>) ++ Rows(<<"f">>))),
    ?assertNot(lists:member(4, Rows(<<"d">>) ++ Rows(<<"f">>))),
    ?assert(lists:member(5, Rows(<<"f">>))),
    % Delivered, and below the floor: its receipts went with its state.
    ?assertEqual([], Rows(<<"x">>)),
    {Sender, SenderID, Target, TargetID, Opts}.

slot_executions() ->
    lists:sum([ N || {slot_execution, _, N, _} <- hb_phase_timer:read() ]).

%% @doc A push of a slot whose state retention deleted is a 410 at once:
%% nothing is computed, nothing delivered again, nothing left running.
late_push_is_410_test_() ->
    {timeout, 180, fun() ->
        {Sender, _SenderID, Target, _TargetID, Opts} = late_push_setup(),
        TargetSlot = receiver_slot(Target, Opts),
        Before = slot_executions(),
        Procs = length(erlang:processes()),
        T0 = erlang:monotonic_time(millisecond),
        lists:foreach(
            fun(Slot) ->
                ?assertMatch(
                    {error, #{ <<"status">> := 410 }},
                    hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => Slot }, Opts)
                )
            end,
            [2, 4]
        ),
        ?assert(erlang:monotonic_time(millisecond) - T0 < 5000),
        timer:sleep(500),
        ?assertEqual(Before, slot_executions()),
        ?assertEqual(TargetSlot, receiver_slot(Target, Opts)),
        ?assert(length(erlang:processes()) =< Procs + 5)
    end}.

%% @doc A push of a stored slot outside the hot window reads its results from
%% the slot's delta envelope: it does not rebuild the slot's state by
%% applying the chain of patches back to the checkpoint.
push_reads_d_envelope_without_materialize_test_() ->
    {timeout, 180, fun() ->
        {Sender, SenderID, _Target, _TargetID, Opts} = late_push_setup(),
        ?assert(lists:member(14, rows(SenderID, <<"d">>, Opts))),
        Before = slot_executions(),
        erlang:trace_pattern({hb_state_patch, apply, 5}, true, [call_count]),
        Res = hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => 14 }, Opts),
        {call_count, Applies} = erlang:trace_info({hb_state_patch, apply, 5}, call_count),
        erlang:trace_pattern({hb_state_patch, apply, 5}, false, [call_count]),
        ?assertMatch({ok, _}, Res),
        ?assert(Applies =< 1),
        ?assertEqual(Before, slot_executions())
    end}.

%%% Round 2: the invariants each of these tests pins, one per test.

%% @doc The delivery store, and how many writes it holds that are not
%% committed yet. A store with no periodic flusher (`flush-every' 0) commits
%% only when somebody flushes it, so a write a crash would lose shows here.
unflushed(Opts) ->
    Store = lib_process_cache:delivery_store(Opts),
    #{ <<"db">> := DB } = hb_store:find(Store),
    elmdb:overlay_count(DB).

no_flusher_opts(Extra) ->
    maps:merge(
        #{ <<"store">> => (hb_test_utils:test_store(hb_store_lmdb))#{ <<"flush-every">> => 0 } },
        Extra
    ).

%% @doc The signed message being posted, as the `q' row holds it now.
queued(ProcID, Slot, Key, Opts) ->
    Store = lib_process_cache:delivery_store(Opts),
    case lib_process_cache:blob_get(Store,
            lib_process_cache:blob_key(ProcID, <<"q">>, Slot), Opts) of
        {ok, Bin} ->
            case maps:get(Key, lib_process_cache:blob_decode(Bin), undefined) of
                {signed, _Target, Codec, _Location, Msg} -> {Codec, hb_message:id(Msg, all, Opts)};
                Other -> Other
            end;
        not_found -> not_found
    end.

%% @doc Every message is posted only once its signed bytes are durable: at
%% the moment of every POST the store holds no write it has not committed.
%% (Kills the mutant that drops the flush before the POST.)
signed_message_durable_before_post_test_() ->
    {timeout, 60, fun() ->
        Self = self(),
        Post =
            fun(TP, T, L, S, O, Real) ->
                Self ! {at_post, unflushed(O)},
                Real(TP, T, L, S, O)
            end,
        {Sender, SID, {A, AID}, {B, BID}, Opts} =
            delivery_setup(no_flusher_opts(#{ <<"delivery-test-post">> => Post })),
        send(Sender, 1, #{ <<"sendto">> => AID, <<"sendto2">> => BID }, Opts),
        {BeforeA, BeforeB} = {receiver_slot(A, Opts), receiver_slot(B, Opts)},
        {ok, _} = compute(Sender, 0, Opts),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        Counts = (fun Gather() -> receive {at_post, N} -> [N | Gather()] after 0 -> [] end end)(),
        ?assertEqual([0, 0], Counts),
        ?assertEqual(BeforeA + 1, receiver_slot(A, Opts)),
        ?assertEqual(BeforeB + 1, receiver_slot(B, Opts))
    end}.

%% @doc A 422 sends the entry back to be signed again, as `ans104@1.0'. The
%% new signature is in the `q' row, committed, before it is posted; so after a
%% crash right then, the node posts that same message again and the target
%% answers 409. One assignment, never two. (Kills the mutant that re-signs
%% without persisting.)
resign_is_durable_before_post_test_() ->
    {timeout, 90, fun() ->
        Self = self(),
        Calls = counters:new(1, []),
        Post =
            fun(TP, T, L, S, O, Real) ->
                counters:add(Calls, 1, 1),
                ID = hb_message:id(S, all, O),
                SID = hb_maps:get(<<"from-process">>, S, undefined, O),
                case counters:get(Calls, 1) of
                    1 ->
                        Self ! {post, 1, ID},
                        {error, #{ <<"status">> => 422 }};
                    2 ->
                        Self ! {post, 2, ID, queued(SID, 0, <<"1">>, O), unflushed(O)},
                        exit(hb_name:lookup({delivery, SID}), kill),
                        Real(TP, T, L, S, O);
                    N ->
                        Self ! {post, N, ID},
                        Real(TP, T, L, S, O)
                end
            end,
        {Sender, SID, {A, AID}, _, Opts} =
            delivery_setup(no_flusher_opts(#{ <<"delivery-test-post">> => Post })),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        Before = receiver_slot(A, Opts),
        {ok, _} = compute(Sender, 0, Opts),
        ID1 = receive {post, 1, I1} -> I1 after 15000 -> erlang:error(no_first_post) end,
        {ID2, Queued, Unflushed} =
            receive {post, 2, I2, Q, U} -> {I2, Q, U} after 15000 -> erlang:error(no_resign) end,
        ?assertNotEqual(ID1, ID2),
        ?assertEqual({<<"ans104@1.0">>, ID2}, Queued),
        ?assertEqual(0, Unflushed),
        ?assert(wait_until(fun() -> receiver_slot(A, Opts) == Before + 1 end, 15000)),
        ?assert(wait_until(fun() -> deliverer(SID) == undefined end, 15000)),
        % The node comes back.
        Already = counter(delivery_already),
        {ok, _} = lib_process_delivery:ensure(SID, Opts),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        ID3 = receive {post, 3, I3} -> I3 after 0 -> erlang:error(no_repost) end,
        ?assertEqual(ID2, ID3),
        ?assertEqual(Already + 1, counter(delivery_already)),
        ?assertEqual(Before + 1, receiver_slot(A, Opts))
    end}.

%% @doc Only a node that holds the identity a process names as its scheduler
%% delivers its outbox. A replica -- the same store and process, another key
%% -- keeps no `o' row, starts no deliverer, signs and posts nothing, and a
%% push there says it does not deliver the process instead of answering 200
%% with no receipts. (Kills the mutant that says every node delivers.)
only_the_scheduler_delivers_test_() ->
    {timeout, 60, fun() ->
        Self = self(),
        Post = fun(TP, T, L, S, O, Real) -> Self ! posted, Real(TP, T, L, S, O) end,
        {Sender, SID, {_A, AID}, _, Opts} =
            delivery_setup(#{ <<"delivery-test-post">> => Post }),
        Replica = Opts#{ <<"priv-wallet">> => ar_wallet:new() },
        ?assert(lib_process_delivery:delivers(SID, Opts)),
        ?assertNot(lib_process_delivery:delivers(SID, Replica)),
        Outbox = #{ <<"1">> => #{ <<"target">> => AID, <<"data">> => <<"x">> } },
        {ok, _} = lib_process_cache:write(SID, 0,
            #{ <<"at-slot">> => 0, <<"results">> => #{ <<"outbox">> => Outbox } }, Replica),
        ?assertEqual([], rows(SID, <<"o">>, Opts)),
        ?assertEqual(undefined, deliverer(SID)),
        ?assertEqual({error, not_scheduler}, lib_process_delivery:ensure(SID, Replica)),
        {error, Refused} =
            hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => 0 }, Replica),
        ?assertMatch(#{ <<"status">> := 421, <<"reason">> := <<"not_delivering">> }, Refused),
        ?assertNotEqual(nomatch,
            binary:match(maps:get(<<"body">>, Refused), <<"does not deliver">>)),
        timer:sleep(500),
        receive posted -> erlang:error(a_replica_posted) after 0 -> ok end
    end}.

%% @doc Whether a node delivers a process is a fact of its definition and the
%% node's keys, true the moment the node is back: a node killed with slots
%% assigned but not computed, and not one `o' row, delivers every one of
%% them when the first read computes them, before its scheduler server for
%% the process is running again.
delivers_before_scheduler_restarts_test_() ->
    {timeout, 60, fun() ->
        {Sender, SID, {A, AID}, {B, BID}, Opts} = delivery_setup(#{}),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        send(Sender, 2, #{ <<"sendto">> => BID }, Opts),
        {BeforeA, BeforeB} = {receiver_slot(A, Opts), receiver_slot(B, Opts)},
        ?assertEqual([], rows(SID, <<"o">>, Opts)),
        % The restart: the scheduler server and everything held in memory go.
        Server = hb_name:lookup({<<"scheduler@1.0">>, hb_opts:address(Opts), SID}),
        ?assert(is_pid(Server)),
        Mon = erlang:monitor(process, Server),
        Server ! stop,
        receive {'DOWN', Mon, process, Server, _} -> ok end,
        ets:delete_all_objects(lib_process_delivery_schedules),
        ?assert(lib_process_delivery:delivers(SID, Opts)),
        {ok, _} = compute(Sender, 1, Opts),
        ?assert(wait_until(fun() -> delivered(SID, [0, 1], Opts) end, 15000)),
        ?assertEqual(BeforeA + 1, receiver_slot(A, Opts)),
        ?assertEqual(BeforeB + 1, receiver_slot(B, Opts))
    end}.

%% @doc A slot whose computation fails carries none of the slot before it:
%% its stored results are its error, with no outbox, so the previous slot's
%% messages are not delivered a second time as its own.
error_slot_carries_no_outbox_test_() ->
    {timeout, 90, fun() ->
        Opts = delivery_opts(#{}),
        {A, AID} = spawn_process(<<"function compute() end">>, Opts),
        Script =
            <<
                "Count = Count or 0\n"
                "function compute(req)\n"
                "  local body = req.body or {}\n"
                "  if body.boom then error('boom') end\n"
                "  Count = Count + 1\n"
                "  local res = { output = { data = tostring(Count) } }\n"
                "  if body.sendto then res.outbox = { { target = body.sendto, data = 'ping' } } end\n"
                "  return { patches = { { path = '/count', value = tostring(Count) } }, results = res }\n"
                "end\n"
            >>,
        Sender = hb_process_test_vectors:lua_patch_process(Script, Opts),
        {ok, _} = hb_cache:write(Sender, Opts),
        SID = hb_message:id(Sender, all, Opts),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        send(Sender, 2, #{ <<"boom">> => <<"yes">> }, Opts),
        Before = receiver_slot(A, Opts),
        {ok, _} = compute(Sender, 1, Opts),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        timer:sleep(1000),
        ?assertEqual([0], rows(SID, <<"x">>, Opts)),
        ?assertEqual(Before + 1, receiver_slot(A, Opts)),
        {ok, Results} = lib_process_cache:read_results(SID, 1, Opts),
        ?assertEqual(not_found, hb_maps:get(<<"outbox">>, Results, not_found, Opts))
    end}.

%% @doc A failure inside the deliverer itself loses nothing: it starts again
%% from the rows, posts the same signed message, and the target answers 409.
deliverer_restarts_from_rows_test_() ->
    {timeout, 60, fun() ->
        Self = self(),
        Post = recording_post(Self, fun(TP, T, L, S, O, Real) -> Real(TP, T, L, S, O) end),
        {Sender, SID, {A, AID}, _, Opts} =
            delivery_setup(#{
                <<"delivery-test-post">> => Post,
                <<"delivery-test-crash">> => {raise, after_post, counters:new(1, [])}
            }),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        Before = receiver_slot(A, Opts),
        Restarts = counter(delivery_restart),
        {ok, _} = compute(Sender, 0, Opts),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        ?assertEqual(Restarts + 1, counter(delivery_restart)),
        ?assertEqual(Before + 1, receiver_slot(A, Opts)),
        IDs = posted_ids(),
        ?assertEqual(2, length(IDs)),
        ?assertEqual(1, length(lists:usort(IDs)))
    end}.

%% @doc A push returns once its chain has run: the slot's message delivered,
%% the target's slot computed and its reply delivered, and the reply's slot
%% computed -- each by its own process's worker and deliverer, the push only
%% waiting. `max-depth' 0 waits for the first hop alone.
push_follows_chain_test_() ->
    {timeout, 90, fun() ->
        Opts =
            delivery_opts(#{
                <<"process-workers">> => true,
                <<"process-worker-spawn">> => true
            }),
        {_Replier, RID} = spawn_process(replier_script(), Opts),
        Sender = hb_process_test_vectors:lua_patch_process(sender_script(), Opts),
        {ok, _} = hb_cache:write(Sender, Opts),
        SID = hb_message:id(Sender, all, Opts),
        send(Sender, 1, #{ <<"sendto">> => RID }, Opts),
        {ok, Pushed} = hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => 0 }, Opts),
        % Back at the sender: the reply was assigned slot 1 and computed.
        ?assertEqual(1, hb_util:int(hb_util:deep_get(<<"1/resulted-in/1/slot">>, Pushed, Opts))),
        ?assertEqual(SID, hb_util:deep_get(<<"1/resulted-in/1/target">>, Pushed, Opts)),
        ?assertEqual({ok, 1}, lib_process_cache:latest_slot(SID, Opts)),
        % `max-depth' 0: the first hop's receipt, nothing downstream.
        send(Sender, 3, #{ <<"sendto">> => RID }, Opts),
        {ok, Shallow} =
            hb_ao:resolve(Sender,
                #{ <<"path">> => <<"push">>, <<"slot">> => 2, <<"max-depth">> => 0 }, Opts),
        ?assertEqual(RID, hb_util:deep_get(<<"1/target">>, Shallow, Opts)),
        ?assertEqual(not_found,
            hb_ao:get(<<"resulted-in">>, hb_ao:get(<<"1">>, Shallow, Opts), not_found, Opts))
    end}.

%%% Round 3: one test per decision.

%% @doc A signing that crashes is retried, never finished: the entry is
%% delivered with a real slot. (Kills the mutant that makes a signing
%% exception a terminal.)
sign_failure_is_retried_test_() ->
    {timeout, 60, fun() ->
        {Sender, SID, {A, AID}, _, Opts} =
            delivery_setup(#{ <<"delivery-test-crash">> => {raise, sign, counters:new(1, [])} }),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        Before = receiver_slot(A, Opts),
        Retries = counter(delivery_sign_retry),
        {ok, _} = compute(Sender, 0, Opts),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        ?assertEqual(Retries + 1, counter(delivery_sign_retry)),
        {ok, Pushed} = hb_ao:resolve(Sender, #{ <<"path">> => <<"push">>, <<"slot">> => 0 }, Opts),
        ?assertEqual(Before + 1, hb_util:int(hb_util:deep_get(<<"1/slot">>, Pushed, Opts))),
        ?assertEqual(not_found, hb_util:deep_get(<<"1/status">>, Pushed, Opts))
    end}.

%% @doc A 422 sends an entry back to be signed again under its target's own
%% policy: here the target's `authority' is an identity of this node, and the
%% second signature is that identity's, as the first was. (Kills the mutant
%% that re-signs with an empty target, i.e. the node's default wallet.)
resign_keeps_target_policy_test_() ->
    {timeout, 60, fun() ->
        Self = self(),
        Calls = counters:new(1, []),
        Post =
            fun(TP, T, L, S, O, Real) ->
                counters:add(Calls, 1, 1),
                N = counters:get(Calls, 1),
                Self ! {signers, N, hb_message:signers(S, O)},
                case N of
                    1 -> {error, #{ <<"status">> => 422 }};
                    _ -> Real(TP, T, L, S, O)
                end
            end,
        Ident = ar_wallet:new(),
        IdentID = hb:address(Ident),
        Opts =
            delivery_opts(#{
                <<"delivery-test-post">> => Post,
                <<"identities">> => #{ IdentID => #{ <<"priv-wallet">> => Ident } }
            }),
        A = hb_message:commit(
                maps:merge(
                    hb_message:uncommitted(
                        hb_process_test_vectors:lua_patch_process(<<"function compute() end">>, Opts),
                        Opts),
                    #{ <<"authority">> => IdentID }),
                Opts),
        {ok, _} = hb_cache:write(A, Opts),
        {ok, _} = hb_ao:resolve(A,
            #{ <<"method">> => <<"POST">>, <<"path">> => <<"schedule">>, <<"body">> => A }, Opts),
        AID = hb_message:id(A, all, Opts),
        Sender = hb_process_test_vectors:lua_patch_process(sender_script(), Opts),
        {ok, _} = hb_cache:write(Sender, Opts),
        SID = hb_message:id(Sender, all, Opts),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        {ok, _} = compute(Sender, 0, Opts),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        Expected = [hb_test_utils:committer(Ident)],
        receive {signers, 1, First} -> ?assertEqual(Expected, First) after 0 -> erlang:error(no_post) end,
        receive {signers, 2, Second} -> ?assertEqual(Expected, Second) after 0 -> erlang:error(no_resign) end
    end}.

%% @doc A target that asked once to be signed differently (a 422: `ans104@1.0')
%% has its later entries signed that way from the start: one 422, and every
%% entry after it posted once.
target_remembers_its_codec_test_() ->
    {timeout, 60, fun() ->
        Self = self(),
        Post =
            fun(TP, T, L, S, O, Real) ->
                Ans104 = lists:member(<<"ans104@1.0">>, hb_message:commitment_devices(S, O)),
                Self ! {post, hb_maps:get(<<"from-slot">>, S, undefined, O), Ans104},
                case Ans104 of
                    false -> {error, #{ <<"status">> => 422 }};
                    true -> Real(TP, T, L, S, O)
                end
            end,
        {Sender, SID, {_A, AID}, _, Opts} =
            delivery_setup(#{ <<"delivery-test-post">> => Post }),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        {ok, _} = compute(Sender, 0, Opts),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        send(Sender, 2, #{ <<"sendto">> => AID }, Opts),
        send(Sender, 3, #{ <<"sendto">> => AID }, Opts),
        {ok, _} = compute(Sender, 2, Opts),
        ?assert(wait_until(fun() -> delivered(SID, [0, 1, 2], Opts) end, 15000)),
        Posts = (fun Gather() -> receive {post, F, C} -> [{F, C} | Gather()] after 0 -> [] end end)(),
        ?assertEqual([{0, false}, {0, true}, {1, true}, {2, true}], lists:sort(Posts))
    end}.

%% @doc Every entry is signed exactly once, also the ones a step signs ahead
%% of their post: ten entries for one target (room for eight in flight), all
%% due at once after a restart. One commit makes all ten durable, every post
%% sends the bytes its `q' row holds, and nothing is signed twice. (Kills the
%% mutant that signs ahead without writing what it signed.)
sign_ahead_is_written_test_() ->
    {timeout, 90, fun() ->
        Self = self(),
        Post =
            fun(TP, T, L, S, O, Real) ->
                From = hb_maps:get(<<"from-slot">>, S, undefined, O),
                ID = hb_message:id(S, all, O),
                SID = hb_maps:get(<<"from-process">>, S, undefined, O),
                Self ! {post, From, ID, queued(SID, From, <<"1">>, O)},
                Real(TP, T, L, S, O)
            end,
        {Sender, SID, {A, AID}, _, Opts} =
            delivery_setup(#{
                <<"delivery-test-post">> => Post,
                <<"delivery-test-crash">> => before_flush
            }),
        [ send(Sender, N, #{ <<"sendto">> => AID }, Opts) || N <- lists:seq(1, 10) ],
        Before = receiver_slot(A, Opts),
        {ok, _} = compute(Sender, 9, Opts),
        ?assert(wait_until(fun() -> rows(SID, <<"o">>, Opts) == lists:seq(0, 9) end, 15000)),
        ?assert(wait_until(fun() -> deliverer(SID) == undefined end, 15000)),
        timer:sleep(500),
        ?assertEqual([], rows(SID, <<"q">>, Opts)),
        {Signed, Commits, Restarts} =
            {counter(delivery_signed), counter(delivery_flush), counter(delivery_restart)},
        {ok, _} = lib_process_delivery:ensure(SID, maps:remove(<<"delivery-test-crash">>, Opts)),
        ?assert(wait_until(fun() -> rows(SID, <<"x">>, Opts) == lists:seq(0, 9) end, 30000)),
        ?assertEqual(Before + 10, receiver_slot(A, Opts)),
        ?assertEqual(Signed + 10, counter(delivery_signed)),
        ?assertEqual(Restarts, counter(delivery_restart)),
        ?assertEqual(Commits + 1, counter(delivery_flush)),
        Posts = (fun Gather() -> receive {post, F, I, Q} -> [{F, I, Q} | Gather()] after 0 -> [] end end)(),
        ?assertEqual(lists:seq(0, 9), lists:sort([ F || {F, _, _} <- Posts ])),
        [ ?assertMatch({_, I}, Q) || {_, I, Q} <- Posts ]
    end}.

%% @doc A non-delta process whose slot fails carries none of the slot before
%% it either: the slot's results are its error, with no outbox, and the
%% earlier message is delivered once. (Kills the mutant that keeps the
%% inbound results on an error slot.)
error_slot_carries_no_outbox_full_state_test_() ->
    {timeout, 90, fun() ->
        Opts = delivery_opts(#{ <<"process-delta-cache">> => false }),
        {A, AID} = spawn_process(<<"function compute() end">>, Opts),
        Sender = hb_process_test_vectors:lua_patch_process(boom_script(), Opts),
        {ok, _} = hb_cache:write(Sender, Opts),
        SID = hb_message:id(Sender, all, Opts),
        send(Sender, 1, #{ <<"sendto">> => AID }, Opts),
        send(Sender, 2, #{ <<"boom">> => <<"yes">> }, Opts),
        Before = receiver_slot(A, Opts),
        {ok, _} = compute(Sender, 1, Opts),
        ?assert(wait_until(fun() -> delivered(SID, [0], Opts) end, 15000)),
        timer:sleep(1000),
        ?assertEqual([0], rows(SID, <<"x">>, Opts)),
        ?assertEqual(Before + 1, receiver_slot(A, Opts)),
        {ok, Results} = lib_process_cache:read_results(SID, 1, Opts),
        ?assertEqual(not_found, hb_maps:get(<<"outbox">>, Results, not_found, Opts))
    end}.

boom_script() ->
    <<
        "Count = Count or 0\n"
        "function compute(req)\n"
        "  local body = req.body or {}\n"
        "  if body.boom then error('boom') end\n"
        "  Count = Count + 1\n"
        "  local res = { output = { data = tostring(Count) } }\n"
        "  if body.sendto then res.outbox = { { target = body.sendto, data = 'ping' } } end\n"
        "  return { patches = { { path = '/count', value = tostring(Count) } }, results = res }\n"
        "end\n"
    >>.

%% @doc A real node says it delivers outboxes, and answers a push over HTTP
%% with the JSON a client judges it by (`parsePushReceipts' in the client):
%% the slot's results with `outbox', a receipt under each outbox key carrying
%% the integer slot its target gave, and `resulted-in' nested for a target
%% this node holds -- the reply's receipt inside it.
real_node_push_receipts_test_() ->
    {timeout, 90, fun() ->
        application:ensure_all_started(inets),
        Opts =
            delivery_opts(#{
                <<"port">> => 10000 + rand:uniform(20000),
                <<"process-workers">> => true,
                <<"process-worker-spawn">> => true
            }),
        Node = hb_http_server:start_node(Opts),
        Get =
            fun(Path, Accept) ->
                {ok, {{_, Status, _}, _, Body}} =
                    httpc:request(get,
                        {hb_util:list(<<Node/binary, Path/binary>>),
                            [{"accept", Accept}, {"accept-bundle", "true"}]},
                        [{timeout, 60000}], [{body_format, binary}]),
                {Status, Body}
            end,
        % As the client asks.
        {200, Advert} = Get(<<"~meta@1.0/info/outbox-delivery">>, "text/plain"),
        ?assertEqual(<<"durable">>, string:trim(Advert)),
        {_Replier, RID} = spawn_process(replier_script(), Opts),
        Sender = hb_process_test_vectors:lua_patch_process(sender_script(), Opts),
        {ok, _} = hb_cache:write(Sender, Opts),
        SID = hb_util:human_id(hb_message:id(Sender, all, Opts)),
        send(Sender, 1, #{ <<"sendto">> => RID }, Opts),
        {200, Text} = Get(<<SID/binary, "~process@1.0/push&slot=0">>, "application/json"),
        io:format(standard_error, "~npush-json: ~s~n", [Text]),
        Body = hb_json:decode(Text),
        Outbox =
            case Body of
                #{ <<"outbox">> := O } -> O;
                #{ <<"results">> := #{ <<"outbox">> := O } } -> O
            end,
        ?assert(is_map(Outbox) andalso map_size(Outbox) > 0),
        lists:foreach(
            fun(Key) ->
                Receipt = maps:get(Key, Body),
                ?assertEqual(RID, maps:get(<<"target">>, Receipt)),
                ?assertMatch({ok, _}, hb_util:safe_int(hb_util:bin(maps:get(<<"slot">>, Receipt)))),
                Reply = maps:get(<<"1">>, maps:get(<<"resulted-in">>, Receipt)),
                ?assertEqual(SID, maps:get(<<"target">>, Reply)),
                ?assertEqual({ok, 1}, hb_util:safe_int(hb_util:bin(maps:get(<<"slot">>, Reply))))
            end,
            maps:keys(Outbox)
        )
    end}.

%%% @doc Publishes a process's schedule to Arweave: every assignment, and the
%%% message it carries, with separate posted and mined-confirmed progress.
%%%
%%% One uploader runs beside each scheduling server whose node publishes
%%% (`scheduler-publish-remote'). The server tells it each slot it confirms;
%%% the uploader reads that slot back from the scheduler's store and posts the
%%% assignment and its body, `?WINDOW' slots to a signed bundle (see
%%% `post_bundle/3' for its shape). Signed windows are flushed before POST.
%%% `posted/<pid>' tracks bundler acceptance; `confirmed/<pid>' tracks only
%%% contiguous windows whose payloads and archive are mined to the configured
%%% depth. Legacy `uploaded/<pid>' is a replay target, never confirmation
%%% evidence. Pending windows are retried byte-for-byte after a timeout.
%%%
%%% Nothing here is on the scheduling path: the server only sends a message,
%%% so a slow or absent bundler slows nobody's assignment. A client asking for
%%% `remote_confirmation' is answered by the uploader, once its slot is up.
-module(dev_scheduler_uploader).
-export([start/2, cursor/2, posted/2]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(WINDOW, 32).
-define(MIN_BACKOFF, 1000).
-define(MAX_BACKOFF, 60000).

%% @doc Start the uploader of `ProcID', linked to the caller.
start(ProcID, Opts) ->
    spawn_link(
        fun() ->
            loop(#{
                id => ProcID,
                opts => Opts,
                cursor => posted(ProcID, Opts),
                confirmed => cursor(ProcID, Opts),
                target => read_cursor(ProcID, <<"uploaded">>, Opts),
                check_at => 0,
                waiting => [],
                backoff => ?MIN_BACKOFF
            })
        end
    ).

%% @doc The highest slot below which (and at which) every slot is confirmed,
%% or -1.
cursor(ProcID, Opts) ->
    read_cursor(ProcID, <<"confirmed">>, Opts).

%% @doc The contiguous range accepted by the bundler, without mining claims.
posted(ProcID, Opts) ->
    read_cursor(ProcID, <<"posted">>, Opts).

read_cursor(ProcID, Kind, Opts) ->
    Store = store(Opts),
    Req = #{ <<"read">> => progress_path(ProcID, Kind), <<"exact">> => true },
    case hb_store:read(Store, Req, Opts) of
        {ok, Slot} -> hb_util:int(Slot);
        _ -> -1
    end.

loop(State0) ->
    State = confirm_window(take(State0, 0)),
    #{cursor := Cursor, target := Target, confirmed := Confirmed} = State,
    case Cursor < Target of
        true -> loop(upload_window(State));
        false ->
            Timeout = case Confirmed < Cursor of true -> 1000; false -> infinity end,
            loop(take(answer(State), Timeout))
    end.

%% @doc Take every slot the server has sent, waiting up to `Timeout' for the
%% first.
take(State = #{ target := Target, waiting := Waiting }, Timeout) ->
    receive
        {durable, Slot, Waiter} ->
            take(
                State#{
                    target => max(Target, Slot),
                    waiting =>
                        case Waiter of
                            none -> Waiting;
                            _ -> [{Slot, Waiter} | Waiting]
                        end
                },
                0
            )
    after Timeout -> State
    end.

%% @doc Post the next `?WINDOW' slots as one bundle, and move the cursor past
%% them. If the bundler does not take it, wait and try again.
upload_window(State = #{ id := ProcID, opts := Opts, cursor := Cursor,
        target := Target, backoff := Backoff }) ->
    Last = min(Target, Cursor + ?WINDOW),
    case post_bundle(ProcID, lists:seq(Cursor + 1, Last), Opts) of
        {ok, PostedLast} ->
            ok = hb_store:write(
                store(Opts),
                #{ progress_path(ProcID, <<"posted">>) => hb_util:bin(PostedLast) },
                Opts
            ),
            ok = hb_store:flush(store(Opts), Opts),
            State#{ cursor => PostedLast, backoff => ?MIN_BACKOFF };
        Error ->
            ?event(warning,
                {schedule_upload_retrying,
                    {proc_id, ProcID},
                    {slot, Cursor + 1},
                    {reason, Error},
                    {backoff_ms, Backoff}
                }
            ),
            timer:sleep(Backoff),
            State#{ backoff => min(?MAX_BACKOFF, Backoff * 2) }
    end.

%% @doc Release clients only through the confirmed contiguous prefix.
answer(State = #{ confirmed := Cursor, waiting := Waiting }) ->
    {Done, Rest} = lists:partition(fun({S, _}) -> S =< Cursor end, Waiting),
    lists:foreach(
        fun({_, {Reply, Message, Assignment}}) ->
            Reply ! {scheduled, Message, Assignment}
        end,
        Done
    ),
    State#{ waiting => Rest }.

%% @doc Sign a bundle and post it: `{ok, LastSlot}', or the reason it was
%% not taken. One request carries the whole window, which keeps a busy node
%% under the bundler's request limit.
%%
%% The bundler parses every item and refuses one that names another message
%% by a link (`ao:<id>') it cannot resolve. Every assignment does (its
%% `body'), and so does a process naming its `module'. So an item goes in
%% under its own ID, indexed by the gateway, only if it carries no link; the
%% rest go in, exact signed bytes, as one archive item signed by the node: an
%% ANS-104 bundle of them, untagged as a bundle so the bundler leaves it
%% whole, found by its `Process', `From-Slot' and `To-Slot' tags. Every
%% message the window links to that this node holds (a process's module, say)
%% goes up with it, so the schedule can be rebuilt from Arweave alone.
post_bundle(ProcID, Slots = [First | _], Opts) ->
    try
        Path = window_path(ProcID, First),
        {Last, Serialized} =
            case hb_store:read(store(Opts),
                    #{ <<"read">> => Path, <<"exact">> => true }, Opts) of
                {ok, Saved} -> binary_to_term(Saved, [safe]);
                {error, not_found} ->
                    Prepared = prepare_bundle(ProcID, Slots, Opts),
                    ok = hb_store:write(store(Opts),
                        #{ Path => term_to_binary(Prepared) }, Opts),
                    ok = hb_store:flush(store(Opts), Opts),
                    Prepared;
                ReadError -> throw({upload_window_read_failed, ReadError})
            end,
        case post(Serialized, Opts) of
            ok ->
                ok = mark_posted(ProcID, First, Opts),
                {ok, Last};
            Error -> Error
        end
    catch Class:Reason -> {error, {Class, Reason}}
    end.

%% @doc Freeze the signed bytes and slot range before the first POST. Retries
%% reuse this record even when the scheduling head advances or the node restarts.
prepare_bundle(ProcID, Slots = [First | _], Opts) ->
    try
        Read = [ read_slot(ProcID, Slot, Opts) || Slot <- Slots ],
        Window =
            [ item(Body, Opts) || {Body, _} <- Read ] ++
            [ item(Assignment, Opts) || {_, Assignment} <- Read ],
        Items = Window ++ linked(Window, Opts),
        {Linking, Plain} = lists:partition(fun(I) -> links(I) =/= [] end, Items),
        Archive = sign(
            bundle_data(Linking),
            [
                {<<"Data-Protocol">>, <<"ao-schedule-archive">>},
                {<<"Content-Type">>, <<"application/octet-stream">>},
                {<<"Process">>, hb_util:human_id(ProcID)},
                {<<"From-Slot">>, hb_util:bin(First)},
                {<<"To-Slot">>, hb_util:bin(lists:last(Slots))}
            ],
            Opts
        ),
        Bundle = sign(
            bundle_data(Plain ++ [Archive]),
            [
                {<<"Bundle-Format">>, <<"binary">>},
                {<<"Bundle-Version">>, <<"2.0.0">>}
            ],
            Opts
        ),
        {lists:last(Slots), ar_bundles:serialize(Bundle)}
    catch Class:Reason -> erlang:raise(Class, Reason, [])
    end.

%% @doc The IDs an item names by link.
links(#tx{ tags = Tags }) ->
    [ ID || {_, <<"ao:", ID/binary>>} <- Tags, byte_size(ID) == 43 ].

%% @doc Every message the items link to, and those link to, that is not among
%% them and that this node holds.
linked(Items, Opts) ->
    Have = lists:flatmap(fun held/1, Items),
    linked(lists:usort(lists:flatmap(fun links/1, Items)) -- Have, Have, Opts).

%% @doc The IDs an item stands for: its own, and the message it carries if the
%% node wrapped one. A process signed with `httpsig@1.0' rides in a wrapper with
%% an ID of its own, and its assignment links to it by the message's ID: without
%% the second, the process went up twice in every first window.
held(I = #tx{ tags = Tags }) ->
    [hb_util:human_id(I#tx.id) | [ ID || {<<"Message-Id">>, ID} <- Tags ]].
linked([], _Have, _Opts) -> [];
linked([ID | IDs], Have, Opts) ->
    case lists:member(ID, Have) of
        true -> linked(IDs, Have, Opts);
        false ->
            case hb_cache:read(ID, Opts) of
                {ok, Msg} ->
                    Item = item(hb_cache:ensure_all_loaded(Msg, Opts), Opts),
                    [Item | linked(IDs ++ links(Item), [ID | Have], Opts)];
                _ -> linked(IDs, [ID | Have], Opts)
            end
    end.

read_slot(ProcID, Slot, Opts) ->
    case dev_scheduler_cache:read(ProcID, Slot, Opts) of
        {ok, Assignment} ->
            {hb_maps:get(<<"body">>, Assignment, #{}, Opts), Assignment};
        not_found -> throw({assignment_not_found, Slot})
    end.

bundle_data(Items) ->
    {undefined, Data} = ar_bundles:serialize_bundle(list, Items, false),
    Data.

sign(Data, Tags, Opts) ->
    ar_bundles:sign_item(
        #tx{ format = ans104, data = Data, data_size = byte_size(Data), tags = Tags },
        hb_opts:get(priv_wallet, hb:wallet(), Opts)
    ).

%% @doc A message as an Arweave item. A message signed as an ANS-104 item is
%% that item, its own ID and signature: only that commitment is taken, since a
%% message read back from the store also carries an unsigned `httpsig@1.0'
%% one. Any other message (signed with `httpsig@1.0', as a process's pushes and
%% the deploy tooling's writes are) cannot be an item of its own, so the node
%% signs an item that carries it whole, commitments and all, as JSON: tagged
%% with its ID, and verifiable by anyone who decodes it.
item(Msg, Opts) ->
    Device = <<"ans104@1.0">>,
    case lists:member(Device, hb_message:commitment_devices(Msg, Opts)) of
        true ->
            hb_message:convert(
                hb_message:with_commitments(
                    #{ <<"commitment-device">> => Device }, Msg, Opts),
                Device,
                <<"structured@1.0">>,
                Opts
            );
        false ->
            sign(
                hb_message:convert(Msg, <<"json@1.0">>, <<"structured@1.0">>, Opts),
                [
                    {<<"Content-Type">>, <<"application/json">>},
                    {<<"Message-Id">>, hb_message:id(Msg, signed, Opts)}
                ],
                Opts
            )
    end.

post(Serialized, Opts) ->
    URL = upload_url(Opts),
    case httpc:request(
            post,
            {URL, [], "application/octet-stream", Serialized},
            [{timeout, 60000}],
            [{body_format, binary}]) of
        {ok, {{_, Status, _}, _, _}} when Status >= 200, Status < 300 -> ok;
        {ok, {{_, Status, _}, Headers, Body}} ->
            {error,
                {Status,
                    {url, hb_util:bin(URL)},
                    {bytes, byte_size(Serialized)},
                    {headers,
                        [ {hb_util:bin(K), hb_util:bin(V)}
                        || {K, V} <- Headers, logged_header(K) ]},
                    {body, binary:part(Body, 0, min(1024, byte_size(Body)))}
                }
            };
        Error -> Error
    end.

%% @doc Where a window is posted: `scheduler-upload-url' whole, or the
%% `bundler-ans104' node's `/tx'. A Turbo-style bundler takes items at
%% `/v1/tx', which no `bundler-ans104' base can name.
upload_url(Opts) ->
    case hb_opts:get(scheduler_upload_url, not_found, Opts) of
        not_found ->
            hb_util:list(hb_opts:get(bundler_ans104, no_bundler, Opts)) ++ "/tx";
        URL -> hb_util:list(URL)
    end.

%% @doc The response headers that say why a bundler refused a window.
logged_header(Name) ->
    Lower = string:lowercase(Name),
    lists:member(Lower, ["retry-after", "content-type", "reason", "status"])
        orelse lists:prefix("x-", Lower).

store(Opts) ->
    hb_opts:get(scheduler_store, hb_opts:get(store, no_viable_store, Opts), Opts).

progress_path(ProcID, Kind) ->
    hb_path:to_binary(
        [<<"~scheduler@1.0">>, Kind, hb_util:human_id(ProcID)]).

window_path(ProcID, First) ->
    hb_path:to_binary([<<"~scheduler@1.0">>, <<"upload-windows">>,
        hb_util:human_id(ProcID), hb_util:bin(First)]).

mark_posted(ProcID, First, Opts) ->
    ok = hb_store:write(store(Opts),
        #{ posted_at_path(ProcID, First) =>
            integer_to_binary(erlang:system_time(second)) }, Opts),
    hb_store:flush(store(Opts), Opts).

posted_at_path(ProcID, First) ->
    hb_path:to_binary([<<"~scheduler@1.0">>, <<"upload-posted-at">>,
        hb_util:human_id(ProcID), hb_util:bin(First)]).

%% @doc Confirm only the next contiguous window. Later windows continue posting
%% while mining is pending. Gateway failures leave the confirmed cursor intact.
confirm_window(State = #{confirmed := C, cursor := P}) when C >= P -> State;
confirm_window(State = #{check_at := At}) ->
    case erlang:system_time(millisecond) >= At of
        false -> State;
        true -> check_window(State)
    end.

check_window(State = #{id := ProcID, opts := Opts, confirmed := C}) ->
    Next = State#{check_at => erlang:system_time(millisecond) +
        hb_opts:get(scheduler_upload_poll_ms, 30000, Opts)},
    try
        Path = window_path(ProcID, C + 1),
        {ok, Saved} = hb_store:read(store(Opts),
            #{<<"read">> => Path, <<"exact">> => true}, Opts),
        {Last, Bytes} = binary_to_term(Saved, [safe]),
        Bundle = ar_bundles:deserialize_item_wrapper(Bytes),
        IDs = lists:usort([hb_util:human_id(I#tx.id) || I <- items(Bundle#tx.data)]),
        case mined(IDs, Opts) of
            true ->
                ok = hb_store:write(store(Opts),
                    #{progress_path(ProcID, <<"confirmed">>) => hb_util:bin(Last)}, Opts),
                ok = hb_store:flush(store(Opts), Opts),
                answer(Next#{confirmed => Last, check_at => 0});
            false ->
                retry_pending(ProcID, C + 1, Bytes, Opts),
                Next
        end
    catch Class:Reason ->
        ?event(warning, {schedule_confirmation_retrying, ProcID, Class, Reason}),
        Next
    end.

%% @doc Repost the identical persisted bundle after the pending timeout.
retry_pending(ProcID, First, Bytes, Opts) ->
    Path = posted_at_path(ProcID, First),
    PostedAt = case hb_store:read(store(Opts),
            #{<<"read">> => Path, <<"exact">> => true}, Opts) of
        {ok, Time} -> hb_util:int(Time);
        {error, not_found} -> 0
    end,
    case erlang:system_time(second) - PostedAt >=
            hb_opts:get(scheduler_upload_retry_seconds, 1800, Opts) of
        true ->
            case post(Bytes, Opts) of
                ok -> mark_posted(ProcID, First, Opts);
                Error -> ?event(warning, {schedule_repost_failed, ProcID, First, Error})
            end;
        false -> ok
    end.

%% @doc Require mined inclusion of every top-level item, including the archive
%% and separately published payloads. Inclusion is confirmation one.
mined(IDs, Opts) ->
    Gateway = hb_util:list(hb_opts:get(scheduler_upload_gateway,
        <<"https://arweave.net">>, Opts)),
    {ok, {{_, 200, _}, _, HeightBody}} = httpc:request(get,
        {Gateway ++ "/height", []}, [{timeout, 15000}], [{body_format, binary}]),
    Height = binary_to_integer(string:trim(HeightBody)),
    Depth = hb_opts:get(scheduler_upload_confirmations, 3, Opts),
    true = is_integer(Depth) andalso Depth >= 1,
    mined_batches(IDs, Height, Depth, Gateway).

mined_batches([], _Height, _Depth, _Gateway) -> true;
mined_batches(IDs, Height, Depth, Gateway) ->
    {Batch, Rest} = lists:split(min(100, length(IDs)), IDs),
    Query = <<"query($ids:[ID!]){transactions(ids:$ids,first:100){edges{node{id block{height}}}}}">>,
    Body = hb_json:encode(#{<<"query">> => Query,
        <<"variables">> => #{<<"ids">> => Batch}}),
    {ok, {{_, 200, _}, _, Response}} = httpc:request(post,
        {Gateway ++ "/graphql", [], "application/json", Body},
        [{timeout, 15000}], [{body_format, binary}]),
    #{<<"data">> := #{<<"transactions">> := #{<<"edges">> := Edges}}} =
        hb_json:decode(Response),
    window_confirmed(Batch, Edges, Height, Depth)
        andalso mined_batches(Rest, Height, Depth, Gateway).

window_confirmed(IDs, Edges, Height, Depth) ->
    Found = [ID || #{<<"node">> := #{<<"id">> := ID,
        <<"block">> := #{<<"height">> := H}}} <- Edges,
        is_integer(H), H >= 0, Height - H + 1 >= Depth],
    lists:all(fun(ID) -> lists:member(ID, Found) end, IDs).

%%% Tests

confirmation_coverage_test() ->
    Edge = fun(ID, Block) ->
        #{<<"node">> => #{<<"id">> => ID, <<"block">> => Block}}
    end,
    A = Edge(<<"archive">>, #{<<"height">> => 98}),
    B = Edge(<<"body">>, #{<<"height">> => 98}),
    IDs = [<<"archive">>, <<"body">>],
    ?assert(window_confirmed(IDs, [A, B], 100, 3)),
    ?assertNot(window_confirmed(IDs, [A, B], 99, 3)),
    ?assertNot(window_confirmed(IDs, [A], 100, 3)),
    ?assertNot(window_confirmed(IDs, [A, A], 100, 3)),
    ?assertNot(window_confirmed(IDs, [A, Edge(<<"body">>, null)], 100, 3)),
    ?assertNot(window_confirmed(IDs,
        [A, Edge(<<"body">>, #{<<"height">> => 101})], 100, 3)).

%% @doc With no bundler the cursor stays where it is, and a bundler that
%% refuses (429) is asked again. Once one takes it, the window goes up as one
%% signed bundle: each slot's message under its own ID, in slot order, then an
%% archive of the assignments; the cursor reaches the last slot and the
%% waiting client is answered.
retries_until_bundler_answers_test_() ->
    {timeout, 120, fun retries_until_bundler_answers/0}.
retries_until_bundler_answers() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listen),
    ok = gen_tcp:close(Listen),
    Self = self(),
    {Gateway, GatewayPID} = stub_gateway(Self),
    Opts = #{
        <<"store">> => [hb_test_utils:test_store()],
        <<"priv-wallet">> => hb:wallet(),
        <<"scheduler-upload-gateway">> => Gateway,
        <<"scheduler-upload-poll-ms">> => 10,
        <<"scheduler-upload-retry-seconds">> => 0,
        <<"bundler-ans104">> =>
            <<"http://localhost:", (integer_to_binary(Port))/binary>>
    },
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Slots = lists:seq(0, 4),
    IDs =
        lists:flatmap(
            fun(Slot) ->
                Body = hb_message:commit(
                    #{ <<"data">> => integer_to_binary(Slot) }, Opts, <<"ans104@1.0">>),
                Assignment = hb_message:commit(
                    #{
                        <<"process">> => ProcID,
                        <<"slot">> => Slot,
                        <<"body">> => Body,
                        <<"type">> => <<"Assignment">>,
                        <<"variant">> => <<"ao.N.1">>
                    },
                    Opts,
                    <<"ans104@1.0">>
                ),
                ok = dev_scheduler_cache:write(ProcID, Assignment, Opts),
                [{hb_message:id(Body, signed, Opts),
                    hb_message:id(Assignment, signed, Opts)}]
            end,
            Slots
        ),
    ok = dev_scheduler_cache:flush(Opts),
    Uploader = start(ProcID, Opts),
    [ Uploader ! {durable, S, none} || S <- Slots ],
    Uploader ! {durable, 4, {Self, msg, assignment}},
    timer:sleep(1500),
    ?assertEqual(-1, cursor(ProcID, Opts)),
    BundlerPID = stub_bundler(Port, [429, 200], Self),
    Posted = receive {posted, 200, Bin} -> Bin after 90000 -> error(no_post) end,
    receive {posted, 429, Retried} -> ?assertEqual(Posted, Retried)
    after 0 -> error(no_refusal) end,
    Bundle = ar_bundles:deserialize_item_wrapper(Posted),
    ?assert(ar_bundles:verify_item(Bundle)),
    {MessageIDs, AssignmentIDs} = lists:unzip(IDs),
    [Archive | Messages] = lists:reverse(items(Bundle#tx.data)),
    ?assertEqual(MessageIDs, [ hb_util:human_id(M#tx.id) || M <- lists:reverse(Messages) ]),
    ?assert(ar_bundles:verify_item(Archive)),
    ?assertEqual(<<"0">>, proplists:get_value(<<"From-Slot">>, Archive#tx.tags)),
    ?assertEqual(<<"4">>, proplists:get_value(<<"To-Slot">>, Archive#tx.tags)),
    Archived = items(Archive#tx.data),
    ?assertEqual(AssignmentIDs, [ hb_util:human_id(A#tx.id) || A <- Archived ]),
    ?assert(lists:all(fun ar_bundles:verify_item/1, Archived)),
    gateway_reply(<<"100">>),
    PendingEdges = [#{<<"node">> => #{<<"id">> => hb_util:human_id(I#tx.id),
        <<"block">> => null}} || I <- items(Bundle#tx.data)],
    gateway_reply(hb_json:encode(#{<<"data">> => #{<<"transactions">> =>
        #{<<"edges">> => PendingEdges}}})),
    receive {posted, 200, Reposted} -> ?assertEqual(Posted, Reposted)
    after 10000 -> error(no_pending_repost) end,
    ?assertEqual(-1, cursor(ProcID, Opts)),
    ?assertEqual(4, posted(ProcID, Opts)),
    receive {scheduled, msg, assignment} -> error(premature_confirmation)
    after 0 -> ok end,
    unlink(Uploader),
    exit(Uploader, kill),
    Restarted = start(ProcID, Opts),
    Restarted ! {durable, 4, {Self, msg, assignment}},
    gateway_reply(<<"100">>),
    Edges = [#{<<"node">> => #{<<"id">> => hb_util:human_id(I#tx.id),
        <<"block">> => #{<<"height">> => 98}}} || I <- items(Bundle#tx.data)],
    gateway_reply(hb_json:encode(#{<<"data">> => #{<<"transactions">> =>
        #{<<"edges">> => Edges}}})),
    receive {scheduled, msg, assignment} -> ok after 10000 -> error(no_answer) end,
    ?assertEqual(4, cursor(ProcID, Opts)),
    unlink(Restarted),
    exit(Restarted, kill),
    unlink(GatewayPID),
    exit(GatewayPID, kill),
    unlink(BundlerPID),
    exit(BundlerPID, kill).

%% @doc Controlled mining evidence without waiting for paid L1 transactions.
stub_gateway(Test) ->
    {ok, Listen} = gen_tcp:listen(0,
        [binary, {active, false}, {reuseaddr, true}, {packet, http_bin}]),
    {ok, Port} = inet:port(Listen),
    Pid = spawn_link(fun() -> gateway_accept(Listen, Test) end),
    {<<"http://localhost:", (integer_to_binary(Port))/binary>>, Pid}.

gateway_accept(Listen, Test) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    gateway_headers(Socket, 0),
    Test ! {gateway_request, self()},
    receive {gateway_response, Body} ->
        gen_tcp:send(Socket, [<<"HTTP/1.1 200 OK\r\ncontent-length: ">>,
            integer_to_binary(byte_size(Body)),
            <<"\r\nconnection: close\r\n\r\n">>, Body])
    after 10000 -> error(no_gateway_response)
    end,
    gen_tcp:close(Socket),
    gateway_accept(Listen, Test).

gateway_headers(Socket, Length) ->
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, {http_header, _, 'Content-Length', _, Value}} ->
            gateway_headers(Socket, binary_to_integer(Value));
        {ok, http_eoh} ->
            ok = inet:setopts(Socket, [{packet, raw}]),
            case Length of
                0 -> ok;
                _ -> {ok, _} = gen_tcp:recv(Socket, Length, 5000), ok
            end;
        {ok, _} -> gateway_headers(Socket, Length)
    end.

gateway_reply(Body) ->
    receive {gateway_request, Pid} -> Pid ! {gateway_response, Body}
    after 10000 -> error(no_gateway_request)
    end.

%% @doc The items of an ANS-104 bundle, in order.
items(<<Count:256/little, Rest/binary>>) ->
    <<Index:(Count * 64)/binary, Data/binary>> = Rest,
    {Items, <<>>} =
        lists:foldl(
            fun(Size, {Acc, Bin}) ->
                <<Item:Size/binary, Next/binary>> = Bin,
                {[ar_bundles:deserialize_item_wrapper(Item) | Acc], Next}
            end,
            {[], Data},
            [ Size || <<Size:256/little, _:32/binary>> <= Index ]
        ),
    lists:reverse(Items).

%% @doc A bundler that answers each request with the next status in
%% `Statuses' (the last one from then on) and sends what it was posted to
%% `Test'.
stub_bundler(Port, Statuses, Test) ->
    {ok, Listen} = gen_tcp:listen(Port,
        [binary, {active, false}, {reuseaddr, true}, {packet, http_bin}]),
    spawn_link(fun() -> stub_accept(Listen, Statuses, Test) end).
stub_accept(Listen, Statuses, Test) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    ok = gen_tcp:controlling_process(Socket, self()),
    stub_accept(Listen, stub_serve(Socket, Statuses, Test, 0), Test).
stub_serve(Socket, Statuses, Test, Length) ->
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, {http_header, _, 'Content-Length', _, Value}} ->
            stub_serve(Socket, Statuses, Test, binary_to_integer(Value));
        {ok, http_eoh} ->
            ok = inet:setopts(Socket, [{packet, raw}]),
            {ok, Body} = gen_tcp:recv(Socket, Length, 5000),
            [Status | Rest] = Statuses,
            Test ! {posted, Status, Body},
            gen_tcp:send(Socket, [<<"HTTP/1.1 ">>, integer_to_binary(Status),
                <<" X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n">>]),
            gen_tcp:close(Socket),
            case Rest of [] -> Statuses; _ -> Rest end;
        {ok, _} -> stub_serve(Socket, Statuses, Test, Length);
        {error, _} -> Statuses
    end.

%% @doc What the uploader reads back is the message that was signed: the same
%% ID, so the item on Arweave is the one the client sent.
read_back_keeps_id_test() ->
    Opts = #{ <<"store">> => [hb_test_utils:test_store()], <<"priv-wallet">> => hb:wallet() },
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Body = hb_message:commit(
        #{ <<"data">> => <<"x">>, <<"action">> => <<"Probe">> }, Opts, <<"ans104@1.0">>),
    Assignment = hb_message:commit(
        #{ <<"process">> => ProcID, <<"slot">> => 0, <<"body">> => Body,
           <<"variant">> => <<"ao.N.1">> },
        Opts, <<"ans104@1.0">>),
    ok = dev_scheduler_cache:write(ProcID, Assignment, Opts),
    {ok, Read} = dev_scheduler_cache:read(ProcID, 0, Opts),
    ?assertEqual(hb_message:id(Body, signed, Opts),
        hb_message:id(hb_maps:get(<<"body">>, Read, #{}, Opts), signed, Opts)),
    ?assertEqual(hb_message:id(Assignment, signed, Opts),
        hb_message:id(Read, signed, Opts)).

%% @doc An ANS-104 message is its own item, ID unchanged. An `httpsig@1.0'
%% message rides in a node-signed item, tagged with its ID, and decodes back to
%% a message whose signature still verifies.
item_test() ->
    Opts = #{ <<"priv-wallet">> => hb:wallet() },
    Ans = hb_message:commit(#{ <<"data">> => <<"a">> }, Opts, <<"ans104@1.0">>),
    AnsItem = item(Ans, Opts),
    ?assert(ar_bundles:verify_item(AnsItem)),
    ?assertEqual(hb_message:id(Ans, signed, Opts),
        hb_util:human_id(AnsItem#tx.id)),
    Sig = hb_message:commit(#{ <<"action">> => <<"Push">>, <<"n">> => 1 },
        Opts#{ <<"priv-wallet">> => ar_wallet:new() }, <<"httpsig@1.0">>),
    Wrapped = item(Sig, Opts),
    ?assert(ar_bundles:verify_item(Wrapped)),
    ?assertEqual(hb_message:id(Sig, signed, Opts),
        proplists:get_value(<<"Message-Id">>, Wrapped#tx.tags)),
    Back = hb_message:convert(Wrapped#tx.data, <<"structured@1.0">>,
        <<"json@1.0">>, Opts),
    ?assert(hb_message:verify(Back, all, Opts)),
    ?assertEqual(hb_message:id(Sig, signed, Opts),
        hb_message:id(Back, signed, Opts)).

%% @doc An item naming a message by link brings that message along, under its
%% own ID, and whatever it links to; a link this node cannot resolve is
%% passed over.
linked_test() ->
    Opts = #{ <<"store">> => [hb_test_utils:test_store()], <<"priv-wallet">> => hb:wallet() },
    Module = hb_message:commit(
        #{ <<"data">> => <<"return 1">>, <<"content-type">> => <<"text/x-lua">> },
        Opts, <<"ans104@1.0">>),
    {ok, _} = hb_cache:write(Module, Opts),
    ModID = hb_message:id(Module, signed, Opts),
    Missing = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Spawn = sign(<<>>, [{<<"module">>, <<"ao:", ModID/binary>>},
        {<<"other">>, <<"ao:", Missing/binary>>}], Opts),
    ?assertEqual([ModID, Missing], links(Spawn)),
    [Item] = linked([Spawn], Opts),
    ?assertEqual(ModID, hb_util:human_id(Item#tx.id)),
    ?assertEqual([], links(Item)).

%% @doc A message the window already carries in a node-signed wrapper is not
%% brought along a second time for the link its assignment makes to it.
linked_wrapped_test() ->
    Opts = #{ <<"store">> => [hb_test_utils:test_store()], <<"priv-wallet">> => hb:wallet() },
    Proc = hb_message:commit(
        #{ <<"type">> => <<"Process">>, <<"data">> => <<"module source">> },
        Opts#{ <<"priv-wallet">> => ar_wallet:new() }, <<"httpsig@1.0">>),
    {ok, _} = hb_cache:write(Proc, Opts),
    ProcID = hb_message:id(Proc, signed, Opts),
    Wrapped = item(Proc, Opts),
    Assignment = sign(<<>>, [{<<"body">>, <<"ao:", ProcID/binary>>}], Opts),
    ?assertEqual([], linked([Wrapped, Assignment], Opts)).

%% @doc A refusal names the bundler's reason: its status, why-headers and body.
refusal_is_logged_whole_test_() ->
    {timeout, 30, fun refusal_is_logged_whole/0}.
refusal_is_logged_whole() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listen),
    ok = gen_tcp:close(Listen),
    Base = <<"http://localhost:", (integer_to_binary(Port))/binary>>,
    Stub = stub_bundler(Port, [402], self()),
    Opts = #{ <<"scheduler-upload-url">> => <<Base/binary, "/v1/tx">> },
    {error, {402, {url, URL}, {bytes, 3}, {headers, Headers}, {body, <<>>}}} =
        post(<<"abc">>, Opts),
    ?assertEqual(<<Base/binary, "/v1/tx">>, URL),
    ?assertEqual([], Headers),
    ?assert(logged_header("x-free-tier-exhausted")),
    ?assert(logged_header("Retry-After")),
    ?assertNot(logged_header("date")),
    receive {posted, 402, <<"abc">>} -> ok after 5000 -> error(no_post) end,
    unlink(Stub),
    exit(Stub, kill).

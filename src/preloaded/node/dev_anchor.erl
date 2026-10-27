%%% @doc `anchor@1.0': posts the schedule of chosen processes to Arweave, in
%%% the background, through an ANS-104 bundler (Turbo), so anyone can audit
%%% it and an attestor can tell when a scheduler serves a different slot-N
%%% assignment from the one it anchored.
%%%
%%% One shipper per process, started by the node's `start' hook, never by
%%% the scheduler: the scheduling path is untouched and waits on nothing
%%% here. The outbox is the scheduler's own store, which holds every
%%% assignment (and the message it carries) durably before its slot is
%%% confirmed, and which store retention never deletes. The shipper keeps
%%% only a cursor, the next slot to anchor, and walks forward from it:
%%%
%%% <pre>
%%%   read slot N from the scheduler store
%%%     -> sign an ANS-104 item with the node wallet; data: the assignment,
%%%        its body (the message) inline, both with their commitments, as
%%%        `json@1.0'
%%%     -> pending/N (synced), POST <bundler>/tx
%%%     -> index record N (synced), delete pending/N
%%% </pre>
%%%
%%% Exactly once. An RSA-PSS signature is randomised, so signing slot N twice
%%% would make two different items. The signed bytes are therefore written
%%% to `pending/N' and synced before the first upload, and every retry,
%%% including one after a crash, re-sends those same bytes: the bundler
%%% answers a repeated item ID with 202 and takes nothing. A slot is done
%%% when its index record is synced; a pending file at or below the cursor
%%% is deleted on start.
%%%
%%% Up to `window' slots are uploaded at once; the index advances only over
%%% the contiguous run of accepted slots, so it never has a hole. A failed
%%% round backs off, doubling from `interval' to `max-backoff'; a bundler
%%% that is down only grows the backlog.
%%%
%%% The index is `<dir>/<process>/index', one fixed 104-byte record per
%%% slot at offset `slot * 104': the slot, the anchored item's ID, the
%%% assignment's signed ID and its message's signed ID. `anchored/3' reads
%%% one record (O(1)), with or without a running shipper. Turbo's receipts
%%% (signed by Turbo) are appended to `<dir>/<process>/receipts.jsonl'.
%%%
%%% Each item is tagged `Process', `Slot', `Assignment' and `Message', so
%%% the anchored schedule can also be found on Arweave by tag and owner.
%%%
%%% Configuration, under the node option `anchor':
%%% <ul>
%%%   <li>`processes': the process IDs to anchor. Required. A node
%%%       allowlist: anchoring is paid and signed by this node.</li>
%%%   <li>`bundler' (`https://upload.ardrive.io'): items go to
%%%       `<bundler>/tx'.</li>
%%%   <li>`dir' (`anchor'): index, receipts and pending items.</li>
%%%   <li>`interval' (1000 ms), `max-backoff' (300000 ms), `window' (8),
%%%       `timeout' (30000 ms per upload).</li>
%%% </ul>
%%% Endpoints, read-only:
%%%   GET /~anchor@1.0/status                       per process: anchored,
%%%                                                 head, backlog, errors
%%%   GET /~anchor@1.0/lookup?process=P&slot=N      the anchored item of N
-module(dev_anchor).
-implements(<<"anchor@1.0">>).
-export([device_info/1, start/3, status/3, lookup/3]).
-export([anchored/3, item/4, status/1, ship_now/2, stop/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(REC, 104).
-define(BUNDLER, <<"https://upload.ardrive.io">>).
-define(DIR, <<"anchor">>).
-define(INTERVAL, 1000).
-define(MAX_BACKOFF, 300000).
-define(WINDOW, 8).
-define(TIMEOUT, 30000).

device_info(_) ->
    #{ exports => [<<"start">>, <<"status">>, <<"lookup">>] }.

%%% Endpoints

%% @doc The node `start' hook: start a shipper for each configured process
%% that has none, and hand the node message back. Configuration comes only
%% from the node message, never from the request. Never fails: anchoring
%% must not stop the node from booting.
start(_Base, Req, Opts) ->
    NodeMsg = hb_maps:get(<<"body">>, Req, Opts, Opts),
    LoopOpts = maps:remove(<<"hashpath">>, Opts),
    try
        lists:foreach(
            fun(ProcID) -> ensure_shipper(ProcID, LoopOpts) end,
            processes(LoopOpts)
        )
    catch C:R -> ?event(error, {anchor_start_failed, C, R})
    end,
    {ok, #{ <<"body">> => NodeMsg }}.

%% @doc Every configured process's anchoring status.
status(_Base, _Req, Opts) ->
    {ok, json(status(Opts))}.

%% @doc The anchored item of `process' at `slot'.
lookup(_Base, Req, Opts) ->
    ProcID = hb_ao:get(<<"process">>, Req, undefined, Opts),
    Slot = hb_util:int(hb_ao:get(<<"slot">>, Req, <<"-1">>, Opts)),
    case anchored(ProcID, Slot, Opts) of
        {ok, Rec} -> {ok, json(Rec)};
        not_found -> {error, #{ <<"status">> => 404, <<"body">> => <<"not anchored">> }}
    end.

%%% API

%% @doc The anchored item for `ProcID' at `Slot': its ID, and the signed IDs
%% of the assignment and message it carries, or `not_found' when the slot
%% has not been anchored yet (or the process is not anchored here).
anchored(ProcID, Slot, Opts) when is_binary(ProcID), is_integer(Slot), Slot >= 0 ->
    case file:open(index_path(ProcID, Opts), [read, raw, binary]) of
        {ok, Fd} ->
            try file:pread(Fd, Slot * ?REC, ?REC) of
                {ok, <<Slot:64, Item:32/binary, Assign:32/binary, Msg:32/binary>>} ->
                    {ok, #{
                        <<"process">> => ProcID,
                        <<"slot">> => Slot,
                        <<"id">> => hb_util:human_id(Item),
                        <<"assignment">> => hb_util:human_id(Assign),
                        <<"message">> => hb_util:human_id(Msg)
                    }};
                _ -> not_found
            after file:close(Fd)
            end;
        {error, _} -> not_found
    end;
anchored(_, _, _) -> not_found.

%% @doc Every configured process's status: the last anchored slot, the
%% scheduler's head (when it runs here), the backlog between them, pending
%% items and the last error.
status(Opts) ->
    maps:from_list(
        [ {ProcID, call(ProcID, status, Opts)} || ProcID <- processes(Opts) ]
    ).

%% @doc Run one shipping round for `ProcID' now and return its status.
ship_now(ProcID, Opts) -> call(ProcID, ship_now, Opts).

%% @doc Stop the shipper of `ProcID'.
stop(ProcID) ->
    case hb_name:lookup(name(ProcID)) of
        Pid when is_pid(Pid) ->
            Ref = erlang:monitor(process, Pid),
            Pid ! stop,
            receive {'DOWN', Ref, process, Pid, _} -> ok after 5000 -> ok end;
        _ -> ok
    end.

%% @doc The anchor item for one assignment of `ProcID', signed by `Wallet':
%% `{ItemID, AssignmentID, MessageID, Serialized}', IDs native. The data is
%% the assignment as `json@1.0', its body inline, with every commitment.
item(ProcID, Assignment, Wallet, Opts) ->
    Slot = hb_util:int(hb_maps:get(<<"slot">>, Assignment, undefined, Opts)),
    Body = hb_maps:get(<<"body">>, Assignment, #{}, Opts),
    AssignID = hb_util:native_id(hb_message:id(Assignment, signed, Opts)),
    MsgID = hb_util:native_id(hb_message:id(Body, signed, Opts)),
    JSON =
        hb_message:convert(
            Assignment,
            #{ <<"device">> => <<"json@1.0">>, <<"bundle">> => true },
            Opts
        ),
    Tags = [
        {<<"Data-Protocol">>, <<"ao">>},
        {<<"Type">>, <<"Schedule-Anchor">>},
        {<<"Variant">>, <<"anchor@1.0">>},
        {<<"Content-Type">>, <<"application/json">>},
        {<<"Codec">>, <<"json@1.0">>},
        {<<"Process">>, hb_util:human_id(ProcID)},
        {<<"Slot">>, integer_to_binary(Slot)},
        {<<"Assignment">>, hb_util:human_id(AssignID)},
        {<<"Message">>, hb_util:human_id(MsgID)}
    ],
    Signed =
        ar_bundles:sign_item(
            #tx{ format = ans104, tags = Tags, data = JSON,
                data_size = byte_size(JSON) },
            Wallet
        ),
    {Signed#tx.id, AssignID, MsgID, ar_bundles:serialize(Signed)}.

%%% The shipper

config(Opts) ->
    case hb_opts:get(anchor, #{}, Opts) of
        Cfg when is_map(Cfg) -> Cfg;
        _ -> #{}
    end.

cfg(Key, Default, Opts) -> hb_maps:get(Key, config(Opts), Default, Opts).

processes(Opts) ->
    case cfg(<<"processes">>, [], Opts) of
        P when is_binary(P) -> [hb_util:human_id(P)];
        P when is_list(P) -> [ hb_util:human_id(ID) || ID <- P ];
        _ -> []
    end.

name(ProcID) -> {<<"anchor@1.0">>, ProcID}.

proc_dir(ProcID, Opts) ->
    <<(hb_util:bin(cfg(<<"dir">>, ?DIR, Opts)))/binary, "/", ProcID/binary>>.

index_path(ProcID, Opts) -> <<(proc_dir(ProcID, Opts))/binary, "/index">>.
pending_dir(ProcID, Opts) -> <<(proc_dir(ProcID, Opts))/binary, "/pending">>.

ensure_shipper(ProcID, Opts) ->
    hb_name:singleton(name(ProcID), fun() -> init(ProcID, Opts) end).

call(ProcID, Msg, Opts) ->
    case hb_name:lookup(name(ProcID)) of
        Pid when is_pid(Pid) ->
            Ref = erlang:monitor(process, Pid),
            Pid ! {Msg, self(), Ref},
            receive
                {Ref, Reply} -> erlang:demonitor(Ref, [flush]), Reply;
                {'DOWN', Ref, process, Pid, Why} ->
                    #{ <<"running">> => false, <<"error">> => term_bin(Why) }
            after cfg(<<"timeout">>, ?TIMEOUT, Opts) * 2 ->
                erlang:demonitor(Ref, [flush]),
                #{ <<"running">> => true, <<"error">> => <<"status timeout">> }
            end;
        _ ->
            #{ <<"running">> => false }
    end.

%% @doc Open the index (cutting a torn last record), drop pending items the
%% index already covers, and enter the loop. The first round waits one
%% interval, so the node finishes starting its stores first.
init(ProcID, Opts) ->
    ok = filelib:ensure_dir(binary_to_list(pending_dir(ProcID, Opts)) ++ "/"),
    {ok, Fd} = file:open(index_path(ProcID, Opts), [read, write, raw, binary]),
    {ok, Size} = file:position(Fd, eof),
    Next = Size div ?REC,
    {ok, _} = file:position(Fd, Next * ?REC),
    ok = file:truncate(Fd),
    case Next of
        0 -> ok;
        _ -> {ok, <<Last:64, _/binary>>} = file:pread(Fd, (Next - 1) * ?REC, ?REC),
             Last = Next - 1
    end,
    lists:foreach(
        fun(Slot) when Slot < Next -> file:delete(pending_path(ProcID, Slot, Opts));
           (_) -> ok
        end,
        pending_slots(ProcID, Opts)
    ),
    Interval = cfg(<<"interval">>, ?INTERVAL, Opts),
    ?event(anchor, {anchor_started, ProcID, {next, Next}}),
    loop(#{
        proc => ProcID, opts => Opts, sched_opts => sched_opts(Opts), fd => Fd,
        next => Next, delay => Interval, failures => 0, last_error => null,
        last_ok => null, uploaded => 0, duplicates => 0
    }).

loop(S = #{ delay := Delay }) ->
    receive
        {status, From, Ref} -> From ! {Ref, view(S)}, loop(S);
        {ship_now, From, Ref} ->
            S1 = ship_round(S),
            From ! {Ref, view(S1)},
            loop(S1);
        stop -> file:close(maps:get(fd, S))
    after Delay ->
        loop(ship_round(S))
    end.

%% @doc One round: sign (or reuse) up to `window' items from the cursor,
%% upload them together, and advance over the accepted prefix.
ship_round(S = #{ proc := ProcID, opts := Opts, next := Next }) ->
    Interval = cfg(<<"interval">>, ?INTERVAL, Opts),
    try
        Items = prepare(Next, cfg(<<"window">>, ?WINDOW, Opts), S, []),
        Results = upload_all(Items, Opts),
        S1 = commit(Items, Results, S),
        case [ X || X <- Results, element(1, X) =/= ok ] of
            [] -> S1#{ delay => Interval, failures => 0 };
            [{error, Why} | _] -> failed(Why, S1)
        end
    catch C:R:T ->
        ?event(error, {anchor_round_failed, ProcID, C, R, T}),
        failed({C, R}, S)
    end.

failed(Why, S = #{ opts := Opts, failures := F }) ->
    Interval = cfg(<<"interval">>, ?INTERVAL, Opts),
    Max = cfg(<<"max-backoff">>, ?MAX_BACKOFF, Opts),
    ?event(anchor, {anchor_upload_failed, maps:get(proc, S), Why}),
    S#{
        delay => min(Max, Interval bsl min(F + 1, 20)),
        failures => F + 1,
        last_error => term_bin(Why)
    }.

%% @doc The items for slots `Slot' onward, up to `N' of them, stopping at
%% the first slot the scheduler store does not hold. A pending item is
%% reused byte for byte; a new one is signed, written and synced (with its
%% directory) before it is returned, so it is never uploaded unsaved.
prepare(_Slot, 0, S, Acc) -> finish_prepare(S, Acc);
prepare(Slot, N, S = #{ proc := ProcID, opts := Opts }, Acc) ->
    case read_pending(ProcID, Slot, Opts) of
        {ok, Item} -> prepare(Slot + 1, N - 1, S, [{Slot, Item, old} | Acc]);
        not_found ->
            case dev_scheduler_cache:read(ProcID, Slot, maps:get(sched_opts, S)) of
                {ok, Assignment} ->
                    Wallet = hb_opts:get(priv_wallet, hb:wallet(), Opts),
                    Item = item(ProcID, Assignment, Wallet, Opts),
                    ok = file:write_file(
                        pending_path(ProcID, Slot, Opts),
                        pending_bin(Item),
                        [raw, sync]
                    ),
                    prepare(Slot + 1, N - 1, S, [{Slot, Item, new} | Acc]);
                not_found -> finish_prepare(S, Acc)
            end
    end.

finish_prepare(#{ proc := ProcID, opts := Opts }, Acc) ->
    case lists:keymember(new, 3, Acc) of
        true -> sync_dir(pending_dir(ProcID, Opts));
        false -> ok
    end,
    [ {Slot, Item} || {Slot, Item, _} <- lists:reverse(Acc) ].

%% A pending file: the three IDs, then the serialized item.
pending_bin({ItemID, AssignID, MsgID, Bin}) ->
    <<ItemID:32/binary, AssignID:32/binary, MsgID:32/binary, Bin/binary>>.

pending_path(ProcID, Slot, Opts) ->
    <<(pending_dir(ProcID, Opts))/binary, "/", (integer_to_binary(Slot))/binary>>.

%% @doc A pending item, checked: a file that does not hold a whole item for
%% this slot (cut short by a crash before its sync, so never uploaded) is
%% deleted and signed again.
read_pending(ProcID, Slot, Opts) ->
    Path = pending_path(ProcID, Slot, Opts),
    case file:read_file(Path) of
        {ok, <<ItemID:32/binary, AssignID:32/binary, MsgID:32/binary, Bin/binary>>} ->
            SlotBin = integer_to_binary(Slot),
            case catch ar_bundles:deserialize(Bin) of
                #tx{ id = ItemID, tags = Tags } ->
                    case lists:keyfind(<<"Slot">>, 1, Tags) of
                        {_, SlotBin} ->
                            {ok, {ItemID, AssignID, MsgID, Bin}};
                        _ -> discard(Path)
                    end;
                _ -> discard(Path)
            end;
        {ok, _} -> discard(Path);
        {error, _} -> not_found
    end.

discard(Path) ->
    ?event(anchor, {anchor_discarding_unsynced_pending, Path}),
    _ = file:delete(Path),
    not_found.

pending_slots(ProcID, Opts) ->
    case file:list_dir(pending_dir(ProcID, Opts)) of
        {ok, Names} ->
            [ list_to_integer(N) || N <- Names, lists:all(fun(C) -> C >= $0 andalso C =< $9 end, N) ];
        _ -> []
    end.

%% @doc Upload every item at once; one result per item, in order.
upload_all(Items, Opts) ->
    Self = self(),
    Refs =
        [
            begin
                Ref = make_ref(),
                spawn(fun() -> Self ! {Ref, catch upload(Bin, Opts)} end),
                Ref
            end
        ||
            {_Slot, {_, _, _, Bin}} <- Items
        ],
    Timeout = cfg(<<"timeout">>, ?TIMEOUT, Opts) + 5000,
    [ receive {Ref, R} -> norm_result(R) after Timeout -> {error, timeout} end || Ref <- Refs ].

norm_result({ok, _, _} = R) -> R;
norm_result({error, _} = E) -> E;
norm_result(Other) -> {error, Other}.

%% @doc POST one serialized item to `<bundler>/tx'. 200: accepted, with
%% Turbo's receipt. 202: the bundler already holds this item ID.
upload(Bin, Opts) ->
    URL = binary_to_list(<<(hb_util:bin(cfg(<<"bundler">>, ?BUNDLER, Opts)))/binary, "/tx">>),
    Timeout = cfg(<<"timeout">>, ?TIMEOUT, Opts),
    case httpc:request(post, {URL, [], "application/octet-stream", Bin},
            [{timeout, Timeout}, {connect_timeout, min(Timeout, 10000)}],
            [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Receipt}} -> {ok, accepted, Receipt};
        {ok, {{_, 201, _}, _, Receipt}} -> {ok, accepted, Receipt};
        {ok, {{_, 202, _}, _, _}} -> {ok, duplicate, <<>>};
        {ok, {{_, 402, _}, _, _}} -> {error, payment_required};
        {ok, {{_, Code, _}, _, _}} -> {error, {http, Code}};
        {error, Why} -> {error, Why}
    end.

%% @doc Record the contiguous run of accepted slots from the cursor: index
%% records written at their slot's offset and synced together, receipts
%% appended, then their pending files deleted.
commit(Items, Results, S = #{ proc := ProcID, opts := Opts, fd := Fd }) ->
    Accepted = accepted_prefix(Items, Results),
    case Accepted of
        [] -> S;
        _ ->
            Receipts =
                [ receipt_line(Slot, ItemID, Receipt)
                || {Slot, {ItemID, _, _, _}, accepted, Receipt} <- Accepted ],
            case Receipts of
                [] -> ok;
                _ ->
                    ok = file:write_file(
                        <<(proc_dir(ProcID, Opts))/binary, "/receipts.jsonl">>,
                        Receipts, [append, raw])
            end,
            ok = file:pwrite(Fd, [
                {Slot * ?REC, <<Slot:64, ItemID/binary, AssignID/binary, MsgID/binary>>}
            ||
                {Slot, {ItemID, AssignID, MsgID, _}, _, _} <- Accepted
            ]),
            ok = file:datasync(Fd),
            lists:foreach(
                fun({Slot, _, _, _}) -> file:delete(pending_path(ProcID, Slot, Opts)) end,
                Accepted
            ),
            {LastSlot, _, _, _} = lists:last(Accepted),
            Dups = length([ x || {_, _, duplicate, _} <- Accepted ]),
            S#{
                next => LastSlot + 1,
                last_ok => os:system_time(millisecond),
                uploaded => maps:get(uploaded, S) + length(Accepted) - Dups,
                duplicates => maps:get(duplicates, S) + Dups
            }
    end.

accepted_prefix([{Slot, Item} | Items], [{ok, Kind, Receipt} | Results]) ->
    [{Slot, Item, Kind, Receipt} | accepted_prefix(Items, Results)];
accepted_prefix(_, _) -> [].

receipt_line(Slot, ItemID, Receipt) ->
    [hb_json:encode(#{
        <<"slot">> => Slot,
        <<"id">> => hb_util:human_id(ItemID),
        <<"receipt">> =>
            case catch hb_json:decode(Receipt) of
                M when is_map(M) -> M;
                _ -> Receipt
            end
    }), $\n].

%% @doc The status of one shipper. `head' is the scheduler's current slot
%% when its server runs on this node, `null' otherwise.
view(S = #{ proc := ProcID, next := Next }) ->
    Head =
        case hb_name:lookup(dev_scheduler_registry:name(ProcID, maps:get(opts, S))) of
            Pid when is_pid(Pid) ->
                Pid ! {info, self()},
                receive {info, #{ current := C }} -> C after 2000 -> null end;
            _ -> null
        end,
    #{
        <<"running">> => true,
        <<"anchored">> => Next - 1,
        <<"head">> => Head,
        <<"backlog">> => case Head of null -> null; _ -> max(0, Head - (Next - 1)) end,
        <<"pending">> => length(pending_slots(ProcID, maps:get(opts, S))),
        <<"uploaded">> => maps:get(uploaded, S),
        <<"duplicates">> => maps:get(duplicates, S),
        <<"failures">> => maps:get(failures, S),
        <<"last-error">> => maps:get(last_error, S),
        <<"last-ok">> => maps:get(last_ok, S)
    }.

%% @doc The options the scheduler reads its store with. The `start' hook
%% runs before the node points `scheduler-store' at the irreplaceable
%% store, so that is done here the same way.
sched_opts(Opts) ->
    case hb_opts:get(<<"scheduler-store">>, undefined, Opts) of
        undefined ->
            case hb_store_irreplaceable:store(Opts) of
                undefined -> Opts;
                Irr -> Opts#{ <<"scheduler-store">> => Irr }
            end;
        _ -> Opts
    end.

sync_dir(Dir) ->
    _ = os:cmd("sync " ++ hb_util:list(Dir) ++ " 2>/dev/null"),
    ok.

term_bin(T) -> hb_util:bin(io_lib:format("~0p", [T])).

json(Term) ->
    #{ <<"content-type">> => <<"application/json">>, <<"body">> => hb_json:encode(Term) }.

%%% Tests

%% @doc Options for a node with its own store, anchoring `Anchor'.
test_opts(Tag, Anchor) ->
    Store = hb_test_utils:test_store(hb_store_lmdb, Tag),
    ok = hb_store:start([Store]),
    #{
        <<"priv-wallet">> => hb:wallet(),
        <<"store">> => [Store],
        <<"scheduler-publish-remote">> => false,
        <<"anchor">> => Anchor
    }.

%% @doc Spawn a process on this node's scheduler; its human ID and message.
test_spawn(Opts) ->
    Proc =
        hb_message:commit(
            #{
                <<"device">> => <<"scheduler@1.0">>,
                <<"type">> => <<"Process">>,
                <<"scheduler-location">> =>
                    hb_util:human_id(ar_wallet:to_address(hb:wallet())),
                <<"test-random-seed">> => rand:uniform(1 bsl 40)
            },
            Opts
        ),
    {ok, _} = hb_ao:resolve(Proc, #{ <<"method">> => <<"POST">>,
        <<"path">> => <<"schedule">>, <<"body">> => Proc }, Opts),
    {hb_util:human_id(hb_message:id(Proc, all, Opts)), Proc}.

test_schedule(Proc, I, Opts) ->
    {ok, _} =
        hb_ao:resolve(Proc, #{ <<"method">> => <<"POST">>,
            <<"path">> => <<"schedule">>,
            <<"body">> =>
                hb_message:commit(
                    #{ <<"type">> => <<"Message">>, <<"test-key">> => I },
                    Opts
                )
        }, Opts).

%% @doc Start the shippers as the node's `start' hook does.
test_start(Opts) ->
    {ok, _} = start(#{}, #{ <<"body">> => Opts }, Opts),
    ok.

test_anchor(Bundler, Tag) ->
    #{
        <<"bundler">> => Bundler,
        <<"dir">> => maps:get(<<"name">>, hb_test_utils:test_store(hb_store_fs, Tag)),
        <<"interval">> => 20,
        <<"max-backoff">> => 200,
        <<"timeout">> => 5000
    }.

%% @doc Wait until `ProcID' is anchored through `Slot'.
wait_anchored(ProcID, Slot, Opts) -> wait_anchored(ProcID, Slot, Opts, 500).
wait_anchored(_ProcID, _Slot, _Opts, 0) -> timeout;
wait_anchored(ProcID, Slot, Opts, N) ->
    case anchored(ProcID, Slot, Opts) of
        {ok, _} -> ok;
        not_found -> timer:sleep(20), wait_anchored(ProcID, Slot, Opts, N - 1)
    end.

%% @doc Wait until the mock has been sent at least `N' distinct items.
wait_items(Ctl, N) -> wait_items(Ctl, N, 250).
wait_items(Ctl, N, 0) -> length(mock_items(Ctl)) >= N;
wait_items(Ctl, N, T) ->
    case length(mock_items(Ctl)) >= N of
        true -> true;
        false -> timer:sleep(20), wait_items(Ctl, N, T - 1)
    end.

%%% A mock bundler: one request per connection. Every item it is sent is
%%% recorded as `{Slot, ItemID}'; its `mode' says how it answers:
%%% `ok' (200 with a receipt, 202 for an ID it holds), `down' (503, nothing
%%% kept), `drop' (kept, connection closed without an answer), `hang'
%%% (kept, never answered) and `{slow, Ms}'.

mock_start(Mode) ->
    Ctl = ets:new(anchor_mock, [public, bag]),
    ets:insert(Ctl, {mode, Mode}),
    {ok, L} = gen_tcp:listen(0, [binary, {packet, raw}, {active, false},
        {reuseaddr, true}, {ip, {127, 0, 0, 1}}]),
    {ok, Port} = inet:port(L),
    Acceptor = spawn(fun() -> receive go -> mock_accept(L, Ctl) end end),
    ok = gen_tcp:controlling_process(L, Acceptor),
    Acceptor ! go,
    {<<"http://127.0.0.1:", (integer_to_binary(Port))/binary>>, Ctl}.

mock_mode(Ctl, Mode) ->
    ets:delete(Ctl, mode),
    ets:insert(Ctl, {mode, Mode}).

%% @doc Every `{Slot, ItemID}' the mock has been sent.
mock_items(Ctl) -> [ {Slot, ID} || {item, Slot, ID} <- ets:lookup(Ctl, item) ].

mock_accept(L, Ctl) ->
    case gen_tcp:accept(L) of
        {ok, Sock} ->
            Pid = spawn(fun() -> receive go -> mock_conn(Sock, Ctl) end end),
            ok = gen_tcp:controlling_process(Sock, Pid),
            Pid ! go,
            mock_accept(L, Ctl);
        _ -> ok
    end.

mock_conn(Sock, Ctl) ->
    {Head, Rest} = mock_head(Sock, <<>>),
    {match, [Len]} =
        re:run(Head, "content-length:\\s*(\\d+)",
            [caseless, {capture, all_but_first, binary}]),
    Body = mock_body(Sock, Rest, binary_to_integer(Len)),
    #tx{ id = ID, tags = Tags } = ar_bundles:deserialize(Body),
    {_, SlotBin} = lists:keyfind(<<"Slot">>, 1, Tags),
    Slot = binary_to_integer(SlotBin),
    [{mode, Mode}] = ets:lookup(Ctl, mode),
    Known = lists:member({Slot, ID}, mock_items(Ctl)),
    case Mode of
        down -> mock_reply(Sock, 503, <<>>);
        drop -> mock_keep(Ctl, Slot, ID, Known), gen_tcp:close(Sock);
        hang -> mock_keep(Ctl, Slot, ID, Known), receive never -> ok end;
        {slow, Ms} -> timer:sleep(Ms), mock_ok(Sock, Ctl, Slot, ID, Known);
        ok -> mock_ok(Sock, Ctl, Slot, ID, Known)
    end.

mock_keep(_Ctl, _Slot, _ID, true) -> ok;
mock_keep(Ctl, Slot, ID, false) -> ets:insert(Ctl, {item, Slot, ID}).

mock_ok(Sock, _Ctl, _Slot, _ID, true) -> mock_reply(Sock, 202, <<>>);
mock_ok(Sock, Ctl, Slot, ID, false) ->
    mock_keep(Ctl, Slot, ID, false),
    mock_reply(Sock, 200, hb_json:encode(#{
        <<"id">> => hb_util:human_id(ID),
        <<"winc">> => <<"0">>,
        <<"timestamp">> => os:system_time(millisecond)
    })).

mock_reply(Sock, Code, Body) ->
    gen_tcp:send(Sock, [<<"HTTP/1.1 ">>, integer_to_binary(Code), <<" X\r\n">>,
        <<"connection: close\r\ncontent-length: ">>,
        integer_to_binary(byte_size(Body)), <<"\r\n\r\n">>, Body]),
    gen_tcp:close(Sock).

mock_head(Sock, Acc) ->
    case binary:split(Acc, <<"\r\n\r\n">>) of
        [Head, Rest] -> {Head, Rest};
        [_] ->
            {ok, More} = gen_tcp:recv(Sock, 0, 10000),
            mock_head(Sock, <<Acc/binary, More/binary>>)
    end.

mock_body(_Sock, Acc, Len) when byte_size(Acc) >= Len -> Acc;
mock_body(Sock, Acc, Len) ->
    {ok, More} = gen_tcp:recv(Sock, 0, 10000),
    mock_body(Sock, <<Acc/binary, More/binary>>, Len).

%% @doc An anchor item is a valid ANS-104 item signed by the node, and its
%% data decodes to the assignment it was made from, whose commitments and
%% whose message's original commitments still verify.
item_verifies_and_round_trips_test() ->
    Opts = test_opts(<<"anchor-item">>, #{}),
    {ProcID, Proc} = test_spawn(Opts),
    test_schedule(Proc, <<"one">>, Opts),
    {ok, Assignment} = dev_scheduler_cache:read(ProcID, 1, Opts),
    {ItemID, AssignID, MsgID, Bin} = item(ProcID, Assignment, hb:wallet(), Opts),
    TX = ar_bundles:deserialize(Bin),
    ?assert(ar_bundles:verify_item(TX)),
    ?assertEqual(ItemID, TX#tx.id),
    ?assertEqual(
        ar_wallet:to_address(hb:wallet()),
        crypto:hash(sha256, TX#tx.owner)
    ),
    ?assertEqual({<<"Slot">>, <<"1">>}, lists:keyfind(<<"Slot">>, 1, TX#tx.tags)),
    ?assertEqual({<<"Process">>, ProcID}, lists:keyfind(<<"Process">>, 1, TX#tx.tags)),
    Decoded =
        hb_message:convert(TX#tx.data, <<"structured@1.0">>, <<"json@1.0">>, Opts),
    ?assert(hb_message:verify(Decoded, all, Opts)),
    ?assertEqual(hb_util:human_id(AssignID), hb_message:id(Decoded, signed, Opts)),
    Body = hb_maps:get(<<"body">>, Decoded, undefined, Opts),
    ?assert(hb_message:verify(Body, all, Opts)),
    ?assertEqual(hb_util:human_id(MsgID), hb_message:id(Body, signed, Opts)),
    ?assertEqual(<<"one">>, hb_maps:get(<<"test-key">>, Body, undefined, Opts)).

%% @doc The shipper anchors every slot once, in order, and the index gives
%% each slot's item and the assignment the scheduler holds for it, also over
%% HTTP.
ships_every_slot_once_test_() ->
    {timeout, 60, fun() ->
        {URL, Ctl} = mock_start(ok),
        Opts0 = test_opts(<<"anchor-ship">>, #{}),
        {ProcID, Proc} = test_spawn(Opts0),
        Opts =
            Opts0#{
                <<"anchor">> =>
                    (test_anchor(URL, <<"anchor-ship-dir">>))#{
                        <<"processes">> => [ProcID]
                    }
            },
        lists:foreach(fun(I) -> test_schedule(Proc, I, Opts) end, lists:seq(1, 20)),
        test_start(Opts),
        ?assertEqual(ok, wait_anchored(ProcID, 20, Opts)),
        Items = mock_items(Ctl),
        ?assertEqual(lists:seq(0, 20), lists:sort([ S || {S, _} <- Items ])),
        lists:foreach(
            fun(Slot) ->
                {ok, Rec} = anchored(ProcID, Slot, Opts),
                {ok, A} = dev_scheduler_cache:read(ProcID, Slot, Opts),
                ?assertEqual(
                    hb_message:id(A, signed, Opts),
                    maps:get(<<"assignment">>, Rec)
                ),
                {_, ID} = lists:keyfind(Slot, 1, Items),
                ?assertEqual(hb_util:human_id(ID), maps:get(<<"id">>, Rec))
            end,
            lists:seq(0, 20)
        ),
        ?assertEqual(not_found, anchored(ProcID, 21, Opts)),
        ?assertMatch(
            #{ <<"anchored">> := 20, <<"pending">> := 0 },
            maps:get(ProcID, status(Opts))
        ),
        % The same lookup over HTTP.
        Node = hb_http_server:start_node(Opts),
        {ok, Res} =
            hb_http:get(
                Node,
                <<"/~anchor@1.0/lookup?process=", ProcID/binary, "&slot=7">>,
                Opts
            ),
        {ok, Rec7} = anchored(ProcID, 7, Opts),
        ?assertEqual(
            maps:get(<<"id">>, Rec7),
            maps:get(<<"id">>, hb_json:decode(hb_maps:get(<<"body">>, Res, <<>>, Opts)))
        ),
        stop(ProcID)
    end}.

%% @doc A shipper killed with uploads in flight, and one whose bundler took
%% items without answering, lose nothing and post each slot's item once:
%% the retry re-sends the same signed bytes, which the bundler already has.
%% A torn index record and a stale pending item are cleared on start.
crash_mid_ship_loses_nothing_test_() ->
    {timeout, 60, fun() ->
        {URL, Ctl} = mock_start(hang),
        Opts0 = test_opts(<<"anchor-crash">>, #{}),
        {ProcID, Proc} = test_spawn(Opts0),
        Opts =
            Opts0#{
                <<"anchor">> =>
                    (test_anchor(URL, <<"anchor-crash-dir">>))#{
                        <<"processes">> => [ProcID],
                        <<"window">> => 4
                    }
            },
        lists:foreach(fun(I) -> test_schedule(Proc, I, Opts) end, lists:seq(1, 9)),
        test_start(Opts),
        % Slots 0-3 are with the bundler, unanswered: kill the shipper.
        ?assert(wait_items(Ctl, 4)),
        exit(hb_name:lookup(name(ProcID)), kill),
        timer:sleep(50),
        ?assertEqual(not_found, anchored(ProcID, 0, Opts)),
        % The bundler takes items and drops the connection: kill again.
        mock_mode(Ctl, drop),
        test_start(Opts),
        ?assert(wait_items(Ctl, 8)),
        exit(hb_name:lookup(name(ProcID)), kill),
        timer:sleep(50),
        mock_mode(Ctl, ok),
        test_start(Opts),
        ?assertEqual(ok, wait_anchored(ProcID, 9, Opts)),
        stop(ProcID),
        ok = file:write_file(index_path(ProcID, Opts), <<"torn">>, [append]),
        ok = file:write_file(pending_path(ProcID, 3, Opts), <<"stale">>),
        lists:foreach(fun(I) -> test_schedule(Proc, I, Opts) end, lists:seq(10, 12)),
        test_start(Opts),
        ?assertEqual(ok, wait_anchored(ProcID, 12, Opts)),
        % One item per slot, ever, and it is the one the index names.
        Items = mock_items(Ctl),
        ?assertEqual(lists:seq(0, 12), lists:sort([ S || {S, _} <- Items ])),
        lists:foreach(
            fun({Slot, ID}) ->
                {ok, Rec} = anchored(ProcID, Slot, Opts),
                ?assertEqual(hb_util:human_id(ID), maps:get(<<"id">>, Rec))
            end,
            Items
        ),
        ?assertMatch(#{ <<"pending">> := 0 }, maps:get(ProcID, status(Opts))),
        stop(ProcID)
    end}.

%% @doc Scheduling throughput is the same with anchoring off and on (the
%% bundler up, slow, or down), and a bundler that is down only grows the
%% backlog.
throughput_unchanged_test_() ->
    {timeout, 600, fun() ->
        N = 300,
        {UpURL, _} = mock_start(ok),
        {SlowURL, _} = mock_start({slow, 500}),
        {DownURL, _} = mock_start(down),
        Run =
            fun(Label, Bundler) ->
                Opts0 = test_opts(<<"anchor-rate">>, #{}),
                {ProcID, Proc} = test_spawn(Opts0),
                Opts =
                    case Bundler of
                        off -> Opts0;
                        _ ->
                            Opts0#{
                                <<"anchor">> =>
                                    (test_anchor(Bundler, <<"anchor-rate-dir">>))#{
                                        <<"processes">> => [ProcID]
                                    }
                            }
                    end,
                Bundler =/= off andalso test_start(Opts) =:= ok,
                T0 = erlang:monotonic_time(microsecond),
                lists:foreach(
                    fun(I) -> test_schedule(Proc, I, Opts) end,
                    lists:seq(1, N)
                ),
                Us = erlang:monotonic_time(microsecond) - T0,
                Rate = N * 1000000 / Us,
                St =
                    case Bundler of
                        off -> #{};
                        _ -> maps:get(ProcID, status(Opts))
                    end,
                io:format(user,
                    "~nanchor throughput ~s: ~.1f msg/s (~p us/msg) ~0p~n",
                    [Label, Rate, Us div N, St]),
                Bundler =/= off andalso stop(ProcID) =:= ok,
                {Rate, St}
            end,
        {Off1, _} = Run("off", off),
        {Up, _} = Run("on, bundler up", UpURL),
        {Slow, _} = Run("on, bundler 500 ms", SlowURL),
        {Down, #{ <<"backlog">> := Backlog }} = Run("on, bundler down", DownURL),
        {Off2, _} = Run("off again", off),
        Off = (Off1 + Off2) / 2,
        ?assert(Backlog >= N),
        ?assert(Up > 0.7 * Off),
        ?assert(Slow > 0.7 * Off),
        ?assert(Down > 0.7 * Off)
    end}.

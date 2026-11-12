%%% @doc This node's own computation of a process scheduled by a `verified'
%%% foreign scheduler S, and the outbox entries it may therefore admit.
%%%
%%% S's signature counts for nothing. A slot of a followed process P is
%%% usable only when all of these hold:
%%%
%%%   1. `lib_bridge_replica' accepted it: signed by P's scheduler,
%%%      next in slot order, continuing the `base-hashpath' chain, carrying a
%%%      message a real signature vouches for and that is for P. S's URL is
%%%      transport only.
%%%   2. Every confirmed window of S's Arweave schedule archive
%%%      (`dev_scheduler_uploader') that covers the slot holds that very
%%%      assignment, mined `confirmations' deep (default 3; inclusion is the
%%%      first). Any window holding a DIFFERENT assignment for the slot is a
%%%      fork: S is frozen and the IDs kept as evidence.
%%%   3. Every input of the slot that claims a process origin passes THIS
%%%      node's door for P (`dev_scheduler_admission:check/3'), as if it had
%%%      been delivered here, and names an outbox entry no earlier slot of P
%%%      took. Whatever S's own door wrote on the assignment is S's word and
%%%      counts for nothing. An input our door refuses stops the follower.
%%%
%%% Each usable slot is computed in the replica's isolated store, with no node
%%% wallet, and its outbox entries are indexed by (P, slot, key) in the
%%% scheduler's store with the provenance the deliverer would stamp
%%% (`lib_process_delivery:origin/3'). The door (`check/5') only reads that
%%% index: it never computes, fetches or waits.
%%%
%%% Following is lazy: a follower starts at the first push from P and keeps
%%% polling while the node runs. Its state is cache; losing it costs a
%%% recompute, not safety.
-module(dev_scheduler_follower).
-device_libraries([lib_bridge_replica, lib_process_cache, lib_process_delivery]).
-export([check/5, ensure/3]).
-export([archived_ids/1, window_confirmed/4]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PREFIX, <<"~scheduler@1.0">>).
-define(DEFAULT_POLL_MS, 5000).
-define(DEFAULT_DEPTH, 3).

%%% The door ------------------------------------------------------------------

%% @doc `ok' when the delivered `Req' is exactly the entry this node verified
%% for its stamps; throws `{refused, 403 | 503, Why}' otherwise.
check(S, Stamps, Req, ProcID, Opts) ->
    #{ <<"admit-process">> := P, <<"admit-slot">> := Slot,
       <<"admit-outbox-key">> := Key, <<"admit-entry">> := EntryID } = Stamps,
    case forked(S, Opts) of
        true -> throw({refused, 403, <<"the sender's scheduler forked its schedule">>});
        false -> ok
    end,
    case entry(P, Slot, Key, Opts) of
        not_found ->
            ensure(P, S, Opts),
            throw({refused, 503, <<"entry not verified yet">>});
        {ok, #{ <<"entry">> := Entry, <<"origin">> := Origin }} ->
            need(hb_message:id(Entry, all, Opts) =:= EntryID, <<"entry ID does not match">>),
            need(maps:get(<<"from-scheduler">>, Origin, none) =/= none
                andalso hb_committer:uri(maps:get(<<"from-scheduler">>, Origin))
                    =:= hb_committer:uri(S),
                <<"entry is not the scheduler's">>),
            Expected =
                lib_process_delivery:augment_message(
                    Origin#{
                        <<"from-slot">> => Slot,
                        <<"from-outbox-key">> => Key,
                        <<"from-entry">> => EntryID
                    },
                    Entry,
                    Opts
                ),
            % The body as delivered, against the entry as computed: every key,
            % the stamps included. Both sides have their target's hint
            % stripped, because the deliverer strips it only when it posts to
            % a known location (`lib_process_delivery:sign_entry/7'). An entry
            % the sender evaluated (`resolve') never matches, because its
            % delivered body is the sender node's evaluation, which nobody
            % else can check.
            need(delivered_id(Expected, Opts) =:= delivered_id(Req, Opts),
                <<"message is not the verified entry">>),
            need((catch hb_util:human_id(hb_maps:get(<<"target">>, Entry, none, Opts)))
                    =:= hb_util:human_id(ProcID),
                <<"entry is for another process">>),
            ok
    end.

need(true, _) -> ok;
need(_, Why) -> throw({refused, 403, Why}).

delivered_id(Msg, Opts) ->
    hb_message:id(
        lib_process_delivery:normalize_message(hb_message:uncommitted(Msg), Opts),
        none, Opts).

%%% The follower --------------------------------------------------------------

%% @doc Start following P (scheduled by S) if nothing follows it yet.
ensure(P, S, Opts) ->
    hb_name:singleton({?MODULE, hb_util:human_id(P)},
        fun() -> follow(P, S, Opts) end).

follow(P, S, Opts) ->
    Cfg = hb_opts:get(<<"scheduler-admission">>, #{}, Opts),
    Url = hb_maps:get(<<"location">>, dev_scheduler_admission:peer(S, Opts), undefined, Opts),
    Dir = hb_util:bin(hb_maps:get(<<"replica-dir">>, Cfg, <<"admission-replica">>, Opts)),
    loop(#{ p => hb_util:human_id(P), s => S, url => Url, dir => Dir, opts => Opts,
        poll => hb_util:int(hb_maps:get(<<"poll-ms">>, Cfg, ?DEFAULT_POLL_MS, Opts)),
        depth => hb_util:int(hb_maps:get(<<"confirmations">>, Cfg, ?DEFAULT_DEPTH, Opts)),
        indexed => indexed(P, Opts), windows => [] }).

loop(F = #{ poll := Poll }) ->
    F2 =
        try pass(F)
        catch Class:Reason:Stack ->
            ?event(warning, {foreign_follower_pass_failed,
                {p, maps:get(p, F)}, {class, Class}, {reason, Reason}, {trace, Stack}}),
            F
        end,
    case maps:get(stopped, F2, false) of
        true -> ok;
        false -> receive after Poll -> loop(F2) end
    end.

%% One pass: bring the replica forward, then index every slot that is
%% verified, archived deep enough and whose inputs we re-admit.
pass(F = #{ p := P, url := Url, dir := Dir, opts := Opts, indexed := Indexed }) ->
    R = lib_bridge_replica:forward(
        lib_bridge_replica:load(P, Url, Opts, Dir)),
    case R of
        #{ slot := Top } when Top > Indexed ->
            index(lists:seq(Indexed + 1, Top), R, with_windows(F));
        _ -> F
    end.

index([], _R, F) -> F;
index([N | Rest], R = #{ proc := Proc, opts := ROpts }, F = #{ p := P, opts := Opts }) ->
    {ok, A0} = hb_cache:read(lib_bridge_replica:slot_path(P, N, none), ROpts),
    A = hb_cache:ensure_all_loaded(A0, ROpts),
    case archived(N, hb_message:id(A, signed, ROpts), F) of
        not_yet -> F;
        {fork, Archived} -> freeze(F, N, Archived, A);
        ok ->
            case readmit(A, P, ROpts, Opts) of
                not_yet -> F;
                {refused, Why} -> stop(F, N, Why);
                {ok, Taken} ->
                    {ok, State} =
                        hb_ao:resolve(Proc,
                            #{ <<"path">> => <<"compute">>, <<"slot">> => N }, ROpts),
                    ok = write_entries(P, N, Proc, State, Taken, ROpts, Opts),
                    index(Rest, R, F#{ indexed := N })
            end
    end.

%% The slot's input as an input of P at THIS node's door: `{ok, Taken}' with
%% the outbox entry it names (`[]' for one that claims no origin), `not_yet'
%% while the door would answer 503, `{refused, Why}' otherwise.
readmit(A, P, ROpts, Opts) ->
    Body = hb_cache:ensure_all_loaded(hb_maps:get(<<"body">>, A, #{}, ROpts), ROpts),
    try dev_scheduler_admission:check(P, Body, Opts) of
        none -> {ok, []};
        {Stamps, _Policy, _Epoch} ->
            Taken = dev_scheduler_admission:ids(Stamps, Opts),
            case [ ID || ID <- Taken, taken(P, ID, Opts) ] of
                [] -> {ok, Taken};
                _ -> {refused, <<"an outbox entry is an input of the process twice">>}
            end
    catch
        throw:{refused, 503, _} -> not_yet;
        throw:{refused, _, Why} -> {refused, Why}
    end.

%%% The archive ---------------------------------------------------------------

%% `ok' when every confirmed archive window over slot N holds this
%% assignment; `{fork, Archived}' when any holds another; `not_yet' when none
%% covers N yet.
archived(N, AssignmentID, #{ windows := Windows }) ->
    case [ W || W = #{ from := From, to := To } <- Windows, From =< N, N =< To ] of
        [] -> not_yet;
        Ws ->
            case [ W || W = #{ ids := IDs } <- Ws, not lists:member(AssignmentID, IDs) ] of
                [] -> ok;
                _ -> {fork, [ maps:get(id, W) || W <- Ws ]}
            end
    end.

%% The archive windows of P that S signed and that are mined deep enough.
with_windows(F = #{ p := P, s := S, depth := Depth, opts := Opts, windows := Have }) ->
    Gateway = hb_util:list(hb_opts:get(<<"scheduler-upload-gateway">>,
        <<"https://arweave.net">>, Opts)),
    {ok, {{_, 200, _}, _, HeightBody}} = httpc:request(get,
        {Gateway ++ "/height", []}, [{timeout, 15000}], [{body_format, binary}]),
    Height = binary_to_integer(string:trim(HeightBody)),
    Edges = archive_edges(Gateway, S, P, #{}),
    Known = [ maps:get(id, W) || W <- Have ],
    New =
        [ begin
              {ok, {{_, 200, _}, _, Data}} = httpc:request(get,
                  {Gateway ++ "/" ++ hb_util:list(ID), []},
                  [{timeout, 60000}], [{body_format, binary}]),
              #{ id => ID, from => hb_util:int(tag(<<"From-Slot">>, Tags)),
                 to => hb_util:int(tag(<<"To-Slot">>, Tags)), ids => archived_ids(Data) }
          end
        || #{ <<"node">> := #{ <<"id">> := ID, <<"tags">> := Tags,
                <<"block">> := Block } } <- Edges,
           not lists:member(ID, Known),
           window_confirmed(Block, Height, Depth, true) ],
    F#{ windows := Have ++ New }.

%% Every archive transaction of P that S signed, page by page.
archive_edges(Gateway, S, P, After) ->
    Query = <<"query($o:[String!],$p:[String!],$a:String){transactions(owners:$o,",
        "tags:[{name:\"Data-Protocol\",values:[\"ao-schedule-archive\"]},",
        "{name:\"Process\",values:$p}],first:100,after:$a,sort:HEIGHT_ASC)",
        "{pageInfo{hasNextPage} edges{cursor node{id tags{name value} block{height}}}}}">>,
    Body = hb_json:encode(#{ <<"query">> => Query, <<"variables">> =>
        After#{ <<"o">> => [hb_util:human_id(S)], <<"p">> => [P] } }),
    {ok, {{_, 200, _}, _, Res}} = httpc:request(post,
        {Gateway ++ "/graphql", [], "application/json", Body},
        [{timeout, 15000}], [{body_format, binary}]),
    #{ <<"data">> := #{ <<"transactions">> := #{
        <<"pageInfo">> := #{ <<"hasNextPage">> := More }, <<"edges">> := Edges } } } =
        hb_json:decode(Res),
    case {More, lists:reverse(Edges)} of
        {true, [#{ <<"cursor">> := Cursor } | _]} ->
            Edges ++ archive_edges(Gateway, S, P, #{ <<"a">> => Cursor });
        _ -> Edges
    end.

%% @doc A window is confirmed when its block is `Depth' deep, inclusion
%% counting as the first (`dev_scheduler_uploader:window_confirmed/4').
window_confirmed(#{ <<"height">> := H }, Height, Depth, _) when is_integer(H), H >= 0 ->
    Height - H + 1 >= Depth;
window_confirmed(_, _, _, _) -> false.

tag(Name, Tags) ->
    case [ V || #{ <<"name">> := N, <<"value">> := V } <- Tags, N =:= Name ] of
        [V | _] -> V;
        [] -> throw({archive_tag_missing, Name})
    end.

%% @doc The message IDs an archive's data holds (`dev_scheduler_uploader'
%% `bundle_data/1'): each item's own ID, and the `Message-Id' of an item that
%% wraps an httpsig-signed message as JSON.
archived_ids(<<Count:256/little, Rest/binary>>) ->
    IndexSize = Count * 64,
    <<Index:IndexSize/binary, Items/binary>> = Rest,
    Entries = [ {Size, ID} || <<Size:256/little, ID:32/binary>> <= Index ],
    archived_ids(Entries, Items).
archived_ids([], _) -> [];
archived_ids([{Size, ID} | Rest], Items) ->
    <<Item:Size/binary, More/binary>> = Items,
    {ok, _, #tx{ tags = Tags }} = ar_bundles:deserialize_header(Item),
    Wrapped = [ V || {<<"Message-Id">>, V} <- Tags ],
    [hb_util:human_id(ID) | Wrapped] ++ archived_ids(Rest, More).

%%% Index and markers ---------------------------------------------------------

%% Slot N's outbox entries, the entries its input took, and the new
%% `indexed' mark: one write, one flush.
write_entries(P, N, Proc, State, Taken, ROpts, Opts) ->
    Results = hb_maps:get(<<"results">>, hb_private:reset(State), #{}, ROpts),
    Origin = lib_process_delivery:origin(P, Proc, ROpts),
    Rows =
        maps:from_list(
            [ {entry_path(P, N, K),
               term_to_binary(#{ <<"entry">> => E, <<"origin">> => Origin })}
            || {K, E} <- maps:to_list(outbox(Results, ROpts)) ]
            ++ [ {taken_path(P, ID), integer_to_binary(N)} || ID <- Taken ]),
    Store = store(Opts),
    ok = hb_store:write(Store, Rows#{ indexed_path(P) => integer_to_binary(N) }, Opts),
    hb_store:flush(Store, Opts).

outbox(Results, Opts) ->
    Entries = lib_process_cache:outbox_entries(
        #{ <<"lower">> => false,
           <<"outbox">> => hb_maps:get(<<"outbox">>, Results, #{}, Opts) }, Opts),
    case Entries of
        L when is_list(L) ->
            maps:from_list(lists:zip(
                [ integer_to_binary(I) || I <- lists:seq(1, length(L)) ], L));
        M -> M
    end.

entry(P, Slot, Key, Opts) ->
    Req = #{ <<"read">> => entry_path(P, hb_util:int(Slot), hb_util:bin(Key)),
        <<"exact">> => true },
    case hb_store:read(store(Opts), Req, Opts) of
        {ok, Bin} -> {ok, binary_to_term(Bin, [safe])};
        _ -> not_found
    end.

indexed(P, Opts) ->
    Req = #{ <<"read">> => indexed_path(P), <<"exact">> => true },
    case hb_store:read(store(Opts), Req, Opts) of
        {ok, Bin} -> hb_util:int(Bin);
        _ -> -1
    end.

taken(P, ID, Opts) ->
    Req = #{ <<"read">> => taken_path(P, ID), <<"exact">> => true },
    case hb_store:read(store(Opts), Req, Opts) of
        {ok, _} -> true;
        _ -> false
    end.

forked(S, Opts) ->
    Req = #{ <<"read">> => fork_path(S), <<"exact">> => true },
    case hb_store:read(store(Opts), Req, Opts) of
        {ok, _} -> true;
        _ -> false
    end.

%% Keep both signed assignments as evidence; refuse all of S until an
%% operator clears the marker.
freeze(F = #{ s := S, p := P, opts := Opts }, N, Archived, Served) ->
    Evidence = hb_json:encode(#{ <<"process">> => P, <<"slot">> => N,
        <<"served">> => hb_message:id(Served, signed, Opts), <<"archived">> => Archived }),
    ok = hb_store:write(store(Opts), #{ fork_path(S) => Evidence }, Opts),
    ok = hb_store:flush(store(Opts), Opts),
    ?event(error, {foreign_scheduler_forked, {scheduler, S}, {process, P}, {slot, N}}),
    F#{ stopped => true }.

stop(F = #{ p := P }, N, Why) ->
    ?event(error, {foreign_follower_stopped, {process, P}, {slot, N}, {why, Why}}),
    F#{ stopped => true }.

store(Opts) ->
    hb_opts:get(scheduler_store, hb_opts:get(store, no_viable_store, Opts), Opts).

entry_path(P, N, Key) ->
    hb_path:to_binary([?PREFIX, <<"admission-entries">>, hb_util:human_id(P),
        integer_to_binary(N), Key]).
taken_path(P, ID) ->
    hb_path:to_binary([?PREFIX, <<"admission-taken">>, hb_util:human_id(P), ID]).
indexed_path(P) ->
    hb_path:to_binary([?PREFIX, <<"admission-indexed">>, hb_util:human_id(P)]).
fork_path(S) ->
    hb_path:to_binary([?PREFIX, <<"admission-forks">>, hb_util:human_id(S)]).

%%% Tests ---------------------------------------------------------------------

archived_ids_round_trip_test() ->
    W = ar_wallet:new(),
    Plain = ar_bundles:sign_item(#tx{ format = ans104, data = <<"a">>, data_size = 1,
        tags = [] }, W),
    Wrapped = ar_bundles:sign_item(#tx{ format = ans104, data = <<"{}">>, data_size = 2,
        tags = [{<<"Message-Id">>, <<"wrapped-id">>}] }, W),
    {undefined, Data} = ar_bundles:serialize_bundle(list, [Plain, Wrapped], false),
    IDs = archived_ids(Data),
    ?assert(lists:member(hb_util:human_id(ar_bundles:id(Plain, signed)), IDs)),
    ?assert(lists:member(<<"wrapped-id">>, IDs)).

window_depth_test() ->
    ?assert(window_confirmed(#{ <<"height">> => 98 }, 100, 3, true)),
    ?assertNot(window_confirmed(#{ <<"height">> => 99 }, 100, 3, true)),
    ?assertNot(window_confirmed(null, 100, 3, true)).

fork_is_detected_test() ->
    F = #{ windows => [#{ id => <<"w">>, from => 0, to => 4, ids => [<<"a1">>] }] },
    ?assertEqual(ok, archived(2, <<"a1">>, F)),
    ?assertEqual({fork, [<<"w">>]}, archived(2, <<"a2">>, F)),
    ?assertEqual(not_yet, archived(5, <<"a1">>, F)).

%% S archives both forks: one confirmed window over the slot holding another
%% assignment is a fork, however many hold ours.
fork_in_any_window_is_a_fork_test() ->
    F = #{ windows => [#{ id => <<"w1">>, from => 0, to => 4, ids => [<<"a1">>] },
                       #{ id => <<"w2">>, from => 2, to => 6, ids => [<<"a2">>] }] },
    ?assertEqual({fork, [<<"w1">>, <<"w2">>]}, archived(2, <<"a1">>, F)),
    ?assertEqual({fork, [<<"w1">>, <<"w2">>]}, archived(2, <<"a2">>, F)),
    ?assertEqual(ok, archived(1, <<"a1">>, F)).

%% An input is judged by our door, not by what S wrote beside it: a stamped
%% body our door refuses stops the follower whatever `admit-*' S attached.
input_is_judged_by_our_door_test() ->
    Opts = #{},
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Bare = #{ <<"body">> => #{ <<"from-process">> => <<"x">> } },
    ?assertMatch({refused, _}, readmit(Bare, P, Opts, Opts)),
    ?assertMatch({refused, _}, readmit(Bare#{ <<"admit-process">> => <<"x">>,
        <<"admit-scheduler">> => <<"x">>, <<"admit-policy">> => <<"trusted">> }, P, Opts, Opts)),
    ?assertEqual({ok, []}, readmit(#{ <<"body">> => #{ <<"action">> => <<"x">> } }, P, Opts, Opts)).

%% A genuine input names its outbox entry once: the same entry in a later
%% slot of P stops the follower.
entry_taken_once_test() ->
    Store = hb_test_utils:test_store(),
    ok = hb_store:start([Store]),
    Peer = ar_wallet:new(),
    PeerAddr = hb_util:human_id(ar_wallet:to_address(Peer)),
    Opts = #{ <<"priv-wallet">> => ar_wallet:new(), <<"store">> => [Store],
        <<"scheduler-admission">> => #{ <<"peers">> => #{ PeerAddr => #{ <<"policy">> => <<"trusted">> } } } },
    Sender = hb_message:commit(#{ <<"type">> => <<"Process">>, <<"scheduler">> => PeerAddr },
        #{ <<"priv-wallet">> => ar_wallet:new() }, <<"httpsig@1.0">>),
    {ok, _} = hb_cache:write(Sender, Opts),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Body = hb_message:commit(#{ <<"target">> => P, <<"action">> => <<"Credit-Notice">>,
        <<"from-process">> => hb_util:human_id(hb_message:id(Sender, all, Opts)),
        <<"from-slot">> => 3, <<"from-outbox-key">> => <<"1">>,
        <<"from-entry">> => hb_util:human_id(crypto:strong_rand_bytes(32)),
        <<"from-scheduler">> => PeerAddr }, #{ <<"priv-wallet">> => Peer }, <<"httpsig@1.0">>),
    {ok, [ID]} = readmit(#{ <<"body">> => Body }, P, Opts, Opts),
    ok = hb_store:write(store(Opts), #{ taken_path(P, ID) => <<"4">> }, Opts),
    ?assertMatch({refused, _}, readmit(#{ <<"body">> => Body }, P, Opts, Opts)).

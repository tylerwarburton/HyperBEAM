%%% @doc A separate store for what this node can never regenerate: every
%%% scheduler assignment, the message it carries (with its commitments and
%%% every blob it reaches), and every process definition. Everything else in
%%% the node's store -- computed states, deltas, checkpoints, cached reads --
%%% is derived from these and can be recomputed.
%%%
%%% RC7 already routes the scheduler's cache through `scheduler-store'
%%% (`dev_scheduler_cache', `dev_location_cache'): assignments, the messages
%%% they carry and the definitions the scheduler writes at spawn. This module
%%% points `scheduler-store' at the irreplaceable store, so those writes land
%%% there, whole (`hb_cache:write/2' writes a message's whole graph to the
%%% first store, so nothing in it depends on a row of the node's store), and
%%% adds it, read-only, to the node's `store' list right after the first
%%% store, so a definition or message read by ID is found there.
%%%
%%% Configuration (off unless set; unset, `node_message/1' returns the node
%%% message unchanged):
%%% <ul>
%%%   <li>`irreplaceable-store': a store, or list of stores. Use a LOCAL
%%%       `hb_store_lmdb' (never on a network filesystem: LMDB's mmap and
%%%       lock file are unsupported there).</li>
%%%   <li>`irreplaceable-export': the offsite journal's options
%%%       (`hb_store_offsite_journal'): the first irreplaceable store is
%%%       wrapped in it, so every write to it is journaled and shipped.</li>
%%% </ul>
%%% An explicit `scheduler-store' in the node message is left as it is.
-module(hb_store_irreplaceable).
-export([store/1, enabled/1, node_message/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc The irreplaceable store list, wrapped in the offsite journal when one
%% is configured, or `undefined'.
store(Opts) ->
    case hb_opts:get(<<"irreplaceable-store">>, undefined, Opts) of
        Store when is_map(Store) -> with_export([Store], Opts);
        [_ | _] = Stores -> with_export(Stores, Opts);
        _ -> undefined
    end.

enabled(Opts) -> store(Opts) =/= undefined.

with_export([First = #{ <<"store-module">> := hb_store_offsite_journal } | Rest], _Opts) ->
    [First | Rest];
with_export([First | Rest], Opts) ->
    case hb_opts:get(<<"irreplaceable-export">>, undefined, Opts) of
        Export when is_map(Export) -> [hb_store_offsite_journal:wrap(First, Export) | Rest];
        _ -> [failing(First) | Rest]
    end.

%% A refused write must not fall through to the next store of a list.
failing(Store) -> maps:merge(#{ <<"store-failure">> => throw }, Store).

%% @doc Start the irreplaceable store, make it the scheduler's store, and add
%% it, read-only, after the first of the node's stores.
node_message(NodeMsg) ->
    case store(NodeMsg) of
        undefined -> NodeMsg;
        Irr ->
            ok = hb_store:start(Irr, #{}, NodeMsg),
            Main = as_list(hb_opts:get(<<"store">>, [], NodeMsg)),
            ReadOnly = [ S#{ <<"access">> => [<<"read">>] } || S <- Irr ],
            Store =
                case Main of
                    [] -> ReadOnly;
                    [First | Rest] -> [First | ReadOnly ++ Rest]
                end,
            Sched =
                case hb_opts:get(<<"scheduler-store">>, undefined, NodeMsg) of
                    undefined -> Irr;
                    Explicit -> Explicit
                end,
            NodeMsg#{ <<"store">> => Store, <<"scheduler-store">> => Sched }
    end.

as_list(S) when is_list(S) -> S;
as_list(S) when is_map(S) -> [S];
as_list(_) -> [].

%%% Tests

unset_is_identity_test() ->
    Opts = #{ <<"store">> => [#{ <<"store-module">> => hb_store_lmdb, <<"name">> => <<"x">> }] },
    ?assertEqual(undefined, store(Opts)),
    ?assertEqual(Opts, node_message(Opts)).

routing_test() ->
    Main = hb_test_utils:test_store(hb_store_lmdb, <<"irr-main">>),
    Irr = hb_test_utils:test_store(hb_store_lmdb, <<"irr-irr">>),
    Node = node_message(#{ <<"store">> => [Main], <<"irreplaceable-store">> => Irr }),
    IrrT = Irr#{ <<"store-failure">> => throw },
    ?assertEqual([Main, IrrT#{ <<"access">> => [<<"read">>] }], maps:get(<<"store">>, Node)),
    ?assertEqual([IrrT], maps:get(<<"scheduler-store">>, Node)).

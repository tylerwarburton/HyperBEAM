%%% @doc Verified community reads through independent HyperBEAM HTTP servers.
%%% Each peer has its own wallet and store.
-module(hb_gateway_read_examples).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Remote reads preserve values and reject a different message's ID.
verified_read_test_() ->
    {timeout, 60, fun verified_read/0}.
verified_read() ->
    {Peer, PeerOpts} = start_node(#{}),
    ReaderOpts = #{ <<"store">> => [hb_test_utils:test_store()] },
    Remote = remote(Peer),
    try
        Message = #{ <<"body">> => <<"community content">> },
        {ok, ID} = hb_cache:write(Message, PeerOpts),
        {ok, Result} = hb_store:read(Remote, ID, ReaderOpts),
        ?assertEqual(Message, hb_message:uncommitted(Result)),
        Missing = hb_util:human_id(crypto:strong_rand_bytes(32)),
        ok = hb_cache:link(ID, Missing, PeerOpts),
        ?assertEqual(
            {error, not_found}, hb_store:read(Remote, Missing, ReaderOpts)
        ),
        % Signed nested data retains its commitments after the remote read.
        Signed =
            hb_message:commit(
                #{
                    <<"body">> => <<"signed content">>,
                    <<"child">> => #{ <<"body">> => <<"nested content">> }
                },
                PeerOpts
            ),
        {ok, _} = hb_cache:write(Signed, PeerOpts),
        SignedID = hb_message:id(Signed, all, PeerOpts),
        {ok, SignedResult} =
            hb_cache:read(SignedID, ReaderOpts#{ <<"store">> => [Remote] }),
        ?assert(hb_message:verify(SignedResult, all, ReaderOpts)),
        ?assertEqual(SignedID, hb_message:id(SignedResult, all, ReaderOpts))
    after
        stop_node(PeerOpts),
        hb_store:stop(maps:get(<<"store">>, ReaderOpts))
    end.

%% @doc A matching commitment ID does not excuse a modified signed value.
tampered_reply_test_() ->
    {timeout, 60, fun tampered_reply/0}.
tampered_reply() ->
    {Peer, PeerOpts} =
        start_node(#{
            <<"on">> => #{
                <<"response">> => #{
                    <<"device">> => #{
                        response => fun(_Base, Req, Opts) ->
                            Reply = hb_maps:get(<<"body">>, Req, Opts),
                            {ok, Req#{
                                <<"body">> => Reply#{ <<"body">> => <<"altered">> }
                            }}
                        end
                    }
                }
            }
        }),
    ReaderOpts = #{ <<"store">> => [hb_test_utils:test_store()] },
    try
        Signed = hb_message:commit(#{ <<"body">> => <<"original">> }, PeerOpts),
        {ok, _} = hb_cache:write(Signed, PeerOpts),
        ID = hb_message:id(Signed, all, PeerOpts),
        {ok, Reply} =
            hb_http:get(Peer, <<"/~cache@1.0/read=", ID/binary>>, ReaderOpts),
        ?assert(hb_maps:is_key(ID, hb_maps:get(<<"commitments">>, Reply))),
        ?assertNot(hb_message:verify(Reply, all, ReaderOpts)),
        ?assertEqual(
            {error, not_found}, hb_store:read(remote(Peer), ID, ReaderOpts)
        )
    after
        stop_node(PeerOpts),
        hb_store:stop(maps:get(<<"store">>, ReaderOpts))
    end.

%% @doc Serial and parallel backstops reject wrong IDs before selecting a peer.
backstops_test_() ->
    [
        {atom_to_list(Mode), {timeout, 60, fun() -> backstops(Mode) end}}
    || Mode <- [serial, parallel]
    ].
backstops(Mode) ->
    {Good, GoodOpts} = start_node(#{}),
    {Bad, BadOpts} = start_node(#{}),
    ReaderOpts = #{ <<"store">> => [hb_test_utils:test_store()] },
    Stores = [remote(Bad), remote(Good)],
    Store =
        case Mode of
            serial -> Stores;
            parallel ->
                (hb_test_utils:test_store(hb_store_multi))#{
                    <<"scope">> => remote,
                    <<"stores">> => Stores
                }
        end,
    try
        {ok, ID} = hb_cache:write(#{ <<"body">> => <<"correct">> }, GoodOpts),
        {ok, WrongID} = hb_cache:write(#{ <<"body">> => <<"wrong">> }, BadOpts),
        ok = hb_cache:link(WrongID, ID, BadOpts),
        Missing = hb_util:human_id(crypto:strong_rand_bytes(32)),
        ?assertEqual(
            {error, not_found}, hb_store:read(Store, Missing, ReaderOpts)
        ),
        lists:foreach(
            fun(_) ->
                {ok, Result} = hb_store:read(Store, ID, ReaderOpts),
                ?assertEqual(<<"correct">>, hb_maps:get(<<"body">>, Result))
            end,
            lists:seq(1, 3)
        )
    after
        hb_store:stop(Store),
        stop_node(GoodOpts),
        stop_node(BadOpts),
        hb_store:stop(maps:get(<<"store">>, ReaderOpts))
    end.

%% @doc Configure a read-only community peer without trusting non-ID paths.
remote(Peer) ->
    #{
        <<"store-module">> => hb_store_remote_node,
        <<"node">> => Peer
    }.

%% @doc Start an independent HTTP node and retain its options for cleanup.
start_node(Extra) ->
    URL =
        hb_http_server:start_node(
            maps:merge(
                #{
                    <<"store">> => [hb_test_utils:test_store()],
                    <<"priv-wallet">> => ar_wallet:new(),
                    <<"port">> => 0
                },
                Extra
            )
        ),
    {URL, hb_http_server:get_opts()}.

%% @doc Stop only the test node's listener and store.
stop_node(Opts) ->
    cowboy:stop_listener(maps:get(<<"http-server">>, Opts)),
    hb_store:stop(maps:get(<<"store">>, Opts)).

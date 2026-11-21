%%% @doc Cross-language fixtures signed by the deployment client. No private
%%% keys are stored: every fixture contains only public signed ANS-104 items.
-module(hb_metadata_test_vectors).
-include_lib("eunit/include/eunit.hrl").

signed_process_links_test_() ->
    [{Name, {timeout, 60, fun() -> signed_process_links(Name) end}}
        || Name <- ["signed-process", "signed-process-header-authority"]].

signed_process_links(Name) ->
    {ok, JSON} = file:read_file("test/metadata/" ++ Name ++ ".json"),
    #{<<"id">> := RootID, <<"items">> := Items,
      <<"source">> := Source, <<"authorities">> := Authorities} = hb_json:decode(JSON),
    Opts = #{<<"store">> => [hb_test_utils:test_store()],
        <<"priv-wallet">> => ar_wallet:load_keyfile("test/key-1.json"),
        <<"scheduling-mode">> => local_confirmation,
        <<"scheduler-publish-remote">> => false},
    [begin
        #{<<"id">> := ID, <<"data">> := Encoded} = Item,
        TX = ar_bundles:deserialize(base64:decode(Encoded)),
        ?assert(ar_bundles:verify_item(TX)),
        ?assertEqual(ID, hb_util:human_id(ar_bundles:id(TX, signed))),
        Message = hb_message:convert(TX, tabm, <<"ans104@1.0">>, Opts),
        {ok, _} = hb_cache:write(Message, Opts),
        Message
    end || Item <- Items],
    {ok, Root} = hb_cache:read(RootID, Opts),
    ?assert(hb_message:verify(Root, signers, Opts)),
    ?assertEqual(RootID, hb_message:id(Root, signed, Opts)),
    Data = {as, <<"message@1.0">>, Root},
    ?assertEqual(<<"Process">>, hb_ao:get(<<"type">>, Data, Opts)),
    ?assertEqual(Source, hb_ao:get(<<"module/body">>, Data, Opts)),
    ?assertEqual(3, hb_ao:get(<<"count">>, Data, Opts)),
    ?assertEqual(true, hb_ao:get(<<"enabled">>, Data, Opts)),
    ?assertEqual(Authorities, hb_ao:get(<<"authority">>, Data, Opts)),
    Loaded = hb_cache:ensure_all_loaded(Root, Opts),
    ?assert(hb_message:verify(Loaded, signers, Opts)),
    Module = maps:get(<<"module">>, Loaded),
    Tampered = Loaded#{<<"module">> => Module#{<<"body">> => <<"tampered executable">>}},
    ?assertThrow({process_not_verified, _}, hb_ao:resolve(
        #{<<"device">> => <<"scheduler@1.0">>},
        #{<<"path">> => <<"schedule">>, <<"method">> => <<"POST">>, <<"body">> => Tampered}, Opts)),
    {ok, Assignment} = hb_ao:resolve(#{<<"device">> => <<"scheduler@1.0">>},
        #{<<"path">> => <<"schedule">>, <<"method">> => <<"POST">>, <<"body">> => Root}, Opts),
    ?assertEqual(RootID, hb_ao:get(<<"process">>, Assignment, Opts)).

peer_cache_reads_do_not_recurse_test() ->
    Backing = hb_test_utils:test_store(hb_store_fs),
    {ok, ID} = hb_cache:write(#{<<"payload">> => <<"remote-only">>},
        #{<<"store">> => [Backing]}),
    Opts = #{<<"store">> => [hb_test_utils:test_store(), Backing#{<<"scope">> => remote}]},
    Base = #{<<"device">> => <<"cache@1.0">>},
    Req = #{<<"path">> => <<"read">>, <<"read">> => ID},
    ?assertMatch({error, _}, hb_ao:resolve(Base, Req#{<<"local-only">> => true}, Opts)),
    {ok, Found} = hb_ao:resolve(Base, Req, Opts),
    ?assertEqual(<<"remote-only">>, hb_ao:get(<<"payload">>, Found, Opts)).

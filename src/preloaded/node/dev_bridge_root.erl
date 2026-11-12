%%% @doc Inline root certificates for data-free Arweave transactions.
%%% The original wallet authorization remains ANS-104/RSA. Committee proofs
%%% use the Ed25519 public keys already bound to members in the root's Sui
%%% registry, with a domain distinct from every external-chain operation.
%%% This device has no signing key or network dependency. The root contract
%%% supplies its own current membership, epoch and process ID.
-module(dev_bridge_root).
-implements(<<"bridge-root@1.0">>).
-export([device_info/0, domain/3, verify/3, assemble/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DOMAIN, <<"PUSD_ROOT_CERTIFICATE_V1">>).

device_info() -> #{exports => [<<"domain">>, <<"verify">>, <<"assemble">>]}.

%% @doc Assemble an independently verifiable carrier from the signed author
%% intent and available committee proofs. Bad or duplicate responses cannot
%% lower the threshold or veto a valid majority. The caller supplies membership
%% from its verified root view; publication grants no authority of its own.
assemble(_Base, Req, Opts) ->
    try
        Root = maps:get(<<"root">>, Req),
        Epoch = hb_util:int(maps:get(<<"epoch">>, Req)),
        Members = hb_util:message_to_ordered_list(maps:get(<<"committee">>, Req)),
        Item = hb_message:convert(maps:get(<<"intent">>, Req), <<"ans104@1.0">>, Opts),
        Root = hb_util:human_id(Item#tx.target),
        Fields = maps:from_list(Item#tx.tags),
        true = map_size(Fields) =:= length(Item#tx.tags),
        Operation0 = maps:without([<<"author">>, <<"certificate-version">>], Fields),
        Operation = case Item#tx.anchor of
            <<>> -> Operation0;
            Anchor -> Operation0#{<<"anchor">> => hb_util:human_id(Anchor)}
        end,
        IntentID = hb_util:human_id(Item#tx.id),
        Bytes = signing_bytes(Root, Epoch, IntentID),
        Responses = hb_util:message_to_ordered_list(maps:get(<<"proofs">>, Req)),
        Available = [begin
            case valid_member_proof(Member, Responses, Bytes) of
                false -> <<>>;
                {ok, Sig} -> <<Index:16/big, Sig/binary>>
            end
        end || {Index, Member} <- lists:zip(lists:seq(0, length(Members) - 1), Members)],
        Proofs = iolist_to_binary(lists:sublist([P || P <- Available, P =/= <<>>],
            length(Members) div 2 + 1)),
        Verify = #{<<"path">> => <<"verify">>, <<"root">> => Root, <<"epoch">> => Epoch,
            <<"committee">> => Members, <<"intent-id">> => IntentID,
            <<"operation">> => hb_json:encode(Operation),
            <<"authorization">> => <<(Item#tx.owner)/binary, (Item#tx.signature)/binary>>,
            <<"endorsements">> => Proofs},
        {ok, _} = hb_ao:resolve(#{<<"device">> => <<"bridge-root@1.0">>}, Verify, Opts),
        Tags = #{<<"Data-Protocol">> => <<"ao">>, <<"Variant">> => <<"ao.N.1">>,
            <<"Action">> => <<"Root-Execute">>, <<"Intent-Id">> => IntentID,
            <<"Root-Operation">> => maps:get(<<"operation">>, Verify),
            <<"Root-Authorization">> => maps:get(<<"authorization">>, Verify),
            <<"Root-Endorsements">> => Proofs},
        AllTags = [{<<"Assign-To">>, Root}, {<<"Type">>, <<"Message">>} | maps:to_list(Tags)],
        true = lists:sum([byte_size(K) + byte_size(V) || {K, V} <- AllTags]) =< 2048,
        % Tag names are values, so AO-Core's key normalization preserves their
        % spelling for Arweave explorers when the publisher serializes them.
        {ok, #{<<"tags">> => [#{<<"name">> => K, <<"value">> => V} || {K, V} <- AllTags],
            <<"intent-id">> => IntentID}}
    catch _:_ -> {error, <<"Cannot assemble inline root certificate">>}
    end.

%% @doc Select one valid proof for this registered member; malformed responses
%% are untrusted input, not a reason to discard other members' signatures.
valid_member_proof(_Member, [], _Bytes) -> false;
valid_member_proof(Member, [Response | Rest], Bytes) ->
    Valid = try
        true = maps:get(<<"signer">>, Response) =:= maps:get(<<"ao">>, Member),
        Signature = hb_util:decode(maps:get(<<"signature">>, Response)),
        64 = byte_size(Signature),
        true = crypto:verify(eddsa, none, Bytes, Signature, [member_key(Member), ed25519]),
        {ok, Signature}
    catch _:_ -> false
    end,
    case Valid of
        false -> valid_member_proof(Member, Rest, Bytes);
        _ -> Valid
    end.

%% @doc The fixed signing domain binds a wallet intent to one root and epoch.
domain(_Base, Req, _Opts) ->
    try
        {ok, signing_bytes(maps:get(<<"root">>, Req),
            hb_util:int(maps:get(<<"epoch">>, Req)),
            maps:get(<<"intent-id">>, Req))}
    catch _:_ -> {error, <<"Invalid root certificate domain">>}
    end.

%% @doc Verify all inline data before returning the original authorized
%% operation. No publisher identity or caller-provided signer count is used.
verify(_Base, Req, Opts) ->
    try
        Root = maps:get(<<"root">>, Req),
        Epoch = hb_util:int(maps:get(<<"epoch">>, Req)),
        OperationBytes = maps:get(<<"operation">>, Req),
        true = is_binary(OperationBytes) andalso byte_size(OperationBytes) =< 2048,
        Operation = hb_json:decode(OperationBytes),
        true = is_map(Operation),
        true = lists:all(fun({K, V}) ->
            is_binary(K) andalso is_binary(V) andalso
                re:run(K, <<"^[a-z][a-z0-9-]*$">>, [{capture, none}]) =:= match
                andalso not hb_private:is_private(K)
                andalso not lists:member(K, [<<"target">>, <<"author">>,
                    <<"certificate-version">>, <<"commitments">>,
                    <<"ao-types">>, <<"body">>, <<"data">>])
        end, maps:to_list(Operation)),
        EpochText = integer_to_binary(Epoch),
        EpochText = maps:get(<<"attestor-epoch">>, Operation),
        <<Owner:512/binary, Signature:512/binary>> = maps:get(<<"authorization">>, Req),
        Author = hb_util:human_id(crypto:hash(sha256, Owner)),
        Payload = Operation#{<<"target">> => Root, <<"author">> => Author,
            <<"certificate-version">> => <<"pusd-root/v1">>},
        Anchor = case maps:get(<<"anchor">>, Payload, not_found) of
            not_found -> <<>>;
            EncodedAnchor -> native_id(EncodedAnchor)
        end,
        Item = ar_tx:reset_ids(#tx{format = ans104, owner = Owner,
            signature = Signature, target = native_id(Root), anchor = Anchor,
            tags = lists:sort(maps:to_list(maps:without([<<"target">>, <<"anchor">>], Payload)))}),
        true = ar_bundles:verify_item(Item),
        IntentID = hb_util:human_id(Item#tx.id),
        IntentID = maps:get(<<"intent-id">>, Req),
        Members = hb_util:message_to_ordered_list(maps:get(<<"committee">>, Req)),
        true = Members =/= [],
        Addresses = [maps:get(<<"ao">>, Member) || Member <- Members],
        true = length(Addresses) =:= length(lists:usort(Addresses)),
        Keys = [member_key(Member) || Member <- Members],
        true = length(Keys) =:= length(lists:usort(Keys)),
        Proofs = maps:get(<<"endorsements">>, Req),
        true = is_binary(Proofs) andalso byte_size(Proofs) =< 2048,
        Signers = verify_proofs(Proofs, Members,
            signing_bytes(Root, Epoch, IntentID), -1, [], []),
        true = length(Signers) > length(Members) div 2,
        Message = hb_message:convert(Item, <<"structured@1.0">>, <<"ans104@1.0">>, Opts),
        {ok, #{<<"operation">> => Message, <<"intent-id">> => IntentID,
            <<"signers">> => Signers}}
    catch _:_ -> {error, <<"Invalid inline root certificate">>}
    end.

%% @doc Proofs are sorted unique committee indices followed by 64-byte
%% signatures. Reusing a public key under two member records counts nowhere.
verify_proofs(<<>>, _Members, _Bytes, _Previous, _Keys, Signers) ->
    lists:reverse(Signers);
verify_proofs(<<Index:16/big, Signature:64/binary, Rest/binary>>,
        Members, Bytes, Previous, Keys, Signers) when Index > Previous ->
    Member = lists:nth(Index + 1, Members),
    PublicKey = member_key(Member),
    false = lists:member(PublicKey, Keys),
    true = crypto:verify(eddsa, none, Bytes, Signature, [PublicKey, ed25519]),
    verify_proofs(Rest, Members, Bytes, Index, [PublicKey | Keys],
        [maps:get(<<"ao">>, Member) | Signers]).

member_key(#{<<"sui">> := <<"0x", Hex:64/binary>>}) -> binary:decode_hex(Hex).

signing_bytes(Root, Epoch, IntentID) when Epoch >= 0, Epoch < 18446744073709551616 ->
    <<?DOMAIN/binary, (native_id(Root))/binary, Epoch:64/big, (native_id(IntentID))/binary>>.

%% @doc Reject noncanonical IDs, including alternate encodings of padding bits.
native_id(ID) when is_binary(ID), byte_size(ID) =:= 43 ->
    Native = hb_util:decode(ID),
    32 = byte_size(Native),
    ID = hb_util:human_id(Native),
    Native.

inline_certificate_test_() ->
    {timeout, 120, fun inline_certificate/0}.

%% @doc Exercise the device with real ANS-104 authorization and independent
%% Ed25519 committee keys, including the three-to-five member threshold change.
inline_certificate() ->
    hb:init(),
    Opts = #{<<"store">> => hb_test_utils:test_store()},
    Wallet = ar_wallet:new(),
    Author = hb_util:human_id(ar_wallet:to_address(Wallet)),
    Root = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Operation = #{<<"action">> => <<"Transfer">>, <<"quantity">> => <<"50">>,
        <<"recipient">> => Root, <<"attestor-epoch">> => <<"7">>,
        <<"root-sequence">> => <<"1">>},
    Signed = hb_message:commit(Operation#{<<"target">> => Root,
        <<"author">> => Author, <<"certificate-version">> => <<"pusd-root/v1">>},
        #{<<"priv-wallet">> => Wallet}, <<"ans104@1.0">>),
    Item = hb_message:convert(Signed, <<"ans104@1.0">>, Opts),
    IntentID = hb_util:human_id(Item#tx.id),
    Device = #{<<"device">> => <<"bridge-root@1.0">>},
    {ok, Bytes} = hb_ao:resolve(Device, #{<<"path">> => <<"domain">>,
        <<"root">> => Root, <<"epoch">> => 7, <<"intent-id">> => IntentID}, Opts),
    Seeds = [<<I:256>> || I <- lists:seq(1, 5)],
    Members = [begin
        {Pub, _} = crypto:generate_key(eddsa, ed25519, Seed),
        #{<<"ao">> => hb_util:human_id(crypto:hash(sha256, Pub)),
            <<"sui">> => <<"0x", (binary:encode_hex(Pub))/binary>>}
    end || Seed <- Seeds],
    Proofs = fun(N) -> iolist_to_binary([begin
        Sig = crypto:sign(eddsa, none, Bytes, [lists:nth(I + 1, Seeds), ed25519]),
        <<I:16/big, Sig/binary>>
    end || I <- lists:seq(0, N - 1)]) end,
    Req = #{<<"path">> => <<"verify">>, <<"root">> => Root, <<"epoch">> => 7,
        <<"intent-id">> => IntentID,
        <<"committee">> => lists:sublist(Members, 3),
        <<"operation">> => hb_json:encode(Operation),
        <<"authorization">> => <<(Item#tx.owner)/binary, (Item#tx.signature)/binary>>,
        <<"endorsements">> => Proofs(2)},
    Run = fun(R) -> hb_ao:resolve(Device, R, Opts) end,
    Responses = [#{<<"signer">> => maps:get(<<"ao">>, lists:nth(I + 1, Members)),
        <<"signature">> => hb_util:encode(crypto:sign(eddsa, none, Bytes,
            [lists:nth(I + 1, Seeds), ed25519]))} || I <- lists:seq(0, 2)],
    Assemble = #{<<"path">> => <<"assemble">>, <<"root">> => Root, <<"epoch">> => 7,
        <<"committee">> => lists:sublist(Members, 3), <<"intent">> => Signed,
        <<"proofs">> => lists:sublist(Responses, 2)},
    {ok, #{<<"tags">> := TagRows, <<"intent-id">> := IntentID}} = Run(Assemble),
    Tags = maps:from_list([{maps:get(<<"name">>, T), maps:get(<<"value">>, T)}
        || T <- hb_util:message_to_ordered_list(TagRows)]),
    ?assertEqual(Proofs(2), maps:get(<<"Root-Endorsements">>, Tags)),
    ?assertMatch({ok, _}, Run(Assemble#{<<"proofs">> => [#{}, hd(Responses) | Responses]})),
    ?assertMatch({error, _}, Run(Assemble#{<<"proofs">> => [hd(Responses), hd(Responses)]})),
    ?assertMatch({error, _}, Run(Assemble#{<<"committee">> => Members})),
    ?assertMatch({ok, _}, Run(Assemble#{<<"committee">> => Members, <<"proofs">> => Responses})),
    ?assertMatch({error, _}, Run(Assemble#{<<"epoch">> => 8})),
    {ok, #{<<"operation">> := Verified, <<"intent-id">> := IntentID}} = Run(Req),
    ?assert(hb_message:verify(Verified, [Author], Opts)),
    ?assertMatch({error, _}, Run(Req#{<<"endorsements">> => Proofs(1)})),
    ?assertMatch({error, _}, Run(Req#{<<"committee">> => Members})),
    ?assertMatch({ok, _}, Run(Req#{<<"committee">> => Members,
        <<"endorsements">> => Proofs(3)})),
    ?assertMatch({error, _}, Run(Req#{<<"epoch">> => 8})),
    ?assertMatch({error, _}, Run(Req#{<<"operation">> =>
        hb_json:encode(Operation#{<<"quantity">> => <<"5000">>})})),
    One = Proofs(1),
    ?assertMatch({error, _}, Run(Req#{<<"endorsements">> => <<One/binary, One/binary>>})),
    ?assertMatch({error, _}, Run(Req#{<<"endorsements">> => <<0:16, 0:512, 1:16, 0:512>>})),
    ?assertMatch({error, _}, Run(Req#{<<"authorization">> => <<0:8192>>})).

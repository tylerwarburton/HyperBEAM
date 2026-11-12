%%% @doc Inline root certificates for data-free Arweave transactions.
%%%
%%% The certificate lives entirely in the carrier's tags: the Arweave scheduler
%%% orders data-free transaction headers only, because a header can be
%%% confirmed while its data is withheld, and an ordering that waited for (or
%%% skipped) withheld data would stall or diverge. Arweave caps a header's tag
%%% names and values at 2,048 bytes, so the certificate's size must not grow
%%% with the committee:
%%%
%%%   Root-Operation      the operation's fields, as JSON
%%%   Root-Authorization  the author's RSA owner and ANS-104 signature
%%%   Root-Endorsements   a signer bitmap (ceil(n/8) bytes, bit i of byte i div
%%%                       8 is member i, least significant first, unused bits
%%%                       zero) followed by ONE 96-byte BLS12-381 aggregate
%%%                       signature of those members over the domain bytes
%%%
%%% Members' BLS keys (`bls', 48-byte compressed G1) are admitted to the root's
%%% committee only with a proof of possession (`possession/3'), which is what
%%% makes summing them safe against rogue keys. Both BLS messages carry their
%%% own domain separation tag, distinct from the RNG/timelock ciphersuite, and
%%% their own message prefix. This device has no signing key or network
%%% dependency; the root supplies its own membership, epoch and process ID.
-module(dev_bridge_root).
-implements(<<"bridge-root@1.0">>).
-device_libraries([lib_bridge_bls]).
-export([device_info/0, domain/3, verify/3, assemble/3, possession/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% The tags and message layouts are lib_bridge_bls's, shared with the
%% attestor that signs them.
-define(DST, (lib_bridge_bls:dst())).
-define(POP_DST, (lib_bridge_bls:pop_dst())).
%% The chains hold at most 16 attestors.
-define(MAX_MEMBERS, 16).

device_info() -> #{exports => [<<"domain">>, <<"verify">>, <<"assemble">>, <<"possession">>]}.

%% @doc Assemble an independently verifiable carrier from the signed author
%% intent and available member endorsements (`signer', `signature': a
%% member's 96-byte BLS signature over the domain bytes). Bad or duplicate
%% responses cannot lower the threshold or veto a valid majority. The caller
%% supplies membership from its verified root view; publication grants no
%% authority of its own.
assemble(_Base, Req, Opts) ->
    try
        Root = maps:get(<<"root">>, Req),
        Epoch = hb_util:int(maps:get(<<"epoch">>, Req)),
        Members = committee(maps:get(<<"committee">>, Req)),
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
        Signed = [{Index, Sig} || {Index, Member} <- lists:zip(lists:seq(0, length(Members) - 1), Members),
            {ok, Sig} <- [valid_member_proof(Member, Responses, Bytes)]],
        Proofs = endorsements(length(Members), Signed),
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

%% @doc The bitmap of a strict majority of signers (the lowest indices) and
%% their aggregate signature.
endorsements(N, Signed) ->
    Chosen = lists:sublist(lists:ukeysort(1, Signed), N div 2 + 1),
    true = length(Chosen) =:= N div 2 + 1,
    Aggregate = blst_nif:aggregate([Sig || {_, Sig} <- Chosen]),
    96 = byte_size(Aggregate),
    <<(bitmap(N, [I || {I, _} <- Chosen]))/binary, Aggregate/binary>>.

bitmap(N, Indices) ->
    Bytes = (N + 7) div 8,
    Bits = lists:foldl(fun(I, Acc) -> Acc bor (1 bsl I) end, 0, Indices),
    <<Bits:(Bytes * 8)/little>>.

%% @doc Select one valid endorsement for this registered member; malformed
%% responses are untrusted input, not a reason to discard other members'.
valid_member_proof(_Member, [], _Bytes) -> false;
valid_member_proof(Member, [Response | Rest], Bytes) ->
    Valid = try
        true = maps:get(<<"signer">>, Response) =:= maps:get(<<"ao">>, Member),
        Signature = hb_util:decode(maps:get(<<"signature">>, Response)),
        96 = byte_size(Signature),
        true = blst_nif:fast_aggregate_verify([member_key(Member)], Bytes, Signature, ?DST),
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

%% @doc A member's proof that it holds the secret of its BLS key: its
%% signature, under the possession tag, over the possession domain, its AO
%% address and the key itself. A key is admitted to a committee only with
%% one, so no member can register a key derived from others' to cancel them
%% out of an aggregate.
possession(_Base, Req, _Opts) ->
    try
        Key = key_bytes(maps:get(<<"bls">>, Req)),
        Proof = decode_any(maps:get(<<"pop">>, Req)),
        96 = byte_size(Proof),
        true = blst_nif:fast_aggregate_verify([Key],
            possession_bytes(maps:get(<<"ao">>, Req), Key), Proof, ?POP_DST),
        {ok, <<"valid">>}
    catch _:_ -> {error, <<"Invalid BLS proof of possession">>}
    end.

possession_bytes(AO, Key) -> lib_bridge_bls:possession_bytes(AO, Key).

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
        Members = committee(maps:get(<<"committee">>, Req)),
        Signers = verify_endorsements(maps:get(<<"endorsements">>, Req), Members,
            signing_bytes(Root, Epoch, IntentID)),
        Message = hb_message:convert(Item, <<"structured@1.0">>, <<"ans104@1.0">>, Opts),
        {ok, #{<<"operation">> => Message, <<"intent-id">> => IntentID,
            <<"signers">> => Signers}}
    catch _:_ -> {error, <<"Invalid inline root certificate">>}
    end.

%% @doc 1 to 16 members, no repeated address or BLS key.
committee(Raw) ->
    Members = hb_util:message_to_ordered_list(Raw),
    N = length(Members),
    true = N >= 1 andalso N =< ?MAX_MEMBERS,
    Addresses = [maps:get(<<"ao">>, M) || M <- Members],
    true = length(lists:usort(Addresses)) =:= N,
    Keys = [member_key(M) || M <- Members],
    true = length(lists:usort(Keys)) =:= N,
    Members.

%% @doc The bitmap is exactly ceil(n/8) bytes with no bit set past member n-1,
%% names a strict majority, and the aggregate verifies under exactly the sum of
%% those members' registered (possession-proven) keys. Returns the signers.
verify_endorsements(Proofs, Members, Bytes) ->
    N = length(Members),
    MapBytes = (N + 7) div 8,
    <<Map:MapBytes/binary, Aggregate:96/binary>> = Proofs,
    <<Bits:(MapBytes * 8)/little>> = Map,
    true = Bits < (1 bsl N),
    Indices = [I || I <- lists:seq(0, N - 1), Bits band (1 bsl I) =/= 0],
    true = length(Indices) > N div 2,
    Chosen = [lists:nth(I + 1, Members) || I <- Indices],
    true = blst_nif:fast_aggregate_verify([member_key(M) || M <- Chosen], Bytes, Aggregate, ?DST),
    [maps:get(<<"ao">>, M) || M <- Chosen].

member_key(#{<<"bls">> := Key}) -> key_bytes(Key).

key_bytes(<<"0x", Hex:96/binary>>) -> binary:decode_hex(Hex);
key_bytes(Key) when byte_size(Key) =:= 48 -> Key.

decode_any(<<"0x", Hex/binary>>) -> binary:decode_hex(Hex);
decode_any(B) when byte_size(B) =:= 96 -> B;
decode_any(B) -> hb_util:decode(B).

signing_bytes(Root, Epoch, IntentID) -> lib_bridge_bls:signing_bytes(Root, Epoch, IntentID).

native_id(ID) -> lib_bridge_bls:native_id(ID).

%%% Tests ---------------------------------------------------------------------

%% A member: a BLS key with its proof of possession, as a node registers it.
test_member(I) ->
    {Secret, Key} = blst_nif:keygen(<<I:256>>),
    AO = hb_util:human_id(crypto:hash(sha256, <<"member", I:32>>)),
    #{secret => Secret, member => #{<<"ao">> => AO,
        <<"bls">> => <<"0x", (binary:encode_hex(Key, lowercase))/binary>>},
        pop => hb_util:encode(blst_nif:sign(Secret, possession_bytes(AO, Key), ?POP_DST))}.

inline_certificate_test_() ->
    {timeout, 120, fun inline_certificate/0}.

%% @doc Real ANS-104 authorization and real BLS aggregates: n=3/k=2,
%% n=9/k=5 and n=16/k=9 committees, forged bitmaps and aggregates refused.
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
    Run = fun(R) -> hb_ao:resolve(Device, R, Opts) end,
    {ok, Bytes} = Run(#{<<"path">> => <<"domain">>, <<"root">> => Root, <<"epoch">> => 7,
        <<"intent-id">> => IntentID}),
    All = [test_member(I) || I <- lists:seq(1, 16)],
    Sign = fun(T) -> blst_nif:sign(maps:get(secret, T), Bytes, ?DST) end,
    MapLen = fun(N) -> (N + 7) div 8 end,
    Req = fun(N, Endorsements) -> #{<<"path">> => <<"verify">>, <<"root">> => Root, <<"epoch">> => 7,
        <<"intent-id">> => IntentID,
        <<"committee">> => [maps:get(member, T) || T <- lists:sublist(All, N)],
        <<"operation">> => hb_json:encode(Operation),
        <<"authorization">> => <<(Item#tx.owner)/binary, (Item#tx.signature)/binary>>,
        <<"endorsements">> => Endorsements} end,
    Endorse = fun(N, Indices) ->
        <<(bitmap(N, Indices))/binary,
          (blst_nif:aggregate([Sign(lists:nth(I + 1, All)) || I <- Indices]))/binary>>
    end,
    lists:foreach(fun({N, K}) ->
        Majority = lists:seq(0, K - 1),
        L = MapLen(N),
        {ok, #{<<"operation">> := Verified, <<"signers">> := Signers}} = Run(Req(N, Endorse(N, Majority))),
        ?assert(hb_message:verify(Verified, [Author], Opts)),
        ?assertEqual(K, length(Signers)),
        % A minority, whoever is in it, is refused.
        ?assertMatch({error, _}, Run(Req(N, Endorse(N, lists:seq(N - K + 1, N - 1))))),
        % The bitmap names a majority but the aggregate is a minority's.
        <<Map:L/binary, Agg/binary>> = Endorse(N, Majority),
        <<_:L/binary, Short/binary>> = Endorse(N, tl(Majority)),
        ?assertMatch({error, _}, Run(Req(N, <<Map/binary, Short/binary>>))),
        % One bit moved to a member who did not sign.
        <<MovedMap:L/binary, _/binary>> = Endorse(N, [N - 1 | tl(Majority)]),
        ?assertMatch({error, _}, Run(Req(N, <<MovedMap/binary, Agg/binary>>))),
        % A bit past the committee, a short or long bitmap, a bad point.
        <<Bits:(L * 8)/little>> = Map,
        case N rem 8 of
            0 -> ok;
            _ -> ?assertMatch({error, _}, Run(Req(N, <<(Bits bor (1 bsl N)):(L * 8)/little, Agg/binary>>)))
        end,
        ?assertMatch({error, _}, Run(Req(N, <<Map/binary, 0, Agg/binary>>))),
        ?assertMatch({error, _}, Run(Req(N, Agg))),
        ?assertMatch({error, _}, Run(Req(N, <<Map/binary, 0:768>>))),
        % The same endorsements at another epoch or for another operation.
        ?assertMatch({error, _}, Run((Req(N, Endorse(N, Majority)))#{<<"epoch">> => 8})),
        ?assertMatch({error, _}, Run((Req(N, Endorse(N, Majority)))#{<<"operation">> =>
            hb_json:encode(Operation#{<<"quantity">> => <<"5000">>})}))
    end, [{3, 2}, {9, 5}, {16, 9}]),
    % More than the chains' 16 members is no committee.
    Seventeen = [maps:get(member, test_member(I)) || I <- lists:seq(1, 17)],
    ?assertMatch({error, _}, Run((Req(16, Endorse(16, lists:seq(0, 8))))#{<<"committee">> => Seventeen})),
    % After a rotation the root hands over the NEW committee: the old members'
    % majority endorses nothing for it, whatever bitmap names it.
    Rotated = [maps:get(member, T) || T <- lists:sublist(All, 4, 3)],
    ?assertMatch({error, _}, Run((Req(3, Endorse(3, [0, 1])))#{<<"committee">> => Rotated})),
    ?assertMatch({error, _}, Run((Req(3, Endorse(3, [0, 1, 2])))#{<<"committee">> => Rotated})),
    % A repeated key is no committee either.
    [M0, M1 | Rest] = [maps:get(member, T) || T <- lists:sublist(All, 3)],
    ?assertMatch({error, _}, Run((Req(3, Endorse(3, [0, 1])))#{<<"committee">> =>
        [M0, M1#{<<"bls">> => maps:get(<<"bls">>, M0)} | Rest]})),
    % Cross-domain replay: the same members signing the same bytes under the
    % RNG/timelock ciphersuite, or as proofs of possession, endorse nothing.
    NulDst = <<"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_">>,
    Foreign = fun(Dst) -> <<(bitmap(3, [0, 1]))/binary, (blst_nif:aggregate(
        [blst_nif:sign(maps:get(secret, lists:nth(I, All)), Bytes, Dst) || I <- [1, 2]]))/binary>> end,
    ?assertMatch({error, _}, Run(Req(3, Foreign(NulDst)))),
    ?assertMatch({error, _}, Run(Req(3, Foreign(?POP_DST)))),
    ?assertMatch({ok, _}, Run(Req(3, Foreign(?DST)))),
    ?assertMatch({error, _}, Run((Req(3, Endorse(3, [0, 1])))#{<<"authorization">> => <<0:8192>>})),
    % Assembly from untrusted responses: bad and duplicate ones cannot veto.
    Responses = [#{<<"signer">> => maps:get(<<"ao">>, maps:get(member, T)),
        <<"signature">> => hb_util:encode(Sign(T))} || T <- lists:sublist(All, 3)],
    Assemble = #{<<"path">> => <<"assemble">>, <<"root">> => Root, <<"epoch">> => 7,
        <<"committee">> => [maps:get(member, T) || T <- lists:sublist(All, 3)], <<"intent">> => Signed,
        <<"proofs">> => lists:sublist(Responses, 2)},
    {ok, #{<<"tags">> := TagRows, <<"intent-id">> := IntentID}} = Run(Assemble),
    Tags = maps:from_list([{maps:get(<<"name">>, T), maps:get(<<"value">>, T)}
        || T <- hb_util:message_to_ordered_list(TagRows)]),
    ?assertEqual(Endorse(3, [0, 1]), maps:get(<<"Root-Endorsements">>, Tags)),
    ?assertMatch({ok, _}, Run(Assemble#{<<"proofs">> => [#{}, hd(Responses) | Responses]})),
    ?assertMatch({error, _}, Run(Assemble#{<<"proofs">> => [hd(Responses), hd(Responses)]})),
    ?assertMatch({error, _}, Run(Assemble#{<<"epoch">> => 8})),
    % A 16-member carrier still fits the 2,048 tag bytes.
    {ok, #{<<"tags">> := Rows16}} = Run(Assemble#{<<"committee">> => [maps:get(member, T) || T <- All],
        <<"proofs">> => [#{<<"signer">> => maps:get(<<"ao">>, maps:get(member, T)),
            <<"signature">> => hb_util:encode(Sign(T))} || T <- All]}),
    ?assert(lists:sum([byte_size(maps:get(<<"name">>, R)) + byte_size(maps:get(<<"value">>, R))
        || R <- hb_util:message_to_ordered_list(Rows16)]) =< 2048).

%% @doc Possession: the member's own proof over its address and key verifies;
%% a missing one, another member's, one for another key, or a signature under
%% the certificate tag does not.
possession_test() ->
    hb:init(),
    Opts = #{<<"store">> => hb_test_utils:test_store()},
    Run = fun(R) -> hb_ao:resolve(#{<<"device">> => <<"bridge-root@1.0">>}, R#{<<"path">> => <<"possession">>}, Opts) end,
    #{member := #{<<"ao">> := AO, <<"bls">> := Key}, pop := Pop, secret := Secret} = test_member(1),
    #{member := #{<<"ao">> := AO2, <<"bls">> := Key2}, pop := Pop2} = test_member(2),
    ?assertMatch({ok, _}, Run(#{<<"ao">> => AO, <<"bls">> => Key, <<"pop">> => Pop})),
    ?assertMatch({error, _}, Run(#{<<"ao">> => AO, <<"bls">> => Key})),
    ?assertMatch({error, _}, Run(#{<<"ao">> => AO, <<"bls">> => Key, <<"pop">> => <<>>})),
    ?assertMatch({error, _}, Run(#{<<"ao">> => AO2, <<"bls">> => Key, <<"pop">> => Pop})),
    ?assertMatch({error, _}, Run(#{<<"ao">> => AO, <<"bls">> => Key2, <<"pop">> => Pop})),
    ?assertMatch({error, _}, Run(#{<<"ao">> => AO2, <<"bls">> => Key2, <<"pop">> => Pop})),
    ?assertMatch({ok, _}, Run(#{<<"ao">> => AO2, <<"bls">> => Key2, <<"pop">> => Pop2})),
    Raw = binary:decode_hex(binary:part(Key, 2, 96)),
    WrongTag = hb_util:encode(blst_nif:sign(Secret, possession_bytes(AO, Raw), ?DST)),
    ?assertMatch({error, _}, Run(#{<<"ao">> => AO, <<"bls">> => Key, <<"pop">> => WrongTag})).

%%% @doc The scheduler's door for messages that claim a process origin.
%%%
%%% A contract believes a `from-process' only when its OWN scheduler admitted
%%% it: the scheduler writes the admission into the assignment it signs (the
%%% `admit-*' keys), never into the message, whose bytes and ID stay as sent.
%%% A message carrying `from-process' is admitted or refused here, never
%%% assigned bare.
%%%
%%% Whose word counts is node policy (`scheduler-admission', rendered from
%%% paralith-purge infra/nodes/trust.json), keyed by the sender's scheduler
%%% address S (its `from-scheduler'). Whatever the policy, S must be the
%%% scheduler the sender's own definition names (its ID is the sender's, so
%%% its bytes vouch for themselves): a scheduler speaks only for the
%%% processes it schedules. Then:
%%%
%%%   - S is this node's own address (`hb_opts:address/1'): `local'. The only
%%%     real signer must be S.
%%%   - S is `trusted': the only real signer must be S. No recompute.
%%%   - S is `verified': S's signature counts for nothing. The entry must be
%%%     one this node recomputed from S's verified, Arweave-confirmed schedule
%%%     (`dev_scheduler_follower'), byte for byte.
%%%   - Anything else: refused.
%%%
%%% Every admitted message must be for the process it is scheduled on, and an
%%% outbox entry (`from-process', `from-slot', `from-outbox-key') is assigned
%%% at most once per process however often it is signed again: its admission
%%% ID joins the message's signature IDs in the scheduler's dedup index.
%%%
%%% Answers: 400 when the message's own bytes can never be admitted anywhere
%%% (a stamp missing or spelled twice, another process's message, a sender
%%% whose definition names another scheduler), which a local deliverer drops
%%% (`lib_process_delivery:by_status/4'); 403 when this node's policy refuses
%%% it (no policy, wrong signer, a commitment that does not verify, a
%%% mismatch, a fork); 503 while a verified entry, or a sender's definition,
%%% is not available yet. A remote deliverer retries all three.
-module(dev_scheduler_admission).
-export([admit/3, check/3, ids/2, keys/2, peer/2, policy/2, real_committers/2]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Commitment algorithms that are signatures. An hmac names whoever it claims
%% to and vouches for nothing (CLAUDE.md, `signer()').
-define(SIGNATURE_ALGS, [<<"rsa-pss-sha512">>, <<"rsa-pss-sha256">>]).
%% The stamps the deliverer writes (`lib_process_delivery:augment_message/3'),
%% by normalised name, and the admission key each becomes.
-define(STAMPS, [
    {<<"fromprocess">>, <<"admit-process">>},
    {<<"fromslot">>, <<"admit-slot">>},
    {<<"fromoutboxkey">>, <<"admit-outbox-key">>},
    {<<"fromentry">>, <<"admit-entry">>},
    {<<"fromscheduler">>, <<"admit-scheduler">>}
]).
-define(PRIV_KEY, <<"scheduler-admission">>).

%% @doc `{ok, Req}' to schedule (with the admission in its private keys when
%% it claims a process origin), or `{error, Response}'.
admit(ProcID, Req, Opts) ->
    try
        case check(ProcID, Req, Opts) of
            none -> {ok, Req};
            {Stamps, Policy, Epoch} ->
                Admission =
                    Stamps#{
                        <<"admit-policy">> => hb_util:bin(Policy),
                        <<"admit-policy-epoch">> => Epoch
                    },
                {ok, hb_private:set(Req, ?PRIV_KEY, Admission, Opts)}
        end
    catch throw:{refused, Status, Why} -> refused(ProcID, Status, Why)
    end.

%% @doc The door's whole judgement of `Req' as an input of `ProcID', without
%% scheduling anything: `none' for a message that claims no process origin,
%% `{Stamps, Policy, Epoch}' for one this node admits, and a thrown
%% `{refused, Status, Why}' otherwise. The follower runs the same function on
%% every input of a schedule it recomputes (`dev_scheduler_follower').
check(ProcID, Req, Opts) ->
    case stamps(Req, Opts) of
        none -> none;
        Stamps = #{ <<"admit-scheduler">> := S, <<"admit-process">> := Sender } ->
            need(for_process(Req, ProcID, Opts), 400, <<"message is for another process">>),
            % Whatever the node's `verify_assignments' says: the policy below
            % reads the commitments' committers.
            need((catch hb_message:verify(Req, all, Opts)) =:= true, 403,
                <<"a commitment does not verify">>),
            {Policy, Epoch} = policy(S, Opts),
            need(Policy =/= none, 403, <<"no admission policy for the sender's scheduler">>),
            need(schedules(S, Sender, Opts), 400,
                <<"the sender's definition names another scheduler">>),
            case Policy of
                verified ->
                    ok = dev_scheduler_follower:check(S, Stamps, Req, ProcID, Opts);
                _ ->
                    need(real_committers(Req, Opts) =:= [hb_committer:uri(S)], 403,
                        <<"the sender's scheduler is not the message's only signer">>)
            end,
            {Stamps, Policy, Epoch}
    end.

%% @doc The admission the door attached to a request, as `admit-*' keys to
%% merge into its assignment; `#{}' for a message that claims no origin.
keys(Msg, Opts) ->
    case hb_private:get(?PRIV_KEY, Msg, not_found, Opts) of
        Admission when is_map(Admission) -> Admission;
        _ -> #{}
    end.

%% @doc The dedup ID of the outbox entry an admission names: `[]' or one ID.
%% Reads `admit-*' keys from any map that holds them, so the same function
%% indexes a request's admission and checks a stored assignment.
ids(Map, Opts) ->
    Get = fun(K) -> hb_maps:get(K, Map, not_found, Opts#{ <<"hashpath">> => ignore }) end,
    case {Get(<<"admit-process">>), Get(<<"admit-slot">>), Get(<<"admit-outbox-key">>)} of
        {P, Slot, Key} when is_binary(P), Slot =/= not_found, Key =/= not_found ->
            [hb_util:human_id(
                crypto:hash(sha256,
                    <<"admit\n", (hb_util:human_id(P))/binary, "\n",
                      (hb_util:bin(hb_util:int(Slot)))/binary, "\n",
                      (hb_util:bin(Key))/binary>>))];
        _ -> []
    end.

%% @doc `{local | trusted | verified | none, Epoch}' for scheduler `S'. Only
%% this node's own address is `local': another identity the node holds is
%% not its scheduler.
policy(S, Opts) ->
    Cfg = hb_opts:get(<<"scheduler-admission">>, #{}, Opts),
    Epoch = hb_util:int(hb_maps:get(<<"epoch">>, Cfg, 0, Opts)),
    Own =
        case hb_opts:address(Opts) of
            undefined -> false;
            Address -> hb_committer:uri(Address) =:= hb_committer:uri(S)
        end,
    case Own of
        true -> {local, Epoch};
        false ->
            case hb_maps:get(<<"policy">>, peer(S, Opts), none, Opts) of
                <<"trusted">> -> {trusted, Epoch};
                <<"verified">> -> {verified, Epoch};
                _ -> {none, Epoch}
            end
    end.

%% @doc Scheduler `S''s entry in the node's `scheduler-admission' peers
%% (`policy', `location'), or `#{}'.
peer(S, Opts) ->
    Cfg = hb_opts:get(<<"scheduler-admission">>, #{}, Opts),
    URI = hb_committer:uri(S),
    case [ V || {A, V} <- maps:to_list(hb_maps:get(<<"peers">>, Cfg, #{}, Opts)),
            hb_committer:uri(A) =:= URI ] of
        [V] -> V;
        _ -> #{}
    end.

%% @doc The committers of `Msg''s commitments that are a real signature
%% (`type' OR `alg'), as committer URIs, sorted and without repeats. Only
%% meaningful once every commitment verified (`check/3' does that first).
real_committers(Msg, Opts) ->
    Comms = hb_maps:get(<<"commitments">>, Msg, #{}, Opts),
    lists:usort(
        [ hb_committer:uri(C)
        || {_, Comm = #{ <<"committer">> := C }} <- maps:to_list(Comms),
           lists:member(
               maps:get(<<"type">>, Comm, maps:get(<<"alg">>, Comm, none)),
               ?SIGNATURE_ALGS) ]).

%%% Internal ------------------------------------------------------------------

%% The `from-*' stamps, as `admit-*' keys: `none' without `from-process'; a
%% refusal when one is spelled twice or one the deliverer always writes is
%% missing. Names are compared lowercased with `-' and `_' removed, as the
%% contracts compare them (CLAUDE.md, tag separators).
stamps(Req, Opts) ->
    Keys = [ K || K <- hb_maps:keys(Req, Opts), is_binary(K) ],
    Found =
        [ {Name, Admit, [ K || K <- Keys, normal(K) =:= Name ]}
        || {Name, Admit} <- ?STAMPS ],
    case lists:keyfind(<<"fromprocess">>, 1, Found) of
        {_, _, []} -> none;
        _ ->
            maps:from_list(
                [ case Ks of
                      [K] -> {Admit, value(Admit, hb_maps:get(K, Req, undefined, Opts))};
                      [] -> throw({refused, 400, <<"missing stamp ", Name/binary>>});
                      _ -> throw({refused, 400, <<"ambiguous stamp ", Name/binary>>})
                  end
                || {Name, Admit, Ks} <- Found ])
    end.

value(Admit, V) ->
    try value2(Admit, V)
    catch _:_ -> throw({refused, 400, <<"unreadable stamp ", Admit/binary>>})
    end.
value2(<<"admit-slot">>, V) -> hb_util:int(V);
value2(<<"admit-process">>, V) -> hb_util:human_id(V);
value2(_, V) when is_binary(V) -> V.

normal(K) ->
    << <<C>> || <<C>> <= string:lowercase(K), C =/= $-, C =/= $_ >>.

%% The message is for `ProcID': its `target' names it, or it is `ProcID''s
%% own definition. (`lib_bridge_replica:message_for_process/4'.)
for_process(Req, ProcID, Opts) ->
    case hb_maps:get(<<"target">>, Req, none, Opts) of
        none ->
            hb_maps:get(<<"type">>, Req, none, Opts) =:= <<"Process">>
                andalso (catch hb_message:id(Req, all, Opts)) =:= hb_util:human_id(ProcID);
        T -> (catch hb_util:human_id(T)) =:= hb_util:human_id(ProcID)
    end.

%% `S' is the one scheduler the sender's definition names, read as a
%% scheduler reads it to sign the sender's assignments
%% (`dev_scheduler_server:commitment_wallets/2'). The definition comes from
%% this node's stores, else from S's node once (then kept here); its ID is
%% `Sender', so where it came from does not matter.
schedules(S, Sender, Opts) ->
    Def = definition(Sender, S, Opts),
    Named = hb_ao:get_first(
        [{Def, <<"scheduler">>}, {Def, <<"scheduler-location">>}], [], Opts),
    (catch dev_scheduler:parse_schedulers(Named)) =:= [hb_committer:uri(S)].

definition(Sender, S, Opts) ->
    SchedulerStore = hb_opts:get(scheduler_store, hb_opts:get(store, no_viable_store, Opts), Opts),
    Here = [ fun() -> hb_cache:read(Sender, O) end
           || O <- [Opts, Opts#{ <<"store">> => SchedulerStore }] ],
    case vouched(Here, Sender, Opts) of
        {ok, Def} -> Def;
        none ->
            Remote =
                case hb_maps:get(<<"location">>, peer(S, Opts), undefined, Opts) of
                    undefined -> [];
                    Url -> [ fun() -> hb_http:get(Url, <<"/", Sender/binary>>, Opts) end ]
                end,
            case vouched(Remote, Sender, Opts) of
                {ok, Def} ->
                    {ok, _} = hb_cache:write(Def, Opts),
                    Def;
                none -> throw({refused, 503, <<"the sender's definition is not available yet">>})
            end
    end.

%% The first candidate that is `Sender''s definition: its ID is `Sender' and
%% every commitment on it verifies.
vouched([], _Sender, _Opts) -> none;
vouched([Get | Rest], Sender, Opts) ->
    Def =
        case catch Get() of
            {ok, Raw} -> catch hb_cache:ensure_all_loaded(Raw, Opts);
            _ -> none
        end,
    case is_map(Def)
            andalso (catch hb_message:id(Def, all, Opts)) =:= Sender
            andalso (catch hb_message:verify(Def, all, Opts)) =:= true of
        true -> {ok, Def};
        false -> vouched(Rest, Sender, Opts)
    end.

need(true, _, _) -> ok;
need(_, Status, Why) -> throw({refused, Status, Why}).

refused(ProcID, Status, Why) ->
    ?event(scheduling, {admission_refused, {proc_id, ProcID}, {status, Status}, {why, Why}}),
    {error, #{ <<"status">> => Status, <<"body">> => Why,
        <<"process">> => hb_util:human_id(ProcID) }}.

%%% Tests ---------------------------------------------------------------------

test_opts() ->
    Store = hb_test_utils:test_store(),
    ok = hb_store:start([Store]),
    #{ <<"priv-wallet">> => ar_wallet:new(), <<"store">> => [Store] }.

addr(W) -> hb_util:human_id(ar_wallet:to_address(W)).

%% A sender process scheduled by `Scheduler', its definition in `Opts''s store.
sender(Scheduler, Opts) ->
    Def = hb_message:commit(
        #{ <<"type">> => <<"Process">>, <<"device">> => <<"process@1.0">>,
           <<"scheduler">> => Scheduler,
           <<"test-random-seed">> => rand:uniform(1 bsl 40) },
        #{ <<"priv-wallet">> => ar_wallet:new() }, <<"httpsig@1.0">>),
    {ok, _} = hb_cache:write(Def, Opts),
    hb_util:human_id(hb_message:id(Def, all, Opts)).

push(Wallet, Target, Overrides, Opts) ->
    hb_message:commit(
        maps:merge(
            #{
                <<"target">> => Target,
                <<"action">> => <<"Credit-Notice">>,
                <<"from-process">> => sender(addr(Wallet), Opts),
                <<"from-slot">> => 7,
                <<"from-outbox-key">> => <<"1">>,
                <<"from-entry">> => hb_util:human_id(crypto:strong_rand_bytes(32)),
                <<"from-scheduler">> => addr(Wallet)
            },
            Overrides),
        #{ <<"priv-wallet">> => Wallet }, <<"httpsig@1.0">>).

with_peer(Opts, W, Policy) ->
    Opts#{ <<"scheduler-admission">> => #{ <<"epoch">> => 3,
        <<"peers">> => #{ addr(W) => #{ <<"policy">> => Policy } } } }.

target() -> hb_util:human_id(crypto:strong_rand_bytes(32)).

status(Res) ->
    case Res of
        {error, #{ <<"status">> := S }} -> S;
        Other -> Other
    end.

ordinary_message_passes_untouched_test() ->
    Opts = test_opts(),
    Msg = hb_message:commit(#{ <<"target">> => <<"x">>, <<"action">> => <<"Ping">> },
        Opts, <<"httpsig@1.0">>),
    ?assertEqual({ok, Msg}, admit(<<"x">>, Msg, Opts)),
    ?assertEqual(#{}, keys(Msg, Opts)).

local_push_admitted_test() ->
    Opts = #{ <<"priv-wallet">> := W } = test_opts(),
    Target = target(),
    {ok, Admitted} = admit(Target, push(W, Target, #{}, Opts), Opts),
    Keys = keys(Admitted, Opts),
    ?assertEqual(<<"local">>, maps:get(<<"admit-policy">>, Keys)),
    ?assertEqual(7, maps:get(<<"admit-slot">>, Keys)),
    ?assertEqual(1, length(ids(Keys, Opts))).

trusted_push_admitted_test() ->
    Peer = ar_wallet:new(),
    Opts = with_peer(test_opts(), Peer, <<"trusted">>),
    Target = target(),
    {ok, Admitted} = admit(Target, push(Peer, Target, #{}, Opts), Opts),
    ?assertMatch(#{ <<"admit-policy">> := <<"trusted">>, <<"admit-policy-epoch">> := 3 },
        keys(Admitted, Opts)).

%% A trusted scheduler speaks for the processes it schedules, and for no
%% other: not another node's, not ours.
trusted_peer_cannot_speak_for_another_schedulers_process_test() ->
    Peer = ar_wallet:new(),
    Opts = #{ <<"priv-wallet">> := Own } = with_peer(test_opts(), Peer, <<"trusted">>),
    Target = target(),
    Elsewhere = sender(addr(ar_wallet:new()), Opts),
    Ours = sender(addr(Own), Opts),
    ?assertEqual(400, status(admit(Target,
        push(Peer, Target, #{ <<"from-process">> => Elsewhere }, Opts), Opts))),
    ?assertEqual(400, status(admit(Target,
        push(Peer, Target, #{ <<"from-process">> => Ours }, Opts), Opts))).

%% A sender whose definition this node cannot find, and whose scheduler has
%% no location to fetch it from, waits.
unknown_sender_waits_test() ->
    Peer = ar_wallet:new(),
    Opts = with_peer(test_opts(), Peer, <<"trusted">>),
    Target = target(),
    Unknown = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assertEqual(503, status(admit(Target,
        push(Peer, Target, #{ <<"from-process">> => Unknown }, Opts), Opts))).

no_policy_refused_test() ->
    Stranger = ar_wallet:new(),
    Opts = test_opts(),
    Target = target(),
    ?assertEqual(403, status(admit(Target, push(Stranger, Target, #{}, Opts), Opts))).

%% Another identity the node holds is not its scheduler.
only_the_nodes_own_address_is_local_test() ->
    Opts = test_opts(),
    Other = ar_wallet:new(),
    ?assertMatch({none, _}, policy(addr(Other),
        Opts#{ <<"identities">> => #{ addr(Other) => #{ <<"priv-wallet">> => Other } } })).

wrong_signer_refused_test() ->
    Peer = ar_wallet:new(),
    Opts = with_peer(test_opts(), Peer, <<"trusted">>),
    Target = target(),
    % Signed by someone else, claiming the trusted peer as its scheduler.
    Forged = push(ar_wallet:new(), Target,
        #{ <<"from-scheduler">> => addr(Peer), <<"from-process">> => sender(addr(Peer), Opts) },
        Opts),
    ?assertEqual(403, status(admit(Target, Forged, Opts))).

%% A commitment that does not verify names nobody, whatever the node's
%% `verify_assignments' option said before the door.
tampered_commitment_refused_test() ->
    Peer = ar_wallet:new(),
    Opts = with_peer(test_opts(), Peer, <<"trusted">>),
    Target = target(),
    Msg = push(Peer, Target, #{}, Opts),
    ?assertEqual(403, status(admit(Target, Msg#{ <<"action">> => <<"Mint">> }, Opts))).

retargeted_refused_test() ->
    Peer = ar_wallet:new(),
    Opts = with_peer(test_opts(), Peer, <<"trusted">>),
    ?assertEqual(400, status(admit(target(), push(Peer, target(), #{}, Opts), Opts))).

ambiguous_stamp_refused_test() ->
    Opts = #{ <<"priv-wallet">> := W } = test_opts(),
    Target = target(),
    ?assertEqual(400, status(admit(Target, push(W, Target, #{ <<"from_slot">> => 8 }, Opts), Opts))).

%% A sender with no scheduler of its own (no `from-scheduler' stamp) is never
%% admissible: refused for good, not retried.
missing_scheduler_stamp_refused_test() ->
    Opts = #{ <<"priv-wallet">> := W } = test_opts(),
    Target = target(),
    Msg = hb_message:commit(
        maps:remove(<<"from-scheduler">>, hb_message:uncommitted(push(W, Target, #{}, Opts))),
        Opts, <<"httpsig@1.0">>),
    ?assertEqual(400, status(admit(Target, Msg, Opts))).

hmac_is_not_a_signer_test() ->
    Opts = #{ <<"priv-wallet">> := W } = test_opts(),
    Msg = push(W, <<"t">>, #{}, Opts),
    ?assertEqual([hb_committer:uri(addr(W))], real_committers(Msg, Opts)).

admission_id_is_the_entry_not_the_signature_test() ->
    Opts = test_opts(),
    A = #{ <<"admit-process">> => hb_util:human_id(<<0:256>>), <<"admit-slot">> => 25,
        <<"admit-outbox-key">> => <<"1">> },
    % `admit-slot' "25" and 25 name the same entry (contracts narrow with int()).
    ?assertEqual(ids(A, Opts), ids(A#{ <<"admit-slot">> => <<"25">> }, Opts)),
    ?assertNotEqual(ids(A, Opts), ids(A#{ <<"admit-outbox-key">> => <<"2">> }, Opts)),
    ?assertEqual([], ids(#{}, Opts)).

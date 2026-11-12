%%% @doc `bridge-attestor@1.0': this node as one attestor of the PUSD bridge
%%% (paralith-mono products/bridge/BRIDGE_NATIVE.md). Formerly `bridge@1.0' on
%%% the old fork; what it signs and sends is unchanged.
%%%
%%% A background loop, started by the node's `start' hook, that on every pass:
%%%
%%%   1. computes PUSD (`bridgeinfo', `withdrawals') from assignments it
%%%      verified itself;
%%%   2. scans Base (through `evm-rpc@1.0'), Solana and Sui for deposits and
%%%      releases since its cursors;
%%%   3. authors a `Bridge-Mint' root certificate for each unminted deposit;
%%%   4. signs each pending withdrawal for its chain, and each applied change
%%%      it was a prior member for, and serves those signatures from
%%%      `chain-sig' (they are never stored on the root);
%%%   5. authors a `Bridge-Released' root certificate for each release it saw;
%%%   6. relays root credits, and authors root certificates for fast-ledger
%%%      exits (`Ledger-Exit') and ledger moves (`Ledger-Move-Root').
%%%
%%% A committee action is never a per-member vote: each is ONE inline root
%%% certificate (pusd.lua Root-Execute) this member signs, a strict majority
%%% endorses through `root-endorse', and one data-free carrier lands, at most
%%% one in flight per member at its next root sequence;
%%%   7. votes on each open membership proposal its operator approved
%%%      (`Attestor-Approve' on PUSD), signing the new set for every chain.
%%%
%%% Succession: when PUSD publishes a `successor' (its set voted to move), the
%%% node moves there, but only if the successor names this PUSD as its
%%% `predecessor' and holds the same set at the same epoch. Where it is is kept
%%% in the `follow-file', so a node that missed several moves walks the chain
%%% one hop per pass. The configured `pusd' is where that walk starts; setting
%%% a different one in the config restarts it from there.
%%%
%%% Every message is committed with the node's own wallet, so the node's address
%%% IS its AO attestor identity. Its Base, Solana and Sui keys live in the
%%% `keys-file', made on first start if absent. The node never pays chain gas:
%%% whoever claims a withdrawal submits the collected signatures.
%%%
%%% Proposals and approvals are made on PUSD by the node's operator, from any
%%% AO wallet; the node only ever signs. Endpoints:
%%%   GET  /~bridge-attestor@1.0/keys    this node's four public keys, to join a set
%%%   GET  /~bridge-attestor@1.0/status  cursors, last pass, last error, RPC hosts
%%%   GET  /~bridge-attestor@1.0/nudge   a bridge transaction was just sent: pass
%%%                                      now, then every `fast-interval' for
%%%                                      `fast-window'
%%%   GET  /~bridge-attestor@1.0/chain-sig?withdrawal=ID | ?proposal=ID
%%%                                      this node's chain signature for a
%%%                                      record its last pass verified
%%%   POST /~bridge-attestor@1.0/root-endorse signs an author-signed root
%%%                                      certificate after local verification
%%%
%%% Passes run every `interval' while idle. A nudge only changes WHEN the next
%%% pass happens, never what it does, so anyone may send one; passes are still
%%% at least `fast-interval' apart however many arrive. A nudge re-arms fast
%%% mode at most once per NUDGE_GAP_MS, and one fast stretch lasts at most three
%%% `fast-window's; after that, nudges wait out COOL_MS.
%%%
%%% The cursors and the deposits and releases still waiting are kept in the
%%% `state-file', so a restart picks up where the node left off.
%%%
%%% One rule: the node acts (attests a deposit, signs a release, acknowledges,
%%% votes, follows a successor) only on PUSD state it computed itself from
%%% assignments it verified (`lib_bridge_replica'). It trusts nothing
%%% another node publishes, its own scheduler's included: every attestor,
%%% wherever PUSD runs, reads PUSD the same way. Until a slot verifies there is
%%% nothing to act on; a slot that does not verify is where the replica stays.
%%%
%%% Configuration, under the node option `bridge':
%%%   pusd, scheduler-url (where PUSD's scheduler is reached: transport only,
%%%   nothing it serves is used unverified), replica-dir (default
%%%   `bridge-replica'), interval, fast-interval, fast-window (ms), keys-file,
%%%   state-file, follow-file,
%%%   fast-ledgers => #{ LedgerProcess => #{ node => SchedulerURL } },
%%%   scheduler-device (`scheduler@1.0' or `arweave-scheduler@1.0'),
%%%   arweave-resend-ms (default 30 minutes),
%%%   base => #{ rpc | rpc-file, contract, chain-id, from-block, log-range },
%%%   solana => #{ rpc | rpc-file, program, commitment }, sui => #{ rpc, package, bridge }
%%% `rpc-file' names a file holding the RPC URL. Use it: a URL carries its
%%% provider key, and everything under `bridge' is served by
%%% GET /~meta@1.0/info/bridge.
-module(dev_bridge_attestor).
-implements(<<"bridge-attestor@1.0">>).
-export([device_info/1, start/3, status/3, keys/3, nudge/3, root_endorse/3, chain_sig/3]).
-export([successor_ok/3]).
-export([withdrawal_signature/5, minted/3]).
-device_libraries([lib_bridge_replica, lib_arweave_scheduler_blocks, lib_bridge_bls]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NAME, {<<"bridge-attestor@1.0">>, loop}).
%% The chain signatures the last pass made, readable by every request without
%% the loop: a flood of claimers never queues behind (or delays) a pass.
-define(CHAIN_SIGS, dev_bridge_attestor_chain_sigs).
-define(RESEND_MS, 60000).
-define(ARWEAVE_RESEND_MS, 1800000).
-define(NUDGE_GAP_MS, 10000).
%% Nudges ignored after a capped fast stretch. Fixed, not `interval': a slow
%% idle interval saves RPC spend and must not slow a busy bridge down.
-define(COOL_MS, 15000).

device_info(_) ->
    #{ exports => [<<"start">>, <<"status">>, <<"keys">>, <<"nudge">>,
        <<"root-endorse">>, <<"chain-sig">>] }.

%%% Endpoints -----------------------------------------------------------------

%% @doc The node `start' hook: spawn the loop once, hand the node message back.
%% Never fails: a bridge problem must not stop the node from booting.
%%
%% The configuration and the loop's options come from `Opts', the node's own
%% message, never from the request: `start' is also reachable over HTTP, and a
%% request must not choose what the loop runs or where it writes. The hook's
%% `hashpath' override is dropped, so the loop runs with the node message as
%% the hook was given it.
start(_Base, Req, Opts) ->
    NodeMsg = hb_maps:get(<<"body">>, Req, Opts, Opts),
    try
        LoopOpts = maps:remove(<<"hashpath">>, Opts),
        case config(LoopOpts) of
            #{ <<"pusd">> := _, <<"scheduler-url">> := _ } = Cfg ->
                case hb_name:lookup(?NAME) of
                    Pid when is_pid(Pid) -> ok;
                    _ ->
                        Pid = spawn(fun() -> init(Cfg, LoopOpts) end),
                        hb_name:register(?NAME, Pid)
                end;
            _ ->
                ?event(bridge, {not_configured})
        end
    catch C:R:T -> ?event(bridge, {start_failed, element(2, failure(C, R, T))})
    end,
    {ok, #{ <<"body">> => NodeMsg }}.

status(_Base, _Req, _Opts) ->
    case call(status) of
        {ok, S} -> {ok, json(S)};
        Error -> Error
    end.

keys(_Base, _Req, _Opts) ->
    case call(keys) of
        {ok, K} -> {ok, json(K)};
        Error -> Error
    end.

nudge(_Base, _Req, _Opts) ->
    case hb_name:lookup(?NAME) of
        Pid when is_pid(Pid) -> Pid ! nudge, {ok, json(#{ <<"ok">> => true })};
        _ -> {error, <<"The bridge loop is not running on this node">>}
    end.

%% @doc Endorse an independently signed root operation. The loop chooses its
%% configured root and verified replica; the requester supplies no state or
%% committee. The answer is this member's Ed25519 proof, not an on-chain vote
%% or publication, and never a signature by the node's AO wallet.
root_endorse(_Base, Req, Opts) ->
    Candidate = hb_maps:get(<<"certificate">>, Req,
        hb_maps:get(<<"body">>, Req, Req, Opts), Opts),
    Message = case Candidate of
        M when is_map(M) -> M;
        _ -> Req
    end,
    case hb_message:with_only_committed(Message, Opts) of
        {ok, Signed} ->
            case call({root_endorse, Signed}) of
                {ok, Endorsement} -> {ok, json(Endorsement)};
                Error -> Error
            end;
        _ -> {error, <<"A signed root operation is required">>}
    end.

%% @doc This member's own chain signature for one record its last pass
%% verified: `withdrawal=<id>' (a pending withdrawal's release on its chain) or
%% `proposal=<id>' (an applied membership, migration or administration change
%% it was a prior member for, one signature per chain). Never computed for a
%% request: answered only from what the pass already signed, so a claimer can
%% neither make the node sign anything nor make it work for an answer.
chain_sig(_Base, Req, Opts) ->
    Query = fun(K) -> hb_maps:get(K, Req, undefined, Opts) end,
    Lookup = case {Query(<<"withdrawal">>), Query(<<"proposal">>)} of
        {W, undefined} when is_binary(W) -> {withdrawal, W};
        {undefined, P} when is_binary(P) -> {proposal, P};
        _ -> invalid
    end,
    case Lookup of
        invalid -> {error, #{<<"status">> => 400, <<"body">> => <<"Name one withdrawal or one proposal">>}};
        _ ->
            case served_chain_sig(Lookup) of
                {ok, Answer} -> {ok, json(Answer)};
                _ -> {error, #{<<"status">> => 404, <<"body">> => <<"No chain signature for that record">>}}
            end
    end.

call(Msg) ->
    case hb_name:lookup(?NAME) of
        Pid when is_pid(Pid) ->
            Ref = make_ref(),
            Pid ! {Msg, self(), Ref},
            receive {Ref, Reply} -> Reply after 60000 -> {error, timeout} end;
        _ ->
            {error, <<"The bridge loop is not running on this node">>}
    end.

json(Term) ->
    #{ <<"content-type">> => <<"application/json">>, <<"body">> => hb_json:encode(Term) }.

config(Opts) ->
    case hb_opts:get(bridge, #{}, Opts) of
        Cfg when is_map(Cfg) -> Cfg;
        _ -> #{}
    end.

%%% The loop ------------------------------------------------------------------

init(Cfg0, Opts) ->
    St = initial_state(Cfg0, Opts),
    ?event(bridge, {started, {ao, maps:get(me, St)}}),
    self() ! pass,
    loop(St).

initial_state(Cfg0, Opts) ->
    Cfg = rpc_files(load_follow(Cfg0)),
    Keys = load_keys(maps:get(<<"keys-file">>, Cfg, <<"bridge-keys.json">>)),
    Me = hb_util:human_id(ar_wallet:to_address(hb_opts:get(priv_wallet, no_viable_wallet, Opts))),
    {Cursors, Deposits, Releases} = load_state(Cfg),
    #{
        cfg => Cfg, opts => Opts, keys => Keys, me => Me,
        cursors => Cursors, deposits => Deposits, releases => Releases, sent => #{},
        last_pass => 0, last_error => null, member => false, namespace => 0,
        timer => undefined, pass_at => 0, fast_until => 0, fast_since => 0, nudge_at => 0,
        skipped => #{}
    }.

loop(St) ->
    receive
        pass ->
            Start = os:system_time(millisecond),
            St1 = St#{ timer => undefined, pass_at => Start },
            St2 = try pass(St1)
                  catch C:R:T ->
                      {LastError, Log} = failure(C, R, T),
                      ?event(bridge, Log),
                      St1#{ last_error => LastError }
                  end,
            Next = next_interval(maps:get(cfg, St2), maps:get(fast_until, St2), os:system_time(millisecond)),
            loop(St2#{ timer => erlang:send_after(Next, self(), pass) });
        nudge ->
            Now = os:system_time(millisecond),
            Cfg = maps:get(cfg, St),
            case nudge_arm(Now, Cfg, St) of
                ignore -> loop(St);
                {ok, St1} ->
                    %% No timer means a pass is already queued; a timer that already
                    %% fired has queued one too. Either way that pass is soon enough.
                    Timer = maps:get(timer, St1),
                    case Timer =/= undefined andalso erlang:cancel_timer(Timer) =/= false of
                        true ->
                            Delay = max(0, maps:get(pass_at, St1) + cfg_ms(Cfg, <<"fast-interval">>, 1000) - Now),
                            loop(St1#{ timer => erlang:send_after(Delay, self(), pass) });
                        false ->
                            loop(St1)
                    end
            end;
        {status, From, Ref} ->
            From ! {Ref, {ok, status_view(St)}},
            loop(St);
        {keys, From, Ref} ->
            From ! {Ref, {ok, public_keys(St)}},
            loop(St);
        {{root_endorse, Message}, From, Ref} ->
            % Only the view the last pass verified: a request never moves the
            % replica, so it cannot make the loop fetch or compute.
            Reply = try
                case {maps:get(root_view, St, none), maps:get(replica, St, none)} of
                    {none, _} -> {error, <<"Verified root state is not available">>};
                    {View, #{stopped := null}} -> root_certificate_approval(Message, View, St);
                    _ -> {error, <<"Verified root state is still catching up">>}
                end
            catch _:_ -> {error, <<"Root certificate endorsement failed">>}
            end,
            From ! {Ref, Reply},
            loop(St);
        {root_publication_done, Pid, Result} ->
            case maps:get(root_publication, St, undefined) of
                {Pid, Monitor, Key} ->
                    erlang:demonitor(Monitor, [flush]),
                    Cleared = maps:remove(root_publication, St),
                    Next = case Result of
                        {ok, _} -> Cleared#{sent := (maps:get(sent, St))#{Key => os:system_time(millisecond)},
                            root_publication_error => null};
                        _ -> Cleared#{root_publication_error => <<"Root certificate publication failed; retained for retry">>}
                    end,
                    loop(Next);
                _ -> loop(St)
            end;
        {'DOWN', Monitor, process, _Pid, _Reason} ->
            case maps:get(root_publication, St, undefined) of
                {_, Monitor, _} -> loop((maps:remove(root_publication, St))#{
                    root_publication_error => <<"Root certificate publisher stopped; retained for retry">>});
                _ -> loop(St)
            end
    end.

%% @doc Check the author, committee epoch, actor and next sequence against
%% locally verified state before adding this node's signature. Execution at
%% the root rechecks balances and all action-specific constraints in canonical
%% order. A lane release may certify a rejection receipt when escrow is empty.
root_certificate_approval(Message, View, St = #{cfg := Cfg, opts := Opts, me := Me}) ->
    try
        Root = maps:get(<<"pusd">>, Cfg),
        Root = maps:get(<<"target">>, Message),
        <<"pusd-root/v1">> = maps:get(<<"certificate-version">>, Message),
        Author = maps:get(<<"author">>, Message),
        true = valid_root_address(Author),
        Info = key(<<"bridgeinfo">>, View),
        Members = maps:get(<<"attestors">>, Info),
        Addresses = [maps:get(<<"ao">>, M) || M <- Members],
        true = length(Addresses) =:= length(lists:usort(Addresses)),
        true = lists:member(Me, Addresses),
        Epoch = integer_to_binary(hb_util:int(maps:get(<<"epoch">>, Info))),
        Epoch = maps:get(<<"attestor-epoch">>, Message),
        Payload = hb_private:reset(hb_message:uncommitted(Message)),
        true = maps:fold(fun(K, V, Acc) ->
            Acc andalso is_binary(K) andalso is_binary(V) andalso
                re:run(K, <<"^[a-z][a-z0-9-]*$">>, [{capture, none}]) =:= match
        end, true, Payload),
        AuthorURI = hb_committer:uri(Author, Opts),
        Commitments = hb_maps:get(<<"commitments">>, Message, #{}, Opts),
        Codecs = [<<"httpsig@1.0">>, <<"ans104@1.0">>, <<"tx@1.0">>],
        true = lists:all(fun(C) ->
            lists:member(maps:get(<<"commitment-device">>, C, undefined), Codecs)
        end, maps:values(Commitments)),
        AuthorIDs = [ID || {ID, Commitment} <- maps:to_list(Commitments),
            maps:get(<<"committer">>, Commitment, undefined) =:= AuthorURI,
            lists:member(maps:get(<<"type">>, Commitment, undefined),
                [<<"rsa-pss-sha256">>, <<"rsa-pss-sha512">>]),
            lists:member(maps:get(<<"commitment-device">>, Commitment, undefined),
                [<<"httpsig@1.0">>, <<"ans104@1.0">>, <<"tx@1.0">>])],
        true = AuthorIDs =/= [],
        true = hb_message:verify(Message, all, Opts),
        true = hb_message:verify(Message, #{<<"ids">> => AuthorIDs}, Opts),
        Covered = hb_message:committed(Message,
            #{<<"ids">> => AuthorIDs}, Opts),
        true = lists:all(fun(K) -> lists:member(K, Covered) end, maps:keys(Payload)),
        Action = maps:get(<<"action">>, Payload),
        By = root_action_author(Action),
        Account = root_certificate_actor(By, Author, Payload, Info, Addresses),
        % A holder or lane runs at its account's next sequence; a committee
        % action is keyed by the record it decides and carries none.
        true = case By of
            member -> not maps:is_key(<<"root-sequence">>, Payload);
            _ ->
                Sequence = root_positive_integer(maps:get(<<"root-sequence">>, Payload)),
                Previous = maps:get(Account, key(<<"rootintents">>, View), #{}),
                Sequence =:= hb_util:int(maps:get(<<"sequence">>, Previous, 0)) + 1
        end,
        % A holder spends what it holds; a lane release is escrow the root
        % checks; a committee action is what THIS member saw for itself.
        true = case By of
            holder -> root_amount(Action, Payload) =< root_balance(Account, View);
            lane -> root_amount(Action, Payload) > 0;
            member -> committee_evidence(Action, Payload, Info, View, St)
        end,
        % The endorsement is this member's Ed25519 proof over the root
        % domain, and nothing else: the node's AO wallet signs no payload a
        % caller chose.
        Proof = #{} = root_inline_proof(Message, AuthorIDs, Members, St),
        {ok, Proof}
    catch _:_ -> {error, <<"Root operation is not eligible for endorsement">>}
    end.

%% @doc The aggregate balances map is a checkpoint in patch-mode PUSD. A
%% touched account's scalar is the current value, including an explicit zero.
root_balance(Account, View = {State, ROpts}) ->
    case hb_ao:get(<<"balance-", Account/binary>>, State, not_found, ROpts) of
        not_found -> hb_util:int(maps:get(Account, key(<<"balances">>, View), 0));
        Current -> hb_util:int(Current)
    end.

%% @doc This member's endorsement: its BLS signature over the root domain,
%% under the committee's own tag (dev_bridge_root). The current set must
%% register exactly this node's BLS key for it; a changed key must be
%% installed locally before this node can endorse that epoch. Non-ANS
%% requests have no inline encoding.
root_inline_proof(Message, AuthorIDs, Members, #{keys := Keys, me := Me, opts := Opts}) ->
    Commitments = maps:get(<<"commitments">>, Message),
    ANS = [ID || ID <- AuthorIDs,
        maps:get(<<"commitment-device">>, maps:get(ID, Commitments)) =:= <<"ans104@1.0">>],
    [Member] = [M || M <- Members, maps:get(<<"ao">>, M) =:= Me],
    case {ANS, maps:find(<<"bls">>, Member)} of
        {[], _} -> none;
        {_, error} -> none;
        {[IntentID], {ok, Registered}} ->
            {Secret, Public} = maps:get(bls, Keys),
            Registered = <<"0x", (dev_bridge_attestor_sign:hex(Public))/binary>>,
            {ok, Bytes} = hb_ao:resolve(#{<<"device">> => <<"bridge-root@1.0">>},
                #{<<"path">> => <<"domain">>, <<"root">> => maps:get(<<"target">>, Message),
                    <<"epoch">> => maps:get(<<"attestor-epoch">>, Message),
                    <<"intent-id">> => IntentID}, Opts),
            #{<<"signer">> => Me, <<"intent-id">> => IntentID,
                <<"signature">> => hb_util:encode(blst_nif:sign(Secret, Bytes, lib_bridge_bls:dst()))}
    end;
root_inline_proof(_Message, _AuthorIDs, _Members, _State) -> none.

%% @doc A root amount or sequence uses the same canonical exact range as Lua.
root_positive_integer(Value) when is_binary(Value) ->
    Number = binary_to_integer(Value),
    true = Number > 0 andalso Number < 9007199254740992,
    Value = integer_to_binary(Number),
    Number.

%% @doc A release carries no quantity of its own: its amount is the sum of its
%% moves, in the contract's one spelling (`id:recipient:quantity' joined by
%% `,', ids strictly ascending). Escrow is the contract's check, not ours: an
%% over-escrow batch must still land so every move gets its rejected receipt.
root_amount(<<"Ledger-Release">>, Payload) ->
    release_total(binary:split(maps:get(<<"moves">>, Payload), <<",">>, [global]), 0, 0);
root_amount(_Action, Payload) ->
    root_positive_integer(maps:get(<<"quantity">>, Payload)).

release_total([], _Last, Total) -> Total;
release_total([Entry | Rest], Last, Total) ->
    [Id, Recipient, Quantity] = binary:split(Entry, <<":">>, [global]),
    Sequence = root_positive_integer(Id),
    true = Sequence > Last andalso valid_root_address(Recipient),
    release_total(Rest, Sequence, Total + root_positive_integer(Quantity)).

valid_root_address(Address) when is_binary(Address), byte_size(Address) =:= 43 ->
    re:run(Address, <<"^[A-Za-z0-9_-]+$">>, [{capture, none}]) =:= match;
valid_root_address(_) -> false.

%% @doc Who authors each root action, as pusd.lua's ROOT_ACTIONS says: a
%% holder, a lane's registered authority, or a committee member. Anything
%% else has no certificate path and is never endorsed.
root_action_author(<<"Transfer">>) -> holder;
root_action_author(<<"Ledger-Enter">>) -> holder;
root_action_author(<<"Bridge-Redeem">>) -> holder;
root_action_author(<<"Ledger-Release">>) -> lane;
root_action_author(<<"Bridge-Mint">>) -> member;
root_action_author(<<"Ledger-Exit">>) -> member;
root_action_author(<<"Ledger-Move-Root">>) -> member;
root_action_author(<<"Bridge-Released">>) -> member;
root_action_author(<<"Ledger-Credited">>) -> member;
root_action_author(<<"Channel-Open">>) -> member;
root_action_author(<<"Channel-Settle">>) -> member;
root_action_author(<<"Channel-Close">>) -> member;
root_action_author(<<"Attestor-Apply">>) -> member.

%% @doc Match the contract's actor: the account whose sequence the operation
%% runs under. A root actor is the wallet whose signature authored the
%% operation, and only that: an operation carrying `from-*' stamps is never
%% endorsed (pusd.lua refuses it too).
root_certificate_actor(lane, Author, Payload, Info, _Members) ->
    [] = root_delivery_stamps(Payload),
    Lane = maps:get(<<"ledger">>, Payload),
    Ledger = maps:get(Lane, maps:get(<<"ledgers">>, Info)),
    Author = maps:get(<<"scheduler">>, Ledger),
    true = maps:get(<<"status">>, Ledger) =/= <<"disabled">>,
    Lane;
root_certificate_actor(member, Author, Payload, _Info, Members) ->
    [] = root_delivery_stamps(Payload),
    true = lists:member(Author, Members),
    Author;
root_certificate_actor(holder, Author, Payload, _Info, _Members) ->
    [] = root_delivery_stamps(Payload),
    Author.

%% @doc One operation field by its normalised name (case, `-' and `_' do not
%% count), present exactly once; `absent' when no spelling of it is.
root_field(Name, Payload) ->
    case [V || {K, V} <- maps:to_list(Payload), root_name(K) =:= Name] of
        [V] -> V;
        [] -> absent
    end.

root_name(K) ->
    binary:replace(binary:replace(hb_util:to_lower(K), <<"-">>, <<>>, [global]),
        <<"_">>, <<>>, [global]).

%% @doc A committee certificate is endorsed only if this member would author
%% that same decision itself: the operation equals one of this node's own
%% committee plans from its last pass -- a deposit its chain scan found and the
%% root has not minted, a lane exit, move or credit its own lane replica
%% shows, a release its chain scan saw, a channel step both lanes' replicas
%% support, a proposal its operator approved. One rule for every committee
%% action. Execution at the root rechecks everything in canonical order.
committee_evidence(_Action, P, _Info, _View, St) ->
    Want = committee_fields(P),
    lists:any(fun({_, Tags}) -> committee_fields(Tags) =:= Want end, maps:get(committee, St, [])).

%% @doc An operation's record fields, names normalised: everything but the
%% certificate's own envelope.
committee_fields(M) ->
    maps:from_list([{root_name(K), V} || {K, V} <- maps:to_list(M),
        not lists:member(root_name(K), [<<"author">>, <<"target">>, <<"certificateversion">>,
            <<"attestorepoch">>, <<"rootsequence">>])]).

root_delivery_stamps(Payload) ->
    [{N, V} || {K, V} <- maps:to_list(Payload),
        N <- [binary:replace(binary:replace(K, <<"-">>, <<>>, [global]),
            <<"_">>, <<>>, [global])],
        N =/= <<"from">>, byte_size(N) >= 4,
        binary:part(N, 0, 4) =:= <<"from">>].

%% @doc Real author signatures are checked against the local committee and
%% balances. Neither a caller's claimed committee nor another wallet's lane
%% signature can elicit an endorsement.
root_certificate_approval_test() ->
    [AuthorWallet, MemberWallet, LaneWallet] = [ar_wallet:new() || _ <- lists:seq(1, 3)],
    Address = fun(W) -> hb_util:human_id(ar_wallet:to_address(W)) end,
    Author = Address(AuthorWallet),
    Me = Address(MemberWallet),
    LaneAuthority = Address(LaneWallet),
    Root = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Lane = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Opts = #{<<"priv-wallet">> => MemberWallet, <<"store">> => hb_test_utils:test_store()},
    {Secret, PublicKey} = BlsKey = blst_nif:keygen(<<42:256>>),
    Registered = <<"0x", (binary:encode_hex(PublicKey, lowercase))/binary>>,
    St = #{cfg => #{<<"pusd">> => Root}, opts => Opts, me => Me, keys => #{bls => BlsKey}},
    Info = #{<<"epoch">> => 7, <<"attestors">> => [#{<<"ao">> => Me, <<"bls">> => Registered}],
        <<"ledgers">> => #{Lane => #{<<"scheduler">> => LaneAuthority,
            <<"status">> => <<"active">>}}},
    State = #{<<"bridgeinfo">> => hb_json:encode(Info),
        <<"balances">> => hb_json:encode(#{Author => <<"100">>, Lane => <<"0">>}),
        <<"rootintents">> => <<"{}">>},
    View = {State, Opts},
    Payload = #{<<"certificate-version">> => <<"pusd-root/v1">>,
        <<"target">> => Root, <<"author">> => Author,
        <<"attestor-epoch">> => <<"7">>, <<"action">> => <<"Transfer">>,
        <<"root-sequence">> => <<"1">>, <<"recipient">> => Me,
        <<"quantity">> => <<"50">>},
    Sign = fun(P, W) -> hb_message:commit(P, #{<<"priv-wallet">> => W}, <<"ans104@1.0">>) end,
    Signed = Sign(Payload, AuthorWallet),
    % The endorsement is the Ed25519 proof alone: the node's AO wallet signs
    % nothing a caller chose.
    {ok, Endorsed} = root_certificate_approval(Signed, View, St),
    ?assertEqual([<<"intent-id">>, <<"signature">>, <<"signer">>],
        lists:sort(maps:keys(Endorsed))),
    % No inline encoding without an ANS-104 item; no stamped root actor.
    ?assertMatch({error, _}, root_certificate_approval(
        hb_message:commit(Payload, #{<<"priv-wallet">> => AuthorWallet}), View, St)),
    ?assertMatch({error, _}, root_certificate_approval(
        Sign(Payload#{<<"from-process">> => Lane, <<"from-slot">> => <<"1">>}, AuthorWallet),
        View, St)),
    BalanceKey = hb_util:lowercase_key(<<"balance-", Author/binary>>),
    ?assertMatch({error, _}, root_certificate_approval(Signed,
        {State#{BalanceKey => <<"0">>}, Opts}, St)),
    FreshCredit = State#{<<"balances">> => <<"{}">>, BalanceKey => <<"100">>},
    ?assertMatch({ok, _}, root_certificate_approval(Signed, {FreshCredit, Opts}, St)),
    ?assertMatch({error, _}, root_certificate_approval(
        Signed#{<<"quantity">> => <<"90">>}, View, St)),
    ?assertMatch({error, _}, root_certificate_approval(
        Sign(Payload#{<<"quantity">> => <<"101">>}, AuthorWallet), View, St)),
    ?assertMatch({error, _}, root_certificate_approval(
        Sign(Payload#{<<"attestor-epoch">> => <<"6">>}, AuthorWallet), View, St)),
    ?assertMatch({error, _}, root_certificate_approval(
        Sign(Payload#{<<"root-sequence">> => <<"2">>}, AuthorWallet), View, St)),
    ?assertMatch({error, _}, root_certificate_approval(
        Sign(Payload, LaneWallet), View, St)),
    Foreign = {State#{<<"bridgeinfo">> => hb_json:encode(
        Info#{<<"attestors">> => [#{<<"ao">> => Author}]})}, Opts},
    ?assertMatch({error, _}, root_certificate_approval(Signed, Foreign, St)),
    Release = (maps:without([<<"quantity">>, <<"recipient">>], Payload))#{
        <<"action">> => <<"Ledger-Release">>, <<"author">> => LaneAuthority,
        <<"ledger">> => Lane, <<"moves">> => <<"3:", Me/binary, ":500,9:", Author/binary, ":1">>},
    ?assertMatch({ok, _}, root_certificate_approval(Sign(Release, LaneWallet), View, St)),
    lists:foreach(fun(Moves) ->
        ?assertMatch({error, _}, root_certificate_approval(
            Sign(Release#{<<"moves">> => Moves}, LaneWallet), View, St))
    end, [<<>>, <<"3:", Me/binary, ":5,3:", Author/binary, ":1">>,
        <<"9:", Me/binary, ":5,3:", Author/binary, ":1">>,
        <<"3:", Me/binary, ":05">>, <<"3:", Me/binary, ":0">>, <<"03:", Me/binary, ":5">>,
        <<"3:short:5">>, <<"3:", Me/binary, ":5,">>, <<"3:", Me/binary>>]),
    ?assertMatch({error, _}, root_certificate_approval(
        Sign(Release#{<<"author">> => Author}, AuthorWallet), View, St)),
    InlineView = View,
    InlineSt = St,
    WalletItem = hb_message:commit(Payload, #{<<"priv-wallet">> => AuthorWallet}, <<"ans104@1.0">>),
    {ok, #{<<"signature">> := Proof, <<"intent-id">> := IntentID}} =
        root_certificate_approval(WalletItem, InlineView, InlineSt),
    {ok, ProofBytes} = hb_ao:resolve(#{<<"device">> => <<"bridge-root@1.0">>},
        #{<<"path">> => <<"domain">>, <<"root">> => Root, <<"epoch">> => 7,
            <<"intent-id">> => IntentID}, Opts),
    ?assert(blst_nif:fast_aggregate_verify([PublicKey], ProofBytes, hb_util:decode(Proof), lib_bridge_bls:dst())),
    _ = Secret,
    % A node whose own key is not the one the set registered endorses nothing:
    % after a rotation it cannot sign for a key it no longer holds.
    ?assertMatch({error, _}, root_certificate_approval(WalletItem, InlineView,
        InlineSt#{keys => #{bls => blst_nif:keygen(<<43:256>>)}})).

%% @doc A committee certificate is endorsed only on this member's own evidence,
%% authored by a member, spelled as the authoring node emits it. Each action:
%% the exact evidence endorses; any field it did not see does not.
committee_certificate_approval_test() ->
    [MeWallet, PeerWallet, StrangerWallet] = [ar_wallet:new() || _ <- lists:seq(1, 3)],
    Address = fun(W) -> hb_util:human_id(ar_wallet:to_address(W)) end,
    Me = Address(MeWallet), Peer = Address(PeerWallet), Stranger = Address(StrangerWallet),
    Id = fun() -> hb_util:human_id(crypto:strong_rand_bytes(32)) end,
    Root = Id(), Lane = Id(), Lane2 = Id(), Holder = Id(),
    Opts = #{<<"priv-wallet">> => MeWallet, <<"store">> => hb_test_utils:test_store()},
    Seed = <<42:256>>,
    Hex = fun(B) -> <<"0x", (binary:encode_hex(B, lowercase))/binary>> end,
    PeerBase = <<7:256>>,
    Token = <<"0x2222222222222222222222222222222222222222">>,
    To = <<"0x3333333333333333333333333333333333333333">>,
    MyBls = blst_nif:keygen(Seed),
    Members = [#{<<"ao">> => Me, <<"bls">> => Hex(element(2, MyBls)),
            <<"base">> => dev_bridge_attestor_sign:base_address(<<8:256>>)},
        #{<<"ao">> => Peer, <<"bls">> => Hex(element(2, blst_nif:keygen(<<9:256>>))),
            <<"base">> => dev_bridge_attestor_sign:base_address(PeerBase)}],
    Info = #{<<"epoch">> => 7, <<"namespace">> => 3, <<"attestors">> => Members,
        <<"ledgers">> => #{Lane => #{<<"status">> => <<"active">>}, Lane2 => #{<<"status">> => <<"active">>}},
        <<"processed">> => #{<<"base">> => #{<<"floor">> => 2, <<"above">> => []}}},
    W = #{<<"id">> => <<"5">>, <<"chain">> => <<"base">>, <<"status">> => <<"pending">>,
        <<"token">> => Token, <<"to">> => To, <<"amount">> => <<"100">>, <<"sigs">> => #{}},
    State = #{<<"bridgeinfo">> => hb_json:encode(Info), <<"rootintents">> => <<"{}">>,
        <<"withdrawals">> => hb_json:encode([W])},
    LaneState = #{
        <<"laneexits">> => hb_json:encode([#{<<"id">> => <<"4">>, <<"status">> => <<"pending">>,
            <<"account">> => Holder, <<"chain">> => <<"base">>, <<"token">> => Token, <<"to">> => To,
            <<"quantity">> => <<"2000000">>}]),
        <<"lanemoves">> => hb_json:encode([#{<<"id">> => <<"9">>, <<"status">> => <<"pending">>,
            <<"source">> => Lane, <<"destination">> => Lane2, <<"account">> => Holder,
            <<"quantity">> => <<"30">>}])},
    Cfg = #{<<"pusd">> => Root, <<"base">> => #{<<"chain-id">> => 8453,
        <<"contract">> => <<"0x1111111111111111111111111111111111111111">>}},
    St0 = #{cfg => Cfg, opts => Opts, me => Me, keys => #{bls => MyBls, base => <<8:256>>},
        deposits => #{<<"base:3">> => #{kind => deposit, chain => <<"base">>, nonce => 3,
            token => Token, recipient => Holder, amount => <<"500">>}},
        lane_views => #{Lane => {LaneState, Opts}}, namespace => 3, index => 0, sent => #{},
        skipped => #{}, committee => []},
    View = {State, Opts},
    % This member's own plans, from the real planners: what it endorses.
    LaneRows = fun(K) -> key(K, {LaneState, Opts}) end,
    Own = [{K, T} || {root, K, T} <- exit_plans(Lane, LaneRows(<<"laneexits">>), [])
        ++ move_root_plans(7, Lane, LaneRows(<<"lanemoves">>), [], #{Lane => #{}, Lane2 => #{}})],
    St1 = attest_deposits(Info, St0),
    St = St1#{committee => maps:get(committee, St1) ++ Own},
    Base = #{<<"certificate-version">> => <<"pusd-root/v1">>, <<"target">> => Root,
        <<"author">> => Peer, <<"attestor-epoch">> => <<"7">>},
    Sign = fun(P, Wallet) -> hb_message:commit(P, #{<<"priv-wallet">> => Wallet}, <<"ans104@1.0">>) end,
    Approve = fun(P) -> root_certificate_approval(Sign(P, PeerWallet), View, St) end,
    Refused = fun(P) -> ?assertMatch({error, _}, Approve(P), P) end,
    Mint = Base#{<<"action">> => <<"Bridge-Mint">>, <<"chain">> => <<"base">>,
        <<"deposit-id">> => <<"base:3">>, <<"token">> => Token, <<"recipient">> => Holder,
        <<"quantity">> => <<"500">>},
    ?assertMatch({ok, #{<<"signer">> := Me}}, Approve(Mint)),
    % Spelled as the JS relay emits it, the same record endorses.
    ?assertMatch({ok, _}, Approve(maps:remove(<<"deposit-id">>, Mint#{<<"depositid">> => <<"base:3">>}))),
    lists:foreach(Refused, [Mint#{<<"quantity">> => <<"501">>}, Mint#{<<"recipient">> => Lane},
        Mint#{<<"deposit-id">> => <<"base:03">>}, Mint#{<<"deposit-id">> => <<"base:4">>},
        Mint#{<<"ledger">> => Lane}, Mint#{<<"token">> => string:uppercase(Token)},
        Mint#{<<"root-sequence">> => <<"1">>}]),
    ?assertMatch({error, _}, root_certificate_approval(Sign(Mint#{<<"author">> => Stranger},
        StrangerWallet), View, St)),
    MintedInfo = Info#{<<"processed">> => #{<<"base">> => #{<<"floor">> => 3}}},
    ?assertEqual([], [P || {<<"mint:", _/binary>>, _} = P <- maps:get(committee,
        attest_deposits(MintedInfo, St0))], "a minted deposit is no plan, so no endorsement"),
    Exit = Base#{<<"action">> => <<"Ledger-Exit">>, <<"ledger">> => Lane, <<"exit-id">> => <<"4">>,
        <<"account">> => Holder, <<"chain">> => <<"base">>, <<"token">> => Token,
        <<"recipient">> => To, <<"quantity">> => <<"2000000">>},
    ?assertMatch({ok, _}, Approve(Exit)),
    lists:foreach(Refused, [Exit#{<<"quantity">> => <<"2000001">>}, Exit#{<<"exit-id">> => <<"5">>},
        Exit#{<<"recipient">> => Token}, Exit#{<<"ledger">> => Lane2}]),
    Move = Base#{<<"action">> => <<"Ledger-Move-Root">>, <<"source">> => Lane,
        <<"destination">> => Lane2, <<"move-id">> => <<"9">>, <<"account">> => Holder,
        <<"quantity">> => <<"30">>},
    ?assertMatch({ok, _}, Approve(Move)),
    lists:foreach(Refused, [Move#{<<"destination">> => Root}, Move#{<<"quantity">> => <<"31">>},
        Move#{<<"move-id">> => <<"10">>}]),
    _ = {PeerBase, W},
    % A proposal is endorsed only once this node's operator approved it.
    Proposal = #{<<"id">> => 12, <<"status">> => <<"open">>, <<"approvals">> => #{Peer => true}},
    Apply = Base#{<<"action">> => <<"Attestor-Apply">>, <<"proposal-id">> => <<"12">>},
    ?assertEqual([], [P || {<<"apply:", _/binary>>, _} = P <- maps:get(committee,
        apply_proposals(Info#{<<"proposals">> => [Proposal]}, St0))]),
    Refused(Apply),
    Approved = apply_proposals(Info#{<<"proposals">> => [Proposal#{<<"approvals">> => #{Me => true}}]}, St0),
    ?assertMatch({ok, _}, root_certificate_approval(Sign(Apply, PeerWallet), View,
        St#{committee => maps:get(committee, Approved)})).

root_endorse_endpoint_test_() ->
    {timeout, 120, fun root_endorse_endpoint/0}.

%% @doc The public device endpoint obtains state from a real independently
%% replayed process. A claimed balance supplied in the request is irrelevant.
root_endorse_endpoint() ->
    hb:init(),
    Opts = (ao_test_opts())#{<<"scheduler-publish-remote">> => false,
        <<"scheduling-mode">> => local_confirmation},
    Me = hb_util:human_id(ar_wallet:to_address(maps:get(<<"priv-wallet">>, Opts))),
    AuthorWallet = ar_wallet:new(),
    Author = hb_util:human_id(ar_wallet:to_address(AuthorWallet)),
    Dir = <<"/tmp/root-endorsement-", (hb_util:human_id(crypto:strong_rand_bytes(32)))/binary>>,
    ok = file:make_dir(Dir),
    NodeKeys = load_keys(<<Dir/binary, "/keys.json">>),
    {_, BlsPublic} = maps:get(bls, NodeKeys),
    Info = hb_json:encode(#{<<"epoch">> => 7,
        <<"attestors">> => [#{<<"ao">> => Me,
            <<"bls">> => <<"0x", (binary:encode_hex(BlsPublic, lowercase))/binary>>}], <<"ledgers">> => #{}}),
    Balances = hb_json:encode(#{Author => <<"100">>, Me => <<"100">>}),
    Script = <<"function compute(base, req) base.bridgeinfo = '", Info/binary,
        "'; base.balances = '", Balances/binary,
        "'; base.rootintents = '{}'; return base end">>,
    Process = hb_message:commit(#{<<"device">> => <<"process@1.0">>,
        <<"execution-device">> => <<"lua@5.3a">>,
        <<"scheduler-device">> => <<"scheduler@1.0">>,
        <<"scheduler-location">> => Me, <<"authority">> => [Me],
        <<"module">> => #{<<"content-type">> => <<"application/lua">>,
            <<"body">> => Script}}, Opts),
    {ok, _} = hb_cache:write(Process, Opts),
    Root = hb_message:id(Process, all, Opts),
    Node = hb_http_server:start_node(Opts),
    Cfg = (ao_test_cfg(Root, Dir))#{<<"scheduler-url">> => url(Node),
        <<"replica-dir">> => <<Dir/binary, "/replica">>},
    St = initial_state(Cfg, Opts),
    schedule_as(AuthorWallet, #{<<"action">> => <<"Ping">>}, Root, Opts),
    Loop = spawn(fun() -> loop(St) end),
    hb_name:register(?NAME, Loop),
    try
        Payload = #{<<"certificate-version">> => <<"pusd-root/v1">>,
            <<"target">> => Root, <<"author">> => Author,
            <<"attestor-epoch">> => <<"7">>, <<"action">> => <<"Transfer">>,
            <<"root-sequence">> => <<"1">>, <<"recipient">> => Me,
            <<"quantity">> => <<"50">>},
        Run = fun(P) ->
            Signed = hb_message:commit(P, #{<<"priv-wallet">> => AuthorWallet}, <<"ans104@1.0">>),
            hb_ao:resolve(#{<<"device">> => <<"bridge-attestor@1.0">>},
                #{<<"path">> => <<"root-endorse">>, <<"certificate">> => Signed,
                    <<"balances">> => #{Author => <<"1000000">>}}, Opts)
        end,
        % Before a pass has verified the root, a request cannot move the
        % replica: it is refused, and nothing was replayed.
        ?assertEqual({error, <<"Verified root state is not available">>}, Run(Payload)),
        ?assertNot(filelib:is_dir(<<Dir/binary, "/replica">>)),
        Loop ! pass,
        % The JSON answer resolves to the proof alone: no AO-wallet certificate.
        % Packaged, the answer stays the JSON body; loose, it resolves to a map.
        Endorsement = case Run(Payload) of
            {ok, #{<<"body">> := JSON}} -> hb_json:decode(JSON);
            {ok, Map} -> Map
        end,
        ?assertMatch(#{<<"signature">> := _, <<"intent-id">> := _}, Endorsement),
        ?assertNot(maps:is_key(<<"certificate">>, Endorsement)),
        ?assertMatch({error, _}, Run(Payload#{<<"quantity">> => <<"101">>})),
        WalletItem = hb_message:commit(Payload,
            #{<<"priv-wallet">> => AuthorWallet}, <<"ans104@1.0">>),
        Item = hb_message:convert(WalletItem, <<"ans104@1.0">>, Opts),
        Bytes = ar_bundles:serialize(Item),
        Endpoint = binary_to_list(<<(url(Node))/binary, "/~bridge-attestor@1.0/root-endorse">>),
        {ok, {{_, 200, _}, _, ResponseBody}} = httpc:request(post,
            {Endpoint, [{"accept", "application/json"}], "application/ans104", Bytes},
            [{timeout, 60000}], [{body_format, binary}]),
        HTTPProof = maps:with([<<"signer">>, <<"intent-id">>, <<"signature">>],
            hb_json:decode(ResponseBody)),
        IntentID = hb_util:human_id(Item#tx.id),
        ?assertEqual(IntentID, maps:get(<<"intent-id">>, HTTPProof)),
        {ok, Domain} = hb_ao:resolve(#{<<"device">> => <<"bridge-root@1.0">>},
            #{<<"path">> => <<"domain">>, <<"root">> => Root, <<"epoch">> => 7,
                <<"intent-id">> => IntentID}, Opts),
        ?assert(blst_nif:fast_aggregate_verify([BlsPublic], Domain,
            hb_util:decode(maps:get(<<"signature">>, HTTPProof)), lib_bridge_bls:dst())),
        CodecProof = fetch_root_proof(url(Node), WalletItem, Opts),
        ?assertEqual(HTTPProof, CodecProof),
        Committee = [#{<<"ao">> => Me,
            <<"bls">> => <<"0x", (binary:encode_hex(BlsPublic, lowercase))/binary>>}],
        ?assertMatch({ok, #{<<"tags">> := _}},
            collect_root_carrier(Root, 7, Committee, WalletItem, St)),
        JournalFile = <<Dir/binary, "/public-root-journal">>,
        PublicState = #{intent => WalletItem},
        save_root_publication(JournalFile, PublicState),
        {ok, JournalBytes} = file:read_file(JournalFile),
        ?assertEqual(PublicState, binary_to_term(JournalBytes, [safe])),
        root_publisher_retry_check(Root, St, hb_json:decode(Info), Opts),
        root_publisher_lifecycle_check(St)
    after
        exit(Loop, shutdown),
        hb_name:unregister(?NAME)
    end.

%% @doc Exercise the actual signed carrier and HTTP submission without paying
%% for an L1 transaction. The gateway fixture rejects its first POST; a fresh
%% worker state must reuse the durable carrier without requesting a new price,
%% anchor, or endorsement after that uncertain/failed broadcast.
root_publisher_retry_check(Root, St, Info, Opts) ->
    Counter = atomics:new(1, []),
    StateFile = maps:get(<<"state-file">>, maps:get(cfg, St)),
    Journal = <<StateFile/binary, ".root-", (hb_util:human_id(crypto:hash(sha256,
        <<Root/binary, "retry-check">>)))/binary>>,
    {Gateway, GatewayOpts} = hb_mock_server:start_arweave_gateway(#{
        price => {200, <<"1">>},
        tx_anchor => {200, hb_util:human_id(<<7:256>>)},
        tx => fun(Req) ->
            {ok, SavedBytes} = file:read_file(Journal),
            #{carrier := SavedCarrier} = binary_to_term(SavedBytes, [safe]),
            Posted = ar_tx:json_struct_to_tx(hb_json:decode(maps:get(<<"body">>, Req))),
            ?assertEqual(SavedCarrier#tx.id, Posted#tx.id),
            ?assertEqual(SavedCarrier#tx.signature, Posted#tx.signature),
            ?assert(ar_tx:verify(Posted)),
            case atomics:add_get(Counter, 1, 1) of
                1 -> {429, <<"Retry later">>};
                _ -> {200, <<"OK">>}
            end
        end}),
    PublisherOpts = maps:merge(Opts, GatewayOpts),
    Publisher = St#{opts := PublisherOpts, root_certificate_info => Info},
    Tags = #{<<"action">> => <<"Transfer">>, <<"recipient">> => Root,
        <<"quantity">> => <<"50">>, <<"rootsequence">> => <<"1">>},
    try
        ?assertMatch({error, _}, publish_root_certificate(<<"retry-check">>, Tags, Publisher)),
        [First] = hb_mock_server:get_requests(tx, 1, Gateway),
        Fresh = (initial_state(maps:get(cfg, St), PublisherOpts))#{root_certificate_info => Info},
        ?assertMatch({ok, _}, publish_root_certificate(<<"retry-check">>, Tags, Fresh)),
        [First, Second] = hb_mock_server:get_requests(tx, 2, Gateway),
        ?assertEqual(maps:get(<<"body">>, First), maps:get(<<"body">>, Second)),
        WireTX = ar_tx:json_struct_to_tx(hb_json:decode(maps:get(<<"body">>, Second))),
        WireTags = maps:from_list(WireTX#tx.tags),
        ?assertMatch({ok, _}, hb_ao:resolve(#{<<"device">> => <<"bridge-root@1.0">>},
            #{<<"path">> => <<"verify">>, <<"root">> => Root, <<"epoch">> => 7,
                <<"committee">> => maps:get(<<"attestors">>, Info),
                <<"intent-id">> => maps:get(<<"Intent-Id">>, WireTags),
                <<"operation">> => maps:get(<<"Root-Operation">>, WireTags),
                <<"authorization">> => maps:get(<<"Root-Authorization">>, WireTags),
                <<"endorsements">> => maps:get(<<"Root-Endorsements">>, WireTags)}, Opts)),
        ?assertEqual(1, length(hb_mock_server:get_requests(Gateway, price))),
        ?assertEqual(1, length(hb_mock_server:get_requests(Gateway, tx_anchor))),
        % The key names the root sequence: a later plan for it reposts the
        % journalled carrier rather than signing a competing one.
        ?assertMatch({ok, _}, publish_root_certificate(<<"retry-check">>,
            Tags#{<<"quantity">> => <<"51">>}, Fresh)),
        [_, _, Third] = hb_mock_server:get_requests(tx, 3, Gateway),
        ?assertEqual(maps:get(<<"body">>, First), maps:get(<<"body">>, Third))
    after hb_mock_server:stop(Gateway)
    end.

%% @doc A failed worker leaves a responsive attestor and a visible error; a
%% successful worker clears that error. Stopping the attestor stops its worker.
root_publisher_lifecycle_check(St) ->
    Parent = self(),
    Start = fun() ->
        Loop = spawn(fun() ->
            LoopPid = self(),
            {Worker, Monitor} = spawn_monitor(fun() ->
                watch_root_publisher_parent(LoopPid),
                receive
                    crash -> exit(test_worker_failure);
                    finish -> LoopPid ! {root_publication_done, self(), {ok, posted}}
                end
            end),
            Parent ! {publisher_ready, LoopPid, Worker},
            loop(St#{root_publication => {Worker, Monitor, <<"test-job">>},
                root_publication_error => <<"prior failure">>})
        end),
        receive {publisher_ready, Loop, Worker} -> {Loop, Worker}
        after 1000 -> error(publisher_start_timeout)
        end
    end,
    Status = fun(Pid) ->
        Ref = make_ref(),
        Pid ! {status, self(), Ref},
        receive {Ref, {ok, Result}} -> Result after 1000 -> error(attestor_unresponsive) end
    end,
    lists:foreach(fun(Outcome) ->
        {Loop, Worker} = Start(),
        try
            Worker ! Outcome,
            hb_util:wait_until(fun() ->
                maps:get(<<"root-publication-running">>, Status(Loop)) =:= false
            end, 2000),
            Result = Status(Loop),
            ?assertEqual(false, maps:get(<<"root-publication-running">>, Result)),
            case Outcome of
                finish -> ?assertEqual(null, maps:get(<<"root-publication-error">>, Result));
                crash -> ?assertNotEqual(null, maps:get(<<"root-publication-error">>, Result))
            end
        after exit(Loop, shutdown)
        end
    end, [crash, finish]),
    {Loop, Worker} = Start(),
    Monitor = erlang:monitor(process, Worker),
    exit(Loop, shutdown),
    receive {'DOWN', Monitor, process, Worker, _} -> ok
    after 2000 -> error(publisher_outlived_attestor)
    end.

%% A failed pass: what `status' shows, and what is logged. Neither carries a
%% URL (RPC URLs hold API keys), a map (the node's options hold its wallet) or
%% a call's arguments: class, reason and module:function/arity:line only.
failure(C, R, T) ->
    Reason = scrub(R),
    Frames = [frame(F) || F <- T],
    Shown = iolist_to_binary(io_lib:format("~0p:~0p ~0p", [
        C, Reason, lists:sublist(Frames, 3)
    ])),
    {binary:part(Shown, 0, min(512, byte_size(Shown))),
        {pass_failed, C, Reason, Frames}}.

frame({M, F, A, Loc}) ->
    Arity = case A of N when is_integer(N) -> N; Args -> length(Args) end,
    {M, F, Arity, proplists:get_value(line, Loc)};
frame(_) -> unknown.

scrub(B) when is_binary(B) ->
    case binary:match(B, <<"://">>) of
        nomatch -> B;
        _ ->
            case catch uri_string:parse(B) of
                #{ host := H } when is_binary(H) -> <<"<url ", H/binary, ">">>;
                _ -> <<"<url>">>
            end
    end;
scrub(M) when is_map(M) -> {map, maps:size(M)};
scrub([H | T]) -> [scrub(H) | scrub(T)];
scrub(T) when is_tuple(T) -> list_to_tuple(scrub(tuple_to_list(T)));
scrub(X) -> X.

%% Fast while a nudge is recent, idle otherwise.
next_interval(Cfg, FastUntil, Now) when Now < FastUntil -> cfg_ms(Cfg, <<"fast-interval">>, 1000);
next_interval(Cfg, _FastUntil, _Now) -> cfg_ms(Cfg, <<"interval">>, 5000).

cfg_ms(Cfg, Key, Default) -> hb_util:int(maps:get(Key, Cfg, Default)).

%% Fast mode for a nudge, within limits: at most one arming per NUDGE_GAP_MS,
%% one stretch of fast passes capped at three `fast-window's, and a capped
%% stretch followed by COOL_MS before the next can start.
nudge_arm(Now, Cfg, St = #{ nudge_at := NudgeAt, fast_since := Since0, fast_until := Until0 }) ->
    Window = cfg_ms(Cfg, <<"fast-window">>, 120000),
    Cap = 3 * Window,
    Fast = Now < Until0,
    Since = case Fast of true -> Since0; false -> Now end,
    Until = min(Now + Window, Since + Cap),
    Cooling = not Fast andalso Until0 =:= Since0 + Cap
        andalso Now < Until0 + ?COOL_MS,
    case Now - NudgeAt < ?NUDGE_GAP_MS orelse Cooling orelse Until =< Until0 of
        true -> ignore;
        false -> {ok, St#{ nudge_at := Now, fast_since := Since, fast_until := Until }}
    end.

%% Everything a pass does, it does on `View': PUSD as this node computed it.
pass(St = #{ cfg := Cfg }) ->
    case view(maps:get(<<"pusd">>, Cfg), St) of
        {ok, View, Replica} -> act(View, St#{ replica => Replica, root_view => View });
        {not_yet, Why} -> St#{ last_error => Why, last_pass => os:system_time(millisecond) }
    end.

act(View, St) ->
    Info = key(<<"bridgeinfo">>, View),
    case maps:get(<<"successor">>, Info, <<>>) of
        Succ when is_binary(Succ), byte_size(Succ) =:= 43 -> follow(Succ, Info, View, St);
        _ -> attest(Info, View, St)
    end.

attest(Info, View, St = #{ cfg := Cfg, me := Me }) ->
    Withdrawals = case key(<<"withdrawals">>, View) of L when is_list(L) -> L; _ -> [] end,
    LedgerDeposits = case key(<<"ledgerdeposits">>, View) of L1 when is_list(L1) -> L1; _ -> [] end,
    LedgerReceipts = case key(<<"ledgerexits">>, View) of L2 when is_list(L2) -> L2; _ -> [] end,
    MoveReceipts = case key(<<"ledgermoves">>, View) of L3 when is_list(L3) -> L3; _ -> [] end,
    Member = lists:any(fun(A) -> maps:get(<<"ao">>, A, <<>>) =:= Me end,
        maps:get(<<"attestors">>, Info, [])),
    case Member of
        false ->
            St#{ member => false, last_pass => os:system_time(millisecond) };
        true ->
            {Found, Cursors} = scan(Cfg, maps:get(cursors, St), maps:get(opts, St)),
            Ns = hb_util:int(maps:get(<<"namespace">>, Info, 0)),
            Index = length(lists:takewhile(fun(A) -> maps:get(<<"ao">>, A, <<>>) =/= Me end,
                maps:get(<<"attestors">>, Info))),
            St1 = remember(Found, St#{ cursors => Cursors, member => true, namespace => Ns,
                skipped => #{}, index => Index, committee => [], lane_views => #{} }),
            % What was found is on disk before the cursors pass it.
            save_state(Cfg, St1),
            St2 = attest_deposits(Info, St1),
            Epoch = hb_util:int(maps:get(<<"epoch">>, Info)),
            St3 = apply_proposals(Info, St2),
            St4 = acknowledge(Withdrawals, chain_signatures(Info, Withdrawals, Epoch, St3)),
            St5 = relay_fast_ledgers(Info, LedgerDeposits, LedgerReceipts,
                MoveReceipts, Epoch, St4#{root_intents => key(<<"rootintents">>, View),
                    root_certificate_info => Info}),
            St6 = committee_certificate(Epoch, channel_certificates(View, St5)),
            save_state(Cfg, St6),
            St6#{ last_pass => os:system_time(millisecond), last_error => null }
    end.

%% Every chain that is configured, each failing on its own.
scan(Cfg, Cursors, Opts) ->
    lists:foldl(
        fun(Chain, {Acc, Cur}) ->
            case maps:get(Chain, Cfg, undefined) of
                undefined -> {Acc, Cur};
                C ->
                    % A chain that raises fails alone: the others still scan.
                    Res = try scan_chain(Chain, C, maps:get(Chain, Cur, undefined), Opts)
                          catch Class:Reason:Trace ->
                              {error, element(2, failure(Class, Reason, Trace))}
                          end,
                    case Res of
                        {ok, Events, Next} -> {Acc ++ Events, Cur#{ Chain => Next }};
                        Error -> ?event(bridge, {scan_failed, Chain, scrub(Error)}), {Acc, Cur}
                    end
            end
        end, {[], Cursors}, [<<"base">>, <<"solana">>, <<"sui">>]).

scan_chain(<<"base">>, C, Cursor, Opts) ->
    case dev_bridge_attestor_chains:base_head(C, Opts) of
        {ok, Head} -> scan_base(C, Cursor, Head, Opts);
        Error -> Error
    end;
scan_chain(<<"solana">>, C, Cursor, Opts) ->
    Until = case Cursor of undefined -> undefined; null -> undefined; S -> S end,
    dev_bridge_attestor_chains:solana_scan(C, Until, Opts);
scan_chain(<<"sui">>, C, Cursor, Opts) ->
    Cur = case Cursor of
        #{ <<"deposit">> := D, <<"released">> := R } -> #{ deposit => undef(D), released => undef(R) };
        _ -> #{}
    end,
    case dev_bridge_attestor_chains:sui_scan(C, Cur, Opts) of
        {ok, Events, #{ deposit := D2, released := R2 }} ->
            {ok, Events, #{ <<"deposit">> => null_if(D2), <<"released">> => null_if(R2) }};
        Error -> Error
    end.

scan_base(C, Cursor, Head, Opts) ->
    From = case Cursor of undefined -> hb_util:int(maps:get(<<"from-block">>, C, Head)); N -> N + 1 end,
    To = min(Head, From + hb_util:int(maps:get(<<"log-range">>, C, 2000)) - 1),
    case To < From of
        true -> {ok, [], Cursor};
        false ->
            case dev_bridge_attestor_chains:base_scan(C, From, To, Opts) of
                {ok, Events} -> {ok, Events, To};
                Error -> Error
            end
    end.

undef(null) -> undefined;
undef(X) -> X.
null_if(undefined) -> null;
null_if(X) -> X.

%% Deposits wait here until PUSD shows them minted; releases until the
%% withdrawal is no longer pending.
remember(Found, St = #{ deposits := D, releases := R }) ->
    lists:foldl(
        fun(E = #{ kind := deposit, chain := Chain, nonce := N }, S) ->
                S#{ deposits := (maps:get(deposits, S))#{ deposit_id(Chain, N) => E } };
           (E = #{ kind := released }, S) ->
                case local_withdrawal(maps:get(id, E), S) of
                    false -> S;
                    Id -> S#{ releases := (maps:get(releases, S))#{ Id => E } }
                end
        end, St#{ deposits := D, releases := R }, Found).

%% Each unminted deposit this node saw is a mint certificate it may author.
attest_deposits(Info, St = #{ deposits := Deps }) ->
    maps:fold(
        fun(Id, #{ chain := Chain, nonce := N } = E, S) ->
            case minted(Info, Chain, N) of
                true -> S#{ deposits := maps:remove(Id, maps:get(deposits, S)) };
                false ->
                    committee_plan(<<"mint:", Id/binary>>, #{
                        <<"action">> => <<"Bridge-Mint">>,
                        <<"deposit-id">> => Id, <<"chain">> => Chain,
                        <<"token">> => maps:get(token, E), <<"recipient">> => maps:get(recipient, E),
                        <<"quantity">> => hb_util:bin(maps:get(amount, E))
                    }, S)
            end
        end, St, Deps).

%% @doc Each open proposal this node's operator approved is an Attestor-Apply
%% certificate this node may author, and the only one it endorses: the person
%% decides, the node holds the key.
apply_proposals(Info, St = #{ me := Me }) ->
    lists:foldl(fun(P, S) ->
        case maps:get(<<"status">>, P, <<>>) =:= <<"open">>
                andalso maps:get(Me, maps:get(<<"approvals">>, P, #{}), false) =:= true of
            true ->
                Id = integer_to_binary(hb_util:int(maps:get(<<"id">>, P))),
                committee_plan(<<"apply:", Id/binary>>, #{ <<"action">> => <<"Attestor-Apply">>,
                    <<"proposal-id">> => Id }, S);
            false -> S
        end
    end, St, [P || P <- maps:get(<<"proposals">>, Info, []), is_map(P)]).

%% @doc The chain signatures this member serves (chain-sig), made from its
%% verified replica only: its release signature for each pending withdrawal,
%% and its signature for each chain over each applied change it was a prior
%% member for. Chain signatures are never stored on the root; a claimer or
%% set-relayer collects k of them from the members and the chain checks them.
%% A record this node cannot sign (a chain it does not run) is skipped alone.
chain_signatures(Info, Withdrawals, Epoch, St = #{ me := Me, cfg := Cfg, keys := Keys }) ->
    Namespace = hb_util:int(maps:get(<<"namespace">>, Info, 0)),
    Releases = maps:from_list([{Id, Sig} || W <- Withdrawals, is_map(W),
        maps:get(<<"status">>, W, <<>>) =:= <<"pending">>,
        Id <- [hb_util:bin(maps:get(<<"id">>, W, <<>>))],
        Sig <- [try withdrawal_signature(W, Epoch, Cfg, Keys, Namespace) catch _:_ -> skip end],
        Sig =/= skip]),
    Changes = maps:from_list([{integer_to_binary(hb_util:int(maps:get(<<"id">>, P))), Sigs}
        || P <- maps:get(<<"proposals">>, Info, []), is_map(P),
        maps:get(<<"status">>, P, <<>>) =:= <<"applied">>,
        lists:any(fun(M) -> maps:get(<<"ao">>, M, <<>>) =:= Me end, maps:get(<<"prior">>, P, [])),
        Sigs <- [proposal_signatures(P, Cfg, Keys)], map_size(Sigs) > 0]),
    Sigs = #{ withdrawal => Releases, proposal => Changes },
    publish_chain_sigs(Me, Sigs),
    St#{ chain_sigs => Sigs }.

%% @doc Hand the pass's signatures to the request side: one ETS row, written
%% only by this process (protected), replaced only when it changes.
publish_chain_sigs(Me, Sigs) ->
    case ets:whereis(?CHAIN_SIGS) of
        undefined -> ets:new(?CHAIN_SIGS, [named_table, protected, set, {read_concurrency, true}]);
        _ -> ok
    end,
    case ets:lookup(?CHAIN_SIGS, current) of
        [{current, Me, Sigs}] -> ok;
        _ -> ets:insert(?CHAIN_SIGS, {current, Me, Sigs})
    end.

%% @doc A request's answer from the published row: nothing is signed for it.
served_chain_sig({Kind, Id}) ->
    try ets:lookup(?CHAIN_SIGS, current) of
        [{current, Me, Sigs}] -> chain_sig_answer(Kind, Id, #{ me => Me, chain_sigs => Sigs });
        [] -> {error, not_found}
    catch error:badarg -> {error, not_found}
    end.

chain_sig_answer(Kind, Id, St = #{ me := Me }) ->
    case maps:find(Id, maps:get(Kind, maps:get(chain_sigs, St, #{}), #{})) of
        {ok, Sig} when Kind =:= withdrawal ->
            {ok, #{ <<"signer">> => Me, <<"withdrawal">> => Id, <<"sig">> => Sig }};
        {ok, Sigs} ->
            {ok, maps:merge(Sigs, #{ <<"signer">> => Me, <<"proposal">> => Id })};
        error -> {error, not_found}
    end.

%%% Committee certificates ------------------------------------------------------

%% @doc A committee action this node may author, in priority order. Every root
%% mutation that needs a majority is ONE inline root certificate (pusd.lua
%% Root-Execute): this member signs the operation, a strict majority endorses
%% it off-chain, one carrier lands it. There is no per-member vote to send.
committee_plan(Key, Tags, St) ->
    St#{ committee => maps:get(committee, St, []) ++ [{Key, Tags}] }.

%% @doc At most one committee certificate in flight. A committee action decides
%% one record and the record keys it: the plan key (with the epoch the intent
%% states) is the journal key, so what was journalled first for a record is
%% what that record publishes until it lands, and a second member's copy, or
%% this member's replay, decides nothing at the root and spends no sequence.
%% One that cannot be endorsed is forgotten (publish_root_certificate) and the
%% next pass plans it afresh; plans take turns, so one this member's peers
%% will not endorse yet (a deposit they have not scanned) never blocks the
%% rest.
committee_certificate(Epoch, St = #{ cfg := Cfg, sent := Sent }) ->
    Now = os:system_time(millisecond),
    Key = fun(Plan) -> <<"root-cert:", (integer_to_binary(Epoch))/binary, ":", Plan/binary>> end,
    Due = [P || P = {Plan, _} <- maps:get(committee, St, []),
        Now - maps:get(Key(Plan), Sent, 0) >= resend_ms(Cfg)],
    case Due of
        [] -> St;
        _ ->
            Turn = maps:get(committee_turn, St, 0),
            {Plan, Tags} = lists:nth(Turn rem length(Due) + 1, Due),
            start_root_publication(Key(Plan), Tags, St#{ committee_turn => Turn + 1 })
    end.

%% @doc The root's channel decisions this node would certify: open, settle and
%% close requests from the lanes it relays, each certified only against both
%% sides' own replicated records (the JS relay's channelPlans, the same rule
%% and the same fields). The lane legs (deposit, results, freeze) stay votes
%% at the lanes and are relayed by relay.mjs.
channel_certificates(View, St = #{ cfg := Cfg }) ->
    Root = maps:get(<<"pusd">>, Cfg),
    RootCh = case key(<<"ledgerchannels">>, View) of M when is_map(M) -> M; _ -> #{} end,
    Allowed = maps:get(fast_ledgers, St, #{}),
    Lanes = maps:fold(fun(L, LV, Acc) ->
        case key(<<"lanechannels">>, LV) of
            C when is_map(C) -> Acc#{ L => C };
            _ -> Acc
        end
    end, #{}, maps:with(maps:keys(Allowed), maps:get(lane_views, St, #{}))),
    lists:foldl(fun({root, K, T}, S) -> committee_plan(K, T, S) end, St,
        channel_plans(Root, RootCh, Lanes, Allowed)).

channel_plans(Root, RootCh, Lanes, Allowed) ->
    Channels = cmap(maps:get(<<"channels">>, RootCh, #{})),
    Decided = maps:from_list([{maps:get(<<"key">>, R), true} || R <- clist(maps:get(<<"receipts">>, RootCh, [])),
        is_binary(maps:get(<<"key">>, R, undefined))]),
    View = fun(L) ->
        case maps:find(L, Lanes) of
            {ok, V} ->
                case {maps:get(<<"ledger">>, V, undefined), maps:get(<<"root">>, V, undefined)} of
                    {L, Root} -> V;
                    _ -> none
                end;
            error -> none
        end
    end,
    lists:append([case View(Requester) of
        none -> [];
        Lane -> lists:append([channel_request_plan(Root, Requester, Q, Channels, Decided, Allowed, View, Lane)
            || Q <- clist(maps:get(<<"requests">>, Lane, []))])
    end || Requester <- lists:sort(maps:keys(Allowed))]).

channel_request_plan(Root, Requester, Q, Channels, Decided, Allowed, View, Lane) ->
    Id = exact(maps:get(<<"id">>, Q, undefined)),
    Gen = exact(maps:get(<<"generation">>, Q, undefined)),
    Peer = maps:get(<<"peer">>, Q, undefined),
    Op = maps:get(<<"op">>, Q, undefined),
    Ok = maps:get(<<"status">>, Q, undefined) =:= <<"pending">> andalso is_integer(Id) andalso Id > 0
        andalso is_integer(Gen) andalso is_binary(Op) andalso Peer =/= Requester
        andalso maps:is_key(Peer, Allowed),
    case Ok of
        false -> [];
        true ->
            IdText = integer_to_binary(Id),
            K = <<Requester/binary, "|", Op/binary, "|", IdText/binary>>,
            Common = #{ <<"root">> => Root, <<"generation">> => integer_to_binary(Gen),
                <<"requestid">> => IdText },
            case maps:is_key(K, Decided) of
                true -> [];
                false when Op =:= <<"open">> ->
                    case exact(maps:get(<<"quantity">>, Q, undefined)) of
                        N when is_integer(N), N > 0 ->
                            [{root, <<"channel-open:", K/binary>>, Common#{ <<"action">> => <<"Channel-Open">>,
                                <<"source">> => Requester, <<"destination">> => Peer,
                                <<"quantity">> => integer_to_binary(N) }}];
                        _ -> []
                    end;
                false when Op =:= <<"settle">> ->
                    settle_plan(K, Common, Requester, Peer, Q, Gen,
                        cmap(maps:get(<<Peer/binary, "|", Requester/binary>>, Channels, #{})), View(Peer), Lane);
                false when Op =:= <<"close">> ->
                    close_plan(K, Common, Requester, Peer, Gen,
                        cmap(maps:get(<<Requester/binary, "|", Peer/binary>>, Channels, #{})), Lane, View(Peer));
                false -> []
            end
    end.

%% A receiver's settle, certified only against the SENDER's own record.
settle_plan(K, Common, Receiver, Sender, Q, Gen, Ch, SenderView, ReceiverView) ->
    Seq = exact(maps:get(<<"seq">>, Q, undefined)),
    Cum = exact(maps:get(<<"cumulative">>, Q, undefined)),
    Plan = fun(C) -> [{root, <<"channel-settle:", K/binary>>, Common#{ <<"action">> => <<"Channel-Settle">>,
        <<"source">> => Sender, <<"destination">> => Receiver, <<"seq">> => integer_to_binary(Seq),
        <<"cumulative">> => integer_to_binary(C) }}] end,
    RootGen = exact(maps:get(<<"generation">>, Ch, undefined)),
    Inbound = cmap(maps:get(Sender, cmap(maps:get(<<"in">>, ReceiverView, #{})), #{})),
    Exact = is_integer(Seq) andalso Seq > 0 andalso is_integer(Cum),
    % The root has moved past this generation: it can only refuse, and
    % records that refusal; a cumulative of 0 can move nothing.
    MovedPast = is_integer(RootGen) andalso (RootGen > Gen orelse
        (RootGen =:= Gen andalso map_get_bin(<<"status">>, Ch) =:= <<"closed">>)),
    if
        not Exact -> [];
        MovedPast -> Plan(0);
        RootGen =/= Gen -> [];
        true ->
            Deposit = exact(maps:get(<<"deposit">>, Ch, undefined)),
            InSeq = exact(maps:get(<<"seq">>, Inbound, undefined)),
            Received = exact(maps:get(<<"received">>, Inbound, undefined)),
            Out = case SenderView of
                none -> #{};
                SV -> cmap(maps:get(Receiver, cmap(maps:get(<<"out">>, SV, #{})), #{}))
            end,
            SentSeq = exact(maps:get(<<"seq">>, Out, undefined)),
            Sent = exact(maps:get(<<"sent">>, Out, undefined)),
            Supported = is_integer(Deposit) andalso Cum =< Deposit
                andalso exact(maps:get(<<"generation">>, Inbound, undefined)) =:= Gen
                andalso is_integer(InSeq) andalso InSeq >= Seq
                andalso is_integer(Received) andalso Received >= Cum
                andalso SenderView =/= none
                andalso exact(maps:get(<<"generation">>, Out, undefined)) =:= Gen
                andalso is_integer(SentSeq) andalso SentSeq >= Seq
                andalso is_integer(Sent) andalso Sent >= Cum
                andalso (SentSeq =/= Seq orelse Sent =:= Cum),
            case Supported of true -> Plan(Cum); false -> [] end
    end.

%% A close, once the receiver is frozen at exactly the sender's final point
%% and has landed everything up to it.
close_plan(K, Common, Sender, Receiver, Gen, Ch, SenderView, ReceiverView) ->
    Out = cmap(maps:get(Receiver, cmap(maps:get(<<"out">>, SenderView, #{})), #{})),
    FinalSeq = exact(maps:get(<<"finalSeq">>, Out, undefined)),
    FinalSent = exact(maps:get(<<"finalSent">>, Out, undefined)),
    AtFinal = map_get_bin(<<"status">>, Out) =:= <<"closing">>
        andalso exact(maps:get(<<"generation">>, Out, undefined)) =:= Gen
        andalso is_integer(FinalSeq) andalso FinalSeq =:= exact(maps:get(<<"seq">>, Out, undefined))
        andalso is_integer(FinalSent) andalso FinalSent =:= exact(maps:get(<<"sent">>, Out, undefined)),
    Deposit = exact(maps:get(<<"deposit">>, Ch, undefined)),
    In = case ReceiverView of
        none -> #{};
        RV -> cmap(maps:get(Sender, cmap(maps:get(<<"in">>, RV, #{})), #{}))
    end,
    Supported = AtFinal
        andalso exact(maps:get(<<"generation">>, Ch, undefined)) =:= Gen
        andalso map_get_bin(<<"status">>, Ch) =:= <<"open">>
        andalso is_integer(Deposit) andalso FinalSent =< Deposit
        andalso ReceiverView =/= none
        andalso exact(maps:get(<<"generation">>, In, undefined)) =:= Gen
        andalso map_get_bin(<<"status">>, In) =:= <<"frozen">>
        andalso exact(maps:get(<<"finalSeq">>, In, undefined)) =:= FinalSeq
        andalso exact(maps:get(<<"finalReceived">>, In, undefined)) =:= FinalSent
        andalso exact(maps:get(<<"seq">>, In, undefined)) =:= FinalSeq
        andalso exact(maps:get(<<"received">>, In, undefined)) =:= FinalSent,
    case Supported of
        false -> [];
        true -> [{root, <<"channel-close:", K/binary>>, Common#{ <<"action">> => <<"Channel-Close">>,
            <<"source">> => Sender, <<"destination">> => Receiver,
            <<"finalseq">> => integer_to_binary(FinalSeq),
            <<"finalcumulative">> => integer_to_binary(FinalSent) }}]
    end.

%% An exact non-negative integer below 2^53, from a JSON number or canonical
%% decimal text; anything else is `undefined'.
exact(N) when is_integer(N), N >= 0, N < 9007199254740992 -> N;
exact(B) when is_binary(B), byte_size(B) > 0, byte_size(B) < 17 ->
    case re:run(B, <<"^(0|[1-9][0-9]*)$">>, [{capture, none}]) of
        match -> exact(binary_to_integer(B));
        nomatch -> undefined
    end;
exact(_) -> undefined.

cmap(M) when is_map(M) -> M;
cmap(_) -> #{}.
clist(L) when is_list(L) -> [X || X <- L, is_map(X)];
clist(_) -> [].
map_get_bin(K, M) -> maps:get(K, M, undefined).

committee_action(A) ->
    lists:member(A, [<<"Bridge-Mint">>, <<"Ledger-Exit">>, <<"Ledger-Move-Root">>,
        <<"Bridge-Released">>, <<"Ledger-Credited">>, <<"Channel-Open">>, <<"Channel-Settle">>,
        <<"Channel-Close">>, <<"Attestor-Apply">>]).

acknowledge(Withdrawals, St = #{ releases := Rel }) ->
    Pending = maps:from_list([{maps:get(<<"id">>, W), true} || W <- Withdrawals,
        maps:get(<<"status">>, W, <<>>) =:= <<"pending">>]),
    maps:fold(
        fun(Id, E, S) ->
            case maps:is_key(Id, Pending) of
                false -> S#{ releases := maps:remove(Id, maps:get(releases, S)) };
                true ->
                    % A release certificate names the destination transaction
                    % this node saw; without one there is nothing to certify.
                    case hb_util:bin(maps:get(tx, E, <<>>)) of
                        <<>> -> S;
                        Tx -> committee_plan(<<"released:", Id/binary>>, #{
                            <<"action">> => <<"Bridge-Released">>, <<"withdrawal-id">> => Id,
                            <<"dest-tx">> => Tx }, S)
                    end
            end
        end, St, Rel).

%%% Fast ledgers --------------------------------------------------------------

credit_processed(Info, CreditId) ->
    Credit = maps:get(<<"credit">>, Info, #{}),
    Floor = hb_util:int(maps:get(<<"floor">>, Credit, 0)),
    N = hb_util:int(CreditId),
    N =< Floor orelse lists:member(N,
        [hb_util:int(X) || X <- maps:get(<<"above">>, Credit, [])]).

credit_plans(Root, Ledger, LaneInfo, Deposits) ->
    lists:filtermap(
        fun(D) ->
            case {maps:get(<<"ledger">>, D, <<>>), maps:get(<<"status">>, D, <<>>)} of
                {Ledger, <<"ready">>} ->
                    DepositId = maps:get(<<"id">>, D),
                    CreditId = maps:get(<<"creditId">>, D),
                    case credit_processed(LaneInfo, CreditId) of
                        true ->
                            {true, {root,
                                <<"lane-credited:", DepositId/binary>>,
                                #{ <<"action">> => <<"Ledger-Credited">>,
                                   <<"depositid">> => DepositId }}};
                        false ->
                            {true, {lane,
                                <<"lane-credit:", Ledger/binary, ":", CreditId/binary>>,
                                #{ <<"action">> => <<"Ledger-Credit">>, <<"root">> => Root,
                                   <<"depositid">> => DepositId, <<"creditid">> => CreditId,
                                   <<"account">> => maps:get(<<"account">>, D),
                                   <<"quantity">> => maps:get(<<"quantity">>, D) }}}
                    end;
                _ -> false
            end
        end,
        Deposits
    ).

exit_receipts(Receipts) ->
    maps:from_list([
        {maps:get(<<"key">>, R), R}
    || R <- Receipts, is_map(R), maps:is_key(<<"key">>, R)
    ]).

exit_plans(Ledger, LaneExits, Receipts) ->
    ReceiptMap = exit_receipts(Receipts),
    lists:filtermap(
        fun(E) ->
            case maps:get(<<"status">>, E, <<>>) of
                <<"pending">> ->
                    ExitId = maps:get(<<"id">>, E),
                    Key = <<Ledger/binary, "|", ExitId/binary>>,
                    case maps:find(Key, ReceiptMap) of
                        error ->
                            {true, {root,
                                <<"lane-exit:", Key/binary>>,
                                #{ <<"action">> => <<"Ledger-Exit">>, <<"ledger">> => Ledger,
                                   <<"exit-id">> => ExitId, <<"account">> => maps:get(<<"account">>, E),
                                   <<"chain">> => maps:get(<<"chain">>, E),
                                   <<"token">> => maps:get(<<"token">>, E),
                                   <<"recipient">> => maps:get(<<"to">>, E),
                                   <<"quantity">> => maps:get(<<"quantity">>, E) }}};
                        {ok, R} ->
                            Status = maps:get(<<"status">>, R),
                            Extra = case Status of
                                <<"accepted">> -> #{ <<"withdrawalid">> => maps:get(<<"withdrawalId">>, R) };
                                <<"rejected">> -> #{ <<"error">> => maps:get(<<"error">>, R) }
                            end,
                            {true, {lane,
                                <<"lane-result:", Key/binary, ":", Status/binary>>,
                                maps:merge(#{ <<"action">> => <<"Ledger-Exit-Result">>,
                                    <<"exitid">> => ExitId, <<"status">> => Status }, Extra)}}
                    end;
                _ -> false
            end
        end,
        LaneExits
    ).

move_receipts(Receipts) ->
    maps:from_list([
        {maps:get(<<"key">>, R), R}
    || R <- Receipts, is_map(R), maps:is_key(<<"key">>, R)
    ]).

%% @doc Root certificates for pending locks whose exact source and destination
%% are both in this node's registered/configured ledger intersection. The
%% certificate carries the epoch.
move_root_plans(_Epoch, Ledger, LaneMoves, Receipts, Allowed) ->
    ReceiptMap = move_receipts(Receipts),
    lists:filtermap(
        fun(M) ->
            MoveId = maps:get(<<"id">>, M, <<>>),
            Destination = maps:get(<<"destination">>, M, <<>>),
            Account = maps:get(<<"account">>, M, <<>>),
            Quantity = maps:get(<<"quantity">>, M, <<>>),
            Eligible = is_binary(MoveId) andalso MoveId =/= <<>>
                andalso is_binary(Destination) andalso is_binary(Account)
                andalso is_binary(Quantity)
                andalso maps:get(<<"status">>, M, <<>>) =:= <<"pending">>
                andalso maps:get(<<"source">>, M, <<>>) =:= Ledger
                andalso maps:is_key(Destination, Allowed),
            case Eligible of
                false -> false;
                true ->
                    Key = <<Ledger/binary, "|", MoveId/binary>>,
                    case maps:is_key(Key, ReceiptMap) of
                        true -> false;
                        false ->
                            {true, {root,
                                <<"ledger-move-root:", Key/binary>>,
                                #{ <<"action">> => <<"Ledger-Move-Root">>,
                                   <<"source">> => Ledger,
                                   <<"destination">> => Destination,
                                   <<"move-id">> => MoveId,
                                   <<"account">> => Account,
                                   <<"quantity">> => Quantity }}}
                    end
            end
        end,
        [M || M <- LaneMoves, is_map(M)]
    ).

%% @doc A durable root receipt drives both destinations independently. This
%% remains retryable after the source lane has finalized and stopped publishing
%% a pending move.
move_receipt_plans(Root, Receipts, Allowed) ->
    lists:append([
        move_receipt_plan(Root, R, Allowed)
    || R <- Receipts, is_map(R)
    ]).

move_receipt_plan(Root, R, Allowed) ->
    Source = maps:get(<<"source">>, R, <<>>),
    Destination = maps:get(<<"destination">>, R, <<>>),
    MoveId = maps:get(<<"moveId">>, R, <<>>),
    Account = maps:get(<<"account">>, R, <<>>),
    Quantity = maps:get(<<"quantity">>, R, <<>>),
    Status = maps:get(<<"status">>, R, <<>>),
    Epoch = maps:get(<<"epoch">>, R, undefined),
    EpochText = try integer_to_binary(hb_util:int(Epoch)) catch _:_ -> undefined end,
    Error = maps:get(<<"error">>, R, <<>>),
    Eligible = maps:get(<<"root">>, R, <<>>) =:= Root
        andalso is_binary(Source) andalso is_binary(Destination)
        andalso is_binary(MoveId) andalso MoveId =/= <<>>
        andalso is_binary(Account) andalso is_binary(Quantity)
        andalso maps:is_key(Source, Allowed)
        andalso (Destination =:= Root orelse maps:is_key(Destination, Allowed))
        andalso (Status =:= <<"accepted">> orelse Status =:= <<"rejected">>)
        andalso is_binary(EpochText)
        andalso (Status =/= <<"rejected">> orelse
            (is_binary(Error) andalso Error =/= <<>>)),
    case Eligible of
        false -> [];
        true ->
            Common = #{ <<"root">> => Root, <<"source">> => Source,
                <<"destination">> => Destination, <<"moveid">> => MoveId,
                <<"account">> => Account, <<"quantity">> => Quantity,
                <<"status">> => Status,
                <<"attestorepoch">> => EpochText },
            ResultExtra = case Status of
                <<"rejected">> -> #{ <<"error">> => Error };
                <<"accepted">> -> #{}
            end,
            Result = {{lane, Source},
                <<"ledger-move-result:", Source/binary, ":", MoveId/binary,
                  ":", Status/binary>>,
                maps:merge(Common#{ <<"action">> => <<"Ledger-Move-Result">> },
                    ResultExtra)},
            case Status of
                _ when Destination =:= Root -> [Result];
                <<"rejected">> -> [Result];
                <<"accepted">> ->
                    case maps:get(<<"creditId">>, R, undefined) of
                        CreditId when is_binary(CreditId), CreditId =/= <<>> ->
                            Credit = {{lane, Destination},
                                <<"ledger-move-credit:", Destination/binary, ":",
                                  CreditId/binary>>,
                                Common#{ <<"action">> => <<"Ledger-Move-Credit">>,
                                    <<"creditid">> => CreditId }},
                            [Credit, Result];
                        _ -> []
                    end
            end
    end.

%% @doc Only a lane's registered authority signs its relinquishment. Other
%% committee members approve the resulting canonical-root intent, not a claim
%% read from an untrusted lane's HTTP state. One certificate releases as many
%% pending exits, lowest id first, as its carrier holds. The key names the
%% sequence, not the batch: the first batch journalled for a sequence is the
%% one published, so exits arriving meanwhile wait for the next sequence
%% instead of minting a competing carrier.
root_release_plans(Root, Ledger, Registry, Moves, Receipts, Epoch, Committee, Rows, Me) ->
    Row = case Rows of RowsMap when is_map(RowsMap) -> maps:get(Ledger, RowsMap, #{}); _ -> #{} end,
    Busy = maps:get(<<"status">>, Row, <<>>) =:= <<"pending">>
        andalso maps:get(<<"epoch">>, Row, -1) =:= Epoch,
    case maps:get(<<"scheduler">>, Registry, undefined) =:= Me andalso not Busy of
        false -> [];
        true ->
            Settled = move_receipts(Receipts),
            Pending = [M || M <- Moves, is_map(M),
                maps:get(<<"status">>, M, <<>>) =:= <<"pending">>,
                maps:get(<<"root">>, M, <<>>) =:= Root,
                maps:get(<<"source">>, M, <<>>) =:= Ledger,
                maps:get(<<"destination">>, M, <<>>) =:= Root,
                is_binary(maps:get(<<"id">>, M, undefined)),
                not maps:is_key(<<Ledger/binary, "|", (maps:get(<<"id">>, M))/binary>>, Settled)],
            case lists:sort(fun(A, B) ->
                    hb_util:int(maps:get(<<"id">>, A)) =< hb_util:int(maps:get(<<"id">>, B))
                end, Pending) of
                [] -> [];
                Sorted ->
                    Sequence = integer_to_binary(hb_util:int(maps:get(<<"sequence">>, Row, 0)) + 1),
                    EpochText = integer_to_binary(Epoch),
                    Operation = #{<<"action">> => <<"Ledger-Release">>, <<"ledger">> => Ledger,
                        <<"attestor-epoch">> => EpochText, <<"root-sequence">> => Sequence},
                    Batch = release_batch(Sorted, Operation, root_operation_budget(Committee), []),
                    [{root, <<"root-release:", Ledger/binary, ":", EpochText/binary, ":",
                            Sequence/binary>>, #{
                        <<"action">> => <<"Ledger-Release">>, <<"ledger">> => Ledger,
                        <<"moves">> => Batch, <<"rootsequence">> => Sequence}}]
            end
    end.

%% @doc Bytes left for `Root-Operation' in a carrier. Arweave allows 2,048
%% bytes of tag names and values; the rest of a carrier is 1,234 fixed bytes
%% (protocol tags, intent id, the 1,024-byte RSA authorization) plus one
%% 66-byte endorsement per member of a strict majority.
root_operation_budget(Committee) ->
    2048 - 1234 - 66 * (Committee div 2 + 1).

%% @doc The longest prefix of the pending exits whose operation fits the
%% budget. The first exit always goes, so an oversized one fails visibly at
%% assembly instead of silently never being released.
release_batch([Move | Rest], Operation, Budget, Entries) ->
    Next = [<<(maps:get(<<"id">>, Move))/binary, ":", (maps:get(<<"account">>, Move))/binary,
        ":", (maps:get(<<"quantity">>, Move))/binary>> | Entries],
    Fits = byte_size(hb_json:encode(Operation#{<<"moves">> => join_moves(Next)})) =< Budget,
    case Fits orelse Entries =:= [] of
        true -> release_batch(Rest, Operation, Budget, Next);
        false -> join_moves(Entries)
    end;
release_batch([], _Operation, _Budget, Entries) -> join_moves(Entries).

join_moves(Reversed) -> iolist_to_binary(lists:join(<<",">>, lists:reverse(Reversed))).

%% @doc One publisher per loop leaves the loop free to endorse requests from
%% its own worker and from peers. Only a successful POST starts the resend
%% delay; the canonical root receipt, not that POST, settles the source move.
start_root_publication(Key, Tags, St = #{cfg := Cfg, sent := Sent}) ->
    case maps:is_key(root_publication, St) orelse
            os:system_time(millisecond) - maps:get(Key, Sent, 0) < resend_ms(Cfg) of
        true -> St;
        false ->
            Parent = self(),
            {Pid, Ref} = spawn_monitor(fun() ->
                watch_root_publisher_parent(Parent),
                Result = try publish_root_certificate(Key, Tags, St)
                    catch _:_ -> {error, root_publication_failed}
                end,
                Parent ! {root_publication_done, self(), Result}
            end),
            St#{root_publication => {Pid, Ref, Key}}
    end.

%% @doc Stop the worker when its attestor stops without linking worker failures
%% back into the attestor. The watcher exits when the worker exits, including a
%% normal completion, so it retains no process or monitor after the job ends.
watch_root_publisher_parent(Parent) ->
    Worker = self(),
    spawn_link(fun() ->
        ParentRef = erlang:monitor(process, Parent),
        WorkerRef = erlang:monitor(process, Worker),
        receive
            {'DOWN', ParentRef, process, Parent, _} -> exit(shutdown);
            {'DOWN', WorkerRef, process, Worker, _} -> ok
        end
    end).

%% @doc Persist public signed artifacts before collection and publication.
%% Each retry reads the same carrier, including its fee, anchor and signature.
publish_root_certificate(Key, Tags, St = #{cfg := Cfg, opts := Opts, me := Me,
        root_certificate_info := Info}) ->
    Root = maps:get(<<"pusd">>, Cfg),
    Epoch = hb_util:int(maps:get(<<"epoch">>, Info)),
    Sequence = case maps:find(<<"rootsequence">>, Tags) of
        {ok, Seq} -> #{<<"root-sequence">> => Seq};
        error -> #{}
    end,
    Payload = (maps:merge(maps:remove(<<"rootsequence">>, Tags), Sequence))#{<<"target">> => Root,
        <<"author">> => Me, <<"certificate-version">> => <<"pusd-root/v1">>,
        <<"attestor-epoch">> => integer_to_binary(Epoch)},
    StateFile = hb_util:bin(maps:get(<<"state-file">>, Cfg, <<"bridge-state.json">>)),
    File = <<StateFile/binary, ".root-", (hb_util:human_id(crypto:hash(sha256, <<Root/binary, Key/binary>>)))/binary>>,
    % The key names the root sequence, not the batch: what was journalled
    % first for it is what this sequence publishes, whatever a later pass plans.
    Saved = case file:read_file(File) of
        {ok, Bin} -> binary_to_term(Bin, [safe]);
        {error, enoent} ->
            Intent = hb_message:commit(Payload, Opts, <<"ans104@1.0">>),
            Initial = #{payload => Payload, intent => Intent},
            save_root_publication(File, Initial),
            Initial;
        {error, Reason} -> error({root_journal_unreadable, Reason})
    end,
    Carrier = case maps:find(carrier, Saved) of
        {ok, Transaction} -> Transaction;
        error ->
            Members = maps:get(<<"attestors">>, Info),
            Intent0 = maps:get(intent, Saved),
            TagRows = case collect_root_carrier(Root, Epoch, Members, Intent0, St) of
                {ok, #{<<"tags">> := Rows}} -> Rows;
                NotEndorsed ->
                    % Nothing was signed for L1: forget the intent, so the
                    % next pass plans this sequence afresh (fail closed).
                    _ = file:delete(File),
                    error({not_endorsed, NotEndorsed})
            end,
            {ok, Price} = hb_ao:resolve(#{<<"device">> => <<"arweave@2.9">>},
                #{<<"path">> => <<"/price">>, <<"size">> => 0}, Opts),
            {ok, Anchor} = hb_ao:resolve(#{<<"device">> => <<"arweave@2.9">>},
                #{<<"path">> => <<"/tx_anchor">>}, Opts),
            Unsigned = ar_tx:normalize(#tx{format = 2, data = <<>>, data_size = 0,
                anchor = Anchor, reward = hb_util:int(Price),
                tags = [{maps:get(<<"name">>, T), maps:get(<<"value">>, T)}
                    || T <- hb_util:message_to_ordered_list(TagRows)]}),
            Signed = ar_tx:normalize(ar_tx:sign(Unsigned,
                hb_opts:get(priv_wallet, no_viable_wallet, Opts))),
            save_root_publication(File, Saved#{carrier => Signed}),
            Signed
    end,
    Message = hb_message:convert(Carrier, <<"structured@1.0">>, <<"tx@1.0">>, Opts),
    hb_ao:resolve(#{<<"device">> => <<"arweave@2.9">>},
        Message#{<<"path">> => <<"tx">>, <<"method">> => <<"POST">>}, Opts).

%% @doc Flush the signed bytes before atomically replacing the public journal.
save_root_publication(File, State) ->
    ok = filelib:ensure_dir(File),
    Tmp = <<File/binary, ".tmp">>,
    ok = file:write_file(Tmp, term_to_binary(State), [raw, binary, sync]),
    ok = file:rename(Tmp, File).

%% @doc Request proofs concurrently. Location records are routing hints only:
%% the assembler checks every proof against the verified membership snapshot.
collect_root_carrier(Root, Epoch, Members, Intent, St = #{opts := Opts}) ->
    Parent = self(),
    Ref = make_ref(),
    Workers = [spawn(fun() ->
        Proof = try request_root_proof(Member, Intent, St) catch _:_ -> #{} end,
        Parent ! {Ref, Proof}
    end) || Member <- Members],
    Request = #{<<"path">> => <<"assemble">>, <<"root">> => Root, <<"epoch">> => Epoch,
        <<"committee">> => Members, <<"intent">> => Intent},
    try collect_root_proofs(Ref, length(Workers), [], Request,
            erlang:monotonic_time(millisecond) + 65000, Opts)
    after lists:foreach(fun(Pid) -> exit(Pid, kill) end, Workers)
    end.

%% @doc Stop as soon as the same root verifier accepts a majority certificate.
collect_root_proofs(_Ref, 0, _Proofs, _Request, _Deadline, _Opts) -> {error, no_majority};
collect_root_proofs(Ref, Pending, Proofs, Request, Deadline, Opts) ->
    receive
        {Ref, Proof} ->
            Next = [Proof | Proofs],
            case hb_ao:resolve(#{<<"device">> => <<"bridge-root@1.0">>},
                    Request#{<<"proofs">> => Next}, Opts) of
                {ok, _} = Ready -> Ready;
                _ -> collect_root_proofs(Ref, Pending - 1, Next, Request, Deadline, Opts)
            end
    after max(0, Deadline - erlang:monotonic_time(millisecond)) -> {error, endorsement_timeout}
    end.

%% @doc Local requests traverse the same verified-state policy as remote ones.
request_root_proof(#{<<"ao">> := Me}, Intent, #{me := Me}) ->
    {ok, Proof} = call({root_endorse, Intent}),
    Proof;
request_root_proof(#{<<"ao">> := Address}, Intent, #{opts := Opts}) ->
    {ok, Location} = hb_ao:resolve(#{<<"device">> => <<"location@1.0">>},
        #{<<"path">> => Address, <<"variant">> => <<"ao.N.1">>}, Opts),
    Node = hb_ao:get(<<"url">>, Location, Opts),
    fetch_root_proof(Node, Intent, Opts).

%% @doc Send the exact signed intent using HyperBEAM's ANS-104 HTTP codec.
fetch_root_proof(Node, Intent, Opts) ->
    {ok, Response} = hb_http:post(Node, <<"/~bridge-attestor@1.0/root-endorse">>,
        Intent#{<<"accept">> => <<"application/json">>},
        Opts#{<<"http-codec">> => <<"ans104@1.0">>}),
    Result = case maps:get(<<"body">>, Response, Response) of
        Bin when is_binary(Bin) -> hb_json:decode(Bin);
        Map -> Map
    end,
    maps:with([<<"signer">>, <<"intent-id">>, <<"signature">>], Result).

fast_ledger_node(Spec, Cfg) when is_binary(Spec) -> Spec;
fast_ledger_node(Spec, Cfg) when is_map(Spec) ->
    maps:get(<<"node">>, Spec, maps:get(<<"scheduler-url">>, Cfg));
fast_ledger_node(_, Cfg) -> maps:get(<<"scheduler-url">>, Cfg).

relay_fast_ledgers(Info, Deposits, Receipts, MoveReceipts, Epoch,
        St = #{ cfg := Cfg }) ->
    Root = maps:get(<<"pusd">>, Cfg),
    Allowed = fast_ledgers(maps:get(<<"ledgers">>, Info, #{}),
        maps:get(<<"fast-ledgers">>, Cfg, #{})),
    St1 = dispatch_move_plans(
        move_receipt_plans(Root, MoveReceipts, Allowed),
        St#{ fast_ledgers => Allowed }
    ),
    maps:fold(
        fun(Ledger, Spec, S) ->
                    guard(<<"ledger:", Ledger/binary>>, S, fun() ->
                        Node = fast_ledger_node(Spec, Cfg),
                        case lane_view(Ledger, Node, S) of
                            {ok, LaneView, _Replica} ->
                                % Kept: a peer's lane certificate is endorsed
                                % only against this node's own lane replica.
                                SV = S#{ lane_views => (maps:get(lane_views, S, #{}))#{ Ledger => LaneView } },
                                LaneInfo = key(<<"laneinfo">>, LaneView),
                                LaneExits = case key(<<"laneexits">>, LaneView) of
                                    L when is_list(L) -> L;
                                    _ -> []
                                end,
                                LaneMoves = case key(<<"lanemoves">>, LaneView) of
                                    L2 when is_list(L2) -> L2;
                                    _ -> []
                                end,
                                Plans = credit_plans(Root, Ledger, LaneInfo, Deposits)
                                    ++ exit_plans(Ledger, LaneExits, Receipts),
                                S2 = lists:foldl(
                                    fun({root, RetryKey, Tags = #{<<"action">> := A}}, Acc) ->
                                            case committee_action(A) of
                                                true -> committee_plan(RetryKey, Tags, Acc);
                                                false -> guard(RetryKey, Acc, fun() ->
                                                    once(RetryKey, Acc, fun() -> send(Tags, Acc) end) end)
                                            end;
                                       ({lane, RetryKey, Tags}, Acc) ->
                                        guard(RetryKey, Acc, fun() ->
                                            once(RetryKey, Acc, fun() ->
                                                send_lane(Ledger, Node, Tags, Acc)
                                            end)
                                        end)
                                    end,
                                    SV,
                                    Plans
                                ),
                                dispatch_move_plans(
                                    move_root_plans(Epoch, Ledger, LaneMoves,
                                        MoveReceipts, Allowed)
                                    ++ root_release_plans(Root, Ledger,
                                        maps:get(Ledger, maps:get(<<"ledgers">>, Info, #{}), #{}),
                                        LaneMoves, MoveReceipts, Epoch,
                                        length(maps:get(<<"attestors">>, Info)),
                                        maps:get(root_intents, St, #{}), maps:get(me, St)), S2
                                );
                            {not_yet, Why} ->
                                S#{ skipped := (maps:get(skipped, S))#{
                                    <<"ledger:", Ledger/binary>> => Why
                                } }
                        end
                    end)
        end,
        St1,
        Allowed
    ).

%% @doc The configured fast ledgers that the root registered, keyed by the
%% root's own spelling of each id. A node's config keys arrive lowercased
%% (HTTP-normalised), but a process id is case-sensitive: matching them
%% exactly found no ledger at all, so no root credit ever reached a lane.
fast_ledgers(Registered, Configured) ->
    ByLower = maps:fold(
        fun(K, Spec, Acc) -> Acc#{ hb_util:to_lower(K) => Spec } end,
        #{},
        Configured
    ),
    maps:fold(
        fun(Ledger, _, Acc) ->
            case maps:find(hb_util:to_lower(Ledger), ByLower) of
                {ok, Spec} -> Acc#{ Ledger => Spec };
                error -> Acc
            end
        end,
        #{},
        Registered
    ).

dispatch_move_plans(Plans, St = #{ cfg := Cfg }) ->
    Configured = maps:get(fast_ledgers, St, #{}),
    lists:foldl(
        fun({root, RetryKey, Tags = #{<<"action">> := <<"Ledger-Release">>}}, Acc) ->
            start_root_publication(RetryKey, Tags, Acc);
        ({root, RetryKey, Tags = #{<<"action">> := <<"Ledger-Move-Root">>}}, Acc) ->
            committee_plan(RetryKey, Tags, Acc);
        ({Target, RetryKey, Tags}, Acc) ->
            guard(RetryKey, Acc, fun() ->
                once(RetryKey, Acc, fun() ->
                    case Target of
                        root -> send(Tags, Acc);
                        {lane, Ledger} ->
                            Spec = maps:get(Ledger, Configured),
                            send_lane(Ledger, fast_ledger_node(Spec, Cfg), Tags, Acc)
                    end
                end)
            end)
        end,
        St,
        Plans
    ).

%% One withdrawal or proposal PUSD published that this node cannot sign (a
%% malformed address, a chain it does not run) is skipped and shown in
%% `status', instead of failing every pass and halting the rest.
guard(Key, St, Fun) ->
    try Fun()
    catch C:R:T ->
        {Why, Log} = failure(C, R, T),
        ?event(bridge, {skipped, Key, Log}),
        St#{ skipped := (maps:get(skipped, St))#{ Key => Why } }
    end.

item_id(#{ <<"id">> := Id }) when is_binary(Id) -> Id;
item_id(#{ <<"id">> := Id }) when is_integer(Id) -> integer_to_binary(Id);
item_id(_) -> <<"?">>.

%% Send at most once per RESEND_MS per key: PUSD is idempotent, but every
%% message is a slot.
once(Key, St = #{ sent := Sent }, Fun) ->
    Now = os:system_time(millisecond),
    Cfg = maps:get(cfg, St),
    Resend = resend_ms(Cfg),
    case Now - maps:get(Key, Sent, 0) < Resend of
        true -> St;
        false ->
            case Fun() of
                {ok, _} -> St#{ sent := Sent#{ Key => Now } };
                Other -> error({send_failed, Other})
            end
    end.

resend_ms(Cfg) ->
    case maps:get(<<"scheduler-device">>, Cfg, <<"scheduler@1.0">>) of
        <<"arweave-scheduler@1.0">> ->
            cfg_ms(Cfg, <<"arweave-resend-ms">>, ?ARWEAVE_RESEND_MS);
        _ -> ?RESEND_MS
    end.

%%% Signatures ----------------------------------------------------------------

%% @doc This node's signature over one withdrawal's release on its chain, as
%% 0x-hex. The chain's withdrawal id carries PUSD's namespace in the high word.
withdrawal_signature(W, Epoch, Cfg, Keys, Namespace) ->
    Chain = maps:get(<<"chain">>, W),
    C = maps:get(Chain, Cfg),
    Root = maps:get(<<"pusd">>, Cfg),
    Id = (Namespace bsl 32) bor hb_util:int(maps:get(<<"id">>, W)),
    Token = maps:get(<<"token">>, W),
    To = maps:get(<<"to">>, W),
    Amount = maps:get(<<"amount">>, W),
    Sig = case Chain of
        <<"base">> ->
            dev_bridge_attestor_sign:base_sign(
                dev_bridge_attestor_sign:base_release(hb_util:int(maps:get(<<"chain-id">>, C)),
                    maps:get(<<"contract">>, C), Root, Epoch, Id, Token, To, Amount),
                maps:get(base, Keys));
        <<"solana">> ->
            dev_bridge_attestor_sign:ed25519_sign(
                dev_bridge_attestor_sign:solana_release(maps:get(<<"program">>, C),
                    Root, Epoch, Id, Token, To, Amount),
                maps:get(solana, Keys));
        <<"sui">> ->
            dev_bridge_attestor_sign:ed25519_sign(
                dev_bridge_attestor_sign:sui_release(maps:get(<<"bridge">>, C),
                    Root, Epoch, Id, Token, To, Amount),
                maps:get(sui, Keys))
    end,
    <<"0x", (dev_bridge_attestor_sign:hex(Sig))/binary>>.

%% @doc The membership proposal's frozen epochs, which must describe the one
%% transition every chain contract accepts.
proposal_epochs(P) ->
    Prior = hb_util:int(maps:get(<<"priorEpoch">>, P)),
    Current = hb_util:int(maps:get(<<"currentEpoch">>, P)),
    New = hb_util:int(maps:get(<<"newEpoch">>, P)),
    case Prior =:= Current andalso New =:= Current + 1 of
        true -> {Current, New};
        false -> error({bad_proposal_epochs, Prior, Current, New})
    end.

%% @doc A migration preserves the current custodian epoch.
migration_epoch(P) ->
    Prior = hb_util:int(maps:get(<<"priorEpoch">>, P)),
    Current = hb_util:int(maps:get(<<"currentEpoch">>, P)),
    New = hb_util:int(maps:get(<<"newEpoch">>, P)),
    case Prior =:= Current andalso New =:= Current of
        true -> Current;
        false -> error({bad_migration_epochs, Prior, Current, New})
    end.

%% @doc The administration proposal's frozen current epoch.
admin_epoch(P) ->
    Prior = hb_util:int(maps:get(<<"priorEpoch">>, P)),
    Current = hb_util:int(maps:get(<<"currentEpoch">>, P)),
    New = hb_util:int(maps:get(<<"newEpoch">>, P)),
    Epoch = hb_util:int(maps:get(<<"epoch">>, P)),
    case Prior =:= Current andalso Current =:= New andalso New =:= Epoch of
        true -> Epoch;
        false -> error({bad_admin_epochs, Prior, Current, New, Epoch})
    end.

%% @doc Chain signatures carried by a majority-controlled administration vote.
admin_vote_tags(P, Cfg, Keys) ->
    Id = integer_to_binary(hb_util:int(maps:get(<<"id">>, P))),
    Epoch = admin_epoch(P),
    EpochText = integer_to_binary(Epoch),
    Base = #{ <<"action">> => <<"Attestor-Vote">>, <<"proposalid">> => Id,
        <<"currentepoch">> => EpochText, <<"newepoch">> => EpochText },
    case maps:get(<<"op">>, P) of
        <<"add-token">> ->
            Admin = maps:get(<<"admin">>, P),
            Chain = maps:get(<<"chain">>, Admin),
            Token = maps:get(<<"token">>, Admin),
            maps:merge(Base, admin_chain_signature(Chain, Token,
                Epoch, Cfg, Keys));
        <<"unpause">> ->
            maps:merge(Base, maps:merge(
                admin_chain_signature(<<"base">>, zero_token(<<"base">>),
                    Epoch, Cfg, Keys, 2),
                maps:merge(
                    admin_chain_signature(<<"solana">>, zero_token(<<"solana">>),
                        Epoch, Cfg, Keys, 2),
                    admin_chain_signature(<<"sui">>, zero_token(<<"sui">>),
                        Epoch, Cfg, Keys, 2)
                )
            ));
        _ -> Base
    end.

admin_chain_signature(Chain, Token, Epoch, Cfg, Keys) ->
    admin_chain_signature(Chain, Token, Epoch, Cfg, Keys, 1).

admin_chain_signature(<<"base">>, Token, Epoch, Cfg, Keys, Action) ->
    C = maps:get(<<"base">>, Cfg),
    Root = maps:get(<<"pusd">>, Cfg),
    Sig = dev_bridge_attestor_sign:base_sign(
        dev_bridge_attestor_sign:base_admin(
            hb_util:int(maps:get(<<"chain-id">>, C)), maps:get(<<"contract">>, C),
            Root, Epoch, Action, Token, true),
        maps:get(base, Keys)
    ),
    #{ <<"basesig">> => signature_hex(Sig) };
admin_chain_signature(<<"solana">>, Token, Epoch, Cfg, Keys, Action) ->
    C = maps:get(<<"solana">>, Cfg),
    Root = maps:get(<<"pusd">>, Cfg),
    Sig = dev_bridge_attestor_sign:ed25519_sign(
        dev_bridge_attestor_sign:solana_admin(
            maps:get(<<"program">>, C), Root, Epoch, Action, Token, true),
        maps:get(solana, Keys)
    ),
    #{ <<"solanasig">> => signature_hex(Sig) };
admin_chain_signature(<<"sui">>, Token, Epoch, Cfg, Keys, Action) ->
    C = maps:get(<<"sui">>, Cfg),
    Root = maps:get(<<"pusd">>, Cfg),
    Sig = dev_bridge_attestor_sign:ed25519_sign(
        dev_bridge_attestor_sign:sui_admin(
            maps:get(<<"bridge">>, C), Root, Epoch, Action, Token, true),
        maps:get(sui, Keys)
    ),
    #{ <<"suisig">> => signature_hex(Sig) }.

signature_hex(Sig) ->
    <<"0x", (dev_bridge_attestor_sign:hex(Sig))/binary>>.

zero_token(<<"base">>) -> <<"0x0000000000000000000000000000000000000000">>;
zero_token(<<"solana">>) -> <<"11111111111111111111111111111111">>;
zero_token(<<"sui">>) -> <<>>.

%% @doc This node's signature, per chain it runs, over an applied change: the
%% chain authorisation for the proposal's whole set at its epochs, for a
%% migration's exact old-root to successor transition, or for an
%% administration action. A chain this node does not run is left out.
proposal_signatures(#{ <<"kind">> := <<"admin">> } = P, Cfg, Keys) ->
    Tags = try admin_vote_tags(P, Cfg, Keys) catch _:_ -> #{} end,
    maps:from_list([{Chain, V} || {K, Chain} <- [{<<"basesig">>, <<"base">>},
        {<<"solanasig">>, <<"solana">>}, {<<"suisig">>, <<"sui">>}], V <- [maps:get(K, Tags, none)], V =/= none]);
proposal_signatures(#{ <<"op">> := <<"migrate">> } = P, Cfg, Keys) ->
    Epoch = migration_epoch(P),
    Root = maps:get(<<"pusd">>, Cfg),
    Successor = maps:get(<<"successor">>, P),
    per_chain(Cfg, #{
        <<"base">> => fun(B) -> dev_bridge_attestor_sign:base_sign(
            dev_bridge_attestor_sign:base_root(hb_util:int(maps:get(<<"chain-id">>, B)),
                maps:get(<<"contract">>, B), Epoch, Root, Successor), maps:get(base, Keys)) end,
        <<"solana">> => fun(Sol) -> dev_bridge_attestor_sign:ed25519_sign(
            dev_bridge_attestor_sign:solana_root(maps:get(<<"program">>, Sol),
                Epoch, Root, Successor), maps:get(solana, Keys)) end,
        <<"sui">> => fun(Sui) -> dev_bridge_attestor_sign:ed25519_sign(
            dev_bridge_attestor_sign:sui_root(maps:get(<<"bridge">>, Sui),
                Epoch, Root, Successor), maps:get(sui, Keys)) end});
proposal_signatures(P, Cfg, Keys) ->
    Set = maps:get(<<"set">>, P),
    {CurrentEpoch, NewEpoch} = proposal_epochs(P),
    Root = maps:get(<<"pusd">>, Cfg),
    Field = fun(K) -> [maps:get(K, M) || M <- Set] end,
    per_chain(Cfg, #{
        <<"base">> => fun(B) -> dev_bridge_attestor_sign:base_sign(
            dev_bridge_attestor_sign:base_attestors(hb_util:int(maps:get(<<"chain-id">>, B)),
                maps:get(<<"contract">>, B), Root, CurrentEpoch, NewEpoch,
                Field(<<"base">>)), maps:get(base, Keys)) end,
        <<"solana">> => fun(Sol) -> dev_bridge_attestor_sign:ed25519_sign(
            dev_bridge_attestor_sign:solana_attestors(maps:get(<<"program">>, Sol),
                Root, CurrentEpoch, NewEpoch, Field(<<"solana">>)), maps:get(solana, Keys)) end,
        <<"sui">> => fun(Sui) -> dev_bridge_attestor_sign:ed25519_sign(
            dev_bridge_attestor_sign:sui_attestors(maps:get(<<"bridge">>, Sui),
                Root, CurrentEpoch, NewEpoch, Field(<<"sui">>)), maps:get(sui, Keys)) end}).

per_chain(Cfg, Signers) ->
    maps:from_list([{Chain, <<"0x", (dev_bridge_attestor_sign:hex(Sig))/binary>>}
        || {Chain, Sign} <- maps:to_list(Signers), C <- [maps:get(Chain, Cfg, none)], C =/= none,
        Sig <- [try Sign(C) catch _:_ -> skip end], Sig =/= skip]).

%%% Succession ----------------------------------------------------------------

%% Move to the successor if it checks out; otherwise stay, say why, and keep
%% attesting here (this PUSD still owes its pending withdrawals).
follow(Succ, Info, View, St = #{ cfg := Cfg }) ->
    Here = maps:get(<<"pusd">>, Cfg),
    Next = case view(Succ, St) of
        {ok, SuccView, _} -> key(<<"bridgeinfo">>, SuccView);
        {not_yet, _} -> #{}
    end,
    case successor_ok(Info, Next, Here) of
        true ->
            Cfg2 = Cfg#{ <<"pusd">> => Succ },
            save_follow(Cfg2),
            ?event(bridge, {followed, {from, Here}, {to, Succ}}),
            St#{ cfg := Cfg2, deposits := #{}, releases := #{}, sent := #{}, namespace := 0,
                last_pass := os:system_time(millisecond), last_error := null };
        {false, Why} ->
            St2 = attest(Info, View, St),
            St2#{ last_error := <<"not following ", Succ/binary, ": ", Why/binary>> }
    end.

%% @doc Does `Next' (the successor's bridgeinfo) carry on from `Info' (the
%% bridgeinfo of `Here')? It must name `Here' back and hold the same members,
%% keys and epoch: the set that voted to move is the set that arrives.
successor_ok(Info, Next, Here) ->
    Members = fun(I) ->
        [[maps:get(K, M, <<>>) || K <- [<<"ao">>, <<"base">>, <<"solana">>, <<"sui">>]]
            || M <- maps:get(<<"attestors">>, I, [])]
    end,
    Epoch = fun(I) -> hb_util:int(maps:get(<<"epoch">>, I, -1)) end,
    case {Next =:= #{}, maps:get(<<"predecessor">>, Next, <<>>) =:= Here,
            Members(Next) =:= Members(Info), Epoch(Next) =:= Epoch(Info)} of
        {true, _, _, _} -> {false, <<"its bridgeinfo cannot be read">>};
        {_, false, _, _} -> {false, <<"it does not name this PUSD">>};
        {_, _, false, _} -> {false, <<"its set differs">>};
        {_, _, _, false} -> {false, <<"its epoch differs">>};
        _ -> true
    end.

follow_file(Cfg) -> maps:get(<<"follow-file">>, Cfg, <<"bridge-follow.json">>).

%% Each chain's `rpc-file', read into its `rpc' in the loop's own state only:
%% the node message keeps the path, never the URL. An unreadable file stops
%% the loop at start.
rpc_files(Cfg) ->
    maps:map(
        fun(_Chain, #{ <<"rpc-file">> := File } = C) ->
                {ok, Url} = file:read_file(File),
                C#{ <<"rpc">> => string:trim(Url) };
           (_Key, Value) -> Value
        end,
        Cfg
    ).

%% The configured PUSD, or where the node has followed it to since.
load_follow(Cfg) ->
    Genesis = maps:get(<<"pusd">>, Cfg),
    case file:read_file(follow_file(Cfg)) of
        {ok, Bin} ->
            case catch hb_json:decode(Bin) of
                #{ <<"genesis">> := Genesis, <<"pusd">> := Pusd } -> Cfg#{ <<"pusd">> => Pusd,
                    <<"genesis">> => Genesis };
                _ -> Cfg#{ <<"genesis">> => Genesis }
            end;
        _ -> Cfg#{ <<"genesis">> => Genesis }
    end.

save_follow(Cfg) ->
    File = follow_file(Cfg),
    Tmp = <<File/binary, ".tmp">>,
    ok = file:write_file(Tmp, hb_json:encode(#{ <<"genesis">> => maps:get(<<"genesis">>, Cfg),
        <<"pusd">> => maps:get(<<"pusd">>, Cfg) })),
    ok = file:rename(Tmp, File).

%%% AO ------------------------------------------------------------------------

%% Commit with the node's wallet and post to PUSD's scheduler, as any wallet
%% client does. What becomes of it, the node learns from its replica.
send(Tags, #{ cfg := Cfg, opts := Opts }) ->
    Pusd = maps:get(<<"pusd">>, Cfg),
    send_at(Pusd, maps:get(<<"scheduler-url">>, Cfg), Tags,
        #{ cfg => Cfg, opts => Opts }).

send_at(Process, Node, Tags, #{ cfg := Cfg, opts := Opts }) ->
    Res = case maps:get(<<"scheduler-device">>, Cfg, <<"scheduler@1.0">>) of
        <<"arweave-scheduler@1.0">> -> send_l1(Process, Tags, Opts);
        _ -> post_scheduled(Process, Node, Tags, Opts)
    end,
    ?event(bridge, {sent, maps:get(<<"action">>, Tags), Res}),
    Res.

%% A fast ledger is scheduled by its own node (`scheduler@1.0'), whatever
%% device orders the root: an L1 transaction addressed to it is never read.
send_lane(Ledger, Node, Tags, #{ opts := Opts }) ->
    Res = post_scheduled(Ledger, Node, Tags, Opts),
    ?event(bridge, {sent, maps:get(<<"action">>, Tags), Res}),
    Res.

post_scheduled(Process, Node, Tags, Opts) ->
    % `subject: self' or the local scheduler schedules the request body.
    Signed = hb_message:commit(Tags#{ <<"target">> => Process,
        <<"type">> => <<"Message">>, <<"subject">> => <<"self">> },
        Opts, <<"httpsig@1.0">>),
    hb_http:post(Node, <<"/", Process/binary, "~process@1.0/schedule">>, Signed, Opts).

%% An Arweave-scheduled process accepts only a signed, data-free L1 transaction
%% header. `Assign-To' routes it without paying the one-time native-recipient
%% premium for a process id that can never own a private key.
send_l1(Process, Tags, Opts) ->
    maybe
        {ok, Price0} ?= hb_ao:resolve(
            #{ <<"device">> => <<"arweave@2.9">> },
            #{ <<"path">> => <<"/price">>, <<"size">> => 0 },
            Opts
        ),
        {ok, Anchor} ?= hb_ao:resolve(
            #{ <<"device">> => <<"arweave@2.9">> },
            #{ <<"path">> => <<"/tx_anchor">> },
            Opts
        ),
        Wallet = hb_opts:get(priv_wallet, no_viable_wallet, Opts),
        Unsigned = l1_unsigned(Process, Tags),
        Signed = ar_tx:normalize(ar_tx:sign(
            Unsigned#tx{ anchor = Anchor, reward = hb_util:int(Price0) },
            Wallet
        )),
        Committed = hb_message:convert(
            Signed, <<"structured@1.0">>, <<"tx@1.0">>, Opts
        ),
        hb_ao:resolve(
            #{ <<"device">> => <<"arweave@2.9">> },
            Committed#{ <<"path">> => <<"tx">>, <<"method">> => <<"POST">> },
            Opts
        )
    end.

l1_unsigned(Process, Tags) ->
    AppTags = [{K, hb_util:bin(V)} || {K, V} <- lists:sort(maps:to_list(Tags))],
    ar_tx:normalize(#tx{
        format = 2,
        data = <<>>,
        data_size = 0,
        tags = [{<<"Assign-To">>, Process}, {<<"Type">>, <<"Message">>} | AppTags]
    }).

%% PUSD (`Pusd', the configured one or a successor) as this node computed it:
%% `{ok, View, Replica}' with the replica's slot and where it stopped, if it
%% did; `{not_yet, Why}' while no slot has verified.
view(Pusd, #{ cfg := Cfg, opts := Opts }) ->
    Dir = hb_util:bin(maps:get(<<"replica-dir">>, Cfg, <<"bridge-replica">>)),
    case lib_bridge_replica:view(Pusd, maps:get(<<"scheduler-url">>, Cfg),
            #{ opts => Opts, dir => Dir }) of
        {ok, State, ROpts, Replica} -> {ok, {State, ROpts}, Replica};
        {not_yet, Why} -> {not_yet, Why}
    end.

lane_view(Ledger, Node, #{ cfg := Cfg, opts := Opts }) ->
    BaseDir = hb_util:bin(maps:get(<<"replica-dir">>, Cfg, <<"bridge-replica">>)),
    Dir = <<BaseDir/binary, "/lane-", Ledger/binary>>,
    case lib_bridge_replica:view(Ledger, Node, #{ opts => Opts, dir => Dir }) of
        {ok, State, ROpts, Replica} -> {ok, {State, ROpts}, Replica};
        {not_yet, Why} -> {not_yet, Why}
    end.

%% One key of a view, as decoded JSON; #{} when absent.
key(Key, {State, ROpts}) -> json_value(hb_ao:get(Key, State, not_found, ROpts)).

%% A key of PUSD's state as decoded JSON: PUSD keeps its tables as JSON text.
json_value(V) when is_map(V); is_list(V) -> V;
json_value(Bin) when is_binary(Bin) ->
    case catch hb_json:decode(Bin) of
        V when is_map(V); is_list(V) -> V;
        _ -> #{}
    end;
json_value(_) -> #{}.

%% @doc Has PUSD minted `<chain>:<nonce>'? Its published watermark says.
minted(Info, Chain, Nonce) ->
    P = maps:get(Chain, maps:get(<<"processed">>, Info, #{}), #{}),
    Nonce =< hb_util:int(maps:get(<<"floor">>, P, 0))
        orelse lists:member(Nonce, [hb_util:int(N) || N <- maps:get(<<"above">>, P, [])]).

deposit_id(Chain, N) -> <<Chain/binary, ":", (integer_to_binary(N))/binary>>.

%% A chain withdrawal id is PUSD's only if its high word is PUSD's namespace,
%% which PUSD publishes in `bridgeinfo'.
local_withdrawal(ChainId, #{ namespace := Ns }) ->
    case ChainId bsr 32 of
        Ns -> integer_to_binary(ChainId band 16#FFFFFFFF);
        _ -> false
    end.

%%% Keys and state on disk ----------------------------------------------------

%% The chain keys and the committee's BLS key, made on first start: hex
%% 32-byte secrets, and the BLS public key beside its secret. A key file
%% from before the BLS key gains one, written once and never replaced: a
%% node's registered BLS key is as permanent as its chain keys. The file is
%% owner-only before a secret byte is in it, and no endpoint returns a secret.
load_keys(File) ->
    case file:read_file(File) of
        {ok, Bin} ->
            K = hb_json:decode(Bin),
            Keys = #{ base => unhex(maps:get(<<"base">>, K)), solana => unhex(maps:get(<<"solana">>, K)),
               sui => unhex(maps:get(<<"sui">>, K)) },
            case {maps:find(<<"bls">>, K), maps:find(<<"bls-public">>, K)} of
                {{ok, Secret}, {ok, Public}} -> Keys#{ bls => {unhex(Secret), unhex(Public)} };
                {error, error} ->
                    WithBls = Keys#{ bls => new_bls_key() },
                    write_keys(File, WithBls),
                    WithBls;
                _ -> error(bls_key_file_is_partial)
            end;
        {error, enoent} ->
            K = #{ base => crypto:strong_rand_bytes(32), solana => crypto:strong_rand_bytes(32),
                   sui => crypto:strong_rand_bytes(32), bls => new_bls_key() },
            write_keys(File, K),
            K
    end.

new_bls_key() ->
    {Secret, Public} = blst_nif:keygen(crypto:strong_rand_bytes(32)),
    {Secret, Public}.

write_keys(File, K) ->
    {Secret, Public} = maps:get(bls, K),
    Fields = [{<<"base">>, maps:get(base, K)}, {<<"solana">>, maps:get(solana, K)},
        {<<"sui">>, maps:get(sui, K)}, {<<"bls">>, Secret}, {<<"bls-public">>, Public}],
    Tmp = <<(hb_util:bin(File))/binary, ".tmp">>,
    ok = file:write_file(Tmp, <<>>),
    ok = file:change_mode(Tmp, 8#600),
    ok = file:write_file(Tmp, hb_json:encode(maps:from_list(
        [{N, dev_bridge_attestor_sign:hex(V)} || {N, V} <- Fields])), [raw, binary, sync]),
    ok = file:rename(Tmp, File).

unhex(<<"0x", H/binary>>) -> dev_bridge_attestor_sign:unhex(H);
unhex(H) -> dev_bridge_attestor_sign:unhex(H).

public_keys(#{ me := Me, keys := K }) ->
    #{
        <<"ao">> => Me,
        <<"base">> => dev_bridge_attestor_sign:base_address(maps:get(base, K)),
        <<"solana">> => dev_bridge_attestor_sign:base58_encode(dev_bridge_attestor_sign:ed25519_public(maps:get(solana, K))),
        <<"sui">> => <<"0x", (dev_bridge_attestor_sign:hex(dev_bridge_attestor_sign:ed25519_public(maps:get(sui, K))))/binary>>,
        <<"bls">> => <<"0x", (dev_bridge_attestor_sign:hex(element(2, maps:get(bls, K))))/binary>>,
        <<"blsPop">> => bls_possession(Me, maps:get(bls, K))
    }.

%% @doc This node's proof of possession of its BLS key, for its registration.
bls_possession(Me, {Secret, Public}) ->
    <<"0x", (dev_bridge_attestor_sign:hex(blst_nif:sign(Secret,
        lib_bridge_bls:possession_bytes(Me, Public), lib_bridge_bls:pop_dst())))/binary>>.

%% Cursors, deposits and releases. A file holding only cursors (the old shape)
%% loads with nothing waiting.
load_state(Cfg) ->
    case file:read_file(maps:get(<<"state-file">>, Cfg, <<"bridge-state.json">>)) of
        {ok, Bin} ->
            case hb_json:decode(Bin) of
                #{ <<"cursors">> := C } = S ->
                    {C, events_in(maps:get(<<"deposits">>, S, #{})),
                        events_in(maps:get(<<"releases">>, S, #{}))};
                C -> {C, #{}, #{}}
            end;
        _ -> {#{}, #{}, #{}}
    end.

save_state(Cfg, #{ cursors := C, deposits := D, releases := R }) ->
    File = maps:get(<<"state-file">>, Cfg, <<"bridge-state.json">>),
    Tmp = <<File/binary, ".tmp">>,
    ok = file:write_file(Tmp, hb_json:encode(#{ <<"cursors">> => C,
        <<"deposits">> => events_out(D), <<"releases">> => events_out(R) })),
    ok = file:rename(Tmp, File).

%% An event is a map with atom keys and `kind' an atom; on disk, all binaries.
events_out(M) ->
    maps:map(fun(_, E) -> maps:fold(fun(K, V, A) -> A#{ atom_to_binary(K) =>
        case is_atom(V) of true -> atom_to_binary(V); false -> V end } end, #{}, E) end, M).

events_in(M) ->
    maps:map(fun(_, E) -> maps:fold(fun
        (<<"kind">>, V, A) -> A#{ kind => binary_to_existing_atom(V) };
        (K, V, A) -> A#{ binary_to_existing_atom(K) => V }
    end, #{}, E) end, M).

%% Only the host of each RPC: the path or query may carry its key.
rpc_hosts(Cfg) ->
    Host = fun(Url) ->
        case catch uri_string:parse(Url) of
            #{ host := H } -> H;
            _ -> null
        end
    end,
    Rpc = fun(Chain, Key, Default) ->
        case maps:get(Chain, Cfg, undefined) of
            C when is_map(C) -> [{Chain, Host(maps:get(Key, C, Default))}];
            _ -> []
        end
    end,
    maps:from_list(Rpc(<<"base">>, <<"rpc">>, null) ++ Rpc(<<"solana">>, <<"rpc">>, null)
        ++ Rpc(<<"sui">>, <<"graphql">>, <<"https://graphql.mainnet.sui.io/graphql">>)
        ++ [{<<"scheduler-url">>, Host(N)} || N <- [maps:get(<<"scheduler-url">>, Cfg, undefined)],
            N =/= undefined]).

status_view(St) ->
    #{
        <<"ao">> => maps:get(me, St),
        <<"pusd">> => maps:get(<<"pusd">>, maps:get(cfg, St)),
        <<"member">> => maps:get(member, St),
        <<"cursors">> => maps:get(cursors, St),
        <<"waiting-deposits">> => maps:size(maps:get(deposits, St)),
        <<"waiting-acks">> => maps:size(maps:get(releases, St)),
        <<"last-pass">> => maps:get(last_pass, St),
        <<"last-error">> => maps:get(last_error, St),
        <<"root-publication-running">> => maps:is_key(root_publication, St),
        <<"root-publication-error">> => maps:get(root_publication_error, St, null),
        <<"replica">> => maps:get(replica, St, null),
        <<"skipped">> => [#{ <<"item">> => K, <<"reason">> => V }
            || {K, V} <- lists:sort(maps:to_list(maps:get(skipped, St, #{})))],
        <<"rpcs">> => rpc_hosts(maps:get(cfg, St))
    }.

%%% Tests ---------------------------------------------------------------------

rpc_files_test() ->
    File = <<"/tmp/bridge-rpc-files-test.txt">>,
    ok = file:write_file(File, <<"https://rpc.example/v2/KEY
">>),
    Cfg = rpc_files(#{ <<"pusd">> => <<"p">>,
        <<"base">> => #{ <<"rpc-file">> => File, <<"chain-id">> => 8453 },
        <<"sui">> => #{ <<"package">> => <<"x">> } }),
    ?assertEqual(<<"https://rpc.example/v2/KEY">>, maps:get(<<"rpc">>, maps:get(<<"base">>, Cfg))),
    ?assertEqual(#{ <<"package">> => <<"x">> }, maps:get(<<"sui">>, Cfg)),
    ?assertEqual(<<"p">>, maps:get(<<"pusd">>, Cfg)).

next_interval_test() ->
    Cfg = #{ <<"interval">> => 15000, <<"fast-interval">> => 1000 },
    ?assertEqual(1000, next_interval(Cfg, 2000, 1999)),
    ?assertEqual(15000, next_interval(Cfg, 2000, 2000)),
    ?assertEqual(5000, next_interval(#{}, 0, 1)).

resend_interval_test() ->
    ?assertEqual(?RESEND_MS, resend_ms(#{})),
    ?assertEqual(?ARWEAVE_RESEND_MS,
        resend_ms(#{ <<"scheduler-device">> => <<"arweave-scheduler@1.0">> })),
    ?assertEqual(900000, resend_ms(#{
        <<"scheduler-device">> => <<"arweave-scheduler@1.0">>,
        <<"arweave-resend-ms">> => 900000
    })).

nudge_arm_test() ->
    Cfg = #{ <<"interval">> => 15000, <<"fast-window">> => 100000 },
    St0 = #{ nudge_at => 0, fast_since => 0, fast_until => 0 },
    {ok, St1} = nudge_arm(1000000, Cfg, St0),
    ?assertMatch(#{ fast_since := 1000000, fast_until := 1100000 }, St1),
    % Too soon after the last one.
    ?assertEqual(ignore, nudge_arm(1005000, Cfg, St1)),
    {ok, St2} = nudge_arm(1010000, Cfg, St1),
    ?assertMatch(#{ fast_since := 1000000, fast_until := 1110000 }, St2),
    % Spam stretches fast mode to three windows and no further.
    StN = lists:foldl(fun(T, S) -> case nudge_arm(T, Cfg, S) of {ok, S2} -> S2; ignore -> S end end,
        St2, lists:seq(1020000, 1290000, 10000)),
    ?assertMatch(#{ fast_until := 1300000 }, StN),
    ?assertEqual(ignore, nudge_arm(1299000, Cfg, StN#{ nudge_at := 0 })),
    % After a capped stretch, COOL_MS before the next, however slow `interval' is.
    ?assertEqual(ignore, nudge_arm(1310000, Cfg, StN)),
    ?assertMatch({ok, #{ fast_since := 1316000, fast_until := 1416000 }}, nudge_arm(1316000, Cfg, StN)),
    Slow = Cfg#{ <<"interval">> => 120000 },
    ?assertMatch({ok, #{ fast_since := 1316000 }}, nudge_arm(1316000, Slow, StN)).

state_file_test() ->
    Dir = <<"/tmp/bridge-state-test">>,
    _ = file:make_dir(Dir),
    File = <<Dir/binary, "/state.json">>,
    Cfg = #{ <<"state-file">> => File },
    % The old shape: cursors alone.
    ok = file:write_file(File, hb_json:encode(#{ <<"base">> => 5 })),
    ?assertEqual({#{ <<"base">> => 5 }, #{}, #{}}, load_state(Cfg)),
    D = #{ <<"base:3">> => #{ kind => deposit, chain => <<"base">>, nonce => 3, token => <<"0xt">>,
        recipient => <<"r">>, amount => <<"10">>, tx => <<"0xa">> } },
    R = #{ <<"7">> => #{ kind => released, chain => <<"sui">>, id => 7, tx => <<"d">> } },
    save_state(Cfg, #{ cursors => #{ <<"base">> => 6 }, deposits => D, releases => R }),
    ?assertEqual({#{ <<"base">> => 6 }, D, R}, load_state(Cfg)).

key_file_mode_test() ->
    Dir = <<"/tmp/bridge-keys-test">>,
    _ = file:make_dir(Dir),
    File = <<Dir/binary, "/keys.json">>,
    _ = file:delete(File),
    K = load_keys(File),
    {ok, Info} = file:read_file_info(File),
    ?assertEqual(8#600, element(8, Info) band 8#777),
    ?assertEqual(K, load_keys(File)).

rpc_hosts_test() ->
    Cfg = #{ <<"scheduler-url">> => <<"https://hyperbeam.tylerw.ai">>,
        <<"base">> => #{ <<"rpc">> => <<"https://base-mainnet.g.alchemy.com/v2/SECRET">> },
        <<"solana">> => #{ <<"rpc">> => <<"https://solana-mainnet.g.alchemy.com/v2/SECRET?x=1">> },
        <<"sui">> => #{ <<"package">> => <<"0x1">> } },
    ?assertEqual(#{ <<"base">> => <<"base-mainnet.g.alchemy.com">>,
        <<"solana">> => <<"solana-mainnet.g.alchemy.com">>, <<"sui">> => <<"graphql.mainnet.sui.io">>,
        <<"scheduler-url">> => <<"hyperbeam.tylerw.ai">> }, rpc_hosts(Cfg)).

proposal_epochs_test() ->
    ?assertEqual({4, 5}, proposal_epochs(#{ <<"priorEpoch">> => 4,
        <<"currentEpoch">> => 4, <<"newEpoch">> => 5 })),
    ?assertError({bad_proposal_epochs, 3, 4, 5}, proposal_epochs(#{
        <<"priorEpoch">> => 3, <<"currentEpoch">> => 4, <<"newEpoch">> => 5 })),
    ?assertError({bad_proposal_epochs, 4, 4, 6}, proposal_epochs(#{
        <<"priorEpoch">> => 4, <<"currentEpoch">> => 4, <<"newEpoch">> => 6 })),
    ?assertEqual(4, migration_epoch(#{ <<"priorEpoch">> => 4,
        <<"currentEpoch">> => 4, <<"newEpoch">> => 4 })),
    ?assertError({bad_migration_epochs, 4, 4, 5}, migration_epoch(#{
        <<"priorEpoch">> => 4, <<"currentEpoch">> => 4, <<"newEpoch">> => 5 })).

successor_ok_test() ->
    Here = <<"here">>,
    Set = [#{ <<"ao">> => <<"a">>, <<"base">> => <<"0x1">>, <<"solana">> => <<"S">>, <<"sui">> => <<"0x2">>,
        <<"operator">> => <<"o">> }],
    Info = #{ <<"attestors">> => Set, <<"epoch">> => 3, <<"successor">> => <<"next">> },
    Next = #{ <<"attestors">> => Set, <<"epoch">> => 3, <<"predecessor">> => Here },
    ?assertEqual(true, successor_ok(Info, Next, Here)),
    ?assertMatch({false, _}, successor_ok(Info, #{}, Here)),
    ?assertMatch({false, _}, successor_ok(Info, Next#{ <<"predecessor">> => <<"other">> }, Here)),
    ?assertMatch({false, _}, successor_ok(Info, maps:remove(<<"predecessor">>, Next), Here)),
    ?assertMatch({false, _}, successor_ok(Info, Next#{ <<"epoch">> => 4 }, Here)),
    Other = [(hd(Set))#{ <<"sui">> => <<"0x3">> }],
    ?assertMatch({false, _}, successor_ok(Info, Next#{ <<"attestors">> => Other }, Here)),
    % An operator is not part of what the chains check.
    Moved = [(hd(Set))#{ <<"operator">> => <<"p">> }],
    ?assertEqual(true, successor_ok(Info, Next#{ <<"attestors">> => Moved }, Here)).

follow_file_test() ->
    Dir = <<"/tmp/bridge-follow-test">>,
    _ = file:make_dir(Dir),
    File = <<Dir/binary, "/follow.json">>,
    _ = file:delete(File),
    Cfg = #{ <<"pusd">> => <<"g">>, <<"follow-file">> => File },
    ?assertEqual(<<"g">>, maps:get(<<"pusd">>, load_follow(Cfg))),
    save_follow((load_follow(Cfg))#{ <<"pusd">> => <<"n">> }),
    ?assertEqual(<<"n">>, maps:get(<<"pusd">>, load_follow(Cfg))),
    % A different configured PUSD restarts the walk from there.
    ?assertEqual(<<"h">>, maps:get(<<"pusd">>, load_follow(Cfg#{ <<"pusd">> => <<"h">> }))).

json_value_test() ->
    Info = #{ <<"epoch">> => 2 },
    Text = hb_json:encode(Info),
    ?assertEqual(Info, json_value(Text)),
    ?assertEqual(Info, json_value(Info)),
    ?assertEqual(#{}, json_value(<<"not json">>)),
    ?assertEqual(#{}, json_value(not_found)).

minted_test() ->
    Info = #{ <<"processed">> => #{ <<"base">> => #{ <<"floor">> => 5, <<"above">> => [7, 9] } } },
    ?assert(minted(Info, <<"base">>, 5)),
    ?assert(minted(Info, <<"base">>, 7)),
    ?assertNot(minted(Info, <<"base">>, 6)),
    ?assertNot(minted(Info, <<"sui">>, 1)).

%% Config keys arrive lowercased; the root's ids are not. The ledger is still
%% found, under the root's spelling, and an unregistered one is not.
fast_ledgers_match_lowercased_config_test() ->
    L1 = <<"icWTtN3_yOktTnAQYRAbzU4cuqygSWlJFslxYWJyUlI">>,
    L2 = <<"6GKgqZqsE5V6_CsvCSk4VAcRbqr9bxmiPRsekXaHWwQ">>,
    Spec = #{ <<"node">> => <<"https://hyperbeam.tylerw.ai">> },
    Configured = #{ hb_util:to_lower(L1) => Spec,
        <<"unregistered0000000000000000000000000000000">> => Spec },
    Registered = #{ L1 => #{}, L2 => #{} },
    ?assertEqual(#{ L1 => Spec }, fast_ledgers(Registered, Configured)),
    ?assertEqual(#{ L1 => Spec }, fast_ledgers(Registered, #{ L1 => Spec })).

fast_ledger_credit_plans_test() ->
    Root = <<"root">>, Ledger = <<"ledger">>,
    Deposit = #{ <<"id">> => <<"base:900">>, <<"creditId">> => <<"2">>,
        <<"ledger">> => Ledger, <<"account">> => <<"alice">>,
        <<"quantity">> => <<"10">>, <<"status">> => <<"ready">> },
    ?assertNot(credit_processed(#{ <<"credit">> => #{ <<"floor">> => 1 } }, <<"2">>)),
    [{lane, _, Credit}] = credit_plans(Root, Ledger,
        #{ <<"credit">> => #{ <<"floor">> => 1, <<"above">> => [] } }, [Deposit]),
    ?assertEqual(<<"2">>, maps:get(<<"creditid">>, Credit)),
    ?assertEqual(<<"base:900">>, maps:get(<<"depositid">>, Credit)),
    [{root, _, Ack}] = credit_plans(Root, Ledger,
        #{ <<"credit">> => #{ <<"floor">> => 2, <<"above">> => [] } }, [Deposit]),
    ?assertEqual(<<"Ledger-Credited">>, maps:get(<<"action">>, Ack)),
    % Canonical holder entry uses the same durable root-credit queue as a
    % bridge deposit; the id stays intact for the lane's replay witness.
    Entry = Deposit#{ <<"id">> => <<"entry:alice:1">>,
        <<"kind">> => <<"entry">>, <<"creditId">> => <<"3">> },
    [{lane, _, EntryCredit}] = credit_plans(Root, Ledger,
        #{ <<"credit">> => #{ <<"floor">> => 2, <<"above">> => [] } }, [Entry]),
    ?assertEqual(<<"entry:alice:1">>, maps:get(<<"depositid">>, EntryCredit)),
    ?assertEqual(<<"3">>, maps:get(<<"creditid">>, EntryCredit)),
    ?assertEqual([], credit_plans(Root, Ledger, #{},
        [Deposit#{ <<"status">> => <<"credited">> }])).

fast_ledger_exit_plans_test() ->
    Ledger = <<"ledger">>,
    Exit = #{ <<"id">> => <<"7">>, <<"status">> => <<"pending">>,
        <<"account">> => <<"alice">>, <<"chain">> => <<"base">>,
        <<"token">> => <<"0xt">>, <<"to">> => <<"0xrecipient">>,
        <<"quantity">> => <<"10">> },
    [{root, _, RootVote}] = exit_plans(Ledger, [Exit], []),
    ?assertEqual(<<"Ledger-Exit">>, maps:get(<<"action">>, RootVote)),
    ?assertEqual(<<"7">>, maps:get(<<"exit-id">>, RootVote)),
    ?assertNot(maps:is_key(<<"releasesig">>, RootVote)),
    Receipt = #{ <<"key">> => <<"ledger|7">>, <<"status">> => <<"accepted">>,
        <<"withdrawalId">> => <<"12">> },
    [{lane, _, Accepted}] = exit_plans(Ledger, [Exit], [Receipt]),
    ?assertEqual(<<"12">>, maps:get(<<"withdrawalid">>, Accepted)),
    Rejected = Receipt#{ <<"status">> => <<"rejected">>, <<"error">> => <<"no reserve">> },
    [{lane, _, Rejection}] = exit_plans(Ledger, [Exit], [Rejected]),
    ?assertEqual(<<"no reserve">>, maps:get(<<"error">>, Rejection)),
    ?assertEqual([], exit_plans(Ledger, [Exit#{ <<"status">> => <<"accepted">> }], [])).

fast_ledger_move_plans_test() ->
    Root = <<"root">>, Source = <<"source">>, Destination = <<"destination">>,
    Allowed = #{ Source => #{}, Destination => #{} },
    Move = #{ <<"id">> => <<"7">>, <<"source">> => Source,
        <<"destination">> => Destination, <<"account">> => <<"alice">>,
        <<"quantity">> => <<"10">>, <<"status">> => <<"pending">> },
    [{root, RootKey, RootVote}] = move_root_plans(4, Source, [Move], [], Allowed),
    ?assertEqual(<<"ledger-move-root:source|7">>, RootKey),
    ?assertEqual(<<"Ledger-Move-Root">>, maps:get(<<"action">>, RootVote)),
    ?assertEqual(<<"7">>, maps:get(<<"move-id">>, RootVote)),
    ?assertNot(maps:is_key(<<"attestorepoch">>, RootVote), "the certificate carries the epoch"),
    ?assertEqual([], move_root_plans(4, Source, [Move], [],
        maps:remove(Destination, Allowed))),
    Receipt = #{ <<"key">> => <<"source|7">>, <<"root">> => Root,
        <<"source">> => Source, <<"destination">> => Destination,
        <<"moveId">> => <<"7">>, <<"creditId">> => <<"9">>,
        <<"account">> => <<"alice">>, <<"quantity">> => <<"10">>,
        <<"epoch">> => 4, <<"status">> => <<"accepted">> },
    ?assertEqual([], move_root_plans(4, Source, [Move], [Receipt], Allowed)),
    [{{lane, Destination}, CreditKey, Credit},
     {{lane, Source}, ResultKey, Result}] =
        move_receipt_plans(Root, [Receipt], Allowed),
    ?assertEqual(<<"ledger-move-credit:destination:9">>, CreditKey),
    ?assertEqual(<<"Ledger-Move-Credit">>, maps:get(<<"action">>, Credit)),
    ?assertEqual(<<"4">>, maps:get(<<"attestorepoch">>, Credit)),
    ?assertEqual(<<"ledger-move-result:source:7:accepted">>, ResultKey),
    ?assertEqual(<<"Ledger-Move-Result">>, maps:get(<<"action">>, Result)),
    Rejected = maps:without([<<"creditId">>], Receipt#{
        <<"status">> => <<"rejected">>, <<"error">> => <<"cap">> }),
    [{{lane, Source}, _, Rejection}] = move_receipt_plans(Root, [Rejected], Allowed),
    ?assertEqual(<<"cap">>, maps:get(<<"error">>, Rejection)),
    ?assertEqual([], move_receipt_plans(Root, [Receipt], maps:remove(Source, Allowed))),
    ?assertEqual([], move_receipt_plans(<<"other-root">>, [Receipt], Allowed)).

%% @doc The root's channel steps this node would certify, from both lanes' own
%% records, with the JS relay's fields: an open, a settle the sender's record
%% supports, a close once the receiver is frozen at the sender's final point.
%% A step the records do not support, or a decided one, is no plan.
channel_plans_test() ->
    Root = <<"root">>, A = <<"lane-a">>, B = <<"lane-b">>,
    Allowed = #{A => #{}, B => #{}},
    Lane = fun(L, Out, In, Requests) -> #{<<"ledger">> => L, <<"root">> => Root, <<"out">> => Out,
        <<"in">> => In, <<"requests">> => Requests} end,
    Open = #{<<"id">> => 3, <<"op">> => <<"open">>, <<"peer">> => B, <<"generation">> => 1,
        <<"quantity">> => <<"50">>, <<"status">> => <<"pending">>},
    [{root, <<"channel-open:lane-a|open|3">>, OpenTags}] =
        channel_plans(Root, #{}, #{A => Lane(A, #{}, #{}, [Open])}, Allowed),
    ?assertEqual(#{<<"action">> => <<"Channel-Open">>, <<"root">> => Root, <<"source">> => A,
        <<"destination">> => B, <<"generation">> => <<"1">>, <<"requestid">> => <<"3">>,
        <<"quantity">> => <<"50">>}, OpenTags),
    Decided = #{<<"receipts">> => [#{<<"key">> => <<"lane-a|open|3">>}]},
    ?assertEqual([], channel_plans(Root, Decided, #{A => Lane(A, #{}, #{}, [Open])}, Allowed)),
    ?assertEqual([], channel_plans(<<"other">>, #{}, #{A => Lane(A, #{}, #{}, [Open])}, Allowed)),
    RootCh = #{<<"channels">> => #{<<"lane-a|lane-b">> => #{<<"generation">> => 1,
        <<"deposit">> => <<"100">>, <<"status">> => <<"open">>}}},
    Settle = #{<<"id">> => 4, <<"op">> => <<"settle">>, <<"peer">> => A, <<"generation">> => 1,
        <<"seq">> => 5, <<"cumulative">> => <<"50">>, <<"status">> => <<"pending">>},
    Sender = fun(Seq, Sent) -> Lane(A, #{B => #{<<"generation">> => 1, <<"seq">> => Seq,
        <<"sent">> => Sent, <<"status">> => <<"open">>}}, #{}, []) end,
    Receiver = Lane(B, #{}, #{A => #{<<"generation">> => 1, <<"seq">> => 5, <<"received">> => <<"50">>}}, [Settle]),
    [{root, _, SettleTags}] = channel_plans(Root, RootCh, #{A => Sender(5, <<"50">>), B => Receiver}, Allowed),
    ?assertEqual({<<"Channel-Settle">>, A, B, <<"5">>, <<"50">>}, {maps:get(<<"action">>, SettleTags),
        maps:get(<<"source">>, SettleTags), maps:get(<<"destination">>, SettleTags),
        maps:get(<<"seq">>, SettleTags), maps:get(<<"cumulative">>, SettleTags)}),
    ?assertEqual([], channel_plans(Root, RootCh, #{A => Sender(4, <<"40">>), B => Receiver}, Allowed),
        "the receiver claims more than the sender sent"),
    ?assertEqual([], channel_plans(Root, RootCh, #{A => Sender(5, <<"49">>), B => Receiver}, Allowed)),
    ?assertEqual([], channel_plans(Root, RootCh, #{B => Receiver}, Allowed), "sender unreadable"),
    Close = #{<<"id">> => 5, <<"op">> => <<"close">>, <<"peer">> => B, <<"generation">> => 1,
        <<"status">> => <<"pending">>},
    Closing = Lane(A, #{B => #{<<"generation">> => 1, <<"seq">> => 7, <<"sent">> => <<"70">>,
        <<"status">> => <<"closing">>, <<"finalSeq">> => 7, <<"finalSent">> => <<"70">>}}, #{}, [Close]),
    Frozen = fun(Seq, Got) -> Lane(B, #{}, #{A => #{<<"generation">> => 1, <<"seq">> => Seq,
        <<"received">> => Got, <<"status">> => <<"frozen">>, <<"finalSeq">> => 7,
        <<"finalReceived">> => <<"70">>}}, []) end,
    [{root, _, CloseTags}] = channel_plans(Root, RootCh, #{A => Closing, B => Frozen(7, <<"70">>)}, Allowed),
    ?assertEqual({<<"Channel-Close">>, <<"7">>, <<"70">>}, {maps:get(<<"action">>, CloseTags),
        maps:get(<<"finalseq">>, CloseTags), maps:get(<<"finalcumulative">>, CloseTags)}),
    ?assertEqual([], channel_plans(Root, RootCh, #{A => Closing, B => Frozen(6, <<"60">>)}, Allowed),
        "credits still in flight").

%% @doc The plan for a root sequence is keyed by that sequence alone, so a
%% retry reuses its journal whatever arrived since. Only an advanced verified
%% root sequence creates a new publication journal; move IDs keep a batch that
%% somehow lands twice from settling twice.
root_release_retry_sequence_test() ->
    Root = <<"root">>, Ledger = <<"lane">>, Me = <<"authority">>,
    Registry = #{<<"scheduler">> => Me},
    Move = #{<<"id">> => <<"8">>, <<"root">> => Root, <<"source">> => Ledger,
        <<"destination">> => Root, <<"status">> => <<"pending">>,
        <<"account">> => <<"holder">>, <<"quantity">> => <<"50">>},
    Rows = #{Ledger => #{<<"sequence">> => <<"4">>, <<"epoch">> => 7,
        <<"status">> => <<"executed">>}},
    Plan = root_release_plans(Root, Ledger, Registry, [Move], [], 7, 3, Rows, Me),
    [{root, Key, Tags}] = Plan,
    ?assertEqual(Plan, root_release_plans(Root, Ledger, Registry, [Move], [], 7, 3, Rows, Me)),
    ?assertEqual(<<"5">>, maps:get(<<"rootsequence">>, Tags)),
    ?assertEqual(<<"8:holder:50">>, maps:get(<<"moves">>, Tags)),
    Later = Move#{<<"id">> => <<"9">>},
    [{root, Key, Grown}] = root_release_plans(Root, Ledger, Registry, [Later, Move], [], 7, 3, Rows, Me),
    ?assertEqual(<<"8:holder:50,9:holder:50">>, maps:get(<<"moves">>, Grown)),
    Advanced = Rows#{Ledger := (maps:get(Ledger, Rows))#{<<"sequence">> => <<"5">>}},
    [{root, NewKey, NewTags}] = root_release_plans(Root, Ledger, Registry, [Move], [], 7, 3, Advanced, Me),
    ?assertNotEqual(Key, NewKey),
    ?assertEqual(Tags#{<<"rootsequence">> := <<"6">>}, NewTags),
    ?assertEqual([], root_release_plans(Root, Ledger, Registry, [Move], [], 7, 3, Rows, <<"stranger">>)),
    lists:foreach(fun(Status) ->
        Receipt = #{<<"key">> => <<"lane|8">>, <<"status">> => Status},
        ?assertEqual([], root_release_plans(Root, Ledger, Registry, [Move], [Receipt], 7, 3, Rows, Me))
    end, [<<"accepted">>, <<"rejected">>]).

%% @doc A batch is the longest run of pending exits, lowest id first, whose
%% carrier fits Arweave's 2,048 tag bytes for the current committee.
root_release_batch_fits_carrier_test() ->
    Id = fun() -> hb_util:human_id(crypto:strong_rand_bytes(32)) end,
    Root = Id(), Ledger = Id(), Me = Id(),
    Moves = [#{<<"id">> => integer_to_binary(N), <<"root">> => Root, <<"source">> => Ledger,
        <<"destination">> => Root, <<"status">> => <<"pending">>,
        <<"account">> => Id(), <<"quantity">> => <<"123456789">>} || N <- lists:seq(1001, 1040)],
    Rows = #{Ledger => #{<<"sequence">> => <<"99999">>}},
    Batch = fun(Committee) ->
        [{root, _, Tags}] = root_release_plans(Root, Ledger, #{<<"scheduler">> => Me},
            lists:reverse(Moves), [], 2, Committee, Rows, Me),
        Entries = binary:split(maps:get(<<"moves">>, Tags), <<",">>, [global]),
        Operation = #{<<"action">> => <<"Ledger-Release">>, <<"ledger">> => Ledger,
            <<"moves">> => maps:get(<<"moves">>, Tags), <<"attestor-epoch">> => <<"2">>,
            <<"root-sequence">> => <<"100000">>},
        Carrier = 1234 + 66 * (Committee div 2 + 1) + byte_size(hb_json:encode(Operation)),
        ?assert(Carrier =< 2048),
        ?assert(Carrier + 1 + byte_size(hd(Entries)) > 2048),
        ?assertEqual(<<"1001">>, hd(binary:split(hd(Entries), <<":">>))),
        length(Entries)
    end,
    ?assertEqual(9, Batch(3)),
    ?assertEqual(4, Batch(11)).

relay_failure_isolation_test() ->
    St = #{ skipped => #{} },
    St2 = guard(<<"ledger:item">>, St, fun() -> error(broken_item) end),
    ?assert(maps:is_key(<<"ledger:item">>, maps:get(skipped, St2))).

admin_vote_tags_test() ->
    Root = <<"qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo">>,
    Bridge = <<"0x1111111111111111111111111111111111111111">>,
    SuiBridge = <<"0x034401905bebdf8c04f3cd5f04f442a39372c8dc321c29edfb4f9cb30b23ab96">>,
    Solana = dev_bridge_attestor_sign:base58_encode(binary:copy(<<1>>, 32)),
    SolanaToken = dev_bridge_attestor_sign:base58_encode(binary:copy(<<2>>, 32)),
    Cfg = #{ <<"pusd">> => Root,
        <<"base">> => #{ <<"chain-id">> => 8453, <<"contract">> => Bridge },
        <<"solana">> => #{ <<"program">> => Solana },
        <<"sui">> => #{ <<"bridge">> => SuiBridge } },
    Keys = #{ base => binary:copy(<<1>>, 32),
        solana => binary:copy(<<2>>, 32), sui => binary:copy(<<3>>, 32) },
    Proposal = #{ <<"id">> => 8, <<"kind">> => <<"admin">>,
        <<"op">> => <<"add-token">>, <<"priorEpoch">> => 4,
        <<"currentEpoch">> => 4, <<"newEpoch">> => 4, <<"epoch">> => 4,
        <<"admin">> => #{ <<"chain">> => <<"solana">>,
            <<"token">> => SolanaToken } },
    Add = admin_vote_tags(Proposal, Cfg, Keys),
    ?assertEqual(<<"4">>, maps:get(<<"currentepoch">>, Add)),
    ?assertEqual(<<"4">>, maps:get(<<"newepoch">>, Add)),
    ?assert(maps:is_key(<<"solanasig">>, Add)),
    ?assertNot(maps:is_key(<<"basesig">>, Add)),
    ?assertNot(maps:is_key(<<"suisig">>, Add)),
    Unpause = admin_vote_tags(Proposal#{ <<"op">> => <<"unpause">>,
        <<"admin">> => #{} }, Cfg, Keys),
    ?assert(maps:is_key(<<"basesig">>, Unpause)),
    ?assert(maps:is_key(<<"solanasig">>, Unpause)),
    ?assert(maps:is_key(<<"suisig">>, Unpause)),
    ?assertError({bad_admin_epochs, 4, 4, 5, 4},
        admin_epoch(Proposal#{ <<"newEpoch">> => 5 })).

l1_unsigned_test() ->
    Process = hb_util:human_id(crypto:strong_rand_bytes(32)),
    TX = l1_unsigned(Process, #{ <<"action">> => <<"Bridge-Sign">>,
        <<"withdrawalid">> => <<"7">> }),
    ?assertEqual(2, TX#tx.format),
    ?assertEqual(0, TX#tx.data_size),
    ?assertEqual(<<>>, TX#tx.data),
    ?assertEqual(<<>>, TX#tx.target),
    ?assertEqual({<<"Assign-To">>, Process},
        lists:keyfind(<<"Assign-To">>, 1, TX#tx.tags)),
    ?assert(lists:member({<<"action">>, <<"Bridge-Sign">>}, TX#tx.tags)).

local_withdrawal_test() ->
    St = #{ namespace => 300 },
    ?assertEqual(<<"7">>, local_withdrawal((300 bsl 32) bor 7, St)),
    ?assertEqual(false, local_withdrawal((299 bsl 32) bor 7, St)).

%% The signature a node posts for a Base withdrawal is the one the contract
%% recovers: the same vector dev_bridge_attestor_sign checks against viem.
withdrawal_signature_base_test() ->
    W = #{ <<"id">> => <<"7">>, <<"chain">> => <<"base">>,
        <<"token">> => <<"0x2222222222222222222222222222222222222222">>,
        <<"to">> => <<"0x3333333333333333333333333333333333333333">>, <<"amount">> => <<"1234567">> },
    Cfg = #{ <<"pusd">> => <<"qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo">>,
        <<"base">> => #{ <<"chain-id">> => 8453,
        <<"contract">> => <<"0x1111111111111111111111111111111111111111">> } },
    ?assertEqual(
        <<"0x7763e686307224e8f0791bd0b59c7ae7bcb4efdb77d903891d2032fc8cb5f129",
          "253a9cc81621497731181da8760a91e117de44990e15dfc8681ce92222ded9861b">>,
        withdrawal_signature(W, 3, Cfg, #{ base => binary:copy(<<1>>, 32) }, 5)).

%%% The AO side on v0.11 ------------------------------------------------------

%% A stand-in PUSD on `lua-patch@1.0': it publishes `bridgeinfo' naming the
%% node as its only attestor, with one open administration vote its operator
%% approved, and records each `Attestor-Vote' as `<proposal>:<committer>;'.
-define(TEST_TOKEN, "0x2222222222222222222222222222222222222222").
-define(TEST_TO, <<"0x3333333333333333333333333333333333333333">>).

fake_pusd(Me, Opts) ->
    Info = hb_json:encode(#{
        <<"epoch">> => 2, <<"namespace">> => 0,
        <<"attestors">> => [#{ <<"ao">> => Me }],
        <<"proposals">> => [#{ <<"id">> => 1, <<"status">> => <<"open">>,
            <<"op">> => <<"pause">>, <<"kind">> => <<"admin">>,
            <<"priorEpoch">> => 2, <<"currentEpoch">> => 2,
            <<"newEpoch">> => 2, <<"epoch">> => 2,
            <<"approvals">> => #{ Me => true },
            <<"votes">> => #{} }]
    }),
    Script = <<
        "Votes = Votes or ''\n"
        "Signs = Signs or ''\n"
        "W = W or {}\n"
        "local function wjson()\n"
        "  local out = {}\n"
        "  for _, w in ipairs(W) do\n"
        "    local sigs = {}\n"
        "    for k, v in pairs(w.sigs) do sigs[#sigs + 1] = '\"' .. k .. '\":\"' .. v .. '\"' end\n"
        "    out[#out + 1] = '{\"id\":\"' .. w.id .. '\",\"chain\":\"base\",\"status\":\"pending\",'\n"
        "      .. '\"token\":\"", ?TEST_TOKEN, "\",\"to\":\"' .. w.to\n"
        "      .. '\",\"amount\":\"' .. w.amount .. '\",\"sigs\":{' .. table.concat(sigs, ',') .. '}}'\n"
        "  end\n"
        "  return '[' .. table.concat(out, ',') .. ']'\n"
        "end\n"
        "function compute(req)\n"
        "  local body = req.body or req\n"
        "  local action = tostring(body.action or '')\n"
        "  local who = ''\n"
        "  for _, c in pairs(body.commitments or {}) do who = tostring(c.committer) end\n"
        "  if action == 'Attestor-Vote' then\n"
        "    Votes = Votes .. tostring(body.proposalid) .. ':' .. who .. ';'\n"
        "  elseif action == 'Redeem' then\n"
        "    W[#W + 1] = { id = tostring(#W + 1), to = tostring(body.to),\n"
        "      amount = tostring(body.amount), sigs = {} }\n"
        "  elseif action == 'Bridge-Sign' then\n"
        "    Signs = Signs .. tostring(body.withdrawalid) .. '=' .. tostring(body.releasesig) .. ';'\n"
        "    for _, w in ipairs(W) do\n"
        "      if w.id == tostring(body.withdrawalid) then w.sigs[who] = tostring(body.releasesig) end\n"
        "    end\n"
        "  end\n"
        "  return { patches = {\n"
        "    { path = '/bridgeinfo', value = [==[", Info/binary, "]==] },\n"
        "    { path = '/withdrawals', value = wjson() },\n"
        "    { path = '/signs', value = Signs },\n"
        "    { path = '/votes', value = Votes } }, results = {} }\n"
        "end\n"
    >>,
    Process = hb_message:commit(#{
        <<"device">> => <<"process@1.0">>,
        <<"type">> => <<"Process">>,
        <<"scheduler-device">> => <<"scheduler@1.0">>,
        <<"execution-device">> => <<"lua-patch@1.0">>,
        <<"module">> => #{ <<"content-type">> => <<"application/lua">>, <<"body">> => Script },
        <<"authority">> => [Me],
        <<"scheduler-location">> => Me
    }, Opts),
    {ok, _} = hb_cache:write(Process, Opts),
    hb_util:human_id(hb_message:id(Process, all, Opts)).

ao_test_opts() ->
    #{
        <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
        <<"priv-wallet">> => ar_wallet:new(),
        <<"port">> => 10000 + rand:uniform(20000),
        <<"hosts-block-internal">> => false
    }.

ao_test_cfg(Pusd, Dir) ->
    _ = file:make_dir(Dir),
    #{ <<"pusd">> => Pusd, <<"keys-file">> => <<Dir/binary, "/keys.json">>,
        <<"state-file">> => <<Dir/binary, "/state.json">>,
        <<"follow-file">> => <<Dir/binary, "/follow.json">> }.

%% One pass against a PUSD: it reads `bridgeinfo', finds itself in the set,
%% and plans ONE Attestor-Apply certificate for the proposal its operator
%% approved. It sends no per-member vote.
vote_reaches_pusd(Cfg0, Opts) ->
    St0 = initial_state(Cfg0, Opts),
    % A first message, so PUSD has computed and published something.
    {ok, _} = send(#{ <<"action">> => <<"Ping">> }, St0),
    {ok, View, _} = view(maps:get(<<"pusd">>, Cfg0), St0),
    ?assertMatch(#{ <<"epoch">> := 2 }, key(<<"bridgeinfo">>, View)),
    St1 = pass(St0),
    ?assertMatch(#{ member := true, last_error := null }, St1),
    ?assertMatch([{<<"apply:1">>, #{<<"action">> := <<"Attestor-Apply">>, <<"proposal-id">> := <<"1">>}}],
        [P || {<<"apply:", _/binary>>, _} = P <- maps:get(committee, St1)]),
    ?assertEqual(<<>>, raw(<<"votes">>, St1)).

%% A plain-string key of the real PUSD, read where it runs.
raw(Key, #{ cfg := Cfg, opts := Opts }) ->
    {ok, Proc} = hb_cache:read(maps:get(<<"pusd">>, Cfg), Opts),
    case hb_ao:resolve(Proc, <<"now/", Key/binary>>, Opts) of
        {ok, V} -> V;
        _ -> <<>>
    end.

%% Signed messages posted to PUSD's scheduler, PUSD read from this node's own
%% replica of it.
pusd_vote_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = ao_test_opts(),
        Node = hb_http_server:start_node(Opts),
        Me = hb_util:human_id(ar_wallet:to_address(hb_opts:get(<<"priv-wallet">>, none, Opts))),
        Pusd = fake_pusd(Me, Opts),
        Dir = <<"/tmp/bridge-ao-vote">>,
        _ = os:cmd("rm -rf " ++ binary_to_list(Dir)),
        Cfg = (ao_test_cfg(Pusd, Dir))#{ <<"scheduler-url">> => url(Node),
            <<"replica-dir">> => <<Dir/binary, "/replica">> },
        vote_reaches_pusd(Cfg, Opts)
    end}.

%% The node's `start' hook runs the loop, and its endpoints answer under the
%% new name.
start_hook_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Opts = ao_test_opts(),
        Dir = <<"/tmp/bridge-ao-start">>,
        Node = hb_http_server:start_node(Opts#{
            <<"on">> => #{ <<"start">> => #{ <<"device">> => <<"bridge-attestor@1.0">> } },
            <<"bridge">> => (ao_test_cfg(<<"no-such-pusd">>, Dir))#{
                <<"scheduler-url">> => <<"http://127.0.0.1:1">> }
        }),
        try
            {ok, Res} = hb_http:get(Node, <<"/~bridge-attestor@1.0/keys">>, Opts),
            Keys = hb_json:decode(hb_maps:get(<<"body">>, Res, Res, Opts)),
            Me = hb_util:human_id(ar_wallet:to_address(hb_opts:get(<<"priv-wallet">>, none, Opts))),
            ?assertMatch(#{ <<"ao">> := Me, <<"base">> := <<"0x", _:40/binary>>,
                <<"sui">> := <<"0x", _:64/binary>> }, Keys),
            {ok, _} = hb_http:get(Node, <<"/~bridge-attestor@1.0/status">>, Opts)
        after
            stop_loop()
        end
    end}.

stop_loop() ->
    case hb_name:lookup(?NAME) of
        Pid when is_pid(Pid) -> hb_name:unregister(?NAME), exit(Pid, kill);
        _ -> ok
    end.

%%% Hardening -----------------------------------------------------------------

%% `start' is reachable over HTTP, so it takes its configuration from the
%% node's own options, never from the request: a request cannot start a loop
%% or make a keys file at a path it chose.
start_ignores_request_config_test() ->
    stop_loop(),
    Dir = <<"/tmp/bridge-start-req">>,
    _ = file:make_dir(Dir),
    File = <<Dir/binary, "/keys.json">>,
    _ = file:delete(File),
    Body = #{ <<"bridge">> => #{ <<"pusd">> => <<"p">>, <<"keys-file">> => File } },
    {ok, #{ <<"body">> := Body }} =
        start(#{}, #{ <<"body">> => Body }, #{ <<"only">> => local }),
    timer:sleep(500),
    Running = hb_name:lookup(?NAME),
    stop_loop(),
    ?assertNot(is_pid(Running)),
    ?assertEqual({error, enoent}, file:read_file_info(File)).

%% One chain raising (here Base, with no `rpc') fails that chain alone: the
%% others still scan, and its cursor stays where it was.
scan_isolates_a_crashing_chain_test() ->
    Cursors = #{ <<"base">> => 7 },
    ?assertEqual({[], Cursors}, scan(#{ <<"base">> => #{} }, Cursors, #{})),
    ?assertEqual({[], Cursors},
        scan(#{ <<"base">> => #{ <<"rpc">> => 1 } }, Cursors, #{})).

%% A failed pass shows and logs no URL (RPC URLs carry API keys) and no
%% call arguments, only class, reason and where.
failure_hides_urls_and_args_test() ->
    Url = <<"https://base-mainnet.g.alchemy.com/v2/SECRET">>,
    T = [{maps, get, [<<"pusd">>, #{ <<"rpc">> => Url }], []},
        {?MODULE, pass, 1, [{file, "dev_bridge_attestor.erl"}, {line, 211}]}],
    {Shown, Logged} = failure(error, {badmatch, {error, {post_json, Url, x}}}, T),
    Text = iolist_to_binary([Shown, io_lib:format("~p", [Logged])]),
    ?assertEqual(nomatch, binary:match(Text, <<"SECRET">>)),
    ?assertNotEqual(nomatch, binary:match(Text, <<"base-mainnet.g.alchemy.com">>)),
    ?assertNotEqual(nomatch, binary:match(Text, <<"211">>)).

%% A withdrawal PUSD publishes that cannot be signed (a bad `to', a chain this
%% node does not run) is skipped and reported; the rest still sign.
malformed_withdrawal_is_skipped_test() ->
    W = fun(Id, Chain, To) -> #{ <<"id">> => Id, <<"chain">> => Chain,
        <<"status">> => <<"pending">>, <<"amount">> => <<"1">>, <<"to">> => To,
        <<"token">> => <<"0x2222222222222222222222222222222222222222">> } end,
    St = #{ me => <<"me">>, namespace => 0, sent => #{}, skipped => #{},
        keys => #{ base => binary:copy(<<1>>, 32) },
        cfg => #{ <<"pusd">> => <<"qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo">>, <<"base">> => #{ <<"chain-id">> => 8453,
            <<"contract">> => <<"0x1111111111111111111111111111111111111111">> } } },
    Good = W(<<"11">>, <<"base">>, <<"0x3333333333333333333333333333333333333333">>),
    St1 = chain_signatures(#{}, [W(<<"9">>, <<"base">>, <<"0xnot-an-address">>),
        W(<<"10">>, <<"eth">>, <<"0x3333333333333333333333333333333333333333">>), not_a_map, Good], 0, St),
    % Only the withdrawal this node can sign is served; the rest are skipped.
    ?assertEqual([<<"11">>], maps:keys(maps:get(withdrawal, maps:get(chain_sigs, St1)))),
    ?assertMatch({ok, #{<<"signer">> := <<"me">>, <<"withdrawal">> := <<"11">>, <<"sig">> := <<"0x", _/binary>>}},
        chain_sig_answer(withdrawal, <<"11">>, St1)),
    % Nothing is signed for a request: an id the pass did not sign is unknown,
    % whatever the requester claims about it.
    ?assertEqual({error, not_found}, chain_sig_answer(withdrawal, <<"9">>, St1)),
    ?assertEqual({error, not_found}, chain_sig_answer(withdrawal, <<"12">>, St1)),
    ?assertEqual({error, not_found}, chain_sig_answer(proposal, <<"11">>, St1)),
    ?assertEqual({error, not_found}, chain_sig_answer(withdrawal, <<"11">>, St)),
    % The endpoint answers from the pass's published row, with no loop
    % running: a request can neither queue behind a pass nor make one sign.
    undefined = hb_name:lookup(?NAME),
    ?assertMatch({ok, _}, chain_sig(#{}, #{ <<"withdrawal">> => <<"11">> }, #{})),
    ?assertMatch({error, #{ <<"status">> := 404 }}, chain_sig(#{}, #{ <<"withdrawal">> => <<"9">> }, #{})),
    ?assertMatch({error, #{ <<"status">> := 400 }},
        chain_sig(#{}, #{ <<"withdrawal">> => <<"11">>, <<"proposal">> => <<"1">> }, #{})).

%% A key file holds the BLS secret beside the chain secrets, owner-only, and
%% no endpoint answer carries a secret. A key file from before the BLS key
%% gains one, once; a partial BLS entry is refused rather than replaced.
bls_key_file_test() ->
    Dir = <<"/tmp/bls-keys-", (hb_util:human_id(crypto:strong_rand_bytes(32)))/binary>>,
    ok = file:make_dir(Dir),
    File = <<Dir/binary, "/keys.json">>,
    K = load_keys(File),
    {Secret, Public} = maps:get(bls, K),
    ?assertEqual(K, load_keys(File)),
    {ok, Info} = file:read_file_info(File),
    ?assertEqual(8#600, element(8, Info) band 8#777),
    Me = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Public0x = public_keys(#{ me => Me, keys => K }),
    ?assertEqual(<<"0x", (dev_bridge_attestor_sign:hex(Public))/binary>>, maps:get(<<"bls">>, Public0x)),
    Answer = hb_json:encode(Public0x),
    ?assertEqual(nomatch, binary:match(Answer, dev_bridge_attestor_sign:hex(Secret))),
    ?assertEqual(nomatch, binary:match(Answer, dev_bridge_attestor_sign:hex(maps:get(base, K)))),
    % The registration proof verifies at the root's possession check.
    ?assertMatch({ok, _}, hb_ao:resolve(#{<<"device">> => <<"bridge-root@1.0">>},
        #{<<"path">> => <<"possession">>, <<"ao">> => Me, <<"bls">> => maps:get(<<"bls">>, Public0x),
            <<"pop">> => maps:get(<<"blsPop">>, Public0x)}, #{})),
    Old = <<Dir/binary, "/old.json">>,
    ok = file:write_file(Old, hb_json:encode(#{<<"base">> => dev_bridge_attestor_sign:hex(maps:get(base, K)),
        <<"solana">> => dev_bridge_attestor_sign:hex(maps:get(solana, K)),
        <<"sui">> => dev_bridge_attestor_sign:hex(maps:get(sui, K))})),
    Upgraded = load_keys(Old),
    ?assertEqual(maps:get(base, K), maps:get(base, Upgraded)),
    ?assertEqual(Upgraded, load_keys(Old)),
    Partial = <<Dir/binary, "/partial.json">>,
    ok = file:write_file(Partial, hb_json:encode(#{<<"base">> => <<"01">>, <<"solana">> => <<"01">>,
        <<"sui">> => <<"01">>, <<"bls">> => dev_bridge_attestor_sign:hex(Secret)})),
    ?assertError(bls_key_file_is_partial, load_keys(Partial)).

%%% The replica: what is signed comes from this node's own PUSD ------------------

%% A wallet's message, scheduled on PUSD where it runs.
schedule_as(Wallet, Tags, Pusd, Opts) ->
    Signed = hb_message:commit(Tags#{ <<"target">> => Pusd },
        Opts#{ <<"priv-wallet">> => Wallet }, <<"httpsig@1.0">>),
    {ok, _} = hb_cache:write(Signed, Opts),
    {ok, Proc} = hb_cache:read(Pusd, Opts),
    {ok, _} = hb_ao:resolve({as, <<"process@1.0">>, Proc},
        #{ <<"path">> => <<"schedule">>, <<"method">> => <<"POST">>, <<"body">> => Signed }, Opts).

%% A scheduler node running PUSD, whose HTTP replies pass through `Hostile'
%% first (identity for an honest node), and an attestor that reads it as its
%% `scheduler-url'. PUSD holds one pending Base withdrawal: 5 to TEST_TO.
replica_setup(Name, MakeHostile) ->
    hb:init(),
    Opts = ao_test_opts(),
    Hostile = MakeHostile(hb_opts:get(<<"priv-wallet">>, none, Opts)),
    Me = hb_util:human_id(ar_wallet:to_address(hb_opts:get(<<"priv-wallet">>, none, Opts))),
    Pusd = fake_pusd(Me, Opts),
    Hook = fun(_, HookReq = #{ <<"request">> := Req, <<"body">> := Body }, _) ->
        Path = hb_util:bin(hb_maps:get(<<"path">>, Req, <<>>, #{})),
        {ok, HookReq#{ <<"body">> => Hostile(Path, Body) }}
    end,
    Node = hb_http_server:start_node(Opts#{
        <<"on">> => #{ <<"response">> => #{ <<"device">> => #{ <<"response">> => Hook } } } }),
    Dir = <<"/tmp/bridge-replica-", Name/binary>>,
    _ = os:cmd("rm -rf " ++ binary_to_list(Dir)),
    Cfg = (ao_test_cfg(Pusd, Dir))#{
        <<"scheduler-url">> => url(Node),
        <<"replica-dir">> => <<Dir/binary, "/replica">>,
        <<"base">> => #{ <<"rpc">> => <<"http://127.0.0.1:1">>, <<"chain-id">> => 8453,
            <<"contract">> => <<"0x1111111111111111111111111111111111111111">> } },
    St = initial_state(Cfg, Opts),
    {ok, _} = send(#{ <<"action">> => <<"Ping">> }, St),
    schedule_as(ar_wallet:new(), #{ <<"action">> => <<"Redeem">>, <<"to">> => ?TEST_TO,
        <<"amount">> => <<"5">> }, Pusd, Opts),
    St.

url(Node) -> hb_util:bin(string:trim(hb_util:list(Node), trailing, "/")).

honest(_Wallet) -> fun(_Path, Body) -> Body end.

%% The release signatures this node serves from chain-sig, as `<id>=<sig>;'.
signs(St) ->
    Map = maps:get(withdrawal, maps:get(chain_sigs, St, #{}), #{}),
    iolist_to_binary([[Id, "=", Sig, ";"] || {Id, Sig} <- lists:sort(maps:to_list(Map))]).

%% The signature this node owes PUSD's real withdrawal 1.
true_signature(St = #{ cfg := Cfg, keys := Keys }) ->
    withdrawal_signature(#{ <<"id">> => <<"1">>, <<"chain">> => <<"base">>,
        <<"token">> => <<?TEST_TOKEN>>, <<"to">> => ?TEST_TO, <<"amount">> => <<"5">> },
        2, Cfg, Keys, maps:get(namespace, St)).

replica_signs_its_own_withdrawal_test_() ->
    {timeout, 120, fun() ->
        St = replica_setup(<<"honest">>, fun honest/1),
        St1 = pass(St),
        ?assertMatch(#{ member := true, last_error := null }, St1),
        ?assertEqual(<<"1=", (true_signature(St1))/binary, ";">>, signs(St1))
    end}.

%% The node running PUSD publishes a withdrawal list of its own making:
%% withdrawal 1 redirected and enlarged, and a 7 that PUSD never made. The
%% attestor signs only what its own replica holds: the real 1.
hostile_withdrawal_list_is_not_signed_test_() ->
    {timeout, 120, fun() ->
        Fake = hb_json:encode([
            #{ <<"id">> => <<"1">>, <<"chain">> => <<"base">>, <<"status">> => <<"pending">>,
                <<"token">> => <<?TEST_TOKEN>>, <<"amount">> => <<"999">>, <<"sigs">> => #{},
                <<"to">> => <<"0x4444444444444444444444444444444444444444">> },
            #{ <<"id">> => <<"7">>, <<"chain">> => <<"base">>, <<"status">> => <<"pending">>,
                <<"token">> => <<?TEST_TOKEN>>, <<"amount">> => <<"999">>, <<"sigs">> => #{},
                <<"to">> => <<"0x4444444444444444444444444444444444444444">> }]),
        Lie = fun(Path, Body) ->
            case binary:match(Path, <<"withdrawals">>) of
                nomatch -> Body;
                _ -> Fake
            end
        end,
        St = replica_setup(<<"list">>, fun(_) -> Lie end),
        St1 = pass(St),
        ?assertEqual(<<"1=", (true_signature(St1))/binary, ";">>, signs(St1))
    end}.

%% Rewrite the Redeem's assignment in every schedule the node serves.
tamper_redeem(Fun) ->
    fun(Path, Body) ->
        case binary:match(Path, <<"schedule">>) of
            nomatch -> Body;
            _ ->
                Opts = #{ <<"hashpath">> => ignore },
                Loaded = hb_cache:ensure_all_loaded(Body, Opts),
                As = hb_maps:get(<<"assignments">>, Loaded, #{}, Opts),
                Loaded#{ <<"assignments">> => maps:map(
                    fun(_, A) when is_map(A) ->
                            case hb_maps:get(<<"action">>, hb_maps:get(<<"body">>, A, #{}, Opts), none, Opts) of
                                <<"Redeem">> -> Fun(A);
                                _ -> A
                            end;
                       (_, A) -> A
                    end, As) }
        end
    end.

%% A schedule whose assignment no longer carries the scheduler's signature:
%% the replica halts there and nothing is signed, not even the real 1.
bad_scheduler_signature_halts_replica_test_() ->
    {timeout, 120, fun() ->
        St = replica_setup(<<"badsig">>,
            fun(_) -> tamper_redeem(fun(A) -> A#{ <<"timestamp">> => 1 } end) end),
        St1 = pass(St),
        ?assertEqual(<<>>, signs(St1)),
        ?assertMatch(#{ stopped := <<"assignment not signed by the scheduler", _/binary>> },
            maps:get(replica, St1))
    end}.

%% A compromised scheduler re-signs an assignment whose message it altered:
%% the message's own signature fails, and nothing is signed.
forged_message_halts_replica_test_() ->
    {timeout, 120, fun() ->
        Forge = fun(Wallet) -> fun(A) ->
            Body = hb_maps:get(<<"body">>, A, #{}, #{}),
            Unsigned = maps:without([<<"commitments">>], A),
            hb_message:commit(Unsigned#{ <<"body">> => Body#{ <<"amount">> => <<"999">> } },
                #{ <<"priv-wallet">> => Wallet })
        end end,
        St = replica_setup(<<"forged">>, fun(W) -> tamper_redeem(Forge(W)) end),
        St1 = pass(St),
        ?assertEqual(<<>>, signs(St1)),
        ?assertMatch(#{ stopped := <<"message signature does not verify", _/binary>> },
            maps:get(replica, St1))
    end}.


%%% Only what was verified is used --------------------------------------------

%% Rewrite every served schedule: each assignment through `Fun(Slot, A)'
%% (a list of assignments to serve in its place), then all of them re-signed
%% by `Wallet' in slot order, the chain carried over the new signatures.
reschedule(Wallet, Fun) ->
    fun(Path, Body) ->
        case binary:match(Path, <<"schedule">>) of
            nomatch -> Body;
            _ ->
                Opts = #{ <<"hashpath">> => ignore },
                Loaded = hb_cache:ensure_all_loaded(Body, Opts),
                As = hb_maps:get(<<"assignments">>, Loaded, #{}, Opts),
                Sorted = [A || {N, A} <- lists:keysort(1,
                    [{catch hb_util:int(K), V} || {K, V} <- maps:to_list(As), is_map(V)]), is_integer(N)],
                case Sorted of [] -> Body; _ ->
                Served = lists:append([Fun(A) || A <- Sorted]),
                First = hb_util:int(hb_maps:get(<<"slot">>, hd(Sorted), 0, Opts)),
                Chain0 = hb_maps:get(<<"base-hashpath">>, hd(Sorted), <<>>, Opts),
                {Signed, _} = lists:foldl(
                    fun(A, {Acc, {N, Chain}}) ->
                        S = hb_message:commit(
                            (maps:without([<<"commitments">>], A))#{
                                <<"slot">> => N, <<"base-hashpath">> => Chain },
                            #{ <<"priv-wallet">> => Wallet }),
                        Next = hb_path:hashpath(Chain, hb_message:id(S, all, #{}),
                            hb_path:hashpath_alg(#{}, #{}), #{}),
                        {Acc#{ integer_to_binary(N) => S }, {N + 1, Next}}
                    end, {#{}, {First, Chain0}}, Served),
                Loaded#{ <<"assignments">> => Signed }
                end
        end
    end.

redeem(A) ->
    hb_maps:get(<<"action">>, hb_maps:get(<<"body">>, A, #{}, #{}), none, #{}) =:= <<"Redeem">>.

%% A server without PUSD's scheduler key names itself PUSD's scheduler in the
%% process it serves, and serves a schedule it signed, the Redeem in it twice.
%% The process's own signature does not cover that: nothing is accepted, and
%% the withdrawal it invented (2) is not signed.
forged_process_is_not_used_test_() ->
    {timeout, 120, fun() ->
        Hostile = ar_wallet:new(),
        HostileAddr = hb_util:human_id(ar_wallet:to_address(Hostile)),
        Twice = reschedule(Hostile, fun(A) ->
            case redeem(A) of true -> [A, A]; false -> [A] end end),
        St = replica_setup(<<"forged-process">>, fun(_) ->
            fun(Path, Body) ->
                case is_map(Body) andalso hb_maps:get(<<"type">>, Body, none, #{}) of
                    <<"Process">> -> Body#{ <<"scheduler-location">> => HostileAddr };
                    _ -> Twice(Path, Body)
                end
            end
        end),
        St1 = pass(St),
        ?assertEqual(nomatch, binary:match(signs(St1), <<"2=">>)),
        ?assertEqual(<<>>, signs(St1))
    end}.

%% A server adds a commitment of its own to an assignment. Only PUSD's
%% scheduler's signature is used, so the chain carries on past it.
added_commitment_is_not_used_test_() ->
    {timeout, 120, fun() ->
        Stranger = ar_wallet:new(),
        Add = fun(Path, Body) ->
            case binary:match(Path, <<"schedule">>) of
                nomatch -> Body;
                _ ->
                    Opts = #{ <<"hashpath">> => ignore },
                    Loaded = hb_cache:ensure_all_loaded(Body, Opts),
                    As = hb_maps:get(<<"assignments">>, Loaded, #{}, Opts),
                    Loaded#{ <<"assignments">> => maps:map(
                        fun(<<"0">>, A) -> hb_message:commit(A, #{ <<"priv-wallet">> => Stranger });
                           (_, A) -> A
                        end, As) }
            end
        end,
        St = replica_setup(<<"added-commitment">>, fun(_) -> Add end),
        St1 = pass(St),
        ?assertEqual(<<"1=", (true_signature(St1))/binary, ";">>, signs(St1))
    end}.

%% State another computation on this node left in the shared hot cache under
%% PUSD's ID is not the replica's: it computes PUSD itself.
hot_cache_is_not_read_test_() ->
    {timeout, 120, fun() ->
        St = #{ cfg := Cfg, me := Me } = replica_setup(<<"hot-cache">>, fun honest/1),
        Pusd = maps:get(<<"pusd">>, Cfg),
        Table = lib_process_cache_hot,
        case ets:whereis(Table) of
            undefined ->
                Self = self(),
                spawn(fun() -> ets:new(Table, [named_table, public, ordered_set]),
                    Self ! table, receive after infinity -> ok end end),
                receive table -> ok end;
            _ -> ok
        end,
        Fake = #{
            <<"bridgeinfo">> => hb_json:encode(#{ <<"attestors">> => [#{ <<"ao">> => Me }],
                <<"namespace">> => 0, <<"epoch">> => 2 }),
            <<"withdrawals">> => hb_json:encode([#{ <<"id">> => <<"9">>, <<"chain">> => <<"base">>,
                <<"status">> => <<"pending">>, <<"token">> => <<?TEST_TOKEN>>, <<"amount">> => <<"999">>,
                <<"sigs">> => #{}, <<"to">> => <<"0x4444444444444444444444444444444444444444">> }])
        },
        [ets:insert(Table, {{Pusd, Slot}, Fake}) || Slot <- [0, 1]],
        St1 = try pass(St) after [ets:delete(Table, {Pusd, Slot}) || Slot <- [0, 1]] end,
        ?assertEqual(<<"1=", (true_signature(St1))/binary, ";">>, signs(St1))
    end}.

%% Replace the Redeem's message with `Make()' and re-sign the assignment as
%% PUSD's (compromised) scheduler, keeping its slot and chain.
replace_redeem(Wallet, Make) ->
    tamper_redeem(fun(A) ->
        Orig = hb_maps:get(<<"body">>, A, #{}, #{}),
        hb_message:commit((maps:without([<<"commitments">>], A))#{ <<"body">> => Make(Orig) },
            #{ <<"priv-wallet">> => Wallet })
    end).

%% A genuinely signed Redeem addressed to another process, scheduled on PUSD:
%% it is not PUSD's message, and nothing is signed.
message_for_another_process_is_not_used_test_() ->
    {timeout, 120, fun() ->
        Other = hb_util:human_id(crypto:strong_rand_bytes(32)),
        Make = fun(_Orig) ->
            hb_message:commit(#{ <<"action">> => <<"Redeem">>, <<"to">> => ?TEST_TO,
                <<"amount">> => <<"5">>, <<"target">> => Other },
                #{ <<"priv-wallet">> => ar_wallet:new() }, <<"httpsig@1.0">>)
        end,
        St = replica_setup(<<"other-target">>, fun(W) -> replace_redeem(W, Make) end),
        St1 = pass(St),
        ?assertEqual(<<>>, signs(St1)),
        ?assertMatch(#{ stopped := <<"message is for another process", _/binary>> },
            maps:get(replica, St1))
    end}.

%% A signed message without a `target', scheduled on PUSD: the honest
%% scheduler takes the process from the path, so any wallet can send one. The
%% assignment says the slot is PUSD's; the replica goes on past it, and the
%% Redeem it carries is signed. Only a provenance failure stops a replica.
message_without_target_is_pusds_test_() ->
    {timeout, 120, fun() ->
        Make = fun(_Orig) ->
            hb_message:commit(#{ <<"action">> => <<"Redeem">>, <<"to">> => ?TEST_TO,
                <<"amount">> => <<"5">> },
                #{ <<"priv-wallet">> => ar_wallet:new() }, <<"httpsig@1.0">>)
        end,
        St = replica_setup(<<"no-target">>, fun(W) -> replace_redeem(W, Make) end),
        St1 = pass(St),
        ?assertEqual(<<"1=", (true_signature(St1))/binary, ";">>, signs(St1))
    end}.

%% A signed Assign-To authorizes the root even without a native TX target.
message_with_assign_to_is_pusds_test_() ->
    {timeout, 120, fun() ->
        Make = fun(Orig) ->
            hb_message:commit(#{<<"action">> => <<"Redeem">>, <<"to">> => ?TEST_TO,
                <<"amount">> => <<"5">>,
                <<"assign-to">> => hb_maps:get(<<"target">>, Orig, #{})},
                #{<<"priv-wallet">> => ar_wallet:new()}, <<"httpsig@1.0">>)
        end,
        St = replica_setup(<<"assign-to">>, fun(W) -> replace_redeem(W, Make) end),
        St1 = pass(St),
        ?assertEqual(<<"1=", (true_signature(St1))/binary, ";">>, signs(St1))
    end}.

%% An hmac names whoever it claims to: a message whose only commitment is an
%% hmac carrying a committer is unsigned, and nothing is signed.
hmac_is_not_a_signature_test_() ->
    {timeout, 120, fun() ->
        Claimed = hb_committer:uri(hb_util:human_id(ar_wallet:to_address(ar_wallet:new()))),
        Make = fun(Orig) ->
            Pusd = hb_maps:get(<<"target">>, Orig, none, #{}),
            M = hb_message:commit(#{ <<"action">> => <<"Redeem">>, <<"to">> => ?TEST_TO,
                <<"amount">> => <<"5">>, <<"target">> => Pusd }, #{}, #{ <<"type">> => <<"unsigned">> }),
            M#{ <<"commitments">> => maps:map(fun(_, C) -> C#{ <<"committer">> => Claimed } end,
                maps:get(<<"commitments">>, M)) }
        end,
        St = replica_setup(<<"hmac">>, fun(W) -> replace_redeem(W, Make) end),
        St1 = pass(St),
        ?assertEqual(<<>>, signs(St1))
    end}.

%% A cursor that cannot be read means starting over from what verifies, not a
%% crash.
unreadable_cursor_starts_over_test_() ->
    {timeout, 120, fun() ->
        St = #{ cfg := Cfg } = replica_setup(<<"cursor">>, fun honest/1),
        _ = pass(St),
        Cursor = <<(maps:get(<<"replica-dir">>, Cfg))/binary, "/", (maps:get(<<"pusd">>, Cfg))/binary,
            ".cursor.json">>,
        ok = file:write_file(Cursor, <<"not json">>),
        St2 = pass(St),
        ?assertMatch(#{ member := true, last_error := null }, St2)
    end}.

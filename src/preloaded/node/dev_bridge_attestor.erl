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
%%%   3. attests each unminted deposit with `Bridge-Mint';
%%%   4. signs each pending withdrawal for its chain with `Bridge-Sign';
%%%   5. acknowledges each release it saw with `Bridge-Released';
%%%   6. votes on each open membership proposal its operator approved
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
%%% AO wallet; the node only ever signs. Endpoints, all read-only:
%%%   GET  /~bridge-attestor@1.0/keys    this node's four public keys, to join a set
%%%   GET  /~bridge-attestor@1.0/status  cursors, last pass, last error, RPC hosts
%%%   GET  /~bridge-attestor@1.0/nudge   a bridge transaction was just sent: pass
%%%                                      now, then every `fast-interval' for
%%%                                      `fast-window'
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
%%% assignments it verified (`dev_bridge_attestor_replica'). It trusts nothing
%%% another node publishes, its own scheduler's included: every attestor,
%%% wherever PUSD runs, reads PUSD the same way. Until a slot verifies there is
%%% nothing to act on; a slot that does not verify is where the replica stays.
%%%
%%% Configuration, under the node option `bridge':
%%%   pusd, scheduler-url (where PUSD's scheduler is reached: transport only,
%%%   nothing it serves is used unverified), replica-dir (default
%%%   `bridge-replica'), interval, fast-interval, fast-window (ms), keys-file,
%%%   state-file, follow-file,
%%%   base => #{ rpc, contract, chain-id, confirmations, from-block, log-range },
%%%   solana => #{ rpc, program, commitment }, sui => #{ rpc, package, bridge }
-module(dev_bridge_attestor).
-implements(<<"bridge-attestor@1.0">>).
-export([device_info/1, start/3, status/3, keys/3, nudge/3]).
-export([proposals_to_vote/2, successor_ok/3]).
-export([withdrawal_signature/4, minted/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NAME, {<<"bridge-attestor@1.0">>, loop}).
-define(RESEND_MS, 60000).
-define(NUDGE_GAP_MS, 10000).
%% Nudges ignored after a capped fast stretch. Fixed, not `interval': a slow
%% idle interval saves RPC spend and must not slow a busy bridge down.
-define(COOL_MS, 15000).

device_info(_) ->
    #{ exports => [<<"start">>, <<"status">>, <<"keys">>, <<"nudge">>] }.

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
    Cfg = load_follow(Cfg0),
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
            loop(St)
    end.

%% A failed pass: what `status' shows, and what is logged. Neither carries a
%% URL (RPC URLs hold API keys), a map (the node's options hold its wallet) or
%% a call's arguments: class, reason and module:function/arity:line only.
failure(C, R, T) ->
    Reason = scrub(R),
    Shown = iolist_to_binary(io_lib:format("~0p:~0p", [C, Reason])),
    {binary:part(Shown, 0, min(512, byte_size(Shown))),
        {pass_failed, C, Reason, [frame(F) || F <- T]}}.

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
        {ok, View, Replica} -> act(View, St#{ replica => Replica });
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
    Member = lists:any(fun(A) -> maps:get(<<"ao">>, A, <<>>) =:= Me end,
        maps:get(<<"attestors">>, Info, [])),
    case Member of
        false ->
            St#{ member => false, last_pass => os:system_time(millisecond) };
        true ->
            {Found, Cursors} = scan(Cfg, maps:get(cursors, St), maps:get(opts, St)),
            Ns = hb_util:int(maps:get(<<"namespace">>, Info, 0)),
            St1 = remember(Found, St#{ cursors => Cursors, member => true, namespace => Ns,
                skipped => #{} }),
            % What was found is on disk before the cursors pass it.
            save_state(Cfg, St1),
            St2 = attest_deposits(Info, St1),
            St3 = sign_withdrawals(Withdrawals, St2),
            St4 = acknowledge(Withdrawals, St3),
            St5 = lists:foldl(
                fun(P, S) ->
                    guard(<<"proposal:", (item_id(P))/binary>>, S, fun() ->
                        Id = hb_util:int(maps:get(<<"id">>, P)),
                        once(<<"vote:", (integer_to_binary(Id))/binary>>, S, fun() -> vote(P, S) end)
                    end)
                end, St4, proposals_to_vote(Info, Me)),
            save_state(Cfg, St5),
            St5#{ last_pass => os:system_time(millisecond), last_error => null }
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
    Safe = Head - hb_util:int(maps:get(<<"confirmations">>, C, 12)),
    From = case Cursor of undefined -> hb_util:int(maps:get(<<"from-block">>, C, Safe)); N -> N + 1 end,
    To = min(Safe, From + hb_util:int(maps:get(<<"log-range">>, C, 2000)) - 1),
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

attest_deposits(Info, St = #{ deposits := Deps }) ->
    maps:fold(
        fun(Id, #{ chain := Chain, nonce := N } = E, S) ->
            case minted(Info, Chain, N) of
                true -> S#{ deposits := maps:remove(Id, maps:get(deposits, S)) };
                false ->
                    once(Id, S, fun() ->
                        send(#{
                            <<"action">> => <<"Bridge-Mint">>,
                            <<"depositid">> => Id, <<"chain">> => Chain,
                            <<"token">> => maps:get(token, E), <<"recipient">> => maps:get(recipient, E),
                            <<"quantity">> => maps:get(amount, E), <<"sourcetx">> => maps:get(tx, E, <<>>)
                        }, S)
                    end)
            end
        end, St, Deps).

sign_withdrawals(Withdrawals, St = #{ me := Me }) ->
    lists:foldl(
        fun(W, S) -> guard(<<"withdrawal:", (item_id(W))/binary>>, S, fun() ->
            Sigs = maps:get(<<"sigs">>, W, #{}),
            Pending = maps:get(<<"status">>, W, <<>>) =:= <<"pending">>,
            Signed = is_map(Sigs) andalso maps:is_key(Me, Sigs),
            Id = maps:get(<<"id">>, W),
            case Pending andalso not Signed of
                false -> S;
                true ->
                    % `releasesig', never `signature': that is the name of the
                    % message's own httpsig signature, and the scheduler refuses
                    % a message that carries both.
                    once(<<"sign:", Id/binary>>, S, fun() ->
                        Sig = withdrawal_signature(W, maps:get(cfg, S), maps:get(keys, S),
                            maps:get(namespace, S)),
                        send(#{ <<"action">> => <<"Bridge-Sign">>, <<"withdrawalid">> => Id,
                            <<"releasesig">> => Sig }, S)
                    end)
            end
        end) end, St, Withdrawals).

acknowledge(Withdrawals, St = #{ releases := Rel }) ->
    Pending = maps:from_list([{maps:get(<<"id">>, W), true} || W <- Withdrawals,
        maps:get(<<"status">>, W, <<>>) =:= <<"pending">>]),
    maps:fold(
        fun(Id, E, S) ->
            case maps:is_key(Id, Pending) of
                false -> S#{ releases := maps:remove(Id, maps:get(releases, S)) };
                true ->
                    once(<<"ack:", Id/binary>>, S, fun() ->
                        send(#{ <<"action">> => <<"Bridge-Released">>, <<"withdrawalid">> => Id,
                            <<"desttx">> => maps:get(tx, E, <<>>) }, S)
                    end)
            end
        end, St, Rel).

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
    case Now - maps:get(Key, Sent, 0) < ?RESEND_MS of
        true -> St;
        false ->
            Fun(),
            St#{ sent := Sent#{ Key => Now } }
    end.

%%% Signatures ----------------------------------------------------------------

%% @doc This node's signature over one withdrawal's release on its chain, as
%% 0x-hex. The chain's withdrawal id carries PUSD's namespace in the high word.
withdrawal_signature(W, Cfg, Keys, Namespace) ->
    Chain = maps:get(<<"chain">>, W),
    C = maps:get(Chain, Cfg),
    Id = (Namespace bsl 32) bor hb_util:int(maps:get(<<"id">>, W)),
    Token = maps:get(<<"token">>, W),
    To = maps:get(<<"to">>, W),
    Amount = maps:get(<<"amount">>, W),
    Sig = case Chain of
        <<"base">> ->
            dev_bridge_attestor_sign:base_sign(
                dev_bridge_attestor_sign:base_release(hb_util:int(maps:get(<<"chain-id">>, C)),
                    maps:get(<<"contract">>, C), Id, Token, To, Amount),
                maps:get(base, Keys));
        <<"solana">> ->
            dev_bridge_attestor_sign:ed25519_sign(
                dev_bridge_attestor_sign:solana_release(maps:get(<<"program">>, C), Id, Token, To, Amount),
                maps:get(solana, Keys));
        <<"sui">> ->
            dev_bridge_attestor_sign:ed25519_sign(
                dev_bridge_attestor_sign:sui_release(maps:get(<<"bridge">>, C), Id, Token, To, Amount),
                maps:get(sui, Keys))
    end,
    <<"0x", (dev_bridge_attestor_sign:hex(Sig))/binary>>.

%% @doc Open proposals this node's operator approved and it has not voted on.
proposals_to_vote(Info, Me) ->
    [P || P <- maps:get(<<"proposals">>, Info, []),
        maps:get(<<"status">>, P, <<>>) =:= <<"open">>,
        maps:get(Me, maps:get(<<"approvals">>, P, #{}), false) =:= true,
        not maps:is_key(Me, maps:get(<<"votes">>, P, #{}))].

%% The vote IS the chain authorisation: this node's signature over the
%% proposal's whole set, at its epoch, for each chain. A migration changes no
%% chain, so its vote carries nothing but the proposal.
vote(#{ <<"op">> := <<"migrate">> } = P, St) ->
    send(#{ <<"action">> => <<"Attestor-Vote">>,
        <<"proposalid">> => integer_to_binary(hb_util:int(maps:get(<<"id">>, P))) }, St);
vote(P, St = #{ cfg := Cfg, keys := Keys }) ->
    Id = hb_util:int(maps:get(<<"id">>, P)),
    Set = maps:get(<<"set">>, P),
    Epoch = hb_util:int(maps:get(<<"epoch">>, P)),
    Field = fun(K) -> [maps:get(K, M) || M <- Set] end,
    Hex = fun(S) -> <<"0x", (dev_bridge_attestor_sign:hex(S))/binary>> end,
    B = maps:get(<<"base">>, Cfg), Sol = maps:get(<<"solana">>, Cfg), Sui = maps:get(<<"sui">>, Cfg),
    send(#{
        <<"action">> => <<"Attestor-Vote">>, <<"proposalid">> => integer_to_binary(Id),
        <<"basesig">> => Hex(dev_bridge_attestor_sign:base_sign(
            dev_bridge_attestor_sign:base_attestors(hb_util:int(maps:get(<<"chain-id">>, B)),
                maps:get(<<"contract">>, B), Epoch, Field(<<"base">>)), maps:get(base, Keys))),
        <<"solanasig">> => Hex(dev_bridge_attestor_sign:ed25519_sign(
            dev_bridge_attestor_sign:solana_attestors(maps:get(<<"program">>, Sol), Epoch, Field(<<"solana">>)),
            maps:get(solana, Keys))),
        <<"suisig">> => Hex(dev_bridge_attestor_sign:ed25519_sign(
            dev_bridge_attestor_sign:sui_attestors(maps:get(<<"bridge">>, Sui), Epoch, Field(<<"sui">>)),
            maps:get(sui, Keys)))
    }, St).

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
    % `subject: self' or the scheduler schedules the request body.
    Signed = hb_message:commit(Tags#{ <<"target">> => Pusd, <<"type">> => <<"Message">>,
        <<"subject">> => <<"self">> }, Opts, <<"httpsig@1.0">>),
    Res = hb_http:post(maps:get(<<"scheduler-url">>, Cfg),
        <<"/", Pusd/binary, "~process@1.0/schedule">>, Signed, Opts),
    ?event(bridge, {sent, maps:get(<<"action">>, Tags), Res}),
    Res.

%% PUSD (`Pusd', the configured one or a successor) as this node computed it:
%% `{ok, View, Replica}' with the replica's slot and where it stopped, if it
%% did; `{not_yet, Why}' while no slot has verified.
view(Pusd, #{ cfg := Cfg, opts := Opts }) ->
    Dir = hb_util:bin(maps:get(<<"replica-dir">>, Cfg, <<"bridge-replica">>)),
    case dev_bridge_attestor_replica:view(Pusd, maps:get(<<"scheduler-url">>, Cfg),
            #{ opts => Opts, dir => Dir }) of
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

%% The chain keys, made on first start. Hex 32-byte secrets.
load_keys(File) ->
    case file:read_file(File) of
        {ok, Bin} ->
            K = hb_json:decode(Bin),
            #{ base => unhex(maps:get(<<"base">>, K)), solana => unhex(maps:get(<<"solana">>, K)),
               sui => unhex(maps:get(<<"sui">>, K)) };
        {error, enoent} ->
            K = #{ base => crypto:strong_rand_bytes(32), solana => crypto:strong_rand_bytes(32),
                   sui => crypto:strong_rand_bytes(32) },
            % Owner-only before a secret byte is in it.
            ok = file:write_file(File, <<>>),
            ok = file:change_mode(File, 8#600),
            ok = file:write_file(File, hb_json:encode(maps:from_list(
                [{atom_to_binary(N), dev_bridge_attestor_sign:hex(V)} || {N, V} <- maps:to_list(K)]))),
            K
    end.

unhex(<<"0x", H/binary>>) -> dev_bridge_attestor_sign:unhex(H);
unhex(H) -> dev_bridge_attestor_sign:unhex(H).

public_keys(#{ me := Me, keys := K }) ->
    #{
        <<"ao">> => Me,
        <<"base">> => dev_bridge_attestor_sign:base_address(maps:get(base, K)),
        <<"solana">> => dev_bridge_attestor_sign:base58_encode(dev_bridge_attestor_sign:ed25519_public(maps:get(solana, K))),
        <<"sui">> => <<"0x", (dev_bridge_attestor_sign:hex(dev_bridge_attestor_sign:ed25519_public(maps:get(sui, K))))/binary>>
    }.

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
        <<"replica">> => maps:get(replica, St, null),
        <<"skipped">> => [#{ <<"item">> => K, <<"reason">> => V }
            || {K, V} <- lists:sort(maps:to_list(maps:get(skipped, St, #{})))],
        <<"rpcs">> => rpc_hosts(maps:get(cfg, St))
    }.

%%% Tests ---------------------------------------------------------------------

next_interval_test() ->
    Cfg = #{ <<"interval">> => 15000, <<"fast-interval">> => 1000 },
    ?assertEqual(1000, next_interval(Cfg, 2000, 1999)),
    ?assertEqual(15000, next_interval(Cfg, 2000, 2000)),
    ?assertEqual(5000, next_interval(#{}, 0, 1)).

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

proposals_to_vote_test() ->
    Me = <<"me">>,
    P = fun(Id, Status, Appr, Votes) -> #{ <<"id">> => Id, <<"status">> => Status,
        <<"approvals">> => Appr, <<"votes">> => Votes } end,
    Info = #{ <<"proposals">> => [
        P(1, <<"open">>, #{ Me => true }, #{}),
        P(2, <<"open">>, #{ <<"other">> => true }, #{}),
        P(3, <<"open">>, #{ Me => true }, #{ Me => #{} }),
        P(4, <<"applied">>, #{ Me => true }, #{})
    ] },
    ?assertEqual([1], [maps:get(<<"id">>, X) || X <- proposals_to_vote(Info, Me)]).

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
    Cfg = #{ <<"base">> => #{ <<"chain-id">> => 8453,
        <<"contract">> => <<"0x1111111111111111111111111111111111111111">> } },
    ?assertEqual(
        <<"0x78338d741889a13ef8a5523422e3a9827f958407e44e028613ec426bb6ea4a00",
          "2ac282b54427fc3c6b5b2e80ec6cd10288c4bf425443d5f163e6b343eaca773d1c">>,
        withdrawal_signature(W, Cfg, #{ base => binary:copy(<<1>>, 32) }, 5)).

%%% The AO side on v0.11 ------------------------------------------------------

%% A stand-in PUSD on `lua-patch@1.0': it publishes `bridgeinfo' naming the
%% node as its only attestor, with one open migration the node's operator
%% approved, and records each `Attestor-Vote' as `<proposal>:<committer>;'.
-define(TEST_TOKEN, "0x2222222222222222222222222222222222222222").
-define(TEST_TO, <<"0x3333333333333333333333333333333333333333">>).

fake_pusd(Me, Opts) ->
    Info = hb_json:encode(#{
        <<"epoch">> => 2, <<"namespace">> => 0,
        <<"attestors">> => [#{ <<"ao">> => Me }],
        <<"proposals">> => [#{ <<"id">> => 1, <<"status">> => <<"open">>,
            <<"op">> => <<"migrate">>, <<"approvals">> => #{ Me => true },
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
%% and votes on the approved proposal. PUSD sees the vote committed by the
%% node's own address.
vote_reaches_pusd(Cfg0, Opts) ->
    St0 = initial_state(Cfg0, Opts),
    Me = maps:get(me, St0),
    % A first message, so PUSD has computed and published something.
    {ok, _} = send(#{ <<"action">> => <<"Ping">> }, St0),
    {ok, View, _} = view(maps:get(<<"pusd">>, Cfg0), St0),
    ?assertMatch(#{ <<"epoch">> := 2 }, key(<<"bridgeinfo">>, View)),
    St1 = pass(St0),
    ?assertMatch(#{ member := true, last_error := null }, St1),
    ?assertEqual(<<"1:", Me/binary, ";">>, raw(<<"votes">>, St1)),
    % Within RESEND_MS the vote is not sent again.
    _ = pass(St1),
    ?assertEqual(<<"1:", Me/binary, ";">>, raw(<<"votes">>, St1)).

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
        cfg => #{ <<"pusd">> => <<"p">>, <<"base">> => #{ <<"chain-id">> => 8453,
            <<"contract">> => <<"0x1111111111111111111111111111111111111111">> } } },
    St1 = sign_withdrawals([W(<<"9">>, <<"base">>, <<"0xnot-an-address">>),
        W(<<"10">>, <<"eth">>, <<"0x3333333333333333333333333333333333333333">>), not_a_map], St),
    ?assertEqual(3, maps:size(maps:get(skipped, St1))),
    ?assertEqual(#{}, maps:get(sent, St1)),
    ?assertMatch(#{ <<"skipped">> := [_, _, _] },
        status_view(St1#{ cursors => #{}, deposits => #{}, releases => #{}, member => true,
            last_pass => 0, last_error => null })).

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

%% The signature this node owes PUSD's real withdrawal 1.
true_signature(St = #{ cfg := Cfg, keys := Keys }) ->
    withdrawal_signature(#{ <<"id">> => <<"1">>, <<"chain">> => <<"base">>,
        <<"token">> => <<?TEST_TOKEN>>, <<"to">> => ?TEST_TO, <<"amount">> => <<"5">> },
        Cfg, Keys, maps:get(namespace, St)).

replica_signs_its_own_withdrawal_test_() ->
    {timeout, 120, fun() ->
        St = replica_setup(<<"honest">>, fun honest/1),
        St1 = pass(St),
        ?assertMatch(#{ member := true, last_error := null }, St1),
        ?assertEqual(<<"1=", (true_signature(St1))/binary, ";">>, raw(<<"signs">>, St1))
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
        ?assertEqual(<<"1=", (true_signature(St1))/binary, ";">>, raw(<<"signs">>, St1))
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
        ?assertEqual(<<>>, raw(<<"signs">>, St1)),
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
        ?assertEqual(<<>>, raw(<<"signs">>, St1)),
        ?assertMatch(#{ stopped := <<"message signature does not verify", _/binary>> },
            maps:get(replica, St1))
    end}.


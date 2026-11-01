%%% @doc `bridge@1.0': this node as one attestor of the PUSD bridge
%%% (paralith-mono products/bridge/BRIDGE_NATIVE.md).
%%%
%%% A background loop, started by the node's `start' hook, that on every pass:
%%%
%%%   1. reads PUSD's published `bridgeinfo' and `withdrawals';
%%%   2. scans Base (through `eth-client@1.0'), Solana and Sui for deposits and
%%%      releases since its cursors;
%%%   3. attests each unminted deposit with `Bridge-Mint';
%%%   4. signs each pending withdrawal for its chain with `Bridge-Sign';
%%%   5. acknowledges each release it saw with `Bridge-Released';
%%%   6. relays root credits, fast-ledger exits and ledger moves;
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
%%% AO wallet; the node only ever signs. Endpoints, all read-only:
%%%   GET  /~bridge@1.0/keys      this node's four public keys, to join a set
%%%   GET  /~bridge@1.0/status    cursors, last pass, last error, RPC hosts
%%%   GET  /~bridge@1.0/nudge     a bridge transaction was just sent: pass now,
%%%                               then every `fast-interval' for `fast-window'
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
%%% Configuration, under the node option `bridge':
%%%   pusd, pusd-node (the URL of the node that runs PUSD; omit when it is this
%%%   node), interval, fast-interval, fast-window (ms), keys-file, state-file, follow-file,
%%%   fast-ledgers => #{ LedgerProcess => #{ node => URL } },
%%%   base => #{ rpc, contract, chain-id, confirmations, from-block, log-range },
%%%   solana => #{ rpc, program, commitment }, sui => #{ rpc, package, bridge }
-module(dev_bridge).
-export([info/1, start/3, status/3, keys/3, nudge/3]).
-export([proposals_to_vote/2, successor_ok/3]).
-export([withdrawal_signature/5, minted/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NAME, {<<"bridge@1.0">>, loop}).
-define(RESEND_MS, 60000).
-define(NUDGE_GAP_MS, 10000).
%% Nudges ignored after a capped fast stretch. Fixed, not `interval': a slow
%% idle interval saves RPC spend and must not slow a busy bridge down.
-define(COOL_MS, 15000).

info(_) ->
    #{ exports => [<<"start">>, <<"status">>, <<"keys">>, <<"nudge">>] }.

%%% Endpoints -----------------------------------------------------------------

%% @doc The node `start' hook: spawn the loop once, hand the node message back.
%% Never fails: a bridge problem must not stop the node from booting.
start(_Base, Req, Opts) ->
    NodeMsg = hb_maps:get(<<"body">>, Req, Opts, Opts),
    try
        case config(NodeMsg) of
            #{ <<"pusd">> := _ } = Cfg ->
                case hb_name:lookup(?NAME) of
                    Pid when is_pid(Pid) -> ok;
                    _ ->
                        Pid = spawn(fun() -> init(Cfg, NodeMsg) end),
                        hb_name:register(?NAME, Pid)
                end;
            _ ->
                ?event(bridge, {not_configured})
        end
    catch C:R:T -> ?event(bridge, {start_failed, C, R, T})
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
    Cfg = load_follow(Cfg0),
    Keys = load_keys(maps:get(<<"keys-file">>, Cfg, <<"bridge-keys.json">>)),
    Me = hb_util:human_id(ar_wallet:to_address(hb_opts:get(priv_wallet, no_viable_wallet, Opts))),
    {Cursors, Deposits, Releases} = load_state(Cfg),
    St = #{
        cfg => Cfg, opts => Opts, keys => Keys, me => Me,
        cursors => Cursors, deposits => Deposits, releases => Releases, sent => #{},
        last_pass => 0, last_error => null, member => false, namespace => 0,
        timer => undefined, pass_at => 0, fast_until => 0, fast_since => 0, nudge_at => 0
    },
    ?event(bridge, {started, {ao, Me}}),
    self() ! pass,
    loop(St).

loop(St) ->
    receive
        pass ->
            Start = os:system_time(millisecond),
            St1 = St#{ timer => undefined, pass_at => Start },
            St2 = try pass(St1)
                  catch C:R:T ->
                      ?event(bridge, {pass_failed, C, R, T}),
                      St1#{ last_error => iolist_to_binary(io_lib:format("~p:~p", [C, R])) }
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

pass(St) ->
    Info = published(<<"bridgeinfo">>, St),
    case maps:get(<<"successor">>, Info, <<>>) of
        Succ when is_binary(Succ), byte_size(Succ) =:= 43 -> follow(Succ, Info, St);
        _ -> attest(Info, St)
    end.

attest(Info, St = #{ cfg := Cfg, me := Me }) ->
    Withdrawals = case published(<<"withdrawals">>, St) of L when is_list(L) -> L; _ -> [] end,
    LedgerDeposits = case published(<<"ledgerdeposits">>, St) of L1 when is_list(L1) -> L1; _ -> [] end,
    LedgerReceipts = case published(<<"ledgerexits">>, St) of L2 when is_list(L2) -> L2; _ -> [] end,
    MoveReceipts = case published(<<"ledgermoves">>, St) of L3 when is_list(L3) -> L3; _ -> [] end,
    Member = lists:any(fun(A) -> maps:get(<<"ao">>, A, <<>>) =:= Me end,
        maps:get(<<"attestors">>, Info, [])),
    case Member of
        false ->
            St#{ member => false, last_pass => os:system_time(millisecond) };
        true ->
            {Found, Cursors} = scan(Cfg, maps:get(cursors, St), maps:get(opts, St)),
            Ns = hb_util:int(maps:get(<<"namespace">>, Info, 0)),
            St1 = remember(Found, St#{ cursors => Cursors, member => true, namespace => Ns }),
            % What was found is on disk before the cursors pass it.
            save_state(Cfg, St1),
            St2 = attest_deposits(Info, St1),
            Epoch = hb_util:int(maps:get(<<"epoch">>, Info)),
            St3 = sign_withdrawals(Withdrawals, Epoch, St2),
            St4 = acknowledge(Withdrawals, St3),
            St5 = relay_fast_ledgers(Info, LedgerDeposits, LedgerReceipts,
                MoveReceipts, Epoch, St4),
            St6 = lists:foldl(
                fun(P, S) ->
                    Id = hb_util:int(maps:get(<<"id">>, P)),
                    once(<<"vote:", (integer_to_binary(Id))/binary>>, S, fun() -> vote(P, S) end)
                end, St5, proposals_to_vote(Info, Me)),
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
                    case scan_chain(Chain, C, maps:get(Chain, Cur, undefined), Opts) of
                        {ok, Events, Next} -> {Acc ++ Events, Cur#{ Chain => Next }};
                        Error -> ?event(bridge, {scan_failed, Chain, Error}), {Acc, Cur}
                    end
            end
        end, {[], Cursors}, [<<"base">>, <<"solana">>, <<"sui">>]).

scan_chain(<<"base">>, C, Cursor, Opts) ->
    {ok, Head} = hb_bridge_chains:base_head(C, Opts),
    Safe = Head - hb_util:int(maps:get(<<"confirmations">>, C, 12)),
    From = case Cursor of undefined -> hb_util:int(maps:get(<<"from-block">>, C, Safe)); N -> N + 1 end,
    To = min(Safe, From + hb_util:int(maps:get(<<"log-range">>, C, 2000)) - 1),
    case To < From of
        true -> {ok, [], Cursor};
        false ->
            case hb_bridge_chains:base_scan(C, From, To, Opts) of
                {ok, Events} -> {ok, Events, To};
                Error -> Error
            end
    end;
scan_chain(<<"solana">>, C, Cursor, Opts) ->
    Until = case Cursor of undefined -> undefined; null -> undefined; S -> S end,
    hb_bridge_chains:solana_scan(C, Until, Opts);
scan_chain(<<"sui">>, C, Cursor, Opts) ->
    Cur = case Cursor of
        #{ <<"deposit">> := D, <<"released">> := R } -> #{ deposit => undef(D), released => undef(R) };
        _ -> #{}
    end,
    case hb_bridge_chains:sui_scan(C, Cur, Opts) of
        {ok, Events, #{ deposit := D2, released := R2 }} ->
            {ok, Events, #{ <<"deposit">> => null_if(D2), <<"released">> => null_if(R2) }};
        Error -> Error
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

sign_withdrawals(Withdrawals, Epoch, St = #{ me := Me }) ->
    lists:foldl(
        fun(W, S) ->
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
                        Sig = withdrawal_signature(W, Epoch, maps:get(cfg, S), maps:get(keys, S),
                            maps:get(namespace, S)),
                        send(#{ <<"action">> => <<"Bridge-Sign">>, <<"withdrawalid">> => Id,
                            <<"releasesig">> => Sig }, S)
                    end)
            end
        end, St, Withdrawals).

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
                                   <<"exitid">> => ExitId, <<"account">> => maps:get(<<"account">>, E),
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

%% @doc Root votes for pending locks whose exact source and destination are
%% both in this node's registered/configured ledger intersection.
move_root_plans(Epoch, Ledger, LaneMoves, Receipts, Allowed) ->
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
                                   <<"moveid">> => MoveId,
                                   <<"account">> => Account,
                                   <<"quantity">> => Quantity,
                                   <<"attestorepoch">> => integer_to_binary(Epoch) }}}
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
        andalso maps:is_key(Destination, Allowed)
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
                <<"status">> => Status, <<"attestorepoch">> => EpochText },
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

fast_ledger_node(Spec) when is_binary(Spec) -> Spec;
fast_ledger_node(Spec) when is_map(Spec) -> maps:get(<<"node">>, Spec, undefined);
fast_ledger_node(_) -> undefined.

relay_fast_ledgers(Info, Deposits, Receipts, MoveReceipts, Epoch,
        St = #{ cfg := Cfg }) ->
    Registered = maps:get(<<"ledgers">>, Info, #{}),
    Configured = maps:get(<<"fast-ledgers">>, Cfg, #{}),
    Root = maps:get(<<"pusd">>, Cfg),
    Allowed = maps:filter(
        fun(Ledger, _Spec) -> maps:is_key(Ledger, Registered) end,
        Configured
    ),
    St1 = dispatch_move_plans(
        move_receipt_plans(Root, MoveReceipts, Allowed), St
    ),
    maps:fold(
        fun(Ledger, Spec, S) ->
            case maps:is_key(Ledger, Allowed) of
                false -> S;
                true ->
                    guard(<<"ledger:", Ledger/binary>>, S, fun() ->
                        Node = fast_ledger_node(Spec),
                        LaneInfo = published_at(Ledger, Node, <<"laneinfo">>, S),
                        LaneExits = case published_at(Ledger, Node, <<"laneexits">>, S) of
                            L when is_list(L) -> L;
                            _ -> []
                        end,
                        LaneMoves = case published_at(Ledger, Node, <<"lanemoves">>, S) of
                            L2 when is_list(L2) -> L2;
                            _ -> []
                        end,
                        Plans = credit_plans(Root, Ledger, LaneInfo, Deposits)
                            ++ exit_plans(Ledger, LaneExits, Receipts),
                        S2 = dispatch_lane_plans(Ledger, Node, Plans, S),
                        dispatch_move_plans(
                            move_root_plans(Epoch, Ledger, LaneMoves,
                                MoveReceipts, Allowed), S2
                        )
                    end)
            end
        end,
        St1,
        Configured
    ).

dispatch_lane_plans(_Ledger, _Node, [], St) -> St;
dispatch_lane_plans(Ledger, Node, [{Destination, Key, Tags}|Rest], St) ->
    St2 = guard(Key, St, fun() ->
        once(Key, St, fun() ->
            case Destination of
                root -> send(Tags, St);
                lane -> send_at(Ledger, Node, Tags, St)
            end
        end)
    end),
    dispatch_lane_plans(Ledger, Node, Rest, St2).

dispatch_move_plans(Plans, St = #{ cfg := Cfg }) ->
    Configured = maps:get(<<"fast-ledgers">>, Cfg, #{}),
    lists:foldl(
        fun({Target, RetryKey, Tags}, Acc) ->
            guard(RetryKey, Acc, fun() ->
                once(RetryKey, Acc, fun() ->
                    case Target of
                        root -> send(Tags, Acc);
                        {lane, Ledger} ->
                            Spec = maps:get(Ledger, Configured),
                            send_at(Ledger, fast_ledger_node(Spec), Tags, Acc)
                    end
                end)
            end)
        end,
        St,
        Plans
    ).

%% One malformed or unavailable ledger item must not stop unrelated bridge
%% work in this pass.
guard(Key, St, Fun) ->
    try Fun()
    catch C:R:T ->
        ?event(bridge, {skipped, Key, C, R, T}),
        St
    end.

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
            hb_bridge_sign:base_sign(
                hb_bridge_sign:base_release(hb_util:int(maps:get(<<"chain-id">>, C)),
                    maps:get(<<"contract">>, C), Root, Epoch, Id, Token, To, Amount),
                maps:get(base, Keys));
        <<"solana">> ->
            hb_bridge_sign:ed25519_sign(
                hb_bridge_sign:solana_release(maps:get(<<"program">>, C),
                    Root, Epoch, Id, Token, To, Amount),
                maps:get(solana, Keys));
        <<"sui">> ->
            hb_bridge_sign:ed25519_sign(
                hb_bridge_sign:sui_release(maps:get(<<"bridge">>, C),
                    Root, Epoch, Id, Token, To, Amount),
                maps:get(sui, Keys))
    end,
    <<"0x", (hb_bridge_sign:hex(Sig))/binary>>.

%% @doc Approved open/applied proposals this node has not voted on. Applied
%% proposals keep collecting signatures so one bad chain signature cannot make
%% an otherwise-settled proposal impossible to submit.
proposals_to_vote(Info, Me) ->
    [P || P <- maps:get(<<"proposals">>, Info, []),
        (maps:get(<<"status">>, P, <<>>) =:= <<"open">> orelse
            maps:get(<<"status">>, P, <<>>) =:= <<"applied">>),
        maps:get(Me, maps:get(<<"approvals">>, P, #{}), false) =:= true,
        not maps:is_key(Me, maps:get(<<"votes">>, P, #{}))].

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
    Sig = hb_bridge_sign:base_sign(
        hb_bridge_sign:base_admin(
            hb_util:int(maps:get(<<"chain-id">>, C)), maps:get(<<"contract">>, C),
            Root, Epoch, Action, Token, true),
        maps:get(base, Keys)
    ),
    #{ <<"basesig">> => signature_hex(Sig) };
admin_chain_signature(<<"solana">>, Token, Epoch, Cfg, Keys, Action) ->
    C = maps:get(<<"solana">>, Cfg),
    Root = maps:get(<<"pusd">>, Cfg),
    Sig = hb_bridge_sign:ed25519_sign(
        hb_bridge_sign:solana_admin(
            maps:get(<<"program">>, C), Root, Epoch, Action, Token, true),
        maps:get(solana, Keys)
    ),
    #{ <<"solanasig">> => signature_hex(Sig) };
admin_chain_signature(<<"sui">>, Token, Epoch, Cfg, Keys, Action) ->
    C = maps:get(<<"sui">>, Cfg),
    Root = maps:get(<<"pusd">>, Cfg),
    Sig = hb_bridge_sign:ed25519_sign(
        hb_bridge_sign:sui_admin(
            maps:get(<<"bridge">>, C), Root, Epoch, Action, Token, true),
        maps:get(sui, Keys)
    ),
    #{ <<"suisig">> => signature_hex(Sig) }.

signature_hex(Sig) -> <<"0x", (hb_bridge_sign:hex(Sig))/binary>>.

zero_token(<<"base">>) -> <<"0x0000000000000000000000000000000000000000">>;
zero_token(<<"solana">>) -> <<"11111111111111111111111111111111">>;
zero_token(<<"sui">>) -> <<>>.

%% The vote IS the chain authorisation: this node's signature over the
%% proposal's whole set, at its epoch, for each chain. A migration signs the
%% exact old-root to successor transition for every chain.
vote(#{ <<"kind">> := <<"admin">> } = P, St = #{ cfg := Cfg, keys := Keys }) ->
    send(admin_vote_tags(P, Cfg, Keys), St);
vote(#{ <<"op">> := <<"migrate">> } = P, St = #{ cfg := Cfg, keys := Keys }) ->
    Id = hb_util:int(maps:get(<<"id">>, P)),
    Epoch = migration_epoch(P),
    Root = maps:get(<<"pusd">>, Cfg),
    Successor = maps:get(<<"successor">>, P),
    Hex = fun(S) -> <<"0x", (hb_bridge_sign:hex(S))/binary>> end,
    B = maps:get(<<"base">>, Cfg), Sol = maps:get(<<"solana">>, Cfg), Sui = maps:get(<<"sui">>, Cfg),
    send(#{
        <<"action">> => <<"Attestor-Vote">>, <<"proposalid">> => integer_to_binary(Id),
        <<"currentepoch">> => integer_to_binary(Epoch), <<"newepoch">> => integer_to_binary(Epoch),
        <<"basesig">> => Hex(hb_bridge_sign:base_sign(
            hb_bridge_sign:base_root(hb_util:int(maps:get(<<"chain-id">>, B)),
                maps:get(<<"contract">>, B), Epoch, Root, Successor), maps:get(base, Keys))),
        <<"solanasig">> => Hex(hb_bridge_sign:ed25519_sign(
            hb_bridge_sign:solana_root(maps:get(<<"program">>, Sol),
                Epoch, Root, Successor), maps:get(solana, Keys))),
        <<"suisig">> => Hex(hb_bridge_sign:ed25519_sign(
            hb_bridge_sign:sui_root(maps:get(<<"bridge">>, Sui),
                Epoch, Root, Successor), maps:get(sui, Keys)))
    }, St);
vote(P, St = #{ cfg := Cfg, keys := Keys }) ->
    Id = hb_util:int(maps:get(<<"id">>, P)),
    Set = maps:get(<<"set">>, P),
    {CurrentEpoch, NewEpoch} = proposal_epochs(P),
    Root = maps:get(<<"pusd">>, Cfg),
    Field = fun(K) -> [maps:get(K, M) || M <- Set] end,
    Hex = fun(S) -> <<"0x", (hb_bridge_sign:hex(S))/binary>> end,
    B = maps:get(<<"base">>, Cfg), Sol = maps:get(<<"solana">>, Cfg), Sui = maps:get(<<"sui">>, Cfg),
    send(#{
        <<"action">> => <<"Attestor-Vote">>, <<"proposalid">> => integer_to_binary(Id),
        <<"currentepoch">> => integer_to_binary(CurrentEpoch),
        <<"newepoch">> => integer_to_binary(NewEpoch),
        <<"basesig">> => Hex(hb_bridge_sign:base_sign(
            hb_bridge_sign:base_attestors(hb_util:int(maps:get(<<"chain-id">>, B)),
                maps:get(<<"contract">>, B), Root, CurrentEpoch, NewEpoch,
                Field(<<"base">>)), maps:get(base, Keys))),
        <<"solanasig">> => Hex(hb_bridge_sign:ed25519_sign(
            hb_bridge_sign:solana_attestors(maps:get(<<"program">>, Sol),
                Root, CurrentEpoch, NewEpoch, Field(<<"solana">>)),
            maps:get(solana, Keys))),
        <<"suisig">> => Hex(hb_bridge_sign:ed25519_sign(
            hb_bridge_sign:sui_attestors(maps:get(<<"bridge">>, Sui),
                Root, CurrentEpoch, NewEpoch, Field(<<"sui">>)),
            maps:get(sui, Keys)))
    }, St).

%%% Succession ----------------------------------------------------------------

%% Move to the successor if it checks out; otherwise stay, say why, and keep
%% attesting here (this PUSD still owes its pending withdrawals).
follow(Succ, Info, St = #{ cfg := Cfg }) ->
    Here = maps:get(<<"pusd">>, Cfg),
    Next = published(<<"bridgeinfo">>, St#{ cfg := Cfg#{ <<"pusd">> => Succ } }),
    case successor_ok(Info, Next, Here) of
        true ->
            Cfg2 = Cfg#{ <<"pusd">> => Succ },
            save_follow(Cfg2),
            ?event(bridge, {followed, {from, Here}, {to, Succ}}),
            St#{ cfg := Cfg2, deposits := #{}, releases := #{}, sent := #{}, namespace := 0,
                last_pass := os:system_time(millisecond), last_error := null };
        {false, Why} ->
            St2 = attest(Info, St),
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

%% Commit with the node's wallet and schedule onto PUSD: locally as dev_push
%% does when PUSD runs here, else by posting the signed message to the
%% scheduler of the node that runs it, as any wallet client does.
send(Tags, #{ cfg := Cfg, opts := Opts }) ->
    Pusd = maps:get(<<"pusd">>, Cfg),
    send_at(Pusd, maps:get(<<"pusd-node">>, Cfg, undefined), Tags,
        #{ cfg => Cfg, opts => Opts }).

send_at(Process, Node, Tags, #{ opts := Opts }) ->
    Res = case Node of
        undefined ->
            Signed = hb_message:commit(Tags#{ <<"target">> => Process }, Opts, <<"httpsig@1.0">>),
            {ok, _} = hb_cache:write(Signed, Opts),
            {ok, Proc} = hb_cache:read(Process, Opts),
            hb_ao:resolve({as, <<"process@1.0">>, Proc},
                #{ <<"path">> => <<"schedule">>, <<"method">> => <<"POST">>, <<"body">> => Signed }, Opts);
        _ ->
            % `subject: self' or the scheduler schedules the request body.
            Signed = hb_message:commit(Tags#{ <<"target">> => Process, <<"type">> => <<"Message">>,
                <<"subject">> => <<"self">> }, Opts, <<"httpsig@1.0">>),
            hb_http:post(Node, <<"/", Process/binary, "~process@1.0/schedule">>, Signed, Opts)
    end,
    ?event(bridge, {sent, maps:get(<<"action">>, Tags), {process, Process}, Res}),
    Res.

%% A published key as decoded JSON, or #{} when absent (the node answers a
%% missing key with its HTML page, never with an error).
published(Key, #{ cfg := Cfg, opts := Opts }) ->
    Pusd = maps:get(<<"pusd">>, Cfg),
    published_at(Pusd, maps:get(<<"pusd-node">>, Cfg, undefined), Key,
        #{ cfg => Cfg, opts => Opts }).

published_at(Process, Node, Key, #{ opts := Opts }) ->
    case Node of
        undefined ->
            {ok, Proc} = hb_cache:read(Process, Opts),
            json_value(hb_ao:get(<<"now/", Key/binary>>, Proc, not_found, Opts));
        _ ->
            Url = <<Node/binary, "/", Process/binary, "~process@1.0/now/", Key/binary>>,
            case hb_bridge_chains:get_body(Url, Opts) of
                {ok, Body} -> json_value(Body);
                _ -> #{}
            end
    end.

%% A published value as decoded JSON. It may arrive as JSON text, as JSON text
%% inside a JSON string, or wrapped in a `body'; an HTML page means absent.
json_value(V) when is_map(V), is_map_key(<<"body">>, V) -> json_value(maps:get(<<"body">>, V));
json_value(V) when is_map(V); is_list(V) -> V;
json_value(<<"<", _/binary>>) -> #{};
json_value(Bin) when is_binary(Bin) ->
    case catch hb_json:decode(Bin) of
        S when is_binary(S) -> json_value(S);
        V when is_map(V); is_list(V) -> json_value(V);
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
                [{atom_to_binary(N), hb_bridge_sign:hex(V)} || {N, V} <- maps:to_list(K)]))),
            K
    end.

unhex(<<"0x", H/binary>>) -> hb_bridge_sign:unhex(H);
unhex(H) -> hb_bridge_sign:unhex(H).

public_keys(#{ me := Me, keys := K }) ->
    #{
        <<"ao">> => Me,
        <<"base">> => hb_bridge_sign:base_address(maps:get(base, K)),
        <<"solana">> => hb_bridge_sign:base58_encode(hb_bridge_sign:ed25519_public(maps:get(solana, K))),
        <<"sui">> => <<"0x", (hb_bridge_sign:hex(hb_bridge_sign:ed25519_public(maps:get(sui, K))))/binary>>
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
        ++ [{<<"pusd-node">>, Host(N)} || N <- [maps:get(<<"pusd-node">>, Cfg, undefined)],
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
    Cfg = #{ <<"pusd-node">> => <<"https://hyperbeam.tylerw.ai">>,
        <<"base">> => #{ <<"rpc">> => <<"https://base-mainnet.g.alchemy.com/v2/SECRET">> },
        <<"solana">> => #{ <<"rpc">> => <<"https://solana-mainnet.g.alchemy.com/v2/SECRET?x=1">> },
        <<"sui">> => #{ <<"package">> => <<"0x1">> } },
    ?assertEqual(#{ <<"base">> => <<"base-mainnet.g.alchemy.com">>,
        <<"solana">> => <<"solana-mainnet.g.alchemy.com">>, <<"sui">> => <<"graphql.mainnet.sui.io">>,
        <<"pusd-node">> => <<"hyperbeam.tylerw.ai">> }, rpc_hosts(Cfg)).

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
    ?assertEqual([1, 4], [maps:get(<<"id">>, X) || X <- proposals_to_vote(Info, Me)]).

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
    ?assertEqual(Info, json_value(hb_json:encode(Text))),
    ?assertEqual(Info, json_value(#{ <<"body">> => Text })),
    ?assertEqual(#{}, json_value(<<"<!doctype html><html>">>)),
    ?assertEqual(#{}, json_value(not_found)).

minted_test() ->
    Info = #{ <<"processed">> => #{ <<"base">> => #{ <<"floor">> => 5, <<"above">> => [7, 9] } } },
    ?assert(minted(Info, <<"base">>, 5)),
    ?assert(minted(Info, <<"base">>, 7)),
    ?assertNot(minted(Info, <<"base">>, 6)),
    ?assertNot(minted(Info, <<"sui">>, 1)).

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
    ?assertEqual(<<"4">>, maps:get(<<"attestorepoch">>, RootVote)),
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

relay_failure_isolation_test() ->
    St = #{ marker => true },
    ?assertEqual(St, guard(<<"ledger:item">>, St,
        fun() -> error(broken_item) end)).

admin_vote_tags_test() ->
    Root = <<"qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo">>,
    Bridge = <<"0x1111111111111111111111111111111111111111">>,
    SuiBridge = <<"0x034401905bebdf8c04f3cd5f04f442a39372c8dc321c29edfb4f9cb30b23ab96">>,
    Solana = hb_bridge_sign:base58_encode(binary:copy(<<1>>, 32)),
    SolanaToken = hb_bridge_sign:base58_encode(binary:copy(<<2>>, 32)),
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

local_withdrawal_test() ->
    St = #{ namespace => 300 },
    ?assertEqual(<<"7">>, local_withdrawal((300 bsl 32) bor 7, St)),
    ?assertEqual(false, local_withdrawal((299 bsl 32) bor 7, St)).

%% The signature a node posts for a Base withdrawal is the one the contract
%% recovers: the same vector hb_bridge_sign checks against viem.
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

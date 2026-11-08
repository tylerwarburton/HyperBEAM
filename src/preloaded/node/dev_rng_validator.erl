%%% @doc `rng-validator@1.0': this node as one validator of an Any-Honest RNG
%%% beacon (Random-Process v2/ANY_HONEST.md). It replaces the
%%% `anyhonest-validator.mjs' daemon: everything a validator does, except
%%% cracking, runs inside the node.
%%%
%%% A background loop, started by the node's `start' hook, that on every pass
%%% reads the beacon's published `beaconinfo' and:
%%%
%%%   1. makes a fresh puzzle (secret `alpha', commit `c = g^alpha mod N') when
%%%      a round can take it, stores the secret, then sends `Commit';
%%%   2. asks a round to close (`Close') once its commit window has passed;
%%%   3. reveals its secret (`Reveal') once the beacon shows the round revealing;
%%%      a reveal due just before its next commit waits for it (<= 500 ms)
%%%      and goes in the same `Act', so a round costs one message, not two;
%%%   4. says `Ready' once it is healthy and its stake is full (with `top-up',
%%%      bonding from the wallet first), and `Pause' when it should stop;
%%%   5. with `exit' set, sends `Exit', then `Finalize-Exit' until it lands.
%%%
%%% Cracking (solving a withheld puzzle without its secret) is NOT done here:
%%% one crack is minutes of a busy CPU core, and it needs no validator key, so
%%% it runs as its own service (`rng-cracker'), which anyone may run for the
%%% beacon's crack bounty.
%%%
%%% Secrets: every `alpha' is written to `secrets-dir/<round>.json' (owner-only)
%%% BEFORE its commit is sent, so a restart can still reveal. Each send uses a
%%% fresh secret, a resend after a void included. A round's file is removed once
%%% the beacon no longer shows that round.
%%%
%%% Identity: messages are signed as ANS-104 items with the wallet in
%%% `wallet-file' if set (an existing validator keeps its address and bond),
%%% else with the node's own wallet. The beacon trusts nothing this node says
%%% beyond its signature, so the beacon state is read from `node' as published.
%%%
%%% Never resend into a lagging beacon: a message the node scheduled WILL be
%%% computed, so it is resent only once the beacon has computed past its slot
%%% (`now/at-slot') and still shows nothing (2026-11-04: resends flooded a
%%% beacon 14,000 slots behind).
%%%
%%% Configuration, under the node option `rng-validator':
%%%   beacon (process id, required), node (URL the beacon is scheduled on,
%%%   required), wallet-file, secrets-dir (default `rng-validator-secrets'),
%%%   interval (ms between passes, default 100), exit (true to leave),
%%%   top-up (true: refill a slashed bond to `stake', from the wallet; only for a
%%%   registered validator, on the configured beacon (never one reached by
%%%   following a successor), on the token pinned from that beacon, and for a
%%%   `stake' of at most 10^7 RNG; otherwise the `bond' command is logged).
%%% Endpoint, read-only:
%%%   GET /~rng-validator@1.0/status   address, round, phase, last error
-module(dev_rng_validator).
-implements(<<"rng-validator@1.0">>).
-export([device_info/1, start/3, status/3]).
-export([plan/3, alpha/0, commit/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NAME, {<<"rng-validator@1.0">>, loop}).
%% The most RNG an automatic top-up ever covers: a beacon (or a hostile
%% successor) asking for more gets the `bond' command logged instead.
-define(MAX_TOP_UP_STAKE, 10000000).
%% Reveal batching (see `plan'), all on the beacon clock: a due reveal waits
%% for a commit at most HOLD_MS away, at most HOLD_MAX_MS after its round
%% closed, and never within REVEAL_MARGIN_MS of its revealDeadline.
-define(HOLD_MS, 400).
-define(HOLD_MAX_MS, 500).
-define(REVEAL_MARGIN_MS, 10000).

device_info(_) ->
    #{ exports => [<<"start">>, <<"status">>] }.

%%% Endpoints -----------------------------------------------------------------

%% @doc The node `start' hook: spawn the loop once, hand the node message back.
%% Never fails: a validator problem must not stop the node from booting.
%% Configuration comes from the node message only, never from the request.
start(_Base, Req, Opts) ->
    NodeMsg = hb_maps:get(<<"body">>, Req, Opts, Opts),
    try
        LoopOpts = maps:remove(<<"hashpath">>, Opts),
        case config(LoopOpts) of
            #{ <<"beacon">> := _, <<"node">> := _ } = Cfg ->
                case hb_name:lookup(?NAME) of
                    Pid when is_pid(Pid) -> ok;
                    _ ->
                        Pid = spawn(fun() -> init(Cfg, LoopOpts) end),
                        hb_name:register(?NAME, Pid)
                end;
            _ ->
                ?event(rng_validator, {not_configured})
        end
    catch C:R:_ -> ?event(rng_validator, {start_failed, C, R})
    end,
    {ok, #{ <<"body">> => NodeMsg }}.

status(_Base, _Req, _Opts) ->
    case hb_name:lookup(?NAME) of
        Pid when is_pid(Pid) ->
            Ref = make_ref(),
            Pid ! {status, self(), Ref},
            receive {Ref, S} -> {ok, json(S)} after 10000 -> {error, timeout} end;
        _ -> {error, <<"The rng-validator loop is not running on this node">>}
    end.

json(Term) ->
    #{ <<"content-type">> => <<"application/json">>, <<"body">> => hb_json:encode(Term) }.

config(Opts) ->
    case hb_opts:get(<<"rng-validator">>, #{}, Opts) of
        Cfg when is_map(Cfg) -> Cfg;
        _ -> #{}
    end.

%%% The loop ------------------------------------------------------------------

init(Cfg, Opts) ->
    St = initial_state(Cfg, Opts),
    ?event(rng_validator, {started, maps:get(me, St)}),
    self() ! pass,
    loop(St).

initial_state(Cfg, Opts) ->
    Wallet = case maps:get(<<"wallet-file">>, Cfg, undefined) of
        undefined -> hb_opts:get(<<"priv-wallet">>, no_viable_wallet, Opts);
        File -> ar_wallet:load_keyfile(hb_util:list(File))
    end,
    Me = hb_util:human_id(ar_wallet:to_address(Wallet)),
    Dir = hb_util:list(maps:get(<<"secrets-dir">>, Cfg, <<"rng-validator-secrets">>)),
    ok = filelib:ensure_path(Dir),
    _ = file:change_mode(Dir, 8#700),
    SignOpts = Opts#{ <<"priv-wallet">> => Wallet },
    Pusher = spawn_link(fun() -> pusher(Cfg, SignOpts) end),
    #{ cfg => Cfg, opts => SignOpts, me => Me, dir => Dir, pusher => Pusher,
        beacon => followed(Dir, maps:get(<<"beacon">>, Cfg)),
        token => pinned(Dir, maps:get(<<"beacon">>, Cfg)), registry => {undefined, #{}},
        sent => #{}, status => #{ <<"address">> => Me }, last_error => null, last_pass => 0,
        probe => probe0(), health => null,
        backoff => #{ faults => undefined, n => 0, until => 0, ready_since => 0 } }.

loop(St = #{ cfg := Cfg }) ->
    receive
        pass ->
            St1 = try pass(St)
                  catch C:R:T ->
                      ?event(rng_validator, {pass_failed, C, R, lists:sublist(T, 3)}),
                      St#{ last_error => iolist_to_binary(io_lib:format("~0p:~0P", [C, R, 12])) }
                  end,
            erlang:send_after(hb_util:int(maps:get(<<"interval">>, Cfg, 100)), self(), pass),
            loop(St1);
        {status, From, Ref} ->
            #{ status := S, last_error := E, last_pass := P } = St,
            From ! {Ref, S#{ <<"last-error">> => E, <<"last-pass">> => P }},
            loop(St)
    end.

%% One pass: read the beacon, act on what `plan' says, prune old secrets.
pass(StIn = #{ me := Me, sent := Sent0, dir := Dir, probe := Probe0 }) ->
    % Before the state read, so the state is at least this far along.
    % Both reads at once, `at-slot' started first, so the state is about as far
    % along as the slot: one wait per pass instead of two in a row. A slot read
    % a little late only lets a resend come early, and a resend also needs its
    % `Ms' to pass.
    Self = self(),
    Ref = make_ref(),
    T0 = os:system_time(millisecond),
    spawn_link(fun() -> Self ! {Ref, catch read(<<"now/at-slot">>, StIn)} end),
    Info = hb_json:decode(read(<<"now/beaconinfo">>, StIn)),
    AtSlot = receive {Ref, AtBin} -> to_int(AtBin, 0) after 30000 -> 0 end,
    Now = os:system_time(millisecond),
    Open = obj(<<"open">>, Info),
    Cfg = maps:get(cfg, StIn),
    {Mine, Registry} = member(Info, Me, Now, StIn),
    St = (pin_token(Info, StIn))#{ registry => Registry },
    Probe1 = probe(Info, Me, Now, Probe0#{ reads := sample(Now - T0, maps:get(reads, Probe0)) }),
    Offset = offset(Probe1),
    Backoff = backoff(Mine, obj(<<"params">>, Info), Now, maps:get(backoff, St)),
    RoundTrip = round_trip(Probe1, Now),
    Health = health(Info, maps:get(health, St, null)),
    Successor = successor(Info, St),
    {Actions, Oldest} = plan(Info, #{ me => Me, sent => Sent0, at_slot => AtSlot, now => Now,
        beacon_now => Now - Offset,
        exit => maps:get(<<"exit">>, Cfg, false) =:= true, moving => Successor =/= none,
        pause => filelib:is_regular(filename:join(Dir, "pause")),
        top_up => maps:get(<<"top-up">>, Cfg, false) =:= true, mine => Mine,
        followed => maps:get(beacon, St) =/= maps:get(<<"beacon">>, Cfg),
        token => maps:get(token, St, undefined),
        backoff_until => maps:get(until, Backoff), round_trip => RoundTrip }, Dir),
    % A commit's round trip is timed from its first send until it shows.
    {Sent, Pending} = lists:foldl(fun(A = {_, Keys, _}, {S, P}) ->
            T1 = os:system_time(millisecond),
            P1 = lists:foldl(fun({<<"Commit">>, R}, PAcc) -> maps:merge(#{ R => T1 }, PAcc);
                                (_, PAcc) -> PAcc end, P, Keys),
            {act(A, S, St#{ now => Now }), P1}
        end, {Sent0, maps:get(pending, Probe1)}, batch(Actions, Info)),
    prune(Dir, Oldest),
    St1 = follow(Successor, obligations(Info, Me),
        St#{ sent => Sent, probe => Probe1#{ pending := Pending }, backoff => Backoff,
             health => Health }),
    % What was sent is only remembered for ten minutes.
    St1#{ sent => maps:filter(fun(_, {At, _}) -> Now - At < 600000 end, maps:get(sent, St1)),
         last_pass => Now, last_error => null,
         status => #{ <<"address">> => Me, <<"round">> => maps:get(<<"round">>, Info, null),
            <<"open">> => maps:get(<<"round">>, Open, null), <<"phase">> => maps:get(<<"phase">>, Open, null),
            <<"suspended">> => maps:get(<<"suspended">>, Mine, null),
            <<"ready">> => maps:get(<<"ready">>, Mine, null), <<"paused">> => maps:get(<<"paused">>, Mine, null),
            <<"stake">> => maps:get(<<"stake">>, Mine, null), <<"faults">> => maps:get(<<"faults">>, Mine, null),
            <<"exiting">> => maps:get(<<"exiting">>, Mine, null), <<"at-slot">> => AtSlot,
            % Local clock: the beacon's cooldown moved by the clock offset.
            <<"rejoin-at">> => max(to_int(maps:get(<<"rejoinAt">>, Mine, 0), 0) + Offset, maps:get(until, Backoff)),
            <<"round-trip">> => case RoundTrip of infinity -> null; _ -> RoundTrip end,
            <<"clock-offset">> => Offset,
            <<"stake-short">> => stake_short(Mine, Info, stake_required(Info)),
            <<"health">> => maps:get(<<"health">>, Info, null),
            <<"beacon">> => maps:get(beacon, St1) } }.

%% @doc What to send now, from the beacon's `beaconinfo' and what was already
%% sent: `{[{Kind, Round, Data}], OldestRound}'. Pure, except a commit's fresh
%% secret, which is written to `Dir' before the commit is returned.
%%
%% On a beacon with the `ready' feature a validator is ready (given rounds,
%% and slashed for every duty it fails) or not. The device says Ready whenever
%% it can work: a member in good standing, not leaving, not following a
%% successor, and no `pause' file in `secrets-dir'. It says Pause when one of
%% those stops being true, then still does every round it was already given.
%% Ready never goes straight back into the rounds just failed: not before the
%% beacon's cooldown (`rejoinAt'), nor this node's own backoff
%% (`backoff_until'), nor unless the worst recent round trip (`round_trip') is
%% under half the commit window, and at most once every 30 s. No waiver on
%% quorum loss: rejoining at once turned slow commits into 108 slashes per
%% validator (2026-11-06). Ready also needs the full stake: the bond plus
%% unclaimed earnings (the beacon refills from those) must reach `stake'.
%% With `top_up', a registered validator's short bond is topped up to `stake'
%% from the wallet: one `X-Bond' Transfer on the beacon's token, at most once
%% per bond value every 10 minutes. Never for a validator found in neither
%% `beaconinfo' nor `beaconvalidators', on a beacon reached by following a
%% successor (`followed'), on a token other than the one pinned from the
%% configured beacon (`token'), or for a `stake' above 10^7 RNG: then a
%% `Bond-Hint' only logs the `bond' command. Older beacons get the
%% suspend/rejoin behaviour they understand.
%%
%% Beacon times (`rejoinAt', `nextRoundAt', `commitDeadline') are compared with
%% `beacon_now', the local clock moved by the measured offset to the
%% scheduler's; `now' is only ever compared with other local times.
plan(Info, Ctx = #{ me := Me, now := Now }, Dir) ->
    BNow = maps:get(beacon_now, Ctx, Now),
    Open = obj(<<"open">>, Info),
    Params = obj(<<"params">>, Info),
    Mine = case maps:get(mine, Ctx, undefined) of undefined -> mine(Info, Me); M -> M end,
    Round = to_int(maps:get(<<"round">>, Info, 0), 0),
    MinCommits = to_int(maps:get(<<"minCommits">>, Params, 2), 2),
    ReadyMode = lists:member(<<"ready">>, list(<<"features">>, Info)),
    IsActive = fun(V) when ReadyMode ->
                       truthy(<<"ready">>, V) andalso not truthy(<<"exiting">>, V) andalso not truthy(<<"overdue">>, V);
                  (V) ->
                       not truthy(<<"suspended">>, V) andalso not truthy(<<"exiting">>, V) andalso not truthy(<<"overdue">>, V)
               end,
    Active = length([V || V <- list(<<"validators">>, Info), IsActive(V)]),
    Due = fun(Kind, R, Ms) -> due({Kind, R}, Ctx, Ms) end,
    Member = Mine =/= #{},
    Exiting = truthy(<<"exiting">>, Mine),
    Stopping = maps:get(exit, Ctx) orelse maps:get(moving, Ctx, false) orelse maps:get(pause, Ctx, false),
    Available = Member andalso IsActive(Mine),
    Standing =
        case ReadyMode of
            true ->
                Ready = truthy(<<"ready">>, Mine),
                Bond = to_int(maps:get(<<"bond">>, Mine, 0), 0),
                StakeParam = to_int(maps:get(<<"stake">>, Params, 0), 0),
                TopUp = stake_short(Mine, Info, StakeParam),
                Token = maps:get(<<"token">>, Info, null),
                Followed = maps:get(followed, Ctx, false),
                Pinned = maps:get(token, Ctx, undefined),
                WantTopUp = maps:get(top_up, Ctx, false) andalso Member andalso TopUp > 0
                    andalso not Ready andalso not Exiting andalso not Stopping,
                Refusal =
                    if
                        not WantTopUp -> none;
                        Followed =:= true -> <<"successor beacon">>;
                        StakeParam > ?MAX_TOP_UP_STAKE -> <<"stake above 10^7 RNG">>;
                        not is_binary(Token); byte_size(Token) =/= 43; Token =/= Pinned ->
                            <<"token is not the pinned one">>;
                        true -> none
                    end,
                [ {<<"Bond">>, Bond, #{ <<"token">> => Token,
                    <<"quantity">> => integer_to_binary(TopUp * unit(Info)) }}
                || WantTopUp, Refusal =:= none, Due(<<"Bond">>, Bond, 600000) ] ++
                [ {<<"Bond-Hint">>, Bond, #{ <<"reason">> => Refusal, <<"rng">> => TopUp }}
                || WantTopUp, Refusal =/= none, Due(<<"Bond-Hint">>, Bond, 600000) ] ++
                [ {<<"Ready">>, 0, #{}}
                || Member, not Ready, not Exiting, not Stopping, not truthy(<<"overdue">>, Mine),
                   stake_short(Mine, Info, stake_required(Info)) =:= 0,
                   BNow >= to_int(maps:get(<<"rejoinAt">>, Mine, 0), 0),
                   Now >= maps:get(backoff_until, Ctx, 0),
                   maps:get(round_trip, Ctx, infinity)
                       < to_int(maps:get(<<"commitWindowMs">>, Params, 5000), 5000) div 2,
                   Due(<<"Ready">>, 0, 30000) ] ++
                [ {<<"Pause">>, 0, #{}}
                || Member, Ready, not Exiting, Stopping, not maps:get(exit, Ctx), Due(<<"Pause">>, 0, 3000) ];
            false ->
                % Suspended: rejoin on our own while the bond still covers the
                % next slash. The cooldown counts finished rounds, so it is
                % waived while too few validators are active to finish one.
                [ {<<"Rejoin">>, Round, #{}}
                || Member, truthy(<<"suspended">>, Mine), not Exiting, not Stopping,
                   to_int(maps:get(<<"bond">>, Mine, 0), 0) >= to_int(maps:get(<<"slash">>, Params, 0), 0),
                   truthy(<<"idle">>, Mine) orelse Active < MinCommits
                       orelse Round >= to_int(maps:get(<<"suspendedAt">>, Mine, 0), 0)
                           + to_int(maps:get(<<"rejoinAfter">>, Params, 0), 0),
                   Due(<<"Rejoin">>, Round, 3000) ]
        end,
    Leave =
        case maps:get(exit, Ctx) andalso Member of
            false -> [];
            true when not Exiting -> [ {<<"Exit">>, 0, #{}} || Due(<<"Exit">>, 0, 3000) ];
            true -> [ {<<"Finalize-Exit">>, 0, #{}} || Due(<<"Finalize-Exit">>, 0, 30000) ]
        end,
    % A round taking its first commit is opened only by an available validator
    % that is not stopping. A round already open is owed by every validator in
    % its set, stopping or not. The cadence and heartbeat decide when to open.
    OpenRound = to_int(maps:get(<<"round">>, Open, 0), 0),
    Phase = maps:get(<<"phase">>, Open, null),
    InSet = lists:member(Me, list(<<"active">>, Open)),
    % CommitIn: beacon ms until this validator may commit to the open round
    % (0: now; the atom `infinity' compares above every number).
    CanOpen = Available andalso not Stopping andalso Active >= MinCommits
        % Request-driven (reserve spent): only rounds with requests.
        andalso (to_int(maps:get(<<"pending">>, Open, 0), 0) > 0
                 orelse (truthy(<<"heartbeat">>, Params)
                         andalso maps:get(<<"mode">>, Info, null) =/= <<"request-driven">>)),
    CommitIn =
        case Phase of
            <<"collecting">> when CanOpen -> max(0, to_int(maps:get(<<"nextRoundAt">>, Info, 0), 0) - BNow);
            <<"commit">> when ReadyMode, InSet -> 0;
            <<"commit">> when not ReadyMode -> case Available andalso not Stopping of true -> 0; false -> infinity end;
            _ -> infinity
        end,
    CommitOwed = not maps:is_key(Me, obj(<<"committed">>, Open)) andalso Due(<<"Commit">>, OpenRound, 1000),
    Commit =
        case CommitIn =:= 0 andalso CommitOwed of
            false -> [];
            true ->
                % A fresh secret on every send, a resend after a void included.
                A = alpha(),
                C = commit(A),
                save_secret(Dir, OpenRound, C, A),
                [{<<"Commit">>, OpenRound, #{ <<"round">> => OpenRound, <<"c">> => C }}]
        end,
    % A reveal due while our next commit is HOLD_MS or less away waits for it
    % and goes in the same Act: one message a round instead of two (2026-11-07:
    % ~6 slots a round at ~21 KB each). `hold_ms' 0 never holds.
    Hold = lists:member(<<"act">>, list(<<"features">>, Info)) andalso Commit =:= [] andalso CommitOwed
        andalso CommitIn =< maps:get(hold_ms, Ctx, ?HOLD_MS),
    RevealWindow = to_int(maps:get(<<"revealWindowMs">>, Params, 30000), 30000),
    Held = fun(X) ->
        RD = to_int(maps:get(<<"revealDeadline">>, X, 0), 0),
        Hold andalso RD - BNow > ?REVEAL_MARGIN_MS andalso BNow - (RD - RevealWindow) < ?HOLD_MAX_MS
    end,
    % Rounds overlap: the open round takes commits while older ones reveal.
    Rounds = list(<<"revealing">>, Info) ++ [Open],
    PerRound = lists:flatmap(fun(X) ->
        R = to_int(maps:get(<<"round">>, X, 0), 0),
        XPhase = maps:get(<<"phase">>, X, null),
        Deadline = to_int(maps:get(<<"commitDeadline">>, X, 0), 0),
        Mine0 = maps:get(Me, obj(<<"committed">>, X), undefined),
        % Reveal once commits are closed: the round's whole active set has
        % committed, or the beacon clock is past commitDeadline. The beacon
        % closes the commit phase the moment the set is complete (closeIfDue),
        % after which nobody can commit, so revealing then cannot be
        % front-run. The phase and set come from this node itself, so a third
        % party cannot spoof them. Waiting for the deadline as well held every
        % round to the full commit window (~0.6 rounds/s, 2026-11-07).
        Set = list(<<"active">>, X),
        Done = obj(<<"committed">>, X),
        Complete = Set =/= [] andalso lists:all(fun(V) -> maps:is_key(V, Done) end, Set),
        % Reveal only once the beacon shows the round revealing. An older beacon
        % is asked to close a round its window has passed; a `ready' beacon
        % closes rounds by itself on every message, and a Close there is only a
        % message in the way of the next commit (three a round, 2026-11-04).
        [ {<<"Close">>, R, #{ <<"round">> => R }}
        || not ReadyMode, XPhase =:= <<"commit">>, Deadline > 0, BNow >= Deadline, Due(<<"Close">>, R, 2000) ] ++
        [ {<<"Reveal">>, R, #{ <<"round">> => R, <<"alpha">> => A }}
        || XPhase =:= <<"reveal">>, Complete orelse BNow >= Deadline, is_binary(Mine0),
           not lists:member(Me, list(<<"revealed">>, X)),
           Due(<<"Reveal">>, R, 3000), not Held(X),
           A <- [secret_for(Mine0, secrets(Dir, R))], A =/= missing ]
    end, Rounds),
    Oldest = lists:min([to_int(maps:get(<<"round">>, X, 0), 0) || X <- Rounds]),
    {Standing ++ Leave ++ Commit ++ PerRound, Oldest}.

%% @doc The secret for a commitment the beacon shows whole, or as the prefix a
%% newer beacon publishes until the round is late.
secret_for(Shown, Secrets) ->
    case maps:get(Shown, Secrets, missing) of
        missing ->
            Len = byte_size(Shown),
            case [A || {C, A} <- maps:to_list(Secrets), byte_size(C) > Len, binary:part(C, 0, Len) =:= Shown] of
                [A | _] -> A;
                [] -> missing
            end;
        A -> A
    end.

%% @doc Rounds this validator still owes something: a reveal for a round it
%% committed to, or a commit to the open round whose set it is in.
obligations(Info, Me) ->
    Open = obj(<<"open">>, Info),
    Owed = [R || X <- list(<<"revealing">>, Info) ++ [Open],
                 maps:is_key(Me, obj(<<"committed">>, X)),
                 not lists:member(Me, list(<<"revealed">>, X)),
                 R <- [to_int(maps:get(<<"round">>, X, 0), 0)]],
    InOpenSet = maps:get(<<"phase">>, Open, null) =:= <<"commit">>
        andalso lists:member(Me, list(<<"active">>, Open))
        andalso not maps:is_key(Me, obj(<<"committed">>, Open)),
    Owed ++ [to_int(maps:get(<<"round">>, Open, 0), 0) || InOpenSet].

%% Sent: #{ {Kind, Round} => {At, Slot} }. Not sent yet, or the beacon has
%% computed past the slot it was given and `Ms' have passed with no effect.
due(Key, #{ sent := Sent, at_slot := AtSlot, now := Now }, Ms) ->
    case maps:get(Key, Sent, undefined) of
        undefined -> true;
        {At, Slot} -> Slot =< AtSlot andalso Now - At > Ms
    end.

%% @doc The messages to send for the actions `plan' returned, as
%% `{Action, Keys, Data}' where `Keys' are the `{Kind, Round}' it settles. On a
%% beacon with the `act' feature, the commit and every reveal go as one `Act'
%% message, halving what a round costs the beacon.
batch(Actions, Info) ->
    One = fun({K, R, D}) -> {K, [{K, R}], D} end,
    IsStep = fun({K, _, _}) -> K =:= <<"Commit">> orelse K =:= <<"Reveal">> end,
    case lists:member(<<"act">>, list(<<"features">>, Info)) of
        false -> lists:map(One, Actions);
        true ->
            {Steps, Rest} = lists:partition(IsStep, Actions),
            lists:map(One, Rest) ++
                case Steps of
                    [] -> [];
                    [Step] -> [One(Step)];
                    _ ->
                        Commit = [D || {<<"Commit">>, _, D} <- Steps],
                        Data = (maps:from_list([{<<"commit">>, C} || C <- Commit]))#{
                            <<"reveals">> => [D || {<<"Reveal">>, _, D} <- Steps] },
                        [{<<"Act">>, [{K, R} || {K, R, _} <- Steps], Data}]
                end
    end.

%% A top-up refused (see `plan'): say what to run instead, sending nothing.
act({<<"Bond-Hint">>, Keys, #{ <<"reason">> := Why, <<"rng">> := Rng }}, Sent, #{ now := Now }) ->
    ?event(rng_validator, {not_topping_up, Why, {run, iolist_to_binary(["bond ", integer_to_binary(Rng)])}}),
    maps:merge(Sent, maps:from_list([{K, {Now, -1}} || K <- Keys]));
%% A bond top-up is a Transfer on the token, to the beacon, tagged `X-Bond'.
%% The token's slot says nothing about the beacon's, so its resend is timed
%% only (slot -1).
act({<<"Bond">>, Keys, #{ <<"token">> := Token, <<"quantity">> := Q }}, Sent,
        St = #{ now := Now, beacon := Beacon }) ->
    Slot = try schedule(Token, #{ <<"action">> => <<"Transfer">>, <<"recipient">> => Beacon,
                   <<"quantity">> => Q, <<"x-bond">> => <<"true">> }, St)
           catch C:R -> ?event(rng_validator, {send_failed, <<"Bond">>, C, R}), -1
           end,
    ?event(rng_validator, {sent, <<"Bond">>, Q, Slot}),
    case {Slot >= 0, maps:find(pusher, St)} of
        {true, {ok, Pusher}} -> Pusher ! {push, Token, Slot};
        _ -> ok
    end,
    maps:merge(Sent, maps:from_list([{K, {Now, -1}} || K <- Keys]));
act({Action, Keys, Data}, Sent, St = #{ now := Now, beacon := Beacon }) ->
    Slot = try send(Action, Data, St)
           catch C:R -> ?event(rng_validator, {send_failed, Action, C, R}), -1
           end,
    ?event(rng_validator, {sent, Action, Keys, Slot}),
    case {Slot >= 0, maps:find(pusher, St)} of
        {true, {ok, Pusher}} -> Pusher ! {push, Beacon, Slot};
        _ -> ok
    end,
    maps:merge(Sent, maps:from_list([{K, {Now, Slot}} || K <- Keys])).

%%% Rejoin health -------------------------------------------------------------

%% Health is measured on the local clock only. Comparing local time with a
%% beacon timestamp (`firstCommitAt') mixed two clocks: a skewed node looked
%% healthy, or never did. Samples:
%%   reads    every beaconinfo read, local start to local end (ms);
%%   commits  `{LocalAt, Ms}': own commit sent (local) until a read shows it
%%            (local), or until it is given up after 30 s (a failure);
%%   pending  `#{Round => LocalSentAt}', commits not yet seen;
%%   offsets  local read time minus the beacon's `at' (its slot's scheduler
%%            time), once per new `at'. The smallest is the clock offset.
probe0() -> #{ reads => [], commits => [], pending => #{}, offsets => [], last_at => undefined }.

%% The last 20 samples (newest first).
sample(Ms, List) -> lists:sublist([Ms | List], 20).

probe(Info, Me, Now, Probe = #{ commits := Commits, pending := Pending, offsets := Offsets, last_at := LastAt }) ->
    Shown = maps:from_list([{to_int(maps:get(<<"round">>, X, 0), 0), maps:is_key(Me, obj(<<"committed">>, X))}
        || X <- list(<<"revealing">>, Info) ++ [obj(<<"open">>, Info)]]),
    {Done, Left} = lists:partition(fun({R, At}) ->
            maps:get(R, Shown, false) orelse Now - At > 30000
        end, maps:to_list(Pending)),
    At = to_int(maps:get(<<"at">>, Info, null), undefined),
    Probe#{ commits := lists:sublist([{Now, Now - T} || {_, T} <- Done] ++ Commits, 20),
            pending := maps:from_list(Left),
            offsets := case is_integer(At) andalso At =/= LastAt of
                           true -> lists:sublist([Now - At | Offsets], 50);
                           false -> Offsets
                       end,
            last_at := At }.

%% @doc Local clock minus the scheduler's: the smallest `Now - at' seen, as
%% its least delayed read. 0 on a beacon that publishes no `at'.
offset(#{ offsets := [] }) -> 0;
offset(#{ offsets := Offsets }) -> lists:min(Offsets).

%% @doc The worst commit round trip of the last 10 minutes; with none, twice
%% the worst read (a send costs about a read). `infinity' until 20 reads.
round_trip(#{ reads := Reads, commits := Commits }, Now) when length(Reads) >= 20 ->
    case [Ms || {At, Ms} <- Commits, Now - At < 600000] of
        [] -> 2 * lists:max(Reads);
        Fresh -> max(lists:max(Reads), lists:max(Fresh))
    end;
round_trip(_, _) -> infinity.

%% The beacon's own `health' (status ok, degraded or halted), logged on change.
health(Info, Prev) ->
    H = maps:get(<<"health">>, Info, null),
    S = case H of #{ <<"status">> := S0 } -> S0; _ -> null end,
    case S =/= Prev andalso S =/= null andalso S =/= <<"ok">> of
        true -> ?event(rng_validator, {beacon_health, H});
        false -> ok
    end,
    S.

%%% Stake ---------------------------------------------------------------------

%% What Ready needs: `stake' (v3: the full stake, every time), or the older
%% beacon's `rejoinBond' while it publishes one. 0 when neither is published.
stake_required(Info) ->
    Params = obj(<<"params">>, Info),
    Stake = to_int(maps:get(<<"stake">>, Params, 0), 0),
    to_int(maps:get(<<"rejoinBond">>, Params, Stake), Stake).

%% @doc RNG still missing for `Required': the bond plus the unclaimed earnings
%% the beacon refills from (`earnedAtoms', when published).
stake_short(Mine, Info, Required) ->
    Bond = to_int(maps:get(<<"bond">>, Mine, 0), 0),
    Earned = to_int(maps:get(<<"earnedAtoms">>, Mine, 0), 0) div unit(Info),
    max(0, Required - Bond - Earned).

%% Atoms per RNG.
unit(Info) -> max(1, to_int(maps:get(<<"unit">>, Info, 1000000), 1000000)).

%% @doc This node's own backoff after a fault, on top of the beacon's cooldown:
%% `rejoinBaseMs' doubled per incident, up to `rejoinCapMs'. A fault while
%% ready starts an incident; later faults of the same incident (rounds already
%% held) do not escalate it. Ready for a cap or longer starts the count over.
backoff(Mine, _Params, _Now, B) when Mine =:= #{} -> B;
backoff(Mine, Params, Now, B = #{ faults := Prev, n := N0, until := Until0, ready_since := Since0 }) ->
    Faults = to_int(maps:get(<<"faults">>, Mine, 0), 0),
    Base = to_int(maps:get(<<"rejoinBaseMs">>, Params, 60000), 60000),
    Cap = to_int(maps:get(<<"rejoinCapMs">>, Params, 3600000), 3600000),
    {N, Until} =
        case is_integer(Prev) andalso Faults > Prev andalso Since0 > 0 of
            true ->
                N1 = case Now - Since0 < Cap of true -> N0 + 1; false -> 1 end,
                ?event(rng_validator, {fault, Faults, {backing_off, N1}}),
                {N1, Now + min(Cap, Base bsl min(N1 - 1, 32))};
            false -> {N0, Until0}
        end,
    Ready = truthy(<<"ready">>, Mine),
    Since = if not Ready -> 0; Since0 =:= 0 -> Now; true -> Since0 end,
    B#{ faults := Faults, n := N, until := Until, ready_since := Since }.

%%% Following a successor -----------------------------------------------------

%% @doc The beacon to move to: `{ok, Id}' when this one names a successor that
%% names it back as its predecessor, else `none'. A beacon that publishes
%% `successorAck' (v3) must also show the successor's `Successor-Ack'. Until
%% the move, no new commits: only what this beacon is still owed is revealed.
successor(Info, St = #{ beacon := Beacon }) ->
    case named_successor(Info, Beacon) of
        none -> none;
        Next ->
            try hb_json:decode(read(<<"now/beaconinfo">>, St#{ beacon => Next })) of
                #{ <<"predecessor">> := Beacon } -> {ok, Next};
                _ -> none
            catch _:_ -> none
            end
    end.

named_successor(Info, Beacon) ->
    Acked = not maps:is_key(<<"successorAck">>, Info) orelse truthy(<<"successorAck">>, Info),
    case maps:get(<<"successor">>, Info, <<>>) of
        Next when Acked, is_binary(Next), byte_size(Next) =:= 43, Next =/= Beacon -> Next;
        _ -> none
    end.

%% Move once nothing is owed here, and remember where we are.
follow({ok, Next}, Owed, St = #{ dir := Dir, cfg := Cfg }) ->
    case Owed of
        [] ->
            ?event(rng_validator, {following, Next}),
            File = filename:join(Dir, "beacon.json"),
            Pinned = case maps:get(token, St, undefined) of T when is_binary(T) -> T; _ -> null end,
            ok = file:write_file(File ++ ".tmp", hb_json:encode(#{
                <<"configured">> => maps:get(<<"beacon">>, Cfg), <<"beacon">> => Next, <<"token">> => Pinned })),
            ok = file:rename(File ++ ".tmp", File),
            % Its rounds are new: commits pending here would only time out.
            St#{ beacon => Next, sent => #{}, probe => probe0(), registry => {undefined, #{}} };
        _ -> St
    end;
follow(none, _Owed, St) -> St.

%% The beacon this validator moved to, while the configured one is unchanged;
%% a different configured beacon starts the walk again from it.
followed(Dir, Configured) ->
    case file:read_file(filename:join(Dir, "beacon.json")) of
        {ok, Bin} ->
            case catch hb_json:decode(Bin) of
                #{ <<"configured">> := Configured, <<"beacon">> := B } when is_binary(B) -> B;
                _ -> Configured
            end;
        _ -> Configured
    end.

%% The token pinned from the configured beacon, saved with the move.
pinned(Dir, Configured) ->
    case file:read_file(filename:join(Dir, "beacon.json")) of
        {ok, Bin} ->
            case catch hb_json:decode(Bin) of
                #{ <<"configured">> := Configured, <<"token">> := T } when is_binary(T), byte_size(T) =:= 43 -> T;
                _ -> undefined
            end;
        _ -> undefined
    end.

%% @doc Pin the token the first time the CONFIGURED beacon names one; a
%% successor's token never replaces it.
pin_token(Info, St = #{ cfg := Cfg, beacon := Beacon }) ->
    case {maps:get(token, St, undefined), Beacon =:= maps:get(<<"beacon">>, Cfg), maps:get(<<"token">>, Info, null)} of
        {undefined, true, T} when is_binary(T), byte_size(T) =:= 43 -> St#{ token => T };
        _ -> St
    end.

%% @doc This validator's entry: from `beaconinfo' (at most 128 validators),
%% else from the full registry `beaconvalidators', read at most every 30 s
%% while not listed. `{Entry, Registry}', Entry `#{}' when found in neither.
member(Info, Me, Now, St) ->
    case mine(Info, Me) of
        M when map_size(M) > 0 -> {M, maps:get(registry, St, {undefined, #{}})};
        _ ->
            Reg = case maps:get(registry, St, {undefined, #{}}) of
                {At, _} when At =:= undefined; Now - At > 30000 ->
                    {Now, registry_entry(catch hb_json:decode(read(<<"now/beaconvalidators">>, St)), Me)};
                Cached -> Cached
            end,
            {element(2, Reg), Reg}
    end.

%% @doc The registry, in whichever shape `beaconvalidators' holds it: a list,
%% `{validators: [...]}', or `{address: entry}' (also as JSON text).
registry_entry(Bin, Me) when is_binary(Bin) ->
    case catch hb_json:decode(Bin) of
        V when is_map(V); is_list(V) -> registry_entry(V, Me);
        _ -> #{}
    end;
registry_entry(List, Me) when is_list(List) ->
    case [V || V <- List, is_map(V), maps:get(<<"address">>, V, null) =:= Me] of
        [V | _] -> V;
        [] -> #{}
    end;
registry_entry(#{ <<"validators">> := List }, Me) when is_list(List) -> registry_entry(List, Me);
registry_entry(Map, Me) when is_map(Map) ->
    case maps:get(Me, Map, undefined) of
        V when is_map(V) -> V#{ <<"address">> => Me };
        _ -> #{}
    end;
registry_entry(_, _) -> #{}.

%% Scheduling a message does not compute it: the node's follower waits
%% (`process-compute-on-schedule-ms') to fold slots, which left the beacon 6-8
%% messages behind. `push&slot' computes the slot now and delivers its outbox,
%% as the old daemon did. One push at a time; queued requests fold into the
%% highest slot, which computes every one before it. Never retried.
pusher(Cfg, Opts) ->
    receive
        {push, Beacon, Slot0} ->
            Slot = newest(Beacon, Slot0),
            % Nothing a push returns or throws may end the pusher.
            Ok = try hb_http:get(maps:get(<<"node">>, Cfg),
                    <<"/", Beacon/binary, "~process@1.0/push&slot=", (integer_to_binary(Slot))/binary>>, Opts) of
                     {ok, _} -> ok;
                     _ -> error
                 catch _:_ -> error
                 end,
            ?event(rng_validator, {pushed, Slot, Ok}),
            pusher(Cfg, Opts)
    end.

newest(Beacon, Slot) ->
    receive {push, Beacon, S} -> newest(Beacon, max(Slot, S)) after 0 -> Slot end.

%% Schedule one signed message on the beacon; its slot, or -1 if unknown.
send(Action, Data, St = #{ beacon := Beacon }) ->
    schedule(Beacon, #{ <<"action">> => Action, <<"data">> => hb_json:encode(Data) }, St).

%% Schedule signed `Tags' on `Target' (the beacon, or its token: one scheduler).
schedule(Target, Tags, #{ cfg := Cfg, opts := Opts }) ->
    Msg = hb_message:commit(Tags#{ <<"target">> => Target, <<"type">> => <<"Message">> },
        Opts, #{ <<"device">> => <<"ans104@1.0">>, <<"bundle">> => true }),
    {ok, Res} = hb_http:post(maps:get(<<"node">>, Cfg),
        Msg#{ <<"path">> => <<"/", Target/binary, "~process@1.0/schedule">> },
        Opts#{ <<"http-codec">> => <<"ans104@1.0">> }),
    to_int(hb_maps:get(<<"slot">>, Res, -1, Opts), -1).

%% A published key of the beacon, as a binary.
read(Key, #{ cfg := Cfg, opts := Opts, beacon := Beacon }) ->
    {ok, Res} = hb_http:get(maps:get(<<"node">>, Cfg),
        <<"/", Beacon/binary, "~process@1.0/", Key/binary>>, Opts),
    case Res of
        Bin when is_binary(Bin) -> Bin;
        Int when is_integer(Int) -> integer_to_binary(Int);
        Map when is_map(Map) -> hb_util:bin(hb_maps:get(<<"body">>, Map, <<>>, Opts))
    end.

%%% Puzzles and secrets -------------------------------------------------------

%% @doc A fresh 256-bit secret, as canonical hex. Never reuse one.
alpha() -> to_hex(binary:decode_unsigned(crypto:strong_rand_bytes(32))).

%% @doc The commit `c = g^alpha mod N' in `rng@1.0''s fixed group (hex in and out).
commit(AlphaHex) ->
    {ok, #{ <<"g">> := G, <<"n">> := N }} = rng(<<"generator">>, #{}),
    to_hex(binary:decode_unsigned(crypto:mod_pow(
        binary_to_integer(G, 16), binary_to_integer(AlphaHex, 16), binary_to_integer(N, 16)))).

to_hex(N) -> string:lowercase(integer_to_binary(N, 16)).

%% A key of `rng@1.0', called by name (preloaded devices are not modules).
rng(Key, Req) -> hb_ao:raw(<<"rng@1.0">>, Key, #{}, Req, #{}).

secret_file(Dir, Round) -> filename:join(Dir, integer_to_list(Round) ++ ".json").

secrets(Dir, Round) ->
    case file:read_file(secret_file(Dir, Round)) of
        {ok, Bin} -> hb_json:decode(Bin);
        _ -> #{}
    end.

%% Every secret sent for a round, `{c: alpha}', owner-only, replaced atomically.
save_secret(Dir, Round, C, A) ->
    File = secret_file(Dir, Round),
    Tmp = File ++ ".tmp",
    ok = file:write_file(Tmp, <<>>),
    ok = file:change_mode(Tmp, 8#600),
    ok = file:write_file(Tmp, hb_json:encode((secrets(Dir, Round))#{ C => A })),
    ok = file:rename(Tmp, File).

%% A secret is only needed until its round is done.
prune(Dir, Oldest) ->
    {ok, Files} = file:list_dir(Dir),
    [file:delete(filename:join(Dir, F)) || F <- Files,
        case string:to_integer(F) of {R, ".json"} -> R < Oldest; _ -> false end].

%%% JSON helpers --------------------------------------------------------------

obj(Key, M) -> case maps:get(Key, M, #{}) of V when is_map(V) -> V; _ -> #{} end.
list(Key, M) -> case maps:get(Key, M, []) of V when is_list(V) -> V; _ -> [] end.
mine(Info, Me) ->
    case [V || V <- list(<<"validators">>, Info), maps:get(<<"address">>, V, null) =:= Me] of
        [V | _] -> V;
        [] -> #{}
    end.
truthy(Key, M) ->
    case maps:get(Key, M, false) of
        V when V =:= false; V =:= null; V =:= 0; V =:= <<"0">>; V =:= <<>> -> false;
        _ -> true
    end.
to_int(V, _) when is_integer(V) -> V;
to_int(V, _) when is_float(V) -> trunc(V);
to_int(V, D) when is_binary(V) ->
    case string:to_integer(string:trim(V)) of {I, _} when is_integer(I) -> I; _ -> D end;
to_int(_, D) -> D.

%%% Tests ---------------------------------------------------------------------

-define(ME, <<"me000000000000000000000000000000000000000000">>).

tmp_dir() ->
    Dir = filename:join("/tmp", "rng-validator-" ++ integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_path(Dir),
    Dir.

info(Open, Extra) ->
    maps:merge(#{
        <<"round">> => 7, <<"nextRoundAt">> => 0,
        <<"params">> => #{ <<"minCommits">> => 2, <<"heartbeat">> => 1, <<"slash">> => 1000,
            <<"rejoinAfter">> => 600 },
        <<"validators">> => [#{ <<"address">> => ?ME, <<"bond">> => 5000 },
            #{ <<"address">> => <<"other">> }],
        <<"open">> => Open, <<"revealing">> => []
    }, Extra).

ctx(Sent) -> #{ me => ?ME, sent => Sent, at_slot => 10, now => 100000, exit => false, round_trip => 100 }.
kinds(Actions) -> [K || {K, _, _} <- Actions].

commit_matches_device_test() ->
    A = alpha(),
    C = commit(A),
    {ok, #{ <<"g">> := G }} = rng(<<"generator">>, #{}),
    ?assertEqual(G, commit(<<"1">>)),
    {ok, R} = rng(<<"verify-reveal">>, #{ <<"c">> => C, <<"alpha">> => A, <<"h">> => G }),
    ?assertEqual(true, maps:get(<<"valid">>, R)).

commit_stores_secret_first_test() ->
    Dir = tmp_dir(),
    {[{<<"Commit">>, 3, #{ <<"c">> := C }}], 3} =
        plan(info(#{ <<"round">> => 3, <<"phase">> => <<"collecting">> }, #{}), ctx(#{}), Dir),
    ?assertMatch(#{ C := _ }, secrets(Dir, 3)),
    {ok, Info} = file:read_file_info(secret_file(Dir, 3)),
    ?assertEqual(8#600, element(8, Info) band 8#777).

no_commit_without_quorum_test() ->
    Info = info(#{ <<"round">> => 3, <<"phase">> => <<"collecting">> },
        #{ <<"validators">> => [#{ <<"address">> => ?ME }, #{ <<"address">> => <<"o">>, <<"suspended">> => true }] }),
    ?assertEqual({[], 3}, plan(Info, ctx(#{}), tmp_dir())).

never_resend_before_slot_computed_test() ->
    Open = #{ <<"round">> => 3, <<"phase">> => <<"collecting">> },
    % Sent long ago at slot 11; the beacon has computed only to 10: wait.
    ?assertEqual({[], 3}, plan(info(Open, #{}), ctx(#{ {<<"Commit">>, 3} => {0, 11} }), tmp_dir())),
    % Computed past it and still nothing: resend, with a fresh secret.
    {A, _} = plan(info(Open, #{}), ctx(#{ {<<"Commit">>, 3} => {0, 9} }), tmp_dir()),
    ?assertEqual([<<"Commit">>], kinds(A)).

reveal_from_stored_secret_test() ->
    Dir = tmp_dir(),
    save_secret(Dir, 3, <<"c1">>, <<"a1">>),
    R = #{ <<"round">> => 3, <<"phase">> => <<"reveal">>, <<"committed">> => #{ ?ME => <<"c1">> },
        <<"revealed">> => [] },
    Open = #{ <<"round">> => 4, <<"phase">> => <<"commit">>, <<"committed">> => #{ ?ME => <<"x">> } },
    {A, 3} = plan(info(Open, #{ <<"revealing">> => [R] }), ctx(#{}), Dir),
    ?assertEqual([{<<"Reveal">>, 3, #{ <<"round">> => 3, <<"alpha">> => <<"a1">> }}], A),
    % Already revealed: nothing.
    {A2, 3} = plan(info(Open, #{ <<"revealing">> => [R#{ <<"revealed">> => [?ME] }] }), ctx(#{}), Dir),
    ?assertEqual([], A2).

close_after_deadline_test() ->
    Open = #{ <<"round">> => 4, <<"phase">> => <<"commit">>, <<"commitDeadline">> => 50,
        <<"committed">> => #{ ?ME => <<"x">> } },
    {A, 4} = plan(info(Open, #{}), ctx(#{}), tmp_dir()),
    ?assertEqual([<<"Close">>], kinds(A)),
    % A `ready' beacon closes rounds itself: no Close.
    Me = #{ <<"address">> => ?ME, <<"ready">> => true },
    {B, 4} = plan(ready_info(Open#{ <<"active">> => [?ME] }, Me, #{}), ctx(#{}), tmp_dir()),
    ?assertEqual([], kinds(B)).

rejoin_when_suspended_test() ->
    Info = info(#{ <<"round">> => 3, <<"phase">> => <<"collecting">> }, #{
        <<"validators">> => [#{ <<"address">> => ?ME, <<"suspended">> => true, <<"idle">> => true,
            <<"bond">> => 5000 }, #{ <<"address">> => <<"o">> }, #{ <<"address">> => <<"p">> }] }),
    {A, _} = plan(Info, ctx(#{}), tmp_dir()),
    ?assertEqual([<<"Rejoin">>], kinds(A)).

exit_then_finalize_test() ->
    Open = #{ <<"round">> => 3, <<"phase">> => <<"collecting">> },
    {A, _} = plan(info(Open, #{}), (ctx(#{}))#{ exit => true }, tmp_dir()),
    ?assertEqual([<<"Exit">>], kinds(A)),
    Exiting = info(Open, #{ <<"validators">> => [#{ <<"address">> => ?ME, <<"exiting">> => true }] }),
    {B, _} = plan(Exiting, (ctx(#{}))#{ exit => true }, tmp_dir()),
    ?assertEqual([<<"Finalize-Exit">>], kinds(B)).

pushes_fold_into_newest_slot_test() ->
    [self() ! {push, <<"b">>, S} || S <- [12, 15, 13]],
    self() ! {push, <<"other">>, 99},
    ?assertEqual(15, newest(<<"b">>, 10)),
    ?assertEqual(4, newest(<<"b">>, 4)),
    receive {push, <<"other">>, 99} -> ok after 0 -> ?assert(false) end.

batch_into_act_test() ->
    A = [{<<"Rejoin">>, 7, #{}}, {<<"Commit">>, 9, #{ <<"c">> => <<"c9">> }},
         {<<"Reveal">>, 8, #{ <<"alpha">> => <<"a8">> }}],
    ?assertEqual([{<<"Rejoin">>, [{<<"Rejoin">>, 7}], #{}},
        {<<"Act">>, [{<<"Commit">>, 9}, {<<"Reveal">>, 8}],
            #{ <<"commit">> => #{ <<"c">> => <<"c9">> }, <<"reveals">> => [#{ <<"alpha">> => <<"a8">> }] }}],
        batch(A, #{ <<"features">> => [<<"act">>] })),
    % Without the feature, or with one step, nothing is merged.
    ?assertEqual(3, length(batch(A, #{}))),
    ?assertEqual([{<<"Commit">>, [{<<"Commit">>, 9}], #{ <<"c">> => <<"c9">> }}],
        batch([lists:nth(2, A)], #{ <<"features">> => [<<"act">>] })).

no_commit_while_moving_test() ->
    Open = #{ <<"round">> => 3, <<"phase">> => <<"collecting">> },
    ?assertEqual({[], 3}, plan(info(Open, #{}), (ctx(#{}))#{ moving => true }, tmp_dir())).

followed_restarts_when_config_changes_test() ->
    Dir = tmp_dir(),
    ok = file:write_file(filename:join(Dir, "beacon.json"),
        hb_json:encode(#{ <<"configured">> => <<"old">>, <<"beacon">> => <<"new">> })),
    ?assertEqual(<<"new">>, followed(Dir, <<"old">>)),
    ?assertEqual(<<"other">>, followed(Dir, <<"other">>)).

ready_info(Open, Me, Extra) ->
    maps:merge(#{
        <<"round">> => 7, <<"nextRoundAt">> => 0, <<"features">> => [<<"act">>, <<"ready">>],
        <<"params">> => #{ <<"minCommits">> => 2, <<"heartbeat">> => 1 },
        <<"validators">> => [Me, #{ <<"address">> => <<"other">>, <<"ready">> => true }],
        <<"open">> => Open, <<"revealing">> => []
    }, Extra).

ready_when_it_can_work_test() ->
    Open = #{ <<"round">> => 3, <<"phase">> => <<"collecting">> },
    NotReady = #{ <<"address">> => ?ME, <<"ready">> => false },
    {A, _} = plan(ready_info(Open, NotReady, #{}), ctx(#{}), tmp_dir()),
    ?assertEqual([<<"Ready">>], kinds(A)),
    % Owing a late output: not in good standing, so no Ready (and no commit).
    {B, _} = plan(ready_info(Open, NotReady#{ <<"overdue">> => true }, #{}), ctx(#{}), tmp_dir()),
    ?assertEqual([], kinds(B)),
    % A pause file: no Ready.
    {C, _} = plan(ready_info(Open, NotReady, #{}), (ctx(#{}))#{ pause => true }, tmp_dir()),
    ?assertEqual([], kinds(C)).

ready_waits_out_cooldown_backoff_and_health_test() ->
    Open = #{ <<"round">> => 3, <<"phase">> => <<"collecting">> },
    NotReady = #{ <<"address">> => ?ME, <<"ready">> => false },
    Plan = fun(Me, Ctx) ->
        kinds(element(1, plan(ready_info(Open, Me, #{}), maps:merge(ctx(#{}), Ctx), tmp_dir())))
    end,
    % The beacon's cooldown: not before `rejoinAt'.
    ?assertEqual([], Plan(NotReady#{ <<"rejoinAt">> => 100001 }, #{})),
    ?assertEqual([<<"Ready">>], Plan(NotReady#{ <<"rejoinAt">> => 100000 }, #{})),
    % This node's own backoff.
    ?assertEqual([], Plan(NotReady, #{ backoff_until => 100001 })),
    % Round trip under half the commit window (5000 by default), and known.
    ?assertEqual([], Plan(NotReady, #{ round_trip => 2500 })),
    ?assertEqual([<<"Ready">>], Plan(NotReady, #{ round_trip => 2499 })),
    ?assertEqual([], Plan(NotReady, #{ round_trip => infinity })),
    % At most once every 30 s.
    ?assertEqual([], Plan(NotReady, #{ sent => #{ {<<"Ready">>, 0} => {100000 - 30000, 1} } })),
    ?assertEqual([<<"Ready">>], Plan(NotReady, #{ sent => #{ {<<"Ready">>, 0} => {100000 - 30001, 1} } })).

round_trip_needs_full_sample_test() ->
    P0 = probe0(),
    ?assertEqual(infinity, round_trip(P0#{ reads := lists:duplicate(19, 10) }, 0)),
    % No commit seen yet: twice the worst read.
    Reads = [30 | lists:duplicate(19, 10)],
    ?assertEqual(60, round_trip(P0#{ reads := Reads }, 0)),
    % A recent commit round trip counts; one older than 10 minutes does not.
    ?assertEqual(700, round_trip(P0#{ reads := Reads, commits := [{1000, 700}] }, 1000 + 599999)),
    ?assertEqual(60, round_trip(P0#{ reads := Reads, commits := [{1000, 700}] }, 1000 + 600000)),
    ?assertEqual(20, length(sample(1, lists:duplicate(20, 0)))).

%% The probe never compares local time with a beacon timestamp: a beacon clock
%% 60 s ahead or behind gives the same round trip, and only moves the offset.
health_is_skew_free_test() ->
    Run = fun(Skew) ->
        % Sent round 4's commit at local 1000; first shown at local 1800.
        P1 = (probe0())#{ pending := #{ 4 => 1000 } },
        Open = fun(C) -> #{ <<"round">> => 4, <<"phase">> => <<"commit">>, <<"committed">> => C,
                            <<"firstCommitAt">> => 1200 + Skew } end,
        P2 = probe(#{ <<"open">> => Open(#{}), <<"at">> => 1200 + Skew }, ?ME, 1500, P1),
        ?assertEqual(#{ 4 => 1000 }, maps:get(pending, P2)),
        P3 = probe(#{ <<"open">> => Open(#{ ?ME => <<"c">> }), <<"at">> => 1700 + Skew }, ?ME, 1800, P2),
        % The same `at' read again later adds no offset sample.
        P4 = probe(#{ <<"open">> => Open(#{ ?ME => <<"c">> }), <<"at">> => 1700 + Skew }, ?ME, 9000, P3),
        {maps:get(commits, P4), maps:get(pending, P4), offset(P4)}
    end,
    ?assertEqual({[{1800, 800}], #{}, 100}, Run(0)),
    ?assertEqual({[{1800, 800}], #{}, 100 - 60000}, Run(60000)),
    ?assertEqual({[{1800, 800}], #{}, 100 + 60000}, Run(-60000)),
    % A commit never shown is given up after 30 s, as a failed round trip.
    P = probe(#{ <<"open">> => #{ <<"round">> => 5 } }, ?ME, 31001, (probe0())#{ pending := #{ 4 => 1000 } }),
    ?assertEqual({[{31001, 30001}], #{}}, {maps:get(commits, P), maps:get(pending, P)}),
    % No `at' published (older beacon): offset 0.
    ?assertEqual(0, offset(probe(#{}, ?ME, 5, probe0()))).

%% Beacon times are compared with the beacon's clock (`beacon_now'), so a
%% local clock 60 s off neither rejoins early nor waits a minute too long.
beacon_times_use_beacon_clock_test() ->
    Open = #{ <<"round">> => 3, <<"phase">> => <<"collecting">> },
    NotReady = #{ <<"address">> => ?ME, <<"ready">> => false, <<"rejoinAt">> => 160000 },
    Plan = fun(Info, Ctx) -> kinds(element(1, plan(Info, maps:merge(ctx(#{}), Ctx), tmp_dir()))) end,
    % Local clock 100000, beacon clock 60 s ahead.
    ?assertEqual([], Plan(ready_info(Open, NotReady, #{}), #{ beacon_now => 159999 })),
    ?assertEqual([<<"Ready">>], Plan(ready_info(Open, NotReady, #{}), #{ beacon_now => 160000 })),
    Me = #{ <<"address">> => ?ME, <<"ready">> => true },
    Next = ready_info(Open, Me, #{ <<"nextRoundAt">> => 160000 }),
    ?assertEqual([], Plan(Next, #{ beacon_now => 159999 })),
    ?assertEqual([<<"Commit">>], Plan(Next, #{ beacon_now => 160000 })).

full_stake_ready_test() ->
    Open = #{ <<"round">> => 3, <<"phase">> => <<"collecting">> },
    Plan = fun(Me, Params, Ctx) ->
        Info = ready_info(Open, Me, #{ <<"unit">> => 1000000 }),
        Info1 = Info#{ <<"params">> => maps:merge(maps:get(<<"params">>, Info), Params) },
        element(1, plan(Info1, maps:merge(ctx(#{}), Ctx), tmp_dir()))
    end,
    Stake = #{ <<"stake">> => 25000 },
    Short = #{ <<"address">> => ?ME, <<"ready">> => false, <<"bond">> => 24000 },
    % A slashed bond: no Ready until the stake is full again.
    ?assertEqual([], kinds(Plan(Short, Stake, #{}))),
    ?assertEqual([<<"Ready">>], kinds(Plan(Short#{ <<"bond">> => 25000 }, Stake, #{}))),
    % Unclaimed earnings the beacon refills from count.
    ?assertEqual([<<"Ready">>], kinds(Plan(Short#{ <<"earnedAtoms">> => <<"1000000000">> }, Stake, #{}))),
    ?assertEqual([], kinds(Plan(Short#{ <<"earnedAtoms">> => <<"999999999">> }, Stake, #{}))),
    % An older beacon's `rejoinBond' is what it asks for.
    ?assertEqual([<<"Ready">>], kinds(Plan(Short, Stake#{ <<"rejoinBond">> => 1000 }, #{}))),
    % With top-up: refill to the full stake from the wallet, once per bond value.
    Token = <<"tok", (binary:copy(<<"0">>, 40))/binary>>,
    Top = fun(Me, Ctx) ->
        Info = ready_info(Open, Me, #{ <<"unit">> => 1000000, <<"token">> => Token }),
        Info1 = Info#{ <<"params">> => maps:merge(maps:get(<<"params">>, Info), Stake) },
        element(1, plan(Info1, maps:merge(ctx(#{}), Ctx#{ top_up => true }), tmp_dir()))
    end,
    ?assertEqual([{<<"Bond">>, 24000, #{ <<"token">> => Token, <<"quantity">> => <<"1000000000">> }}],
        Top(Short, #{ token => Token })),
    ?assertEqual([], Top(Short, #{ token => Token, sent => #{ {<<"Bond">>, 24000} => {100000 - 1000, -1} } })),
    % Found in neither beaconinfo nor the registry: never an automatic bond.
    ?assertEqual([], Top(#{ <<"address">> => <<"someone-else">> }, #{ token => Token })),
    ?assertEqual([], Top(Short, #{ token => Token, mine => #{} })),
    % On a successor reached by following, a token not pinned, or a stake
    % above 10^7 RNG: only the command is logged.
    ?assertMatch([{<<"Bond-Hint">>, 24000, #{ <<"reason">> := <<"successor beacon">>, <<"rng">> := 1000 }}],
        Top(Short, #{ token => Token, followed => true })),
    ?assertMatch([{<<"Bond-Hint">>, 24000, #{ <<"reason">> := <<"token is not the pinned one">> }}],
        Top(Short, #{ token => <<"evil", (binary:copy(<<"0">>, 39))/binary>> })),
    ?assertMatch([{<<"Bond-Hint">>, 24000, #{ <<"reason">> := <<"token is not the pinned one">> }}], Top(Short, #{})),
    Huge = fun(Ctx) ->
        Info = ready_info(Open, Short, #{ <<"unit">> => 1000000, <<"token">> => Token }),
        Info1 = Info#{ <<"params">> => maps:merge(maps:get(<<"params">>, Info), #{ <<"stake">> => 10000001 }) },
        element(1, plan(Info1, maps:merge(ctx(#{}), Ctx#{ top_up => true, token => Token }), tmp_dir()))
    end,
    ?assertMatch([{<<"Bond-Hint">>, 24000, #{ <<"reason">> := <<"stake above 10^7 RNG">>, <<"rng">> := 9976001 }}], Huge(#{})),
    % Never while leaving or paused, and not without the option.
    ?assertEqual([], Top(Short, #{ token => Token, pause => true })),
    ?assertEqual([], kinds(Plan(Short, Stake, #{ top_up => false }))).

request_driven_opens_only_requested_rounds_test() ->
    Me = #{ <<"address">> => ?ME, <<"ready">> => true },
    Open = #{ <<"round">> => 3, <<"phase">> => <<"collecting">> },
    Plan = fun(O) -> kinds(element(1, plan(ready_info(O, Me, #{ <<"mode">> => <<"request-driven">> }),
        ctx(#{}), tmp_dir()))) end,
    ?assertEqual([], Plan(Open)),
    ?assertEqual([<<"Commit">>], Plan(Open#{ <<"pending">> => 1 })).

beacon_health_read_test() ->
    ?assertEqual(null, health(#{}, null)),
    ?assertEqual(<<"halted">>, health(#{ <<"health">> => #{ <<"status">> => <<"halted">>, <<"eligible">> => 1 } }, null)),
    ?assertEqual(<<"ok">>, health(#{ <<"health">> => #{ <<"status">> => <<"ok">> } }, <<"halted">>)).

%% The phase is the node's word: no reveal before the beacon clock passes
%% the round's commitDeadline.
reveal_waits_for_commit_deadline_test() ->
    Dir = tmp_dir(),
    save_secret(Dir, 3, <<"c1">>, <<"a1">>),
    R = #{ <<"round">> => 3, <<"phase">> => <<"reveal">>, <<"commitDeadline">> => 105000,
        <<"committed">> => #{ ?ME => <<"c1">> }, <<"revealed">> => [] },
    Open = #{ <<"round">> => 4, <<"phase">> => <<"commit">>, <<"committed">> => #{ ?ME => <<"x">> } },
    Info = info(Open, #{ <<"revealing">> => [R] }),
    ?assertEqual([], element(1, plan(Info, (ctx(#{}))#{ beacon_now => 104999 }, Dir))),
    ?assertEqual([<<"Reveal">>], kinds(element(1, plan(Info, (ctx(#{}))#{ beacon_now => 105000 }, Dir)))).

%% A complete set closed the commit phase: reveal before commitDeadline. One
%% member short: wait for the deadline.
reveal_early_when_set_complete_test() ->
    Dir = tmp_dir(),
    save_secret(Dir, 3, <<"c1">>, <<"a1">>),
    R = #{ <<"round">> => 3, <<"phase">> => <<"reveal">>, <<"commitDeadline">> => 105000,
        <<"active">> => [?ME, <<"v2">>, <<"v3">>],
        <<"committed">> => #{ ?ME => <<"c1">>, <<"v2">> => <<"y">>, <<"v3">> => <<"z">> },
        <<"revealed">> => [] },
    Open = #{ <<"round">> => 4, <<"phase">> => <<"commit">>, <<"committed">> => #{ ?ME => <<"x">> } },
    Plan = fun(X, BNow) -> element(1, plan(info(Open, #{ <<"revealing">> => [X] }), (ctx(#{}))#{ beacon_now => BNow }, Dir)) end,
    ?assertEqual([<<"Reveal">>], kinds(Plan(R, 100000))),
    Short = R#{ <<"committed">> => #{ ?ME => <<"c1">>, <<"v2">> => <<"y">> } },
    ?assertEqual([], Plan(Short, 104999)),
    ?assertEqual([<<"Reveal">>], kinds(Plan(Short, 105000))).

%% A reveal due within HOLD_MS of the next commit waits for it and goes in the
%% same Act; never longer than HOLD_MAX_MS after its round closed, nor near
%% its revealDeadline, nor when the commit is already sent or `act' missing.
reveal_waits_for_next_commit_test() ->
    Dir = tmp_dir(),
    save_secret(Dir, 6, <<"c6">>, <<"a6">>),
    % Round 6 closed at 99900 (revealDeadline - revealWindowMs).
    R = #{ <<"round">> => 6, <<"phase">> => <<"reveal">>, <<"active">> => [?ME],
        <<"committed">> => #{ ?ME => <<"c6">> }, <<"revealed">> => [], <<"revealDeadline">> => 129900 },
    Open = #{ <<"round">> => 7, <<"phase">> => <<"collecting">> },
    Info = fun(Extra) ->
        I = info(Open, maps:merge(#{ <<"revealing">> => [R], <<"nextRoundAt">> => 100300,
            <<"features">> => [<<"act">>] }, Extra)),
        I#{ <<"params">> => (maps:get(<<"params">>, I))#{ <<"revealWindowMs">> => 30000 } }
    end,
    Plan = fun(I, C) -> kinds(element(1, plan(I, maps:merge(ctx(#{}), C), Dir))) end,
    ?assertEqual([], Plan(Info(#{}), #{})),
    {Both, 6} = plan(Info(#{}), (ctx(#{}))#{ now => 100300, beacon_now => 100300 }, Dir),
    ?assertEqual([<<"Commit">>, <<"Reveal">>], kinds(Both)),
    ?assertMatch([{<<"Act">>, _, _}], batch(Both, Info(#{}))),
    Stuck = Info(#{ <<"nextRoundAt">> => 99900 + ?HOLD_MAX_MS + 100 }),
    ?assertEqual([], Plan(Stuck, #{ beacon_now => 99900 + ?HOLD_MAX_MS - 1 })),
    ?assertEqual([<<"Reveal">>], Plan(Stuck, #{ beacon_now => 99900 + ?HOLD_MAX_MS })),
    ?assertEqual([<<"Reveal">>], Plan(Info(#{ <<"nextRoundAt">> => 100000 + ?HOLD_MS + 1 }), #{})),
    ?assertEqual([<<"Reveal">>], Plan(Info(#{}), #{ sent => #{ {<<"Commit">>, 7} => {100000, 99} } })),
    ?assertEqual([<<"Reveal">>], Plan(Info(#{ <<"features">> => [] }), #{})),
    ?assertEqual([<<"Reveal">>], Plan(Info(#{}), #{ hold_ms => 0 })),
    Short = Info(#{ <<"revealing">> => [R#{ <<"revealDeadline">> => 99900 + ?REVEAL_MARGIN_MS - 1 }] }),
    ShortW = Short#{ <<"params">> => (maps:get(<<"params">>, Short))#{ <<"revealWindowMs">> => ?REVEAL_MARGIN_MS - 1 } },
    ?assertEqual([<<"Reveal">>], Plan(ShortW, #{})).

registry_entry_shapes_test() ->
    E = #{ <<"address">> => ?ME, <<"bond">> => 7 },
    ?assertEqual(E, registry_entry([#{ <<"address">> => <<"o">> }, E], ?ME)),
    ?assertEqual(E, registry_entry(#{ <<"validators">> => [E] }, ?ME)),
    ?assertEqual(E, registry_entry(#{ ?ME => #{ <<"bond">> => 7 } }, ?ME)),
    ?assertEqual(#{}, registry_entry(#{ <<"validators">> => [] }, ?ME)),
    ?assertEqual(#{}, registry_entry({'EXIT', read_failed}, ?ME)),
    % Listed in beaconinfo: no registry read at all (St has no node to read).
    St = #{ registry => {undefined, #{}} },
    ?assertEqual({E, {undefined, #{}}}, member(#{ <<"validators">> => [E] }, ?ME, 100000, St)),
    % Not listed, registry read fresh within 30 s: the cached entry.
    ?assertEqual({E, {90000, E}}, member(#{ <<"validators">> => [] }, ?ME, 100000, #{ registry => {90000, E} })).

pin_token_test() ->
    B = <<"beacon", (binary:copy(<<"0">>, 37))/binary>>,
    N = <<"next", (binary:copy(<<"0">>, 39))/binary>>,
    T = <<"tok", (binary:copy(<<"0">>, 40))/binary>>,
    T2 = <<"evil", (binary:copy(<<"0">>, 39))/binary>>,
    Cfg = #{ <<"beacon">> => B },
    ?assertEqual(T, maps:get(token, pin_token(#{ <<"token">> => T }, #{ cfg => Cfg, beacon => B }))),
    % Pinned once: a later token is ignored.
    ?assertEqual(T, maps:get(token, pin_token(#{ <<"token">> => T2 }, #{ cfg => Cfg, beacon => B, token => T }))),
    % Never pinned from a successor.
    ?assertEqual(error, maps:find(token, pin_token(#{ <<"token">> => T2 }, #{ cfg => Cfg, beacon => N }))).

successor_needs_ack_test() ->
    B = <<"beacon", (binary:copy(<<"0">>, 37))/binary>>,
    N = <<"next", (binary:copy(<<"0">>, 39))/binary>>,
    % An older beacon publishes no ack: the predecessor check alone decides.
    ?assertEqual(N, named_successor(#{ <<"successor">> => N }, B)),
    % A v3 beacon: only once the successor has acked.
    ?assertEqual(none, named_successor(#{ <<"successor">> => N, <<"successorAck">> => false }, B)),
    ?assertEqual(N, named_successor(#{ <<"successor">> => N, <<"successorAck">> => true }, B)),
    ?assertEqual(none, named_successor(#{ <<"successor">> => <<>> }, B)),
    ?assertEqual(none, named_successor(#{ <<"successor">> => B, <<"successorAck">> => true }, B)).

backoff_doubles_per_incident_test() ->
    Params = #{ <<"rejoinBaseMs">> => 1000, <<"rejoinCapMs">> => 10000 },
    B0 = #{ faults => undefined, n => 0, until => 0, ready_since => 0 },
    Ready = fun(F) -> #{ <<"ready">> => true, <<"faults">> => F } end,
    Down = fun(F) -> #{ <<"ready">> => false, <<"faults">> => F } end,
    B1 = backoff(Ready(0), Params, 100, B0),
    % First fault while ready: wait the base.
    B2 = backoff(Down(1), Params, 200, B1),
    ?assertMatch(#{ n := 1, until := 1200 }, B2),
    % More faults of the same incident (not ready since): no escalation.
    B3 = backoff(Down(2), Params, 300, B2),
    ?assertMatch(#{ n := 1, until := 1200 }, B3),
    % Back, then a fault within the cap: doubled.
    B4 = backoff(Ready(2), Params, 2000, B3),
    ?assertMatch(#{ n := 2, until := 4500 }, backoff(Down(3), Params, 2500, B4)),
    % Ready for the cap or longer: the count starts over.
    ?assertMatch(#{ n := 1, until := 21000 }, backoff(Down(3), Params, 20000, B4)).

pause_still_owes_its_rounds_test() ->
    Me = #{ <<"address">> => ?ME, <<"ready">> => true },
    Collecting = #{ <<"round">> => 3, <<"phase">> => <<"collecting">> },
    % Ready and a pause file: say Pause, and open no new round.
    {A, _} = plan(ready_info(Collecting, Me, #{}), (ctx(#{}))#{ pause => true }, tmp_dir()),
    ?assertEqual([<<"Pause">>], kinds(A)),
    % Paused, but in the set of a round already open: still commit.
    InSet = #{ <<"round">> => 3, <<"phase">> => <<"commit">>, <<"active">> => [?ME, <<"other">>],
        <<"committed">> => #{ <<"other">> => <<"x">> } },
    Paused = #{ <<"address">> => ?ME, <<"ready">> => false, <<"paused">> => true },
    {B, _} = plan(ready_info(InSet, Paused, #{}), (ctx(#{}))#{ pause => true }, tmp_dir()),
    ?assertEqual([<<"Commit">>], kinds(B)),
    % Not in the set: never commit to it.
    {C, _} = plan(ready_info(InSet#{ <<"active">> => [<<"other">>] }, Me, #{}), ctx(#{}), tmp_dir()),
    ?assertEqual([], kinds(C)).

reveal_by_prefix_test() ->
    Dir = tmp_dir(),
    C = commit(alpha()),
    save_secret(Dir, 3, C, <<"a1">>),
    Me = #{ <<"address">> => ?ME, <<"ready">> => true },
    R = #{ <<"round">> => 3, <<"phase">> => <<"reveal">>,
        <<"committed">> => #{ ?ME => binary:part(C, 0, 16) }, <<"revealed">> => [] },
    {A, 3} = plan(ready_info(#{ <<"round">> => 4, <<"phase">> => <<"commit">>, <<"active">> => [] }, Me,
        #{ <<"revealing">> => [R] }), ctx(#{}), Dir),
    ?assertEqual([{<<"Reveal">>, 3, #{ <<"round">> => 3, <<"alpha">> => <<"a1">> }}], A).

obligations_test() ->
    Info = #{ <<"open">> => #{ <<"round">> => 5, <<"phase">> => <<"commit">>, <<"active">> => [?ME] },
        <<"revealing">> => [#{ <<"round">> => 4, <<"committed">> => #{ ?ME => <<"c">> }, <<"revealed">> => [] },
                            #{ <<"round">> => 3, <<"committed">> => #{ ?ME => <<"c">> }, <<"revealed">> => [?ME] }] },
    ?assertEqual([4, 5], obligations(Info, ?ME)).

prune_old_secrets_test() ->
    Dir = tmp_dir(),
    save_secret(Dir, 2, <<"c">>, <<"a">>),
    save_secret(Dir, 5, <<"c">>, <<"a">>),
    prune(Dir, 3),
    ?assertEqual(["5.json"], element(2, file:list_dir(Dir))).

%%% The beacon on a real node -------------------------------------------------

%% A stand-in beacon on `lua@5.3a' (the real one's execution device). It
%% publishes `beaconinfo' with `Me' as a validator, logs every message as
%% `action|committer|data;', and once `Me' commits shows round 1 revealing.
fake_beacon(Me, Opts) ->
    Script = <<
        "Log = Log or ''\n"
        "C = C or ''\n"
        "Revealed = Revealed or false\n"
        "local ME = '", Me/binary, "'\n"
        "function compute(base, req)\n"
        "  local msg = req.body or {}\n"
        "  local who = ''\n"
        "  for _, c in pairs(msg.commitments or {}) do\n"
        "    if type(c) == 'table' and c.committer then who = tostring(c.committer) end\n"
        "  end\n"
        "  local action = tostring(msg.action or msg.Action or '')\n"
        "  local data = tostring(msg.data or msg.Data or '')\n"
        "  Log = Log .. action .. '|' .. who .. '|' .. data .. ';'\n"
        "  if action == 'Commit' and who == ME and C == '' then C = data:match('\"c\":\"(%x+)\"') end\n"
        "  if action == 'Reveal' and who == ME then Revealed = true end\n"
        "  local open, revealing = '{\"round\":1,\"phase\":\"collecting\",\"committed\":{}}', '[]'\n"
        "  if C ~= '' then\n"
        "    open = '{\"round\":2,\"phase\":\"collecting\",\"committed\":{}}'\n"
        "    revealing = '[{\"round\":1,\"phase\":\"reveal\",\"committed\":{\"' .. ME .. '\":\"' .. C\n"
        "      .. '\"},\"revealed\":' .. (Revealed and ('[\"' .. ME .. '\"]') or '[]') .. '}]'\n"
        "  end\n"
        "  base.beaconinfo = '{\"round\":0,\"nextRoundAt\":0,\"params\":{\"minCommits\":2,\"heartbeat\":1,'\n"
        "    .. '\"slash\":1000,\"rejoinAfter\":600},\"validators\":[{\"address\":\"' .. ME\n"
        "    .. '\",\"bond\":5000},{\"address\":\"other\"}],\"open\":' .. open .. ',\"revealing\":' .. revealing .. '}'\n"
        "  base.log = Log\n"
        "  return base\n"
        "end\n"
    >>,
    NodeAddr = hb_util:human_id(ar_wallet:to_address(hb_opts:get(<<"priv-wallet">>, none, Opts))),
    Process = hb_message:commit(#{
        <<"device">> => <<"process@1.0">>,
        <<"type">> => <<"Process">>,
        <<"scheduler-device">> => <<"scheduler@1.0">>,
        <<"execution-device">> => <<"lua@5.3a">>,
        <<"module">> => #{ <<"content-type">> => <<"application/lua">>, <<"body">> => Script },
        <<"authority">> => [NodeAddr],
        <<"scheduler-location">> => NodeAddr
    }, Opts),
    {ok, _} = hb_cache:write(Process, Opts),
    hb_util:human_id(hb_message:id(Process, all, Opts)).

%% Commit, then reveal, through a node's HTTP API: the beacon sees each message
%% signed by the `wallet-file' address (not the node's), its data intact, and
%% the revealed secret opens the commit. Nothing is sent twice.
commit_reveal_over_http_test_() ->
    {timeout, 120, fun() ->
        hb:init(),
        Opts = #{ <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => ar_wallet:new(), <<"port">> => 10000 + rand:uniform(20000),
            <<"hosts-block-internal">> => false },
        Node = hb_http_server:start_node(Opts),
        Dir = tmp_dir(),
        WalletFile = filename:join(Dir, "wallet.json"),
        Wallet = ar_wallet:new(),
        ok = file:write_file(WalletFile, ar_wallet:to_json(Wallet)),
        Me = hb_util:human_id(ar_wallet:to_address(Wallet)),
        Beacon = fake_beacon(Me, Opts),
        St0 = initial_state(#{ <<"beacon">> => Beacon, <<"node">> => Node,
            <<"wallet-file">> => hb_util:bin(WalletFile),
            <<"secrets-dir">> => hb_util:bin(filename:join(Dir, "secrets")) }, Opts),
        ?assertEqual(Me, maps:get(me, St0)),
        % A first message, so the beacon has computed and published something.
        _ = send(<<"Ping">>, #{}, St0),
        St1 = pass(St0),
        ?assertMatch(#{ last_error := null }, St1),
        St2 = pass(St1),
        St3 = pass(St2),
        Log = read(<<"now/log">>, St3),
        Entries = [E || E <- binary:split(Log, <<";">>, [global]), E =/= <<>>],
        ?event(debug_rng_validator, {log, Entries}),
        Sent = [{K, W, hb_json:decode(D)} || E <- Entries, [K, W, D] <- [binary:split(E, <<"|">>, [global])]],
        % Every message is signed by the wallet-file address.
        ?assertEqual([Me], lists:usort([W || {_, W, _} <- Sent])),
        % Round 1: one commit, one reveal, and the secret opens the commit.
        [C] = [C0 || {<<"Commit">>, _, #{ <<"round">> := 1, <<"c">> := C0 }} <- Sent],
        [A] = [A0 || {<<"Reveal">>, _, #{ <<"round">> := 1, <<"alpha">> := A0 }} <- Sent],
        ?assertEqual(C, commit(A)),
        % Round 2 opened meanwhile: committed to once.
        ?assertEqual(1, length([x || {<<"Commit">>, _, #{ <<"round">> := 2 }} <- Sent]))
    end}.

%% The node's `start' hook runs the loop as the node's own wallet, and
%% `status' answers while the beacon cannot be reached.
start_hook_test_() ->
    {timeout, 60, fun() ->
        hb:init(),
        Wallet = ar_wallet:new(),
        Opts = #{ <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
            <<"priv-wallet">> => Wallet, <<"port">> => 10000 + rand:uniform(20000),
            <<"hosts-block-internal">> => false },
        Node = hb_http_server:start_node(Opts#{
            <<"on">> => #{ <<"start">> => #{ <<"device">> => <<"rng-validator@1.0">> } },
            <<"rng-validator">> => #{ <<"beacon">> => <<"no-such-beacon">>,
                <<"node">> => <<"http://127.0.0.1:1">>,
                <<"secrets-dir">> => hb_util:bin(filename:join(tmp_dir(), "s")) }
        }),
        try
            {ok, Res} = hb_http:get(Node, <<"/~rng-validator@1.0/status">>, Opts),
            S = hb_json:decode(hb_maps:get(<<"body">>, Res, Res, Opts)),
            Me = hb_util:human_id(ar_wallet:to_address(Wallet)),
            ?assertMatch(#{ <<"address">> := Me }, S)
        after
            case hb_name:lookup(?NAME) of
                Pid when is_pid(Pid) -> hb_name:unregister(?NAME), exit(Pid, kill);
                _ -> ok
            end
        end
    end}.

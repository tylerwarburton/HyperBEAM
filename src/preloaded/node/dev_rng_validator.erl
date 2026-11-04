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
%%%   4. rejoins (`Rejoin') after a suspension while its bond covers a slash;
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
%%%   interval (ms between passes, default 100), exit (true to leave).
%%% Endpoint, read-only:
%%%   GET /~rng-validator@1.0/status   address, round, phase, last error
-module(dev_rng_validator).
-implements(<<"rng-validator@1.0">>).
-export([device_info/1, start/3, status/3]).
-export([plan/3, alpha/0, commit/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NAME, {<<"rng-validator@1.0">>, loop}).

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
    #{ cfg => Cfg, opts => Opts#{ <<"priv-wallet">> => Wallet }, me => Me, dir => Dir,
        sent => #{}, status => #{ <<"address">> => Me }, last_error => null, last_pass => 0 }.

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
pass(St = #{ me := Me, sent := Sent0, dir := Dir }) ->
    % Before the state read, so the state is at least this far along.
    AtSlot = to_int(read(<<"now/at-slot">>, St), 0),
    Info = hb_json:decode(read(<<"now/beaconinfo">>, St)),
    Now = os:system_time(millisecond),
    {Actions, Oldest} = plan(Info, #{ me => Me, sent => Sent0, at_slot => AtSlot,
        now => Now, exit => maps:get(<<"exit">>, maps:get(cfg, St), false) =:= true }, Dir),
    Sent = lists:foldl(fun(A, S) -> act(A, S, St#{ now => Now }) end, Sent0, Actions),
    prune(Dir, Oldest),
    Open = obj(<<"open">>, Info),
    Mine = mine(Info, Me),
    % What was sent is only remembered for ten minutes.
    St#{ sent => maps:filter(fun(_, {At, _}) -> Now - At < 600000 end, Sent),
         last_pass => Now, last_error => null,
         status => #{ <<"address">> => Me, <<"round">> => maps:get(<<"round">>, Info, null),
            <<"open">> => maps:get(<<"round">>, Open, null), <<"phase">> => maps:get(<<"phase">>, Open, null),
            <<"suspended">> => maps:get(<<"suspended">>, Mine, null),
            <<"exiting">> => maps:get(<<"exiting">>, Mine, null), <<"at-slot">> => AtSlot } }.

%% @doc What to send now, from the beacon's `beaconinfo' and what was already
%% sent: `{[{Kind, Round, Data}], OldestRound}'. Pure, except a commit's fresh
%% secret, which is written to `Dir' before the commit is returned.
plan(Info, Ctx = #{ me := Me, now := Now }, Dir) ->
    Open = obj(<<"open">>, Info),
    Params = obj(<<"params">>, Info),
    Mine = mine(Info, Me),
    Round = to_int(maps:get(<<"round">>, Info, 0), 0),
    MinCommits = to_int(maps:get(<<"minCommits">>, Params, 2), 2),
    Active = length([V || V <- list(<<"validators">>, Info),
        not truthy(<<"suspended">>, V), not truthy(<<"exiting">>, V), not truthy(<<"overdue">>, V)]),
    Due = fun(Kind, R, Ms) -> due({Kind, R}, Ctx, Ms) end,
    Member = Mine =/= #{},
    Suspended = truthy(<<"suspended">>, Mine),
    Exiting = truthy(<<"exiting">>, Mine),
    Bond = to_int(maps:get(<<"bond">>, Mine, 0), 0),
    % Suspended: rejoin on our own while the bond still covers the next slash.
    % The cooldown counts finished rounds, so it is waived while too few
    % validators are active to finish one: otherwise all-suspended waits forever.
    Rejoin =
        [ {<<"Rejoin">>, Round, #{}}
        || Member, Suspended, not Exiting, not maps:get(exit, Ctx),
           Bond >= to_int(maps:get(<<"slash">>, Params, 0), 0),
           truthy(<<"idle">>, Mine) orelse Active < MinCommits
               orelse Round >= to_int(maps:get(<<"suspendedAt">>, Mine, 0), 0)
                   + to_int(maps:get(<<"rejoinAfter">>, Params, 0), 0),
           Due(<<"Rejoin">>, Round, 3000) ],
    Leave =
        case maps:get(exit, Ctx) andalso Member of
            false -> [];
            true when not Exiting -> [ {<<"Exit">>, 0, #{}} || Due(<<"Exit">>, 0, 3000) ];
            true -> [ {<<"Finalize-Exit">>, 0, #{}} || Due(<<"Finalize-Exit">>, 0, 30000) ]
        end,
    % Commit as soon as the cadence allows: every round with the heartbeat on,
    % else only rounds with requests waiting. A round with fewer active
    % validators than minCommits can only void; the beacon refuses to open it.
    OpenRound = to_int(maps:get(<<"round">>, Open, 0), 0),
    Commit =
        case Member andalso not Suspended andalso not Exiting andalso not maps:get(exit, Ctx)
            andalso Active >= MinCommits
            andalso lists:member(maps:get(<<"phase">>, Open, null), [<<"collecting">>, <<"commit">>])
            andalso (to_int(maps:get(<<"pending">>, Open, 0), 0) > 0 orelse truthy(<<"heartbeat">>, Params))
            andalso Now >= to_int(maps:get(<<"nextRoundAt">>, Info, 0), 0)
            andalso not maps:is_key(Me, obj(<<"committed">>, Open))
            andalso Due(<<"Commit">>, OpenRound, 1000) of
            false -> [];
            true ->
                % A fresh secret on every send, a resend after a void included.
                A = alpha(),
                C = commit(A),
                save_secret(Dir, OpenRound, C, A),
                [{<<"Commit">>, OpenRound, #{ <<"round">> => OpenRound, <<"c">> => C }}]
        end,
    % Rounds overlap: the open round takes commits while older ones reveal.
    Rounds = list(<<"revealing">>, Info) ++ [Open],
    PerRound = lists:flatmap(fun(X) ->
        R = to_int(maps:get(<<"round">>, X, 0), 0),
        Phase = maps:get(<<"phase">>, X, null),
        Deadline = to_int(maps:get(<<"commitDeadline">>, X, 0), 0),
        Mine0 = maps:get(Me, obj(<<"committed">>, X), undefined),
        % If the window has passed by our clock but it still shows commits, ask
        % it to close; reveal only once the beacon shows the round revealing.
        [ {<<"Close">>, R, #{ <<"round">> => R }}
        || Phase =:= <<"commit">>, Deadline > 0, Now >= Deadline, Due(<<"Close">>, R, 2000) ] ++
        [ {<<"Reveal">>, R, #{ <<"round">> => R, <<"alpha">> => A }}
        || Phase =:= <<"reveal">>, is_binary(Mine0),
           not lists:member(Me, list(<<"revealed">>, X)),
           Due(<<"Reveal">>, R, 3000),
           A <- [maps:get(Mine0, secrets(Dir, R), missing)], A =/= missing ]
    end, Rounds),
    Oldest = lists:min([to_int(maps:get(<<"round">>, X, 0), 0) || X <- Rounds]),
    {Rejoin ++ Leave ++ Commit ++ PerRound, Oldest}.

%% Sent: #{ {Kind, Round} => {At, Slot} }. Not sent yet, or the beacon has
%% computed past the slot it was given and `Ms' have passed with no effect.
due(Key, #{ sent := Sent, at_slot := AtSlot, now := Now }, Ms) ->
    case maps:get(Key, Sent, undefined) of
        undefined -> true;
        {At, Slot} -> Slot =< AtSlot andalso Now - At > Ms
    end.

act({Kind, Round, Data}, Sent, St = #{ now := Now }) ->
    Slot = try send(Kind, Data, St)
           catch C:R -> ?event(rng_validator, {send_failed, Kind, C, R}), -1
           end,
    ?event(rng_validator, {sent, Kind, Round, Slot}),
    Sent#{ {Kind, Round} => {Now, Slot} }.

%% Schedule one signed message on the beacon; its slot, or -1 if unknown.
send(Action, Data, #{ cfg := Cfg, opts := Opts }) ->
    Beacon = maps:get(<<"beacon">>, Cfg),
    Msg = hb_message:commit(#{
        <<"target">> => Beacon, <<"type">> => <<"Message">>, <<"action">> => Action,
        <<"data">> => hb_json:encode(Data)
    }, Opts, #{ <<"device">> => <<"ans104@1.0">>, <<"bundle">> => true }),
    {ok, Res} = hb_http:post(maps:get(<<"node">>, Cfg),
        Msg#{ <<"path">> => <<"/", Beacon/binary, "~process@1.0/schedule">> },
        Opts#{ <<"http-codec">> => <<"ans104@1.0">> }),
    to_int(hb_maps:get(<<"slot">>, Res, -1, Opts), -1).

%% A published key of the beacon, as a binary.
read(Key, #{ cfg := Cfg, opts := Opts }) ->
    Beacon = maps:get(<<"beacon">>, Cfg),
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

ctx(Sent) -> #{ me => ?ME, sent => Sent, at_slot => 10, now => 100000, exit => false }.
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
    ?assertEqual([<<"Close">>], kinds(A)).

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

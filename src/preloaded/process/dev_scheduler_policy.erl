%%% @doc Which processes this node schedules: the node's `scheduler-policy'.
%%%
%%% One option, two modes:
%%%
%%%   #{ <<"mode">> => <<"open">>, <<"deny">> => #{ PID => Message } }
%%%   #{ <<"mode">> => <<"owners">>, <<"owners">> => [Address, ...],
%%%      <<"deny">> => #{ PID => Message },
%%%      <<"refusal-message">> => Message }
%%%
%%% With no option the node schedules everything, as before.
%%%
%%% `deny' is checked first, in both modes: a spawn of, or a message to, a
%%% denied process is refused with 402 and its message. Its keys are compared
%%% lowercased, because loading a node config lowercases map keys; a deny list
%%% can only refuse, so a lowercase collision costs nothing but a refusal.
%%%
%%% In `owners' mode a process is admitted when its spawn was signed by an
%%% owner, or when it was spawned by a process this node admits: the spawn's
%%% `from-process' as the scheduler's door admitted it (`admit-process',
%%% `dev_scheduler_admission'), never as the message claims it, so a process's
%%% children keep working and a stranger cannot borrow an admitted parent. A
%%% message is admitted when its process is. Anything else is refused with
%%% 403 and `refusal-message'. Reads are never refused.
%%%
%%% Whether an existing process is admitted is read back from its first
%%% assignment (its spawn and the door's `admit-process'), so the answer
%%% survives a restart and covers processes spawned before the policy was set.
-module(dev_scheduler_policy).
-export([deny/2, admit/4]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_REFUSAL, <<"This node does not schedule this process.">>).
%% Admitted processes, remembered per owner list: admission never changes
%% for a process while the policy stands.
-define(ADMITTED, dev_scheduler_policy_admitted).

%% @doc `ok', or `{error, 402 response}' when `ProcID' is denied.
deny(ProcID, Opts) ->
    Deny = hb_maps:get(<<"deny">>, policy(Opts), #{}, Opts),
    Key = lowercase(hb_util:human_id(ProcID)),
    case [ M || {K, M} <- maps:to_list(hb_cache:ensure_all_loaded(Deny, Opts)),
            lowercase(hb_util:bin(K)) =:= Key ] of
        [] -> ok;
        [Message | _] -> refuse(ProcID, 402, hb_util:bin(Message))
    end.

%% @doc `ok', or `{error, 403 response}' when the node's policy does not admit
%% `Req' (already verified and through the door) for `ProcID'.
admit(ProcID, Type, Req, Opts) ->
    Policy = policy(Opts),
    case hb_maps:get(<<"mode">>, Policy, <<"open">>, Opts) of
        <<"owners">> ->
            Owners = owners(Policy, Opts),
            case admitted(ProcID, Type, Req, Owners, Opts) of
                true -> ok;
                false ->
                    refuse(ProcID, 403,
                        hb_util:bin(hb_maps:get(<<"refusal-message">>, Policy,
                            ?DEFAULT_REFUSAL, Opts)))
            end;
        _ -> ok
    end.

%%% Internal ------------------------------------------------------------------

policy(Opts) ->
    case hb_opts:get(<<"scheduler-policy">>, #{}, Opts) of
        Policy when is_map(Policy) -> Policy;
        _ -> #{}
    end.

owners(Policy, Opts) ->
    lists:usort([ address(O) ||
        O <- hb_util:message_to_ordered_list(
                hb_maps:get(<<"owners">>, Policy, [], Opts), Opts) ]).

%% A spawn: by its parent as the door admitted it, else by its signers. Any
%% other message: by its process.
admitted(ProcID, <<"Process">>, Req, Owners, Opts) ->
    spawn_admitted(Req, Owners, Opts) andalso remember(ProcID, Owners);
admitted(ProcID, _Type, _Req, Owners, Opts) ->
    known(ProcID, Owners, Opts).

spawn_admitted(Spawn, Owners, Opts) ->
    case hb_maps:get(<<"admit-process">>,
            dev_scheduler_admission:keys(Spawn, Opts), none, Opts) of
        Parent when is_binary(Parent) -> known(Parent, Owners, Opts);
        _ -> owner_signed(Spawn, Owners, Opts)
    end.

%% A process this node scheduled, judged from its first assignment.
known(ProcID, Owners, Opts) ->
    case remembered(ProcID, Owners) of
        true -> true;
        false ->
            case dev_scheduler_cache:read(hb_util:human_id(ProcID), 0, Opts) of
                {ok, Assignment} ->
                    A = hb_cache:ensure_all_loaded(Assignment, Opts),
                    Spawn = hb_maps:get(<<"body">>, A, #{}, Opts),
                    Admitted =
                        case hb_maps:get(<<"admit-process">>, A, none, Opts) of
                            Parent when is_binary(Parent) -> known(Parent, Owners, Opts);
                            _ -> owner_signed(Spawn, Owners, Opts)
                        end,
                    Admitted andalso remember(ProcID, Owners);
                _ -> false
            end
    end.

owner_signed(Msg, Owners, Opts) ->
    Signers =
        case catch hb_message:signers(Msg, Opts) of
            L when is_list(L) -> L;
            _ -> []
        end,
    lists:any(fun(S) -> lists:member(address(S), Owners) end, Signers).

address(S) ->
    case hb_committer:address(hb_util:bin(S)) of
        {ok, A} -> hb_util:human_id(A);
        error -> hb_util:bin(S)
    end.

remembered(ProcID, Owners) ->
    ensure_table(),
    ets:member(?ADMITTED, {erlang:phash2(Owners), hb_util:human_id(ProcID)}).

remember(ProcID, Owners) ->
    ensure_table(),
    ets:insert(?ADMITTED, {{erlang:phash2(Owners), hb_util:human_id(ProcID)}}),
    true.

%% Owned by a process that never exits: a table dies with its owner.
ensure_table() ->
    case ets:whereis(?ADMITTED) of
        undefined ->
            Parent = self(),
            Ref = make_ref(),
            spawn(fun() ->
                try ets:new(?ADMITTED, [named_table, public, set]) of
                    _ -> Parent ! {Ref, ok}, receive after infinity -> ok end
                catch error:badarg -> Parent ! {Ref, ok}
                end
            end),
            receive {Ref, ok} -> ok after 5000 -> ok end;
        _ -> ok
    end.

lowercase(Bin) -> string:lowercase(Bin).

refuse(ProcID, Status, Message) ->
    ?event(scheduling, {policy_refused, {proc_id, ProcID}, {status, Status}}),
    {error, #{ <<"status">> => Status, <<"body">> => Message,
        <<"process">> => hb_util:human_id(ProcID) }}.

%%% Tests ---------------------------------------------------------------------

addr(W) -> hb_util:human_id(ar_wallet:to_address(W)).

%% A node whose policy is `Policy', and a process definition it schedules.
policy_node(Policy) ->
    Store = hb_test_utils:test_store(),
    ok = hb_store:start([Store]),
    Node = ar_wallet:new(),
    Opts = #{ <<"priv-wallet">> => Node, <<"store">> => [Store],
        <<"scheduler-policy">> => Policy },
    {Node, Opts}.

definition(Node, Seed) ->
    #{ <<"device">> => <<"scheduler@1.0">>, <<"type">> => <<"Process">>,
       <<"scheduler-location">> => addr(Node), <<"test-random-seed">> => Seed }.

spawn_as(Signer, Def, Opts) ->
    Spawn = hb_message:commit(Def, #{ <<"priv-wallet">> => Signer }),
    {Spawn, hb_ao:resolve(Spawn, #{ <<"method">> => <<"POST">>,
        <<"path">> => <<"schedule">>, <<"body">> => Spawn }, Opts)}.

send_as(Signer, Proc, Opts) ->
    PID = hb_util:human_id(hb_message:id(Proc, all, Opts)),
    Msg = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"target">> => PID,
        <<"action">> => <<"Ping">> }, #{ <<"priv-wallet">> => Signer }),
    hb_ao:resolve(Proc, #{ <<"method">> => <<"POST">>,
        <<"path">> => <<"schedule">>, <<"body">> => Msg }, Opts).

owners_policy(Owner) ->
    #{ <<"mode">> => <<"owners">>, <<"owners">> => [addr(Owner)],
       <<"refusal-message">> => <<"operator only">> }.

owner_spawn_admitted_test() ->
    Owner = ar_wallet:new(),
    {Node, Opts} = policy_node(owners_policy(Owner)),
    {Proc, Res} = spawn_as(Owner, definition(Node, <<"owner">>), Opts),
    ?assertMatch({ok, _}, Res),
    ?assertMatch({ok, _}, send_as(ar_wallet:new(), Proc, Opts)).

stranger_spawn_refused_test() ->
    {Node, Opts} = policy_node(owners_policy(ar_wallet:new())),
    {_Proc, Res} = spawn_as(ar_wallet:new(), definition(Node, <<"stranger">>), Opts),
    ?assertMatch({error, #{ <<"status">> := 403, <<"body">> := <<"operator only">> }}, Res).

%% A process spawned by an admitted process, through the door (the node's own
%% deliverer signs the child with `from-*' stamps), is admitted.
child_of_admitted_process_admitted_test() ->
    Owner = ar_wallet:new(),
    {Node, Opts} = policy_node(owners_policy(Owner)),
    {Parent, {ok, _}} = spawn_as(Owner, definition(Node, <<"parent">>), Opts),
    ParentID = hb_util:human_id(hb_message:id(Parent, all, Opts)),
    Child = (definition(Node, <<"child">>))#{
        <<"from-process">> => ParentID, <<"from-slot">> => 1,
        <<"from-outbox-key">> => <<"1">>,
        <<"from-entry">> => hb_util:human_id(crypto:strong_rand_bytes(32)) },
    {ChildMsg, Res} = spawn_as(Node, Child, Opts),
    ?assertMatch({ok, _}, Res),
    ?assertMatch({ok, _}, send_as(ar_wallet:new(), ChildMsg, Opts)),
    % The same stamps signed by a stranger are not the door's word.
    Forged = Child#{ <<"test-random-seed">> => <<"forged">> },
    ?assertMatch({_, {error, #{ <<"status">> := 403 }}}, spawn_as(ar_wallet:new(), Forged, Opts)).

denied_process_gets_402_test() ->
    Owner = ar_wallet:new(),
    {Node, Opts0} = policy_node(#{ <<"mode">> => <<"open">> }),
    {Proc, {ok, _}} = spawn_as(Owner, definition(Node, <<"denied">>), Opts0),
    PID = hb_util:human_id(hb_message:id(Proc, all, Opts0)),
    % As a JSON config gives it: the deny list's keys lowercased.
    Opts = Opts0#{ <<"scheduler-policy">> => #{ <<"mode">> => <<"open">>,
        <<"deny">> => #{ string:lowercase(PID) => <<"not whitelisted">> } } },
    ?assertMatch({error, #{ <<"status">> := 402, <<"body">> := <<"not whitelisted">> }},
        send_as(Owner, Proc, Opts)).

open_mode_admits_strangers_test() ->
    {Node, Opts} = policy_node(#{ <<"mode">> => <<"open">>,
        <<"deny">> => #{ <<"someoneelse">> => <<"no">> } }),
    {Proc, Res} = spawn_as(ar_wallet:new(), definition(Node, <<"open">>), Opts),
    ?assertMatch({ok, _}, Res),
    ?assertMatch({ok, _}, send_as(ar_wallet:new(), Proc, Opts)).

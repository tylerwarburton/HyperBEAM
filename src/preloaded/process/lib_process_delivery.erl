%%% @doc Durable, exactly-once delivery of every computed slot's outbox.
%%%
%%% Only the node that schedules a process delivers its outbox (`delivers/2'):
%%% a node holding an identity the process names as its scheduler, with a
%%% durable (LMDB) delivery store. That is a fact of the process definition and
%%% the node's keys, true from the first instant after a restart, whatever has
%%% or has not started yet. There the process worker stores each slot's outbox,
%%% unlowered, as the slot's `o' row (`lib_process_cache'), in the same batch
%%% as the slot's state, and casts the slot to the process's deliverer: one
%%% Erlang process per source process, registered as `{delivery, ProcID}'.
%%% Nobody else ever turns an outbox entry into a signed message. One rule
%%% makes it exactly-once:
%%%
%%%   An entry is signed once, its signed message is written to the slot's
%%%   `q' row, and the store is committed BEFORE the first POST. Every later
%%%   attempt -- a retry, the deliverer after a crash, the node after a
%%%   restart -- posts the same signed message again.
%%%
%%% Signatures are randomized, so a second signature of the same entry would
%%% be a second message to its target; the scheduler refuses a message it has
%%% already assigned with a 409 that carries its slot (`dev_scheduler_server'),
%%% so a repost of the same signed message is assigned at most once. An entry
%%% is finished only by:
%%%
%%%   - a 200 with an assignment, or a 409 `Message already scheduled.' -- each
%%%     with the integer slot it was given;
%%%   - a proven terminal: the target does not exist (a gateway said so), or
%%%     this node's own scheduler refused these exact bytes for good (400,
%%%     413).
%%%
%%% Anything else -- any other answer of a remote scheduler, a 404, a throttle,
%%% a timeout, a network error, an unreachable gateway, a signing, lookup or
%%% post that crashed -- is retried, backing off from 1 s to 10 min, with no
%%% cap on attempts: an entry is never given up. A 307 or a 422 says the
%%% message was not assigned, so the entry is signed again (for the new
%%% location, or as `ans104@1.0') like a new one, and the target remembers it:
%%% its later entries are signed that way from the start.
%%%
%%% Entries are independent. Each has its own backoff; a target has at most
%%% `?TARGET_INFLIGHT' posts in flight; nothing waits behind an entry that
%%% fails. The contracts this node runs take their messages in any order (the
%%% token's transfer streams and mint watermark, the vault's payout window),
%%% as they did when clients pushed concurrently.
%%%
%%% Each step signs what it is about to post (and up to `?SIGN_AHEAD' more of
%%% the same target's entries), writes those `q' rows and the `x' rows of the
%%% slots finished since the last commit, deletes the `o' and `q' rows of the
%%% slots whose `x' is written, and commits all of it in one LMDB transaction
%%% -- the one forced commit there is -- then posts. A step that signed
%%% nothing commits nothing: its `x' rows ride the store's periodic flush.
%%%
%%%   pstate/<proc>/o/<slot>   the outbox (`lib_process_cache')
%%%   pstate/<proc>/q/<slot>   #{ Key => {signed, Target, Codec, Location, Msg} }
%%%   pstate/<proc>/x/<slot>   #{ Key => Receipt }: the slot is delivered
%%%
%%% A crash before that commit leaves `o' (or `q') beside an `x', or neither
%%% `x' nor the deletes: the slot is skipped or posted again with the same
%%% bytes, and retention deletes leftovers. Nothing ever deletes `o' or `q'
%%% without its `x'.
%%%
%%% The deliverer holds references, not messages: per entry its target and
%%% whether it is signed. Outbox entries and signed bytes are read from the
%%% rows when a step needs them. If anything in it fails, it starts again from
%%% the rows, backing off.
-module(lib_process_delivery).
-export([notify/3, ensure/2, await_delivered/3, sweep/1, pending/2]).
-export([delivers/2, delivers/3, metrics/0]).
-export([read_target/2, recently_missing/2, remember_missing/2]).
-export([augment_message/3, apply_security/4, normalize_message/2]).
-export([remote_schedule_result/3, find_type/2, parse_redirect/2]).
-export([classify/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Outbox targets that recently failed to resolve, e.g. wallet addresses. A
%% target the node does not hold is looked up in the gateway; this cache saves
%% repeating that round trip. One rule: the cache only ever saves the remote
%% lookup of an id the gateway said does not exist. Only IDs and only a
%% definitive `{error, not_found}' are remembered; a remembered target is still
%% read from the local stores; and each target is looked up remotely again
%% once its entry is `push-missing-target-ttl' seconds old (0 disables). At
%% most `?MISSING_TARGETS_MAX' entries: when full, expired ones are swept, and
%% if none have expired a new miss is not remembered.
-define(MISSING_TARGETS, dev_push_missing_targets).
-define(MISSING_TARGETS_MAX, 100000).
-define(DEFAULT_MISSING_TARGET_TTL, 3600).

%% Who is waiting for which slot to be delivered: `{{ProcID, Slot, Ref}, Pid}'.
-define(WAITERS, lib_process_delivery_waiters).
%% Whether this node schedules a process: `{{ProcID, Address}, boolean()}'.
-define(SCHEDULES, lib_process_delivery_schedules).
%% Each deliverer's backlog: `{ProcID, Pid, Entries, OldestSince}'.
-define(BACKLOG, lib_process_delivery_backlog).

%% Posts in flight to one target, at most.
-define(TARGET_INFLIGHT, 8).
%% Gateway lookups in flight from one deliverer, at most.
-define(LOOKUPS, 4).
%% How many more of a target's entries a step signs with the ones it posts.
-define(SIGN_AHEAD, 16).

-define(FIRST_BACKOFF_MS, 1000).
-define(MAX_BACKOFF_MS, 600000).

%%% Public API.

%% @doc Whether this node delivers `ProcID''s outbox: it holds an identity
%% the process names as its scheduler, and a durable delivery store. A replica
%% or a bench node that computes the process never signs for it.
delivers(ProcID, Opts) -> delivers(ProcID, undefined, Opts).
delivers(ProcID, Process, Opts) ->
    store(Opts) =/= none andalso schedules(hb_util:human_id(ProcID), Process, Opts).

%% Known once per process and node address: the definition never changes.
schedules(ProcID, Process, Opts) ->
    ensure_table(?SCHEDULES, set),
    Key = {ProcID, hb_opts:address(Opts)},
    case ets:lookup(?SCHEDULES, Key) of
        [{_, Known}] -> Known;
        [] ->
            case definition(ProcID, Process, Opts) of
                not_found -> false;
                Definition ->
                    Known = is_scheduler(Definition, Opts),
                    ets:insert(?SCHEDULES, {Key, Known}),
                    Known
            end
    end.

definition(_ProcID, Process, _Opts) when is_map(Process) -> Process;
definition(ProcID, _, Opts) ->
    case hb_cache:read(ProcID, hb_store:scope(Opts, local)) of
        {ok, Process} -> Process;
        _ -> not_found
    end.

%% The same test the scheduler device makes before it schedules a process
%% here (`dev_scheduler:is_local_scheduler/4').
is_scheduler(Process, Opts) ->
    Location =
        hb_ao:get_first(
            [{Process, <<"scheduler">>}, {Process, <<"scheduler-location">>}],
            not_found,
            Opts#{ <<"hashpath">> => ignore }
        ),
    lists:any(
        fun(Scheduler) -> element(1, hb_opts:as(Scheduler, Opts)) == ok end,
        case Location of
            L when is_list(L) -> L;
            B when is_binary(B) ->
                binary:split(binary:replace(B, <<"\"">>, <<>>, [global]),
                    <<",">>, [global, trim_all]);
            _ -> []
        end
    ).

%% @doc A slot of `ProcID' was stored with an outbox: hand it to the process's
%% deliverer, starting one when there is none. Never blocks the caller.
notify(ProcID, Slot, Opts) ->
    case hb_name:lookup({delivery, ProcID}) of
        Pid when is_pid(Pid) -> Pid ! {ready, Slot}, ok;
        _ ->
            ServerID = get(server_id),
            spawn(fun() ->
                put(server_id, ServerID),
                _ = ensure(ProcID, Opts)
            end),
            ok
    end.

%% @doc The deliverer of `ProcID', started if there is none. `{error, Reason}'
%% when this node does not deliver the process or cannot read it.
ensure(ProcID, Opts) ->
    case hb_name:lookup({delivery, ProcID}) of
        Pid when is_pid(Pid) -> {ok, Pid};
        _ ->
            Caller = self(),
            Ref = make_ref(),
            ServerID = get(server_id),
            {Pid, Mon} =
                spawn_monitor(fun() ->
                    put(server_id, ServerID),
                    init(ProcID, deliverer_opts(Opts), {Caller, Ref})
                end),
            receive
                {Ref, started} -> erlang:demonitor(Mon, [flush]), {ok, Pid};
                {Ref, {exists, Other}} -> erlang:demonitor(Mon, [flush]), {ok, Other};
                {Ref, {error, Reason}} -> erlang:demonitor(Mon, [flush]), {error, Reason};
                {'DOWN', Mon, process, Pid, Reason} -> {error, {deliverer_failed, Reason}}
            end
    end.

%% @doc Wait until slot `Slot' of `ProcID' is delivered and return its
%% receipts, or `{error, not_delivering}' when this node does not deliver the
%% process, or `{error, no_delivery_record}' when it holds neither the slot's
%% outbox nor its receipts. There is no deadline: the wait is the caller's,
%% and a caller that goes away (an HTTP client that disconnects) leaves at
%% most one registry row, deleted when the slot is delivered. Delivery itself
%% never depends on anybody waiting.
await_delivered(ProcID, Slot, Opts) ->
    case delivers(ProcID, Opts) of
        false -> {error, not_delivering};
        true ->
            ensure_table(?WAITERS, ordered_set),
            Ref = make_ref(),
            ets:insert(?WAITERS, {{ProcID, Slot, Ref}, self()}),
            try await_delivered(ProcID, Slot, Ref, store(Opts), Opts)
            after ets:delete(?WAITERS, {ProcID, Slot, Ref})
            end
    end.

await_delivered(ProcID, Slot, Ref, Store, Opts) ->
    case receipts(ProcID, Slot, Store, Opts) of
        {ok, Receipts} -> {ok, Receipts};
        not_found ->
            case has_row(ProcID, <<"o">>, Slot, Store, Opts) of
                false ->
                    % The deliverer may have finished it between the two reads.
                    case receipts(ProcID, Slot, Store, Opts) of
                        {ok, Receipts} -> {ok, Receipts};
                        not_found -> {error, no_delivery_record}
                    end;
                true ->
                    case ensure(ProcID, Opts) of
                        {ok, Pid} ->
                            Pid ! {ready, Slot},
                            Mon = erlang:monitor(process, Pid),
                            receive
                                {delivered, Ref, Receipts} ->
                                    erlang:demonitor(Mon, [flush]),
                                    {ok, Receipts};
                                {'DOWN', Mon, process, Pid, _} ->
                                    await_delivered(ProcID, Slot, Ref, Store, Opts)
                            end;
                        {error, Reason} -> {error, Reason}
                    end
            end
    end.

%% @doc Start a deliverer for every process with an outbox this node has not
%% delivered. Run once at boot: a slot stored before a restart is still owed.
sweep(Opts) ->
    case store(Opts) of
        none -> 0;
        Store ->
            Procs =
                case hb_store:list([Store], <<"pstate">>, Opts) of
                    {ok, Names} -> Names;
                    _ -> []
                end,
            length(
                [ ok
                || P <- Procs,
                   ?IS_ID(P),
                   pending_slots(P, Store, Opts) =/= [],
                   element(1, ensure(P, Opts)) == ok ]
            )
    end.

%% @doc The slots of `ProcID' with an outbox that is not delivered yet.
pending(ProcID, Opts) ->
    case store(Opts) of
        none -> [];
        Store -> pending_slots(ProcID, Store, Opts)
    end.

%% @doc The node's delivery backlog, for the metrics endpoint: every
%% deliverer's pending entries and oldest pending slot, every process
%% worker's mailbox, and the waiter rows of both. Node-wide sums and maxima,
%% read when asked; nothing per process.
metrics() ->
    Now = erlang:monotonic_time(second),
    Backlog =
        [ Row || Row = {_, Pid, _, _} <- table_rows(?BACKLOG), is_process_alive(Pid) ],
    Mailbox =
        lists:sum(
            [ case erlang:process_info(Pid, message_queue_len) of
                  {message_queue_len, N} -> N;
                  undefined -> 0
              end
            || {{process_worker, _}, Pid} <- hb_name:all(), is_pid(Pid) ]),
    [
        {hb_delivery_pending_entries, "Outbox entries not delivered yet.",
            lists:sum([ N || {_, _, N, _} <- Backlog ])},
        {hb_delivery_oldest_pending_seconds, "Age of the oldest undelivered slot.",
            lists:max([0 | [ Now - S || {_, _, _, S} <- Backlog, is_integer(S) ]])},
        {hb_worker_mailbox_len, "Messages queued at process workers.", Mailbox},
        {hb_worker_waiter_rows, "Requests waiting on a slot or a delivery.",
            table_size(dev_process_worker_waiters) + table_size(?WAITERS)}
    ].

table_rows(Name) -> try ets:tab2list(Name) catch error:badarg -> [] end.
table_size(Name) ->
    case ets:info(Name, size) of
        N when is_integer(N) -> N;
        _ -> 0
    end.

%%% The deliverer.

deliverer_opts(Opts) ->
    Opts#{
        <<"await-inprogress">> => false,
        <<"hashpath">> => ignore,
        <<"spawn-worker">> => false
    }.

init(ProcID, Opts, {Caller, Ref}) ->
    process_flag(message_queue_data, off_heap),
    hb_phase_timer:set_process(ProcID),
    case hb_name:register({delivery, ProcID}) of
        error ->
            Caller ! {Ref, {exists, hb_name:lookup({delivery, ProcID})}};
        ok ->
            case start_state(ProcID, Opts) of
                {ok, State} ->
                    Caller ! {Ref, started},
                    ?event(delivery, {deliverer_started, {process, ProcID}}),
                    run(State);
                {error, Reason} ->
                    hb_name:unregister({delivery, ProcID}),
                    ?event(delivery, {deliverer_refused, {process, ProcID}, {reason, Reason}}),
                    Caller ! {Ref, {error, Reason}}
            end
    end.

start_state(ProcID, Opts) ->
    case {store(Opts), hb_cache:read(ProcID, Opts)} of
        {none, _} -> {error, no_delivery_store};
        {Store, {ok, RawProcess}} ->
            Process = hb_cache:ensure_all_loaded(RawProcess, Opts),
            case delivers(ProcID, Process, Opts) of
                false -> {error, not_scheduler};
                true ->
                    {ok,
                        #{
                            proc_id => ProcID,
                            origin => origin(ProcID, Process, Opts),
                            opts => Opts,
                            store => Store
                        }}
            end;
        _ -> {error, process_not_found}
    end.

%% The working state, rebuilt from the rows whenever the deliverer (re)starts:
%%   slots     gb_tree Slot => #{ left, receipts, since }
%%   entries   {Slot, Key} => #{ target, q, delay }: q is `new', `signed' or
%%             `{resign, Codec, Location}'; delay its last backoff
%%   queues    Target => queue of {Slot, Key} that are due, waiting for room
%%   inflight  Target => its posts in flight
%%   owed      Target => its entries not finished
%%   targets   Target => {process, P} | {looking, Delay} | {waiting, Delay}
%%             | missing, while it is owed anything
%%   hints     Target => {Codec, Location} it last asked to be signed with,
%%             kept while the deliverer runs
%%   helpers   Pid => {Monitor, {post, Slot, Key} | {lookup, Target}}
%%   lookups   targets waiting for one of the `?LOOKUPS' lookup places
%%   timers    gb_set of {DueMs, Slot, Key}: entries backing off
%%   touched   targets to move in the next step
%%   signed    Slot => #{ Key => Signed }: signed this step, not in `q' yet
%%   rows      {Kind, Slot} => row, read during one step only
%%   done      slots finished since the last write
%%   unpurged  slots whose `x' is written and `o'/`q' not yet deleted
fresh(State) ->
    State#{
        slots => gb_trees:empty(),
        entries => #{},
        queues => #{},
        inflight => #{},
        owed => #{},
        targets => #{},
        hints => #{},
        helpers => #{},
        lookups => queue:new(),
        free_lookups => ?LOOKUPS,
        timers => gb_sets:empty(),
        touched => #{},
        signed => #{},
        rows => #{},
        done => [],
        unpurged => []
    }.

%% The deliverer, supervised by itself: whatever fails in its loop, it starts
%% again from the rows, after a backoff. Helpers still running from before
%% answer with pids it no longer knows, and are ignored; an entry one of them
%% was posting is posted again, with the same bytes.
run(State) ->
    try loop(step(load_pending(fresh(State))))
    catch Class:Reason:Stack ->
        Delay = next_backoff(get(delivery_restart_backoff)),
        put(delivery_restart_backoff, Delay),
        ?event(warning, {deliverer_failed, {process, maps:get(proc_id, State)},
            {class, Class}, {reason, Reason}, {stack, {trace, Stack}}, {retry_ms, Delay}}),
        record(delivery_restart),
        receive after Delay -> ok end,
        run(State)
    end.

%% @doc The provenance stamps of the process, the same for every entry.
origin(ProcID, Process, Opts) ->
    #{
        <<"process">> => ProcID,
        <<"from-base">> => calculate_base_id(Process, Opts),
        <<"from-uncommitted">> =>
            lib_process:process_id(Process, #{ <<"commitments">> => <<"none">> }, Opts),
        <<"from-scheduler">> => hb_ao:get(<<"scheduler">>, Process, undefined, Opts),
        <<"from-authority">> => hb_ao:get(<<"authority">>, Process, undefined, Opts)
    }.

loop(State) ->
    erase(delivery_restart_backoff),
    receive
        Msg -> loop(step(drain(handle(Msg, State))))
    after timeout(State) ->
        case idle(State) of
            true -> idle_exit(State);
            false -> loop(step(State))
        end
    end.

%% Now when a step left work behind, at the next backoff when entries wait on
%% one, after the idle time when nothing is owed.
timeout(#{ touched := Touched }) when map_size(Touched) > 0 -> 0;
timeout(State = #{ timers := Timers, opts := Opts }) ->
    case idle(State) of
        true -> hb_util:int(hb_opts:get(process_worker_max_idle, 300_000, Opts));
        false ->
            case gb_sets:is_empty(Timers) of
                true -> infinity;
                false ->
                    {Due, _, _} = gb_sets:smallest(Timers),
                    max(0, Due - now_ms())
            end
    end.

%% Everything already queued is handled before the step, so one step -- one
%% commit -- covers all of it.
drain(State) ->
    receive Msg -> drain(handle(Msg, State))
    after 0 -> State
    end.

handle({ready, Slot}, State) -> load_slot(Slot, State);
handle({posted, Pid, ID, Signing, Outcome}, State = #{ helpers := Helpers }) ->
    case maps:take(Pid, Helpers) of
        {{Mon, {post, Slot, Key}}, Rest} ->
            erlang:demonitor(Mon, [flush]),
            outcome(Slot, Key, ID, Signing, Outcome, landed(Slot, Key, State#{ helpers := Rest }));
        _ -> State
    end;
handle({looked_up, Pid, Result}, State = #{ helpers := Helpers }) ->
    case maps:take(Pid, Helpers) of
        {{Mon, {lookup, Target}}, Rest} ->
            erlang:demonitor(Mon, [flush]),
            looked_up(Target, Result, State#{ helpers := Rest });
        _ -> State
    end;
handle({'DOWN', _Mon, process, Pid, Reason}, State = #{ helpers := Helpers, proc_id := ProcID }) ->
    case maps:take(Pid, Helpers) of
        {{_, {post, Slot, Key}}, Rest} ->
            % A post that crashed -- a failed read of its target, a store it
            % could not reach -- backs off that entry alone.
            ?event(warning, {delivery_post_crashed, {process, ProcID}, {slot, Slot},
                {key, Key}, {reason, Reason}}),
            record(delivery_post_crash),
            backoff(Slot, Key, landed(Slot, Key, State#{ helpers := Rest }));
        {{_, {lookup, Target}}, Rest} ->
            looked_up(Target, {error, {lookup_crashed, Reason}}, State#{ helpers := Rest });
        _ -> State
    end;
handle({lookup_retry, Target}, State = #{ targets := Targets }) ->
    case Targets of
        #{ Target := {waiting, Delay} } ->
            touch(Target, State#{ targets := Targets#{ Target := {retry, Delay} } });
        _ -> State
    end;
handle(_Other, State) ->
    State.

idle(#{ slots := Slots, helpers := Helpers }) ->
    gb_trees:is_empty(Slots) andalso map_size(Helpers) == 0.

%% Nothing pending and nothing in flight for the idle time: delete what
%% finished slots left, leave the name, and look once more. A slot cast to
%% the old name after it was left is in the store already (its `o' row is
%% written before the cast), so it is found here or by whoever registers next.
idle_exit(State = #{ proc_id := ProcID, store := Store, opts := Opts, unpurged := Unpurged }) ->
    Unpurged == [] orelse commit_store(State),
    hb_name:unregister({delivery, ProcID}),
    catch ets:delete(?BACKLOG, ProcID),
    case pending_slots(ProcID, Store, Opts) of
        [] -> ok;
        _ ->
            case hb_name:register({delivery, ProcID}) of
                ok -> run(State);
                error -> ok
            end
    end.

load_pending(State = #{ proc_id := ProcID, store := Store, opts := Opts }) ->
    lists:foldl(fun load_slot/2, State, pending_slots(ProcID, Store, Opts)).

%% A slot is loaded once. One whose rows cannot be read is looked at again
%% after a backoff; it holds back no other slot.
load_slot(Slot, State = #{ slots := Slots }) ->
    case gb_trees:is_defined(Slot, Slots) of
        true -> State;
        false ->
            try read_slot(Slot, State)
            catch Class:Reason ->
                ?event(warning, {delivery_slot_unreadable, {slot, Slot},
                    {class, Class}, {reason, Reason}}),
                erlang:send_after(?MAX_BACKOFF_MS, self(), {ready, Slot}),
                State
            end
    end.

%% Only references are kept: each entry's target, and whether `q' holds its
%% signed message already.
read_slot(Slot, State = #{ proc_id := ProcID, store := Store, opts := Opts }) ->
    case receipts(ProcID, Slot, Store, Opts) of
        {ok, _} ->
            % Delivered already: whatever `o' or `q' is left is a leftover.
            State#{ unpurged := [Slot | maps:get(unpurged, State)] };
        not_found ->
            case get_row(ProcID, <<"o">>, Slot, Store, Opts) of
                not_found -> State;
                {ok, Row} ->
                    Signed =
                        case get_row(ProcID, <<"q">>, Slot, Store, Opts) of
                            {ok, Q} -> Q;
                            not_found -> #{}
                        end,
                    add_slot(Slot, lib_process_cache:outbox_entries(Row, Opts), Signed, State)
            end
    end.

add_slot(Slot, Outbox, Signed, State = #{ slots := Slots, done := Done }) ->
    Keys = maps:keys(Outbox),
    State2 =
        State#{
            slots := gb_trees:insert(Slot,
                #{ left => length(Keys), receipts => #{}, since => erlang:monotonic_time(second) },
                Slots),
            % An outbox with no entries owes nothing: it is finished as it is.
            done := case Keys of [] -> [Slot | Done]; _ -> Done end
        },
    lists:foldl(
        fun(Key, Acc = #{ entries := Entries, owed := Owed }) ->
            case entry_target(maps:get(Key, Outbox)) of
                undefined ->
                    receipt(Slot, Key, #{ <<"status">> => <<"terminal">>,
                        <<"reason">> => <<"no_target">> }, Acc);
                Target ->
                    Q = case maps:get(Key, Signed, undefined) of
                            {signed, _, _, _, _} -> signed;
                            _ -> new
                        end,
                    enqueue(Slot, Key,
                        Acc#{
                            entries := Entries#{ {Slot, Key} => #{ target => Target, q => Q } },
                            owed := Owed#{ Target => maps:get(Target, Owed, 0) + 1 }
                        })
            end
        end,
        State2,
        Keys
    ).

entry_target(Msg) when is_map(Msg) ->
    case hb_maps:get(<<"target">>, Msg, undefined, #{}) of
        T when is_binary(T) -> extract(target, T);
        _ -> undefined
    end;
entry_target(_) -> undefined.

target_of(Slot, Key, #{ entries := Entries }) ->
    maps:get(target, maps:get({Slot, Key}, Entries)).

%% An entry is due: it joins its target's queue.
enqueue(Slot, Key, State = #{ queues := Queues }) ->
    Target = target_of(Slot, Key, State),
    touch(Target,
        State#{ queues := Queues#{ Target => queue:in({Slot, Key},
            maps:get(Target, Queues, queue:new())) } }).

touch(Target, State = #{ touched := Touched }) ->
    State#{ touched := Touched#{ Target => true } }.

%%% A step.

%% Move every touched target as far as it goes now: take its due entries up
%% to its room in flight, sign the ones that need it (and a few more behind
%% them), then write the rows and commit them once, and only then post. A
%% signed message is never posted before it is durable.
step(State0) ->
    State1 = due(State0),
    {State2, Posts} =
        maps:fold(fun(Target, _, Acc) -> move(Target, Acc) end,
            {State1#{ touched := #{} }, []}, maps:get(touched, State1)),
    State3 = commit(State2),
    State4 = lists:foldl(fun spawn_post/2, State3, Posts),
    backlog(State4),
    State4#{ signed := #{}, rows := #{} }.

%% The entries whose backoff has run out are due again.
due(State = #{ timers := Timers }) ->
    Now = now_ms(),
    case gb_sets:is_empty(Timers) of
        true -> State;
        false ->
            case gb_sets:take_smallest(Timers) of
                {{Due, Slot, Key}, Rest} when Due =< Now ->
                    due(enqueue(Slot, Key, State#{ timers := Rest }));
                _ -> State
            end
    end.

move(Target, {State = #{ queues := Queues }, Posts}) ->
    case queue:is_empty(maps:get(Target, Queues, queue:new())) of
        true -> {State, Posts};
        false ->
            case target_process(Target, State) of
                {ok, Process, State2} -> take(Target, Process, State2, Posts);
                {terminal, Reason, State2} -> {terminal_all(Target, Reason, State2), Posts};
                {wait, State2} -> {State2, Posts}
            end
    end.

%% Take as many of the target's due entries as it has room for in flight.
take(Target, Process, State = #{ queues := Queues, inflight := Inflight }, Posts) ->
    Room = ?TARGET_INFLIGHT - maps:get(Target, Inflight, 0),
    {Taken, Rest} = split(Room, maps:get(Target, Queues)),
    State2 = State#{ queues := Queues#{ Target := Rest } },
    {State3, Posts2} =
        lists:foldl(fun(Entry, Acc) -> prepare(Target, Process, Entry, Acc) end,
            {State2, Posts}, Taken),
    Entries = maps:get(entries, State),
    case lists:any(fun(E) -> maps:get(q, maps:get(E, Entries)) =/= signed end, Taken) of
        true -> {sign_ahead(Target, Process, queue:to_list(Rest), State3), Posts2};
        false -> {State3, Posts2}
    end.

split(N, Queue) when N =< 0 -> {[], Queue};
split(N, Queue) ->
    {Front, Back} = queue:split(min(N, queue:len(Queue)), Queue),
    {queue:to_list(Front), Back}.

%% An entry about to be posted: signed now if `q' does not hold it yet.
prepare(Target, Process, {Slot, Key}, {State, Posts}) ->
    case maps:get({Slot, Key}, maps:get(entries, State)) of
        #{ q := signed } -> {posting(Target, State), [{Slot, Key} | Posts]};
        #{ q := Q } ->
            case sign(Target, Process, Slot, Key, Q, State) of
                {ok, State2} -> {posting(Target, State2), [{Slot, Key} | Posts]};
                {terminal, Reason, State2} ->
                    {finish(Slot, Key, terminal_receipt(Target, Reason), State2), Posts};
                {retry, State2} ->
                    record(delivery_sign_retry),
                    {backoff(Slot, Key, State2), Posts}
            end
    end.

posting(Target, State = #{ inflight := Inflight }) ->
    State#{ inflight := Inflight#{ Target => maps:get(Target, Inflight, 0) + 1 } }.

%% The target's next unsigned entries are signed with the ones being posted,
%% so the commit before those posts makes them durable too and each of them
%% posts later without a commit of its own.
sign_ahead(Target, Process, Waiting, State) ->
    lists:foldl(
        fun({Slot, Key}, Acc) ->
            case maps:get({Slot, Key}, maps:get(entries, Acc)) of
                #{ q := new } ->
                    case sign(Target, Process, Slot, Key, new, Acc) of
                        {ok, Acc2} -> Acc2;
                        {_, Acc2} -> Acc2
                    end;
                _ -> Acc
            end
        end,
        State,
        lists:sublist(Waiting, ?SIGN_AHEAD)
    ).

%% Sign an entry, as its target last asked to be signed (a new entry) or as
%% the scheduler that sent it back said (`{resign, Codec, Location}'). The
%% signed message goes to the step's `signed' map, which `commit/1' writes to
%% `q': the only place a later post reads it from.
sign(Target, Process, Slot, Key, Q, State = #{ hints := Hints, opts := Opts }) ->
    {Codec, Location} =
        case Q of
            new -> maps:get(Target, Hints, {default_codec(Opts), undefined});
            {resign, C, L} -> {C, L}
        end,
    {Msg, State2} = outbox_entry(Slot, Key, State),
    case sign_entry(Process, Msg, Slot, Key, Codec, Location, State2) of
        Signed = {signed, _, _, _, _} ->
            record(delivery_signed),
            #{ signed := SignedMap, entries := Entries } = State2,
            {ok,
                State2#{
                    signed := SignedMap#{ Slot => (maps:get(Slot, SignedMap, #{}))#{ Key => Signed } },
                    entries := maps:update_with({Slot, Key}, fun(E) -> E#{ q := signed } end, Entries)
                }};
        {terminal, Reason} -> {terminal, Reason, State2};
        retry -> {retry, State2}
    end.

outbox_entry(Slot, Key, State = #{ proc_id := ProcID, store := Store, opts := Opts }) ->
    {Outbox, State2} =
        row(<<"o">>, Slot, State,
            fun() ->
                {ok, Row} = get_row(ProcID, <<"o">>, Slot, Store, Opts),
                lib_process_cache:outbox_entries(Row, Opts)
            end),
    {maps:get(Key, Outbox), State2}.

%% A row read once per step, whatever number of its entries the step needs.
row(Kind, Slot, State = #{ rows := Rows }, Read) ->
    case Rows of
        #{ {Kind, Slot} := Row } -> {Row, State};
        _ ->
            Row = Read(),
            {Row, State#{ rows := Rows#{ {Kind, Slot} => Row } }}
    end.

terminal_receipt(Target, Reason) ->
    #{ <<"target">> => Target, <<"status">> => <<"terminal">>,
        <<"reason">> => hb_util:bin(io_lib:format("~0p", [Reason])) }.

%% Every due entry of a target proven not to exist is finished.
terminal_all(Target, Reason, State = #{ queues := Queues }) ->
    lists:foldl(
        fun({Slot, Key}, Acc) -> finish(Slot, Key, terminal_receipt(Target, Reason), Acc) end,
        State#{ queues := Queues#{ Target := queue:new() } },
        queue:to_list(maps:get(Target, Queues))
    ).

%% An entry is finished: its receipt is recorded, and a target owed nothing
%% more is forgotten -- all but how it asked to be signed, which only the
%% few targets that ever sent an entry back have.
finish(Slot, Key, Receipt, State = #{ entries := Entries, owed := Owed }) ->
    {#{ target := Target }, Rest} = maps:take({Slot, Key}, Entries),
    State2 =
        case maps:get(Target, Owed) of
            1 ->
                #{ queues := Q, inflight := I, targets := T } = State,
                State#{
                    owed := maps:remove(Target, Owed),
                    queues := maps:remove(Target, Q),
                    inflight := maps:remove(Target, I),
                    targets := maps:remove(Target, T)
                };
            N -> State#{ owed := Owed#{ Target := N - 1 } }
        end,
    receipt(Slot, Key, Receipt, State2#{ entries := Rest }).

receipt(Slot, Key, Receipt, State = #{ slots := Slots, done := Done }) ->
    SS = #{ receipts := R, left := Left } = gb_trees:get(Slot, Slots),
    State2 =
        State#{
            slots := gb_trees:update(Slot, SS#{ receipts := R#{ Key => Receipt }, left := Left - 1 }, Slots)
        },
    case Left - 1 of
        0 -> State2#{ done := [Slot | Done] };
        _ -> State2
    end.

%% Write the step's rows: `q' for each slot signed into, `x' for each slot
%% finished. Only a `q' has to be durable before anything else happens --
%% before its POST -- so only a step that signed something commits, and that
%% one commit also carries the `x' rows written since the last one and the
%% deletes of the `o' and `q' rows those `x' rows replace. A receipt needs no
%% commit of its own: lost to a crash, its entries are posted again with the
%% same bytes and the 409s give the same slots back.
commit(State = #{ signed := Signed, done := [] }) when map_size(Signed) == 0 ->
    State;
commit(State = #{ proc_id := ProcID, store := Store, opts := Opts, slots := Slots,
                  signed := Signed, done := Done, unpurged := Unpurged }) ->
    crash_point(before_flush, State),
    maps:foreach(
        fun(Slot, New) ->
            Old =
                case get_row(ProcID, <<"q">>, Slot, Store, Opts) of
                    {ok, Q} -> Q;
                    not_found -> #{}
                end,
            ok = put_row(ProcID, <<"q">>, Slot, maps:merge(Old, New), Store, Opts)
        end,
        Signed
    ),
    State2 =
        lists:foldl(
            fun(Slot, Acc = #{ slots := S }) ->
                #{ receipts := Receipts } = gb_trees:get(Slot, S),
                ok = put_row(ProcID, <<"x">>, Slot, Receipts, Store, Opts),
                ?event(delivery, {delivered, {process, ProcID}, {slot, Slot},
                    {entries, map_size(Receipts)}}),
                wake(ProcID, Slot, Receipts),
                Acc#{ slots := gb_trees:delete(Slot, S) }
            end,
            State#{ done := [], unpurged := Done ++ Unpurged, slots := Slots },
            lists:usort(Done)
        ),
    case map_size(Signed) of
        0 -> State2;
        _ -> commit_store(State2)
    end.

%% Commit every buffered write and delete the `o' and `q' rows of the slots
%% whose `x' is written: one LMDB transaction, so the deletes are never
%% durable without the `x' rows.
commit_store(State = #{ proc_id := ProcID, store := Store, opts := Opts, unpurged := Slots }) ->
    T0 = erlang:monotonic_time(microsecond),
    ok = lib_process_cache:blob_commit(Store,
        [ key(ProcID, Kind, Slot) || Slot <- lists:usort(Slots), Kind <- [<<"q">>, <<"o">>] ],
        Opts),
    hb_phase_timer:record(delivery_flush, erlang:monotonic_time(microsecond) - T0),
    State#{ unpurged := [] }.

%%% Targets.

%% @doc The process message of a target. A target this node holds is read at
%% once; one the gateway said does not exist is a terminal at once; any other
%% is looked up by a monitored helper, one at a time per target and at most
%% `?LOOKUPS' at once, so a slow or unreachable gateway holds back only the
%% entries of the targets it is asked about.
target_process(Target, State = #{ targets := Targets, opts := Opts }) ->
    case maps:get(Target, Targets, undefined) of
        {process, Process} -> {ok, Process, State};
        missing -> {terminal, no_process, State};
        {looking, _} -> {wait, State};
        {waiting, _} -> {wait, State};
        Retry ->
            case hb_cache:read(Target, hb_store:scope(Opts, local)) of
                {ok, Process} ->
                    Loaded = hb_cache:ensure_all_loaded(Process, Opts),
                    {ok, Loaded, State#{ targets := Targets#{ Target => {process, Loaded} } }};
                _ ->
                    case recently_missing(Target, missing_ttl(Opts)) of
                        true -> {terminal, no_process, State};
                        false ->
                            Delay = case Retry of {retry, D} -> D; _ -> undefined end,
                            {wait, lookup(Target, Delay, State)}
                    end
            end
    end.

missing_ttl(Opts) ->
    hb_opts:get(<<"push-missing-target-ttl">>, ?DEFAULT_MISSING_TARGET_TTL, Opts).

lookup(Target, Delay, State = #{ targets := Targets, free_lookups := 0, lookups := Waiting }) ->
    State#{ targets := Targets#{ Target => {looking, Delay} }, lookups := queue:in(Target, Waiting) };
lookup(Target, Delay, State = #{ targets := Targets, free_lookups := Free, helpers := Helpers, opts := Opts }) ->
    ServerID = get(server_id),
    Self = self(),
    {Pid, Mon} =
        spawn_monitor(fun() ->
            put(server_id, ServerID),
            Self ! {looked_up, self(), read_target(Target, Opts)}
        end),
    State#{
        targets := Targets#{ Target => {looking, Delay} },
        free_lookups := Free - 1,
        helpers := Helpers#{ Pid => {Mon, {lookup, Target}} }
    }.

%% A lookup answered: its place goes to the next target waiting for one.
looked_up(Target, Result, State = #{ targets := Targets, opts := Opts, free_lookups := Free }) ->
    State2 =
        case Targets of
            #{ Target := {looking, Delay} } ->
                case Result of
                    {ok, Process} ->
                        touch(Target, State#{ targets := Targets#{ Target :=
                            {process, hb_cache:ensure_all_loaded(Process, Opts)} } });
                    {error, Miss} when Miss == not_found; Miss == recently_not_found ->
                        % A gateway says there is no such target: a proven
                        % terminal for every entry of it.
                        touch(Target, State#{ targets := Targets#{ Target := missing } });
                    _ ->
                        % The gateway could not answer: ask again later. The
                        % entries wait unsigned; nothing is given up.
                        record(delivery_lookup_retry),
                        Next = next_backoff(Delay),
                        erlang:send_after(Next, self(), {lookup_retry, Target}),
                        State#{ targets := Targets#{ Target := {waiting, Next} } }
                end;
            _ -> State
        end,
    next_lookup(State2#{ free_lookups := Free + 1 }).

next_lookup(State = #{ lookups := Waiting, targets := Targets }) ->
    case queue:out(Waiting) of
        {empty, _} -> State;
        {{value, Target}, Rest} ->
            case Targets of
                #{ Target := {looking, Delay} } ->
                    lookup(Target, Delay, State#{ lookups := Rest });
                _ -> next_lookup(State#{ lookups := Rest })
            end
    end.

next_backoff(undefined) -> ?FIRST_BACKOFF_MS;
next_backoff(Prev) -> min(?MAX_BACKOFF_MS, Prev * 2).

%% An entry backs off on its own, from its own last delay.
backoff(Slot, Key, State = #{ entries := Entries, timers := Timers }) ->
    Entry = maps:get({Slot, Key}, Entries),
    Delay = next_backoff(maps:get(delay, Entry, undefined)),
    State#{
        entries := Entries#{ {Slot, Key} := Entry#{ delay => Delay } },
        timers := gb_sets:add({now_ms() + Delay, Slot, Key}, Timers)
    }.

now_ms() -> erlang:monotonic_time(millisecond).

record(Counter) -> hb_phase_timer:record(Counter, 0).

default_codec(Opts) ->
    hb_opts:get(scheduler_default_commitment_spec, <<"httpsig@1.0">>, Opts).

%%% Signing.

%% @doc Sign one outbox entry for its target: evaluate it if it asks for an
%% evaluation, stamp its provenance, and commit it under the target's policy.
%% A signing that fails is retried (`retry'): it may have failed for a reason
%% that passes (a store read, a policy that could not be resolved).
sign_entry(TargetProcess, RawMsg, Slot, Key, Codec, Location, State = #{ origin := Origin0, opts := Opts }) ->
    try
        crash_point(sign, State),
        Target = entry_target(RawMsg),
        MsgToPush =
            case maybe_evaluate_message(RawMsg, Opts) of
                {ok, R} -> R;
                Err ->
                    #{
                        <<"resolve">> => <<"error">>,
                        <<"target">> => maps:get(<<"process">>, Origin0),
                        <<"status">> => 400,
                        <<"outbox-index">> => Key,
                        <<"reason">> => Err,
                        <<"source">> => RawMsg
                    }
            end,
        Origin =
            Origin0#{
                <<"from-slot">> => Slot,
                <<"from-outbox-key">> => Key,
                <<"from-entry">> => hb_message:id(RawMsg, all, Opts)
            },
        Augmented =
            case Location of
                undefined -> augment_message(Origin, MsgToPush, Opts);
                _ -> normalize_message(augment_message(Origin, MsgToPush, Opts), Opts)
            end,
        Signed = apply_security(Augmented, TargetProcess, Codec, Opts),
        case hb_message:signers(Signed, Opts) of
            [] -> {terminal, no_signer};
            _ ->
                {signed, Target, Codec, Location,
                    hb_cache:ensure_all_loaded(Signed, Opts)}
        end
    catch
        Class:Reason:Stack ->
            ?event(delivery, {sign_failed, {slot, Slot}, {key, Key},
                {class, Class}, {reason, Reason}, {stack, {trace, Stack}}}),
            retry
    end.

%%% Posting.

%% A post runs in a monitored helper with the signed message as `q' holds it:
%% from this step's commit, or read from the row.
spawn_post({Slot, Key}, State = #{ opts := Opts, targets := Targets, helpers := Helpers }) ->
    {{signed, Target, Codec, Location, Signed}, State2} = signed_message(Slot, Key, State),
    Self = self(),
    TargetProcess =
        case maps:get(Target, Targets, undefined) of
            {process, P} -> P;
            _ -> undefined
        end,
    ServerID = get(server_id),
    Post = post_fun(Opts),
    % Only this node's own scheduler can refuse bytes for good: a target it
    % schedules, posted here. Any other answer -- a remote scheduler's, even
    % when this node proxied the post to it -- can be retried.
    Where =
        case Location == undefined andalso schedules(Target, TargetProcess, Opts) of
            true -> local;
            false -> remote
        end,
    {Pid, Mon} =
        spawn_monitor(fun() ->
            put(server_id, ServerID),
            Outcome = classify(Post(TargetProcess, Target, Location, Signed, Opts), Where, Opts),
            Self ! {posted, self(), hb_message:id(Signed, all, Opts), {Codec, Location}, Outcome}
        end),
    State2#{ helpers := Helpers#{ Pid => {Mon, {post, Slot, Key}} } }.

signed_message(Slot, Key, State = #{ signed := Signed, proc_id := ProcID, store := Store, opts := Opts }) ->
    case Signed of
        #{ Slot := #{ Key := Msg } } -> {Msg, State};
        _ ->
            {Q, State2} =
                row(<<"q">>, Slot, State,
                    fun() ->
                        {ok, Row} = get_row(ProcID, <<"q">>, Slot, Store, Opts),
                        Row
                    end),
            {maps:get(Key, Q), State2}
    end.

%% A post came back: its place in flight is free.
landed(Slot, Key, State = #{ inflight := Inflight }) ->
    Target = target_of(Slot, Key, State),
    touch(Target, State#{ inflight := maps:update_with(Target, fun(N) -> N - 1 end, Inflight) }).

-ifdef(TEST).
%% A test may stand in for the network: `delivery-test-post' is called as
%% `Fun(TargetProcess, Target, Location, Signed, Opts, RealPost)'.
post_fun(Opts) ->
    case hb_opts:get(<<"delivery-test-post">>, undefined, Opts) of
        Fun when is_function(Fun, 6) ->
            fun(TP, T, L, S, O) -> Fun(TP, T, L, S, O, fun post_signed/5) end;
        _ -> fun post_signed/5
    end.

%% A test may stop the deliverer dead at a point of its work, as a crash or a
%% `kill -9' of the node would: `delivery-test-crash' names the point. With
%% `{raise, Point, Counter}' the deliverer fails there once, as a bug would.
crash_point(Point, #{ opts := Opts }) ->
    case hb_opts:get(<<"delivery-test-crash">>, undefined, Opts) of
        Point -> exit(self(), kill);
        {raise, Point, Counter} ->
            case counters:get(Counter, 1) of
                0 -> counters:add(Counter, 1, 1), erlang:error(test_crash);
                _ -> ok
            end;
        _ -> ok
    end.
-else.
post_fun(_Opts) -> fun post_signed/5.
crash_point(_Point, _State) -> ok.
-endif.

%% @doc Post a signed message: to the target's scheduler through this node
%% (which proxies it to a remote scheduler the node routes to), or to the
%% remote location a 307 named.
post_signed(TargetProcess, Target, undefined, Signed, Opts) ->
    Process =
        case TargetProcess of
            undefined ->
                {ok, P} = read_target(Target, Opts),
                P;
            P -> P
        end,
    hb_ao:resolve(
        {as, <<"process@1.0">>, Process},
        #{ <<"path">> => <<"schedule">>, <<"method">> => <<"POST">>, <<"body">> => Signed },
        Opts#{ <<"cache-control">> => [<<"no-store">>] }
    );
post_signed(_TargetProcess, _Target, Location, Signed, Opts) ->
    {Node, RedirectPath} = parse_redirect(Location, Opts),
    Path =
        case find_type(Signed, Opts) of
            <<"Process">> -> <<"/schedule">>;
            _ -> RedirectPath
        end,
    hb_http:post(Node, Path, hb_maps:without([<<"path">>], Signed, Opts), Opts).

%% @doc What a scheduler's answer to a POST means for the entry. Only an
%% answer that proves the message was assigned (with its integer slot), or
%% that this node's own scheduler (`local') refuses these exact bytes for
%% good, finishes it. A 307 or 422 asks for it to be signed again. Anything
%% else is retried.
classify({ok, Res}, Where, Opts) when is_map(Res) ->
    by_status(status(Res, 200, Opts), Where, Res, Opts);
classify({error, Res}, Where, Opts) when is_map(Res) ->
    by_status(status(Res, undefined, Opts), Where, Res, Opts);
classify(_, _Where, _Opts) -> retry.

by_status(200, _Where, Res, Opts) -> assigned(assigned, Res, Opts);
by_status(409, _Where, Res, Opts) -> assigned(already, Res, Opts);
by_status(307, _Where, Res, Opts) ->
    case hb_ao:get(<<"location">>, Res, not_found, Opts) of
        Location when is_binary(Location) -> {redirect, Location};
        _ -> retry
    end;
by_status(422, _Where, _Res, _Opts) -> downgrade;
by_status(S, local, _Res, _Opts) when S == 400; S == 413 -> {terminal, S};
by_status(_, _Where, _Res, _Opts) -> retry.

%% A receipt carries the slot the target gave, as an integer, or there is no
%% receipt yet: the repost is answered 409 with it.
assigned(Kind, Res, Opts) ->
    case hb_ao:get(<<"slot">>, Res, undefined, Opts) of
        N when is_integer(N), N >= 0 -> {Kind, N};
        B when is_binary(B) ->
            case hb_util:safe_int(B) of
                {ok, N} when N >= 0 -> {Kind, N};
                _ -> retry
            end;
        _ -> retry
    end.

status(Res, Default, Opts) ->
    case hb_ao:get(<<"status">>, Res, Default, Opts) of
        S when is_integer(S) -> S;
        S when is_binary(S) ->
            case hb_util:safe_int(S) of {ok, I} -> I; _ -> Default end;
        Other -> Other
    end.

outcome(Slot, Key, ID, {Codec, Location}, Outcome, State = #{ proc_id := ProcID, hints := Hints }) ->
    Target = target_of(Slot, Key, State),
    ?event(delivery, {posted, {process, ProcID}, {slot, Slot}, {key, Key},
        {target, Target}, {outcome, Outcome}}),
    record(outcome_counter(Outcome)),
    crash_point(after_post, State),
    case Outcome of
        {Assigned, AssignedSlot} when Assigned == assigned; Assigned == already ->
            finish(Slot, Key,
                #{ <<"id">> => ID, <<"target">> => Target, <<"slot">> => AssignedSlot },
                State);
        {terminal, Status} ->
            finish(Slot, Key,
                #{ <<"id">> => ID, <<"target">> => Target,
                    <<"status">> => <<"terminal">>, <<"code">> => Status },
                State);
        {redirect, NewLocation} ->
            % Not assigned: signed again for the location, like a new entry,
            % and so is every later entry of this target.
            resign(Slot, Key, {Codec, NewLocation}, State#{ hints := Hints#{ Target => {Codec, NewLocation} } });
        downgrade when Codec =/= <<"ans104@1.0">> ->
            Signing = {<<"ans104@1.0">>, Location},
            resign(Slot, Key, Signing, State#{ hints := Hints#{ Target => Signing } });
        _ ->
            backoff(Slot, Key, State)
    end.

resign(Slot, Key, {Codec, Location}, State = #{ entries := Entries }) ->
    enqueue(Slot, Key,
        State#{ entries := maps:update_with({Slot, Key},
            fun(E) -> E#{ q := {resign, Codec, Location} } end, Entries) }).

outcome_counter({assigned, _}) -> delivery_assigned;
outcome_counter({already, _}) -> delivery_already;
outcome_counter({terminal, _}) -> delivery_terminal;
outcome_counter({redirect, _}) -> delivery_resign;
outcome_counter(downgrade) -> delivery_resign;
outcome_counter(retry) -> delivery_retry.

wake(ProcID, Slot, Receipts) ->
    ensure_table(?WAITERS, ordered_set),
    wake(ProcID, Slot, Receipts, ets:next(?WAITERS, {ProcID, Slot, 0})).

%% Every reference sorts after 0, so the walk visits only this slot's rows.
wake(ProcID, Slot, Receipts, Key = {ProcID, Slot, Ref}) ->
    Next = ets:next(?WAITERS, Key),
    case ets:lookup(?WAITERS, Key) of
        [{_, Pid}] -> Pid ! {delivered, Ref, Receipts};
        [] -> ok
    end,
    ets:delete(?WAITERS, Key),
    wake(ProcID, Slot, Receipts, Next);
wake(_ProcID, _Slot, _Receipts, _) ->
    ok.

%% This deliverer's row of the node's backlog (`metrics/0').
backlog(#{ proc_id := ProcID, slots := Slots, entries := Entries }) ->
    ensure_table(?BACKLOG, set),
    Oldest =
        case gb_trees:is_empty(Slots) of
            true -> none;
            false -> maps:get(since, element(2, gb_trees:smallest(Slots)))
        end,
    ets:insert(?BACKLOG, {ProcID, self(), map_size(Entries), Oldest}).

%%% Rows.

store(Opts) -> lib_process_cache:delivery_store(Opts).

key(ProcID, Kind, Slot) -> lib_process_cache:blob_key(ProcID, Kind, Slot).

put_row(ProcID, Kind, Slot, Term, Store, Opts) ->
    lib_process_cache:blob_put(Store, key(ProcID, Kind, Slot),
        lib_process_cache:blob_encode(Term), Opts).

get_row(ProcID, Kind, Slot, Store, Opts) ->
    case lib_process_cache:blob_get(Store, key(ProcID, Kind, Slot), Opts) of
        {ok, Bin} -> {ok, lib_process_cache:blob_decode(Bin)};
        not_found -> not_found
    end.

has_row(ProcID, Kind, Slot, Store, Opts) ->
    get_row(ProcID, Kind, Slot, Store, Opts) =/= not_found.

receipts(ProcID, Slot, Store, Opts) ->
    get_row(ProcID, <<"x">>, Slot, Store, Opts).

%% The `o' rows with no `x': `o' rows are few (they go once delivered), and
%% `x' rows stay as long as their slot's state, so they are read one by one,
%% never listed.
pending_slots(ProcID, Store, Opts) ->
    [ S || S <- lib_process_cache:blob_slots(ProcID, <<"o">>, Store),
           not has_row(ProcID, <<"x">>, S, Store, Opts) ].

%% The table is owned by a process that never exits: a table dies with its
%% owner, and the first caller is usually short-lived.
ensure_table(Name, Type) ->
    case ets:whereis(Name) of
        undefined ->
            Alias = erlang:alias([reply]),
            {Owner, Mon} =
                spawn_monitor(fun() ->
                    try ets:new(Name, [named_table, public, Type]) of
                        _ ->
                            Alias ! {Alias, created},
                            receive after infinity -> ok end
                    catch error:badarg -> Alias ! {Alias, exists}
                    end
                end),
            receive
                {Alias, _} -> ok;
                {'DOWN', Mon, process, Owner, _} -> ok
            end,
            erlang:unalias(Alias),
            erlang:demonitor(Mon, [flush]),
            ok;
        _ -> ok
    end.

%%% Target lookups.

%% @doc Read an outbox target (see `?MISSING_TARGETS'). A remembered miss is
%% read from the local stores only and gives `{error, recently_not_found}'; a
%% local hit clears the entry. A gateway that cannot be reached is
%% `{error, gateway_unavailable}' -- a reason to retry, never a terminal miss.
read_target(Target, Opts) ->
    TTL = hb_opts:get(<<"push-missing-target-ttl">>, ?DEFAULT_MISSING_TARGET_TTL, Opts),
    case recently_missing(Target, TTL) of
        true ->
            case hb_cache:read(Target, hb_store:scope(Opts, local)) of
                {ok, _} = Found ->
                    forget_missing(Target),
                    Found;
                _ -> {error, recently_not_found}
            end;
        false ->
            case hb_cache:read(Target, Opts#{ <<"gateway-report-unavailable">> => true }) of
                {error, not_found} ->
                    remember_missing(Target, TTL),
                    {error, not_found};
                not_found ->
                    remember_missing(Target, TTL),
                    {error, not_found};
                Other -> Other
            end
    end.

forget_missing(Target) ->
    try ets:delete(?MISSING_TARGETS, Target) catch error:badarg -> true end,
    ok.

recently_missing(Target, TTL) when ?IS_ID(Target), is_integer(TTL), TTL > 0 ->
    try ets:lookup(?MISSING_TARGETS, Target) of
        [{Target, At}] -> erlang:monotonic_time(second) - At < TTL;
        [] -> false
    catch error:badarg -> false
    end;
recently_missing(_, _) -> false.

remember_missing(Target, TTL) when ?IS_ID(Target), is_integer(TTL), TTL > 0 ->
    ensure_missing_table(),
    Now = erlang:monotonic_time(second),
    try
        case ets:info(?MISSING_TARGETS, size) >= ?MISSING_TARGETS_MAX of
            true ->
                ets:select_delete(
                    ?MISSING_TARGETS,
                    [{{'_', '$1'}, [{'=<', '$1', Now - TTL}], [true]}]
                );
            false -> ok
        end,
        case ets:info(?MISSING_TARGETS, size) < ?MISSING_TARGETS_MAX of
            true -> ets:insert(?MISSING_TARGETS, {Target, Now});
            false -> ok
        end
    catch error:badarg -> ok
    end,
    ok;
remember_missing(_, _) -> ok.

ensure_missing_table() ->
    case ets:whereis(?MISSING_TARGETS) of
        undefined ->
            Alias = erlang:alias([reply]),
            {Owner, Mon} =
                spawn_monitor(
                    fun() ->
                        try ets:new(?MISSING_TARGETS, [named_table, public, set, {read_concurrency, true}]) of
                            _ ->
                                Alias ! {Alias, created},
                                receive after infinity -> ok end
                        catch error:badarg -> Alias ! {Alias, exists}
                        end
                    end
                ),
            receive
                {Alias, _} -> ok;
                {'DOWN', Mon, process, Owner, _} -> ok
            after 5000 -> ok
            end,
            erlang:unalias(Alias),
            erlang:demonitor(Mon, [flush]),
            receive {Alias, _} -> ok after 0 -> ok end,
            ok;
        _ -> ok
    end.

%%% Signing (moved from `dev_push').

%% @doc If the outbox message has a `resolve' path, evaluate it and schedule
%% the result, keeping the message's `target'.
maybe_evaluate_message(Message, Opts) ->
    case hb_ao:get(<<"resolve">>, Message, Opts) of
        not_found ->
            {ok, Message};
        ResolvePath ->
            ReqMsg = maps:without([<<"target">>], Message),
            ResolveOpts = Opts#{ <<"force-message">> => true },
            case hb_ao:resolve(ReqMsg#{ <<"path">> => ResolvePath }, ResolveOpts) of
                {ok, EvalRes} ->
                    {ok, EvalRes#{ <<"target">> => hb_ao:get(<<"target">>, Message, Opts) }};
                Err -> Err
            end
    end.

%% @doc Strip a target's hint, as a 307 resolves it.
normalize_message(MsgToPush, Opts) ->
    hb_ao:set(
        MsgToPush,
        #{ <<"target">> => target_id(MsgToPush, Opts) },
        Opts#{ <<"hashpath">> => ignore }
    ).

target_id(MsgToPush, Opts) ->
    case hb_ao:get(<<"target">>, MsgToPush, Opts) of
        not_found -> undefined;
        RawTarget -> extract(target, RawTarget)
    end.

extract(target, Raw) ->
    {Target, _} = split_target(Raw),
    Target.

split_target(RawTarget) ->
    case binary:split(RawTarget, [<<"?">>, <<"&">>]) of
        [Target, QStr] -> {Target, QStr};
        _ -> {RawTarget, <<>>}
    end.

%% @doc The base ID of a process: its uncommitted ID without its `authority'
%% and `scheduler' keys.
calculate_base_id(GivenProcess, Opts) ->
    Process =
        case hb_ao:get(<<"process">>, GivenProcess, Opts#{ <<"hashpath">> => ignore }) of
            not_found -> GivenProcess;
            Proc -> Proc
        end,
    BaseProcess =
        hb_ao:set(
            Process,
            #{ <<"authority">> => unset, <<"scheduler">> => unset },
            Opts#{ <<"hashpath">> => ignore }
        ),
    {ok, BaseID} =
        hb_ao:resolve(
            BaseProcess,
            #{ <<"path">> => <<"id">>, <<"committers">> => <<"none">> },
            Opts
        ),
    BaseID.

%% @doc Set the keys that tell the recipient where the message came from.
%% `from-slot' is the sender's slot whose computation emitted the message and
%% `from-outbox-key' its key in that slot's outbox, so (`from-process',
%% `from-slot', `from-outbox-key') names exactly one emitted message.
%% `from-entry' is that entry's ID as emitted, before these keys were added.
augment_message(Origin, ToSched, Opts) ->
    hb_message:uncommitted(
        hb_ao:set(
            ToSched,
            #{
                <<"data-protocol">> => <<"ao">>,
                <<"variant">> => <<"ao.N.1">>,
                <<"type">> => <<"Message">>,
                <<"from-process">> => maps:get(<<"process">>, Origin),
                <<"from-slot">> => maps:get(<<"from-slot">>, Origin),
                <<"from-outbox-key">> => maps:get(<<"from-outbox-key">>, Origin),
                <<"from-entry">> => maps:get(<<"from-entry">>, Origin),
                <<"from-uncommitted">> => maps:get(<<"from-uncommitted">>, Origin),
                <<"from-base">> => maps:get(<<"from-base">>, Origin),
                <<"from-scheduler">> => maps:get(<<"from-scheduler">>, Origin),
                <<"from-authority">> => maps:get(<<"from-authority">>, Origin)
            },
            Opts#{ <<"hashpath">> => ignore }
        )
    ).

%% @doc Apply the recipient's security policy to the message: its `policy',
%% else its `authority' (one committer or a comma separated list), else the
%% node's default wallet.
apply_security(Msg, TargetProcess, Codec, Opts) ->
    hb_message:paranoid_verify(push_result, hb_message:uncommitted(Msg), Opts),
    apply_security(policy, Msg, TargetProcess, Codec, Opts).
apply_security(policy, Msg, TargetProcess, Codec, Opts) ->
    case hb_ao:get(<<"policy">>, TargetProcess, not_found, Opts) of
        not_found -> apply_security(authority, Msg, TargetProcess, Codec, Opts);
        Policy ->
            case hb_ao:resolve(Policy, Opts) of
                {ok, PolicyOpts} ->
                    case hb_ao:get(<<"accept-committers">>, PolicyOpts, Opts) of
                        not_found ->
                            apply_security(authority, Msg, TargetProcess, Codec, Opts);
                        Committers ->
                            commit_result(Msg, Committers, Codec, Opts)
                    end;
                {error, _Error} ->
                    apply_security(authority, Msg, TargetProcess, Codec, Opts)
            end
    end;
apply_security(authority, Msg, TargetProcess, Codec, Opts) ->
    case hb_ao:get(<<"authority">>, TargetProcess, Opts) of
        not_found -> apply_security(default, Msg, TargetProcess, Codec, Opts);
        Authorities when is_list(Authorities) ->
            commit_result(
                Msg,
                [hb_committer:uri(A, Opts) || A <- Authorities],
                Codec,
                Opts
            );
        Authority ->
            commit_result(
                Msg,
                [ hb_committer:uri(A, Opts) || A <- hb_util:binary_to_strings(Authority) ],
                Codec,
                Opts
            )
    end;
apply_security(default, Msg, _TargetProcess, Codec, Opts) ->
    commit_result(Msg, [hb_opts:committer(Opts)], Codec, Opts).

commit_result(Msg, [], Codec, Opts) ->
    case hb_opts:get(push_always_sign, true, Opts) of
        true -> hb_message:commit(hb_message:uncommitted(Msg), Opts, Codec);
        false -> Msg
    end;
commit_result(Msg, Committers, Codec, Opts) ->
    Signed =
        lists:foldl(
            fun(Committer, Acc) ->
                case hb_opts:as(Committer, Opts) of
                    {ok, CommitterOpts} -> hb_message:commit(Acc, CommitterOpts, Codec);
                    {error, not_found} ->
                        ?event(push, {policy_warning, {unknown_committer, Committer}}, Opts),
                        Acc
                end
            end,
            hb_message:uncommitted(Msg),
            Committers
        ),
    case hb_message:signers(Signed, Opts) of
        [] -> commit_result(Msg, [], Codec, Opts);
        _ -> Signed
    end.

%% @doc Post a message to the scheduler a `location' names, following its
%% redirects. Used for a push's initial message, not for outbox entries.
remote_schedule_result(Location, SignedReq, Opts) ->
    {Node, RedirectPath} = parse_redirect(Location, Opts),
    Path =
        case find_type(SignedReq, Opts) of
            <<"Process">> -> <<"/schedule">>;
            _ -> RedirectPath
        end,
    case hb_http:post(Node, Path, hb_maps:without([<<"path">>], SignedReq, Opts), Opts) of
        {ok, Res} ->
            case hb_ao:get(<<"status">>, Res, 200, Opts) of
                200 -> {ok, Res};
                307 ->
                    remote_schedule_result(hb_ao:get(<<"location">>, Res, Opts), SignedReq, Opts)
            end;
        {error, Res} ->
            {error, Res}
    end.

find_type(Req, Opts) ->
    hb_ao:get_first([{Req, <<"type">>}, {Req, <<"body/type">>}], Opts).

%% @doc The node and path a scheduler's `location' gives. If
%% `hb_hostname:is_blocked/2' blocks its host, throw `blocked_host'.
parse_redirect(Location, Opts) ->
    case hb_hostname:is_blocked(Location, Opts) of
        true -> throw(blocked_host);
        false -> ok
    end,
    Parsed = uri_string:parse(Location),
    Node =
        uri_string:recompose(
            (hb_maps:remove(query, Parsed, Opts))#{ path => <<"/schedule">> }
        ),
    {Node, hb_maps:get(path, Parsed, undefined, Opts)}.

%%% Tests

classify_test() ->
    O = #{},
    L = fun(R) -> classify(R, local, O) end,
    Rm = fun(R) -> classify(R, remote, O) end,
    % Assigned: the receipt carries the slot as an integer, whatever the
    % scheduler spelled it as; a 200 with no slot is no receipt yet.
    ?assertEqual({assigned, 4}, L({ok, #{ <<"status">> => 200, <<"slot">> => 4 }})),
    ?assertEqual({assigned, 4}, L({ok, #{ <<"slot">> => 4 }})),
    ?assertEqual({assigned, 4}, Rm({ok, #{ <<"status">> => <<"200">>, <<"slot">> => <<"4">> }})),
    ?assertEqual(retry, L({ok, #{ <<"status">> => 200 }})),
    ?assertEqual(retry, Rm({ok, #{ <<"slot">> => <<"x">> }})),
    ?assertEqual(retry, Rm({ok, <<"an html page">>})),
    ?assertEqual({already, 3},
        Rm({error, #{ <<"status">> => 409, <<"slot">> => <<"3">>,
            <<"body">> => <<"Message already scheduled.">> }})),
    ?assertEqual({redirect, <<"http://x/y">>},
        L({ok, #{ <<"status">> => 307, <<"location">> => <<"http://x/y">> }})),
    ?assertEqual(retry, L({ok, #{ <<"status">> => 307 }})),
    ?assertEqual(downgrade, Rm({error, #{ <<"status">> => 422 }})),
    % Only this node's own scheduler can refuse bytes for good.
    [ ?assertEqual({terminal, S}, L({error, #{ <<"status">> => S }})) || S <- [400, 413] ],
    [ ?assertEqual(retry, Rm({error, #{ <<"status">> => S }})) || S <- [400, 401, 403, 413] ],
    % Nothing that might yet be assigned is ever finished.
    [ ?assertEqual(retry, L({error, #{ <<"status">> => S }}))
    || S <- [401, 403, 404, 405, 408, 410, 425, 429, 500, 502, 503, 504] ],
    ?assertEqual(retry, L({error, #{ <<"status">> => 404,
        <<"body">> => <<"Cannot fully load message to schedule.">> }})),
    ?assertEqual(retry, L({ok, #{ <<"status">> => 202, <<"slot">> => 1 }})),
    ?assertEqual(retry, L({error, timeout})),
    ?assertEqual(retry, Rm({error, scheduler_timeout})),
    ?assertEqual(retry, Rm({failure, econnrefused})).

missing_target_memory_test() ->
    ID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assertNot(recently_missing(ID, 600)),
    remember_missing(ID, 600),
    ?assert(recently_missing(ID, 600)),
    ?assertNot(recently_missing(ID, 0)),
    forget_missing(ID),
    ?assertNot(recently_missing(ID, 600)).

%%% @doc A per-message trace: where each message's milliseconds went on this
%%% node, one row per event, in a bounded ring served read-only on
%%% `/~hyperbuddy@1.0/trace?since=<cursor>&limit=N'.
%%%
%%% Three kinds of row, all keyed by process and slot so a collector joins
%%% them to each other and to the client that signed the message (`mid' is
%%% the message's signed ID, the ID the client computed):
%%%
%%%   `h' (http)       the signed HTTP request carrying it arrived, its body
%%%                    was read, and it was answered (by `mid');
%%%   `s' (scheduled)  the scheduler queued, assigned and made it durable;
%%%   `c' (computed)   the process computed it: queue wait, every phase of
%%%                    the slot, when its state became readable, and the
%%%                    load (backlog, mailbox, computes in flight, run queue)
%%%                    when it started;
%%%   `d' (delivered)  one outbox entry of a slot was posted to its target and
%%%                    answered (`lib_process_delivery'): `(target, tslot)' is
%%%                    the child of `(pid, slot)', the edge a flow follows;
%%%   `e' (event)      a warning or error event, with the process and slot it
%%%                    names (`hb_event').
%%%
%%% A computed slot's phases come from the notes `hb_slot_log:run/1' collects
%%% (Lua prep, call, decode, apply, GC; LMDB put, commit, scan), from the
%%% execution and the store separately, so `store_commit' is the LMDB commit
%%% and sync the store phase waited on and `store_put' the writes it buffered.
%%%
%%% The ring holds `msg-trace-size' rows (default 32,768; 1,024-262,144). A
%%% row is a tuple of integers and at most three binaries (process ID,
%%% message ID or target, action of at most 64 bytes): under 700 bytes, so
%%% the default ring stays under ~22 MB. `msg-trace' false records nothing;
%%% the option is read with the phase timers (`hb_phase_timer:configure/1'),
%%% and tracing is off whenever they are. Recording is one counter update and
%%% one insert. Keys and actions are binaries, never atoms.
-module(hb_msg_trace).
-export([configure/1, enabled/0, new_tables/0, reset/0, sweep/0]).
-export([open/2, opened/0, close/10, abandon/0]).
-export([scheduled/2, durable/2, delivered/7, event/4]).
-export([arrived/0, body_read/0, replied/1]).
-export([read/2, report/2, signed_id/1]).
-include_lib("eunit/include/eunit.hrl").

-define(T, hb_msg_trace).
-define(LIVE, hb_msg_trace_live).
-define(CONFIG, {?MODULE, config}).
-define(DEFAULT_SIZE, 32768).
-define(MIN_SIZE, 1024).
-define(MAX_SIZE, 262144).
-define(MAX_LIMIT, 5000).
-define(OPEN, {?MODULE, open}).
-define(SCHED, {?MODULE, sched}).
-define(ARRIVED, {?MODULE, arrived}).
%% A row whose sequence number is taken but not yet written is waited for,
%% unless this many newer rows have been written since: then it is skipped.
-define(GAP_SKIP, 256).

%%% Configuration.

%% @doc Read `msg-trace' and `msg-trace-size'. Called by
%% `hb_phase_timer:configure/1'.
configure(Opts) ->
    On =
        case hb_opts:get(<<"msg-trace">>, true, Opts) of
            false -> false;
            <<"false">> -> false;
            _ -> true
        end,
    Size =
        case catch hb_util:int(hb_opts:get(<<"msg-trace-size">>, ?DEFAULT_SIZE, Opts)) of
            N when is_integer(N) -> max(?MIN_SIZE, min(?MAX_SIZE, N));
            _ -> ?DEFAULT_SIZE
        end,
    Config = {On, Size},
    case config() of
        Config -> ok;
        _ ->
            persistent_term:put(?CONFIG, Config),
            % A smaller ring keeps none of the bigger one's rows.
            reset()
    end,
    ok.

config() -> persistent_term:get(?CONFIG, {true, ?DEFAULT_SIZE}).

%% @doc Whether a row would be recorded now.
enabled() ->
    element(1, config()) andalso hb_phase_timer:enabled().

ring_size() -> element(2, config()).

%% @doc Create the tables. Only `hb_phase_timer''s owner calls this.
new_tables() ->
    ets:new(?T, [named_table, public, set, {write_concurrency, true},
        {read_concurrency, true}]),
    ets:new(?LIVE, [named_table, public, set, {write_concurrency, true}]).

%% @doc Forget every row.
reset() ->
    try ets:delete_all_objects(?T) catch error:badarg -> true end,
    try ets:delete_all_objects(?LIVE) catch error:badarg -> true end,
    erase(?OPEN),
    ok.

%% @doc Forget the computes in flight of Erlang processes that died in one.
sweep() ->
    try
        [ ets:delete(?LIVE, P) || {P, _, _, _} <- ets:tab2list(?LIVE),
            not is_process_alive(P) ]
    catch error:badarg -> ok
    end,
    ok.

%%% Recording a computed slot.

%% @doc A compute of `Slot' of `ProcID' starts: it is about to read the slot's
%% assignment. The load is read now, before the slot changes it.
open(ProcID, Slot) ->
    case enabled() of
        false -> ok;
        true ->
            Now = erlang:system_time(millisecond),
            Inflight =
                try
                    ets:insert(?LIVE, {self(), ProcID, Slot, Now}),
                    ets:info(?LIVE, size)
                catch error:badarg -> null
                end,
            put(?OPEN, {ProcID, Slot, erlang:monotonic_time(microsecond), Now,
                undefined, backlog(ProcID, Slot), mailbox(), Inflight,
                erlang:statistics(total_run_queue_lengths)}),
            ok
    end.

%% @doc The assignment is read; the slot's own work begins.
opened() ->
    case get(?OPEN) of
        Open when is_tuple(Open), element(5, Open) == undefined ->
            put(?OPEN, setelement(5, Open, erlang:monotonic_time(microsecond)));
        _ -> ok
    end,
    ok.

%% @doc The compute opened by `open/2' stopped before computing a slot.
abandon() ->
    erase(?OPEN),
    try ets:delete(?LIVE, self()) catch error:badarg -> true end,
    ok.

%% @doc A slot is computed and, unless `Status' is `failure', stored and
%% readable. `ExecNotes' and `StoreNotes' are what `hb_slot_log:run/1'
%% returned for the execution and the store.
close(ProcID, Slot, Req, Action, Status, PrepUs, ExecUs, StoreUs, ExecNotes,
        StoreNotes) ->
    Open = erase(?OPEN),
    try ets:delete(?LIVE, self()) catch error:badarg -> true end,
    case enabled() of
        false -> ok;
        true ->
            EndMs = erlang:system_time(millisecond),
            {StartMs, NextUs, Backlog, Mailbox, Inflight, RunQ} =
                case Open of
                    {ProcID, Slot, T0, Wall, T1, B, M, I, R} when is_integer(T1) ->
                        {Wall, T1 - T0, B, M, I, R};
                    {ProcID, Slot, _T0, Wall, _, B, M, I, R} ->
                        {Wall, null, B, M, I, R};
                    _ ->
                        % Not opened here (a compute that did not come
                        % through `compute_to_slot'): its own work is all
                        % that is known.
                        {EndMs - (PrepUs + ExecUs + StoreUs) div 1000, null,
                            null, null, null, null}
                end,
            insert({c, ProcID, Slot, signed_id(body(Req)), hb_slot_log:action(Action),
                Status, sched_ms(Req), StartMs, EndMs, NextUs, PrepUs, ExecUs,
                StoreUs, ExecNotes, StoreNotes, Backlog, Mailbox, Inflight, RunQ})
    end.

backlog(ProcID, Slot) ->
    % The compute's process ID is already the table's human-readable key.
    try ets:lookup(dev_scheduler_durable, ProcID) of
        [{_, Durable}] when is_integer(Durable) -> max(0, Durable - Slot + 1);
        _ -> null
    catch _:_ -> null
    end.

mailbox() ->
    case erlang:process_info(self(), message_queue_len) of
        {message_queue_len, N} -> N;
        _ -> null
    end.

body(Req) when is_map(Req) -> maps:get(<<"body">>, Req, undefined);
body(_) -> undefined.

%% The scheduler's time for the assignment, in ms. A process scheduled on
%% Arweave stamps seconds; the collector tells the two apart.
sched_ms(Req) when is_map(Req) ->
    case maps:get(<<"timestamp">>, Req, undefined) of
        T when is_integer(T) -> T;
        T when is_binary(T) ->
            case hb_util:safe_int(T) of
                {ok, I} -> I;
                _ -> null
            end;
        _ -> null
    end;
sched_ms(_) -> null.

%% @doc The signed ID of a message, read from its commitments without
%% computing anything: the ID of its first commitment that is a signature
%% (an hmac commitment names no signer). `null' when it carries none.
signed_id(#{ <<"commitments">> := Comms }) when is_map(Comms) ->
    Signed =
        [ ID || {ID, C} <- maps:to_list(Comms), is_binary(ID),
            byte_size(ID) == 43, is_map(C), not hmac(C) ],
    case lists:sort(Signed) of
        [ID | _] -> ID;
        [] -> null
    end;
signed_id(_) -> null.

hmac(C) ->
    Alg = maps:get(<<"type">>, C, maps:get(<<"alg">>, C, <<>>)),
    is_binary(Alg) andalso binary:match(Alg, <<"hmac">>) =/= nomatch.

%%% Recording an HTTP request that schedules a message.

%% @doc An HTTP request arrived (its handler process starts). Cheap enough
%% for every request: one clock read and one process dictionary write.
arrived() ->
    put(?ARRIVED, {erlang:system_time(millisecond), null}),
    ok.

%% @doc The request's body is read.
body_read() ->
    case get(?ARRIVED) of
        {At, _} -> put(?ARRIVED, {At, erlang:system_time(millisecond)});
        _ -> ok
    end,
    ok.

%% @doc The request carrying the signed `Message' is answered now: an `h'
%% row with when it arrived and its body was read, joined to the `s' row and
%% the client's write by `mid'. Arrival to reply is everything the node did
%% for the POST: body, parsing, signature checks, scheduling. An unsigned
%% request (a read) writes nothing.
replied(Message) ->
    case get(?ARRIVED) of
        {At, Body} ->
            case enabled() andalso signed_id(Message) of
                Mid when is_binary(Mid) ->
                    insert({h, Mid, At, Body, erlang:system_time(millisecond)});
                _ -> ok
            end;
        _ -> ok
    end,
    ok.

%%% Recording a scheduled message.

%% @doc The scheduler server assigned `Slot' to `Message', which entered its
%% queue at `EnqMs'. Held by the server until the slot is durable.
scheduled(Assignment, {Message, EnqMs}) ->
    case enabled() of
        false -> ok;
        true ->
            try
                Slot = hb_util:int(maps:get(<<"slot">>, Assignment)),
                Held = case get(?SCHED) of M when is_map(M) -> M; _ -> #{} end,
                put(?SCHED, Held#{ Slot =>
                    {signed_id(Message), EnqMs, sched_ms(Assignment),
                        erlang:system_time(millisecond), mailbox()} })
            catch _:_ -> ok
            end
    end,
    ok.

%% @doc Every slot of `ProcID' up to `Through' is durable.
durable(ProcID, Through) ->
    case get(?SCHED) of
        Held when is_map(Held), map_size(Held) > 0 ->
            Now = erlang:system_time(millisecond),
            {Done, Rest} = maps:fold(
                fun(Slot, V, {D, R}) when Slot =< Through -> {[{Slot, V} | D], R};
                   (Slot, V, {D, R}) -> {D, R#{ Slot => V }}
                end, {[], #{}}, Held),
            put(?SCHED, Rest),
            N = length(Done),
            case enabled() of
                true ->
                    [ insert({s, ProcID, Slot, Mid, EnqMs, AssignedMs, HeldMs, Now,
                        N, Queue})
                    || {Slot, {Mid, EnqMs, AssignedMs, HeldMs, Queue}} <- lists:sort(Done) ];
                false -> ok
            end;
        _ -> ok
    end,
    ok.

%%% Recording a delivered outbox entry.

%% @doc An outbox entry of `Slot' of `ProcID' to `Target' was posted at
%% `PostMono' (monotonic ms) and answered `Outcome' now. `TSlot' is the slot
%% the target gave it, when it gave one, and `Mid' the ID of the message
%% posted: the parent of the target's slot `TSlot' is this one's slot.
delivered(ProcID, Slot, Target, Outcome, TSlot, PostMono, Mid) ->
    case enabled() of
        false -> ok;
        true ->
            Now = erlang:system_time(millisecond),
            Took = erlang:monotonic_time(millisecond) - PostMono,
            insert({d, ProcID, Slot, Target, TSlot, Outcome, Now - Took, Now, Mid})
    end.

%%% Recording a warning or an error.

%% @doc A warning or error event (`hb_event'). The process and slot are the
%% first a `{proc_id | process | process_id | target, ID}' and a `{slot, N}'
%% tuple in the event's top three levels name; the text is the event printed
%% to depth 12, at most 400 bytes. Never fails the logger.
event(Topic, X, Mod, Line) ->
    case enabled() of
        false -> ok;
        true ->
            try
                {Pid, Slot} = names(X, 3, {null, null}),
                Text = iolist_to_binary(io_lib:format("~0P", [X, 12])),
                insert({e, Pid, Slot, Topic, short(atom_or_bin(Mod), 64),
                    case is_integer(Line) of true -> Line; false -> null end,
                    erlang:system_time(millisecond), short(Text, 400)})
            catch _:_ -> ok
            end
    end,
    ok.

names(_, 0, Acc) -> Acc;
names({K, V}, _, {null, S}) when is_binary(V), byte_size(V) == 43,
        (K == proc_id orelse K == process orelse K == process_id orelse K == target) ->
    {V, S};
names({slot, N}, _, {P, null}) when is_integer(N) -> {P, N};
names(T, D, Acc) when is_tuple(T) -> names(tuple_to_list(T), D, Acc);
names([H | Rest], D, Acc) when is_list(Rest) -> names(Rest, D, names(H, D - 1, Acc));
names(_, _, Acc) -> Acc.

atom_or_bin(A) when is_atom(A) -> atom_to_binary(A);
atom_or_bin(B) when is_binary(B) -> B;
atom_or_bin(L) when is_list(L) -> unicode:characters_to_binary(L);
atom_or_bin(_) -> <<>>.

short(B, N) when byte_size(B) > N -> binary:part(B, 0, N);
short(B, _) -> B.

%%% The ring.

insert(Row) ->
    try
        Seq = ets:update_counter(?T, seq, 1, {seq, 0}),
        ets:insert(?T, {Seq rem ring_size(), Seq, Row})
    catch error:badarg -> ok
    end,
    ok.

head() ->
    try ets:lookup(?T, seq) of
        [{seq, N}] -> N;
        [] -> 0
    catch error:badarg -> 0
    end.

%% @doc Rows after cursor `Since', oldest first, at most `Limit'. Returns
%% `{Rows, Next, Head, Dropped}': `Next' is the cursor to ask from next time,
%% `Dropped' the rows after `Since' the ring no longer holds.
read(Since, Limit) ->
    Head = head(),
    Oldest = max(1, Head - ring_size() + 1),
    % A cursor past the head is from before a restart or a reset: start over.
    From = case Since > Head of true -> Oldest; false -> max(Since + 1, Oldest) end,
    Dropped = max(0, From - min(Since, Head) - 1),
    To = min(Head, From + max(1, min(?MAX_LIMIT, Limit)) - 1),
    {Rows, Next} = collect(From, To, Head, [], From - 1),
    {Rows, Next, Head, Dropped}.

collect(S, To, _Head, Acc, Last) when S > To -> {lists:reverse(Acc), Last};
collect(S, To, Head, Acc, Last) ->
    case lookup(S rem ring_size()) of
        [{_, S, Row}] -> collect(S + 1, To, Head, [{S, Row} | Acc], S);
        _ when Head - S > ?GAP_SKIP -> collect(S + 1, To, Head, Acc, S);
        % Taken but not written yet: stop, and read it next time.
        _ -> {lists:reverse(Acc), Last}
    end.

lookup(Key) ->
    try ets:lookup(?T, Key) catch error:badarg -> [] end.

%% @doc The JSON body of `/~hyperbuddy@1.0/trace'.
report(Since, Limit) ->
    {Rows, Next, Head, Dropped} = read(Since, Limit),
    {On, Size} = config(),
    #{
        <<"v">> => 1,
        <<"at">> => erlang:system_time(millisecond),
        <<"on">> => On andalso hb_phase_timer:enabled(),
        <<"size">> => Size,
        <<"head">> => Head,
        <<"next">> => Next,
        <<"dropped">> => Dropped,
        <<"computing">> => computing(),
        <<"rows">> => [ json(Seq, Row) || {Seq, Row} <- Rows ]
    }.

computing() ->
    try
        [ #{ <<"pid">> => P, <<"slot">> => S, <<"since">> => T }
        || {_, P, S, T} <- ets:tab2list(?LIVE) ]
    catch error:badarg -> []
    end.

json(Seq, {c, P, Slot, Mid, Act, Status, Sched, Start, End, Next, Prep, Exec,
        Store, ExecNotes, StoreNotes, Backlog, Mailbox, Inflight, RunQ}) ->
    % The VM columns hold either runtime's phases (`hb_slot_log:vm_phases/1').
    VM = hb_slot_log:vm_phases(ExecNotes),
    #{
        <<"q">> => Seq, <<"k">> => <<"c">>, <<"pid">> => P, <<"slot">> => Slot,
        <<"mid">> => Mid, <<"act">> => Act, <<"st">> => atom_to_binary(Status),
        <<"sched">> => Sched, <<"start">> => Start, <<"end">> => End,
        <<"us">> => #{
            <<"next">> => Next,
            <<"prep">> => Prep,
            <<"exec">> => Exec,
            <<"lua_prep">> => maps:get(prep, VM),
            <<"lua">> => maps:get(run, VM),
            <<"decode">> => maps:get(decode, VM),
            <<"apply">> => maps:get(apply, VM),
            <<"gc">> => maps:get(gc, VM),
            <<"exec_lmdb">> =>
                maps:get(lmdb_put, ExecNotes, 0) + maps:get(lmdb_commit, ExecNotes, 0)
                    + maps:get(lmdb_scan, ExecNotes, 0),
            <<"store">> => Store,
            <<"store_put">> => maps:get(lmdb_put, StoreNotes, 0),
            <<"store_commit">> => maps:get(lmdb_commit, StoreNotes, 0),
            <<"store_scan">> => maps:get(lmdb_scan, StoreNotes, 0)
        },
        <<"backlog">> => Backlog, <<"mailbox">> => Mailbox,
        <<"inflight">> => Inflight, <<"runq">> => RunQ
    };
json(Seq, {s, P, Slot, Mid, Enq, Assigned, Held, Durable, Batch, Queue}) ->
    #{
        <<"q">> => Seq, <<"k">> => <<"s">>, <<"pid">> => P, <<"slot">> => Slot,
        <<"mid">> => Mid, <<"enq">> => Enq, <<"assigned">> => Assigned,
        <<"held">> => Held, <<"durable">> => Durable, <<"batch">> => Batch,
        <<"queue">> => Queue
    };
json(Seq, {h, Mid, At, Body, Replied}) ->
    #{
        <<"q">> => Seq, <<"k">> => <<"h">>, <<"mid">> => Mid, <<"arrived">> => At,
        <<"body">> => Body, <<"replied">> => Replied
    };
json(Seq, {e, P, Slot, Topic, Mod, Line, At, Text}) ->
    #{
        <<"q">> => Seq, <<"k">> => <<"e">>, <<"pid">> => P, <<"slot">> => Slot,
        <<"topic">> => atom_to_binary(Topic), <<"mod">> => Mod, <<"line">> => Line,
        <<"at">> => At, <<"text">> => printable(Text)
    };
json(Seq, {d, P, Slot, Target, TSlot, Outcome, Post, Done, Mid}) ->
    #{
        <<"q">> => Seq, <<"k">> => <<"d">>, <<"pid">> => P, <<"slot">> => Slot,
        <<"target">> => Target, <<"tslot">> => TSlot,
        <<"out">> => atom_to_binary(Outcome), <<"post">> => Post,
        <<"done">> => Done, <<"mid">> => Mid
    }.

%% A printed event as JSON-safe text: bytes outside printable ASCII are `?'.
printable(B) -> << <<(if C >= 16#20, C =< 16#7E -> C; true -> $? end)>> || <<C>> <= B >>.

%%% Tests

with(Opts, Fun) ->
    hb_phase_timer:record(msg_trace_test, 0), % the owner creates the tables
    hb_phase_timer:configure(Opts),
    hb_phase_timer:reset(),
    try Fun()
    after
        hb_phase_timer:configure(#{}),
        hb_phase_timer:reset()
    end.

-define(MID, <<"MID-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx">>).
-define(HMAC, <<"HMAC-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx">>).
-define(PROC, <<"PROC-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx">>).

req(Slot) ->
    #{ <<"slot">> => Slot, <<"timestamp">> => 1000,
        <<"body">> => #{ <<"action">> => <<"Act">>,
            <<"commitments">> => #{
                ?MID => #{ <<"type">> => <<"rsa-pss-sha512">> },
                ?HMAC => #{ <<"type">> => <<"hmac-sha256">> } } } }.

compute(P, Slot) ->
    open(P, Slot),
    opened(),
    {_, ok, Notes} = hb_slot_log:run(fun() -> hb_slot_log:note(lua_patch_lua, 30), ok end),
    {_, ok, SNotes} =
        hb_slot_log:run(fun() ->
            hb_slot_log:note(lmdb_put, 5), hb_slot_log:note(lmdb_commit, 70), ok end),
    close(P, Slot, req(Slot), <<"Act">>, ok, 1, 40, 80, Notes, SNotes).

rows(Since) ->
    #{ <<"rows">> := Rows } = hb_json:decode(hb_json:encode(report(Since, 100))),
    Rows.

compute_row_test() ->
    with(#{}, fun() ->
        compute(<<"proc-1">>, 3),
        [Row] = rows(0),
        ?assertMatch(
            #{ <<"k">> := <<"c">>, <<"pid">> := <<"proc-1">>, <<"slot">> := 3,
                <<"mid">> := ?MID, <<"act">> := <<"Act">>, <<"st">> := <<"ok">>,
                <<"sched">> := 1000, <<"inflight">> := 1,
                <<"us">> := #{ <<"lua">> := 30, <<"exec">> := 40, <<"store">> := 80,
                    <<"store_put">> := 5, <<"store_commit">> := 70,
                    <<"next">> := N } } when is_integer(N),
            Row),
        ?assertEqual([], computing())
    end).

ring_and_cursor_test() ->
    with(#{ <<"msg-trace-size">> => 1024 }, fun() ->
        [ compute(<<"p">>, S) || S <- lists:seq(1, 1500) ],
        {Rows, Next, Head, Dropped} = read(0, 100),
        ?assertEqual(1500, Head),
        ?assertEqual(476, Dropped),
        ?assertEqual(100, length(Rows)),
        ?assertEqual(576, Next),
        ?assertMatch([{477, _} | _], Rows),
        {Rest, 1500, _, 0} = read(Next, 5000),
        ?assertEqual(924, length(Rest)),
        ?assertMatch({[], 1500, 1500, 0}, read(1500, 10)),
        % A cursor from before a restart starts over at the oldest row.
        ?assertMatch({[{477, _} | _], _, 1500, 0}, read(99999, 10)),
        ?assert(ets:info(?T, size) =< 1025)
    end).

off_test() ->
    with(#{ <<"msg-trace">> => false }, fun() ->
        compute(<<"p">>, 1),
        scheduled(#{ <<"slot">> => 1 }, {#{}, 0}),
        durable(<<"p">>, 1),
        delivered(<<"p">>, 1, <<"t">>, assigned, 4, erlang:monotonic_time(millisecond), null),
        ?assertEqual({[], 0, 0, 0}, read(0, 10)),
        ?assertEqual(undefined, get(?OPEN))
    end).

sched_and_delivery_test() ->
    with(#{}, fun() ->
        scheduled(#{ <<"slot">> => 4, <<"timestamp">> => 5 }, {maps:get(<<"body">>, req(4)), 2}),
        scheduled(#{ <<"slot">> => 5, <<"timestamp">> => 6 }, {#{}, 3}),
        durable(<<"p">>, 4),
        delivered(<<"p">>, 4, <<"t">>, assigned, 9, erlang:monotonic_time(millisecond) - 7, ?MID),
        durable(<<"p">>, 5),
        [S4, D, S5] = rows(0),
        ?assertMatch(#{ <<"k">> := <<"s">>, <<"slot">> := 4, <<"mid">> := ?MID,
            <<"enq">> := 2, <<"assigned">> := 5, <<"batch">> := 1 }, S4),
        ?assertMatch(#{ <<"k">> := <<"s">>, <<"slot">> := 5, <<"mid">> := null }, S5),
        #{ <<"k">> := <<"d">>, <<"tslot">> := 9, <<"out">> := <<"assigned">>, <<"mid">> := ?MID,
            <<"post">> := Post, <<"done">> := Done } = D,
        ?assert(Done - Post >= 7)
    end).

abandon_and_sweep_test() ->
    with(#{}, fun() ->
        open(<<"p">>, 1),
        ?assertMatch([_], computing()),
        abandon(),
        ?assertEqual([], computing()),
        Pid = spawn(fun() -> open(<<"q">>, 1) end),
        Ref = monitor(process, Pid),
        receive {'DOWN', Ref, _, _, _} -> ok end,
        ?assertMatch([_], computing()),
        sweep(),
        ?assertEqual([], computing())
    end).

event_test() ->
    with(#{}, fun() ->
        event(warning, {delivery_post_crashed, {process, ?PROC}, {slot, 7},
            {reason, <<1, 2, "x">>}}, lib_process_delivery, 12),
        event(error, just_text, some_mod, undefined),
        [A, B] = rows(0),
        ?assertMatch(#{ <<"k">> := <<"e">>, <<"pid">> := ?PROC, <<"slot">> := 7,
            <<"topic">> := <<"warning">>, <<"mod">> := <<"lib_process_delivery">>,
            <<"line">> := 12 }, A),
        ?assertMatch(#{ <<"pid">> := null, <<"slot">> := null,
            <<"text">> := <<"just_text">> }, B)
    end).

http_row_test() ->
    with(#{}, fun() ->
        replied(maps:get(<<"body">>, req(1))), % not a request process: nothing
        arrived(),
        body_read(),
        replied(#{}), % unsigned: nothing
        replied(maps:get(<<"body">>, req(1))),
        erase(?ARRIVED),
        [Row] = rows(0),
        ?assertMatch(#{ <<"k">> := <<"h">>, <<"mid">> := ?MID }, Row),
        #{ <<"arrived">> := A, <<"body">> := B, <<"replied">> := H } = Row,
        ?assert(A =< B andalso B =< H)
    end).

signed_id_test() ->
    ?assertEqual(?MID, signed_id(maps:get(<<"body">>, req(1)))),
    ?assertEqual(null, signed_id(#{})),
    ?assertEqual(null, signed_id({link, <<"x">>, #{}})),
    ?assertEqual(null, signed_id(#{ <<"commitments">> =>
        #{ ?HMAC => #{ <<"type">> => <<"hmac-sha256">> } } })),
    ?assertEqual(43, byte_size(?MID)),
    ?assertEqual(43, byte_size(?HMAC)).

microbench_test_() ->
    {timeout, 60, fun() ->
        with(#{}, fun() ->
            N = 50000,
            Req = req(1),
            {Us, _} =
                timer:tc(fun() ->
                    [ begin
                        open(?PROC, I),
                        opened(),
                        close(?PROC, I, Req, <<"Order.Place">>, ok, 100,
                            5000, 300, #{ lua_patch_lua => 2000 },
                            #{ lmdb_put => 40, lmdb_commit => 200 })
                      end
                    || I <- lists:seq(1, N) ]
                end),
            PerSlot = Us / N,
            ?debugFmt("msg trace: ~.2f us per computed slot over ~p", [PerSlot, N]),
            % ~6 us in a release build; the test build runs several times
            % slower. The game slot bench measures it against real slots.
            ?assert(PerSlot < 100)
        end)
    end}.

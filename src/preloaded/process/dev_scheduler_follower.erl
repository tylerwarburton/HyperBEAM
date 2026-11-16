%%% @doc This node's own computation of a process scheduled by a `verified'
%%% foreign scheduler S, and the outbox entries it may therefore admit.
%%%
%%% S's signature counts for nothing. A slot of a followed process P is
%%% usable only when all of these hold:
%%%
%%%   1. `lib_bridge_replica' accepted it: signed by P's scheduler,
%%%      next in slot order, continuing the `base-hashpath' chain, carrying a
%%%      message a real signature vouches for and that is for P. S's URL is
%%%      transport only.
%%%   2. Every confirmed window of S's Arweave schedule archive
%%%      (`dev_scheduler_uploader') that covers the slot holds that very
%%%      assignment, mined `confirmations' deep (default 3; inclusion is the
%%%      first). Any window holding a DIFFERENT assignment for the slot is a
%%%      fork: S is frozen and the IDs kept as evidence.
%%%   3. Every input of the slot that claims a process origin passes THIS
%%%      node's door for P (`dev_scheduler_admission:check/3'), as if it had
%%%      been delivered here, and names an outbox entry no earlier slot of P
%%%      took. Whatever S's own door wrote on the assignment is S's word and
%%%      counts for nothing. An input our door refuses stops the follower.
%%%
%%% Each usable slot is computed in the replica's isolated store, with no node
%%% wallet, and its outbox entries are indexed by (P, slot, key) in the
%%% scheduler's store with the provenance the deliverer would stamp
%%% (`lib_process_delivery:origin/3'). The door (`check/5') only reads that
%%% index: it never computes, fetches or waits.
%%%
%%% Following is lazy: a follower starts at the first push from P and keeps
%%% running while the node runs. Its state is cache; losing it costs a
%%% recompute, not safety.
%%%
%%% It polls only as fast as a push is waiting. While a push of P has been
%%% refused 503 for a slot not yet indexed, the replica is brought forward
%%% every `poll-ms' (S's own node, not Arweave). Otherwise it is brought
%%% forward every `idle-poll-ms', to index ahead of the next push. Arweave is
%%% read only when the replica holds slots not yet indexed, at most every
%%% `archive-poll-ms': a window needs `confirmations' blocks of about two
%%% minutes each, so asking more often only spends the gateway's rate limit.
%%% A gateway that refuses (429, 5xx, a transport error, a body that does not
%%% parse) is not asked again before its `Retry-After', nor sooner than a
%%% backoff that doubles per refusal in a row; the next gateway of
%%% `scheduler-follower-gateways' is asked instead. A refusal is never a
%%% crash: the slot waits (fail closed). Windows fetched before a refusal are
%%% kept, so a read of hundreds of windows advances even when the gateway
%%% rate-limits part-way; only windows over slots not indexed yet are fetched.
%%%
%%% The gateways are asked in the order listed, every read: answering once
%%% does not promote a fallback above the first. A gateway that answers but
%%% reports no block for an item (turbo-gateway.com, for every archive item)
%%% confirms nothing, and promoting it once froze every follower on 11-16:
%%% arweave.net 429'd one read and was never asked again.
-module(dev_scheduler_follower).
-device_libraries([lib_bridge_replica, lib_process_cache, lib_process_delivery,
    lib_arweave_scheduler_blocks]).
-export([check/5, ensure/3]).
-export([archived_ids/1, window_confirmed/4]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PREFIX, <<"~scheduler@1.0">>).
-define(DEFAULT_POLL_MS, 5000).
-define(DEFAULT_IDLE_POLL_MS, 300000).
-define(DEFAULT_ARCHIVE_POLL_MS, 60000).
-define(MIN_GATEWAY_BACKOFF, 30000).
-define(MAX_GATEWAY_BACKOFF, 600000).
-define(DEFAULT_DEPTH, 3).
-define(DEFAULT_GATEWAY, <<"https://arweave.net">>).
%% Slots judged, then computed by one call, per batch of the index.
-define(INDEX_BATCH, 500).
%% The deliverer's longest backoff (`lib_process_delivery').
-define(MAX_RETRY_AFTER_MS, 600000).
-define(PROGRESS, dev_scheduler_follower_progress).
%% A listing of the archive asks only for windows mined at most this many
%% blocks below the highest it holds (about 100 minutes), so a late-indexed
%% item still shows; every `?FULL_LISTING_EVERY'-th listing is whole.
-define(RELIST_BLOCKS, 50).
-define(FULL_LISTING_EVERY, 10).

%%% The door ------------------------------------------------------------------

%% @doc `ok' when the delivered `Req' is exactly the entry this node verified
%% for its stamps; throws `{refused, 403 | 503, Why}' otherwise.
check(Signer, Stamps, Req, ProcID, Opts) ->
    % Admission names a scheduler by its committer URI (`uri-did-ar-...');
    % the follower keys, queries and stores by its address.
    S = scheduler_address(Signer),
    #{ <<"admit-process">> := P, <<"admit-slot">> := Slot,
       <<"admit-outbox-key">> := Key, <<"admit-entry">> := EntryID } = Stamps,
    case forked(S, Opts) of
        true -> throw({refused, 403, <<"the sender's scheduler forked its schedule">>});
        false -> ok
    end,
    case entry(P, Slot, Key, Opts) of
        not_found ->
            % A push is waiting for this slot: the follower polls fast until
            % it is indexed, and the deliverer is told when to ask again.
            ensure(P, S, Opts) ! {wanted, hb_util:int(Slot)},
            case retry_after(P, hb_util:int(Slot), Opts) of
                undefined -> throw({refused, 503, <<"entry not verified yet">>});
                Secs ->
                    throw({refused, 503, <<"entry not verified yet">>,
                        #{ <<"retry-after">> => Secs }})
            end;
        {ok, #{ <<"entry">> := Entry, <<"origin">> := Origin }} ->
            need(hb_message:id(Entry, all, Opts) =:= EntryID, <<"entry ID does not match">>),
            Expected =
                lib_process_delivery:augment_message(
                    Origin#{
                        <<"from-slot">> => Slot,
                        <<"from-outbox-key">> => Key,
                        <<"from-entry">> => EntryID
                    },
                    Entry,
                    Opts
                ),
            % The body as delivered, against the entry as computed: every key,
            % the stamps included. Both sides have their target's hint
            % stripped, because the deliverer strips it only when it posts to
            % a known location (`lib_process_delivery:sign_entry/7'). An entry
            % the sender evaluated (`resolve') never matches, because its
            % delivered body is the sender node's evaluation, which nobody
            % else can check.
            need(delivered_id(Expected, Opts) =:= delivered_id(Req, Opts),
                <<"message is not the verified entry">>),
            need((catch hb_util:human_id(hb_maps:get(<<"target">>, Entry, none, Opts)))
                    =:= hb_util:human_id(ProcID),
                <<"entry is for another process">>),
            ok
    end.

need(true, _) -> ok;
need(_, Why) -> throw({refused, 403, Why}).

%% The address a scheduler URI or address names; a name that is neither is
%% refused, never a crash in the door.
scheduler_address(S) ->
    case hb_committer:address(S) of
        {ok, Address} -> hb_util:human_id(Address);
        error -> throw({refused, 403, <<"the sender's scheduler is not an address">>})
    end.

delivered_id(Msg, Opts) ->
    hb_message:id(
        lib_process_delivery:normalize_message(hb_message:uncommitted(Msg), Opts),
        none, Opts).

%%% The follower --------------------------------------------------------------

%% @doc Start following P (scheduled by S) if nothing follows it yet; the
%% follower's PID.
ensure(P, S, Opts) ->
    hb_name:singleton({?MODULE, hb_util:human_id(P)},
        fun() -> follow(P, S, Opts) end).

follow(P, S, Opts) ->
    Cfg = hb_opts:get(<<"scheduler-admission">>, #{}, Opts),
    Url = hb_maps:get(<<"location">>, dev_scheduler_admission:peer(S, Opts), undefined, Opts),
    Dir = hb_util:bin(hb_maps:get(<<"replica-dir">>, Cfg, <<"admission-replica">>, Opts)),
    Ms = fun(Key, Default) -> hb_util:int(hb_maps:get(Key, Cfg, Default, Opts)) end,
    Indexed = indexed(P, Opts),
    loop(#{ p => hb_util:human_id(P), s => S, url => Url, dir => Dir, opts => Opts,
        poll => Ms(<<"poll-ms">>, ?DEFAULT_POLL_MS),
        idle_poll => Ms(<<"idle-poll-ms">>, ?DEFAULT_IDLE_POLL_MS),
        archive_poll => Ms(<<"archive-poll-ms">>, ?DEFAULT_ARCHIVE_POLL_MS),
        depth => Ms(<<"confirmations">>, ?DEFAULT_DEPTH),
        gateways => gateways(Opts), archive_at => now_ms(),
        indexed => Indexed, wanted => Indexed, windows => [],
        progress => {now_ms(), Indexed, undefined} }).

%% @doc The gateways the follower reads Arweave from, in order of preference:
%% `scheduler-follower-gateways', else `scheduler-upload-gateway', else
%% arweave.net. Each carries when it may next be asked and its backoff.
gateways(Opts) ->
    Listed =
        dev_scheduler_uploader:list_option(
            hb_opts:get(<<"scheduler-follower-gateways">>, [], Opts), Opts),
    URLs =
        case Listed of
            [] -> [hb_util:bin(hb_opts:get(<<"scheduler-upload-gateway">>,
                    ?DEFAULT_GATEWAY, Opts))];
            _ -> Listed
        end,
    [ #{ url => URL, not_before => now_ms(), backoff => 0 } || URL <- URLs ].

loop(F) ->
    F2 =
        try pass(F)
        catch Class:Reason:Stack ->
            ?event(warning, {foreign_follower_pass_failed,
                {p, maps:get(p, F)}, {class, Class}, {reason, Reason}, {trace, Stack}}),
            F
        end,
    case maps:get(stopped, F2, false) of
        true -> ok;
        false -> wait(F2#{ next_at => now_ms() + interval(F2) })
    end.

%% Sleep until the next pass. A push refused for a slot not yet indexed
%% brings it forward to `poll-ms' from now, if it was further off.
wait(F = #{ next_at := At }) ->
    receive
        {wanted, Slot} -> wait(wanted(F, Slot, now_ms()))
    after max(0, At - now_ms()) -> loop(F)
    end.

wanted(F = #{ wanted := Wanted, next_at := At, poll := Poll }, Slot, Now) ->
    F#{ wanted := max(Wanted, Slot), next_at := min(At, Now + Poll) }.

%% Fast while a push waits for a slot not yet indexed; slow otherwise.
interval(#{ wanted := Wanted, indexed := Indexed, poll := Poll })
        when Wanted > Indexed -> Poll;
interval(#{ idle_poll := Idle }) -> Idle.

%% Monotonic, and possibly negative: every deadline starts at `now_ms()', never 0.
now_ms() -> erlang:monotonic_time(millisecond).

%%% Progress ------------------------------------------------------------------

%% @doc When, in whole seconds, a push of slot `Slot' of P is worth posting
%% again: the follower's estimate of when it indexes the slot, from its
%% measured rate, between `poll-ms' and ten minutes (the deliverer's longest
%% backoff). `undefined' when there is no estimate: the follower has indexed
%% nothing since it started, or has already indexed the slot (the entry is
%% not in its outbox, and waiting will not change that). A deliverer told
%% nothing backs off from 1 s to 10 min, so a push whose slot was indexed
%% just after its 10-minute backoff began waited 10 minutes for nothing.
retry_after(P, Slot, Opts) ->
    Poll = hb_util:int(hb_maps:get(<<"poll-ms">>,
        hb_opts:get(<<"scheduler-admission">>, #{}, Opts), ?DEFAULT_POLL_MS, Opts)),
    case progress_of(hb_util:human_id(P)) of
        {Indexed, Rate} when Slot > Indexed, Rate > 0 ->
            Ms = min(?MAX_RETRY_AFTER_MS, max(Poll, round((Slot - Indexed) * 1000 / Rate))),
            (Ms + 999) div 1000;
        _ -> undefined
    end.

%% Record the follower's indexed slot and its rate in slots per second,
%% smoothed over batches.
progress(F = #{ p := P, indexed := Indexed }) ->
    Now = now_ms(),
    Rate =
        case maps:get(progress, F, undefined) of
            {At, From, Old} when Now > At ->
                New = (Indexed - From) * 1000 / (Now - At),
                case Old of
                    undefined -> New;
                    _ -> 0.5 * Old + 0.5 * New
                end;
            {_, _, Old} -> Old;
            undefined -> undefined
        end,
    ensure_progress_table(),
    case Rate of
        undefined -> ok;
        _ -> ets:insert(?PROGRESS, {P, Indexed, Rate})
    end,
    F#{ progress => {Now, Indexed, Rate} }.

progress_of(P) ->
    try ets:lookup(?PROGRESS, P) of
        [{_, Indexed, Rate}] -> {Indexed, Rate};
        [] -> undefined
    catch error:badarg -> undefined
    end.

%% Owned by a process that never exits: a table dies with its owner.
ensure_progress_table() ->
    case ets:whereis(?PROGRESS) of
        undefined ->
            Parent = self(),
            Ref = make_ref(),
            spawn(fun() ->
                try ets:new(?PROGRESS, [named_table, public, set]) of
                    _ -> Parent ! {Ref, ok}, receive after infinity -> ok end
                catch error:badarg -> Parent ! {Ref, ok}
                end
            end),
            receive {Ref, ok} -> ok after 5000 -> ok end;
        _ -> ok
    end.

%% One pass: bring the replica forward, then index every slot that is
%% verified, archived deep enough and whose inputs we re-admit.
pass(F = #{ p := P, url := Url, dir := Dir, opts := Opts, indexed := Indexed }) ->
    R = lib_bridge_replica:forward(
        lib_bridge_replica:load(P, Url, Opts, Dir, keep)),
    case R of
        #{ slot := Top } when Top > Indexed ->
            prune(index(Indexed + 1, Top, R, with_windows(F, now_ms())));
        _ -> F
    end.

%% Index `From'..`Top', `?INDEX_BATCH' slots at a time. Each batch is judged
%% slot by slot first (archived, re-admitted); the slots that pass are then
%% computed by ONE call for the last of them, which computes and stores every
%% slot before it on the way, and their entries are written in one write.
%% Computing slot by slot replayed from the last VM snapshot on every call
%% (`lib_bridge_replica:execution_opts/2'). A slot that does not pass ends
%% the batch after the slots before it are indexed.
index(From, Top, _R, F) when From > Top -> F;
index(From, Top, R, F) ->
    Last = min(Top, From + ?INDEX_BATCH - 1),
    {Ready, Halt} = judge(lists:seq(From, Last), R, F, []),
    F2 = index_batch(lists:reverse(Ready), R, F),
    case Halt of
        none -> index(Last + 1, Top, R, F2);
        not_yet -> F2;
        {fork, N, Archived, A} -> freeze(F2, N, Archived, A);
        {refused, N, Why} -> stop(F2, N, Why)
    end.

%% `{Ready, Halt}': the slots from the first of `Slots' on that may be
%% indexed, newest first, each with the entries its input took, and why the
%% run stopped (`none' when every slot passed).
judge([], _R, _F, Ready) -> {Ready, none};
judge([N | Rest], R = #{ opts := ROpts }, F = #{ p := P, opts := Opts }, Ready) ->
    {ok, A0} = hb_cache:read(lib_bridge_replica:slot_path(P, N, none), ROpts),
    A = hb_cache:ensure_all_loaded(A0, ROpts),
    case archived(N, hb_message:id(A, signed, ROpts), F) of
        not_yet -> {Ready, not_yet};
        {fork, Archived} -> {Ready, {fork, N, Archived, A}};
        ok ->
            TakenHere = lists:append([ T || {_, T} <- Ready ]),
            case readmit(A, P, ROpts, Opts, TakenHere) of
                not_yet -> {Ready, not_yet};
                {refused, Why} -> {Ready, {refused, N, Why}};
                {ok, Taken} -> judge(Rest, R, F, [{N, Taken} | Ready])
            end
    end.

index_batch([], _R, F) -> F;
index_batch(Ready, #{ proc := Proc, opts := ROpts }, F = #{ p := P, opts := Opts }) ->
    {Last, _} = lists:last(Ready),
    {ok, _} =
        hb_ao:resolve(Proc, #{ <<"path">> => <<"compute">>, <<"slot">> => Last }, ROpts),
    ok = write_entries(P, Ready, Proc, ROpts, Opts),
    progress(F#{ indexed := Last }).

%% Windows wholly at or below the indexed slot can no longer hold back or
%% fork a slot to index: `archived/3' scans the rest for every slot.
prune(F = #{ windows := Windows, indexed := Indexed }) ->
    F#{ windows := [ W || W = #{ to := To } <- Windows, To > Indexed ] }.

%% The slot's input as an input of P at THIS node's door: `{ok, Taken}' with
%% the outbox entry it names (`[]' for one that claims no origin), `not_yet'
%% while the door would answer 503, `{refused, Why}' otherwise. `TakenHere'
%% are the entries earlier slots of the same batch took, not written yet.
readmit(A, P, ROpts, Opts) -> readmit(A, P, ROpts, Opts, []).
readmit(A, P, ROpts, Opts, TakenHere) ->
    Body = hb_cache:ensure_all_loaded(hb_maps:get(<<"body">>, A, #{}, ROpts), ROpts),
    try dev_scheduler_admission:check(P, Body, Opts) of
        none ->
            % S may not admit what our door would not: its `admit-*' would be
            % believed when we recompute P.
            case hb_maps:get(<<"admit-process">>, A, not_found, ROpts) of
                not_found -> {ok, []};
                _ -> {refused, <<"S admitted an input our door does not">>}
            end;
        {Stamps, _Policy, _Epoch} ->
            Taken = dev_scheduler_admission:ids(Stamps, Opts),
            case [ ID || ID <- Taken, lists:member(ID, TakenHere) orelse taken(P, ID, Opts) ] of
                [] -> {ok, Taken};
                _ -> {refused, <<"an outbox entry is an input of the process twice">>}
            end
    catch
        throw:{refused, 503, _} -> not_yet;
        throw:{refused, 503, _, _} -> not_yet;
        throw:{refused, _, Why} -> {refused, Why}
    end.

%%% The archive ---------------------------------------------------------------

%% `ok' when every confirmed archive window over slot N holds this
%% assignment; `{fork, Archived}' when any holds another; `not_yet' when none
%% covers N yet.
archived(N, AssignmentID, #{ windows := Windows }) ->
    case [ W || W = #{ from := From, to := To } <- Windows, From =< N, N =< To ] of
        [] -> not_yet;
        Ws ->
            case [ W || W = #{ ids := IDs } <- Ws, not lists:member(AssignmentID, IDs) ] of
                [] -> ok;
                _ -> {fork, [ maps:get(id, W) || W <- Ws ]}
            end
    end.

%% The archive windows of P that S signed and that are mined deep enough, read
%% from the first gateway that may be asked and answers, once `archive-poll-ms'
%% has passed since the last read. Every window fetched is kept, also from a
%% gateway that refused part-way.
with_windows(F = #{ archive_at := At }, Now) when Now < At -> F;
with_windows(F = #{ gateways := Gateways, archive_poll := Every }, Now) ->
    case read_archive(Gateways, Now, F, []) of
        {ok, F2, Gateways2} ->
            (listed(F2))#{ gateways := Gateways2, archive_at := Now + Every };
        {none, F2, Gateways2} ->
            % Every gateway is cooling down: ask again when the first may be.
            Next = lists:min([ NB || #{ not_before := NB } <- Gateways2 ]),
            (listed(F2))#{ gateways := Gateways2, archive_at := max(Next, Now + 1000) }
    end.

%% Count a read, and keep the highest block any window held was mined in.
listed(F = #{ windows := Windows }) ->
    Heights = [ H || #{ height := H } <- Windows, is_integer(H) ],
    F#{ listings => maps:get(listings, F, 0) + 1,
        top_height => lists:max([maps:get(top_height, F, -1) | Heights]) }.

%% The lowest block a listing asks for windows from: none on the first read
%% and every `?FULL_LISTING_EVERY'-th, else `?RELIST_BLOCKS' below the
%% highest block a held window was mined in. A whole listing is 100 windows a
%% request: 124 requests a read for one process of 10,000 slots archived a
%% few slots a window, every `archive-poll-ms' (2026-11-16), which is what
%% the gateway rate-limited.
floor_height(F = #{ top_height := Top }) when is_integer(Top), Top >= 0 ->
    case maps:get(listings, F, 0) rem ?FULL_LISTING_EVERY of
        0 -> undefined;
        _ -> max(0, Top - ?RELIST_BLOCKS)
    end;
floor_height(_) -> undefined.

%% Try each gateway in the order listed, skipping one not to be asked yet; a
%% gateway that fails is backed off and the next one tried, starting from the
%% windows the failed one did fetch. The order never changes.
read_archive([], _Now, F, Tried) -> {none, F, lists:reverse(Tried)};
read_archive([G = #{ not_before := NB } | Rest], Now, F, Tried) when NB > Now ->
    read_archive(Rest, Now, F, [G | Tried]);
read_archive([G = #{ url := URL } | Rest], Now, F = #{ p := P, windows := Have }, Tried) ->
    case new_windows(hb_util:list(URL), F) of
        {ok, New} ->
            {ok, F#{ windows := Have ++ New },
                lists:reverse(Tried) ++ [G#{ backoff := 0 } | Rest]};
        {refused, New, Class, Reason} ->
            G2 = back_off(G, retry_after(Reason), Now),
            ?event(warning, {foreign_follower_gateway_refused,
                {p, P}, {gateway, URL}, {reason, refusal(Class, Reason)},
                {windows_kept, length(New)},
                {retry_in_ms, maps:get(not_before, G2) - Now}}),
            read_archive(Rest, Now, F#{ windows := Have ++ New }, [G2 | Tried])
    end.

%% Not before the gateway's `Retry-After', nor sooner than double the last
%% backoff (at least `?MIN_GATEWAY_BACKOFF', at most `?MAX_GATEWAY_BACKOFF').
back_off(G = #{ backoff := Last }, RetryAfter, Now) ->
    Doubled = min(?MAX_GATEWAY_BACKOFF, max(?MIN_GATEWAY_BACKOFF, 2 * Last)),
    G#{ backoff := Doubled, not_before := Now + max(Doubled, RetryAfter) }.

retry_after({gateway_status, _Status, RetryAfter}) -> RetryAfter;
retry_after(_) -> 0.

refusal(_, {gateway_status, Status, _}) -> {status, Status};
refusal(Class, Reason) -> {Class, Reason}.

%% `{ok, Windows}', or `{refused, Fetched, Class, Reason}' with the windows
%% fetched before the gateway refused.
new_windows(Gateway, F) ->
    try wanted_windows(Gateway, F) of
        Wanted -> fetch_windows(Gateway, Wanted, [])
    catch Class:Reason -> {refused, [], Class, Reason}
    end.

%% The confirmed windows not held yet that reach past the indexed slot, once
%% each (arweave.net lists every archive item twice, once with its block and
%% once without), lowest slots first: those are what indexing waits on.
wanted_windows(Gateway, F = #{ p := P, s := S, depth := Depth, windows := Have,
        indexed := Indexed }) ->
    Height = binary_to_integer(string:trim(gateway(get, Gateway, "/height", none, 15000))),
    Edges = archive_edges(Gateway, S, P, floor_height(F), #{}),
    Known = [ maps:get(id, W) || W <- Have ],
    lists:keysort(2, lists:ukeysort(1,
        [ {ID, hb_util:int(tag(<<"From-Slot">>, Tags)), To, maps:get(<<"height">>, Block)}
        || #{ <<"node">> := #{ <<"id">> := ID, <<"tags">> := Tags,
                <<"block">> := Block } } <- Edges,
           not lists:member(ID, Known),
           window_confirmed(Block, Height, Depth, true),
           To <- [hb_util:int(tag(<<"To-Slot">>, Tags))],
           To > Indexed ])).

fetch_windows(_Gateway, [], Got) -> {ok, lists:reverse(Got)};
fetch_windows(Gateway, [{ID, From, To, Height} | Rest], Got) ->
    try archived_ids(gateway(get, Gateway, "/raw/" ++ hb_util:list(ID), none, 60000)) of
        IDs ->
            fetch_windows(Gateway, Rest,
                [#{ id => ID, from => From, to => To, ids => IDs, height => Height } | Got])
    catch Class:Reason -> {refused, lists:reverse(Got), Class, Reason}
    end.

%% Every archive transaction of P that S signed, mined at or above `Floor'
%% (`undefined': at any height), page by page.
archive_edges(Gateway, S, P, Floor, After) ->
    {Params, BlockArg, Vars} =
        case Floor of
            undefined -> {<<>>, <<>>, After};
            _ -> {<<",$m:Int">>, <<"block:{min:$m},">>, After#{ <<"m">> => Floor }}
        end,
    Query = <<"query($o:[String!],$p:[String!]!,$a:String", Params/binary,
        "){transactions(owners:$o,",
        "tags:[{name:\"Data-Protocol\",values:[\"ao-schedule-archive\"]},",
        "{name:\"Process\",values:$p}],", BlockArg/binary,
        "first:100,after:$a,sort:HEIGHT_ASC)",
        "{pageInfo{hasNextPage} edges{cursor node{id tags{name value} block{height}}}}}">>,
    Body = hb_json:encode(#{ <<"query">> => Query, <<"variables">> =>
        Vars#{ <<"o">> => [hb_util:human_id(S)], <<"p">> => [P] } }),
    Res = gateway(post, Gateway, "/graphql", Body, 15000),
    #{ <<"data">> := #{ <<"transactions">> := #{
        <<"pageInfo">> := #{ <<"hasNextPage">> := More }, <<"edges">> := Edges } } } =
        hb_json:decode(Res),
    case {More, lists:reverse(Edges)} of
        {true, [#{ <<"cursor">> := Cursor } | _]} ->
            Edges ++ archive_edges(Gateway, S, P, Floor, #{ <<"a">> => Cursor });
        _ -> Edges
    end.

%% One gateway request: the body of a 200, else throws
%% `{gateway_status, Status, RetryAfterMs}'.
gateway(Method, Gateway, Path, Body, Timeout) ->
    Request =
        case Method of
            get -> {Gateway ++ Path, []};
            post -> {Gateway ++ Path, [], "application/json", Body}
        end,
    case httpc:request(Method, Request, [{timeout, Timeout}], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Res}} -> Res;
        {ok, {{_, Status, _}, Headers, _}} ->
            throw({gateway_status, Status, retry_after_ms(Headers)});
        {error, Reason} -> throw({gateway_unreachable, Reason})
    end.

%% A `Retry-After' in seconds, as milliseconds; 0 when absent or a date.
retry_after_ms(Headers) ->
    case catch list_to_integer(string:trim(
            proplists:get_value("retry-after", Headers, ""))) of
        Secs when is_integer(Secs), Secs >= 0 -> Secs * 1000;
        _ -> 0
    end.

%% @doc A window is confirmed when its block is `Depth' deep, inclusion
%% counting as the first (`dev_scheduler_uploader:window_confirmed/4').
window_confirmed(#{ <<"height">> := H }, Height, Depth, _) when is_integer(H), H >= 0 ->
    Height - H + 1 >= Depth;
window_confirmed(_, _, _, _) -> false.

tag(Name, Tags) ->
    case [ V || #{ <<"name">> := N, <<"value">> := V } <- Tags, N =:= Name ] of
        [V | _] -> V;
        [] -> throw({archive_tag_missing, Name})
    end.

%% @doc The message IDs an archive's data holds (`dev_scheduler_uploader'
%% `bundle_data/1'): each item's own ID, and the `Message-Id' of an item that
%% wraps an httpsig-signed message as JSON.
archived_ids(<<Count:256/little, Rest/binary>>) ->
    IndexSize = Count * 64,
    <<Index:IndexSize/binary, Items/binary>> = Rest,
    Entries = [ {Size, ID} || <<Size:256/little, ID:32/binary>> <= Index ],
    archived_ids(Entries, Items).
archived_ids([], _) -> [];
archived_ids([{Size, ID} | Rest], Items) ->
    <<Item:Size/binary, More/binary>> = Items,
    {ok, _, #tx{ tags = Tags }} = ar_bundles:deserialize_header(Item),
    Wrapped = [ V || {<<"Message-Id">>, V} <- Tags ],
    [hb_util:human_id(ID) | Wrapped] ++ archived_ids(Rest, More).

%%% Index and markers ---------------------------------------------------------

%% The outbox entries of each `{N, Taken}' slot, read from the replica's
%% stored results of the slot, the entries its input took, and the new
%% `indexed' mark (the last slot): one write, one flush.
write_entries(P, Ready, Proc, ROpts, Opts) ->
    Origin = lib_process_delivery:origin(P, Proc, ROpts),
    ProcID = lib_process:process_id(Proc, #{}, ROpts),
    Rows =
        maps:from_list(lists:append(
            [ [ {entry_path(P, N, K),
                 term_to_binary(#{ <<"entry">> => E, <<"origin">> => Origin })}
              || {K, E} <- maps:to_list(outbox(results(ProcID, N, ROpts), ROpts)) ]
              ++ [ {taken_path(P, ID), integer_to_binary(N)} || ID <- Taken ]
            || {N, Taken} <- Ready ])),
    {Last, _} = lists:last(Ready),
    Store = store(Opts),
    ok = hb_store:write(Store, Rows#{ indexed_path(P) => integer_to_binary(Last) }, Opts),
    hb_store:flush(Store, Opts).

%% A computed slot's results, as stored: without materializing its state.
results(ProcID, N, ROpts) ->
    {ok, Results} = lib_process_cache:read_results(ProcID, N, ROpts),
    Results.

outbox(Results, Opts) ->
    Entries = lib_process_cache:outbox_entries(
        #{ <<"lower">> => false,
           <<"outbox">> => hb_maps:get(<<"outbox">>, Results, #{}, Opts) }, Opts),
    case Entries of
        L when is_list(L) ->
            maps:from_list(lists:zip(
                [ integer_to_binary(I) || I <- lists:seq(1, length(L)) ], L));
        M -> M
    end.

entry(P, Slot, Key, Opts) ->
    Req = #{ <<"read">> => entry_path(P, hb_util:int(Slot), hb_util:bin(Key)),
        <<"exact">> => true },
    case hb_store:read(store(Opts), Req, Opts) of
        {ok, Bin} -> {ok, binary_to_term(Bin, [safe])};
        _ -> not_found
    end.

indexed(P, Opts) ->
    Req = #{ <<"read">> => indexed_path(P), <<"exact">> => true },
    case hb_store:read(store(Opts), Req, Opts) of
        {ok, Bin} -> hb_util:int(Bin);
        _ -> -1
    end.

taken(P, ID, Opts) ->
    Req = #{ <<"read">> => taken_path(P, ID), <<"exact">> => true },
    case hb_store:read(store(Opts), Req, Opts) of
        {ok, _} -> true;
        _ -> false
    end.

forked(S, Opts) ->
    Req = #{ <<"read">> => fork_path(S), <<"exact">> => true },
    case hb_store:read(store(Opts), Req, Opts) of
        {ok, _} -> true;
        _ -> false
    end.

%% Keep both signed assignments as evidence; refuse all of S until an
%% operator clears the marker.
freeze(F = #{ s := S, p := P, opts := Opts }, N, Archived, Served) ->
    Evidence = hb_json:encode(#{ <<"process">> => P, <<"slot">> => N,
        <<"served">> => hb_message:id(Served, signed, Opts), <<"archived">> => Archived }),
    ok = hb_store:write(store(Opts), #{ fork_path(S) => Evidence }, Opts),
    ok = hb_store:flush(store(Opts), Opts),
    ?event(error, {foreign_scheduler_forked, {scheduler, S}, {process, P}, {slot, N}}),
    F#{ stopped => true }.

stop(F = #{ p := P }, N, Why) ->
    ?event(error, {foreign_follower_stopped, {process, P}, {slot, N}, {why, Why}}),
    F#{ stopped => true }.

store(Opts) ->
    hb_opts:get(scheduler_store, hb_opts:get(store, no_viable_store, Opts), Opts).

entry_path(P, N, Key) ->
    hb_path:to_binary([?PREFIX, <<"admission-entries">>, hb_util:human_id(P),
        integer_to_binary(N), Key]).
taken_path(P, ID) ->
    hb_path:to_binary([?PREFIX, <<"admission-taken">>, hb_util:human_id(P), ID]).
indexed_path(P) ->
    hb_path:to_binary([?PREFIX, <<"admission-indexed">>, hb_util:human_id(P)]).
fork_path(S) ->
    hb_path:to_binary([?PREFIX, <<"admission-forks">>, hb_util:human_id(S)]).

%%% Tests ---------------------------------------------------------------------

archived_ids_round_trip_test() ->
    W = ar_wallet:new(),
    Plain = ar_bundles:sign_item(#tx{ format = ans104, data = <<"a">>, data_size = 1,
        tags = [] }, W),
    Wrapped = ar_bundles:sign_item(#tx{ format = ans104, data = <<"{}">>, data_size = 2,
        tags = [{<<"Message-Id">>, <<"wrapped-id">>}] }, W),
    {undefined, Data} = ar_bundles:serialize_bundle(list, [Plain, Wrapped], false),
    IDs = archived_ids(Data),
    ?assert(lists:member(hb_util:human_id(ar_bundles:id(Plain, signed)), IDs)),
    ?assert(lists:member(<<"wrapped-id">>, IDs)).

window_depth_test() ->
    ?assert(window_confirmed(#{ <<"height">> => 98 }, 100, 3, true)),
    ?assertNot(window_confirmed(#{ <<"height">> => 99 }, 100, 3, true)),
    ?assertNot(window_confirmed(null, 100, 3, true)).

fork_is_detected_test() ->
    F = #{ windows => [#{ id => <<"w">>, from => 0, to => 4, ids => [<<"a1">>] }] },
    ?assertEqual(ok, archived(2, <<"a1">>, F)),
    ?assertEqual({fork, [<<"w">>]}, archived(2, <<"a2">>, F)),
    ?assertEqual(not_yet, archived(5, <<"a1">>, F)).

%% S archives both forks: one confirmed window over the slot holding another
%% assignment is a fork, however many hold ours.
fork_in_any_window_is_a_fork_test() ->
    F = #{ windows => [#{ id => <<"w1">>, from => 0, to => 4, ids => [<<"a1">>] },
                       #{ id => <<"w2">>, from => 2, to => 6, ids => [<<"a2">>] }] },
    ?assertEqual({fork, [<<"w1">>, <<"w2">>]}, archived(2, <<"a1">>, F)),
    ?assertEqual({fork, [<<"w1">>, <<"w2">>]}, archived(2, <<"a2">>, F)),
    ?assertEqual(ok, archived(1, <<"a1">>, F)).

%% An input is judged by our door, not by what S wrote beside it: a stamped
%% body our door does not admit is an ordinary input, and stops the follower
%% when S attached `admit-*' to it.
input_is_judged_by_our_door_test() ->
    Opts = #{},
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Bare = #{ <<"body">> => #{ <<"from-process">> => <<"x">> } },
    ?assertEqual({ok, []}, readmit(Bare, P, Opts, Opts)),
    ?assertMatch({refused, _}, readmit(Bare#{ <<"admit-process">> => <<"x">>,
        <<"admit-scheduler">> => <<"x">>, <<"admit-policy">> => <<"trusted">> }, P, Opts, Opts)),
    ?assertEqual({ok, []}, readmit(#{ <<"body">> => #{ <<"action">> => <<"x">> } }, P, Opts, Opts)).

%% A genuine input names its outbox entry once: the same entry in a later
%% slot of P stops the follower.
entry_taken_once_test() ->
    Store = hb_test_utils:test_store(),
    ok = hb_store:start([Store]),
    Peer = ar_wallet:new(),
    PeerAddr = hb_util:human_id(ar_wallet:to_address(Peer)),
    Opts = #{ <<"priv-wallet">> => ar_wallet:new(), <<"store">> => [Store],
        <<"scheduler-admission">> => #{ <<"peers">> =>
            [#{ <<"address">> => PeerAddr, <<"policy">> => <<"trusted">> }] } },
    Sender = hb_message:commit(#{ <<"type">> => <<"Process">>, <<"scheduler">> => PeerAddr },
        #{ <<"priv-wallet">> => ar_wallet:new() }, <<"httpsig@1.0">>),
    {ok, _} = hb_cache:write(Sender, Opts),
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Body = hb_message:commit(#{ <<"target">> => P, <<"action">> => <<"Credit-Notice">>,
        <<"from-process">> => hb_util:human_id(hb_message:id(Sender, all, Opts)),
        <<"from-slot">> => 3, <<"from-outbox-key">> => <<"1">>,
        <<"from-entry">> => hb_util:human_id(crypto:strong_rand_bytes(32)),
        <<"from-scheduler">> => PeerAddr }, #{ <<"priv-wallet">> => Peer }, <<"httpsig@1.0">>),
    {ok, [ID]} = readmit(#{ <<"body">> => Body }, P, Opts, Opts),
    % Taken by an earlier slot of the same batch, not written yet.
    ?assertMatch({refused, _}, readmit(#{ <<"body">> => Body }, P, Opts, Opts, [ID])),
    ok = hb_store:write(store(Opts), #{ taken_path(P, ID) => <<"4">> }, Opts),
    ?assertMatch({refused, _}, readmit(#{ <<"body">> => Body }, P, Opts, Opts)).

%% A follower state for the archive tests, reading through `Gateways'.
archive_state(Gateways) ->
    #{ p => hb_util:human_id(crypto:strong_rand_bytes(32)),
       s => hb_util:human_id(crypto:strong_rand_bytes(32)),
       opts => #{}, depth => 3, poll => 5000, idle_poll => ?DEFAULT_IDLE_POLL_MS,
       archive_poll => ?DEFAULT_ARCHIVE_POLL_MS, archive_at => now_ms(),
       gateways => gateways(#{ <<"scheduler-follower-gateways">> => Gateways }),
       indexed => -1, wanted => -1, windows => [] }.

%% A gateway answering `Route(Method, Path)' with `{Status, Headers, Body}',
%% telling `Test' of every request as `{gateway_hit, Name, Path}'.
stub_gateway(Name, Route, Test) ->
    {ok, _} = application:ensure_all_started(inets),
    {ok, Listen} = gen_tcp:listen(0,
        [binary, {active, false}, {reuseaddr, true}, {packet, http_bin}]),
    {ok, Port} = inet:port(Listen),
    Pid = spawn_link(fun() -> stub_accept(Listen, Name, Route, Test) end),
    {<<"http://localhost:", (integer_to_binary(Port))/binary>>, Pid}.

stub_accept(Listen, Name, Route, Test) ->
    case gen_tcp:accept(Listen) of
        {ok, Socket} ->
            stub_serve(Socket, Name, Route, Test),
            stub_accept(Listen, Name, Route, Test);
        {error, _} -> ok
    end.

stub_serve(Socket, Name, Route, Test) ->
    {ok, {http_request, Method, {abs_path, Path}, _}} = gen_tcp:recv(Socket, 0, 5000),
    ok = stub_body(Socket, 0),
    Test ! {gateway_hit, Name, Path},
    {Status, Headers, Body} = Route(Method, Path),
    gen_tcp:send(Socket, [<<"HTTP/1.1 ">>, integer_to_binary(Status), <<" X\r\n">>,
        [ [K, <<": ">>, V, <<"\r\n">>] || {K, V} <- Headers ],
        <<"content-length: ">>, integer_to_binary(byte_size(Body)),
        <<"\r\nconnection: close\r\n\r\n">>, Body]),
    gen_tcp:close(Socket).

stub_body(Socket, Length) ->
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, {http_header, _, 'Content-Length', _, Value}} ->
            stub_body(Socket, binary_to_integer(Value));
        {ok, http_eoh} ->
            ok = inet:setopts(Socket, [{packet, raw}]),
            case Length of
                0 -> ok;
                _ -> {ok, _} = gen_tcp:recv(Socket, Length, 5000), ok
            end;
        {ok, _} -> stub_body(Socket, Length)
    end.

hits(Name) ->
    receive {gateway_hit, Name, _} -> 1 + hits(Name) after 200 -> 0 end.

stop_stubs(Pids) -> [ begin unlink(P), exit(P, kill) end || P <- Pids ].

%% A gateway that rate-limits is not asked again before its `Retry-After' and
%% the read neither crashes nor changes the windows (they fail closed); each
%% refusal in a row doubles the wait, which `Retry-After' can only lengthen.
rate_limit_backs_off_test_() ->
    {timeout, 30, fun rate_limit_backs_off/0}.
rate_limit_backs_off() ->
    Limited = fun(_, _) -> {429, [{<<"retry-after">>, <<"120">>}], <<"Too Many Requests">>} end,
    {URL, Pid} = stub_gateway(limited, Limited, self()),
    F = archive_state([URL]),
    Now = now_ms(),
    F1 = with_windows(F, Now),
    ?assertEqual(1, hits(limited)),
    ?assertEqual([], maps:get(windows, F1)),
    [G1] = maps:get(gateways, F1),
    ?assertEqual(Now + 120000, maps:get(not_before, G1)),
    ?assertEqual(Now + 120000, maps:get(archive_at, F1)),
    % Asked again before Retry-After: no request at all.
    ?assertEqual(F1, with_windows(F1, Now + 60000)),
    ?assertEqual(0, hits(limited)),
    Later = Now + 120000,
    F2 = with_windows(F1, Later),
    ?assertEqual(1, hits(limited)),
    [G2] = maps:get(gateways, F2),
    ?assertEqual(2 * maps:get(backoff, G1), maps:get(backoff, G2)),
    ?assertEqual(Later + 120000, maps:get(not_before, G2)),
    % The index waits on the archive rather than failing.
    ?assertEqual(not_yet, archived(0, <<"a">>, F2)),
    % A short Retry-After never shortens the backoff below its floor.
    ?assertEqual(Now + ?MIN_GATEWAY_BACKOFF,
        maps:get(not_before, back_off(#{ backoff => 0, not_before => Now }, 7000, Now))),
    ?assertEqual(7000, retry_after_ms([{"retry-after", "7"}])),
    ?assertEqual(0, retry_after_ms([{"retry-after", "Wed, 21 Oct 2015 07:28:00 GMT"}])),
    ?assertEqual(?MAX_GATEWAY_BACKOFF,
        maps:get(backoff, back_off(#{ backoff => ?MAX_GATEWAY_BACKOFF, not_before => Now }, 0, Now))),
    stop_stubs([Pid]).

%% A gateway that refuses (429 or 5xx) hands the read to the next one, which is
%% then asked first; the refusing one is backed off.
gateway_failover_test_() ->
    {timeout, 30, fun gateway_failover/0}.
gateway_failover() ->
    W = ar_wallet:new(),
    Assignment = ar_bundles:sign_item(#tx{ format = ans104, data = <<"a">>,
        data_size = 1, tags = [] }, W),
    AssignmentID = hb_util:human_id(ar_bundles:id(Assignment, signed)),
    {undefined, Data} = ar_bundles:serialize_bundle(list, [Assignment], false),
    WindowID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Edges = hb_json:encode(#{ <<"data">> => #{ <<"transactions">> => #{
        <<"pageInfo">> => #{ <<"hasNextPage">> => false },
        <<"edges">> => [#{ <<"cursor">> => <<"c">>, <<"node">> => #{
            <<"id">> => WindowID,
            <<"tags">> => [#{ <<"name">> => <<"From-Slot">>, <<"value">> => <<"0">> },
                           #{ <<"name">> => <<"To-Slot">>, <<"value">> => <<"0">> }],
            <<"block">> => #{ <<"height">> => 98 } } }] } } }),
    WindowPath = <<"/raw/", WindowID/binary>>,
    Good =
        fun('GET', <<"/height">>) -> {200, [], <<"100">>};
           ('POST', <<"/graphql">>) -> {200, [], Edges};
           ('GET', Path) when Path =:= WindowPath -> {200, [], Data};
           (_, _) -> {404, [], <<>>}
        end,
    {Down, DownPid} = stub_gateway(down, fun(_, _) -> {503, [], <<>>} end, self()),
    {Limited, LimitedPid} = stub_gateway(limited,
        fun(_, _) -> {429, [{<<"retry-after">>, <<"7">>}], <<>>} end, self()),
    {Up, UpPid} = stub_gateway(up, Good, self()),
    F = archive_state(<<Down/binary, ",", Limited/binary, ",", Up/binary>>),
    Now = now_ms(),
    F1 = with_windows(F, Now),
    ?assertEqual(1, hits(down)),
    ?assertEqual(1, hits(limited)),
    ?assertEqual(3, hits(up)),
    ?assertMatch([#{ id := WindowID, from := 0, to := 0 }], maps:get(windows, F1)),
    ?assertEqual(ok, archived(0, AssignmentID, F1)),
    % The order stays as listed: answering does not promote a fallback.
    ?assertEqual([Down, Limited, Up], [ U || #{ url := U } <- maps:get(gateways, F1) ]),
    [#{ not_before := DownAt }, #{ not_before := LimitedAt }, _] = maps:get(gateways, F1),
    ?assert(DownAt >= Now + ?MIN_GATEWAY_BACKOFF),
    ?assert(LimitedAt >= Now + ?MIN_GATEWAY_BACKOFF),
    ?assertEqual(Now + ?DEFAULT_ARCHIVE_POLL_MS, maps:get(archive_at, F1)),
    % Once their backoff has passed the first gateways are asked again, and
    % refuse again (each backoff doubles); a window already held is not
    % fetched again.
    F2 = with_windows(F1, Now + ?DEFAULT_ARCHIVE_POLL_MS),
    ?assertEqual(1, hits(down)),
    ?assertEqual(1, hits(limited)),
    ?assertEqual(2, hits(up)),
    ?assertEqual(maps:get(windows, F1), maps:get(windows, F2)),
    [#{ backoff := DownBackoff }, _, _] = maps:get(gateways, F2),
    ?assertEqual(2 * ?MIN_GATEWAY_BACKOFF, DownBackoff),
    stop_stubs([DownPid, LimitedPid, UpPid]).

%% One archive window over `From'..`To' (one signed item per slot), mined at
%% `Height' (`null': no block): `{WindowID, Edge, Data, AssignmentIDs}'.
archive_window(From, To, Height) ->
    W = ar_wallet:new(),
    Items = [ ar_bundles:sign_item(#tx{ format = ans104, data = integer_to_binary(S),
        data_size = byte_size(integer_to_binary(S)), tags = [] }, W)
        || S <- lists:seq(From, To) ],
    {undefined, Data} = ar_bundles:serialize_bundle(list, Items, false),
    WindowID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    {WindowID, edge(WindowID, From, To, Height), Data,
        [ hb_util:human_id(ar_bundles:id(I, signed)) || I <- Items ]}.

edge(WindowID, From, To, Height) ->
    #{ <<"cursor">> => WindowID, <<"node">> => #{ <<"id">> => WindowID,
        <<"tags">> => [#{ <<"name">> => <<"From-Slot">>, <<"value">> => integer_to_binary(From) },
                       #{ <<"name">> => <<"To-Slot">>, <<"value">> => integer_to_binary(To) }],
        <<"block">> => case Height of null -> null; _ -> #{ <<"height">> => Height } end } }.

edges_body(Edges) ->
    hb_json:encode(#{ <<"data">> => #{ <<"transactions">> => #{
        <<"pageInfo">> => #{ <<"hasNextPage">> => false }, <<"edges">> => Edges } } }).

%% A gateway listing `Edges' and serving the data of `Windows'.
archive_gateway(Edges, Windows) ->
    fun('GET', <<"/height">>) -> {200, [], <<"100">>};
       ('POST', <<"/graphql">>) -> {200, [], edges_body(Edges)};
       ('GET', <<"/raw/", ID/binary>>) ->
            case lists:keyfind(ID, 1, Windows) of
                {ID, _, Data, _} -> {200, [], Data};
                false -> {404, [], <<>>}
            end;
       (_, _) -> {404, [], <<>>}
    end.

%% 2026-11-16: arweave.net answered one read 429 and turbo-gateway.com, which
%% reports no block for any archive item, answered instead. Promoted to first,
%% it was the only gateway ever asked again, so no window was confirmed and
%% every push of the verified scheduler waited 503 for good. The gateway
%% listed first is asked again once its backoff has passed.
blockless_fallback_does_not_displace_first_gateway_test_() ->
    {timeout, 30, fun blockless_fallback_does_not_displace_first_gateway/0}.
blockless_fallback_does_not_displace_first_gateway() ->
    Mined = {WindowID, Edge, _, [AssignmentID]} = archive_window(0, 0, 98),
    Calls = counters:new(1, []),
    Arweave =
        fun(M, P) ->
            counters:add(Calls, 1, 1),
            case counters:get(Calls, 1) of
                1 -> {429, [], <<>>};
                _ -> (archive_gateway([Edge], [Mined]))(M, P)
            end
        end,
    {ArURL, ArPid} = stub_gateway(arweave, Arweave, self()),
    {TurboURL, TurboPid} = stub_gateway(turbo,
        archive_gateway([edge(WindowID, 0, 0, null)], [Mined]), self()),
    F = archive_state(<<ArURL/binary, ",", TurboURL/binary>>),
    Now = now_ms(),
    F1 = with_windows(F, Now),
    ?assertEqual(1, hits(arweave)),
    % height and graphql: nothing confirmed, nothing fetched.
    ?assertEqual(2, hits(turbo)),
    ?assertEqual([], maps:get(windows, F1)),
    ?assertEqual(not_yet, archived(0, AssignmentID, F1)),
    F2 = with_windows(F1, Now + ?DEFAULT_ARCHIVE_POLL_MS),
    ?assertEqual(3, hits(arweave)),
    ?assertEqual(0, hits(turbo)),
    ?assertMatch([#{ id := WindowID }], maps:get(windows, F2)),
    ?assertEqual(ok, archived(0, AssignmentID, F2)),
    stop_stubs([ArPid, TurboPid]).

%% A gateway that refuses part-way through a read keeps what it fetched, and
%% the next read asks only for the rest. Windows wholly at or below the
%% indexed slot, and the second, blockless listing arweave.net gives every
%% archive item, are never fetched.
partial_read_is_kept_test_() ->
    {timeout, 30, fun partial_read_is_kept/0}.
partial_read_is_kept() ->
    Old = {OldID, OldEdge, _, _} = archive_window(0, 3, 90),
    A = {AID, AEdge, _, [A4, _]} = archive_window(4, 5, 95),
    B = {BID, BEdge, _, [_, B7]} = archive_window(6, 7, 96),
    Windows = [Old, A, B],
    Edges = [OldEdge, AEdge, BEdge, edge(AID, 4, 5, null)],
    BPath = <<"/raw/", BID/binary>>,
    Flaky =
        fun('GET', Path) when Path =:= BPath -> {429, [{<<"retry-after">>, <<"1">>}], <<>>};
           (M, P) -> (archive_gateway(Edges, Windows))(M, P)
        end,
    {FlakyURL, FlakyPid} = stub_gateway(flaky, Flaky, self()),
    F = (archive_state([FlakyURL]))#{ indexed => 3 },
    Now = now_ms(),
    F1 = with_windows(F, Now),
    % height, graphql, A, then B refused: A is kept and Old never asked for.
    ?assertEqual(4, hits(flaky)),
    ?assertEqual([AID], [ maps:get(id, W) || W <- maps:get(windows, F1) ]),
    ?assertEqual(ok, archived(4, A4, F1)),
    ?assertEqual(not_yet, archived(7, B7, F1)),
    stop_stubs([FlakyPid]),
    {GoodURL, GoodPid} = stub_gateway(good, archive_gateway(Edges, Windows), self()),
    F2 = with_windows(F1#{ gateways := gateways(
        #{ <<"scheduler-follower-gateways">> => [GoodURL] }) }, Now + ?DEFAULT_ARCHIVE_POLL_MS),
    % height, graphql and B only.
    ?assertEqual(3, hits(good)),
    ?assertEqual(lists:sort([AID, BID]),
        lists:sort([ maps:get(id, W) || W <- maps:get(windows, F2) ])),
    ?assertNot(lists:member(OldID, [ maps:get(id, W) || W <- maps:get(windows, F2) ])),
    ?assertEqual(ok, archived(7, B7, F2)),
    stop_stubs([GoodPid]).

%% With no push waiting the follower polls at the idle interval; a push
%% refused for a slot not yet indexed brings the next pass to `poll-ms', and
%% it falls back to idle once that slot is indexed. Arweave is not read
%% between archive polls.
no_polling_when_nothing_pending_test() ->
    F = (archive_state([<<"http://localhost:1">>]))#{ indexed => 4, wanted => 4 },
    ?assertEqual(?DEFAULT_IDLE_POLL_MS, interval(F)),
    Now = now_ms(),
    Waiting = wanted(F#{ next_at => Now + ?DEFAULT_IDLE_POLL_MS }, 6, Now),
    ?assertEqual(Now + 5000, maps:get(next_at, Waiting)),
    ?assertEqual(5000, interval(Waiting)),
    % A push for a slot already indexed is not a reason to poll fast.
    ?assertEqual(?DEFAULT_IDLE_POLL_MS, interval(wanted(F#{ next_at => Now }, 3, Now))),
    ?assertEqual(?DEFAULT_IDLE_POLL_MS, interval(Waiting#{ indexed := 6 })),
    Read = Waiting#{ archive_at := Now + 1000 },
    ?assertEqual(Read, with_windows(Read, Now)).

%%% Batches and progress -------------------------------------------------------

%% A Lua process that sends one message a slot, its data the slot's count.
counting_sender(Opts) ->
    Script = <<
        "Count = Count or 0\n"
        "function compute(req)\n"
        "  Count = Count + 1\n"
        "  local value = tostring(Count)\n"
        "  return {\n"
        "    patches = { { path = '/count', value = value } },\n"
        "    results = { output = { data = value },\n"
        "      outbox = { { target = 'x', data = 'ping-' .. value } } }\n"
        "  }\n"
        "end\n"
    >>,
    Process = hb_process_test_vectors:lua_patch_process(Script, Opts),
    {ok, _} = hb_cache:write(Process, Opts),
    Process.

%% The options a replica computes under (`lib_bridge_replica:execution_opts/2'),
%% with `Target' as its `process-snapshot-target'.
replica_like_opts(Target) ->
    #{ <<"store">> => hb_test_utils:test_store(hb_store_lmdb),
       <<"priv-wallet">> => ar_wallet:new(),
       <<"process-state-blobs">> => true, <<"process-delta-cache">> => false,
       <<"process-snapshot-slots">> => 1000, <<"process-snapshot-time">> => false,
       <<"process-snapshot-target">> => Target,
       <<"process-compute-on-schedule">> => false, <<"process-workers">> => false,
       <<"spawn-worker">> => false, <<"process-worker-spawn">> => false,
       <<"await-inprogress">> => false }.

executions() ->
    lists:sum([ N || {slot_execution, _, N, _} <- hb_phase_timer:read() ]).

compute(Process, Slot, Opts) ->
    {ok, _} = hb_ao:resolve(Process, #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, Opts).

%% 2026-11-16: the follower computed its replica one call per slot, and a
%% call resumes from the newest VM snapshot, which with interval snapshots
%% alone was up to 1,000 slots back: 0.07 slots/s. With the target slot
%% snapshotted every slot executes once, slot by slot or in a batch.
replica_slot_executes_once_test_() ->
    {timeout, 120, fun replica_slot_executes_once/0}.
replica_slot_executes_once() ->
    application:ensure_all_started(hb),
    Slots = 8,
    Run =
        fun(Target) ->
            Opts = replica_like_opts(Target),
            Process = counting_sender(Opts),
            [ hb_process_test_vectors:schedule_test_message(Process, <<"go">>, #{}, Opts)
            || _ <- lists:seq(1, Slots) ],
            Before = executions(),
            [ compute(Process, N, Opts) || N <- lists:seq(0, Slots - 1) ],
            executions() - Before
        end,
    ?assertEqual(Slots, Run(true)),
    % Without it, each call replays the slots before it again (29 executions
    % for 8 slots here; slot 0 is a checkpoint).
    ?assert(Run(false) > 3 * Slots).

%% One call computes a batch, and each slot's outbox is read back from its
%% stored results, as the follower indexes it.
batch_outboxes_read_from_stored_results_test_() ->
    {timeout, 120, fun batch_outboxes_read_from_stored_results/0}.
batch_outboxes_read_from_stored_results() ->
    application:ensure_all_started(hb),
    Opts = replica_like_opts(true),
    Process = counting_sender(Opts),
    ProcID = lib_process:process_id(Process, #{}, Opts),
    [ hb_process_test_vectors:schedule_test_message(Process, <<"go">>, #{}, Opts)
    || _ <- lists:seq(1, 6) ],
    Before = executions(),
    compute(Process, 5, Opts),
    ?assertEqual(6, executions() - Before),
    lists:foreach(
        fun(N) ->
            [Entry] = maps:values(outbox(results(ProcID, N, Opts), Opts)),
            ?assertEqual(<<"ping-", (integer_to_binary(N + 1))/binary>>,
                hb_maps:get(<<"data">>, Entry, undefined, Opts))
        end,
        lists:seq(0, 5)),
    % The next batch resumes from the last one's snapshot.
    [ hb_process_test_vectors:schedule_test_message(Process, <<"go">>, #{}, Opts)
    || _ <- lists:seq(1, 3) ],
    compute(Process, 8, Opts),
    ?assertEqual(9, executions() - Before).

%% The deliverer is told to post again when the follower should have indexed
%% the slot, at its measured rate, between `poll-ms' and ten minutes; told
%% nothing without a rate or for a slot already indexed.
retry_after_follows_progress_test() ->
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assertEqual(undefined, retry_after(P, 50, #{})),
    F = progress(#{ p => P, indexed => 100, progress => {now_ms() - 10000, 0, undefined} }),
    {100, Rate} = progress_of(P),
    ?assert(Rate > 9 andalso Rate =< 10),
    % 100 slots ahead at ~10 slots/s.
    Ahead = retry_after(P, 200, #{}),
    ?assert(Ahead >= 10 andalso Ahead =< 12),
    ?assertEqual(5, retry_after(P, 101, #{})),
    ?assertEqual(600, retry_after(P, 1000000, #{})),
    ?assertEqual(undefined, retry_after(P, 100, #{})),
    ?assertEqual(undefined, retry_after(P, 3, #{})),
    % The rate is smoothed over batches.
    timer:sleep(5),
    _ = progress(F#{ indexed := 100 }),
    {100, Slower} = progress_of(P),
    ?assert(Slower < Rate).

%% Windows the index has passed are dropped.
windows_pruned_test() ->
    F = #{ windows => [#{ id => <<"a">>, from => 0, to => 4, ids => [] },
                       #{ id => <<"b">>, from => 5, to => 9, ids => [] }], indexed => 4 },
    ?assertEqual([<<"b">>], [ maps:get(id, W) || W <- maps:get(windows, prune(F)) ]).

%% A listing is whole the first time and every `?FULL_LISTING_EVERY'-th
%% time, and otherwise starts `?RELIST_BLOCKS' below the highest block held.
listing_floor_test() ->
    ?assertEqual(undefined, floor_height(#{})),
    Held = listed(#{ windows => [#{ id => <<"a">>, from => 0, to => 1, ids => [],
        height => 1000 }] }),
    ?assertEqual(1000, maps:get(top_height, Held)),
    ?assertEqual(1000 - ?RELIST_BLOCKS, floor_height(Held)),
    % Pruned windows leave the highest block as it was.
    Pruned = listed(Held#{ windows := [] }),
    ?assertEqual(1000, maps:get(top_height, Pruned)),
    ?assertEqual(undefined,
        floor_height(Pruned#{ listings := ?FULL_LISTING_EVERY })).

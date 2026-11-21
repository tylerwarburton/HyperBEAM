%%% @doc Publishes a process's schedule to Arweave: every assignment, and the
%%% message it carries, with separate posted and mined-confirmed progress.
%%%
%%% One uploader runs beside each scheduling server whose node publishes
%%% (`scheduler-publish-remote'). The server tells it each slot it confirms;
%%% the uploader reads that slot back from the scheduler's store and posts a
%%% window of slots as ONE signed archive item (see `prepare_window/6' for its
%%% shape): once `window-slots' slots are waiting, or the oldest has waited
%%% `latency-ms'. Signed windows are flushed before POST.
%%% `posted/<pid>' tracks bundler acceptance; `confirmed/<pid>' tracks only
%%% contiguous windows whose payloads and archive are mined to the configured
%%% depth. Legacy `uploaded/<pid>' is a replay target, never confirmation
%%% evidence. Pending windows are retried byte-for-byte after a timeout.
%%%
%%% Nothing here is on the scheduling path: the server only sends a message,
%%% so a slow or absent bundler slows nobody's assignment. A client asking for
%%% `remote_confirmation' is answered by the uploader, once its slot is up.
%%%
%%% Uploading costs the node's bundler credit, so only the processes the
%%% operator lists are archived. Anyone may schedule on a public node; nobody
%%% else's schedule is paid for. A process on `scheduler-upload-processes' is
%%% archived whole (every signed message, compressed); one on
%%% `scheduler-fingerprint-processes' only by the ID of each window's last
%%% assignment, which commits to every slot before it through the assignments'
%%% `base-hashpath' chain. Either lets another node's follower
%%% (`dev_scheduler_follower') verify the process; only the first lets anyone
%%% rebuild it from Arweave alone. Off both lists nothing is uploaded.
%%%
%%% A fingerprint window waits `scheduler-fingerprint-latency-ms' (default an
%%% hour) instead of `latency-ms': it pays the item fee for 32 bytes.
%%%
%%% Every process this node schedules and publishes for also has its spawn,
%%% and the module it names by ID, posted once as the signed items they
%%% are (`scheduler-upload-spawns', default true): an explorer such as Lunar
%%% opens a process by its ID on Arweave, whatever its schedule's mode. A
%%% spawn that is not an ANS-104 item cannot keep its ID there and is not
%%% posted. `uploaded-spawn/<id>' records each that was taken.
%%%
%%% A bundler that answers 402 (or `x-free-tier-exhausted') is unfunded: that
%%% is logged once as an error, `status/0' says so, and the uploader waits
%%% `scheduler-upload-unfunded-backoff-ms' (default 15 minutes) between tries
%%% instead of a minute; a `retry-after' is honoured. `status/0' also counts
%%% the bytes taken, per mode, and their cost a day at
%%% `scheduler-upload-usd-per-gib' (default 47.29: Turbo by card, 2026-11-21).
%%%
%%% Format 2 (`Archive-Format: 2'), the only one written: tags `Process',
%%% `From-Slot', `To-Slot', `Last-Assignment' (the signed ID of slot
%%% `To-Slot''s assignment) and `Archive-Data'. `full': the data is the window's
%%% ANS-104 bundle (bodies, assignments, linked messages: exact signed bytes),
%%% zlib-compressed at level 9 (`Archive-Encoding: zlib'); `fingerprint': the
%%% data is the 32 raw bytes of `Last-Assignment'. Format 1 (no
%%% `Archive-Format') was an outer bundle of link-free items plus an
%%% uncompressed archive item; windows persisted in it still post and confirm.
-module(dev_scheduler_uploader).
-export([start/2, cursor/2, posted/2, allowed/2, mode/2, list_option/2]).
-export([archive_bundle/2, prepare_window/6, status/0]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(FORMAT, 2).
-define(DEFAULT_WINDOW_SLOTS, 1000).
%% Uncompressed bytes a full window may carry before it is cut (at least one
%% slot always goes).
-define(DEFAULT_WINDOW_BYTES, 8 * 1024 * 1024).
-define(DEFAULT_LATENCY_MS, 60000).
-define(MIN_BACKOFF, 1000).
-define(MAX_BACKOFF, 60000).
-define(DEFAULT_FINGERPRINT_LATENCY_MS, 3600000).
-define(DEFAULT_UNFUNDED_BACKOFF_MS, 900000).
-define(DEFAULT_USD_PER_GIB, 47.29).
-define(STATUS, {?MODULE, status}).

%% @doc Start the uploader of `ProcID', linked to the caller, or `undefined'
%% for a process on neither upload list.
start(ProcID, Opts) ->
    ok = upload_spawn(ProcID, Opts),
    case mode(ProcID, Opts) of
        none ->
            ?event(warning, {schedule_upload_skipped, {proc_id, ProcID},
                {reason, not_in_scheduler_upload_processes}}),
            undefined;
        Mode -> start_uploader(ProcID, Mode, Opts)
    end.

%% @doc Whether this node pays to archive `ProcID' whole.
allowed(ProcID, Opts) -> mode(ProcID, Opts) =:= full.

%% @doc `full', `fingerprint' or `none': how this node archives `ProcID'.
%% A process on both lists is archived whole.
mode(ProcID, Opts) ->
    Listed = fun(Key) ->
        lists:member(hb_util:human_id(ProcID), list_option(hb_opts:get(Key, [], Opts), Opts))
    end,
    case {Listed(scheduler_upload_processes), Listed(scheduler_fingerprint_processes)} of
        {true, _} -> full;
        {false, true} -> fingerprint;
        _ -> none
    end.

%% @doc A list-valued node option as a list of binaries, however it arrived:
%% a list, a numbered message, a JSON array or a comma-separated binary.
%% Absent or empty is `[]'.
list_option(Value, Opts) when is_map(Value) ->
    list_option(hb_util:message_to_ordered_list(Value, Opts), Opts);
list_option(Value, _Opts) when is_list(Value), Value =/= [], is_integer(hd(Value)) ->
    list_option(list_to_binary(Value), _Opts);
list_option(Value, _Opts) when is_list(Value) ->
    [ B || V <- Value, B <- [string:trim(hb_util:bin(V))], B =/= <<>> ];
list_option(<<"[", _/binary>> = JSON, Opts) ->
    list_option(hb_json:decode(JSON), Opts);
list_option(Value, Opts) when is_binary(Value) ->
    list_option(binary:split(Value, <<",">>, [global]), Opts);
list_option(_, _Opts) -> [].

start_uploader(ProcID, Mode, Opts) ->
    Int = fun(Key, Default) -> hb_util:int(hb_opts:get(Key, Default, Opts)) end,
    spawn_link(
        fun() ->
            loop(#{
                id => ProcID,
                mode => Mode,
                opts => Opts,
                cursor => posted(ProcID, Opts),
                confirmed => cursor(ProcID, Opts),
                target => read_cursor(ProcID, <<"uploaded">>, Opts),
                check_at => 0,
                waiting => [],
                backoff => ?MIN_BACKOFF,
                pending_since => undefined,
                window_slots =>
                    max(1, Int(scheduler_upload_window_slots, ?DEFAULT_WINDOW_SLOTS)),
                window_bytes => Int(scheduler_upload_window_bytes, ?DEFAULT_WINDOW_BYTES),
                latency =>
                    case Mode of
                        fingerprint ->
                            Int(scheduler_fingerprint_latency_ms,
                                ?DEFAULT_FINGERPRINT_LATENCY_MS);
                        full -> Int(scheduler_upload_latency_ms, ?DEFAULT_LATENCY_MS)
                    end
            })
        end
    ).

%% @doc The highest slot below which (and at which) every slot is confirmed,
%% or -1.
cursor(ProcID, Opts) ->
    read_cursor(ProcID, <<"confirmed">>, Opts).

%% @doc The contiguous range accepted by the bundler, without mining claims.
posted(ProcID, Opts) ->
    read_cursor(ProcID, <<"posted">>, Opts).

read_cursor(ProcID, Kind, Opts) ->
    Store = store(Opts),
    Req = #{ <<"read">> => progress_path(ProcID, Kind), <<"exact">> => true },
    case hb_store:read(Store, Req, Opts) of
        {ok, Slot} -> hb_util:int(Slot);
        _ -> -1
    end.

loop(State0) ->
    State = pending(confirm_window(take(State0, 0)), now_ms()),
    case due(State, now_ms()) of
        true -> loop(upload_window(State));
        false -> loop(take(answer(State), wake_after(State, now_ms())))
    end.

%% Since when the oldest slot not posted yet has waited: set when one first
%% waits, kept while any does.
pending(State = #{ cursor := C, target := T }, _Now) when C >= T ->
    State#{ pending_since => undefined };
pending(State = #{ pending_since := undefined }, Now) ->
    State#{ pending_since => Now };
pending(State, _Now) -> State.

%% A window goes up once it is full or its oldest slot has waited `latency'.
due(#{ cursor := C, target := T }, _Now) when C >= T -> false;
due(#{ cursor := C, target := T, window_slots := Slots }, _Now) when T - C >= Slots -> true;
due(#{ pending_since := Since, latency := Latency }, Now) -> Now - Since >= Latency.

%% Wake for the next confirmation check or the next window, whichever is
%% first. Not `erlang:min/2' over `infinity': OTP's range analysis of it
%% folded `wake_after(...) =:= 3000' to false although the value was 3000.
wake_after(State = #{ confirmed := Confirmed, cursor := Cursor }, Now) ->
    Check = [ 1000 || Confirmed < Cursor ],
    Flush =
        case State of
            #{ pending_since := undefined } -> [];
            #{ pending_since := Since, latency := Latency } -> [max(0, Since + Latency - Now)]
        end,
    case Check ++ Flush of
        [] -> infinity;
        Waits -> lists:min(Waits)
    end.

now_ms() -> erlang:monotonic_time(millisecond).

%% @doc Take every slot the server has sent, waiting up to `Timeout' for the
%% first.
take(State = #{ target := Target, waiting := Waiting }, Timeout) ->
    receive
        {durable, Slot, Waiter} ->
            take(
                State#{
                    target => max(Target, Slot),
                    waiting =>
                        case Waiter of
                            none -> Waiting;
                            _ -> [{Slot, Waiter} | Waiting]
                        end
                },
                0
            )
    after Timeout -> State
    end.

%% @doc Post the next window, and move the cursor past it. If the bundler does
%% not take it, wait and try again.
upload_window(State = #{ id := ProcID, opts := Opts, cursor := Cursor,
        target := Target, backoff := Backoff, window_slots := Slots }) ->
    Last = min(Target, Cursor + Slots),
    case post_window(ProcID, Cursor + 1, Last, State) of
        {ok, PostedLast} ->
            ok = hb_store:write(
                store(Opts),
                #{ progress_path(ProcID, <<"posted">>) => hb_util:bin(PostedLast) },
                Opts
            ),
            ok = hb_store:flush(store(Opts), Opts),
            funded(true),
            State#{ cursor => PostedLast, backoff => ?MIN_BACKOFF };
        Error ->
            Wait = refused_wait(ProcID, Error, Backoff, Opts),
            ?event(warning,
                {schedule_upload_retrying,
                    {proc_id, ProcID},
                    {slot, Cursor + 1},
                    {reason, Error},
                    {backoff_ms, Wait}
                }
            ),
            timer:sleep(Wait),
            State#{ backoff => min(?MAX_BACKOFF, Backoff * 2) }
    end.

%% How long to wait after a refused post: the unfunded backoff when it was
%% for want of payment, else the backoff or the bundler's `retry-after'.
refused_wait(ID, Error, Backoff, Opts) ->
    case unfunded(Error) of
        true ->
            funded(false, ID, Error),
            hb_util:int(hb_opts:get(scheduler_upload_unfunded_backoff_ms,
                ?DEFAULT_UNFUNDED_BACKOFF_MS, Opts));
        false -> max(Backoff, retry_after_ms(Error))
    end.

%% @doc The uploads of this node: whether its bundler is funded (and since
%% when not), and per mode the bytes and posts it took since the node
%% started, with their rate and cost a day.
status() ->
    S = persistent_term:get(?STATUS, #{}),
    Days = max(1 / 24, (now_ms() - maps:get(started_ms, S, now_ms())) / 86400000),
    Price = maps:get(usd_per_gib, S, ?DEFAULT_USD_PER_GIB),
    Modes =
        maps:map(
            fun(_, #{ bytes := B, posts := N }) ->
                PerDay = B / Days,
                #{ bytes => B, posts => N, bytes_per_day => round(PerDay),
                   usd_per_day => PerDay / (1 bsl 30) * Price }
            end,
            maps:get(posted, S, #{})),
    maps:merge(maps:with([unfunded_since, last_refusal], S),
        #{ funded => not maps:get(unfunded, S, false), posted => Modes }).

status_update(Fun) ->
    S = persistent_term:get(?STATUS, #{ started_ms => now_ms() }),
    persistent_term:put(?STATUS, Fun(S)).

%% A post taken: its bytes, under its mode (`full', `fingerprint', `spawn').
count_posted(Mode, Bytes, Opts) ->
    status_update(fun(S) ->
        Posted = maps:get(posted, S, #{}),
        #{ bytes := B, posts := N } = maps:get(Mode, Posted, #{ bytes => 0, posts => 0 }),
        S#{ usd_per_gib =>
                hb_opts:get(scheduler_upload_usd_per_gib, ?DEFAULT_USD_PER_GIB, Opts),
            posted => Posted#{ Mode => #{ bytes => B + Bytes, posts => N + 1 } } }
    end).

%% The bundler took a post: funded again, if it was not.
funded(true) ->
    case maps:get(unfunded, persistent_term:get(?STATUS, #{}), false) of
        true ->
            ?event(warning, {schedule_upload_funded_again}),
            status_update(fun(S) -> maps:without([unfunded_since], S#{ unfunded => false }) end);
        false -> ok
    end.

%% A refusal for want of payment: an error the first time, then quiet.
funded(false, ID, Error) ->
    case maps:get(unfunded, persistent_term:get(?STATUS, #{}), false) of
        true -> ok;
        false ->
            ?event(error, {schedule_upload_unfunded, {id, ID}, {refusal, Error}}),
            status_update(fun(S) ->
                S#{ unfunded => true, unfunded_since => now_ms(), last_refusal => Error }
            end)
    end.

%% @doc Whether a bundler refused for want of payment: 402, or Turbo's
%% `x-free-tier-exhausted' header.
unfunded({error, {402, _, _, _, _}}) -> true;
unfunded({error, {_Status, _URL, _Bytes, {headers, Headers}, _Body}}) ->
    lists:member({<<"x-free-tier-exhausted">>, <<"true">>},
        [ {string:lowercase(K), string:lowercase(V)} || {K, V} <- Headers ]);
unfunded(_) -> false.

%% A refusal's `retry-after' in milliseconds, or 0.
retry_after_ms({error, {_Status, _URL, _Bytes, {headers, Headers}, _Body}}) ->
    case [ V || {K, V} <- Headers, string:lowercase(K) =:= <<"retry-after">> ] of
        [V | _] -> try binary_to_integer(V) * 1000 catch _:_ -> 0 end;
        [] -> 0
    end;
retry_after_ms(_) -> 0.

%%% Spawns.

%% @doc Post `ProcID''s spawn, and the module it names by ID, as the signed
%% ANS-104 items they are, once, off the caller: a node that publishes
%% uploads them for every process it schedules.
upload_spawn(ProcID, Opts) ->
    case hb_opts:get(scheduler_upload_spawns, true, Opts) of
        false -> ok;
        _ ->
            _ = spawn(fun() ->
                case upload_item(ProcID, Opts) of
                    {ok, Msg} ->
                        case hb_maps:get(<<"module">>, Msg, undefined, Opts) of
                            Module when ?IS_ID(Module) -> upload_item(Module, Opts);
                            _ -> ok
                        end;
                    _ -> ok
                end
            end),
            ok
    end.

%% Post one stored message under its own ID, unless it was posted before:
%% `{ok, Msg}' when the message is stored here.
upload_item(ID0, Opts) ->
    ID = hb_util:human_id(ID0),
    Marker = hb_path:to_binary([<<"~scheduler@1.0">>, <<"uploaded-spawn">>, ID]),
    case hb_cache:read(ID, Opts) of
        {ok, Raw} ->
            Msg = hb_cache:ensure_all_loaded(Raw, Opts),
            Done = hb_store:read(store(Opts), #{ <<"read">> => Marker, <<"exact">> => true }, Opts),
            Signed = lists:member(<<"ans104@1.0">>, hb_message:commitment_devices(Msg, Opts)),
            case {Done, Signed} of
                {{ok, _}, _} -> ok;
                {_, false} -> ?event(warning, {spawn_not_uploaded, {id, ID}, {reason, not_ans104}});
                {_, true} ->
                    post_item(ID, ar_bundles:serialize(item(Msg, Opts)), Marker, ?MIN_BACKOFF, Opts)
            end,
            {ok, Msg};
        _ ->
            ?event(warning, {spawn_not_uploaded, {id, ID}, {reason, not_stored}}),
            not_found
    end.

post_item(ID, Serialized, Marker, Backoff, Opts) ->
    case post(Serialized, Opts) of
        ok ->
            count_posted(spawn, byte_size(Serialized), Opts),
            funded(true),
            ok = hb_store:write(store(Opts), #{ Marker => <<"posted">> }, Opts),
            ok = hb_store:flush(store(Opts), Opts);
        Error ->
            Wait = refused_wait(ID, Error, Backoff, Opts),
            ?event(warning, {spawn_upload_retrying, {id, ID}, {reason, Error}, {backoff_ms, Wait}}),
            timer:sleep(Wait),
            post_item(ID, Serialized, Marker, min(?MAX_BACKOFF, Backoff * 2), Opts)
    end.

%% @doc Release clients only through the confirmed contiguous prefix.
answer(State = #{ confirmed := Cursor, waiting := Waiting }) ->
    {Done, Rest} = lists:partition(fun({S, _}) -> S =< Cursor end, Waiting),
    lists:foreach(
        fun({_, {Reply, Message, Assignment}}) ->
            Reply ! {scheduled, Message, Assignment}
        end,
        Done
    ),
    State#{ waiting => Rest }.

%% @doc Sign a window and post it: `{ok, LastSlot}', or the reason it was
%% not taken. One request carries the whole window, which keeps a busy node
%% under the bundler's request limit. A window already signed for `First'
%% (in either format) is posted again byte for byte.
post_window(ProcID, First, Max, #{ mode := Mode, window_bytes := Bytes, opts := Opts }) ->
    try
        Path = window_path(ProcID, First),
        {Last, Serialized} =
            case hb_store:read(store(Opts),
                    #{ <<"read">> => Path, <<"exact">> => true }, Opts) of
                {ok, Saved} -> saved_window(binary_to_term(Saved, [safe]));
                {error, not_found} ->
                    Prepared = prepare_window(ProcID, First, Max, Mode, Bytes, Opts),
                    ok = hb_store:write(store(Opts),
                        #{ Path => term_to_binary(Prepared) }, Opts),
                    ok = hb_store:flush(store(Opts), Opts),
                    saved_window(Prepared);
                ReadError -> throw({upload_window_read_failed, ReadError})
            end,
        case post(Serialized, Opts) of
            ok ->
                ok = mark_posted(ProcID, First, Opts),
                count_posted(Mode, byte_size(Serialized), Opts),
                {ok, Last};
            Error -> Error
        end
    catch Class:Reason -> {error, {Class, Reason}}
    end.

%% A persisted window as `{LastSlot, Bytes}': format 2 is tagged, format 1
%% (written before compression) is the bare pair.
saved_window({?FORMAT, Last, Bytes}) -> {Last, Bytes};
saved_window({Last, Bytes}) -> {Last, Bytes}.

%% @doc Freeze the signed bytes and slot range before the first POST. Retries
%% reuse this record even when the scheduling head advances or the node restarts.
%%
%% One item signed by the node, found by its `Process', `From-Slot' and
%% `To-Slot' tags and carrying no link (`ao:<id>'), which a bundler would
%% refuse to take unresolved. In a full window every message the window links
%% to that this node holds (a process's module, say) goes in with it, so the
%% schedule can be rebuilt from Arweave alone. The item's ID is not
%% reproducible (RSA-PSS signatures are salted); its data is, for the same
%% window.
prepare_window(ProcID, First, Max, Mode, MaxBytes, Opts) ->
    {Read, Items, LastSlot} =
        case Mode of
            full ->
                {R, I} = read_window(ProcID, First, Max, MaxBytes, Opts),
                {R, I, First + length(R) - 1};
            fingerprint -> {[read_slot(ProcID, Max, Opts)], [], Max}
        end,
    {_, LastAssignment} = lists:last(Read),
    LastID = hb_util:human_id(hb_message:id(LastAssignment, signed, Opts)),
    Tags = [
        {<<"Data-Protocol">>, <<"ao-schedule-archive">>},
        {<<"Archive-Format">>, integer_to_binary(?FORMAT)},
        {<<"Process">>, hb_util:human_id(ProcID)},
        {<<"From-Slot">>, hb_util:bin(First)},
        {<<"To-Slot">>, hb_util:bin(LastSlot)},
        {<<"Last-Assignment">>, LastID}
    ],
    Item =
        case Mode of
            full ->
                sign(compress(bundle_data(Items ++ linked(Items, Opts))),
                    Tags ++ [
                        {<<"Archive-Data">>, <<"full">>},
                        {<<"Archive-Encoding">>, <<"zlib">>},
                        {<<"Content-Type">>, <<"application/octet-stream">>}
                    ],
                    Opts);
            fingerprint ->
                sign(hb_util:native_id(LastID),
                    Tags ++ [{<<"Archive-Data">>, <<"fingerprint">>}], Opts)
        end,
    {?FORMAT, LastSlot, ar_bundles:serialize(Item)}.

%% The slots `First'..`Max' and their items, bodies first then assignments
%% (format 1's order), cut before the slot that would take the window past
%% `MaxBytes' uncompressed; the first slot always goes.
read_window(ProcID, First, Max, MaxBytes, Opts) ->
    read_window(ProcID, First, Max, MaxBytes, Opts, 0, []).
read_window(_ProcID, Slot, Max, _MaxBytes, _Opts, _Size, Acc) when Slot > Max ->
    window_items(lists:reverse(Acc));
read_window(ProcID, Slot, Max, MaxBytes, Opts, Size, Acc) ->
    {Body, Assignment} = read_slot(ProcID, Slot, Opts),
    BodyItem = item(Body, Opts),
    AssignmentItem = item(Assignment, Opts),
    Next = Size + byte_size(ar_bundles:serialize(BodyItem))
        + byte_size(ar_bundles:serialize(AssignmentItem)),
    case Acc =/= [] andalso Next > MaxBytes of
        true -> window_items(lists:reverse(Acc));
        false ->
            read_window(ProcID, Slot + 1, Max, MaxBytes, Opts, Next,
                [{{Body, Assignment}, BodyItem, AssignmentItem} | Acc])
    end.

window_items(Slots) ->
    {[ R || {R, _, _} <- Slots ],
        [ B || {_, B, _} <- Slots ] ++ [ A || {_, _, A} <- Slots ]}.

%% Deterministic for the same input and zlib build: level 9, zlib framing.
compress(Data) ->
    Z = zlib:open(),
    try
        ok = zlib:deflateInit(Z, 9, deflated, 15, 9, default),
        Out = iolist_to_binary(zlib:deflate(Z, Data, finish)),
        ok = zlib:deflateEnd(Z),
        Out
    after zlib:close(Z)
    end.

%% @doc The ANS-104 bundle of signed items a format 2 `full' archive carries,
%% from its tags and data; `{error, Why}' for a window that carries none.
%% Format 1 data is already that bundle. For offline readers: the follower
%% needs only the tags.
archive_bundle(Tags, Data) ->
    Get = fun(Name) -> proplists:get_value(Name, Tags) end,
    case {Get(<<"Archive-Format">>), Get(<<"Archive-Data">>), Get(<<"Archive-Encoding">>)} of
        {undefined, _, _} -> {ok, Data};
        {<<"2">>, <<"full">>, <<"zlib">>} -> {ok, zlib:uncompress(Data)};
        {<<"2">>, <<"fingerprint">>, _} -> {error, fingerprint_only};
        Other -> {error, {unknown_archive_format, Other}}
    end.

%% @doc The IDs an item names by link.
links(#tx{ tags = Tags }) ->
    [ ID || {_, <<"ao:", ID/binary>>} <- Tags, byte_size(ID) == 43 ].

%% @doc Every message the items link to, and those link to, that is not among
%% them and that this node holds.
linked(Items, Opts) ->
    Have = lists:flatmap(fun held/1, Items),
    linked(lists:usort(lists:flatmap(fun links/1, Items)) -- Have, Have, Opts).

%% @doc The IDs an item stands for: its own, and the message it carries if the
%% node wrapped one. A process signed with `httpsig@1.0' rides in a wrapper with
%% an ID of its own, and its assignment links to it by the message's ID: without
%% the second, the process went up twice in every first window.
held(I = #tx{ tags = Tags }) ->
    [hb_util:human_id(I#tx.id) | [ ID || {<<"Message-Id">>, ID} <- Tags ]].
linked([], _Have, _Opts) -> [];
linked([ID | IDs], Have, Opts) ->
    case lists:member(ID, Have) of
        true -> linked(IDs, Have, Opts);
        false ->
            case hb_cache:read(ID, Opts) of
                {ok, Msg} ->
                    Item = item(hb_cache:ensure_all_loaded(Msg, Opts), Opts),
                    [Item | linked(IDs ++ links(Item), [ID | Have], Opts)];
                _ -> linked(IDs, [ID | Have], Opts)
            end
    end.

read_slot(ProcID, Slot, Opts) ->
    case dev_scheduler_cache:read(ProcID, Slot, Opts) of
        {ok, Assignment} ->
            {hb_maps:get(<<"body">>, Assignment, #{}, Opts), Assignment};
        not_found -> throw({assignment_not_found, Slot})
    end.

bundle_data(Items) ->
    {undefined, Data} = ar_bundles:serialize_bundle(list, Items, false),
    Data.

sign(Data, Tags, Opts) ->
    ar_bundles:sign_item(
        #tx{ format = ans104, data = Data, data_size = byte_size(Data), tags = Tags },
        hb_opts:get(priv_wallet, hb:wallet(), Opts)
    ).

%% @doc A message as an Arweave item. A message signed as an ANS-104 item is
%% that item, its own ID and signature: only that commitment is taken, since a
%% message read back from the store also carries an unsigned `httpsig@1.0'
%% one. Any other message (signed with `httpsig@1.0', as a process's pushes and
%% the deploy tooling's writes are) cannot be an item of its own, so the node
%% signs an item that carries it whole, commitments and all, as JSON: tagged
%% with its ID, and verifiable by anyone who decodes it.
item(Msg, Opts) ->
    Device = <<"ans104@1.0">>,
    case lists:member(Device, hb_message:commitment_devices(Msg, Opts)) of
        true ->
            hb_message:convert(
                hb_message:with_commitments(
                    #{ <<"commitment-device">> => Device }, Msg, Opts),
                Device,
                <<"structured@1.0">>,
                Opts
            );
        false ->
            sign(
                hb_message:convert(Msg, <<"json@1.0">>, <<"structured@1.0">>, Opts),
                [
                    {<<"Content-Type">>, <<"application/json">>},
                    {<<"Message-Id">>, hb_message:id(Msg, signed, Opts)}
                ],
                Opts
            )
    end.

post(Serialized, Opts) ->
    URL = upload_url(Opts),
    case hb_http_gate:httpc_request(
            post,
            {URL, [], "application/octet-stream", Serialized},
            [{timeout, 60000}],
            [{body_format, binary}],
            Opts) of
        {ok, {{_, Status, _}, _, _}} when Status >= 200, Status < 300 -> ok;
        {ok, {{_, Status, _}, Headers, Body}} ->
            {error,
                {Status,
                    {url, hb_util:bin(URL)},
                    {bytes, byte_size(Serialized)},
                    {headers,
                        [ {hb_util:bin(K), hb_util:bin(V)}
                        || {K, V} <- Headers, logged_header(K) ]},
                    {body, binary:part(Body, 0, min(1024, byte_size(Body)))}
                }
            };
        Error -> Error
    end.

%% @doc Where a window is posted: `scheduler-upload-url' whole, or the
%% `bundler-ans104' node's `/tx'. A Turbo-style bundler takes items at
%% `/v1/tx', which no `bundler-ans104' base can name.
upload_url(Opts) ->
    case hb_opts:get(scheduler_upload_url, not_found, Opts) of
        not_found ->
            hb_util:list(hb_opts:get(bundler_ans104, no_bundler, Opts)) ++ "/tx";
        URL -> hb_util:list(URL)
    end.

%% @doc The response headers that say why a bundler refused a window.
logged_header(Name) ->
    Lower = string:lowercase(Name),
    lists:member(Lower, ["retry-after", "content-type", "reason", "status"])
        orelse lists:prefix("x-", Lower).

store(Opts) ->
    hb_opts:get(scheduler_store, hb_opts:get(store, no_viable_store, Opts), Opts).

progress_path(ProcID, Kind) ->
    hb_path:to_binary(
        [<<"~scheduler@1.0">>, Kind, hb_util:human_id(ProcID)]).

window_path(ProcID, First) ->
    hb_path:to_binary([<<"~scheduler@1.0">>, <<"upload-windows">>,
        hb_util:human_id(ProcID), hb_util:bin(First)]).

mark_posted(ProcID, First, Opts) ->
    ok = hb_store:write(store(Opts),
        #{ posted_at_path(ProcID, First) =>
            integer_to_binary(erlang:system_time(second)) }, Opts),
    hb_store:flush(store(Opts), Opts).

posted_at_path(ProcID, First) ->
    hb_path:to_binary([<<"~scheduler@1.0">>, <<"upload-posted-at">>,
        hb_util:human_id(ProcID), hb_util:bin(First)]).

%% @doc Confirm only the next contiguous window. Later windows continue posting
%% while mining is pending. Gateway failures leave the confirmed cursor intact.
confirm_window(State = #{confirmed := C, cursor := P}) when C >= P -> State;
confirm_window(State = #{check_at := At}) ->
    case erlang:system_time(millisecond) >= At of
        false -> State;
        true -> check_window(State)
    end.

check_window(State = #{id := ProcID, opts := Opts, confirmed := C}) ->
    Next = State#{check_at => erlang:system_time(millisecond) +
        hb_opts:get(scheduler_upload_poll_ms, 30000, Opts)},
    try
        Path = window_path(ProcID, C + 1),
        {ok, Saved} = hb_store:read(store(Opts),
            #{<<"read">> => Path, <<"exact">> => true}, Opts),
        Record = binary_to_term(Saved, [safe]),
        {Last, Bytes} = saved_window(Record),
        IDs = posted_ids(Record),
        case mined(IDs, Opts) of
            true ->
                ok = hb_store:write(store(Opts),
                    #{progress_path(ProcID, <<"confirmed">>) => hb_util:bin(Last)}, Opts),
                ok = hb_store:flush(store(Opts), Opts),
                answer(Next#{confirmed => Last, check_at => 0});
            false ->
                retry_pending(ProcID, C + 1, Bytes, Opts),
                Next
        end
    catch Class:Reason ->
        ?event(warning, {schedule_confirmation_retrying, ProcID, Class, Reason}),
        Next
    end.

%% The items a persisted window must have mined: the archive item itself in
%% format 2; every item of the outer bundle in format 1.
posted_ids({?FORMAT, _Last, Bytes}) ->
    [hb_util:human_id((ar_bundles:deserialize_item_wrapper(Bytes))#tx.id)];
posted_ids({_Last, Bytes}) ->
    Bundle = ar_bundles:deserialize_item_wrapper(Bytes),
    lists:usort([hb_util:human_id(I#tx.id) || I <- items(Bundle#tx.data)]).

%% @doc Repost the identical persisted bundle after the pending timeout.
retry_pending(ProcID, First, Bytes, Opts) ->
    Path = posted_at_path(ProcID, First),
    PostedAt = case hb_store:read(store(Opts),
            #{<<"read">> => Path, <<"exact">> => true}, Opts) of
        {ok, Time} -> hb_util:int(Time);
        {error, not_found} -> 0
    end,
    case erlang:system_time(second) - PostedAt >=
            hb_opts:get(scheduler_upload_retry_seconds, 1800, Opts) of
        true ->
            case post(Bytes, Opts) of
                ok -> mark_posted(ProcID, First, Opts);
                Error -> ?event(warning, {schedule_repost_failed, ProcID, First, Error})
            end;
        false -> ok
    end.

%% @doc Require mined inclusion of every top-level item, including the archive
%% and separately published payloads. Inclusion is confirmation one.
mined(IDs, Opts) ->
    Gateway = hb_util:list(hb_opts:get(scheduler_upload_gateway,
        <<"https://arweave.net">>, Opts)),
    {ok, {{_, 200, _}, _, HeightBody}} = hb_http_gate:httpc_request(get,
        {Gateway ++ "/height", []}, [{timeout, 15000}], [{body_format, binary}],
        Opts),
    Height = binary_to_integer(string:trim(HeightBody)),
    Depth = hb_opts:get(scheduler_upload_confirmations, 3, Opts),
    true = is_integer(Depth) andalso Depth >= 1,
    mined_batches(IDs, Height, Depth, Gateway, Opts).

mined_batches([], _Height, _Depth, _Gateway, _Opts) -> true;
mined_batches(IDs, Height, Depth, Gateway, Opts) ->
    {Batch, Rest} = lists:split(min(100, length(IDs)), IDs),
    Query = <<"query($ids:[ID!]){transactions(ids:$ids,first:100){edges{node{id block{height}}}}}">>,
    Body = hb_json:encode(#{<<"query">> => Query,
        <<"variables">> => #{<<"ids">> => Batch}}),
    {ok, {{_, 200, _}, _, Response}} = hb_http_gate:httpc_request(post,
        {Gateway ++ "/graphql", [], "application/json", Body},
        [{timeout, 15000}], [{body_format, binary}], Opts),
    #{<<"data">> := #{<<"transactions">> := #{<<"edges">> := Edges}}} =
        hb_json:decode(Response),
    window_confirmed(Batch, Edges, Height, Depth)
        andalso mined_batches(Rest, Height, Depth, Gateway, Opts).

window_confirmed(IDs, Edges, Height, Depth) ->
    Found = [ID || #{<<"node">> := #{<<"id">> := ID,
        <<"block">> := #{<<"height">> := H}}} <- Edges,
        is_integer(H), H >= 0, Height - H + 1 >= Depth],
    lists:all(fun(ID) -> lists:member(ID, Found) end, IDs).

%%% Tests

confirmation_coverage_test() ->
    Edge = fun(ID, Block) ->
        #{<<"node">> => #{<<"id">> => ID, <<"block">> => Block}}
    end,
    A = Edge(<<"archive">>, #{<<"height">> => 98}),
    B = Edge(<<"body">>, #{<<"height">> => 98}),
    IDs = [<<"archive">>, <<"body">>],
    ?assert(window_confirmed(IDs, [A, B], 100, 3)),
    ?assertNot(window_confirmed(IDs, [A, B], 99, 3)),
    ?assertNot(window_confirmed(IDs, [A], 100, 3)),
    ?assertNot(window_confirmed(IDs, [A, A], 100, 3)),
    ?assertNot(window_confirmed(IDs, [A, Edge(<<"body">>, null)], 100, 3)),
    ?assertNot(window_confirmed(IDs,
        [A, Edge(<<"body">>, #{<<"height">> => 101})], 100, 3)).

%% @doc With no bundler the cursor stays where it is, and a bundler that
%% refuses (429) is asked again. Once one takes it, the window goes up as one
%% signed format 2 item: every slot's message then its assignment, exact
%% signed bytes, compressed, tagged with the last assignment's ID; the cursor
%% reaches the last slot and the waiting client is answered once that one
%% item is mined.
retries_until_bundler_answers_test_() ->
    {timeout, 120, fun retries_until_bundler_answers/0}.
retries_until_bundler_answers() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listen),
    ok = gen_tcp:close(Listen),
    Self = self(),
    {Gateway, GatewayPID} = stub_gateway(Self),
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Opts = #{
        <<"store">> => [hb_test_utils:test_store()],
        <<"priv-wallet">> => hb:wallet(),
        <<"scheduler-upload-gateway">> => Gateway,
        <<"scheduler-upload-poll-ms">> => 10,
        <<"scheduler-upload-retry-seconds">> => 0,
        <<"scheduler-upload-processes">> => [ProcID],
        <<"scheduler-upload-latency-ms">> => 0,
        <<"bundler-ans104">> =>
            <<"http://localhost:", (integer_to_binary(Port))/binary>>
    },
    Slots = lists:seq(0, 4),
    IDs =
        lists:flatmap(
            fun(Slot) ->
                Body = hb_message:commit(
                    #{ <<"data">> => integer_to_binary(Slot) }, Opts, <<"ans104@1.0">>),
                Assignment = hb_message:commit(
                    #{
                        <<"process">> => ProcID,
                        <<"slot">> => Slot,
                        <<"body">> => Body,
                        <<"type">> => <<"Assignment">>,
                        <<"variant">> => <<"ao.N.1">>
                    },
                    Opts,
                    <<"ans104@1.0">>
                ),
                ok = dev_scheduler_cache:write(ProcID, Assignment, Opts),
                [{hb_message:id(Body, signed, Opts),
                    hb_message:id(Assignment, signed, Opts)}]
            end,
            Slots
        ),
    ok = dev_scheduler_cache:flush(Opts),
    Uploader = start(ProcID, Opts),
    [ Uploader ! {durable, S, none} || S <- Slots ],
    Uploader ! {durable, 4, {Self, msg, assignment}},
    timer:sleep(1500),
    ?assertEqual(-1, cursor(ProcID, Opts)),
    BundlerPID = stub_bundler(Port, [429, 200], Self),
    Posted = receive {posted, 200, Bin} -> Bin after 90000 -> error(no_post) end,
    receive {posted, 429, Retried} -> ?assertEqual(Posted, Retried)
    after 0 -> error(no_refusal) end,
    Archive = ar_bundles:deserialize_item_wrapper(Posted),
    ?assert(ar_bundles:verify_item(Archive)),
    {MessageIDs, AssignmentIDs} = lists:unzip(IDs),
    Tag = fun(Name) -> proplists:get_value(Name, Archive#tx.tags) end,
    ?assertEqual(<<"ao-schedule-archive">>, Tag(<<"Data-Protocol">>)),
    ?assertEqual(<<"2">>, Tag(<<"Archive-Format">>)),
    ?assertEqual(<<"full">>, Tag(<<"Archive-Data">>)),
    ?assertEqual(<<"0">>, Tag(<<"From-Slot">>)),
    ?assertEqual(<<"4">>, Tag(<<"To-Slot">>)),
    ?assertEqual(hb_util:human_id(lists:last(AssignmentIDs)), Tag(<<"Last-Assignment">>)),
    {ok, Inflated} = archive_bundle(Archive#tx.tags, Archive#tx.data),
    ?assert(byte_size(Archive#tx.data) < byte_size(Inflated)),
    Archived = items(Inflated),
    ?assertEqual(MessageIDs ++ AssignmentIDs,
        [ hb_util:human_id(I#tx.id) || I <- Archived ]),
    ?assert(lists:all(fun ar_bundles:verify_item/1, Archived)),
    gateway_reply(<<"100">>),
    PendingEdges = [#{<<"node">> => #{<<"id">> => hb_util:human_id(Archive#tx.id),
        <<"block">> => null}}],
    gateway_reply(hb_json:encode(#{<<"data">> => #{<<"transactions">> =>
        #{<<"edges">> => PendingEdges}}})),
    receive {posted, 200, Reposted} -> ?assertEqual(Posted, Reposted)
    after 10000 -> error(no_pending_repost) end,
    ?assertEqual(-1, cursor(ProcID, Opts)),
    ?assertEqual(4, posted(ProcID, Opts)),
    receive {scheduled, msg, assignment} -> error(premature_confirmation)
    after 0 -> ok end,
    unlink(Uploader),
    exit(Uploader, kill),
    Restarted = start(ProcID, Opts),
    Restarted ! {durable, 4, {Self, msg, assignment}},
    gateway_reply(<<"100">>),
    Edges = [#{<<"node">> => #{<<"id">> => hb_util:human_id(Archive#tx.id),
        <<"block">> => #{<<"height">> => 98}}}],
    gateway_reply(hb_json:encode(#{<<"data">> => #{<<"transactions">> =>
        #{<<"edges">> => Edges}}})),
    receive {scheduled, msg, assignment} -> ok after 10000 -> error(no_answer) end,
    ?assertEqual(4, cursor(ProcID, Opts)),
    unlink(Restarted),
    exit(Restarted, kill),
    unlink(GatewayPID),
    exit(GatewayPID, kill),
    unlink(BundlerPID),
    exit(BundlerPID, kill).

%% @doc Controlled mining evidence without waiting for paid L1 transactions.
stub_gateway(Test) ->
    {ok, Listen} = gen_tcp:listen(0,
        [binary, {active, false}, {reuseaddr, true}, {packet, http_bin}]),
    {ok, Port} = inet:port(Listen),
    Pid = spawn_link(fun() -> gateway_accept(Listen, Test) end),
    {<<"http://localhost:", (integer_to_binary(Port))/binary>>, Pid}.

gateway_accept(Listen, Test) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    gateway_headers(Socket, 0),
    Test ! {gateway_request, self()},
    receive {gateway_response, Body} ->
        gen_tcp:send(Socket, [<<"HTTP/1.1 200 OK\r\ncontent-length: ">>,
            integer_to_binary(byte_size(Body)),
            <<"\r\nconnection: close\r\n\r\n">>, Body])
    after 10000 -> error(no_gateway_response)
    end,
    gen_tcp:close(Socket),
    gateway_accept(Listen, Test).

gateway_headers(Socket, Length) ->
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, {http_header, _, 'Content-Length', _, Value}} ->
            gateway_headers(Socket, binary_to_integer(Value));
        {ok, http_eoh} ->
            ok = inet:setopts(Socket, [{packet, raw}]),
            case Length of
                0 -> ok;
                _ -> {ok, _} = gen_tcp:recv(Socket, Length, 5000), ok
            end;
        {ok, _} -> gateway_headers(Socket, Length)
    end.

gateway_reply(Body) ->
    receive {gateway_request, Pid} -> Pid ! {gateway_response, Body}
    after 10000 -> error(no_gateway_request)
    end.

%% @doc The items of an ANS-104 bundle, in order.
items(<<Count:256/little, Rest/binary>>) ->
    <<Index:(Count * 64)/binary, Data/binary>> = Rest,
    {Items, <<>>} =
        lists:foldl(
            fun(Size, {Acc, Bin}) ->
                <<Item:Size/binary, Next/binary>> = Bin,
                {[ar_bundles:deserialize_item_wrapper(Item) | Acc], Next}
            end,
            {[], Data},
            [ Size || <<Size:256/little, _:32/binary>> <= Index ]
        ),
    lists:reverse(Items).

%% @doc A bundler that answers each request with the next status in
%% `Statuses' (the last one from then on) and sends what it was posted to
%% `Test'.
stub_bundler(Port, Statuses, Test) ->
    {ok, Listen} = gen_tcp:listen(Port,
        [binary, {active, false}, {reuseaddr, true}, {packet, http_bin}]),
    spawn_link(fun() -> stub_accept(Listen, Statuses, Test) end).
stub_accept(Listen, Statuses, Test) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    ok = gen_tcp:controlling_process(Socket, self()),
    stub_accept(Listen, stub_serve(Socket, Statuses, Test, 0), Test).
stub_serve(Socket, Statuses, Test, Length) ->
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, {http_header, _, 'Content-Length', _, Value}} ->
            stub_serve(Socket, Statuses, Test, binary_to_integer(Value));
        {ok, http_eoh} ->
            ok = inet:setopts(Socket, [{packet, raw}]),
            {ok, Body} = gen_tcp:recv(Socket, Length, 5000),
            [Status | Rest] = Statuses,
            Test ! {posted, Status, Body},
            gen_tcp:send(Socket, [<<"HTTP/1.1 ">>, integer_to_binary(Status),
                <<" X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n">>]),
            gen_tcp:close(Socket),
            case Rest of [] -> Statuses; _ -> Rest end;
        {ok, _} -> stub_serve(Socket, Statuses, Test, Length);
        {error, _} -> Statuses
    end.

%% @doc A 402, or Turbo's free-tier header on any status, is a bundler that
%% wants paying: the uploader waits the unfunded backoff and says so in
%% `status/0' until a post is taken. A `retry-after' is honoured otherwise.
unfunded_backs_off_test() ->
    Refusal = fun(Status, Headers) ->
        {error, {Status, {url, <<"u">>}, {bytes, 1}, {headers, Headers}, {body, <<>>}}}
    end,
    ?assert(unfunded(Refusal(402, []))),
    ?assert(unfunded(Refusal(403, [{<<"X-Free-Tier-Exhausted">>, <<"true">>}]))),
    ?assertNot(unfunded(Refusal(503, [{<<"retry-after">>, <<"7">>}]))),
    Opts = #{ <<"scheduler-upload-unfunded-backoff-ms">> => 900000 },
    persistent_term:erase(?STATUS),
    ?assertEqual(900000, refused_wait(<<"p">>, Refusal(402, []), 1000, Opts)),
    ?assertMatch(#{ funded := false }, status()),
    ?assertEqual(7000, refused_wait(<<"p">>, Refusal(503, [{<<"Retry-After">>, <<"7">>}]), 1000, Opts)),
    count_posted(full, 1 bsl 30, Opts),
    funded(true),
    #{ funded := true, posted := #{ full := #{ bytes := B, usd_per_day := Usd } } } = status(),
    ?assertEqual(1 bsl 30, B),
    % A GiB taken within the first hour counts as a day's worth 24 times.
    ?assert(Usd > 47.29 * 23).

%% @doc What the uploader reads back is the message that was signed: the same
%% ID, so the item on Arweave is the one the client sent.
read_back_keeps_id_test() ->
    Opts = #{ <<"store">> => [hb_test_utils:test_store()], <<"priv-wallet">> => hb:wallet() },
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Body = hb_message:commit(
        #{ <<"data">> => <<"x">>, <<"action">> => <<"Probe">> }, Opts, <<"ans104@1.0">>),
    Assignment = hb_message:commit(
        #{ <<"process">> => ProcID, <<"slot">> => 0, <<"body">> => Body,
           <<"variant">> => <<"ao.N.1">> },
        Opts, <<"ans104@1.0">>),
    ok = dev_scheduler_cache:write(ProcID, Assignment, Opts),
    {ok, Read} = dev_scheduler_cache:read(ProcID, 0, Opts),
    ?assertEqual(hb_message:id(Body, signed, Opts),
        hb_message:id(hb_maps:get(<<"body">>, Read, #{}, Opts), signed, Opts)),
    ?assertEqual(hb_message:id(Assignment, signed, Opts),
        hb_message:id(Read, signed, Opts)).

%% @doc An ANS-104 message is its own item, ID unchanged. An `httpsig@1.0'
%% message rides in a node-signed item, tagged with its ID, and decodes back to
%% a message whose signature still verifies.
item_test() ->
    Opts = #{ <<"priv-wallet">> => hb:wallet() },
    Ans = hb_message:commit(#{ <<"data">> => <<"a">> }, Opts, <<"ans104@1.0">>),
    AnsItem = item(Ans, Opts),
    ?assert(ar_bundles:verify_item(AnsItem)),
    ?assertEqual(hb_message:id(Ans, signed, Opts),
        hb_util:human_id(AnsItem#tx.id)),
    Sig = hb_message:commit(#{ <<"action">> => <<"Push">>, <<"n">> => 1 },
        Opts#{ <<"priv-wallet">> => ar_wallet:new() }, <<"httpsig@1.0">>),
    Wrapped = item(Sig, Opts),
    ?assert(ar_bundles:verify_item(Wrapped)),
    ?assertEqual(hb_message:id(Sig, signed, Opts),
        proplists:get_value(<<"Message-Id">>, Wrapped#tx.tags)),
    Back = hb_message:convert(Wrapped#tx.data, <<"structured@1.0">>,
        <<"json@1.0">>, Opts),
    ?assert(hb_message:verify(Back, all, Opts)),
    ?assertEqual(hb_message:id(Sig, signed, Opts),
        hb_message:id(Back, signed, Opts)).

%% @doc An item naming a message by link brings that message along, under its
%% own ID, and whatever it links to; a link this node cannot resolve is
%% passed over.
linked_test() ->
    Opts = #{ <<"store">> => [hb_test_utils:test_store()], <<"priv-wallet">> => hb:wallet() },
    Module = hb_message:commit(
        #{ <<"data">> => <<"return 1">>, <<"content-type">> => <<"text/x-lua">> },
        Opts, <<"ans104@1.0">>),
    {ok, _} = hb_cache:write(Module, Opts),
    ModID = hb_message:id(Module, signed, Opts),
    Missing = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Spawn = sign(<<>>, [{<<"module">>, <<"ao:", ModID/binary>>},
        {<<"other">>, <<"ao:", Missing/binary>>}], Opts),
    ?assertEqual([ModID, Missing], links(Spawn)),
    [Item] = linked([Spawn], Opts),
    ?assertEqual(ModID, hb_util:human_id(Item#tx.id)),
    ?assertEqual([], links(Item)).

%% @doc A message the window already carries in a node-signed wrapper is not
%% brought along a second time for the link its assignment makes to it.
linked_wrapped_test() ->
    Opts = #{ <<"store">> => [hb_test_utils:test_store()], <<"priv-wallet">> => hb:wallet() },
    Proc = hb_message:commit(
        #{ <<"type">> => <<"Process">>, <<"data">> => <<"module source">> },
        Opts#{ <<"priv-wallet">> => ar_wallet:new() }, <<"httpsig@1.0">>),
    {ok, _} = hb_cache:write(Proc, Opts),
    ProcID = hb_message:id(Proc, signed, Opts),
    Wrapped = item(Proc, Opts),
    Assignment = sign(<<>>, [{<<"body">>, <<"ao:", ProcID/binary>>}], Opts),
    ?assertEqual([], linked([Wrapped, Assignment], Opts)).

%% @doc A refusal names the bundler's reason: its status, why-headers and body.
refusal_is_logged_whole_test_() ->
    {timeout, 30, fun refusal_is_logged_whole/0}.
refusal_is_logged_whole() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listen),
    ok = gen_tcp:close(Listen),
    Base = <<"http://localhost:", (integer_to_binary(Port))/binary>>,
    Stub = stub_bundler(Port, [402], self()),
    Opts = #{ <<"scheduler-upload-url">> => <<Base/binary, "/v1/tx">> },
    {error, {402, {url, URL}, {bytes, 3}, {headers, Headers}, {body, <<>>}}} =
        post(<<"abc">>, Opts),
    ?assertEqual(<<Base/binary, "/v1/tx">>, URL),
    ?assertEqual([], Headers),
    ?assert(logged_header("x-free-tier-exhausted")),
    ?assert(logged_header("Retry-After")),
    ?assertNot(logged_header("date")),
    receive {posted, 402, <<"abc">>} -> ok after 5000 -> error(no_post) end,
    unlink(Stub),
    exit(Stub, kill).

%% @doc The upload list arrives as a list, a numbered message, a JSON array or
%% a comma-separated binary; absent or empty, it lists nothing.
list_option_test() ->
    A = hb_util:human_id(crypto:strong_rand_bytes(32)),
    B = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Opts = #{},
    ?assertEqual([A, B], list_option([A, B], Opts)),
    ?assertEqual([A, B], list_option(#{ <<"1">> => A, <<"2">> => B }, Opts)),
    ?assertEqual([A, B], list_option(hb_json:encode([A, B]), Opts)),
    ?assertEqual([A, B], list_option(<<A/binary, ", ", B/binary>>, Opts)),
    ?assertEqual([A], list_option(binary_to_list(A), Opts)),
    ?assertEqual([], list_option([], Opts)),
    ?assertEqual([], list_option(<<>>, Opts)),
    ?assertEqual([], list_option(not_found, Opts)),
    ?assert(allowed(A, #{ <<"scheduler-upload-processes">> => <<A/binary, ",", B/binary>> })),
    ?assertNot(allowed(A, #{ <<"scheduler-upload-processes">> => [B] })),
    ?assertNot(allowed(A, #{})).

%% @doc A process off the upload list, or any process on a node with no list,
%% gets no uploader: the bundler is never asked, and no window is written.
unlisted_is_never_uploaded_test_() ->
    {timeout, 30, fun unlisted_is_never_uploaded/0}.
unlisted_is_never_uploaded() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listen),
    ok = gen_tcp:close(Listen),
    Bundler = stub_bundler(Port, [200], self()),
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Listed = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Base = #{
        <<"store">> => [hb_test_utils:test_store()],
        <<"priv-wallet">> => hb:wallet(),
        <<"bundler-ans104">> =>
            <<"http://localhost:", (integer_to_binary(Port))/binary>>
    },
    Body = hb_message:commit(#{ <<"data">> => <<"0">> }, Base, <<"ans104@1.0">>),
    Assignment = hb_message:commit(
        #{ <<"process">> => ProcID, <<"slot">> => 0, <<"body">> => Body,
           <<"type">> => <<"Assignment">>, <<"variant">> => <<"ao.N.1">> },
        Base, <<"ans104@1.0">>),
    ok = dev_scheduler_cache:write(ProcID, Assignment, Base),
    ok = dev_scheduler_cache:flush(Base),
    lists:foreach(
        fun(Opts) ->
            ?assertEqual(undefined, start(ProcID, Opts)),
            ?assertNot(allowed(ProcID, Opts))
        end,
        [Base, Base#{ <<"scheduler-upload-processes">> => [Listed] },
            Base#{ <<"scheduler-upload-processes">> => <<>> }]
    ),
    receive {posted, _, _} -> error(unlisted_process_uploaded)
    after 1500 -> ok
    end,
    ?assertEqual(-1, posted(ProcID, Base)),
    ?assertEqual({error, not_found}, hb_store:read(store(Base),
        #{ <<"read">> => window_path(ProcID, 0), <<"exact">> => true }, Base)),
    unlink(Bundler),
    exit(Bundler, kill).

%% Write `Slots' of `ProcID' as ANS-104 bodies and assignments; their
%% `{BodyID, AssignmentID}'s.
write_slots(ProcID, Slots, Opts) ->
    IDs =
        [ begin
            Body = hb_message:commit(
                #{ <<"data">> => integer_to_binary(Slot) }, Opts, <<"ans104@1.0">>),
            Assignment = hb_message:commit(
                #{ <<"process">> => ProcID, <<"slot">> => Slot, <<"body">> => Body,
                   <<"type">> => <<"Assignment">>, <<"variant">> => <<"ao.N.1">> },
                Opts, <<"ans104@1.0">>),
            ok = dev_scheduler_cache:write(ProcID, Assignment, Opts),
            {hb_message:id(Body, signed, Opts), hb_message:id(Assignment, signed, Opts)}
          end
        || Slot <- Slots ],
    ok = dev_scheduler_cache:flush(Opts),
    IDs.

test_opts() ->
    #{ <<"store">> => [hb_test_utils:test_store()], <<"priv-wallet">> => hb:wallet() }.

%% @doc A full window's data is the same bytes every time it is prepared (the
%% signature, and so the item's ID, is salted), and smaller than what it
%% carries; a fingerprint window carries only the last assignment's ID.
window_shapes_test() ->
    Opts = test_opts(),
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    IDs = write_slots(ProcID, lists:seq(0, 9), Opts),
    {_, LastID} = lists:last(IDs),
    {2, 9, A} = prepare_window(ProcID, 0, 9, full, ?DEFAULT_WINDOW_BYTES, Opts),
    {2, 9, B} = prepare_window(ProcID, 0, 9, full, ?DEFAULT_WINDOW_BYTES, Opts),
    ItemA = ar_bundles:deserialize_item_wrapper(A),
    ItemB = ar_bundles:deserialize_item_wrapper(B),
    ?assert(ar_bundles:verify_item(ItemA)),
    ?assertEqual(ItemA#tx.data, ItemB#tx.data),
    ?assertNotEqual(ItemA#tx.id, ItemB#tx.id),
    {ok, Inflated} = archive_bundle(ItemA#tx.tags, ItemA#tx.data),
    ?assert(byte_size(ItemA#tx.data) < byte_size(Inflated)),
    ?assertEqual(hb_util:human_id(LastID),
        proplists:get_value(<<"Last-Assignment">>, ItemA#tx.tags)),
    {2, 9, F} = prepare_window(ProcID, 3, 9, fingerprint, ?DEFAULT_WINDOW_BYTES, Opts),
    Fp = ar_bundles:deserialize_item_wrapper(F),
    ?assert(ar_bundles:verify_item(Fp)),
    ?assertEqual(hb_util:native_id(LastID), Fp#tx.data),
    ?assertEqual(<<"fingerprint">>, proplists:get_value(<<"Archive-Data">>, Fp#tx.tags)),
    ?assertEqual(<<"3">>, proplists:get_value(<<"From-Slot">>, Fp#tx.tags)),
    ?assertEqual(<<"9">>, proplists:get_value(<<"To-Slot">>, Fp#tx.tags)),
    ?assertEqual({error, fingerprint_only}, archive_bundle(Fp#tx.tags, Fp#tx.data)),
    ?assert(byte_size(F) < 1300).

%% @doc A window is cut before the slot that takes it past the byte cap, and
%% always carries at least one slot.
byte_cap_cuts_window_test() ->
    Opts = test_opts(),
    ProcID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    IDs = write_slots(ProcID, lists:seq(0, 5), Opts),
    {2, 0, One} = prepare_window(ProcID, 0, 5, full, 1, Opts),
    Item = ar_bundles:deserialize_item_wrapper(One),
    {ok, Inflated} = archive_bundle(Item#tx.tags, Item#tx.data),
    [{Body0, Assignment0} | _] = IDs,
    ?assertEqual([Body0, Assignment0],
        [ hb_util:human_id(I#tx.id) || I <- items(Inflated) ]),
    {2, Two, _} = prepare_window(ProcID, 0, 5, full, 2 * byte_size(Inflated), Opts),
    ?assert(Two >= 1 andalso Two < 5).

%% @doc A window persisted by the format 1 build (an outer bundle, before
%% compression) still posts byte for byte and confirms on its own items.
legacy_window_record_test() ->
    Opts = test_opts(),
    Plain = sign(<<"m">>, [], Opts),
    Archive = sign(bundle_data([sign(<<"a">>, [], Opts)]),
        [{<<"Data-Protocol">>, <<"ao-schedule-archive">>}], Opts),
    Outer = ar_bundles:serialize(sign(bundle_data([Plain, Archive]),
        [{<<"Bundle-Format">>, <<"binary">>}, {<<"Bundle-Version">>, <<"2.0.0">>}], Opts)),
    ?assertEqual({7, Outer}, saved_window(binary_to_term(term_to_binary({7, Outer})))),
    ?assertEqual(lists:usort([hb_util:human_id(Plain#tx.id), hb_util:human_id(Archive#tx.id)]),
        posted_ids({7, Outer})),
    New = ar_bundles:serialize(Archive),
    ?assertEqual({3, New}, saved_window({2, 3, New})),
    ?assertEqual([hb_util:human_id(Archive#tx.id)], posted_ids({2, 3, New})),
    ?assertEqual({ok, <<"raw">>}, archive_bundle([], <<"raw">>)).

%% @doc A window goes up when `window-slots' slots wait or the oldest has
%% waited `latency'; the uploader sleeps until whichever comes first.
batching_test() ->
    S = #{ cursor => 10, target => 10, confirmed => 10, pending_since => undefined,
        window_slots => 100, latency => 5000 },
    ?assertNot(due(pending(S, 0), 0)),
    ?assertEqual(infinity, wake_after(pending(S, 0), 0)),
    Waiting = pending(S#{ target => 15 }, 10),
    ?assertEqual(10, maps:get(pending_since, Waiting)),
    ?assertNot(due(Waiting, 5009)),
    ?assert(due(Waiting, 5010)),
    ?assertEqual(3000, wake_after(Waiting, 2010)),
    ?assertEqual(1000, wake_after(Waiting#{ confirmed => 5 }, 2010)),
    % A later slot does not restart the wait; a full window goes at once.
    ?assertEqual(10, maps:get(pending_since, pending(Waiting#{ target => 16 }, 900))),
    ?assert(due(Waiting#{ target => 110 }, 10)),
    ?assertEqual(undefined, maps:get(pending_since, pending(Waiting#{ cursor => 15 }, 900))).

%% @doc The full list wins; the fingerprint list archives only fingerprints.
mode_test() ->
    A = hb_util:human_id(crypto:strong_rand_bytes(32)),
    B = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Opts = #{ <<"scheduler-upload-processes">> => [A],
        <<"scheduler-fingerprint-processes">> => [A, B] },
    ?assertEqual(full, mode(A, Opts)),
    ?assertEqual(fingerprint, mode(B, Opts)),
    ?assertNot(allowed(B, Opts)),
    ?assertEqual(none, mode(hb_util:human_id(crypto:strong_rand_bytes(32)), Opts)).

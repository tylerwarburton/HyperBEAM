%%% @doc The cold tier: a node's history on a shared filesystem (an NFS share
%%% mounted on every node), so that its local disk holds only hot data.
%%%
%%% A process is its spawn and its ordered signed messages; everything else
%%% is cache. Locally a node keeps its head states, its newest checkpoints and
%%% its recent messages. On the share, in its own folder, it keeps:
%%%
%%%   slots/<pid>/<from>-<to>.hbc    every assignment and its message, as the
%%%                                  rows the scheduler wrote (packed:
%%%                                  `hb_cache'), a range per file
%%%   checkpoints/<pid>/<slot>-<ms>.hbc
%%%                                  every snapshot checkpoint, so an old
%%%                                  read replays at most the slots between
%%%                                  two snapshots; past `checkpoint-days'
%%%                                  (30) only the oldest of each UTC day
%%%   replica/<pusd>/<slot>.hbc      the newest snapshot of each bridge
%%%                                  replica (`lib_bridge_replica'), two kept
%%%
%%% Each file is `HBC1', the SHA-256 of the rest, then a compressed term. A
%%% file is written to `.tmp', synced, renamed and read back before anything
%%% counts it as shipped.
%%%
%%% The share is a residential link and goes away. Nothing on a request path
%%% writes to it: a server ships on a timer, and every filesystem call is
%%% made in a process of its own and abandoned after `io-timeout-ms', so a
%%% hung mount costs a timeout, never a blocked node. A failed tick marks the
%%% share unavailable (`status/0', the `hb_cold_available' gauge) and the next
%%% tries again, backing off to `?MAX_BACKOFF_MS'. Local data is deleted only
%%% after its copy was confirmed, and only once it is older than
%%% `keep-local-seconds': an outage costs local disk, never data, and the
%%% shipper catches up from its markers when the share is back.
%%%
%%% Reads fall back: a slot not held locally is read from this node's folder,
%%% then from the other nodes' folders (`peers'), then from Arweave (the
%%% schedule's archive windows, `dev_scheduler_uploader'). Anything read from
%%% another node or from Arweave is verified first: every signature, the
%%% process and slot it names, and that the process's scheduler signed it. A
%%% checkpoint or replica snapshot is taken only from this node's folder or a
%%% peer marked `trusted' (never the public node), and a replica snapshot
%%% only if the same build wrote it.
%%%
%%% Configuration, `cold-tier' (off when unset):
%%%   path                the node's own folder (read and written)
%%%   peers               [#{ node, path, trusted }]: other nodes' folders,
%%%                       read only
%%%   keep-local-seconds  messages and assignments kept locally (604,800)
%%%   checkpoint-days     every checkpoint kept on the share this long (30)
%%%   interval-ms         time between ticks (60,000)
%%%   io-timeout-ms       one filesystem call, at most (10,000)
%%%   range-slots         slots per file, at most (1,000)
%%%   arweave-gateway     for the last fallback (`https://arweave.net'),
%%%                       `false' to never ask
-module(hb_store_cold).
-export([maybe_start/1, status/0, tick/1, config/1]).
-export([read_assignment/3, restore_checkpoint/3, ship_replica/3, restore_replica/4]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SERVER, hb_store_cold_server).
-define(STATUS, {?MODULE, status}).
-define(ASSIGNMENTS, <<"~scheduler@1.0/assignments">>).
-define(MARK, <<"~cold@1.0">>).
-define(LAG, 16).
-define(MAX_BACKOFF_MS, 600000).
-define(DAY_MS, 86400000).

%%% Configuration

%% @doc The cold tier's settings, or `undefined' when it is off.
config(Opts) ->
    case hb_opts:get(<<"cold-tier">>, undefined, Opts) of
        C = #{ <<"path">> := Path } ->
            Int = fun(K, D) -> hb_util:int(maps:get(K, C, D)) end,
            #{
                path => hb_util:list(Path),
                peers =>
                    [ #{ node => hb_util:bin(maps:get(<<"node">>, P, <<"peer">>)),
                         path => hb_util:list(maps:get(<<"path">>, P)),
                         trusted => hb_util:atom(maps:get(<<"trusted">>, P, false)) =:= true }
                    || P <- peers(maps:get(<<"peers">>, C, [])) ],
                keep_local_ms => Int(<<"keep-local-seconds">>, 604800) * 1000,
                checkpoint_days_ms => Int(<<"checkpoint-days">>, 30) * ?DAY_MS,
                interval => Int(<<"interval-ms">>, 60000),
                io_timeout => Int(<<"io-timeout-ms">>, 10000),
                range => max(1, Int(<<"range-slots">>, 1000)),
                gateway =>
                    case maps:get(<<"arweave-gateway">>, C, <<"https://arweave.net">>) of
                        false -> false;
                        <<"false">> -> false;
                        G -> hb_util:list(G)
                    end
            };
        _ -> undefined
    end.

peers(L) when is_list(L) -> [ P || P <- L, is_map(P), is_map_key(<<"path">>, P) ];
peers(M) when is_map(M) -> peers(hb_util:message_to_ordered_list(M));
peers(_) -> [].

%%% The server

%% @doc Start the shipper when `cold-tier' is set.
maybe_start(Opts) ->
    case config(Opts) of
        undefined -> ok;
        _ ->
            hb_name:singleton(?SERVER, fun() -> server(Opts, 0) end),
            ok
    end.

%% A failed tick backs off, doubling up to `?MAX_BACKOFF_MS'.
server(Opts, Backoff) ->
    Interval = maps:get(interval, config(Opts)),
    Next =
        case catch tick(Opts) of
            ok -> 0;
            _ -> min(?MAX_BACKOFF_MS, max(Interval, Backoff * 2))
        end,
    receive
        {tick_now, From, Ref} -> From ! {Ref, ok}, server(Opts, Next)
    after max(Interval, Next) -> server(Opts, Next)
    end.

%% @doc The share's state: whether the last tick reached it, since when not,
%% what this node still has to ship, and what it shipped.
status() -> persistent_term:get(?STATUS, #{ available => true }).

status_update(Fun) -> persistent_term:put(?STATUS, Fun(status())).

%% @doc One tick: ship new slots, delete local slots that are shipped and
%% old, ship daily checkpoints. `ok', or the first failure (the share is then
%% marked unavailable; nothing local was touched past what was confirmed).
tick(Opts) ->
    Cfg = config(Opts),
    Result =
        try
            ok = ship_slots(Cfg, Opts),
            ok = prune_slots(Cfg, Opts),
            ok = ship_checkpoints(Cfg, Opts)
        catch
            throw:{cold_io, IOWhy} -> {error, IOWhy};
            C:E -> {error, {C, E}}
        end,
    case Result of
        ok ->
            case maps:get(available, status(), true) of
                false -> ?event(warning, {cold_tier_available_again});
                true -> ok
            end,
            set_gauge(hb_cold_available, 1),
            status_update(fun(S) ->
                maps:without([unavailable_since, last_error],
                    S#{ available => true, last_tick => erlang:system_time(millisecond) })
            end);
        {error, Why} ->
            case maps:get(available, status(), true) of
                true -> ?event(error, {cold_tier_unavailable, Why});
                false -> ok
            end,
            set_gauge(hb_cold_available, 0),
            status_update(fun(S) ->
                S#{ available => false, last_error => Why,
                    unavailable_since =>
                        maps:get(unavailable_since, S, erlang:system_time(millisecond)) }
            end)
    end,
    Result.

set_gauge(Name, Value) ->
    try
        hb_prometheus:declare(gauge, [{name, Name}, {help, "Cold tier"}]),
        prometheus_gauge:set(Name, Value)
    catch _:_ -> ok
    end.

%%% Filesystem calls, bounded

%% Run `Fun' in a process of its own: its result, or `{cold_io, timeout}' is
%% thrown after `io-timeout-ms'. A call on a hung mount is left behind.
io(Cfg, Fun) ->
    Parent = self(),
    Ref = make_ref(),
    {Pid, Mon} = spawn_monitor(fun() -> Parent ! {Ref, catch Fun()} end),
    receive
        {Ref, {'EXIT', Why}} -> erlang:demonitor(Mon, [flush]), throw({cold_io, Why});
        {Ref, Res} -> erlang:demonitor(Mon, [flush]), Res;
        {'DOWN', Mon, process, Pid, Why} -> throw({cold_io, Why})
    after maps:get(io_timeout, Cfg) ->
        erlang:demonitor(Mon, [flush]),
        exit(Pid, kill),
        throw({cold_io, timeout})
    end.

%% Write a term as a cold file, durably, and read it back.
put_file(Cfg, Path, Term) ->
    Body = term_to_binary(Term, [{compressed, 6}]),
    Bin = <<"HBC1", (crypto:hash(sha256, Body))/binary, Body/binary>>,
    io(Cfg, fun() ->
        Tmp = Path ++ ".tmp",
        ok = filelib:ensure_dir(Path),
        ok = file:write_file(Tmp, Bin, [raw, binary, sync]),
        ok = file:rename(Tmp, Path),
        {ok, Bin} = file:read_file(Path),
        ok
    end),
    byte_size(Bin).

%% A cold file's term, or `error' if it is absent or damaged.
get_file(Cfg, Path) ->
    case io(Cfg, fun() -> file:read_file(Path) end) of
        {ok, <<"HBC1", Hash:32/binary, Body/binary>>} ->
            case crypto:hash(sha256, Body) of
                Hash -> {ok, binary_to_term(Body, [safe])};
                _ -> ?event(warning, {cold_file_damaged, Path}), error
            end;
        _ -> error
    end.

list_dir(Cfg, Dir) ->
    case io(Cfg, fun() -> file:list_dir(Dir) end) of
        {ok, Names} -> [ N || N <- Names, lists:suffix(".hbc", N) ];
        _ -> []
    end.

pad(N) -> lists:flatten(io_lib:format("~12..0B", [N])).

%%% Slots

irr(Opts) ->
    case hb_store_irreplaceable:store(Opts) of
        [S | _] -> S;
        _ -> hd(hb_store:normalize_stores(
                case hb_opts:get(scheduler_store, hb_opts:get(store, [], Opts), Opts) of
                    M when is_map(M) -> [M]; L -> L end))
    end.

inner(#{ <<"store-module">> := hb_store_offsite_journal, <<"inner">> := I }) -> I;
inner(S) -> S.

mark_key(Kind, P) -> <<?MARK/binary, "/", Kind/binary, "/", P/binary>>.

read_int(Store, Key, Default) ->
    case hb_store:read([Store], #{ <<"read">> => Key, <<"exact">> => true }, #{}) of
        {ok, V} -> binary_to_integer(V);
        _ -> Default
    end.

list_names(Store, Path) ->
    case hb_store:list([Store], Path, #{}) of
        {ok, L} -> L;
        _ -> []
    end.

slots_of(Store, P) ->
    lists:sort([ I || N <- list_names(Store, <<?ASSIGNMENTS/binary, "/", P/binary>>),
                      I <- [catch binary_to_integer(N)], is_integer(I) ]).

slot_path(P, S) -> <<?ASSIGNMENTS/binary, "/", P/binary, "/", (integer_to_binary(S))/binary>>.

%% Every raw row a slot is read from: its assignment path, the rows that
%% path links to, the packed row and the rows of every ID it answers for.
closure(Store, Path) ->
    {Rows, _} = closure(Store, Path, {[], #{}}),
    lists:reverse(Rows).
closure(_Store, Path, Acc = {_, Seen}) when is_map_key(Path, Seen) -> Acc;
closure(Store, Path, {Rows, Seen0}) ->
    Seen = Seen0#{ Path => true },
    case hb_store:read([Store], #{ <<"read">> => Path, <<"exact">> => true }, #{}) of
        {ok, Value} ->
            Next =
                case Value of
                    <<"ao:", ID/binary>> when ?IS_ID(ID) -> [ID];
                    _ -> hb_cache:packed_links(Value)
                end,
            lists:foldl(fun(N, A) -> closure(Store, N, A) end, {[{Path, Value} | Rows], Seen}, Next);
        _ -> {Rows, Seen}
    end.

%% Ship every slot older than the newest `?LAG' of each process, a range per
%% file. The `shipped' mark moves only once the file is read back.
ship_slots(Cfg, Opts) ->
    Store = irr(Opts),
    Procs = list_names(Store, ?ASSIGNMENTS),
    Lag = lists:sum(
        [ ship_process(Cfg, Store, P, Opts) || P <- Procs, ?IS_ID(P) ]),
    set_gauge(hb_cold_lag_slots, Lag),
    status_update(fun(S) -> S#{ lag_slots => Lag } end),
    ok.

%% The slots of `P' still to ship after this tick.
ship_process(Cfg, Store, P, Opts) ->
    Shipped = read_int(Store, mark_key(<<"shipped">>, P), -1),
    Slots = [ S || S <- slots_of(Store, P), S > Shipped ],
    case length(Slots) > ?LAG of
        false -> length(Slots);
        true ->
            Ready = lists:sublist(Slots, min(maps:get(range, Cfg), length(Slots) - ?LAG)),
            From = hd(Ready), To = lists:last(Ready),
            Rows = lists:append([ closure(Store, slot_path(P, S)) || S <- Ready ]),
            Ms = assignment_ms(P, To, Opts),
            File = filename:join([maps:get(path, Cfg), "slots", hb_util:list(P),
                pad(From) ++ "-" ++ pad(To) ++ ".hbc"]),
            Bytes = put_file(Cfg, File,
                #{ process => P, from => From, to => To, newest_ms => Ms, rows => Rows }),
            ok = hb_store:write([Store], #{
                mark_key(<<"shipped">>, P) => integer_to_binary(To),
                <<(mark_key(<<"range">>, P))/binary, "/", (list_to_binary(pad(To)))/binary>> =>
                    term_to_binary({From, To, Ms}) }, #{}),
            ok = hb_store:flush([Store], #{}),
            status_update(fun(S) -> S#{ shipped_bytes => maps:get(shipped_bytes, S, 0) + Bytes } end),
            length(Slots) - length(Ready)
    end.

assignment_ms(P, S, Opts) ->
    case hb_store_retention:assignment_time(P, S, Opts#{ <<"scheduler-store">> => [irr(Opts)] }) of
        {ok, Ms} -> Ms;
        _ -> erlang:system_time(millisecond)
    end.

%% Delete, locally, the slots of every shipped range whose newest assignment
%% is older than `keep-local-seconds'. Each process's newest slot stays, and
%% so does every `message-slots' row (the scheduler's dedupe index).
prune_slots(Cfg, Opts) ->
    Store = irr(Opts),
    #{ <<"db">> := DB } = hb_store:find(inner(Store)),
    Old = erlang:system_time(millisecond) - maps:get(keep_local_ms, Cfg),
    lists:foreach(
        fun(P) ->
            Ranges = list_names(Store, mark_key(<<"range">>, P)),
            Newest = lists:max([-1 | slots_of(Store, P)]),
            lists:foreach(
                fun(Name) ->
                    Key = <<(mark_key(<<"range">>, P))/binary, "/", Name/binary>>,
                    {ok, V} = hb_store:read([Store], #{ <<"read">> => Key, <<"exact">> => true }, #{}),
                    {From, To, Ms} = binary_to_term(V, [safe]),
                    case Ms < Old andalso To < Newest of
                        true ->
                            Keys = lists:usort([ K || S <- lists:seq(From, To),
                                                      {K, _} <- closure(Store, slot_path(P, S)) ]),
                            {ok, _} = elmdb:delete_batch(DB, Keys ++ [Key]),
                            status_update(fun(St) ->
                                St#{ pruned_slots => maps:get(pruned_slots, St, 0) + To - From + 1 } end);
                        false -> ok
                    end
                end,
                lists:sort(Ranges))
        end,
        [ P || P <- list_names(Store, ?ASSIGNMENTS), ?IS_ID(P) ]),
    ok.

%% @doc An assignment of `P' that this node does not hold: from its own
%% folder, then the peers' (verified), then Arweave (verified). `{ok, Msg}'
%% or `not_found'. Never throws: the share being gone is a miss.
read_assignment(P0, Slot, Opts) ->
    P = hb_util:human_id(P0),
    case config(Opts) of
        undefined -> not_found;
        Cfg ->
            Sources =
                [ {own, maps:get(path, Cfg)} ] ++
                [ {peer, maps:get(path, Peer)} || Peer <- maps:get(peers, Cfg) ],
            from_sources(Sources, Cfg, P, Slot, Opts)
    end.

from_sources([], Cfg, P, Slot, Opts) ->
    try from_arweave(Cfg, P, Slot, Opts)
    catch _:Why ->
        ?event(warning, {cold_arweave_read_failed, P, Slot, Why}),
        not_found
    end;
from_sources([{Kind, Dir} | Rest], Cfg, P, Slot, Opts) ->
    Res =
        try from_folder(Cfg, Dir, P, Slot, Opts) of
            {ok, A} when Kind =:= own -> {ok, A};
            {ok, A} ->
                case verified(A, P, Slot, Opts) of
                    true -> {ok, A};
                    false -> ?event(warning, {cold_peer_slot_refused, Dir, P, Slot}), not_found
                end;
            not_found -> not_found
        catch _:_ -> not_found
        end,
    case Res of
        {ok, _} -> Res;
        not_found -> from_sources(Rest, Cfg, P, Slot, Opts)
    end.

from_folder(Cfg, Dir, P, Slot, Opts) ->
    Files = list_dir(Cfg, filename:join([Dir, "slots", hb_util:list(P)])),
    Covering =
        [ F || F <- Files,
               [A, B] <- [string:split(filename:rootname(F), "-")],
               list_to_integer(A) =< Slot, Slot =< list_to_integer(B) ],
    case Covering of
        [] -> not_found;
        [F | _] ->
            case get_file(Cfg, filename:join([Dir, "slots", hb_util:list(P), F])) of
                {ok, #{ rows := Rows }} -> read_rows(Rows, slot_path(P, Slot), Opts);
                error -> not_found
            end
    end.

%% Read a message from raw rows, through a store of its own.
read_rows(Rows, Path, Opts) ->
    Store = #{ <<"store-module">> => hb_store_volatile,
               <<"name">> => <<"cold-", (hb_util:human_id(crypto:strong_rand_bytes(32)))/binary>> },
    ok = hb_store:start(Store),
    try
        ok = hb_store:write([Store], maps:from_list(Rows), #{}),
        ROpts = Opts#{ <<"store">> => [Store] },
        case hb_cache:read(Path, ROpts) of
            {ok, Msg} -> {ok, hb_cache:ensure_all_loaded(Msg, ROpts)};
            _ -> not_found
        end
    after
        catch hb_store:reset(Store),
        catch hb_store:stop(Store)
    end.

%% A slot from elsewhere: every signature verifies, it names `P' and `Slot',
%% one of `P''s schedulers signed it, and its message verifies. A process this
%% node has no definition of cannot be checked, so nothing of it is taken.
verified(A, P, Slot, Opts) ->
    Body = hb_maps:get(<<"body">>, A, #{}, Opts),
    Checks = [
        {process, fun() -> hb_util:human_id(hb_ao:get(<<"process">>, A, Opts)) =:= P end},
        {slot, fun() -> hb_util:int(hb_ao:get(<<"slot">>, A, Opts)) =:= Slot end},
        {signature, fun() -> hb_message:verify(A, all, Opts) =:= true end},
        {message, fun() -> (not is_map(Body)) orelse hb_message:verify(Body, all, Opts) =:= true end},
        {scheduler, fun() ->
            Schedulers = schedulers(P, Opts),
            lists:any(fun(S) -> lists:member(S, Schedulers) end,
                [ Addr || S <- hb_message:signers(A, Opts),
                          {ok, Addr} <- [hb_committer:address(S)] ])
        end}
    ],
    case [ Name || {Name, Check} <- Checks, (catch Check()) =/= true ] of
        [] -> true;
        Failed -> ?event(warning, {cold_slot_not_verified, P, Slot, Failed}), false
    end.

schedulers(P, Opts) ->
    case hb_cache:read(P, Opts) of
        {ok, Proc} ->
            lists:usort(lists:append(
                [ [ hb_util:human_id(string:trim(X))
                  || X <- binary:split(hb_util:bin(V), <<",">>, [global, trim_all]) ]
                || K <- [<<"scheduler">>, <<"scheduler-location">>],
                   V <- [hb_maps:get(K, Proc, <<>>, Opts)], is_binary(V) ]));
        _ -> []
    end.

%% The slot from the process's archive windows on Arweave: the window that
%% covers it, its assignment and message items, verified.
from_arweave(#{ gateway := false }, _P, _Slot, _Opts) -> not_found;
from_arweave(#{ gateway := Gateway }, P, Slot, Opts) ->
    Query = <<"query($p:[String!]!){transactions(tags:[",
        "{name:\"Data-Protocol\",values:[\"ao-schedule-archive\"]},",
        "{name:\"Process\",values:$p},{name:\"Archive-Format\",values:[\"2\"]}],",
        "first:100,sort:HEIGHT_DESC){edges{node{id tags{name value}}}}}">>,
    Res = gateway(post, Gateway, "/graphql",
        hb_json:encode(#{ <<"query">> => Query, <<"variables">> => #{ <<"p">> => [P] } })),
    #{ <<"data">> := #{ <<"transactions">> := #{ <<"edges">> := Edges } } } = hb_json:decode(Res),
    Windows =
        [ ID || #{ <<"node">> := #{ <<"id">> := ID, <<"tags">> := Tags } } <- Edges,
                tag(<<"Archive-Data">>, Tags) =/= <<"fingerprint">>,
                From <- [catch binary_to_integer(tag(<<"From-Slot">>, Tags))],
                To <- [catch binary_to_integer(tag(<<"To-Slot">>, Tags))],
                is_integer(From), is_integer(To), From =< Slot, Slot =< To ],
    first_window(Windows, Gateway, P, Slot, Opts).

first_window([], _Gateway, _P, _Slot, _Opts) -> not_found;
first_window([ID | Rest], Gateway, P, Slot, Opts) ->
    Data = zlib:uncompress(gateway(get, Gateway, "/raw/" ++ hb_util:list(ID), none)),
    Msgs = [ hb_message:convert(Item, <<"structured@1.0">>, <<"ans104@1.0">>, Opts)
           || Item <- bundle_items(Data) ],
    ByID = maps:from_list([ {hb_message:id(M, signed, Opts), M} || M <- Msgs ]),
    % The assignment names its message by ID: it is verified as signed, then
    % given the message whose signed ID that is, which must verify too.
    Found =
        [ A#{ <<"body">> => Body }
        || A <- Msgs,
           (catch hb_util:int(hb_maps:get(<<"slot">>, A, -1, Opts))) =:= Slot,
           (catch hb_util:human_id(hb_maps:get(<<"process">>, A, <<>>, Opts))) =:= P,
           B <- [hb_maps:get(<<"body">>, A, none, Opts)], is_binary(B),
           Body <- [maps:get(hb_util:human_id(B), ByID, none)], is_map(Body),
           verified(A, P, Slot, Opts),
           hb_message:verify(Body, all, Opts) =:= true ],
    case Found of
        [A | _] -> {ok, A};
        [] -> first_window(Rest, Gateway, P, Slot, Opts)
    end.

%% The items of a list bundle (`ar_bundles:serialize_bundle(list, ...)').
bundle_items(<<Count:256/little, Rest/binary>>) ->
    IndexSize = Count * 64,
    <<Index:IndexSize/binary, Items/binary>> = Rest,
    items([ Size || <<Size:256/little, _:32/binary>> <= Index ], Items).
items([], _) -> [];
items([Size | Sizes], Bin) ->
    <<Item:Size/binary, More/binary>> = Bin,
    [ar_bundles:deserialize(Item) | items(Sizes, More)].

tag(Name, Tags) ->
    case [ V || #{ <<"name">> := N, <<"value">> := V } <- Tags, N =:= Name ] of
        [V | _] -> V;
        [] -> undefined
    end.

gateway(Method, Gateway, Path, Body) ->
    Request =
        case Method of
            get -> {Gateway ++ Path, []};
            post -> {Gateway ++ Path, [], "application/json", Body}
        end,
    {ok, {{_, 200, _}, _, Res}} =
        hb_http_gate:httpc_request(Method, Request, [{timeout, 30000}],
            [{body_format, binary}], #{}),
    Res.

%%% Checkpoints

%% Ship, per process, the oldest snapshot checkpoint of each UTC day that is
%% not shipped yet: the daily and weekly ones retention keeps.
ship_checkpoints(Cfg, Opts) ->
    case catch hb_store_retention:store(Opts) of
        Main = #{} -> ship_checkpoints(Cfg, Main, Opts);
        _ -> ok
    end.
ship_checkpoints(Cfg, Main, _Opts) ->
    lists:foreach(
        fun(P) ->
            Mark = mark_key(<<"checkpoint">>, P),
            Shipped = read_int(Main, Mark, -1),
            New = [ S || S <- pstate_slots(Main, P, <<"s">>), S > Shipped ],
            [ ship_checkpoint(Cfg, Main, P, S) || S <- New ],
            thin_checkpoints(Cfg, P)
        end,
        [ P || P <- list_names(Main, <<"pstate">>), ?IS_ID(P) ]),
    ok.

%% Every snapshot checkpoint is shipped, so an old read replays at most the
%% slots between two snapshots (`dev_process', `process-snapshot-slots' and
%% `-replay-ms'); the `checkpoint' mark is the newest shipped.
ship_checkpoint(Cfg, Main, P, S) ->
    Rows = snapshot_rows(Main, P, S),
    Ms = case snap_ms(Main, P, S) of {ok, T} -> T; error -> erlang:system_time(millisecond) end,
    File = filename:join([maps:get(path, Cfg), "checkpoints", hb_util:list(P),
        pad(S) ++ "-" ++ integer_to_list(Ms) ++ ".hbc"]),
    Bytes = put_file(Cfg, File, #{ process => P, slot => S, rows => Rows }),
    ok = hb_store:write([Main], #{ mark_key(<<"checkpoint">>, P) => integer_to_binary(S) }, #{}),
    status_update(fun(St) -> St#{ shipped_bytes => maps:get(shipped_bytes, St, 0) + Bytes } end).

%% On the share too, checkpoints older than `checkpoint-days' are thinned to
%% the oldest of each UTC day, so the folder grows by one a day per process.
thin_checkpoints(Cfg, P) ->
    Dir = filename:join([maps:get(path, Cfg), "checkpoints", hb_util:list(P)]),
    Old = erlang:system_time(millisecond) - maps:get(checkpoint_days_ms, Cfg),
    Dated = [ {Ms, S, F} || F <- list_dir(Cfg, Dir), {S, Ms} <- [checkpoint_name(F)], Ms < Old ],
    Keep = maps:values(lists:foldl(
        fun({Ms, S, F}, M) ->
            maps:update_with(Ms div ?DAY_MS, fun({S0, F0}) -> min({S0, F0}, {S, F}) end, {S, F}, M)
        end, #{}, Dated)),
    [ io(Cfg, fun() -> file:delete(filename:join(Dir, F)) end)
    || {_, S, F} <- Dated, not lists:member({S, F}, Keep) ],
    ok.

%% A checkpoint file's slot and time: `<slot>-<ms>.hbc'.
checkpoint_name(F) ->
    case string:split(filename:rootname(F), "-") of
        [S, Ms] ->
            case {catch list_to_integer(S), catch list_to_integer(Ms)} of
                {SI, MI} when is_integer(SI), is_integer(MI) -> {SI, MI};
                _ -> error
            end;
        _ -> error
    end.

snap_ms(Store, P, S) ->
    case hb_store:read([Store], #{ <<"read">> => blob_key(P, <<"s">>, S), <<"exact">> => true }, #{}) of
        {ok, Marker} ->
            case hb_store_retention:marker_time(Marker) of
                {ok, Ms} -> {ok, Ms};
                _ -> error
            end;
        _ -> error
    end.

%% A snapshot checkpoint's rows: its `s' marker, its `f' state and chunks.
snapshot_rows(Store, P, S) ->
    Keys = [ blob_key(P, K, S) || K <- [<<"s">>, <<"f">>] ],
    Rows = [ {K, V} || K <- Keys,
                       {ok, V} <- [hb_store:read([Store], #{ <<"read">> => K, <<"exact">> => true }, #{})] ],
    Chunks =
        [ {CK, CV}
        || {K, <<"pstate-chunks:", N/binary>>} <- Rows,
           I <- lists:seq(1, binary_to_integer(N)),
           CK <- [<<K/binary, ".", (integer_to_binary(I))/binary>>],
           {ok, CV} <- [hb_store:read([Store], #{ <<"read">> => CK, <<"exact">> => true }, #{})] ],
    Chunks ++ Rows.

blob_key(P, Kind, S) ->
    <<"pstate/", P/binary, "/", Kind/binary, "/", (list_to_binary(pad(S)))/binary>>.

pstate_slots(Store, P, Kind) ->
    lists:sort([ I || N <- list_names(Store, <<"pstate/", P/binary, "/", Kind/binary>>),
                      I <- [catch binary_to_integer(N)], is_integer(I) ]).

%% @doc Put back the newest shipped checkpoint of `P' at or below `Target'
%% that is newer than any held locally, from this node's folder or a trusted
%% peer's: `{ok, Slot}' or `not_found'. A process's state is a function of
%% its messages, so a checkpoint is only as good as whoever computed it.
restore_checkpoint(P0, Target, Opts) ->
    P = hb_util:human_id(P0),
    case config(Opts) of
        undefined -> not_found;
        Cfg ->
            try
                Main = hb_store_retention:store(Opts),
                Local = [ S || S <- pstate_slots(Main, P, <<"s">>), S =< Target ],
                Floor = lists:max([-1 | Local]),
                Dirs = [ maps:get(path, Cfg) | [ maps:get(path, Peer) || Peer <- maps:get(peers, Cfg),
                                                                         maps:get(trusted, Peer) ] ],
                Candidates =
                    lists:reverse(lists:sort(
                        [ {S, filename:join([Dir, "checkpoints", hb_util:list(P), F])}
                        || Dir <- Dirs,
                           F <- list_dir(Cfg, filename:join([Dir, "checkpoints", hb_util:list(P)])),
                           {S, _} <- [checkpoint_name(F)],
                           S > Floor, S =< Target ])),
                restore_first(Cfg, Main, Candidates)
            catch _:_ -> not_found
            end
    end.

restore_first(_Cfg, _Main, []) -> not_found;
restore_first(Cfg, Main, [{S, File} | Rest]) ->
    case get_file(Cfg, File) of
        {ok, #{ slot := S, rows := Rows }} ->
            ok = hb_store:write([Main], maps:from_list(Rows), #{}),
            ok = hb_store:flush([Main], #{}),
            {ok, S};
        _ -> restore_first(Cfg, Main, Rest)
    end.

%%% Replica snapshots

%% @doc Ship the newest snapshot of a replica's computed store (`Computed'),
%% tagged with this build, and keep only the newest two in this node's
%% folder. Off the caller; a share that is gone costs nothing.
ship_replica(Pusd, Computed, Opts) ->
    case config(Opts) of
        undefined -> ok;
        Cfg ->
            _ = spawn(fun() ->
                try
                    case lists:reverse(pstate_slots(Computed, Pusd, <<"s">>)) of
                        [S | _] ->
                            Dir = filename:join([maps:get(path, Cfg), "replica", hb_util:list(Pusd)]),
                            File = filename:join(Dir, pad(S) ++ ".hbc"),
                            case io(Cfg, fun() -> filelib:is_file(File) end) of
                                true -> ok;
                                false ->
                                    _ = put_file(Cfg, File, #{ process => Pusd, slot => S,
                                        build => build(), rows => snapshot_rows(Computed, Pusd, S) }),
                                    Old = lists:nthtail(min(2, length(list_dir(Cfg, Dir))),
                                        lists:reverse(lists:sort(list_dir(Cfg, Dir)))),
                                    [ io(Cfg, fun() -> file:delete(filename:join(Dir, F)) end) || F <- Old ]
                            end;
                        [] -> ok
                    end
                catch _:Why -> ?event(warning, {cold_replica_ship_failed, Pusd, Why})
                end
            end),
            ok
    end.

%% @doc Seed an empty replica computed store from the newest replica snapshot
%% this build wrote, in this node's folder or a trusted peer's: `{ok, Slot}'
%% (the replica then computes on from it instead of from slot 0), or
%% `not_found'.
restore_replica(Pusd, Computed, Opts, _VerifiedSlot) ->
    case config(Opts) of
        undefined -> not_found;
        Cfg ->
            try
                Dirs = [ maps:get(path, Cfg) | [ maps:get(path, Peer) || Peer <- maps:get(peers, Cfg),
                                                                         maps:get(trusted, Peer) ] ],
                Candidates =
                    lists:reverse(lists:sort(
                        [ {S, filename:join([Dir, "replica", hb_util:list(Pusd), F])}
                        || Dir <- Dirs,
                           F <- list_dir(Cfg, filename:join([Dir, "replica", hb_util:list(Pusd)])),
                           S <- [catch list_to_integer(filename:rootname(F))], is_integer(S) ])),
                Build = build(),
                first_replica(Cfg, Computed, Build, Candidates)
            catch _:_ -> not_found
            end
    end.

first_replica(_Cfg, _Computed, _Build, []) -> not_found;
first_replica(Cfg, Computed, Build, [{S, File} | Rest]) ->
    case get_file(Cfg, File) of
        {ok, #{ slot := S, build := Build, rows := Rows }} ->
            ok = hb_store:write([Computed], maps:from_list(Rows), #{}),
            ok = hb_store:flush([Computed], #{}),
            {ok, S};
        _ -> first_replica(Cfg, Computed, Build, Rest)
    end.

build() ->
    try file:consult(filename:join(code:priv_dir(hb), "hb_buildinfo")) of
        {ok, [#{ <<"source">> := Source }]} -> hb_util:bin(Source);
        _ -> <<"unknown">>
    catch _:_ -> <<"unknown">>
    end.

%%% Tests

-define(TEST_DIR, "/tmp/hb-cold-test").

test_env(Name) ->
    Dir = ?TEST_DIR ++ "/" ++ Name ++ "-" ++ integer_to_list(erlang:unique_integer([positive])),
    os:cmd("rm -rf " ++ Dir),
    Main = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => list_to_binary(Dir ++ "/main") },
    Irr = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => list_to_binary(Dir ++ "/irr") },
    ok = hb_store:start([Main, Irr]),
    Sched = ar_wallet:new(),
    Opts = #{ <<"store">> => [Main, Irr#{ <<"access">> => [<<"read">>] }],
              <<"scheduler-store">> => [Irr], <<"priv-wallet">> => Sched,
              <<"match-index">> => false,
              <<"cold-tier">> => #{ <<"path">> => list_to_binary(Dir ++ "/share/me"),
                                    <<"keep-local-seconds">> => 1,
                                    <<"io-timeout-ms">> => 2000,
                                    <<"arweave-gateway">> => false } },
    Address = hb_util:human_id(ar_wallet:to_address(Sched)),
    Proc = hb_message:commit(#{ <<"type">> => <<"Process">>, <<"scheduler">> => Address,
        <<"test-seed">> => integer_to_binary(erlang:unique_integer()) }, Opts, <<"ans104@1.0">>),
    {ok, _} = hb_cache:write(Proc, Opts#{ <<"store">> => [Main] }),
    P = hb_message:id(Proc, signed, Opts),
    #{ dir => Dir, main => Main, irr => Irr, opts => Opts, p => P, sched => Sched }.

%% Schedule `Slots' of `P' as the scheduler writes them, `AgeMs' old.
assign(#{ irr := Irr, opts := Opts, p := P }, Slots, AgeMs) ->
    Client = #{ <<"priv-wallet">> => ar_wallet:new() },
    lists:foreach(
        fun(N) ->
            Body = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"target">> => P,
                <<"n">> => integer_to_binary(N) }, Client, <<"ans104@1.0">>),
            A = hb_message:commit(#{ <<"type">> => <<"Assignment">>, <<"process">> => P,
                <<"slot">> => N, <<"timestamp">> => erlang:system_time(millisecond) - AgeMs,
                <<"body">> => Body }, Opts#{ <<"cache-pack">> => true }, <<"ans104@1.0">>),
            WOpts = Opts#{ <<"store">> => [Irr], <<"cache-pack">> => true },
            {ok, _} = hb_cache:write(A, WOpts),
            ok = hb_cache:link(hb_message:id(A, signed, Opts), slot_path(P, N), WOpts)
        end,
        Slots),
    ok = hb_store:flush([Irr], #{}).

local(#{ irr := Irr, p := P }, N) ->
    case hb_store:read([Irr], #{ <<"read">> => slot_path(P, N), <<"exact">> => true }, #{}) of
        {ok, _} -> true;
        _ -> false
    end.

share(Opts) -> binary_to_list(maps:get(<<"path">>, maps:get(<<"cold-tier">>, Opts))).

%% @doc Old slots are shipped, deleted locally once confirmed, and read back
%% from the share; the newest stay local.
slots_shipped_pruned_and_read_back_test_() ->
    {timeout, 120, fun() ->
        Env = #{ opts := Opts, p := P } = test_env("ship"),
        assign(Env, lists:seq(0, 39), 3600000),
        ok = tick(Opts),
        % The newest `?LAG' are not shipped; the rest are, and are gone here.
        ?assertNot(local(Env, 0)),
        ?assertNot(local(Env, 23)),
        ?assert(local(Env, 24)),
        ?assert(local(Env, 39)),
        {ok, A} = read_assignment(P, 5, Opts),
        ?assertEqual(5, hb_util:int(hb_ao:get(<<"slot">>, A, Opts))),
        ?assert(hb_message:verify(A, all, Opts)),
        ?assertEqual(<<"5">>, hb_ao:get(<<"body/n">>, A, Opts)),
        ?assertMatch(#{ available := true, lag_slots := 16 }, status()),
        % Young slots are shipped but kept.
        assign(Env, lists:seq(40, 79), 0),
        ok = tick(Opts),
        ?assert(local(Env, 50)),
        ?assertMatch({ok, _}, read_assignment(P, 10, Opts))
    end}.

%% @doc The share goes away mid-run: the tick fails, the node keeps every
%% slot locally, and once the share is back the shipper catches up.
share_gone_and_back_test_() ->
    {timeout, 120, fun() ->
        Env = #{ opts := Opts, p := P } = test_env("gone"),
        assign(Env, lists:seq(0, 19), 3600000),
        ok = tick(Opts),
        ?assertNot(local(Env, 3)),
        % The share is gone: its folder is a file now.
        Share = share(Opts),
        os:cmd("mv " ++ Share ++ " " ++ Share ++ ".away && touch " ++ Share),
        assign(Env, lists:seq(20, 59), 3600000),
        ?assertMatch({error, _}, tick(Opts)),
        ?assertMatch(#{ available := false }, status()),
        ?assert(lists:all(fun(N) -> local(Env, N) end, lists:seq(20, 59))),
        % A read of a shipped slot misses, and does not fail.
        ?assertEqual(not_found, read_assignment(P, 3, Opts)),
        % Back: everything shipped catches up, and the reads return.
        os:cmd("rm -f " ++ Share ++ " && mv " ++ Share ++ ".away " ++ Share),
        ok = tick(Opts),
        ?assertMatch(#{ available := true }, status()),
        ?assertNot(local(Env, 30)),
        {ok, A} = read_assignment(P, 30, Opts),
        ?assertEqual(30, hb_util:int(hb_ao:get(<<"slot">>, A, Opts)))
    end}.

%% @doc A filesystem call that hangs (a dead NFS mount) costs the timeout,
%% never the caller.
hung_call_times_out_test() ->
    Cfg = #{ io_timeout => 200 },
    T0 = erlang:monotonic_time(millisecond),
    ?assertThrow({cold_io, timeout}, io(Cfg, fun() -> timer:sleep(5000) end)),
    ?assert(erlang:monotonic_time(millisecond) - T0 < 1000).

%% @doc A slot read from another node's folder is used only if it verifies
%% and the process's scheduler signed it.
peer_slots_verified_test_() ->
    {timeout, 120, fun() ->
        Peer = #{ opts := POpts, p := P } = test_env("peer"),
        assign(Peer, lists:seq(0, 19), 3600000),
        ok = tick(POpts),
        PeerShare = list_to_binary(share(POpts)),
        % A node that has the process's definition, and reads the peer.
        Me = #{ opts := MOpts0 } = test_env("reader"),
        {ok, Proc} = hb_cache:read(P, POpts),
        {ok, _} = hb_cache:write(hb_cache:ensure_all_loaded(Proc, POpts),
            MOpts0#{ <<"store">> => [maps:get(main, Me)] }),
        Cold = maps:get(<<"cold-tier">>, MOpts0),
        MOpts = MOpts0#{ <<"cold-tier">> => Cold#{ <<"peers">> =>
            [#{ <<"node">> => <<"peer">>, <<"path">> => PeerShare }] } },
        % (Of 20 slots the newest 16 stay unshipped: slots 0-3 are there.)
        {ok, A} = read_assignment(P, 2, MOpts),
        ?assertEqual(2, hb_util:int(hb_ao:get(<<"slot">>, A, MOpts))),
        % Signed by a key that is not the process's scheduler: refused.
        Forged = (test_env("forger"))#{ p => P },
        assign(Forged, lists:seq(0, 19), 3600000),
        #{ opts := FOpts } = Forged,
        ok = tick(FOpts),
        MOpts2 = MOpts0#{ <<"cold-tier">> => Cold#{ <<"peers">> =>
            [#{ <<"node">> => <<"forger">>, <<"path">> => list_to_binary(share(FOpts)) }] } },
        ?assertEqual(not_found, read_assignment(P, 2, MOpts2))
    end}.

%% @doc Daily checkpoints are shipped; one is put back where none is held,
%% from this node's folder or a trusted peer's only.
checkpoints_test_() ->
    {timeout, 120, fun() ->
        #{ main := Main, opts := Opts } = test_env("ckpt"),
        Q = hb_util:human_id(crypto:strong_rand_bytes(32)),
        Day = ?DAY_MS, Now = erlang:system_time(millisecond),
        Marker = fun(Ms) -> term_to_binary({1, #{ <<"timestamp">> => Ms }}) end,
        ok = hb_store:write([Main], #{
            blob_key(Q, <<"s">>, 10) => Marker(Now - 2 * Day),
            blob_key(Q, <<"f">>, 10) => <<"state-10">>,
            blob_key(Q, <<"s">>, 20) => Marker(Now - 2 * Day + 1000),
            blob_key(Q, <<"f">>, 20) => <<"state-20">>,
            blob_key(Q, <<"s">>, 30) => Marker(Now),
            blob_key(Q, <<"f">>, 30) => <<"pstate-chunks:2">>,
            <<(blob_key(Q, <<"f">>, 30))/binary, ".1">> => <<"a">>,
            <<(blob_key(Q, <<"f">>, 30))/binary, ".2">> => <<"b">> }, #{}),
        ok = hb_store:flush([Main], #{}),
        ok = tick(Opts),
        Share = share(Opts),
        Shipped = filelib:wildcard(Share ++ "/checkpoints/" ++ binary_to_list(Q) ++ "/*.hbc"),
        % Every snapshot is shipped.
        ?assertEqual(3, length(Shipped)),
        #{ <<"db">> := DB } = hb_store:find(Main),
        {ok, _} = elmdb:delete_batch(DB, [ blob_key(Q, K, S) || K <- [<<"s">>, <<"f">>], S <- [10, 20, 30] ]),
        ?assertEqual({ok, 20}, restore_checkpoint(Q, 25, Opts)),
        ?assertEqual({ok, <<"state-20">>}, hb_store:read([Main], blob_key(Q, <<"f">>, 20), #{})),
        % Older than `checkpoint-days', a day keeps only its oldest (10).
        Cold0 = maps:get(<<"cold-tier">>, Opts),
        ok = tick(Opts#{ <<"cold-tier">> => Cold0#{ <<"checkpoint-days">> => 1 } }),
        ?assertEqual([10, 30], lists:sort([ S || F <- filelib:wildcard(Share ++ "/checkpoints/"
            ++ binary_to_list(Q) ++ "/*.hbc"), {S, _} <- [checkpoint_name(filename:basename(F))] ])),
        % From a peer only when trusted.
        #{ opts := ROpts0 } = test_env("ckpt-reader"),
        Cold = maps:get(<<"cold-tier">>, ROpts0),
        Peer = fun(T) -> ROpts0#{ <<"cold-tier">> => Cold#{ <<"peers">> =>
            [#{ <<"node">> => <<"p">>, <<"path">> => list_to_binary(Share), <<"trusted">> => T }] } } end,
        ?assertEqual(not_found, restore_checkpoint(Q, 100, Peer(false))),
        ?assertEqual({ok, 30}, restore_checkpoint(Q, 100, Peer(true)))
    end}.

%% @doc A replica snapshot is shipped and seeds an empty computed store of
%% the same build; one of another build is never used.
replica_snapshot_test_() ->
    {timeout, 60, fun() ->
        #{ main := Computed, opts := Opts, dir := Dir } = test_env("replica"),
        Pusd = hb_util:human_id(crypto:strong_rand_bytes(32)),
        ok = hb_store:write([Computed], #{
            blob_key(Pusd, <<"s">>, 500) => term_to_binary({1, #{}}),
            blob_key(Pusd, <<"f">>, 500) => <<"head">> }, #{}),
        ok = ship_replica(Pusd, Computed, Opts),
        File = share(Opts) ++ "/replica/" ++ binary_to_list(Pusd) ++ "/" ++ pad(500) ++ ".hbc",
        ?assert(lists:any(fun(_) -> timer:sleep(100), filelib:is_file(File) end, lists:seq(1, 50))),
        Fresh = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => list_to_binary(Dir ++ "/fresh") },
        ok = hb_store:start(Fresh),
        ?assertEqual({ok, 500}, restore_replica(Pusd, Fresh, Opts, -1)),
        ?assertEqual({ok, <<"head">>}, hb_store:read([Fresh], blob_key(Pusd, <<"f">>, 500), #{})),
        % Another build's snapshot is not used.
        Cfg = config(Opts),
        {ok, T} = get_file(Cfg, File),
        _ = put_file(Cfg, File, T#{ build => <<"other">> }),
        Fresh2 = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => list_to_binary(Dir ++ "/fresh2") },
        ok = hb_store:start(Fresh2),
        ?assertEqual(not_found, restore_replica(Pusd, Fresh2, Opts, -1))
    end}.

%% @doc The last fallback: a slot in an archive window on Arweave, verified.
arweave_fallback_test_() ->
    {timeout, 60, fun() ->
        #{ opts := Opts0, p := P, sched := Sched } = test_env("arweave"),
        Client = #{ <<"priv-wallet">> => ar_wallet:new() },
        Body = hb_message:commit(#{ <<"type">> => <<"Message">>, <<"target">> => P,
            <<"n">> => <<"7">> }, Client, <<"ans104@1.0">>),
        A = hb_message:commit(#{ <<"type">> => <<"Assignment">>, <<"process">> => P,
            <<"slot">> => 7, <<"timestamp">> => 1, <<"body">> => hb_message:id(Body, signed, Opts0) },
            Opts0#{ <<"priv-wallet">> => Sched }, <<"ans104@1.0">>),
        Items = [ hb_message:convert(M, <<"ans104@1.0">>, <<"structured@1.0">>, Opts0) || M <- [A, Body] ],
        {undefined, Data} = ar_bundles:serialize_bundle(list, Items, false),
        WindowID = hb_util:human_id(crypto:strong_rand_bytes(32)),
        Tag = fun(N, V) -> #{ <<"name">> => N, <<"value">> => V } end,
        Edges = hb_json:encode(#{ <<"data">> => #{ <<"transactions">> => #{ <<"edges">> =>
            [#{ <<"node">> => #{ <<"id">> => WindowID, <<"tags">> =>
                [Tag(<<"From-Slot">>, <<"0">>), Tag(<<"To-Slot">>, <<"9">>),
                 Tag(<<"Archive-Data">>, <<"full">>)] } }] } } }),
        Raw = <<"/raw/", WindowID/binary>>,
        Gateway = stub_gateway(
            fun('POST', <<"/graphql">>) -> {200, Edges};
               ('GET', Path) when Path =:= Raw -> {200, zlib:compress(Data)};
               (_, _) -> {404, <<>>}
            end),
        Cold = maps:get(<<"cold-tier">>, Opts0),
        Opts = Opts0#{ <<"cold-tier">> => Cold#{ <<"arweave-gateway">> => Gateway } },
        {ok, Got} = read_assignment(P, 7, Opts),
        ?assertEqual(7, hb_util:int(hb_ao:get(<<"slot">>, Got, Opts))),
        ?assertEqual(<<"7">>, hb_ao:get(<<"body/n">>, Got, Opts)),
        ?assertEqual(not_found, read_assignment(P, 8, Opts))
    end}.

stub_gateway(Route) ->
    {ok, _} = application:ensure_all_started(inets),
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}, {packet, http_bin}]),
    {ok, Port} = inet:port(Listen),
    spawn(fun() -> stub_accept(Listen, Route) end),
    <<"http://localhost:", (integer_to_binary(Port))/binary>>.

stub_accept(Listen, Route) ->
    case gen_tcp:accept(Listen) of
        {ok, Socket} ->
            {ok, {http_request, Method, {abs_path, Path}, _}} = gen_tcp:recv(Socket, 0, 5000),
            Len = stub_headers(Socket, 0),
            ok = inet:setopts(Socket, [{packet, raw}]),
            _ = case Len of 0 -> ok; _ -> gen_tcp:recv(Socket, Len, 5000) end,
            {Status, Body} = Route(Method, Path),
            gen_tcp:send(Socket, [<<"HTTP/1.1 ">>, integer_to_binary(Status), <<" X\r\n">>,
                <<"content-length: ">>, integer_to_binary(byte_size(Body)),
                <<"\r\nconnection: close\r\n\r\n">>, Body]),
            gen_tcp:close(Socket),
            stub_accept(Listen, Route);
        {error, _} -> ok
    end.

stub_headers(Socket, Len) ->
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, {http_header, _, 'Content-Length', _, V}} -> stub_headers(Socket, binary_to_integer(V));
        {ok, http_eoh} -> Len;
        {ok, _} -> stub_headers(Socket, Len)
    end.

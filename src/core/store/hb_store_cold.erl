%%% @doc The cold tier: a node's history on a shared filesystem (an NFS share
%%% mounted on every node), so that its local disk holds only hot data.
%%%
%%% One invariant: local disk is bounded at steady state, whatever the length
%%% of the history. A process is its spawn and its ordered signed messages;
%%% everything else is cache. So:
%%%
%%%   - a signed row (an assignment and its message) leaves local disk only
%%%     below the process's hot window (`hot-slots', its newest slots) and
%%%     only once a verified copy of it is on the share;
%%%   - a derived row (a computed state, a snapshot) never waits for the
%%%     share (`hb_store_retention'); the share gets a sample of snapshots,
%%%     the newest one at most every `checkpoint-every-seconds', so a slow
%%%     link costs replay depth for old reads, never local disk.
%%%
%%% On the share, in its own folder, a node keeps:
%%%
%%%   slots/<pid>/<from>-<to>.hbc    every assignment and its message, as the
%%%                                  rows the scheduler wrote (packed:
%%%                                  `hb_cache'), a range per file
%%%   checkpoints/<pid>/<slot>-<ms>-<seq>.hbc
%%%                                  the sampled snapshot checkpoints (the
%%%                                  `seq'th of the process), thinned with
%%%                                  age but never to none: by default every
%%%                                  one for 30 days, 1 in 5 after, 1 in 25
%%%                                  after a year (`checkpoint-thinning')
%%%   replica/<pusd>/<slot>.hbc      the newest snapshot of each bridge
%%%                                  replica (`lib_bridge_replica'), two kept
%%%
%%% Each file is `HBC1', the SHA-256 of the rest, then a compressed term,
%%% written to `.tmp', synced and renamed. A slot range then goes through two
%%% marks, both rows of the scheduler store beside the slots:
%%%
%%%   written   the file is on the share (`~cold@1.0/pending/<pid>/<to>')
%%%   verified  its bytes were proven to be the share's (`verify'), and only
%%%             then is the range row (`~cold@1.0/range/<pid>/<to>') written
%%%             and the `shipped' mark moved; nothing else may delete
%%%
%%% `verify' is `readback' (drop the client's cached pages, read the file
%%% back, check its digest) or `coldcheck' (ask the share's own host, at
%%% `verify-url', to read the file from its disk and check it: one round trip
%%% for up to 64 files, and no byte of them comes back over the link; the
%%% host's `coldcheck' service answers each node for its own folder only).
%%%
%%% The scheduler's dedupe rows go with their slots: a slot's `slot-ids' row
%%% (the IDs it was assigned under) ships in its range file, and at the delete
%%% its `message-slots' rows are replaced by filters (`hb_store_dedupe'),
%%% written in the same transaction. A message whose IDs the filters cannot
%%% rule out is checked against the range file on the share (`seen/4'); if
%%% the share cannot be read it waits, never is assigned twice.
%%% Either way the digest proven is the one the node wrote into the file's
%%% header. A file's name is fixed by its process and
%%% range, so a crash between the file and its mark writes the same file
%%% again: shipping is exactly-once in effect. A range row is the only thing
%%% a local delete is ever decided from, and a delete takes only the rows a
%%% slot owns (`owned/3'), never slot 0 (whose packed row the process's own ID
%%% links to), in one LMDB transaction.
%%%
%%% The share is a residential link and goes away. Nothing on a request path
%%% writes to it. A server ships continuously: each round gives every process
%%% a few ranges, `concurrency' processes at a time, each range a work item
%%% with its own timeout (`io-timeout-ms' plus its bytes at `min-rate-bytes'
%%% a second) and its own mark, so a timeout costs one item, never the round.
%%% Every filesystem call runs in a process of its own and is abandoned at its
%%% timeout: a hung mount costs a timeout, never a blocked node. A round in
%%% which every item failed marks the share unavailable (`status/0', the
%%% `hb_cold_available' gauge) and the next backs off to `?MAX_BACKOFF_MS'. An
%%% outage costs local disk, never data: the hot window and the disk budget
%%% (`hb_store_budget') decide what may go, and an unshipped signed row never
%%% does.
%%%
%%% A process's window is also capped in bytes: its newest slots up to
%%% `hot-slots' or up to `hot-bytes-per-process' of their rows, whichever
%%% comes first, never below `hot-slots-floor'. Its bytes per slot are those
%%% of the ranges it shipped (a running average, `slot_bytes/2'). The cap is
%%% per process because the number of processes grows with every spawn; the
%%% node-level backstop is the disk budget (`hb_store_budget').
%%%
%%% Under disk pressure (`hb_store_budget' at `soft' or above) the hot window
%%% shrinks to `hot-slots-floor' and no checkpoint is shipped, so the link
%%% carries only what local disk is waiting on.
%%%
%%% Shipping is paced: a round that shipped B bytes is followed by a pause of
%%% at least B / `ship-bytes-per-second', so a backlog (a node that takes up
%%% the cold tier with a full window, or catches up after an outage) uses
%%% the link at that rate and no faster.
%%%
%%% A message read by ID after its slot left local disk is served from the
%%% share. Each range file has a sidecar, `<from>-<to>.ids', that lists every
%%% ID its rows answer for. The share's host indexes those sidecars
%%% (`coldcheck' `/locate'), so the index of every ID ever pruned grows on the
%%% share and never locally. In the node's `store' list, after its local
%%% stores, `#{ <<"store-module">> => hb_store_cold }' makes the cold tier a
%%% read-only store of `remote' scope. A read of an ID asks the host where the
%%% ID is, reads that range file (its digest checked), and answers with the
%%% ID's row, the packed message `hb_cache' unpacks. The newest few files read
%%% are kept in memory, so a page of history costs one file read. An index can
%%% only cause a miss: every answer is read back from the file.
%%%
%%% The scheduler's store is journaled to the share too (`hb_store_offsite_
%%% journal'), so every message reached it twice: in a journal segment and in
%%% a range file. Once every slot a shipped segment covers has a verified
%%% range file, and the segment holds nothing else (`retire-journal', on by
%%% default), the journal retires it: the share keeps each message once. The
%%% local journal, which crash recovery replays, is never touched. A node that
%%% lost its scheduler's store restores it from the range files and what is
%%% left of the journal (`restore/2').
%%%
%%% A bridge or admission replica keeps the schedule it verified in a store of
%%% its own (`lib_bridge_replica'), in the scheduler's layout, and that store
%%% follows the same rules: a replica registers it (`register_source/1'), and
%%% its ranges ship to the same `slots/<pid>/' folder, named
%%% `<from>-<to>-<tag>.hbc' (`tag' from the store's name, so a replica's file
%%% never replaces the scheduler's own), and leave local disk below the same
%%% hot window once verified. A replica that recomputes from an old slot (a
%%% new build) reads it back from there, as any old read does.
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
%%%   path                     the node's own folder (read and written)
%%%   peers                    [#{ node, path, trusted }]: other nodes'
%%%                            folders, read only
%%%   hot-slots                newest slots of each process kept locally
%%%                            (500,000)
%%%   hot-bytes-per-process    the rows of a process's window, at most
%%%                            (5 GiB): whichever of it and `hot-slots'
%%%                            comes first
%%%   ship-bytes-per-second    the shipper's pace (8 MiB)
%%%   hot-slots-floor          the same under disk pressure (2,000: the
%%%                            lost-push repair window)
%%%   prune-local-slots        false holds every local slot (true)
%%%   verify                   `readback' or `coldcheck' (`readback')
%%%   verify-url               the share host's `coldcheck' service, e.g.
%%%                            `http://10.77.0.1:8099' (`coldcheck' only:
%%%                            without it nothing is ever verified, so
%%%                            nothing is deleted)
%%%   verify-prefix            where `path' is inside the folder `coldcheck'
%%%                            answers this node for (empty: it is that
%%%                            folder; a lab node in a subfolder names it)
%%%   checkpoint-every-seconds the newest snapshot is shipped at most this
%%%                            often per process (3,600)
%%%   checkpoint-thinning      [[Days, N], ...]: past Days, keep the
%%%                            checkpoints whose `seq' is a multiple of N,
%%%                            and in every age band at least one
%%%                            ([[30, 5], [365, 25]])
%%%   concurrency              processes shipped at once (4)
%%%   ranges-per-round         ranges per process per round (4)
%%%   range-slots, range-bytes a range's size, at most (1,000; 8 MiB)
%%%   interval-ms              time between rounds once caught up (60,000)
%%%   io-timeout-ms            one filesystem call, at most, plus its bytes
%%%                            at `min-rate-bytes' a second (10,000; 262,144)
%%%   arweave-gateway          for the last fallback (`https://arweave.net'),
%%%                            `false' to never ask
-module(hb_store_cold).
-export([maybe_start/1, status/0, tick/1, config/1]).
-export([read_assignment/3, restore_checkpoint/3, ship_replica/3, restore_replica/4]).
-export([seen/4, register_source/1, restore/2]).
%% A read-only store (`store-module' `hb_store_cold'): reads by ID from the share.
-export([scope/1, read/3]).
-ifdef(TEST).
-export([test_env/2, assign/3, share/1]).
-endif.
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SERVER, hb_store_cold_server).
-define(STATUS, {?MODULE, status}).
-define(ASSIGNMENTS, <<"~scheduler@1.0/assignments">>).
-define(MARK, <<"~cold@1.0">>).
-define(PACKED, "hbpk1:").
-define(LAG, 16).
-define(MAX_BACKOFF_MS, 600000).
%% Between rounds while a backlog remains: the shipper runs flat out, at low
%% priority, and yields this long so a round's bookkeeping never spins.
-define(BUSY_PAUSE_MS, 200).
-define(DAY_MS, 86400000).

%%% Configuration

%% @doc The cold tier's settings, or `undefined' when it is off.
config(Opts) ->
    case hb_opts:get(<<"cold-tier">>, undefined, Opts) of
        C = #{ <<"path">> := Path } ->
            Int = fun(K, D) -> hb_util:int(maps:get(K, C, D)) end,
            Floor = max(1, Int(<<"hot-slots-floor">>, 2000)),
            #{
                path => hb_util:list(Path),
                peers =>
                    [ #{ node => hb_util:bin(maps:get(<<"node">>, P, <<"peer">>)),
                         path => hb_util:list(maps:get(<<"path">>, P)),
                         trusted => hb_util:atom(maps:get(<<"trusted">>, P, false)) =:= true }
                    || P <- peers(maps:get(<<"peers">>, C, [])) ],
                hot_slots => max(Floor, Int(<<"hot-slots">>, 500000)),
                hot_bytes => max(1, Int(<<"hot-bytes-per-process">>, 5368709120)),
                ship_rate => max(1, Int(<<"ship-bytes-per-second">>, 8388608)),
                retire_journal =>
                    hb_util:atom(maps:get(<<"retire-journal">>, C, true)) =:= true,
                hot_floor => Floor,
                prune_local_slots =>
                    hb_util:atom(maps:get(<<"prune-local-slots">>, C, true)) =:= true,
                verify =>
                    case hb_util:bin(maps:get(<<"verify">>, C, <<"readback">>)) of
                        <<"coldcheck">> -> coldcheck;
                        _ -> readback
                    end,
                verify_url =>
                    case maps:get(<<"verify-url">>, C, undefined) of
                        undefined -> undefined;
                        U -> hb_util:list(U)
                    end,
                verify_prefix => hb_util:list(maps:get(<<"verify-prefix">>, C, <<>>)),
                checkpoint_every_ms => max(0, Int(<<"checkpoint-every-seconds">>, 3600)) * 1000,
                thinning => thinning(maps:get(<<"checkpoint-thinning">>, C, [[30, 5], [365, 25]])),
                concurrency => max(1, Int(<<"concurrency">>, 4)),
                ranges_per_round => max(1, Int(<<"ranges-per-round">>, 4)),
                range => max(1, Int(<<"range-slots">>, 1000)),
                range_bytes => max(1, Int(<<"range-bytes">>, 8388608)),
                interval => Int(<<"interval-ms">>, 60000),
                io_timeout => Int(<<"io-timeout-ms">>, 10000),
                min_rate => max(1, Int(<<"min-rate-bytes">>, 262144)),
                gateway =>
                    case maps:get(<<"arweave-gateway">>, C, <<"https://arweave.net">>) of
                        false -> false;
                        <<"false">> -> false;
                        G -> hb_util:list(G)
                    end
            };
        _ -> undefined
    end.

%% `[[Days, N], ...]' as `[{AgeMs, N}]', oldest band last. A malformed entry
%% is dropped: the default is never fewer checkpoints than that.
thinning(L) when is_list(L) ->
    lists:sort(
        [ {D * ?DAY_MS, N}
        || E <- L, [D0, N0] <- [case E of [_, _] -> E; _ -> [x, x] end],
           D <- [catch hb_util:int(D0)], N <- [catch hb_util:int(N0)],
           is_integer(D), D >= 0, is_integer(N), N >= 1 ]);
thinning(M) when is_map(M) -> thinning(hb_util:message_to_ordered_list(M));
thinning(_) -> [].

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

%% Rounds back to back while a backlog remains, then one every
%% `interval-ms'. A round in which nothing could be done backs off, doubling
%% up to `?MAX_BACKOFF_MS'.
server(Opts, Backoff) ->
    process_flag(priority, low),
    Cfg = config(Opts),
    Interval = maps:get(interval, Cfg),
    {Wait, Next} =
        case catch round(Cfg, Opts) of
            {ok, true} -> {paced(Cfg), 0};
            {ok, false} -> {Interval, 0};
            _ ->
                B = min(?MAX_BACKOFF_MS, max(Interval, Backoff * 2)),
                {B, B}
        end,
    receive
        {tick_now, From, Ref} -> From ! {Ref, ok}, server(Opts, Next)
    after Wait -> server(Opts, Next)
    end.

%% The pause after a round with a backlog left: long enough that the round's
%% bytes went at `ship-bytes-per-second' at most.
paced(Cfg) ->
    S = status(),
    Bytes = maps:get(last_round_bytes, S, 0),
    Ms = maps:get(last_round_ms, S, 0),
    max(?BUSY_PAUSE_MS, Bytes * 1000 div maps:get(ship_rate, Cfg) - Ms).

%% @doc The share's state: whether the last round reached it, since when not,
%% what this node still has to ship, and what it shipped.
status() -> persistent_term:get(?STATUS, #{ available => true }).

status_update(Fun) -> persistent_term:put(?STATUS, Fun(status())).

%% @doc One round (`round/2'): `ok', or why nothing at all could be done.
tick(Opts) ->
    case round(config(Opts), Opts) of
        {ok, _} -> ok;
        Error -> Error
    end.

%% Ship signed rows (every process, a few ranges each, and confirm what was
%% written), delete what is verified and below the hot window, then ship the
%% checkpoints that are due. `{ok, More}', `More' when a backlog remains, if
%% anything reached the share or nothing failed; else the first failure, and
%% the share is marked unavailable. Local deletes are decided from verified
%% range rows only, so they run whatever the share's state.
round(Cfg, Opts) ->
    Started = erlang:monotonic_time(millisecond),
    Level = hb_store_budget:level(),
    Slots = run_phase(slots, fun() -> ship_slots(Cfg, Opts) end),
    Prune =
        case maps:get(prune_local_slots, Cfg) of
            true -> run_phase(prune, fun() -> prune_slots(Cfg, Level, Opts) end);
            false -> #{}
        end,
    Retire = run_phase(journal, fun() -> retire_journal(Cfg, Opts) end),
    Checkpoints =
        case hb_store_budget:rank(Level) >= hb_store_budget:rank(soft) of
            true -> #{ skipped => Level };
            false -> run_phase(checkpoints, fun() -> ship_checkpoints(Cfg, Opts) end)
        end,
    Reached = maps:get(done, Slots, 0) + maps:get(done, Checkpoints, 0),
    Failures = lists:append([ maps:get(failures, R, []) || R <- [Slots, Prune, Retire, Checkpoints] ]),
    Result =
        case {Failures, Reached} of
            {[{_, Why} | _], 0} -> {error, Why};
            _ -> {ok, maps:get(more, Slots, false)}
        end,
    status_update(fun(S) ->
        S#{ level => Level,
            last_round => erlang:system_time(millisecond),
            last_round_ms => erlang:monotonic_time(millisecond) - Started,
            last_round_bytes => maps:get(bytes, Slots, 0) + maps:get(bytes, Checkpoints, 0),
            last_failures => lists:sublist(Failures, 8),
            shipped_bytes => maps:get(shipped_bytes, S, 0)
                + maps:get(bytes, Slots, 0) + maps:get(bytes, Checkpoints, 0),
            slot_bytes => maps:get(slot_bytes, S, 0) + maps:get(bytes, Slots, 0),
            checkpoint_bytes => maps:get(checkpoint_bytes, S, 0) + maps:get(bytes, Checkpoints, 0),
            verified_ranges => maps:get(verified_ranges, S, 0) + maps:get(verified, Slots, 0),
            verified_checkpoints => maps:get(verified_checkpoints, S, 0) + maps:get(done, Checkpoints, 0),
            pruned_slots => maps:get(pruned_slots, S, 0) + maps:get(pruned, Prune, 0),
            retired_segments => maps:get(retired_segments, S, 0) + maps:get(retired, Retire, 0) }
    end),
    available(Result),
    Result.

available({ok, _}) ->
    case maps:get(available, status(), true) of
        false -> ?event(warning, {cold_tier_available_again});
        true -> ok
    end,
    set_gauge(hb_cold_available, 1),
    status_update(fun(S) ->
        maps:without([unavailable_since, last_error],
            S#{ available => true, last_tick => erlang:system_time(millisecond) })
    end);
available({error, Why}) ->
    case maps:get(available, status(), true) of
        true -> ?event(error, {cold_tier_unavailable, Why});
        false -> ok
    end,
    set_gauge(hb_cold_available, 0),
    status_update(fun(S) ->
        S#{ available => false, last_error => Why,
            unavailable_since => maps:get(unavailable_since, S, erlang:system_time(millisecond)) }
    end).

%% A phase's stats (`#{ done, failures, ... }'); a phase that crashes is one
%% failure. Its last stats and duration are in `status/0'.
run_phase(Name, Fun) ->
    Started = erlang:monotonic_time(millisecond),
    Stats =
        try Fun()
        catch
            throw:{cold_io, Why} -> #{ failures => [{Name, Why}] };
            C:E -> #{ failures => [{Name, {C, E}}] }
        end,
    Ms = erlang:monotonic_time(millisecond) - Started,
    status_update(fun(S) ->
        Phases = maps:get(phases, S, #{}),
        S#{ phases => Phases#{ Name => maps:without([failures], Stats#{ ms => Ms }) } }
    end),
    Stats.

%% `Fun' over `Items', at most `N' at once, each in a process of its own at
%% low priority. A worker that crashes is that item's failure. Workers are
%% linked to the round: whatever ends the round ends them, as a crash of the
%% node would, so no worker outlives the round that planned its work.
pmap(Fun, Items, N) ->
    Trap = process_flag(trap_exit, true),
    try pmap(Fun, Items, N, #{}, [])
    after
        process_flag(trap_exit, Trap)
    end.
pmap(_Fun, [], _N, Running, Acc) when map_size(Running) =:= 0 -> lists:reverse(Acc);
pmap(Fun, [Item | Rest], N, Running, Acc) when map_size(Running) < N ->
    {Pid, Ref} =
        spawn_opt(fun() ->
            process_flag(priority, low),
            exit({done, Fun(Item)})
        end, [link, monitor]),
    pmap(Fun, Rest, N, Running#{ Ref => {Item, Pid} }, Acc);
pmap(Fun, Items, N, Running, Acc) ->
    receive
        {'DOWN', Ref, process, _, Reason} when is_map_key(Ref, Running) ->
            {Item, Pid} = maps:get(Ref, Running),
            receive {'EXIT', Pid, _} -> ok end,
            R =
                case Reason of
                    {done, Stats} -> Stats;
                    Other -> #{ failures => [{Item, Other}] }
                end,
            pmap(Fun, Items, N, maps:remove(Ref, Running), [R | Acc])
    end.

sum(Key, Stats) -> lists:sum([ maps:get(Key, S, 0) || S <- Stats ]).

set_gauge(Name, Value) ->
    try
        hb_prometheus:declare(gauge, [{name, Name}, {help, "Cold tier"}]),
        prometheus_gauge:set(Name, Value)
    catch _:_ -> ok
    end.

%%% Filesystem calls, bounded

%% Run `Fun' in a process of its own (`hb_file:bounded/2'): its result, or
%% `{cold_io, timeout}' is thrown after `io-timeout-ms'. A call on a hung
%% mount is left behind. Every call in `Fun' is an `hb_file' one, made in that
%% process: through the node's one `file_server_2', a hung mount held every
%% file call of the node, and every compute that read `hb_store_fs' waited.
io(Cfg, Fun) ->
    case hb_file:bounded(Fun, maps:get(io_timeout, Cfg)) of
        {ok, Res} -> Res;
        {error, timeout} -> throw({cold_io, timeout});
        {error, {'EXIT', Why}} -> throw({cold_io, Why})
    end.

%% Write a term as a cold file, durably: `.tmp', synced, renamed. `{Bytes,
%% Digest}': `Digest' is the SHA-256 of the body, which the file's header
%% carries and which `verify' proves the share holds (`verified_files/3').
%% The call may take `io-timeout-ms' plus the file's bytes at
%% `min-rate-bytes' a second.
put_file(Cfg, Path, Term) ->
    Body = term_to_binary(Term, [{compressed, 6}]),
    Digest = crypto:hash(sha256, Body),
    Bin = <<"HBC1", Digest/binary, Body/binary>>,
    io(sized(Cfg, byte_size(Bin)), fun() ->
        Tmp = Path ++ ".tmp",
        ok = hb_file:ensure_dir(Path),
        ok = hb_file:write_file(Tmp, Bin, [binary, sync]),
        ok = hb_file:rename(Tmp, Path)
    end),
    {byte_size(Bin), Digest}.

%% The timeout of a call that moves `Bytes' over the link.
sized(Cfg, Bytes) ->
    Cfg#{ io_timeout => maps:get(io_timeout, Cfg) + Bytes * 1000 div maps:get(min_rate, Cfg) }.

%% Whether the share holds each file of `Files' (`[{RelPath, Digest,
%% Bytes}]', paths relative to this node's folder) exactly as written: for
%% each in order, `true', or `false' (absent, or other bytes: write it
%% again). A share that cannot be asked throws `{cold_io, _}', and nothing is
%% decided. `readback' drops the client's cached pages of a file before
%% reading it, so its bytes come from the server: a read just after a write
%% is otherwise answered from the NFS client's own cache. `coldcheck' asks
%% the share's host (`verify-url'), 64 files to a request.
verified_files(_Cfg, []) -> [];
verified_files(#{ verify := readback } = Cfg, Files) ->
    [ case io(sized(Cfg, Bytes), fun() -> hb_file:read_file_uncached(
              filename:join(maps:get(path, Cfg), Rel)) end) of
          {ok, <<"HBC1", Digest:32/binary, Body/binary>>} -> crypto:hash(sha256, Body) =:= Digest;
          _ -> false
      end
    || {Rel, Digest, Bytes} <- Files ];
verified_files(#{ verify := coldcheck, verify_url := undefined }, _Files) ->
    throw({cold_io, no_verify_url});
verified_files(#{ verify := coldcheck } = Cfg, Files) when length(Files) > 64 ->
    {First, Rest} = lists:split(64, Files),
    verified_files(Cfg, First) ++ verified_files(Cfg, Rest);
verified_files(#{ verify := coldcheck, verify_url := Url } = Cfg, Files) ->
    Asked = fun(Rel) ->
        case maps:get(verify_prefix, Cfg, "") of
            "" -> Rel;
            Prefix -> filename:join(Prefix, Rel)
        end
    end,
    Query = lists:join("&", [ "path=" ++ uri_string:quote(Asked(Rel)) || {Rel, _, _} <- Files ]),
    Answer =
        io(Cfg, fun() ->
            hb_http_gate:httpc_request(get, {Url ++ "/verify?" ++ lists:flatten(Query), []},
                [{timeout, maps:get(io_timeout, Cfg)}], [{body_format, binary}], #{})
        end),
    case Answer of
        {ok, {{_, 200, _}, _, Json}} ->
            Verdicts = hb_json:decode(Json),
            [ coldcheck_verdict(maps:get(list_to_binary(Asked(Rel)), Verdicts, #{}), Digest)
            || {Rel, Digest, _} <- Files ];
        Other -> throw({cold_io, {coldcheck, Other}})
    end.

%% A `coldcheck' answer for one file: proven only when the host read the
%% digest we wrote; a missing file or other bytes is written again; anything
%% else (a refused path: a misconfigured share) decides nothing.
coldcheck_verdict(#{ <<"ok">> := true, <<"sha256">> := Hex }, Digest) ->
    case catch binary:decode_hex(Hex) of
        Digest -> true;
        _ -> false
    end;
coldcheck_verdict(#{ <<"error">> := Why }, _Digest)
        when Why =:= <<"digest">>; Why =:= <<"header">>; Why =:= <<"No such file or directory">> ->
    false;
coldcheck_verdict(Other, _Digest) -> throw({cold_io, {coldcheck, Other}}).

%% A cold file's term, or `error' if it is absent or damaged.
get_file(Cfg, Path) ->
    case io(Cfg, fun() -> hb_file:read_file(Path) end) of
        {ok, <<"HBC1", Hash:32/binary, Body/binary>>} ->
            case crypto:hash(sha256, Body) of
                Hash -> {ok, binary_to_term(Body, [safe])};
                _ -> ?event(warning, {cold_file_damaged, Path}), error
            end;
        _ -> error
    end.

list_dir(Cfg, Dir) ->
    case io(Cfg, fun() -> hb_file:list_dir(Dir) end) of
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

%% A process's newest slot, found by probing up from `Hint' (a slot known
%% shipped, or -1): its slots above every deleted one are contiguous, so a
%% few reads find it, where listing them cost every slot in the window, every
%% round (100,000 a process). -1 for none.
head(Store, P, Hint) ->
    case has_slot(Store, P, Hint + 1) of
        false -> Hint;
        true -> gallop(Store, P, Hint + 1, 1)
    end.

gallop(Store, P, Known, Step) ->
    case has_slot(Store, P, Known + Step) of
        true -> gallop(Store, P, Known + Step, Step * 2);
        false -> bisect(Store, P, Known, Known + Step)
    end.

bisect(_Store, _P, Lo, Hi) when Hi - Lo =< 1 -> Lo;
bisect(Store, P, Lo, Hi) ->
    Mid = (Lo + Hi) div 2,
    case has_slot(Store, P, Mid) of
        true -> bisect(Store, P, Mid, Hi);
        false -> bisect(Store, P, Lo, Mid)
    end.

has_slot(_Store, _P, S) when S < 0 -> false;
has_slot(Store, P, S) ->
    case hb_store:read([Store], #{ <<"read">> => slot_path(P, S), <<"exact">> => true }, #{}) of
        {ok, _} -> true;
        _ -> false
    end.

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

%% Ship every process's slots past its `written' mark, all but its newest
%% `?LAG', `ranges-per-round' ranges of it per round and `concurrency'
%% processes at once, and confirm what was written before. Stats:
%% `#{ done, verified, bytes, more, failures }'.
ship_slots(Cfg, Opts) ->
    Work =
        [ {Cfg#{ tag => Tag }, Store, P}
        || {Store, Tag} <- sources(Opts), P <- list_names(Store, ?ASSIGNMENTS), ?IS_ID(P) ],
    Stats =
        pmap(fun({SCfg, Store, P}) -> ship_process(SCfg, Store, P, Opts) end, Work,
            maps:get(concurrency, Cfg)),
    Lag = sum(lag, Stats),
    set_gauge(hb_cold_lag_slots, Lag),
    status_update(fun(S) -> S#{ lag_slots => Lag } end),
    #{ done => sum(done, Stats), verified => sum(verified, Stats), bytes => sum(bytes, Stats),
       lag => Lag,
       more => lists:any(fun(S) -> maps:get(more, S, false) end, Stats),
       failures => lists:append([ maps:get(failures, S, []) || S <- Stats ]) }.

%% One process: confirm its written ranges, write up to `ranges-per-round'
%% more, and confirm again (`readback' confirms at once). A failure ends this
%% process's turn, never another's.
ship_process(Cfg, Store, P, Opts) ->
    {Verified0, Failed0} = confirm(Cfg, Store, P),
    Written =
        max(read_int(Store, mark_key(<<"shipped">>, P), -1),
            read_int(Store, mark_key(<<"written">>, P), -1)),
    Head = head(Store, P, Written),
    % A slot written per round at most `ranges-per-round' x `range-slots':
    % never a list of the whole backlog.
    Last = min(Head - ?LAG, Written + maps:get(ranges_per_round, Cfg) * maps:get(range, Cfg)),
    Ready = case Last > Written of true -> lists:seq(Written + 1, Last); false -> [] end,
    {Done, Bytes, Used, Failed1} =
        case Failed0 of
            [] -> write_ranges(Cfg, Store, P, Ready, maps:get(ranges_per_round, Cfg), Opts, {0, 0, 0});
            _ -> {0, 0, 0, []}
        end,
    {Verified1, Failed2} =
        case Done of
            0 -> {0, []};
            _ -> confirm(Cfg, Store, P)
        end,
    Shipped = read_int(Store, mark_key(<<"shipped">>, P), -1),
    #{ done => Done + Verified0 + Verified1, verified => Verified0 + Verified1,
       bytes => Bytes, more => Written + Used < Head - ?LAG,
       lag => max(0, Head - Shipped),
       failures => Failed0 ++ Failed1 ++ Failed2 }.

%% Write ranges of `Slots', each at most `range-slots' slots and about
%% `range-bytes' bytes, `N' of them at most. `{Ranges, Bytes, SlotsUsed,
%% Failures}'.
write_ranges(_Cfg, _Store, _P, [], _N, _Opts, {D, B, U}) -> {D, B, U, []};
write_ranges(_Cfg, _Store, _P, _Slots, 0, _Opts, {D, B, U}) -> {D, B, U, []};
write_ranges(Cfg, Store, P, Slots, N, Opts, {D, B, U}) ->
    {Range, Rows} = take_range(Store, P, Slots, maps:get(range, Cfg), maps:get(range_bytes, Cfg)),
    try write_range(Cfg, Store, P, Range, Rows, Opts) of
        Bytes ->
            write_ranges(Cfg, Store, P, lists:nthtail(length(Range), Slots), N - 1, Opts,
                {D + 1, B + Bytes, U + length(Range)})
    catch
        throw:{cold_io, Why} -> {D, B, U, [{P, Why}]};
        C:E -> {D, B, U, [{P, {C, E}}]}
    end.

%% The first slots of `Slots' for one file, and their rows.
take_range(Store, P, Slots, MaxSlots, MaxBytes) ->
    take_range(Store, P, Slots, MaxSlots, MaxBytes, [], [], 0).
take_range(Store, P, [S | Rest], MaxSlots, MaxBytes, Range, Rows, Bytes)
        when MaxSlots > 0, (Range =:= [] orelse Bytes < MaxBytes) ->
    R = closure(Store, slot_path(P, S)) ++ slot_ids_row(Store, P, S),
    Size = lists:sum([ byte_size(K) + byte_size(V) || {K, V} <- R ]),
    take_range(Store, P, Rest, MaxSlots - 1, MaxBytes, [S | Range], [R | Rows], Bytes + Size);
take_range(_Store, _P, _Slots, _MaxSlots, _MaxBytes, Range, Rows, _Bytes) ->
    {lists:reverse(Range), lists:append(lists:reverse(Rows))}.

%% One range's file, and its `written' mark: a `pending' row naming the
%% file's hash, which `confirm/3' turns into a range row once it is proven.
write_range(Cfg, Store, P, Range, Rows, Opts) ->
    From = hd(Range), To = lists:last(Range),
    Ms = assignment_ms(Store, P, To, Opts),
    File = range_file(Cfg, P, From, To),
    {Bytes, Digest} =
        put_file(Cfg, File,
            #{ process => P, from => From, to => To, newest_ms => Ms, rows => Rows }),
    % The IDs the range answers for, for the share host's index (`locate/2').
    IDs = iolist_to_binary([ [K, $\n] || {K, _} <- Rows, ?IS_ID(K), byte_size(K) =:= 43 ]),
    io(Cfg, fun() ->
        Ids = filename:rootname(File) ++ ".ids",
        ok = hb_file:write_file(Ids ++ ".tmp", IDs, [binary]),
        ok = hb_file:rename(Ids ++ ".tmp", Ids)
    end),
    Raw = lists:sum([ byte_size(K) + byte_size(V) || {K, V} <- Rows ]),
    ok = hb_store:write([Store], #{
        pending_key(P, To) => term_to_binary({From, To, Ms, Digest, Bytes}),
        mark_key(<<"written">>, P) => integer_to_binary(To),
        mark_key(<<"slot-bytes">>, P) =>
            integer_to_binary(slot_bytes(read_int(Store, mark_key(<<"slot-bytes">>, P), 0),
                Raw div length(Range))) }, #{}),
    ok = hb_store:flush([Store], #{}),
    Bytes.

%% Turn `P''s written ranges whose files are verified, oldest first, into
%% range rows, moving the `shipped' mark, until the first that is not. The
%% range row is written and the pending row deleted in ONE transaction
%% (`elmdb:delete_batch/2' commits pending writes with its deletes), so a
%% crash leaves one or the other, and a pending row left behind is confirmed
%% again, to the same row. A file that does not hold what was written sends
%% the `written' mark back before it, to be written again; a share that
%% cannot be asked decides nothing. `{Verified, Failures}'.
confirm(Cfg, Store, P) ->
    #{ <<"db">> := DB } = hb_store:find(inner(Store)),
    Pending =
        [ {<<(pending_prefix(P))/binary, "/", Name/binary>>, binary_to_term(V, [safe])}
        || Name <- lists:sort(list_names(Store, pending_prefix(P))),
           {ok, V} <- [hb_store:read([Store], #{ <<"read">> => <<(pending_prefix(P))/binary,
                "/", Name/binary>>, <<"exact">> => true }, #{})] ],
    try verified_files(Cfg, [ {range_rel(Cfg, P, From, To), Digest, Bytes}
                             || {_, {From, To, _, Digest, Bytes}} <- Pending ]) of
        Verdicts -> promote(Store, DB, P, lists:zip(Pending, Verdicts), 0)
    catch
        throw:{cold_io, Why} -> {0, [{P, Why}]}
    end.

promote(_Store, _DB, _P, [], N) -> {N, []};
promote(Store, DB, P, [{{Key, {From, To, Ms, _, _}}, true} | Rest], N) ->
    Shipped = max(To, read_int(Store, mark_key(<<"shipped">>, P), -1)),
    ok = hb_store:write([Store], #{
        range_key(P, To) => term_to_binary({From, To, Ms}),
        mark_key(<<"shipped">>, P) => integer_to_binary(Shipped) }, #{}),
    {ok, _} = elmdb:delete_batch(DB, [Key]),
    promote(Store, DB, P, Rest, N + 1);
promote(Store, DB, P, [{{_Key, {From, To, _, _, _}}, false} | _] = Rest, N) ->
    ?event(warning, {cold_range_not_on_share, P, From, To}),
    ok = hb_store:write([Store], #{
        mark_key(<<"written">>, P) => integer_to_binary(From - 1) }, #{}),
    {ok, _} = elmdb:delete_batch(DB, [ K || {{K, _}, _} <- Rest ]),
    {N, []}.

%% A range's file below this node's folder: `<from>-<to>.hbc' for the
%% scheduler's own store, `<from>-<to>-<tag>.hbc' for a replica's.
range_rel(Cfg, P, From, To) ->
    Tag = case maps:get(tag, Cfg, "") of "" -> ""; T -> "-" ++ T end,
    filename:join(["slots", hb_util:list(P), pad(From) ++ "-" ++ pad(To) ++ Tag ++ ".hbc"]).

range_file(Cfg, P, From, To) -> filename:join(maps:get(path, Cfg), range_rel(Cfg, P, From, To)).

%% The stores whose slots ship: the scheduler's own, and each replica store
%% registered (`register_source/1'), with its file tag.
-define(SOURCES, {?MODULE, sources}).
sources(Opts) ->
    [ {irr(Opts), ""} |
      [ {Store, Tag} || {Tag, Store} <- maps:to_list(persistent_term:get(?SOURCES, #{})) ] ].

%% @doc Ship and prune `Store' too: a replica's verified schedule, in the
%% scheduler's layout. Its files are tagged with a digest of its name.
register_source(Store = #{ <<"name">> := Name }) ->
    Tag = lists:sublist(binary_to_list(binary:encode_hex(crypto:hash(sha256, Name), lowercase)), 8),
    Sources = persistent_term:get(?SOURCES, #{}),
    case maps:get(Tag, Sources, undefined) =:= Store of
        true -> ok;
        false -> persistent_term:put(?SOURCES, Sources#{ Tag => Store })
    end.

pending_prefix(P) -> mark_key(<<"pending">>, P).
pending_key(P, To) -> <<(pending_prefix(P))/binary, "/", (list_to_binary(pad(To)))/binary>>.
range_prefix(P) -> mark_key(<<"range">>, P).
range_key(P, To) -> <<(range_prefix(P))/binary, "/", (list_to_binary(pad(To)))/binary>>.

assignment_ms(Store, P, S, Opts) ->
    case hb_store_retention:assignment_time(P, S, Opts#{ <<"scheduler-store">> => [Store] }) of
        {ok, Ms} -> Ms;
        _ -> erlang:system_time(millisecond)
    end.

%% Delete, locally, every verified range that lies wholly below its
%% process's hot window (`hot-slots' newest slots; `hot-slots-floor' under
%% disk pressure): the rows its slots own and its range row, in one LMDB
%% transaction. Slot 0 stays, and so does every `message-slots' row (the
%% scheduler's dedupe index). `#{ done, pruned }'.
prune_slots(Cfg, Level, Opts) ->
    Procs =
        [ {{Store, P}, DB, Head, Ranges}
        || {Store, _} <- sources(Opts),
           #{ <<"db">> := DB } <- [hb_store:find(inner(Store))],
           P <- list_names(Store, ?ASSIGNMENTS), ?IS_ID(P),
           Ranges <- [lists:sort(list_names(Store, range_prefix(P)))],
           Shipped <- [read_int(Store, mark_key(<<"shipped">>, P), -1)],
           Head <- [head(Store, P, Shipped)] ],
    Windows = windows(Cfg, Level,
        [ {K, read_int(Store, mark_key(<<"slot-bytes">>, P), 0)} || {K = {Store, P}, _, _, _} <- Procs ], Opts),
    Counts =
        [ prune_ranges(Store, DB, P, Head - maps:get(K, Windows), Ranges, 0)
        || {K = {Store, P}, DB, Head, Ranges} <- Procs, Ranges =/= [] ],
    status_update(fun(S) -> S#{ windows_capped => maps:size(maps:filter(
        fun(_, W) -> W < maps:get(hot_slots, Cfg) end, Windows)) } end),
    #{ done => length([ C || C <- Counts, C > 0 ]), pruned => lists:sum(Counts) }.

%% Each process's hot window: its newest `hot-slots', or as many as fit in
%% `hot-bytes-per-process' at its bytes per slot, whichever is fewer, never
%% below `hot-slots-floor'; the floor alone under disk pressure.
windows(Cfg, Level, Procs, _Opts) ->
    Floor = maps:get(hot_floor, Cfg),
    Pressed = hb_store_budget:rank(Level) >= hb_store_budget:rank(soft),
    maps:from_list(
        [ {K, case Pressed of
                  true -> Floor;
                  false -> max(Floor, min(maps:get(hot_slots, Cfg), fits(Cfg, PerSlot)))
              end}
        || {K, PerSlot} <- Procs ]).

fits(_Cfg, PerSlot) when PerSlot =< 0 -> infinity;
fits(Cfg, PerSlot) -> maps:get(hot_bytes, Cfg) div PerSlot.

%% A process's bytes per slot, the rows of its slots as written: a running
%% average over the ranges it shipped, each new range a quarter of it.
slot_bytes(0, New) -> New;
slot_bytes(Old, New) -> (3 * Old + New) div 4.

prune_ranges(_Store, _DB, _P, _Below, [], N) -> N;
prune_ranges(Store, DB, P, Below, [Name | Rest], N) ->
    Key = <<(range_prefix(P))/binary, "/", Name/binary>>,
    case hb_store:read([Store], #{ <<"read">> => Key, <<"exact">> => true }, #{}) of
        {ok, V} ->
            {From, To, _Ms} = binary_to_term(V, [safe]),
            case To =< Below of
                false -> N;
                true ->
                    Slots = lists:seq(max(1, From), To),
                    Owned = [ owned(Store, P, S) || S <- Slots ],
                    Keys = lists:usort(lists:append([ K || {K, _} <- Owned ])),
                    IDs = lists:append([ I || {_, I} <- Owned ]),
                    % The filters that replace the dedupe rows, then the
                    % delete: `delete_batch/2' commits both in one transaction.
                    ok = hb_store:write([Store],
                        hb_store_dedupe:add(Store, P, From, To, IDs), #{}),
                    {ok, _} = elmdb:delete_batch(DB, Keys ++ [Key]),
                    prune_ranges(Store, DB, P, Below, Rest, N + length(Slots))
            end;
        _ -> prune_ranges(Store, DB, P, Below, Rest, N)
    end.

%% The rows slot `S' of `P' owns, and its dedupe IDs: `{Keys, IDs}'. The
%% rows are its slot path, its packed row (`hb_cache', `cache-pack'), every
%% row that links to that packed row (its other IDs, its message's IDs), its
%% `slot-ids' row and the `message-slots' rows of those IDs that name it. A
%% row its closure reaches that links anywhere else is another's and stays: a
%% process's own ID, which links to the packed row of its slot 0, or a
%% message stored exploded. An assignment stored exploded owns only its slot
%% path here.
owned(Store, P, S) ->
    Rows = closure(Store, slot_path(P, S)),
    Packed = [ K || {K, <<?PACKED, _/binary>>} <- Rows ],
    Links = [ <<"ao:", K/binary>> || K <- Packed ],
    IDs = slot_ids(Store, P, S),
    Dedupe =
        [ message_slot_path(P, ID) || ID <- IDs,
          read_int(Store, message_slot_path(P, ID), -1) =:= S ],
    {[ slot_path(P, S) | [ slot_ids_path(P, S) || IDs =/= [] ] ] ++ Dedupe ++
        [ K || {K, V} <- Rows, lists:member(K, Packed) orelse lists:member(V, Links) ],
     IDs}.

%% The dedupe IDs the scheduler wrote for a slot (`dev_scheduler_cache:
%% write_message_slot/4'), and the row that holds them.
slot_ids_path(P, S) ->
    <<"~scheduler@1.0/slot-ids/", P/binary, "/", (integer_to_binary(S))/binary>>.
message_slot_path(P, ID) -> <<"~scheduler@1.0/message-slots/", P/binary, "/", ID/binary>>.

slot_ids_row(Store, P, S) ->
    case hb_store:read([Store], #{ <<"read">> => slot_ids_path(P, S), <<"exact">> => true }, #{}) of
        {ok, V} -> [{slot_ids_path(P, S), V}];
        _ -> []
    end.

slot_ids(Store, P, S) ->
    case slot_ids_row(Store, P, S) of
        [{_, V}] -> binary_to_term(V, [safe]);
        [] -> []
    end.

%% @doc Whether a message with dedupe IDs `IDs' was assigned to `P' in a slot
%% that left local disk: `{ok, Slot, ID}', `not_found', or `{unavailable,
%% Why}' when the filters cannot rule it out and the range's file on the
%% share cannot be read. Only this node's own folder is asked: a dedupe
%% answer is never taken from another node.
seen(Store, P, IDs, Opts) ->
    case hb_store_dedupe:candidates(Store, P, IDs) of
        [] -> not_found;
        Candidates ->
            case config(Opts) of
                undefined -> {unavailable, no_cold_tier};
                Cfg -> seen_in(Cfg, P, Candidates)
            end
    end.

seen_in(_Cfg, _P, []) -> not_found;
seen_in(Cfg, P, [{ID, From, To} | Rest]) ->
    try get_file(Cfg, range_file(Cfg, P, From, To)) of
        {ok, #{ process := P, from := From, to := To, rows := Rows }} ->
            Found =
                [ S || {K, V} <- Rows,
                       <<"~scheduler@1.0/slot-ids/", Tail/binary>> <- [K],
                       [P, SBin] <- [binary:split(Tail, <<"/">>)],
                       S <- [binary_to_integer(SBin)],
                       lists:member(ID, binary_to_term(V, [safe])) ],
            case Found of
                [S | _] -> {ok, S, ID};
                [] -> seen_in(Cfg, P, Rest)
            end;
        _ -> {unavailable, {range_unreadable, P, From, To}}
    catch
        throw:{cold_io, Why} -> {unavailable, Why}
    end.

%% Retire the journal's shipped segments whose every slot has a verified
%% range file (the journal lists only segments that hold nothing else).
retire_journal(#{ retire_journal := false }, _Opts) -> #{};
retire_journal(_Cfg, Opts) ->
    case irr(Opts) of
        Irr = #{ <<"store-module">> := hb_store_offsite_journal } ->
            Covers = hb_store_offsite_journal:covers(Irr),
            Shipped = fun(P) -> read_int(Irr, mark_key(<<"shipped">>, P), -1) end,
            Names =
                [ N || {N, #{ slots := Slots, other := false }} <- Covers,
                       lists:all(fun({P, S}) -> Shipped(P) >= S end, maps:to_list(Slots)) ],
            case Names of
                [] -> #{};
                _ ->
                    case hb_store_offsite_journal:retire(Irr, Names) of
                        {ok, New, _Gone} -> #{ retired => New };
                        Other -> #{ failures => [{journal, Other}] }
                    end
            end;
        _ -> #{}
    end.

%% @doc Rebuild a scheduler's store that was lost, into `Target', from this
%% node's folder on the share: every slot of every range file (the rows as
%% they were, their `message-slots' rows derived from their `slot-ids'),
%% then what is left of the journal (`hb_store_offsite_journal:restore/3',
%% which skips retired segments and refuses a result that skips a slot), then
%% the cold tier's marks, from the files on the share. The prune then moves
%% the history back off local disk, and the dedupe rows into filters, as it
%% always does. `{ok, Report}' or `{error, Why}'.
restore(Opts, Target) ->
    Cfg = config(Opts),
    Dir = filename:join(maps:get(path, Cfg), "slots"),
    Procs = [ P || P <- names_in(Cfg, Dir), ?IS_ID(list_to_binary(P)) ],
    Ranges =
        lists:append(
            [ restore_process(Cfg, Target, list_to_binary(P), filename:join(Dir, P)) || P <- Procs ]),
    Journal =
        case irr(Opts) of
            Irr = #{ <<"store-module">> := hb_store_offsite_journal } ->
                hb_store_offsite_journal:restore([hb_store_offsite_journal:target(Irr)], Target);
            _ -> {ok, #{}}
        end,
    case Journal of
        {ok, Report} ->
            Marks =
                maps:from_list(lists:append(
                    [ [ {range_key(P, To), term_to_binary({From, To, Ms})},
                        {mark_key(<<"shipped">>, P), integer_to_binary(Top)},
                        {mark_key(<<"written">>, P), integer_to_binary(Top)} ]
                    || {P, From, To, Ms} <- Ranges,
                       Top <- [lists:max([ T || {Q, _, T, _} <- Ranges, Q =:= P ])] ])),
            ok = hb_store:write([Target], Marks, #{}),
            ok = hb_store:flush([Target], #{}),
            {ok, Report#{ range_files => length(Ranges) }};
        Error -> Error
    end.

restore_process(Cfg, Target, P, Dir) ->
    [ begin
          {ok, #{ process := P, from := From, to := To, newest_ms := Ms, rows := Rows }} =
              get_file(sized(Cfg, 64 * 1024 * 1024), filename:join(Dir, F)),
          Dedupe =
              [ {message_slot_path(P, ID), integer_to_binary(S)}
              || {K, V} <- Rows,
                 <<"~scheduler@1.0/slot-ids/", Tail/binary>> <- [K],
                 [P, SBin] <- [binary:split(Tail, <<"/">>)],
                 S <- [binary_to_integer(SBin)],
                 ID <- binary_to_term(V, [safe]) ],
          ok = hb_store:write([Target], maps:from_list(Rows ++ Dedupe), #{}),
          {P, From, To, Ms}
      end
    || F <- list_dir(Cfg, Dir),
       % The scheduler's own files; a replica's (tagged) are not its slots.
       length(string:split(filename:rootname(F), "-", all)) =:= 2 ].

names_in(Cfg, Dir) ->
    case io(Cfg, fun() -> hb_file:list_dir(Dir) end) of
        {ok, Names} -> Names;
        _ -> []
    end.

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
               [A, B | _] <- [string:split(filename:rootname(F), "-", all)],
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
            lists:any(fun(S) -> lists:member(S, Schedulers) end, signer_addresses(A, Opts))
        end}
    ],
    case [ Name || {Name, Check} <- Checks, (catch Check()) =/= true ] of
        [] -> true;
        Failed ->
            ?event(warning, {cold_slot_not_verified, P, Slot, Failed,
                {schedulers, (catch schedulers(P, Opts))},
                {signers, (catch signer_addresses(A, Opts))}}),
            false
    end.

%% @doc The addresses that signed a slot: each committer's, and, for an
%% ANS-104 signature (RSA or Ed25519, a node's `scheduler-key-type'), the
%% address of the key itself, which the `signature' check has verified. An
%% item decoded without its committer (one the committer device could not be
%% resolved for) still names the key that signed it. Nothing else names
%% anybody: an hmac or unsigned commitment has no key.
signer_addresses(A, Opts) ->
    Committers =
        [ Addr || S <- hb_message:signers(A, Opts), {ok, Addr} <- [hb_committer:address(S)] ],
    Keys =
        [ hb_util:human_id(ar_wallet:to_address(hb_util:decode(Owner), ar_tx:deserialize_sig_type(Type)))
        || #{ <<"type">> := Type, <<"owner">> := Owner } <-
               hb_maps:values(hb_maps:get(<<"commitments">>, A, #{}, Opts), Opts),
           lists:member(Type, [?RSA_SIGN_TYPE, ?EDDSA_SIGN_TYPE]) ],
    lists:usort(Committers ++ Keys).

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

%%% Reads by ID (a store)

%% @doc The cold tier is slow and may be away: never a local read.
scope(_StoreOpts) -> remote.

%% @doc Read an ID's row from the share: where the share's host says it is,
%% from that range file. `{ok, Value}' (a packed message, whose children read
%% by their IDs too), or `{error, not_found}'. Never throws.
read(_StoreOpts, #{ <<"read">> := Key }, NodeOpts) when is_binary(Key), byte_size(Key) =:= 43 ->
    case config(NodeOpts) of
        #{ verify := coldcheck, verify_url := Url } = Cfg when Url =/= undefined ->
            try by_id(Cfg, Key)
            catch _:Why ->
                ?event(cold_tier, {cold_read_by_id_failed, Key, Why}),
                {error, not_found}
            end;
        _ -> {error, not_found}
    end;
read(_StoreOpts, _Req, _NodeOpts) -> {error, not_found}.

by_id(Cfg, ID) ->
    case locate(Cfg, ID) of
        {ok, Rel} ->
            Rows = range_rows(Cfg, Rel),
            case lists:keyfind(ID, 1, Rows) of
                {ID, <<"ao:", Target/binary>>} ->
                    case lists:keyfind(Target, 1, Rows) of
                        {Target, V} -> {ok, V};
                        false -> {error, not_found}
                    end;
                {ID, V} -> {ok, V};
                false -> {error, not_found}
            end;
        not_found -> {error, not_found}
    end.

%% Where the share's host has `ID': the range file's path below this node's
%% folder, or `not_found'.
locate(#{ verify_url := Url, verify_prefix := Prefix } = Cfg, ID) ->
    Answer =
        io(Cfg, fun() ->
            hb_http_gate:httpc_request(get, {Url ++ "/locate?id=" ++ binary_to_list(ID), []},
                [{timeout, maps:get(io_timeout, Cfg)}], [{body_format, binary}], #{})
        end),
    case Answer of
        {ok, {{_, 200, _}, _, Json}} ->
            case maps:get(ID, hb_json:decode(Json), #{}) of
                #{ <<"path">> := Path } -> {ok, below_prefix(hb_util:list(Path), Prefix)};
                _ -> not_found
            end;
        _ -> not_found
    end.

below_prefix(Path, "") -> Path;
below_prefix(Path, Prefix) ->
    case lists:prefix(Prefix ++ "/", Path) of
        true -> lists:nthtail(length(Prefix) + 1, Path);
        false -> Path
    end.

%% A range file's rows: from the newest few read (an ETS table, its owner a
%% process of its own), or from the share.
-define(READ_CACHE, hb_store_cold_read_cache).
-define(READ_CACHE_FILES, 8).
range_rows(Cfg, Rel) ->
    Table = read_cache(),
    case ets:lookup(Table, Rel) of
        [{Rel, _, Rows}] -> Rows;
        [] ->
            {ok, #{ rows := Rows }} = get_file(sized(Cfg, 64 * 1024 * 1024),
                filename:join(maps:get(path, Cfg), Rel)),
            ets:insert(Table, {Rel, erlang:unique_integer([monotonic]), Rows}),
            case ets:info(Table, size) > ?READ_CACHE_FILES of
                true ->
                    [{_, Oldest} | _] = lists:keysort(2, [ {R, N} || {R, N, _} <- ets:tab2list(Table) ]),
                    ets:delete(Table, Oldest);
                false -> ok
            end,
            Rows
    end.

read_cache() ->
    case ets:whereis(?READ_CACHE) of
        undefined ->
            Self = self(),
            hb_name:singleton(?READ_CACHE, fun() ->
                catch ets:new(?READ_CACHE, [named_table, public, set, {read_concurrency, true}]),
                Self ! {?READ_CACHE, ready},
                receive stop -> ok end
            end),
            receive {?READ_CACHE, ready} -> ok after 1000 -> ok end,
            ?READ_CACHE;
        _ -> ?READ_CACHE
    end.

%%% Checkpoints

%% Ship, per process, its newest snapshot checkpoint when it is newer than
%% the last one shipped and at least `checkpoint-every-seconds' younger.
%% Only the newest is ever a candidate: retention always keeps it, and a link
%% that falls behind skips snapshots instead of queueing them. A snapshot is
%% derived (its process's messages rebuild it), so nothing local waits for
%% one to ship. `#{ done, bytes, failures }'.
ship_checkpoints(Cfg, Opts) ->
    case catch hb_store_retention:store(Opts) of
        Main = #{} ->
            Results =
                [ ship_newest(Cfg, Main, P) || P <- list_names(Main, <<"pstate">>), ?IS_ID(P) ],
            #{ done => length([ ok || {ok, _} <- Results ]),
               bytes => lists:sum([ B || {ok, B} <- Results ]),
               failures => [ F || {error, F} <- Results ] };
        _ -> #{}
    end.

ship_newest(Cfg, Main, P) ->
    Mark = read_int(Main, mark_key(<<"checkpoint">>, P), -1),
    case lists:reverse(pstate_slots(Main, P, <<"s">>)) of
        [S | _] when S > Mark ->
            Ms = case snap_ms(Main, P, S) of {ok, T} -> T; error -> erlang:system_time(millisecond) end,
            Last = read_int(Main, mark_key(<<"checkpoint-ms">>, P), -1),
            case Last < 0 orelse Ms - Last >= maps:get(checkpoint_every_ms, Cfg) of
                true ->
                    try
                        Bytes = ship_checkpoint(Cfg, Main, P, S, Ms),
                        thin_checkpoints(Cfg, P),
                        {ok, Bytes}
                    catch
                        throw:{cold_io, Why} -> {error, {P, Why}};
                        C:E -> {error, {P, {C, E}}}
                    end;
                false -> skip
            end;
        _ -> skip
    end.

%% One checkpoint file; the `checkpoint' mark is the newest shipped, and
%% `checkpoint-ms' its time.
ship_checkpoint(Cfg, Main, P, S, Ms) ->
    Rows = snapshot_rows(Main, P, S),
    Seq = read_int(Main, mark_key(<<"checkpoint-seq">>, P), -1) + 1,
    File = filename:join([maps:get(path, Cfg), "checkpoints", hb_util:list(P),
        pad(S) ++ "-" ++ integer_to_list(Ms) ++ "-" ++ integer_to_list(Seq) ++ ".hbc"]),
    {Bytes, _Hash} = put_file(Cfg, File, #{ process => P, slot => S, rows => Rows }),
    ok = hb_store:write([Main], #{ mark_key(<<"checkpoint">>, P) => integer_to_binary(S),
                                  mark_key(<<"checkpoint-seq">>, P) => integer_to_binary(Seq),
                                  mark_key(<<"checkpoint-ms">>, P) => integer_to_binary(Ms) }, #{}),
    ok = hb_store:flush([Main], #{}),
    Bytes.

%% On the share, checkpoints are thinned with age, never to none
%% (`thin_plan/3').
thin_checkpoints(Cfg, P) ->
    Dir = filename:join([maps:get(path, Cfg), "checkpoints", hb_util:list(P)]),
    Files = [ {Seq, S, Ms, F} || F <- list_dir(Cfg, Dir), {S, Ms, Seq} <- [checkpoint_name(F)] ],
    [ io(Cfg, fun() -> hb_file:delete(filename:join(Dir, F)) end)
    || F <- thin_plan(Files, erlang:system_time(millisecond), maps:get(thinning, Cfg)) ],
    ok.

%% The checkpoint files to delete. Each is in the band of the oldest
%% `{AgeMs, N}' it is at least `AgeMs' old for (none: younger than every
%% band, kept). In a band a checkpoint stays when its `seq' is a multiple of
%% `N'; and a band that would keep none keeps its newest, so no age is left
%% without a checkpoint to replay from. The ratios divide one another (5,
%% 25) in the default, so what a band keeps the next older keeps too.
thin_plan(Files, Now, Bands) ->
    Band =
        fun(Ms) ->
            case [ B || B = {Age, _} <- Bands, Now - Ms >= Age ] of
                [] -> none;
                In -> lists:last(In)
            end
        end,
    Grouped = maps:groups_from_list(fun({_, _, Ms, _}) -> Band(Ms) end, Files),
    lists:append(
        [ case [ X || X = {Seq, _, _, _} <- Members, Seq rem N =:= 0 ] of
              [] ->
                  {_, _, _, Newest} = lists:max(Members),
                  [ F || {_, _, _, F} <- Members, F =/= Newest ];
              Kept -> [ F || X = {_, _, _, F} <- Members, not lists:member(X, Kept) ]
          end
        || {{_, N}, Members} <- maps:to_list(Grouped) ]).

%% A checkpoint file's slot, time and sequence: `<slot>-<ms>-<seq>.hbc'.
checkpoint_name(F) ->
    case [ catch list_to_integer(X) || X <- string:split(filename:rootname(F), "-", all) ] of
        [S, Ms, Seq] when is_integer(S), is_integer(Ms), is_integer(Seq) -> {S, Ms, Seq};
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
    Read = fun(K) -> snapshot_row(Store, K) end,
    Rows = [ {K, Read(K)} || K <- Keys ],
    Chunks = lists:append([ snapshot_chunks(K, V, Read) || {K, V} <- Rows ]),
    case checkpoint_rows(P, S, Chunks ++ Rows) of
        {ok, _} -> Chunks ++ Rows;
        error -> erlang:error({invalid_checkpoint, P, S})
    end.

%% Each checkpoint row must be present before the file can be shipped.
snapshot_row(Store, Key) ->
    case hb_store:read([Store], #{ <<"read">> => Key, <<"exact">> => true }, #{}) of
        {ok, Value} when is_binary(Value) -> Value;
        _ -> erlang:error({incomplete_checkpoint, Key})
    end.

%% A chunked row names every part in order.
snapshot_chunks(Key, <<"pstate-chunks:", Digits/binary>>, Read) ->
    Count = binary_to_integer(Digits),
    true = Count > 0,
    snapshot_chunks(Key, Count, Read, []);
snapshot_chunks(_Key, _Value, _Read) -> [].

snapshot_chunks(_Key, 0, _Read, Rows) -> Rows;
snapshot_chunks(Key, N, Read, Rows) ->
    Part = <<Key/binary, ".", (integer_to_binary(N))/binary>>,
    snapshot_chunks(Key, N - 1, Read, [{Part, Read(Part)} | Rows]).

%% Accept just this checkpoint's marker, state and declared chunks. The
%% marker ties the full encoded state to the keys for this process and slot.
checkpoint_rows(P, S, Rows) ->
    try
        Map = maps:from_list(Rows),
        true = map_size(Map) =:= length(Rows),
        Read = fun(K) ->
            case maps:find(K, Map) of
                {ok, B} when is_binary(B) -> B;
                _ -> throw(invalid_checkpoint_row)
            end
        end,
        MarkerKey = blob_key(P, <<"s">>, S),
        StateKey = blob_key(P, <<"f">>, S),
        MarkerRow = Read(MarkerKey),
        StateRow = Read(StateKey),
        MarkerParts = snapshot_chunks(MarkerKey, MarkerRow, Read),
        StateParts = snapshot_chunks(StateKey, StateRow, Read),
        true = map_size(Map) =:= 2 + length(MarkerParts) + length(StateParts),
        Marker = iolist_to_binary(snapshot_payload(MarkerRow, MarkerParts)),
        State = snapshot_payload(StateRow, StateParts),
        true = snapshot_matches(Marker, State, S),
        {ok, Map}
    catch _:_ -> error
    end.

snapshot_payload(Row, []) -> Row;
snapshot_payload(_Row, Parts) -> [ B || {_, B} <- Parts ].

%% An empty or undigested legacy marker still requires a usable snapshot at
%% the requested slot. A digest-bearing marker covers the reassembled data.
snapshot_matches(<<>>, State, Slot) -> legacy_snapshot(State, Slot);
snapshot_matches(Marker, State, Slot) ->
    case binary_to_term(Marker, [safe]) of
        {1, #{ <<"state-digest">> := Digest }} ->
            crypto:hash(sha256, State) =:= Digest;
        {1, Fields} when is_map(Fields) -> legacy_snapshot(State, Slot);
        _ -> false
    end.

legacy_snapshot(State, Slot) ->
    case binary_to_term(iolist_to_binary(State), [safe]) of
        {1, #{ <<"at-slot">> := Slot, <<"snapshot">> := _ }} -> true;
        _ -> false
    end.

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
                           {S, _, _} <- [checkpoint_name(F)],
                           S > Floor, S =< Target ])),
                restore_first(Cfg, Main, P, Candidates)
            catch _:_ -> not_found
            end
    end.

restore_first(_Cfg, _Main, _P, []) -> not_found;
restore_first(Cfg, Main, P, [{S, File} | Rest]) ->
    case read_checkpoint(Cfg, P, S, File) of
        {ok, Rows} ->
            ok = hb_store:write([Main], Rows, #{}),
            ok = hb_store:flush([Main], #{}),
            {ok, S};
        error -> restore_first(Cfg, Main, P, Rest)
    end.

%% A corrupted file or one with rows from another checkpoint is a miss.
read_checkpoint(Cfg, P, S, File) ->
    try get_file(Cfg, File) of
        {ok, #{ process := P, slot := S, rows := Rows }} ->
            checkpoint_rows(P, S, Rows);
        _ -> error
    catch error:badarg -> error
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
                            case io(Cfg, fun() -> hb_file:is_file(File) end) of
                                true -> ok;
                                false ->
                                    _ = put_file(Cfg, File, #{ process => Pusd, slot => S,
                                        build => build(), rows => snapshot_rows(Computed, Pusd, S) }),
                                    Old = lists:nthtail(min(2, length(list_dir(Cfg, Dir))),
                                        lists:reverse(lists:sort(list_dir(Cfg, Dir)))),
                                    [ io(Cfg, fun() -> hb_file:delete(filename:join(Dir, F)) end) || F <- Old ]
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
                first_replica(Cfg, Computed, Pusd, Build, Candidates)
            catch _:_ -> not_found
            end
    end.

first_replica(_Cfg, _Computed, _P, _Build, []) -> not_found;
first_replica(Cfg, Computed, P, Build, [{S, File} | Rest]) ->
    Result =
        try get_file(Cfg, File) of
            {ok, #{ process := P, slot := S, build := Build, rows := Rows }} ->
                checkpoint_rows(P, S, Rows);
            _ -> error
        catch error:badarg -> error
        end,
    case Result of
        {ok, Verified} ->
            ok = hb_store:write([Computed], Verified, #{}),
            ok = hb_store:flush([Computed], #{}),
            {ok, S};
        error -> first_replica(Cfg, Computed, P, Build, Rest)
    end.

build() ->
    try file:consult(filename:join(code:priv_dir(hb), "hb_buildinfo")) of
        {ok, [#{ <<"source">> := Source }]} -> hb_util:bin(Source);
        _ -> <<"unknown">>
    catch _:_ -> <<"unknown">>
    end.

%%% Tests

-define(TEST_DIR, "/tmp/hb-cold-test").

test_env(Name) -> test_env(Name, ?RSA_KEY_TYPE).
test_env(Name, KeyType) ->
    Dir = ?TEST_DIR ++ "/" ++ Name ++ "-" ++ integer_to_list(erlang:unique_integer([positive])),
    os:cmd("rm -rf " ++ Dir),
    % 1 GiB maps, not the default 2 TiB: a VM's address space holds about 60
    % of those, and this module opens more.
    Main = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => list_to_binary(Dir ++ "/main"),
              <<"capacity">> => 1 bsl 30 },
    Irr = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => list_to_binary(Dir ++ "/irr"),
             <<"capacity">> => 1 bsl 30 },
    ok = hb_store:start([Main, Irr]),
    Sched = ar_wallet:new(KeyType),
    Opts = #{ <<"store">> => [Main, Irr#{ <<"access">> => [<<"read">>] }],
              <<"scheduler-store">> => [Irr], <<"priv-wallet">> => Sched,
              <<"match-index">> => false,
              <<"cold-tier">> => #{ <<"path">> => list_to_binary(Dir ++ "/share/me"),
                                    <<"hot-slots">> => 16,
                                    <<"hot-slots-floor">> => 16,
                                    <<"checkpoint-every-seconds">> => 0,
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
                <<"variant">> => <<"ao.N.1">>,
                <<"slot">> => N, <<"timestamp">> => erlang:system_time(millisecond) - AgeMs,
                <<"body">> => Body }, Opts#{ <<"cache-pack">> => true }, <<"ans104@1.0">>),
            WOpts = Opts#{ <<"store">> => [Irr], <<"cache-pack">> => true },
            {ok, _} = hb_cache:write(A, WOpts),
            ok = hb_cache:link(hb_message:id(A, signed, Opts), slot_path(P, N), WOpts),
            % The dedupe rows, as `dev_scheduler_cache:write_message_slot/4'.
            IDs = lists:sort([ hb_util:human_id(ID) || {ID, C} <- maps:to_list(
                maps:get(<<"commitments">>, Body)), is_map_key(<<"committer">>, C) ]),
            ok = hb_store:write([Irr], maps:from_list(
                [ {slot_ids_path(P, N), term_to_binary(IDs)} |
                  [ {message_slot_path(P, ID), integer_to_binary(N)} || ID <- IDs ] ]), #{}),
            persistent_term:put({?MODULE, test_ids, P, N}, IDs)
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
        % The newest `?LAG' are not shipped; the rest are, and lie below the
        % hot window (16), so are gone here. Slot 0 (the spawn) always stays.
        ?assert(local(Env, 0)),
        ?assertNot(local(Env, 1)),
        ?assertNot(local(Env, 23)),
        ?assert(local(Env, 24)),
        ?assert(local(Env, 39)),
        {ok, A} = read_assignment(P, 5, Opts),
        ?assertEqual(5, hb_util:int(hb_ao:get(<<"slot">>, A, Opts))),
        ?assert(hb_message:verify(A, all, Opts)),
        ?assertEqual(<<"5">>, hb_ao:get(<<"body/n">>, A, Opts)),
        ?assertMatch(#{ available := true, lag_slots := 16 }, status()),
        % A range inside the hot window is shipped and kept, whatever its age.
        assign(Env, lists:seq(40, 79), 0),
        Cold = maps:get(<<"cold-tier">>, Opts),
        Hot = fun(N) -> Opts#{ <<"cold-tier">> => Cold#{ <<"hot-slots">> => N } } end,
        ok = tick(Hot(40)),
        ?assert(local(Env, 50)),
        ?assertMatch({ok, _}, read_assignment(P, 50, Opts)),
        % Once the window passes it, it goes.
        ok = tick(Hot(16)),
        ?assertNot(local(Env, 50)),
        ?assert(local(Env, 64)),
        ?assertMatch({ok, _}, read_assignment(P, 10, Opts)),
        ?assertMatch({ok, _}, read_assignment(P, 50, Opts))
    end}.

%% @doc Under disk pressure the hot window shrinks to its floor.
pressure_shrinks_the_hot_window_test_() ->
    {timeout, 120, fun() ->
        Env = #{ opts := Opts0 } = test_env("pressure"),
        Cold = maps:get(<<"cold-tier">>, Opts0),
        Opts = Opts0#{ <<"cold-tier">> => Cold#{ <<"hot-slots">> => 1000 } },
        assign(Env, lists:seq(0, 59), 0),
        ok = tick(Opts),
        ?assert(local(Env, 10)),
        try
            hb_store_budget:override(soft),
            ok = tick(Opts),
            ?assertNot(local(Env, 10)),
            ?assert(local(Env, 44)),
            ?assertMatch(#{ level := soft, phases := #{ checkpoints := _ } }, status())
        after
            hb_store_budget:override(undefined)
        end
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

%% @doc Shipping to the share and reading back from it never use the node's
%% one `file_server_2': a hung share held it, and with it every file call of
%% the node (2026-11-23). With the server suspended, a tick and a read work.
does_not_use_the_file_server_test_() ->
    {timeout, 120, fun() ->
        Env = #{ opts := Opts, p := P } = test_env("no-fs"),
        assign(Env, lists:seq(0, 39), 3600000),
        {ok, {Tick, Read}} = hb_file:without_file_server(
            fun() -> {tick(Opts), read_assignment(P, 5, Opts)} end, 60000),
        ?assertEqual(ok, Tick),
        ?assertNot(local(Env, 5)),
        {ok, A} = Read,
        ?assertEqual(5, hb_util:int(hb_ao:get(<<"slot">>, A, Opts)))
    end}.

%% @doc A slot read from another node's folder is used only if it verifies
%% and the process's scheduler signed it.
peer_slots_verified_test_() ->
    {timeout, 120, fun() -> peer_slots_verified(?RSA_KEY_TYPE) end}.

%% @doc The same with an Ed25519 scheduling key (`scheduler-key-type'): the
%% key the process names is the key that signed, and a slot another Ed25519
%% key signed is refused.
peer_slots_verified_ed25519_test_() ->
    {timeout, 120, fun() -> peer_slots_verified(?EDDSA_KEY_TYPE) end}.

peer_slots_verified(KeyType) ->
        Peer = #{ opts := POpts, p := P } = test_env("peer", KeyType),
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
        Forged = (test_env("forger", KeyType))#{ p => P },
        assign(Forged, lists:seq(0, 19), 3600000),
        #{ opts := FOpts } = Forged,
        ok = tick(FOpts),
        MOpts2 = MOpts0#{ <<"cold-tier">> => Cold#{ <<"peers">> =>
            [#{ <<"node">> => <<"forger">>, <<"path">> => list_to_binary(share(FOpts)) }] } },
        ?assertEqual(not_found, read_assignment(P, 2, MOpts2)).


%% @doc The newest checkpoint is shipped when it is due; one is put back where
%% none is held, from this node's folder or a trusted peer's only.
checkpoints_test_() ->
    {timeout, 120, fun() ->
        #{ main := Main, opts := Opts } = test_env("ckpt"),
        Q = hb_util:human_id(crypto:strong_rand_bytes(32)),
        Day = ?DAY_MS, Now = erlang:system_time(millisecond),
        Marker = fun(Ms) -> term_to_binary({1, #{ <<"timestamp">> => Ms }}) end,
        State20 = test_snapshot(20, <<"state-20">>),
        State30 = test_snapshot(30, crypto:strong_rand_bytes(6010)),
        <<Part1:4000/binary, Part2/binary>> = State30,
        Share = share(Opts),
        Files = fun() -> filelib:wildcard(Share ++ "/checkpoints/" ++ binary_to_list(Q) ++ "/*.hbc") end,
        Write = fun(Rows) -> ok = hb_store:write([Main], Rows, #{}), ok = hb_store:flush([Main], #{}) end,
        % Two snapshots at once: only the newest is a candidate.
        Write(#{ blob_key(Q, <<"s">>, 5) => Marker(Now - 3 * Day),
                 blob_key(Q, <<"f">>, 5) => test_snapshot(5, <<"state-5">>),
                 blob_key(Q, <<"s">>, 10) => Marker(Now - 2 * Day),
                 blob_key(Q, <<"f">>, 10) => test_snapshot(10, <<"state-10">>) }),
        ok = tick(Opts),
        ?assertEqual(1, length(Files())),
        Write(#{ blob_key(Q, <<"s">>, 20) => Marker(Now - 2 * Day + 1000),
                 blob_key(Q, <<"f">>, 20) => State20 }),
        ok = tick(Opts),
        Write(#{ blob_key(Q, <<"s">>, 30) => Marker(Now),
                 blob_key(Q, <<"f">>, 30) => <<"pstate-chunks:2">>,
                 <<(blob_key(Q, <<"f">>, 30))/binary, ".1">> => Part1,
                 <<(blob_key(Q, <<"f">>, 30))/binary, ".2">> => Part2 }),
        ok = tick(Opts),
        ?assertEqual(3, length(Files())),
        % Not yet due: a snapshot an hour or less after the last one shipped.
        ColdH = maps:get(<<"cold-tier">>, Opts),
        Hourly = Opts#{ <<"cold-tier">> => ColdH#{ <<"checkpoint-every-seconds">> => 3600 } },
        Write(#{ blob_key(Q, <<"s">>, 40) => Marker(Now + 1000),
                 blob_key(Q, <<"f">>, 40) => test_snapshot(40, <<"state-40">>) }),
        ok = tick(Hourly),
        ?assertEqual(3, length(Files())),
        #{ <<"db">> := DB0 } = hb_store:find(Main),
        {ok, _} = elmdb:delete_batch(DB0, [ blob_key(Q, K, S) || K <- [<<"s">>, <<"f">>], S <- [5, 40] ]),
        #{ <<"db">> := DB } = hb_store:find(Main),
        {ok, _} = elmdb:delete_batch(DB, [ blob_key(Q, K, S) || K <- [<<"s">>, <<"f">>], S <- [10, 20, 30] ]),
        ?assertEqual({ok, 20}, restore_checkpoint(Q, 25, Opts)),
        ?assertEqual({ok, State20}, hb_store:read([Main], blob_key(Q, <<"f">>, 20), #{})),
        % Thinned as the next one ships. Past one day, 1 in 2: of 10 (seq 0)
        % and 20 (seq 1), 10 stays; 30 and 50 are younger and stay.
        Cold0 = maps:get(<<"cold-tier">>, Opts),
        Write(#{ blob_key(Q, <<"s">>, 50) => Marker(Now + 2000),
                 blob_key(Q, <<"f">>, 50) => test_snapshot(50, <<"state-50">>) }),
        ok = tick(Opts#{ <<"cold-tier">> => Cold0#{ <<"checkpoint-thinning">> => [[1, 2]] } }),
        ?assertEqual([10, 30, 50], lists:sort([ S || F <- filelib:wildcard(Share ++ "/checkpoints/"
            ++ binary_to_list(Q) ++ "/*.hbc"), {S, _, _} <- [checkpoint_name(filename:basename(F))] ])),
        % From a peer only when trusted.
        #{ opts := ROpts0 } = test_env("ckpt-reader"),
        Cold = maps:get(<<"cold-tier">>, ROpts0),
        Peer = fun(T) -> ROpts0#{ <<"cold-tier">> => Cold#{ <<"peers">> =>
            [#{ <<"node">> => <<"p">>, <<"path">> => list_to_binary(Share), <<"trusted">> => T }] } } end,
        ?assertEqual(not_found, restore_checkpoint(Q, 100, Peer(false))),
        ?assertEqual({ok, 30}, restore_checkpoint(Q, 45, Peer(true))),
        ?assertEqual({ok, 50}, restore_checkpoint(Q, 100, Peer(true)))
    end}.

%% @doc A newest snapshot with a row missing or damaged is not shipped until
%% it is whole. After it is repaired, restore reproduces the
%% complete rows of either form of checkpoint.
incomplete_checkpoints_stop_shipping_test_() ->
    [ {atom_to_list(Missing), {timeout, 120, fun() -> incomplete_checkpoint(Missing) end}}
    || Missing <- [full_state, chunk, digest_mismatch] ].

incomplete_checkpoint(Missing) ->
    #{ main := Main, opts := Opts, p := P } = test_env("incomplete-" ++ atom_to_list(Missing)),
    Cfg = config(Opts),
    Now = erlang:system_time(millisecond),
    S = fun(N) -> blob_key(P, <<"s">>, N) end,
    F = fun(N) -> blob_key(P, <<"f">>, N) end,
    State10 = test_snapshot(10, <<"small state">>),
    State20 = test_snapshot(20, <<"small state">>),
    State30 = test_snapshot(30, <<"small state">>),
    Large = test_snapshot(20, crypto:strong_rand_bytes(6000)),
    <<Part1:4000/binary, Part2/binary>> = Large,
    Marker = fun(State) ->
        term_to_binary({1, #{
            <<"timestamp">> => Now, <<"build">> => <<"test">>,
            <<"state-digest">> => crypto:hash(sha256, State) }}, [compressed])
    end,
    ok = hb_store:write([Main], #{ (S(10)) => Marker(State10), (F(10)) => State10 }, #{}),
    ok = hb_store:flush([Main], #{}),
    ok = tick(Opts),
    Mark = mark_key(<<"checkpoint">>, P),
    Seq = mark_key(<<"checkpoint-seq">>, P),
    F20 = F(20),
    Chunk1 = <<F20/binary, ".1">>,
    Chunk2 = <<F20/binary, ".2">>,
    {Rows, Repaired} =
        case Missing of
            full_state -> {#{ (S(20)) => Marker(State20) }, #{ F20 => State20 }};
            chunk ->
                {#{ (S(20)) => Marker(Large), F20 => <<"pstate-chunks:2">>,
                    Chunk1 => Part1 }, #{ Chunk2 => Part2 }};
            digest_mismatch ->
                {#{ (S(20)) => Marker(Large), F20 => <<"pstate-chunks:2">>,
                    Chunk1 => Part1, Chunk2 => <<"damaged">> }, #{ Chunk2 => Part2 }}
        end,
    ok = hb_store:write([Main], Rows, #{}),
    ok = hb_store:flush([Main], #{}),
    Dir = filename:join([share(Opts), "checkpoints", hb_util:list(P)]),
    Result = tick(Opts),
    ?assertMatch({{error, _}, 10, 0, 1},
        {Result, read_int(Main, Mark, -1), read_int(Main, Seq, -1),
            length(list_dir(Cfg, Dir))}),
    ok = hb_store:write([Main], Repaired, #{}),
    ok = hb_store:flush([Main], #{}),
    ok = tick(Opts),
    ?assertEqual({20, 1, 2},
        {read_int(Main, Mark, -1), read_int(Main, Seq, -1), length(list_dir(Cfg, Dir))}),
    ok = hb_store:write([Main], #{ (S(30)) => Marker(State30), (F(30)) => State30 }, #{}),
    ok = hb_store:flush([Main], #{}),
    ok = tick(Opts),
    ?assertEqual({30, 2, 3},
        {read_int(Main, Mark, -1), read_int(Main, Seq, -1), length(list_dir(Cfg, Dir))}),
    Expected = maps:merge(Rows, Repaired),
    Keys = maps:keys(Expected),
    #{ <<"db">> := DB } = hb_store:find(Main),
    {ok, _} = elmdb:delete_batch(DB, Keys),
    ?assertEqual({ok, 20}, restore_checkpoint(P, 20, Opts)),
    ?assertEqual(Expected, maps:from_list(
        [ {K, V} || K <- Keys,
            {ok, V} <- [hb_store:read([Main], #{ <<"read">> => K, <<"exact">> => true }, #{})] ])).

%% @doc Restore skips invalid archives without writing any of their rows.
checkpoint_restore_validates_payload_test_() ->
    {timeout, 60, fun() ->
        #{ main := Main, opts := Opts, p := P } = test_env("restore-verified"),
        Cfg = config(Opts),
        Root = filename:join([share(Opts), "checkpoints", hb_util:list(P)]),
        Marker = fun(B) -> term_to_binary({1, #{
            <<"state-digest">> => crypto:hash(sha256, B) }}) end,
        RowsAt = fun(Slot, State, Mark) ->
            [{blob_key(P, <<"s">>, Slot), Mark}, {blob_key(P, <<"f">>, Slot), State}]
        end,
        S5 = test_snapshot(5, <<"legacy">>),
        L5 = filename:join(Root, pad(5) ++ "-0-0.hbc"),
        S10 = test_snapshot(10, crypto:strong_rand_bytes(6000)),
        <<Part1:4000/binary, Part2/binary>> = S10,
        F10 = blob_key(P, <<"f">>, 10),
        K1 = <<F10/binary, ".1">>,
        K2 = <<F10/binary, ".2">>,
        Good = [{K1, Part1}, {K2, Part2} | RowsAt(10, <<"pstate-chunks:2">>, Marker(S10))],
        Archive = #{ process => P, slot => 10, rows => Good },
        Other = hb_util:human_id(crypto:strong_rand_bytes(32)),
        ExtraKey = blob_key(Other, <<"f">>, 10),
        L10 = filename:join(Root, pad(10) ++ "-0-1.hbc"),
        Invalid = [
            Archive#{ process => Other },
            Archive#{ slot => 5 },
            Archive#{ rows => lists:keydelete(K2, 1, Good) },
            Archive#{ rows => lists:keyreplace(K2, 1, Good, {K2, <<0>>}) },
            Archive#{ rows => [{ExtraKey, S10} | Good] },
            Archive#{ rows => [hd(Good) | Good] },
            Archive#{ rows => RowsAt(10, S5, <<>>) }
        ],
        lists:foreach(
            fun(Unusable) ->
                _ = put_file(Cfg, L10, Unusable),
                ?assertEqual(not_found, restore_checkpoint(P, 10, Opts)),
                ?assertEqual({error, not_found}, hb_store:read([Main], F10, #{})),
                ?assertEqual({error, not_found}, hb_store:read([Main], ExtraKey, #{}))
            end,
            Invalid),
        % Even an intact HBC hash cannot make an unusable checkpoint a base.
        _ = put_file(Cfg, L5, #{ process => P, slot => 5, rows => RowsAt(5, S5, <<>>) }),
        ?assertEqual({ok, 5}, restore_checkpoint(P, 10, Opts)),
        ?assertEqual({ok, S5}, hb_store:read([Main], blob_key(P, <<"f">>, 5), #{})),
        _ = put_file(Cfg, L10, Archive),
        ?assertEqual({ok, 10}, restore_checkpoint(P, 10, Opts)),
        ?assertEqual(maps:from_list(Good), maps:from_list(
            [ {K, V} || {K, _} <- Good, {ok, V} <- [hb_store:read([Main], K, #{})] ]))
    end}.

test_snapshot(Slot, Snapshot) ->
    term_to_binary({1, #{ <<"at-slot">> => Slot, <<"snapshot">> => Snapshot }},
        [{compressed, 1}]).

%% @doc Thinning never leaves an age band without a checkpoint, keeps every
%% `N'th in each band, and is stable: thinning what it kept deletes nothing.
%% Two years of checkpoints at uneven intervals, and bands too sparse for
%% their ratio.
thinning_never_empties_a_band_test() ->
    Day = ?DAY_MS, Now = 1000 * Day,
    Bands = thinning([[30, 5], [365, 25]]),
    Check =
        fun(Files) ->
            Doomed = thin_plan(Files, Now, Bands),
            Kept = [ X || X = {_, _, _, F} <- Files, not lists:member(F, Doomed) ],
            BandOf = fun(Ms) -> Age = Now - Ms,
                         if Age >= 365 * Day -> old; Age >= 30 * Day -> mid; true -> new end end,
            [ ?assert(lists:any(fun({_, _, Ms, _}) -> BandOf(Ms) =:= B end, Kept))
            || B <- lists:usort([ BandOf(Ms) || {_, _, Ms, _} <- Files ]) ],
            % Every checkpoint younger than 30 days stays.
            ?assertEqual([ X || X = {_, _, Ms, _} <- Files, BandOf(Ms) =:= new ],
                         [ X || X = {_, _, Ms, _} <- Kept, BandOf(Ms) =:= new ]),
            % No gap of kept sequence numbers wider than the band's ratio,
            % where the band had enough to keep.
            [ ?assert(B - A =< case BandOf(MsB) of old -> 25; mid -> 5; new -> 1 end)
            || {{A, _, MsA, _}, {B, _, MsB, _}} <- lists:zip(lists:droplast(lists:sort(Kept)), tl(lists:sort(Kept))),
               BandOf(MsA) =:= BandOf(MsB) ],
            ?assertEqual([], thin_plan(Kept, Now, Bands)),
            Kept
        end,
    Name = fun(Seq, Ms) -> {Seq, Seq * 10, Ms, integer_to_list(Seq) ++ ".hbc"} end,
    % Dense: one every 6 hours for two years.
    Check([ Name(Seq, Now - (2920 - Seq) * Day div 4) || Seq <- lists:seq(0, 2920) ]),
    % Sparse: 3 checkpoints over a year old (seqs 1-3, none a multiple of
    % 25), 4 between 30 days and a year (none a multiple of 5), 2 recent.
    Check([ Name(Seq, Ms) || {Seq, Ms} <- [{1, Now - 700 * Day}, {2, Now - 600 * Day},
        {3, Now - 400 * Day}, {6, Now - 300 * Day}, {7, Now - 200 * Day}, {8, Now - 100 * Day},
        {9, Now - 40 * Day}, {11, Now - 10 * Day}, {12, Now - Day}] ]),
    ok.

%% @doc A replica snapshot is shipped and seeds an empty computed store of
%% the same build; one of another build is never used.
replica_snapshot_test_() ->
    {timeout, 60, fun() ->
        #{ main := Computed, opts := Opts, dir := Dir } = test_env("replica"),
        Pusd = hb_util:human_id(crypto:strong_rand_bytes(32)),
        Head = test_snapshot(500, <<"head">>),
        ok = hb_store:write([Computed], #{
            blob_key(Pusd, <<"s">>, 500) => term_to_binary({1, #{}}),
            blob_key(Pusd, <<"f">>, 500) => Head }, #{}),
        ok = ship_replica(Pusd, Computed, Opts),
        File = share(Opts) ++ "/replica/" ++ binary_to_list(Pusd) ++ "/" ++ pad(500) ++ ".hbc",
        ?assert(lists:any(fun(_) -> timer:sleep(100), filelib:is_file(File) end, lists:seq(1, 50))),
        Fresh = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => list_to_binary(Dir ++ "/fresh") },
        ok = hb_store:start(Fresh),
        ?assertEqual({ok, 500}, restore_replica(Pusd, Fresh, Opts, -1)),
        ?assertEqual({ok, Head}, hb_store:read([Fresh], blob_key(Pusd, <<"f">>, 500), #{})),
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
    {timeout, 60, fun() -> arweave_fallback(?RSA_KEY_TYPE) end}.

%% @doc The same window, signed by an Ed25519 scheduling key.
arweave_fallback_ed25519_test_() ->
    {timeout, 60, fun() -> arweave_fallback(?EDDSA_KEY_TYPE) end}.

arweave_fallback(KeyType) ->
        #{ opts := Opts0, p := P, sched := Sched } = test_env("arweave", KeyType),
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
        ?assertEqual(not_found, read_assignment(P, 8, Opts)).


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

%% @doc An Ed25519 assignment whose commitment lost its committer is still the
%% scheduler's: its verified key names it. One signed by another Ed25519 key,
%% or carrying only an hmac, names nobody the process lists.
signer_addresses_test() ->
    Sched = ar_wallet:new(?EDDSA_KEY_TYPE),
    Addr = hb_util:human_id(ar_wallet:to_address(Sched)),
    A = hb_message:commit(#{ <<"type">> => <<"Assignment">>, <<"slot">> => 1 },
        #{ <<"priv-wallet">> => Sched }, <<"ans104@1.0">>),
    ?assertEqual([Addr], signer_addresses(A, #{})),
    Bare = A#{ <<"commitments">> => maps:map(fun(_, C) -> maps:remove(<<"committer">>, C) end,
        maps:get(<<"commitments">>, A)) },
    ?assertEqual([], hb_message:signers(Bare, #{})),
    ?assertEqual([Addr], signer_addresses(Bare, #{})),
    Other = hb_message:commit(#{ <<"type">> => <<"Assignment">>, <<"slot">> => 1 },
        #{ <<"priv-wallet">> => ar_wallet:new(?EDDSA_KEY_TYPE) }, <<"ans104@1.0">>),
    ?assertNot(lists:member(Addr, signer_addresses(Other, #{}))),
    Hmac = #{ <<"commitments">> => #{ <<"h">> => #{ <<"type">> => <<"hmac-sha256">>,
        <<"owner">> => hb_util:encode(maps:get(<<"owner">>, Sched)) } } },
    ?assertEqual([], signer_addresses(Hmac, #{})).

%%% Ownership, verification and crashes

%% Write slot 0 of the environment's process as the scheduler does: the
%% definition, then its assignment packed, which links the process's own ID
%% to slot 0's row.
assign_spawn(#{ irr := Irr, opts := Opts, p := P }) ->
    {ok, Proc} = hb_cache:read(P, Opts),
    A = hb_message:commit(#{ <<"type">> => <<"Assignment">>, <<"process">> => P,
        <<"variant">> => <<"ao.N.1">>, <<"slot">> => 0,
        <<"timestamp">> => erlang:system_time(millisecond) - 3600000, <<"body">> => Proc },
        Opts#{ <<"cache-pack">> => true }, <<"ans104@1.0">>),
    WOpts = Opts#{ <<"store">> => [Irr], <<"cache-pack">> => true },
    {ok, _} = hb_cache:write(Proc, Opts#{ <<"store">> => [Irr] }),
    {ok, _} = hb_cache:write(A, WOpts),
    ok = hb_cache:link(hb_message:id(A, signed, Opts), slot_path(P, 0), WOpts),
    ok = hb_store:flush([Irr], #{}).

%% A second process in the same environment.
another_process(Env = #{ main := Main, opts := Opts, sched := Sched }) ->
    Proc = hb_message:commit(#{ <<"type">> => <<"Process">>,
        <<"scheduler">> => hb_util:human_id(ar_wallet:to_address(Sched)),
        <<"test-seed">> => integer_to_binary(erlang:unique_integer()) }, Opts, <<"ans104@1.0">>),
    {ok, _} = hb_cache:write(Proc, Opts#{ <<"store">> => [Main] }),
    Env#{ p => hb_message:id(Proc, signed, Opts) }.

raw(Store, Key) ->
    case hb_store:read([Store], #{ <<"read">> => Key, <<"exact">> => true }, #{}) of
        {ok, V} -> {ok, V};
        _ -> not_found
    end.

%% Every range row of the process names a file on the share that holds
%% every slot of the range.
ranges_hold(Env, Opts) -> ranges_hold(Env, Opts, "").
ranges_hold(#{ irr := Irr, p := P }, Opts, Tag) ->
    Cfg = (config(Opts))#{ tag => Tag },
    lists:foreach(
        fun(Name) ->
            {ok, V} = raw(Irr, <<(range_prefix(P))/binary, "/", Name/binary>>),
            {From, To, _} = binary_to_term(V),
            {ok, #{ from := From, to := To, rows := Rows }} =
                get_file(Cfg, range_file(Cfg, P, From, To)),
            Keys = [ K || {K, _} <- Rows ],
            [ ?assert(lists:member(slot_path(P, S), Keys)) || S <- lists:seq(From, To) ]
        end,
        list_names(Irr, range_prefix(P))).

%% Every slot of the process up to `Newest' is held locally or read back,
%% verified, from the share.
nothing_lost(Env = #{ irr := Irr, p := P }, Opts, Newest) ->
    [ ?assert(local(Env, S) orelse element(1, read_assignment(P, S, Opts)) =:= ok)
    || S <- lists:seq(0, Newest) ],
    % And no slot's message could be assigned again: its dedupe row is here,
    % or the filters and the share find it.
    [ ?assert(read_int(Irr, message_slot_path(P, ID), -1) =:= S orelse
              seen(Irr, P, [ID], Opts) =:= {ok, S, ID})
    || S <- lists:seq(0, Newest), ID <- persistent_term:get({?MODULE, test_ids, P, S}, []) ],
    ranges_hold(Env, Opts).

%% @doc A prune deletes only the rows its slots own: never slot 0, never the
%% process's own ID (which links to slot 0's row), never a row a slot's
%% closure reaches that links somewhere else.
prune_takes_only_owned_rows_test_() ->
    {timeout, 120, fun() ->
        Env = #{ irr := Irr, opts := Opts, p := P } = test_env("owned"),
        assign_spawn(Env),
        assign(Env, lists:seq(1, 39), 3600000),
        IDRow = raw(Irr, P),
        ?assertMatch({ok, <<"ao:", _/binary>>}, IDRow),
        Rows5 = closure(Irr, slot_path(P, 5)),
        [Packed5] = [ K || {K, <<?PACKED, _/binary>>} <- Rows5 ],
        % One of its message's IDs (not the assignment's own, which the slot
        % path links through).
        {ok, <<"ao:", Signed5/binary>>} = raw(Irr, slot_path(P, 5)),
        [Child | _] = [ K || {K, V} <- Rows5, V =:= <<"ao:", Packed5/binary>>, K =/= Signed5 ],
        Other = hb_util:human_id(crypto:strong_rand_bytes(32)),
        ok = hb_store:write([Irr], #{ Child => <<"ao:", Other/binary>>, Other => <<"kept">> }, #{}),
        ok = hb_store:flush([Irr], #{}),
        ok = tick(Opts),
        ?assertNot(local(Env, 5)),
        ?assertEqual(not_found, raw(Irr, Packed5)),
        ?assertEqual({ok, <<"ao:", Other/binary>>}, raw(Irr, Child)),
        ?assertEqual({ok, <<"kept">>}, raw(Irr, Other)),
        ?assert(local(Env, 0)),
        ?assertEqual(IDRow, raw(Irr, P)),
        ?assertMatch({ok, _}, hb_cache:read(P, Opts#{ <<"store">> => [Irr] }))
    end}.

%% A stub of the share host's `coldcheck' service (`verify-url') over the
%% share folder `Share': it reads each file asked for and checks its header's
%% digest, except that it reports `{"ok": false, "error": "digest"}' for
%% every path in the ETS table `Lies'.
stub_coldcheck(Share, Lies) ->
    stub_gateway(fun(_Method, Path) ->
        [_, Query] = binary:split(Path, <<"?">>),
        Rels = [ R || {<<"path">>, R} <- uri_string:dissect_query(Query) ],
        Verdict =
            fun(Rel) ->
                case {ets:member(Lies, Rel), file:read_file(filename:join(Share, Rel))} of
                    {true, _} -> #{ <<"ok">> => false, <<"error">> => <<"digest">> };
                    {false, {ok, <<"HBC1", D:32/binary, Body/binary>>}} ->
                        case crypto:hash(sha256, Body) of
                            D -> #{ <<"ok">> => true, <<"sha256">> => binary:encode_hex(D, lowercase) };
                            _ -> #{ <<"ok">> => false, <<"error">> => <<"digest">> }
                        end;
                    {false, _} -> #{ <<"ok">> => false, <<"error">> => <<"No such file or directory">> }
                end
            end,
        {200, hb_json:encode(maps:from_list([ {R, Verdict(R)} || R <- Rels ]))}
    end).

%% @doc With `verify' `coldcheck', a range is deleted only once the share's
%% host has read back the digest the node wrote. Without a host to ask
%% nothing is deleted; a host that reads other bytes sends the range back to
%% be written again.
coldcheck_verify_test_() ->
    {timeout, 120, fun() ->
        Env = #{ opts := Opts0, p := P } = test_env("coldcheck"),
        Cold = maps:get(<<"cold-tier">>, Opts0),
        Share = share(Opts0),
        Lies = ets:new(coldcheck_lies, [public, set]),
        With = fun(Extra) -> Opts0#{ <<"cold-tier">> => maps:merge(Cold#{
            <<"verify">> => <<"coldcheck">>, <<"range-slots">> => 10 }, Extra) } end,
        assign(Env, lists:seq(0, 39), 3600000),
        % No host to ask: written, never proven, never deleted.
        ok = tick(With(#{})),
        ?assert(local(Env, 5)),
        ?assertMatch(#{ last_failures := [{_, no_verify_url} | _] }, status()),
        % The host answers for the folder above this node's (`verify-prefix').
        Opts = With(#{ <<"verify-url">> => stub_coldcheck(filename:dirname(Share), Lies),
                       <<"verify-prefix">> => list_to_binary(filename:basename(Share)) }),
        Rel2 = list_to_binary(filename:join(filename:basename(Share), range_rel(#{}, P, 10, 19))),
        ets:insert(Lies, {Rel2}),
        ok = tick(Opts),
        ?assertNot(local(Env, 5)),
        ?assert(local(Env, 15)),
        ets:delete(Lies, Rel2),
        ok = tick(Opts),
        ?assertNot(local(Env, 15)),
        ?assertNot(local(Env, 23)),
        ?assert(local(Env, 24)),
        ?assertMatch({ok, _}, read_assignment(P, 15, Opts)),
        ranges_hold(Env, Opts)
    end}.

%% @doc One process whose files cannot be written does not hold up another:
%% the round succeeds for the rest, names the failure, and the stuck process
%% catches up once it can.
one_failure_is_one_item_test_() ->
    {timeout, 120, fun() ->
        Env1 = #{ opts := Opts, p := P1 } = test_env("one-failure"),
        Env2 = #{ p := P2 } = another_process(Env1),
        assign(Env1, lists:seq(0, 39), 3600000),
        assign(Env2, lists:seq(0, 39), 3600000),
        Blocked = share(Opts) ++ "/slots/" ++ binary_to_list(P2),
        ok = filelib:ensure_dir(Blocked),
        ok = file:write_file(Blocked, <<"not a folder">>),
        ok = tick(Opts),
        ?assertNot(local(Env1, 5)),
        ?assert(local(Env2, 5)),
        #{ last_failures := Failures } = status(),
        ?assert(lists:keymember(P2, 1, Failures)),
        ?assertNot(lists:keymember(P1, 1, Failures)),
        ok = file:delete(Blocked),
        ok = tick(Opts),
        ?assertNot(local(Env2, 5)),
        ?assertMatch({ok, _}, read_assignment(P2, 5, Opts))
    end}.

%% @doc A round killed at any moment -- mid-file, between a file and its
%% mark, between a range row and its delete, mid-delete -- loses nothing and
%% deletes nothing twice: after every kill each slot is local or on the
%% share and every range row's file holds its slots; a clean round finishes
%% the job, and a round after that has nothing left to delete.
killed_rounds_lose_nothing_test_() ->
    {timeout, 600, fun() ->
        Env = #{ opts := Opts0 } = test_env("killed"),
        Cold = maps:get(<<"cold-tier">>, Opts0),
        Opts = Opts0#{ <<"cold-tier">> =>
            Cold#{ <<"range-slots">> => 3, <<"ranges-per-round">> => 50 } },
        Delays = [0, 1, 2, 3, 5, 8, 13, 20, 30, 50, 80, 130, 200, 320],
        Newest =
            lists:foldl(
                fun(Delay, Next) ->
                    assign(Env, lists:seq(Next, Next + 9), 3600000),
                    Pid = spawn(fun() -> tick(Opts) end),
                    timer:sleep(Delay),
                    exit(Pid, kill),
                    nothing_lost(Env, Opts, Next + 9),
                    Next + 10
                end,
                0, Delays) - 1,
        ok = tick(Opts),
        nothing_lost(Env, Opts, Newest),
        [ ?assertNot(local(Env, S)) || S <- lists:seq(1, Newest - 16) ],
        [ ?assert(local(Env, S)) || S <- [0 | lists:seq(Newest - 15, Newest)] ],
        Pruned = maps:get(pruned_slots, status()),
        ok = tick(Opts),
        ?assertEqual(Pruned, maps:get(pruned_slots, status()))
    end}.

%% @doc The dedupe rows of a pruned slot move into filters in the same
%% transaction: the rows are gone, every one of the slot's IDs is still found
%% (its file read back from the share), a new ID is not, and with the share
%% gone an ID the filters cannot rule out is `unavailable', never `not_found'.
dedupe_moves_to_filters_test_() ->
    {timeout, 120, fun() ->
        Env = #{ irr := Irr, opts := Opts, p := P } = test_env("dedupe"),
        assign(Env, lists:seq(0, 39), 3600000),
        ok = tick(Opts),
        ?assertNot(local(Env, 5)),
        [ID5] = persistent_term:get({?MODULE, test_ids, P, 5}),
        ?assertEqual(not_found, raw(Irr, message_slot_path(P, ID5))),
        ?assertEqual(not_found, raw(Irr, slot_ids_path(P, 5))),
        ?assertEqual({ok, 5, ID5}, seen(Irr, P, [ID5], Opts)),
        New = hb_util:human_id(crypto:strong_rand_bytes(32)),
        ?assertEqual(not_found, seen(Irr, P, [New], Opts)),
        % A slot still in the hot window keeps its row.
        [ID30] = persistent_term:get({?MODULE, test_ids, P, 30}),
        ?assertEqual({ok, <<"30">>}, raw(Irr, message_slot_path(P, ID30))),
        Share = share(Opts),
        os:cmd("mv " ++ Share ++ " " ++ Share ++ ".away"),
        ?assertMatch({unavailable, _}, seen(Irr, P, [ID5], Opts)),
        ?assertEqual(not_found, seen(Irr, P, [New], Opts)),
        os:cmd("mv " ++ Share ++ ".away " ++ Share),
        ?assertEqual({ok, 5, ID5}, seen(Irr, P, [ID5], Opts))
    end}.

%% A stub of the share host's `/locate' over the share folder `Share': it
%% scans the `.ids' sidecars for the ID asked.
stub_locate(Share, Prefix) ->
    stub_gateway(fun(_Method, Path) ->
        [_, Query] = binary:split(Path, <<"?">>),
        [{<<"id">>, ID}] = uri_string:dissect_query(Query),
        Found =
            [ F || F <- filelib:wildcard(Share ++ "/slots/*/*.ids"),
                   {ok, B} <- [file:read_file(F)],
                   lists:member(ID, binary:split(B, <<"\n">>, [global])) ],
        Answer =
            case Found of
                [F | _] ->
                    Rel = filename:join([Prefix, "slots", filename:basename(filename:dirname(F)),
                        filename:rootname(filename:basename(F)) ++ ".hbc"]),
                    #{ ID => #{ <<"path">> => list_to_binary(Rel) } };
                [] -> #{ ID => #{ <<"error">> => <<"not_found">> } }
            end,
        {200, hb_json:encode(Answer)}
    end).

%% @doc After its slot left local disk, a message reads by its signed ID, and
%% its assignment by its own, through the cold tier as a store, as Lunar and
%% the sites read history; an ID the share does not have is a miss.
read_by_id_after_prune_test_() ->
    {timeout, 120, fun() ->
        Env = #{ irr := Irr, opts := Opts0, p := P } = test_env("by-id"),
        Cold = maps:get(<<"cold-tier">>, Opts0),
        Share = share(Opts0),
        Opts = Opts0#{ <<"cold-tier">> => Cold#{ <<"verify">> => <<"coldcheck">>,
            <<"verify-url">> => stub_coldcheck(filename:dirname(Share), ets:new(no_lies, [public])),
            <<"verify-prefix">> => list_to_binary(filename:basename(Share)) } },
        assign(Env, lists:seq(0, 39), 3600000),
        {ok, A5} = hb_cache:read(slot_path(P, 5), Opts#{ <<"store">> => [Irr] }),
        Body = hb_ao:get(<<"body">>, A5, Opts),
        BodyID = hb_message:id(Body, signed, Opts),
        AID = hb_message:id(A5, signed, Opts),
        ok = tick(Opts),
        ?assertNot(local(Env, 5)),
        ?assertMatch({error, not_found}, hb_store:read([Irr], #{ <<"read">> => BodyID, <<"exact">> => true }, #{})),
        Reader = Opts#{
            <<"cold-tier">> => (maps:get(<<"cold-tier">>, Opts))#{
                <<"verify-url">> => stub_locate(Share, filename:basename(Share)) },
            <<"store">> => [Irr, #{ <<"store-module">> => hb_store_cold }] },
        {ok, Read} = hb_cache:read(BodyID, Reader),
        ?assertEqual(<<"5">>, hb_ao:get(<<"n">>, Read, Reader)),
        ?assert(hb_message:verify(Read, all, Reader)),
        {ok, ReadA} = hb_cache:read(AID, Reader),
        ?assertEqual(5, hb_util:int(hb_ao:get(<<"slot">>, ReadA, Reader))),
        ?assertEqual({error, not_found},
            read(#{}, #{ <<"read">> => hb_util:human_id(crypto:strong_rand_bytes(32)) }, Reader))
    end}.

%% @doc A replica's verified store follows the scheduler's rules: registered,
%% its ranges ship to the process's folder under their own tag (never over
%% the scheduler's files for the same slots), leave local disk below the hot
%% window, and read back from the share.
replica_store_ships_and_prunes_test_() ->
    {timeout, 120, fun() ->
        Env = #{ dir := Dir, opts := Opts, p := P } = test_env("replica"),
        Rep = #{ <<"store-module">> => hb_store_lmdb, <<"capacity">> => 1 bsl 30,
                 <<"name">> => list_to_binary(Dir ++ "/replica/" ++ binary_to_list(P)) },
        ok = hb_store:start([Rep]),
        RepEnv = Env#{ irr => Rep },
        assign(Env, lists:seq(0, 39), 3600000),
        assign(RepEnv, lists:seq(0, 39), 3600000),
        ok = register_source(Rep),
        try
            ok = tick(Opts),
            Files = [ filename:basename(F) || F <- filelib:wildcard(share(Opts) ++ "/slots/" ++ binary_to_list(P) ++ "/*.hbc") ],
            Tagged = [ F || F <- Files, length(string:split(filename:rootname(F), "-", all)) =:= 3 ],
            ?assert(length(Tagged) >= 1),
            ?assert(length(Files) > length(Tagged)),
            ?assertNot(local(RepEnv, 5)),
            ?assert(local(RepEnv, 0)),
            ?assert(local(RepEnv, 39)),
            ?assertNot(local(Env, 5)),
            {ok, A} = read_assignment(P, 5, Opts),
            ?assertEqual(5, hb_util:int(hb_ao:get(<<"slot">>, A, Opts))),
            [Tag] = maps:keys(persistent_term:get(?SOURCES)),
            ranges_hold(RepEnv, Opts, Tag)
        after
            persistent_term:erase(?SOURCES)
        end
    end}.

%% @doc A process's newest slot is found by probing, also above deleted ones.
head_probe_test_() ->
    {timeout, 60, fun() ->
        Env = #{ irr := Irr, opts := Opts, p := P } = test_env("head"),
        ?assertEqual(-1, head(Irr, P, -1)),
        assign(Env, lists:seq(0, 39), 3600000),
        ?assertEqual(39, head(Irr, P, -1)),
        ?assertEqual(39, head(Irr, P, 17)),
        ?assertEqual(39, head(Irr, P, 39)),
        ok = tick(Opts),
        ?assertNot(local(Env, 5)),
        ?assertEqual(39, head(Irr, P, read_int(Irr, mark_key(<<"shipped">>, P), -1)))
    end}.

%% @doc A process's window is capped in bytes too: with its bytes per slot
%% known, it keeps only the newest slots that fit, never below the floor,
%% while a process of small slots keeps its whole window.
per_process_byte_cap_test_() ->
    {timeout, 120, fun() ->
        EnvA = #{ irr := Irr, opts := Opts0, p := PA } = test_env("cap"),
        EnvB = #{ p := PB } = another_process(EnvA),
        Cold = maps:get(<<"cold-tier">>, Opts0),
        Opts = Opts0#{ <<"cold-tier">> => Cold#{ <<"hot-slots">> => 1000, <<"range-slots">> => 5,
            <<"ranges-per-round">> => 100 } },
        assign(EnvA, lists:seq(0, 59), 3600000),
        assign(EnvB, lists:seq(0, 59), 3600000),
        ok = tick(Opts),
        % Nothing below 1,000 slots or 1 GiB goes.
        ?assert(local(EnvA, 10)),
        ?assert(local(EnvB, 10)),
        PerSlot = read_int(Irr, mark_key(<<"slot-bytes">>, PB), 0),
        ?assert(PerSlot > 1000),
        % B's slots counted as 50x larger: a cap of 30 of A's slots is
        % under 1 of B's, so B keeps its floor (16) and A its newest 30.
        ok = hb_store:write([Irr], #{ mark_key(<<"slot-bytes">>, PB) => integer_to_binary(PerSlot * 50) }, #{}),
        ok = hb_store:flush([Irr], #{}),
        Capped = Opts#{ <<"cold-tier">> => (maps:get(<<"cold-tier">>, Opts))#{
            <<"hot-bytes-per-process">> => 30 * read_int(Irr, mark_key(<<"slot-bytes">>, PA), 0) } },
        ok = tick(Capped),
        ?assertNot(local(EnvA, 25)),
        ?assert(local(EnvA, 35)),
        ?assertNot(local(EnvB, 40)),
        ?assert(local(EnvB, 44)),
        ?assertEqual(18, slot_bytes(10, 42)),
        ?assertEqual(42, slot_bytes(0, 42))
    end}.

%% @doc A round's bytes set the pause after it: the shipper runs at its pace.
paced_test() ->
    Before = status(),
    try
        status_update(fun(S) -> S#{ last_round_bytes => 8388608, last_round_ms => 100 } end),
        ?assertEqual(900, paced(#{ ship_rate => 8388608 })),
        status_update(fun(S) -> S#{ last_round_bytes => 1000, last_round_ms => 100 } end),
        ?assertEqual(?BUSY_PAUSE_MS, paced(#{ ship_rate => 8388608 }))
    after
        persistent_term:put(?STATUS, Before)
    end.

%% A test environment whose scheduler's store is journaled to the share, as
%% on a node with `irreplaceable-export'.
journaled_env(Name) ->
    Env = #{ dir := Dir, opts := Opts0, irr := Plain } = test_env(Name),
    Share = share(Opts0),
    Irr = hb_store_offsite_journal:wrap(Plain#{ <<"name">> => list_to_binary(Dir ++ "/irrj") },
        #{ <<"path">> => list_to_binary(Share ++ "/irreplaceable"),
           <<"journal">> => list_to_binary(Dir ++ "/irrj-journal"),
           <<"compress">> => false }),
    ok = hb_store:start([Irr]),
    Main = maps:get(main, Env),
    Opts = Opts0#{ <<"store">> => [Main, Irr#{ <<"access">> => [<<"read">>] }],
                   <<"scheduler-store">> => [Irr] },
    Env#{ irr => Irr, opts => Opts }.

manifest(Opts) ->
    {ok, B} = file:read_file(share(Opts) ++ "/irreplaceable/manifest.log"),
    [ L || L <- binary:split(B, <<"\n">>, [global]), L =/= <<>> ].

%% @doc Once every slot a shipped journal segment covers has a verified range
%% file, the segment is retired: a manifest line, then the file goes, and the
%% share keeps each message once. A crash between the line and the delete
%% loses nothing and the next round finishes it; and a node that lost its
%% scheduler's store restores every slot, with its dedupe rows, from the
%% range files and what is left of the journal.
journal_duplicates_retire_test_() ->
    {timeout, 180, fun() ->
        Env = #{ irr := Irr, opts := Opts, p := P } = journaled_env("retire"),
        assign(Env, lists:seq(0, 39), 3600000),
        idle = hb_store_offsite_journal:ship_now(Irr),
        [{Seg1, #{ slots := #{ P := 39 }, other := false }}] = hb_store_offsite_journal:covers(Irr),
        % Slots up to 23 have range files: the segment (up to 39) waits.
        ok = tick(Opts),
        ?assertEqual([], [ L || L = <<"retired ", _/binary>> <- manifest(Opts) ]),
        assign(Env, lists:seq(40, 79), 3600000),
        idle = hb_store_offsite_journal:ship_now(Irr),
        % A crash after the retirement line, before the delete.
        persistent_term:put({hb_store_offsite_journal, checkpoint_crash}, retire_after_line),
        try ok = tick(Opts)
        after persistent_term:erase({hb_store_offsite_journal, checkpoint_crash})
        end,
        Seg1File = share(Opts) ++ "/irreplaceable/" ++ binary_to_list(Seg1),
        ?assert(lists:member(<<"retired ", Seg1/binary>>, manifest(Opts))),
        ?assert(filelib:is_file(Seg1File)),
        % The next round deletes it.
        ok = tick(Opts),
        ?assertNot(filelib:is_file(Seg1File)),
        ?assertNot(local(Env, 30)),
        % The scheduler's store is lost: everything comes back from the share.
        Fresh = (hb_test_utils:test_store(hb_store_lmdb, <<"retire-restore">>))#{ <<"capacity">> => 1 bsl 30 },
        ok = hb_store:start([Fresh]),
        {ok, Report} = restore(Opts, Fresh),
        ?assertMatch(#{ retired := 1 }, Report),
        [ ?assertMatch({ok, _}, hb_store:read([Fresh], #{ <<"read">> => slot_path(P, S),
            <<"exact">> => true }, #{})) || S <- lists:seq(0, 79) ],
        [ID5] = persistent_term:get({?MODULE, test_ids, P, 5}),
        ?assertEqual({ok, <<"5">>}, raw(Fresh, message_slot_path(P, ID5))),
        ?assertEqual(read_int(Irr, mark_key(<<"shipped">>, P), -1),
                     read_int(Fresh, mark_key(<<"shipped">>, P), -1)),
        {ok, A30} = read_rows(closure(Fresh, slot_path(P, 30)), slot_path(P, 30), Opts),
        ?assertEqual(30, hb_util:int(hb_ao:get(<<"slot">>, A30, Opts)))
    end}.

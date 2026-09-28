%%% @doc Retention planning for the process cache.
%%%
%%% The store keeps one computed state per slot forever. Measured on a 149 GiB
%%% production corpus (370 processes, 1,985,938 computed slots, 1,985,957
%%% assignment slots):
%%%
%%% <ul>
%%%   <li>a delta slot is ~9-24 KB logically, but explodes into ~325 LMDB rows
%%%       of ~160 bytes, costing ~48 KB of leaf pages</li>
%%%   <li>a checkpoint slot carries a Lua VM image in one incompressible
%%%       `snapshot' binary. The largest processes' checkpoints run 14-24 MB,
%%%       but the corpus-wide mean over all 4,094 of them is ~4.1 MB. The
%%%       non-snapshot public state is ~4-5 MB on a large process, <em>not</em>
%%%       "under 1 MB" as an earlier version of this comment said -- that error
%%%       came from summing only the seven largest fields and ignoring ~1,622
%%%       others</li>
%%%   <li>so the store is ~23% VM snapshots (the overflow pages) and ~73%
%%%       delta field-explosion (the leaf pages)</li>
%%%   <li>checkpoints do <em>not</em> land on one cadence. Of 370 processes, 162
%%%       checkpoint every 1000 slots, 161 carry a single one at slot 0, and the
%%%       rest land at irregular gaps of 50 or less: only a delta-bearing result
%%%       reaches `process-delta-checkpoint-slots', and everything else uses
%%%       `process-snapshot-slots' (50) or `process-snapshot-time' (900 s)</li>
%%% </ul>
%%%
%%% Assignments and their messages are the signed ordering commitments -- the
%%% chain itself -- and are never candidates for collection. Computed states
%%% are derived: given a process definition, the assignments and a checkpoint,
%%% any of them can be recomputed. This module plans which computed slots to
%%% retain.
%%%
%%% Two halves: `plan/2' sizes the work, `collect/3' enacts it.
%%%
%%% Enactment is <em>copy-based</em>, and that is not a preference. There is no
%%% delete primitive anywhere in the stack: the pinned `elmdb' Rust NIF exports
%%% only `env_open', `env_sync', `env_close', `env_close_by_name', `env_status',
%%% `db_open', `db_close', `put', `put_batch', `get', `flush', `overlay_count',
%%% `iterator', `iterator_next', `foreach', `fold', `map', `list', `read_prefix'
%%% and `match' -- no `del', no `mdb_del', no `drop'. The `hb_store' behaviour's
%%% callbacks are `start/3', `stop/3', `reset/3', `group/3', `link/3', `type/3',
%%% `read/3', `write/3', `list/3', `match/3' and `resolve/3' -- again no delete.
%%% And `hb_store_lmdb:reset/3' is `rm -Rf' of the whole store directory. So
%%% `collect/3' copies the retained set into a fresh store and leaves BOTH in
%%% place; an operator swaps them after validating the copy.
%%%
%%% Because the copy is the only way to drop anything, the copy must be exact.
%%% `collect/3' therefore moves <em>raw rows</em> -- key and value byte for byte,
%%% link markers and group markers included -- rather than re-serialising
%%% messages through `hb_cache:write/2'. Re-serialising the signed chain would
%%% put the collector in the business of reproducing commitment bytes, which is
%%% exactly the risk not worth taking.
%%%
%%% Retention classes are decided by <em>reading each slot's class</em>, never by
%%% arithmetic on the slot number. `plan/2' prices slots by the writer's
%%% checkpoint cadence because that is cheap and good enough for a projection,
%%% but the cadence is not reliable ground truth: `dev_process_cache' writes a
%%% full checkpoint whenever `should_checkpoint/4' sees a `snapshot' key, and
%%% `dev_process:should_snapshot/3' produces one every
%%% `process-snapshot-slots' <em>or</em> every `process-snapshot-time' seconds
%%% for a non-delta process. A collector that assumed cadence 1000 would drop
%%% real VM snapshots. `classify_root/2' looks at the stored row instead.
%%%
%%% Exact slot counts are cheap (one `list_numbered' per process) but reading
%%% 1.99M states to size them is not, so sizes are sampled per process and
%%% extrapolated. Every returned figure says whether it is counted or
%%% projected.
-module(hb_store_gc).
-export([plan/1, plan/2, plan_process/3]).
-export([collect/3, collect/4]).
-export([classify_root/2, classify_slot/3, retention/4, window_start/4]).
-export([assignment_timestamp_probe/3]).
-include_lib("eunit/include/eunit.hrl").

%% Retain every computed slot at or above (head - KeepRecent). The delta chain
%% is backward-linked and `dev_process_cache' hard-matches when materializing,
%% so a retained window must be contiguous back to a checkpoint or reads of it
%% fail rather than degrade.
-define(DEFAULT_KEEP_RECENT, 1000).
%% Below the recent window, retain one checkpoint every N slots as a replay
%% anchor. Must be a multiple of the writer's checkpoint cadence to land on
%% slots that actually hold a full state.
-define(DEFAULT_ANCHOR_EVERY, 10000).
%% Slots sampled per process to estimate mean delta and checkpoint size.
-define(DEFAULT_SAMPLE, 6).
%% The writer's checkpoint cadence (`process-delta-checkpoint-slots', compiled
%% default 1000). Used to classify which slots hold a full state.
-define(DEFAULT_CHECKPOINT_EVERY, 1000).
%% Corpus-measured means, used only when a process yields no readable sample of
%% that kind, so a projection is never silently zero.
-define(FALLBACK_CHECKPOINT_BYTES, 24000000).
-define(FALLBACK_DELTA_BYTES, 12000).
%% Processes and slots-per-process used for the global checkpoint mean. Kept
%% small: each read is ~24 MB.
-define(DEFAULT_CHECKPOINT_PROCS, 4).
-define(DEFAULT_CHECKPOINT_SAMPLE, 2).

-define(SCHED_PREFIX, <<"~scheduler@1.0/assignments">>).
%% Link-chain hops followed before giving up. `hb_store_lmdb' allows 1000;
%% real chains in this store are two deep.
-define(MAX_DEREF, 64).

%% @doc Plan retention across every process in the store.
plan(Opts) -> plan(#{}, Opts).
plan(Policy, Opts) ->
    Procs = hb_cache:list(?SCHED_PREFIX, Opts),
    %% A checkpoint read costs ~24 MB, so sampling them per process means tens
    %% of GB of reads and the planner never finishes. Take one global mean from
    %% a handful of processes instead -- but from the BIGGEST ones. Checkpoint
    %% size tracks game-state size, the largest processes hold most of the
    %% bytes, and sampling the first few in list order (which are small) put
    %% the mean at 5.7 MB against 14-24 MB measured on a large process.
    Counts = [ {length(computed_slots(P, Opts)), P} || P <- Procs ],
    Biggest = [ P || {_, P} <- lists:sublist(lists:reverse(lists:sort(Counts)),
                                             checkpoint_procs(Policy)) ],
    MeanCkpt = global_mean_checkpoint(Biggest, Policy, Opts),
    Plans = [ plan_process(P, Policy#{ mean_checkpoint => MeanCkpt }, Opts)
            || P <- Procs ],
    Ok = [ Pl || Pl = #{ slots := S } <- Plans, S > 0 ],
    Sum = fun(K) -> lists:sum([ maps:get(K, Pl, 0) || Pl <- Ok ]) end,
    Retain = Sum(retain_bytes),
    Drop = Sum(drop_bytes),
    Total = Retain + Drop,
    #{
        processes => length(Procs),
        processes_with_slots => length(Ok),
        slots_counted => Sum(slots),
        retain_slots => Sum(retain_slots),
        drop_slots => Sum(drop_slots),
        retain_checkpoints => Sum(retain_checkpoints),
        drop_checkpoints => Sum(drop_checkpoints),
        retain_bytes_projected => Retain,
        drop_bytes_projected => Drop,
        total_bytes_projected => Total,
        mean_checkpoint_bytes_sampled =>
            case Ok of
                [] -> 0;
                [#{ mean_checkpoint_bytes := MC } | _] -> MC
            end,
        reclaim_pct_projected =>
            case Total of 0 -> 0; _ -> (Drop * 100) div Total end,
        policy => policy(Policy),
        note =>
            <<"slots counted exactly; bytes projected from per-process "
              "samples. Enacting requires copy-and-swap: no delete "
              "primitive exists.">>
    }.

checkpoint_procs(Policy) -> maps:get(checkpoint_procs, policy(Policy)).

%% @doc Mean full-state size, sampled across a few of the largest processes.
global_mean_checkpoint(Procs, Policy, Opts) ->
    #{ checkpoint_every := Ckpt, checkpoint_sample := NS } = policy(Policy),
    Sizes =
        lists:flatten(
            [ begin
                Slots = computed_slots(P, Opts),
                Ckpts = [ S || S <- lists:sort(Slots), Ckpt > 0,
                               S rem Ckpt == 0, S > 0 ],
                [ B
                ||  S <- evenly(Ckpts, NS),
                    B <- [slot_bytes(P, S, Opts)],
                    B > 0
                ]
              end
            || P <- Procs
            ]
        ),
    mean(Sizes, ?FALLBACK_CHECKPOINT_BYTES).

%% @doc Plan one process. Returns counted slot figures and projected bytes.
plan_process(ProcID, Policy, Opts) ->
    #{ keep_recent := Keep, anchor_every := Anchor, sample := N,
       checkpoint_every := Ckpt } = policy(Policy),
    case computed_slots(ProcID, Opts) of
        [] ->
            #{ process => ProcID, slots => 0 };
        Slots ->
            Head = lists:max(Slots),
            {Retain, Drop} =
                lists:partition(
                    fun(S) -> retain_slot(S, Head, Keep, Anchor) end,
                    Slots
                ),
            MeanCheckpoint =
                maps:get(mean_checkpoint, Policy, ?FALLBACK_CHECKPOINT_BYTES),
            MeanDelta = sample_delta_bytes(ProcID, Slots, N, Ckpt, Opts),
            {RetainCkpt, _} = split_kinds(Retain, Ckpt),
            {DropCkpt, _} = split_kinds(Drop, Ckpt),
            #{
                process => ProcID,
                slots => length(Slots),
                head => Head,
                retain_slots => length(Retain),
                drop_slots => length(Drop),
                retain_checkpoints => RetainCkpt,
                drop_checkpoints => DropCkpt,
                mean_checkpoint_bytes => MeanCheckpoint,
                mean_delta_bytes => MeanDelta,
                retain_bytes => project(Retain, Ckpt, MeanCheckpoint, MeanDelta),
                drop_bytes => project(Drop, Ckpt, MeanCheckpoint, MeanDelta)
            }
    end.

%% @doc A slot is retained if it is inside the contiguous recent window, or is
%% a replay anchor below it.
retain_slot(Slot, Head, Keep, Anchor) ->
    Slot >= Head - Keep orelse (Anchor > 0 andalso Slot rem Anchor == 0).

%% @doc Checkpoints are the slots that hold a full state; everything else is a
%% delta. Classification uses the WRITER's cadence, not the retention policy:
%% a slot at a multiple of `CheckpointEvery' physically holds a ~24 MB state
%% whether or not the policy retains it.
project(Slots, CheckpointEvery, MeanCheckpoint, MeanDelta) ->
    {Checkpoints, Deltas} = split_kinds(Slots, CheckpointEvery),
    (Checkpoints * MeanCheckpoint) + (Deltas * MeanDelta).

split_kinds(Slots, CheckpointEvery) ->
    C =
        case CheckpointEvery of
            0 -> 0;
            _ -> length([ S || S <- Slots, S rem CheckpointEvery == 0 ])
        end,
    {C, length(Slots) - C}.

computed_slots(ProcID, Opts) ->
    Path = <<"computed/", (hb_util:human_id(ProcID))/binary, "/slot">>,
    try hb_cache:list_numbered(Path, Opts) catch _:_ -> [] end.

%% @doc Sample a few slots to estimate mean checkpoint and delta size. Falls
%% back to corpus-measured means when a process has too few readable slots,
%% so a projection is never silently zero.
%% @doc Mean delta size for one process. Deltas only -- checkpoint slots are
%% excluded by the writer's cadence, because letting a 24 MB full state into
%% this mean inflated a whole-corpus projection to 1.7x the store's real size.
sample_delta_bytes(ProcID, Slots, N, CheckpointEvery, Opts) ->
    Deltas =
        case CheckpointEvery of
            0 -> lists:sort(Slots);
            _ -> [ S || S <- lists:sort(Slots), S rem CheckpointEvery =/= 0 ]
        end,
    Sizes =
        [ B
        ||  S <- evenly(Deltas, N),
            B <- [slot_bytes(ProcID, S, Opts)],
            B > 0
        ],
    mean(Sizes, ?FALLBACK_DELTA_BYTES).

mean([], Default) -> Default;
mean(L, _) -> lists:sum(L) div length(L).

evenly(L, N) when N =< 0 -> L;
evenly(L, N) ->
    Len = length(L),
    case Len =< N of
        true -> L;
        false ->
            Step = Len div N,
            [ lists:nth(1 + (I * Step), L) || I <- lists:seq(0, N - 1) ]
    end.

slot_bytes(ProcID, Slot, Opts) ->
    Path =
        <<"computed/", (hb_util:human_id(ProcID))/binary, "/slot/",
          (integer_to_binary(Slot))/binary>>,
    try
        {ok, Msg} = hb_cache:read(Path, Opts),
        byte_size(term_to_binary(hb_cache:ensure_all_loaded(Msg, Opts)))
    catch _:_ -> 0
    end.

policy(Policy) ->
    #{
        keep_recent => maps:get(keep_recent, Policy, ?DEFAULT_KEEP_RECENT),
        anchor_every => maps:get(anchor_every, Policy, ?DEFAULT_ANCHOR_EVERY),
        %% The writer's cadence, NOT a policy choice: it decides which slots
        %% physically hold a full state. Pricing a real checkpoint as a delta
        %% is a ~2000x error per slot, so this must match
        %% `process-delta-checkpoint-slots'.
        checkpoint_every =>
            maps:get(checkpoint_every, Policy, ?DEFAULT_CHECKPOINT_EVERY),
        sample => maps:get(sample, Policy, ?DEFAULT_SAMPLE),
        checkpoint_procs =>
            maps:get(checkpoint_procs, Policy, ?DEFAULT_CHECKPOINT_PROCS),
        checkpoint_sample =>
            maps:get(checkpoint_sample, Policy, ?DEFAULT_CHECKPOINT_SAMPLE)
    }.

%%% ------------------------------------------------------------------
%%% Enactment: copy the retained set into a fresh store.
%%% ------------------------------------------------------------------

-define(SCHED_ROOT, <<"~scheduler@1.0">>).
-define(COMPUTED_ROOT, <<"computed">>).
%% Retention window in seconds of assignment timestamp below the head. §17 of
%% `docs/fork/STORE-GROWTH.md': one day.
-define(DEFAULT_KEEP_SECONDS, 86400).
%% Slots retained below the head whatever the timestamps say, so a process whose
%% assignments carry no usable timestamp still keeps a contiguous window.
-define(DEFAULT_KEEP_FLOOR, 1000).
%% How far below the time-derived window start to walk looking for the nearest
%% full state. The window must reach one: `dev_process_cache:materialize/3'
%% walks deltas backwards one `base-slot' at a time, so a window that begins on
%% a delta whose base was dropped is unreadable at its bottom edge. Bounded so a
%% pathological process cannot walk forever.
-define(DEFAULT_BASE_SEARCH, 20000).
%% Rows per `hb_store:write/3' call into the destination.
-define(BATCH_ROWS, 256).
%% Bytes accumulated before a batch is flushed regardless of row count: a single
%% `snapshot/body' row is ~19 MB and must not be packed with 255 others.
-define(BATCH_BYTES, 8 * 1024 * 1024).
%% Guard against a `read_prefix' on an accidentally short prefix -- the hazard
%% that once materialised 26.5M rows (~21 GiB) into a single term.
-define(MAX_SUBTREE_ROWS, 250000).
%% Processes collected concurrently. Copying is bound by random-read latency at
%% queue depth one, not by bandwidth or CPU: measured on the corpus, a serial pass
%% held the array at ~8.7k IOPS / 68 MB/s at 73% utilisation with 29% of one core
%% -- an NVMe mirror that will do an order of magnitude more with requests in
%% flight. The unit of concurrency is one whole process, so no two workers share a
%% retention decision; they share only the source (read-only) and the destination's
%% batching writer, which the live node already writes to from hundreds of Erlang
%% processes at once. 1 is the serial path.
-define(DEFAULT_WORKERS, 1).
%% Free space on the destination's filesystem below which collection stops.
%% LMDB never shrinks a store, so running the disk out mid-copy leaves a file
%% that cannot be reclaimed except by deleting it.
-define(DEFAULT_MIN_FREE, 64 * 1024 * 1024 * 1024).

%% @doc Copy the retained set from the store in `SrcOpts' into the store in
%% `DstOpts', and leave both in place. Nothing is deleted; the source is never
%% written. An operator swaps the stores after validating the copy.
%%
%% Policy keys, all optional:
%% <ul>
%%   <li>`keep_seconds' -- retention window for computed states, in seconds of
%%       assignment timestamp below the newest assignment. Default 86400 (§17).
%%       `0' disables the time window, leaving `keep_floor' alone.</li>
%%   <li>`keep_floor' -- slots below the head always retained. Default 1000.</li>
%%   <li>`now' -- reference timestamp in milliseconds. Default: the newest
%%       assignment timestamp anywhere in the set being collected, used for every
%%       process, so a long-dormant process does not get its own private
%%       24-hour window and keep it forever.</li>
%%   <li>`min_free_bytes' -- stop before the destination's filesystem has less
%%       than this free. Default 64 GiB.</li>
%%   <li>`dry_run' -- classify and size everything, write nothing. Run this
%%       first: the report's `bytes' is the exact logical size of the rows the
%%       real pass would copy.</li>
%%   <li>`base_search' -- how far below the window start to look for the full
%%       state the window must reach. Default 20000.</li>
%%   <li>`workers' -- processes collected concurrently. Default 1. Copying is
%%       bound by random-read latency at queue depth one, so this is worth
%%       raising: 12 workers took the corpus from 8.7k to 69k read IOPS.</li>
%%   <li>`progress' -- `fun((Report) -> any())', called after each process.</li>
%% </ul>
collect(Policy, SrcOpts, DstOpts) ->
    collect(Policy, SrcOpts, DstOpts, all).
collect(RawPolicy, SrcOpts, DstOpts, Which) ->
    Policy = collect_policy(RawPolicy),
    SrcStore = sole_store(SrcOpts),
    DstStore = sole_store(DstOpts),
    ok = refuse_unsafe(SrcStore, DstStore),
    ok = hb_store:start([SrcStore], #{}, SrcOpts),
    ok = hb_store:start([DstStore], #{}, DstOpts),
    Ctx0 =
        #{
            src_store => SrcStore,
            src_opts => SrcOpts,
            src_db => store_db(SrcStore),
            dst_store => DstStore,
            dst_opts => DstOpts,
            policy => Policy,
            dry_run => maps:get(dry_run, Policy)
        },
    Procs =
        case Which of
            all -> hb_cache:list(?SCHED_PREFIX, SrcOpts);
            List when is_list(List) -> [ hb_util:human_id(P) || P <- List ]
        end,
    %% One reference time for the whole store, not one per process. A per-process
    %% reference would give a process that stopped months ago its own private
    %% 24-hour window and keep that history forever; `keep_floor' is what
    %% guarantees every process stays readable near its own head.
    Ctx = Ctx0#{ policy := Policy#{ now => reference_now(Ctx0, Procs, Policy) } },
    %% The structural group markers the namespaces hang from. Copied as single
    %% rows, never as subtrees: `read_prefix' on `~scheduler@1.0/assignments'
    %% would materialise every assignment row in the store into one term.
    {Markers, MarkerKeys} =
        with_seen(Ctx, fun(C) ->
            lists:foldl(
                fun(Marker, A) -> copy_row(C, Marker, A) end,
                new_acc(),
                [?SCHED_ROOT, ?SCHED_PREFIX, ?COMPUTED_ROOT]
            )
        end),
    Acc1 = bump(distinct_keys, MarkerKeys, Markers),
    {AccN, Plans} = collect_all(Ctx, Procs, Acc1),
    %% `hb_store_lmdb' batches writes through an async Rust flush worker and
    %% returns `ok' whether or not anything reached disk. Stopping the store
    %% forces the flush; without it every measurement below reads an empty file.
    case maps:get(dry_run, Ctx) of
        true -> ok;
        false -> ok = hb_store:stop([DstStore], #{}, DstOpts)
    end,
    report(AccN, Plans, maps:get(policy, Ctx), SrcStore, DstStore).

%% @doc Give `Fun' a context with its own visited set and report how big that set
%% grew.
%%
%% The set is scoped to <em>one process</em>, and that is a memory decision made
%% the hard way. Held across a whole run it reached **20.7 GB resident plus 4.6 GB
%% swapped at 167 of 370 processes** on the corpus **[M]** -- it accumulates ~26
%% entries per assignment and there are 1.99M assignments, so a whole-store pass
%% does not fit in 62 GB. Per process it is bounded by the largest process
%% (~1.7M entries) instead.
%%
%% The cost of the narrower scope is that content shared <em>between</em> processes
%% is read and written more than once. Both are harmless -- a `put' of the same
%% key and value is idempotent -- but it means `rows' and `bytes' in the report are
%% <em>rows written</em>, an upper bound on the distinct bytes copied. The
%% authoritative output size is the store on disk, and the slot and assignment
%% counts are unaffected because they are counted per process.
with_seen(Ctx, Fun) ->
    Tab = ets:new(hb_store_gc_seen, [set, private]),
    try
        {Fun(Ctx#{ seen => Tab }), ets:info(Tab, size)}
    after
        ets:delete(Tab)
    end.

%% @doc Collect every process, serially or across `workers' of them.
collect_all(Ctx, Procs, Acc0) ->
    case maps:get(workers, maps:get(policy, Ctx)) of
        1 -> collect_serial(Ctx, Procs, Acc0);
        N -> collect_parallel(Ctx, Procs, Acc0, N)
    end.

collect_serial(Ctx, Procs, Acc0) ->
    {Acc, Plans} = fold_procs(Ctx, Procs, length(Procs), Acc0),
    {Acc, lists:reverse(Plans)}.

fold_procs(Ctx, Procs, Total, Acc0) ->
    lists:foldl(
        fun(ProcID, {A0, Ps}) ->
            ok = check_free_space(Ctx),
            {{A1, Plan}, Keys} =
                with_seen(Ctx, fun(C) -> collect_process(C, ProcID, A0) end),
            A2 = bump(distinct_keys, Keys, bump(procs_done, 1, A1)),
            report_progress(Ctx, Total, A2, Plan),
            {A2, [Plan | Ps]}
        end,
        {Acc0, []},
        Procs
    ).

%% @doc Round-robin the processes across `N' workers so the few very large ones do
%% not all land on one, then merge the per-worker accumulators.
%%
%% The unit of concurrency is one whole process, so no two workers share a
%% retention decision: they share only the source, which is open read-only, and the
%% destination's batching writer, which the live node already writes to from
%% hundreds of Erlang processes at once. A worker that dies fails the whole run
%% rather than leaving a partial copy behind a plausible-looking total.
collect_parallel(Ctx, Procs, Acc0, N) ->
    Parent = self(),
    Total = length(Procs),
    Chunks = [ C || C <- round_robin(Procs, N), C =/= [] ],
    Pids =
        [ element(1,
            spawn_monitor(fun() -> worker(Parent, Ctx, Chunk) end))
        || Chunk <- Chunks ],
    gather(Ctx, Total, length(Pids), Acc0, [], sets:from_list(Pids)).

worker(Parent, Ctx, Chunk) ->
    {Acc, Plans} =
        lists:foldl(
            fun(ProcID, {A0, Ps}) ->
                ok = check_free_space(Ctx),
                {{A1, Plan}, Keys} =
                    with_seen(Ctx, fun(C) -> collect_process(C, ProcID, A0) end),
                Parent ! {gc_proc_done, Plan},
                {bump(distinct_keys, Keys, A1), [Plan | Ps]}
            end,
            {new_acc(), []},
            Chunk
        ),
    Parent ! {gc_worker_done, self(), Acc, lists:reverse(Plans)}.

gather(_Ctx, _Total, 0, Acc, Plans, _Live) -> {Acc, Plans};
gather(Ctx, Total, Left, Acc, Plans, Live) ->
    receive
        {gc_proc_done, Plan} ->
            Acc1 = bump(procs_done, 1, Acc),
            report_progress(Ctx, Total, Acc1, Plan),
            gather(Ctx, Total, Left, Acc1, Plans, Live);
        {gc_worker_done, Pid, WAcc, WPlans} ->
            gather(Ctx, Total, Left - 1,
                   merge_acc(maps:remove(procs_done, WAcc), Acc),
                   Plans ++ WPlans, sets:del_element(Pid, Live));
        {'DOWN', _Ref, process, Pid, Reason} ->
            case {sets:is_element(Pid, Live), Reason} of
                {false, _} ->
                    %% Already accounted for by its `gc_worker_done'.
                    gather(Ctx, Total, Left, Acc, Plans, Live);
                {true, _} ->
                    erlang:error({collect_worker_died, Reason})
            end
    end.

merge_acc(From, Into) ->
    maps:fold(
        fun(max_subtree, V, A) ->
                maps:update_with(max_subtree, fun(O) -> max(O, V) end, V, A);
           (K, V, A) when is_integer(V) ->
                maps:update_with(K, fun(O) -> O + V end, V, A);
           (_K, _V, A) -> A
        end,
        Into,
        From
    ).

round_robin(Items, N) ->
    Filled =
        lists:foldl(
            fun({I, Item}, T) ->
                Slot = (I rem N) + 1,
                setelement(Slot, T, [Item | element(Slot, T)])
            end,
            erlang:make_tuple(N, []),
            lists:zip(lists:seq(0, length(Items) - 1), Items)
        ),
    [ lists:reverse(element(I, Filled)) || I <- lists:seq(1, N) ].

collect_policy(Policy) ->
    (policy(Policy))#{
        keep_seconds => maps:get(keep_seconds, Policy, ?DEFAULT_KEEP_SECONDS),
        keep_floor => maps:get(keep_floor, Policy, ?DEFAULT_KEEP_FLOOR),
        now => maps:get(now, Policy, undefined),
        dry_run => maps:get(dry_run, Policy, false),
        base_search => maps:get(base_search, Policy, ?DEFAULT_BASE_SEARCH),
        min_free_bytes => maps:get(min_free_bytes, Policy, ?DEFAULT_MIN_FREE),
        progress => maps:get(progress, Policy, undefined),
        workers => max(1, maps:get(workers, Policy, ?DEFAULT_WORKERS))
    }.

%% @doc Refuse to run unless the source is shut out of the write path and is a
%% different store from the destination. The source holds a signed chain that no
%% layer of this stack can restore; a typo that aimed the destination at it would
%% rewrite the thing being protected.
refuse_unsafe(Src, Dst) ->
    SrcName = maps:get(<<"name">>, Src, undefined),
    DstName = maps:get(<<"name">>, Dst, undefined),
    case SrcName of
        DstName -> erlang:error({collect_source_is_destination, SrcName});
        _ -> ok
    end,
    Found =
        {maps:get(<<"read-only">>, Src, false),
         maps:get(<<"access">>, Src, undefined)},
    %% `<<"access">>' is a list of policy groups (`hb_store:is_admissible/2'),
    %% and `[<<"read">>]' admits only `read', `resolve', `list', `type', `match',
    %% `start', `stop' and `scope' -- `write', `link', `group' and `reset' become
    %% inadmissible before they reach the module. `<<"read-only">>' is the second
    %% belt: `hb_store_lmdb:write/3' and `link/3' short-circuit on it.
    case Found of
        {true, [<<"read">>]} -> ok;
        _ ->
            erlang:error(
                {collect_source_not_read_only,
                    {required,
                        [{<<"read-only">>, true},
                         {<<"access">>, [<<"read">>]}]},
                    {found, Found}}
            )
    end.

sole_store(Opts) ->
    case hb_opts:get(<<"store">>, no_viable_store, Opts) of
        [Store] when is_map(Store) -> Store;
        Store when is_map(Store) -> Store;
        Other -> erlang:error({collect_needs_exactly_one_store, Other})
    end.

new_acc() ->
    #{
        rows => 0, bytes => 0, closures => 0, misses => 0, max_subtree => 0,
        unfollowed_link_keys => 0, ledger_bytes => 0, computed_bytes => 0,
        distinct_keys => 0,
        procs_done => 0, assignment_slots => 0,
        retain_slots => 0, drop_slots => 0,
        retain_in_window => 0, retain_checkpoint => 0,
        drop_delta => 0, drop_anchor => 0, drop_state => 0,
        drop_checkpoint => 0, drop_unknown => 0
    }.

bump(Key, N, Acc) -> maps:update_with(Key, fun(V) -> V + N end, N, Acc).

%%% Per-process collection.

collect_process(Ctx, RawID, Acc0) ->
    ID = hb_util:human_id(RawID),
    Policy = maps:get(policy, Ctx),
    AssignSlots = lists:sort(assignment_slots(Ctx, ID)),
    ComputedSlots = lists:sort(computed_slots(ID, maps:get(src_opts, Ctx))),
    %% Forever: the process definition and every assignment. §17 -- the
    %% definition is what makes anything computable, the assignments are the
    %% signed chain, and at 2,648 B/slot they are 5% of the cost.
    Acc1 = copy_closure(Ctx, ID, Acc0),
    Acc2 = copy_row(Ctx, <<?SCHED_PREFIX/binary, "/", ID/binary>>, Acc1),
    Acc3 =
        lists:foldl(
            fun(Slot, A) ->
                bump(assignment_slots, 1,
                     copy_closure(Ctx, assignment_path(ID, Slot), A))
            end,
            Acc2,
            AssignSlots
        ),
    {Retain, Drop, Meta} =
        retention(Ctx, ID, #{ slots => ComputedSlots,
                              assignment_slots => AssignSlots }, Policy),
    Acc4 = copy_row(Ctx, <<"computed/", ID/binary>>, Acc3),
    Acc5 = copy_row(Ctx, <<"computed/", ID/binary, "/slot">>, Acc4),
    Acc6 =
        lists:foldl(
            fun({Slot, Class}, A) ->
                copy_computed_slot(
                    Ctx, ID, Slot,
                    bump(retain_counter(Class), 1, bump(retain_slots, 1, A))
                )
            end,
            Acc5,
            Retain
        ),
    Acc7 =
        lists:foldl(
            fun({_Slot, Class}, A) ->
                bump(drop_counter(Class), 1, bump(drop_slots, 1, A))
            end,
            Acc6,
            Drop
        ),
    %% The two halves are copied in separate phases, so the counters bracket them
    %% without any extra plumbing, and the two add up to this process's whole
    %% contribution: §16 prices the ledger at 2,648 B/slot and 5% of the total,
    %% and this is where that claim can be checked against the store itself.
    Ledger = maps:get(bytes, Acc3) - maps:get(bytes, Acc0),
    Computed = maps:get(bytes, Acc7) - maps:get(bytes, Acc3),
    Acc8 = bump(ledger_bytes, Ledger, Acc7),
    Acc9 = bump(computed_bytes, Computed, Acc8),
    {Acc9, Meta#{ process => ID,
                  assignments => length(AssignSlots),
                  computed => length(ComputedSlots),
                  retained => length(Retain),
                  dropped => length(Drop),
                  ledger_bytes => Ledger,
                  computed_bytes => Computed }}.

retain_counter(in_window) -> retain_in_window;
retain_counter(checkpoint) -> retain_checkpoint.

drop_counter(delta) -> drop_delta;
drop_counter(anchor) -> drop_anchor;
drop_counter(state) -> drop_state;
drop_counter(checkpoint) -> drop_checkpoint;
drop_counter(unknown) -> drop_unknown.

assignment_slots(Ctx, ID) ->
    try hb_cache:list_numbered(
            <<?SCHED_PREFIX/binary, "/", ID/binary>>,
            maps:get(src_opts, Ctx))
    catch _:_ -> []
    end.

assignment_path(ID, Slot) ->
    <<?SCHED_PREFIX/binary, "/", ID/binary, "/",
      (integer_to_binary(Slot))/binary>>.

computed_path(ID, Slot) ->
    <<"computed/", ID/binary, "/slot/", (integer_to_binary(Slot))/binary>>.

%% @doc Copy one computed slot: the `slot/N' alias, the state root it points at,
%% and the `computed/<id>/<root-id>' alias `dev_process_cache:link_result/5'
%% writes beside it. Copying one alias and not the other would leave a state
%% readable by one of its two names only.
copy_computed_slot(Ctx, ID, Slot, Acc) ->
    Key = computed_path(ID, Slot),
    Acc1 = copy_closure(Ctx, Key, Acc),
    case raw_get(Ctx, Key) of
        {ok, <<"link:", Root/binary>>} ->
            copy_row(Ctx, <<"computed/", ID/binary, "/", Root/binary>>, Acc1);
        _ -> Acc1
    end.

%%% Retention: which computed slots survive.

%% @doc Split a process's computed slots into retained and dropped, each tagged
%% with the class that decided it.
%%
%% Three rules, in order:
%% <ol>
%%   <li>every slot at or above the window start is retained, contiguously</li>
%%   <li>the window start is lowered to the nearest slot holding a full state,
%%       so the backward delta walk terminates inside the retained set</li>
%%   <li>below the window a slot survives only if it carries a VM snapshot,
%%       which is what `dev_process_cache:latest/4' looks for when
%%       `dev_process:rewind/4' needs somewhere to restart from</li>
%% </ol>
retention(_Ctx, _ID, #{ slots := [] }, _Policy) ->
    {[], [], #{ head => undefined, window_start => undefined }};
retention(Ctx, ID, #{ slots := Slots, assignment_slots := AssignSlots },
          Policy) ->
    Head = lists:max(Slots),
    TimeStart = window_start(Ctx, ID, AssignSlots, Policy),
    FloorStart = Head - maps:get(keep_floor, Policy),
    RawStart = min(TimeStart, FloorStart),
    Classify = fun(S) -> classify_slot(Ctx, ID, S) end,
    Start = lower_to_full_state(Slots, RawStart, Policy, Classify),
    {Retain, Drop} =
        lists:foldl(
            fun(S, {R, D}) when S >= Start ->
                    {[{S, in_window} | R], D};
               (S, {R, D}) ->
                    case Classify(S) of
                        checkpoint -> {[{S, checkpoint} | R], D};
                        Class -> {R, [{S, Class} | D]}
                    end
            end,
            {[], []},
            Slots
        ),
    {lists:reverse(Retain), lists:reverse(Drop),
     #{ head => Head, window_start => Start,
        time_window_start => TimeStart, floor_start => FloorStart }}.

%% @doc Walk down from `From' to the nearest slot holding a full state. Returns
%% `From' when it already is one.
%%
%% When `base_search' slots yield none, the fallback is the process's
%% <em>lowest</em> slot, not the lowest slot searched. Stopping at the lowest
%% searched slot would leave the retained window beginning on a delta whose base
%% had been dropped, and `dev_process_cache:materialize/3' would fail on it --
%% the one shape of hole this whole function exists to prevent. 161 of the
%% corpus's 370 processes carry exactly one full state (slot 0, which
%% `dev_process_cache:should_checkpoint/4' forces), so a long enough process of
%% that shape reaches the cap; erring towards the lowest slot keeps it readable.
lower_to_full_state(Slots, From, Policy, Classify) ->
    case lists:reverse([ S || S <- Slots, S =< From ]) of
        [] -> From;
        Descending ->
            case find_full_state(Descending, Classify,
                                 maps:get(base_search, Policy)) of
                {ok, Full} -> Full;
                none -> lists:min(Descending)
            end
    end.

find_full_state([], _Classify, _Budget) -> none;
find_full_state(_Slots, _Classify, 0) -> none;
find_full_state([Slot | Rest], Classify, Budget) ->
    case Classify(Slot) of
        delta -> find_full_state(Rest, Classify, Budget - 1);
        _ -> {ok, Slot}
    end.

%% @doc The lowest slot inside the retention window, by binary search over
%% assignment timestamps. Timestamps are written in slot order, so a search
%% suffices; the answer is then widened downwards while the slot below it is
%% still inside the window, which stays correct if a timestamp is out of order
%% and only ever retains more.
window_start(_Ctx, _ID, [], _Policy) -> 0;
window_start(Ctx, ID, AssignSlots, Policy) ->
    case maps:get(keep_seconds, Policy) of
        0 -> no_time_window();
        Secs ->
            Sorted = list_to_tuple(lists:sort(AssignSlots)),
            N = tuple_size(Sorted),
            Now =
                case maps:get(now, Policy) of
                    Unset when Unset == undefined; Unset == 0 ->
                        assignment_timestamp(Ctx, ID, element(N, Sorted));
                    Given -> Given
                end,
            case Now of
                not_found -> no_time_window();
                _ ->
                    Cutoff = Now - (Secs * 1000),
                    Found = search_slot(Ctx, ID, Sorted, 1, N, Cutoff),
                    widen(Ctx, ID, Sorted, Found, Cutoff)
            end
    end.

%% Nothing qualifies on time; `keep_floor' alone decides.
no_time_window() -> 16#FFFFFFFFFFFFFFFF.

search_slot(_Ctx, _ID, Sorted, Lo, Hi, _Cutoff) when Lo >= Hi ->
    element(Lo, Sorted);
search_slot(Ctx, ID, Sorted, Lo, Hi, Cutoff) ->
    Mid = (Lo + Hi) div 2,
    case assignment_timestamp(Ctx, ID, element(Mid, Sorted)) of
        not_found -> search_slot(Ctx, ID, Sorted, Mid + 1, Hi, Cutoff);
        TS when TS >= Cutoff -> search_slot(Ctx, ID, Sorted, Lo, Mid, Cutoff);
        _ -> search_slot(Ctx, ID, Sorted, Mid + 1, Hi, Cutoff)
    end.

%% Bounded: an out-of-order timestamp costs at most this many extra probes, and
%% under-widening only narrows the window -- `lower_to_full_state/4' still puts
%% the bottom edge on a full state, so readability does not depend on it.
widen(Ctx, ID, Sorted, Slot, Cutoff) ->
    widen(Ctx, ID, Sorted, Slot, Cutoff, min(4096, tuple_size(Sorted))).
widen(_Ctx, _ID, _Sorted, Slot, _Cutoff, 0) -> Slot;
widen(Ctx, ID, Sorted, Slot, Cutoff, Budget) ->
    case index_of(Sorted, Slot) of
        I when I > 1 ->
            Prev = element(I - 1, Sorted),
            case assignment_timestamp(Ctx, ID, Prev) of
                TS when is_integer(TS), TS >= Cutoff ->
                    widen(Ctx, ID, Sorted, Prev, Cutoff, Budget - 1);
                _ -> Slot
            end;
        _ -> Slot
    end.

index_of(Sorted, Slot) -> index_of(Sorted, Slot, 1, tuple_size(Sorted)).
index_of(_Sorted, _Slot, Lo, Hi) when Lo > Hi -> 1;
index_of(Sorted, Slot, Lo, Hi) ->
    Mid = (Lo + Hi) div 2,
    case element(Mid, Sorted) of
        Slot -> Mid;
        V when V < Slot -> index_of(Sorted, Slot, Mid + 1, Hi);
        _ -> index_of(Sorted, Slot, Lo, Mid - 1)
    end.

%% @doc The `timestamp' an assignment carries, in milliseconds. Exported so an
%% operator can check what reference time a policy would pick before running one.
assignment_timestamp_probe(Ctx, ID, Slot) ->
    assignment_timestamp(Ctx, ID, Slot).

%% @doc The `timestamp' an assignment carries, in milliseconds.
assignment_timestamp(Ctx, ID, Slot) ->
    case deref(Ctx, assignment_path(ID, Slot)) of
        not_found -> not_found;
        Root ->
            case read_row_value(Ctx, <<Root/binary, "/timestamp">>) of
                {ok, Bin} ->
                    try binary_to_integer(Bin) catch _:_ -> not_found end;
                not_found -> not_found
            end
    end.

%%% Classification.

classify_slot(Ctx, ID, Slot) ->
    case deref(Ctx, computed_path(ID, Slot)) of
        not_found -> unknown;
        Root -> classify_root(Ctx, Root)
    end.

%% @doc Which retention class the state stored at `Root' belongs to. Read, not
%% inferred from the slot number.
%%
%% `checkpoint' carries a Lua VM snapshot and is the only class
%% `dev_process_cache:latest/4' will hand `dev_process:rewind/4' as a resume
%% base, so it is what §17 keeps forever. `state' is a full public state with no
%% VM image: readable, but useless as a resume base. `delta' and `anchor' are
%% window-only.
classify_root(Ctx, Root) ->
    case has_row(Ctx, <<Root/binary, "/snapshot+link">>) orelse
         has_row(Ctx, <<Root/binary, "/snapshot">>) of
        true -> checkpoint;
        false ->
            case read_row_value(Ctx, <<Root/binary, "/cache-format">>) of
                {ok, <<"process-delta@1.0">>} -> delta;
                {ok, <<"process-anchor@1.0">>} -> anchor;
                {ok, _} -> state;
                not_found ->
                    case has_row(Ctx, Root) of
                        true -> state;
                        false -> unknown
                    end
            end
    end.

has_row(Ctx, Key) -> raw_get(Ctx, Key) =/= not_found.

%% @doc The concrete value stored at a key, links and `raw:' escapes resolved.
read_row_value(Ctx, Key) ->
    case raw_get(Ctx, Key) of
        not_found -> not_found;
        {ok, Raw} -> read_value(Ctx, Raw)
    end.

%%% Raw row access.
%%%
%%% The copy must move link and group markers verbatim and the `hb_store' read
%%% path resolves them away, so the source is read at the row level. That couples
%%% the read side to `hb_store_lmdb'. The write side stays on the public
%%% `hb_store:write/3' interface, which for LMDB stores a row byte-identically to
%%% the one read: `link/3' and `group/3' are themselves `write/3' of
%%% `"link:<target>"' and `"group"'.

store_db(#{ <<"store-module">> := hb_store_lmdb } = Store) ->
    #{ <<"db">> := DB } = hb_store:find(Store),
    DB;
store_db(Store) ->
    erlang:error({collect_source_must_be_lmdb, Store}).

raw_get(Ctx, Key) ->
    case elmdb:get(maps:get(src_db, Ctx), Key) of
        {ok, Value} -> {ok, Value};
        _ -> not_found
    end.

%% @doc Follow a chain of `link:' rows to the key that actually holds a value.
deref(Ctx, Key) -> deref(Ctx, Key, ?MAX_DEREF).
deref(_Ctx, _Key, 0) -> not_found;
deref(Ctx, Key, Budget) ->
    case raw_get(Ctx, Key) of
        {ok, <<"link:", Target/binary>>} when byte_size(Target) > 0 ->
            deref(Ctx, Target, Budget - 1);
        {ok, _} -> Key;
        not_found -> not_found
    end.

%% @doc Every row at `Prefix' or strictly below it. `elmdb:read_prefix/2' matches
%% raw bytes, so a scan of `computed/<id>/slot/1' also returns slots 10, 100 and
%% 1000; the filter restores path semantics.
raw_subtree(Ctx, Prefix) ->
    case elmdb:read_prefix(maps:get(src_db, Ctx), Prefix) of
        {ok, Rows} -> {ok, [ R || R <- Rows, in_subtree(Prefix, R) ]};
        _ -> not_found
    end.

in_subtree(Prefix, {Prefix, _}) -> true;
in_subtree(Prefix, {Key, _}) ->
    Sz = byte_size(Prefix),
    byte_size(Key) > Sz
        andalso binary:part(Key, 0, Sz) == Prefix
        andalso binary:part(Key, Sz, 1) == <<"/">>.

%%% Copying.

%% @doc Copy exactly one row, then whatever it links to. Used for structural
%% group markers, whose subtrees are the whole store.
copy_row(Ctx, Key, Acc) ->
    case seen(Ctx, Key) of
        true -> Acc;
        false ->
            case raw_get(Ctx, Key) of
                not_found -> bump(misses, 1, Acc);
                {ok, Value} ->
                    Rows = [{Key, Value}],
                    follow_links(Ctx, Rows, put_rows(Ctx, Rows, Acc))
            end
    end.

%% @doc Copy a row and, when it is a group, its whole subtree; then follow every
%% link found. Content-addressed roots are copied this way: a message root is a
%% flat group of its fields, a checkpoint root that plus a nested
%% `snapshot/body'.
copy_closure(Ctx, Key, Acc) ->
    case seen(Ctx, Key) of
        true -> Acc;
        false ->
            case raw_get(Ctx, Key) of
                not_found -> bump(misses, 1, Acc);
                {ok, <<"group">>} -> copy_subtree(Ctx, Key, Acc);
                {ok, Value} ->
                    Rows = [{Key, Value}],
                    follow_links(Ctx, Rows, put_rows(Ctx, Rows, Acc))
            end
    end.

copy_subtree(Ctx, Key, Acc) ->
    case raw_subtree(Ctx, Key) of
        not_found -> bump(misses, 1, Acc);
        {ok, Rows} ->
            case length(Rows) > ?MAX_SUBTREE_ROWS of
                true -> erlang:error({collect_subtree_too_large, Key,
                                      length(Rows)});
                false -> ok
            end,
            Bytes = rows_bytes(Rows),
            Acc1 =
                maps:update_with(
                    max_subtree, fun(M) -> max(M, Bytes) end, Bytes, Acc),
            follow_links(Ctx, Rows,
                put_rows(Ctx, Rows, bump(closures, 1, Acc1)))
    end.

%% @doc Follow both kinds of reference out of a set of copied rows.
%%
%% There are two, and missing the second loses data silently. The first is the
%% store's own `"link:<key>"' marker, which `hb_store_lmdb' writes for
%% `hb_store:link/3'. The second is the message layer's: `hb_link:normalize/3'
%% stores a submessage as `<key>+link => <ID>', and `hb_cache:ensure_loaded/3'
%% reads it back by reading the message at `<ID>'. Because
%% `hb_cache:is_immediate_value/2' excludes `+link' keys from inline storage, that
%% `<ID>' is itself held behind a `link:data/<hash>' row -- so the `"link:"' rule
%% alone copies the row that <em>names</em> the submessage and stops there.
%%
%% Measured on the corpus: an assignment's `body+link' resolves to
%% `data/tdsn3...', whose 43-byte value is the ID of the signed game message. A
%% collector following only `"link:"' markers copies every assignment envelope
%% and none of the messages they commit to, while reporting zero misses. A
%% checkpoint's `snapshot+link' hides its ~16 MB VM image the same way.
follow_links(Ctx, Rows, Acc) ->
    lists:foldl(
        fun(Row, A) -> follow_row(Ctx, Row, A) end,
        Acc,
        Rows
    ).

follow_row(Ctx, {Key, Value}, Acc) ->
    Acc1 =
        case Value of
            <<"link:", Target/binary>> when byte_size(Target) > 0 ->
                copy_closure(Ctx, Target, Acc);
            _ -> Acc
        end,
    case hb_link:is_link_key(Key) of
        false -> Acc1;
        true ->
            case read_value(Ctx, Value) of
                {ok, ID} when byte_size(ID) == 42;
                              byte_size(ID) == 43;
                              byte_size(ID) == 44 ->
                    copy_closure(Ctx, ID, Acc1);
                _ ->
                    %% A `+link' whose target is not an ID is not something this
                    %% store's reader knows how to follow either; count it so a
                    %% new encoding cannot pass unnoticed.
                    bump(unfollowed_link_keys, 1, Acc1)
            end
    end.

%% @doc The concrete bytes a stored value stands for: follow `"link:"' markers to
%% the row that holds the payload, then strip the `raw:' escape
%% `hb_cache:encode_immediate_value/1' adds to values that would otherwise look
%% like markers.
read_value(Ctx, <<"link:", Target/binary>>) when byte_size(Target) > 0 ->
    case deref_value(Ctx, Target, ?MAX_DEREF) of
        not_found -> not_found;
        {ok, V} -> {ok, unescape(Target, V)}
    end;
read_value(_Ctx, Value) -> {ok, unescape(<<>>, Value)}.

deref_value(_Ctx, _Key, 0) -> not_found;
deref_value(Ctx, Key, Budget) ->
    case raw_get(Ctx, Key) of
        {ok, <<"link:", Target/binary>>} when byte_size(Target) > 0 ->
            deref_value(Ctx, Target, Budget - 1);
        {ok, Value} -> {ok, unescape(Key, Value)};
        not_found -> not_found
    end.

unescape(<<"data/", _/binary>>, Value) -> Value;
unescape(_Key, <<"raw:", Value/binary>>) -> Value;
unescape(_Key, Value) -> Value.

%% Entry points only, not every copied row: the visited set bounds re-reads of
%% shared content-addressed blobs without growing to the size of the output.
seen(Ctx, Key) -> not ets:insert_new(maps:get(seen, Ctx), {Key}).

rows_bytes(Rows) ->
    lists:sum([ byte_size(K) + byte_size(V) || {K, V} <- Rows ]).

put_rows(Ctx, Rows, Acc) ->
    Acc1 = bump(rows, length(Rows), bump(bytes, rows_bytes(Rows), Acc)),
    case maps:get(dry_run, Ctx) of
        true -> Acc1;
        false ->
            lists:foreach(
                fun(Batch) ->
                    ok = hb_store:write(
                        [maps:get(dst_store, Ctx)],
                        maps:from_list(Batch),
                        maps:get(dst_opts, Ctx)
                    )
                end,
                batches(Rows)
            ),
            Acc1
    end.

batches(Rows) -> batches(Rows, [], 0, 0, []).
batches([], [], _N, _B, Out) -> lists:reverse(Out);
batches([], Cur, _N, _B, Out) -> lists:reverse([lists:reverse(Cur) | Out]);
batches([R = {K, V} | Rest], Cur, N, B, Out) ->
    Size = byte_size(K) + byte_size(V),
    Full =
        Cur =/= [] andalso
        (N + 1 > ?BATCH_ROWS orelse B + Size > ?BATCH_BYTES),
    case Full of
        true -> batches(Rest, [R], 1, Size, [lists:reverse(Cur) | Out]);
        false -> batches(Rest, [R | Cur], N + 1, B + Size, Out)
    end.

%%% Reporting.

%% @doc Call the caller's `progress' fun, if any, after each process. A whole-store
%% pass over the 149 GiB corpus takes hours, so a run with no way to watch it is a
%% run an operator will kill on suspicion.
report_progress(Ctx, Total, Acc, Plan) ->
    case maps:get(progress, maps:get(policy, Ctx)) of
        undefined -> ok;
        Fun when is_function(Fun, 1) ->
            catch Fun(Acc#{ processes_total => Total, last_process => Plan }),
            ok
    end.

%% @doc Stop before the destination's filesystem fills. A dry run needs no space
%% and skips the check.
%%
%% This matters more than it looks: LMDB never shrinks a store, so a copy that
%% runs the disk out leaves a file whose pages cannot be reclaimed except by
%% deleting the whole thing -- and the same full disk is what the live node needs
%% to keep writing.
check_free_space(Ctx) ->
    case maps:get(dry_run, Ctx) of
        true -> ok;
        false ->
            Floor = maps:get(min_free_bytes, maps:get(policy, Ctx)),
            Dir = maps:get(<<"name">>, maps:get(dst_store, Ctx), <<".">>),
            case free_bytes(Dir) of
                unknown -> ok;
                Free when Free >= Floor -> ok;
                Free ->
                    %% Flush what has been copied so far, then refuse to add more.
                    catch hb_store:stop(
                        [maps:get(dst_store, Ctx)], #{}, maps:get(dst_opts, Ctx)),
                    erlang:error(
                        {collect_out_of_disk,
                            {free, Free}, {floor, Floor}, {destination, Dir}})
            end
    end.

%% @doc Free bytes on the filesystem holding `Dir', or `unknown'. `df' is the
%% only portable answer available to a release that does not start `os_mon'.
free_bytes(Dir) ->
    try
        Out = os:cmd("df -Pk " ++ hb_util:list(Dir) ++ " 2>/dev/null"),
        case string:tokens(Out, "\n") of
            [_Header, Line | _] ->
                case string:tokens(Line, " ") of
                    [_FS, _Total, _Used, Avail | _] ->
                        list_to_integer(Avail) * 1024;
                    _ -> unknown
                end;
            _ -> unknown
        end
    catch _:_ -> unknown
    end.

%% @doc The newest assignment timestamp in the set being collected, used as
%% "now" for the retention window. `not_found' when no assignment carries a
%% usable timestamp, in which case `keep_floor' alone decides.
reference_now(Ctx, Procs, Policy) ->
    case maps:get(now, Policy) of
        undefined ->
            lists:foldl(
                fun(RawID, Best) ->
                    ID = hb_util:human_id(RawID),
                    case lists:sort(assignment_slots(Ctx, ID)) of
                        [] -> Best;
                        Slots ->
                            case assignment_timestamp(Ctx, ID, lists:last(Slots)) of
                                TS when is_integer(TS), TS > Best -> TS;
                                _ -> Best
                            end
                    end
                end,
                0,
                Procs
            );
        Given -> Given
    end.

report(Acc, Plans, Policy, SrcStore, DstStore) ->
    Acc#{
        source => maps:get(<<"name">>, SrcStore, undefined),
        destination => maps:get(<<"name">>, DstStore, undefined),
        dry_run => maps:get(dry_run, Policy),
        keep_seconds => maps:get(keep_seconds, Policy),
        keep_floor => maps:get(keep_floor, Policy),
        reference_now => maps:get(now, Policy),
        workers => maps:get(workers, Policy),
        min_free_bytes => maps:get(min_free_bytes, Policy),
        %% Summed over the per-process visited sets, so a key reachable from two
        %% processes counts twice -- it is a work measure, not a key count.
        distinct_keys_visited => maps:get(distinct_keys, Acc, 0),
        processes => length(Plans),
        plans => lists:reverse(Plans),
        note =>
            <<"copy-only: the source store is untouched and both stores remain "
              "on disk. Validate the copy, then swap.">>
    }.

%%% Tests

retain_slot_keeps_recent_window_test() ->
    %% Everything within Keep of the head survives, contiguously.
    ?assert(retain_slot(9000, 10000, 1000, 0)),
    ?assert(retain_slot(10000, 10000, 1000, 0)),
    ?assertNot(retain_slot(8999, 10000, 1000, 0)).

retain_slot_keeps_anchors_below_window_test() ->
    %% Anchors survive arbitrarily far below the window; neighbours do not.
    ?assert(retain_slot(10000, 100000, 1000, 10000)),
    ?assertNot(retain_slot(10001, 100000, 1000, 10000)),
    ?assert(retain_slot(0, 100000, 1000, 10000)).

recent_window_is_contiguous_test() ->
    %% The materializer walks the chain backwards, so the retained window must
    %% have no holes: assert every slot in it is kept.
    Head = 5000, Keep = 100,
    Kept = [ S || S <- lists:seq(Head - Keep, Head),
                  retain_slot(S, Head, Keep, 10000) ],
    ?assertEqual(Keep + 1, length(Kept)).

project_separates_checkpoints_from_deltas_test() ->
    %% Two checkpoints at 0 and 1000 plus three deltas, at cadence 1000.
    Bytes = project([0, 1, 2, 3, 1000], 1000, 1000000, 10),
    ?assertEqual((2 * 1000000) + (3 * 10), Bytes).

project_prices_by_writer_cadence_not_policy_test() ->
    %% Slot 1000 is a real checkpoint even when the retention anchor is 10000.
    %% Pricing it as a delta was the bug that inflated the corpus projection.
    ?assertEqual(1000000 + 10, project([1000, 1001], 1000, 1000000, 10)).

sample_delta_bytes_excludes_checkpoints_test() ->
    %% With no readable slots the fallback stands, but the point is that slot
    %% 1000 is never offered to the sampler at cadence 1000.
    ?assertEqual(?FALLBACK_DELTA_BYTES,
        sample_delta_bytes(<<"none">>, [1000, 2000], 4, 1000, #{})).

checkpoint_procs_defaults_test() ->
    ?assertEqual(?DEFAULT_CHECKPOINT_PROCS, checkpoint_procs(#{})),
    ?assertEqual(9, checkpoint_procs(#{checkpoint_procs => 9})).

split_kinds_counts_checkpoints_test() ->
    ?assertEqual({2, 3}, split_kinds([0, 1, 2, 3, 1000], 1000)),
    ?assertEqual({0, 2}, split_kinds([1, 2], 0)).

evenly_spreads_and_never_exceeds_test() ->
    ?assertEqual(4, length(evenly(lists:seq(1, 1000), 4))),
    ?assertEqual([1, 2], evenly([1, 2], 6)).

mean_falls_back_rather_than_zero_test() ->
    ?assertEqual(12000, mean([], 12000)),
    ?assertEqual(15, mean([10, 20], 12000)).

%%% Enactment tests.
%%%
%%% `dev_process_cache:process_cache_suite_test_/0' generates zero tests
%%% (`hb_store:test_stores/0' returns maps, the comprehension destructures
%%% `{Name, Opts}' tuples), so nothing here leans on it: each case builds its own
%%% pair of LMDB stores and is a plain `_test_' generator.

%% A store the collector will accept as a source.
gc_test_src(Dir) ->
    #{
        <<"store-module">> => hb_store_lmdb,
        <<"name">> => Dir,
        <<"capacity">> => 1024 * 1024 * 1024,
        <<"read-only">> => true,
        <<"access">> => [<<"read">>]
    }.

gc_test_writable(Dir) ->
    #{
        <<"store-module">> => hb_store_lmdb,
        <<"name">> => Dir,
        <<"capacity">> => 1024 * 1024 * 1024
    }.

gc_test_dir(Suffix) ->
    Dir =
        iolist_to_binary(
            [
                <<"cache-TEST/hb-store-gc-">>,
                integer_to_binary(erlang:unique_integer([positive])),
                <<"-">>, Suffix
            ]
        ),
    filelib:ensure_dir(binary_to_list(<<Dir/binary, "/x">>)),
    Dir.

%% Two stores over one directory: one writable (to build the fixture), one
%% read-only (to hand the collector).
gc_test_stores(Suffix) ->
    Dir = gc_test_dir(Suffix),
    {gc_test_writable(Dir), gc_test_src(Dir)}.

gc_opts(Store) -> #{ <<"store">> => [Store] }.

gc_put(Store, Rows) ->
    ok = hb_store:start([Store], #{}, gc_opts(Store)),
    ok = hb_store:write([Store], maps:from_list(Rows), gc_opts(Store)),
    ok = hb_store:stop([Store], #{}, gc_opts(Store)),
    ok.

gc_id(Prefix, N) ->
    Base = iolist_to_binary([Prefix, integer_to_binary(N)]),
    Pad = 43 - byte_size(Base),
    <<Base/binary, (binary:copy(<<"_">>, Pad))/binary>>.

%% @doc A fixture shaped like the production store: the assignment index links to
%% a signed envelope whose `body+link' reaches the message only through a
%% `data/<hash>' row holding an ID, and computed slots alternate between
%% snapshot-bearing checkpoints and deltas. The `+link' indirection is the point:
%% a collector that follows only `link:' markers copies every envelope and loses
%% every message, silently.
gc_fixture(Slots, CheckpointEvery) -> gc_fixture(0, Slots, CheckpointEvery).
gc_fixture(N, Slots, CheckpointEvery) ->
    ProcID = gc_id(<<"proc">>, N),
    Def = gc_id(<<"pdef">>, N),
    Tag = integer_to_binary(N),
    Base =
        [
            {<<"~scheduler@1.0">>, <<"group">>},
            {<<"~scheduler@1.0/assignments">>, <<"group">>},
            {<<"~scheduler@1.0/assignments/", ProcID/binary>>, <<"group">>},
            {<<"computed">>, <<"group">>},
            {<<"computed/", ProcID/binary>>, <<"group">>},
            {<<"computed/", ProcID/binary, "/slot">>, <<"group">>},
            {ProcID, <<"link:", Def/binary>>},
            {Def, <<"group">>},
            {<<Def/binary, "/device">>, <<"process@1.0">>}
        ],
    Rows =
        lists:foldl(
            fun(Slot, Acc) ->
                Asg = gc_id(<<"asg", Tag/binary>>, Slot),
                Msg = gc_id(<<"msg", Tag/binary>>, Slot),
                MsgRef = gc_id(<<"mref", Tag/binary>>, Slot),
                State = gc_id(<<"st", Tag/binary>>, Slot),
                Snap = gc_id(<<"snap", Tag/binary>>, Slot),
                SnapRef = gc_id(<<"sref", Tag/binary>>, Slot),
                S = integer_to_binary(Slot),
                Assignment =
                    [
                        {<<"~scheduler@1.0/assignments/", ProcID/binary, "/", S/binary>>,
                            <<"link:", Asg/binary>>},
                        {Asg, <<"group">>},
                        {<<Asg/binary, "/slot">>, S},
                        {<<Asg/binary, "/timestamp">>,
                            integer_to_binary(1000000 + (Slot * 1000))},
                        {<<Asg/binary, "/type">>, <<"Assignment">>},
                        %% The `+link' shape: the row points at a `data/' blob
                        %% whose contents are the ID of the real message.
                        {<<Asg/binary, "/body+link">>, <<"link:data/", MsgRef/binary>>},
                        {<<"data/", MsgRef/binary>>, Msg},
                        {Msg, <<"group">>},
                        {<<Msg/binary, "/action">>, <<"Play">>},
                        {<<Msg/binary, "/payload">>,
                            <<"link:data/", (gc_id(<<"pay", Tag/binary>>, Slot))/binary>>},
                        {<<"data/", (gc_id(<<"pay", Tag/binary>>, Slot))/binary>>,
                            binary:copy(<<"payload-bytes.">>, 8)}
                    ],
                Computed =
                    [
                        {<<"computed/", ProcID/binary, "/slot/", S/binary>>,
                            <<"link:", State/binary>>},
                        {<<"computed/", ProcID/binary, "/", State/binary>>,
                            <<"link:", State/binary>>},
                        {State, <<"group">>},
                        {<<State/binary, "/at-slot">>, S}
                    ] ++
                    case Slot rem CheckpointEvery == 0 of
                        true ->
                            [
                                {<<State/binary, "/snapshot+link">>,
                                    <<"link:data/", SnapRef/binary>>},
                                {<<"data/", SnapRef/binary>>, Snap},
                                {Snap, <<"group">>},
                                {<<Snap/binary, "/body">>,
                                    binary:copy(<<"vm-image.">>, 64)}
                            ];
                        false ->
                            [
                                {<<State/binary, "/cache-format">>,
                                    <<"process-delta@1.0">>},
                                {<<State/binary, "/base-slot">>,
                                    integer_to_binary(Slot - 1)}
                            ]
                    end,
                Assignment ++ Computed ++ Acc
            end,
            Base,
            lists:seq(0, Slots - 1)
        ),
    {ProcID, Rows}.

gc_collect_fixture(Suffix, Slots, CheckpointEvery, Policy) ->
    {[ProcID], Src, Dst, Report} =
        gc_collect_fixtures(Suffix, 1, Slots, CheckpointEvery, Policy),
    {ProcID, Src, Dst, Report}.

%% `NProcs' independent processes in one store, so a parallel run has more than
%% one worker's worth of work to spread.
gc_collect_fixtures(Suffix, NProcs, Slots, CheckpointEvery, Policy) ->
    {Writable, Src} = gc_test_stores(Suffix),
    Built = [ gc_fixture(N, Slots, CheckpointEvery)
            || N <- lists:seq(0, NProcs - 1) ],
    ok = gc_put(Writable, lists:append([ R || {_, R} <- Built ])),
    ProcIDs = [ P || {P, _} <- Built ],
    Dst = gc_test_writable(gc_test_dir(<<Suffix/binary, "-out">>)),
    Report = collect(Policy, gc_opts(Src), gc_opts(Dst), ProcIDs),
    {ProcIDs, Src, Dst, Report}.

%%% The copy must be exact.

collect_copies_every_assignment_test_() ->
    {timeout, 60, fun() ->
        {ProcID, Src, Dst, Report} =
            gc_collect_fixture(<<"assign">>, 40, 10,
                #{ keep_seconds => 0, keep_floor => 5 }),
        SrcSlots =
            lists:sort(
                hb_cache:list_numbered(
                    <<"~scheduler@1.0/assignments/", ProcID/binary>>,
                    gc_opts(Src))),
        DstSlots =
            lists:sort(
                hb_cache:list_numbered(
                    <<"~scheduler@1.0/assignments/", ProcID/binary>>,
                    gc_opts(Dst))),
        ?assertEqual(lists:seq(0, 39), SrcSlots),
        ?assertEqual(SrcSlots, DstSlots),
        ?assertEqual(40, maps:get(assignment_slots, Report)),
        ?assertEqual(0, maps:get(misses, Report)),
        ?assertEqual(0, maps:get(unfollowed_link_keys, Report))
    end}.

%% The regression that matters: `body+link' reaches the message only through a
%% `data/' row whose value is an ID. Following `link:' markers alone copies the
%% envelope, reports zero misses, and loses the message.
collect_follows_link_suffixed_keys_test_() ->
    {timeout, 60, fun() ->
        {ProcID, Src, Dst, _} =
            gc_collect_fixture(<<"plink">>, 12, 10,
                #{ keep_seconds => 0, keep_floor => 2 }),
        Path = <<"~scheduler@1.0/assignments/", ProcID/binary, "/7">>,
        {ok, Before} = hb_cache:read(Path, gc_opts(Src)),
        {ok, After} = hb_cache:read(Path, gc_opts(Dst)),
        LoadedBefore = hb_cache:ensure_all_loaded(Before, gc_opts(Src)),
        LoadedAfter = hb_cache:ensure_all_loaded(After, gc_opts(Dst)),
        ?assertMatch(#{ <<"body">> := #{ <<"action">> := <<"Play">> } },
                     LoadedBefore),
        ?assertEqual(LoadedBefore, LoadedAfter)
    end}.

collect_keeps_snapshot_checkpoints_forever_test_() ->
    {timeout, 60, fun() ->
        {ProcID, Src, Dst, Report} =
            gc_collect_fixture(<<"ckpt">>, 40, 10,
                #{ keep_seconds => 0, keep_floor => 5 }),
        Kept =
            lists:sort(
                hb_cache:list_numbered(
                    <<"computed/", ProcID/binary, "/slot">>, gc_opts(Dst))),
        %% Head is 39, floor keeps 34..39, and that window is lowered to slot 30
        %% (a checkpoint) so the delta walk terminates. Checkpoints 0, 10 and 20
        %% survive below it; the deltas between them do not.
        ?assertEqual([0, 10, 20] ++ lists:seq(30, 39), Kept),
        ?assertEqual(3, maps:get(retain_checkpoint, Report)),
        ?assertEqual(27, maps:get(drop_delta, Report)),
        ?assertEqual(0, maps:get(drop_checkpoint, Report)),
        %% The VM image itself came across, not just the row that names it. It
        %% hides behind `snapshot+link -> link:data/<hash> -> <id>', so this is
        %% the same indirection the `body+link' case tests, one level deeper.
        Path = <<"computed/", ProcID/binary, "/slot/20">>,
        {ok, SrcState} = hb_cache:read(Path, gc_opts(Src)),
        {ok, DstState} = hb_cache:read(Path, gc_opts(Dst)),
        LoadedSrc = hb_cache:ensure_all_loaded(SrcState, gc_opts(Src)),
        LoadedDst = hb_cache:ensure_all_loaded(DstState, gc_opts(Dst)),
        ?assertMatch(#{ <<"snapshot">> := #{ <<"body">> := _ } }, LoadedSrc),
        ?assertEqual(LoadedSrc, LoadedDst)
    end}.

collect_drops_are_clean_not_crashes_test_() ->
    {timeout, 60, fun() ->
        {ProcID, _Src, Dst, _} =
            gc_collect_fixture(<<"clean">>, 40, 10,
                #{ keep_seconds => 0, keep_floor => 5 }),
        %% Slot 5 was dropped: a read must say so, not crash.
        Res =
            hb_cache:read(
                <<"computed/", ProcID/binary, "/slot/5">>, gc_opts(Dst)),
        ?assertEqual({error, not_found}, Res)
    end}.

collect_window_follows_timestamps_test_() ->
    {timeout, 60, fun() ->
        %% Timestamps are 1000 ms apart and the head is slot 39, so a 25-second
        %% window reaches slot 14; that is lowered to checkpoint 10 so the delta
        %% walk terminates. `keep_floor' of 1 would have stopped at 30, so this
        %% asserts the clock decided, not the floor.
        {ProcID, _Src, Dst, Report} =
            gc_collect_fixture(<<"window">>, 40, 10,
                #{ keep_seconds => 25, keep_floor => 1 }),
        Kept =
            lists:sort(
                hb_cache:list_numbered(
                    <<"computed/", ProcID/binary, "/slot">>, gc_opts(Dst))),
        ?assertEqual([0] ++ lists:seq(10, 39), Kept),
        ?assertEqual(10, maps:get(window_start, hd(maps:get(plans, Report))))
    end}.

collect_dry_run_writes_nothing_test_() ->
    {timeout, 60, fun() ->
        {ProcID, _Src, Dst, Report} =
            gc_collect_fixture(<<"dry">>, 20, 10,
                #{ keep_seconds => 0, keep_floor => 5, dry_run => true }),
        ?assert(maps:get(rows, Report) > 0),
        ?assert(maps:get(bytes, Report) > 0),
        ?assertEqual(
            [],
            hb_cache:list_numbered(
                <<"~scheduler@1.0/assignments/", ProcID/binary>>,
                gc_opts(Dst)))
    end}.

%%% Guards.

collect_refuses_writable_source_test() ->
    Writable = gc_test_writable(<<"cache-TEST/gc-src">>),
    Dst = gc_test_writable(<<"cache-TEST/gc-dst">>),
    ?assertError(
        {collect_source_not_read_only, _, _},
        collect(#{}, gc_opts(Writable), gc_opts(Dst), [])
    ).

collect_refuses_source_as_destination_test() ->
    Src = gc_test_src(<<"cache-TEST/gc-same">>),
    ?assertError(
        {collect_source_is_destination, _},
        collect(#{}, gc_opts(Src), gc_opts(gc_test_writable(<<"cache-TEST/gc-same">>)), [])
    ).

%%% Unit-level invariants.

in_subtree_respects_path_boundaries_test() ->
    %% `read_prefix' matches raw bytes: a scan of `slot/1' also returns `slot/10'
    %% and `slot/1000'. Copying those is harmless; classifying by them is not.
    P = <<"computed/x/slot/1">>,
    ?assert(in_subtree(P, {P, <<"v">>})),
    ?assert(in_subtree(P, {<<P/binary, "/at-slot">>, <<"v">>})),
    ?assertNot(in_subtree(P, {<<"computed/x/slot/10">>, <<"v">>})),
    ?assertNot(in_subtree(P, {<<"computed/x/slot/1000">>, <<"v">>})).

lower_to_full_state_reaches_a_base_test() ->
    Slots = lists:seq(0, 100),
    Class = fun(S) -> case S rem 10 of 0 -> checkpoint; _ -> delta end end,
    Policy = #{ base_search => 1000 },
    %% From a delta, walk down to the checkpoint that terminates the walk.
    ?assertEqual(50, lower_to_full_state(Slots, 57, Policy, Class)),
    %% From a checkpoint, stay put.
    ?assertEqual(60, lower_to_full_state(Slots, 60, Policy, Class)),
    %% With no full state in reach, keep more rather than less.
    AllDelta = fun(_) -> delta end,
    ?assertEqual(0, lower_to_full_state(Slots, 57, Policy, AllDelta)).

batches_split_by_rows_and_bytes_test() ->
    Small = [ {<<"k">>, <<"v">>} || _ <- lists:seq(1, 600) ],
    ?assertEqual(3, length(batches(Small))),
    ?assertEqual(600, lists:sum([ length(B) || B <- batches(Small) ])),
    %% A ~19 MB snapshot row must not be packed with 255 others.
    Big = [ {<<"k">>, binary:copy(<<0>>, 9 * 1024 * 1024)} || _ <- lists:seq(1, 3) ],
    ?assertEqual(3, length(batches(Big))),
    ?assertEqual([], batches([])).

unescape_undoes_the_marker_escape_test() ->
    %% `hb_cache:encode_immediate_value/1' prefixes values that would look like
    %% markers; a `data/' payload is never escaped.
    ?assertEqual(<<"group">>, unescape(<<"k">>, <<"raw:group">>)),
    ?assertEqual(<<"link:x">>, unescape(<<"k">>, <<"raw:link:x">>)),
    ?assertEqual(<<"raw:x">>, unescape(<<"data/abc">>, <<"raw:x">>)),
    ?assertEqual(<<"plain">>, unescape(<<"k">>, <<"plain">>)).

sole_store_rejects_store_lists_test() ->
    One = gc_test_src(<<"cache-TEST/one">>),
    ?assertEqual(One, sole_store(#{ <<"store">> => [One] })),
    ?assertEqual(One, sole_store(#{ <<"store">> => One })),
    ?assertError(
        {collect_needs_exactly_one_store, _},
        sole_store(#{ <<"store">> => [One, One] })
    ).

free_bytes_reads_a_filesystem_test() ->
    %% Whatever the number, it must be a positive integer for a real directory
    %% and `unknown' for nonsense, because the guard treats `unknown' as "do not
    %% block" and must not be reachable by accident.
    ?assert(is_integer(free_bytes(<<".">>))),
    ?assert(free_bytes(<<".">>) > 0),
    ?assertEqual(unknown, free_bytes(<<"/nonexistent-path-for-hb-store-gc">>)).

collect_stops_before_filling_the_disk_test_() ->
    {timeout, 60, fun() ->
        %% An impossible floor must abort rather than write.
        {Writable, Src} = gc_test_stores(<<"disk">>),
        {ProcID, Rows} = gc_fixture(4, 10),
        ok = gc_put(Writable, Rows),
        Dst = gc_test_writable(gc_test_dir(<<"disk-out">>)),
        ?assertError(
            {collect_out_of_disk, _, _, _},
            collect(
                #{ min_free_bytes => 1 bsl 60 },
                gc_opts(Src), gc_opts(Dst), [ProcID])
        )
    end}.

lower_to_full_state_widens_to_the_lowest_slot_at_the_cap_test() ->
    %% Only slot 0 is a full state, and the search gives up before reaching it.
    %% The answer must still be 0: stopping at the lowest slot searched would put
    %% the window's bottom edge on a delta whose base was dropped.
    Slots = lists:seq(0, 500),
    Class = fun(0) -> checkpoint; (_) -> delta end,
    ?assertEqual(0, lower_to_full_state(Slots, 400, #{ base_search => 10 },
                                        Class)),
    ?assertEqual(0, lower_to_full_state(Slots, 400, #{ base_search => 1000 },
                                        Class)).

collect_accounts_for_every_byte_it_copies_test_() ->
    {timeout, 60, fun() ->
        %% `ledger_bytes' and `computed_bytes' bracket the two copy phases, so
        %% together they must account for everything but the three shared
        %% namespace markers. A drift here means a phase moved and the §16 ledger
        %% share is being measured against the wrong denominator.
        {_ProcID, _Src, _Dst, Report} =
            gc_collect_fixture(<<"split">>, 30, 10,
                #{ keep_seconds => 0, keep_floor => 5, dry_run => true }),
        Ledger = maps:get(ledger_bytes, Report),
        Computed = maps:get(computed_bytes, Report),
        Total = maps:get(bytes, Report),
        ?assert(Ledger > 0),
        ?assert(Computed > 0),
        %% The only rows outside both phases are `~scheduler@1.0',
        %% `~scheduler@1.0/assignments' and `computed': three `group' markers.
        Markers = byte_size(<<"~scheduler@1.0">>) + byte_size(<<"group">>)
                + byte_size(<<"~scheduler@1.0/assignments">>) + byte_size(<<"group">>)
                + byte_size(<<"computed">>) + byte_size(<<"group">>),
        ?assertEqual(Total, Ledger + Computed + Markers)
    end}.

collect_reports_progress_per_process_test_() ->
    {timeout, 60, fun() ->
        Self = self(),
        {_ProcID, _Src, _Dst, _Report} =
            gc_collect_fixture(<<"progress">>, 10, 10,
                #{ keep_seconds => 0, keep_floor => 2, dry_run => true,
                   progress => fun(P) -> Self ! {progress, P} end }),
        receive
            {progress, P} ->
                ?assertEqual(1, maps:get(procs_done, P)),
                ?assertEqual(1, maps:get(processes_total, P)),
                ?assertMatch(#{ process := _ }, maps:get(last_process, P))
        after 1000 -> ?assert(false)
        end
    end}.

round_robin_spreads_the_big_ones_test() ->
    %% Processes arrive in key order, which puts similarly sized ones together,
    %% so consecutive items must land on different workers.
    ?assertEqual([[1, 4, 7], [2, 5], [3, 6]], round_robin([1,2,3,4,5,6,7], 3)),
    ?assertEqual([[1], [], []], round_robin([1], 3)),
    ?assertEqual([[]], round_robin([], 1)).

merge_acc_sums_counts_and_maxes_the_max_test() ->
    A = #{ rows => 3, bytes => 10, max_subtree => 50, note => <<"x">> },
    B = #{ rows => 4, bytes => 1, max_subtree => 20 },
    M = merge_acc(A, B),
    ?assertEqual(7, maps:get(rows, M)),
    ?assertEqual(11, maps:get(bytes, M)),
    ?assertEqual(50, maps:get(max_subtree, M)),
    %% Non-integers are not merged: there is no sensible sum for them.
    ?assertEqual(false, maps:is_key(note, M)).

collect_is_identical_serial_and_parallel_test_() ->
    {timeout, 300, fun() ->
        %% Five processes, same policy, four workers against one. Every counted
        %% figure must match and both copies must answer the same reads -- which is
        %% the whole point of making the unit of concurrency a whole process.
        Policy = #{ keep_seconds => 0, keep_floor => 5 },
        {Ps1, _S1, D1, R1} = gc_collect_fixtures(<<"ser">>, 5, 30, 10, Policy),
        {Ps2, _S2, D2, R2} =
            gc_collect_fixtures(<<"par">>, 5, 30, 10, Policy#{ workers => 4 }),
        ?assertEqual(Ps1, Ps2),
        ?assertEqual(5, maps:get(procs_done, R1)),
        ?assertEqual(5, maps:get(procs_done, R2)),
        [ ?assertEqual({K, maps:get(K, R1)}, {K, maps:get(K, R2)})
        || K <- [rows, bytes, closures, misses, max_subtree, assignment_slots,
                 retain_slots, drop_slots, retain_in_window, retain_checkpoint,
                 drop_delta, drop_state, drop_unknown, unfollowed_link_keys,
                 ledger_bytes, computed_bytes, procs_done, processes] ],
        [ ?assertEqual(
            lists:sort(hb_cache:list_numbered(
                <<"computed/", P/binary, "/slot">>, gc_opts(D1))),
            lists:sort(hb_cache:list_numbered(
                <<"computed/", P/binary, "/slot">>, gc_opts(D2))))
        || P <- Ps1 ],
        %% And the plans, which is what an operator reads afterwards.
        ?assertEqual(
            lists:sort([ maps:get(process, X) || X <- maps:get(plans, R1) ]),
            lists:sort([ maps:get(process, X) || X <- maps:get(plans, R2) ]))
    end}.

collect_fails_the_run_if_a_worker_dies_test_() ->
    {timeout, 60, fun() ->
        %% A worker that dies has left a partial copy; the run must not report a
        %% total that silently omits a process.
        Parent = self(),
        Ctx = #{ policy => #{ workers => 2, progress => undefined },
                 dry_run => true },
        Pid = spawn(fun() -> receive never -> ok end end),
        spawn(fun() ->
            %% `collect_parallel/4' monitors its workers with `spawn_monitor';
            %% the monitor has to exist here too or no DOWN ever arrives.
            erlang:monitor(process, Pid),
            Res = (catch gather(Ctx, 1, 1, new_acc(), [],
                                sets:from_list([Pid]))),
            Parent ! {res, Res}
        end),
        timer:sleep(50),
        exit(Pid, kill),
        receive
            {res, R} -> ?assertMatch({'EXIT', {{collect_worker_died, _}, _}}, R)
        after 5000 -> ?assert(false)
        end
    end}.

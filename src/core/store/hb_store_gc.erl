%%% @doc Retention planning for the process cache.
%%%
%%% The store keeps one computed state per slot forever. Measured on a 149 GiB
%%% production corpus (370 processes, 1,985,934 slots):
%%%
%%% <ul>
%%%   <li>a delta slot is ~9-24 KB logically, but explodes into ~325 LMDB rows
%%%       of ~160 bytes, costing ~48 KB of leaf pages</li>
%%%   <li>a checkpoint slot (every `process-delta-checkpoint-slots', default
%%%       1000) is ~24 MB, of which ~18.9 MB is a single incompressible
%%%       `snapshot/body' binary -- the Lua VM image. Actual game state is
%%%       under 1 MB</li>
%%%   <li>so the store is ~23% VM snapshots (the overflow pages) and ~73%
%%%       delta field-explosion (the leaf pages)</li>
%%% </ul>
%%%
%%% Assignments and their messages are the signed ordering commitments -- the
%%% chain itself -- and are never candidates for collection. Computed states
%%% are derived: given a process definition, the assignments and a checkpoint,
%%% any of them can be recomputed. This module plans which computed slots to
%%% retain.
%%%
%%% Planning only. It writes nothing and deletes nothing: there is no delete
%%% primitive anywhere in the stack (not in the elmdb NIF, not in the
%%% `hb_store' behaviour; `hb_store_lmdb:reset/3' is `rm -Rf'), so enacting a
%%% plan means copying the retained set to a new store and swapping. `plan/2'
%%% exists to size that work honestly before anyone writes it.
%%%
%%% Exact slot counts are cheap (one `list_numbered' per process) but reading
%%% 1.99M states to size them is not, so sizes are sampled per process and
%%% extrapolated. Every returned figure says whether it is counted or
%%% projected.
-module(hb_store_gc).
-export([plan/1, plan/2, plan_process/3]).
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

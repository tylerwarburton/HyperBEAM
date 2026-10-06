%%% @doc A separate, configurable store for the data this node can never
%%% regenerate: the essentials.
%%%
%%% The essentials are every scheduler assignment and the message it carries
%%% (with its commitments and every blob it reaches), every process definition,
%%% the scheduler's other keys (slot links, the upload watermark), and the small
%%% namespaces `~location@1.0', `~bundler@1.0', `~arweave@2.9' and
%%% `~meta@1.0'. Everything else in the main store -- computed states, deltas,
%%% checkpoints -- is derived from them and can be collected
%%% (`hb_store_gc:retain/1').
%%%
%%% Configuration (all off by default; unset, every function here is the
%%% identity and the node behaves exactly as before):
%%% <ul>
%%%   <li>`essentials-store': a store, or list of stores, written with the
%%%       essentials. The recommended value is a LOCAL `hb_store_lmdb' -- on the
%%%       main store's disk or another local one. LMDB must never be placed on
%%%       a network filesystem (mmap and its lock file are unsupported there).
%%%       An `hb_store_fs' on a remote mount also works, but puts that mount's
%%%       latency on every POST /schedule; see `essentials-export' instead.</li>
%%%   <li>`essentials-export': a message configuring the asynchronous,
%%%       append-only, resumable export of every essential write to a remote
%%%       path (`hb_store_export'). The schedule path only casts a record to a
%%%       local journal writer; a slow or absent mount only grows the local
%%%       backlog, which is bounded.</li>
%%% </ul>
%%%
%%% Routing: writers of essentials (`dev_scheduler_cache', `dev_location_cache',
%%% `dev_bundler_cache', `dev_arweave_block_cache') write through `opts/1',
%%% whose store list is the essentials store(s) followed by the node's store:
%%% writes land in the first (the essentials store) and reads fall through to
%%% the main store for anything written before the essentials store existed.
%%% `hb_cache:write/2' writes the whole message graph to that first store, so
%%% the essentials store is self-contained: no essential depends on a row of
%%% the main store, and collecting the main store can never break one. The
%%% node's own `store' list gains the essentials store (read access only) right
%%% after its first store, so a process definition or message read by ID is
%%% found there (`node_store/1').
-module(hb_store_essentials).
-export([store/1, enabled/1, opts/1, node_store/1, node_message/1]).
-export([migrate/2, migrate/3, verify/2, verify/3]).
-include("include/hb.hrl").

-define(ASSIGNMENTS, <<"~scheduler@1.0/assignments">>).
-include_lib("eunit/include/eunit.hrl").

%% @doc The essentials store list, or `undefined' when none is configured.
store(Opts) ->
    case hb_opts:get(<<"essentials-store">>, undefined, Opts) of
        undefined -> undefined;
        not_found -> undefined;
        [] -> undefined;
        Store when is_map(Store) -> with_export([Store], Opts);
        Stores when is_list(Stores) -> with_export(Stores, Opts)
    end.

enabled(Opts) -> store(Opts) =/= undefined.

%% @doc Wrap the first essentials store in the exporter when an export is
%% configured. Idempotent: an already wrapped store is left alone.
with_export([First = #{ <<"store-module">> := hb_store_export } | Rest], _Opts) ->
    [First | Rest];
with_export([First | Rest], Opts) ->
    case hb_opts:get(<<"essentials-export">>, undefined, Opts) of
        Export when is_map(Export) ->
            [hb_store_export:wrap(First, Export) | Rest];
        _ -> [First | Rest]
    end.

%% @doc Options for a writer of essentials: the essentials store first, then
%% the node's stores (without duplicates). Unchanged when no essentials store
%% is configured.
opts(Opts) ->
    case store(Opts) of
        undefined -> Opts;
        Ess ->
            Main = as_list(hb_opts:get(<<"store">>, [], Opts)),
            Opts#{ <<"store">> => Ess ++ without(Main, Ess) }
    end.

%% @doc The node's store list with the essentials store(s) inserted, read-only,
%% after its first store. Reads of a definition or message by ID find it
%% there; writes never reach it through this list. Unchanged when no
%% essentials store is configured.
node_store(Opts) ->
    Main = as_list(hb_opts:get(<<"store">>, [], Opts)),
    case store(Opts) of
        undefined -> hb_opts:get(<<"store">>, [], Opts);
        Ess ->
            ReadOnly = [ S#{ <<"access">> => [<<"read">>] } || S <- Ess ],
            case without(Main, Ess) of
                [] -> ReadOnly;
                [First | Rest] -> [First | ReadOnly ++ Rest]
            end
    end.

%% @doc Apply `node_store/1' to a node message and start the essentials store.
node_message(NodeMsg) ->
    case store(NodeMsg) of
        undefined -> NodeMsg;
        Ess ->
            ok = hb_store:start(Ess, #{}, NodeMsg),
            NodeMsg#{ <<"store">> => node_store(NodeMsg) }
    end.

as_list(S) when is_list(S) -> S;
as_list(S) when is_map(S) -> [S];
as_list(_) -> [].

%% Drop from `Stores' every store that names the same instance as one in
%% `Remove' (module and name), including access-restricted copies and the
%% exporter's inner store.
without(Stores, Remove) ->
    Ids = lists:flatmap(fun ids/1, Remove),
    [ S || S <- Stores, not lists:any(fun(I) -> lists:member(I, Ids) end, ids(S)) ].

ids(S = #{ <<"store-module">> := hb_store_export, <<"inner">> := Inner }) ->
    [store_id(S) | ids(Inner)];
ids(S) -> [store_id(S)].

store_id(S = #{ <<"store-module">> := Mod }) ->
    {Mod, maps:get(<<"name">>, S, Mod)};
store_id(S) -> S.

%%% Migration: one-shot copy of the essentials of a node that kept everything
%%% in one store.

%% @doc Copy every essential from `SrcOpts' (the node's existing single store)
%% into the essentials store of `DstOpts'. Runs online: the source is only
%% read, and every copy is an idempotent put of the same row, so it can be
%% rerun after an interruption. Returns counts.
%%
%% The copy itself is `hb_store_gc:collect/4' with nothing computed retained:
%% its root set is exactly the essentials (process definitions, the scheduler
%% namespace, every assignment's closure following both `link:' rows and
%% `+link' keys), so the logic that copied 1,211,993 assignments of the
%% corpus byte-identically is reused rather than re-derived. The small
%% namespaces and trusted-device archives it does not know are copied here.
migrate(SrcOpts, DstOpts) -> migrate(SrcOpts, DstOpts, #{}).
migrate(SrcOpts, RawDstOpts, Policy) ->
    %% Rows are copied raw into the local store; an export wrapping it is told
    %% to write a base image afterwards, since those rows bypassed its journal.
    {DstOpts, Wrapper} =
        case hb_opts:get(<<"store">>, undefined, RawDstOpts) of
            [W = #{ <<"store-module">> := hb_store_export, <<"inner">> := I }] ->
                {RawDstOpts#{ <<"store">> => [I] }, W};
            W = #{ <<"store-module">> := hb_store_export, <<"inner">> := I } ->
                {RawDstOpts#{ <<"store">> => [I] }, W};
            _ -> {RawDstOpts, none}
        end,
    Report =
        hb_store_gc:collect(
            Policy#{
                keep_seconds => 0,
                keep_floor => -1,
                retain_computed => false
            },
            SrcOpts,
            DstOpts
        ),
    Extra = hb_store_gc:copy_essential_namespaces(SrcOpts, DstOpts, Policy),
    case Wrapper of
        none -> ok;
        _ -> ok = hb_store_export:resync(Wrapper)
    end,
    maps:merge(Report, Extra).

%% @doc Byte-for-byte verification of a migration: every assignment of every
%% process, read by its `~scheduler@1.0/assignments/<P>/<N>' path from the
%% source alone and from the destination alone and fully loaded, must be
%% identical, as must every process definition (`<P>'). Returns `{ok, Counts}' or `{error, Mismatches}'.
verify(SrcOpts, DstOpts) -> verify(SrcOpts, DstOpts, all).
verify(SrcOpts, DstOpts, Which) ->
    Procs =
        case Which of
            all -> hb_cache:list(?ASSIGNMENTS, SrcOpts);
            L -> L
        end,
    Full =
        fun(Opts, Path) ->
            case hb_cache:read(Path, Opts) of
                {ok, M} ->
                    term_to_binary(hb_cache:ensure_all_loaded(M, Opts), [deterministic]);
                Other -> {missing, Other}
            end
        end,
    {N, Bad} =
        lists:foldl(
            fun(P, {Count, Errs}) ->
                Slots = hb_cache:list_numbered(<<?ASSIGNMENTS/binary, "/", P/binary>>, SrcOpts),
                Errs1 =
                    case Full(SrcOpts, P) =:= Full(DstOpts, P) of
                        true -> Errs;
                        false -> [{definition, P} | Errs]
                    end,
                lists:foldl(
                    fun(S, {C, E}) ->
                        Path = <<?ASSIGNMENTS/binary, "/", P/binary, "/",
                                 (integer_to_binary(S))/binary>>,
                        case Full(SrcOpts, Path) of
                            {missing, _} = Miss -> {C + 1, [{assignment, P, S, Miss} | E]};
                            Bin ->
                                case Full(DstOpts, Path) of
                                    Bin -> {C + 1, E};
                                    _ -> {C + 1, [{assignment, P, S} | E]}
                                end
                        end
                    end,
                    {Count, Errs1},
                    Slots
                )
            end,
            {0, []},
            Procs
        ),
    case Bad of
        [] -> {ok, #{ processes => length(Procs), assignments => N }};
        _ -> {error, lists:reverse(Bad)}
    end.

%%% Tests

opts_identity_without_essentials_test() ->
    Opts = #{ <<"store">> => [#{ <<"store-module">> => hb_store_lmdb, <<"name">> => <<"x">> }] },
    ?assertEqual(Opts, opts(Opts)),
    ?assertEqual(maps:get(<<"store">>, Opts), node_store(Opts)),
    ?assertEqual(Opts, node_message(Opts)).

node_store_inserts_read_only_after_first_test() ->
    Main = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => <<"main">> },
    Fs = #{ <<"store-module">> => hb_store_fs, <<"name">> => <<"fs">> },
    Ess = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => <<"ess">> },
    Opts = #{ <<"store">> => [Main, Fs], <<"essentials-store">> => Ess },
    ?assertEqual(
        [Main, Ess#{ <<"access">> => [<<"read">>] }, Fs],
        node_store(Opts)
    ),
    % Writers see the essentials store first and the main stores after it,
    % without a second copy of the read-only entry.
    ?assertEqual(
        [Ess, Main, Fs],
        maps:get(<<"store">>, opts(Opts#{ <<"store">> => node_store(Opts) }))
    ).

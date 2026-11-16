%%% @doc This attestor's own computation of PUSD. The attestor acts only on
%%% the state this module computes, and this module uses only what it
%%% verified: of every message it is given (the process, each assignment, each
%%% assignment's message) it keeps just what real signatures vouch for, and
%%% nothing else is read, stored or computed on.
%%%
%%% `view/3' brings the replica forward and returns PUSD's state after the
%%% last verified slot. The scheduler's URL is transport only.
%%%
%%%   - The process: what a real signature vouches for, and only if that is
%%%     the message whose ID is PUSD's.
%%%   - An assignment: what PUSD's scheduler's signature vouches for, if it is
%%%     the next slot of PUSD and its `base-hashpath' continues the chain.
%%%   - Its message: what a real signature on it vouches for, if it names no
%%%     other process as its `target'. A message with no signature at all is
%%%     an unsigned input, exactly as PUSD receives it: the assignment, not
%%%     the message, is what put it in the schedule.
%%%
%%% A real signature is a commitment with a committer (`hb_message:signers/2':
%%% any signature type HB verifies) that verifies; an hmac names nobody and
%%% vouches for nothing. Which of those PUSD believes is PUSD's own rule.
%%%
%%% Only the assignment's provenance, or a signature that fails to verify,
%%% stops the replica: the first such slot is where the replica stays; the attestor
%%% acts on the verified state before it. Accepted slots go to a store of the
%%% replica's own, and PUSD is computed there with options that read nothing
%%% computed elsewhere on the node (no workers, no shared hot cache, no
%%% look-ahead, no joining another request's computation) and with the
%%% result-affecting options pinned to what our nodes run. The cursor (slot,
%%% chain, last assignment, build) is kept beside the store; a cursor that
%%% cannot be read or does not match the store means starting over from slot 0.
%%% The store also holds every slot's computed state, and that is the work of
%%% the build that computed it: a cursor written by another build (or by one
%%% from before cursors named their build) drops the store and replays from
%%% slot 0, so a node upgrade that changes a result is never hidden behind a
%%% result the old code cached.
-module(lib_bridge_replica).
-export([view/3]).
%% For `dev_scheduler_follower', which follows any process this way.
-export([load/4, forward/1, slot_path/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Assignments verified per call of `forward/1', at most: a replica far
%% behind catches up over several calls, acting meanwhile on the verified
%% state it has.
-define(MAX_SLOTS, 5000).
%% Slots `view/3' verifies and computes between looks at its deadline.
-define(SLICE_SLOTS, 25).

%% @doc `{ok, State, Opts, Info}' with PUSD's state after the last verified
%% slot, the options to read it with, and `#{slot, stopped}'; or
%% `{not_yet, Why}' when no slot has verified yet.
%%
%% `deadline' (monotonic ms, default `infinity') bounds the call: the replica
%% is verified and computed `slice' slots at a time, and no slice after the
%% first starts once the deadline has passed, so a call costs the deadline
%% plus one slice at most, however far behind the replica is. A replica cut short says so in
%% `stopped' and picks up there next call. Without that bound a replica
%% dropped by a new build replayed thousands of slots inside one call, and
%% the attestor whose loop made the call passed nothing for an hour
%% (2026-11-16: every node, after the efbce1663 restart).
view(Pusd, Url, Args = #{ opts := NodeOpts, dir := Dir }) ->
    R = catch_up(load(Pusd, Url, NodeOpts, Dir),
        maps:get(slice, Args, ?SLICE_SLOTS), maps:get(deadline, Args, infinity)),
    Info = #{ slot => maps:get(slot, R), stopped => maps:get(stopped, R) },
    case R of
        #{ slot := -1 } -> {not_yet, maps:get(stopped, R)};
        #{ slot := Slot, opts := ROpts } ->
            case compute(R) of
                {ok, State} -> {ok, State, ROpts, Info};
                error -> {not_yet, <<"PUSD did not compute at slot ", (integer_to_binary(Slot))/binary>>}
            end
    end.

%% Verify and compute `Slice' slots at a time until the replica is caught up
%% or stopped, or `Deadline' has passed. Computing each slice caches its
%% state, so the next slice, or the next call, starts from it.
catch_up(R = #{ proc := undefined }, _Slice, _Deadline) -> R;
catch_up(R, Slice, Deadline) ->
    case advance(R, Slice) of
        {R1, 0} ->
            % The slice was used up: the schedule may hold more.
            case compute(R1) =/= error andalso before(Deadline) of
                true -> catch_up(R1, Slice, Deadline);
                false -> R1#{ stopped := catching_up(R1) }
            end;
        {R1, _} -> R1
    end.

before(infinity) -> true;
before(Deadline) -> erlang:monotonic_time(millisecond) < Deadline.

catching_up(#{ scheduler_device := <<"arweave-scheduler@1.0">> }) ->
    <<"catching up to the canonical Arweave schedule">>;
catching_up(_) -> <<"catching up to the schedule">>.

%% PUSD's state at the replica's slot, or `error'.
compute(#{ proc := Proc, slot := Slot, opts := ROpts }) ->
    case hb_ao:resolve(Proc, #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, ROpts) of
        {ok, State} -> {ok, State};
        _ ->
            %% Remote Arweave-root nodes expose the slot route as
            %% compute&slot=N. Keep the original resolver first for
            %% local processes, then use the canonical HTTP-shaped
            %% route before declaring the replica unavailable.
            Path = <<"compute&slot=", (integer_to_binary(Slot))/binary>>,
            case hb_ao:resolve(Proc, #{ <<"path">> => Path }, ROpts) of
                {ok, State2} -> {ok, State2};
                _ -> error
            end
    end.

%%% Vouching ------------------------------------------------------------------

%% @doc `Msg' as far as real signatures vouch for it: only its commitments that
%% have a committer, by one of `Committers' (`any': by anyone), and only the
%% keys they commit; `false' when there is no such commitment or any of them
%% fails to verify.
vouched(Msg, Committers, Opts) when is_map(Msg) ->
    Commitments = hb_maps:get(<<"commitments">>, Msg, #{}, Opts),
    Real = maps:filter(fun(_, C) -> real(C, Committers) end,
        case is_map(Commitments) of true -> Commitments; false -> #{} end),
    Only = Msg#{ <<"commitments">> => Real },
    case map_size(Real) > 0 andalso (catch hb_message:verify(Only, all, Opts)) of
        true ->
            {ok, Kept} = hb_message:with_only_committed(Only, Opts),
            Kept;
        _ -> false
    end;
vouched(_, _, _) -> false.

real(#{ <<"committer">> := Committer }, Committers) ->
    Committers =:= any orelse lists:member(hb_committer:uri(Committer), Committers);
real(_, _) -> false.

%% A scheduled message as PUSD receives it: vouched when it carries a
%% signature, unsigned when it carries none; `false' when a signature on it
%% does not verify.
body(Msg, Opts) when is_map(Msg) ->
    Comms = hb_maps:get(<<"commitments">>, Msg, #{}, Opts),
    Signed = is_map(Comms) andalso
        maps:size(maps:filter(fun(_, C) -> real(C, any) end, Comms)) > 0,
    case Signed of
        true -> vouched(Msg, any, Opts);
        false -> hb_message:uncommitted(Msg)
    end;
body(_, _) -> false.

%% A served body as `body/2' vouches for it, or, when a signed ANS-104 item
%% does not verify as served, the same item fetched again from the scheduler
%% as its raw bytes. A HyperBEAM scheduler serves a stored ANS-104 item's
%% `original-tags' (and a nested item's) as `ao:' links inside its
%% commitment, where the codec decodes no link, so the served form of a
%% signed spawn can never verify (2026-11-16: every lane replica stopped at
%% slot 0 after a rebuild, and no lane move reached the root). The raw item
%% is the bytes its signer signed; it is believed only if its own signature
%% verifies and its signed ID is the one the scheduler served, which the
%% scheduler's signature on the assignment already covers.
served_body(RawBody, canonical_arweave, _R, Opts) -> body(RawBody, Opts);
served_body(RawBody, signed_scheduler, #{ url := Url }, Opts) ->
    case body(RawBody, Opts) of
        false -> raw_item(RawBody, Url, Opts);
        Body -> Body
    end.

raw_item(RawBody, Url, Opts) when is_map(RawBody) ->
    Comms = case hb_maps:get(<<"commitments">>, RawBody, #{}, Opts) of
        M when is_map(M) -> M;
        _ -> #{}
    end,
    IDs = [ID || {ID, C} <- maps:to_list(Comms), real(C, any), ?IS_ID(ID),
        maps:get(<<"commitment-device">>, C, none) =:= <<"ans104@1.0">>],
    case IDs of
        [ID] ->
            try
                {ok, Item} = hb_http:get(Url,
                    #{ <<"path">> => <<"/", ID/binary>>, <<"accept">> => <<"application/ans104">> },
                    Opts#{ <<"http-client">> => httpc, <<"protocol">> => http2 }),
                case vouched(hb_cache:ensure_all_loaded(Item, Opts), any, Opts) of
                    false -> false;
                    Vouched ->
                        case hb_message:id(Vouched, signed, Opts) of
                            ID -> Vouched;
                            _ -> false
                        end
                end
            catch _:_ -> false
            end;
        _ -> false
    end;
raw_item(_, _, _) -> false.

%%% The replica -----------------------------------------------------------------

load(Pusd, Url, NodeOpts, Dir) ->
    Base = <<Dir/binary, "/", Pusd/binary>>,
    Store = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => Base },
    Cursor = <<Base/binary, ".cursor.json">>,
    Build = build(NodeOpts),
    ok = filelib:ensure_dir(binary_to_list(<<Base/binary, "/">>)),
    ok = same_build(Cursor, Build, Store),
    ok = hb_store:start([Store]),
    ROpts = execution_opts(NodeOpts, Store),
    R = #{ pusd => Pusd, url => Url, opts => ROpts, http => NodeOpts,
        cursor => Cursor, build => Build,
        proc => undefined, schedulers => [], scheduler_device => <<"scheduler@1.0">>,
        slot => -1, chain => Pusd, last => none,
        stopped => null },
    resume(process(R)).

%% This node's build: its source commit and when it was built.
build(Opts) ->
    {ok, B} = hb_ao:resolve(#{ <<"device">> => <<"meta@1.0">> }, #{ <<"path">> => <<"build">> }, Opts),
    <<(hb_util:bin(hb_maps:get(<<"source">>, B, <<"unknown">>, Opts)))/binary, "@",
        (hb_util:bin(hb_maps:get(<<"build-time">>, B, 0, Opts)))/binary>>.

%% A store another build computed is dropped before it is opened: the store
%% first, then the cursor, so a halt between them drops it again next time.
same_build(Cursor, Build, Store) ->
    case file:read_file(Cursor) of
        {error, enoent} -> ok;
        {ok, Bin} ->
            case catch hb_json:decode(Bin) of
                #{ <<"build">> := Build } -> ok;
                _ -> drop(Cursor, Store)
            end;
        _ -> drop(Cursor, Store)
    end.

drop(Cursor, Store) ->
    ok = hb_store:reset([Store]),
    ok = file:delete(Cursor).

%% @doc Replayed code gets the same key isolation as public HTTP execution.
%% The attestor retains its signing keys outside the foreign Lua environment.
%%
%% The canonical Arweave index is the one exception to the replica's own
%% store: it is chain data this node's sync verified (recomputed `indep_hash'
%% and `tx_root'), not PUSD state any build computed, so a new build has
%% nothing to recompute in it. Kept in the replica's store it was dropped with
%% every build and re-walked from the anchor, every transaction header of
%% every block through a rate-limited gateway: the root replica sat at slot 7
%% of 207 for hours after each upgrade (2026-11-16), and no root certificate
%% could be checked meanwhile. A node without a dedicated index store still
%% keeps it in the replica's.
execution_opts(NodeOpts, Store) ->
    (maps:without(?NODE_KEY_OPTS ++ [<<"hashpath">>, <<"lua-device-preload">>], NodeOpts))#{
        <<"store">> => [Store],
        <<"scheduler-store">> => [Store],
        <<"arweave-scheduler-store">> =>
            case hb_opts:get(arweave_scheduler_store, not_found, NodeOpts) of
                Index when is_list(Index), Index =/= [] -> Index;
                Index when is_map(Index) -> [Index];
                _ -> [Store]
            end,
        <<"arweave-scheduler-confirmation-depth">> => max(2,
            hb_util:int(hb_opts:get(arweave_scheduler_confirmation_depth, 2, NodeOpts))),
        <<"process-store">> => [Store],
        <<"process-workers">> => false,
        <<"spawn-worker">> => false,
        <<"process-worker-spawn">> => false,
        <<"process-delta-cache">> => false,
        <<"process-now-from-cache">> => false,
        <<"scheduler-lookahead">> => false,
        <<"scheduler-follow-redirects">> => false,
        <<"await-inprogress">> => false,
        % Result-affecting options, pinned to what our nodes run
        % (paralith-mono infra/nodes/node-config.v011.json).
        <<"lua-patch-invalid-patches">> => <<"drop">>,
        <<"lua-device-sandbox">> => []
    }.

replica_does_not_lend_its_validator_key_test() ->
    hb:init(),
    Store = hb_test_utils:test_store(),
    Privileged = #{<<"priv-wallet">> => ar_wallet:new(), <<"store">> => [Store],
        <<"cache-control">> => [<<"no-cache">>, <<"no-store">>],
        <<"identities">> => #{<<"validator">> => ar_wallet:new()}},
    Restricted = execution_opts(Privileged, Store),
    ?assertNot(maps:is_key(<<"priv-wallet">>, Restricted)),
    ?assertNot(maps:is_key(<<"identities">>, Restricted)),
    Script = <<
        "function attempt()\n",
        " local ok, count = pcall(function()\n",
        "  local status, signed = ao.resolve({device='message@1.0'},\n",
        "   {path='commit', ['message-subject']='payload',\n",
        "    payload={action='Root-Approve'}, ['commitment-device']='httpsig@1.0', type='signed'})\n",
        "  if status == 'ok' then return #ao.signers(signed) end\n",
        "  return 0\n",
        " end)\n",
        " return ok and count or 0\n",
        "end\n",
        "function read_value() return ao.get('value', {value='available'}) end"
    >>,
    Base = #{<<"device">> => <<"lua@5.3a">>, <<"module">> =>
        #{<<"content-type">> => <<"application/lua">>, <<"body">> => Script}},
    ?assertEqual({ok, 1}, hb_ao:resolve(Base, <<"attempt">>, Privileged)),
    Denied = hb_ao:resolve(Base, <<"attempt">>, Restricted),
    case Denied of
        {ok, 0} -> ok;
        {failure, #{<<"status">> := 500, <<"body">> := Error}} ->
            % A denied commit may fail while the Lua boundary encodes the
            % host's error tuple. It must specifically be a commit refusal.
            ?assertNotEqual(nomatch, binary:match(Error, <<"cannot_commit">>));
        _ -> ?assertEqual({ok, 0}, Denied)
    end,
    ?assertEqual({ok, <<"available">>}, hb_ao:resolve(Base, <<"read_value">>, Restricted)).

%% The process, stored or fetched, as far as a real signature vouches for it,
%% and only if that is PUSD.
process(R = #{ pusd := Pusd, url := Url, opts := ROpts, http := Opts }) ->
    Candidates = [
        fun() -> {ok, P} = hb_cache:read(Pusd, ROpts), hb_cache:ensure_all_loaded(P, ROpts) end,
        fun() -> {ok, P} = hb_cache:read(Pusd, Opts), hb_cache:ensure_all_loaded(P, Opts) end,
        fun() ->
            {ok, P} = hb_http:get(Url, <<"/", Pusd/binary>>, Opts),
            hb_cache:ensure_all_loaded(P, Opts)
        end
    ],
    case first_process(Candidates, Pusd, Opts) of
        undefined -> R#{ stopped := <<"PUSD's process is not available yet">> };
        Proc ->
            case copy_module(Proc, Opts, ROpts) of
                ok ->
                    {ok, _} = hb_cache:write(Proc, ROpts),
                    SchedulerDevice = hb_maps:get(
                        <<"scheduler-device">>, Proc, <<"scheduler@1.0">>, ROpts),
                    R#{ proc := replica_process(Proc, SchedulerDevice),
                        schedulers := schedulers(Proc, ROpts),
                        scheduler_device := SchedulerDevice };
                _ -> R#{ stopped := <<"PUSD's module is not available yet">> }
            end
    end.

copy_module(Proc, SourceOpts, ReplicaOpts) ->
    case hb_maps:get(<<"module">>, Proc, none, SourceOpts) of
        ModuleID when is_binary(ModuleID), byte_size(ModuleID) =:= 43 ->
            case hb_cache:read(ModuleID, SourceOpts) of
                {ok, RawModule} ->
                    Module = hb_cache:ensure_all_loaded(RawModule, SourceOpts),
                    case catch hb_message:verify(Module, all, SourceOpts) of
                        true ->
                            case hb_cache:write(Module, ReplicaOpts) of
                                {ok, _} -> ok;
                                Error -> Error
                            end;
                        _ -> {error, invalid_module_signature}
                    end;
                Error -> Error
            end;
        _ -> ok
    end.

%% The replica has already verified and copied each assignment into its own
%% local scheduler store. Preserve the signed process as identity, but execute
%% that isolated copy through `scheduler@1.0'; it intentionally has no global
%% Arweave sync record and performs no network scheduling of its own.
replica_process(Proc, <<"arweave-scheduler@1.0">>) ->
    Proc#{
        <<"process">> => Proc,
        <<"scheduler-device">> => <<"scheduler@1.0">>
    };
replica_process(Proc, _) -> Proc.

first_process([], _Pusd, _Opts) -> undefined;
first_process([Get | Rest], Pusd, Opts) ->
    Vouched = vouched(catch Get(), any, Opts),
    case Vouched =/= false andalso (catch hb_message:id(Vouched, all, Opts)) of
        Pusd -> Vouched;
        _ -> first_process(Rest, Pusd, Opts)
    end.

%% Pick up at the cursor if it names what the store holds; otherwise start
%% over from slot 0.
resume(R = #{ proc := undefined }) -> R;
resume(R = #{ cursor := File, pusd := Pusd, opts := ROpts,
        scheduler_device := SchedulerDevice }) ->
    try
        {ok, Bin} = file:read_file(File),
        #{ <<"slot">> := Slot, <<"chain">> := Chain, <<"last">> := Last } = hb_json:decode(Bin),
        {ok, A} = hb_cache:read(slot_path(Pusd, Slot, SchedulerDevice), ROpts),
        Last = hb_message:id(hb_cache:ensure_all_loaded(A, ROpts), signed, ROpts),
        R#{ slot := Slot, chain := Chain, last := Last }
    catch _:_ -> R
    end.

%% Accept assignments until one fails, the scheduler has no more, or this
%% call's share is used.
forward(R = #{ proc := undefined }) -> R;
forward(R) ->
    case advance(R, ?MAX_SLOTS) of
        {R1 = #{ scheduler_device := <<"arweave-scheduler@1.0">> }, 0} ->
            R1#{ stopped := catching_up(R1) };
        {R1, _} -> R1
    end.

%% Accept up to `Left' assignments: `{R, LeftOver}', and a left-over of 0
%% means the share was used up before the schedule was.
advance(R, 0) -> {R, 0};
advance(R = #{ slot := Slot }, Left) ->
    case fetch(R, Slot + 1) of
        {error, Why} -> {R#{ stopped := Why }, Left};
        [] -> {R, Left};
        As -> advance_each(As, R, Left)
    end.

advance_each([], R, Left) -> advance(R, Left);
advance_each(_, R, 0) -> {R, 0};
advance_each([A | Rest], R = #{ slot := Slot }, Left) ->
    case step(A, R) of
        {ok, Kept, Id} -> advance_each(Rest, accept(Kept, Id, R), Left - 1);
        {refused, Why} ->
            ?event(bridge, {replica_stopped, Slot + 1, Why}),
            {R#{ stopped := <<Why/binary, " at slot ", (integer_to_binary(Slot + 1))/binary>> }, Left}
    end.

%% @doc Arweave ordering is constructed locally from confirmed blocks. A peer's
%% unsigned assignment metadata is never proof of inclusion, finality or order.
fetch(#{pusd := Pusd, scheduler_device := <<"arweave-scheduler@1.0">>, opts := Opts}, From) ->
    case catch hb_ao:resolve(#{<<"device">> => <<"arweave-scheduler@1.0">>},
            #{<<"path">> => <<"schedule">>, <<"target">> => Pusd,
                <<"from">> => From}, Opts) of
        {ok, Res} ->
            try
                As = hb_maps:get(<<"assignments">>, Res, #{}, Opts),
                Ordered = lists:keysort(1, [{hb_util:int(K), V} || {K, V} <- hb_maps:to_list(As, Opts)]),
                [{canonical_arweave, hb_cache:ensure_loaded(A, Opts)} || {N, A} <- Ordered, N >= From]
            catch _:_ -> {error, <<"Canonical Arweave assignments are unavailable">>}
            end;
        _ -> {error, <<"Canonical Arweave synchronization is unavailable">>}
    end;
%% The signed assignments a conventional scheduler serves from `From' on.
fetch(#{ pusd := Pusd, url := Url, http := Opts }, From) ->
    Path = <<"/", Pusd/binary, "~process@1.0/schedule?from=", (integer_to_binary(From))/binary>>,
    case catch hb_http:get(Url, Path, Opts#{ <<"http-client">> => httpc, <<"protocol">> => http2 }) of
        {ok, Res} ->
            case catch hb_cache:ensure_all_loaded(
                    hb_ao:get(<<"assignments">>, Res, #{}, Opts#{ <<"hashpath">> => ignore }), Opts) of
                As when is_map(As) ->
                    [A || {N, A} <- lists:keysort(1, [{catch hb_util:int(K), V} || {K, V} <- maps:to_list(As)]),
                        is_integer(N), N >= From];
                _ -> []
            end;
        _ -> []
    end.

%% `A' as the next slot: what PUSD's scheduler vouches for, carrying what the
%% message's signer vouches for, and the ID the chain continues with.
step({canonical_arweave, A}, R = #{scheduler_device := <<"arweave-scheduler@1.0">>}) ->
    step(A, R, canonical_arweave);
step(_A, #{scheduler_device := <<"arweave-scheduler@1.0">>}) ->
    {refused, <<"Arweave assignments must come from local canonical synchronization">>};
step(A, R) -> step(A, R, signed_scheduler).

step(A, R = #{ pusd := Pusd, slot := Slot, chain := Chain, schedulers := Schedulers,
        scheduler_device := SchedulerDevice, http := HTTPOpts, opts := ReplicaOpts }, Origin) ->
    Opts = case Origin of
        canonical_arweave -> ReplicaOpts;
        signed_scheduler -> maps:without(?NODE_KEY_OPTS, HTTPOpts)
    end,
    try
        Kept =
            case SchedulerDevice of
                <<"arweave-scheduler@1.0">> ->
                    need(arweave_assignment(A, Pusd, Slot + 1, Opts),
                        <<"invalid Arweave assignment provenance">>);
                _ ->
                    need(vouched(A, Schedulers, Opts),
                        <<"assignment not signed by the scheduler">>)
            end,
        Get = fun(K) -> hb_maps:get(K, Kept, not_found, Opts) end,
        need(hb_util:int(Get(<<"slot">>)) =:= Slot + 1, <<"slot out of order">>),
        need(hb_util:human_id(Get(<<"process">>)) =:= Pusd, <<"assignment for another process">>),
        need(
            SchedulerDevice =:= <<"arweave-scheduler@1.0">>
                orelse Get(<<"base-hashpath">>) =:= Chain,
            <<"assignment breaks the hash chain">>
        ),
        RawBody = case Origin of
            canonical_arweave -> hb_cache:read_all_commitments(
                hb_cache:ensure_loaded(Get(<<"body">>), Opts), Opts);
            signed_scheduler -> hb_cache:ensure_all_loaded(Get(<<"body">>), Opts)
        end,
        Body = need(served_body(RawBody, Origin, R, Opts),
            <<"message signature does not verify">>),
        % The vouched assignment says whose slot this is; a message that names
        % a process at all must name PUSD.
        need(message_for_process(Body, Pusd, Slot + 1, Opts),
            <<"message is for another process">>),
        AssignmentID =
            case SchedulerDevice of
                <<"arweave-scheduler@1.0">> -> none;
                _ -> hb_message:id(Kept, all, Opts)
            end,
        {ok, Kept#{ <<"body">> => Body }, AssignmentID}
    catch
        throw:{refused, Why} -> {refused, Why};
        Class:Reason:Stacktrace ->
            ?event(bridge, {replica_malformed_assignment,
                {class, Class}, {reason, Reason}, {trace, Stacktrace}}),
            % The public status serves this: a fixed reason, never the call's
            % arguments (node options, keys, RPC URLs). The detail is logged.
            {refused, <<"malformed assignment: ", (atom_to_binary(Class))/binary>>}
    end.

arweave_assignment(A, Pusd, Slot, Opts) when is_map(A) ->
    Get = fun(K) -> hb_maps:get(K, A, not_found, Opts) end,
    Hash = Get(<<"block-hash">>),
    try
        Valid = hb_util:int(Get(<<"slot">>)) =:= Slot
            andalso hb_util:human_id(Get(<<"process">>)) =:= Pusd
            andalso Get(<<"type">>) =:= <<"Assignment">>
            andalso hb_util:int(Get(<<"block-height">>)) >= 0
            andalso hb_util:int(Get(<<"block-index">>)) >= 0
            andalso hb_util:int(Get(<<"timestamp">>)) >= 0
            andalso is_binary(Hash) andalso byte_size(Hash) =:= 64
            andalso validated_block(hb_util:int(Get(<<"block-height">>)), Hash, Opts),
        case Valid of true -> A; false -> false end
    catch _:_ -> false
    end;
arweave_assignment(_, _, _, _) -> false.

%% The block an Arweave assignment names is the block this node's own sync
%% verified at that height (dev_arweave_scheduler_sync: recomputed
%% indep_hash and tx_root, confirmed by its descendants).
validated_block(Height, Hash, Opts) ->
    case lib_arweave_scheduler_blocks:read_block(Height, Opts) of
        {ok, Block} ->
            hb_util:encode(hb_util:decode(
                hb_maps:get(<<"indep-hash">>, Block, <<>>, Opts))) =:= Hash;
        _ -> false
    end.

%% Any signature type HB verifies vouches for a body (an ECDSA L1 tx is one);
%% a body with none is unsigned input; only a signature that fails to verify
%% stops the replica.
body_is_what_pusd_receives_test() ->
    W = ar_wallet:new_ecdsa(),
    TX = ar_tx:sign(#tx{ format = 2, target = crypto:strong_rand_bytes(32),
        tags = [{<<"Action">>, <<"Transfer">>}] }, W),
    Ecdsa = hb_message:convert(TX, <<"structured@1.0">>, <<"tx@1.0">>, #{}),
    ?assert(hb_message:verify(Ecdsa, all, #{})),
    ?assertMatch(#{ <<"action">> := <<"Transfer">> }, body(Ecdsa, #{})),
    Unsigned = #{ <<"action">> => <<"Transfer">>, <<"quantity">> => <<"1">> },
    ?assertEqual(Unsigned, body(Unsigned, #{})),
    Rsa = hb_message:commit(Unsigned, #{ <<"priv-wallet">> => ar_wallet:new() }),
    ?assertMatch(#{ <<"quantity">> := <<"1">> }, body(Rsa, #{})),
    ?assertEqual(false, body(Rsa#{ <<"quantity">> => <<"2">> }, #{})).

%% The replica computes in its own store but reads the node's verified
%% Arweave index, which survives a new build; without one it keeps its own.
replica_reads_the_nodes_arweave_index_test() ->
    Replica = #{ <<"store-module">> => hb_store_volatile, <<"name">> => <<"replica">> },
    Index = #{ <<"store-module">> => hb_store_volatile, <<"name">> => <<"index">> },
    Shared = execution_opts(#{ <<"arweave-scheduler-store">> => [Index] }, Replica),
    ?assertEqual([Index], maps:get(<<"arweave-scheduler-store">>, Shared)),
    ?assertEqual([Replica], maps:get(<<"store">>, Shared)),
    ?assertEqual([Replica], maps:get(<<"scheduler-store">>, Shared)),
    ?assertEqual([Replica], maps:get(<<"process-store">>, Shared)),
    ?assertEqual([Replica], maps:get(<<"arweave-scheduler-store">>, execution_opts(#{}, Replica))).

%% A signed ANS-104 body whose `original-tags' arrive as links (as a node
%% serves a stored item) does not verify; the raw item, fetched by its signed
%% ID, does. A raw item under another ID, or none, is not believed.
served_ans104_body_is_refetched_raw_test_() ->
    {timeout, 60, fun served_ans104_body_is_refetched_raw/0}.

served_ans104_body_is_refetched_raw() ->
    hb:init(),
    Store = hb_test_utils:test_store(),
    NodeOpts = #{ <<"store">> => [Store], <<"priv-wallet">> => ar_wallet:new() },
    Node = hb_http_server:start_node(NodeOpts),
    Signed = hb_message:commit(#{ <<"type">> => <<"Process">>, <<"name">> => <<"lane">>,
        <<"device">> => <<"process@1.0">>, <<"variant">> => <<"ao.N.1">> },
        #{ <<"priv-wallet">> => ar_wallet:new() }, <<"ans104@1.0">>),
    % As a node holds it: parsed from the raw item, with its `original-tags'.
    Item = hb_message:convert(
        hb_message:convert(Signed, <<"ans104@1.0">>, <<"structured@1.0">>, #{}),
        <<"structured@1.0">>, <<"ans104@1.0">>, #{}),
    {ok, _} = hb_cache:write(Item, NodeOpts),
    [{ID, C}] = maps:to_list(maps:get(<<"commitments">>, Item)),
    Tags = maps:get(<<"original-tags">>, C, [1, 2, 3, 4]),
    Served = Item#{ <<"commitments">> => #{ ID => C#{ <<"original-tags">> =>
        [<<"ao:", (hb_util:human_id(crypto:strong_rand_bytes(32)))/binary>> || _ <- Tags] } } },
    Opts = #{},
    ?assertEqual(false, body(Served, Opts)),
    R = #{ url => hb_util:bin(Node) },
    Body = served_body(Served, signed_scheduler, R, Opts),
    ?assertNotEqual(false, Body),
    ?assertEqual(ID, hb_message:id(Body, signed, Opts)),
    ?assertEqual(<<"lane">>, hb_maps:get(<<"name">>, Body, Opts)),
    % Canonical Arweave input is never fetched from a peer.
    ?assertEqual(false, served_body(Served, canonical_arweave, R, Opts)),
    % An ID the scheduler does not hold is not believed.
    Other = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assertEqual(false, served_body(Served#{ <<"commitments">> => #{ Other => C } },
        signed_scheduler, R, Opts)),
    % An item that verifies as served is never refetched.
    ?assertEqual(ID, hb_message:id(served_body(Item, signed_scheduler,
        #{ url => <<"http://127.0.0.1:1">> }, Opts), signed, Opts)).

arweave_assignment_names_a_validated_block_test() ->
    Store = hb_test_utils:test_store(hb_store_volatile, <<"replica-validated">>),
    ok = hb_store:start(Store),
    Opts = #{ <<"store">> => [Store] },
    Pusd = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Hash = hb_util:encode(crypto:strong_rand_bytes(48)),
    A = #{<<"process">> => Pusd, <<"slot">> => 1,
        <<"type">> => <<"Assignment">>, <<"block-height">> => 100,
        <<"block-index">> => 0, <<"timestamp">> => 1700000000,
        <<"block-hash">> => Hash},
    try
        % No block this node verified at that height.
        ?assertEqual(false, arweave_assignment(A, Pusd, 1, Opts)),
        {ok, _} = lib_arweave_scheduler_blocks:write_block(100,
            #{ <<"height">> => 100, <<"indep-hash">> => Hash }, Opts),
        ?assertEqual(A, arweave_assignment(A, Pusd, 1, Opts)),
        % A block hash this node did not verify at that height.
        ?assertEqual(false, arweave_assignment(
            A#{ <<"block-hash">> => hb_util:encode(crypto:strong_rand_bytes(48)) },
            Pusd, 1, Opts))
    after
        hb_store:stop(Store)
    end.

peer_arweave_metadata_is_not_authority_test() ->
    Pusd = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Claimed = #{<<"process">> => Pusd, <<"slot">> => 1,
        <<"type">> => <<"Assignment">>, <<"block-height">> => 100,
        <<"block-index">> => 0, <<"timestamp">> => 1700000000,
        <<"block-hash">> => hb_util:encode(crypto:strong_rand_bytes(48))},
    ?assertMatch({refused, _}, step(Claimed,
        #{scheduler_device => <<"arweave-scheduler@1.0">>})).

%% A slot that crashes a check is refused with the same scrubbed text as a
%% failed pass: the public status never shows a call's arguments.
replica_failure_is_scrubbed_test() ->
    Store = hb_test_utils:test_store(hb_store_volatile, <<"replica-scrubbed">>),
    ok = hb_store:start(Store),
    Pusd = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Secret = <<"https://rpc.example/v2/KEY">>,
    Opts = #{<<"store">> => [Store], <<"rpc">> => Secret},
    Hash = hb_util:encode(crypto:strong_rand_bytes(48)),
    {ok, _} = lib_arweave_scheduler_blocks:write_block(100,
        #{<<"height">> => 100, <<"indep-hash">> => Hash}, Opts),
    % A verified block, and an assignment with no message to load.
    A = #{<<"process">> => Pusd, <<"slot">> => 1,
        <<"type">> => <<"Assignment">>, <<"block-height">> => 100,
        <<"block-index">> => 0, <<"timestamp">> => 1700000000,
        <<"block-hash">> => Hash},
    {refused, Why} = step({canonical_arweave, A}, #{pusd => Pusd, slot => 0,
        chain => none, schedulers => [], http => #{},
        scheduler_device => <<"arweave-scheduler@1.0">>,
        opts => Opts}),
    hb_store:stop(Store),
    ?assertMatch(<<"malformed assignment: ", _/binary>>, Why),
    ?assertEqual(nomatch, binary:match(Why, <<"KEY">>)).

canonical_schedule_is_read_locally_test_() ->
    {timeout, 60, fun canonical_schedule_is_read_locally/0}.

canonical_schedule_is_read_locally() ->
    hb:init(),
    Store = hb_test_utils:test_store(),
    Hook = fun(_, Request, _) ->
        {ok, Request#{<<"body">> => #{<<"status">> => 200,
            <<"content-type">> => <<"application/json">>, <<"body">> => <<"{\"height\":20}">>}}}
    end,
    Node = hb_http_server:start_node(#{<<"port">> => 0,
        <<"on">> => #{<<"response">> => #{<<"device">> => #{<<"response">> => Hook}}}}),
    % The scheduler reads Arweave only from the node's `gateway'.
    Opts = (execution_opts(#{}, Store))#{<<"arweave-scheduler-from">> => 10,
        <<"arweave-scheduler-from-hash">> => <<"anchor">>,
        <<"gateway">> => hb_util:bin(Node)},
    Pusd = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Put = fun(Path, Value) ->
        {ok, ID} = hb_cache:write(Value, Opts),
        ok = hb_cache:link(ID, Path, Opts)
    end,
    Put(<<"~arweave-scheduler@1.0/sync/global">>,
        #{<<"from">> => 10, <<"to">> => 18, <<"from-hash">> => <<"anchor">>}),
    Put(<<"~arweave-scheduler@1.0/sync/process/", Pusd/binary>>,
        #{<<"process">> => Pusd, <<"spawn-ordinate">> => <<"10-0">>,
            <<"synced-to">> => 18, <<"next-slot">> => 2}),
    Canonical = #{<<"process">> => Pusd, <<"slot">> => 1,
        <<"type">> => <<"Assignment">>, <<"body">> => #{<<"source">> => <<"local-index">>}},
    Put(<<"~arweave-scheduler@1.0/assignments/", Pusd/binary, "/1">>, Canonical),
    [{canonical_arweave, Read}] = fetch(#{pusd => Pusd,
        scheduler_device => <<"arweave-scheduler@1.0">>, opts => Opts,
        url => <<"http://127.0.0.1:1">>, http => Opts}, 1),
    Body = hb_cache:ensure_loaded(hb_maps:get(<<"body">>, Read, Opts), Opts),
    ?assertEqual(<<"local-index">>, hb_maps:get(<<"source">>, Body, Opts)).

message_for_process(Body, Pusd, Slot, Opts) ->
    Target = hb_maps:get(<<"target">>, Body, none, Opts),
    AssignTo = hb_maps:get(<<"assign-to">>, Body, none, Opts),
    case {Target, AssignTo, Slot} of
        {T, _, _} when T =/= none ->
            (catch hb_util:human_id(T)) =:= Pusd;
        {none, A, _} when is_binary(A) ->
            lists:member(Pusd, binary:split(A, <<",">>, [global, trim_all]));
        {none, none, 0} ->
            hb_maps:get(<<"type">>, Body, none, Opts) =:= <<"Process">>
                andalso (catch hb_message:id(Body, all, Opts)) =:= Pusd;
        {none, none, _} -> true;
        _ -> false
    end.

need(false, Why) -> throw({refused, Why});
need(true, _) -> true;
need(Value, _) -> Value.

accept(Kept, Id, R = #{ pusd := Pusd, slot := Slot, chain := Chain, proc := Proc, opts := ROpts,
        scheduler_device := SchedulerDevice, http := Opts, cursor := File, build := Build }) ->
    % Kept where the scheduler cache keeps a slot, so PUSD computes from it.
    {ok, _} = hb_cache:write(Kept, ROpts),
    Stored = hb_message:id(Kept, signed, ROpts),
    ok = hb_cache:link(Stored, slot_path(Pusd, Slot + 1, SchedulerDevice), ROpts),
    ok = hb_store:flush(hb_opts:get(store, [], ROpts), ROpts),
    NextChain =
        case SchedulerDevice of
            <<"arweave-scheduler@1.0">> -> Chain;
            _ -> hb_path:hashpath(Chain, Id, hb_path:hashpath_alg(Proc, Opts), Opts)
        end,
    Next = R#{ slot := Slot + 1, last := Stored, chain := NextChain },
    ok = write_durably(File, hb_json:encode(#{ <<"slot">> => Slot + 1,
        <<"chain">> => maps:get(chain, Next), <<"last">> => Stored, <<"build">> => Build })),
    Next.

%% The cursor reaches the disk before it replaces the old one.
write_durably(File, Bin) ->
    Tmp = <<File/binary, ".tmp">>,
    {ok, Fd} = file:open(Tmp, [write, raw, binary]),
    ok = file:write(Fd, Bin),
    ok = file:sync(Fd),
    ok = file:close(Fd),
    file:rename(Tmp, File).

%% Where the scheduler cache keeps a slot (dev_scheduler_cache's layout; that
%% module is another package, so it is not called).
slot_path(Pusd, Slot, _) ->
    hb_path:to_binary([<<"~scheduler@1.0">>, <<"assignments">>, Pusd, integer_to_binary(Slot)]).

%% The schedulers PUSD names, as committer URIs.
schedulers(Proc, Opts) ->
    List = case hb_ao:get_first([{Proc, <<"scheduler">>}, {Proc, <<"scheduler-location">>}], [], Opts) of
        L when is_list(L) -> L;
        B when is_binary(B) -> binary:split(B, <<",">>, [global, trim_all]);
        _ -> []
    end,
    [hb_committer:uri(string:trim(S)) || S <- List].

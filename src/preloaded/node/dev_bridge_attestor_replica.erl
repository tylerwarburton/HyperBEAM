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
%%%   - Its message: what a real signature on it vouches for, if it is
%%%     addressed to PUSD.
%%%
%%% A real signature is a commitment whose algorithm (`type' or `alg') is a
%%% signature algorithm and that verifies; an hmac names whoever it claims to
%%% and vouches for nothing. This is PUSD's own `provenSigner' rule.
%%%
%%% The first assignment that fails is where the replica stays; the attestor
%%% acts on the verified state before it. Accepted slots go to a store of the
%%% replica's own, and PUSD is computed there with options that read nothing
%%% computed elsewhere on the node (no workers, no shared hot cache, no
%%% look-ahead, no joining another request's computation) and with the
%%% result-affecting options pinned to what our nodes run. The cursor (slot,
%%% chain, last assignment) is kept beside the store; a cursor that cannot be
%%% read or does not match the store means starting over from slot 0.
-module(dev_bridge_attestor_replica).
-export([view/3]).
-include("include/hb.hrl").

%% Assignments verified per call, at most: a replica far behind catches up
%% over several passes, acting meanwhile on the verified state it has.
-define(MAX_SLOTS, 5000).
%% Commitment algorithms that are signatures (pusd.lua SIGNATURE_ALGS).
-define(SIGNATURE_ALGS, [<<"rsa-pss-sha512">>, <<"rsa-pss-sha256">>]).

%% @doc `{ok, State, Opts, Info}' with PUSD's state after the last verified
%% slot, the options to read it with, and `#{slot, stopped}'; or
%% `{not_yet, Why}' when no slot has verified yet.
view(Pusd, Url, #{ opts := NodeOpts, dir := Dir }) ->
    R = forward(load(Pusd, Url, NodeOpts, Dir)),
    Info = #{ slot => maps:get(slot, R), stopped => maps:get(stopped, R) },
    case R of
        #{ slot := -1 } -> {not_yet, maps:get(stopped, R)};
        #{ proc := Proc, slot := Slot, opts := ROpts } ->
            case hb_ao:resolve(Proc, #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, ROpts) of
                {ok, State} -> {ok, State, ROpts, Info};
                _ -> {not_yet, <<"PUSD did not compute at slot ", (integer_to_binary(Slot))/binary>>}
            end
    end.

%%% Vouching ------------------------------------------------------------------

%% @doc `Msg' as far as real signatures vouch for it: only its commitments that
%% are a signature algorithm, by one of `Committers' (`any': by anyone), and
%% only the keys they commit; `false' when there is no such commitment or any
%% of them fails to verify.
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

real(C = #{ <<"committer">> := Committer }, Committers) ->
    lists:member(maps:get(<<"type">>, C, maps:get(<<"alg">>, C, none)), ?SIGNATURE_ALGS)
        andalso (Committers =:= any orelse lists:member(hb_committer:uri(Committer), Committers));
real(_, _) -> false.

%%% The replica -----------------------------------------------------------------

load(Pusd, Url, NodeOpts, Dir) ->
    Base = <<Dir/binary, "/", Pusd/binary>>,
    Store = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => Base },
    ok = filelib:ensure_dir(binary_to_list(<<Base/binary, "/">>)),
    ok = hb_store:start([Store]),
    ROpts = (maps:without([<<"hashpath">>, <<"lua-device-preload">>], NodeOpts))#{
        <<"store">> => [Store],
        <<"scheduler-store">> => [Store],
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
    },
    R = #{ pusd => Pusd, url => Url, opts => ROpts, http => NodeOpts,
        cursor => <<Base/binary, ".cursor.json">>,
        proc => undefined, schedulers => [], slot => -1, chain => Pusd, last => none,
        stopped => null },
    resume(process(R)).

%% The process, stored or fetched, as far as a real signature vouches for it,
%% and only if that is PUSD.
process(R = #{ pusd := Pusd, url := Url, opts := ROpts, http := Opts }) ->
    Candidates = [
        fun() -> {ok, P} = hb_cache:read(Pusd, ROpts), hb_cache:ensure_all_loaded(P, ROpts) end,
        fun() ->
            {ok, P} = hb_http:get(Url, <<"/", Pusd/binary>>, Opts),
            hb_cache:ensure_all_loaded(P, Opts)
        end
    ],
    case first_process(Candidates, Pusd, Opts) of
        undefined -> R#{ stopped := <<"PUSD's process is not available yet">> };
        Proc ->
            {ok, _} = hb_cache:write(Proc, ROpts),
            R#{ proc := Proc, schedulers := schedulers(Proc, ROpts) }
    end.

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
resume(R = #{ cursor := File, pusd := Pusd, opts := ROpts }) ->
    try
        {ok, Bin} = file:read_file(File),
        #{ <<"slot">> := Slot, <<"chain">> := Chain, <<"last">> := Last } = hb_json:decode(Bin),
        {ok, A} = hb_cache:read(slot_path(Pusd, Slot), ROpts),
        Last = hb_message:id(hb_cache:ensure_all_loaded(A, ROpts), signed, ROpts),
        R#{ slot := Slot, chain := Chain, last := Last }
    catch _:_ -> R
    end.

%% Accept assignments until one fails, the scheduler has no more, or this
%% call's share is used.
forward(R = #{ proc := undefined }) -> R;
forward(R) -> forward(R, ?MAX_SLOTS).

forward(R, 0) -> R;
forward(R = #{ slot := Slot }, Left) ->
    case fetch(R, Slot + 1) of
        [] -> R;
        As -> forward_each(As, R, Left)
    end.

forward_each([], R, Left) -> forward(R, Left);
forward_each(_, R, 0) -> R;
forward_each([A | Rest], R = #{ slot := Slot }, Left) ->
    case step(A, R) of
        {ok, Kept, Id} -> forward_each(Rest, accept(Kept, Id, R), Left - 1);
        {refused, Why} ->
            ?event(bridge, {replica_stopped, Slot + 1, Why}),
            R#{ stopped := <<Why/binary, " at slot ", (integer_to_binary(Slot + 1))/binary>> }
    end.

%% The assignments the scheduler serves from `From' on, in slot order; none
%% when it cannot be read.
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
step(A, #{ pusd := Pusd, slot := Slot, chain := Chain, schedulers := Schedulers, http := Opts }) ->
    try
        Kept = need(vouched(A, Schedulers, Opts), <<"assignment not signed by the scheduler">>),
        Get = fun(K) -> hb_ao:get(K, Kept, not_found, Opts#{ <<"hashpath">> => ignore }) end,
        need(hb_util:int(Get(<<"slot">>)) =:= Slot + 1, <<"slot out of order">>),
        need(hb_util:human_id(Get(<<"process">>)) =:= Pusd, <<"assignment for another process">>),
        need(Get(<<"base-hashpath">>) =:= Chain, <<"assignment breaks the hash chain">>),
        Body = need(vouched(hb_cache:ensure_all_loaded(Get(<<"body">>), Opts), any, Opts),
            <<"message signature does not verify">>),
        need(hb_maps:get(<<"target">>, Body, none, Opts) =:= Pusd, <<"message is for another process">>),
        {ok, Kept#{ <<"body">> => Body }, hb_message:id(Kept, all, Opts)}
    catch
        throw:{refused, Why} -> {refused, Why};
        _:_ -> {refused, <<"malformed assignment">>}
    end.

need(false, Why) -> throw({refused, Why});
need(true, _) -> true;
need(Value, _) -> Value.

accept(Kept, Id, R = #{ pusd := Pusd, slot := Slot, chain := Chain, proc := Proc, opts := ROpts,
        http := Opts, cursor := File }) ->
    % Kept where the scheduler cache keeps a slot, so PUSD computes from it.
    {ok, _} = hb_cache:write(Kept, ROpts),
    Stored = hb_message:id(Kept, signed, ROpts),
    ok = hb_cache:link(Stored, slot_path(Pusd, Slot + 1), ROpts),
    ok = hb_store:flush(hb_opts:get(store, [], ROpts), ROpts),
    Next = R#{ slot := Slot + 1, last := Stored,
        chain := hb_path:hashpath(Chain, Id, hb_path:hashpath_alg(Proc, Opts), Opts) },
    ok = write_durably(File, hb_json:encode(#{ <<"slot">> => Slot + 1,
        <<"chain">> => maps:get(chain, Next), <<"last">> => Stored })),
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
slot_path(Pusd, Slot) ->
    hb_path:to_binary([<<"~scheduler@1.0">>, <<"assignments">>, Pusd, integer_to_binary(Slot)]).

%% The schedulers PUSD names, as committer URIs.
schedulers(Proc, Opts) ->
    List = case hb_ao:get_first([{Proc, <<"scheduler">>}, {Proc, <<"scheduler-location">>}], [], Opts) of
        L when is_list(L) -> L;
        B when is_binary(B) -> binary:split(B, <<",">>, [global, trim_all]);
        _ -> []
    end,
    [hb_committer:uri(string:trim(S)) || S <- List].

%%% @doc This attestor's own computation of PUSD. The attestor acts only on
%%% the state this module computes, from assignments it verified itself; it
%%% trusts nothing another node publishes.
%%%
%%% `view/3' brings the replica forward and returns PUSD's state after the
%%% last verified slot. It fetches the process and its assignments from the
%%% scheduler's URL, which is transport only: nothing it serves is used until
%%% it verifies.
%%%
%%%   - The process is accepted when its ID is PUSD's.
%%%   - An assignment is accepted when it is the next slot of PUSD, one of the
%%%     schedulers PUSD names signed it, its `base-hashpath' continues the
%%%     chain of the slots before it, and its message carries a signed
%%%     commitment and every commitment on it verifies. The scheduler orders
%%%     messages; it cannot write them.
%%%
%%% The first assignment that is not accepted ends the replica there: it keeps
%%% the state before it and says why. Accepted assignments go to a store of the
%%% replica's own, which nothing else on the node writes (the replica's options
%%% turn off workers, the shared hot cache and look-ahead, and give the
%%% scheduler cache that store), and PUSD is computed there with its own
%%% device. The cursor (slot, chain, last assignment) is kept beside the store.
-module(dev_bridge_attestor_replica).
-export([view/3]).
-include("include/hb.hrl").

%% Assignments verified per call, at most: a replica far behind catches up
%% over several passes, acting meanwhile on the verified state it has.
-define(MAX_SLOTS, 5000).

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

%%% The replica -----------------------------------------------------------------

load(Pusd, Url, NodeOpts, Dir) ->
    Base = <<Dir/binary, "/", Pusd/binary>>,
    Store = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => Base },
    ok = filelib:ensure_dir(binary_to_list(<<Base/binary, "/">>)),
    ok = hb_store:start([Store]),
    ROpts = (maps:remove(<<"hashpath">>, NodeOpts))#{
        <<"store">> => [Store],
        <<"scheduler-store">> => [Store],
        <<"process-store">> => [Store],
        <<"process-workers">> => false,
        <<"spawn-worker">> => false,
        <<"process-worker-spawn">> => false,
        <<"process-delta-cache">> => false,
        <<"process-now-from-cache">> => false,
        <<"scheduler-lookahead">> => false,
        <<"scheduler-follow-redirects">> => false
    },
    R = #{ pusd => Pusd, url => Url, opts => ROpts, http => NodeOpts,
        cursor => <<Base/binary, ".cursor.json">>,
        proc => undefined, schedulers => [], slot => -1, chain => Pusd, last => none,
        stopped => null },
    resume(process(R)).

%% The process: from the replica's store, or fetched and accepted by its ID.
process(R = #{ pusd := Pusd, url := Url, opts := ROpts, http := Opts }) ->
    Proc = case hb_cache:read(Pusd, ROpts) of
        {ok, P} -> hb_cache:ensure_all_loaded(P, ROpts);
        _ ->
            case catch hb_http:get(Url, <<"/", Pusd/binary>>, Opts) of
                {ok, P} ->
                    Loaded = hb_cache:ensure_all_loaded(P, Opts),
                    {ok, Clean} = hb_message:with_only_committed(Loaded, Opts),
                    case catch hb_message:id(Clean, all, Opts) of
                        Pusd -> {ok, _} = hb_cache:write(Clean, ROpts), Clean;
                        _ -> undefined
                    end;
                _ -> undefined
            end
    end,
    case Proc of
        undefined -> R#{ stopped := <<"PUSD's process is not available yet">> };
        _ -> R#{ proc := Proc, schedulers := schedulers(Proc, ROpts) }
    end.

%% Pick up at the cursor, which names the last assignment it accepted.
resume(R = #{ proc := undefined }) -> R;
resume(R = #{ cursor := File, pusd := Pusd, opts := ROpts }) ->
    case file:read_file(File) of
        {ok, Bin} ->
            #{ <<"slot">> := Slot, <<"chain">> := Chain, <<"last">> := Last } = hb_json:decode(Bin),
            Held = case hb_cache:read(slot_path(Pusd, Slot), ROpts) of
                {ok, A} -> hb_message:id(hb_cache:ensure_all_loaded(A, ROpts), signed, ROpts);
                _ -> none
            end,
            case Held of
                Last -> R#{ slot := Slot, chain := Chain, last := Last };
                _ -> R#{ stopped := <<"the replica's store does not hold its cursor">>, done => true }
            end;
        _ -> R
    end.

%% Accept assignments until one is refused, the scheduler has no more, or
%% this call's share is used.
forward(R = #{ proc := undefined }) -> R;
forward(R = #{ done := true }) -> R;
forward(R) -> forward(R, ?MAX_SLOTS).

forward(R, 0) -> R;
forward(R = #{ slot := Slot }, Left) ->
    case fetch(R, Slot + 1) of
        [] -> R;
        As -> forward_each(As, R, Left)
    end.

forward_each([], R, Left) -> forward(R, Left);
forward_each(_, R, 0) -> R;
forward_each([A | Rest], R, Left) ->
    case refusal(A, R) of
        none -> forward_each(Rest, accept(A, R), Left - 1);
        Why ->
            ?event(bridge, {replica_stopped, slot(R) + 1, Why}),
            R#{ stopped := <<Why/binary, " at slot ", (integer_to_binary(slot(R) + 1))/binary>> }
    end.

slot(#{ slot := Slot }) -> Slot.

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

%% Why `A' cannot be the next slot, or `none'.
refusal(A, #{ pusd := Pusd, slot := Slot, chain := Chain, schedulers := Schedulers, http := Opts }) ->
    Get = fun(K) -> hb_ao:get(K, A, not_found, Opts#{ <<"hashpath">> => ignore }) end,
    Checks = [
        {fun() -> hb_util:int(Get(<<"slot">>)) =:= Slot + 1 end, <<"slot out of order">>},
        {fun() -> hb_util:human_id(Get(<<"process">>)) =:= Pusd end, <<"assignment for another process">>},
        {fun() -> lists:any(fun(S) -> hb_message:verify(A, [S], Opts) end, Schedulers) end,
            <<"assignment not signed by the scheduler">>},
        {fun() -> Get(<<"base-hashpath">>) =:= Chain end, <<"assignment breaks the hash chain">>},
        {fun() -> signed_message(hb_cache:ensure_all_loaded(Get(<<"body">>), Opts), Opts) end,
            <<"message signature does not verify">>}
    ],
    hd([Why || {Check, Why} <- Checks, (catch Check()) =/= true] ++ [none]).

%% A real signature is on the message, and every commitment on it verifies.
signed_message(Msg, Opts) ->
    is_map(Msg) andalso hb_message:signers(Msg, Opts) =/= [] andalso hb_message:verify(Msg, all, Opts).

accept(A, R = #{ pusd := Pusd, slot := Slot, chain := Chain, proc := Proc, opts := ROpts,
        http := Opts, cursor := File }) ->
    % Kept where the scheduler cache keeps a slot, so PUSD computes from it.
    {ok, _} = hb_cache:write(A, ROpts),
    Signed = hb_message:id(A, signed, ROpts),
    ok = hb_cache:link(Signed, slot_path(Pusd, Slot + 1), ROpts),
    ok = hb_store:flush(hb_opts:get(store, [], ROpts), ROpts),
    Id = hb_message:id(A, all, Opts),
    Next = R#{ slot := Slot + 1, last := Signed,
        chain := hb_path:hashpath(Chain, Id, hb_path:hashpath_alg(Proc, Opts), Opts) },
    Tmp = <<File/binary, ".tmp">>,
    ok = file:write_file(Tmp, hb_json:encode(#{ <<"slot">> => Slot + 1,
        <<"chain">> => maps:get(chain, Next), <<"last">> => Signed })),
    ok = file:rename(Tmp, File),
    Next.

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

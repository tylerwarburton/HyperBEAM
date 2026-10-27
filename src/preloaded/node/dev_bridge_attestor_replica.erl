%%% @doc This attestor's own replica of PUSD: what it signs comes from here,
%%% never from the published state of the node that runs PUSD.
%%%
%%% Each `sync/1' fetches the assignments after its cursor from PUSD's
%%% scheduler (the `pusd-node') and accepts one only when:
%%%
%%%   - it is the next slot of this process;
%%%   - one of the process's schedulers signed it;
%%%   - its `base-hashpath' continues the chain of the slots before it, so a
%%%     scheduler cannot show this node one history and replace part of it;
%%%   - its body carries a signed commitment, and every commitment on it
%%%     verifies: the scheduler can order messages, not write them.
%%%
%%% Slot 0's body is the process itself, accepted only if its ID is PUSD's.
%%% Accepted assignments go to a store of the replica's own; `state/1'
%%% computes PUSD there with the same device as everywhere else. That store
%%% is never written by anything else on the node: the replica's options turn
%%% off workers, the shared hot cache and look-ahead, and point the scheduler
%%% cache at it. Its cursor (slot, chain, last assignment) is kept beside it
%%% and checked against the store on every start.
%%%
%%% Anything that does not verify halts the replica at the last good slot,
%%% and a replica that could not reach the scheduler's head is behind: either
%%% way `sync/1' says so and the attestor signs nothing.
-module(dev_bridge_attestor_replica).
-export([new/3, sync/1, state/1, slot/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Batches fetched per sync before it reports itself still behind.
-define(MAX_BATCHES, 50).

%% @doc A replica of `Pusd', fed from `Node', kept under `Dir'.
new(Pusd, Node, #{ opts := NodeOpts, dir := Dir }) ->
    Store = #{ <<"store-module">> => hb_store_lmdb,
        <<"name">> => <<Dir/binary, "/", Pusd/binary>> },
    ok = filelib:ensure_dir(binary_to_list(<<Dir/binary, "/", Pusd/binary, "/">>)),
    ok = hb_store:start([Store]),
    ROpts = (maps:remove(<<"hashpath">>, NodeOpts))#{
        <<"store">> => [Store],
        <<"scheduler-store">> => [Store],
        <<"process-store">> => [],
        <<"process-workers">> => false,
        <<"spawn-worker">> => false,
        <<"process-worker-spawn">> => false,
        <<"process-delta-cache">> => false,
        <<"process-now-from-cache">> => false,
        <<"scheduler-lookahead">> => false,
        <<"scheduler-follow-redirects">> => false
    },
    R = #{ pusd => Pusd, node => Node, opts => ROpts, http_opts => NodeOpts,
        cursor_file => <<Dir/binary, "/", Pusd/binary, ".cursor.json">>,
        slot => -1, chain => Pusd, last => undefined, proc => undefined,
        schedulers => [] },
    load_cursor(R).

slot(#{ slot := Slot }) -> Slot.

%% @doc Catch up with the scheduler. `{ok, R}' when every slot it serves has
%% been verified; `{error, Why, R}' when it halted or is behind, with `R'
%% holding what did verify.
sync(R = #{ halted := Why }) -> {error, Why, R};
sync(R) -> sync(R, ?MAX_BATCHES).

sync(R, 0) -> {error, <<"behind: the scheduler has more slots than one sync takes">>, R};
sync(R = #{ slot := Slot }, Batches) ->
    case fetch(R, Slot + 1) of
        {ok, [], _} -> {ok, R};
        {ok, Assignments, More} ->
            case accept(Assignments, R) of
                {ok, R1} when More -> sync(R1, Batches - 1);
                {ok, R1} -> sync(R1, Batches - 1);
                {halt, Why, R1} ->
                    ?event(bridge, {replica_halted, slot(R1) + 1, Why}),
                    {error, Why, R1#{ halted => Why }}
            end;
        {error, Why} ->
            {error, <<"behind: ", Why/binary>>, R}
    end.

%% @doc PUSD's state after the replica's last verified slot.
state(#{ proc := undefined }) -> {error, <<"replica is empty">>};
state(#{ proc := Proc, slot := Slot, opts := ROpts }) ->
    case hb_ao:resolve(Proc, #{ <<"path">> => <<"compute">>, <<"slot">> => Slot }, ROpts) of
        {ok, State} -> {ok, State};
        Other -> {error, iolist_to_binary(io_lib:format("compute failed at ~p: ~0P", [Slot, Other, 6]))}
    end.

%%% Fetching -----------------------------------------------------------------

%% The assignments from `From' on, in slot order, and whether the scheduler
%% said there are more.
fetch(#{ pusd := Pusd, node := Node, http_opts := Opts }, From) ->
    Path = <<"/", Pusd/binary, "~process@1.0/schedule?from=", (integer_to_binary(From))/binary>>,
    try hb_http:get(Node, Path, Opts#{ <<"http-client">> => httpc, <<"protocol">> => http2 }) of
        {ok, Res} ->
            case hb_util:int(hb_ao:get(<<"status">>, Res, 200, Opts)) of
                200 ->
                    Raw = hb_cache:ensure_all_loaded(
                        hb_ao:get(<<"assignments">>, Res, #{}, Opts#{ <<"hashpath">> => ignore }), Opts),
                    List = case is_map(Raw) of
                        true ->
                            [A || {K, A} <- lists:keysort(1,
                                    [{hb_util:int(K), V} || {K, V} <- maps:to_list(Raw),
                                        K =/= <<"priv">>, K =/= <<"commitments">>]),
                                K >= From];
                        false -> []
                    end,
                    More = hb_util:atom(hb_ao:get(<<"continues">>, Res, false, Opts)) =:= true,
                    {ok, List, More};
                Status -> {error, <<"schedule status ", (integer_to_binary(Status))/binary>>}
            end;
        {error, _} -> {error, <<"schedule unreachable">>}
    catch _:_ -> {error, <<"schedule unreadable">>}
    end.

%%% Verifying ----------------------------------------------------------------

accept([], R) -> {ok, R};
accept([A | Rest], R) ->
    case verify(A, R) of
        {ok, R1} -> accept(Rest, R1);
        {halt, Why} -> {halt, Why, R}
    end.

%% @doc Accept the next assignment, or say why not.
verify(A, R = #{ pusd := Pusd, slot := Slot, chain := Chain, opts := ROpts,
        http_opts := Opts }) ->
    Next = Slot + 1,
    Get = fun(K) -> hb_ao:get(K, A, not_found, Opts#{ <<"hashpath">> => ignore }) end,
    Body = hb_cache:ensure_all_loaded(Get(<<"body">>), Opts),
    R1 = case Next of 0 -> R#{ proc => Body }; _ -> R end,
    Schedulers = case Next of 0 -> schedulers(Body, Opts); _ -> maps:get(schedulers, R) end,
    Checks = [
        {fun() -> catch hb_util:int(Get(<<"slot">>)) =:= Next end, <<"slot out of order">>},
        {fun() -> hb_util:human_id(Get(<<"process">>)) =:= Pusd end, <<"assignment is for another process">>},
        {fun() -> Next =/= 0 orelse signed_id(Body, Opts) =:= Pusd end, <<"slot 0 is not this process">>},
        {fun() -> Schedulers =/= [] andalso
            lists:any(fun(S) -> hb_message:verify(A, [S], Opts) end, Schedulers) end,
            <<"assignment not signed by the scheduler">>},
        {fun() -> Get(<<"base-hashpath">>) =:= Chain end, <<"assignment breaks the hash chain">>},
        {fun() -> body_ok(Body, Opts) end, <<"message signature does not verify">>}
    ],
    case failed(Checks) of
        none ->
            ok = dev_scheduler_cache:write(Pusd, A, ROpts),
            Id = hb_util:human_id(hb_message:id(A, all, Opts)),
            Alg = hb_path:hashpath_alg(maps:get(proc, R1), Opts),
            R2 = R1#{ slot := Next, last := Id, schedulers := Schedulers,
                chain := hb_path:hashpath(Chain, Id, Alg, Opts) },
            ok = save_cursor(R2),
            {ok, R2};
        Why ->
            {halt, <<Why/binary, " at slot ", (integer_to_binary(Next))/binary>>}
    end.

failed([]) -> none;
failed([{Check, Why} | Rest]) ->
    case catch Check() of
        true -> failed(Rest);
        _ -> Why
    end.

%% Some commitment on the message is a real signature, and all of them verify.
body_ok(Body, Opts) when is_map(Body) ->
    hb_message:signers(Body, Opts) =/= [] andalso hb_message:verify(Body, all, Opts);
body_ok(_, _) -> false.

signed_id(Msg, Opts) -> hb_util:human_id(hb_message:id(Msg, signed, Opts)).

schedulers(Proc, Opts) ->
    case catch dev_scheduler:parse_schedulers(
            hb_ao:get_first([{Proc, <<"scheduler">>}, {Proc, <<"scheduler-location">>}], [], Opts)) of
        L when is_list(L) -> L;
        _ -> []
    end.

%%% The cursor ----------------------------------------------------------------

save_cursor(#{ cursor_file := File, slot := Slot, chain := Chain, last := Last }) ->
    Tmp = <<File/binary, ".tmp">>,
    ok = file:write_file(Tmp, hb_json:encode(#{ <<"slot">> => Slot, <<"chain">> => Chain,
        <<"last">> => Last })),
    file:rename(Tmp, File).

%% Resume from the cursor, if the store still holds the assignment it names.
%% Otherwise the replica is halted: the store has changed under it.
load_cursor(R = #{ cursor_file := File, pusd := Pusd, opts := ROpts }) ->
    case file:read_file(File) of
        {error, enoent} -> R;
        {ok, Bin} ->
            #{ <<"slot">> := Slot, <<"chain">> := Chain, <<"last">> := Last } = hb_json:decode(Bin),
            Held = case dev_scheduler_cache:read(Pusd, Slot, ROpts) of
                {ok, A} -> hb_util:human_id(hb_message:id(hb_cache:ensure_all_loaded(A, ROpts), all, ROpts));
                _ -> none
            end,
            Proc = case dev_scheduler_cache:read(Pusd, 0, ROpts) of
                {ok, A0} -> hb_cache:ensure_all_loaded(hb_ao:get(<<"body">>, A0, ROpts), ROpts);
                _ -> undefined
            end,
            case Held =:= Last andalso Proc =/= undefined of
                true -> R#{ slot := Slot, chain := Chain, last := Last, proc := Proc,
                    schedulers := schedulers(Proc, ROpts) };
                false -> R#{ halted => <<"replica store does not hold its cursor">> }
            end
    end.

%%% @doc The published state of each process, by key, in memory: what
%%% `GET /<pid>~process@1.0/now/<key>' answers, without resolving the process.
%%%
%%% With `process-now-from-cache' (the node serves `now' from the newest
%%% stored slot), that read loaded the process definition, resolved
%%% `process@1.0' and `now', materialised the newest stored state and resolved
%%% the key on it: ~43k reductions and ~15 ms of CPU a read (bench/msgpath.mjs,
%%% `--poll-ms'). The process cache tells this module every slot it stores
%%% (`update/5', after the store and its hot cache), with the slot's patch
%%% list when the slot is a delta: the rows of the keys the patches name, and
%%% of `results' and `at-slot', which every slot sets, are replaced; a slot
%%% without one (a checkpoint, a full state) replaces them all. A slot older
%%% than the one held is ignored: the view only moves forward, as the head
%%% does.
%%%
%%% `read/3' answers only what the view holds for the process's newest slot,
%%% and only for a request the generic path would answer from that same
%%% state: no `max-age' (in the request or the node's `process-now-max-age'),
%%% `process-now-from-cache' set. Anything else -- a key the view does not
%%% hold, a process it has not seen since the node started -- is `miss', and
%%% the request goes the generic way, so absent keys, errors and status codes
%%% are the generic path's own. The answer is the value as the generic path's
%%% result: forced to a message when the request forces messages
%%% (`hb_ao:force_message/2').
%%%
%%% It also keeps the `results' of each process's newest `?RESULTS_KEEP'
%%% slots, so `GET /<pid>~process@1.0/compute&slot=<n>/results/<path>' -- a
%%% client reading what its own slot answered -- is answered without
%%% copying slot n's whole state out of the hot cache and resolving on it.
%%%
%%% Rows: `{{ProcID, Key}, Value}', `{{ProcID, {results, Slot}}, Results}'
%%% and `{{ProcID, '$slot'}, Slot}'.
-module(hb_process_view).
-export([update/5, read/3, slot/1, forget/1, enabled/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TABLE, hb_process_view).
-define(SLOT, '$slot').
%% Keys every computed slot sets besides its patches (`hb_state_patch:apply/5').
-define(EVERY_SLOT, [<<"results">>, <<"at-slot">>]).
%% Never published by `now' (`dev_process:without_snapshot').
-define(UNSERVED, [<<"snapshot">>, <<"priv">>]).
-define(RESULTS_KEEP, 512).

enabled(Opts) ->
    hb_util:atom(hb_opts:get(<<"process-now-view">>, true, Opts)) =/= false.

%% @doc Record slot `Slot' of `ProcID', its public state `Public'. `Patches'
%% is the slot's patch list, or `full' when the slot is not a delta of the
%% one before it.
update(RawProcID, Slot, Public, Patches, Opts) when is_integer(Slot), is_map(Public) ->
    ProcID = hb_util:human_id(RawProcID),
    case enabled(Opts) of
        false -> ok;
        true ->
            Tab = table(),
            case slot(ProcID) of
                Held when is_integer(Held), Slot =< Held -> ok;
                Held ->
                    Keys =
                        case {Patches, Held} of
                            {_, none} -> all;
                            {full, _} -> all;
                            _ when Held =/= Slot - 1 -> all;
                            _ ->
                                case hb_state_patch:roots(Patches, Opts) of
                                    {ok, Roots} -> lists:usort(Roots ++ ?EVERY_SLOT);
                                    error -> all
                                end
                        end,
                    write(Tab, ProcID, Keys, Public),
                    case maps:find(<<"results">>, Public) of
                        {ok, Results} when is_map(Results) ->
                            ets:insert(Tab, {{ProcID, {results, Slot}}, Results});
                        _ -> ok
                    end,
                    ets:delete(Tab, {ProcID, {results, Slot - ?RESULTS_KEEP}}),
                    ets:insert(Tab, {{ProcID, ?SLOT}, Slot}),
                    ok
            end
    end;
update(_ProcID, _Slot, _Public, _Patches, _Opts) -> ok.

write(Tab, ProcID, all, Public) ->
    % The keys only: the slots' results stay.
    ets:select_delete(Tab, [{{{ProcID, '$1'}, '_'}, [{is_binary, '$1'}], [true]}]),
    ets:insert(Tab,
        [ {{ProcID, K}, V} || {K, V} <- maps:to_list(Public), served(K) ]);
write(Tab, ProcID, Keys, Public) ->
    lists:foreach(
        fun(K) ->
            case served(K) andalso maps:find(K, Public) of
                false -> ok;
                {ok, V} -> ets:insert(Tab, {{ProcID, K}, V});
                error -> ets:delete(Tab, {ProcID, K})
            end
        end,
        Keys
    ).

served(K) when is_binary(K) ->
    not lists:member(K, ?UNSERVED) andalso not hb_private:is_private(K);
served(_) -> false.

%% @doc The slot the view holds for a process, or `none'.
slot(ProcID) ->
    try ets:lookup_element(?TABLE, {ProcID, ?SLOT}, 2)
    catch error:badarg -> none
    end.

%% @doc Drop a process from the view.
forget(ProcID) ->
    try ets:select_delete(?TABLE, [{{{ProcID, '_'}, '_'}, [], [true]}]), ok
    catch error:badarg -> ok
    end.

%% @doc `{ok, Result}' (as `hb_ao:resolve_many/2' gives it) for a `now' read of `Key' of `ProcID' the view can
%% answer as the generic path would, else `miss'. `Req' is the request
%% singleton, `Opts' the request's resolution options.
read(ProcID, Key, Req, Opts) ->
    case servable(Req, Opts) andalso slot(ProcID) =/= none of
        false -> miss;
        true ->
            try ets:lookup(?TABLE, {ProcID, Key}) of
                [{_, Value}] -> hb_ao:force_message({ok, Value}, Opts);
                [] -> miss
            catch error:badarg -> miss
            end
    end.
read(Path, Req, Opts) ->
    case parse(Path) of
        {ok, ProcID, Key} -> read(ProcID, Key, Req, Opts);
        {results, ProcID, Slot, Parts} -> read_results(ProcID, Slot, Parts, Req, Opts);
        miss -> miss
    end.

%% A value under the `results' of a slot the view holds, at or below its
%% newest: what the generic path reads from the slot's stored state.
read_results(ProcID, Slot, Parts, Req, Opts) ->
    case enabled(Opts) andalso unforced(Req) andalso slot(ProcID) of
        Held when is_integer(Held), Slot =< Held ->
            try ets:lookup(?TABLE, {ProcID, {results, Slot}}) of
                [{_, Results}] ->
                    case walk(Parts, Results) of
                        {ok, Value} -> hb_ao:force_message({ok, Value}, Opts);
                        miss -> miss
                    end;
                [] -> miss
            catch error:badarg -> miss
            end;
        _ -> miss
    end.

walk([], Value) -> {ok, Value};
walk([Part | Rest], Map) when is_map(Map) ->
    case maps:find(Part, Map) of
        {ok, Value} -> walk(Rest, Value);
        error -> miss
    end;
walk(_Parts, _Value) -> miss.

unforced(Req) ->
    not (is_map(Req) andalso
        (is_map_key(<<"only-if-cached">>, Req) orelse is_map_key(<<"cache-control">>, Req))).

servable(Req, Opts) ->
    enabled(Opts) andalso
        hb_util:atom(hb_opts:get(process_now_from_cache, false, Opts)) == true andalso
        hb_opts:get(process_now_max_age, infinity, Opts) == infinity andalso
        not (is_map(Req) andalso
                (is_map_key(<<"max-age">>, Req) orelse
                 is_map_key(<<"only-if-cached">>, Req) orelse
                 is_map_key(<<"cache-control">>, Req))).

%% @doc `/<43-character id>~process@1.0/now/<key>', the key a single path
%% part in its normal (lowercase) form and the request without a query.
parse(<<"/", ID:43/binary, "~process@1.0/now/", Key/binary>>) when byte_size(Key) > 0 ->
    case hb_util:is_human_id(ID) andalso binary:match(Key, [<<"/">>, <<"?">>, <<"&">>, <<"~">>, <<"+">>]) == nomatch
            andalso hb_util:to_lower(Key) == Key of
        true -> {ok, ID, Key};
        false -> miss
    end;
parse(<<"/", ID:43/binary, "~process@1.0/compute&slot=", Rest/binary>>) ->
    case binary:split(Rest, <<"/">>, [global]) of
        [SlotBin, <<"results">> | Parts] when Parts =/= [] ->
            Simple =
                lists:all(
                    fun(P) ->
                        byte_size(P) > 0 andalso hb_util:to_lower(P) == P andalso
                            binary:match(P, [<<"?">>, <<"&">>, <<"~">>, <<"+">>, <<"=">>]) == nomatch
                    end,
                    Parts
                ),
            case hb_util:is_human_id(ID) andalso Simple andalso hb_util:safe_int(SlotBin) of
                {ok, Slot} when Slot >= 0 -> {results, ID, Slot, Parts};
                _ -> miss
            end;
        _ -> miss
    end;
parse(_) -> miss.

%% The table is owned by a process that never exits.
table() ->
    case ets:whereis(?TABLE) of
        undefined ->
            Self = self(),
            Ref = make_ref(),
            spawn(fun() ->
                try ets:new(?TABLE, [named_table, public, set,
                        {read_concurrency, true}, {write_concurrency, true}]) of
                    _ -> Self ! Ref, receive never -> ok end
                catch error:badarg -> Self ! Ref
                end
            end),
            receive Ref -> ok end,
            ets:whereis(?TABLE);
        Tab -> Tab
    end.

%%% Tests

update_and_read_test() ->
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Opts = #{ <<"process-now-from-cache">> => true },
    ok = update(P, 1, #{ <<"count">> => <<"1">>, <<"keep">> => <<"k">>,
                         <<"snapshot">> => <<"s">>, <<"at-slot">> => 1 }, full, Opts),
    ?assertEqual({ok, #{ <<"ao-result">> => <<"body">>, <<"body">> => <<"1">> }}, read(P, <<"count">>, #{}, Opts)),
    ?assertEqual(miss, read(P, <<"snapshot">>, #{}, Opts)),
    ok = update(P, 2, #{ <<"count">> => <<"2">>, <<"keep">> => <<"k">>, <<"at-slot">> => 2 },
        [#{ <<"path">> => <<"/count">>, <<"value">> => <<"2">> }], Opts),
    ?assertEqual({ok, #{ <<"ao-result">> => <<"body">>, <<"body">> => <<"2">> }}, read(P, <<"count">>, #{}, Opts)),
    ?assertEqual({ok, #{ <<"ao-result">> => <<"body">>, <<"body">> => 2 }}, read(P, <<"at-slot">>, #{}, Opts)),
    ?assertEqual({ok, #{ <<"ao-result">> => <<"body">>, <<"body">> => <<"k">> }}, read(P, <<"keep">>, #{}, Opts)),
    % Older slots never move the view back.
    ok = update(P, 1, #{ <<"count">> => <<"1">> }, full, Opts),
    ?assertEqual({ok, #{ <<"ao-result">> => <<"body">>, <<"body">> => <<"2">> }}, read(P, <<"count">>, #{}, Opts)),
    % A deleted key is gone; a request with max-age goes the generic way.
    ok = update(P, 3, #{ <<"keep">> => <<"k">>, <<"at-slot">> => 3 },
        [#{ <<"path">> => <<"/count">>, <<"delete">> => true }], Opts),
    ?assertEqual(miss, read(P, <<"count">>, #{}, Opts)),
    ?assertEqual(miss, read(P, <<"keep">>, #{ <<"max-age">> => <<"5">> }, Opts)),
    % A gap in the slots rewrites every key.
    ok = update(P, 7, #{ <<"other">> => <<"o">>, <<"at-slot">> => 7 },
        [#{ <<"path">> => <<"/other">>, <<"value">> => <<"o">> }], Opts),
    ?assertEqual(miss, read(P, <<"keep">>, #{}, Opts)),
    ?assertEqual({ok, #{ <<"ao-result">> => <<"body">>, <<"body">> => <<"o">> }}, read(P, <<"other">>, #{}, Opts)),
    forget(P).

results_test() ->
    P = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Opts = #{},
    R = fun(N) -> #{ <<"output">> => #{ <<"data">> => integer_to_binary(N) } } end,
    ok = update(P, 1, #{ <<"results">> => R(1), <<"at-slot">> => 1 }, full, Opts),
    ok = update(P, 2, #{ <<"results">> => R(2), <<"at-slot">> => 2 },
        [#{ <<"path">> => <<"/x">>, <<"value">> => <<"1">> }], Opts),
    Path = fun(N, Rest) -> <<"/", P/binary, "~process@1.0/compute&slot=",
        (integer_to_binary(N))/binary, "/results/", Rest/binary>> end,
    Body = fun(V) -> {ok, #{ <<"ao-result">> => <<"body">>, <<"body">> => V }} end,
    ?assertEqual(Body(<<"1">>), read(Path(1, <<"output/data">>), #{}, Opts)),
    ?assertEqual(Body(<<"2">>), read(Path(2, <<"output/data">>), #{}, Opts)),
    ?assertEqual({ok, #{ <<"data">> => <<"2">> }}, read(Path(2, <<"output">>), #{}, Opts)),
    % Ahead of the view, absent, or asked the generic way: a miss.
    ?assertEqual(miss, read(Path(3, <<"output/data">>), #{}, Opts)),
    ?assertEqual(miss, read(Path(2, <<"absent">>), #{}, Opts)),
    ?assertEqual(miss, read(Path(2, <<"output/data">>), #{ <<"only-if-cached">> => true }, Opts)),
    % A full rewrite of the keys keeps the slots' results.
    ok = update(P, 9, #{ <<"results">> => R(9), <<"at-slot">> => 9 }, full, Opts),
    ?assertEqual(Body(<<"2">>), read(Path(2, <<"output/data">>), #{}, Opts)),
    forget(P).

parse_test() ->
    ID = hb_util:human_id(crypto:strong_rand_bytes(32)),
    ?assertEqual({ok, ID, <<"count">>}, parse(<<"/", ID/binary, "~process@1.0/now/count">>)),
    ?assertEqual(miss, parse(<<"/", ID/binary, "~process@1.0/now/a/b">>)),
    ?assertEqual(miss, parse(<<"/", ID/binary, "~process@1.0/now/Count">>)),
    ?assertEqual(miss, parse(<<"/", ID/binary, "~process@1.0/now">>)),
    ?assertEqual(miss, parse(<<"/", ID/binary, "~process@1.0/compute/count">>)).

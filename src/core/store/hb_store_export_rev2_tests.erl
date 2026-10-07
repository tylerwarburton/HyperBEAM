%%% The fourth review's export fuzz, adopted: definitions, nested
%%% sub-messages, mutable ~location@1.0 and upload marks, killed helpers,
%%% random resyncs. `REV_ROUNDS=<n>' (skipped when unset).
-module(hb_store_export_rev2_tests).
-include_lib("eunit/include/eunit.hrl").
-compile([export_all, nowarn_export_all]).

key(P, I) -> <<"~scheduler@1.0/assignments/", P/binary, "/", (integer_to_binary(I))/binary>>.
loc(P) -> <<"~location@1.0/", P/binary>>.
mark(P) -> <<"~scheduler@1.0/uploaded/", P/binary>>.

writer(Store) ->
    case ets:lookup(hb_store_export_registry, maps:get(<<"name">>, Store)) of
        [{_, Pid, _}] -> Pid; _ -> none
    end.

settle(Store, 0) -> hb_store_export:status(Store);
settle(Store, K) ->
    St = (catch hb_store_export:sync_export(Store)),
    case is_map(St) andalso {maps:get(catchup_running, St, false), maps:get(dropping, St, false),
          maps:get(local_segments, St, 0), maps:get(open_gaps, St, [])} of
        {false, false, 0, []} -> St;
        _ -> timer:sleep(200), settle(Store, K - 1)
    end.

fuzz_test_() ->
    {timeout, 7200, fun() ->
        N = list_to_integer(os:getenv("REV_ROUNDS", "0")),
        Rs = [ round_(I) || I <- lists:seq(1, N) ],
        io:format(user, "~nREV_EXPORT_FUZZ rounds=~p exact=~p loud_fail=~p~n",
                  [N, length([ x || ok <- Rs ]), length([ x || {failed, _} <- Rs ])]),
        ?assertEqual(N, length([ x || ok <- Rs ]))
    end}.

round_(Round) ->
    Seed = erlang:unique_integer([positive]),
    rand:seed(exsss, {Seed, Round, 23}),
    application:ensure_all_started(hb),
    Dir = "cache-TEST/rev-ex2-" ++ integer_to_list(Seed),
    Remote = Dir ++ "/remote",
    Inner = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => hb_util:bin(Dir ++ "/lmdb") },
    MaxPending = lists:nth(rand:uniform(3), [10, 40, 200]),
    Store = hb_store_export:wrap(Inner, #{
        <<"path">> => hb_util:bin(Remote),
        <<"journal">> => hb_util:bin(Dir ++ "/journal"),
        <<"max-pending">> => MaxPending,
        <<"segment-bytes">> => 65536, <<"segment-ms">> => 200, <<"ship-interval-ms">> => 20 }),
    Opts = #{ <<"store">> => [Store] },
    ok = hb_store:start([Store], #{}, Opts),
    Procs = [ hb_util:human_id(crypto:strong_rand_bytes(32)) || _ <- lists:seq(1, 6) ],
    Self = self(),
    Writers =
        [ spawn_link(fun() ->
              % A definition first, written as the scheduler does (message at
              % its ID), then assignments; a mutable location record and an
              % upload mark that goes up and, sometimes, back.
              {ok, _} = hb_cache:write(#{ <<"type">> => <<"Process">>, <<"p">> => P,
                                           <<"seed">> => crypto:strong_rand_bytes(40) }, Opts),
              Loop = fun L(I) ->
                  receive {stop, From} -> From ! {written, P, I - 1}
                  after 0 ->
                      {ok, ID} = hb_cache:write(#{ <<"v">> => integer_to_binary(I), <<"p">> => P,
                                                   <<"nested">> => #{ <<"b">> => crypto:strong_rand_bytes(60) } }, Opts),
                      ok = hb_store:link([Store], #{ key(P, I) => ID }, Opts),
                      ok = hb_store:write([Store], #{ loc(P) => integer_to_binary(I rem 3) }, Opts),
                      ok = hb_store:write([Store], #{ mark(P) => integer_to_binary(max(0, I - rand:uniform(2))) }, Opts),
                      case rand:uniform(50) of 1 -> timer:sleep(rand:uniform(5)); _ -> ok end,
                      L(I + 1)
                  end
              end,
              Loop(0)
          end)
        || P <- Procs ],
    Deadline = erlang:monotonic_time(millisecond) + 20000,
    Chaos =
        fun C(Ev) ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true -> Ev;
                false ->
                    timer:sleep(rand:uniform(300)),
                    Off = string:tokens(os:getenv("REV_OFF", ""), ","),
                    Pick0 = rand:uniform(8),
                    Pick = case lists:member(integer_to_list(Pick0), Off) of true -> 8; false -> Pick0 end,
                    E = case Pick of
                            1 -> case writer(Store) of
                                     W when is_pid(W) -> catch erlang:suspend_process(W),
                                         timer:sleep(rand:uniform(50)), exit(W, kill), crash;
                                     _ -> none end;
                            2 -> case writer(Store) of W when is_pid(W) -> exit(W, kill), kill; _ -> none end;
                            3 -> % kill a helper (catch-up or target worker) linked to the writer
                                case writer(Store) of
                                    W when is_pid(W) ->
                                        case catch erlang:process_info(W, links) of
                                            {links, L} ->
                                                Hs = [ X || X <- L, is_pid(X), X =/= self(),
                                                            not lists:member(X, Writers) ],
                                                case Hs of [] -> none;
                                                    _ -> exit(lists:nth(rand:uniform(length(Hs)), Hs), kill), helper
                                                end;
                                            _ -> none
                                        end;
                                    _ -> none
                                end;
                            4 -> case filelib:is_dir(Remote) andalso
                                          not filelib:is_file(Remote ++ ".off") of
                                     true -> ok = file:rename(Remote, Remote ++ ".off"),
                                             case file:write_file(Remote, <<"down">>) of
                                                 ok -> outage;
                                                 Er -> erlang:error({harness_outage_failed, Er})
                                             end;
                                     false -> none end;
                            5 -> up(Remote), up;
                            6 -> catch hb_store_export:shutdown_all(), clean_stop;
                            7 -> catch hb_store_export:resync(Store), resync;
                            8 -> none
                        end,
                    C([E | Ev])
            end
        end,
    Events = Chaos([]),
    [ W ! {stop, Self} || W <- Writers ],
    Written = maps:from_list([ receive {written, P, N} -> {P, N} end || P <- Procs ]),
    up(Remote),
    Fin = settle(Store, 400),
    Target = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => hb_util:bin(Dir ++ "/restored") },
    Result = (catch hb_store_export:restore(Remote, Target, #{ <<"require-definitions">> => false })),
    Counts = maps:from_list([ {K, length([ x || X <- Events, X == K ])}
                              || K <- [crash, kill, helper, outage, up, clean_stop, resync] ]),
    Out = verdict(Result, Store, Inner, Target, Procs, Written, Round, MaxPending, Counts, Fin, Remote, Dir),
    % Each round's LMDB environments map 2 TiB of address space: close them,
    % or ~30 rounds exhaust the 128 TiB a process has and the next open fails.
    catch hb_store:stop([Store], #{}, Opts),
    catch hb_store:stop([Target], #{}, #{}),
    Out.

verdict(Result, Store, Inner, Target, Procs, Written, Round, MaxPending, Counts, Fin, Remote, Dir) ->
    _ = Store,
    case Result of
        {ok, _} ->
            TOpts = #{ <<"store">> => [Target] },
            Snap = fun(O) -> [ {P, I, case catch hb_cache:read(key(P, I), O) of
                                         {ok, M} -> catch hb_cache:ensure_all_loaded(M, O);
                                         Er -> {missing, Er} end}
                               || P <- Procs, I <- lists:seq(0, maps:get(P, Written)) ]
                            ++ [ {P, K, catch hb_store:read([St], K, #{})}
                               || P <- Procs, K <- [loc(P), mark(P)],
                                  St <- [case O of #{ <<"store">> := [X] } -> X end] ] end,
            Local = Snap(#{ <<"store">> => [Inner] }),
            Rest = Snap(TOpts),
            Diff = [ {A, B} || {A, B} <- lists:zip(Local, Rest), A =/= B ],
            io:format(user, "~nREV_ROUND ~p mp=~p ev=~p written=~p restore=ok diff=~p ~P~n",
                      [Round, MaxPending, Counts, lists:sum(maps:values(Written)), length(Diff),
                       [ {element(2,A), element(3,A), element(3,B)} || {A,B} <- lists:sublist(Diff, 3)], 12]),
            case Diff of [] -> ok; _ -> {diff, length(Diff)} end;
        Other ->
            io:format(user, "~nREV_ROUND ~p mp=~p ev=~p restore=FAILED ~P settle=~P~n",
                      [Round, MaxPending, Counts, case Other of {'EXIT', {Rsn, _}} -> Rsn; _ -> Other end, 40, maps:with([open_gaps, catchup_running, dropping, local_segments, errors, last_error], Fin), 10]),
            catch investigate(Other, Inner, Remote, Dir),
            {failed, Other}
    end.

investigate({'EXIT', {{export_assignment_unreadable, P, Sl, Why}, _}}, Inner, Remote, _Dir) ->
    #{ <<"db">> := DB } = hb_store:find(Inner),
    AKey = key(P, Sl),
    {ok, AVal} = elmdb:get(DB, AKey),
    ID = case Why of
             {necessary_message_not_found, _, Txt} ->
                 [_, I] = binary:split(Txt, <<"): ">>), I;
             _ -> <<"link:", A/binary>> = AVal, A
         end,
    LocalRows = case elmdb:read_prefix(DB, ID) of {ok, R} -> length(R); E -> E end,
    {ok, Names} = file:list_dir(Remote),
    Hits = lists:flatmap(
        fun(N) ->
            case file:read_file(filename:join(Remote, N)) of
                {ok, B0} when byte_size(B0) > 8 ->
                    B = case B0 of <<31,139,_/binary>> -> zlib:gunzip(B0); _ -> B0 end,
                    Recs = recs(B),
                    [ {N, element(1, R)} || R <- Recs, mentions(R, ID) ] ++
                    [ {N, akey} || R <- Recs, mentions(R, AKey) ];
                _ -> []
            end
        end, lists:sort(Names)),
    io:format(user, "~nINVESTIGATE slot=~p id=~p local_rows=~p remote_hits=~p files=~p~n",
              [Sl, ID, LocalRows, lists:usort(Hits), lists:sort(Names)]).

recs(<<Len:32, _C:32, B:Len/binary, Rest/binary>>) ->
    T = try binary_to_term(B) catch _:_ -> bad end,
    case T of
        {raw, Rows} when is_list(Rows) -> [ {raw, K, V} || {K, V} <- Rows ];
        {Op, M} when is_map(M) -> [ {Op, K, V} || {K, V} <- maps:to_list(M) ];
        {Op, L} when is_list(L) -> [ {Op, X, x} || X <- L ];
        O -> [{other, O, x}]
    end ++ recs(Rest);
recs(_) -> [].

mentions({_, K, _}, ID) when is_binary(K) ->
    binary:match(K, ID) =/= nomatch;
mentions(_, _) -> false.

up(Remote) ->
    case filelib:is_dir(Remote ++ ".off") of
        true -> ok = file:delete(Remote), ok = file:rename(Remote ++ ".off", Remote);
        false -> ok
    end.

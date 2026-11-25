%%% @doc A device that renders a REPL-like interface for AO-Core via HTML.
-module(dev_hyperbuddy).
-export([device_info/1, format/3, return_file/2]).
-export([metrics/3, events/3, slots/3, trace/3, phases/3, pstate/3]).
-export([throw/3]).
-include_lib("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Export an explicit list of files via http. Filenames added to the 
%% `hyperbuddy-serve' key of the node message will be served as static files.
%% Each filename must point to a path relative to the HyperBEAM instance's
%% build subdirectory as follows: `priv/html/hyperbuddy@1.0'.
device_info(Opts) ->
    ServedRoutes = hb_opts:get(<<"hyperbuddy-serve">>, #{}, Opts),
    #{
        default => fun serve/4,
        serve => ServedRoutes#{
            % Default message viewer page:
            <<"index">> => <<"index.html">>,
            <<"bundle.js">> => <<"bundle.js">>,
            <<"fonts.css">> => <<"fonts.css">>,
            <<"favicon-light.png">> => <<"favicon-light.png">>,
            <<"favicon-dark.png">> => <<"favicon-dark.png">>,
            <<"font-dm-sans-italic.ttf">> => <<"font-dm-sans-italic.ttf">>,
            <<"font-dm-sans-variable.ttf">> => <<"font-dm-sans-variable.ttf">>,
            <<"font-geist-mono-variable.ttf">> => <<"font-geist-mono-variable.ttf">>,
            % Error pages:
            <<"404.html">> => <<"404.html">>,
            <<"500.html">> => <<"500.html">>,
            <<"styles.css">> => <<"styles.css">>,
            <<"script.js">> => <<"script.js">>
        },
        excludes => [<<"return_file">>]
    }.

%% @doc The main HTML page for the REPL device.
-spec metrics(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ body := binary(), _ => _ }}.
metrics(_, Req, Opts) ->
    case hb_opts:get(prometheus, not hb_features:test(), Opts) of
        true ->
            {_, HeaderList, Body} =
            prometheus_http_impl:reply(
                #{path => true,
                headers => 
                    fun(Name, Default) ->
                        hb_ao:get(Name, Req, Default, Opts)
                    end,
                registry => prometheus_registry:exists(<<"default">>),
                standalone => false}
            ),
            RawHeaderMap =
                hb_maps:from_list(
                    prometheus_cowboy:to_cowboy_headers(HeaderList)
                ),
            Headers =
                hb_maps:map(
                    fun(_, Value) -> hb_util:bin(Value) end,
                    RawHeaderMap,
					Opts
                ),
            % Prometheus states its exposition format in a `version' parameter.
            % `0.0.4' is not a valid structured-field value, so the parameter is
            % dropped and the bare media type served.
            {ok,
                Headers#{
                    <<"content-type">> => <<"text/plain">>,
                    <<"body">> => Body
                }
            };
        false ->
            {ok, #{ <<"body">> => <<"Prometheus metrics disabled.">> }}
    end.

%% @doc Per-process slot timing as JSON: cumulative counters per process and
%% per action (`hb_slot_log'), percentiles over the ring rows inside `window'
%% seconds (10-600, default 60), each process's worker phases, scheduled and
%% computed slots and mailbox, and the node's phase totals and delivery
%% backlog. `pid' names one process. Reads ETS only; nothing blocks.
-spec slots(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) -> {ok, #{ _ => _ }}.
slots(_, Req, _Opts) ->
    Window =
        case catch hb_util:int(maps:get(<<"window">>, Req, 60)) of
            W when is_integer(W) -> max(10, min(600, W));
            _ -> 60
        end,
    Named =
        case maps:get(<<"pid">>, Req, undefined) of
            P when is_binary(P), byte_size(P) > 0 -> P;
            _ -> undefined
        end,
    Encode = fun() -> hb_json:encode(slots_body(Window, Named)) end,
    % The whole-node scrape (no `pid') is served from a 1 s memo, so any
    % number of readers costs at most one report a second.
    Encoded =
        case Named of
            undefined -> hb_slot_log:memo({slots, Window}, 1000, Encode);
            _ -> Encode()
        end,
    {ok, #{
        <<"content-type">> => <<"application/json">>,
        <<"cache-control">> => <<"no-store">>,
        <<"body">> => Encoded
    }}.

%% @doc Every node-wide phase timer (`hb_phase_timer') as JSON: per phase its
%% count, total and mean, and the p50, p90, p99 of its log2 histogram (each
%% the upper bound of its bucket, so exact to a factor of two), with the
%% scheduler run queues as they are now. Reads ETS only.
-spec phases(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) -> {ok, #{ _ => _ }}.
phases(_, _Req, _Opts) ->
    {ok, #{
        <<"content-type">> => <<"application/json">>,
        <<"cache-control">> => <<"no-store">>,
        <<"body">> => hb_json:encode(phases_body())
    }}.

phases_body() ->
    Normal = erlang:system_info(schedulers),
    Queues = erlang:statistics(run_queue_lengths_all),
    {Sched, Dirty} = lists:split(min(Normal, length(Queues)), Queues),
    #{
        <<"v">> => 1,
        <<"at">> => erlang:system_time(millisecond),
        <<"uptime_s">> => element(1, erlang:statistics(wall_clock)) div 1000,
        % Work done by every process since the node started: per message, a
        % measure of the Erlang-side cost that other load on the host does
        % not inflate, as CPU time does. NIF time (RSA, LMDB) is not in it.
        <<"reductions">> => element(1, erlang:statistics(exact_reductions)),
        <<"run_queue">> => #{
            <<"schedulers">> => lists:sum(Sched),
            <<"dirty_cpu">> => lists:nth(1, Dirty ++ [0]),
            <<"dirty_io">> => lists:nth(2, Dirty ++ [0, 0])
        },
        <<"phases">> =>
            maps:from_list(
                [ {hb_util:bin(P), phase_stats(T, N, Buckets)}
                || {P, T, N, Buckets} <- hb_phase_timer:read_hist() ])
    }.

phase_stats(T, N, Buckets) ->
    [P50, P90, P99] = hb_phase_timer:percentiles(Buckets, [0.5, 0.9, 0.99]),
    #{
        <<"count">> => N,
        <<"total_us">> => T,
        <<"mean_us">> => T div max(N, 1),
        <<"p50_us">> => P50,
        <<"p90_us">> => P90,
        <<"p99_us">> => P99
    }.

%% @doc The message trace (`hb_msg_trace') as JSON: the rows after cursor
%% `since' (default 0), oldest first, at most `limit' (1-5000, default
%% 1000), with `next', the cursor to ask from next time. Reads ETS only.
-spec trace(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) -> {ok, #{ _ => _ }}.
trace(_, Req, _Opts) ->
    Int =
        fun(Key, Default) ->
            case catch hb_util:int(maps:get(Key, Req, Default)) of
                N when is_integer(N), N >= 0 -> N;
                _ -> Default
            end
        end,
    {ok, #{
        <<"content-type">> => <<"application/json">>,
        <<"cache-control">> => <<"no-store">>,
        <<"body">> =>
            hb_json:encode(hb_msg_trace:report(Int(<<"since">>, 0), Int(<<"limit">>, 1000)))
    }}.

slots_body(Window, Named) ->
    #{ ring := Ring, slot_log := SlotLog, unassigned := Unassigned,
        processes := Procs } = hb_slot_log:report(#{ window => Window, pid => Named }),
    Phases = maps:from_list([ {P, {N, T}} || {P, T, N, _} <- hb_phase_timer:read() ]),
    Labelled = hb_phase_timer:read_processes(),
    #{
        <<"v">> => 1,
        <<"at">> => erlang:system_time(millisecond),
        <<"uptime_s">> => element(1, erlang:statistics(wall_clock)) div 1000,
        <<"window_s">> => Window,
        <<"ring">> => Ring,
        <<"slot_log">> => SlotLog,
        <<"node">> => #{
            <<"phases">> =>
                maps:from_list(
                    [ {atom_to_binary(P), count_us(maps:get(P, Phases, {0, 0}))}
                    || P <- [schedule, push, now_cache_latest, slot_next,
                            slot_execution, slot_store, lmdb_put, lmdb_commit,
                            lmdb_scan] ]),
            <<"delivery">> => delivery(),
            <<"unassigned">> => #{ <<"slot_execution">> => Unassigned }
        },
        <<"processes">> => [ slot_process(P, Labelled) || P <- Procs ]
    }.

count_us({N, Us}) -> #{ <<"count">> => N, <<"us">> => Us }.

slot_process(#{ <<"pid">> := Pid, <<"last_slot">> := Computed } = Proc, Labelled) ->
    % The scheduler's own table, read directly: `dev_scheduler_cache' is a
    % preloaded device library packaged under a name of its own, so calling
    % it from here was undefined, caught, and every process read as not
    % scheduled here (Paralith, 2026-11-25: all 44).
    Durable =
        try ets:lookup(dev_scheduler_durable, hb_util:human_id(Pid)) of
            [{_, D}] when is_integer(D) -> D;
            _ -> null
        catch _:_ -> null
        end,
    Mailbox =
        case catch hb_name:lookup({process_worker, Pid}) of
            Worker when is_pid(Worker) ->
                case erlang:process_info(Worker, message_queue_len) of
                    {message_queue_len, N} -> N;
                    undefined -> null
                end;
            _ -> null
        end,
    (maps:remove(<<"last_slot">>, Proc))#{
        <<"computed_slot">> => Computed,
        <<"durable_slot">> => Durable,
        <<"scheduled_here">> => is_integer(Durable),
        <<"backlog">> =>
            case is_integer(Durable) of
                true -> max(0, Durable - Computed);
                false -> null
            end,
        <<"mailbox">> => Mailbox,
        <<"worker">> =>
            maps:from_list(
                [ {atom_to_binary(Phase), count_us({N, T})}
                || {Phase, P, T, N, _} <- Labelled, P =:= Pid,
                   lists:member(Phase, [slot_next, worker_await, worker_execute,
                       ensure_loaded, compute_cache_read, delivery_flush]) ])
    }.

%% @doc The newest stored rows of one process's state, as the store lists
%% them (`pstate/<pid>/<kind>/<slot>'): for each of `f' (full states), `s'
%% (VM snapshot markers) and `d' (deltas) the `limit' highest slots (default
%% 20, at most 1,000), highest first, and the newest of each. Listings only:
%% no row is read or decoded, and the chunks of a chunked row are skipped.
-spec pstate(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) -> {ok, #{ _ => _ }} | {error, #{ _ => _ }}.
pstate(_, Req, Opts) ->
    Pid = maps:get(<<"pid">>, Req, <<>>),
    Limit =
        case catch hb_util:int(maps:get(<<"limit">>, Req, 20)) of
            L when is_integer(L) -> max(1, min(1000, L));
            _ -> 20
        end,
    case is_binary(Pid) andalso hb_util:is_human_id(Pid) of
        false -> {error, #{ <<"status">> => 400, <<"body">> => <<"pid: a process id">> }};
        true ->
            Store = hb_opts:get(process_store, hb_opts:get(store, [], Opts), Opts),
            Kinds =
                maps:from_list(
                    [ {K, pstate_rows(Store, <<"pstate/", Pid/binary, "/", K/binary>>, Limit)}
                    || K <- [<<"f">>, <<"s">>, <<"d">>] ]),
            {ok, #{
                <<"content-type">> => <<"application/json">>,
                <<"cache-control">> => <<"no-store">>,
                <<"body">> => hb_json:encode(Kinds#{ <<"pid">> => Pid, <<"limit">> => Limit })
            }}
    end.

%% The highest `N' slots under `Prefix', paging down past chunk names.
pstate_rows(Store, Prefix, N) -> pstate_rows(Store, Prefix, N, none, [], 200).
pstate_rows(_Store, _Prefix, N, _From, Acc, 0) -> lists:sublist(Acc, N);
pstate_rows(Store, Prefix, N, From, Acc, Pages) ->
    Req0 = #{ <<"list">> => Prefix, <<"limit">> => 256, <<"direction">> => desc },
    Page =
        case catch hb_store:list(Store,
                case From of none -> Req0; _ -> Req0#{ <<"from">> => From } end, #{}) of
            {ok, Names} -> [ Name || Name <- Names, Name =/= From ];
            _ -> []
        end,
    Acc1 = Acc ++ [ I || Name <- Page, I <- [catch binary_to_integer(Name)], is_integer(I) ],
    case length(Acc1) >= N orelse Page == [] of
        true -> lists:sublist(Acc1, N);
        false -> pstate_rows(Store, Prefix, N, lists:last(Page), Acc1, Pages - 1)
    end.

%% The node's delivery backlog (`lib_process_delivery:metrics/0'), read
%% through `process@1.0' as the metrics collector reads it. A preloaded device
%% is packaged under a name of its own, so a direct call to the library from
%% here was undefined, caught, and reported 0 for every gauge (public,
%% 2026-11-16: `pending_entries' 0 while `/~push@1.0/pending' listed 17).
delivery() ->
    Names = #{
        hb_delivery_pending_entries => <<"pending_entries">>,
        hb_delivery_oldest_pending_seconds => <<"oldest_pending_s">>,
        hb_worker_mailbox_len => <<"worker_mailbox">>,
        hb_worker_waiter_rows => <<"waiter_rows">>
    },
    Metrics =
        try
            {ok, Gauges} =
                hb_ao:raw(<<"process@1.0">>, #{}, #{ <<"path">> => <<"deliveries">> },
                    #{ <<"process-delivery-metrics">> => true }),
            Gauges
        catch _:_ -> []
        end,
    maps:merge(
        maps:from_list([ {K, 0} || K <- maps:values(Names) ]),
        maps:from_list(
            [ {maps:get(Name, Names), Value}
            || {Name, _, Value} <- Metrics, is_map_key(Name, Names) ])
    ).

%% @doc Return the current event counters in the reply's body.
-spec events(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) -> {ok, #{ _ => _ }}.
events(_, _Req, _Opts) ->
    {ok, #{
        <<"ao-result">> => <<"body">>,
        <<"body">> => hb_event:counters()
    }}.

%% @doc Employ HyperBEAM's internal pretty printer to format a message.
%% 
%% The request and node message can also be printed if desired by changing the
%% `format` key in the `format` call. This can be achieved easily using the
%% default key semantics:
%% ```
%% GET /.../~hyperbuddy@1.0/format=request
%% ```
%% Or a list of environment components:
%% ```
%% GET /.../~hyperbuddy@1.0/format+list=request,node
%% ```
%% Valid components are `base`, `request`, and `node`. The string `all` can also
%% be used to quickly include all of the components.
%% 
%% The `truncate-keys` key can also be used to truncate the number of keys
%% printed for each component. The default value is `infinity` (print all keys).
%% ```
%% GET /.../~hyperbuddy@1.0/format=request?truncate-keys=20
%% ```
%% `resolve-links' sets the link depth to resolve. It defaults to the node's
%% `debug-resolve-links' option; `true' resolves all levels.
-spec format(
    #{ _ => _ },
    #{
        format => [binary()] | binary(),
        'truncate-keys' => integer() | infinity,
        'resolve-links' => integer() | boolean(),
        _ => _
    },
    #{ _ => _ }
) ->
    {ok, #{ body := binary(), _ => _ }}
    | {error, #{ status := integer(), body := binary() }}.
format(Base, Req, Opts) ->
    % Find the scope of the environment that should be printed.
    Scope =
        case maps:get(<<"format">>, Req, <<"base">>) of
            <<"all">> -> [<<"base">>, <<"request">>, <<"node">>];
            Messages when is_list(Messages) -> Messages;
            SingleScope -> [SingleScope]
        end,
    ?event(debug_format, {using_scope, Scope}),
    case lists:member(<<"node">>, Scope) andalso
            hb_opts:get(mode, prod, Opts) =/= debug of
        true ->
            {error, #{
                <<"status">> => 403,
                <<"body">> => <<"Node formatting requires debug mode.">>
            }};
        false ->
            format(Base, Req, Opts, Scope)
    end.

%% @doc Format the permitted environment components.
format(Base, Req, Opts, Scope) ->
    CombinedMsg =
        hb_maps:with(
            Scope,
            #{
                <<"base">> => maps:without([<<"device">>], hb_private:reset(Base)),
                <<"request">> => maps:without([<<"path">>], hb_private:reset(Req)),
                <<"node">> => hb_private:reset(Opts)
            },
            Opts
        ),
    MsgBeforeLoad =
        if map_size(CombinedMsg) == 1 ->
            hb_maps:get(hd(maps:keys(CombinedMsg)), CombinedMsg, #{}, Opts);
        true ->
            CombinedMsg
        end,
    ResolveLinks =
        maps:get(
            <<"resolve-links">>,
            Req,
            hb_opts:get(debug_resolve_links, false, Opts)
        ),
    TruncateKeys = maps:get(<<"truncate-keys">>, Req, infinity),
    ?event(debug_format, {using_truncation, TruncateKeys}),
    {ok,
        #{
            <<"body">> =>
                hb_util:bin(
                    hb_format:message(
                        MsgBeforeLoad,
                        Opts#{
                            <<"linkify-mode">> => discard,
                            <<"cache-control">> => [<<"no-cache">>, <<"no-store">>],
                            <<"debug-resolve-links">> => ResolveLinks,
                            <<"debug-print-truncate">> => TruncateKeys
                        }
                    )
                )
        }
    }.

%% @doc Test key for validating the behavior of the `500` HTTP response.
-spec throw(#{ _ => _ }, #{ _ => _ }, #{ mode => atom(), _ => _ }) ->
    {error, binary()}.
throw(_Msg, _Req, Opts) ->
    case hb_opts:get(mode, prod, Opts) of
        prod -> {error, <<"Forced-throw unavailable in `prod` mode.">>};
        % The page that shows the reason needs no options: they carry the
        % node's private keys.
        debug -> throw(intentional_error)
    end.

%% @doc Serve a file from the priv directory. Only serves files that are explicitly
%% listed in the `routes' field of the `device_info/1' return value.
-spec serve(term(), #{ _ => _ }, #{ _ => _ }, map()) -> term().
serve(<<"keys">>, M1, _M2, Opts) ->
    hb_ao:raw(<<"message@1.0">>, <<"keys">>, M1, #{}, Opts);
serve(<<"set">>, M1, M2, Opts) ->
    hb_ao:raw(<<"message@1.0">>, <<"set">>, M1, M2, Opts);
serve(Key, _, _, Opts) ->
    ?event({hyperbuddy_serving, Key}),
    ServeRoutes = hb_maps:get(serve, device_info(Opts), #{}, Opts),
    case hb_maps:find(Key, ServeRoutes, Opts) of
        {ok, Filename} -> return_file(<<"hyperbuddy@1.0">>, Filename, #{});
        error -> {error, not_found}
    end.

%% @doc Read a file from disk and serve it as a static HTML page.
return_file(Device, Name) ->
    return_file(Device, Name, #{}).
return_file(Device, <<"404.html">>, Template) ->
    return_file(
        Device,
        <<"error.html">>,
        maps:merge(hb_ao_errors:page(404, #{}, #{}), Template)
    );
return_file(Device, Name, Template) ->
    hb_http_server:static(Device, Name, Template, #{}).

%%% Tests

slots_endpoint_test() ->
    hb_phase_timer:configure(#{}),
    hb_phase_timer:reset(),
    Pid = <<"slots-endpoint-proc">>,
    Slot =
        fun(N) ->
            {_, _, Notes} = hb_slot_log:run(fun() -> hb_slot_log:note(lua_patch_lua, 4) end),
            hb_slot_log:end_slot(Pid, N, <<"Order.Place">>, ok, 1, 10, 2, Notes)
        end,
    Read =
        fun(Params) ->
            {ok, Res} = hb_ao:resolve(
                #{ <<"device">> => <<"hyperbuddy@1.0">> },
                Params#{ <<"path">> => <<"slots">> },
                #{}
            ),
            ?assertEqual(<<"application/json">>, maps:get(<<"content-type">>, Res)),
            ?assertEqual(<<"no-store">>, maps:get(<<"cache-control">>, Res)),
            hb_json:decode(maps:get(<<"body">>, Res))
        end,
    Slot(7),
    One = Read(#{ <<"window">> => <<"5">> }),
    ?assertEqual(
        lists:sort([<<"v">>, <<"at">>, <<"uptime_s">>, <<"window_s">>, <<"ring">>,
            <<"slot_log">>, <<"node">>, <<"processes">>]),
        lists:sort(maps:keys(One))),
    ?assertEqual(10, maps:get(<<"window_s">>, One)),
    ?assertEqual(
        lists:sort([<<"phases">>, <<"delivery">>, <<"unassigned">>]),
        lists:sort(maps:keys(maps:get(<<"node">>, One)))),
    [Proc] = [ P || P = #{ <<"pid">> := Id } <- maps:get(<<"processes">>, One), Id =:= Pid ],
    ?assertEqual(
        lists:sort([<<"pid">>, <<"computed_slot">>, <<"durable_slot">>,
            <<"scheduled_here">>, <<"backlog">>, <<"mailbox">>, <<"n">>, <<"span_s">>,
            <<"ms">>, <<"counters">>, <<"worker">>, <<"actions">>]),
        lists:sort(maps:keys(Proc))),
    ?assertMatch(
        #{ <<"computed_slot">> := 7, <<"durable_slot">> := null,
            <<"scheduled_here">> := false, <<"backlog">> := null, <<"n">> := 1,
            <<"counters">> := #{ <<"slots">> := 1, <<"lua_us">> := 4 },
            <<"actions">> := [#{ <<"action">> := <<"Order.Place">>, <<"count">> := 1 }] },
        Proc),
    timer:sleep(2),
    Slot(8),
    Two = Read(#{ <<"window">> => <<"9999">>, <<"pid">> => Pid }),
    ?assertEqual(600, maps:get(<<"window_s">>, Two)),
    ?assert(maps:get(<<"at">>, Two) > maps:get(<<"at">>, One)),
    ?assertMatch([#{ <<"computed_slot">> := 8, <<"counters">> := #{ <<"slots">> := 2 } }],
        maps:get(<<"processes">>, Two)),
    ?assertEqual(60, maps:get(<<"window_s">>, Read(#{ <<"window">> => <<"soon">> }))),
    ?assertEqual([], maps:get(<<"processes">>, Read(#{ <<"pid">> => <<"unknown">> }))),
    hb_phase_timer:reset().

%% @doc `pstate' lists a process's newest state rows by kind, highest first,
%% without the chunks of a chunked row; a pid that is not one is refused.
pstate_endpoint_test() ->
    Store = hb_test_utils:test_store(hb_store_lmdb),
    Opts = #{ <<"store">> => [Store] },
    Pid = hb_util:human_id(crypto:strong_rand_bytes(32)),
    Key = fun(K, S) -> <<"pstate/", Pid/binary, "/", K/binary, "/",
        (iolist_to_binary(io_lib:format("~12..0B", [S])))/binary>> end,
    ok = hb_store:write([Store],
        maps:from_list(
            [ {Key(<<"d">>, S), <<"d">>} || S <- lists:seq(1, 30) ] ++
            [ {Key(<<"f">>, S), <<"f">>} || S <- [0, 10, 20] ] ++
            [ {<<(Key(<<"f">>, 20))/binary, ".", (integer_to_binary(C))/binary>>, <<"c">>}
            || C <- lists:seq(1, 300) ] ++
            [ {Key(<<"s">>, 20), <<"s">>} ]),
        Opts),
    {ok, Res} = pstate(#{}, #{ <<"pid">> => Pid, <<"limit">> => <<"5">> }, Opts),
    Body = hb_json:decode(maps:get(<<"body">>, Res)),
    ?assertEqual([20, 10, 0], maps:get(<<"f">>, Body)),
    ?assertEqual([20], maps:get(<<"s">>, Body)),
    ?assertEqual([30, 29, 28, 27, 26], maps:get(<<"d">>, Body)),
    ?assertMatch({error, #{ <<"status">> := 400 }}, pstate(#{}, #{ <<"pid">> => <<"x">> }, Opts)).

trace_endpoint_test() ->
    hb_phase_timer:record(trace_endpoint_test, 0),
    hb_phase_timer:configure(#{}),
    hb_phase_timer:reset(),
    Read =
        fun(Params) ->
            {ok, Res} = hb_ao:resolve(
                #{ <<"device">> => <<"hyperbuddy@1.0">> },
                Params#{ <<"path">> => <<"trace">> },
                #{}
            ),
            ?assertEqual(<<"application/json">>, maps:get(<<"content-type">>, Res)),
            hb_json:decode(maps:get(<<"body">>, Res))
        end,
    ?assertMatch(#{ <<"head">> := 0, <<"rows">> := [] }, Read(#{})),
    [ begin
        hb_msg_trace:open(<<"p">>, S),
        hb_msg_trace:close(<<"p">>, S, #{}, <<"A">>, ok, 1, 2, 3, #{}, #{})
      end || S <- [1, 2, 3] ],
    #{ <<"rows">> := [_, _], <<"next">> := 2, <<"head">> := 3 } =
        Read(#{ <<"limit">> => <<"2">> }),
    #{ <<"rows">> := [#{ <<"slot">> := 3, <<"k">> := <<"c">> }], <<"next">> := 3 } =
        Read(#{ <<"since">> => <<"2">>, <<"limit">> => <<"bad">> }),
    hb_phase_timer:reset().

phases_endpoint_test() ->
    hb_phase_timer:record(phases_endpoint_test, 0),
    hb_phase_timer:configure(#{}),
    hb_phase_timer:reset(),
    [ hb_phase_timer:record(lua_patch_lua, U) || U <- [10, 20, 3000] ],
    hb_phase_timer:record_node({http, now, resolve}, 5),
    {ok, Res} = hb_ao:resolve(
        #{ <<"device">> => <<"hyperbuddy@1.0">> },
        #{ <<"path">> => <<"phases">> },
        #{}
    ),
    ?assertEqual(<<"application/json">>, maps:get(<<"content-type">>, Res)),
    #{ <<"phases">> := Phases, <<"run_queue">> := #{ <<"schedulers">> := _ } } =
        hb_json:decode(maps:get(<<"body">>, Res)),
    ?assertMatch(#{ <<"count">> := 3, <<"total_us">> := 3030, <<"p50_us">> := 32,
            <<"p99_us">> := 4096 },
        maps:get(<<"lua_patch_lua">>, Phases)),
    ?assertMatch(#{ <<"count">> := 1 }, maps:get(<<"http_now_resolve">>, Phases)),
    hb_phase_timer:reset().

%% The slots endpoint reports the same delivery backlog as the metrics
%% endpoint: a live deliverer's row of entries is counted.
delivery_backlog_is_reported_test() ->
    Table = lib_process_delivery_backlog,
    Owner =
        case ets:whereis(Table) of
            undefined ->
                Self = self(),
                Pid = spawn(fun() ->
                    ets:new(Table, [named_table, public, set]),
                    Self ! {self(), ready},
                    receive stop -> ok end
                end),
                receive {Pid, ready} -> Pid end;
            _ -> none
        end,
    Proc = <<"delivery-backlog-test-proc">>,
    ets:insert(Table, {Proc, self(), 17, erlang:monotonic_time(second) - 5,
        erlang:monotonic_time(millisecond)}),
    try
        #{ <<"pending_entries">> := Pending, <<"oldest_pending_s">> := Oldest } = delivery(),
        ?assert(Pending >= 17),
        ?assert(Oldest >= 5)
    after
        ets:delete(Table, Proc),
        Owner =:= none orelse (Owner ! stop)
    end.

node_format_requires_debug_test() ->
    Base = #{ <<"device">> => <<"hyperbuddy@1.0">>, <<"body">> => <<"base">> },
    Opts = #{ <<"mode">> => prod, <<"config-marker">> => <<"node-marker">> },
    lists:foreach(
        fun(Scope) ->
            ?assertMatch({error, #{ <<"status">> := 403 }}, hb_ao:resolve(
                Base, #{ <<"path">> => <<"format">>, <<"format">> => Scope }, Opts
            ))
        end,
        [<<"node">>, <<"all">>, [<<"base">>, <<"node">>]]
    ),
    lists:foreach(
        fun(Scope) ->
            ?assertMatch({ok, _}, hb_ao:resolve(
                Base, #{ <<"path">> => <<"format">>, <<"format">> => Scope }, Opts
            ))
        end,
        [<<"base">>, <<"request">>]
    ),
    {ok, #{ <<"body">> := Debug }} = hb_ao:resolve(
        Base, #{ <<"path">> => <<"format">>, <<"format">> => <<"node">> },
        Opts#{ <<"mode">> => debug, <<"priv-test">> => <<"private-marker">> }
    ),
    ?assertNotEqual(nomatch, binary:match(Debug, <<"node-marker">>)),
    ?assertEqual(nomatch, binary:match(Debug, <<"private-marker">>)).

return_templated_file_test() ->
    {ok, #{ <<"body">> := Body }} =
        return_file(
            <<"hyperbuddy@1.0">>,
            <<"500.html">>,
            #{
                <<"error">> => <<"This is an error message.">>
            }
        ),
    ?assertNotEqual(
        binary:match(Body, <<"This is an error message.">>),
        nomatch
    ).

return_custom_json_test() ->
    Base = hb_util:bin(code:priv_dir(hb)),
    Filename = <<Base/binary, "/html/hyperbuddy@1.0/test.json">>,
    ok = file:write_file(Filename, <<"{\"status\":\"ok\"}">>),
    try
        ?assertMatch(
            {ok,
                #{
                    <<"body">> := JSONBin,
                    <<"content-type">> := <<"application/json">>
                }
            } when byte_size(JSONBin) > 0,
            hb_ao:resolve(
                #{
                    <<"device">> => <<"hyperbuddy@1.0">>
                },
                <<"custom.json">>,
                #{
                    <<"hyperbuddy-serve">> => #{
                        <<"custom.json">> => <<"test.json">>
                    }
                }
            )
        )
    after
        file:delete(Filename)
    end.

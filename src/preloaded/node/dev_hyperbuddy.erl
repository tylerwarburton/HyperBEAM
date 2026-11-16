%%% @doc A device that renders a REPL-like interface for AO-Core via HTML.
-module(dev_hyperbuddy).
-export([device_info/1, format/3, return_file/2]).
-export([metrics/3, events/3, slots/3]).
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
                            slot_execution, slot_store] ]),
            <<"delivery">> => delivery(),
            <<"unassigned">> => #{ <<"slot_execution">> => Unassigned }
        },
        <<"processes">> => [ slot_process(P, Labelled) || P <- Procs ]
    }.

count_us({N, Us}) -> #{ <<"count">> => N, <<"us">> => Us }.

slot_process(#{ <<"pid">> := Pid, <<"last_slot">> := Computed } = Proc, Labelled) ->
    Durable =
        case catch dev_scheduler_cache:durable(Pid) of
            D when is_integer(D) -> D;
            _ -> null
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
    ets:insert(Table, {Proc, self(), 17, erlang:monotonic_time(second) - 5}),
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

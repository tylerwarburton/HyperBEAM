%%% @doc DNS device tests through AO-Core and real UDP/TCP sockets.
-module(dev_dns_test_vectors).
-include_lib("eunit/include/eunit.hrl").

%% @doc Only the node's options enable a listener, and starts are not cached.
start_test() ->
    ?assertMatch(
        {error, #{ <<"status">> := 400 }},
        hb_ao:resolve(
            #{ <<"device">> => <<"dns@1.0">> },
            #{ <<"path">> => <<"start">>, <<"dns">> => #{ <<"port">> => 0 }},
            #{}
        )
    ),
    with_dns([], fun(_Port, Opts) ->
        ?assertMatch(
            {error, #{ <<"status">> := 409, <<"body">> := <<"dns-already-started">> }},
            hb_ao:resolve(#{ <<"device">> => <<"dns@1.0">> }, <<"start">>, Opts)
        )
    end).

%% @doc A DNS client receives REFUSED unless an application supplies policy.
refused_test() ->
    with_dns([], fun(Port, Opts) ->
        ?assertEqual({error, not_authorized}, hb_ao:resolve(
            #{ <<"device">> => <<"dns@1.0">> },
            #{ <<"path">> => <<"resolve">>, <<"name">> => <<"example.test">>,
                <<"type">> => <<"a">>, <<"class">> => <<"in">> },
            Opts
        )),
        Response = udp_query(Port, question("example.test", a)),
        ?assertEqual(5, inet_dns:header(inet_dns:msg(Response, header), rcode)),
        ?assertEqual([], inet_dns:msg(Response, anlist))
    end).

%% @doc Typed questions keep numeric codes and leave resolver extensions lazy.
typed_question_test() ->
    Handler = handler(fun(Req, _Opts) ->
        {ok, #{ <<"rcode">> => 0, <<"question">> => Req }}
    end),
    with_dns(Handler, fun(_Port, Opts) ->
        Request = #{
            <<"path">> => <<"resolve">>,
            <<"name">> => link(<<"Example.test">>, Opts),
            <<"class">> => <<"65281">>, <<"id">> => <<"42">>,
            <<"recursion-desired">> => <<"true">>,
            <<"extension">> => link(#{ <<"value">> => <<"kept">> }, Opts)
        },
        lists:foreach(fun({Type, Expected}) ->
            {ok, Reply} = hb_ao:resolve(
                #{ <<"device">> => <<"dns@1.0">> },
                Request#{ <<"type">> => Type },
                Opts
            ),
            Question = hb_maps:get(<<"question">>, Reply, undefined, Opts),
            ?assertMatch(#{
                <<"name">> := <<"Example.test">>, <<"type">> := Expected,
                <<"class">> := 65281, <<"id">> := 42,
                <<"recursion-desired">> := true,
                <<"extension">> := {link, _, _}
            }, Question),
            ?assertEqual(<<"kept">>, hb_maps:get(<<"value">>,
                hb_maps:get(<<"extension">>, Question, undefined, Opts),
                undefined, Opts))
        end, [{65280, 65280}, {<<"65280">>, 65280}, {<<"a">>, <<"a">>}])
    end).

%% @doc Result forms are unchanged over AO-Core and have defined DNS semantics.
result_forms_test() ->
    Answer = #{
        <<"name">> => <<"example.test">>, <<"type">> => <<"a">>,
        <<"data">> => <<"192.0.2.1">>
    },
    Details = #{ <<"rcode">> => 0, <<"recursion-available">> => true },
    Cases = [
        {{ok, [Answer]}, {0, true, false}, [{192, 0, 2, 1}]},
        {{ok, #{ <<"1">> => Answer }}, {0, true, false}, [{192, 0, 2, 1}]},
        {{ok, #{ <<"answers">> => [Answer] }}, {0, false, false}, [{192, 0, 2, 1}]},
        {{ok, []}, {0, true, false}, []},
        {{ok, #{}}, {0, true, false}, []},
        {{ok, #{ <<"rcode">> => 0 }}, {0, false, false}, []},
        {{ok, #{ <<"rcode">> => 5 }}, {5, false, false}, []},
        {{error, not_found}, {3, false, false}, []},
        {{error, not_authorized}, {5, false, false}, []},
        {{error, not_implemented}, {4, false, false}, []},
        {{error, Details}, {1, false, true}, []},
        {{failure, Details}, {2, false, true}, []}
    ],
    lists:foreach(fun({Result, Flags, Data}) ->
        Handler = handler(fun(_Req, _Opts) -> Result end),
        ?assertEqual(Result, hb_private:reset(hb_ao:resolve(
            #{ <<"device">> => <<"dns@1.0">> },
            #{ <<"path">> => <<"resolve">>, <<"name">> => <<"example.test">>,
                <<"type">> => <<"a">>, <<"class">> => <<"in">> },
            #{
                <<"on">> => #{ <<"dns-resolve">> => Handler },
                <<"hashpath">> => ignore,
                <<"cache-control">> => [<<"no-cache">>, <<"no-store">>]
            }
        ))),
        with_dns(Handler, fun(Port, _Opts) ->
            lists:foreach(fun(Transport) ->
                Response = query(Transport, Port, question("example.test", a)),
                Header = inet_dns:msg(Response, header),
                ?assertEqual(Flags, {
                    inet_dns:header(Header, rcode),
                    inet_dns:header(Header, aa),
                    inet_dns:header(Header, ra)
                }),
                ?assertEqual(Data,
                    [inet_dns:rr(RR, data) || RR <- inet_dns:msg(Response, anlist)])
            end, [udp, tcp])
        end)
    end, Cases).

%% @doc Exercise real UDP and TCP queries through the AO-Core hook interface.
records_test() ->
    Parent = self(),
    Handler = handler(fun(Req, _Opts) ->
        Parent ! {dns_request, Req},
        Type = hb_maps:get(<<"type">>, Req),
        Data = case Type of
            <<"a">> -> <<"192.0.2.1">>;
            <<"aaaa">> -> <<"2001:db8::1">>;
            <<"txt">> -> [<<"part one">>, <<"part two">>]
        end,
        {ok, [#{
            <<"name">> => hb_maps:get(<<"name">>, Req),
            <<"type">> => Type, <<"ttl">> => 60, <<"data">> => Data
        }]}
    end),
    with_dns(Handler, fun(Port, _Opts) ->
        lists:foreach(fun({Type, Expected}) ->
            lists:foreach(fun(TCP) ->
                {ok, Response} = inet_res:resolve("example.test", in, Type, [
                    {nameservers, [{{127, 0, 0, 1}, Port}]},
                    {usevc, TCP}, {retry, 1}, {timeout, 1000}
                ]),
                [RR] = inet_dns:msg(Response, anlist),
                ?assertEqual(Expected, inet_dns:rr(RR, data)),
                ?assertEqual(60, inet_dns:rr(RR, ttl)),
                ?assertEqual(true,
                    inet_dns:header(inet_dns:msg(Response, header), aa)),
                receive
                    {dns_request, Req} ->
                        ?assertEqual(<<"example.test">>, hb_maps:get(<<"name">>, Req)),
                        ?assertEqual(atom_to_binary(Type), hb_maps:get(<<"type">>, Req)),
                        ?assertEqual(<<"in">>, hb_maps:get(<<"class">>, Req)),
                        ?assertEqual(true, hb_maps:get(<<"recursion-desired">>, Req)),
                        ?assertEqual(
                            case TCP of true -> <<"tcp">>; false -> <<"udp">> end,
                            hb_maps:get(<<"transport">>, Req)
                        ),
                        ?assertMatch(#{ <<"address">> := <<"127.0.0.1">> },
                            hb_maps:get(<<"peer">>, Req)),
                        ?assertNot(hb_maps:is_key(<<"body">>, Req))
                after 1000 -> error(hook_not_called)
                end
            end, [false, true])
        end, [
            {a, {192, 0, 2, 1}},
            {aaaa, {16#2001, 16#db8, 0, 0, 0, 0, 0, 1}},
            {txt, ["part one", "part two"]}
        ])
    end).

%% @doc Linked replies and numbered answers preserve record and string order.
linked_results_test() ->
    Handler = handler(fun(Req, Opts) ->
        Name = hb_maps:get(<<"name">>, Req),
        Answers = link(hb_util:list_to_numbered_message([
            link(#{
                <<"name">> => link(Name, Opts),
                <<"type">> => link(<<"txt">>, Opts),
                <<"data">> => #{ <<"1">> => link(Value, Opts) }
            }, Opts)
        || Value <- [<<"first value">>, <<"second value">>]]), Opts),
        {ok, case Name of
            <<"answers.example.test">> -> Answers;
            <<"reply.example.test">> -> link(#{
                <<"authoritative">> => true, <<"answers">> => Answers
            }, Opts)
        end}
    end),
    with_dns(Handler, fun(Port, _Opts) ->
        lists:foreach(fun(Name) ->
            Response = udp_query(Port, question(Name, txt)),
            ?assertEqual(true,
                inet_dns:header(inet_dns:msg(Response, header), aa)),
            ?assertEqual([["first value"], ["second value"]],
                [inet_dns:rr(RR, data) || RR <- inet_dns:msg(Response, anlist)])
        end, ["answers.example.test", "reply.example.test"])
    end).

%% @doc Error categories override linked reply codes without dropping sections.
linked_error_details_test() ->
    lists:foreach(fun({Status, RCode}) ->
        Handler = handler(fun(_Req, Opts) ->
            {Status, link(#{
                <<"rcode">> => 0, <<"recursion-available">> => true,
                <<"additional">> => [#{
                    <<"name">> => <<"ns.example.test">>,
                    <<"type">> => <<"a">>, <<"data">> => <<"192.0.2.53">>
                }]
            }, Opts)}
        end),
        with_dns(Handler, fun(Port, _Opts) ->
            lists:foreach(fun(Transport) ->
                Response = query(Transport, Port, question("example.test", a)),
                Header = inet_dns:msg(Response, header),
                ?assertEqual(RCode, inet_dns:header(Header, rcode)),
                ?assertEqual(true, inet_dns:header(Header, ra)),
                ?assertEqual([{192, 0, 2, 53}],
                    [inet_dns:rr(RR, data) || RR <- inet_dns:msg(Response, arlist)])
            end, [udp, tcp])
        end)
    end, [{error, 1}, {failure, 2}]).

%% @doc Negative answers, referrals and future RR types are policy, not transport.
sections_test() ->
    SOA = #{
        <<"name">> => <<"example.test">>, <<"type">> => <<"soa">>,
        <<"data">> => #{
            <<"mname">> => <<"ns.example.test">>,
            <<"rname">> => <<"admin.example.test">>, <<"serial">> => 1,
            <<"refresh">> => 3600, <<"retry">> => 600,
            <<"expire">> => 86400, <<"minimum">> => 60
        }
    },
    Handler = handler(fun(Req, _Opts) ->
        {ok, case hb_maps:get(<<"type">>, Req) of
            <<"a">> -> #{
                <<"rcode">> => 3, <<"authoritative">> => true,
                <<"authority">> => [SOA]
            };
            65280 -> #{ <<"answers">> => [#{
                <<"name">> => <<"example.test">>, <<"type">> => 65280,
                <<"data">> => <<0, 1, 255, 128>>
            }] };
            <<"ns">> -> #{
                <<"authority">> => [#{
                    <<"name">> => <<"example.test">>, <<"type">> => <<"ns">>,
                    <<"data">> => <<"ns.example.test">>
                }],
                <<"additional">> => [#{
                    <<"name">> => <<"ns.example.test">>, <<"type">> => <<"a">>,
                    <<"data">> => <<"192.0.2.53">>
                }]
            };
            _ -> #{ <<"authoritative">> => true, <<"authority">> => [SOA] }
        end}
    end),
    with_dns(Handler, fun(Port, _Opts) ->
        Negative = udp_query(Port, question("absent.example.test", a)),
        ?assertEqual(3, inet_dns:header(inet_dns:msg(Negative, header), rcode)),
        [Authority] = inet_dns:msg(Negative, nslist),
        ?assertEqual({"ns.example.test", "admin.example.test", 1, 3600, 600, 86400, 60},
            inet_dns:rr(Authority, data)),
        Opaque = udp_query(Port, question("example.test", 65280)),
        [Unknown] = inet_dns:msg(Opaque, anlist),
        ?assertEqual(<<0, 1, 255, 128>>, inet_dns:rr(Unknown, data)),
        Referral = udp_query(Port, question("example.test", ns)),
        ?assertEqual([], inet_dns:msg(Referral, anlist)),
        ?assertEqual(["ns.example.test"],
            [inet_dns:rr(RR, data) || RR <- inet_dns:msg(Referral, nslist)]),
        ?assertEqual([{192, 0, 2, 53}],
            [inet_dns:rr(RR, data) || RR <- inet_dns:msg(Referral, arlist)]),
        NoData = udp_query(Port, question("example.test", mx)),
        ?assertEqual(0, inet_dns:header(inet_dns:msg(NoData, header), rcode)),
        ?assertEqual([], inet_dns:msg(NoData, anlist)),
        ?assertEqual([Authority], inet_dns:msg(NoData, nslist))
    end).

%% @doc Named compound records use message fields rather than Erlang tuples.
compound_records_test() ->
    Cases = [
        {mx, #{ <<"preference">> => 10, <<"exchange">> => <<"mail.example.test">> },
            {10, "mail.example.test"}},
        {srv, #{ <<"priority">> => 1, <<"weight">> => 2, <<"port">> => 443,
            <<"target">> => <<"node.example.test">> }, {1, 2, 443, "node.example.test"}},
        {caa, #{ <<"flags">> => 0, <<"tag">> => <<"issue">>,
            <<"value">> => <<"ca.example.test">> }, {0, "issue", "ca.example.test"}},
        {uri, #{ <<"priority">> => 1, <<"weight">> => 2,
            <<"target">> => <<"https://example.test/">> }, {1, 2, "https://example.test/"}},
        {naptr, #{ <<"order">> => 1, <<"preference">> => 2, <<"flags">> => <<"u">>,
            <<"services">> => <<"E2U+sip">>, <<"regexp">> => <<"!x!é!"/utf8>>,
            <<"replacement">> => <<".">> }, {1, 2, "u", "e2u+sip", "!x!é!", "."}},
        {hinfo, #{ <<"cpu">> => <<"ARM">>, <<"os">> => <<"Linux">> }, {"ARM", "Linux"}},
        {minfo, #{ <<"rmailbx">> => <<"mail.example.test">>,
            <<"emailbx">> => <<"error.example.test">> },
            {"mail.example.test", "error.example.test"}},
        {cname, <<"node.example.test">>, "node.example.test"},
        {ptr, <<"node.example.test">>, "node.example.test"}
    ],
    Handler = handler(fun(Req, _Opts) ->
        Type = hb_maps:get(<<"type">>, Req),
        {_, Data, _} = lists:keyfind(binary_to_existing_atom(Type), 1, Cases),
        {ok, [#{
            <<"name">> => hb_maps:get(<<"name">>, Req),
            <<"type">> => Type, <<"data">> => Data
        }]}
    end),
    with_dns(Handler, fun(Port, _Opts) ->
        lists:foreach(fun({Type, _, Expected}) ->
            Response = udp_query(Port, question("example.test", Type)),
            [RR] = inet_dns:msg(Response, anlist),
            ?assertEqual(Expected, inet_dns:rr(RR, data))
        end, Cases)
    end).

%% @doc Hook exceptions and invalid records fail one query, not the listener.
failure_test() ->
    Handler = handler(fun(Req, _Opts) ->
        case hb_maps:get(<<"type">>, Req) of
            <<"a">> -> error(test_hook_failure);
            <<"aaaa">> -> {ok, <<"not a DNS reply">>};
            <<"ns">> -> invalid_result;
            <<"txt">> -> {ok, [#{
                <<"name">> => <<"example.test">>, <<"type">> => <<"txt">>,
                <<"data">> => [binary:copy(<<"x">>, 256)]
            }]};
            _ -> {ok, #{ <<"rcode">> => 0 }}
        end
    end),
    with_dns(Handler, fun(Port, _Opts) ->
        lists:foreach(fun({Type, RCode}) ->
            Response = udp_query(Port, question("example.test", Type)),
            ?assertEqual(RCode, inet_dns:header(inet_dns:msg(Response, header), rcode))
        end, [{a, 2}, {aaaa, 2}, {ns, 2}, {txt, 2}, {mx, 0}])
    end).

%% @doc Bound UDP answers, negotiate EDNS and serve the full answer over TCP.
truncation_test() ->
    Text = lists:duplicate(4, binary:copy(<<"x">>, 200)),
    Handler = handler(fun(Req, _Opts) -> {ok, [#{
        <<"name">> => hb_maps:get(<<"name">>, Req),
        <<"type">> => <<"txt">>, <<"data">> => Text
    }]} end),
    with_dns(Handler, fun(Port, _Opts) ->
        Query = question("example.test", txt),
        Truncated = udp_packet(Port, inet_dns:encode(Query, false)),
        ?assert(byte_size(Truncated) =< 512),
        ?assertMatch(<<42:16, 1:1, _:5, 1:1, _/bitstring>>, Truncated),
        EDNS = inet_dns:make_msg(Query, arlist, [inet_dns:make_rr([
            {type, opt}, {udp_payload_size, 1232}
        ])]),
        Full = udp_query(Port, EDNS),
        [RR] = inet_dns:msg(Full, anlist),
        ?assertEqual([binary_to_list(T) || T <- Text], inet_dns:rr(RR, data)),
        {ok, TCP} = inet_res:resolve("example.test", in, txt, [
            {nameservers, [{{127, 0, 0, 1}, Port}]}, {usevc, true},
            {retry, 1}, {timeout, 1000}
        ]),
        ?assertEqual(inet_dns:msg(Full, anlist), inet_dns:msg(TCP, anlist)),
        BadVersion = udp_query(Port, inet_dns:make_msg(Query, arlist, [
            inet_dns:make_rr([{type, opt}, {version, 1}])
        ])),
        [OPT] = inet_dns:msg(BadVersion, arlist),
        ?assertEqual(1, inet_dns:rr(OPT, ext_rcode)),
        ?assertEqual(0, inet_dns:rr(OPT, version))
    end).

%% @doc Malformed packets and unsupported operations do not invoke policy.
malformed_test() ->
    with_dns([], fun(Port, _Opts) ->
        ?assertMatch(<<42:16, 1:1, _:11, 1:4, _/binary>>,
            udp_packet(Port, <<42:16, 0:8>>)),
        ?assertMatch(<<42:16, 1:1, _:11, 1:4, _/binary>>,
            udp_packet(Port, <<42:16, 0:16, 1:16, 0:48, 16#c00c:16, 1:16, 1:16>>)),
        Query = question("example.test", a),
        Header = inet_dns:make_header(inet_dns:msg(Query, header), opcode, notify),
        NotImplemented = udp_query(Port, inet_dns:make_msg(Query, header, Header)),
        ?assertEqual(4,
            inet_dns:header(inet_dns:msg(NotImplemented, header), rcode)),
        ?assertEqual(5, inet_dns:header(
            inet_dns:msg(udp_query(Port, Query), header), rcode
        ))
    end).

%% @doc A failed UDP bind must release the TCP port opened during startup.
bind_failure_test() ->
    IP = {127, 0, 0, 1},
    {ok, Occupied} = gen_udp:open(0, [binary, {ip, IP}]),
    {ok, {_, Port}} = inet:sockname(Occupied),
    Opts = #{
        <<"dns">> => #{ <<"port">> => Port, <<"address">> => <<"127.0.0.1">> },
        <<"cache-control">> => [<<"no-cache">>, <<"no-store">>]
    },
    try
        ?assertMatch({error, #{ <<"status">> := 409 }},
            hb_ao:resolve(#{ <<"device">> => <<"dns@1.0">> }, <<"start">>, Opts)),
        gen_udp:close(Occupied),
        ?assertMatch({ok, #{ <<"dns-port">> := Port }},
            hb_ao:resolve(#{ <<"device">> => <<"dns@1.0">> }, <<"start">>, Opts))
    after
        gen_udp:close(Occupied),
        dev_dns_server:stop(IP, Port)
    end.

%% @doc Boot through on/start and resolve the same policy over DNS and HTTP.
boot_test_() ->
    {timeout, 30, fun() ->
        Parent = self(),
        Wallet = ar_wallet:new(),
        Store = [hb_test_utils:test_store()],
        hb_store:start(Store),
        try
            Node = hb_http_server:start_node(#{
                <<"port">> => 0, <<"priv-wallet">> => Wallet,
                <<"store">> => Store,
                <<"dns">> => #{ <<"port">> => 0, <<"address">> => <<"127.0.0.1">> },
                <<"on">> => #{
                    <<"start">> => [
                        #{ <<"device">> => <<"dns@1.0">> },
                        #{ <<"device">> => #{ start => fun(_Base, Req, _Opts) ->
                            Parent ! {dns_started, Req},
                            {ok, Req}
                        end } }
                    ],
                    <<"dns-resolve">> => handler(fun(Req, _Opts) ->
                        {ok, [#{
                            <<"name">> => hb_maps:get(<<"name">>, Req),
                            <<"type">> => <<"a">>, <<"data">> => <<"192.0.2.1">>
                        }]}
                    end)
                }
            }),
            receive
                {dns_started, Req} ->
                    ?assertEqual(Wallet,
                        hb_maps:get(<<"priv-wallet">>, hb_maps:get(<<"body">>, Req))),
                    Response = udp_query(hb_maps:get(<<"dns-port">>, Req),
                        question("example.test", a)),
                    [RR] = inet_dns:msg(Response, anlist),
                    ?assertEqual({192, 0, 2, 1}, inet_dns:rr(RR, data)),
                    ?assertEqual({ok, <<"192.0.2.1">>},
                        hb_http:get(Node,
                            <<"/~dns@1.0/resolve/1/data?name=example.test&type=a&class=in">>,
                            #{}
                        ))
            after 1000 -> error(start_hook_not_called)
            end
        after
            dev_dns_server:stop({127, 0, 0, 1}, 0),
            cowboy:stop_listener(hb_util:human_id(ar_wallet:to_address(Wallet))),
            hb_store:reset(Store)
        end
    end}.

%% @doc TCP framing accepts split writes and repeated questions on one socket.
tcp_stream_test() ->
    Handler = handler(fun(Req, _Opts) -> {ok, [#{
        <<"name">> => hb_maps:get(<<"name">>, Req), <<"type">> => <<"txt">>,
        <<"data">> => [integer_to_binary(erlang:unique_integer([positive]))]
    }]} end),
    with_dns(Handler, fun(Port, _Opts) ->
        {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port,
            [binary, {active, false}, {packet, raw}], 1000),
        try
            Packet = inet_dns:encode(question("example.test", txt), false),
            <<First, Rest/binary>> = <<(byte_size(Packet)):16, Packet/binary>>,
            ok = gen_tcp:send(Socket, <<First>>),
            ok = gen_tcp:send(Socket, [Rest,
                <<(byte_size(Packet)):16, Packet/binary>>]),
            inet:setopts(Socket, [{packet, 2}]),
            [FirstAnswer, SecondAnswer] = lists:map(fun(_) ->
                {ok, Reply} = gen_tcp:recv(Socket, 0, 1000),
                {ok, Response} = inet_dns:decode(Reply, false),
                ?assertEqual(0,
                    inet_dns:header(inet_dns:msg(Response, header), rcode)),
                inet_dns:msg(Response, anlist)
            end, [1, 2]),
            ?assertNotEqual(FirstAnswer, SecondAnswer)
        after
            gen_tcp:close(Socket)
        end
    end).

%% @doc Write real linked values into the test's isolated HyperBEAM store. A
%% message is returned as a link to its ID, and a binary as a lazy link to its
%% path in the store.
link(Value, Opts) ->
    case hb_cache:write(Value, Opts) of
        {ok, ID} when is_map(Value) -> {link, <<"ao:", ID/binary>>, #{}};
        {ok, Name} -> {link, Name, #{ <<"lazy">> => true }}
    end.

%% @doc Start an isolated listener via AO-Core and always release its sockets.
with_dns(Handler, Test) ->
    Opts = #{
        <<"dns">> => #{ <<"port">> => 0, <<"address">> => <<"127.0.0.1">> },
        <<"on">> => #{ <<"dns-resolve">> => Handler },
        <<"store">> => [hb_test_utils:test_store()],
        <<"cache-control">> => [<<"no-cache">>, <<"no-store">>]
    },
    hb_store:start(hb_maps:get(<<"store">>, Opts)),
    try
        {ok, Started} = hb_ao:resolve(
            #{ <<"device">> => <<"dns@1.0">> }, <<"start">>, Opts
        ),
        Test(hb_maps:get(<<"dns-port">>, Started), Opts)
    after
        dev_dns_server:stop({127, 0, 0, 1}, 0),
        hb_store:reset(hb_maps:get(<<"store">>, Opts))
    end.

%% @doc Execute a resolver policy as an ordinary AO-Core device.
handler(Fun) -> #{
    <<"device">> => #{
        dns_resolve => fun(_Base, Req, Opts) -> Fun(Req, Opts) end
    }
}.

%% @doc Build a unicast question using OTP's DNS codec.
question(Name, Type) ->
    inet_dns:make_msg([
        {header, inet_dns:make_header([{id, 42}, {rd, true}])},
        {qdlist, [inet_dns:make_dns_query([
            {domain, Name}, {type, Type}, {class, in}
        ])]}
    ]).

%% @doc Send a real UDP query and check that its ID and question are preserved.
udp_query(Port, Query) ->
    query(udp, Port, Query).

%% @doc Exercise the same reply contract over both DNS transports.
query(Transport, Port, Query) ->
    Encoded = inet_dns:encode(Query, false),
    Packet = case Transport of
        udp -> udp_packet(Port, Encoded);
        tcp ->
            {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port,
                [binary, {active, false}, {packet, 2}], 1000),
            try
                ok = gen_tcp:send(Socket, Encoded),
                {ok, Reply} = gen_tcp:recv(Socket, 0, 1000),
                Reply
            after
                gen_tcp:close(Socket)
            end
    end,
    {ok, Response} = inet_dns:decode(Packet, false),
    ?assertEqual(42, inet_dns:header(inet_dns:msg(Response, header), id)),
    ?assertEqual(inet_dns:msg(Query, qdlist), inet_dns:msg(Response, qdlist)),
    ?assertEqual(inet_dns:header(inet_dns:msg(Query, header), rd),
        inet_dns:header(inet_dns:msg(Response, header), rd)),
    Response.

udp_packet(Port, Query) ->
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
    try
        ok = gen_udp:send(Socket, {127, 0, 0, 1}, Port, Query),
        {ok, {_, Port, Packet}} = gen_udp:recv(Socket, 0, 1000),
        Packet
    after
        gen_udp:close(Socket)
    end.

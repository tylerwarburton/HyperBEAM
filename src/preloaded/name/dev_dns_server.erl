%%% @doc Small UDP/TCP transport for the DNS device. OTP handles DNS packets
%%% and TCP's two-byte framing; the callback supplies the response bytes.
-module(dev_dns_server).
-export([start/3, stop/2]).
-include("include/hb.hrl").

-define(UDP_WORKERS, 64).
-define(TCP_WORKERS, 8).
-define(TCP_TIMEOUT, 5000).

%% @doc Bind both transports before reporting startup. A repeated start is an
%% error, including concurrent attempts to start the same configured listener.
start(IP, Port, Handle) ->
    Name = {?MODULE, IP, Port},
    case hb_name:lookup(Name) of
        undefined ->
            Parent = self(),
            {PID, Ref} = spawn_monitor(fun() -> init(Name, Parent, IP, Port, Handle) end),
            receive
                {PID, {ok, _} = Result} ->
                    erlang:demonitor(Ref, [flush]), Result;
                {PID, {error, _} = Error} ->
                    receive {'DOWN', Ref, process, PID, _} -> Error end;
                {'DOWN', Ref, process, PID, Reason} -> {error, Reason}
            end;
        _ -> {error, 'dns-already-started'}
    end.

%% @doc Stop this transport and wait until its sockets have been released.
stop(IP, Port) ->
    case hb_name:lookup({?MODULE, IP, Port}) of
        undefined -> ok;
        PID ->
            Ref = monitor(process, PID),
            PID ! stop,
            receive {'DOWN', Ref, process, PID, _} -> ok end
    end.

%% @doc Own the sockets and linked workers for the lifetime of the listener.
init(Name, Parent, IP, Port, Handle) ->
    case hb_name:register(Name) of
        error -> Parent ! {self(), {error, 'dns-already-started'}};
        ok ->
            try
                Family = case tuple_size(IP) of 4 -> inet; 8 -> inet6 end,
                case gen_tcp:listen(Port, [
                    Family, binary, {ip, IP}, {active, false}, {packet, 2},
                    {reuseaddr, true}, {send_timeout, ?TCP_TIMEOUT}
                ]) of
                    {ok, TCP} ->
                        {ok, {_, BoundPort}} = inet:sockname(TCP),
                        case gen_udp:open(BoundPort, [
                            Family, binary, {ip, IP}, {active, once}
                        ]) of
                            {ok, UDP} ->
                                [spawn_link(fun() -> accept(TCP, Handle) end)
                                    || _ <- lists:seq(1, ?TCP_WORKERS)],
                                Parent ! {self(), {ok, BoundPort}},
                                udp(UDP, Handle, 0);
                            Error -> Parent ! {self(), Error}
                        end;
                    Error -> Parent ! {self(), Error}
                end
            after
                hb_name:unregister(Name)
            end
    end.

%% @doc Bound in-flight datagrams without serializing potentially slow hooks.
udp(Socket, Handle, Active) ->
    receive
        {udp, Socket, IP, Port, Packet} ->
            Parent = self(),
            spawn_link(fun() ->
                try
                    case dispatch(Handle, Packet, peer(udp, IP, Port)) of
                        ignore -> ok;
                        Response -> gen_udp:send(Socket, IP, Port, Response)
                    end
                after
                    Parent ! done
                end
            end),
            case Active + 1 < ?UDP_WORKERS of
                true -> inet:setopts(Socket, [{active, once}]);
                false -> ok
            end,
            udp(Socket, Handle, Active + 1);
        done ->
            case Active =:= ?UDP_WORKERS of
                true -> inet:setopts(Socket, [{active, once}]);
                false -> ok
            end,
            udp(Socket, Handle, Active - 1);
        stop -> exit(shutdown)
    end.

%% @doc A fixed set of acceptors bounds concurrent TCP connections. Each
%% connection can carry successive queries and releases its worker when idle.
accept(Listener, Handle) ->
    case gen_tcp:accept(Listener) of
        {ok, Socket} ->
            try
                case inet:peername(Socket) of
                    {ok, {IP, Port}} -> tcp(Socket, Handle, peer(tcp, IP, Port));
                    {error, _} -> ok
                end
            after
                gen_tcp:close(Socket)
            end,
            accept(Listener, Handle);
        {error, closed} -> ok
    end.

tcp(Socket, Handle, Peer) ->
    case gen_tcp:recv(Socket, 0, ?TCP_TIMEOUT) of
        {ok, Packet} ->
            case dispatch(Handle, Packet, Peer) of
                ignore -> tcp(Socket, Handle, Peer);
                Response ->
                    case gen_tcp:send(Socket, Response) of
                        ok -> tcp(Socket, Handle, Peer);
                        {error, _} -> ok
                    end
            end;
        {error, _} -> ok
    end.

%% @doc Do not let an invalid request or application error kill a listener.
dispatch(Handle, Packet, Peer) ->
    try Handle(Packet, Peer)
    catch
        Class:Reason ->
            ?event(dns, {request_failed, Class, Reason}),
            ignore
    end.

peer(Transport, IP, Port) ->
    #{
        <<"transport">> => atom_to_binary(Transport),
        <<"peer">> =>
            #{
                <<"address">> => list_to_binary(inet:ntoa(IP)),
                <<"port">> => Port
            }
    }.

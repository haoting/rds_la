%% Author: haoting.wq
%% Created: 2012-5-2
%% Description: TODO: Add description to rds_la_socket
-module(back_socket).
%%
%% Exported Functions
%%

-export([listen/3, connect/4, connect/5, accept/1, recv/3, send/2, close/1,
         port/1, peername/1, sockname/1, getopts/2, setopts/2, type/1, ipv6_supported/0]).
-export([inet_ntoa/1, inet_addr/1, port_command/2, host_to_ip/1, host_to_ip/2]).

-define(ACCEPT_TIMEOUT, 2000).

listen(Port, Opts, SSLOpts) ->
    case SSLOpts of
        undefined ->
            gen_tcp:listen(Port, Opts);
        _ ->
            case ssl:listen(Port, Opts ++ SSLOpts) of
                {ok, ListenSocket} ->
                    {ok, {ssl, ListenSocket}};
                {error, _} = Err ->
                    Err
            end
    end.

connect(Host, Port, Opts, SSLOpts) ->
    case SSLOpts of
        undefined ->
            gen_tcp:connect(Host, Port, Opts);
        _ ->
            case ssl:connect(Host, Port, Opts ++ SSLOpts) of
                {ok, ConnectSocket} ->
                    {ok, {ssl, ConnectSocket}};
                {error, _} = Err ->
                    Err
            end
    end.

connect(Host, Port, Opts, SSLOpts, Timeout) ->
    case SSLOpts of
        undefined ->
            gen_tcp:connect(Host, Port, Opts, Timeout);
        _ ->
            case ssl:connect(Host, Port, Opts ++ SSLOpts, Timeout) of
                {ok, ConnectSocket} ->
                    {ok, {ssl, ConnectSocket}};
                {error, _} = Err ->
                    Err
            end
    end.

accept({ssl, ListenSocket}) ->
    % There's a bug in ssl:transport_accept/2 at the moment, which is the
    % reason for the try...catch block. Should be fixed in OTP R14.
    try ssl:transport_accept(ListenSocket) of
        {ok, Socket} ->
            case ssl:ssl_accept(Socket) of
                ok ->
                    {ok, {ssl, Socket}};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    catch
        error:{badmatch, {error, Reason}} ->
            {error, Reason}
    end;
accept(ListenSocket) ->
    gen_tcp:accept(ListenSocket, ?ACCEPT_TIMEOUT).

recv({ssl, Socket}, Length, Timeout) ->
    ssl:recv(Socket, Length, Timeout);
recv(Socket, Length, Timeout) ->
    gen_tcp:recv(Socket, Length, Timeout).

send({ssl, Socket}, Data) ->
    ssl:send(Socket, Data);
send(Socket, Data) ->
    gen_tcp:send(Socket, Data).

port_command({ssl, Socket}, Data) ->
    case ssl:send(Socket, Data) of
        ok ->
            self() ! {inet_reply, {ssl, Socket}, ok},
            true;
        {error, Reason} ->
            erlang:error(Reason)
    end;
port_command(Socket, Data) ->
    try erlang:port_command(Socket, Data) of
	    false -> {error,busy};
	    true -> true
    catch
	    error:_Error -> {error,einval}
    end.

close({ssl, Socket}) ->
    ssl:close(Socket);
close(Socket) ->
    gen_tcp:close(Socket).

port({ssl, Socket}) ->
    case ssl:sockname(Socket) of
        {ok, {_, Port}} ->
            {ok, Port};
        {error, _} = Err ->
            Err
    end;
port(Socket) ->
    inet:port(Socket).

peername({ssl, Socket}) ->
    ssl:peername(Socket);
peername(Socket) ->
    inet:peername(Socket).

sockname({ssl, Socket}) ->
    ssl:sockname(Socket);
sockname(Socket) ->
    inet:sockname(Socket).

getopts({ssl, Socket}, Opts) ->
    ssl:getopts(Socket, Opts);
getopts(Socket, Opts) ->
    inet:getopts(Socket, Opts).

setopts({ssl, Socket}, Opts) ->
    ssl:setopts(Socket, Opts);
setopts(Socket, Opts) ->
    inet:setopts(Socket, Opts).

type({ssl, _}) ->
    ssl;
type(_) ->
    plain.

inet_ntoa({A,B,C,D}) ->
    (A bsl 24) + (B bsl 16) + (C bsl 8) + D.

inet_addr(IpTuple) when is_tuple(IpTuple) ->
    IpTuple;
inet_addr(IpList) when is_list(IpList) ->
    {ok, IpTuple} = inet_parse:address(IpList),
	IpTuple.

ipv6_supported() ->
    case (catch inet:getaddr("localhost", inet6)) of
        {ok, _Addr} ->
            true;
        {error, _} ->
            false
    end.

host_to_ip(Host) ->
    host_to_ip(Host, inet).

host_to_ip(Host, Family) ->
    inet:getaddr(Host, Family).


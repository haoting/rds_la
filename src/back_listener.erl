-module(back_listener).

-include("back_proxy.hrl").

-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/5, start_link/6, new_proxy/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(RECVBUF_SIZE, 8192).
-define(BACKLOG, 128).

-record(state, {
        back_proxy_sup_name,
        host,
        ip,
        port,
        ssl_opts = undefined,
        listen_socket = null}).
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Prefix, tcp, Host, Port, ConnetOpts) ->
    start_link(Prefix, tcp, Host, Port, undefined, ConnetOpts);
start_link(Prefix, ssl, Host, Port, ConnetOpts) ->
     SSLOpts = [
                 {ssl_imp, new},
                 {certfile, local_path(["priv", "server-cert.pem"])},
                 {keyfile, local_path(["priv", "server-key.pem"])},
                 {cacertfile, local_path(["priv", "ca-cert.pem"])}
                ],
    start_link(Prefix, ssl, Host, Port, SSLOpts, ConnetOpts).

start_link(Prefix, _, Host, Port, SSLOpts, ConnetOpts) ->
    MyName = ?prefix_to_back_listener_name(Prefix),
    gen_server:start_link({local, MyName}, ?MODULE, [Prefix, Host, Port, SSLOpts, ConnetOpts], []).

new_proxy(MyName) ->
    cast(MyName, new_proxy).
%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Prefix, Host, Port, SSLOpts, ConnetOpts]) ->
    cast({listen, Host, Port, SSLOpts, ConnetOpts}),
    BackProxySupName = ?prefix_to_back_proxy_sup_name(Prefix),
    {ok, #state{back_proxy_sup_name = BackProxySupName, host = Host, port=Port}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({listen, Host, Port, SSLOpts, ConnetOpts}, State) ->
    ListenOpts = parse_connect_options(ConnetOpts),
    Ip = case Host of
        "any" -> any;
        _ -> {ok, RIp} = back_socket:host_to_ip(Host), RIp
    end,
	Opts = case Ip of
        any ->
            case back_socket:ipv6_supported() of % IPv4, and IPv6 if supported
                true -> [inet, inet6 | ListenOpts];
                _ -> ListenOpts
            end;
        {_, _, _, _} -> % IPv4
            [inet, {ip, Ip} | ListenOpts];
        {_, _, _, _, _, _, _, _} -> % IPv6
            [inet6, {ip, Ip} | ListenOpts]
    end,
    {ok, LS} = listen(Port, Opts, SSLOpts),
    back_proxy_sup:new_proxy(State#state.back_proxy_sup_name, LS),
    {noreply, State#state{ip=Ip, port=Port, ssl_opts=SSLOpts, listen_socket = LS}};

handle_cast(new_proxy, State = #state{listen_socket = LS}) ->
    back_proxy_sup:new_proxy(State#state.back_proxy_sup_name, LS),
	{noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {stop, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

cast(Msg) ->
    gen_server:cast(self(), Msg).

cast(MyName, Msg) ->
    gen_server:cast(MyName, Msg).

listen(Port, Opts, SSLOpts) ->
    back_socket:listen(Port, Opts, SSLOpts).

base_option() ->
    [binary,
     {reuseaddr, true},
     {backlog, ?BACKLOG},
     {recbuf, ?RECVBUF_SIZE},
     {active, false},
     {nodelay, false}].

parse_connect_options(ConnectOpts) ->
    parse_connect_options(ConnectOpts, base_option()).

parse_connect_options([ConnectOpt|Left], Acc) ->
    parse_connect_options(Left, parse_connect_option(ConnectOpt, Acc));
parse_connect_options([], Acc) -> Acc.

%parse_connect_option({packet, PacketHeader}, Acc) ->
%    Acc ++ [{packet, PacketHeader}];
parse_connect_option(_, Acc) ->
    Acc.

get_base_dir(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

local_path(Components, Module) ->
    filename:join([get_base_dir(Module) | Components]).

local_path(Components) ->
    local_path(Components, ?MODULE).
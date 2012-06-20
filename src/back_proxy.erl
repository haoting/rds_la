-module(back_proxy).

-include("back_proxy.hrl").

-behaviour(gen_server).

-include("logger_header.hrl").

-define(SERVER, ?MODULE).

%% Performance Guidline:
%% 1. Synchronous call:
%%    Without set of set_socket_opts(Pid, DstNP, [{delay_send, true}]),
%%    performance is the same as gen_server:call by erlang rpc,
%%    reach QPS of 6.9K ~ 8.1K, each request cost 142 ~ 144 us
%% 2. Asynchronous call:
%%    You must start back_proxy with connect option {active, true},
%%    or back_proxy will be deadlock by set_active_once,
%%    performance is the same as Asynchronous cast
%% 3. Asynchronous cast:
%%    With set of set_socket_opts(Pid, DstNP, [{delay_send, true}]),
%%    performance is a little less than gen_server:cast by erlang rpc,
%%    reach QPS of 160K ~ 175K, each request cost 5.6 ~ 6.3 us,
%%    performance of gen_server:cast reach Qps 220K, each request cost 4.5 us,
%%    back_proxy is 1 ~ 2 us slower than erlang rpc

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/5, stop/1]).
-export([remote_cast/3, remote_call/3]).
-export([local_cast/2, local_call/2]).
-export([add_timer/2, del_timer/1]).
-export([sync_send_to/2]).
-export([allsr/1, execute/2, process_flag/3, system_info/2]).
-export([get_socket_opts/3, set_socket_opts/3]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([behaviour_info/1]).

-define(SEND_RECONNECT_TIME_INTERVAL, 5 * 1000).    %% 5 seconds

-define(PERF_REPORT_QUANTITY, 10000).
-define(PERF_QPS_INIT(Ref), perf_qps:mperf_init(Ref)).
-define(PERF_QPS_SCOPE(Ref, Position, Prefix), perf_qps:mperf_scope(Ref, Position, Prefix, ?PERF_REPORT_QUANTITY)).
-define(PERF_QPS_QPS(Ref, Prefix), perf_qps:mperf_qps(Ref, Prefix, ?PERF_REPORT_QUANTITY)).

-record(state, {
    back_listener_name,
    mod,
    mod_state,
    connect_opts,
    %% send only
    local_from = undefined,
    %% receive socket
    rsocket,
    %% send socket ets table
    ssocket_table               %% socket_entry
}).

-record(socket_entry, {
    dstnp,                      %% {Node, Port}
    host,
    socket = undefined,
    reconnectable = true,
    watched = false
}).

behaviour_info(callbacks) ->
    [
        %% ------------------------------------------------------------
        %% Function: init/1
        %% Proto: init(ModArg)
        %% Description: Initialize module
        %% Returns:
        %%          stop                                                   |
        %%          {ok, ModState}                                         |
        %% ----------------------------------------------------------
        {init, 1},

        %% ------------------------------------------------------------
        %% Function: terminate/2
        %% Proto: terminate(Reason, ModState)
        %% Description: Terminate module
        %% Returns:
        %%          ok
        %% ----------------------------------------------------------
        {terminate, 2},

        %% ------------------------------------------------------------
        %% {DstNP, Msg: [Binary | Term]}:
        %%          {local,        Term}    ->   from local pid
        %%          {client,       Binary}  ->   server recv from remote client
        %%          {{Node, Port}, Binary}  ->   server/client recv from cascade
        %% Callback Return Operations:
        %%          {wait_msg, ModState}                                   |
        %%          {noreply, ModState}                                    |
        %%          {{reply, Reply}, ModState}                             |
        %%          {{watch_send, DstNP, Binary}, ModState}                |
        %%          {{nowatch_send, DstNP, Binary}, ModState}              |
        %%          {{clear_watch, DstNPList, Then}, ModState}
        %% ----------------------------------------------------------

        %% ------------------------------------------------------------
        %% Function: handle_msg/3
        %% Proto: handle_msg(DstNP, Binary, ModState)
        %% Description: Handling waited messages
        %% ----------------------------------------------------------
        {handle_msg, 3},

        %% ------------------------------------------------------------
        %% Function: handle_local_info/2
        %% Proto: handle_local_info(Term, ModState)
        %% Description: Handling local miscellaneous messages
        %% Miscellaneous Msg From local:
        %%          {local_call, Term}                                     |
        %%          {local_cast, Term}                                     |
        %%          {remote_call, DstNP, Term}                             |
        %%          {remote_cast, DstNP, Term}                             |
        %%          {last_send_failed, DstNP, LastOperation, FaildResult}  |
        %%          {connection_down, DstNP}
        %% ----------------------------------------------------------
        {handle_local_info, 2},

        %% ------------------------------------------------------------
        %% Function: handle_nowait/1
        %% Proto: handle_nowait(ModState)
        %% Description: Continue handling no wait
        %% ----------------------------------------------------------
        {handle_nowait, 1},

        %% ------------------------------------------------------------
        %% Function: handle_timer/2
        %% Proto: handle_timer(TimerMsg, ModState)
        %% Description: Handling timer
        %% ----------------------------------------------------------
        {handle_timer, 2}

        %% Becareful when mod return {{watch_send, DstNP, Binary}, ModState},
        %% back_proxy will add watch to the socket of DstNP automatically,
        %% and it can be cleared from thses method:
        %% 1. mod return {{clear_watch, DstNPList, Then}, ModState}
        %% 2. 
    ];
behaviour_info(_Other) ->
    undefined.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

%% Becaeful:
%% Mod & ModArg: for internal protocol implement
%% PacketHead: 0 for protocol not use erlang term
%%             1,2,4 for protocol use erlang term
%% Active: once for unidirectional cast & synchronous call with flow-control
%%         true for unidirectional cast & synchronous call & asynchronous call without flow-control
%% LS: undefined for client
%%     Listen Socket Port for server & proxy
start_link(Prefix, Mod, ModArg, ConnetOpts, LS) ->
    gen_server:start_link(?MODULE, [Prefix, Mod, ModArg, ConnetOpts, LS], []).

stop(Pid) ->
    cast(Pid, stop).

remote_cast(Pid, DstNP, Term) ->
    cast(Pid, {remote_cast, DstNP, Term}).
remote_call(Pid, DstNP, Term) ->
    call(Pid, {remote_call, DstNP, Term}).

local_cast(Pid, Term) ->
    cast(Pid, {local_cast, Term}).
local_call(Pid, Term) ->
    call(Pid, {local_call, Term}).

%% called from internal of mod
add_timer(Interval, TimerMsg) ->
    erlang:send_after(Interval, self(), {internal_timer, TimerMsg}).
del_timer(TRef) ->
    erlang:cancel_timer(TRef).

%% called from internal of mod
sync_send_to(DstNP, Binary) ->
    handle_sync_send(DstNP, Binary).

allsr(Pid) ->
    call(Pid, allsr).

execute(Pid, Fun) ->
    call(Pid, {execute, Fun}).

%% These 2 api mainly for process performance optimizie
%% There are some options expecially useful for process :
%%   min_bin_vheap_size
%%   min_heap_size
%%   priority
%%   scheduler_id

process_flag(Pid, Flag, Value) ->
    execute(Pid, fun() -> erlang:process_flag(Flag, Value) end).

system_info(Pid, Type) ->
    execute(Pid, fun() -> erlang:system_info(Type) end).

%% These 2 api mainly for socket performance optimizie
%% For unidirectional cast, there are 3 socket options expecially useful:
%%   delay_send
%%   sndbuf
%%   recbuf

get_socket_opts(Pid, DstNP, Opts) ->
    call(Pid, {get_socket_opts, DstNP, Opts}).

set_socket_opts(Pid, DstNP, Opts) ->
    call(Pid, {set_socket_opts, DstNP, Opts}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Prefix, Mod, ModArg, ConnetOpts, LS]) ->
    cast(self(), {init, ModArg, LS}),
    %?PERF_QPS_INIT(handle_info_tcp),
    %?PERF_QPS_INIT(remote_call),
    %?PERF_QPS_INIT(remote_cast),
    %?PERF_QPS_INIT(handle_msg),
    %?PERF_QPS_INIT(local_handle_msg_wait),
    %?PERF_QPS_INIT(local_handle_local_info),
    %?PERF_QPS_INIT(before_do_send),
    %?PERF_QPS_INIT(do_send),
    %?PERF_QPS_INIT(local_reply),
    %?PERF_QPS_INIT(get_ssocket),
    %?PERF_QPS_INIT(set_ssocket),
    %?PERF_QPS_INIT(get_ssocket_dst),
    %?PERF_QPS_INIT(remote_call_process),
    ParsedConnectOpts = parse_connect_options(ConnetOpts),
    ?DEBUG("ParsedConnectOpts: ~p~n", [ParsedConnectOpts]),
    BackListenerName = ?prefix_to_back_listener_name(Prefix),
    {ok, #state{back_listener_name = BackListenerName, mod = Mod, connect_opts = ParsedConnectOpts}}.

handle_call({remote_call, DstNP, Term}, From, State = #state{local_from = OFrom}) ->
    %?PERF_QPS_SCOPE(remote_call_process, start, "back_proxy remote_call"),
    %?PERF_QPS_SCOPE(remote_call, start, "back_proxy remote_call"),
    case OFrom of
        undefined ->
            Res = case handle_local_info({remote_call, DstNP, Term}, State#state{local_from = From}) of
                {ok, NState} -> {noreply, NState};
                {Error, NState} -> {reply, Error, NState}
            end,
            %?PERF_QPS_SCOPE(remote_call, stop, "back_proxy remote_call"),
            ?PERF_QPS_QPS(remote_call, "back_proxy remote_call"),
            Res;
        _ -> {reply, {error, in_call}, State}
    end;

handle_call({local_call, Term}, From, State = #state{local_from = OFrom}) ->
    case OFrom of
        undefined ->
            case handle_local_info({local_call, Term}, State#state{local_from = From}) of
                {ok, NState} -> {noreply, NState};
                {Error, NState} -> {reply, Error, NState}
            end;
        _ -> {reply, {error, in_call}, State}
    end;

handle_call(allsr, _From, State = #state{rsocket = RSocket, ssocket_table = SSocketTable}) ->
    {reply, {{receiver, list_receive_socket(RSocket)},
             {sender, list_all_send_socket(SSocketTable)}}, State};

handle_call({execute, Fun}, _From, State) ->
    {reply, Fun(), State};

handle_call({get_socket_opts, DstNP, Opts}, _From,
    State = #state{rsocket = RSocket, ssocket_table = SSocketTable}) ->
    Ret = case DstNP of
        undefined -> undefined;
        rsocket -> getopts(RSocket, Opts);
        _ -> get_dstnp_ssocket_opts(DstNP, Opts, SSocketTable)
    end,
    {reply, Ret, State};

handle_call({set_socket_opts, DstNP, Opts}, _From,
    State = #state{rsocket = RSocket, ssocket_table = SSocketTable}) ->
    case DstNP of
        undefined -> ok;
        rsocket -> setopts(RSocket, Opts);
        _ -> set_dstnp_ssocket_opts(DstNP, Opts, SSocketTable)
    end,
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({init, ModArg, LS}, State) ->
    handle_init(ModArg, LS, State);

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast({remote_cast, DstNP, Term}, State) ->
    %?PERF_QPS_SCOPE(remote_cast, start, "back_proxy remote_cast"),
    {_, NState} = handle_local_info({remote_cast, DstNP, Term}, State),
    %?PERF_QPS_SCOPE(remote_cast, stop, "back_proxy remote_cast"),
    ?PERF_QPS_QPS(remote_cast, "back_proxy remote_cast"),
    {noreply, NState};

handle_cast({local_cast, Term}, State) ->
    case handle_local_info({local_cast, Term}, State) of
        {ok, NState} -> {noreply, NState};
        {_Error, NState} -> {noreply, NState}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

%% for receiving
handle_info({tcp, Socket, Binary}, State) ->
    %?PERF_QPS_SCOPE(handle_info_tcp, start, "back_proxy handle_info_tcp"),
    {_Res, NState} = handle_msg(Socket, Binary, State),
    %?PERF_QPS_SCOPE(handle_info_tcp, stop, "back_proxy handle_info_tcp"),
    %?PERF_QPS_QPS(handle_info_tcp, "back_proxy handle_info_tcp"),
    %?DEBUG("tcp hand_msg res: ~p~n", [Res]),
    %?PERF_QPS_SCOPE(remote_call_process, stop, "back_proxy remote_call"),
    {noreply, NState};

%% for sending
handle_info({inet_reply, _, ok}, State) ->
	{noreply, State};

%% send only proxy not suffer from the broken of the recv socket
handle_info({inet_reply, RSocket, _}, State = #state{rsocket = RSocket}) ->
    %% reply_to_client_failed
    {stop, normal, State};

handle_info({inet_reply, SSocket, _}, State) ->
    handle_connection_down(SSocket, State);

%% send only proxy not suffer from the broken of the recv socket
handle_info({tcp_closed, RSocket}, State = #state{rsocket = RSocket}) ->
    %% client_closed
    {stop, normal, State};

handle_info({tcp_closed, SSocket}, State) ->
    handle_connection_down(SSocket, State);

handle_info({set_ssocket_reconnectable, DstNP},
    State = #state{ssocket_table = SSocketTable}) ->
    set_dstnp_ssocket_reconnectable(DstNP, SSocketTable),
    {noreply, State};

handle_info({internal_timer, TimerMsg}, State) ->
    {_, NState} = handle_timer(TimerMsg, State),
    {noreply, NState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    handle_full_stop(Reason, State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal State Function Definitions
%% ------------------------------------------------------------------

handle_init(ModArg, LS, State = #state{mod = Mod}) ->
    ?DEBUG("initialize~n", []),
    case LS of
        undefined -> handle_init_mod(Mod, ModArg, undefined, State);
        _ -> handle_init_accept(ModArg, LS, State)
    end.

handle_init_accept(ModArg, LS, State = #state{back_listener_name = BackListenerName,
                                              mod = Mod, connect_opts = ConnetOpts}) ->
    clear_msg_box(),
    case catch accept(LS) of
        {ok, RSocket} ->
            ?DEBUG("accepted~n", []),
            set_packet_head_init(RSocket, ConnetOpts),
            set_active_init(RSocket, ConnetOpts),
            back_listener:new_proxy(BackListenerName),
            ?DEBUG("new_proxy~n", []),
            handle_init_mod(Mod, ModArg, RSocket, State);
        {error, closed} ->
            ?DEBUG("accept error: closed~n", []),
            back_listener:new_proxy(BackListenerName),
            {stop, normal, State};
        {error, timeout} ->
            handle_init_accept(ModArg, LS, State);
        {error, esslaccept} ->
            ?DEBUG("accept error: esslaccept~n", []),
            back_listener:new_proxy(BackListenerName),
            {stop, normal, State};
        {error, _Reason} ->
            %?DEBUG("accept error: ~p~n", [Reason]),
            back_listener:new_proxy(BackListenerName),
            {stop, normal, State};
        _Other ->
            %?DEBUG("accept error: ~p~n", [Other]),
            back_listener:new_proxy(BackListenerName),
            {stop, normal, State}
    end.

handle_init_mod(Mod, ModArg, RSocket, State = #state{connect_opts = ConnetOpts}) ->
    case Mod:init(ModArg) of
        {ok, ModState} ->
            SSocketTable = init_ssocket(),
            put(ssocket_table, SSocketTable),
            put(connect_opts, ConnetOpts),
            ?DEBUG("RSocket: ~p, SSocketTable:~p~n", [RSocket, SSocketTable]),
            {noreply, State#state{mod_state = ModState,
                                  rsocket = RSocket,
                                  ssocket_table = SSocketTable}};
        stop ->
            %% init_mod_failed
            {stop, normal, State}
    end.

handle_connection_down(SSocket, State = #state{ssocket_table = SSocketTable}) ->
    ?DEBUG("connection down~n", []),
    case close_send_socket(SSocket, SSocketTable) of
        ok -> {noreply, State};
        ConnectionDownMsg = {connection_down, _DstNP} ->
            {_, NState} = handle_local_info(ConnectionDownMsg, State),
            {noreply, NState}
    end.

handle_full_stop(Reason, #state{
    mod = Mod, mod_state = ModState, rsocket = RSocket, ssocket_table = SSocketTable}) ->
    ?DEBUG("handle_full_stop for reason: ~p~n", [Reason]),
    Mod:terminate(Reason, ModState),
    close(RSocket),
    close_all_send_socket(SSocketTable),
    destroy_ssocket(SSocketTable).

%% ------------------------------------------------------------------
%% Connection Options
%% ------------------------------------------------------------------

default_connect_opts() -> {0, once}.

get_active({_PacketHead, Active}) -> Active.

get_packet_head({PacketHead, _Active}) -> PacketHead.

parse_connect_options(ConnectOpts) ->
    parse_connect_options(ConnectOpts, default_connect_opts()).

parse_connect_options([ConnectOpt|Left], Acc) ->
    parse_connect_options(Left, parse_connect_option(ConnectOpt, Acc));
parse_connect_options([], Acc) -> Acc.

parse_connect_option({active, Active}, {PacketHeader, _}) -> {PacketHeader, Active};
parse_connect_option({packet, PacketHeader}, {_, Active}) -> {PacketHeader, Active}.
%% ------------------------------------------------------------------
%% Internal Small Function Definitions
%% ------------------------------------------------------------------

call(Pid, Msg) ->
    gen_server:call(Pid, Msg, infinity).

cast(Pid, Msg) ->
    gen_server:cast(Pid, Msg).

accept(LS) ->
    back_socket:accept(LS).

set_packet_head_init(Socket, ConnetOpts) ->
    back_socket:setopts(Socket, [{packet, get_packet_head(ConnetOpts)}]).

set_active_init(Socket, ConnetOpts) ->
    back_socket:setopts(Socket, [{active, get_active(ConnetOpts)}]).

set_active(Socket, ConnetOpts) ->
    case get_active(ConnetOpts) of
        once -> back_socket:setopts(Socket, [{active, once}]);
        _ -> ok
    end.

connect(Ip, Port, PacketHead) ->
    Opts = [inet, binary, {active, false}, {packet, PacketHead}, {reuseaddr, true}, {nodelay, true}],
    do_connect(Ip, Port, Opts).

do_connect(Ip, Port, Opts) ->
    do_connect(Ip, Port, Opts, undefined).

do_connect(Ip, Port, Opts, SSLOpts) ->
    back_socket:connect(Ip, Port, Opts, SSLOpts).

send(SSocket, Binary) ->
   ?DEBUG("send socket local hostname: ~p~n", [back_socket:sockname(SSocket)]),
    case back_socket:port_command(SSocket, Binary) of
        true -> ok;
        Other -> Other
    end.

sync_send(SSocket, Binary) ->
    back_socket:send(SSocket, Binary).

close(undefined) ->
    ok;
close(Socket) ->
    back_socket:close(Socket).

getopts(undefined, _Opts) ->
    undefined;
getopts(Socket, Opts) ->
    back_socket:getopts(Socket, Opts).

setopts(undefined, _Opts) ->
    ok;
setopts(Socket, Opts) ->
    back_socket:setopts(Socket, Opts).

clear_msg_box() ->
    receive
        _ -> clear_msg_box()
    after
        0 -> ok
    end.
%% ------------------------------------------------------------------
%% Send Socket Table Operations
%% ------------------------------------------------------------------

close_send_socket(SSocket, SSocketTable) ->
    case get_ssocket_dst(SSocket, SSocketTable) of
        undefined -> ok;
        SocketEntry = #socket_entry{dstnp = DstNP, watched = Watched} ->
            case Watched of
                false ->
                    ?DEBUG("ssocket not watched: ~p~n", [DstNP]),
                    set_ssocket(set_ssocket_undefined_and_nowatched(SocketEntry), SSocketTable),
                    ok;
                true ->
                    ?DEBUG("ssocket watched: ~p~n", [DstNP]),
                    set_ssocket(set_ssocket_undefined_and_nowatched(SocketEntry), SSocketTable),
                    {connection_down, DstNP}
            end
    end.

close_all_send_socket(undefined) -> ok;
close_all_send_socket(SSocketTable) ->
    ets:foldl(
        fun(#socket_entry{socket = Socket}, Acc)->
               close(Socket),
               Acc;
           (_, Acc)-> Acc
        end, undefined, SSocketTable).

get_ssocket_dst(SSocket, SSocketTable) ->
    Pattern = #socket_entry{socket = SSocket, _ = '_'},
    %?PERF_QPS_SCOPE(get_ssocket_dst, start, "back_proxy get_ssocket_dst"),
    Res = case ets:match_object(SSocketTable, Pattern, 1) of
        '$end_of_table' -> undefined;
        {[SSocketEntry], _} -> SSocketEntry;
        _ -> undefined
    end,
    %?PERF_QPS_SCOPE(get_ssocket_dst, stop, "back_proxy get_ssocket_dst"),
    Res.

set_dstnp_ssocket_opts(DstNP, Opts, SSocketTable) ->
    get_ssocket(DstNP, fun set_ssocket_opts/2, Opts, SSocketTable).

get_dstnp_ssocket_opts(DstNP, Opts, SSocketTable) ->
    get_ssocket(DstNP, fun get_ssocket_opts/2, Opts, SSocketTable).

set_dstnp_ssocket_reconnectable(DstNP, SSocketTable) ->
    get_and_set_ssocket(DstNP, fun set_ssocket_reconnectable/1, SSocketTable).

clear_dstnp_ssocket_watched(DstNP, SSocketTable) ->
    get_and_set_ssocket(DstNP, fun clear_ssocket_watched/1, SSocketTable).

clear_ssocket_watched_list(DstNPList, SSocketTable) ->
    lists:foreach(fun(DstNP) ->
                      clear_dstnp_ssocket_watched(DstNP, SSocketTable)
                  end, DstNPList).

list_receive_socket(RSocket) ->
    case RSocket of
        undefined -> undefined;
        _ ->
            {_, Local} = back_socket:sockname(RSocket),
            {_, Remote} = back_socket:peername(RSocket),
            {Local, '->', Remote}
    end.

list_all_send_socket(SSocketTable) ->
    ets:foldl(
        fun(#socket_entry{dstnp = DstNP, socket = Socket}, Acc)->
            S = case Socket of
                undefined ->
                    {DstNP, dead};
                _ ->
                    {_, Local} = back_socket:sockname(Socket),
                    {_, Remote} = back_socket:peername(Socket),
                    {DstNP, Local, '->', Remote}
            end,
            [S|Acc]
        end, [], SSocketTable).

%% ------------------------------------------------------------------
%% Socket Entry Util Operations
%% ------------------------------------------------------------------

node_to_host(Node) ->
    [_, Host] = string:tokens(atom_to_list(Node), "@"),
    Host.

new_ssocket_entry(DstNP = {Node, _Port}) ->
    #socket_entry{dstnp = DstNP, host = node_to_host(Node)}.

set_ssocket_opts(SocketEntry, Opts) ->
    setopts(SocketEntry#socket_entry.socket, Opts).

get_ssocket_opts(SocketEntry, Opts) ->
    getopts(SocketEntry#socket_entry.socket, Opts).

set_ssocket_and_unreconnectable(SSocket, SocketEntry) ->
    SocketEntry#socket_entry{socket = SSocket, reconnectable = false}.

set_ssocket_ssocket_undefined(SocketEntry = #socket_entry{socket = SSocket}) ->
    close(SSocket),
    SocketEntry#socket_entry{socket = undefined}.

set_ssocket_reconnectable(SocketEntry) ->
    SocketEntry#socket_entry{reconnectable = true}.

set_ssocket_unreconnectable(SocketEntry) ->
    SocketEntry#socket_entry{reconnectable = false}.

set_ssocket_watched(SocketEntry) ->
    SocketEntry#socket_entry{watched = true}.

clear_ssocket_watched(SocketEntry) ->
    SocketEntry#socket_entry{watched = false}.

set_ssocket_undefined_and_nowatched(SocketEntry = #socket_entry{socket = SSocket}) ->
    close(SSocket),
    SocketEntry#socket_entry{socket = undefined, watched = false}.

%% ------------------------------------------------------------------
%% Socket Entry Raw Operations
%% ------------------------------------------------------------------

init_ssocket() ->
    ets:new(?MODULE, [set, public, {keypos, #socket_entry.dstnp}]).

destroy_ssocket(undefined) -> ok;
destroy_ssocket(SSocketTable) ->
    ets:delete(SSocketTable).

get_ssocket(DstNP, SSocketTable) ->
    %?DEBUG("get_ssocket of ~p~n", [DstNP]),
    %?PERF_QPS_SCOPE(get_ssocket, start, "back_proxy get_ssocket"),
    Got = case ets:match_object(SSocketTable, #socket_entry{dstnp = DstNP, _ = '_'}) of
        [] -> undefined;
        [SocketEntry] -> SocketEntry
    end,
    %?PERF_QPS_SCOPE(get_ssocket, stop, "back_proxy get_ssocket"),
    Got.

set_ssocket(SocketEntry, SSocketTable) ->
    %?DEBUG("set_ssocket of ~p~n", [SocketEntry]),
    %?PERF_QPS_SCOPE(set_ssocket, start, "back_proxy set_ssocket"),
    Set = ets:insert(SSocketTable, SocketEntry),
    %?PERF_QPS_SCOPE(set_ssocket, stop, "back_proxy set_ssocket"),
    Set.

get_ssocket(DstNP, Fun, Arg, SSocketTable) ->
    case get_ssocket(DstNP, SSocketTable) of
        undefined -> undefined;
        SocketEntry -> Fun(SocketEntry, Arg)
    end.

get_and_set_ssocket(DstNP, Fun, SSocketTable) ->
    case get_ssocket(DstNP, SSocketTable) of
        undefined -> ok;
        SocketEntry -> set_ssocket(Fun(SocketEntry), SSocketTable)
    end.

%get_and_set_ssocket(DstNP, Fun, Arg, SSocketTable) ->
%    case get_ssocket(DstNP, SSocketTable) of
%        undefined -> ok;
%        SocketEntry -> set_ssocket(Fun(SocketEntry, Arg), SSocketTable)
%    end.

%del_ssocket(DstNP, SSocketTable) ->
%    ets:match_delete(SSocketTable, #socket_entry{dstnp = DstNP, _ = '_'}).


%% ------------------------------------------------------------------
%% Protocol Function Definitions
%% ------------------------------------------------------------------

%% Send -------------------------------------------------------------

ensure_connect(Host, Port, ConnectOpts) ->
    {ok, Ip} = back_socket:host_to_ip(Host),
    case connect(Ip, Port, get_packet_head(ConnectOpts)) of
        {ok, SSocket} ->
            ?DEBUG("connect success to dst ip: ~p port: ~p~n", [Ip, Port]),
            set_active_init(SSocket, ConnectOpts),
            {ok, SSocket};
        {error, Reason} ->
            ?DEBUG("connect failed to dst ip: ~p port: ~p~n", [Ip, Port]),
            {error, Reason}
    end.

do_send(SSocket, Binary, SendFun) ->
	%io:format("send binary length: ~p~ncontent:~p~n", [erlang:byte_size(Binary), Binary]),
    %?PERF_QPS_SCOPE(before_do_send, stop, "back_proxy before_do_send"),
    %?PERF_QPS_SCOPE(do_send, start, "back_proxy do_send"),
	Res = case SendFun(SSocket, Binary) of
		ok -> ok;
		Error -> Error
	end,
    %?PERF_QPS_SCOPE(do_send, stop, "back_proxy do_send"),
    Res.

remote_direct_ssend(SocketEntry, Binary, Watched, SSocketTable, SendFun) ->
    case do_send(SocketEntry#socket_entry.socket, Binary, SendFun) of
        ok ->
            case Watched of
                true -> set_ssocket(set_ssocket_watched(SocketEntry), SSocketTable);
                false -> ok
            end,
            ok;
        Error ->
            set_ssocket(set_ssocket_ssocket_undefined(SocketEntry), SSocketTable),
            Error
    end.

remote_connect_ssend(SocketEntry = #socket_entry{dstnp = DstNP},
    Binary, Watched, SSocketTable, ConnectOpts, SendFun) ->
    case SocketEntry#socket_entry.reconnectable of
        true ->
            erlang:send_after(?SEND_RECONNECT_TIME_INTERVAL, self(), {set_ssocket_reconnectable, DstNP}),
            {_Node, Port} = DstNP,
            case ensure_connect(SocketEntry#socket_entry.host, Port, ConnectOpts) of
                {error, _} ->
                    set_ssocket(set_ssocket_unreconnectable(SocketEntry), SSocketTable),
                    {error, no_socket};
                {ok, ConnectedSocket} ->
                    case do_send(ConnectedSocket, Binary, SendFun) of
                        ok ->
                            WatchedSocketEntry = case Watched of
                                true -> set_ssocket_watched(SocketEntry);
                                false -> SocketEntry
                            end,
                            set_ssocket(set_ssocket_and_unreconnectable(ConnectedSocket, WatchedSocketEntry),
                                        SSocketTable),
                            ok;
                        Error ->
                            close(ConnectedSocket),
                            set_ssocket(set_ssocket_unreconnectable(SocketEntry), SSocketTable),
                            Error
                    end
            end;
        false ->
            %% the ssocket must be in ssocket_table, and it can't connect to dstnp
            {error, no_socket}
    end.

remote_ssend(DstNP, Binary, Watched, SendFun) ->
    SSocketTable = get(ssocket_table),
    ConnectOpts = get(connect_opts),
    Result = case get_ssocket(DstNP, SSocketTable) of
        SocketEntry when SocketEntry#socket_entry.socket =/= undefined ->
            remote_direct_ssend(SocketEntry, Binary, Watched, SSocketTable, SendFun);
        NullOrUndefined ->
            SocketEntry = case NullOrUndefined of
                undefined -> new_ssocket_entry(DstNP);
                NSocketEntry -> NSocketEntry
            end,
            remote_connect_ssend(SocketEntry, Binary, Watched, SSocketTable, ConnectOpts, SendFun)
    end,
    Result.

handle_nowatch_send(DstNP, Binary) ->
    %?PERF_QPS_SCOPE(before_do_send, start, "back_proxy before_do_send"),
    remote_ssend(DstNP, Binary, false, fun send/2).

handle_watch_send(DstNP, Binary) ->
    %?PERF_QPS_SCOPE(before_do_send, start, "back_proxy before_do_send"),
    remote_ssend(DstNP, Binary, true, fun send/2).

handle_sync_send(DstNP, Binary) ->
    remote_ssend(DstNP, Binary, true, fun sync_send/2).

remote_reply(Socket, Binary, State) ->
    %?PERF_QPS_SCOPE(before_do_send, start, "back_proxy before_do_send"),
    do_send(Socket, Binary, fun send/2),
    {ok, State}.

local_reply(Reply, State = #state{local_from = LocalFrom}) ->
    Result = case LocalFrom of
        undefined -> ok;
        _ ->
            %?PERF_QPS_SCOPE(local_reply, start, "back_proxy local_reply"),
            gen_server:reply(LocalFrom, Reply),
            %?PERF_QPS_SCOPE(local_reply, stop, "back_proxy local_reply"),
            ok
    end,
    {Result, State#state{local_from = undefined}}.

%% Receive ----------------------------------------------------------

local_handle_msg_wait(From, Binary, State = #state{mod = Mod, mod_state = ModState}) ->
    %?PERF_QPS_SCOPE(local_handle_msg_wait, start, "back_proxy local_handle_msg_wait"),
    %% to get performance, you can perf them in your protocol
    Handled = Mod:handle_msg(From, Binary, ModState),
    %?PERF_QPS_SCOPE(local_handle_msg_wait, stop, "back_proxy local_handle_msg_wait"),
    local_handle_operation(Handled, State).

local_handle_nowait(State = #state{mod = Mod, mod_state = ModState}) ->
    Handled = Mod:handle_nowait(ModState),
    local_handle_operation(Handled, State).

local_handle_timer(TimerMsg, State = #state{mod = Mod, mod_state = ModState}) ->
    Handled = Mod:handle_timer(TimerMsg, ModState),
    local_handle_operation(Handled, State).

local_handle_local_info(Term, State = #state{mod = Mod, mod_state = ModState}) ->
    %?PERF_QPS_SCOPE(local_handle_local_info, start, "back_proxy local_handle_local_info"),
    Handled = Mod:handle_local_info(Term, ModState),
    %?PERF_QPS_SCOPE(local_handle_local_info, stop, "back_proxy local_handle_local_info"),
    local_handle_operation(Handled, State).

local_handle_send_result(_DstNP, _LastOperation, ok, State) ->
    local_handle_nowait(State);
local_handle_send_result(DstNP, LastOperation, FaildResult, State) ->
    SendFailedMsg = {last_send_failed, DstNP, LastOperation, FaildResult},
    local_handle_local_info(SendFailedMsg, State).

local_handle_operation({LastOperation, NModState}, State = #state{rsocket = RSocket}) ->
    case LastOperation of
        wait_msg ->
            ?DEBUG("local_handle_operation:~n    wait_msg~n", []),
            {ok, State#state{mod_state = NModState}};
        noreply ->
            ?DEBUG("local_handle_operation:~n    noreply~n", []),
            local_handle_nowait(State#state{mod_state = NModState});
        {reply, Reply} ->
            {DstNP, {SendResult, NState}} = case RSocket =:= undefined of
                true ->
                    ?DEBUG("local_handle_operation:~n    local {reply, ~p}~n", [Reply]),
                    %% local reply is no need to watch
                    {local, local_reply(Reply, State#state{mod_state = NModState})};
                false ->
                    ?DEBUG("local_handle_operation:~n    remote {reply, ~p}~n", [Reply]),
                    %% receive socket is no need to watch
                    {client, remote_reply(RSocket, Reply, State#state{mod_state = NModState})}
            end,
            local_handle_send_result(DstNP, LastOperation, SendResult, NState);
        {watch_send, DstNP, Binary} ->
            ?DEBUG("local_handle_operation:~n    {watch_send, ~p, ~p}~n", [DstNP, Binary]),
            %% normal send socket need to watch
            SendResult = handle_watch_send(DstNP, Binary),
			local_handle_send_result(DstNP, LastOperation, SendResult, State#state{mod_state = NModState});
        {nowatch_send, DstNP, Binary} ->
            ?DEBUG("local_handle_operation:~n    {nowatch_send, ~p, ~p}~n", [DstNP, Binary]),
            _SendResult = handle_nowatch_send(DstNP, Binary),
			local_handle_nowait(State#state{mod_state = NModState});
        {clear_watch, DstNPList, Then} ->
            ?DEBUG("local_handle_operation:~n    {clear_watch, ~p,~n        ~p}~n", [DstNPList, Then]),
            %% for quickly process clear_watch, we play a trick here 
            %% and make clear_watch be a 2 step operation:
            %% 1. clear_watch itself
            %% 2. another operation may go on
            clear_ssocket_watched_list(DstNPList, State#state.ssocket_table),
            local_handle_operation(Then, State#state{mod_state = NModState})
    end.

handle_msg(Socket, Binary,
    State = #state{rsocket = RSocket, connect_opts = ConnetOpts, ssocket_table = SSocketTable}) ->
    ?DEBUG("tcp receive msg: ~p~n", [Binary]),
    From = if
        Socket =:= RSocket -> client;
        true ->
            %% because dict:find cost at least 4 ~ 5 us,
            %% here parse a dict cost 5 ~ 6 us when there is only one element in the dict,
            %% so we use ets table to keep the socket entry
            RFrom = case get_ssocket_dst(Socket, SSocketTable) of
                undefined -> undefined;
                #socket_entry{dstnp = DstNP} -> DstNP
            end,
            RFrom
    end,
    if
        From =:= undefined -> {ok, State};
        true ->

            %% put set_active_once before local_handle_msg_wait is more effective for
            %% asynchronous msgs, client can send a msg from now on, and server may 
            %% get a new msg to process continuously after finished current msg,
            %% so here we will get a pipeline
            %% but it is no use for synchronous because synchronous client will never
            %% send a new msg before it get the result of last msg, so we will never
            %% get a pipeline from synchronous client
            set_active(Socket, ConnetOpts),

            %?PERF_QPS_SCOPE(handle_msg, start, "back_proxy handle_msg"),
            {Res, NState} = local_handle_msg_wait(From, Binary, State),
            %?PERF_QPS_SCOPE(handle_msg, stop, "back_proxy handle_msg"),
            {Res, NState}
    end.

handle_local_info(Term, State) ->
    local_handle_local_info(Term, State).

handle_timer(TimerMsg, State) ->
    local_handle_timer(TimerMsg, State).

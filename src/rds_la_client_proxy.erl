%%% -------------------------------------------------------------------
%%% Author  : haoting.wq
%%% Description :
%%%
%%% Created : 2012-6-19
%%% -------------------------------------------------------------------
-module(rds_la_client_proxy).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("logger_header.hrl").
%% --------------------------------------------------------------------
%% External exports
-export([start_link/0, start_link/2, stop/0]).
-export([add_user/1, add_user/2, del_user/1, get_user_dstnps/1]).
-export([append_user_log_async/3, query_user_dump_slow/6, query_user_sql_stats/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(DEFAULT_APPEND_NUM, 3).
-define(DEFAULT_QUERY_NUM, 1).

-record(state, {append_num = 0, append_list = [], query_num = 0, query_list = []}).

%% ====================================================================
%% External functions
%% ====================================================================

start_link() ->
    start_link(?DEFAULT_APPEND_NUM, ?DEFAULT_QUERY_NUM).

start_link(AppendNum, QueryNum) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [AppendNum, QueryNum], []).

stop() ->
    cast(stop).

add_user(User) ->
    add_user(User, undefined).

add_user(User, Props) ->
    call({add_user, User, Props}).

del_user(User) ->
    call({del_user, User}).

get_user_dstnps(User) ->
    call({get_user_dstnps, User}).

append_user_log_async(User, ProxyId, Record) ->
    cast({append_user_log_async, User, ProxyId, Record}).

query_user_dump_slow(User, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity) ->
    call({query_user_dump_slow, User, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity}).

query_user_sql_stats(User, DateStart, DateEnd, PageStart, PageEnd) ->
    call({query_user_sql_stats, User, DateStart, DateEnd, PageStart, PageEnd}).

%% ====================================================================
%% Server functions
%% ====================================================================

cast(Msg) ->
    gen_server:cast(?SERVER, Msg).

call(Msg) ->
    gen_server:call(?SERVER, Msg, infinity).

init([AppendNum, QueryNum]) ->
    poll_index_init(),
    cast(init),
    {ok, #state{append_num = AppendNum, query_num = QueryNum}}.

handle_call({add_user, User, Props}, _From, State) ->
    Res = case Props of
        undefined -> rds_la_controller_protocol:add_user(User);
        _ -> rds_la_controller_protocol:add_user(User, Props)
    end,
    {reply, Res, State};

handle_call({del_user, User}, _From, State) ->
    Res = rds_la_controller_protocol:del_user(User),
    {reply, Res, State};

handle_call({get_user_dstnps, User}, _From, State) ->
    Res = rds_la_controller_protocol:get_user_dstnps(User),
    {reply, Res, State};

handle_call({query_user_dump_slow, User,
             DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity}, _From,
    State = #state{query_num = QueryNum, query_list = QueryList}) ->
    NextPid = pid_next(query_index, QueryNum, QueryList),
    Res = rds_la_store_protocol:query_user_dump_slow(NextPid,
        User, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity),
    {reply, Res, State};

handle_call({query_user_sql_stats, User,
             DateStart, DateEnd, PageStart, PageEnd}, _From,
    State = #state{query_num = QueryNum, query_list = QueryList}) ->
    NextPid = pid_next(query_index, QueryNum, QueryList),
    Res = rds_la_store_protocol:query_user_sql_stats(NextPid,
        User, DateStart, DateEnd, PageStart, PageEnd),
    {reply, Res, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({append_user_log_async, User, ProxyId, Record},
    State = #state{append_num = AppendNum, append_list = AppendList}) ->
    NextPid = pid_next(append_index, AppendNum, AppendList),
    rds_la_store_protocol:append_user_log_async(NextPid, User, ProxyId, Record),
    {noreply, State};

handle_cast(init, State = #state{append_num = AppendNum, query_num = QueryNum}) ->
    AppendList = start_store_client(AppendNum),
    QueryList = start_store_client(QueryNum),
    {noreply, State#state{append_list = AppendList, query_list = QueryList}};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------
start_store_client(N) ->
    [begin
         {ok, Pid} = rds_la_client_sup:new_store_client(),
         Pid
     end || _I <- lists:seq(1, N)].

poll_index_init() ->
    put(append_index, 0),
    put(query_index, 0).

index_next(Type, DstNum) ->
    NIndex = case get(Type) of
        DstNum -> 1;
        Index -> Index + 1
    end,
    put(Type, NIndex),
    NIndex.

pid_next(Type, DstNum, PidList) ->
    Next = index_next(Type, DstNum),
    lists:nth(Next, PidList).
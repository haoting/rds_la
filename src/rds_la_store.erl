%%% -------------------------------------------------------------------
%%% Author  : haoting.wq
%%% Description :
%%%
%%% Created : 2012-6-11
%%% -------------------------------------------------------------------
-module(rds_la_store).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("logger_header.hrl").
-include("rds_la_log.hrl").
%% --------------------------------------------------------------------
%% External exports
-export([start_link/0, start_link/1, stop/0]).
-export([add_user/1, add_user/2, del_user/1, get_user/1, local_users/0]).
-export([add_user_node/2, add_user_node/3, del_user_node/2, local_users_node/1]).
-export([append_log_async/3, append_log_async/4, append_log_sync/3, append_log_sync/4]).
-export([query_log/3, query_log/4, merge_query_results/3]).
-export([default_user_props/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(PROXY_SUB_LB_NUM, 3).

-record(state, {dir, proxy_id_list = [], indexers = []}).

%% ====================================================================
%% External functions
%% ====================================================================

start_link() ->
    Users = rds_la_api:all_users_on_node(node()),
    start_link(Users).

start_link(Users) ->
    Dir = rds_la_config:la_store_dir(),
    ok = filelib:ensure_dir(Dir ++ "/"),
    Indexers = rds_la_config:la_indexers(),
    ProxyIdList = rds_la_config:proxy_id_list(),
    %% concurrent_append_hack at start step
    NSubProxyIdList = la_proxyid_list_hack(ProxyIdList),
	gen_server:start_link({local, ?SERVER}, ?MODULE, [Users, Dir, Indexers, NSubProxyIdList], []).

stop() ->
    cast(stop).

add_user(User) ->
    add_user(User, default_user_props()).

add_user(User, Props) ->
    call({add_user, User, Props}).

del_user(User) ->
    call({del_user, User}).

get_user(User) ->
    get_user_handler(User).

local_users() ->
    call(local_users).

add_user_node(User, Node) ->
    add_user_node(User, Node, default_user_props()).

add_user_node(User, Node, Props) ->
    call(Node, {add_user, User, Props}).

del_user_node(User, Node) ->
    call(Node, {del_user, User}).

local_users_node(Node) ->
    call(Node, local_users).

append_log_async(User, ProxyId, Record) ->
	?DEBUG("Receive log ~p from proxy ~p", [Record, ProxyId]),
	case find_user_handler(User) of
		{ok, Handler} ->
			?DEBUG("Handler: ~p~n", [Handler]),
            %% concurrent_append_hack at append log step
            {NProxyId, NRecord} = la_log_hack(ProxyId, Record),
			try rds_la_handler:append_async(Handler, NProxyId, NRecord#la_record.timestamp, NRecord)
            catch
                _:_ -> put({user_handler, User}, undefined)
            end;
		_ -> void
	end.

append_log_async(User, Node, ProxyId, Record) ->
    {NProxyId, NRecord} = la_log_hack(ProxyId, Record),
    cast(Node, {append_log, User, NProxyId, NRecord}).

append_log_sync(User, ProxyId, Record) ->
    append_log_sync(User, node(), ProxyId, Record).

append_log_sync(User, Node, ProxyId, Record) ->
    {NProxyId, NRecord} = la_log_hack(ProxyId, Record),
    call(Node, {append_log, User, NProxyId, NRecord}).

find_user_handler_with_cache(User) ->
    case get({user_handler, User}) of
        undefined ->
            case get_user_handler(User) of
                no_handler ->
                    case call({find_user_handler, User}) of
                        {ok, UserHandler} ->
                            put({user_handler, User}, UserHandler),
                            {ok, UserHandler};
                        {error, Reason} -> {error, Reason}
                    end;
                UserHandler ->
                    put({user_handler, User}, UserHandler),
                    {ok, UserHandler}
            end;
        UserHandler -> {ok, UserHandler}
    end.

find_user_handler(User) ->
    case get_user_handler(User) of
        no_handler ->
            case call({find_user_handler, User}) of
                {ok, UserHandler} ->
                    put({user_handler, User}, UserHandler),
                    {ok, UserHandler};
                {error, Reason} -> {error, Reason}
            end;
        UserHandler ->
            put({user_handler, User}, UserHandler),
            {ok, UserHandler}
    end.

query_log(User, Type, Condition) ->
    call({query_log, User, Type, Condition}).

query_log(User, Node, Type, Condition) ->
    call(Node, {query_log, User, Type, Condition}).

merge_query_results(Type, ResultList, Condition) ->
    case type_to_index_module(Type) of
        {error, Reason} -> {error, Reason};
        IndexModule ->
             rds_la_handler:merge_query_results(IndexModule, ResultList, Condition)
     end.

%% ====================================================================
%% Server functions
%% ====================================================================
call(Node, Msg) ->
	gen_server:call({?SERVER, Node}, Msg, infinity).

cast(Node, Msg) ->
    gen_server:cast({?SERVER, Node}, Msg).

call(Msg) ->
	gen_server:call(?SERVER, Msg, infinity).

cast(Msg) ->
	gen_server:cast(?SERVER, Msg).

init([Users, Dir, Indexers, ProxyIdList]) ->
    cast({init, Users}),
    {ok, #state{dir = Dir, indexers = Indexers, proxy_id_list = ProxyIdList}}.

handle_call({find_user_handler, User}, _From, State) ->
	case ensure_handler_start(State, User, "find_user_handler", default_user_props()) of
		{_, {error, Reason}} -> {reply, {error, Reason}, State};
		{NState, Handler} -> {reply, {ok, Handler}, NState}
	end;

handle_call({append_log, User, ProxyId, Record}, _From, State) ->
	?DEBUG("append for user ~s from ~p with log ~p~n", [User, ProxyId, Record]),
	case ensure_handler_start(State, User, "find_user_handler", default_user_props()) of
		{_, {error, Reason}} -> {reply, {error, Reason}, State};
		{NState, Handler} ->
            rds_la_handler:append_async(Handler, ProxyId, Record#la_record.timestamp, Record),
            {reply, ok, NState}
	end;

handle_call({query_log, User, Type, Condition}, From, State) ->
	?DEBUG("query ~p for user ~s with condition ~p~n", [Type, User, Condition]),
	case ensure_handler_start(State, User, "find_user_handler", default_user_props()) of
		{_, {error, Reason}} -> {reply, {error, Reason}, State};
		{NState, Handler} ->
            case do_query(Type, Handler, From, Condition) of
			    {error, Reason} -> {reply, {error, Reason}, NState};
			    _ -> {noreply, NState}
            end
	end;

handle_call({add_user, User, Props}, _From, State) ->
	case ensure_handler_start(State, User, "add_user", Props) of
		{_, {error, Reason}} -> {reply, {error, Reason}, State};
		{NState, _} -> {reply, ok, NState}
	end;

handle_call({del_user, User}, _From, State) ->
    {reply, ok, del_handler(State, User)};

handle_call(local_users, _From, State) ->
    {reply, rds_la_epoolsup:all_handler(), State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({append_log, User, ProxyId, Record}, State) ->
	?DEBUG("append for user ~s from ~p with log ~p~n", [User, ProxyId, Record]),
	case ensure_handler_start(State, User, "find_user_handler", default_user_props()) of
		{_, {error, _Reason}} -> {noreply, State};
		{NState, Handler} ->
            rds_la_handler:append_async(Handler, ProxyId, Record#la_record.timestamp, Record),
            {noreply, NState}
	end;

handle_cast({init, Users}, State) ->
    {noreply, ensure_all_handler_start(Users, State)};

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

default_user_props() ->
    [
        {concurrent_write_num, 3}
    ].

type_to_index_module(dump_slow) -> rds_la_indexer_sql_dumpslow;
type_to_index_module(sql_stats) -> rds_la_indexer_sql_stats;
type_to_index_module(_) -> {error, unsupport_index}.

do_query(Type, Handler, From, Condition) ->
    case type_to_index_module(Type) of
        {error, Reason} -> {error, Reason};
        IndexModule ->
             rds_la_handler:query_async(Handler, From, IndexModule, Condition)
     end.

ensure_all_handler_start(Users, State) ->
    {NState, _} = lists:foldl(
        fun(User, {State0, _}) -> add_handler(State0, User, default_user_props()) end,
        {State, ok}, Users),
    NState.

ensure_handler_start(State, User, Info, Props) ->
	case get_user_handler(User) of
		no_handler ->
			?DEBUG("Ensure user ~p exist by ~p", [User, Info]),
			case add_handler(State, User, Props) of
				{_, {error, Reason}} ->
					?DEBUG("But failed to start handler", []),
					{State, {error, Reason}};
				{NState, Handler} ->
					?DEBUG("And success to start handler", []),
					{NState, Handler}
			end;
		Handler -> {State, Handler}
	end.

-define(SQL_PREFIX, "sql").

add_handler(State = #state{dir = Dir, proxy_id_list = ProxyIdList, indexers = Indexers}, User, _Props) ->
	?INFO("start handler for User ~p", [User]),
	UserDir = filename:join(Dir, User),
	ok = filelib:ensure_dir(UserDir ++ "/"),
	case rds_la_epoolsup:add_handler(User, UserDir, ?SQL_PREFIX, Indexers, ProxyIdList) of
		{ok, Handler} ->
            %% we must wait for the handler to start, because of the write op of handler is async
            %% user may send msg before handler started
            rds_la_handler:wait_for_start(Handler),
            %add_user_metastore(User, Props),
			{State, Handler};
		{error, Reason} ->
			?ERROR("create handler for user ~p error ~p", [User, Reason]),
			{State, {error, failed_to_start}}
	end.

del_handler(State = #state{dir = Dir}, User) ->
	?INFO("Delete handler for User ~p", [User]),
	case get_user_handler(User) of
		no_handler -> 
			State;
		Handler ->
            %?DEBUG("Delete handler ~p", [Handler]),
			ok = rds_la_epoolsup:del_handler(Handler),
			ok = file_util:recursive_del(filename:join(Dir, User)),
            %del_user_metastore(User),
			State
	end.

get_user_handler(User) ->
	case rds_la_epoolsup:get_handler(User) of
		[] -> no_handler;
		[{_, Handler}] -> Handler
	end.

%% concurrent_append_hack

la_log_hack(ProxyId, Record) ->
	ProxyIdSub = case get(last_use_proxy_id) of
		undefined -> 1;
		?PROXY_SUB_LB_NUM -> 1;
		LastUsed -> LastUsed + 1
	end,
	put(last_use_proxy_id, ProxyIdSub),
    TimeStamp = calendar:datetime_to_gregorian_seconds(erlang:localtime()) - 
                calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
    {io_lib:format("~s_~p", [ProxyId, ProxyIdSub]), Record#la_record{timestamp = TimeStamp}}.

la_proxyid_list_hack(ProxyIdList) ->
    la_proxyid_list_hack(ProxyIdList, []).

la_proxyid_list_hack([], Acc) -> Acc;
la_proxyid_list_hack([ProxyId|ProxyIdList], Acc) ->
    la_proxyid_list_hack(ProxyIdList, Acc ++ la_proxyid_hack(ProxyId)).

la_proxyid_hack(ProxyId) ->
    [io_lib:format("~s_~p", [ProxyId, ProxyIdSub]) || ProxyIdSub <- lists:seq(1, ?PROXY_SUB_LB_NUM)].

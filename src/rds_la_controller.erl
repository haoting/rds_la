%%% -------------------------------------------------------------------
%%% Author  : haoting.wq
%%% Description :
%%%
%%% Created : 2012-6-14
%%% -------------------------------------------------------------------
-module(rds_la_controller).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

%% --------------------------------------------------------------------
%% External exports
-export([start_link/0, stop/0, controller_nodes/0]).

%% specific node

-export([all_users_on_node/1, all_users_on_node/2]).
-export([all_append_users_on_node/1, all_append_users_on_node/2]).
-export([all_query_users_on_node/1, all_query_users_on_node/2]).

-export([add_user_metastore/3, add_user_metastore/2]).
-export([del_user_metastore/2, del_user_metastore/1]).
-export([get_user_metastore/2, get_user_metastore/1]).
-export([set_user_metastore/3, set_user_metastore/2]).

-export([add_user_store/3, add_user_store/4]).
-export([del_user_store/2, del_user_store/3]).

-export([add_user_node/3, add_user_node/4]).
-export([get_user_nodes/1, get_user_nodes/2]).
-export([del_user_node/2, del_user_node/3]).

-export([add_user_nodes/3, add_user_nodes/4]).
-export([del_user_nodes/2, del_user_nodes/3]).

-export([add_user_append_node/3, add_user_append_node/4]).
-export([get_user_append_nodes/1, get_user_append_nodes/2]).
-export([del_user_append_node/2, del_user_append_node/3]).

-export([add_user_append_nodes/3, add_user_append_nodes/4]).
-export([del_user_append_nodes/2, del_user_append_nodes/3]).

-export([add_user_query_node/3, add_user_query_node/4]).
-export([get_user_query_nodes/1, get_user_query_nodes/2]).
-export([del_user_query_node/2, del_user_query_node/3]).

-export([add_user_query_nodes/3, add_user_query_nodes/4]).
-export([del_user_query_nodes/2, del_user_query_nodes/3]).

-export([append_user_node_log_sync/4, append_user_node_log_sync/5]).
-export([query_user_node_log/4, query_user_node_log/5]).

-export([query_user_node_dump_slow/4, query_user_node_dump_slow/5]).
-export([query_user_node_dump_slow/7, query_user_node_dump_slow/8]).

-export([query_user_node_sql_stats/4, query_user_node_sql_stats/5]).
-export([query_user_node_sql_stats/6, query_user_node_sql_stats/7]).

%% distribute

-export([add_user/1, add_user/2, del_user/1]).
-export([add_user_from_controller/2, add_user_from_controller/3, del_user_from_controller/2]).

-export([append_user_log_sync/3, append_user_log_sync/4]).
-export([query_user_log/3, query_user_log/4]).

-export([query_user_dump_slow/3, query_user_dump_slow/4]).
-export([query_user_dump_slow/6, query_user_dump_slow/7]).

-export([query_user_sql_stats/3, query_user_sql_stats/4]).
-export([query_user_sql_stats/5, query_user_sql_stats/6]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%% ====================================================================
%% External functions
%% ====================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    cast(stop).

controller_nodes() ->
    rds_la_config:service_nodes(rds_la_controller).

random_controller_nodes() ->
    ControllerNodes = controller_nodes(),
    Nth = random_in(length(ControllerNodes)),
    {Tail, Head} = lists:split(Nth, ControllerNodes),
    Head ++ Tail.

%% specific node

all_users_on_node(Node) ->
    call_any_success({all_users_on_node, Node}).
all_users_on_node(Controller, Node) ->
    call(Controller, {all_users_on_node, Node}).

all_append_users_on_node(Node) ->
    call_any_success({all_append_users_on_node, Node}).
all_append_users_on_node(Controller, Node) ->
    call(Controller, {all_append_users_on_node, Node}).

all_query_users_on_node(Node) ->
    call_any_success({all_query_users_on_node, Node}).
all_query_users_on_node(Controller, Node) ->
    call(Controller, {all_query_users_on_node, Node}).

add_user_metastore(User, Props) ->
    call_any_success({add_user_metastore, User, Props}).
add_user_metastore(Controller, User, Props) ->
    call(Controller, {add_user_metastore, User, Props}).

del_user_metastore(User) ->
    call_any_success({del_user_metastore, User}).
del_user_metastore(Controller, User) ->
    call(Controller, {del_user_metastore, User}).

get_user_metastore(User) ->
    call_any_success({get_user_metastore, User}).
get_user_metastore(Controller, User) ->
    call(Controller, {get_user_metastore, User}).

set_user_metastore(User, Props) ->
    call_any_success({set_user_metastore, User, Props}).
set_user_metastore(Controller, User, Props) ->
    call(Controller, {set_user_metastore, User, Props}).

add_user_store(User, Node, Props) ->
    call_any_success({add_user_store, User, Node, Props}).
add_user_store(Controller, User, Node, Props) ->
    call(Controller, {add_user_store, User, Node, Props}).

del_user_store(User, Node) ->
    call_any_success({del_user_store, User, Node}).
del_user_store(Controller, User, Node) ->
    call(Controller, {del_user_store, User, Node}).

add_user_node(User, Node, Props) ->
    call_any_success({add_user_node, User, Node, Props}).
add_user_node(Controller, User, Node, Props) ->
    call(Controller, {add_user_node, User, Node, Props}).

get_user_nodes(User) ->
    call_any_success({get_user_nodes, User}).
get_user_nodes(Controller, User) ->
    call(Controller, {get_user_nodes, User}).

del_user_node(User, Node) ->
    call_any_success({del_user_node, User, Node}).
del_user_node(Controller, User, Node) ->
    call(Controller, {del_user_node, User, Node}).

add_user_nodes(User, Nodes, Props) ->
    call_any_success({add_user_nodes, User, Nodes, Props}).
add_user_nodes(Controller, User, Nodes, Props) ->
    call(Controller, {add_user_nodes, User, Nodes, Props}).

del_user_nodes(User, Nodes) ->
    call_any_success({del_user_nodes, User, Nodes}).
del_user_nodes(Controller, User, Nodes) ->
    call(Controller, {del_user_nodes, User, Nodes}).

add_user_append_node(User, Node, Props) ->
    call_any_success({add_user_append_node, User, Node, Props}).
add_user_append_node(Controller, User, Node, Props) ->
    call(Controller, {add_user_append_node, User, Node, Props}).

get_user_append_nodes(User) ->
    call_any_success({get_user_append_nodes, User}).
get_user_append_nodes(Controller, User) ->
    call(Controller, {get_user_append_nodes, User}).

del_user_append_node(User, Node) ->
    call_any_success({del_user_append_node, User, Node}).
del_user_append_node(Controller, User, Node) ->
    call(Controller, {del_user_append_node, User, Node}).

add_user_append_nodes(User, Nodes, Props) ->
    call_any_success({add_user_append_nodes, User, Nodes, Props}).
add_user_append_nodes(Controller, User, Nodes, Props) ->
    call(Controller, {add_user_append_nodes, User, Nodes, Props}).

del_user_append_nodes(User, Nodes) ->
    call_any_success({del_user_append_nodes, User, Nodes}).
del_user_append_nodes(Controller, User, Nodes) ->
    call(Controller, {del_user_append_nodes, User, Nodes}).

add_user_query_node(User, Node, Props) ->
    call_any_success({add_user_query_node, User, Node, Props}).
add_user_query_node(Controller, User, Node, Props) ->
    call(Controller, {add_user_query_node, User, Node, Props}).

get_user_query_nodes(User) ->
    call_any_success({get_user_query_nodes, User}).
get_user_query_nodes(Controller, User) ->
    call(Controller, {get_user_query_nodes, User}).

del_user_query_node(User, Node) ->
    call_any_success({del_user_query_node, User, Node}).
del_user_query_node(Controller, User, Node) ->
    call(Controller, {del_user_query_node, User, Node}).

add_user_query_nodes(User, Nodes, Props) ->
    call_any_success({add_user_query_nodes, User, Nodes, Props}).
add_user_query_nodes(Controller, User, Nodes, Props) ->
    call(Controller, {add_user_query_nodes, User, Nodes, Props}).

del_user_query_nodes(User, Nodes) ->
    call_any_success({del_user_query_nodes, User, Nodes}).
del_user_query_nodes(Controller, User, Nodes) ->
    call(Controller, {del_user_query_nodes, User, Nodes}).

append_user_node_log_sync(User, Node, ProxyId, Record) ->
    call_any_success({append_user_node_log_sync, User, Node, ProxyId, Record}).
append_user_node_log_sync(Controller, User, Node, ProxyId, Record) ->
    call(Controller, {append_user_node_log_sync, User, Node, ProxyId, Record}).
    
query_user_node_log(User, Node, Type, Condition) ->
    call_any_success({query_user_node_log, User, Node, Type, Condition}).
query_user_node_log(Controller, User, Node, Type, Condition) ->
    call(Controller, {query_user_node_log, User, Node, Type, Condition}).
    
query_user_node_dump_slow(User, Node, DateStart, DateEnd) ->
    call_any_success({query_user_node_dump_slow, User, Node, DateStart, DateEnd}).
query_user_node_dump_slow(Controller, User, Node, DateStart, DateEnd) ->
    call(Controller, {query_user_node_dump_slow, User, Node, DateStart, DateEnd}).
    
query_user_node_dump_slow(User, Node, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity) ->
    call_any_success({query_user_node_dump_slow, User, Node, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity}).
query_user_node_dump_slow(Controller, User, Node, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity) ->
    call(Controller, {query_user_node_dump_slow, User, Node, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity}).
    
query_user_node_sql_stats(User, Node, DateStart, DateEnd) ->
    call_any_success({query_user_node_sql_stats, User, Node, DateStart, DateEnd}).
query_user_node_sql_stats(Controller, User, Node, DateStart, DateEnd) ->
    call(Controller, {query_user_node_sql_stats, User, Node, DateStart, DateEnd}).
    
query_user_node_sql_stats(User, Node, DateStart, DateEnd, PageStart, PageEnd) ->
    call_any_success({query_user_node_sql_stats, User, Node, DateStart, DateEnd, PageStart, PageEnd}).
query_user_node_sql_stats(Controller, User, Node, DateStart, DateEnd, PageStart, PageEnd) ->
    call(Controller, {query_user_node_sql_stats, User, Node, DateStart, DateEnd, PageStart, PageEnd}).

%% distribute

add_user(User) ->
    add_user(User, rds_la_api:default_user_props()).
add_user(User, Props) ->
    call_any_success({add_user, User, Props}).
del_user(User) ->
    call_any_success({del_user, User}).

add_user_from_controller(Controller, User) ->
    add_user_from_controller(Controller, User, rds_la_api:default_user_props()).
add_user_from_controller(Controller, User, Props) ->
    call(Controller, {add_user, User, Props}).
del_user_from_controller(Controller, User) ->
    call(Controller, {del_user, User}).

append_user_log_sync(User, ProxyId, Record) ->
    call_any_success({append_user_log_sync, User, ProxyId, Record}).
append_user_log_sync(Controller, User, ProxyId, Record) ->
    call(Controller, {append_user_log_sync, User, ProxyId, Record}).
    
query_user_log(User, Type, Condition) ->
    call_any_success({query_user_log, User, Type, Condition}).
query_user_log(Controller, User, Type, Condition) ->
    call(Controller, {query_user_log, User, Type, Condition}).
    
query_user_dump_slow(User, DateStart, DateEnd) ->
    call_any_success({query_user_dump_slow, User, DateStart, DateEnd}).
query_user_dump_slow(Controller, User, DateStart, DateEnd) ->
    call(Controller, {query_user_dump_slow, User, DateStart, DateEnd}).
    
query_user_dump_slow(User, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity) ->
    call_any_success({query_user_dump_slow, User, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity}).
query_user_dump_slow(Controller, User, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity) ->
    call(Controller, {query_user_dump_slow, User, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity}).
    
query_user_sql_stats(User, DateStart, DateEnd) ->
    call_any_success({query_user_sql_stats, User, DateStart, DateEnd}).
query_user_sql_stats(Controller, User, DateStart, DateEnd) ->
    call(Controller, {query_user_sql_stats, User, DateStart, DateEnd}).
    
query_user_sql_stats(User, DateStart, DateEnd, PageStart, PageEnd) ->
    call_any_success({query_user_sql_stats, User, DateStart, DateEnd, PageStart, PageEnd}).
query_user_sql_stats(Controller, User, DateStart, DateEnd, PageStart, PageEnd) ->
    call(Controller, {query_user_sql_stats, User, DateStart, DateEnd, PageStart, PageEnd}).

%% ====================================================================
%% Server functions
%% ====================================================================
call(Node, Msg) ->
    gen_server:call({?SERVER, Node}, Msg, infinity).

call_any_success(Msg) ->
    call_any_success(random_controller_nodes(), Msg).

call_any_success([Node|Nodes], Msg) ->
    case catch call(Node, Msg) of
        {node_ok, Res} -> Res;
        _ -> call_any_success(Nodes, Msg)
    end;
call_any_success([], _Msg) ->
    {error, all_controller_down}.

%call_all(Msg) ->
%    call_all(controller_nodes(), Msg).

%call_all_success(Nodes, Msg) ->
%    lists:foldl(
%        fun(Node, Acc) ->
%            case call(Node, Msg) of
%                {node_ok, Res} -> [Res|Acc];
%                _ -> Acc
%            end
%        end, [], Nodes).

%cast(Node, Msg) ->
%    gen_server:cast({?SERVER, Node}, Msg).

cast(Msg) ->
    gen_server:cast(?SERVER, Msg).

init([]) ->
    {ok, #state{}}.

%% specific node

handle_call({all_users_on_node, Node}, _From, State) ->
    {reply, node_ok_res(rds_la_api:all_users_on_node(Node)), State};
handle_call({all_append_users_on_node, Node}, _From, State) ->
    {reply, node_ok_res(rds_la_api:all_append_users_on_node(Node)), State};
handle_call({all_query_users_on_node, Node}, _From, State) ->
    {reply, node_ok_res(rds_la_api:all_query_users_on_node(Node)), State};

handle_call({add_user_metastore, User, Props}, _From, State) ->
    {reply, node_ok_res(rds_la_api:add_user_metastore(User, Props)), State};
handle_call({del_user_metastore, User}, _From, State) ->
    {reply, node_ok_res(rds_la_api:del_user_metastore(User)), State};
handle_call({get_user_metastore, User}, _From, State) ->
    {reply, node_ok_res(rds_la_api:get_user_metastore(User)), State};
handle_call({set_user_metastore, User, Props}, _From, State) ->
    {reply, node_ok_res(rds_la_api:set_user_metastore(User, Props)), State};

handle_call({add_user_store, User, Node, Props}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:add_user_store(User, Node, Props)), State};
handle_call({del_user_store, User, Node}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:del_user_store(User, Node)), State};

handle_call({add_user_node, User, Node, Props}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:add_user_node(User, Node, Props)), State};
handle_call({get_user_nodes, User}, _From, State) ->
    {reply, node_ok_res(rds_la_api:get_user_nodes(User)), State};
handle_call({del_user_node, User, Node}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:del_user_node(User, Node)), State};
handle_call({add_user_nodes, User, Nodes, Props}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:add_user_nodes(User, Nodes, Props)), State};
handle_call({del_user_nodes, User, Nodes}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:del_user_nodes(User, Nodes)), State};

handle_call({add_user_append_node, User, Node, Props}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:add_user_append_node(User, Node, Props)), State};
handle_call({get_user_append_nodes, User}, _From, State) ->
    {reply, node_ok_res(rds_la_api:get_user_append_nodes(User)), State};
handle_call({del_user_append_node, User, Node}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:del_user_append_node(User, Node)), State};
handle_call({add_user_append_nodes, User, Nodes, Props}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:add_user_append_nodes(User, Nodes, Props)), State};
handle_call({del_user_append_nodes, User, Nodes}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:del_user_append_nodes(User, Nodes)), State};

handle_call({add_user_query_node, User, Node, Props}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:add_user_query_node(User, Node, Props)), State};
handle_call({get_user_query_nodes, User}, _From, State) ->
    {reply, node_ok_res(rds_la_api:get_user_query_nodes(User)), State};
handle_call({del_user_query_node, User, Node}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:del_user_query_node(User, Node)), State};
handle_call({add_user_query_nodes, User, Nodes, Props}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:add_user_query_nodes(User, Nodes, Props)), State};
handle_call({del_user_query_nodes, User, Nodes}, _From, State) ->
    {reply, node_ok_res(catch rds_la_api:del_user_query_nodes(User, Nodes)), State};

handle_call({append_user_node_log_sync, User, Node, ProxyId, Record}, _From, State) ->
    AppendNodes = get_user_append_nodes_internal(User),
    call_in_node_fun_result(
        fun() ->
            catch rds_la_api:append_user_log_sync(User, Node, ProxyId, Record)
        end, Node, AppendNodes, State);
handle_call({query_user_node_log, User, Node, Type, Condition}, _From, State) ->
    QueryNodes = get_user_query_nodes_internal(User),
    call_in_node_fun_result(
        fun() ->
            catch rds_la_api:query_user_log(User, Node, Type, Condition)
        end, Node, QueryNodes, State);
handle_call({query_user_node_dump_slow, User, Node, DateStart, DateEnd}, _From, State) ->
    QueryNodes = get_user_query_nodes_internal(User),
    call_in_node_fun_result(
        fun() ->
            catch rds_la_api:query_user_dump_slow(User, Node, DateStart, DateEnd)
        end, Node, QueryNodes, State);
handle_call({query_user_node_dump_slow, User, Node, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity},
            _From, State) ->
    QueryNodes = get_user_query_nodes_internal(User),
    call_in_node_fun_result(
        fun() ->
            catch rds_la_api:query_user_dump_slow(User, Node, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity)
        end, Node, QueryNodes, State);
handle_call({query_user_node_sql_stats, User, Node, DateStart, DateEnd}, _From, State) ->
    QueryNodes = get_user_query_nodes_internal(User),
    call_in_node_fun_result(
        fun() ->
            catch rds_la_api:query_user_sql_stats(User, Node, DateStart, DateEnd)
        end, Node, QueryNodes, State);
handle_call({query_user_node_sql_stats, User, Node, DateStart, DateEnd, PageStart, PageEnd}, _From, State) ->
    QueryNodes = get_user_query_nodes_internal(User),
    call_in_node_fun_result(
        fun() ->
            catch rds_la_api:query_user_sql_stats(User, Node, DateStart, DateEnd, PageStart, PageEnd)
        end, Node, QueryNodes, State);

%% distribute
handle_call({add_user, User, Props}, _From, State) ->
    UserNodes = rds_la_api:user_nodes(User),
    SuccessfulNodes = add_user_in_nodes(User, Props, UserNodes),
    {reply, node_ok_res({ok, SuccessfulNodes}), State};
handle_call({del_user, User}, _From, State) ->
    UserNodes = rds_la_api:get_user_nodes(User),
    AppendNodes = proplists:get_value(append_nodes, UserNodes),
    QueryNodes = proplists:get_value(query_nodes, UserNodes),
    del_user_in_nodes(User, lists:usort(AppendNodes ++ QueryNodes)),
    {reply, node_ok_res(ok), State};

handle_call({append_user_log_sync, User, ProxyId, Record}, _From, State) ->
    AppendNodes = get_user_append_nodes_internal(User),
    Ret = case AppendNodes of
        [] -> {error, no_nodes};
        _ ->
            RNum = random_in(length(AppendNodes)),
            Node = lists:nth(RNum, AppendNodes),
            catch rds_la_api:append_user_log_sync(User, Node, ProxyId, Record)
    end,
    {reply, node_ok_res(Ret), State};

handle_call({query_user_log, User, Type, Condition}, _From, State) ->
    query_all_node_fun_result(
        fun(Node) ->
            catch rds_la_api:query_user_log(User, Node, Type, Condition)
        end, User, Type, Condition, State);

handle_call({query_user_dump_slow, User, DateStart, DateEnd}, _From, State) ->
    query_all_node_fun_result(
        fun(Node) ->
            catch rds_la_api:query_user_dump_slow(User, Node, DateStart, DateEnd)
        end, User, dump_slow, makeup_generic_condition(DateStart, DateEnd), State);

handle_call({query_user_dump_slow, User, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity}, _From, State) ->
    query_all_node_fun_result(
        fun(Node) ->
            catch rds_la_api:query_user_dump_slow(User, Node, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity)
        end, User, dump_slow, makeup_generic_condition(DateStart, DateEnd) ++ 
                              makeup_dump_slow_condition(MinExecTime, MaxExecTime, Quantity), State);

handle_call({query_user_sql_stats, User, DateStart, DateEnd}, _From, State) ->
    query_all_node_fun_result(
        fun(Node) ->
            catch rds_la_api:query_user_sql_stats(User, Node, DateStart, DateEnd)
        end, User, sql_stats, makeup_generic_condition(DateStart, DateEnd), State);

handle_call({query_user_sql_stats, User, DateStart, DateEnd, PageStart, PageEnd}, _From, State) ->
    query_all_node_fun_result(
        fun(Node) ->
            catch rds_la_api:query_user_sql_stats(User, Node, DateStart, DateEnd, PageStart, PageEnd)
        end, User, sql_stats, makeup_generic_condition(DateStart, DateEnd) ++
                              makeup_sql_stats_condition(PageStart, PageEnd), State);

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

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

node_ok_res(Res) ->
    {node_ok, Res}.

add_user_in_nodes(User, Props, UserNodes) ->
    ConcurrentNodes = rds_la_api:prop_concurrent_nodes(Props),
    rds_la_api:add_user_metastore(User, Props),
    add_user_store_in_nodes(User, UserNodes, Props, ConcurrentNodes).

add_user_store_in_nodes(User, UserNodes, Props, ConcurrentNodes) ->
    add_user_store_in_nodes(User, UserNodes, Props, ConcurrentNodes, []).

add_user_store_in_nodes(_User, _UserNodes, _Props, 0, SuccessfulNodes) -> SuccessfulNodes;
add_user_store_in_nodes(_User, [], _Props, _ConcurrentNodes, SuccessfulNodes) -> SuccessfulNodes;
add_user_store_in_nodes(User, [Node|UserNodes], Props, ConcurrentNodes, SuccessfulNodes) ->
    case catch rds_la_api:add_user_node(User, Node, Props) of
        ok ->
            add_user_store_in_nodes(User, UserNodes, Props, ConcurrentNodes - 1, [Node|SuccessfulNodes]);
        _ ->
            add_user_store_in_nodes(User, UserNodes, Props, ConcurrentNodes, SuccessfulNodes)
    end.

del_user_in_nodes(User, UserNodes) ->
    del_user_store_in_nodes(User, UserNodes),
    rds_la_api:del_user_metastore(User).

del_user_store_in_nodes(_User, []) -> ok;
del_user_store_in_nodes(User, [Node|UserNodes]) ->
    catch rds_la_api:del_user_node(User, Node),
    del_user_store_in_nodes(User, UserNodes).

call_in_node_fun_result(Fun, Node, Nodes, State) ->
    Ret = case lists:member(Node, Nodes) of
        true -> Fun();
        false -> {error, wrong_node}
    end,
    {reply, node_ok_res(Ret), State}.

random_in(N) ->
    random:seed(now()),
    random:uniform(N).

query_all_node_fun_result(Fun, User, Type, Condition, State) ->
    QueryNodes = get_user_query_nodes_internal(User),
    Ret = case QueryNodes of
        [] -> {error, no_nodes};
        _ ->
            ResultList = lists:foldl(
                fun(Node, Acc) ->
                    case Fun(Node) of
                        {ok, R} -> [R|Acc];
                        _ -> Acc
                    end
                end, [], QueryNodes),
            {ok, rds_la_api:merge_query_results(Type, ResultList, Condition)}
    end,
    {reply, node_ok_res(Ret), State}.

makeup_generic_condition(DateStart, DateEnd) ->
    SecondsStart = rds_la_lib:universal_to_seconds(DateStart),
    SecondsEnd = rds_la_lib:universal_to_seconds(DateEnd),
    [{date_start, SecondsStart}, {date_end, SecondsEnd}].

makeup_dump_slow_condition(MinExecTime, MaxExecTime, Quantity) ->
    [{min_exectime, MinExecTime}, {max_exectime, MaxExecTime}, {quantity, Quantity}].

makeup_sql_stats_condition(PageStart, PageEnd) ->
    [{page_start, PageStart}, {page_end, PageEnd}].

get_user_append_nodes_internal(User) ->
    case rds_la_api:get_user_append_nodes(User) of
        [] -> [];
        [{_, AppendNodes}] -> AppendNodes
    end.

get_user_query_nodes_internal(User) ->
    case rds_la_api:get_user_query_nodes(User) of
        [] -> [];
        [{_, AppendNodes}] -> AppendNodes
    end.

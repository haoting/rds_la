%% Author: haoting.wq
%% Created: 2012-6-18
%% Description: TODO: Add description to rds_la_network_protocol
-module(rds_la_controller_protocol).

-behaviour(back_proxy).
%%
%% Include files
%%
-include("logger_header.hrl").
%%
%% Exported Functions
%%
-export([init/1, terminate/2]).
-export([handle_msg/3, handle_nowait/1, handle_timer/2, handle_local_info/2]).

-export([new_controller_client/1]).
-export([ping/2]).
-export([add_user/1, add_user/2, del_user/1, get_user_dstnps/1]).
-export([get_user_append_dstnps/1, get_user_query_dstnps/1]).

-export([update_user_nodes/2]).

-record(pstate, {controller_dstnp_list = [], last_state = default}).

-define(SERVER, rds_la_controller_client).
-define(USER_APPEND_DSTNP_CACHE, user_append_dstnp_cache).
-define(USER_QUERY_DSTNP_CACHE, user_query_dstnp_cache).

%%
%% API Functions
%%

new_controller_client(Prefix) ->
    Pid = case whereis(?SERVER) of
        undefined ->
            {ok, NPid} = back_proxy_api:new_send_proxy_by_prefix(Prefix),
            register(?SERVER, NPid),
            NPid;
        OPid -> OPid
    end,
    {ok, Pid}.

%% TODO: adjust all functions which called rds_la_api:* and rds_la_config:* at client point
default_user_props() -> rds_la_api:default_user_props().

%% for client
ping(Node, Port) ->
    back_proxy:local_call(?SERVER, {ping, {Node, Port}}).

add_user(User) ->
    add_user(User, default_user_props()).
add_user(User, Props) ->
    back_proxy:local_call(?SERVER, {add_user, User, Props}).

del_user(User) ->
    back_proxy:local_call(?SERVER, {del_user, User}).

get_user_dstnps(User) ->
    back_proxy:local_call(?SERVER, {get_user_nodes, User}).

get_user_append_dstnps(User) ->
    case get_user_append_dstnp_cache(User) of
        [] ->
            get_user_dstnps(User),
            case get_user_append_dstnp_cache(User) of
                [] -> [];
                [{_, AppendDstNPs}] -> AppendDstNPs
            end;
        [{_, AppendDstNPs}] -> AppendDstNPs
    end.

get_user_query_dstnps(User) ->
    case get_user_query_dstnp_cache(User) of
        [] ->
            get_user_dstnps(User),
            case get_user_query_dstnp_cache(User) of
                [] -> [];
                [{_, QueryDstNPs}] -> QueryDstNPs
            end;
        [{_, QueryDstNPs}] -> QueryDstNPs
    end.

%% for controller
update_user_nodes(Pid, User) ->
    back_proxy:local_cast(Pid, {update_user_nodes, User}).

%% ------------------------------------------------------------------
%% back_proxy Function Definitions
%% ------------------------------------------------------------------

%% Initialize
init([ControllerDstNPList]) ->
    NControllerDstNPList = case ControllerDstNPList of
        [] -> [];
        _ ->
            Nth = random_in(length(ControllerDstNPList)),
            {Tail, Head} = lists:split(Nth, ControllerDstNPList),
            Head ++ Tail
    end,
    init_user_append_dstnp_cache(),
    init_user_query_dstnp_cache(),
    {ok, #pstate{controller_dstnp_list = NControllerDstNPList}}.

%% Terminate
terminate(_Reason, _ModState) ->
    ok.

%% Remote Msg Come

handle_msg(From, Binary, ModState) ->
    ?DEBUG("remote receive msg: ~p~n", [Binary]),
    Term = binary_to_term(Binary),
    handle_remote(From, Term, ModState).

%% Internal State Transmit
handle_nowait(ModState) ->
    {wait_msg, ModState}.

%% Timer
handle_timer(_TimerMsg, ModState) ->
    {wait_msg, ModState}.

%% Local Call/Cast

%% last_send_failed
%% local_cast
handle_local_info({local_cast, Term}, ModState) ->
    ?DEBUG("local receive msg: ~p ~n", [Term]),
    handle_local(Term, ModState);

%% local_call
handle_local_info({local_call, Term}, ModState) ->
    ?DEBUG("local receive msg: ~p ~n", [Term]),
    handle_local(Term, ModState);

handle_local_info({last_send_failed, DstNP, _LastOperation, _FaildResult}, ModState) ->
    ?DEBUG("last send failed ~p~n", [DstNP]),
    {wait_msg, ModState};

%% connection_down
handle_local_info({connection_down, DstNP}, ModState) ->
    ?DEBUG("connection down ~p~n", [DstNP]),
    handle_connection_down(DstNP, ModState);

handle_local_info(_Msg, ModState) ->
    {wait_msg, ModState}.

%%
%% Local Functions
%%
random_in(N) ->
    random:seed(now()),
    random:uniform(N).

nodes_to_dstnps(Nodes) ->
    [{Node, rds_la_config:la_store_listen_port(Node)}|| Node <- Nodes].

user_nodes_to_dstnps(UserNodes) ->
    AppendNodes = proplists:get_value(append_nodes, UserNodes, []),
    QueryNodes = proplists:get_value(query_nodes, UserNodes, []),
    [{append_dstnps, nodes_to_dstnps(AppendNodes)},
     {query_dstnps, nodes_to_dstnps(QueryNodes)}].

%% Remote Receive

%% for controller
handle_remote(_From, ping, ModState) ->
    reply_remote_term(pong, ModState);

%% for controller
handle_remote(_From, {add_user, User, Props}, ModState) ->
    Res = rds_la_controller:add_user(User, Props),
    reply_remote_term(Res, ModState);

%% for controller
handle_remote(_From, {del_user, User}, ModState) ->
    Res = rds_la_controller:del_user(User),
    reply_remote_term(Res, ModState);

%% for client
handle_remote(_From, {reply, Reply}, ModState) ->
    reply_local(Reply, ModState);

%% for controller
handle_remote(_From, {get_user_nodes, User}, ModState) ->
    UserNodes = case rds_la_api:get_user_nodes(User) of
        {error, _Reason} -> [];
        UN -> UN
    end,
    UserDstNPs = user_nodes_to_dstnps(UserNodes),
    reply_remote_binary(term_to_binary({reply_user_nodes, User, UserDstNPs}), ModState);

%% for client
handle_remote(_From, {reply_user_nodes, User, UserNodes}, ModState) ->
    update_dstnp_cache(User, UserNodes),
    reply_local(UserNodes, ModState);

%% for client
handle_remote(_From, {update_user_nodes, User, UserNodes}, ModState) ->
    update_dstnp_cache(User, UserNodes),
    {wait_msg, ModState};

handle_remote(_From, Binary, ModState) ->
    ?DEBUG("remote receive msg: ~p~n", [Binary]),
    {wait_msg, ModState}.

reply_local(Term, ModState) ->
    {{reply, Term}, ModState#pstate{last_state = default}}.

reply_remote_term(Term, ModState) ->
    Binary = term_to_binary({reply, Term}),
    {{reply, Binary}, ModState}.

reply_remote_binary(Binary, ModState) ->
    {{reply, Binary}, ModState}.

%% Local Receive

%% for client
handle_local({ping, DstNP}, ModState) ->
    handle_local_sync(DstNP, ping, ModState);

%% for client
handle_local({add_user, User, Props}, ModState) ->
    handle_local_controller_sync({add_user, User, Props}, ModState);

%% for client
handle_local({del_user, User}, ModState) ->
    handle_local_controller_sync({del_user, User}, ModState);

%% for client
handle_local({get_user_nodes, User}, ModState) ->
    handle_local_controller_sync({get_user_nodes, User}, ModState);

%% for controller
handle_local({update_user_nodes, User}, ModState) ->
    UserNodes = case rds_la_api:get_user_nodes(User) of
        {error, _Reason} -> [];
        UN -> UN
    end,
    UserDstNPs = user_nodes_to_dstnps(UserNodes),
    reply_remote_binary(term_to_binary({update_user_nodes, User, UserDstNPs}), ModState);

handle_local(_Term, ModState) ->
    {wait_msg, ModState}.

handle_local_sync(DstNP, Term, ModState) ->
    Binary = term_to_binary(Term),
    case back_proxy:sync_send_to(DstNP, Binary) of
        {error, Reason} -> {{reply, {error, Reason}}, ModState};
        ok -> {wait_msg, ModState#pstate{last_state = {single_call, DstNP}}}
    end.

handle_local_controller_sync(Term, ModState = #pstate{controller_dstnp_list = ControllerDstNPList}) ->
    Binary = term_to_binary(Term),
    case handle_local_controller(Binary, ControllerDstNPList) of
        {error, Reason} -> {{reply, {error, Reason}}, ModState};
        ok ->
            {wait_msg, ModState#pstate{last_state = {multi_call, Term}}};
        {ok, NControllerDstNP} ->
            {wait_msg, ModState#pstate{controller_dstnp_list = NControllerDstNP,
                                       last_state = {multi_call, Term}}}
    end.

handle_local_controller(Binary, [DstNP|ControllerDstNPList]) ->
    case back_proxy:sync_send_to(DstNP, Binary) of
        {error, _Reason} -> handle_local_controller(Binary, ControllerDstNPList, [DstNP]);
        ok -> ok
    end.

handle_local_controller(_Binary, [], _BadAcc) ->
    {error, no_controller};
handle_local_controller(Binary, [DstNP|ControllerDstNPList], BadAcc) ->
    case back_proxy:sync_send_to(DstNP, Binary) of
        {error, _Reason} ->
            handle_local_controller(Binary, ControllerDstNPList, [DstNP|BadAcc]);
        ok -> {ok, [DstNP|ControllerDstNPList] ++ BadAcc}
    end.

handle_connection_down(DstNP, ModState= #pstate{last_state = LastState}) ->
    case LastState of
        {single_call, DstNP} -> reply_local({error, connection_down}, ModState);
        {multi_call, Term} -> handle_local_controller_sync(Term, ModState);
        _ -> {wait_msg, ModState}
    end.

%% Cache Operation

%% append nodes cache

update_dstnp_cache(User, UserNodes) ->
    AppendDstNPs = proplists:get_value(append_dstnps, UserNodes, []),
    QueryDstNPs = proplists:get_value(query_dstnps, UserNodes, []),
    add_user_append_dstnp_cache(User, AppendDstNPs),
    add_user_query_dstnp_cache(User, QueryDstNPs).

init_user_append_dstnp_cache() ->
	case ets:info(?USER_APPEND_DSTNP_CACHE) of
		undefined -> ets:new(?USER_APPEND_DSTNP_CACHE, [named_table, public, {read_concurrency, true}]);
		_ -> ?USER_APPEND_DSTNP_CACHE
	end.

add_user_append_dstnp_cache(User, Nodes) ->
	ets:insert(?USER_APPEND_DSTNP_CACHE, {User, Nodes}).

%del_user_append_dstnp_cache(User) ->
%	ets:delete(?USER_APPEND_DSTNP_CACHE, User).

get_user_append_dstnp_cache(User) ->
	ets:lookup(?USER_APPEND_DSTNP_CACHE, User).

%% query nodes cache

init_user_query_dstnp_cache() ->
	case ets:info(?USER_QUERY_DSTNP_CACHE) of
		undefined -> ets:new(?USER_QUERY_DSTNP_CACHE, [named_table, public, {read_concurrency, true}]);
		_ -> ?USER_QUERY_DSTNP_CACHE
	end.

add_user_query_dstnp_cache(User, Nodes) ->
	ets:insert(?USER_QUERY_DSTNP_CACHE, {User, Nodes}).

%del_user_query_dstnp_cache(User) ->
%	ets:delete(?USER_QUERY_DSTNP_CACHE, User).

get_user_query_dstnp_cache(User) ->
	ets:lookup(?USER_QUERY_DSTNP_CACHE, User).

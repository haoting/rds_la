%% Author: haoting.wq
%% Created: 2012-6-14
%% Description: TODO: Add description to rds_la_controller
-module(rds_la_api).

%%
%% Include files
%%
-include("logger_header.hrl").

%%
%% Exported Functions
%%

-export([all_users_on_node/0, all_users_on_node/1]).
-export([all_append_users_on_node/0, all_append_users_on_node/1]).
-export([all_query_users_on_node/0, all_query_users_on_node/1]).

-export([add_user_metastore/1, add_user_metastore/2, del_user_metastore/1, get_user_meta/1, set_user_meta/2]).
-export([add_user_meta_nodes/2, del_user_meta_nodes/2, get_user_meta_nodes/1]).
-export([add_user_meta_append_nodes/2, del_user_meta_append_nodes/2, get_user_meta_append_nodes/1]).
-export([add_user_meta_query_nodes/2, del_user_meta_query_nodes/2, get_user_meta_query_nodes/1]).

-export([add_user_store/2, add_user_store/3, del_user_store/2]).
-export([add_user_node/2, add_user_node/3, get_user_nodes/1, del_user_node/2]).
-export([add_user_nodes/2, add_user_nodes/3, del_user_nodes/2]).
-export([add_user_append_node/2, add_user_append_node/3, get_user_append_nodes/1, del_user_append_node/2]).
-export([add_user_append_nodes/2, add_user_append_nodes/3, del_user_append_nodes/2]).
-export([add_user_query_node/2, add_user_query_node/3, get_user_query_nodes/1, del_user_query_node/2]).
-export([add_user_query_nodes/2, add_user_query_nodes/3, del_user_query_nodes/2]).

-export([append_user_log_async/3, append_user_log_async/4, append_user_log_sync/3, append_user_log_sync/4]).
-export([query_user_log/4]).

-export([query_user_dump_slow/4, query_user_dump_slow/7]).
-export([query_user_sql_stats/4, query_user_sql_stats/6]).
-export([merge_query_results/3]).

-export([default_user_props/0]).
-export([prop_concurrent_nodes/1]).

-export([user_nodes/1, user_nodes/2, user_nodes/3]).

%%
%% API Functions
%%

%%-----------------------------------------------------

all_users_on_node() ->
    all_users_on_node(node()).

all_users_on_node(Node) ->
    lists:usort(rds_la_metastore:all_user_on_node_for_append(Node) ++
                rds_la_metastore:all_user_on_node_for_query(Node)).

all_append_users_on_node() ->
    all_append_users_on_node(node()).

all_append_users_on_node(Node) ->
    rds_la_metastore:all_user_on_node_for_append(Node).

all_query_users_on_node() ->
    all_query_users_on_node(node()).

all_query_users_on_node(Node) ->
    rds_la_metastore:all_user_on_node_for_query(Node).

%%-----------------------------------------------------

add_user_metastore(User) ->
    add_user_metastore(User, default_user_props()).

add_user_metastore(User, Props) ->
    ensure_user_exist(User, Props),
    rds_la_metastore:add_node_record(User),
    ok.

del_user_metastore(User) ->
    rds_la_metastore:del_node_record(User),
    ensure_user_not_exist(User),
    ok.

get_user_meta(User) ->
    rds_la_metastore:get_user(User).

set_user_meta(User, Props) ->
    rds_la_metastore:set_user(User, Props).

add_user_meta_nodes(User, Nodes) ->
    rds_la_metastore:add_nodes(User, Nodes).

del_user_meta_nodes(User, Nodes) ->
    rds_la_metastore:del_nodes(User, Nodes).

get_user_meta_nodes(User) ->
    rds_la_metastore:get_nodes(User).

add_user_meta_append_nodes(User, Nodes) ->
    rds_la_metastore:add_append_nodes(User, Nodes).

del_user_meta_append_nodes(User, Nodes) ->
    rds_la_metastore:del_append_nodes(User, Nodes).

get_user_meta_append_nodes(User) ->
    rds_la_metastore:get_append_nodes(User).

add_user_meta_query_nodes(User, Nodes) ->
    rds_la_metastore:add_query_nodes(User, Nodes).

del_user_meta_query_nodes(User, Nodes) ->
    rds_la_metastore:del_query_nodes(User, Nodes).

get_user_meta_query_nodes(User) ->
    rds_la_metastore:get_query_nodes(User).
%%-----------------------------------------------------

add_user_store(User, Node) ->
    rds_la_store:add_user_node(User, Node, default_user_props()).

add_user_store(User, Node, Props) ->
    rds_la_store:add_user_node(User, Node, Props).

del_user_store(User, Node) ->
    rds_la_store:del_user_node(User, Node).

%%-----------------------------------------------------

add_user_node(User, Node) ->
    add_user_node(User, Node, default_user_props()).

add_user_node(User, Node, Props) ->
    ensure_user_exist(User, Props),
    case catch add_user_store(User, Node, Props) of
        ok -> add_user_meta_nodes(User, [Node]);
        {error, Reason} -> {error, Reason};
        _ -> {error, node_down}
    end.

get_user_nodes(User) ->
    get_user_meta_nodes(User).

del_user_node(User, Node) ->
    case rds_la_metastore:get_user(User) of
        [] -> ok;
        _ ->
            del_user_meta_nodes(User, [Node]),
            del_user_store(User, Node)
    end.

add_user_nodes(User, Nodes) -> 
    add_user_nodes(User, Nodes, default_user_props()).

add_user_nodes(User, Nodes, Props) when is_list(Nodes) ->
    Res = [add_user_node(User, Node, Props)|| Node <- Nodes],
    parse_result(add_user_nodes, Res).

del_user_nodes(User, Nodes) when is_list(Nodes) ->
    Res = [del_user_node(User, Node)|| Node <- Nodes],
    parse_result(del_user_nodes, Res).

%%-----------------------------------------------------

add_user_append_node(User, Node) ->
    add_user_append_node(User, Node, default_user_props()).

add_user_append_node(User, Node, Props) ->
    ensure_user_exist(User, Props),
    add_user_meta_append_nodes(User, [Node]),
    add_user_store(User, Node, Props).

get_user_append_nodes(User) ->
    get_user_meta_append_nodes(User).

del_user_append_node(User, Node) ->
    case rds_la_metastore:get_user(User) of
        [] -> ok;
        _ -> del_user_meta_append_nodes(User, [Node])
    end.
    %% there is no need to delete store for append node

add_user_append_nodes(User, Nodes) -> 
    add_user_append_nodes(User, Nodes, default_user_props()).

add_user_append_nodes(User, Nodes, Props) when is_list(Nodes) ->
    Res = [add_user_append_node(User, Node, Props)|| Node <- Nodes],
    parse_result(add_user_append_nodes, Res).

del_user_append_nodes(User, Nodes) when is_list(Nodes) ->
    Res = [del_user_append_node(User, Node)|| Node <- Nodes],
    parse_result(del_user_append_nodes, Res).

%%-----------------------------------------------------

add_user_query_node(User, Node) ->
    add_user_query_node(User, Node, default_user_props()).

add_user_query_node(User, Node, Props) ->
    ensure_user_exist(User, Props),
    add_user_meta_query_nodes(User, [Node]),
    add_user_store(User, Node, Props).

get_user_query_nodes(User) ->
    get_user_meta_query_nodes(User).

del_user_query_node(User, Node) ->
    case rds_la_metastore:get_user(User) of
        [] -> ok;
        _ ->
            del_user_meta_query_nodes(User, [Node]),
            del_user_store(User, Node)
    end.

add_user_query_nodes(User, Nodes) -> 
    add_user_query_nodes(User, Nodes, default_user_props()).

add_user_query_nodes(User, Nodes, Props) when is_list(Nodes) ->
    Res = [add_user_query_node(User, Node, Props)|| Node <- Nodes],
    parse_result(add_user_query_nodes, Res).

del_user_query_nodes(User, Nodes) when is_list(Nodes) ->
    Res = [del_user_query_node(User, Node)|| Node <- Nodes],
    parse_result(del_user_query_nodes, Res).

%%-----------------------------------------------------

append_user_log_async(User, ProxyId, Record) ->
    rds_la_store:append_log_async(User, ProxyId, Record).

append_user_log_async(User, Node, ProxyId, Record) ->
    rds_la_store:append_log_async(User, Node, ProxyId, Record).

append_user_log_sync(User, ProxyId, Record) ->
    rds_la_store:append_log_sync(User, node(), ProxyId, Record).

append_user_log_sync(User, Node, ProxyId, Record) ->
    rds_la_store:append_log_sync(User, Node, ProxyId, Record).

query_user_log(User, Node, Type, Condition) ->
    case Node =:= node() of
        true -> rds_la_store:query_log(User, Type, Condition);
        false -> rds_la_store:query_log(User, Node, Type, Condition)
    end.

%% DateStart & DateEnd are date format in erlang like that:
%% {{Year, Month, Date}, {Hour, Minute, Second}}

query_user_dump_slow(User, Node, DateStart, DateEnd) ->
    query_user_dump_slow(User, Node, DateStart, DateEnd, undefined, undefined, undefined).

query_user_dump_slow(User, Node, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity) ->
    SecondsStart = rds_la_lib:universal_to_seconds(DateStart),
    SecondsEnd = rds_la_lib:universal_to_seconds(DateEnd),
    Main = [{date_start, SecondsStart}, {date_end, SecondsEnd}],
    Extra = filter_condition(min_exectime, MinExecTime) ++
            filter_condition(max_exectime, MaxExecTime) ++
            filter_condition(quantity, Quantity),
    query_user_log(User, Node, dump_slow, Main ++ Extra).

query_user_sql_stats(User, Node, DateStart, DateEnd) ->
    query_user_sql_stats(User, Node, DateStart, DateEnd, undefined, undefined).

query_user_sql_stats(User, Node, DateStart, DateEnd, PageStart, PageEnd) ->
    SecondsStart = rds_la_lib:universal_to_seconds(DateStart),
    SecondsEnd = rds_la_lib:universal_to_seconds(DateEnd),
    Main = [{date_start, SecondsStart}, {date_end, SecondsEnd}],
    Extra = filter_condition(page_start, PageStart) ++
            filter_condition(page_end, PageEnd),
    query_user_log(User, Node, sql_stats, Main ++ Extra).

merge_query_results(Type, ResultList, Condition) ->
    rds_la_store:merge_query_results(Type, ResultList, Condition).

%%-----------------------------------------------------

user_nodes(User) ->
    user_nodes(User, rds_la_config:service_nodes(rds_la_store)).

user_nodes(User, AllNodes) ->
    user_nodes(User, AllNodes, fun user_name_hash/1).

user_nodes(User, AllNodes, HashFun) ->
    UserHash = HashFun(User),
    Nth = UserHash rem length(AllNodes),
    {Tail, Head} = lists:split(Nth, AllNodes),
    Head ++ Tail.

user_name_hash(User) ->
    chash(User).

chash(ObjectName) ->
    <<IndexAsInt:160/integer>> = crypto:sha(term_to_binary(ObjectName)),
    IndexAsInt.

%%-----------------------------------------------------

-define(DEFAULT_CONCURRENT_NODES, 3).

default_user_props() ->
    [
        {concurrent_nodes, ?DEFAULT_CONCURRENT_NODES}
    ] ++
    rds_la_store:default_user_props().

prop_concurrent_nodes(Props) ->
    proplists:get_value(concurrent_nodes, Props, ?DEFAULT_CONCURRENT_NODES).

%%-----------------------------------------------------

%%
%% Local Functions
%%

%ensure_user_exist(User) ->
%    ensure_user_exist(User, default_user_props()).

ensure_user_exist(User, Props) ->
    rds_la_metastore:add_user(User, Props).

ensure_user_not_exist(User) ->
    rds_la_metastore:del_user(User).

parse_result(Context, Res) ->
    {NOk, NError} = lists:foldl(
        fun(ok, {Ok, Error}) -> {Ok + 1, Error};
           ({error, _Reason}, {Ok, Error}) -> {Ok, Error + 1}
        end, {0, 0}, Res),
    Ret = {Context, NOk, NError},
    ?DEBUG("~p successful: ~p, failed: ~p", [Ret]),
    Ret.

filter_condition(_Key, undefined) -> [];
filter_condition(Key, Value) -> [{Key, Value}].

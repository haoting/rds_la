%% Author: haoting.wq
%% Created: 2012-5-28
%% Description: TODO: Add description to rds_la_metastore
-module(rds_la_metastore).

-behaviour(metastore_mnesia).

%%
%% Include files
%%
-include("logger_header.hrl").
-include("metastore_header.hrl").
%%
%% Exported Functions
%%
-export([is_running/1, is_inited/0, current_versions/0, table_definitions/0, table_upgrade/0]).
-export([initialize/0, deinitialize/0]).

-export([all_user/0, all_user_on_node_for_query/1, all_user_on_node_for_append/1]).
-export([add_user/2, set_user/2, del_user/1, get_user/1]).
-export([set_user_prop/3, del_user_prop/2, get_user_prop/2]).
-export([set_config/2, del_config/1, get_config/1]).
-export([add_node_record/1, get_node_record/1, del_node_record/1]).
-export([add_nodes/2, get_nodes/1, del_nodes/2]).
-export([add_append_nodes/2, get_append_nodes/1, del_append_nodes/2]).
-export([add_query_nodes/2, get_query_nodes/1, del_query_nodes/2]).

-export([subscribe_route_event/0, unsubscribe_route_event/0]).
-export([route_record_to_user/1, route_key_to_user/1]).

-define(INITIALIZED_FILE, "initialized_file").

-record(config, {k, v}).
-record(user, {name, props = []}).
-record(route, {name, append_nodes = [], query_nodes = []}).

%%
%% API Functions
%%
is_running(Node) ->
    case rpc:call(Node, application, which_applications, [infinity]) of
        {badrpc, _} ->
            false;
        _        ->
            %proplists:is_defined(rabbit, Apps),
            true
    end.

is_inited() ->
    Initialize = initialized_file_exist(),
    ?DEBUG("Initialize: ~p~n", [Initialize]),
    Initialize.

current_versions() ->
    ['V1'].
    %['V1', 'V2'].

current_version() ->
    case current_versions() of
        [V] -> V;
        [_|_] = VL -> [V] = tl(VL), V
    end.

table_definitions() ->
    table_definitions(current_version()).

table_definitions('V1') ->
    [
        config_table_define(),
        user_table_define(),
        route_table_define()
    ];
table_definitions('V2') ->
    [
        config_table_define(),
        user_table_define(),
        route_table_define()
    ].

table_upgrade() ->
    table_upgrade(current_version()).

table_upgrade('V1') ->
    [
    ];
table_upgrade('V2') ->
    [
    ].

initialized_file() ->
    {ok, PWD} = file:get_cwd(),
    InitializeFile = filename:join(PWD, ?INITIALIZED_FILE),
    ?DEBUG("Initialize File: ~p~n", [InitializeFile]),
    InitializeFile.

initialized_file_exist() ->
    case file:read_file_info(initialized_file()) of
        {error, _Reason} -> false;
        {ok, _FileInfo} -> true
    end.

initialize() ->
    file_util:write_file_term(initialized_file(), initialized).

deinitialize() ->
    file:delete(initialized_file()).

set_config(K, V) ->
    mnesia:dirty_write(#config{k = K, v = V}).

get_config(K) ->
    case catch mnesia:dirty_read({config, K}) of
        [#config{v = V}] -> V;
        [] -> {error, no_config};
        {'EXIT', {aborted, _}} -> {error, no_config};
        _ -> {error, config_duplicated}
    end.

del_config(K) ->
    mnesia:dirty_delete({config, K}).

add_user(User, Props) ->
    Fun = fun([]) ->
                 {ok, #user{name = User, props = Props}};
             ([_UR]) ->
                 {ignore, []}
    end,
    get_map_set_table(user, User, Fun).

del_user(User) ->
    del_table(user, User).

set_user(User, Props) ->
    set_table(user, #user{name = User, props = Props}).

get_user(User) ->
    get_table(user, User).

set_user_prop(User, Key, Value) ->
    Fun = fun([]) ->
                 {ok, #user{name = User, props = set_prop([], Key, Value)}};
             ([UR = #user{props = OProps}]) ->
                 {ok, UR#user{props = set_prop(OProps, Key, Value)}}
    end,
    get_map_set_table(user, User, Fun).

get_user_prop(User, Key) ->
    Fun = fun([#user{props = Props}]) ->
                 case get_prop(Props, Key) of
                     undefined -> [];
                     V -> V
                 end
    end,
    get_table(user, User, Fun).

del_user_prop(User, Key) ->
    Fun = fun([]) ->
                 {ignore, []};
             ([UR = #user{props = OProps}]) ->
                 {ok, UR#user{props = del_prop(OProps, Key)}}
    end,
    get_map_set_table(user, User, Fun).

add_node_record(User) ->
    Fun = fun([]) ->
                 {ok, #route{name = User}};
             ([_UR]) ->
                 {ignore, []}
    end,
    get_map_set_table(route, User, Fun).

get_node_record(User) ->
    Fun = fun([RR]) ->
                 [RR]
    end,
    get_table(route, User, Fun).

del_node_record(User) ->
    del_table(route, User).

add_nodes(User, Nodes) when is_list(Nodes) ->
    Fun = fun([]) ->
                 {ok, #route{name = User, append_nodes = Nodes, query_nodes = Nodes}};
             ([UR = #route{append_nodes = AppendNodes, query_nodes = QueryNodes}]) ->
                 {ok, UR#route{append_nodes = (AppendNodes -- Nodes) ++ Nodes,
                               query_nodes = (QueryNodes -- Nodes) ++ Nodes}}
    end,
    get_map_set_table(route, User, Fun).

get_nodes(User) ->
    Fun = fun([#route{append_nodes = AppendNodes, query_nodes = QueryNodes}]) ->
                 [{append_nodes, AppendNodes}, {query_nodes, QueryNodes}]
    end,
    get_table(route, User, Fun).

del_nodes(User, Nodes) when is_list(Nodes) ->
    Fun = fun([]) ->
                 {ignore, []};
             ([UR = #route{append_nodes = AppendNodes, query_nodes = QueryNodes}]) ->
                 {ok, UR#route{append_nodes = AppendNodes -- Nodes, query_nodes = QueryNodes -- Nodes}}
    end,
    get_map_set_table(route, User, Fun).

add_append_nodes(User, Nodes) when is_list(Nodes) ->
    Fun = fun([]) ->
                 {ok, #route{name = User, append_nodes = sNodes}};
             ([UR = #route{append_nodes = AppendNodes}]) ->
                 {ok, UR#route{append_nodes = (AppendNodes -- Nodes) ++ Nodes}}
    end,
    get_map_set_table(route, User, Fun).

get_append_nodes(User) ->
    Fun = fun([#route{append_nodes = AppendNodes}]) ->
                 [{append_nodes, AppendNodes}]
    end,
    get_table(route, User, Fun).

del_append_nodes(User, Nodes) when is_list(Nodes) ->
    Fun = fun([]) ->
                 {ignore, []};
             ([UR = #route{append_nodes = AppendNodes}]) ->
                 {ok, UR#route{append_nodes = AppendNodes -- Nodes}}
    end,
    get_map_set_table(route, User, Fun).

add_query_nodes(User, Nodes) when is_list(Nodes) ->
    Fun = fun([]) ->
                 {ok, #route{name = User, query_nodes = Nodes}};
             ([UR = #route{query_nodes = QueryNodes}]) ->
                 {ok, UR#route{query_nodes = (QueryNodes -- Nodes) ++ Nodes}}
    end,
    get_map_set_table(route, User, Fun).

get_query_nodes(User) ->
    Fun = fun([#route{query_nodes = QueryNodes}]) ->
                 [{query_nodes, QueryNodes}]
    end,
    get_table(route, User, Fun).

del_query_nodes(User, Nodes) when is_list(Nodes) ->
    Fun = fun([]) ->
                 {ignore, []};
             ([UR = #route{query_nodes = QueryNodes}]) ->
                 {ok, UR#route{query_nodes = QueryNodes -- Nodes}}
    end,
    get_map_set_table(route, User, Fun).

all_user() ->
    case mnesia:dirty_match_object(#route{name = '$1', _ = '_'}) of
        UserList when is_list(UserList) ->
            [Name || #route{name = Name} <- UserList];
        _ -> []
    end.

all_user_on_node_for_query(Node) ->
    case mnesia:dirty_match_object(#route{name = '$1', _ = '_'}) of
        UserList when is_list(UserList) ->
            [Name || #route{name = Name, query_nodes = QueryNodes} <- UserList,
                     lists:member(Node, QueryNodes)];
        _ -> []
    end.

all_user_on_node_for_append(Node) ->
    case mnesia:dirty_match_object(#route{name = '$1', _ = '_'}) of
        UserList when is_list(UserList) ->
            [Name || #route{name = Name, append_nodes = AppendNodes} <- UserList,
                     lists:member(Node, AppendNodes)];
        _ -> []
    end.

-define(ROUTE_TABLE_EVENT, {table, route, simple}).

subscribe_route_event() ->
    mnesia:subscribe(?ROUTE_TABLE_EVENT).

unsubscribe_route_event() ->
    mnesia:unsubscribe(?ROUTE_TABLE_EVENT).

route_record_to_user(#route{name = User}) ->
    User.

route_key_to_user(User) ->
    User.
%%
%% Local Functions
%%

set_table(_Table, Record) ->
    case mnesia:transaction(
          fun()->
              mnesia:write(Record)
          end)of
        {atomic, _} -> ok;
        _ -> error
    end.

get_table(Table, Key) ->
    case mnesia:transaction(
          fun()->
              mnesia:read({Table, Key})
          end)of
        {atomic, Return} -> Return;
        _ -> []
    end.

get_table(Table, Key, Fun) ->
    case mnesia:transaction(
          fun()->
              case mnesia:read({Table, Key}) of
                  [] -> [];
                  V -> Fun(V)
              end
          end)of
        {atomic, Return} -> Return;
        {aborted, Reason} -> {error, Reason}
    end.

del_table(Table, Key) ->
    case mnesia:transaction(
          fun()->
              mnesia:delete({Table, Key})
          end)of
        {atomic, _} -> ok;
        _ -> error
    end.

get_map_set_table(Table, Key, Fun) ->
    case mnesia:transaction(
          fun()->
              case Fun(mnesia:read({Table, Key})) of
                  {ok, NV} -> mnesia:write(NV);
                  {ignore, _} -> ok;
                  {{error, Reason}, _} -> {error, Reason}
              end
          end)of
        {atomic, Return} -> Return;
        {aborted, Reason} -> {error, Reason}
    end.

set_prop(PropList, Key, Value) ->
    proplists:delete(Key, PropList) ++ [{Key, Value}].

get_prop(PropList, Key) ->
    proplists:get_value(Key, PropList).

del_prop(PropList, Key) ->
    proplists:delete(Key, PropList).

%% V1
config_table_define() ->
    #metastore_table{
        name = config,
        definition = [
            {record_name, config},
            {attributes, record_info(fields, config)},
            {disc_copies, [node()]}
        ],
        auxiliary = [
            {match, #config{k = '_', v = '_'}}
        ]
    }.

user_table_define() ->
    #metastore_table{
        name = user,
        definition = [
            {record_name, user},
            {attributes, record_info(fields, user)},
            {disc_copies, [node()]}
        ],
        auxiliary = [
            {match, #user{name = '_', _ = '_'}}
        ]
    }.

route_table_define() ->
    #metastore_table{
        name = route,
        definition = [
            {record_name, route},
            {attributes, record_info(fields, route)},
            {disc_copies, [node()]}
        ],
        auxiliary = [
            {match, #route{name = '_', _ = '_'}}
        ]
    }.

%% only used for test
%-export([upgrade_route/0]).
%route_upgrade() ->
%    #metastore_version{
%        version = 'V2',
%        mfa = {?MODULE, upgrade_route, []}
%    }.
%upgrade_route() ->
%    metastore_mnesia:create_table(route_table_define()),
%    ok.
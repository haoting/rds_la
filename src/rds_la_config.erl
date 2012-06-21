%% Author: haoting.wq
%% Created: 2012-6-11
%% Description: TODO: Add description to rds_la_config
-module(rds_la_config).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-compile(export_all).
-export([get_service_env/3, get_node_service_env/5, get_all_env/3]).
-export([get_service_config/3, set_service_config/3, del_service_config/2]).
-export([get_node_service_config/5, set_node_service_config/5, del_node_service_config/4]).

-export([get_cluster_nodes/0, set_cluster_nodes/1]).
-export([have_service/1, service_nodes/1, service_nodes_env/1]).
-export([proxy_id_list/0]).
-export([la_store_dir/0, la_controller_dir/0, la_indexers/0, la_log_max_kept_days/0, la_dump_slow_topn/0]).
-export([la_controller_listen_port/1, la_store_listen_port/1]).

-define(DEFAULT_CLUSTER_NODES, []).
-define(DEFAULT_LA_STORE_DIR, "/tmp/rds/ladir").
-define(DEFAULT_LA_CONTROLLER_DIR, "/tmp/rds/lacdir").
-define(DEFAULT_LA_INDEXERS, []).
-define(DEFAULT_LA_LOG_MAX_KEPT_DAYS, 30).
-define(DEFAULT_LA_DUMP_SLOW_TOPN, 100).
-define(DEFAULT_LA_CONTROLLER_LISTEN_PORT, 9016).
-define(DEFAULT_LA_STORE_LISTEN_PORT, 9018).
-define(DEFAULT_PROXY_ID_LIST, []).

%%
%% API Functions
%%

%% Sample Config File:
%% [
%%     {rds_la,
%%         [
%%             {cluster_nodes, []},
%%             {proxy_id_list, [{"0", 'rds_proxy@127.0.0.1'}]},
%%             {rds_la_indexers, [rds_la_indexer_sql_dumpslow, rds_la_indexer_sql_stats]},
%%             {rds_la_dump_slow_topn, 100},
%%             {rds_la_log_max_kept_days, 30},
%%             {rds_la_controller,
%%                 [
%%                     [
%%                         {node, 'rds_la_append@127.0.0.1'},
%%                         {port, 9016}
%%                     ]
%%                 ]
%%             },
%%             {rds_la_client,
%%                 [
%%                     [{node, 'rds_la_client@127.0.0.1'}]
%%                 ]
%%             },
%%             {rds_la_store,
%%                 [
%%                     [
%%                         {node, 'rds_la_query@127.0.0.1'},
%%                         {dir, "/tmp/rds/ladir"},
%%                         {port, 9018}
%%                     ]
%%                 ]
%%             }
%%         ]
%%     }
%% ]
get_cluster_nodes()->
    get_service_config(rds_la, cluster_nodes, ?DEFAULT_CLUSTER_NODES).
set_cluster_nodes(Nodes)->
    set_service_config(rds_la, cluster_nodes, Nodes).

have_service(Service) ->
    Node = node(),
    Node == get_node_service_config(rds_la, Service, Node, node, no_node).

service_nodes(Service) ->
    case get_service_config(rds_la, Service, no_config) of
        no_config -> [];
        ServiceConfig -> get_args(node, ServiceConfig)
    end.

service_nodes_env(Service) ->
    case get_service_env(rds_la, Service, no_config) of
        no_config -> [];
        ServiceConfig -> get_args(node, ServiceConfig)
    end.

la_store_dir()->
	get_node_service_config(rds_la, rds_la_store, node(), dir, ?DEFAULT_LA_STORE_DIR).

la_controller_dir() ->
	get_node_service_config(rds_la, rds_la_controller, node(), dir, ?DEFAULT_LA_CONTROLLER_DIR).

la_indexers() ->
    get_service_config(rds_la, rds_la_indexers, ?DEFAULT_LA_INDEXERS).

la_log_max_kept_days() ->
    get_service_config(rds_la, rds_la_log_max_kept_days, ?DEFAULT_LA_LOG_MAX_KEPT_DAYS).

la_dump_slow_topn() ->
    get_service_config(rds_la, rds_la_dump_slow_topn, ?DEFAULT_LA_DUMP_SLOW_TOPN).

la_controller_listen_port(Node) ->
    get_node_service_config(rds_la, rds_la_controller, Node, port, ?DEFAULT_LA_CONTROLLER_LISTEN_PORT).

la_store_listen_port(Node) ->
    get_node_service_config(rds_la, rds_la_store, Node, port, ?DEFAULT_LA_STORE_LISTEN_PORT).

la_controller_dstnps() ->
    ControllerNodes = service_nodes_env(rds_la_controller),
    [{Node, get_node_service_env(rds_la, rds_la_controller, Node, port, ?DEFAULT_LA_CONTROLLER_LISTEN_PORT)}
     || Node <- ControllerNodes].

proxy_id_list() ->
    [ID || {ID, _Node} <- get_service_config(rds_la, proxy_id_list, ?DEFAULT_PROXY_ID_LIST)].
%%
%% Local Functions
%%

%% for static global env
get_service_env(App, Service, Default) ->
    case application:get_env(App, Service) of
        {ok, []} -> Default;
        {ok, Value} -> Value;
        _ -> Default
    end.

%% for static node env
get_node_service_env(App, Service, Node, Key, Default) ->
	case application:get_env(App, Service) of
		{ok, NodeCfgs} ->
            case split_node_arg(Key, Node, NodeCfgs) of
                {error, Reason} -> {error, Reason};
                {_, []} -> Default;
                {_, [NodeCfg]} ->
				    case proplists:get_value(Key, NodeCfg) of
				        undefined -> Default;
				    	Value -> Value
				    end
            end;
		_ -> Default
	end.

get_all_env(App, Service, Key) ->
    lists:foldl(
      fun(Node, Acc) ->
              case get_node_service_env(App, Service, Node, Key, undefined) of
                  undefined -> Acc;
                  {error, _} -> Acc;
                  Id -> [{Node, Id}|Acc]
              end
      end,
      [],
      get_cluster_nodes()).

%% for dynamic global config
get_service_config(App, Service, Default) ->
    case get_metastore_config({App, Service}) of
        {error, no_config} -> get_service_env(App, Service, Default);
        Config -> Config
    end.
set_service_config(App, Service, Value) ->
    set_metastore_config({App, Service}, Value).
del_service_config(App, Service) ->
    del_metastore_config({App, Service}).

%% for dynamic node config
get_node_service_config(App, Service, Node, Key, Default) ->
	case get_metastore_config({App, Service}) of
		{error, no_config} ->
            get_node_service_env(App, Service, Node, Key, Default);
		NodeCfgs ->
            case split_node_arg(Key, Node, NodeCfgs) of
                {error, Reason} -> {error, Reason};
                {_, []} -> Default;
                {_, [NodeCfg]} ->
				    case proplists:get_value(Key, NodeCfg) of
				        undefined ->
                            get_node_service_env(App, Service, Node, Key, Default);
				    	Value -> Value
				    end
            end
	end.
set_node_service_config(App, Service, Node, Key, Value) ->
	case get_metastore_config({App, Service}) of
		{error, no_config} ->
            set_service_config(App, Service, makeup_nodes_config(Node, Key, Value, []));
        NodeCfgs ->
    		case split_node_arg(Key, Node, NodeCfgs) of
       		 {error, Reason} -> {error, Reason};
       		 {ONodeCfgs, []} ->
           		 set_service_config(App, Service, makeup_nodes_config(Node, Key, Value, ONodeCfgs));
      		  {ONodeCfgs, [NodeCfg]} ->
           		 NNodeCfg = proplists:delete(Key, NodeCfg) ++ [{Key, Value}],
           		 set_service_config(App, Service, makeup_nodes_config(NNodeCfg, ONodeCfgs))
  		  end
    end.
del_node_service_config(App, Service, Node, Key) ->
    case get_metastore_config({App, Service}) of
		{error, no_config} -> ok;
        NodeCfgs ->
   		    case split_node_arg(Key, Node, NodeCfgs) of
                {error, Reason} -> {error, Reason};
                {_ONodeCfgs, []} -> ok;
                {ONodeCfgs, [NodeCfg]} ->
                    NNodeCfg = proplists:delete(Key, NodeCfg),
                    set_service_config(App, Service, makeup_nodes_config(NNodeCfg, ONodeCfgs))
            end
    end.

%% dynamic config internal
get_metastore_config(Key) ->
    rds_la_metastore:get_config(Key).

set_metastore_config(Key, Value) ->
    rds_la_metastore:set_config(Key, Value).

del_metastore_config(Key) ->
    rds_la_metastore:del_config(Key).

makeup_nodes_config(NodeCfg, NodeCfgs) ->
    [NodeCfg | NodeCfgs].

makeup_nodes_config(Node, Key, Value, NodeCfgs) ->
    [[{node, Node}, {Key, Value}] | NodeCfgs].

get_args(Key, NodeCfgs) ->
    get_args(Key, NodeCfgs, []).

get_args(_Key, [], AccKeyCfg) ->
    AccKeyCfg;
get_args(Key, [NodeCfg|NodeCfgs], AccKeyCfg) ->
    case proplists:get_value(Key, NodeCfg) of
        undefined ->
            get_args(Key, NodeCfgs, AccKeyCfg);
        Value ->
            get_args(Key, NodeCfgs, [Value|AccKeyCfg])
    end.

split_node_arg(Key, Node, NodeCfgs) ->
    split_node_arg(Key, Node, NodeCfgs, [], []).

split_node_arg(_Key, _Node, [], AccNodeCfgs, AccNodeCfg) ->
    case check_node_arg(AccNodeCfg) of
        ok -> {AccNodeCfgs, AccNodeCfg};
        {error, Reason} -> {error, Reason}
    end;
split_node_arg(Key, Node, [NodeCfg|NodeCfgs], AccNodeCfgs, AccNodeCfg) ->
    case proplists:get_value(node, NodeCfg) =:= Node of
        false ->
            split_node_arg(Key, Node, NodeCfgs, [NodeCfg|AccNodeCfgs], AccNodeCfg);
        true ->
            split_node_arg(Key, Node, NodeCfgs, AccNodeCfgs, [NodeCfg|AccNodeCfg])
    end.

check_node_arg([_]) -> ok;
check_node_arg([_|_]) -> {error, config_duplicated};
check_node_arg([]) -> ok.
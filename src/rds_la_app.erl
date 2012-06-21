-module(rds_la_app).

-include("logger_header.hrl").

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    rds_la_lib:ensure_started(mnesia),
    ControllerNodes = rds_la_config:service_nodes(rds_la_controller),
    case rds_la_config:have_service(rds_la_controller) of
        true -> init_metastore_disc(ControllerNodes);
        false -> init_metastore_readonly(ControllerNodes)
    end,
    Sup = rds_la_sup:start_link(),
    ?DEBUG("Start Sup: ~p~n", [Sup]),
    Sup.

stop(_State) ->
    ControllerNodes = rds_la_config:service_nodes(rds_la_controller),
    case rds_la_config:have_service(rds_la_controller) of
        true -> terminate_metastore_disc(ControllerNodes);
        false -> ok
    end,
    ok.

init_metastore_disc(Nodes) ->
    metastore_mnesia:init(Nodes, rds_la_metastore),
    case rds_la_metastore:is_inited() of
        false -> rds_la_metastore:initialize();
        true -> ok
    end.

terminate_metastore_disc(Nodes) ->
    metastore_mnesia:terminate(Nodes, rds_la_metastore).

init_metastore_readonly(ExtraNodes) ->
    metastore_mnesia:init_read_only(ExtraNodes, rds_la_metastore).
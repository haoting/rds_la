%%% -------------------------------------------------------------------
%%% Author  : haoting.wq
%%% Description :
%%%
%%% Created : 2012-6-18
%%% -------------------------------------------------------------------
-module(rds_la_network_sup).

-behaviour(supervisor).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("logger_header.hrl").
%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------
-export([start_link/0]).
-export([controller_servers/0]).

%% --------------------------------------------------------------------
%% Internal exports
%% --------------------------------------------------------------------
-export([init/1]).

%% --------------------------------------------------------------------
%% Macros
%% --------------------------------------------------------------------
-define(SERVER, ?MODULE).
-define(CONTROLLER_PREFIX, rds_la_controller).
-define(STORE_PREFIX, rds_la_store).
-define(ACTIVE, once).
-define(PACKET_LEN, 4).
%% --------------------------------------------------------------------
%% Records
%% --------------------------------------------------------------------

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), ?CHILD2(I, Type, I, [])).
-define(CHILD2(I, Type, ID, Args), {ID, {I, start_link, Args}, permanent, 5000, Type, [I]}).
-define(CHILD3(ID, Type, Mod, Fun, Args), {ID, {Mod, Fun, Args}, permanent, 5000, Type, [Mod]}).

%% ====================================================================
%% External functions
%% ====================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

controller_servers() ->
    back_proxy_api:which_children(?CONTROLLER_PREFIX).

%% ====================================================================
%% Server functions
%% ====================================================================
%% --------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok,  {SupFlags,  [ChildSpec]}} |
%%          ignore                          |
%%          {error, Reason}
%% --------------------------------------------------------------------

init([]) ->
    ServiceSpecs = services_specs(),
    ?DEBUG("ServiceSpecs: ~n~p~n", [ServiceSpecs]),
    {ok, {{one_for_one, 5, 10}, ServiceSpecs}}.

services_specs()->
    ControllerListenPort = rds_la_config:la_controller_listen_port(node()),

    StoreListenPort = rds_la_config:la_store_listen_port(node()),

    HaveStoreService = rds_la_config:have_service(rds_la_store),
    HaveControllerService = rds_la_config:have_service(rds_la_controller),
    [Spec || {Spec, Enable} <-
    [
        {rds_la_store_back_sup_spec(StoreListenPort),                     HaveStoreService},
        {rds_la_controller_back_sup_spec(ControllerListenPort),           HaveControllerService},
        {rds_la_controller_monitor_sup_spec(),                            HaveControllerService}
    ],
        Enable =:= true
    ].

%% ====================================================================
%% Internal functions
%% ====================================================================

rds_la_controller_back_sup_spec(ListenPort) ->
    ?CHILD3(rds_la_controller_back_sup, supervisor, back_proxy_api, start_link_listener_sup, 
			[?CONTROLLER_PREFIX, tcp, "0", ListenPort, rds_la_controller_protocol, [undefined],
             [{packet, ?PACKET_LEN}, {active, ?ACTIVE}]]).

rds_la_store_back_sup_spec(ListenPort) ->
    ?CHILD3(rds_la_store_back_sup, supervisor, back_proxy_api, start_link_listener_sup, 
			[?STORE_PREFIX, tcp, "0", ListenPort, rds_la_store_protocol, [],
             [{packet, ?PACKET_LEN}, {active, ?ACTIVE}]]).

rds_la_controller_monitor_sup_spec() ->
    ?CHILD(rds_la_controller_monitor, worker).

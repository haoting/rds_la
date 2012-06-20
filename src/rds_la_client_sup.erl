%%% -------------------------------------------------------------------
%%% Author  : haoting.wq
%%% Description :
%%%
%%% Created : 2012-6-19
%%% -------------------------------------------------------------------
-module(rds_la_client_sup).

-behaviour(supervisor).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("logger_header.hrl").
%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------
-export([start_link/0]).
-export([new_controller_client/0, new_store_client/0]).

%% --------------------------------------------------------------------
%% Internal exports
%% --------------------------------------------------------------------
-export([init/1]).

%% --------------------------------------------------------------------
%% Macros
%% --------------------------------------------------------------------
-define(SERVER, ?MODULE).

-define(CONTROLLER_CLIENT_PREFIX, rds_la_controller_client).
-define(STORE_CLIENT_PREFIX, rds_la_store_client).
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

new_controller_client() ->
    rds_la_controller_protocol:new_controller_client(?CONTROLLER_CLIENT_PREFIX).

new_store_client() ->
    back_proxy_api:new_send_proxy_by_prefix(?STORE_CLIENT_PREFIX).

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
    ControllerDstNPList = rds_la_config:la_controller_dstnps(),
    [
        rds_la_controller_client_back_sup_spec(ControllerDstNPList),
        rds_la_controller_client_spec(),
        rds_la_store_client_back_sup_spec(),
        rds_la_client_proxy_spec()
    ].

%% ====================================================================
%% Internal functions
%% ====================================================================

rds_la_controller_client_back_sup_spec(ControllerDstNPList) ->
    ?CHILD3(rds_la_controller_client_back_sup, supervisor, back_proxy_api, start_link_nolistener_sup, 
			[?CONTROLLER_CLIENT_PREFIX, rds_la_controller_protocol, [ControllerDstNPList],
             [{packet, ?PACKET_LEN}, {active, ?ACTIVE}]]).

rds_la_controller_client_spec() ->
    ?CHILD3(rds_la_controller_client, worker, ?MODULE, new_controller_client, []).

rds_la_store_client_back_sup_spec() ->
    ?CHILD3(rds_la_store_client_back_sup, supervisor, back_proxy_api, start_link_nolistener_sup, 
			[?STORE_CLIENT_PREFIX, rds_la_store_protocol, [],
             [{packet, ?PACKET_LEN}, {active, ?ACTIVE}]]).

rds_la_client_proxy_spec() ->
    ?CHILD(rds_la_client_proxy, worker).

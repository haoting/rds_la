%%% -------------------------------------------------------------------
%%% Author  : haoting.wq
%%% Description :
%%%
%%% Created : 2012-6-11
%%% -------------------------------------------------------------------
-module(rds_la_log_sup).

-behaviour(supervisor).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("logger_header.hrl").
%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------
-export([start_link/0]).

%% --------------------------------------------------------------------
%% Internal exports
%% --------------------------------------------------------------------
-export([
	 init/1
        ]).

%% --------------------------------------------------------------------
%% Macros
%% --------------------------------------------------------------------
-define(SERVER, ?MODULE).

-define(CHILD(I, Type), ?CHILD2(I, Type, I, [])).
-define(CHILD2(I, Type, ID, Args), {ID, {I, start_link, Args}, permanent, 5000, Type, [I]}).
-define(CHILD3(ID, Type, Mod, Fun, Args), {ID, {Mod, Fun, Args}, permanent, 5000, Type, [Mod]}).
%% --------------------------------------------------------------------
%% Records
%% --------------------------------------------------------------------

%% ====================================================================
%% External functions
%% ====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

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
    HaveStoreService = rds_la_config:have_service(rds_la_store),
    HaveControllerService = rds_la_config:have_service(rds_la_controller),
    HaveClientService = rds_la_config:have_service(rds_la_client),
    [Spec || {Spec, Enable} <-
    [
        {rds_la_tlog_sup_spec(),                         HaveStoreService},
        {rds_la_gen_indexer_sup_spec(),                  HaveStoreService},
        {rds_la_handler_sup_spec(),                      HaveStoreService},
        {rds_la_store_spec(),                            HaveStoreService},
        {rds_la_controller_spec(),                       HaveControllerService},
        {rds_la_network_sup_spec(),                      HaveControllerService or HaveStoreService},
        {rds_la_client_sup_spec(),                       HaveClientService}
    ],
        Enable =:= true
    ].

%% ====================================================================
%% Internal functions
%% ====================================================================

rds_la_controller_spec() ->
	?CHILD(rds_la_controller, worker).

rds_la_network_sup_spec() ->
	?CHILD(rds_la_network_sup, supervisor).

rds_la_store_spec() ->
	?CHILD(rds_la_store, worker).

rds_la_handler_sup_spec() ->
	?CHILD3(rds_la_handler_sup, worker, rds_la_epoolsup, rds_la_handler_sup_start, []).

rds_la_gen_indexer_sup_spec() ->
	?CHILD3(rds_la_gen_indexer_sup, worker, rds_la_epoolsup, rds_la_gen_indexer_sup_start, []).

rds_la_tlog_sup_spec() ->
	?CHILD3(rds_la_tlog_sup, worker, rds_la_epoolsup, rds_la_tlog_sup_start, []).

rds_la_client_sup_spec() ->
	?CHILD(rds_la_client_sup, supervisor).


-module(rds_la_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), ?CHILD2(I, Type, I, [])).
-define(CHILD2(I, Type, ID, Args), {ID, {I, start_link, Args}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, {{one_for_one, 5, 10},
          [
              rds_la_log_sup_spec()
          ]}}.

rds_la_log_sup_spec() ->
    ?CHILD(rds_la_log_sup, supervisor).
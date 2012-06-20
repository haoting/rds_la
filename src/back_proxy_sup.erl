-module(back_proxy_sup).

-include("back_proxy.hrl").

-behaviour(supervisor).
-define(SERVER, ?MODULE).

%% API
-export([start_link/4, new_proxy/2, delete_proxy/2, which_children/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Prefix, Mod, ModArg, ConnetOpts) ->
    MyName = ?prefix_to_back_proxy_sup_name(Prefix),
    supervisor:start_link({local, MyName}, ?MODULE, [Prefix, Mod, ModArg, ConnetOpts]).

%% return: {ok, Pid}
new_proxy(MyName, LS) ->
    supervisor:start_child(MyName, [LS]).

delete_proxy(MyName, Pid) ->
    supervisor:terminate_child(MyName, Pid).

which_children(MyName) ->
    supervisor:which_children(MyName).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(BackProxyArg) ->
    {ok, {{simple_one_for_one, 0, 1}, 
        [{back_proxy,
            {back_proxy, start_link, BackProxyArg},
            temporary, brutal_kill, worker, []
        }]}}.
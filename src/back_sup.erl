-module(back_sup).

-include("back_proxy.hrl").

-behaviour(supervisor).
-define(SERVER, ?MODULE).

%% API
-export([start_link_listener/7, start_link_nolistener/4]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(CHILD2(I, Type, ID, Args), {ID, {I, start_link, Args}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link_listener(Prefix, NType, Host, Port, Mod, ModArg, ConnetOpts) ->
    MyName = ?prefix_to_back_sup_name(Prefix),
    supervisor:start_link({local, MyName}, ?MODULE, [Prefix, NType, Host, Port, Mod, ModArg, ConnetOpts]).

start_link_nolistener(Prefix, Mod, ModArg, ConnetOpts) ->
    MyName = ?prefix_to_back_sup_name(Prefix),
    supervisor:start_link({local, MyName}, ?MODULE, [Prefix, Mod, ModArg, ConnetOpts]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([Prefix, NType, Host, Port, Mod, ModArg, ConnetOpts]) ->
    {ok, {{one_for_one, 5, 10},
            [
                back_proxy_sup_spec(Prefix, Mod, ModArg, ConnetOpts),
                back_listener_spec(Prefix, NType, Host, Port, ConnetOpts)
            ]}};
init([Prefix, Mod, ModArg, ConnetOpts]) ->
    {ok, {{one_for_one, 5, 10},
            [
                back_proxy_sup_spec(Prefix, Mod, ModArg, ConnetOpts)
            ]}}.

back_proxy_sup_spec(Prefix, Mod, ModArg, ConnetOpts) ->
	?CHILD2(back_proxy_sup, supervisor, back_proxy_sup, [Prefix, Mod, ModArg, ConnetOpts]).
back_listener_spec(Prefix, NType, Host, Port, ConnetOpts) ->
	?CHILD2(back_listener, worker, back_listener, [Prefix, NType, Host, Port, ConnetOpts]).
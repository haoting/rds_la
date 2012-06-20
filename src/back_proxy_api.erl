%% Author: haoting.wq
%% Created: 2012-5-31
%% Description: TODO: Add description to back_proxy_api
-module(back_proxy_api).

%%
%% Include files
%%
-include("back_proxy.hrl").
%%
%% Exported Functions
%%
-export([start_link_listener_sup/7, start_link_nolistener_sup/4]).
-export([which_children/1]).
-export([new_send_proxy_by_prefix/1, new_send_proxy_by_name/1]).
-export([prefix_to_back_sup_name/1, prefix_to_back_proxy_sup_name/1]).

%%
%% API Functions
%%
start_link_listener_sup(Prefix, NType, Host, Port, Mod, ModArg, ConnetOpts) ->
    back_sup:start_link_listener(Prefix, NType, Host, Port, Mod, ModArg, ConnetOpts).

start_link_nolistener_sup(Prefix, Mod, ModArg, ConnetOpts) ->
    back_sup:start_link_nolistener(Prefix, Mod, ModArg, ConnetOpts).

which_children(Prefix) ->
    BackProxySupName = ?prefix_to_back_proxy_sup_name(Prefix),
    back_proxy_sup:which_children(BackProxySupName).

new_send_proxy_by_prefix(Prefix) ->
    BackProxySupName = ?prefix_to_back_proxy_sup_name(Prefix),
    back_proxy_sup:new_proxy(BackProxySupName, undefined).

new_send_proxy_by_name(BackProxySupName) ->
    back_proxy_sup:new_proxy(BackProxySupName, undefined).

prefix_to_back_sup_name(Prefix) ->
    ?prefix_to_back_sup_name(Prefix).

prefix_to_back_proxy_sup_name(Prefix) ->
    ?prefix_to_back_proxy_sup_name(Prefix).

%%
%% Local Functions
%%


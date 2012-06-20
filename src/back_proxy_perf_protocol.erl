%% Author: haoting.wq
%% Created: 2012-5-23
%% Description: TODO: Add description to back_proxy_perf_protocol
-module(back_proxy_perf_protocol).

-behaviour(back_proxy).
%%
%% Include files
%%
-include("logger_header.hrl").
%%
%% Exported Functions
%%
-export([init/1, terminate/2]).
-export([handle_msg/3, handle_nowait/1, handle_timer/2, handle_local_info/2]).

-export([aping/1, sping/1, aping/3, sping/3]).

-record(bppp_state, {}).

%%
%% API Functions
%%

aping(Pid) -> back_proxy:local_cast(Pid, <<"APING">>).
sping(Pid) -> back_proxy:local_call(Pid, <<"SPING">>).

aping(Pid, Node, Port) -> back_proxy:remote_cast(Pid, {Node, Port}, <<"APING">>).
sping(Pid, Node, Port) -> back_proxy:remote_call(Pid, {Node, Port}, <<"SPING">>).

%% ------------------------------------------------------------------
%% back_proxy Function Definitions
%% ------------------------------------------------------------------

init(_ModArg) -> 
    {ok, #bppp_state{}}.

terminate(_Reason, _ModState) ->
    ok.

handle_msg(_From, Binary, ModState) ->
    ?DEBUG("remote receive msg: ~p~n", [Binary]),
    handle(Binary, ModState).

handle_nowait(ModState) ->
    {wait_msg, ModState}.

handle_timer(_TimerMsg, ModState) ->
    {wait_msg, ModState}.

handle_local_info({remote_call, DstNP, Binary}, ModState) ->
    ?DEBUG("local receive msg: ~p~n", [Binary]),
    handle_remote(DstNP, Binary, ModState);
handle_local_info({remote_cast, DstNP, Binary}, ModState) ->
    ?DEBUG("local receive msg: ~p~n", [Binary]),
    handle_remote(DstNP, Binary, ModState);
handle_local_info({local_call, Binary}, ModState) ->
    ?DEBUG("local receive msg: ~p~n", [Binary]),
    handle(Binary, ModState);
handle_local_info(_LocalInfo, ModState) ->
    {wait_msg, ModState}.

%%
%% Local Functions
%%
handle_remote(DstNP, Binary, ModState) ->
    {{nowatch_send, DstNP, Binary}, ModState}.

handle(<<"APING">>, ModState) ->
    ?DEBUG("process APING in node ~p~n", [node()]),
    {wait_msg, ModState};
handle(<<"SPING">>, ModState) ->
    ?DEBUG("process SPING in node ~p~n", [node()]),
    {{reply, <<"SPONG">>}, ModState};
handle(<<"SPONG">>, ModState) ->
    ?DEBUG("process SPONG in node ~p~n", [node()]),
    {{reply, <<"SPONG">>}, ModState};
handle(_Binary, ModState) ->
    {wait_msg, ModState}.

%% Author: haoting.wq
%% Created: 2012-6-12
%% Description: TODO: Add description to rds_la_query_worker
-module(rds_la_query_worker).

%%
%% Include files
%%
-include("logger_header.hrl").

%%
%% Exported Functions
%%
-export([start_link/2, exec/2]).

%%
%% API Functions
%%

start_link(Client, F) ->
    proc_lib:spawn_link(?MODULE, exec, [Client, F]).

exec(Client, F) ->
		Msg = try
			{ok, F() }
		catch
			_C:E ->
				?ERROR("execute query error ~p ~p~n", [E, erlang:get_stacktrace()]),
				{error, {E, erlang:get_stacktrace()}}
		end,
		gen_server:reply(Client, Msg).

%%
%% Local Functions
%%


%% Author: haoting.wq
%% Created: 2012-6-12
%% Description: TODO: Add description to rds_la_analyze
-module(rds_la_analyze).

-export([init/0, analyze_log/1]).

-on_load(init/0).

-define(APPNAME, rds_la).

init() ->
	case code:priv_dir(?APPNAME) of
		{error, _} ->
			error_logger:format("~w priv dir not found~n", [?APPNAME]),
			exit(error);
		PrivDir ->
			erlang:load_nif(filename:join([PrivDir, "rds_la_analyze_nifs"]), 0)
	end.
%	erlang:load_nif(filename:join(["./priv", "rds_la_analyze_nifs"]), 0).

analyze_log(_Data) ->
	erlang:nif_error(nif_not_loaded).

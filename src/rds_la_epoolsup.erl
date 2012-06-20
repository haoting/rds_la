%% Author: haoting.wq
%% Created: 2012-6-12
%% Description: TODO: Add description to rds_la_epoolsup
-module(rds_la_epoolsup).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([rds_la_tlog_sup_start/0, rds_la_tlog_sup_stop/0]).
-export([add_tlog/3, del_tlog/2, del_tlog/1, get_tlog/2]).

-export([rds_la_gen_indexer_sup_start/0, rds_la_gen_indexer_sup_stop/0]).
-export([add_gen_indexer/4, del_gen_indexer/1]).

-export([rds_la_handler_sup_start/0, rds_la_handler_sup_stop/0]).
-export([add_handler/5, del_handler/1, get_handler/1, all_handler/0]).

-define(START_SUP(Name, Child), epoolsup:start_link(Name, {Child, start_link, [], stop})).
-define(STOP_SUP(Name), epoolsup:stop(Name)).
%%
%% API Functions
%%
%% tlog
rds_la_tlog_sup_start() ->
    ?START_SUP(rds_la_tlog_sup, rds_la_tlog).

rds_la_tlog_sup_stop() ->
    ?STOP_SUP(rds_la_tlog_sup).

add_tlog(Dir, Name, Config) ->
    Key = {filename:absname(Dir), Name},
    epoolsup:add_child(rds_la_tlog_sup, Key, [Dir, Name, Config]).

del_tlog(Dir, Name) ->
    Key = {filename:absname(Dir), Name},
    epoolsup:del_child(rds_la_tlog_sup, Key).

del_tlog(Pid) ->
    epoolsup:del_child_pid(rds_la_tlog_sup, Pid).

get_tlog(Dir, Name) ->
    Key = {filename:absname(Dir), Name},
    case epoolsup:get_child(rds_la_tlog_sup, Key) of
        [] -> {error, no_tlog};
        [{_DirName, TLog}] -> {ok, TLog}
    end.

%% gen_indexer
rds_la_gen_indexer_sup_start() ->
    ?START_SUP(rds_la_gen_indexer_sup, rds_la_gen_indexer).

rds_la_gen_indexer_sup_stop() ->
    ?STOP_SUP(rds_la_gen_indexer_sup).

add_gen_indexer(Module, Dir, DataSourceName, Opts) ->
    Key = {Module, Dir, DataSourceName},
    epoolsup:add_child(rds_la_gen_indexer_sup, Key, [Module, Dir, DataSourceName, Opts]).

del_gen_indexer(Pid) ->
    epoolsup:del_child_pid(rds_la_gen_indexer_sup, Pid).

%% handler
rds_la_handler_sup_start() ->
    ?START_SUP(rds_la_handler_sup, rds_la_handler).

rds_la_handler_sup_stop() ->
    ?STOP_SUP(rds_la_handler_sup).

add_handler(User, Dir, Name, IndexModules, ProxyList) ->
    epoolsup:add_child(rds_la_handler_sup, User, [Dir, Name, IndexModules, ProxyList]).

del_handler(Pid) ->
    epoolsup:del_child_pid(rds_la_handler_sup, Pid).

get_handler(User) ->
    epoolsup:get_child(rds_la_handler_sup, User).

all_handler() ->
    epoolsup:all_child(rds_la_handler_sup).
%%
%% Local Functions
%%

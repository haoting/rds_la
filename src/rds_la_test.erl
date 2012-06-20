%% Author: haoting.wq
%% Created: 2012-5-3
%% Description: TODO: Add description to rds_la_test
-module(rds_la_test).

-include("logger_header.hrl").
-include("rds_la_log.hrl").

-compile(export_all).

-define(LISTEN_PORT, 9016).
-define(CASCADE_PORT, 9018).
-define(ACTIVE, once).

-define(PERF_REPORT_QUANTITY, 10000).
-define(PERF_QPS_INIT(Ref), perf_qps:mperf_init(Ref)).
-define(PERF_QPS_SCOPE(Ref, Position, Prefix), perf_qps:mperf_scope(Ref, Position, Prefix, ?PERF_REPORT_QUANTITY)).

-define(PREFIX, rds_la).
-define(BACKUP_FILE, "rds_la_mnesia_backup").
%%
%% API Functions
%%
%% -------------------------------------------------------------------------------
%% test back_proxy
start_sender() ->
    back_proxy_api:start_link_nolistener_sup(?PREFIX, rds_la_test_protocol, [], [{packet, 4}, {active, ?ACTIVE}]),
    back_proxy_api:new_send_proxy_by_prefix(?PREFIX).

test_local_call_append(Pid, Key, Value) ->
    rds_la_test_protocol:local_call_append(Pid, Key, Value).
test_local_call_lookup(Pid, Key) ->
    rds_la_test_protocol:local_call_lookup(Pid, Key).
test_local_cast_append(Pid, Key, Value) ->
    rds_la_test_protocol:local_cast_append(Pid, Key, Value).

start_receiver() ->
    back_proxy_api:start_link_listener_sup(?PREFIX, tcp, "0", ?LISTEN_PORT, rds_la_test_protocol, [], [{packet, 4}, {active, ?ACTIVE}]).

test_remote_call_append(Pid, Node, Key, Value) ->
    rds_la_test_protocol:remote_call_append(Pid, Node, ?LISTEN_PORT, Key, Value).
test_remote_call_lookup(Pid, Node, Key) ->
    rds_la_test_protocol:remote_call_lookup(Pid, Node, ?LISTEN_PORT, Key).
test_remote_cast_append(Pid, Node, Key, Value) ->
    rds_la_test_protocol:remote_cast_append(Pid, Node, ?LISTEN_PORT, Key, Value).
test_remote_call_append_fast(Pid, Node, Key, Value) ->
    rds_la_test_protocol:remote_call_append_fast(Pid, Node, ?LISTEN_PORT, Key, Value).
test_remote_call_lookup_fast(Pid, Node, Key) ->
    rds_la_test_protocol:remote_call_lookup_fast(Pid, Node, ?LISTEN_PORT, Key).
test_remote_call_append_async(Pid, Node, Key, Value) ->
    rds_la_test_protocol:remote_call_append_async(Pid, Node, ?LISTEN_PORT, Key, Value).
test_remote_call_lookup_async(Pid, Node, Key) ->
    rds_la_test_protocol:remote_call_lookup_async(Pid, Node, ?LISTEN_PORT, Key).

test_remote_call_append_multiple(Pid, Node, Key, Value, Times) ->
    rds_la_test:loop(Times, fun() -> test_remote_call_append(Pid, Node, Key, Value) end).
test_remote_call_lookup_multiple(Pid, Node, Key, Times) ->
    rds_la_test:loop(Times, fun() -> test_remote_call_lookup(Pid, Node, Key) end).
test_remote_cast_append_multiple(Pid, Node, Key, Value, Times) ->
    rds_la_test:loop(Times, fun() -> test_remote_cast_append(Pid, Node, Key, Value) end).
test_remote_call_append_fast_multiple(Pid, Node, Key, Value, Times) ->
    rds_la_test:loop(Times, fun() -> test_remote_call_append_fast(Pid, Node, Key, Value) end).
test_remote_call_lookup_fast_multiple(Pid, Node, Key, Times) ->
    rds_la_test:loop(Times, fun() -> test_remote_call_lookup_fast(Pid, Node, Key) end).
test_remote_call_append_async_multiple(Pid, Node, Key, Value, Times) ->
    rds_la_test:loop(Times, fun() -> test_remote_call_append_async(Pid, Node, Key, Value) end).
test_remote_call_lookup_async_multiple(Pid, Node, Key, Times) ->
    rds_la_test:loop(Times, fun() -> test_remote_call_lookup_async(Pid, Node, Key) end).

start_cascade() ->
    back_proxy_api:start_link_listener_sup(?PREFIX, tcp, "0", ?CASCADE_PORT, rds_la_test_protocol, [], [{packet, 4}, {active, ?ACTIVE}]).

test_cascade_call_append(Pid, Node, CNode, Key, Value) ->
    rds_la_test_protocol:cascade_call_append(Pid, [{Node, ?LISTEN_PORT},{CNode, ?CASCADE_PORT}], Key, Value).
test_cascade_call_lookup(Pid, Node, CNode, Key) ->
    rds_la_test_protocol:cascade_call_lookup(Pid, [{Node, ?LISTEN_PORT},{CNode, ?CASCADE_PORT}], Key).
test_cascade_cast_append(Pid, Node, CNode, Key, Value) ->
    rds_la_test_protocol:cascade_cast_append(Pid, [{Node, ?LISTEN_PORT},{CNode, ?CASCADE_PORT}], Key, Value).

start_ping_sender() -> 
    back_proxy_api:start_link_nolistener_sup(?PREFIX, back_proxy_perf_protocol, [], [{packet, 0}, {active, ?ACTIVE}]),
    back_proxy_api:new_send_proxy_by_prefix(?PREFIX).
start_ping_receiver() ->
    back_proxy_api:start_link_listener_sup(?PREFIX, tcp, "0", ?LISTEN_PORT, back_proxy_perf_protocol, [], [{packet, 0}, {active, ?ACTIVE}]).

test_aping(Pid) ->
    back_proxy_perf_protocol:aping(Pid).
test_sping(Pid) ->
    back_proxy_perf_protocol:sping(Pid).
test_aping(Pid, Node) ->
    back_proxy_perf_protocol:aping(Pid, Node, ?LISTEN_PORT).
test_sping(Pid, Node) ->
    back_proxy_perf_protocol:sping(Pid, Node, ?LISTEN_PORT).

test_aping_multi(_Pid, 0) -> ok;
test_aping_multi(Pid, N) ->
    back_proxy_perf_protocol:aping(Pid),
    test_aping_multi(Pid, N - 1).

test_sping_multi(_Pid, 0) -> ok;
test_sping_multi(Pid, N) ->
    back_proxy_perf_protocol:sping(Pid),
    test_sping_multi(Pid, N - 1).

test_aping_multi(_Pid, _Node, 0) -> ok;
test_aping_multi(Pid, Node, N) ->
    back_proxy_perf_protocol:aping(Pid, Node, ?LISTEN_PORT),
    test_aping_multi(Pid, Node, N - 1).

test_sping_multi(_Pid, _Node, 0) -> ok;
test_sping_multi(Pid, Node, N) ->
    back_proxy_perf_protocol:sping(Pid, Node, ?LISTEN_PORT),
    test_sping_multi(Pid, Node, N - 1).

loop(0, _Fun) -> ok;
loop(N, Fun) ->
    Fun(),
    loop(N - 1, Fun).

loop_perf_scope(N, Fun) ->
    ?PERF_QPS_INIT(loop_perf),
    do_loop_perf_scope(N, Fun).

do_loop_perf_scope(0, _Fun) -> ok;
do_loop_perf_scope(N, Fun) ->
    ?PERF_QPS_SCOPE(loop_perf, start, "loop_perf"),
	Fun(),
    ?PERF_QPS_SCOPE(loop_perf, stop, "loop_perf"),
    do_loop_perf_scope(N - 1, Fun).

set_min_heap_size(Pid, Value) ->
    back_proxy:process_flag(Pid, min_heap_size, Value).
set_min_bin_vheap_size(Pid, Value) ->
    back_proxy:process_flag(Pid, min_bin_vheap_size, Value).
set_scheduler(Pid, Value) ->
    back_proxy:process_flag(Pid, scheduler, Value).
set_priority(Pid, Value) ->
    back_proxy:process_flag(Pid, priority, Value).

get_scheduler(Pid) ->
    back_proxy:system_info(Pid, scheduler_id).

%% The full back_proxy test step:
%% Total test case: A.* + B.* + C.* + D.*
%% ---------------------------------------------------------------------------
%% A. Local
%%    1. start_sender
%%    2. local test: test_local_call_append | test_local_call_lookup | test_local_cast_append
%% ---------------------------------------------------------------------------
%% B. Remote
%%    1. start_sender
%%    2. remote no connection test: test_remote_call_append | test_remote_call_lookup | test_remote_cast_append
%%    3. start_receiver
%%    4. remote connection test: test_remote_call_append | test_remote_call_lookup | test_remote_cast_append
%%    5. stop_receiver
%%    6. check sender process status
%%    7. remote connection down test: test_remote_call_append | test_remote_call_lookup | test_remote_cast_append
%%    8. start_receiver
%%    9. remote connection recover test: test_remote_call_append | test_remote_call_lookup | test_remote_cast_append
%%   10. stop_sender
%%   11. check receiver process status
%%   12. start_sender
%%   13. sender reconnect test: test_remote_call_append | test_remote_call_lookup | test_remote_cast_append
%% ---------------------------------------------------------------------------
%% C. Cascade
%%    1. start_sender | start_receiver
%%    2. cascade no connection test: test_cascade_call_append | test_cascade_call_lookup | test_cascade_cast_append
%%    3. start_cascade
%%    4. cascade connection test: test_cascade_call_append | test_cascade_call_lookup | test_cascade_cast_append
%%    5. stop_cascade
%%    6. check receiver process status
%%    7. cascade connection down test: test_cascade_call_append | test_cascade_call_lookup | test_cascade_cast_append
%%    8. start_cascade
%%    9. cascade connection recover test: test_cascade_call_append | test_cascade_call_lookup | test_cascade_cast_append
%%   10. stop_receiver
%%   11. check cascade process status
%%   12. broker connection down test: test_cascade_call_append | test_cascade_call_lookup | test_cascade_cast_append
%%   13. start_receiver
%%   14. broker connection recover test: test_cascade_call_append | test_cascade_call_lookup | test_cascade_cast_append
%%   15. stop_sender
%%   16. check receiver process status
%%   17. check cascade process status
%%   18. start_sender
%%   19. sender reconnect test: test_cascade_call_append | test_cascade_call_lookup | test_cascade_cast_append
%% ---------------------------------------------------------------------------
%% D. Long Time Services
%%    1. start_sender | start_receiver, set execute time of handle_call in rds_la_test_protocol to 20 seconds
%%    2. remote connection break test: test_remote_call_append | test_remote_call_lookup,
%%       stop_receiver
%%    3. start_sender | start_receiver | start_cascade, set execute time of handle_call in rds_la_test_protocol to 20 seconds
%%    4. cascade connection break test: test_cascade_call_append | test_cascade_call_lookup,
%%       stop_cascade


%% -------------------------------------------------------------------------------
%% test metastore_mnesia

test_metastore_mnesia_start(Nodes) ->
    mnesia:start(),
    metastore_mnesia:init(Nodes, rds_la_metastore).

test_metastore_mnesia_stop(Nodes) ->
    metastore_mnesia:terminate(Nodes),
    mnesia:stop().

test_initialize_rds_la_metastore() ->
    rds_la_metastore:initialize().

test_deinitialize_rds_la_metastore() ->
    rds_la_metastore:deinitialize().

test_metastore_upgrade_online(Nodes) ->
    metastore_mnesia:upgrade_online(Nodes, rds_la_metastore).

test_metastore_backup_online(Nodes) ->
    metastore_mnesia:backup_online(Nodes, rds_la_metastore, ?BACKUP_FILE).

test_metastore_restore_online(Nodes) ->
    metastore_mnesia:restore_online(Nodes, rds_la_metastore, ?BACKUP_FILE).

%% The full metastore_mnesia test step:
%% Total test case: [A.* + B.*] * [ram | disc]
%% ---------------------------------------------------------------------------
%% A. Basic Test:
%%    1. Primary Node Initialize                                  [ram | disc]
%%    2. Primary Node Restart                                     [ram->ram | ram->disc | disc->disc | disc->ram]
%%    3. Secondary Node Copy Initialize                           [ram | disc]
%%    4. Secondary Node Restart                                   [ram->ram | ram->disc | disc->disc | disc->ram]
%%    5. Primary Node Upgrade                                     [disc]
%%    6. Secondary Node Copy Upgrade                              [ram | disc]
%%    7. Secondary Node Upgrade                                   [ram | disc]
%% ---------------------------------------------------------------------------
%% B. Abnormal Test:
%%    1. Primary Node Upgrade                                     [ram]
%%    2. Primary Node Shutdown Abnormally Restart                 [-ram->ram- | -ram->disc- | disc->disc | -disc->ram-]
%%    3. Secondary Node Shutdown Abnormally Restart               [ram->ram | ram->disc | disc->disc | -disc->ram-]
%%    4. Primary Node Shutdown Abnormally Upgrade                 [-ram- | disc]
%%    5. Secondary Node Shutdown Abnormally Upgrade               [ram | disc]
%%    6. Multi Nodes Initialize Simultaneously                    [-ram- | -disc-]
%%    7. Multi Nodes Restart Simultaneously                       [-ram->ram- | -ram->disc- | -disc->disc- | -disc->ram-]
%%    8. Multi Nodes Upgrade Simultaneously                       [ram | disc]
%%    9. Multi Nodes Shutdown Abnormally Restart                  [ram->ram | ram->disc | disc->disc | disc->ram]
%%   10. Multi Nodes Shutdown Abnormally Upgrade                  [ram | disc]
%%   11. Multi Nodes Shutdown Simultaneously Restart              [ram->ram | ram->disc | disc->disc | disc->ram]
%%   12. Multi Nodes Shutdown Simultaneously Upgrade              [ram->ram | ram->disc | disc->disc | disc->ram]
%%   13. Initialize Node & Non-Initialize Node Coexist
%% ---------------------------------------------------------------------------
%% C. Online Test:
%%    1. Secondary Node Online Backup
%%    2. Secondary Node Online Restore
%%    3. Secondary Node Restart & Online Upgrade
%%    4. Primary Node Restart & Offline Upgrade

%% -------------------------------------------------------------------------------
%% test rds_la_store


test_add_user_store(User) ->
    rds_la_api:add_user_store(User, node()).

test_del_user_store(User) ->
    rds_la_api:del_user_store(User, node()).

test_store_add_user(User) ->
    test_store_add_user(User, node()).
test_store_add_user(User, Node) ->
    rds_la_api:add_user_node(User, Node).

test_store_del_user(User) ->
    test_store_del_user(User, node()).
test_store_del_user(User, Node) ->
    rds_la_api:del_user_node(User, Node).

test_store_get_user(User) ->
    rds_la_store:get_user(User).

test_store_append_log_async_node(User, Node) ->
    test_store_append_log_async_node(User, Node, "0").
test_store_append_log_async_node(User, Node, ProxyId) ->
    test_store_append_log_async_node(User, Node, ProxyId, "select @@version_comment limit 1").
test_store_append_log_async_node(User, Node, ProxyId, Query) ->
    rds_la_api:append_user_log_async(User, Node, ProxyId, makeup_log(User, Query)).

test_store_append_log_async(User) ->
    test_store_append_log_async(User, "0").
test_store_append_log_async(User, ProxyId) ->
    test_store_append_log_async(User, ProxyId, "select @@version_comment limit 1").
test_store_append_log_async(User, ProxyId, Query) ->
    rds_la_api:append_user_log_async(User, ProxyId, makeup_log(User, Query)).

test_store_append_log_sync_node(User, Node) ->
    test_store_append_log_sync_node(User, Node, "0").
test_store_append_log_sync_node(User, Node, ProxyId) ->
    test_store_append_log_sync_node(User, Node, ProxyId, "select @@version_comment limit 1").
test_store_append_log_sync_node(User, Node, ProxyId, Query) ->
    rds_la_api:append_user_log_sync(User, Node, ProxyId, makeup_log(User, Query)).

test_store_append_log_sync(User) ->
    test_store_append_log_sync(User, "0").
test_store_append_log_sync(User, ProxyId) ->
    test_store_append_log_sync(User, ProxyId, "select @@version_comment limit 1").
test_store_append_log_sync(User, ProxyId, Query) ->
    rds_la_api:append_user_log_sync(User, ProxyId, makeup_log(User, Query)).

test_store_query_log_dump_slow(User, DateStartStr, DateEndStr) ->
    test_store_query_log_dump_slow(User, node(), DateStartStr, DateEndStr).

test_store_query_log_dump_slow(User, Node, DateStartStr, DateEndStr) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_api:query_user_dump_slow(User, Node, DateStart, DateEnd).

test_store_query_log_dump_slow(User, DateStartStr, DateEndStr, MinExecTime, MaxExecTime, Quantity) ->
    test_store_query_log_dump_slow(User, node(), DateStartStr, DateEndStr, MinExecTime, MaxExecTime, Quantity).

test_store_query_log_dump_slow(User, Node, DateStartStr, DateEndStr, MinExecTime, MaxExecTime, Quantity) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_api:query_user_dump_slow(User, Node, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity).

test_store_query_log_sql_stats(User, DateStartStr, DateEndStr) ->
    test_store_query_log_sql_stats(User, node(), DateStartStr, DateEndStr).

test_store_query_log_sql_stats(User, Node, DateStartStr, DateEndStr) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_api:query_user_sql_stats(User, Node, DateStart, DateEnd).

test_store_query_log_sql_stats(User, DateStartStr, DateEndStr, PageStart, PageEnd) ->
    test_store_query_log_sql_stats(User, node(), DateStartStr, DateEndStr, PageStart, PageEnd).

test_store_query_log_sql_stats(User, Node, DateStartStr, DateEndStr, PageStart, PageEnd) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_api:query_user_sql_stats(User, Node, DateStart, DateEnd, PageStart, PageEnd).

%% The full rds_la_store test step:
%% Total test case: [A.*]
%% ---------------------------------------------------------------------------
%% A. Basic Test:
%%    1. Add User
%%    2. Delete User
%%    3. Get User
%%    4. Append User Log
%%    5. Query User Dump Slow Log
%%    6. Query User SQL Stats Log

%% -------------------------------------------------------------------------------
%% test rds_la_controller

test_controller_add_user_store(User, Node) ->
    Props = rds_la_api:default_user_props(),
    rds_la_controller:add_user_store(User, Node, Props).
test_controller_del_user_store(User, Node) ->
    rds_la_controller:add_user_store(User, Node).

test_controller_add_user_node(User, Node) ->
    Props = rds_la_api:default_user_props(),
    rds_la_controller:add_user_node(User, Node, Props).
test_controller_del_user_node(User, Node) ->
    rds_la_controller:del_user_node(User, Node).
test_controller_get_user_nodes(User) ->
    rds_la_controller:get_user_nodes(User).

test_controller_add_user_append_node(User, Node) ->
    Props = rds_la_api:default_user_props(),
    rds_la_controller:add_user_append_node(User, Node, Props).
test_controller_del_user_append_node(User, Node) ->
    rds_la_controller:del_user_append_node(User, Node).
test_controller_get_user_append_nodes(User) ->
    rds_la_controller:get_user_append_nodes(User).

test_controller_add_user_query_node(User, Node) ->
    Props = rds_la_api:default_user_props(),
    rds_la_controller:add_user_query_node(User, Node, Props).
test_controller_del_user_query_node(User, Node) ->
    rds_la_controller:del_user_query_node(User, Node).
test_controller_get_user_query_nodes(User) ->
    rds_la_controller:get_user_query_nodes(User).

test_controller_add_user(User) ->
    rds_la_controller:add_user(User).
test_controller_del_user(User) ->
    rds_la_controller:del_user(User).

test_controller_append_user_node_log(User, Node) ->
    test_controller_append_user_node_log(User, Node, "0").
test_controller_append_user_node_log(User, Node, ProxyId) ->
    test_controller_append_user_node_log(User, Node, ProxyId, "select @@version_comment limit 1").
test_controller_append_user_node_log(User, Node, ProxyId, Query) ->
    rds_la_controller:append_user_node_log_sync(User, Node, ProxyId, makeup_log(User, Query)).

test_controller_query_user_node_log_dump_slow(User, Node, DateStartStr, DateEndStr) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_controller:query_user_node_dump_slow(User, Node, DateStart, DateEnd).

test_controller_query_user_node_log_dump_slow(User, Node, DateStartStr, DateEndStr, MinExecTime, MaxExecTime, Quantity) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_controller:query_user_node_dump_slow(User, Node, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity).

test_store_query_user_node_log_sql_stats(User, Node, DateStartStr, DateEndStr) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_controller:query_user_node_sql_stats(User, Node, DateStart, DateEnd).

test_store_query_user_node_log_sql_stats(User, Node, DateStartStr, DateEndStr, PageStart, PageEnd) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_controller:query_user_node_sql_stats(User, Node, DateStart, DateEnd, PageStart, PageEnd).

test_controller_append_user_log(User) ->
    test_controller_append_user_log(User, "0").
test_controller_append_user_log(User, ProxyId) ->
    test_controller_append_user_log(User, ProxyId, "select @@version_comment limit 1").
test_controller_append_user_log(User, ProxyId, Query) ->
    rds_la_controller:append_user_log_sync(User, ProxyId, makeup_log(User, Query)).

test_controller_query_user_log_dump_slow(User, DateStartStr, DateEndStr) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_controller:query_user_dump_slow(User, DateStart, DateEnd).

test_controller_query_user_log_dump_slow(User, DateStartStr, DateEndStr, MinExecTime, MaxExecTime, Quantity) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_controller:query_user_node_dump_slow(User, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity).

test_store_query_user_log_sql_stats(User, DateStartStr, DateEndStr) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_controller:query_user_sql_stats(User, DateStart, DateEnd).

test_store_query_user_log_sql_stats(User, DateStartStr, DateEndStr, PageStart, PageEnd) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_controller:query_user_sql_stats(User, DateStart, DateEnd, PageStart, PageEnd).

%% The full rds_la_controller test step:
%% Total test case: [A.*]
%% ---------------------------------------------------------------------------
%% A. Basic Test:
%%    1. Add User Store
%%    2. Delete User Store
%%    3. Add User Node
%%    4. Get User Node
%%    5. Add User
%%    6. Del User
%%    7. Delete User Node
%%    8. Add User Append Node
%%    9. Get User Append Node
%%   10. Delete User Append Node
%%   11. Add User Query Node
%%   12. Get User Query Node
%%   13. Delete User Query Node
%%   14. Append User Node Log
%%   15. Query User Node Dump Slow Log
%%   16. Query User Node SQL Stats Log
%%   17. Append User Log
%%   18. Query User Dump Slow Log
%%   19. Query User SQL Stats Log

%% -------------------------------------------------------------------------------
%% test rds_la_network

%% at client node
test_network_ping_node(Node, Port) ->
    rds_la_controller_protocol:ping(Node, Port).

test_network_add_user(User) ->
    rds_la_client_proxy:add_user(User).
test_network_add_user(User, Props) ->
    rds_la_client_proxy:add_user(User, Props).

test_network_del_user(User) ->
    rds_la_client_proxy:del_user(User).

test_network_get_user_dstnps(User) ->
    rds_la_client_proxy:get_user_dstnps(User).

test_network_get_user_append_dstnps(User) ->
    rds_la_controller_protocol:get_user_append_dstnps(User).

test_network_get_user_query_dstnps(User) ->
    rds_la_controller_protocol:get_user_query_dstnps(User).

test_network_new_store_client() ->
    rds_la_client_sup:new_store_client().

test_network_append_user_log_async(User) ->
    test_network_append_user_log_async(User, "0").
test_network_append_user_log_async(User, ProxyId) ->
    test_network_append_user_log_async(User, ProxyId, "select @@version_comment limit 1").
test_network_append_user_log_async(User, ProxyId, Query) ->
    test_network_append_user_record_log_async(User, ProxyId, makeup_log(User, Query)).

test_network_append_user_record_log_async(User, ProxyId, Record) ->
    rds_la_client_proxy:append_user_log_async(User, ProxyId, Record).

test_network_query_user_dump_slow(User, DateStartStr, DateEndStr, MinExecTime, MaxExecTime, Quantity) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_client_proxy:query_user_dump_slow(User, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity).

test_network_query_user_sql_stats(User, DateStartStr, DateEndStr, PageStart, PageEnd) ->
    DateStart = date_string_to_date(DateStartStr),
    DateEnd = date_string_to_date(DateEndStr),
    rds_la_client_proxy:query_user_sql_stats(User, DateStart, DateEnd, PageStart, PageEnd).


%% The full rds_la_network test step:
%% Total test case: [A.*] + [B.*]
%% ---------------------------------------------------------------------------
%% A. Basic Test:
%%    1. Ping
%%    2. Add User
%%    3. Del User
%%    4. Get User Nodes
%%    4. Get User Append DstNPs
%%    4. Get User Query DstNPs
%%    5. Append User Log
%%    6. Query User Dump Slow Log
%%    7. Query User SQL Stats Log
%% ---------------------------------------------------------------------------
%% B. Performance Test:
%%    1. Append User Log



%%
%% Local Functions
%%
-define(BASEQT, 120).
-define(BASERT, 160).
-define(RANDOMI, 80).

makeup_log(User, Query) ->
    TimeStamp = calendar:datetime_to_gregorian_seconds(erlang:localtime()) -
                calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
    #la_record{
        user = User,
        timestamp = TimeStamp,
        'query' = Query,
        query_time = ?BASEQT + random:uniform(?RANDOMI),
        response_time = ?BASERT + random:uniform(?RANDOMI)
    }.

date_string_to_date(DateStr) ->
    rds_la_lib:string_to_localtime(DateStr).
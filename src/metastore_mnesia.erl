%% Author: haoting.wq
%% Created: 2012-5-25
%% Description: TODO: Add description to metastore_mnesia
-module(metastore_mnesia).

%% metastore_mnesia is a disc metastore used mnesia to implement.
%% node may be shutdown normally and abnormally, we distinguish
%% these two different situations by checking if the 
%% nodes_running_at_shutdown file exist in two main location, and
%% we limit that:
%% location                          limit
%% 1. change_schema                  can not change disc node to ram node,
%%                                   the same as can not change cluster topology,
%%                                   if keep the node type or make ram node to disc node,
%%                                   there is no problem
%% 2. upgrade_schema->upgrade_mode   can not do upgrade when abnormally shutdown,
%%                                   must rollback to the old version and shutdown normally


%%
%% Include files
%%
-include("logger_header.hrl").
-include("metastore_header.hrl").
%%
%% Exported Functions
%%
-export([init/2, init_read_only/2, terminate/1]).
-export([current_version/0, create_table/1]).
-export([backup_online/2, backup_online/3, restore_online/3, upgrade_online/2]).

-export([behaviour_info/1]).

-define(VERSION_FILENAME, "schema_version").
-define(UPGRADE_LOCK_FILENAME, "schema_upgrade_lock").
-define(RUNNING_NODES_FILENAME, "nodes_running_at_shutdown").

-define(RPC_TIMEOUT, 1000).                         % 1s
-define(MNESIA_WAIT_TIMEOUT, 300000).               % 5min
-define(MNESIA_WAIT_TABLES_TIMEOUT, 300000).        % 5min
-define(UPGRADE_WAIT_TIMEOUT, 5000).                % 5s

behaviour_info(callbacks) ->
    [
        %% ------------------------------------------------------------
        %% Function: is_running/1
        %% Proto: is_running(Node)
        %% Description: Check if Node is running
        %% Returns:
        %%          true                                                   |
        %%          false                                                  |
        %% ----------------------------------------------------------
        {is_running, 1},

        %% ------------------------------------------------------------
        %% Function: is_inited/0
        %% Proto: is_inited()
        %% Description: Check if system is initialized
        %% Returns:
        %%          true                                                   |
        %%          false                                                  |
        %% ----------------------------------------------------------
        {is_inited, 0},

        %% ------------------------------------------------------------
        %% Function: current_versions/0
        %% Proto: current_versions()
        %% Description: Get system current version list,
        %%              Include all version history
        %% Returns:
        %%          Version List like ['V1', 'V2']
        %% ----------------------------------------------------------
        {current_versions, 0},

        %% ------------------------------------------------------------
        %% Function: table_definitions/0
        %% Proto: table_definitions()
        %% Description: Get system table definition list,
        %%              Each definition is a record of #metastore_table
        %%              defined in metastore_header.hrl
        %% Returns:
        %%          Table definition list
        %% ----------------------------------------------------------
        {table_definitions, 0},

        %% ------------------------------------------------------------
        %% Function: table_upgrade/0
        %% Proto: table_upgrade()
        %% Description: Get system table upgrade list,
        %%              Each definition is a record of #metastore_version
        %%              defined in metastore_header.hrl
        %% Returns:
        %%          Table upgrade list
        %% ----------------------------------------------------------
        {table_upgrade, 0}
    ];
behaviour_info(_Other) ->
    undefined.

%%
%% API Functions
%%

%% Any node in scope of Nodes is disc node, if a node is not in Nodes,
%% and it called init(Nodes, Mod), it must be a ram node
init(Nodes, Mod) ->
    ensure_mnesia_running(),
    ok = init_db(Nodes, Mod),
    ok = global:sync(),
    %% there is a problem here:
    %% if the node is closed abnormally, this file will not exist at next time start,
    %% and if you upgrade mnesia, all nodes think they are the primary
    ok = delete_previously_running_nodes(),
    ok.

init_read_only(ExtraNodes, Mod) ->
    ensure_mnesia_running(),
    init_db_read_only(ExtraNodes, Mod).

terminate(Nodes) ->
    WantDiscNode = should_be_disc_node(Nodes),
    case WantDiscNode of
        true ->
            record_running_nodes();
        false ->
            ok
    end,
    ok.

init_db(Nodes, Mod) ->
    init_db(Nodes, Mod, Mod:is_inited()).

init_db(Nodes, Mod, false) ->
    OtherNodes = Nodes -- [node()],
    case mnesia:change_config(extra_db_nodes, OtherNodes) of
        {ok, []} ->
            ?DEBUG("Primary Node create schema~n", []),
            create_schema(Mod, Nodes);
        {ok, [_|_]} ->
            ?DEBUG("Secondary Node copy schema~n", []),
            copy_schema(Mod, Nodes);
        {error, Reason} -> throw({error, {unable_to_join_cluster_for_init, node(), Nodes, Reason}})
    end;
init_db(Nodes, Mod, true) ->
    case upgrades_required(Mod) of
        {ok, []} ->
            ?DEBUG("No need to upgrade schema, disc node change schema~n", []),
            change_schema(Mod, Nodes, disc_non_initialize_start);
        {ok, UpgradeVersion} ->
            ?DEBUG("Need to upgrade schema~n", []),
            upgrade_schema(Mod, Nodes, UpgradeVersion);
        {error, schema_version_not_exist} ->
            ?DEBUG("No need to upgrade schema, ram node change schema~n", []),
            change_schema(Mod, Nodes, ram_non_initialize_start);
        {error, Reason} ->
            throw({error, {invalid_version, node(), Nodes, Reason}})
    end;
init_db(Nodes, Mod, Initialized) ->
    throw({error, {invalid_init, Initialized, Nodes, Mod}}).

init_db_read_only(ExtraNodes, Mod) ->
    OtherNodes = ExtraNodes -- [node()],
    case mnesia:change_config(extra_db_nodes, OtherNodes) of
        {ok, []} ->
            throw({error, {unable_to_join_cluster_for_init, node(), ExtraNodes, other_nodes_not_running}});
        {ok, [_|_]} ->
            ?DEBUG("Read-only Node wait schema~n", []),
            wait_schema(Mod);
        {error, Reason} ->
            throw({error, {unable_to_join_cluster_for_init, node(), ExtraNodes, Reason}})
    end.

upgrade_online(Nodes, Mod) ->
    case upgrades_required(Mod) of
        {ok, []} ->
            ?DEBUG("No need to upgrade schema online~n", []),
            ok;
        {ok, UpgradeVersion} ->
            ?DEBUG("Need to upgrade schema online~n", []),
            upgrade_schema_online(Mod, Nodes, UpgradeVersion);
        {error, Reason} ->
            throw({error, {invalid_version, node(), Nodes, Reason}})
    end.

backup_online(Nodes, Mod) ->
    backup_schema_online(Mod, Nodes, undefined).

backup_online(Nodes, Mod, File) ->
    backup_schema_online(Mod, Nodes, File).

restore_online(Nodes, Mod, File) ->
    restore_schema_online(Mod, Nodes, File).
%%
%% Local Functions
%%

%% -------------------------------------------------------------------------------
%% schema operation: create | copy | check | upgrade (primary, secondary)

create_schema(Mod, Nodes) ->
    WantDiscNode = should_be_disc_node(Nodes),
    stop_mnesia(),
    case WantDiscNode of
        true -> ensure_ok(mnesia:create_schema([node()]), cannot_create_schema);
        false  -> ensure_ok(mnesia:delete_schema([node()]), cannot_delete_schema)
    end,
    start_mnesia(),
    create_tables(Mod, WantDiscNode),
    check_schema(Mod),
    case WantDiscNode of
        true -> update_version(Mod);
        false  -> ok
    end.

create_tables(Mod, WantDiscNode) ->
    lists:foreach(
        fun (MetastoreTab = #metastore_table{name = Tab, definition = TabDef}) ->
            case create_table(MetastoreTab) of
                {atomic, ok} -> ok;
                {aborted, Reason} ->
                    throw({error, {table_creation_failed, Tab, TabDef, Reason}})
            end
        end,
        table_definitions(Mod, WantDiscNode)),
    ok.

create_table(#metastore_table{name = Tab, definition = TabDef}) ->
    mnesia:create_table(Tab, TabDef).

wait_schema(Mod) ->
    wait_for_tables(Mod).

copy_schema(Mod, Nodes) ->
    OtherNodes = Nodes -- [node()],
    WantDiscNode = should_be_disc_node(Nodes),
    ExistUpgradedNode = distribute_version_check(Mod, OtherNodes, eq),
    case ExistUpgradedNode of
        true ->
            copy_schema_internal(Mod, WantDiscNode);
        false ->
            die("Remote node version is not match.~nPlease stop and "
                "check nodes version.~n", [])
    end.

copy_schema_internal(Mod, WantDiscNode) ->
    ?DEBUG("Will copy schema for Mod: ~p~n", [Mod]),
    ok = wait_for_tables(Mod),
    ?DEBUG("Wait for tables finish~n", []),
    ok = ensure_mnesia_dir(),
    ok = copy_or_change_table(schema, WantDiscNode),
    ok = copy_or_change_tables(Mod, WantDiscNode),
    ?DEBUG("Copy Tables finish~n", []),
    ok = check_schema(Mod),
    case WantDiscNode of
        true -> update_version(Mod);
        false  -> ok
    end.

copy_or_change_tables(Mod, WantDiscNode) ->
    lists:foreach(
      fun (#metastore_table{name = Tab, definition = TabDef}) ->
              HasDiscCopies     = table_has_copy_type(TabDef, disc_copies),
              HasDiscOnlyCopies = table_has_copy_type(TabDef, disc_only_copies),
              StorageType =
                  if
                      WantDiscNode =:= true ->
                          if
                              HasDiscCopies     -> disc_copies;
                              HasDiscOnlyCopies -> disc_only_copies;
                              true              -> ram_copies
                          end;
                      WantDiscNode =:= false ->
                          ram_copies
                  end,
              ok = copy_or_change_table_internal(Tab, StorageType)
      end,
      table_definitions(Mod, WantDiscNode)),
    ok.

copy_or_change_table(Tab, true) ->
    copy_or_change_table_internal(Tab, disc_copies);
copy_or_change_table(Tab, false) ->
    copy_or_change_table_internal(Tab, ram_copies).

copy_or_change_table_internal(Tab, StorageType) ->
    OriginalType = mnesia:table_info(Tab, storage_type),
    {atomic, ok} = if
        OriginalType == unknown ->
            mnesia:add_table_copy(Tab, node(), StorageType);
        OriginalType /= StorageType ->
            mnesia:change_table_copy_type(Tab, node(), StorageType);
        true -> {atomic, ok}
    end,
    ok.

change_schema(Mod, Nodes, Step) ->
    AfterUs = read_previously_running_nodes(),
    case AfterUs of
        [] ->
            %% Even if this node is a ram node, As long as it is the last shutdown node,
            %% it will start anyway
            ?DEBUG("The last shutdown node only check schema~n", []),
            check_schema(Mod);
        null ->
            IsDiscNode = is_disc_node(),
            WantDiscNode = should_be_disc_node(Nodes),
            case {IsDiscNode, WantDiscNode} of
                {true, true} ->
                    check_schema(Mod);
                {false, _} ->
                    do_change_schema_from(Mod, Nodes, Step);
                _ ->
                    throw({error, {Step, do_not_change_cluster_after_abnormally_shutdown}})
            end;
        [_|_] ->
            ?DEBUG("Check if need to change schema type~n", []),
            do_change_schema_from(Mod, Nodes, Step)
    end.

do_change_schema_from(Mod, Nodes, Step) ->
    OtherNodes = Nodes -- [node()],
    case mnesia:change_config(extra_db_nodes, OtherNodes) of
        {ok, []} ->
            throw({error, {Step, no_nodes_to_connect}});
        {ok, [_|_]} ->
            change_schema_internal(Mod, Nodes);
        {error, Reason} ->
            throw({error, {Step, Reason}})
    end.

change_schema_internal(Mod, Nodes) ->
    IsDiscNode = is_disc_node(),
    WantDiscNode = should_be_disc_node(Nodes),
    ok = wait_for_tables(Mod),
    case {IsDiscNode, WantDiscNode} of
        {false, false} ->                             % {ram->ram}
            ?DEBUG("Node type ram not change~n", []),
            check_schema(Mod);
        {false, true} ->                              % {ram->disc}
            ?DEBUG("Node type change from ram to disc~n", []),
            ok = copy_or_change_table(schema, WantDiscNode),
            ok = copy_or_change_tables(Mod, WantDiscNode),
            check_schema(Mod),
            update_version(Mod);
        {true, false} ->                              % {disc->ram}
            ?DEBUG("Node type change from disc to ram~n", []),
            ok = copy_or_change_tables(Mod, WantDiscNode),
            ok = copy_or_change_table(schema, WantDiscNode),
            check_schema(Mod),
            remove_version();
        {true, true} ->                               % {disc->disc}
            ?DEBUG("Node type disc not change~n", []),
            check_schema(Mod)
    end.

check_schema(Mod) ->
    case check_schema_integrity(Mod) of
        ok ->
            ?DEBUG("Check schema ok~n", []),
            ok;
        {error, Reason} ->
            ?DEBUG("Check schema error: ~p~n", [Reason]),
            throw({error, {schema_integrity_check_failed, Reason}})
    end.

check_schema_integrity(Mod) ->
    Tables = mnesia:system_info(tables),
    case check_tables(Mod, fun (MetastoreTable) ->
                          check_table_attributes(MetastoreTable, Tables)
                      end) of
        ok     -> ok = wait_for_tables(Mod),
                  check_tables(Mod, fun check_table_content/1);
        Other  -> Other
    end.

check_table_attributes(#metastore_table{name = Tab, definition = TabDef}, Tables) ->
    case lists:member(Tab, Tables) of
        false -> {error, {table_missing, Tab}};
        true  ->
            {_, ExpAttrs} = proplists:lookup(attributes, TabDef),
            case mnesia:table_info(Tab, attributes) of
                ExpAttrs -> ok;
                Attrs    -> {error, {table_attributes_mismatch, Tab, ExpAttrs, Attrs}}
            end
    end.

check_table_content(#metastore_table{name = Tab, auxiliary = TabAux}) ->
    {_, Match} = proplists:lookup(match, TabAux),
    case mnesia:dirty_first(Tab) of
        '$end_of_table' ->
            ok;
        Key ->
            ObjList = mnesia:dirty_read(Tab, Key),
            MatchComp = ets:match_spec_compile([{Match, [], ['$_']}]),
            case ets:match_spec_run(ObjList, MatchComp) of
                ObjList -> ok;
                _       -> {error, {table_content_invalid, Tab, Match, ObjList}}
            end
    end.

check_tables(Mod, Fun) ->
    case [Error || MetastoreTable <- table_definitions(Mod, is_disc_node()),
                   case Fun(MetastoreTable) of
                       ok             -> Error = none, false;
                       {error, Error} -> true
                   end] of
        []     -> ok;
        Errors -> {error, Errors}
    end.

upgrade_schema(Mod, _Nodes, []) ->
    check_schema(Mod);
upgrade_schema(Mod, Nodes, UpgradeVersion) ->
    stop_mnesia(),
    CV = current_version(),
    ensure_backup_taken(CV),
    ?DEBUG("Backup Taken~n", []),
    AllNodes = lists:usort(Nodes ++ all_clustered_nodes()),

    UpgradeMode = upgrade_mode(Mod, AllNodes),
    ?DEBUG("Upgrade Mode: ~p~n", [UpgradeMode]),

    Res = case UpgradeMode of
        primary ->
            ?DEBUG("Primary Node upgrade schema~n", []),
            primary_upgrade_schema(Mod, AllNodes, UpgradeVersion);
        secondary ->
            ?DEBUG("Secondary Node followed copy schema~n", []),
            secondary_upgrade_schema(Mod, AllNodes)
    end,
    ?DEBUG("Upgrade Result: ~p~n", [Res]),
    case Res of
        ok ->
            ensure_backup_removed(CV),
            ok;
        {error, Reason} ->
            throw({error, {failed_to_upgrade, Reason}})
    end.

upgrade_mode(Mod, Nodes) ->
    OtherNodes = Nodes -- [node()],
    RunningNodes = nodes_running(Mod, OtherNodes),
    ?DEBUG("RunningNodes: ~p~n", [RunningNodes]),
    AfterUs = read_previously_running_nodes(),
    CV = current_version(),
    case RunningNodes of
        [] ->
            case {is_disc_node(), AfterUs} of
                {true, []}  ->
                    primary;
                {true, null} ->
                    die("Cluster upgrade needed but I am closed abnormally, "
                        "you should recover first.~nPlease rollback to the "
					    "old version ~p and upgrade later.~n", [CV]);
                {true, [_|_]}  ->
                    Filename = running_nodes_filename(),
                    die("Cluster upgrade needed but other disc nodes shut "
                        "down after this one.~nPlease first start the "
                        "disc node last shutdown.~n~nNote: if several disc "
                        "nodes were shut down simultaneously they may "
                        "all~nshow this message. In which case, remove "
                        "the lock file on one of them and~nstart that node. "
                        "The lock file on this node is:~n~n ~s ", [Filename]);
                {false, _} ->
                    die("Cluster upgrade needed but this is a ram node.~n"
                        "Please first start the last disc node to shut down.",
                        [])
            end;
        [_|_] = OtherRunningNodes ->
            ExistUpgradedNode = distribute_version_check(Mod, OtherRunningNodes, eq),
            case {ExistUpgradedNode, AfterUs} of
                {true, _} ->
                    secondary;
                {false, []} ->
                    die("Cluster upgrade needed but Secondary nodes are "
                        "running before Primary node.~nPlease stop the Secondary "
                        "nodes, and start the Primary node first.~n", []);
                {false, null} ->
                    die("Cluster upgrade needed but I am closed abnormally, "
                        "you should recover first.~nPlease rollback to the "
					    "old version ~p and upgrade later.~n", [CV]);
                {false, [_|_]} ->
                     die("Cluster upgrade needed but other nodes unupgraded are "
                         "running,~p~nbut I am a Secondary node.~n"
                         "Please close other running nodes and start Primary node "
                         "first.~n", [])
            end
    end.

apply_upgrade_version(Version, Mod) ->
    case get_version_upgrade(Version, Mod) of
        [#metastore_version{mfa = {M, F, A}}] ->
            ?DEBUG("Upgrade MFA: ~p~n", [{M, F, A}]),
            erlang:apply(M, F, A);
        [] ->
            ok;
        _Other ->
            throw({error, wrong_upgrade_function})
    end.

%% Change of node type do not affect the upgrade because:
%% Primary node must be a disc node, if you change type when it upgrade,
%% the process for upgrading will discontinue;
%% Secondary node need delete its old schema copy, and retrieve its new copy soon,
%% at that time, it will rebuild a new copy of its type.

primary_upgrade_schema(Mod, Nodes, UpgradeVersion) ->
    OtherNodes = Nodes -- [node()],
    ok = lock_file(lock_filename()),
    error_logger:info_msg("upgrades: ~w to apply~n", [length(UpgradeVersion)]),
    ensure_ok(mnesia:start(), cannot_start_mnesia),
    force_tables(Mod),
    case OtherNodes of
        [] -> ok;
        _  -> error_logger:info_msg("mnesia upgrades: Breaking cluster~n", []),
              [{atomic, ok} = del_table_copy(schema, Node) || Node <- OtherNodes]
    end,
    ?DEBUG("UpgradeVersion: ~p~n", [UpgradeVersion]),
    [apply_upgrade_version(Version, Mod) || Version <- UpgradeVersion],
    ok = check_schema(Mod),
    ok = update_version(Mod),
    ok = unlock_file(lock_filename()).

secondary_upgrade_schema(Mod, Nodes) ->
    ensure_ok(delete_schema([node()]), cannot_delete_schema),
    ensure_ok(mnesia:start(), cannot_start_mnesia),
    OtherNodes = Nodes -- [node()],
    WantDiscNode = should_be_disc_node(Nodes),
    case mnesia:change_config(extra_db_nodes, OtherNodes) of
        {ok, []} ->
            throw({error, {unable_to_join_cluster_for_secondary_upgrade, node(), Nodes, no_nodes_to_connect}});
        {ok, [_|_]} ->
            ?DEBUG("Secondary Node follow to upgrade and copy schema~n", []),
            copy_schema_internal(Mod, WantDiscNode);
        {error, Reason} ->
            throw({error, {unable_to_join_cluster_for_secondary_upgrade, node(), Nodes, Reason}})
    end.

delete_schema(Nodes) ->
    mnesia:delete_schema(Nodes).

del_table_copy(TableName, Node) ->
    mnesia:del_table_copy(TableName, Node).

check_nodes_online(Mod, Nodes, Step, Fun) ->
    ensure_mnesia_running(),
    OtherNodes = Nodes -- [node()],
    RunningNodes = lists:usort(nodes_running(Mod, OtherNodes)),
    USNodes = lists:usort(OtherNodes),
    case RunningNodes =:= USNodes of
        false ->
            throw({error, node_not_online_when, Step});
        true ->
            Fun(RunningNodes)
    end.

upgrade_schema_online(Mod, Nodes, UpgradeVersion) ->
    UpgrageFun = fun(RunningNodes) ->
        ExistUpgradedNode = distribute_version_check(Mod, RunningNodes, ge),
        case ExistUpgradedNode of
            true ->
                throw({error, other_node_version_ge_than_me});
            false ->
                ok = lock_file(lock_filename()),
                error_logger:info_msg("upgrades: ~w to apply~n", [length(UpgradeVersion)]),
                ?DEBUG("Online UpgradeVersion: ~p~n", [UpgradeVersion]),
                [apply_upgrade_version(Version, Mod) || Version <- UpgradeVersion],
                ok = check_schema(Mod),
                ok = update_version(Mod),
                ok = unlock_file(lock_filename())
        end
    end,
    check_nodes_online(Mod, Nodes, upgrade, UpgrageFun),
    ok.

backup_schema_online(Mod, Nodes, File) ->
    BackupFun = fun(RunningNodes) ->
        ensure_backup_online_taken(RunningNodes, current_version(), File)
    end,
    check_nodes_online(Mod, Nodes, backup, BackupFun),
    ok.

restore_schema_online(Mod, Nodes, File) ->
    RestoreFun = fun(RunningNodes) ->
        ensure_restore_online_taken(RunningNodes, File)
    end,
    check_nodes_online(Mod, Nodes, restore, RestoreFun),
    ok.
%% -------------------------------------------------------------------------------
%% system & mnesia operation

nodes_running(Mod, Nodes) ->
    [N || N <- Nodes, is_running(Mod, N)].

%is_running() -> is_running(node()).

is_running(Mod, Node) ->
    case rpc:call(Node, Mod, is_running, [Node]) of
        {badrpc, _} -> false;
        true        -> true;
        _           -> false
    end.

die(Msg, Args) ->
    %% We don't throw or exit here since that gets thrown
    %% straight out into do_boot, generating an erl_crash.dump
    %% and displaying any error message in a confusing way.
    error_logger:error_msg(Msg, Args),
    io:format("~n~n****~n~n" ++ Msg ++ "~n~n****~n~n~n", Args),
    error_logger:logfile(close),
    halt(1).

ensure_ok(ok, _) -> ok;
ensure_ok({error, Reason}, ErrorTag) -> throw({error, {ErrorTag, Reason}}).

mnesia_dir() -> mnesia:system_info(directory).

is_disc_node() -> mnesia:system_info(use_dir).

should_be_disc_node(Nodes) ->
    Nodes == [] orelse lists:member(node(), Nodes).

running_clustered_nodes() ->
    mnesia:system_info(running_db_nodes).

all_clustered_nodes() ->
    mnesia:system_info(db_nodes).

ensure_mnesia_dir() ->
    MnesiaDir = mnesia_dir() ++ "/",
    case filelib:ensure_dir(MnesiaDir) of
        {error, Reason} ->
            throw({error, {cannot_create_mnesia_dir, MnesiaDir, Reason}});
        ok ->
            ok
    end.

ensure_mnesia_running() ->
    case mnesia:system_info(is_running) of
        yes ->
            ok;
        starting ->
            wait_for(mnesia_running),
            ensure_mnesia_running();
        Reason when Reason =:= no; Reason =:= stopping ->
            throw({error, mnesia_not_running})
    end.

ensure_mnesia_not_running() ->
    case mnesia:system_info(is_running) of
        no ->
            ok;
        stopping ->
            wait_for(mnesia_not_running),
            ensure_mnesia_not_running();
        Reason when Reason =:= yes; Reason =:= starting ->
            throw({error, mnesia_unexpectedly_running})
    end.

wait_for(Condition) ->
    error_logger:info_msg("Waiting for ~p...~n", [Condition]),
    timer:sleep(1000).

start_mnesia() ->
    ensure_ok(mnesia:start(), cannot_start_mnesia),
    ensure_mnesia_running().

stop_mnesia() ->
    stopped = mnesia:stop(),
    ensure_mnesia_not_running().

force_tables(Mod) ->
    [mnesia:force_load_table(T) || T <- table_names(table_definitions(Mod))].

wait_for_tables(Mod) ->
    TableNames = table_names(table_definitions(Mod)),
    ?DEBUG("Wait for tables: ~p~n", [TableNames]),
    case mnesia:wait_for_tables(TableNames, ?MNESIA_WAIT_TABLES_TIMEOUT) of
        ok ->
            ok;
        {timeout, BadTabs} ->
            throw({error, {timeout_waiting_for_tables, BadTabs}});
        {error, Reason} ->
            throw({error, {failed_waiting_for_tables, Reason}})
    end.


%% -------------------------------------------------------------------------------
%% table define

table_definitions(Mod) ->
    table_definitions(Mod, true).

table_definitions(Mod, true) ->
    ?DEBUG("Mod ~p disc definitions~n", [Mod]),
    Mod:table_definitions();
table_definitions(Mod, false) ->
    ?DEBUG("Mod ~p ram definitions~n", [Mod]),
    [MetastoreTab#metastore_table{definition = table_copy_type_to_ram(TabDef)} ||
     MetastoreTab = #metastore_table{definition = TabDef}
     <- Mod:table_definitions()].

table_copy_type_to_ram(TabDef) ->
    [{disc_copies, []}, {ram_copies, [node()]}
     | proplists:delete(ram_copies, proplists:delete(disc_copies, TabDef))].

table_has_copy_type(TabDef, DiscType) ->
    lists:member(node(), proplists:get_value(DiscType, TabDef, [])).

table_names(TabDef) ->
    [TableName||#metastore_table{name = TableName} <- TabDef].

%% -------------------------------------------------------------------------------
%% backup

backup_dir(Version) ->
    mnesia_dir() ++ "-upgrade-backup-" ++  atom_to_list(Version).

ensure_backup_taken(Version) ->
    case filelib:is_file(lock_filename()) of
        false -> case filelib:is_dir(backup_dir(Version)) of
                     false -> ok = take_backup(Version);
                     _     -> ok
                 end;
        true  -> throw({error, previous_upgrade_failed})
    end.

take_backup(Version) ->
    BackupDir = backup_dir(Version),
    case file_util:recursive_copy(mnesia_dir(), BackupDir) of
        ok         -> error_logger:info_msg("upgrades: Mnesia dir backed up to ~p~n",
                           [BackupDir]);
        {error, E} -> throw({could_not_back_up_mnesia_dir, E})
    end.

ensure_backup_removed(Version) ->
    case filelib:is_dir(backup_dir(Version)) of
        true -> ok = remove_backup(Version);
        _    -> ok
    end.

remove_backup(Version) ->
    ok = file_util:recursive_del([backup_dir(Version)]),
    error_logger:info_msg("upgrades: Mnesia backup removed~n", []).

backup_online_file(Version) ->
    {ok, PWD} = file:get_cwd(),
    PWD ++ "/MnesiaBackupOnline-upgrade-backup-" ++  atom_to_list(Version).

predo_online(Nodes) ->
    mnesia:stop(),
    mnesia:delete_schema([node()]),
    mnesia:start(),
    mnesia:change_config(extra_db_nodes, Nodes).

ensure_backup_online_taken(Nodes, Version, RFile) ->
    File = case RFile of
        undefined -> backup_online_file(Version);
        _ -> RFile
    end,
    predo_online(Nodes),
    do_backup_online(File),
    ok.

do_backup_online(File) ->
    mnesia:change_table_copy_type(schema, node(), disc_copies),
    mnesia:wait_for_tables(mnesia:system_info(tables), infinity),
    [mnesia:change_table_copy_type(Table, node(), disc_copies)
     || Table <- mnesia:system_info(tables), Table =/= schema],
    mnesia:backup(File).

ensure_restore_online_taken(Nodes, File) ->
    predo_online(Nodes),
    do_restore_online(File),
    ok.

do_restore_online(File) ->
    mnesia:restore(File, [{default_op, clear_tables}]).
%% -------------------------------------------------------------------------------
%% version schema file & upgrade record operation

schema_filename() ->
    filename:join(mnesia_dir(), ?VERSION_FILENAME).

update_version(Mod) ->
    Version = Mod:current_versions(),
    case file_util:write_file_term(schema_filename(), Version) of
        ok -> ok;
        {error, Reason} -> throw({cannot_write_current_version, Reason})
    end.

remove_version() ->
    file:delete(schema_filename()).

read_version() ->
    case file_util:read_file_term(schema_filename()) of
        {ok, CV} -> {ok, CV};
        {error, enoent} -> {error, enoent};
        {error, Reason} -> throw({cannot_read_current_version, Reason})
    end.

upgrades_required(Mod) ->
    NewVersions = Mod:current_versions(),
    case read_version() of
        {error, enoent} -> {error, schema_version_not_exist};
        {ok, CurrentVersions} ->
            ?DEBUG("CurrentVersions: ~p, NewVersions: ~p~n", [CurrentVersions, NewVersions]),
            case lists:prefix(CurrentVersions, NewVersions) of
                true ->
                    {ok, lists:sublist(NewVersions, length(CurrentVersions) + 1,
                                  length(NewVersions) - length(CurrentVersions))};
                false ->
                    {error, version_not_available}
            end
    end.

parse_versions(Versions) ->
    case Versions of
        [] -> null;
        [V] -> V;
        [_|_] = VL -> [V] = tl(VL), V
    end.

distribute_version_check(Mod, Nodes, Condition) ->
    {RemoteVersions, _} = rpc:multicall(Nodes, metastore_mnesia, current_version, [], ?RPC_TIMEOUT),
    case [ RemoteVersion || RemoteVersion <- RemoteVersions,
                            remote_version_compare_desired(Mod, RemoteVersion, Condition)] of
        [] -> false;
        _ -> true
    end.

remote_version_compare_desired(Mod, RV, eq) ->
    DV = desired_version(Mod),
    RV =:= DV;
remote_version_compare_desired(Mod, RV, ge) ->
    DVList = Mod:current_versions(),
    (RV =:= parse_versions(DVList)) orelse not lists:member(RV, DVList);
remote_version_compare_desired(_Mod, _RV, _Condition) ->
    false.

desired_version(Mod) ->
    parse_versions(Mod:current_versions()).

current_version() ->
    case read_version() of
        {ok, CV} ->
            parse_versions(CV);
        {error, enoent} ->
            null
    end.

get_version_upgrade(Version, Mod) ->
    lists:filter(fun(#metastore_version{version = V}) ->
                     Version =:= V
                 end, Mod:table_upgrade()).

%% -------------------------------------------------------------------------------
%% upgrade lock file

lock_filename() ->
    filename:join(mnesia_dir(), ?UPGRADE_LOCK_FILENAME).

lock_file(Path) ->
    case filelib:is_file(Path) of
        true  -> {error, eexist};
        false -> 
            {ok, Lock} = file:open(Path, [raw, write]),
            ok = file:close(Lock)
    end.

unlock_file(Path) ->
    file:delete(Path).

%% -------------------------------------------------------------------------------
%% running nodes file

running_nodes_filename() ->
    filename:join(mnesia_dir(), ?RUNNING_NODES_FILENAME).

record_running_nodes() ->
    FileName = running_nodes_filename(),
    Nodes = running_clustered_nodes() -- [node()],
    %% Don't check the result: we're shutting down anyway and this is
    %% a best-effort-basis.
    file_util:write_file_term(FileName, Nodes),
    ok.

read_previously_running_nodes() ->
    FileName = running_nodes_filename(),
    case file_util:read_file_term(FileName) of
        {ok, Nodes}   -> Nodes;
        {error, enoent} -> null;
        {error, Reason} -> throw({error, {cannot_read_previous_nodes_file, FileName, Reason}})
    end.

delete_previously_running_nodes() ->
    FileName = running_nodes_filename(),
    case file:delete(FileName) of
        ok              -> ok;
        {error, enoent} -> ok;
        {error, Reason} -> throw({error, {cannot_delete_previous_nodes_file,
                                          FileName, Reason}})
    end.

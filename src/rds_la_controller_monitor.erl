%%% -------------------------------------------------------------------
%%% Author  : haoting.wq
%%% Description :
%%%
%%% Created : 2012-6-19
%%% -------------------------------------------------------------------
-module(rds_la_controller_monitor).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("logger_header.hrl").
%% --------------------------------------------------------------------
%% External exports
-export([start_link/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%% ====================================================================
%% External functions
%% ====================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    cast(stop).
%% ====================================================================
%% Server functions
%% ====================================================================
cast(Msg) ->
    gen_server:cast(?SERVER, Msg).

init([]) ->
    rds_la_metastore:subscribe_route_event(),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% process route event

handle_info({mnesia_table_event, Event}, State) ->
    handle_mnesia_table_event(Event),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    rds_la_metastore:unsubscribe_route_event(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------

handle_mnesia_table_event({write, NewRecord, _ActivityId}) ->
    User = rds_la_metastore:route_record_to_user(NewRecord),
    ?DEBUG("User ~p route info changed", [User]),
    notify_controller_servers(User);

handle_mnesia_table_event({delete_object, OldRecord, _ActivityId}) ->
    User = rds_la_metastore:route_record_to_user(OldRecord),
    ?DEBUG("User ~p route info deleted", [User]),
    notify_controller_servers(User);

handle_mnesia_table_event({delete, {_Tab, Key}, _ActivityId}) ->
    User = rds_la_metastore:route_key_to_user(Key),
    ?DEBUG("User ~p route info total deleted", [User]),
    notify_controller_servers(User);

handle_mnesia_table_event(_Event) ->
    ok.

notify_controller_servers(User) ->
    case catch rds_la_network_sup:controller_servers() of
        ControllerServerList when is_list(ControllerServerList) ->
            ?DEBUG("User ~p, Will notify ~p~n", [User, ControllerServerList]),
            notify_controller_servers(ControllerServerList, User);
        _ -> ok
    end.

notify_controller_servers([], _User) ->
    ok;
notify_controller_servers([ControllerServer|ControllerServerList], User) ->
    notify_controller_server(ControllerServer, User),
    notify_controller_servers(ControllerServerList, User).

notify_controller_server({_ID, Child, _Type, _Module}, User) ->
    rds_la_controller_protocol:update_user_nodes(Child, User).

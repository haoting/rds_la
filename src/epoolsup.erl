%%% -------------------------------------------------------------------
%%% Author  : haoting.wq
%%% Description :
%%%
%%% Created : 2012-6-12
%%% -------------------------------------------------------------------
-module(epoolsup).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("logger_header.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/2, stop/1]).
-export([add_child/2, add_child/3, del_child/2, del_child_pid/2, get_child/2, get_key/2, all_child/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {name, child_spec}).

-define(SERVER, ?MODULE).
%% ====================================================================
%% External functions
%% ====================================================================
%% ChildSpec:
%% {Module, StartFun, StartArg, StopFun}
start_link(Name, ChildSpec) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, ChildSpec], []).

stop(Name) ->
    cast(Name, stop).

add_child(Name, Key) ->
    add_child(Name, Key, []).

add_child(Name, Key, Arg) ->
    call(Name, {add_child, Key, Arg}).

del_child(Name, Key) ->
    call(Name, {del_child, Key}).

del_child_pid(Name, Pid) when is_pid(Pid) ->
    call(Name, {del_child_pid, Pid}).

get_child(Name, Key) ->
    get_ets(Name, Key).

get_key(Name, Pid) when is_pid(Pid) ->
    get_ets_key(Name, Pid).

all_child(Name) ->
    all_ets(Name).
%% ====================================================================
%% Server functions
%% ====================================================================

call(Name, Msg) ->
	gen_server:call(Name, Msg, infinity).

cast(Name, Msg) ->
	gen_server:cast(Name, Msg).

init([Name, ChildSpec]) ->
    process_flag(trap_exit, true),
    init_ets(Name),
    {ok, #state{name = Name, child_spec = ChildSpec}}.

handle_call({add_child, Key, Arg}, _From, State = #state{
    name = Name, child_spec = {Module, StartFun, StartArg, _StopFun}}) ->
    Args = StartArg ++ Arg,
    case do_start_child_i(Module, StartFun, Args) of
        {ok, undefined} ->
	        {reply, {ok, undefined}, State};
	    {ok, Pid} ->
	        add_ets(Name, Key, Pid),
	        {reply, {ok, Pid}, State};
	    {ok, Pid, Extra} ->
	        add_ets(Name, Key, Pid),
	        {reply, {ok, Pid, Extra}, State};
	    What ->
	        {reply, What, State}
    end;

handle_call({del_child, Key}, _From, State = #state{
    name = Name, child_spec = {Module, _StartFun, _StartArg, StopFun}}) ->
    case get_ets(Name, Key) of
        [] ->
            {reply, ok, State};
        Pids ->
            %?DEBUG("Sup ~p delete child ~p", [Name, Pids]),
            [begin
                 do_stop_child_i(Module, StopFun, Pid)
			 end || Pid <- Pids],
            del_ets(Name, Key),
            {reply, ok, State}
    end;

handle_call({del_child_pid, Pid}, _From, State = #state{
    name = Name, child_spec = {Module, _StartFun, _StartArg, StopFun}}) ->
    case get_ets_key(Name, Pid) of
        [] ->
            {reply, ok, State};
        _ ->
            %?DEBUG("Sup ~p delete child ~p", [Name, Pid]),
            do_stop_child_i(Module, StopFun, Pid),
            del_ets_pid(Name, Pid),
            {reply, ok, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, #state{name = Name} = State) ->
    del_ets_pid(Name, Pid),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State = #state{name = Name}) ->
    fini_ets(Name),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------
do_start_child_i(M, F, A) ->
    case catch apply(M, F, A) of
	    {ok, Pid} when is_pid(Pid) ->
	        {ok, Pid};
	    {ok, Pid, Extra} when is_pid(Pid) ->
	        {ok, Pid, Extra};
    	ignore ->
	        {ok, undefined};
	    {error, Error} ->
	        {error, Error};
	    What ->
	        {error, What}
    end.

do_stop_child_i(M, F, Pid) ->
	catch apply(M, F, [Pid]),
    case catch erlang:process_info(Pid) of
        undefined -> ok;
        {'EXIT', _} -> ok;
        _ -> do_stop_child_i(M, F, Pid)
    end.

init_ets(TableName) ->
	case ets:info(TableName) of
		undefined -> ets:new(TableName, [named_table, public, {read_concurrency, true}]);
		_ -> TableName
	end.

fini_ets(TableName) ->
    ets:delete(TableName).

add_ets(TableName, Key, Pid) ->
	ets:insert_new(TableName, {Key, Pid}).

del_ets_pid(TableName, Pid) ->
    ets:match_delete(TableName, {'_', Pid}).

del_ets(TableName, Key) ->
	ets:delete(TableName, Key).

get_ets(TableName, Key) ->
	ets:lookup(TableName, Key).

get_ets_key(TableName, Pid) ->
    ets:match(TableName, {'$1', Pid}).

all_ets(TableName) ->
    ets:match(TableName, '$1').
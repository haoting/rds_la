%% Author: haoting.wq
%% Created: 2012-6-11
%% Description: TODO: Add description to rds_la_handler
-module(rds_la_handler).

-behavior(gen_server).

-include("logger_header.hrl").
-include("rds_la_log.hrl").

-export([start_link/4, stop/1, wait_for_start/1]).
-export([append_sync/4, append_async/4]).
-export([query_sync/2, query_async/3, query_sync/3, query_async/4, merge_query_results/3]).
-export([init/1, handle_call/3, handle_cast/2, 
         handle_info/2, terminate/2, code_change/3]).

-record(proxy, {writer, indexers}).
-record(state, {initialized = false, dir, name, indexmodules, proxylist, proxys}).

%%% Called by Supervisor

start_link(Dir, Name, IndexModules, ProxyList) ->
    gen_server:start_link(?MODULE, [Dir, Name, IndexModules, ProxyList], []).

stop(Pid) ->
    cast(Pid, stop).

wait_for_start(Pid) ->
    case call(Pid, initialized) of
        true -> ok;
        false -> wait_for_start(Pid)
    end.

%%% publics

append_async(Pid, ProxyId, TimeStamp, Term) ->
    ?DEBUG("Writing", []),
    case get_proxy_writer(Pid, ProxyId) of
        undefined ->
            ?DEBUG("No Proxy", []),
            cast(Pid, {append, ProxyId, TimeStamp, Term});
        {TLog, Indexers} ->
            rds_la_tlog:write_async(TLog, TimeStamp, Term),
            [rds_la_gen_indexer:add_item(Instance, TimeStamp, Term) || {_Module, Instance} <- Indexers]
    end.

append_sync(Pid, ProxyId, TimeStamp, Term) ->
    call(Pid, {append, ProxyId, TimeStamp, Term}).

get_proxy_writer(Pid, ProxyId) ->
    case get({Pid, ProxyId}) of
        undefined ->
            case call(Pid, {get_proxy_writer, ProxyId}) of
                undefined -> undefined;
                TLog ->
                    ?DEBUG("Get Writer from LA Handler", []),
                    put({Pid, ProxyId}, TLog),
                    TLog
            end;
        TLog -> ?DEBUG("Get Writer from Local Cache", []), TLog
    end.

query_sync(Pid, Condition) ->
    call(Pid, {'query', Condition}).

query_sync(Pid, IndexModule, Condition) ->
    call(Pid, {'query', IndexModule, Condition}).

query_async(Pid, From, Condition) ->
    cast(Pid, {'query', From, Condition}).

query_async(Pid, From, IndexModule, Condition) ->
    cast(Pid, {'query', From, IndexModule, Condition}).

merge_query_results(IndexModule, ResultList, Condition) ->
    rds_la_gen_indexer:merge_results(IndexModule, ResultList, Condition).
%%% Behavioral Interface

call(Pid, Msg) -> gen_server:call(Pid, Msg, infinity).

cast(Pid, Msg) -> gen_server:cast(Pid, Msg).

init([Dir, Name, IndexModules, ProxyList]) ->
    process_flag(trap_exit, true),
    gen_server:cast(self(), init),
    {ok, #state{dir = Dir, name = Name,
                indexmodules = IndexModules,
                proxylist = ProxyList,
                proxys = dict:new()}}.

handle_cast(init, State) ->
    gen_server:cast(self(), wait_indexers),
    {noreply, init_proxys(State)}; 

handle_cast(wait_indexers, State) ->
    NState = wait_proxys(State),
    {noreply, NState#state{initialized = true}};

handle_cast({'query', From, Condition}, State) ->
    handle_query(State, From, Condition);
    
handle_cast({'query', From, IndexModule, Condition}, State) ->
    handle_query(State, From, IndexModule, Condition);

handle_cast({append, ProxyId, TimeStamp, Term}, State) ->
    case dict:is_key(ProxyId, State#state.proxys) of
        true ->
            #proxy{writer = TLog, indexers = Indexers} =
                      dict:fetch(ProxyId, State#state.proxys),
            rds_la_tlog:write_async(TLog, TimeStamp, Term),
            [rds_la_gen_indexer:add_item(Instance, TimeStamp, Term)
            || {_Module, Instance} <- Indexers],
            {noreply, State};
        false ->
            NState = init_proxys(State, [ProxyId]),
            case dict:is_key(ProxyId, NState#state.proxys) of
                true ->
                    #proxy{writer = TLog, indexers = Indexers} =
                              dict:fetch(ProxyId, NState#state.proxys),
                    rds_la_tlog:write_async(TLog, TimeStamp, Term),
                    [rds_la_gen_indexer:add_item(Instance, TimeStamp, Term)
                    || {_Module, Instance} <- Indexers],
                    {noreply, NState};
                false ->
                    {noreply, NState}
            end
    end;

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Event, State) ->
    {noreply, State}.

handle_call(initialized, _From, State = #state{ initialized = Initialized}) ->
    {reply, Initialized, State};

handle_call({get_proxy_writer, ProxyId}, _From, State = #state{ proxys = ProxyDict}) ->
    Ret = case dict:find(ProxyId, ProxyDict) of
        error -> undefined;
        {ok, #proxy{writer = TLog, indexers = Indexers}} ->
            ?DEBUG("TLog: ~p", [TLog]),
            {TLog, Indexers}
    end,
    ?DEBUG("Ret: ~p", [Ret]),
    {reply, Ret, State};

handle_call({append, ProxyId, TimeStamp, Term}, _From, State) ->
    case dict:is_key(ProxyId, State#state.proxys) of
        true ->
            #proxy{writer = TLog, indexers = Indexers} =
                      dict:fetch(ProxyId, State#state.proxys),
            rds_la_tlog:write_sync(TLog, TimeStamp, Term),
            ?DEBUG("Indexers: ~p", [Indexers]),
            [rds_la_gen_indexer:add_item(Instance, TimeStamp, Term)
            || {_Module, Instance} <- Indexers],
            {reply, ok, State};
        false ->
            NState = init_proxys(State, [ProxyId]),
            case dict:is_key(ProxyId, NState#state.proxys) of
                true ->
                    #proxy{writer = TLog, indexers = Indexers} =
                              dict:fetch(ProxyId, NState#state.proxys),
                    rds_la_tlog:write_sync(TLog, TimeStamp, Term),
                    [rds_la_gen_indexer:add_item(Instance, TimeStamp, Term)
                    || {_Module, Instance} <- Indexers],
                    {reply, ok, NState};
                false ->
                    {reply, ok, NState}
            end
    end;

handle_call({'query', Condition}, From, State) ->
    handle_query(State, From, Condition);

handle_call({'query', IndexModule, Condition}, From, State) ->
    handle_query(State, From, IndexModule, Condition);

handle_call(_Event, _From, State)  ->
    {reply, ok, State}.

handle_info({'EXIT', Pid, Reason}, State) ->
    IndexerMap =
    lists:foldl(
      fun({ProxyId, Proxy}, Acc) ->
              lists:foldl(
                fun({Module, Indexer}, Acc1) ->
                        dict:store(Indexer, {ProxyId, Module}, Acc1)
                end,
                Acc,
                Proxy#proxy.indexers)
      end,
      dict:new(),
      dict:to_list(State#state.proxys)),
    case dict:find(Pid, IndexerMap) of
        {ok, {ProxyId, Module}} ->
            ?ERROR("Indexer type ~p for proxy ~p dies reason ~p", [Module, ProxyId, Reason]),
            Proxy = dict:fetch(ProxyId, State#state.proxys),
            {noreply,
             State#state{proxys = 
                dict:update(ProxyId, Proxy#proxy{indexers = 
                    proplists:delete(Module, Proxy#proxy.indexers)}, 
                        State#state.proxys)}};
        error ->
            ?ERROR("unknown Pid ~p dies ~p", [Pid, Reason]),
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %?DEBUG("rds_la_handler stop for reason: ~p", [Reason]),
    fini_proxys(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% internals

fullname(Name, ProxyId) ->
    Name ++ "_" ++ ProxyId.

init_proxys(State) ->
    init_proxys(State, State#state.proxylist).

init_proxys(State, []) -> State;
init_proxys(State, [ProxyId|Rest]) ->
    State1 = 
    case init_proxy(State, ProxyId) of
        {ok, Proxy} ->
            State#state{proxys=dict:store(ProxyId, Proxy, State#state.proxys)};
        {error, _} ->
            State
    end,
    init_proxys(State1, Rest).
    
init_proxy(State, ProxyId) ->
    #state{dir = Dir, name = Name, indexmodules = IndexModules} = State,
    FullName = fullname(Name, ProxyId),
    case rds_la_epoolsup:add_tlog(Dir, FullName, []) of
        {ok, TLog} ->
            Indexers = [begin
                            {ok, Instance} = rds_la_epoolsup:add_gen_indexer(Module, Dir, FullName, []),
                            {Module, Instance}
                        end
                       || Module <- IndexModules],
            {ok, #proxy{writer = TLog, indexers = Indexers}};
        Error ->
            {error, Error}
    end.

wait_proxys(State) ->
    wait_proxys(State, State#state.proxylist).

wait_proxys(State, []) -> State;
wait_proxys(State, [ProxyId|Rest]) ->
    State1 = 
    case dict:is_key(ProxyId, State#state.proxys) of
        true ->
            Proxy = dict:fetch(ProxyId, State#state.proxys),
            Indexers1 = 
            [ {Module1, Instance1}
            || {ok, Module1, Instance1} <- 
                   [ {rds_la_gen_indexer:wait_until_opened(Instance), Module, Instance}
                   || {Module, Instance} <- Proxy#proxy.indexers]],
            Proxy1 = Proxy#proxy{indexers = Indexers1},
            State#state{proxys = dict:store(ProxyId,Proxy1,State#state.proxys)};
        false -> State
    end,
    wait_proxys(State1, Rest).

fini_proxys(#state{proxys = Proxys}) ->
    fini_proxy(dict:to_list(Proxys)).

fini_proxy([]) -> ok;
fini_proxy([{_ProxyId, Proxy}|Proxys]) ->
    #proxy{writer = TLog, indexers = Indexers} = Proxy,
    case TLog of
        undefined ->
            ok;
        _ ->
            [ _ = rds_la_epoolsup:del_gen_indexer(Instance)
            || {_Module, Instance} <- Indexers],
			%?DEBUG("rds_la_handler stop tlog ~p", [TLog]),
            _ = rds_la_epoolsup:del_tlog(TLog),
            ok
    end,
    fini_proxy(Proxys).

handle_query(State, From, IndexModule, Condition) ->
    AllInstances = lists:reverse(lists:foldl(
        fun({_ProxyId, Proxy}, Acc) ->
            Indexers = Proxy#proxy.indexers,
			case proplists:get_value(IndexModule, Indexers) of
				undefined -> Acc;
				Instance -> [Instance|Acc]
		    end
        end, [], dict:to_list(State#state.proxys))),
    rds_la_query_worker:start_link(From,
        fun() ->
            rds_la_gen_indexer:query_indexes(IndexModule, AllInstances, Condition)
        end),
    {noreply, State}.

handle_query(State, From, Condition)->
    rds_la_query_worker:start_link(From,
        fun() -> perform_query(State, Condition) end),
    {noreply, State}.

-define(DEFAULT_PAGE_SIZE, 100).

perform_query(#state{dir = Dir, name = Name, proxys = Proxys}, Condition) ->
    DateStart = proplists:get_value(date_start, Condition),
    DateEnd = proplists:get_value(date_end, Condition),
    PageStart = proplists:get_value(page_start, Condition, 0),
    PageSize = proplists:get_value(page_size, Condition, ?DEFAULT_PAGE_SIZE),
    AllReaders = lists:reverse(lists:foldl(
        fun({ProxyId, Proxy}, Acc) ->
            TLog = Proxy#proxy.writer,
			case TLog of
				undefined -> Acc;
				_ ->
                    FullName = fullname(Name, ProxyId),
                    [{TLog, FullName}|Acc]
		    end
        end, [], dict:to_list(Proxys))),
    ReaderHeap =
        lists:foldl(
          fun({_TLog, FullName}, Acc) ->
            {ok, Reader} = rds_la_tlog:open_reader(Dir, FullName),
            case rds_la_tlog:find(Reader, DateStart) of
                true ->
                    {ok, {TimeStamp, Term}} = rds_la_tlog:next(Reader),
                    if TimeStamp =< DateEnd ->
                           case gb_trees:lookup(TimeStamp, Acc) of
                               none ->
                                   gb_trees:insert(TimeStamp, [{Reader,Term}], Acc);
                               {value, ExistedReaders} ->
                                   gb_trees:update(TimeStamp, [{Reader,Term}|ExistedReaders], Acc)
                           end;
                       true ->
                           rds_la_tlog:close_reader(Reader),
                           Acc
                    end;
                false ->
                    rds_la_tlog:close_reader(Reader),
                    Acc
            end
          end, gb_trees:empty(), AllReaders),
    lists:reverse(perform_query(ReaderHeap, DateEnd, PageStart, PageSize, [])).

perform_query(ReaderHeap, _End, _PageStart, 0, Acc) ->
    [ rds_la_tlog:close_reader(Reader) || {Reader, _} <- gb_trees:values(ReaderHeap)],
    Acc;
perform_query(ReaderHeap, End, PageStart, PageSize, Acc) ->
    case gb_trees:is_empty(ReaderHeap) of
        true -> Acc;
        false ->
            {TimeStamp, [{Reader,Term}|Rest]} = gb_trees:smallest(ReaderHeap),
            ReaderHeap1 = 
            case rds_la_tlog:next(Reader) of
                {ok, {TimeStamp1, Term1}} when TimeStamp1 =:= TimeStamp ->
                    gb_trees:update(TimeStamp, [{Reader,Term1}|Rest]);
                {ok, {TimeStamp1, Term1}} ->
                    ReaderHeapTemp = 
                        if Rest =:= [] ->
                               gb_trees:delete(TimeStamp, ReaderHeap);
                           true ->
                               gb_trees:update(TimeStamp, Rest, ReaderHeap)
                        end,
                    if TimeStamp1 > End ->
                           rds_la_tlog:close_reader(Reader),
                           ReaderHeapTemp;
                       true ->
                           case gb_trees:lookup(TimeStamp1, ReaderHeapTemp) of
                               none ->
                                   gb_trees:insert(TimeStamp1,
                                                   [{Reader, Term1}],
                                                   ReaderHeapTemp);
                               {value, ExistedReaders} ->
                                   gb_trees:update(TimeStamp1,
                                                   [{Reader, Term1}|ExistedReaders],
                                                   ReaderHeapTemp)
                           end
                    end;
                _ ->
                    rds_la_tlog:close_reader(Reader),
                    gb_trees:update(TimeStamp, Rest, ReaderHeap)
            end,
            if PageStart > 1 ->
                   perform_query(ReaderHeap1, End, PageStart-1, PageSize, Acc);
               true ->
                   perform_query(ReaderHeap1, End, 1, PageSize -1, [{TimeStamp, Term}|Acc])
            end
    end.

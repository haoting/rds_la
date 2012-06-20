%% Author: haoting.wq
%% Created: 2012-6-11
%% Description: TODO: Add description to rds_la_gen_indexer
-module(rds_la_gen_indexer).

-export([start_link/4, stop/1, wait_until_opened/1]).
-export([add_item/3, query_index/4, query_indexes/3]).
-export([merge_results/3]).
-export([behaviour_info/1]).
-export([init/1, handle_call/3, handle_cast/2, 
         handle_info/2, terminate/2, code_change/3]).

-include("logger_header.hrl").
-include("rds_la_tlog.hrl").
-include("rds_la_log.hrl").

-record(state, {module, dir, chunksize,
                ds_name, index_name, index_writer,
                rtindex, rtindex_start, rtindex_end, 
                rtindex_items}).

-record(chunk, {start_timestamp, end_timestamp, index_data}).

-define(DEFAULT_CHUNK_SIZE, 10000). % 10k

%%% Publics

start_link(Module, Dir, DataSourceName, Opts) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [Module, Dir, DataSourceName, Opts], []),
    %ok = wait_until_opened(Pid),
    {ok, Pid}.

wait_until_opened(Pid) ->
    call(Pid, wait_until_opened).

stop(Pid) ->
    cast(Pid, stop).

add_item(Pid, TimeStamp, Term) ->
    cast(Pid, {add_item, TimeStamp, Term}).

get_index(Pid, Start, End) ->
    call(Pid, {get_index, Start, End}).

query_index(Pid, Start, End, QueryOpts) ->
    call(Pid, {query_index, Start, End, QueryOpts}).

query_indexes(IndexModule, AllInstances, Condition) ->
    DateStart = proplists:get_value(date_start, Condition),
    DateEnd = proplists:get_value(date_end, Condition),
	Indexes = lists:foldl(
        fun(Instance, Merged) ->
            {ok, Index} = get_index(Instance, DateStart, DateEnd),
			IndexModule:merge_index(Index, Merged)
		end, IndexModule:init_index(), AllInstances),
    IndexModule:query_index(Indexes, Condition).

merge_results(Module, ResultList, Condition) when is_list(ResultList) ->
    Res = merge_result_list(Module, ResultList, Module:init_index()),
    Module:query_index(Res, Condition).

merge_result_list(_Module, [], Acc) -> Acc;
merge_result_list(Module, [Result|ResultList], Acc) ->
    NAcc = Module:merge_index(Module:from_result(Result), Acc),
    merge_result_list(Module, ResultList, NAcc).

%%% client side helpers

call(Pid, Msg) ->
    gen_server:call(Pid, Msg, infinity).

cast(Pid, Msg) ->
    gen_server:cast(Pid, Msg).

%%% Define custom behavior

behaviour_info(callbacks) ->
    [
     {name,0},
     {init_index, 0},
     {add_to_index, 2},
     {merge_index, 2},
     {query_index, 2},
     {from_result, 1}
    ];
behaviour_info(_Other) ->
    undefined.

%%% Behavioral Interface

init([Module, Dir, DataSourceName, Opts]) ->
    IndexName = DataSourceName ++ "_" ++ Module:name(),
    ChunkSize = proplists:get_value(chunksize, Opts, ?DEFAULT_CHUNK_SIZE),

    gen_server:cast(self(), init),
    
    {ok, #state{module = Module, dir = Dir, chunksize = ChunkSize,
                ds_name = DataSourceName, index_name = IndexName,
                rtindex = Module:init_index(), rtindex_items = 0}}.

handle_cast({add_item, TimeStamp, Term}, State) ->
    ?INFO("Add for ~p", [State#state.module]),
    {noreply, add_itemx(TimeStamp, Term, State)};

handle_cast(init, State) ->
    #state{dir = Dir, index_name = IndexName} = State,
    case rds_la_epoolsup:add_tlog(Dir, IndexName, []) of
        {ok, TLog} ->
            {noreply, catchup(State#state{index_writer = TLog})};
        Error ->
            {stop, Error, State}
    end;

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Event, State) ->
    {noreply, State}.

handle_call(wait_until_opened, _From, State) ->
    {reply, ok, State};

handle_call({get_index, Start, End}, From, State) ->
    rds_la_query_worker:start_link(From,
        fun() ->
            Result = get_indexx(State, Start, End),
            Result#chunk.index_data
        end),
    {noreply, State};

handle_call({query_index, Start, End, QueryOpts}, From, State) ->
    rds_la_query_worker:start_link(From,
        fun() ->
            Module = State#state.module,
            Result = get_indexx(State, Start, End),
%            io:format("Result: ~p~n", [Result]),
            Module:query_index(Result#chunk.index_data, QueryOpts)
        end),
    {noreply, State};

handle_call(_Event, _From, State) ->
    {reply, ok, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{index_writer = TLog}) ->
    %?DEBUG("rds_la_gen_indexer stop for reason: ~p", [Reason]),
    case TLog of
       undefined -> ok;
       _ ->
           %?DEBUG("rds_la_gen_indexer stop tlog ~p", [TLog]),
           rds_la_epoolsup:del_tlog(TLog)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internals

catchup(State) ->
    #state{module = Module, dir = Dir, index_name = IndexName} = State,
    {ok, IndexReader} = rds_la_tlog:open_reader(Dir, IndexName),
    NextIndexPoint =
        case rds_la_tlog:last(IndexReader) of
            ?TERMID_NIL -> 
                -1;
            TermId ->
                % TODO: if rds_la_tlog down for some reason, we must adjust it by some method
                case rds_la_tlog:find(IndexReader, TermId) of
                    true -> ok;
                    Error ->
                        ?ERROR("Error ~p when catchup for module ~p~n    dir: ~p index_name: ~p, termId: ~p~n",
                        [Error, Module, Dir, IndexName, TermId])
                end,
                {ok, {_, Chunk}} = rds_la_tlog:next(IndexReader),
                Chunk#chunk.end_timestamp + 1
        end,
    ok = rds_la_tlog:close_reader(IndexReader),
    catchup(State#state{rtindex_start = NextIndexPoint,
                        rtindex_end = NextIndexPoint}, NextIndexPoint).

catchup(State, Begin) ->
    #state{dir = Dir, ds_name = DsName} = State,
    %%% TODO:fix bug here, avoid repeat indexing
    {ok, DataSource} = rds_la_tlog:open_reader(Dir, DsName),
    State1 = 
        case rds_la_tlog:last(DataSource) of
            ?TERMID_NIL -> 
                State;
            TermId ->
                ?INFO("~p ~p last input is ~p, start catchup from ~p", [Dir, DsName, TermId#termid.timestamp, Begin]),
                if TermId#termid.timestamp < Begin ->
                       State;
                   true ->
                       true = rds_la_tlog:find(DataSource, Begin),
                       catchup1(State, DataSource)
                end
        end,
    ok = rds_la_tlog:close_reader(DataSource),
    State1.

catchup1(State, DataSource) ->
    case rds_la_tlog:next(DataSource) of
        {ok, {TimeStamp, Term}} ->
            catchup1(add_itemx(TimeStamp, Term, State), DataSource);
        eof ->
            State;
        {error, _} ->
            State
    end.

add_itemx(TimeStamp, Term, State) ->
    #state{module = Module,
           chunksize = ChunkSize,
           index_writer = TLog,
           rtindex = RtIndex,
           rtindex_start = RtIndexStart,
           rtindex_end = RtIndexEnd,
           rtindex_items = RtIndexItems} = State,
    State1 = 
    if RtIndexStart =:= -1 ->
           State#state{rtindex_start = TimeStamp};
      RtIndexItems >= ChunkSize
       andalso TimeStamp > RtIndexEnd ->
           rds_la_tlog:write_async(TLog, RtIndexStart,
                          #chunk{start_timestamp = RtIndexStart,
                                 end_timestamp = RtIndexEnd,
                                 index_data = RtIndex}),
           State#state{rtindex = Module:init_index(),
                       rtindex_start = TimeStamp,
                       rtindex_items = 0};
       true ->
           State
    end,
    State1#state{rtindex = Module:add_to_index(State1#state.rtindex, Term),
                 rtindex_end = TimeStamp,
                 rtindex_items = State1#state.rtindex_items + 1}.

-define(DEFAULT_PAGE_SIZE, 100).

get_indexx(State, Start, End) ->
    #state{module = Module,
           rtindex = RtIndex,
           rtindex_start = RtIndexStart,
           rtindex_end = RtIndexEnd} = State,
    if Start =:= RtIndexStart andalso End >= RtIndexEnd ->
           #chunk{start_timestamp = RtIndexStart,
                  end_timestamp = End,
                  index_data = RtIndex};
       Start < RtIndexStart andalso End >= RtIndexEnd ->
           merge(Module,
                 get_index_in_disc(State, Start, RtIndexStart-1),
                 #chunk{start_timestamp = RtIndexStart,
                        end_timestamp = End,
                        index_data = RtIndex});
       true ->
           get_index_in_disc(State, Start, End)
    end.

get_index_in_disc(State, Start, End) ->
    Module = State#state.module,
    IndexedResult = get_index_in_index(State, Start, End),
    #chunk{start_timestamp = Start1,
           end_timestamp = End1} = IndexedResult,
    if Start1 =:= End1 ->
           get_index_in_ds(State, Start, End);
       true ->
           if Start < Start1 andalso End1 < End ->
                  merge(Module,
                        get_index_in_ds(State, Start, Start1-1),
                        IndexedResult,
                        get_index_in_ds(State, End1+1, End));
              Start < Start1 ->
                  merge(Module,
                        get_index_in_ds(State, Start, Start1-1),
                        IndexedResult);
              End1 < End ->
                  merge(Module,
                        IndexedResult,
                        get_index_in_ds(State, End1+1, End));
              true ->
                  IndexedResult
           end
    end.

get_index_in_index(State, Start, End) ->
    #state{module = Module, dir = Dir,
           index_name = IndexName} = State,
    {ok, IndexReader} = rds_la_tlog:open_reader(Dir, IndexName),
    Result = 
        case rds_la_tlog:find(IndexReader, Start) of
            true ->
                {ok, {_, Chunk}} = rds_la_tlog:next(IndexReader),
                if Chunk#chunk.start_timestamp =< End andalso 
                                   Chunk#chunk.end_timestamp =< End ->
                    get_index_in_index1(Module, IndexReader, End, Chunk);
                   true ->
                        #chunk{start_timestamp = Start,
                               end_timestamp = Start,
                               index_data = Module:init_index()}
                end;
            false ->
                #chunk{start_timestamp = Start,
                       end_timestamp = Start,
                       index_data = Module:init_index()};
            _ ->
                #chunk{start_timestamp = Start,
                       end_timestamp = Start,
                       index_data = Module:init_index()}
        end,
    ok = rds_la_tlog:close_reader(IndexReader),
    Result.

get_index_in_index1(Module, IndexReader, End, Acc) ->
    case rds_la_tlog:next(IndexReader) of
        {ok, {_, Chunk}} when Chunk#chunk.start_timestamp =< End andalso
                                            Chunk#chunk.end_timestamp =< End ->
            get_index_in_index1(
              Module, IndexReader, End,
              merge(Module, Acc, Chunk));
        _ -> Acc
    end.

get_index_in_ds(State, Start, End) ->
    #state{module = Module, dir = Dir,
           ds_name = DsName} = State,
    {ok, DsReader} = rds_la_tlog:open_reader(Dir, DsName),
    Index =
        case rds_la_tlog:find(DsReader, Start) of
            true ->
                get_index_in_ds1(Module, DsReader, End, Module:init_index());
            false ->
                Module:init_index()
        end,
    ok = rds_la_tlog:close_reader(DsReader),
    #chunk{start_timestamp = Start,
           end_timestamp = End,
           index_data = Index}.

get_index_in_ds1(Module, DsReader, End, Index) ->
    case rds_la_tlog:next(DsReader) of
        {ok, {TS, Item}} when TS =< End ->
            get_index_in_ds1(Module, DsReader, End,
                                 Module:add_to_index(Index, Item));
        _ ->
            Index
    end.

merge(Module, Result1, Result2) ->
    #chunk{start_timestamp = Start,
           index_data = Indexdata1} = Result1,
    #chunk{end_timestamp = End,
           index_data = Indexdata2} = Result2,
    #chunk{start_timestamp = Start,
           end_timestamp = End,
           index_data = Module:merge_index(Indexdata1, Indexdata2)}.

merge(Module, Result1, Result2, Result3) ->
    merge(Module, merge(Module, Result1, Result2), Result3).

%% Author: haoting.wq
%% Created: 2012-6-11
%% Description: TODO: Add description to rds_la_indexer_sql_stats
-module(rds_la_indexer_sql_stats).

-behavior(rds_la_gen_indexer).

-include("logger_header.hrl").
-include("rds_la_log.hrl").

-export([name/0,
         init_index/0,
         add_to_index/2,
         merge_index/2,
         query_index/2,
         from_result/1
        ]).

-record(sql_stats_state, {tree}).

-define(DEFAULT_PAGE_SIZE, 100).

%%% Behavior callbacks for gen_indexer

name() ->  "stats".

init_index() ->  #sql_stats_state{tree = gb_trees:empty()}.

add_to_index(State = #sql_stats_state{tree = Tree}, Record) ->
    State#sql_stats_state{tree = push(Tree, Record)}.

merge_index(#sql_stats_state{tree = Tree1}, #sql_stats_state{tree = Tree2}) ->
    #sql_stats_state{tree = merge(Tree1, Tree2)}.

query_index(#sql_stats_state{tree = Tree}, QueryOpts) ->
    PageStart = case proplists:get_value(page_start, QueryOpts, 1) of
        undefined -> 1;
        PS -> PS
    end,
    PageSize = case proplists:get_value(page_size, QueryOpts, ?DEFAULT_PAGE_SIZE) of
        undefined -> ?DEFAULT_PAGE_SIZE;
        PE -> PE
    end,
    Res = to_list(Tree),
    ResSize = gb_trees:size(Tree),
    case PageStart > ResSize of
        true ->
            {ResSize, []};
        false->
            {ResSize, lists:sublist(Res, PageStart, PageSize)}
    end.

from_result({_ResSize, Result}) ->
    #sql_stats_state{tree = from_list(Result)}.

%%% private

push(Tree, LaRecord=#la_record{'query'=Query}) when is_binary(Query) ->
    case rds_la_analyze:analyze_log(Query) of
        {ok, keep} -> push_to(Tree, LaRecord);
        {ok, {replace, Replaced}} -> push_to(Tree, LaRecord#la_record{'query' = Replaced});
        _ -> Tree
    end;
push(Tree, LaRecord=#la_record{'query'=Query}) when is_list(Query) ->
    push(Tree, LaRecord#la_record{'query'=list_to_binary(Query)}).

push_to(Tree, LaRecord=#la_record{'query'=Query}) ->
    case gb_trees:lookup(Query, Tree) of
        none->
            gb_trees:insert(Query, init_entry(LaRecord), Tree);
        {value, OldEntry} ->
            gb_trees:update(Query, update_entry(LaRecord, OldEntry), Tree)
    end.

merge(T1, T2) ->
    OrdDict = merge_iter(gb_trees:iterator(T1), gb_trees:iterator(T2)),
    gb_trees:from_orddict(OrdDict).

merge_iter(Iter1, Iter2) -> lists:reverse(merge_iter(Iter1, Iter2, [])).

merge_iter(Iter1, Iter2, OrdDict) ->
    case gb_trees:next(Iter1) of
        none -> append_iter(Iter2, OrdDict);
        {Key1, Value1, Iter1N} ->
            case gb_trees:next(Iter2) of
                none -> append_iter(Iter1, OrdDict);
                {Key2, Value2, Iter2N} ->
                    if Key1 < Key2 ->
                           merge_iter(Iter1N, Iter2, [{Key1, Value1}|OrdDict]);
                       Key2 < Key1 ->
                           merge_iter(Iter1, Iter2N, [{Key2, Value2}|OrdDict]);
                       true ->
                           merge_iter(Iter1N, Iter2N, [{Key1, merge_entry(Value1, Value2)}|OrdDict])
                    end
            end
    end.

append_iter(Iter, OrdDict) ->
    case gb_trees:next(Iter) of
        none -> OrdDict;
        {Key, Value, IterN} ->
            append_iter(IterN, [{Key, Value}|OrdDict])
    end.

to_list(T) ->
    L = gb_trees:to_list(T),
    L1 = lists:map(fun({Command,{MinCostTime,MaxCostTime,TotalCostTime, LastTimeStamp, Times}})
                          -> #la_stats{command = Command,
                                       min_cost_time = MinCostTime,
                                       max_cost_time = MaxCostTime,
                                       avg_cost_time = trunc(TotalCostTime/Times),
                                       last_execute_timestamp =LastTimeStamp,
                                       times = Times}
                   end, L),                           
    lists:sort(fun(#la_stats{avg_cost_time = C1}, #la_stats{avg_cost_time = C2})-> C1 > C2 end, L1).

from_list(L) ->
    L1 = lists:map(fun(#la_stats{command = Command,
                                 min_cost_time = MinCostTime,
                                 max_cost_time = MaxCostTime,
                                 avg_cost_time = AvgCostTime,
                                 last_execute_timestamp = LastTimeStamp,
                                 times = Times})
                          -> {Command, {MinCostTime, MaxCostTime, AvgCostTime * Times, LastTimeStamp, Times}}
                   end, L), 
    gb_trees:from_orddict(L1).

init_entry(#la_record{timestamp=TimeStamp, response_time=ResponseTime}) ->
    {ResponseTime, ResponseTime, ResponseTime, TimeStamp, 1}.

update_entry(#la_record{timestamp=TimeStamp, response_time=ResponseTime},
             {OldMinCostTime, OldMaxCostTime, OldTotalCostTime,_OldTimeStamp,OldTimes} ) ->
    {min(ResponseTime, OldMinCostTime),
     max(ResponseTime, OldMaxCostTime),
     OldTotalCostTime + ResponseTime,
     TimeStamp,
     OldTimes + 1}.

merge_entry( {MinCostTime1,MaxCostTime1,TotalCostTime1,LastExecuteTimeStamp1,Times1},
             {MinCostTime2,MaxCostTime2,TotalCostTime2,LastExecuteTimeStamp2,Times2} ) ->
    {min(MinCostTime1, MinCostTime2),
     max(MaxCostTime1, MaxCostTime2),
     TotalCostTime1 + TotalCostTime2,
     max(LastExecuteTimeStamp1, LastExecuteTimeStamp2),
     Times1+Times2}.

%% Author: haoting.wq
%% Created: 2012-6-11
%% Description: TODO: Add description to rds_la_indexer_sql_dumpslow
-module(rds_la_indexer_sql_dumpslow).

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

-export([filter_dump_slow/2]).

-record(dump_slow_state, {topn, pq}).

-define(DUMP_SLOW_STATS_FILE_SUFFIX, "dps").

%%% Behavior callbacks

name() ->  "dumpslow".

init_index() ->
    TopN = rds_la_config:la_dump_slow_topn(),
	init_index(TopN).

init_index(TopN) ->
	#dump_slow_state{topn = TopN,
					 pq = rds_la_priority_queue:new(TopN, 
					     fun(Record) -> -Record#la_record.response_time end)}.

add_to_index(State = #dump_slow_state{pq = PQ}, Record) ->
	State#dump_slow_state{pq = rds_la_priority_queue:push(PQ, Record)}.

merge_index(#dump_slow_state{topn = TopN1, pq = PQ1}, 
			#dump_slow_state{topn = TopN2, pq = PQ2}) ->
    TopN = erlang:min(TopN1, TopN2),
    lists:foldl(
      fun(Record, NState) ->
              add_to_index(NState, Record)
      end,
      init_index(TopN),
	  merge_records(rds_la_priority_queue:to_list(PQ1), rds_la_priority_queue:to_list(PQ2), TopN, [])).

merge_records(_, _, 0, Acc) -> Acc;
merge_records(L1 = [H1|T1], L2 = [H2|T2], N, Acc) ->
	if
		% slower first
		H1#la_record.response_time >= H2#la_record.response_time ->
			merge_records(T1, L2, N-1, [H1|Acc]);
		true ->
			merge_records(L1, T2, N-1, [H2|Acc])
	end;
merge_records([], [H|T], N, Acc) -> merge_records([], T, N-1, [H|Acc]); 
merge_records([H|T], [], N, Acc) -> merge_records(T, [], N-1, [H|Acc]); 
merge_records([], [], _, Acc) -> Acc.

query_index(#dump_slow_state{pq = PQ}, QueryOpts) ->
    filter_dump_slow(rds_la_priority_queue:to_list(PQ), QueryOpts).

from_result(Result) ->
    lists:foldl(
        fun(Record, Acc) ->
            add_to_index(Acc, Record)
        end, init_index(), Result).

filter_dump_slow(Results, Condition) when is_list(Condition) ->
    Min = proplists:get_value(min_exectime, Condition),
    Max = proplists:get_value(max_exectime, Condition),
    Filtered = lists:foldl(
        fun(Record = #la_record{response_time = ResponseTime}, Acc) ->
            case cmp_dump_slow(Min, Max, ResponseTime) of
                true -> [Record|Acc];
                false -> Acc
            end
        end, [], Results
    ),
    Quantity = proplists:get_value(quantity, Condition),
    limit_dump_slow(Quantity, Filtered).

cmp_dump_slow(Min, Max, ResponseTime) ->
    LessThanMin = case Min of
        undefined -> true;
        _ -> ResponseTime >= Min
    end,
    GreaterThanMax = case Max of
        undefined -> true;
        _ -> ResponseTime =< Max
    end,
    LessThanMin and GreaterThanMax.

limit_dump_slow(undefined, Filtered) -> Filtered;
limit_dump_slow(Quantity, Filtered) ->
    case length(Filtered) > Quantity of
        false -> Filtered;
        true ->
            lists:sublist(lists:sort(
                fun(#la_record{response_time = R1}, #la_record{response_time = R2}) ->
                    %% keep the record with longer response time
                    case R1 =< R2 of
                        true -> false;
                        false -> true
                    end
               end, Filtered), Quantity)
    end.

%% Author: haoting.wq
%% Created: 2012-6-12
%% Description: TODO: Add description to pq
%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
-module(rds_la_priority_queue).

-export([new/0, new/1, new/2]).
-export([push/2, size/1, to_list/1]).

-record(priority_queue, {gbt, size = 0, limit, getkey}).

new()->
    #priority_queue{gbt = gb_trees:empty()}.

new(MaxSize) when is_integer(MaxSize) andalso MaxSize > 0 ->
    #priority_queue{gbt = gb_trees:empty(), limit = MaxSize}.

new(MaxSize,GetKey) when is_integer(MaxSize) andalso MaxSize > 0 ->
    #priority_queue{gbt = gb_trees:empty(), limit = MaxSize, getkey = GetKey}.

push(Q = #priority_queue{gbt = T, size = Size, limit = Limit}, E) ->
    {K, V} = e2kv(Q, E),
    if
        Limit =:= undefined -> do_push(Q#priority_queue{size = Size + 1}, K, V);
        Size < Limit -> do_push(Q#priority_queue{size = Size + 1}, K, V);
        true ->
            {LK, _} = gb_trees:largest(T),
            if
                K >= LK -> Q;
                true -> do_replace(Q, K, V)
            end
    end.

to_list(_ = #priority_queue{gbt = T}) ->
    lists:flatten(lists:map( fun({_,VList})-> VList end, gb_trees:to_list(T))).

size(PQ) ->
    PQ#priority_queue.size.

e2kv( _ = #priority_queue{getkey = GetKey}, E ) ->
    case GetKey of
        undefined -> {E, E};
        _ ->  {GetKey(E), E}
    end.

do_push(Q = #priority_queue{gbt = T}, K, V) ->
    case gb_trees:lookup(K, T) of
        none->
            Q#priority_queue{ gbt = gb_trees:insert(K, [V], T) };
        {value, VList} ->
            Q#priority_queue{ gbt = gb_trees:update(K, [V|VList], T) }
    end.

do_replace(Q = #priority_queue{gbt = T}, K, V) ->
    {LK, [_|LVList]} = gb_trees:largest(T),
    NT = case LVList of
        [] -> gb_trees:delete(LK, T);
        _ -> gb_trees:update(LK, LVList, T)
    end,
    do_push(Q#priority_queue{gbt = NT},K, V).

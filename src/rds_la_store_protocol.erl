%% Author: haoting.wq
%% Created: 2012-6-18
%% Description: TODO: Add description to rds_la_store_protocol
-module(rds_la_store_protocol).

-behaviour(back_proxy).
%%
%% Include files
%%
-include("logger_header.hrl").
-include("rds_la_log.hrl").

%%
%% Exported Functions
%%
-export([init/1, terminate/2]).
-export([handle_msg/3, handle_nowait/1, handle_timer/2, handle_local_info/2]).

-export([append_user_log_async/4]).
-export([query_user_dump_slow/7, query_user_sql_stats/6]).

-record(pstate, {last_query_type_condition, left_query_dstnps = 0, query_from_dstnps = dict:new()}).

%%
%% API Functions
%%

append_user_log_async(Pid, User, ProxyId, Record) ->
    back_proxy:local_cast(Pid, {append_user_log_async, User, ProxyId, Record}).

query_user_dump_slow(Pid, User, DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity) ->
    Condition = {DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity},
    back_proxy:local_call(Pid, {query_user_log_sync, User, dump_slow, Condition}).

query_user_sql_stats(Pid, User, DateStart, DateEnd, PageStart, PageEnd) ->
    Condition = {DateStart, DateEnd, PageStart, PageEnd},
    back_proxy:local_call(Pid, {query_user_log_sync, User, sql_stats, Condition}).

init(_ModArg) -> 
    {ok, #pstate{}}.

terminate(_Reason, _ModState) ->
    ok.

handle_msg(From, Binary, ModState) ->
    ?DEBUG("remote receive msg: ~p~n", [Binary]),
    Term = binary_to_term(Binary),
    handle_remote(From, Term, ModState).

handle_nowait(ModState) ->
    {wait_msg, ModState}.

handle_timer(_TimerMsg, ModState) ->
    {wait_msg, ModState}.

handle_local_info({local_cast, Term}, ModState) ->
    ?DEBUG("local receive msg: ~p ~n", [Term]),
    handle_local(Term, ModState);

handle_local_info({local_call, Term}, ModState) ->
    ?DEBUG("local receive msg: ~p ~n", [Term]),
    handle_local(Term, ModState);

handle_local_info({last_send_failed, DstNP, _LastOperation, _FaildResult}, ModState) ->
    ?DEBUG("last send failed ~p~n", [DstNP]),
    {wait_msg, ModState};

%% connection_down
handle_local_info({connection_down, DstNP}, ModState) ->
    ?DEBUG("connection down ~p~n", [DstNP]),
    handle_connection_down(DstNP, ModState);

handle_local_info(_Msg, ModState) ->
    {wait_msg, ModState}.

%%
%% Local Functions
%%

%% for store client
handle_local({append_user_log_async, User, ProxyId, Record}, ModState) ->
    case rds_la_controller_protocol:get_user_append_dstnps(User) of
        [] -> {wait_msg, ModState};
        AppendDstNPs ->
            append_user_log_to_node_async(User, ProxyId, Record, AppendDstNPs, ModState)
    end;

handle_local({query_user_log_sync, User, Type, Condition}, ModState) ->
    case rds_la_controller_protocol:get_user_query_dstnps(User) of
        [] -> reply_local({error, no_query_nodes}, ModState);
        QueryDstNPs ->
            query_user_log_from_node_sync(User, Type, Condition, QueryDstNPs, ModState)
    end;

handle_local(_Term, ModState) ->
    {wait_msg, ModState}.

append_user_log_to_node_async(User, ProxyId, Record, DstNPs, ModState) ->
    Msg = term_to_binary({append_async, ProxyId, Record}),
    NIndex = case get({User, append_index}) of
        undefined -> 0;
        Index -> (Index + 1) rem length(DstNPs)
    end,
    put({User, append_index},  NIndex),
    handle_remote_async(lists:nth(NIndex + 1, DstNPs), Msg, ModState).

query_user_log_from_node_sync(User, Type, Condition, QueryDstNPs, ModState) ->
    Msg = term_to_binary({query_sync, User, Type, Condition}),
    SuccessfulDstNPs = send_query_msg(Msg, QueryDstNPs),
    NQueryFromDstNPs = lists:foldl(
        fun(DstNP, AccDict) ->
            dict:store(DstNP, no_result, AccDict)
        end, dict:new(), SuccessfulDstNPs),
    {wait_msg, ModState#pstate{last_query_type_condition = {Type, Condition},
                               left_query_dstnps = length(SuccessfulDstNPs),
                               query_from_dstnps = NQueryFromDstNPs}}.

send_query_msg(Msg, DstNPs) ->
    send_query_msg(Msg, DstNPs, []).

send_query_msg(_Msg, [], SuccessfulAcc) ->
    SuccessfulAcc;
send_query_msg(Msg, [DstNP|DstNPs], SuccessfulAcc) ->
    case back_proxy:sync_send_to(DstNP, Msg) of
        {error, _Reason} -> send_query_msg(Msg, DstNPs, SuccessfulAcc);
        ok -> send_query_msg(Msg, DstNPs, [DstNP|SuccessfulAcc])
    end.

%% for store & store client

handle_remote(From, Term, ModState) ->
    case Term of
        %% for store
        {append_async, ProxyId, Record} ->
            handle_append(ProxyId, Record),
            {wait_msg, ModState};
        {query_sync, User, Type, Condition} ->
            Res = handle_query(User, Type, Condition),
            reply_remote_term({query_reply, Res}, ModState);
        %% for store client
        {query_reply, Res} ->
            handle_query_reply(From, Res, ModState)
    end.

handle_append(ProxyId, Record) ->
    rds_la_api:append_user_log_async(Record#la_record.user, ProxyId, Record).

handle_query(User, dump_slow, {DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity}) ->
    rds_la_api:query_user_dump_slow(User, node(), DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity);
handle_query(User, sql_stats, {DateStart, DateEnd, PageStart, PageEnd}) ->
    rds_la_api:query_user_sql_stats(User, node(), DateStart, DateEnd, PageStart, PageEnd).

handle_query_reply(From, Res,
    ModState = #pstate{last_query_type_condition = {Type, Condition},
                       left_query_dstnps= LeftN,
                       query_from_dstnps = QueryFromDstNPs}) ->
    ?DEBUG("Query Reply from ~p~n", [From]),
    case dict:find(From, QueryFromDstNPs) of
        {ok, no_result} ->
            NQueryFromDstNPs = dict:store(From, Res, QueryFromDstNPs),
            NLeftN = LeftN - 1,
            case NLeftN of
                0 ->
                    Reply = merge_query_reply(Type, Condition, NQueryFromDstNPs),
                    reply_local(Reply, ModState#pstate{last_query_type_condition = undefined,
                                                     left_query_dstnps = 0,
                                                     query_from_dstnps = dict:new()});
                _ ->
                    {wait_msg, ModState#pstate{left_query_dstnps = NLeftN,
                                               query_from_dstnps = NQueryFromDstNPs}}
            end;
        _ -> {wait_msg, ModState}
    end.

merge_query_reply(Type, Condition, QueryFromDstNPs) ->
    ResultList = dict:fold(
        fun(_DstNP, QueryReply, Acc) ->
            case QueryReply of
                {ok, R} -> [R|Acc];
                _ -> Acc
            end
        end, [], QueryFromDstNPs),
    {ok, do_merge(Type, ResultList, Condition)}.

do_merge(Type, ResultList, Condition) ->
    rds_la_api:merge_query_results(Type, ResultList, type_condition(Type, Condition)).

type_condition(dump_slow, {DateStart, DateEnd, MinExecTime, MaxExecTime, Quantity}) ->
    makeup_generic_condition(DateStart, DateEnd) ++
    makeup_dump_slow_condition(MinExecTime, MaxExecTime, Quantity);
type_condition(sql_stats, {DateStart, DateEnd, PageStart, PageEnd}) ->
    makeup_generic_condition(DateStart, DateEnd) ++
    makeup_sql_stats_condition(PageStart, PageEnd).

makeup_generic_condition(DateStart, DateEnd) ->
    SecondsStart = rds_la_lib:universal_to_seconds(DateStart),
    SecondsEnd = rds_la_lib:universal_to_seconds(DateEnd),
    [{date_start, SecondsStart}, {date_end, SecondsEnd}].

makeup_dump_slow_condition(MinExecTime, MaxExecTime, Quantity) ->
    [{min_exectime, MinExecTime}, {max_exectime, MaxExecTime}, {quantity, Quantity}].

makeup_sql_stats_condition(PageStart, PageEnd) ->
    [{page_start, PageStart}, {page_end, PageEnd}].

%% for store client

handle_connection_down(DstNP,
    ModState = #pstate{left_query_dstnps= LeftN}) ->
    case LeftN > 0 of
        true -> handle_query_reply(DstNP, {error, connection_down}, ModState);
        false -> {wait_msg, ModState}
    end.

handle_remote_async(DstNP, Binary, ModState) ->
    {{nowatch_send, DstNP, Binary}, ModState}.

reply_local(Term, ModState) ->
    {{reply, Term}, ModState}.

reply_remote_term(Term, ModState) ->
    Binary = term_to_binary(Term),
    {{reply, Binary}, ModState}.

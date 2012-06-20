%%% -------------------------------------------------------------------
%%% Author  : haoting.wq
%%% Description :
%%%
%%% Created : 2012-5-3
%%% -------------------------------------------------------------------
-module(rds_la_test_protocol).

-include("logger_header.hrl").

-behaviour(back_proxy).

%% gen_server callbacks
-export([init/1, terminate/2]).
-export([handle_msg/3, handle_nowait/1, handle_timer/2, handle_local_info/2]).

-record(la_pstate, {
    sbuf_dict = dict:new(),        %% {{Node, Port}, SBuf}
    tmpq = queue:new(),
    storage = dict:new(),
    last_state = default           %% default | {remote_call, DstNP, Ref} | | | |
}).

-export([local_call_append/3, local_call_lookup/2, local_cast_append/3]).
-export([remote_call_append/5, remote_call_lookup/4, remote_cast_append/5]).
-export([remote_call_append_fast/5, remote_call_lookup_fast/4]).
-export([remote_call_append_async/5, remote_call_lookup_async/4]).
-export([cascade_call_append/4, cascade_call_lookup/3, cascade_cast_append/4]).

-define(PERF_REPORT_QUANTITY, 10000).
-define(PERF_QPS_INIT(Ref), perf_qps:mperf_init(Ref)).
-define(PERF_QPS_SCOPE(Ref, Position, Prefix), perf_qps:mperf_scope(Ref, Position, Prefix, ?PERF_REPORT_QUANTITY)).
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
%% make sure:
%% 1. if your key and value are very large, they should be binary,
%%    this must improve your performance
%% 2. if your key and value are small, they could be atomic

%% asynchronous performance may suffer from these reason:
%% 1. min_bin_vheap_size (major) & min_heap_size (minor)
%%    min_bin_vheap_size should neither too big nor too small, 1~4 MB is advised
%%    min_bin_vheap_size affect msgs and binary payload
%%    min_heap_size affect term payload,
%%    actually it is affect the gc of erlang for different type of payload:
%%        msgs and binary use min_bin_vheap_size, term use min_heap_size
%% 2. process and port must in same scheduler for both client and server
%% 3. serialize & deserialize should not too frequently,
%%        the payload need to do this must not be too long
%% 4. the order for the match of fsm must be arrange carefully, 
%%        if the state is frequently accessed, it must be arrangeed frontly,
%%        the best method is reduce the out-degree of a state,
%%        and lower down the compare times, it called fast-path

local_call_append(Pid, Key, Value) ->
    adjust_reply(do_deserialize(back_proxy:local_call(Pid, make_append(Key, Value)))).
local_call_lookup(Pid, Key) ->
    adjust_reply(do_deserialize(back_proxy:local_call(Pid, make_lookup(Key)))).
local_cast_append(Pid, Key, Value) ->
    back_proxy:local_cast(Pid, make_append(Key, Value)).

remote_call_append(Pid, Node, Port, Key, Value) ->
    adjust_reply(do_deserialize(back_proxy:remote_call(Pid, {Node, Port}, make_append(Key, Value)))).
remote_call_lookup(Pid, Node, Port, Key) ->
    adjust_reply(do_deserialize(back_proxy:remote_call(Pid, {Node, Port}, make_lookup(Key)))).
remote_cast_append(Pid, Node, Port, Key, Value) ->
    back_proxy:remote_cast(Pid, {Node, Port}, make_append(Key, Value)).

remote_call_append_fast(Pid, Node, Port, Key, Value) ->
    adjust_reply(do_deserialize(back_proxy:remote_call(Pid, {Node, Port}, make_fast(make_append(Key, Value))))).
remote_call_lookup_fast(Pid, Node, Port, Key) ->
    adjust_reply(do_deserialize(back_proxy:remote_call(Pid, {Node, Port}, make_fast(make_lookup(Key))))).

remote_call_append_async(Pid, Node, Port, Key, Value) ->
    back_proxy:remote_cast(Pid, {Node, Port}, make_fast(make_append(Key, Value))).
remote_call_lookup_async(Pid, Node, Port, Key) ->
    back_proxy:remote_cast(Pid, {Node, Port}, make_fast(make_lookup(Key))).

cascade_call_append(Pid, DstNPList, Key, Value) ->
    adjust_reply(do_deserialize(back_proxy:local_call(Pid, makeup_cascade(DstNPList, make_append(Key, Value))))).
cascade_call_lookup(Pid, DstNPList, Key) ->
    adjust_reply(do_deserialize(back_proxy:local_call(Pid, makeup_cascade(DstNPList, make_lookup(Key))))).
cascade_cast_append(Pid, DstNPList, Key, Value) ->
    back_proxy:local_cast(Pid, makeup_cascade(DstNPList, make_append(Key, Value))).

adjust_reply({fast_reply, Res}) -> Res;
adjust_reply({reply, _Ref, {cascade, reply, Res}}) -> Res;
adjust_reply({reply, _Ref, Res}) -> Res;
adjust_reply(Res) -> Res.
%% ------------------------------------------------------------------
%% gen_proxy Function Definitions
%% ------------------------------------------------------------------

init(_) ->
    ?PERF_QPS_INIT(do_serialize),
    ?PERF_QPS_INIT(do_deserialize),
    ?PERF_QPS_INIT(handle_fsm),
    ?PERF_QPS_INIT(handle_local_info),
    ?PERF_QPS_INIT(dict_store),
    ?PERF_QPS_INIT(dict_find),
    ?DEBUG("rds_la_test_protocol initialized, pid ~p~n", [self()]),
    {ok, #la_pstate{}}.

terminate(_Reason, _ModState) ->
    ?DEBUG("rds_la_test_protocol terminate, pid ~p~n", [self()]),
    ok.

%%
handle_msg(From, Binary, ModState = #la_pstate{tmpq = TmpQ}) ->
    ?DEBUG("will handle tcp msg from ~p, content: ~p~n", [From, Binary]),
    handle_nowait(ModState#la_pstate{tmpq = queue:in({From, do_deserialize(Binary)}, TmpQ)}).

handle_nowait(ModState = #la_pstate{tmpq = TmpQ, last_state = LastState}) ->
    case queue:out(TmpQ) of
        {empty, _} -> {wait_msg, ModState};
        {{value, {From, Term}}, NTmpQ} ->
            ?DEBUG("handle_nowait: {~p, ~p}~n", [From, Term]),
            %?PERF_QPS_SCOPE(handle_fsm, start, "rds_la_test_protocol handle_fsm"),
            Res = handle_fsm(LastState, From, Term, ModState#la_pstate{tmpq = NTmpQ}),
            %?PERF_QPS_SCOPE(handle_fsm, stop, "rds_la_test_protocol handle_fsm"),
            Res
    end.

handle_timer(_TimerMsg, ModState) ->
    {wait_msg, ModState}.

handle_local_info(LocalInfo, ModState = #la_pstate{tmpq = TmpQ}) ->
    QIn = queue:in({local, LocalInfo}, TmpQ),
    %?PERF_QPS_SCOPE(handle_local_info, start, "rds_la_test_protocol handle_local_info"),
    Res = handle_nowait(ModState#la_pstate{tmpq = QIn}),
    %?PERF_QPS_SCOPE(handle_local_info, stop, "rds_la_test_protocol handle_local_info"),
    Res.


%% ------------------------------------------------------------------
%% Serialize / Deserialize
%% ------------------------------------------------------------------

do_serialize(Term) ->
    %?PERF_QPS_SCOPE(do_serialize, start, "rds_la_test_protocol do_serialize"),
    Binary = term_to_binary(Term),
    %?PERF_QPS_SCOPE(do_serialize, stop, "rds_la_test_protocol do_serialize"),
    Binary.

do_deserialize(Binary) ->
    %?PERF_QPS_SCOPE(do_deserialize, start, "rds_la_test_protocol do_deserialize"),
    Res = try binary_to_term(Binary) of
        Term -> Term
    catch
        _:_ -> {error, illedge_binary}
    end,
    %?PERF_QPS_SCOPE(do_deserialize, stop, "rds_la_test_protocol do_deserialize"),
    Res.

%% ------------------------------------------------------------------
%% Finite State Maschine Operations
%% ------------------------------------------------------------------

%% handle_fsm(LastState, MsgFrom, MsgTerm, ModState)

%% special cascade msgs, only for experiment


%% handle_local_info msgs:

%% normal call & reply

handle_fsm(LastState, From, Term = {_Local, {cascade, _ToDstNPList, _Term}}, ModState) ->
    handle_cascade(LastState, From, Term, ModState);
handle_fsm(LastState, From, Term = {call, _Ref, {cascade, _ToDstNPList, _Term}}, ModState) ->
    handle_cascade(LastState, From, Term, ModState);
handle_fsm(LastState, From, Term = {cast, {cascade, _ToDstNPList, _Term}}, ModState) ->
    handle_cascade(LastState, From, Term, ModState);

handle_fsm(default, local, {local_call, Term}, ModState) ->
    {Res, NModState} = handle_call(local, Term, ModState),
    NRes = makeup_reply(undefined, Res),
    {{reply, do_serialize(NRes)}, NModState};
handle_fsm(default, local, {local_cast, Term}, ModState) ->
    NModState = handle_cast(local, Term, ModState),
    {noreply, NModState};

%% async fast call
handle_fsm(default, local, {remote_cast, DstNP, {fast, Term}}, ModState) ->
    {{nowatch_send, DstNP, do_serialize(makeup_fast_call(Term))}, ModState};

%% remote cast
handle_fsm(default, local, {remote_cast, DstNP, Term}, ModState) ->
    NTerm = makeup_cast(Term),
    {{nowatch_send, DstNP, do_serialize(NTerm)}, ModState};

%% fast call & reply
%% client send
handle_fsm(default, local, {remote_call, DstNP, {fast, Term}}, ModState) ->
    {{nowatch_send, DstNP, do_serialize(makeup_fast_call(Term))}, ModState};
%% server receive & handle & send
handle_fsm(default, From, {fast_call, Term}, ModState) ->
    {Res, NModState} = handle_fast_call(From, Term, ModState),
    NRes = makeup_fast_reply(Res),
    {{reply, do_serialize(NRes)}, NModState};
%% client receive
handle_fsm(default, _DstNP, Res = {fast_reply, _Term}, ModState) ->
    NModState = ModState#la_pstate{last_state = default},
    {{reply, do_serialize(Res)}, NModState};

%% remote call
handle_fsm(default, local, {remote_call, DstNP, Term}, ModState) ->
    {Ref, NTerm} = makeup_call(Term),
    {{watch_send, DstNP, do_serialize(NTerm)},
        ModState#la_pstate{last_state = {remote_call, DstNP, Ref}}};

%% reply can't come here, only watch_send could be here
handle_fsm({remote_call, DstNP, Ref}, local, {last_send_failed, DstNP, _LastOperation, FaildResult}, ModState) ->
    NModState = ModState#la_pstate{last_state = default},
    Res = makeup_reply(Ref, FaildResult),
    {{clear_watch, [DstNP], {{reply, do_serialize(Res)}, NModState}}, NModState};

handle_fsm({remote_call, DstNP, Ref}, local, {connection_down, DstNP}, ModState) ->
    ?DEBUG("connection down ~p~n", [DstNP]),
    NModState = ModState#la_pstate{last_state = default},
    Res = makeup_reply(Ref, {connection_down, DstNP}),
    {{clear_watch, [DstNP], {{reply, do_serialize(Res)}, NModState}}, NModState};

%% back_proxy:close_send_socket help to finish clear_watch
handle_fsm({remote_call, DstNP, Ref}, DstNP, Res = {reply, Ref, _Term}, ModState) ->
    NModState = ModState#la_pstate{last_state = default},
    {{clear_watch, [DstNP], {{reply, do_serialize(Res)}, NModState}}, NModState};

%% handle_msg msgs, mainly for protocol:

%% for server
handle_fsm(default, From, {call, Ref, Term}, ModState) ->
    {Res, NModState} = handle_call(From, Term, ModState),
    NRes = makeup_reply(Ref, Res),
    {{reply, do_serialize(NRes)}, NModState};
handle_fsm(default, From, {cast, Term}, ModState) ->
    NModState = handle_cast(From, Term, ModState),
    {noreply, NModState};
%% for client
handle_fsm({remote_call, DstNP, Ref}, DstNP, Res = {reply, Ref, _Term}, ModState) ->
    NModState = ModState#la_pstate{last_state = default},
    {{clear_watch, [DstNP], {{reply, do_serialize(Res)}, NModState}}, NModState};

%% other msg can't be handled
handle_fsm(default, _From, _Term, ModState) ->
    %?DEBUG("Wrong Message from ~p~nContent: ~p~n", [From, Term]),
    {noreply, ModState}.

%% ------------------------------------------------------------------
%% Internal Services
%% ------------------------------------------------------------------
make_fast(Term) ->
    {fast, Term}.

makeup_ref_call(Ref, Term) ->
    {Ref, {call, Ref, Term}}.

makeup_call(Term) ->
    %%        2.5us            3.5us
    Ref = {erlang:node(), erlang:localtime()},
    {Ref, {call, Ref, Term}}.

makeup_cast(Term) ->
    {cast, Term}.

makeup_reply(Ref, Term) ->
    {reply, Ref, Term}.

makeup_fast_call(Term) ->
    {fast_call, Term}.

makeup_fast_reply(Term) ->
    {fast_reply, Term}.

handle_call(_From, CallCommand, ModState) ->
%    timer:sleep(20 * 1000),                                %% for test
    handle_command(CallCommand, ModState).

handle_cast(_From, CastCommand, ModState) ->
    {_, NModState} = handle_command(CastCommand, ModState),
    NModState.

handle_fast_call(_From, CallCommand, ModState) ->
    handle_command(CallCommand, ModState).

make_append(Key, Value) ->
    {append, Key, Value}.

make_lookup(Key) ->
    {lookup, Key}.

handle_command({append, Key, Value}, ModState = #la_pstate{storage = Storage}) ->
    ?DEBUG("append ~p ~p~n", [Key, Value]),
    %?PERF_QPS_SCOPE(dict_store, start, "rds_la_test_protocol dict_store"),
    %Stored = Storage,
    %?PERF_QPS_SCOPE(dict_store, stop, "rds_la_test_protocol dict_store"),
    %{{ok, node()}, ModState#la_pstate{storage = Stored}};
    {ok, ModState#la_pstate{storage = dict:store(Key, Value, Storage)}};
handle_command({lookup, Key}, ModState = #la_pstate{storage = Storage}) ->
    %?PERF_QPS_SCOPE(dict_find, start, "rds_la_test_protocol dict_find"),
    Res = dict:find(Key, Storage),
    %?PERF_QPS_SCOPE(dict_find, stop, "rds_la_test_protocol dict_find"),
    ?DEBUG("lookup ~p, result ~p~n", [Key, Res]),
    %{{Res, node()}, ModState};
    {Res, ModState};
handle_command(_, ModState) ->
    {{error, unsupport_command}, ModState}.

%% ------------------------------------------------------------------
%% Cascade Services
%% ------------------------------------------------------------------

makeup_cascade(ToDstNPList, Term) ->
    {cascade, ToDstNPList, Term}.

handle_cascade(default, local, {local_call, {cascade, ToDstNPList, Term}}, ModState) ->
    case ToDstNPList of
        [] ->
            handle_fsm(default, local, {local_call, Term}, ModState);
        [ToDstNP|Left] ->
            ?DEBUG("local_call ToDstNP: ~p, Left: ~p~n", [ToDstNP, Left]),
            {Ref, NTerm} = makeup_call(makeup_cascade(Left, Term)),
            {{watch_send, ToDstNP, do_serialize(NTerm)},
                ModState#la_pstate{last_state = {remote_call, ToDstNP, Ref}}}
    end;
handle_cascade(default, local, {local_cast, {cascade, ToDstNPList, Term}}, ModState) ->
    case ToDstNPList of
        [] ->
            handle_fsm(default, local, {local_cast, Term}, ModState);
        [ToDstNP|Left] ->
            ?DEBUG("local_cast ToDstNP: ~p, Left: ~p~n", [ToDstNP, Left]),
            NTerm = makeup_cast(makeup_cascade(Left, Term)),
            {{nowatch_send, ToDstNP, do_serialize(NTerm)}, ModState}
    end;
handle_cascade(default, From, {call, Ref, {cascade, ToDstNPList, Term}}, ModState) ->
    case ToDstNPList of
        [] ->
            {Res, NModState} = handle_call(From, Term, ModState),
            NTerm = makeup_reply(Ref, makeup_cascade(reply, Res)),
            {{reply, do_serialize(NTerm)}, NModState};
        [ToDstNP|Left] ->
            ?DEBUG("call ToDstNP: ~p, Left: ~p~n", [ToDstNP, Left]),
            {_, NTerm} = makeup_ref_call(Ref, makeup_cascade(Left, Term)),
            {{watch_send, ToDstNP, do_serialize(NTerm)},
                ModState#la_pstate{last_state = {remote_call, ToDstNP, Ref}}}
    end;
handle_cascade(default, From, {cast, {cascade, ToDstNPList, Term}}, ModState) ->
    case ToDstNPList of
        [] ->
            NModState = handle_cast(From, Term, ModState),
            {noreply, NModState};
        [ToDstNP|Left] ->
            ?DEBUG("cast ToDstNP: ~p, Left: ~p~n", [ToDstNP, Left]),
            NTerm = makeup_cast(makeup_cascade(Left, Term)),
            {{nowatch_send, ToDstNP, do_serialize(NTerm)}, ModState}
    end.

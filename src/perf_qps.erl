-module(perf_qps).

-include("logger_header.hrl").

-export([perf_init/0, perf_qps/1, perf_qps/2, perf_scope/1, perf_scope/2, perf_scope/3]).
-export([mperf_init/1, mperf_qps/2, mperf_qps/3, mperf_scope/2, mperf_scope/3, mperf_scope/4]).
-export([loop/2, qperf_fun/2, qperf_fun/3, qperf_ofun/2, qperf_ofun/3]).

-define(DEFAULT_LASTN, 1000).

-record(mperf_entry, {
    mperf_sstart = erlang:now(),
    mperf_snum = 0,
    mperf_sacc = 0,
    mperf_qnum = 0,
    mperf_qstart = erlang:now()
}).

%% This is a lib for calculate qps

%%--------------------------------------------------------------

%% perf
perf_init() ->
    put(perf_sstart, erlang:now()),
    put(perf_snum, 0),
    put(perf_sacc, 0),
    put(perf_qnum, 0),
    put(perf_qstart, erlang:now()).

perf_qps(Prefix) ->
    perf_qps(Prefix, ?DEFAULT_LASTN).

perf_qps(Prefix, DstNum) when DstNum > 0 ->
    Num = get(perf_qnum) + 1,
    case Num >= DstNum of
        true ->
            Diff = timer:now_diff(erlang:now(), get(perf_qstart)),
            put(perf_qnum, 0),
            put(perf_qstart, erlang:now()),
            report_perf(Prefix, DstNum, Diff);
        false ->
            put(perf_qnum, Num)
    end.

%% Position: start | stop
perf_scope(Position) ->
    perf_scope(Position, self()).

perf_scope(Position, Prefix) ->
    perf_scope(Position, Prefix, ?DEFAULT_LASTN).

perf_scope(start, _Prefix, _DstNum) ->
    put(perf_sstart, erlang:now());
perf_scope(stop, Prefix, DstNum) when DstNum > 0 ->
    Diff =timer:now_diff(erlang:now(), get(perf_sstart)),
    Acc = get(perf_sacc),
    Num = get(perf_snum) + 1,
    case Num >= DstNum of
        true ->
            put(perf_snum, 0),
            put(perf_sacc, 0),
            put(perf_sstart, erlang:now()),
            report_perf(Prefix, DstNum, Acc + Diff);
        false ->
            put(perf_snum, Num),
            put(perf_sacc, Acc + Diff)
    end.

%% mperf
%% no matter if mperf is initialized, the cost of a pair of mperf_scope is 6 ~ 7 us,
%% this is a cost can't be ignore, so you had better not use them in nested
mperf_init(Ref) ->
    put({mperf, Ref}, #mperf_entry{}).

mperf_qps(Ref, Prefix) ->
    mperf_qps(Ref, Prefix, ?DEFAULT_LASTN).

mperf_qps(Ref, Prefix, DstNum) when DstNum > 0 ->
    case get({mperf, Ref}) of
        undefined -> undefined;
        ME = #mperf_entry{mperf_qnum = OQNum, mperf_qstart = QStart} ->
            QNum = OQNum + 1,
            case QNum >= DstNum of
                true ->
                    Diff = timer:now_diff(erlang:now(), QStart),
                    put({mperf, Ref}, ME#mperf_entry{mperf_qnum = 0,
                                                     mperf_qstart = erlang:now()}),
                    report_perf(Prefix, DstNum, Diff);
                false ->
                    put({mperf, Ref}, ME#mperf_entry{mperf_qnum = QNum})
            end
    end.

%% Position: start | stop
mperf_scope(Ref, Position) ->
    mperf_scope(Ref, Position, self()).

mperf_scope(Ref, Position, Prefix) ->
    mperf_scope(Ref, Position, Prefix, ?DEFAULT_LASTN).

mperf_scope(Ref, start, _Prefix, _DstNum) ->
    case get({mperf, Ref}) of
        undefined -> undefined;
        ME -> put({mperf, Ref}, ME#mperf_entry{mperf_sstart = erlang:now()})
    end;
mperf_scope(Ref, stop, Prefix, DstNum) when DstNum > 0 ->
    case get({mperf, Ref}) of
        undefined -> undefined;
        ME = #mperf_entry{mperf_snum = OSNum, mperf_sstart = SStart, mperf_sacc = SAcc} ->
            Diff = timer:now_diff(erlang:now(), SStart),
            SNum = OSNum + 1,
            case SNum >= DstNum of
                true ->
                    put({mperf, Ref}, ME#mperf_entry{mperf_snum = 0,
                                                     mperf_sstart = erlang:now(),
                                                     mperf_sacc = 0}),
                    report_perf(Prefix, DstNum, SAcc + Diff);
                false ->
                    put({mperf, Ref}, ME#mperf_entry{mperf_snum = SNum,
                                                     mperf_sacc = SAcc + Diff})
            end
    end.

loop(Fun, DstNum) when DstNum > 0 ->
    Fun(),
    loop(Fun, DstNum - 1);
loop(_Fun, 0) -> ok;
loop(_Fun, _DstNum) ->
    {error, invalid_arg}.

qperf_fun(Fun, DstNum) ->
    qperf_fun(self(), Fun, DstNum).

qperf_fun(Prefix, Fun, DstNum) ->
    Start = erlang:now(),
    loop(Fun, DstNum),
    Stop = erlang:now(),
    Acc = timer:now_diff(Stop, Start),
    report_perf(Prefix, DstNum, Acc).

qperf_ofun(Fun, DstNum) ->
    qperf_ofun(self(), Fun, DstNum).

qperf_ofun(Prefix, Fun, DstNum) ->
    Start = erlang:now(),
    Fun(),
    Stop = erlang:now(),
    Acc = timer:now_diff(Stop, Start),
    report_perf(Prefix, DstNum, Acc).
%%--------------------------------------------------------------

report_perf(Prefix, Num, Acc) ->
    io:format("For ~p Got ~p Msg in ~p us, qps: ~p, each cost: ~p us~n",
        [Prefix, Num, Acc, Num * 1000 * 1000 / Acc, Acc / Num]).
%% Author: haoting.wq
%% Created: 2012-6-13
%% Description: TODO: Add description to rds_la_lib
-module(rds_la_lib).

-export([ms/1, cookie_hash/0, ensure_started/1,
         is_running/0, is_running/1, nodes_running/1]).
-export([current_timestamp/0, string_to_localtime/1, localtime_to_seconds/1, universal_to_seconds/1]).

ms(App) ->
    FName = atom_to_list(App) ++ ".app",
    case code:where_is_file(FName) of
        non_existing ->
            {error, {file:format_error(enoent), FName}};
        FullName ->
            case file:consult(FullName) of
                {ok, [{application, App, AppFile}]} ->
                    case lists:keysearch(modules, 1, AppFile) of
                        {value, {modules, Mods}} ->
                            Mods;
                        _ ->
                            {error, {invalid_format, modules}}
                    end;
                Error ->
                    {error, {invalid_format, Error}}
            end
     end.


cookie_hash() ->
    base64:encode_to_string(erlang:md5(atom_to_list(erlang:get_cookie()))).

ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.

is_running() -> is_running(node()).

is_running(Node) ->
    case rpc:call(Node, application, which_applications, [infinity]) of
        {badrpc, _} -> false;
        Apps        -> proplists:is_defined(rds, Apps)
    end.

nodes_running(Nodes) ->
    [N || N <- Nodes, is_running(N)].

current_timestamp() ->
    {M,S,_} = erlang:now(),
    M*1000000 + S.

localtime_to_seconds({{Y,Mon,D},{H,Min,S}})->
    try
        [UTime] = calendar:local_time_to_universal_time_dst({{Y,Mon,D},{H,Min,S}}),
        Time = calendar:datetime_to_gregorian_seconds(UTime),
        Base = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
        Time-Base
    catch 
        _:_ -> 0
    end.

universal_to_seconds({{Y,Mon,D},{H,Min,S}})->
    try
        Time = calendar:datetime_to_gregorian_seconds({{Y,Mon,D},{H,Min,S}}),
        Base = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
        Time-Base
    catch
        _:_ -> 0
    end.

%% yyyy-mm-dd hh:mm:ss -> {{yyyy,mm,dd},{hh,mm,ss}}
string_to_localtime(TimeStr) ->
    try 
        [Date, Time] = string:tokens(TimeStr, " "),
        D = parse_date(Date),
        T = parse_time(Time),
        {D, T}
    catch
        {'EXIT', _Err} -> undefined
    end.

%% yyyy-mm-dd
parse_date(Date) ->
    [Y, M, D] = string:tokens(Date, "-"),
    Date1 = {list_to_integer(Y), list_to_integer(M), list_to_integer(D)},
    case calendar:valid_date(Date1) of
        true ->
            Date1;
        _ ->
            false
    end.

%% hh:mm:ss
parse_time(Time) ->
    [H, M, S] = string:tokens(Time, ":"),
    {[H1, M1, S1], true} = check_list([{H, 24}, {M, 60}, {S, 60}]),
    {H1, M1, S1}.

check_list(List) ->
    lists:mapfoldl(
      fun({L, N}, B)->
          V = list_to_integer(L),
          if
              (V >= 0) and (V =< N) ->
                  {V, B};
              true ->
                  {false, false}
          end
      end, true, List).
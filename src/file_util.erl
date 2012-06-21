%% Author: haoting.wq
%% Created: 2012-5-28
%% Description: TODO: Add description to file_util
-module(file_util).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([recursive_copy/2, recursive_del/1]).
-export([read_file_term/1, write_file_term/2]).

%%
%% API Functions
%%

read_file_term(FileName) ->
    case file:read_file(FileName) of
        {ok, Binary} -> {ok, binary_to_term(Binary)};
        {error, Reason} -> {error, Reason}
    end.

write_file_term(FileName, Term) ->
    ok = filelib:ensure_dir(FileName),
    file:write_file(FileName, term_to_binary(Term)).

recursive_copy(Src, Dest) ->
    %% Note that this uses the 'file' module and, hence, shouldn't be
    %% run on many processes at once.
    case filelib:is_dir(Src) of
        false -> case file:copy(Src, Dest) of
                     {ok, _Bytes}    -> ok;
                     {error, enoent} -> ok; %% Path doesn't exist anyway
                     {error, Err}    -> {error, {Src, Dest, Err}}
                 end;
        true  -> case file:list_dir(Src) of
                     {ok, FileNames} ->
                         case file:make_dir(Dest) of
                             ok ->
                                 lists:foldl(
                                   fun (FileName, ok) ->
                                           recursive_copy(
                                             filename:join(Src, FileName),
                                             filename:join(Dest, FileName));
                                       (_FileName, Error) ->
                                           Error
                                   end, ok, FileNames);
                             {error, Err} ->
                                 {error, {Src, Dest, Err}}
                         end;
                     {error, Err} ->
                         {error, {Src, Dest, Err}}
                 end
    end.

recursive_del(Dir) ->
    case file:list_dir(Dir) of
        {ok, Names} ->
            lists:foreach(
                fun(Name)->
                    Path = filename:join(Dir, Name),
                    case filelib:is_dir(Path) of
                        true ->
                             recursive_del(Path);
                           false ->
                               file:delete(Path)
                    end
                end, Names),
            _ = file:del_dir(Dir),
            ok;
        {error, _} ->
            ok
    end.

%%
%% Local Functions
%%

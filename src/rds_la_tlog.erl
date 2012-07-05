%% Author: haoting.wq
%% Created: 2012-6-11
%% Description: TODO: Add description to rds_la_tlog

-module(rds_la_tlog).

%%% A Timestamp counted disc logger, which supports:
%%% * mutiple simultaneous readers and writers, all writings're
%%%   sychronized inside serverside thread while all readings 
%%%   take place at clientside
%%% * self maintained skiplist index to accelerate seeks
%%% * data files are splitted when it reaches the size limit
%%% * aged logs are cleaned periodically

-behavior(gen_server).

%% come from file.hrl
-record(file_descriptor,
	{module :: module(),     % Module that handles this kind of file
	 data   :: term()}).     % Module dependent data
%% come from prim_file.erl
-define(FILE_WRITE, 4).
-define(FILE_RESP_OK,          0).

-include("logger_header.hrl").
-include("rds_la_tlog.hrl").

-export([start_link/3, stop/1]).

-export([write_sync/3, write_async/3]).
-export([open_reader/2, open_reader/3, first/1, next/1, last/1, current/1,
         find/2, close_reader/1]).

-export([init/1, handle_call/3, handle_cast/2, 
         handle_info/2, terminate/2, code_change/3]).

-record(writer, {num, datfd, idxfd,
                 last_num,
                 last_offset,
                 last_termid = ?TERMID_NIL,
                 max_datfile_size}).

-record(read_scope, {num1, offset1, termid1 = ?TERMID_NIL,
                     num2, offset2, termid2 = ?TERMID_NIL}).

-record(reader, {dir, name, scope, num, datfd,
                 last_termid = ?TERMID_NIL}).

-record(fileinfo, {num,
                   start_offset, size, 
                   first_termid = ?TERMID_NIL,
                   last_termid = ?TERMID_NIL
                   }).

-record(state, {dir, name, config,
                fileinfos,
                writer, readers}).

-define(COLLECT_INTERVAL, 60000). % every minute
-define(DEFAULT_SKIP_INTERVAL, 128).
-define(DEFAULT_MAX_DATFILE_SIZE, 1073741824). % 1G per file
%-define(DEFAULT_MAX_DATFILE_SIZE, 1 * 1024 * 1024).

-define(DAT, "dat").
-define(IDX, "idx").
-define(FIRST_NUM, 1).

%%% publics


%%% Called by Supervisor

start_link(Dir, Name, Config) ->
    {ok, TLog} = gen_server:start_link(?MODULE, [Dir,Name,Config], []),
    init_done(TLog),
    {ok, TLog}.

init_done(TLog) ->
    call(TLog, init_done).

stop(TLog) ->
    cast(TLog, stop).

write_sync(TLog, TimeStamp, Term) ->
    call(TLog, {write, TimeStamp, Term}).

write_async(TLog, TimeStamp, Term) ->
    cast(TLog, {write, TimeStamp, Term}).

open_reader(Dir, Name) ->
    {ok, TLog} = rds_la_epoolsup:get_tlog(Dir, Name),
    open_reader(Dir, Name, TLog).

open_reader(Dir, Name, TLog) ->
    RRef = make_ref(),
    {ok, ReadScope} = call(TLog, {register_reader, RRef}),
    Reader = open_readerx(Dir, Name, ReadScope),
    put({tlog, RRef}, TLog),
    put({reader, RRef}, Reader),
    {ok, RRef}.

close_reader(RRef) ->
    case get({tlog, RRef}) of
        undefined ->
            {error, invalid_tlog_reader};
        TLog ->
            Reader = get({reader, RRef}),
            ok = close_readerx(Reader),
            ok = call(TLog, {unregister_reader, RRef}),
            _ = erase({tlog, RRef}),
            _ = erase({reader, RRef}),
            ok
    end.

first(RRef) ->
    get_reader(RRef, fun(Reader) -> Reader#reader.scope#read_scope.termid1 end).
            
last(RRef) ->
    get_reader(RRef, fun(Reader) -> Reader#reader.scope#read_scope.termid2 end).

current(RRef) ->
    get_reader(RRef, fun(Reader) -> Reader#reader.last_termid end).

next(RRef) ->
    get_reader(RRef,
        fun(Reader) ->
            {Reader1, Ret} = nextx(Reader),
            put({reader, RRef}, Reader1),
            Ret
        end
    ).

find(RRef, TermId = #termid{}) ->
    get_reader(RRef,
        fun(Reader) ->
            {Reader1, Ret} = findx(Reader, TermId),
            put({reader, RRef}, Reader1),
            Ret
        end
    );

find(RRef, TimeStamp) ->
    find(RRef, make_termid(TimeStamp, 0)).

%%% client side helpers

get_reader(RRef, Fun) ->
    case get({tlog, RRef}) of
        undefined -> {error, invalid_tlog_reader};
        _ ->
            Reader = get({reader, RRef}),
            Fun(Reader)
    end.

call(Pid, Msg) -> gen_server:call(Pid, Msg, infinity).
cast(Pid, Msg) -> gen_server:cast(Pid, Msg).

%%% Behavioral Interface

init([Dir,Name,Config]) ->
    State = #state{dir = Dir,
           name = Name,
           fileinfos = [],
           config = Config,
           readers = dict:new() },
    gen_server:cast(self(), init),
    {ok, State}.

handle_cast({write, TimeStamp, Term}, State) ->
    ?DEBUG("Will write Log: ~p~n", [Term]),
    {State1,_} = writex(State, TimeStamp, Term),
    {noreply, State1};

handle_cast(init, State) ->
    #state{ dir = Dir,
            name = Name,
            config = Config} = State,
    FileInfos = try
                    boot_recovery(Dir, Name, fast)
                catch
                    _:Error ->
                        ?WARN("fast boot recovery fail due to ~p, try full boot recovery", [Error]),
                        boot_recovery(Dir, Name, full)
                end,
    State1 =
    case FileInfos of
       [] -> 
           Num = ?FIRST_NUM,
           new_fileinfo(State#state{
            writer = open_writerx(Dir, Name, Num, ?TERMID_NIL, Config)}, Num);
       [LastFileInfo|Rest] ->
           LastTermId = if LastFileInfo#fileinfo.last_termid =:= ?TERMID_NIL ->
                             case Rest of
                                 [] -> ?TERMID_NIL;
                                 [SecondLastFileInfo|_] -> 
                                     SecondLastFileInfo#fileinfo.last_termid
                             end;
                           true ->
                               LastFileInfo#fileinfo.last_termid
                        end,
           State#state{fileinfos = FileInfos,
                writer = open_writerx(Dir, Name,
                                      LastFileInfo#fileinfo.num,
                                      LastTermId,
                                      Config)}
    end,
    erlang:send_after(?COLLECT_INTERVAL, self(), collect),
    ?DEBUG("tlog ~p ~p init done", [Dir, Name]),
%    process_flag(priority, high),
    {noreply, State1};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Event, State) ->
    {noreply, State}.

handle_call(init_done, _From, State) ->
    {reply, ok, State};

handle_call({register_reader,RRef}, _From, State = #state{writer = Writer}) ->
    ReadScope = get_read_scope(State),
    sync_writerx(Writer),
    {reply, {ok,ReadScope}, register_reader(State, RRef, ReadScope)};

handle_call({unregister_reader,RRef}, _From, State) ->
    case is_reader(State, RRef) of
        true ->
            {reply, ok, unregister_reader(State, RRef)};
        false ->
            {reply, {error, reader_not_exists}, State}
    end;

handle_call({write, TimeStamp, Term}, _From, State) ->
    {State1,_} = writex(State, TimeStamp, Term),
    {reply, ok, State1};

handle_call(_Event, _From, State)  ->
    {reply, ok, State}.

handle_info(collect, State) ->
    State1 = 
        case dict:size(State#state.readers) of
            0 -> collect(State);
            _ -> State
        end,
    erlang:send_after(?COLLECT_INTERVAL, self(), collect),
    {noreply, State1};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{writer = Writer}) ->
    %?DEBUG("rds_la_tlog stop for reason: ~p", [Reason]),
    case Writer of
        undefined -> ok;
        _ -> close_writerx(Writer), ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% internals

open_writerx(Dir, Name, Num, LastTermId, Config) ->
    MaxDatFileSize = proplists:get_value(max_datfile_size, Config,
                                         ?DEFAULT_MAX_DATFILE_SIZE),
    #writer{ num = Num,
             datfd = bitcask_nifs_open_file(Dir, Name, Num, ?DAT, [append]),
             idxfd = bitcask_nifs_open_idx(Dir, Name, [append]),
             last_termid = LastTermId,
             max_datfile_size = MaxDatFileSize
           }.

close_writerx(Writer) ->
    #writer{datfd = Datfd, idxfd = Idxfd} = Writer,
    ok = bitcask_nifs_close_file(Datfd),
    ok = bitcask_nifs_close_file(Idxfd).

reopen_writerx(Writer, Dir, Name, Num) ->
    sync_writerx(Writer),
    ?DEBUG("Writer Synced~n", []),
    bitcask_nifs_close_file(Writer#writer.datfd),
    ?DEBUG("Close Old Data Fd: ~p~n", [Writer#writer.datfd]),
    NDatfd = bitcask_nifs_open_file(Dir, Name, Num, ?DAT, [append]),
    ?DEBUG("Opened New Data Fd: ~p~n", [NDatfd]),
    Writer#writer{ num = Num, datfd = NDatfd}.

sync_writerx(#writer{datfd = Datfd, idxfd = Idxfd}) ->
    bitcask_nifs_sync(Datfd),
    bitcask_nifs_sync(Idxfd),
    ok.

writex(State = #state{writer = Writer}, TimeStamp, Term) ->
    #writer{num = Num,
            datfd = Datfd,
            idxfd = Idxfd,
            last_termid = LastTermId} = Writer,
    #termid{timestamp = LastTS, number = LastNO} = LastTermId,
    Offset = get_last_filesize(State),
    ?DEBUG("TimeStamp: ~p, LastTS: ~p", [TimeStamp, LastTS]),
    TermId =
        if TimeStamp =:= LastTS ->
               make_termid(LastTS, LastNO + 1);
           TimeStamp > LastTS ->
               ok = bitcask_nifs_write_idx(Idxfd, TimeStamp, Num, Offset),
               make_termid(TimeStamp, 0);
           true ->
               ?DEBUG("message ~p arrive out of order in file ~p/~p , omit", 
                     [TimeStamp, State#state.dir, State#state.name]),
               out_of_order
        end,
    ?DEBUG("TermId: ~p~n", [TermId]),
    case TermId of
        out_of_order -> {State, out_of_order};
        _ ->
            Delta = bitcask_nifs_write_file(Datfd, TermId, Term),
            Writer1 = Writer#writer{last_num = Num, last_offset = Offset, last_termid = TermId},
            State1 = update_last_fileinfo(State, Delta, TermId),
            {may_split(State1#state{writer = Writer1}), {ok, TermId}}
    end.

%termid_to_timestamp(TermId) -> TermId#termid.timestamp.

make_termid(TimeStamp, Number) ->
    #termid{timestamp = TimeStamp, number = Number}.

get_read_scope(#state{fileinfos = FileInfos, writer = Writer}) ->
    
    FirstFileInfo = lists:last(FileInfos),
    #fileinfo{num = Num1, start_offset = Offset1, first_termid = TermId1} = FirstFileInfo,
    #writer{last_num = Num2, last_offset = Offset2, last_termid = TermId2} = Writer,
    #read_scope{num1 = Num1, offset1 = Offset1, termid1 = TermId1,
                num2 = Num2, offset2 = Offset2, termid2 = TermId2}.

register_reader(State = #state{readers = Readers}, RRef, ReadScope) ->
    State#state{ readers = dict:store(RRef, ReadScope, Readers) }.

is_reader(#state{readers = Readers}, RRef) ->
    dict:is_key(RRef, Readers).

unregister_reader(State = #state{readers = Readers}, RRef) ->
    State#state{ readers = dict:erase(RRef, Readers)}.

open_readerx(Dir,Name,Scope) ->
    #read_scope{num1 = Num1, offset1=Offset1} = Scope,
    Datfd = open_file(Dir, Name, Num1, ?DAT, [read]),
    {ok, _} = file:position(Datfd, {bof, Offset1}),
    #reader{dir = Dir, name = Name, scope = Scope, 
            num = Num1, datfd = Datfd}.
    
close_readerx(Reader) ->
    #reader{datfd = Datfd} = Reader,
    ok = file:close(Datfd).

reopen_readerx(Reader, Num, Offset) ->
    #reader{dir = Dir, name = Name, num = OldNum,
            scope = Scope, datfd = Datfd} = Reader,
    #read_scope{num1 = Num1, offset1 = Offset1, 
                num2 = Num2, offset2 = Offset2} = Scope,
    if {Num, Offset} < {Num1, Offset1} ->
           {error, too_small};
       {Num, Offset} > {Num2, Offset2} ->
           {error, too_big};
       true ->
           case Num =:= OldNum of
               true ->
                   {ok, _} = file:position(Datfd, {bof, Offset}),
                   {ok, Reader};
               false ->
                   ok = file:close(Datfd),
                   Datfd1 = open_file(Dir, Name, Num, ?DAT, [read]),
                   {ok, _} = file:position(Datfd1, {bof, Offset}),
                   Reader1 = Reader#reader{num = Num, datfd = Datfd1},
                   {ok, Reader1}
           end
    end.

nextx(Reader) ->
    #reader{dir = Dir, name = Name, num = Num,
            scope = Scope, datfd = Datfd,
            last_termid = LastTermId} = Reader,
    #read_scope{termid2 = TermId2} = Scope,
    case LastTermId < TermId2 of 
        true ->
            case read_file(Datfd) of
                {ok, _, TermId, Binary} ->
                    {Reader#reader{last_termid = TermId},
                     {ok, {TermId#termid.timestamp,
                           binary_to_term(Binary)}}};
                eof ->
%                    {ok, Reader1} = reopen_readerx(Reader, Num+1, 0),
%                    nextx(Reader1);
                    case reopen_readerx(Reader, Num+1, 0) of
                        {ok, Reader1} ->
                            nextx(Reader1);
                        {error, Error} ->
                            ?ERROR("reopen reader error ~p, Reader ~n~p~n, Dest ~p, LastTermId~p, TermId2: ~p",
                                 [Error, Reader, {Num+1,0}, LastTermId, TermId2]),
                            {Reader, eof}
                    end;
                {error, Reason } = Error->
                    ?ERROR("read dat file ~s/~s error ~p", [Dir, Name, Reason]),
                    {Reader, Error} 
            end;
        false ->
            {Reader, eof}
    end.

findx(Reader, Dest) ->
    #reader{dir = Dir, name = Name, scope = Scope} = Reader,
    #read_scope{termid2 = TermId2} = Scope,
    if Dest > TermId2 ->
           {Reader, false};
       true ->
            IdxFd = open_idx(Dir, Name, [read]),
            case find_idx(IdxFd, Dest#termid.timestamp) of
                {ok, Num, Offset} ->
                    ok = file:close(IdxFd),
                    {ok, Reader1} = reopen_readerx(Reader, Num, Offset),
                    find1(Reader1, Dest);
                eof ->
                    ?ERROR("idx corrupted ~s/~s.IDX", [Dir, Name]),
                    ok = file:close(IdxFd),
                    {Reader, {error, idx_corrupted}};
                {error, Reason} = Error ->
                    ?ERROR("read idx file ~s/~s.IDX error ~p", [Dir, Name, Reason]),
                    ok = file:close(IdxFd),
                    {Reader, Error}
            end
    end.

find1(Reader, #termid{number = 0}) -> 
    {Reader#reader{last_termid = ?TERMID_NIL}, true};
find1(Reader, Dest) ->
    #reader{dir = Dir, name = Name,
            num = Num, datfd = Datfd} = Reader,
    case scan_file(Datfd) of
        {ok, Delta, TermId} ->
            if TermId < Dest ->
                   find1(Reader, Dest);
               true ->
                   {ok, _} = file:position(Datfd, {cur, -Delta}),
                   {Reader#reader{last_termid = ?TERMID_NIL}, true}
            end;
        eof ->
            {ok, Reader1} = reopen_readerx(Reader, Num+1, 0),
            find1(Reader1, Dest);
        {error, Reason } = Error->
            ?ERROR("scan dat file ~s/~s error ~p", [Dir, Name, Reason]),
            {Reader, Error} 
    end.

%%% operating fileinfo

get_last_filesize(#state{fileinfos = [LastFileInfo|_]}) ->
    #fileinfo{size = Size} = LastFileInfo,
    Size.

update_last_fileinfo(State = #state{fileinfos = [LastFileInfo|Rest]}, Delta, TermId) ->
    #fileinfo{size = Size, first_termid = FirstTermId} = LastFileInfo,
    LastFileInfo1 = if FirstTermId =:= ?TERMID_NIL ->
                           LastFileInfo#fileinfo{first_termid = TermId};
                       true ->
                           LastFileInfo
                    end,
    State#state{fileinfos = [LastFileInfo1#fileinfo{size = Size + Delta,
                                                    last_termid = TermId}
                            |Rest]}.

new_fileinfo(State = #state{fileinfos = FileInfos}, Num) ->
    FileInfo = #fileinfo{ num = Num, start_offset = 0, size = 0},
    State#state{fileinfos = [FileInfo|FileInfos]}.

%%% split/purge files

may_split(State = #state{fileinfos = [LastFileInfo|_],
    writer = #writer{max_datfile_size = MaxDatFileSize}})
  when LastFileInfo#fileinfo.size < MaxDatFileSize ->
    State;

may_split(State = #state{dir = Dir, name = Name, writer = Writer}) ->
    #writer{num = Num} = Writer,
    Num1 = Num + 1,
    Writer1 = reopen_writerx(Writer, Dir, Name, Num1),
    new_fileinfo(State#state{writer = Writer1}, Num1).

%%% boot recovery

%% do a quick check using index
boot_recovery(Dir,Name, fast) ->
    IndexedLastOffsets = check_idxfd(Dir, Name),
    Range = [case dict:find(Num, IndexedLastOffsets) of
                 {ok, LastOffset} ->
                     {Num, LastOffset};
                 error ->
                     {Num, 0}
             end
            || Num <- range(Dir, Name)],
    FileInfos = check_files(Dir,Name,Range),
    ?DEBUG("tlog ~p ~p boot fast recovery done", [Dir, Name]),
    FileInfos;

%% full check row by row
boot_recovery(Dir,Name, full) ->
    FileInfos = check_files(Dir,Name,[{Num, 0} || Num <- range(Dir, Name)]),
    %% TODO: rebuild index
    ?DEBUG("tlog ~p ~p boot full recovery done", [Dir, Name]),
    FileInfos.

check_idxfd(Dir, Name) ->
    Acc0 = dict:new(),
    case idx_exists(Dir, Name) of
        true ->
            IdxFd = open_idx(Dir, Name, [read]),
            Acc = check_idxfd(Dir, Name, IdxFd, 0, Acc0),
            ok = file:close(IdxFd),
            Acc;
        false ->
            Acc0
    end.
check_idxfd(Dir, Name, IdxFd, Size, Acc) ->
    case read_idx(IdxFd) of
        {ok, {_, Num,OFF}} ->
            {ok, Size1} = file:position(IdxFd, {cur, 0}),
            Acc1 = dict:store(Num, OFF, Acc),
            check_idxfd(Dir, Name, IdxFd, Size1, Acc1);
        eof -> Acc;
        {error, Reason} ->
            ?ERROR("read idx file ~s/~s.~s@~w error ~p, truncate reset content",
                   [Dir, Name, ?IDX, Size, Reason]),
            {ok, _} = file:position(IdxFd, {bof, Size}),
            ok = file:truncate(IdxFd),
            Acc
    end.

check_files(Dir, Name, Range) ->
    check_files(Dir, Name, Range, []).
    
check_files(_Dir, _Name, [], Acc) -> Acc;
check_files(Dir, Name, [{Num,StartCheckPoint}|Rest], Acc) ->
    FileInfo0 = #fileinfo{num = Num, start_offset = 0, size = 0},
    Fd = open_file(Dir, Name, Num, ?DAT, [read, write]),
    FileInfo = 
        if StartCheckPoint > 0 ->
               {ok, _, FirstTermId} = scan_file(Fd),
               {ok, _} = file:position(Fd, {bof, StartCheckPoint}),
               {ok, Delta, CheckPointTermId} = scan_file(Fd),
               check_datfd(Dir, Name, Num, Fd,
                           FileInfo0#fileinfo{size = StartCheckPoint + Delta,
                                              first_termid = FirstTermId,
                                              last_termid = CheckPointTermId});
           true ->
               check_datfd(Dir, Name, Num, Fd, FileInfo0)
        end,
    ok = file:close(Fd),
    check_files(Dir, Name, Rest, [FileInfo|Acc]).

check_datfd(Dir, Name, Num, Fd, FileInfo = #fileinfo{size = Size}) ->
    case scan_file(Fd) of
        {ok, Delta, TermId} ->
            FirstTermId = if FileInfo#fileinfo.first_termid =:= ?TERMID_NIL ->
                                 TermId;
                             true ->
                                 FileInfo#fileinfo.first_termid
                          end,
            check_datfd(Dir, Name, Num, Fd, 
                        FileInfo#fileinfo{size = Size + Delta,
                                          first_termid = FirstTermId,
                                          last_termid = TermId});
        eof ->
            FileInfo;
        {error, Reason} ->
            ?ERROR("read file ~s/~s@~w error ~p, truncate rest content",
                   [Dir, filenum_to_name(Name, ?DAT, Num), Size, Reason]),
            {ok, _} = file:position(Fd, {bof, Size}),
            ok = file:truncate(Fd),
            FileInfo
    end.

%%% clean aged records
collect(State) ->
    #state{dir = Dir, name = Name,
           config = _Config, fileinfos = FileInfos} = State,
    MaxKeptDays = rds_la_config:la_log_max_kept_days(),
    CurrentTimeStamp = rds_la_lib:universal_to_seconds(erlang:localtime()),
    OldTimeStamp = CurrentTimeStamp - MaxKeptDays*3600*24,
    
    case FileInfos of
        [] ->
            % no file at the creation time
            State;
        [LastFileInfo|Rest] ->
            % always keep the most recent file
            RFileInfos = lists:reverse(Rest),
            {RFileInfos1, RFileInfos2} =
                lists:splitwith(
                  fun(FileInfo) ->
                          LastTermId = FileInfo#fileinfo.last_termid,
                          LastTermId#termid.timestamp =< OldTimeStamp
                  end, RFileInfos),
            case RFileInfos1 of
                [] -> ok;
                _ ->
                    [
                     begin
                         FileName = filename:join(Dir,filenum_to_name(Name, ?DAT, Num)),
                         ?DEBUG("collect file ~p", [FileName]),
                         file:delete(FileName)
                     end
                    ||#fileinfo{num = Num} <- RFileInfos1]
            end,
            State#state{fileinfos = [LastFileInfo|lists:reverse(RFileInfos2)]}
        end.

%%% handle dat files

range(Dir, Name) ->
    lists:sort( [filename_to_num(FileName)
                || FileName <- filelib:wildcard(
                     filename:join(Dir, Name ++ "." ++ ?DAT ++ ".*"))]).

filenum_to_name(Name, Type, Num) ->
    Name ++ "." ++ Type ++ "." ++ io_lib:format("~6..0w", [Num]).

filename_to_num(FileName) ->
    Tokens = string:tokens(FileName, "."),
    {N, _} = string:to_integer(lists:last(Tokens)),
    N.

open_file(Dir, Name, Num, Type, Modes) ->
    Path = filename:join(Dir, filenum_to_name(Name, Type, Num)),
    RealModes = [binary, raw] ++ Modes,
    ?DEBUG("Open file: ~p~n", [Path]),
    case file:open(Path, RealModes) of
        {ok, Fd} -> Fd;
        {error, Reason} ->
            ?DEBUG("Failed Open for ~p~n", [Reason]),
            throw({tlog_open_file_error, Path, RealModes, Reason});
        Other ->
            ?DEBUG("Other: ~p~n", [Other]),
            throw(unknown_error)
    end.

scan_file(Fd) ->
    try case file:read(Fd, 4) of
            {ok, <<TS:32>>} ->
                {ok, <<NO:32>>} = file:read(Fd, 4),
                {ok, <<Size:32>>} = file:read(Fd, 4),
                file:position(Fd, {cur, Size}),
                {ok, <<255:8>>} = file:read(Fd, 1),
                {ok, Size + 13, make_termid(TS, NO)};
            eof -> eof
        end
    catch
        C:E -> {error, {C,E}}
    end.

read_file(Fd) ->
    try case file:read(Fd, 4) of
            {ok, <<TS:32>>} ->
                {ok, <<NO:32>>} = file:read(Fd, 4),
                {ok, <<Size:32>>} = file:read(Fd, 4),
                {ok, <<Binary/binary>>} = file:read(Fd, Size),
                {ok, <<255:8>>} = file:read(Fd, 1),
                {ok, Size + 13, make_termid(TS, NO), Binary};
            eof -> eof
        end
    catch
        C:E -> {error, {C,E}}
    end.

%%% handle idx file

idx_filename(Dir, Name) ->
    filename:join(Dir, Name ++ "." ++ ?IDX).

idx_exists(Dir, Name) ->
    filelib:is_file(idx_filename(Dir,Name)).

open_idx(Dir, Name, Modes) ->
    Path = idx_filename(Dir, Name),
    case file:open(Path, [binary, raw] ++ Modes) of
        {ok, Fd} -> Fd;
        {error, Reason} ->
            throw({tlog_open_idx_error, Dir, Name, Reason})
    end.

find_idx(Fd, TimeStamp) ->
    case read_idx(Fd) of
        {ok, {TS, Num, OFF}} ->
            if TS >= TimeStamp -> {ok, Num, OFF};
               true -> find_idx(Fd, TimeStamp)
            end;
        eof -> eof;
        Error -> Error 
    end.

read_idx(Fd) ->
   try case file:read(Fd, 16) of
           {ok, <<TS:32,Num:32,OFF:64>> } -> {ok, {TS, Num,OFF}};
           eof -> eof
       end
   catch
       C:E -> {error, {C,E}}
   end.

%% -----------------------------------------------------------

bitcask_nifs_open_file(Dir, Name, Num, Type, Modes) ->
    Path = filename:join(Dir, filenum_to_name(Name, Type, Num)),
    ?DEBUG("Open file: ~p~n", [Path]),
    case bitcask_nifs:file_open(Path, Modes) of
        {ok, Fd} -> Fd;
        {error, Reason} ->
            ?DEBUG("Failed Open for ~p~n", [Reason]),
            throw({tlog_open_file_error, Path, Modes, Reason});
        Other ->
            ?DEBUG("Other: ~p~n", [Other]),
            throw(unknown_error)
    end.

bitcask_nifs_open_idx(Dir, Name, Modes) ->
    Path = idx_filename(Dir, Name),
    case bitcask_nifs:file_open(Path, Modes) of
        {ok, Fd} -> Fd;
        {error, Reason} ->
            throw({tlog_open_idx_error, Dir, Name, Reason})
    end.

bitcask_nifs_close_file(FD) ->
    bitcask_nifs:file_close(FD).

bitcask_nifs_write(FD, Binary) ->
    bitcask_nifs:file_write(FD, Binary).

%bitcask_nifs_read(FD, Size) ->
%    case bitcask_nifs:file_read(FD, Size) of
%        {ok, Content} -> {ok, Content};
%        eof -> {error, eof};
%        {error, Reason} -> {error, Reason}
%    end.
            
bitcask_nifs_sync(FD) ->
    bitcask_nifs:file_sync(FD).

bitcask_nifs_write_idx(Fd, TimeStamp, Num, Offset) ->
    bitcask_nifs_write(Fd, <<TimeStamp:32, Num:32, Offset:64>>).

bitcask_nifs_write_file(Fd, #termid{timestamp = TS, number = NO}, Term) ->
    Binary = term_to_binary(Term),
    Size = byte_size(Binary),
    ok = bitcask_nifs_write(Fd, <<TS:32, NO:32, Size:32, Binary/binary, 255:8>>),
    Size + 13.



%-define(DEBUG(Format, Arg), io:format(Format ++ "~n", Arg)).
%-define(ERROR(Format, Arg), io:format(Format ++ "~n", Arg)).
%-define(WARN(Format, Arg), io:format(Format ++ "~n", Arg)).
%-define(INFO(Format, Arg), io:format(Format ++ "~n", Arg)).


-define(DEBUG(_Format, _Arg), ok).
-define(ERROR(_Format, _Arg), ok).
-define(WARN(_Format, _Arg), ok).
-define(INFO(_Format, _Arg), ok).

GCC=gcc
ERLANG_INCLUDE=/usr/local/lib/erlang/usr/include

$GCC -o priv/rds_la_analyze_nifs.so -fpic -shared -I$ERLANG_INCLUDE c_src/rds_la_analyze.c

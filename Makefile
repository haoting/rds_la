

ERLC = erlc
ERL = erl
GCC = gcc

all:
	$(GCC) -o priv/rds_la_analyze_nifs.so -fpic -shared -I/usr/local/lib/erlang/usr/include ./c_src/rds_la_analyze.c
	cp src/rds_la.app.src ebin/rds_la.app
	$(ERLC) -I include -pa ebin -o ebin src/*.erl

run:
	$(ERL) -pa ebin -config sys.config -args_file vm.args

clean:
	rm ebin/* -rf

cleandb:
	rm -f initialized_file
	rm -rf Mnesia.*
	rm -rf ladir/*

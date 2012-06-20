
REBAR=./rebar

compile:
	@$(REBAR) get-deps compile
	@$(REBAR) generate

clean:
	@$(REBAR) clean
	@rm -f test/*.beam
	@rm -f -r logs rel/rds_la
	@rm -f -r deps

build_plt:
	-@dialyzer -pa deps/mochiweb/ebin --build_plt \
	--apps erts kernel stdlib sasl mnesia compiler \
	crypto runtime_tools eunit mochiweb inets ssl \
	xmerl syntax_tools public_key sasl hipe os_mon tools

check:  compile
	-@${REBAR} xref
	-@dialyzer ebin --verbose -Wunmatched_returns -Werror_handling -Wrace_conditions

test:   compile
	@rm -rf .eunit
	@mkdir -p .eunit
	@$(REBAR) skip_deps=true eunit
	@$(REBAR) ct

run:
	@rel/rds_la/bin/rds_la console

eunit:
	@$(REBAR) skip_deps=true eunit


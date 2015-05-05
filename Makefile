REBAR = ./rebar

.PHONY: deps test doc

all: deps compile

clean:
	$(REBAR) clean

compile:
	$(REBAR) compile

compile-fast:
	$(REBAR) compile skip_deps=true

console:
	erl -pa deps/*/ebin/ -pa ebin/ -sname ooh_wee

deps:
	$(REBAR) get-deps

distclean: clean
	$(REBAR) delete-deps

test:
	$(REBAR) skip_deps=true ct

dialyzer: compile
	@dialyzer -Wno_undefined_callbacks

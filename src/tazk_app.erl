-module(tazk_app).

-behaviour(application).

-include("tazk.hrl").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    EnsureBasePath =
        fun(Pid) ->
                tazk_utils:did_create(
                  ezk:create(Pid, ?TAZK_BASE_PATH, <<>>)
                 )
        end,
    tazk_utils:with_connection(EnsureBasePath),
    tazk_sup:start_link().

stop(_State) ->
    ok.

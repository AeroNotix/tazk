-module(tazk_app).

-behaviour(application).

-include("tazk.hrl").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    AllPaths = [?TAZK_BASE_PATH, ?TAZK_LOCK_PATH,
                ?TAZK_WORKER_PATH, ?TAZK_RESULT_PATH],
    EnsureBasePath =
        fun(Pid) ->
                [begin
                     DidCreate = ezk:create(Pid, Path, <<>>),
                     tazk_utils:did_create(DidCreate)
                 end || Path <- AllPaths]
        end,
    tazk_utils:with_connection(EnsureBasePath),
    tazk_sup:start_link().

stop(_State) ->
    ok.

-module(tazk_task_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([start_child/1]).

-export([init/1]).

-define(SERVER, ?MODULE).
-define(CHILD_MOD, tazk_worker).


start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_child(TaskGroup) ->
    supervisor:start_child(?SERVER, [TaskGroup]).

init([]) ->
    RestartStrategy = simple_one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 2000,
    Type = worker,

    AChild = {?CHILD_MOD, {?CHILD_MOD, start_link, []},
              Restart, Shutdown, Type, [?CHILD_MOD]},

    {ok, {SupFlags, [AChild]}}.

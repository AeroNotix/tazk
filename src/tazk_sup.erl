-module(tazk_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    {ok, { {one_for_all, 5, 10},
           [?CHILD(tazk_master, worker),
            ?CHILD(tazk_task_sup, supervisor)]} }.

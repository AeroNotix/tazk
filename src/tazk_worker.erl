-module(tazk_worker).

-behaviour(gen_server).

-export([code_change/3]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([init/1]).
-export([start_link/1]).
-export([terminate/2]).


-define(SERVER, ?MODULE).
-define(WATCH_TAG, new_tasks).

-record(state, {
          zk_conn :: pid(),
          task_group :: binary(),
          pending_tasks :: queue(),
          in_flight_worker :: {pid(), {binary(), {module(), atom(), list()}}}
         }).

start_link(TaskGroup) ->
    gen_server:start_link(?MODULE, [TaskGroup], []).

init([TaskGroup]) ->
    {ok, Pid} = tazk_utils:create_connection(),
    case tazk:lock_task_group(Pid, TaskGroup) of
        ok ->
            PendingTasks = get_pending_tasks(Pid, TaskGroup),
            %% TODO: Kick off the first task here, monitor it for
            %% normal return, on normal return, mark it as done. The
            %% task itself should update some state somewhere to say
            %% it is done.
            {ok, #state{zk_conn=Pid, task_group=TaskGroup, pending_tasks=PendingTasks}};
        {error, lock_failed} ->
            %% TODO: Make this a gen_fsm which waits until some other
            %% joint crashes?  maybe if we rely on supervision trees
            %% to restart the other tazk_worker we should be ok?
            {stop, normal}
    end.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_pending_tasks(Pid, TaskGroup) ->
    {Base, _} = tazk:task_paths(TaskGroup),
    {ok, Tasks} = ezk:ls(Pid, Base, self(), ?WATCH_TAG),
    sort_tasks(Tasks).

sort_tasks(Tasks) ->
    io:format("~p~n", [Tasks]),
    queue:from_list(Tasks).

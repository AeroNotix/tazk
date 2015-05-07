-module(tazk_worker_spawner).

-behaviour(gen_server).

-export([reply/2]).
-export([code_change/3]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([init/1]).
-export([start_link/1]).
-export([terminate/2]).


-define(SERVER, ?MODULE).
-define(WATCH_TAG, new_tasks).
-define(WORKER_TASK_CHANGED, worker_task_changed).

-record(state, {
          zk_conn :: pid(),
          task_group :: binary(),
          pending_tasks :: queue(),
          in_flight_request :: list()
         }).

reply(Worker, Msg) ->
    gen_server:call(Worker, {reply, Msg}).

start_link(TaskGroup) ->
    gen_server:start_link(?MODULE, [TaskGroup], []).

init([TaskGroup]) ->
    {ok, Pid} = tazk_utils:create_connection(),
    case tazk:lock_task_group(Pid, TaskGroup) of
        ok ->
            PendingTasks = get_pending_tasks(Pid, queue:new(), TaskGroup),
            %% TODO: Kick off the first task here, monitor it for
            %% normal return, on normal return, mark it as done. The
            %% task itself should update some state somewhere to say
            %% it is done
            State0 = #state{zk_conn=Pid, task_group=TaskGroup, pending_tasks=PendingTasks},
            kick_off_next_task(State0);
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

handle_info({?WATCH_TAG, {_TaskPath, child_changed, _}},
           #state{zk_conn=Pid, task_group=TG, pending_tasks=PT, in_flight_request=IFR}=State) ->
    NextQueue = get_pending_tasks(Pid, PT, TG),
    NextState0 = State#state{pending_tasks=NextQueue},
    NextState1 =
        case IFR of
            undefined ->
                {ok, NS} = kick_off_next_task(NextState0),
                NS;
            _Other ->
                NextState0
        end,
    {noreply, NextState1};
handle_info({?WORKER_TASK_CHANGED, {TaskWorkerPath, node_deleted, _}=TaskInfo},
             #state{zk_conn=Pid, in_flight_request=TaskWorkerPath}=State) ->
    ResultPath = tazk:task_worker_path_to_result_path(TaskWorkerPath),
    case ezk:get(Pid, ResultPath) of
        {ok, {Data, _}} ->
            lager:error("Fkn result: ~p", [Data]),
            ok;
        {error, _} = E ->
            lager:error("Fkn result: ~p", [E]),
            ok
    end,
    lager:debug("Pending task completed", [TaskInfo]),
    {ok, NextState} = kick_off_next_task(State#state{in_flight_request=undefined}),
    {noreply, NextState}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_pending_tasks(Pid, CurrentTasks, TaskGroup) ->
    CTL = queue:to_list(CurrentTasks),
    {Base, _} = tazk:task_paths(TaskGroup),
    {ok, Tasks} = ezk:ls(Pid, Base, self(), ?WATCH_TAG),
    AllTasks = lists:append(CTL, Tasks),
    sort_tasks(lists:usort(AllTasks)).

sort_tasks(Tasks) ->
    SortTaskIds =
        fun(A, B) ->
                TaskIdA = get_task_id(A),
                TaskIdB = get_task_id(B),
                TaskIdA < TaskIdB
        end,
    SortedTasks = lists:sort(SortTaskIds, Tasks),
    queue:from_list(SortedTasks).

get_task_id(<<"task", Rest/binary>>) ->
    binary_to_integer(Rest).

kick_off_next_task(#state{zk_conn=Pid, task_group=TG, pending_tasks=PendingTasks,
                          in_flight_request=undefined}=State) ->
    case queue:out(PendingTasks) of
        {{value, Task}, RestPending} ->
            NextState = State#state{pending_tasks=RestPending},
            TaskPath = tazk:full_task_path(TG, Task),
            case ezk:get(Pid, TaskPath) of
                {ok, {Data, _}} ->
                    {M, F, A} = MFA = binary_to_term(Data),
                    case tazk_worker:start(TG, Task, M, F, A) of
                        {ok, _} ->
                            monitor_worker_node(Task, NextState);
                        {error, normal} ->
                            ok = delete_task(Task, NextState),
                            kick_off_next_task(NextState);
                        {error, _} = E ->
                            lager:error("Error spawning tazk_worker: ~p", [{E, MFA, TaskPath}]),
                            ok = delete_task(Task, NextState),
                            kick_off_next_task(NextState)
                    end;
                {error, _} = E ->
                    %% TODO handle disconnection
                    lager:error("Error getting data for tazk ~p~n", [E]),
                    {ok, NextState}
            end;
        {empty, EmptyQueue} ->
            {ok, State#state{pending_tasks=EmptyQueue, in_flight_request=undefined}}
    end.

monitor_worker_node(Task, #state{task_group=TaskGroup, zk_conn=Pid}=State) ->
    TaskWorkerPath = tazk:task_worker_path(TaskGroup, Task),
    case ezk:exists(Pid, TaskWorkerPath, self(), ?WORKER_TASK_CHANGED) of
        {ok, _} ->
            {ok, State#state{in_flight_request=TaskWorkerPath}};
        {error, no_dir} ->
            NextState = State#state{in_flight_request=undefined},
            case lookup_result_for_worker(Task, State) of
                {ok, _Result} ->
                    %% Post to TaskGroup notifications
                    ok = delete_task(Task, NextState),
                    {ok, NextState};
                {error, no_result} ->
                    ok = delete_task(Task, NextState),
                    {ok, NextState}
            end
    end.

delete_task(Task, #state{task_group=TG, zk_conn=Pid}) ->
    TaskPath = tazk:full_task_path(TG, Task),
    {ok, TaskPath} = ezk:delete(Pid, TaskPath),
    ok.

lookup_result_for_worker(_Task, _State) ->
    %% TODO Look up result, clear task.
    {ok, paulgay}.

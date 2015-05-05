-module(tazk_worker).

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

-record(state, {
          zk_conn :: pid(),
          task_group :: binary(),
          pending_tasks :: queue(),
          in_flight_request :: {pid(), {binary(), reference(), {module(), atom(), list()}}}
         }).

reply(Worker, Msg) ->
    gen_server:call(Worker, {reply, Msg}).

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
    NewPendingTasks = get_pending_tasks(Pid, TG),
    NextQueue = queue:join(PT, NewPendingTasks),
    NextState0 = State#state{pending_tasks=NextQueue},
    NextState1 =
        case IFR of
            undefined ->
                {ok, NS} = kick_off_next_task(NextState0),
                NS;
            %% the cloudi must flow. TODO: Turn into something more weidly
            {_, {_, _, {_, _, _}}} ->
                NextState0
        end,
    {noreply, NextState1};
handle_info({'DOWN', Ref, process, Pid, normal},
           #state{zk_conn=ZKPid, in_flight_request={Pid, {TaskPath, Ref, _}}}=State) ->
    {ok, TaskPath} = ezk:delete(ZKPid, TaskPath),
    NextState0 = State#state{in_flight_request=undefined},
    {ok, NextState1} = kick_off_next_task(NextState0),
    {noreply, NextState1};
handle_info({'DOWN', _, process, _}, State) ->
    %% Do we want to handle this like this? i.e. down messages from
    %% the not current IFR?
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
    SortTaskIds =
        fun(A, B) ->
                TaskIdA = get_task_id(A),
                TaskIdB = get_task_id(B),
                TaskIdA < TaskIdB
        end,
    SortedTasks = lists:sort(SortTaskIds, Tasks),
    io:format("~p~n", [SortedTasks]),
    queue:from_list(SortedTasks).

get_task_id(<<"task", Rest/binary>>) ->
    binary_to_integer(Rest).

kick_off_next_task(#state{zk_conn=Pid, task_group=TG, pending_tasks=PendingTasks}=State) ->
    %% TOOD: Learn how to program
    case queue:out(PendingTasks) of
        {{value, Task}, RestPending} ->
            NextState = State#state{pending_tasks=RestPending},
            TaskPath = tazk:full_task_path(TG, Task),
            case ezk:get(Pid, TaskPath) of
                {ok, {Data, _}} ->
                    case b2t(Data) of
                        undefined ->
                            io:format("No task data~n"),
                            %% TODO: Mark that shit as "no data" in
                            %% results path
                            {ok, TaskPath} = ezk:delete(Pid, TaskPath),
                            kick_off_next_task(NextState);
                        {M, F, A} ->
                            FullArgs = A ++ [self()],
                            case erlang:function_exported(M, F, 1) of
                                true ->
                                    %% TODO: Track this shit,
                                    %% {ok, Pid} -> monitor it and wait
                                    %% ok        -> expect reply by gen_server call
                                    try
                                        case M:F(FullArgs) of
                                            {ok, TaskPid} ->
                                                Ref = monitor(process, TaskPid),
                                                IFR =
                                                    {TaskPid, {TaskPath, Ref, {M, F, FullArgs}}},
                                                {ok, NextState#state{in_flight_request=IFR}};
                                            ok ->
                                                %% Do Something with a timer here
                                                {ok, NextState}
                                        end
                                    catch E:R ->
                                            io:format("crash when running: ~p~n", [{E,R, {M,F,A}}]),
                                            %% TODO: Mark that shit as "crashed" in
                                            %% results path
                                            {ok, TaskPath} = ezk:delete(Pid, TaskPath),
                                            kick_off_next_task(NextState)
                                    end;
                                false ->
                                    %% TODO: Mark that shit as
                                    %% unrunnable in results path
                                    io:format("Function doesn't exist: ~p~n", [{M, F, FullArgs}]),
                                    {ok, TaskPath} = ezk:delete(Pid, TaskPath),
                                    kick_off_next_task(NextState)
                            end
                    end;
                {error, _} = E ->
                    io:format("~p~n", [E]),
                    {ok, NextState}
            end;
        {empty, EmptyQueue} ->
            {ok, State#state{pending_tasks=EmptyQueue}}
    end.

b2t(<<>>) ->
    undefined;
b2t(B) when is_binary(B) ->
    binary_to_term(B).

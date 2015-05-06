-module(tazk_worker).

-behaviour(gen_server).

-export([code_change/3]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([init/1]).
-export([start/5]).
-export([terminate/2]).


-define(SERVER, ?MODULE).

-record(state, {
          zk_conn :: pid(),
          task_path :: list(),
          mfa :: {atom(), atom(), list()}
         }).

start(TaskGroup, Task, M,F,A) ->
    gen_server:start(?MODULE, [TaskGroup, Task, M,F,A], []).

init([TaskGroup, Task, M, F, A]) ->
    lager:debug("start task: ~p~n", [{Task, M, F, A}]),
    {ok, Pid} = tazk_utils:create_connection(),
    TaskWorkerPath = tazk:task_worker_path(TaskGroup, Task),
    case result_already_exists(Pid, TaskWorkerPath) of
        false ->
            NowMs = tic:now_to_epoch_msecs(),
            NodeBin = term_to_binary({node(), NowMs}),
            case ezk:create(Pid, TaskWorkerPath, NodeBin, e) of
                {ok, TaskWorkerPath} ->
                    self() ! start_task,
                    {ok, #state{task_path=TaskWorkerPath, zk_conn=Pid, mfa={M, F, A}}};
                {error, Reason} ->
                    {stop, Reason}
            end;
        true ->
            {stop, normal}
    end.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(start_task,
            #state{mfa={M, F, A}, zk_conn=Pid, task_path=TaskPath}=State) ->
    %% TODO: Handle reality
    try apply(M, F, A) of
        Anything ->
            write_result(Anything, TaskPath, State)
    catch
        E:R ->
            write_result({E, R}, TaskPath, State)
    after
        ok = ezk:end_connection(Pid, normal)
    end,
    {stop, normal, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

write_result(Term, TaskPath, #state{zk_conn=Pid}) ->
    ResultPath = tazk:task_worker_path_to_result_path(TaskPath),
    NowMs = tic:now_to_epoch_msecs(),
    TermBin = term_to_binary({Term, NowMs}),
    {ok, ResultPath} = ezk:create(Pid, ResultPath, TermBin),
    ok.

%% TODO refactor to take state with pid, not pid directly
result_already_exists(Pid, TaskPath) ->
    ResultPath = tazk:task_worker_path_to_result_path(TaskPath),
    case ezk:exists(Pid, ResultPath) of
        {error, no_dir} ->
            false;
        {ok, _} ->
            true
    end.

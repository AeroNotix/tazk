-module(tazk_worker).

-behaviour(gen_server).

-export([code_change/3]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([init/1]).
-export([start/4]).
-export([terminate/2]).


-define(SERVER, ?MODULE).

-record(state, {
          zk_conn :: pid(),
          task_path :: list(),
          mfa :: {atom(), atom(), list()}
         }).

start(Task, M,F,A) ->
    gen_server:start(?MODULE, [Task, M,F,A], []).

init([Task, M, F, A]) ->
    lager:debug("start task: ~p~n", [{Task, M, F, A}]),
    {ok, Pid} = tazk_utils:create_connection(),
    TaskWorkerPath = tazk:task_worker_path(Task),
    NodeBin = term_to_binary(node()),
    case ezk:create(Pid, TaskWorkerPath, NodeBin, e) of
        {ok, TaskWorkerPath} ->
            self() ! start_task,
            {ok, #state{task_path=TaskWorkerPath, zk_conn=Pid, mfa={M, F, A}}};
        {error, Reason} ->
            {stop, Reason}
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
            write_result({E,R}, TaskPath, State)
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
    TermBin = term_to_binary(Term),
    {ok, ResultPath} = ezk:create(Pid, ResultPath, TermBin),
    ok.

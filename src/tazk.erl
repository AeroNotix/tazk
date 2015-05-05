-module(tazk).

-include("tazk.hrl").

-export([submit/2]).
-export([delete_all_tasks/1]).
-export([ls/1]).


submit(TaskGroup, {M, F, A}=MFA)
  when is_list(TaskGroup) andalso is_atom(M) andalso is_atom(F)
       andalso is_list(A) ->
    case tazk_utils:create_connection() of
        {ok, Pid} ->
            try
                TaskData = term_to_binary(MFA),
                submit(Pid, TaskGroup, TaskData)
            after
                ezk:end_connection(Pid, normal)
            end;
        {error, no_server_reached} = E->
            E
    end.

submit(Pid, TaskGroup, TaskData)
  when is_pid(Pid) andalso is_list(TaskGroup) andalso is_binary(TaskData) ->
    {Base, Full} = task_paths(TaskGroup),
    case ezk:create(Pid, Base, <<>>) of
        {ok, Base} ->
            do_submit(Pid, Full, TaskData);
        {error, dir_exists} ->
            do_submit(Pid, Full, TaskData);
        {error, no_zk_connection} = E ->
            E
    end.

do_submit(Pid, TaskPath, TaskData) ->
    case ezk:create(Pid, TaskPath, TaskData, s) of
        {error, no_zk_connection} = E ->
            E;
        {ok, _} ->
            ok
    end.

ls(TaskGroup) ->
    {Base, _} = task_paths(TaskGroup),
    ListChildren = fun(Pid) -> ezk:ls(Pid, Base) end,
    tazk_utils:with_connection(ListChildren).

delete_all_tasks(TaskGroup) ->
    {Base, _} = task_paths(TaskGroup),
    DeleteTasks =
        fun(Pid) ->
                {ok, Tasks} = ezk:ls(Pid, Base),
                [ok = delete_task(Pid, Base, Task) || Task <- Tasks],
                ok
        end,
    tazk_utils:with_connection(DeleteTasks).


delete_task(Pid, Base, Task) when is_binary(Task) ->
    delete_task(Pid, Base, binary_to_list(Task));
delete_task(Pid, Base, Task) when is_list(Task) ->
    {ok, _} = ezk:delete(Pid, Base ++ "/" ++ Task),
    ok.

task_paths(P) when is_list(P) ->
    First = ?TAZK_BASE_PATH ++ "/" ++ P,
    Full = First ++ "/task",
    {First, Full}.

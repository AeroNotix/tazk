-module(tazk).

-include("tazk.hrl").

-export([delete_all_tasks/1]).
-export([delete_group/1]).
-export([lock_task_group/2]).
-export([task_paths/1]).
-export([full_task_path/2]).
-export([task_worker_path/2]).
-export([task_worker_path_to_result_path/1]).
-export([ls/1]).
-export([submit/2]).


submit(TaskGroup, {M, F, A}=MFA)
  when is_list(TaskGroup) andalso is_atom(M) andalso is_atom(F)
       andalso is_list(A) ->
    case erlang:function_exported(M, F, length(A)) of
        false ->
            {error, not_a_function};
        true ->
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
            end
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
                case ezk:ls(Pid, Base) of
                    {ok, Tasks} ->
                        [ok = delete_task(Pid, Base, Task) || Task <- Tasks],
                        ok;
                    {error, no_dir} ->
                        ok
                end
        end,
    tazk_utils:with_connection(DeleteTasks).

delete_group(TaskGroup) ->
    {Base, _} = task_paths(TaskGroup),
    DeleteGroup =
        fun(Pid) ->
                case ezk:delete(Pid, Base) of
                    {ok, Base} -> ok;
                    {error, no_dir} -> ok
                end
        end,
    tazk_utils:with_connection(DeleteGroup).

delete_task(Pid, Base, Task) when is_binary(Task) ->
    delete_task(Pid, Base, binary_to_list(Task));
delete_task(Pid, Base, Task) when is_list(Task) ->
    {ok, _} = ezk:delete(Pid, Base ++ "/" ++ Task),
    ok.

task_paths(TaskGroup) when is_list(TaskGroup) ->
    First = ?TAZK_BASE_PATH ++ "/" ++ TaskGroup,
    Full = First ++ "/task",
    {First, Full};
task_paths(TaskGroup) when is_binary(TaskGroup) ->
    task_paths(binary_to_list(TaskGroup)).

full_task_path(TaskGroup, TaskPath)  ->
    TGL = make_list(TaskGroup),
    TPL = make_list(TaskPath),
    ?TAZK_BASE_PATH ++ "/" ++ TGL ++ "/" ++ TPL.

lock_task_group(Pid, TaskGroup) ->
    Path = ?TAZK_LOCK_PATH ++ "/" ++ TaskGroup,
    case ezk:create(Pid, Path, <<>>, e) of
        {error, dir_exists} ->
            {error, lock_failed};
        {ok, _} ->
            ok
    end.

task_worker_path(TaskGroup, Task) ->
    %% TODO, make this return a nested path,
    %% needs more work to create before assigning work
    TL = make_list(Task),
    TGL = make_list(TaskGroup),
    ?TAZK_WORKER_PATH ++ "/" ++ TGL ++ TL.

task_worker_path_to_result_path("/tazk_workers/" ++ TId) ->
    ?TAZK_RESULT_PATH ++ "/" ++ TId.

make_list(B) when is_binary(B) ->
    binary_to_list(B);
make_list(L) when is_list(L) ->
    L.

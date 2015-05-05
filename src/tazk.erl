-module(tazk).

-export([submit/2]).
-export([delete_all_tasks/1]).
-export([ls/1]).


submit(TaskGroup, TaskData)
  when is_list(TaskGroup) andalso is_binary(TaskData) ->
    case tazk_utils:create_connection() of
        {ok, Pid} ->
            try
                submit(Pid, TaskGroup, TaskData)
            after
                ezk:end_connection(Pid, normal)
            end;
        {error, no_server_reached} = E->
            E
    end;
submit(TaskGroup, TaskData) when is_list(TaskGroup) ->
    submit(TaskGroup, term_to_binary(TaskData)).

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
            %% Here we should see if the node was created. It's
            %% sequential, how?
            E;
        {ok, _} ->
            ok
    end.

ls(TaskGroup) ->
    {Base, _} = task_paths(TaskGroup),
    {ok, Pid} = tazk_utils:create_connection(),
    try
        ezk:ls(Pid, Base)
    after
        ezk:end_connection(Pid, normal)
    end.

delete_all_tasks(TaskGroup) ->
    {ok, Pid} = tazk_utils:create_connection(),
    {Base, _} = task_paths(TaskGroup),
    {ok, Tasks} = ezk:ls(Pid, Base),
    try
        [ok = delete_task(Pid, Base, Task) || Task <- Tasks],
        ok
    after
        ezk:end_connection(Pid, normal)
    end.

delete_task(Pid, Base, Task) when is_binary(Task) ->
    delete_task(Pid, Base, binary_to_list(Task));
delete_task(Pid, Base, Task) when is_list(Task) ->
    {ok, _} = ezk:delete(Pid, Base ++ "/" ++ Task),
    ok.

task_paths(P) when is_list(P) ->
    First = "/" ++ P,
    Full = First ++ "/task",
    {First, Full}.

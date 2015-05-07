-module(tazk_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").


init_per_suite(Config) ->
    random:seed(now()),
    {ok, _} = application:ensure_all_started(ezk),
    Group = "test_group" ++ base64:encode_to_string(
                              crypto:rand_bytes(4)
                             ),
    ok = clear_test_tasks(),
    {ok, _} = application:ensure_all_started(tazk),
    %% We have to call the module first because otherwise
    %% erlang:function_exported/3 doesn't work.
    tazk_test_module:call(self(), foo),
    wait_for_n(foo, 1),
    [{num_tasks, 100}, {task_group, Group}] ++ Config.

all() ->
    [t_submit_tasks].

wait_for_n(_Value, 0) ->
    ok;
wait_for_n(Value, N) ->
    receive
        Value ->
            wait_for_n(Value, N-1)
    after
        1000 ->
            {error, timeout}
    end.

t_submit_tasks(Config) ->
    Group = ?config(task_group, Config),
    NumTasks = ?config(num_tasks, Config),
    ReplyWith = called,
    [ok = tazk:submit(Group, {tazk_test_module, call, [self(), ReplyWith]}) || _ <- lists:seq(1, NumTasks)],
    ok = wait_for_n(ReplyWith, NumTasks).

clear_test_tasks() ->
    {ok, Pid} = tazk_utils:create_connection(),
    case ezk:ls(Pid, "/tazk/tazk_tasks") of
        {ok, Tasks} ->
            [ok = delete_if_test_task(Pid, Task) || Task <- Tasks],
            ok;
        {error, no_dir} ->
            ok
    end.

delete_if_test_task(Pid, <<"test_group", _/binary>> = Task) ->
    {ok, _} = ezk:delete(Pid, "/tazk/tazk_tasks/" ++ binary_to_list(Task)),
    ok;
delete_if_test_task(_, _) ->
    ok.

-module(tazk_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

suite() ->
    [{timetrap,{seconds,30}}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(tazk),
    Group = "test_group",
    ok = tazk:delete_all_tasks(Group),
    [{num_tasks, 10}, {task_group, Group}] ++ Config.

all() ->
    [t_submit_tasks].

t_submit_tasks(Config) ->
    Group = ?config(task_group, Config),
    NumTasks = ?config(num_tasks, Config),
    [tazk:submit(Group, {erlang, node, []}) || _ <- lists:seq(1, NumTasks)],
    {ok, PendingTasks} = tazk:ls(Group),
    NumTasks = length(PendingTasks).

-module(tazk_master).

-behaviour(gen_server).

-include("tazk.hrl").

-export([code_change/3]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([init/1]).
-export([start_link/0]).
-export([terminate/2]).

-define(SERVER, ?MODULE).
-define(WATCH_TAG, new_task_group).

-record(state, {zk_conn :: pid(), seen :: set()}).


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, Pid} = tazk_utils:create_connection(),
    {ok, TaskGroups} = ezk:ls(Pid, ?TAZK_BASE_PATH, self(), ?WATCH_TAG),
    [ok = spawn_worker_for_group(TaskGroup) || TaskGroup <- TaskGroups],
    TaskSet = sets:from_list(TaskGroups),
    {ok, #state{zk_conn=Pid, seen=TaskSet}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({?WATCH_TAG, {?TAZK_BASE_PATH, child_changed, _}},
            #state{zk_conn=Pid, seen=Seen}=State) ->
    {ok, TaskGroups} = ezk:ls(Pid, ?TAZK_BASE_PATH, self(), ?WATCH_TAG),
    NewTasks = new_task_groups(Seen, TaskGroups),
    [ok = spawn_worker_for_group(TaskGroup) || TaskGroup <- NewTasks],
    NextSeen = update_task_groups(Seen, NewTasks),
    {noreply, State#state{seen=NextSeen}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

spawn_worker_for_group(TaskGroup) ->
    io:format("Spawning worker for task: ~p~n", [TaskGroup]),
    %% Spawn and link to workers here.
    ok.

new_task_groups(Current, New) ->
    sets:to_list(sets:subtract(sets:from_list(New), Current)).

update_task_groups(Current, New) ->
    lists:foldl(
      fun(E, S) ->
              sets:add_element(E, S)
      end, Current, New).

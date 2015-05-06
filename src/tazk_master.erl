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

-record(state, {
          zk_conn :: pid(),
          seen :: set()
         }).


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    %% The other supervision tree needs to be up.
    self() ! do_init,
    {ok, do_init}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({?WATCH_TAG, {?TAZK_BASE_PATH, child_changed, _}},
            #state{zk_conn=Pid, seen=Seen}=State) ->
    {ok, TaskGroups} = ezk:ls(Pid, ?TAZK_BASE_PATH, self(), ?WATCH_TAG),
    NewTasks = new_task_groups(Seen, TaskGroups),
    NextSeen =
        lists:foldl(fun spawn_workers/2, Seen, NewTasks),
    {noreply, State#state{seen=NextSeen}};
handle_info(do_init, do_init) ->
    {ok, Pid} = tazk_utils:create_connection(),
    {ok, NewTasks} = ezk:ls(Pid, ?TAZK_BASE_PATH, self(), ?WATCH_TAG),
    NextSeen  =
        lists:foldl(fun spawn_workers/2, sets:new(), NewTasks),
    {noreply, #state{zk_conn=Pid, seen=NextSeen}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

new_task_groups(Current, New) ->
    sets:to_list(sets:subtract(sets:from_list(New), Current)).

spawn_worker_for_group(TaskGroup) ->
    tazk_task_sup:start_child(TaskGroup).

spawn_workers(TaskGroup, SeenSet) ->
    case spawn_worker_for_group(TaskGroup) of
        {error, normal} ->
            lager:warning("Worker already exists for: ~p~n", [TaskGroup]),
            SeenSet;
        {ok, _WorkerPid} ->
            sets:add_element(TaskGroup, SeenSet)
    end.

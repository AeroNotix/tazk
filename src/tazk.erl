-module(tazk).

-export([submit/2]).


submit(Path, TaskData)
  when is_list(Path) andalso is_binary(TaskData) ->
    case create_connection() of
        {ok, Pid} ->
            submit(Pid, Path, TaskData);
        {error, no_server_reached} = E->
            E
    end;
submit(Path, TaskData) when is_list(Path) ->
    submit(Path, term_to_binary(TaskData)).

submit(Pid, Path, TaskData)
  when is_pid(Pid) andalso is_list(Path) andalso is_binary(TaskData) ->
    ok.

create_connection() ->
    Servers = application:get_env(zootils, zookeeper_servers, []),
    ezk:start_connection(Servers).

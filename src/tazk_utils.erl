-module(tazk_utils).

-export([create_connection/0]).

create_connection() ->
    Servers = application:get_env(tazk, zookeeper_servers, []),
    ezk:start_connection(Servers).

-module(tazk_utils).

-export([create_connection/0]).
-export([with_connection/1]).
-export([did_create/1]).

create_connection() ->
    Servers = application:get_env(tazk, zookeeper_servers, []),
    ezk:start_connection(Servers).

did_create({ok, _}) ->
    ok;
did_create({error, dir_exists}) ->
    ok.

with_connection(F) ->
    {ok, P} = tazk_utils:create_connection(),
    try
        F(P)
    after
        ezk:end_connection(P, normal)
    end.

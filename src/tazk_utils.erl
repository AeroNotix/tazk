-module(tazk_utils).

-export([create_connection/0]).
-export([with_connection/1]).
-export([did_create/1]).

create_connection() ->
    Servers = application:get_env(tazk, zookeeper_servers, []),
    %% we pass self() to it because it monitors this pid and closes
    %% the connection when we die.
    %% Further details: https://gist.github.com/AeroNotix/59b049ae8481eaeb88aa
    ezk:start_connection(Servers, [self()]).

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

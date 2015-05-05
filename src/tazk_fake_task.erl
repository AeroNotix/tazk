-module(tazk_fake_task).

-export([foo/1]).


foo([Arg, ReplyTo]) ->
    {ok, spawn(fun() ->
                       io:format("~p~n", [{Arg, ReplyTo}]),
                       tazk_worker:reply(ReplyTo, {everything, ok})
               end)}.

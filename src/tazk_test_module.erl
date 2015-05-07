-module(tazk_test_module).

-export([call/2]).


call(Pid, Msg) ->
    try
        Pid ! Msg
    catch
        E:R ->
            ct:pal("E: ~p", [{E,R}])
    end.

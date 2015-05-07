-module(bigset).

-compile([export_all]).

add_read() ->
    io:format("Adding to set~n"),
    ok = bigset_client:update(<<"m">>, [<<"rdb">>], []),
    io:format("reading from set~n"),
    Res = bigset_client:read(<<"m">>, []),
    io:format("Read result ~p~n", [Res]).

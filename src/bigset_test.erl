-module(bigset_test).

-compile(export_all).

test() ->
    {ok, A} = bigset_proto:start_link(a),
    {ok, B} = bigset_proto:start_link(b),
    {ok, RepA} = bigset_proto:insert(A, <<"set1">>, <<"russell">>),
    ok = bigset_proto:store_replica(B, <<"set1">>, RepA),
    Res = bigset_proto:get(B, <<"set1">>),
    io:format("B res ~p~n", [Res]),
    {ok, RepA2} = bigset_proto:insert(A, <<"set1">>, <<"bob">>),
    io:format("Rep2 ~p~n", [RepA2]),
    {ok, RepA3} = bigset_proto:insert(A, <<"set1">>, <<"sally">>),
    ok = bigset_proto:store_replica(B, <<"set1">>, RepA3),
    Res2 = bigset_proto:get(B, <<"set1">>),
    io:format("B res ~p~n", [Res2]),
    {ok, RepB} = bigset_proto:insert(B, <<"set1">>, <<"ali">>),
    io:format("RepB ~p~n", [RepB]),
    Res3 = bigset_proto:get(B, <<"set1">>),
    io:format("B res ~p~n", [Res3]),
    Res4 = bigset_proto:get(A, <<"set1">>),
    io:format("A res ~p~n", [Res4]).

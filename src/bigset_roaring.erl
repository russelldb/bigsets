-module(bigset_roaring).

-compile([export_all]).

new() ->
    eroaring:new().

serialize(R) ->
    eroaring:serialize(R).

union(R1, R2) ->
    eroaring:union(R1, R2).

add(Int, R) when is_integer(Int)  ->
    eroaring:add(R, Int).

add_all(L, R) when is_list(L) ->
    eroaring:add(R, L).

remove(Int, R) when is_integer(Int)  ->
    eroaring:remove(R, Int).

member(Int, R) when is_integer(Int) ->
    eroaring:contains(R, Int).

cardinality(R) ->
    eroaring:cardinality(R).

compress(R) ->
    eroaring:run_optimize(R).






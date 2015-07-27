-module(level_exper).

-compile([export_all]).

doit() ->
    %% open a level handle
    %% add a clock plus 100 elements for 3 sets
    %% get an iterator and see if they're sorted


    ok.

make_clock_key(Set) when is_binary(Set) ->
    Len = byte_size(Set),
    ClockElement = <<0:1, 0:1>>,
    %% <<Set length, Set, Element Length, Element, Actor lentgh, actor, counter, Tsb>>
    %% for the clock the element is zero bit, the actor is one zero bit, and the counter is zero.
    %5 The clock should always be the first element in the set
    <<Len:32/integer, Set:Len/binary, 2:32/integer, ClockElement:2, 1:32/integer , 0:1, 0:32/integer, 0:1>>.

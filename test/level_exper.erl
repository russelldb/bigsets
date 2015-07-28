-module(level_exper).

-compile([export_all]).

doit() ->
    %% open a level handle
    %% add a clock plus 100 elements for 3 sets
    %% get an iterator and see if they're sorted


    ok.

make_clock_key(Set) when is_binary(Set) ->
    Len = byte_size(Set),
    %% <<Set length, Set>>
    <<Len:32/integer, Set:Len/binary>>.

make_elem_key(Set, Elem, Actor, Dot, add) ->
    make_elem_key(Set, Elem, Actor, Dot, 0);
make_elem_key(Set, Elem, Actor, Dot, remove) ->
    make_elem_key(Set, Elem, Actor, Dot, 1);
make_elem_key(Set, Elem, Actor, Cntr, TSB) ->
    SetLen = byte_size(Set),
    ElemLen = byte_size(Elem),
    ActorLen = byte_size(Actor),
    <<SetLen:32/integer, Set:SetLen/binary,
      ElemLen:32/integer, Elem:ElemLen/binary,
      ActorLen:32/integer, Actor:ActorLen/binary,
      Cntr:32/integer, TSB:1>>.

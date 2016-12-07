%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% just some bitmasks
%%% @end
%%% Created : 23 Nov 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_masks).

-compile(export_all).

full_word(W) ->
    (1 bsl W) -1.

unset_from(V, From, W) ->
    Mask = unset_from(From, W),
    V band Mask.

unset_from(From, W) ->
    (1 bsl (From rem W)) -1.

unset_to(V, To, W) ->
    Mask = unset_to(To, W),
    V band Mask.

unset_to(To, W) ->
    bnot (1 bsl ((To rem W) +1) -1).

set_from(V, From, W) ->
    Mask = set_from(From, W),
    V bor Mask.

set_from(From, W) ->
    M = (1 bsl (W - (From rem W))) -1,
    M bsl (From rem W).

set_to(V, To, W) ->
    Mask = set_to(To, W),
    V bor Mask.

set_to(To, W) ->
    (1 bsl ((To rem W) +1) -1).

set(Value, I, WordSize) ->
    Value bor (1 bsl (I rem WordSize)).

unset(V, I, W) ->
    V band (bnot (1 bsl (I rem W))).

member(V, I, W) ->
    V band (1 bsl (I rem W)) =/= 0.

range_mask(Lo, Hi, W) ->
    bnot (unset_to(Lo-1, W) band unset_from(Hi+1, W)).

print(V, W) ->
    io:fwrite("~*.2.0B~n", [W,V]).

to_list(V) ->
    expand(V,0, 0, []).

expand(V, V,  _, Acc) ->
    lists:reverse(Acc);
expand(V, _VL, N, Acc) ->
    Acc2 =
        case (V band 1) of
            1 ->
                [N|Acc];
            0 ->
                Acc
        end,
    expand(V bsr 1, V, N+1, Acc2).

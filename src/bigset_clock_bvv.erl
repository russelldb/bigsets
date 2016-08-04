%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 29 Jul 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_clock_bvv).

-behaviour(bigset_gen_clock).

-export([add_dot/2,
         add_dots/2,
         all_nodes/1,
         complement/2,
         descends/2,
         dominates/2,
         equal/2,
         fresh/0,
         fresh/1,
         get_dot/2,
         increment/2,
         intersection/2,
         is_compact/1,
         merge/1,
         merge/2,
         seen/2,
         subtract_seen/2,
         to_bin/1
        ]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([make_dotcloud_entry/3]).
-endif.

fresh() ->
    swc_node:new().

fresh({Actor, Cnt}) ->
    swc_node:store_entry(Actor, {Cnt, 0}, swc_node:new()).

add_dot(Dot, Clock) ->
    swc_node:add(Clock, Dot).

add_dots(Dots, Clock) ->
    lists:foldl(fun(Dot, ClockAcc) ->
                        add_dot(Dot, ClockAcc)
                end,
                Clock,
                Dots).

all_nodes(Clock) ->
    swc_node:ids(Clock).

complement(_, _) ->
    ok.

descends(_, _) ->
    ok.

dominates(_, _) ->
    ok.

equal(A, B) ->
    A == B.

get_dot(Actor, Clock) ->
    {Base, 0} = swc_node:get(Actor, Clock),
    {Actor, Base}.

increment(Actor, Clock) ->
    {Cntr, Clock2} = swc_node:event(Clock, Actor),
    {{Actor, Cntr}, Clock2}.

intersection(_A, _B) ->
    ok.

is_compact(Clock) ->
    Ids = all_nodes(Clock),
    is_compact(Clock, Ids).

is_compact(_Clock, []) ->
    true;
is_compact(Clock, [Id | Rest]) ->
    case swc_node:get(Id, Clock) of
        {_N, 0} ->
            is_compact(Clock, Rest);
        {_, _} ->
            false
    end.

merge(Clocks) ->
    lists:foldl(fun merge/2,
                fresh(),
                Clocks).

merge(A, B) ->
    swc_node:merge(A, B).

seen(_Dot, _Clock) ->
    ok.

subtract_seen(_, _) ->
    [].

to_bin(Clock) ->
    term_to_binary(Clock, [compressed]).

-ifdef(TEST).

%% How big are clocks?
clock_size_test() ->
    bigset_gen_clock:clock_size_test(?MODULE).

make_dotcloud_entry(Clock, Actor, Events) ->
    {Base, DC}=_Entry = swc_node:get(Actor, Clock),

    DC2 = lists:foldl(fun(Event, DCAcc) ->
                              DCAcc bor (1 bsl (Event-Base-1))
                      end,
                      DC,
                      Events),

    E2 = swc_node:norm({Base, DC2}),

    swc_node:blind_store_entry(Actor, E2, Clock).

-endif.


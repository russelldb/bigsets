%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created :  1 Feb 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_clock_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).

%% @doc Property testing ...
-spec prop_descends() -> eqc:property().
prop_descends() ->
    ?FORALL({_Clock1, _Clock2}, clock_pair(),
            true).

clock_pair() ->
    ?LET(Actors, list(binary(8)), clocks(Actors)).

clocks(Actors) ->
    {clock(sublist(Actors)),
     clock(sublist(Actors))}.

clock(Actors) ->
    clock(Actors, bigset_clock:fresh()).

clock([], Clock) ->
    Clock;
clock([Actor | Rest], Acc) ->
    clock(Rest, gen_entry(Actor, Acc)).

gen_entry(Actor, Clock) ->
    {Actor, Clock}.

clock() ->
    ?LET(Base, base(),
         ?LET(DotCloud,
              list(?LET({A, B}, dot_seed(Base),
                        dot_list(A, B))),
              {Base, DotCloud})).

base() ->
    list(base_entry()).

dot_seed([]) ->
    empty_base();
dot_seed(L) ->
    oneof([elements(L),
           empty_base()]).

empty_base() ->
    {actor(), 0}.

base_entry() ->
    {actor(), gen_int(0)}.

dot_list() ->
    ?LET({Actor, Base}, base_entry(), dot_list(Actor, Base+2)).

actor() ->
    binary(8).

dot_list(Actor, Min) ->
    {Actor, ?LET(L, non_empty(list(gen_int(Min+2))), lists:usort(L))}.

gen_int(Min) ->
    ?SUCHTHAT(X, ?LET(I , frequency([{0, largeint()}, {1, int()}]), abs(I)), X > Min).

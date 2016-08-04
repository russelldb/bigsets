%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% generate some clocks for Paul
%%% @end
%%% Created : 20 Jan 2016 by Russell Brown <russelldb@basho.com>

-module(gen_clocks).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

prop_gen() ->
    ?FORALL({Clock1, Clock2}, {clock(), clock()}, begin
                                                      Merged = bigset_clock:merge(Clock1, Clock2),
                                                      dump_bigset_clocks_to_file(Clock1, Clock2, Merged),
                                                      true
                                                  end).


dump_bigset_clocks_to_file(Clock1, Clock2, Merged) ->
    file:write_file("merge_clocks", clocks(Clock1, Clock2, Merged), [append]).


clocks(C1, C2, C3) ->
    io_lib:format("~w : ~w : ~w~n", [C1, C2, C3]).

clock_bytes(Clock) ->
    Bin = term_to_binary(Clock),
    io_lib:format("~w : ~w~n", [Clock, Bin]).


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


%% actors(MaxActors, M) ->
%%     N = crypto:rand_uniform(1, MaxActors),
%%     [crypto:rand_bytes(M) || _ <- lists:seq(1, N)].

%% clock(MaxActors, ActorSizeBytes) ->
%%     Actors = actors(MaxActors, ActorSizeBytes),
%%     Base = base(Actors),
%%     DotCloud = dot_cloud(Base),
%%     {Base, DotCloud}.

-endif.


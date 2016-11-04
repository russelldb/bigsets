%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% stolen from bloom.erl and hashtree.erl
%%% @end
%%% Created : 29 Jul 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_bitarray).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-compile([export_all]).
-endif.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         from_list/1,
         get/2,
         is_empty/1,
         is_subset/2,
         member/2,
         union/2,
         new/1,
         resize/1,
         set/2,
         set_all/2,
         size/1,
         to_list/1,
         unset/2
        ]).

-export_type([bit_array/0]).

 %% @TODO What size is "best"?
-define(W, 128).

-type bit() :: 0 | 1.
-type bit_array() :: array:array(bit()).

%%%===================================================================
%%% bitarray
%%%===================================================================

-spec new(integer()) -> bit_array().
new(N) ->
     array:new([{size, (N-1) div ?W + 1}, {default, 0}, {fixed, false}]).

-spec set(integer(), bit_array()) -> bit_array().
set(I, A) ->
    AI = I div ?W,
    V = array:get(AI, A),
    V1 = V bor (1 bsl (I rem ?W)),
    array:set(AI, V1, A).

-spec unset(pos_integer(), bit_array()) -> bit_array().
unset(I, A) ->
    AI = I div ?W,
    V = array:get(AI, A),
    V1 = V band (bnot (1 bsl (I rem ?W))),
    array:set(AI, V1, A).

-spec set_all([pos_integer()], bit_array()) -> bit_array().
set_all(Ints, A) ->
    lists:foldl(fun(I, Acc) ->
                        set(I, Acc)
                end,
                A,
                Ints).

-spec get(integer(), bit_array()) -> boolean().
get(I, A) ->
    AI = I div ?W,
    V = array:get(AI, A),
    V band (1 bsl (I rem ?W)) =/= 0.

-spec size(bit_array()) -> pos_integer().
size(A) ->
    array:sparse_foldl(fun(I, V, Acc) ->
                              cnt(V, I * ?W, Acc)
                      end,
                      0,
                      A).

-spec is_empty(bit_array()) -> boolean().
is_empty(A) ->
    array:sparse_size(A) == 0.

-spec member(pos_integer(), bit_array()) -> boolean().
member(I, A) ->
    get(I, A).

-spec to_list(bit_array()) -> [integer()].
to_list(A) ->
    lists:reverse(
      array:sparse_foldl(fun(I, V, Acc) ->
                                 expand(V, I * ?W, Acc)
                         end, [], A)).

from_list([]) ->
    new(10);
from_list(L) ->
    set_all(L, new(lists:max(L))).


%% @doc union too bit_array's into a single bit_array. Set
%% union. NOTE: this assumes the same word size!
-spec union(bit_array(), bit_array()) -> bit_array().
union(A, B) ->
    NewSize = max(array:sparse_size(A), array:sparse_size(B)),
    %% Assume same size words
    {MergedA, RemainingB0} = array:sparse_foldl(fun(I, VA, {MergedA, RemainingB}) ->
                                                       VB = array:get(I, B),
                                                       MergedV = VA bor VB,
                                                       {array:set(I, MergedV, MergedA),
                                                        array:reset(I, RemainingB)}
                                               end,
                                               {new(NewSize), B},
                                               A),
    RemainingB = array:resize(RemainingB0),
    array:sparse_foldl(fun(I, VB, Acc) ->
                               array:set(I, VB, Acc)
                       end,
                       RemainingB,
                       MergedA).

%% @doc return true if `A' is a subset of `B', false otherwise.
-spec is_subset(bit_array(), bit_array()) -> boolean().
is_subset(A, B) ->
    %% Assumes same word size. A ⊂ B = A ∪ B == B, right?  Fold over
    %% the words, and as soon as the union of any pair of words is not
    %% == to word, throw. We fold over A
    (catch array:sparse_foldl(fun(I, VA, true) ->
                                      VB = array:get(I, B),
                                      if (VA bor VB) == VB ->
                                              true;
                                         true ->
                                              throw(false)
                                      end
                              end,
                              true,
                              A)).

%% @doc resize ensures that unset values are no longer accessible, and
%% take up no space.
-spec resize(bit_array()) -> bit_array().
resize(A) ->
    array:resize(A).

%%  Convert bit vector into list of integers, with
%% optional offset.  expand(2#01, 0, []) -> [0] expand(2#10, 0, []) ->
%% [1] expand(2#1101, 0, []) -> [3,2,0] expand(2#1101, 1, []) ->
%% [4,3,1] expand(2#1101, 10, []) -> [13,12,10] expand(2#1101, 100,
%% []) -> [103,102,100]
        expand(0, _, Acc) ->
                    Acc;
        expand(V, N, Acc) ->
                    Acc2 =
                        case (V band 1) of
                            1 ->
                                [N|Acc];
                            0 ->
                                Acc
                        end,
                    expand(V bsr 1, N+1, Acc2).

%% Same as above but the acc is a running total
cnt(0, _, Acc) ->
    Acc;
cnt(V, N, Acc) ->
    Acc2 =
        case (V band 1) of
            1 ->
                Acc +1;
            0 ->
                Acc
        end,
    cnt(V bsr 1, N+1, Acc2).

-ifdef(TEST).
set_test() ->
    A = new(10),
    A2 = set(100, A),
    ?assert(get(100, A2)).

unset_test() ->
    A = new(10),
    A2 = set(100, A),
    A3 = set(101, A2),
    ?assert(get(100, A3)),
    ?assert(get(101, A3)),
    A4 = unset(100, A3),
    ?assertNot(get(100, A4)),
    ?assert(get(101, A4)).

-endif.

-ifdef(EQC).

prop_union() ->
    ?FORALL({SetA, SetB}, {gen_set(), gen_set()},
            begin
                BS1 = from_list(SetA),
                BS2 = from_list(SetB),
                U = union(BS1, BS2),
                L = to_list(U),
                Expected = lists:umerge(SetA, SetB),
                measure(length_a, length(SetA),
                        measure(length_b, length(SetB),
                                measure(sparsity_a, sparsity(SetA),
                                        measure(sparsity_b, sparsity(SetB),
                                                measure(sparsity_u, sparsity(Expected),
                                                        measure(length_u, length(Expected),
                                                                L == Expected))))))
            end).

prop_subset() ->
    ?FORALL({SetA, SetB}, frequency([{1, {gen_set(), gen_set()}},
                                     {1, gen_subset_pair()}]),
            begin
                BSA= from_list(SetA),
                BSB = from_list(SetB),
                IsSubset = lists:umerge(SetA, SetB) == SetB,
                aggregate([IsSubset],
                          IsSubset == is_subset(BSA, BSB))
            end).


prop_list() ->
    ?FORALL(Set, gen_set(),
            begin
                BA = from_list(Set),
                measure(length, length(Set),
                        measure(sparsity, sparsity(Set),
                                to_list(BA) == Set))
            end).

%% Generate a set of pos_integer()
-spec gen_set() -> [pos_integer()].
gen_set() ->
    ?LET(Size, choose(1, 10000),
         ?LET(L, lists:seq(1, Size), sublist(L))).

%% Generate a pair of sets where A is a subset of B
-spec gen_subset_pair() -> {list(pos_integer()),
                            list(pos_integer())}.
gen_subset_pair() ->
    ?LET(Super, gen_set(), {sublist(Super), Super}).

%% @private a rough measure of how sparse a set is. Take the max
%% element and divide it by the size of the set.
-spec sparsity(list()) -> pos_integer().
sparsity([]) ->
    1;
sparsity(Set) ->
    lists:max(Set) div length(Set).

-endif.

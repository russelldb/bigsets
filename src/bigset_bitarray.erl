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
         unset/2,
         subtract/2,
         subtract_range/2
        ]).

-export_type([bit_array/0]).

 %% @TODO What word size is "best"?
-define(W, 128).
-define(FULL_WORD, ((1 bsl ?W) -1)).

-type bit() :: 0 | 1.
-type bit_array() :: array:array(bit()).
-type range() :: {pos_integer(), pos_integer()}.

%%%===================================================================
%%% bitarray
%%%===================================================================

-spec new(integer()) -> bit_array().
new(N) ->
     array:new([{size, (N-1) div ?W + 1}, {default, 0}, {fixed, false}]).

%% @doc create a bitset from a range, inclusive
-spec from_range(range()) -> bit_array().
from_range({Lo, Hi}) when Lo > Hi ->
    new(10);
    %% create an add mask for each word contained in the range full
    %% words are just (1 bsl ?W) -1.  part of a word is the bnot of
    %% the range to unset a part used in subtract range.
from_range({Lo, Hi}) when Lo =< Hi, (Lo div ?W) == (Hi div ?W) ->
    %% range is all in same word
    set_word_from_to(Lo, Hi, new(?W));
from_range({Lo, Hi}) when Lo =< Hi, (Hi div ?W) - (Lo div ?W) == 1 ->
    %% contiguous words
    A2 = set_word_from(Lo, new(?W)),
    set_word_to(Hi, A2);
from_range({Lo, Hi}) when Lo =< Hi ->
    %% some full words to be set
    A2 = set_word_from(Lo, new(?W)),
    A3 = set_word_to(Hi, A2),
    set_range((Lo div ?W) +1, (Hi div ?W) -1, A3).

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


%% @doc union two bit_arrays into a single bit_array. Set
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

%% @doc return only the elements in `A' that are not also elements of
%% `B'.
-spec subtract(bit_array(), bit_array()) -> bit_array().
subtract(A, B) ->
    %% Assumes same word size.
    array:sparse_foldl(fun(I, VA, Comp) ->
                               VB = array:get(I, B),
                               VComp = (VA band (bnot VB)),
                               array:set(I, VComp, Comp)
                       end,
                       array:new(),
                       A).

%% @doc given a `range()' and a `bit_array()' returns a `bit_array()'
%% of all the elements in `range()' that are not also in `B'. It's
%% subtract(from_range(Range), B).
-spec range_subtract(range(), bit_array()) -> bit_array().
range_subtract(Range, B) ->
    A = from_range(Range),
    subtract(A, B).

%% @doc return only the elements in `A' that are not in the range
%% defined by `Range' inclusive.
-spec subtract_range(bit_array(), range()) -> bit_array().
subtract_range(A, {Lo, Hi}) when Lo =< Hi, (Lo div ?W) == (Hi div ?W) ->
    %% range is all in same word
    unset_word_from_to(Lo, Hi, A);
subtract_range(A, {Lo, Hi}) when Lo =< Hi, (Hi div ?W) - (Lo div ?W) == 1 ->
    %% contiguous words
    A2 = unset_word_from(Lo, A),
    unset_word_to(Hi, A2);
subtract_range(A, {Lo, Hi}) when Lo =< Hi ->
    %% some full words to be unset
    A2 = unset_word_from(Lo, A),
    A3 = unset_word_to(Hi, A2),
    unset_range((Lo div ?W) +1, (Hi div ?W) -1, A3);
subtract_range(_Range, BS) ->
    %% If Lo > Hi there is no range to subtract
    BS.

%% remove a range that is within one word
unset_word_from_to(Lo, Hi, A) ->
    case {word_start(Lo), word_end(Hi)} of
        {true, true} ->
            %% the range _is_ the word, so reset it
            array:reset(Lo div ?W, A);
        {true, false} ->
            %% Lo is the word start, remove everything from start to
            %% Hi
            unset_word_to(Hi, A);
        {false, true} ->
            %% Hi is the word end, remove everything from Lo to the
            %% end.
            unset_word_from(Lo, A);
        _ ->
            %% we need to remove from Lo to Hi
            Mask = range_mask(Lo, Hi),
            apply_mask(Mask, Lo div ?W, A)
    end.

%% set a range that is within one word
set_word_from_to(Lo, Hi, A) ->
    case {word_start(Lo), word_end(Hi)} of
        {true, true} ->
            %% the range _is_ the word, so full word it
            array:set(Lo div ?W, ?FULL_WORD, A);
        {true, false} ->
            %% Lo is the word start, add everything from start to
            %% Hi
            set_word_to(Hi, A);
        {false, true} ->
            %% Hi is the word end, add everything from Lo to the
            %% end.
            set_word_from(Lo, A);
        _ ->
            %% we need to add from Lo to Hi
            Mask = bnot range_mask(Lo, Hi),
            apply_smask(Mask, Lo div ?W, A)
    end.

unset_word_to(Hi, A) ->
    Mask = to_mask(Hi),
    apply_mask(Mask, Hi div ?W, A).

unset_word_from(Lo, A) ->
    Mask = from_mask(Lo),
    apply_mask(Mask, Lo div ?W, A).

set_word_to(Hi, A) ->
    Mask = (1 bsl ((Hi rem ?W) +1) -1),
    apply_smask(Mask, Hi div ?W, A).

set_word_from(Lo, A) ->
    M = (1 bsl (?W - (Lo rem ?W))) -1,
    Mask = M bsl (Lo rem ?W),
    apply_smask(Mask, Lo div ?W, A).

set_from_mask(Lo) ->
    M = (1 bsl (?W - (Lo rem ?W))) -1,
    M bsl (Lo rem ?W).

apply_mask(Mask, Index, Array) ->
    Value0 = array:get(Index, Array),
    Value = Mask band Value0,
    array:set(Index, Value, Array).

apply_smask(Mask, Index, Array) ->
    Value0 = array:get(Index, Array),
    Value = Mask bor Value0,
    array:set(Index, Value, Array).

%% clear all the bits in the word in the range Lo - Hi, inclusive
range_mask(Lo, Hi) ->
    ToMask = to_mask(Lo-1), %% can't be < 0, see unset_word_from_to
    FromMask = from_mask(Hi+1), %% can't be > ?W, see unset_word_from_to
    bnot ( FromMask band ToMask).

%% clear all the bits in the word from N up, inclusive
from_mask(N) ->
     (1 bsl (N rem ?W)) - 1.

%% clear all the bits in the word up to N, inclusive
to_mask(N) ->
    bnot (1 bsl  ((N rem ?W) +1)  - 1).

%% Reset full words from Lo to Hi inclusive.
unset_range(Lo, Lo, A) ->
    array:reset(Lo, A);
unset_range(Lo, Hi, A) ->
    unset_range(Lo+1, Hi, array:reset(Lo, A)).

%% set full words from Lo to Hi inclusive.
set_range(Lo, Lo, A) ->
    array:set(Lo, ?FULL_WORD, A);
set_range(Lo, Hi, A) ->
    set_range(Lo+1, Hi, array:set(Lo, ?FULL_WORD, A)).


%% TODO: if these are only used in dotclouds, then 2 is the word start
%% of the first word, not zero.
word_start(N) when (N rem ?W) == 0 ->
    true;
word_start(_) ->
    false.

word_end(N) when (N rem ?W) == ?W-1 ->
    true;
word_end(_) ->
    false.

%% @doc resize ensures that unset values are no longer accessible, and
%% take up no space.
-spec resize(bit_array()) -> bit_array().
resize(A) ->
    array:resize(A).

%%  Convert bit vector into list of integers, with
%% optional offset.expand(2#01, 0, []) -> [0] expand(2#10, 0, []) ->
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

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_test_() ->
    {timeout, 60*8, [
                     {timeout, 60, ?_assertEqual(true,
                                                         eqc:quickcheck(eqc:testing_time(Time, ?QC_OUT(Prop()))))} ||
                        {Time, Prop} <- [{30, fun prop_subtract_range/0},
                                         {30, fun prop_range_subtract/0},
                                         {5, fun prop_unset_from_mask/0},
                                         {5, fun prop_unset_to_mask/0},
                                         {5, fun prop_unset_range_mask/0},
                                         {10, fun prop_union/0},
                                         {10, fun prop_subset/0},
                                         {5, fun prop_list/0},
                                         {10, fun prop_subtract/0},
                                         {30, fun prop_from_range/0},
                                         {5, fun prop_set_from_mask/0}]
                    ]}.


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

%% test that from mask removes all bits from, and including, the Nth
%% bit to the end of the word
from_mask_test() ->
    Offset = 0 * ?W,
    Members = lists:seq(Offset, (Offset+100)),
    %% Set values into word bitset
    Value = lists:foldl(fun(I, Acc) ->
                                Acc bor (1 bsl (I rem ?W))
                        end,
                        0,
                        Members),
    ?assertEqual(Members, lists:reverse(expand(Value, Offset, []))),

    From = 10,
    Mask = from_mask(Offset+From),
    Value2 = Mask band Value,
    ?assertEqual(lists:seq(Offset, Offset+(From-1)), lists:reverse(expand(Value2, Offset, []))).

%% test that to mask removes all bits set up to, and including the Nth
%% bit
to_mask_test() ->
    Offset = 10 * ?W,
    Members = lists:seq(Offset, (Offset+100)),
    %% Set values into word bitset
    Value = lists:foldl(fun(I, Acc) ->
                                Acc bor (1 bsl (I rem ?W))
                        end,
                        0,
                        Members),
    ?assertEqual(Members, lists:reverse(expand(Value, Offset, []))),

    To = 95,
    Mask = to_mask(Offset+To),
    Value2 = Mask band Value,
    ?assertEqual(lists:seq(Offset+To+1, Offset+100), lists:reverse(expand(Value2, Offset, []))).

-endif.

-ifdef(EQC).

-define(MAX_SET, 10000).
%% EQC Props


%% @doc property that tests that we can make a bitarry that contains
%% the range `{Hi, Lo}'. A lot like from list if lists:seq(Lo, Hi).
prop_from_range() ->
    ?FORALL({Lo, Hi}=Range, random_range(),
            begin
                Expected = lists:seq(Lo, Hi),
                BS = from_range(Range),
                to_list(BS) == Expected
            end).

%% @doc property that tests that for any range `{Lo, Hi}' and any
%% bitset, the returned bitset from `subtract_range' has removed all
%% entries in the bitset as though `range' where a bitset of
%% contiguous values. Bitset subtracts Range.
prop_subtract_range() ->
    ?FORALL({Set, {Lo, Hi}=Range}, {gen_set(), gen_range()},
            begin
                BS = from_list(Set),
                Expected = lists:filter(fun(E) -> E < Lo orelse E > Hi end, Set),
                Actual = to_list(subtract_range(BS, Range)),
                ?WHENFAIL(
                   begin
                       io:format("range ~p~n", [Range]),
                       io:format("Set ~p~n", [Set]),
                       print_difference(Expected, Actual)
                   end,
                   aggregate(with_title(range_out_of_bounds), range_out_of_bounds(Range, Set),
                             aggregate(with_title(range_removes_all), [[] == Expected],
                                       measure(set_before, length(Set),
                                               measure(set_after, length(Expected),
                                                       measure(range, Hi-Lo,
                                                               equals(Expected, Actual)))))))
            end).

%% @doc property that tests that for any range `{Lo, Hi}' and any
%% bitset, the returned bitset from `range_subtract' has removed all
%% entries in the bister as though `range' where a bitset of
%% contiguous values that subtracted the bitset.
prop_range_subtract() ->
    ?FORALL({Set, {Lo, Hi}=Range}, {gen_set(), gen_range()},
            begin
                BS = from_list(Set),
                B = lists:seq(Lo, Hi),
                Expected = ordsets:subtract(B, Set),
                Actual = to_list(range_subtract(Range, BS)),
                ?WHENFAIL(
                   begin
                       io:format("range ~p~n", [Range]),
                       io:format("Set ~p~n", [Set]),
                       print_difference(Expected, Actual)
                   end,
                   aggregate(with_title(bitset_removes_all), [[] == Expected],
                             measure(set_before, length(B),
                                     measure(set_after, length(Expected),
                                             measure(range, Hi-Lo,
                                                     equals(Expected, Actual))))))
            end).

%% @private prettty print the rogue elements between expected and
%% actual from prop_subtract_range/0
print_difference(Expected, Actual) ->
    io:format("in Expected not in Actual ~p~n", [ordsets:subtract(Expected, Actual)]),
    io:format("in Actual not in Expected ~p~n", [ordsets:subtract(Actual, Expected)]).

range_out_of_bounds({_Lo, _Hi}, []) ->
    [true];
range_out_of_bounds({Lo, Hi}, Set) ->
    [Lo < lists:min(Set) orelse Hi > lists:max(Set)].

%% @doc generate a range {Lo, Hi} so that Lo =< Hi. Lo and Hi both are
%% between 1, ?MAX_SET. ?MAX_SET is the size of the set in gen_set @TODO
%% word range boundary reset tests coverage shows we rarely generate
%% ranges: in one word, that start on a word, that end on a word.
gen_range() ->
    oneof([random_range(),
           word_start_range(),
           word_end_range(),
           one_word_range()
          , whole_word_range()
          ]).

word_start_range() ->
    ?LET(Lo, choose(0, ?MAX_SET div ?W), {Lo * ?W, choose(Lo * ?W, ?MAX_SET)}).

word_end_range() ->
    ?LET(Hi, choose(1, ?MAX_SET div ?W), {choose(0, (Hi * ?W) -1), (Hi * ?W) -1}).

one_word_range() ->
    ?LET({Range0, Mult},
         {[choose(1, ?W-1), choose(1, ?W-1)], choose(1, ?MAX_SET div ?W)},
         list_to_tuple(lists:sort(lists:map(fun(E) ->
                                                    E * Mult end, Range0)))).

whole_word_range() ->
    ?LET(Mult, choose(0, ?MAX_SET div ?W), {?W * Mult, (?W * Mult) + ?W -1}).

random_range() ->
    ?LET([A, B],
         [choose(1, ?MAX_SET), choose(1, ?MAX_SET)],
         list_to_tuple(lists:sort([A, B]))
        ).

%% @doc test that the `from_mask' resets all bits in a word greater
%% than or equal to `From'
prop_unset_from_mask() ->
    ?FORALL(Mult, choose(0, 100),
            ?FORALL({Offset, From}, {Mult*?W, choose(0, ?W-1)},
                    ?FORALL(Members, gen_members(Offset),
                            begin
                                Value = lists:foldl(fun(I, Acc) ->
                                                            Acc bor (1 bsl (I rem ?W))
                                                    end,
                                                    0,
                                                    Members),
                                Mask = from_mask(Offset+From),
                                Value2 = Mask band Value,
                                Expected = lists:filter(fun(E) -> E < Offset+From end, Members),
                                measure(members, length(Members),
                                        Expected ==  lists:reverse(expand(Value2, Offset, [])))
                            end))).

%% @doc test that the `set_from_mask' sets all bits in a word greater
%% than or equal to `From'
prop_set_from_mask() ->
    ?FORALL(From, choose(0, ?W-1),
            ?FORALL(Members, gen_members(0),
                    begin
                        Value = lists:foldl(fun(I, Acc) ->
                                                    Acc bor (1 bsl (I rem ?W))
                                            end,
                                            0,
                                            Members),
                        Mask =  set_from_mask(From),
                        Value2 = Mask bor Value,
                        Expected = lists:umerge(Members, lists:seq(From, ?W-1)),
                        ?WHENFAIL(
                           begin
                               io:format("Value ~p~n",[Value]),
                               io:format("From ~p~n", [From]),
                               io:format("Entry ~p~n", [From]),
                               io:format("Mask ~p~n", [Mask])
                           end,
                           measure(members, length(Members),
                                   Expected ==  lists:reverse(expand(Value2, 0, []))))
                    end)).

%% @doc test that `to_mask' resets all bits in a word less than or
%% equal to `To'
prop_unset_to_mask() ->
    ?FORALL(Mult, choose(0, 100),
            ?FORALL({Offset, To}, {Mult*?W, choose(0, ?W-1)},
                    ?FORALL(Members, gen_members(Offset),
                            begin
                                Value = lists:foldl(fun(I, Acc) ->
                                                            Acc bor (1 bsl (I rem ?W))
                                                    end,
                                                    0,
                                                    Members),
                                Mask = to_mask(Offset+To),
                                Value2 = Mask band Value,
                                Expected = lists:filter(fun(E) -> E > Offset+To end, Members),
                                measure(members, length(Members),
                                        Expected ==  lists:reverse(expand(Value2, Offset, [])))
                            end))).
%% @doc test that `range_mask' resets all bits that are greater than
%% or equal to `Lo' and less than or equal to `Hi'. NOTE: that
%% `range_mask' expects non-contiguous and non-word-boundary input
%% (see subtract_range.)
prop_unset_range_mask() ->
    ?FORALL(Mult, choose(0, 100),
            ?FORALL({Offset, Hi, Lo}, {Mult*?W, choose(1, ?W-2), choose(1, ?W-2)},
                    ?FORALL(Members, gen_members(Offset),
                            begin
                                Value = lists:foldl(fun(I, Acc) ->
                                                            Acc bor (1 bsl (I rem ?W))
                                                    end,
                                                    0,
                                                    Members),

                                [Lo1, Hi1] = lists:sort([Hi, Lo]),
                                Mask = range_mask(Offset+Lo1, Offset+Hi1),
                                Value2 = Mask band Value,
                                Expected = lists:filter(fun(E) -> E < Offset+Lo1 orelse E > Offset+Hi1 end, Members),
                                ?WHENFAIL(begin
                                              io:format("Set ~p~n", [Members]),
                                              io:format("Range ~p - ~p ~n", [Offset+Lo1, Offset+Hi1])
                                          end,
                                measure(members, length(Members),
                                        equals(Expected, lists:reverse(expand(Value2, Offset, [])))))
                            end))).


%% The members for a single word in the bitset
gen_members(Offset) ->
    ?LET(Set, lists:seq(Offset, Offset+(?W-1)), sublist(Set)).

%% @doc property that checks, whatever the pair of sets, a calling
%% `union' returns a single set with all members from each set.
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

%% @doc property that checks that when `is_subset' is called with a
%% pair of sets, true is returned only when the second argument is a
%% subset (all it's members are also members) of the first.
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

%% @doc property that checks that given a pair of bitarrays,
%% `subtract' will return a bitarray containing only the elements in
%% the first bitarray that are _NOT_ in the second. See
%% (ord)sets:subtract/2
prop_subtract() ->
    ?FORALL({SetA, SetB}, frequency([{1, {gen_set(), gen_set()}},
                                     {1, gen_subset_pair()}]),
            begin
                IsSubset = lists:umerge(SetA, SetB) == SetB,
                BSA = from_list(SetA),
                BSB = from_list(SetB),
                Actual = to_list(subtract(BSA, BSB)),
                Expected = ordsets:subtract(SetA, SetB),
                aggregate([IsSubset],
                          Expected == Actual)
            end).

%% @doc property for the roundtrip to/from list. Mainly here as a
%% basis to depend on to/from list in the more interesting properties
%% above.
prop_list() ->
    ?FORALL(Set, gen_set(),
            begin
                BA = from_list(Set),
                measure(length, length(Set),
                        measure(sparsity, sparsity(Set),
                                (to_list(BA) == Set) andalso length(Set) == bigset_bitarray:size(BA)))
            end).

%% Generate a set of pos_integer()
-spec gen_set() -> [pos_integer()].
gen_set() ->
    ?LET(Size, choose(1, ?MAX_SET),
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

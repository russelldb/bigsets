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
         add_range/2,
         compact_contiguous/2,
         from_list/1,
         from_range/1,
         get/2,
         intersection/2,
         is_empty/1,
         is_subset/2,
         member/2,
         new/1,
         range_subtract/2,
         resize/1,
         set/2,
         set_all/2,
         size/1,
         subtract/2,
         subtract_range/2,
         to_list/1,
         union/2,
         unset/2
        ]).

-export_type([bit_array/0]).

 %% @TODO What word size is "best"?
-define(W, 128).
-define(FULL_WORD(W), ((1 bsl W) -1)).
-define(FULL_WORD, ?FULL_WORD(?W)).

-record(bit_array, {word_size=?W,
                    array :: array:array(bit())}).

-type bit() :: 0 | 1.
-type array() :: array:array(bit()).
-type bit_array() :: #bit_array{}.
-type range() :: {non_neg_integer(), non_neg_integer()}.

%%%===================================================================
%%% bitarray API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% create a new `bit_array()' with `N' slots. The word size will be
%% the default, set in the macro `?W'.
%% @end
%%--------------------------------------------------------------------
-spec new(integer()) -> bit_array().
new(N) ->
    A = new_array(N),
    #bit_array{word_size=?W, array=A}.

%%--------------------------------------------------------------------
%% @doc
%% create a bitset from a `Range'=`{Low, High}', inclusive. Returned
%% bit_array will have the macro `?W' default word size
%% @end
%%--------------------------------------------------------------------
-spec from_range(range()) -> bit_array().
from_range({Low, Hi}=Range)->
    Slots = (Hi - Low) div ?W,
    add_range(Range, new(Slots)).

%%--------------------------------------------------------------------
%% @doc
%% Add the range `{Low, High}' to the bit_array `A'. Return the new
%% bit_array.
%% @end
%%--------------------------------------------------------------------
-spec add_range(range(), bit_array()) -> bit_array().
add_range({Lo, Hi}, A) when Lo > Hi ->
    A;
%% create an add mask for each word contained in the range full
%% words are just (1 bsl W) -1.  part of a word is the bnot of
%% the range to unset a part used in subtract range.
add_range({Lo, Hi}, BA=#bit_array{word_size=W, array=A})
  when Lo =< Hi, (Lo div W) == (Hi div W) ->
    %% range is all in same word
    A2 = set_word_from_to(Lo, Hi, A, W),
    BA#bit_array{array=A2};
add_range({Lo, Hi}, BA=#bit_array{array=A, word_size=W})
  when Lo =< Hi, (Hi div W) - (Lo div W) == 1 ->
    %% contiguous words
    A2 = set_word_from(Lo, A, W),
    A3 = set_word_to(Hi, A2, W),
    BA#bit_array{array=A3};
add_range({Lo, Hi}, BA) when Lo =< Hi ->
    %% some full words to be set
    #bit_array{word_size=W, array=A} = BA,
    A2 = set_word_from(Lo, A, W),
    A3 = set_word_to(Hi, A2, W),
    A4 = set_range((Lo div W) +1, (Hi div W) -1, A3, W),
    BA#bit_array{array=A4}.

%%--------------------------------------------------------------------
%% @doc
%% Add the range `{Low, High}' to the bit_array `A'. Return the new
%% bit_array.
%% @end
%%--------------------------------------------------------------------
%% @doc return only the elements in `A' that are not in the range
%% defined by `Range' inclusive.
-spec subtract_range(bit_array(), range()) -> bit_array().
subtract_range(BA=#bit_array{word_size=W, array=A}, {Lo, Hi})
  when Lo =< Hi, (Lo div W) == (Hi div W) ->
    %% range is all in same word
    A2 = unset_word_from_to(Lo, Hi, A, W),
    BA#bit_array{array=A2};
subtract_range(BA=#bit_array{word_size=W, array=A}, {Lo, Hi})
  when Lo =< Hi, (Hi div W) - (Lo div W) == 1 ->
    %% contiguous words
    A2 = unset_word_from(Lo, A, W),
    A3 = unset_word_to(Hi, A2, W),
    BA#bit_array{array=A3};
subtract_range(BA=#bit_array{word_size=W, array=A}, {Lo, Hi})
  when Lo =< Hi ->
    %% some full words to be unset
    A2 = unset_word_from(Lo, A, W),
    A3 = unset_word_to(Hi, A2, W),
    A4 = unset_range((Lo div W) +1, (Hi div W) -1, A3),
    BA#bit_array{array=A4};
subtract_range(_Range, BA) ->
    %% If Lo > Hi there is no range to subtract
    BA.

%%--------------------------------------------------------------------
%% @doc
%% set the bit at `I' in bit_array `BA' and return the new bit_array.
%% @end
%%--------------------------------------------------------------------
-spec set(integer(), bit_array()) -> bit_array().
set(I, BA=#bit_array{}) ->
    #bit_array{word_size=W, array=A} = BA,
    A2 = set(I, A, W),
    BA#bit_array{array=A2}.

%%--------------------------------------------------------------------
%% @doc
%% unset the bit at `I' in bit_array `BA' and return the new bit_array.
%% @end
%%--------------------------------------------------------------------
-spec unset(pos_integer(), bit_array()) -> bit_array().
unset(I, BA=#bit_array{}) ->
    #bit_array{word_size=W, array=A} = BA,
    A2 = unset(I, A, W),
    BA#bit_array{array=A2}.

%%--------------------------------------------------------------------
%% @doc
%% set the bits from the list of integers `Ints' in bit_array `BA' and
%% return the new bit_array.
%% @end
%%--------------------------------------------------------------------
-spec set_all([pos_integer()], bit_array()) -> bit_array().
set_all(Ints, BA=#bit_array{}) ->
    #bit_array{word_size=W, array=A} = BA,
    A2 = lists:foldl(fun(I, Acc) ->
                             set(I, Acc, W)
                     end,
                     A,
                     Ints),
    BA#bit_array{array=A2}.

%%--------------------------------------------------------------------
%% @doc
%% Is the bit at `I' set in the bit_array `BA', returns true if so,
%% false otherwise.
%% @end
%%--------------------------------------------------------------------
-spec get(integer(), bit_array()) -> boolean().
get(I, BA=#bit_array{}) ->
    #bit_array{word_size=W, array=A} = BA,
    get(I, A, W).

%%--------------------------------------------------------------------
%% @doc
%% How many bits are set in the array. The bit set cardinality.
%% @end
%%--------------------------------------------------------------------
-spec size(bit_array()) -> non_neg_integer().
size(BA=#bit_array{}) ->
    #bit_array{word_size=W, array=A} = BA,
    size(A, W).

%%--------------------------------------------------------------------
%% @doc
%% returns true if no bits are set, false otherwise.
%% @end
%%--------------------------------------------------------------------
-spec is_empty(bit_array()) -> boolean().
is_empty(BA=#bit_array{}) ->
    #bit_array{array=A} = BA,
    array:sparse_size(A) == 0.

%%--------------------------------------------------------------------
%% @doc
%% synonym of `get/2' @see get/2
%% @end
%%--------------------------------------------------------------------
-spec member(pos_integer(), bit_array()) -> boolean().
member(I, BA) ->
    get(I, BA).

%%--------------------------------------------------------------------
%% @doc
%% create a list of integers that represents all set bits in the
%% bit_array `BA'
%% @end
%%--------------------------------------------------------------------
-spec to_list(bit_array()) -> [integer()].
to_list(BA=#bit_array{}) ->
    #bit_array{word_size=W, array=A} = BA,
    lists:reverse(
      array:sparse_foldl(fun(I, V, Acc) ->
                                 expand(V, I * W, Acc)
                         end, [], A)).

%%--------------------------------------------------------------------
%% @doc
%% create a new bit_array populate with the values in the given list
%% of integers. Returns a bit_array with default word size.
%% @end
%%--------------------------------------------------------------------
-spec from_list(list(non_neg_integer())) -> bit_array().
from_list([]) ->
    new(10);
from_list(L) ->
    set_all(L, new(lists:max(L))).

%%--------------------------------------------------------------------
%% @doc
%% union two bit_arrays into a single bit_array. Set union. NOTE: this
%% requires both arrays have the same word size!
%% @end
%%--------------------------------------------------------------------
-spec union(bit_array(), bit_array()) -> bit_array().
union(BA1=#bit_array{word_size=W}, BA2=#bit_array{word_size=W}) ->
    #bit_array{array=A} = BA1,
    #bit_array{array=B} = BA2,
    NewSize = max(array:sparse_size(A), array:sparse_size(B)),
    {MergedA, RemainingB0} = array:sparse_foldl(fun(I, VA, {MergedA, RemainingB}) ->
                                                        VB = array:get(I, B),
                                                        MergedV = VA bor VB,
                                                        {array:set(I, MergedV, MergedA),
                                                         array:reset(I, RemainingB)}
                                                end,
                                                {new_array(NewSize), B},
                                                A),
    RemainingB = array:resize(RemainingB0),
    U = array:sparse_foldl(fun(I, VB, Acc) ->
                                   array:set(I, VB, Acc)
                           end,
                           RemainingB,
                           MergedA),
    #bit_array{word_size=W, array=U}.

%%--------------------------------------------------------------------
%% @doc
%% returns the intersection two bit_arrays as a single bit_array. Set
%% intersection. NOTE: this requires both array have the same word
%% size!
%% @end
%%--------------------------------------------------------------------
-spec intersection(bit_array(), bit_array()) -> bit_array().
intersection(BA1=#bit_array{word_size=W}, BA2=#bit_array{word_size=W}) ->
    #bit_array{array=A} = BA1,
    #bit_array{array=B} = BA2,
    {SizeA , SizeB} = {array:sparse_size(A), array:sparse_size(B)},
    NewSize = min(SizeA, SizeB),
    %% fold the smallest
    {LHS, RHS} = if SizeA =< SizeB -> {A, B};
                    true -> {B, A} end,
    Res = array:sparse_foldl(fun(I, V0, Acc) ->
                                     Word = array:get(I, RHS),
                                     V = Word band V0,
                                     array:set(I, V, Acc)
                             end,
                             new_array(NewSize),
                             LHS),
    Intersection = array:resize(Res),
    #bit_array{word_size=W, array=Intersection}.

%%--------------------------------------------------------------------
%% @doc
%% is_subset returns true if the bit_array `BA1' is a subset of the
%% bit_array `BA2'. NOTE: this requires both array have the same word
%% size!
%% @end
%%--------------------------------------------------------------------
-spec is_subset(bit_array(), bit_array()) -> boolean().
is_subset(BA1=#bit_array{word_size=W}, BA2=#bit_array{word_size=W}) ->
    %% Assumes same word size. A ⊂ B = A ∪ B == B, right?  Fold over
    %% the words, and as soon as the union of any pair of words is not
    %% == to word, throw. We fold over A
    #bit_array{array=A} = BA1,
    #bit_array{array=B} = BA2,
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

%%--------------------------------------------------------------------
%% @doc
%% subtract returns a bit_array containing only the elements in `BA1'
%% that are not also elements of `BA2'. NOTE: this requires both array
%% have the same word size!
%% @end
%%--------------------------------------------------------------------
-spec subtract(bit_array(), bit_array()) -> bit_array().
subtract(BA1=#bit_array{word_size=W}, BA2=#bit_array{word_size=W}) ->
    #bit_array{array=A} = BA1,
    #bit_array{array=B} = BA2,

    NewArray = array:sparse_foldl(fun(I, VA, Comp) ->
                                          VB = array:get(I, B),
                                          VComp = (VA band (bnot VB)),
                                          array:set(I, VComp, Comp)
                                  end,
                                  new_array(10),
                                  A),
    #bit_array{word_size=W, array=NewArray}.

%%--------------------------------------------------------------------
%% @doc
%% range_subtract given a `range()' and a `bit_array()' returns a
%% `bit_array()' of all the elements in `range()' that are not also in
%% `B'. It's subtract(from_range(Range), B). NOTE: returned array has
%% `B's word size.
%% @end
%%--------------------------------------------------------------------
-spec range_subtract(range(), bit_array()) -> bit_array().
range_subtract(Range, B=#bit_array{word_size=W}) ->
    %% @TODO for large contiguous ranges we can probably save the
    %% resources of building actual entries.
    A = from_range(Range, W),
    subtract(A, B).

%%--------------------------------------------------------------------
%% @doc
%% given a pos_integer() `N', remove everything in the bit_array()
%% `Array' that is less than, or equal to `N'. Also remove everything
%% that is contiguous with `N'. Return the highest removed set bit,
%% and the new bit_array. In logical clock terms we expect `N' to
%% represent the highest base event seen by an actor.
%% @end
%%--------------------------------------------------------------------
-spec compact_contiguous(non_neg_integer(), bit_array()) ->
                                {non_neg_integer(), bit_array()}.
compact_contiguous(N, Array=#bit_array{}) ->
    %% this removes all up to N
    BA=#bit_array{array=A, word_size=W} = subtract_range(Array, {0, N}),
    %% this removes all in sequence from N
    {NewBase, NewArray} = unset_contiguous_with(N, A, W),
    {NewBase, BA#bit_array{array=NewArray}}.

%%--------------------------------------------------------------------
%% @doc
%% resize ensures that unset values are no longer accessible, and take
%% up no space. returns the bit_array resized.
%% @end
%%--------------------------------------------------------------------
-spec resize(bit_array()) -> bit_array().
resize(BA=#bit_array{array=A}) ->
    BA#bit_array{array=array:resize(A)}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private create a new array of size `N'.
new_array(N) ->
    array:new([{size, N}, {default, 0}, {fixed, false}]).

%% @private set a bit in the array.
-spec set(integer(), array(), pos_integer()) -> array().
set(I, A, W) ->
    AI = I div W,
    V = array:get(AI, A),
    V1 = V bor (1 bsl (I rem W)),
    array:set(AI, V1, A).

%% @private unset a bit in the array
-spec unset(integer(), array(), pos_integer()) -> array().
unset(I, A, W) ->
    AI = I div W,
    V = array:get(AI, A),
    V1 = V band (bnot (1 bsl (I rem W))),
    array:set(AI, V1, A).

%% @private is bit `I' set?
-spec get(integer(), array(), pos_integer()) -> boolean().
get(I, A, W) ->
    AI = I div W,
    V = array:get(AI, A),
    V band (1 bsl (I rem W)) =/= 0.

%% @private cardinality of the bit set
-spec size(array(), pos_integer()) -> non_neg_integer().
size(A, W) ->
    array:sparse_foldl(fun(I, V, Acc) ->
                               cnt(V, I * W, Acc)
                       end,
                       0,
                       A).

%% @private set a range that is within one word
-spec set_word_from_to(non_neg_integer(), non_neg_integer(), array(), pos_integer()) ->
                              array().
set_word_from_to(Lo, Hi, A, W) ->
    case {word_start(Lo, W), word_end(Hi, W)} of
        {true, true} ->
            %% the range _is_ the word, so full word it
            array:set(Lo div W, ?FULL_WORD(W), A);
        {true, false} ->
            %% Lo is the word start, add everything from start to
            %% Hi
            set_word_to(Hi, A, W);
        {false, true} ->
            %% Hi is the word end, add everything from Lo to the
            %% end.
            set_word_from(Lo, A, W);
        _ ->
            %% we need to add from Lo to Hi
            Mask = bnot range_mask(Lo, Hi, W),
            apply_smask(Mask, Lo div W, A)
    end.

%% @private set all bits from 0 to `Hi'
-spec set_word_to(non_neg_integer(), array(), pos_integer()) ->
                         array().
set_word_to(Hi, A, W) ->
    Mask = (1 bsl ((Hi rem W) +1) -1),
    apply_smask(Mask, Hi div W, A).

%% @private set all bits from `Lo' to `W'-1 (i.e. the end of the
%% word.)
-spec set_word_from(non_neg_integer(), array(), pos_integer()) ->
                           array().
set_word_from(Lo, A, W) ->
    Mask = set_from_mask(Lo, W),
    apply_smask(Mask, Lo div W, A).

%% @private set the entries in array `A' to full words from Lo to Hi
%% inclusive. Return new array.
-spec set_range(non_neg_integer(), non_neg_integer(), array(), pos_integer()) ->
                       array().
set_range(Lo, Lo, A, W) ->
    array:set(Lo, ?FULL_WORD(W), A);
set_range(Lo, Hi, A, W) ->
    set_range(Lo+1, Hi, array:set(Lo, ?FULL_WORD(W), A), W).

%% @private bor bitmask `Mask' onto the entry at `Index' in `Array',
%% return the new `Array' with the updated entry.
-spec apply_smask(integer(), non_neg_integer(), array()) ->
                         array().
apply_smask(Mask, Index, Array) ->
    Value0 = array:get(Index, Array),
    Value = Mask bor Value0,
    array:set(Index, Value, Array).

%% @private band bitmask `Mask' onto the entry at `Index' in `Array',
%% return the new `Array' with the updated entry.
-spec apply_mask(integer(), non_neg_integer(), array()) ->
                         array().
apply_mask(Mask, Index, Array) ->
    Value0 = array:get(Index, Array),
    Value = Mask band Value0,
    array:set(Index, Value, Array).

%% @private from_range: create a bitset from `range()' with word size
%% `W'
-spec from_range(range(), pos_integer()) -> bit_array().
from_range({Low, High}=Range, W) ->
    Slots = (High - Low) div W,
    add_range(Range, new(Slots)).

%% @private
%% remove a range that is within one word
-spec unset_word_from_to(pos_integer(), pos_integer(), array(), pos_integer()) ->
                                array().
unset_word_from_to(Lo, Hi, A, W) ->
    case {word_start(Lo, W), word_end(Hi, W)} of
        {true, true} ->
            %% the range _is_ the word, so reset it
            array:reset(Lo div W, A);
        {true, false} ->
            %% Lo is the word start, remove everything from start to
            %% Hi
            unset_word_to(Hi, A, W);
        {false, true} ->
            %% Hi is the word end, remove everything from Lo to the
            %% end.
            unset_word_from(Lo, A, W);
        _ ->
            %% we need to remove from Lo to Hi
            Mask = range_mask(Lo, Hi, W),
            apply_mask(Mask, Lo div W, A)
    end.

%% @private unset all bits in the word containing `High' from 0th to
%% `High', returns updated array()
-spec unset_word_to(non_neg_integer(), array(), pos_integer()) ->
                           array().
unset_word_to(High, A, W) ->
    Mask = to_mask(High, W),
    apply_mask(Mask, High div W, A).

%% @private unset all the bits in the word containing `Lo' from `Lo'
%% to `W'th-1 position, returns updated array().
-spec unset_word_from(non_neg_integer(), array(), pos_integer()) ->
                             array().
unset_word_from(Low, A, W) ->
    Mask = from_mask(Low, W),
    apply_mask(Mask, Low div W, A).

%% @private create a mask that sets all the bits in a word of length
%% `W' from `Lo' to `W'th-1 bits. Returns mask.
-spec set_from_mask(non_neg_integer(), pos_integer()) -> pos_integer().
set_from_mask(Lo, W) ->
    M = (1 bsl (W - (Lo rem W))) -1,
    M bsl (Lo rem W).

%% @private create a mask that when band'ed with a word of `W' length,
%% clears all the bits in the word in the range Lo - Hi, inclusive
-spec range_mask(pos_integer(), pos_integer(), pos_integer()) ->
                        neg_integer().
range_mask(Lo, Hi, W) ->
    ToMask = to_mask(Lo-1, W), %% can't be < 0, see unset_word_from_to
    FromMask = from_mask(Hi+1, W), %% can't be > ?W, see unset_word_from_to
    bnot ( FromMask band ToMask).

%% @private create a mask that when band'ed with a word of `W' length
%% clears all the bits in the word from N up to `W'th-1 bit. Returns
%% the mask.
-spec from_mask(non_neg_integer(), pos_integer()) -> pos_integer().
from_mask(N, W) ->
    (1 bsl (N rem W)) - 1.

%% @private create a mask that can be band'ed with a word to clear all
%% the bits in the word up to N, inclusive
-spec to_mask(pos_integer(), pos_integer()) -> pos_integer().
to_mask(N, W) ->
    bnot (1 bsl  ((N rem W) +1)  - 1).

%% @private Reset full words from Lo to Hi inclusive.
-spec unset_range(non_neg_integer(), non_neg_integer(), array()) ->
                         array().
unset_range(Lo, Lo, A) ->
    array:reset(Lo, A);
unset_range(Lo, Hi, A) ->
    unset_range(Lo+1, Hi, array:reset(Lo, A)).

%% @private return true if `N' is the first bit of a word of length `W'
%% TODO: if these are only used in dotclouds, then 2 is the word start
%% of the first word, not zero.
-spec word_start(non_neg_integer(), pos_integer()) -> boolean().
word_start(N, W) when (N rem W) == 0 ->
    true;
word_start(_, _) ->
    false.

%% @private return true if `N' is the last bit of a word of length `W'
-spec word_end(non_neg_integer(), pos_integer()) -> boolean().
word_end(N, W) when (N rem W) == W-1 ->
    true;
word_end(_, _) ->
    false.

%% @private Convert bit vector into list of integers, with
%% optional offset.expand(2#01, 0, []) -> [0] expand(2#10, 0, []) ->
%% [1] expand(2#1101, 0, []) -> [3,2,0] expand(2#1101, 1, []) ->
%% [4,3,1] expand(2#1101, 10, []) -> [13,12,10] expand(2#1101, 100,
%% []) -> [103,102,100]
-spec expand(Value :: pos_integer(), Offset::non_neg_integer(), list(non_neg_integer())) ->
                    list(non_neg_integer()).
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

%% @private Same as `expand/3' above but the acc is a running total of
%% all set bits (a count.)
-spec cnt(Value :: pos_integer(), Offset::non_neg_integer(), non_neg_integer()) ->
                 non_neg_integer().
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

%% @private kind of "find first unset" but unsets all up to
%% first-unset and returns value of first_unset-1. Clears all the set
%% bits contiguous with `N'. Called when all bits up to, and including
%% `N' have been unset.
-spec unset_contiguous_with(pos_integer(), array(), pos_integer()) ->
                                   {pos_integer(), array()}.
unset_contiguous_with(N, A, W) ->
    %% TODO eqc never hits the "full word" option. Unit test?
    FullWord = ?FULL_WORD(W),
    try
        array:sparse_foldl(fun(Index, _Value, {M, Acc}) when ((M+1) div W) < Index ->
                                   throw({break, {M, Acc}});
                              (Index, FW, {_M, Acc}) when FW == FullWord ->
                                   {(Index*W)+(W-1), array:reset(Index, Acc)};
                              (Index, Value, {M, Acc})  ->
                                   %% all bits up to and including N
                                   %% are unset, this is not a fully
                                   %% set word. Is bit at N+1 set? if
                                   %% yes, check next bit, if you get
                                   %% to end of word, unset word, and
                                   %% fold on, if unset bit > N and
                                   %% unset-bit =< word end then clear
                                   %% up to unset-bit, return
                                   %% unset-bit-index and array
                                   %% (i.e. throw to break fold)
                                   case find_first_zero_after_n(M+1, Value, W) of
                                       {unset_to, X} ->
                                           %% unset word to X and break,
                                           %% we're done
                                           throw({break, {X, unset_word_to(X, Acc, W)}});
                                       unset_word ->
                                           {(Index*W)+(W-1), array:reset(Index, Acc)}
                                   end
                           end,
                           {N, A},
                           A)
    catch {break, Acc} ->
            Acc
    end.

%% @private find the first unset bit in a word of length `W' after the
%% bit represented by `N'. returns instruction to the caller.
-spec find_first_zero_after_n(pos_integer(), non_neg_integer(), pos_integer()) ->
                                     {unset_to, pos_integer()} |
                                     unset_word.
find_first_zero_after_n(N, V, W) ->
    BitIndex = N rem W,
    V2 = (V bsr BitIndex),
    case V2 band 1 of
        0 ->
            {unset_to, N-1};
        1 ->
            case BitIndex == W-1 of
                true ->
                    %% end of the word, exit, zero whole word
                    unset_word;
                false ->
                    %% keep going through word
                    find_first_zero_after_n(N+1, V, W)
            end
    end.

-ifdef(TEST).

%%%===================================================================
%%% Tests
%%%===================================================================

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_test_() ->
    TestList = [{10, fun prop_subset/0},
                {10, fun prop_subtract/0},
                {10, fun prop_union/0},
                {10, fun prop_intersection/0},
                {20, fun prop_add_range/0},
                {30, fun prop_from_range/0},
                {30, fun prop_range_subtract/0},
                {30, fun prop_subtract_range/0},
                {5, fun prop_list/0},
                {5, fun prop_set_from_mask/0},
                {5, fun prop_unset_from_mask/0},
                {5, fun prop_unset_range_mask/0},
                {5, fun prop_unset_to_mask/0},
                {10, fun prop_find_first_zero_after/0},
                {20, fun prop_compact_contiguous/0}],

    {timeout, 60*length(TestList), [
                                    {timeout, 60, ?_assertEqual(true,
                                                                eqc:quickcheck(eqc:testing_time(Time, ?QC_OUT(Prop()))))} ||
                                       {Time, Prop} <- TestList]}.


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
    Mask = from_mask(Offset+From, ?W),
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
    Mask = to_mask(Offset+To, ?W),
    Value2 = Mask band Value,
    ?assertEqual(lists:seq(Offset+To+1, Offset+100), lists:reverse(expand(Value2, Offset, []))).

find_first_zero_after_n_test() ->
    V = lists:foldl(fun(I, Acc) ->
                            Acc bor (1 bsl (I rem ?W))
                    end,
                    0,
                    lists:seq(1, 50) ++ lists:seq(52, 102)),
    ?assertEqual({unset_to, 50}, find_first_zero_after_n(1, V, ?W)),
    ?assertEqual({unset_to, 102}, find_first_zero_after_n(52, V, ?W)),
    ?assertEqual(unset_word, find_first_zero_after_n(100, ?FULL_WORD, ?W)).

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

%% @doc property that tests that we can make a bitarry that contains
%% the range `{Hi, Lo}'. A lot like from list if lists:seq(Lo, Hi).
prop_add_range() ->
    ?FORALL({Set, {Lo, Hi}=Range}, {gen_set(), random_range()},
            begin
                ToAdd = lists:seq(Lo, Hi),
                Expected = lists:umerge(Set, ToAdd),
                BS = from_list(Set),
                Actual = add_range(Range, BS),
                to_list(Actual) == Expected
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
                                Mask = from_mask(Offset+From, ?W),
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
                        Mask =  set_from_mask(From, ?W),
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
                                Mask = to_mask(Offset+To, ?W),
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
                                Mask = range_mask(Offset+Lo1, Offset+Hi1, ?W),
                                Value2 = Mask band Value,
                                Expected = lists:filter(fun(E) -> E < Offset+Lo1 orelse E > Offset+Hi1 end, Members),
                                ?WHENFAIL(begin
                                              io:format("Set ~p~n", [Members]),
                                              io:format("Range ~p - ~p ~n", [Offset+Lo1, Offset+Hi1])
                                          end,
                                measure(members, length(Members),
                                        equals(Expected, lists:reverse(expand(Value2, Offset, [])))))
                            end))).

%% @doc property for the "find first zero after" function.
prop_find_first_zero_after() ->
    %% We need a word, and an N
    ?FORALL(N, choose(0, ?W-1),
            ?FORALL(Set, ?LET(S, lists:seq(N+1, ?W-1), sublist(S)),
                    begin
                        V = lists:foldl(fun(I, Acc) ->
                                                Acc bor (1 bsl (I rem ?W))
                                        end,
                                        0,
                                        Set),
                        Actual = find_first_zero_after_n(N, V, ?W),
                        UnsetTo = lists:foldl(fun(E, Acc) ->
                                                      if E == Acc ->
                                                              E+1;
                                                         true ->
                                                              Acc
                                                      end
                                              end,
                                              N,
                                              Set),
                        Expected = if (UnsetTo-1) == ?W-1 -> unset_word;
                                      true -> {unset_to, UnsetTo-1}
                                   end,
                        ?WHENFAIL(
                           begin
                               io:format("N ~p~n", [N]),
                               io:format("Set ~p~n", [Set]),
                               io:format("Value ~p~n", [V])
                           end,
                        equals(Expected, Actual))
                    end)).

%% @doc property for `compact_contiguous' as used by
%% bigset_clock_ba. Given a number N and bit array BA, returns M (the
%% highest number contiguous to N), and a bitarray that is BA with all
%% bits up and including M unset.
prop_compact_contiguous() ->
    ?FORALL({Set, N}, {gen_set(), choose(0, ?MAX_SET)},
            begin
                BS = from_list(Set),
                {ActualN, ActualBS} = compact_contiguous(N, BS),
                %% remove all less than or equal to N
                Set2 = lists:dropwhile(fun(E) -> E =< N end, Set),
                %% Remove all in sequence with N
                {ExpectedN, ExpectedSet} = remove_contiguous(N+1, Set2),
                ?WHENFAIL(
                   begin
                       io:format("Bit Array ~p~n", [BS]),
                       io:format("Set ~p~n", [Set]),
                       io:format("N ~p~n", [N]),
                       io:format("Bit Array Compacted ~p~n", [ActualBS])
                   end,
                conjunction([{new_base, equals(ExpectedN, ActualN)},
                             {new_set, equals(ExpectedSet, to_list(ActualBS))}]))
            end).

remove_contiguous(N, [N | Rest]) ->
    remove_contiguous(N+1, Rest);
remove_contiguous(N, Set) ->
    {N-1, Set}.

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

%% @doc property that checks, whatever the pair of sets, a calling
%% `intersection' returns a single set with only members from both
%% sets.
prop_intersection() ->
    ?FORALL({SetA, SetB}, {gen_set(), gen_set()},
            begin
                BS1 = from_list(SetA),
                BS2 = from_list(SetB),
                I = intersection(BS1, BS2),
                L = to_list(I),
                Expected = ordsets:intersection(SetA, SetB),
                measure(length_a, length(SetA),
                        measure(length_b, length(SetB),
                                measure(sparsity_a, sparsity(SetA),
                                        measure(sparsity_b, sparsity(SetB),
                                                measure(sparsity_i, sparsity(Expected),
                                                        measure(length_i, length(Expected),
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
%% element and divide it by the size of the set. Bigger is more
%% sparse.
-spec sparsity(list()) -> pos_integer().
sparsity([]) ->
    1;
sparsity(Set) ->
    lists:max(Set) div length(Set).

-endif.

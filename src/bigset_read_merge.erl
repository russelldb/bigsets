%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Basho Technologies
%%% @doc
%%% Funtions for merging sets/partial sets and generating repair data
%%% from results
%%% @end
%%% Created : 15 Jun 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_read_merge).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("bigset.hrl").

-ifdef(TEST).
-compile(export_all).
-else.
-export([merge_sets/1]).
-endif.

-type partition() :: pos_integer().

%% @doc merge a list of (possibly partial) sets into a single set and
%% a dictionary of repairs.  @TODO what is the `partition()' tag for a
%% set that is the result of merging two(or more) results? A list of
%% all the partitions that are in the dict?
-spec merge_sets([{partition(), bigset_clock:clock(), elements()}]) ->
                        {repairs(), bigset_clock:clock(), elements()}.
merge_sets([]) ->
    {[], bigset_clock:fresh(), []};
merge_sets([{_P, Clock, Elements}]) ->
    {[], {Clock, Elements}};
merge_sets(Sets) ->
    lists:foldl(fun(Set, {RepairsAcc, SetAcc}) ->
                        merge_set(SetAcc, Set, [], RepairsAcc)
                end,
                {[], hd(Sets)},
                tl(Sets)).

%% @doc given a pair of sets, and a dictionary of repairs, merge into
%% one set.
merge_set({_P1, Clock1, []}, {_P2, Clock2, []}, ElementAcc, RepairAcc) ->
    {RepairAcc, bigset_clock:merge(Clock1, Clock2), lists:reverse(ElementAcc)};
merge_set({P1, Clock1, [{Element, Dots1} | T1]}, {P2, Clock2, [{Element2, _Dots2} | _T2]=E2}, ElementAcc, RepairAcc) when Element < Element2 ->
    {RepairAcc2, SurvivingDots} = handle_dots(Element, {P1, Dots1}, {P2, Clock2}, RepairAcc),
    ElementAcc2 = maybe_store_dots(Element, SurvivingDots, ElementAcc),
    merge_set({P1, Clock1, T1}, {P2, Clock2, E2}, ElementAcc2, RepairAcc2);
merge_set({P1, Clock1, [{Element, _Dots1} | _T1]=E1}, {P2, Clock2, [{Element2, Dots2} | T2]}, ElementAcc, RepairAcc) when Element > Element2 ->
    {RepairAcc2, SurvivingDots} = handle_dots(Element2, {P2, Dots2}, {P1, Clock1}, RepairAcc),
    ElementAcc2 = maybe_store_dots(Element2, SurvivingDots, ElementAcc),
    merge_set({P1, Clock1, E1}, {P2, Clock2, T2}, ElementAcc2, RepairAcc2);
merge_set({P1, Clock1, [{Element, Dots1} | T1]}, {P2, Clock2, [{Element, Dots2} | T2]}, ElementAcc, RepairAcc)  ->
    {RepairAcc2, SurvivingDots} = merge_dots(Element,
                                             {P1, Dots1, Clock1},
                                             {P2, Dots2, Clock2},
                                             RepairAcc),
    ElementAcc2 = maybe_store_dots(Element, SurvivingDots, ElementAcc),
    merge_set({P1, Clock1, T1}, {P2, Clock2, T2}, ElementAcc2, RepairAcc2);
merge_set({P1, Clock1, [{Element, Dots1} | T1]}, {P2, Clock2, []}, ElementAcc, RepairAcc) ->
    {RepairAcc2, SurvivingDots} = handle_dots(Element, {P1, Dots1}, {P2, Clock2}, RepairAcc),
    ElementAcc2 = maybe_store_dots(Element, SurvivingDots, ElementAcc),
    merge_set({P1, Clock1, T1}, {P2, Clock2, []}, ElementAcc2, RepairAcc2);
merge_set({P1, Clock1, []}, {P2, Clock2, [{Element, Dots2} | T2]}, ElementAcc, RepairAcc) ->
    {RepairAcc2, SurvivingDots} = handle_dots(Element, {P2, Dots2}, {P1, Clock1}, RepairAcc),
    ElementAcc2 = maybe_store_dots(Element, SurvivingDots, ElementAcc),
    merge_set({P1, Clock1, []}, {P2, Clock2, T2}, ElementAcc2, RepairAcc2).

%% @priv return a `dot_list()' of the surviving dots from `Dots1' and
%% `Dots2' where to survive means a dot is either: in both lists, or
%% in only one list and is unseen by the opposite clock. The
%% intersection of `Dots1' and `Dots2' plus the results of filtering
%% the two remaining subsets against a clock. Also returns a
%% `repairs()'
-spec merge_dots(member(), {partition(), dot_list(), bigset_clock:clock()},
                 {partition(), dot_list(), bigset_clock:clock()}, repairs()) ->
                        {repairs(), dot_list()}.
merge_dots(Element, {P1, Dots1, Clock1}, {P2, Dots2, Clock2}, Repairs0) ->
    Both = intersection(Dots1, Dots2),
    P1Unique = subtract(Dots1, Dots2),
    P2Unique = subtract(Dots2, Dots1),
    {Repairs1, P1Keep} = handle_dots(Element, {P1, P1Unique}, {P2, Clock2}, Repairs0, Both),
    {Repairs2, P2Keep} = handle_dots(Element, {P2, P2Unique}, {P1, Clock1}, Repairs1, P1Keep),
    {Repairs2, P2Keep}.

-spec handle_dots(member(), dot_list(), bigset_clock:clock(), repairs()) -> {repairs(), dot_list()}.
handle_dots(Element, {P1, Dots}, {P2, Clock}, Repairs0) ->
    handle_dots(Element, {P1, Dots}, {P2, Clock}, Repairs0, []).

-spec handle_dots(member(), dot_list(), bigset_clock:clock(), repairs(), dot_list()) -> {repairs(), dot_list()}.
handle_dots(Element, {P1, Dots}, {P2, Clock}, Repairs0, DotAcc) ->
    lists:foldl(fun(Dot, {Repairs, Keep}) ->
                        handle_dot(Element, {P1, Dot}, {P2, Clock}, Repairs, Keep)
                end,
                {Repairs0, DotAcc},
                Dots).

handle_dot(Element, {P1, Dot}, {P2, Clock}, Repairs, DotAcc) ->
    case bigset_clock:seen(Dot, Clock) of
        true ->
            %% P1 needs to drop this dot
            {repair_remove(P1, Element, Dot, Repairs),
             DotAcc};
        false ->
            %% P2 needs to add this dot
            {repair_add(P2, Element, Dot, Repairs),
             lists:umerge([Dot], DotAcc)}
    end.

-spec repair_remove(partition() | [partition()],
                    member(), dot(), repairs()) ->
                           repairs().
repair_remove(Partition, Element, Dot, Repairs) ->
    repair_update(remove, Partition, Element, Dot, Repairs).

-spec repair_add(partition() | [partition()],
                 member(), dot(), repairs()) ->
                        repairs().
repair_add(Partition, Element, Dot, Repairs) ->
    repair_update(add, Partition, Element, Dot, Repairs).


repair_update(Action, Partitions, Element, Dot, Repairs)
  when is_list(Partitions) ->
    lists:foldl(fun(Partition, Acc) ->
                        repair_update(Action, Partition, Element, Dot, Acc)
                end,
                Repairs,
                Partitions);
repair_update(Action, Partition, Element, Dot, Repairs) ->
    Repair0 = get_repair(Partition, Repairs),
    Repair = add_repair(Action, Element, Dot, Repair0),
    set_repair(Partition, Repair, Repairs).

%% @TODO either make repairs a dict, or work with the three tuple
%% defined elsewhere!
get_repair(Partition, Repairs) ->
    case lists:keyfind(Partition, 1, Repairs) of
        {Partition, _Adds, _Removes}=Tuple ->
            Tuple;
        false ->
            %% @TODO should be a record??
            {Partition, [], []}
    end.

set_repair(Partion, Repair, Repairs) ->
    [Repair | lists:keydelete(Partion, 1, Repairs)].

add_repair(add, Element, Dot, Repair) ->
    add_repair(2, Element, Dot, Repair);
add_repair(remove, Element, Dot, Repair) ->
    add_repair(3, Element, Dot, Repair);
add_repair(N, Element, Dot, Repair) when N == 2; N == 3 ->
    Updated = update_element(Element, Dot, element(N, Repair)),
    erlang:setelement(N, Repair, Updated).

update_element(Element, Dot, Entries) ->
    orddict:update(Element, fun(L) ->
                                    lists:umerge([Dot], L)
                            end,
                   [Dot],
                   Entries).

intersection(Dots1, Dots2) ->
    ordsets:intersection(ordsets:from_list(Dots1), ordsets:from_list(Dots2)).

subtract(Dots1, Dots2) ->
    ordsets:subtract(ordsets:from_list(Dots1), ordsets:from_list(Dots2)).

maybe_store_dots(_Elements, []=_Dots, Elements) ->
    Elements;
maybe_store_dots(Element, Dots, Elements) ->
    [{Element, Dots} | Elements].

-ifdef(TEST).

get_repair_test() ->
    Partition = 1,
    ?assertEqual({Partition, [], []}, get_repair(Partition, [])),
    Expected = {Partition,
                [{<<"e1">>, [{a, 3}, {b, 5}, {d, 1}]},
                 {<<"e4">>, [{a, 6}]}],
                [{<<"e100">>, [{a, 12}, {b, 56}]}]},
    Repairs = [Expected,
               {2, [], [{<<"e99">>, [{a, 3}]}]},
               {100, [{<<"e9">>, [{x, 13}]}], []}
              ],
    ?assertEqual(Expected, get_repair(Partition, Repairs)).

add_repair_test() ->
    Elem = <<"e1">>,
    Dot = {a, 1},
    EmptyRepair = {1, [], []},
    AddExpected = {1, [{Elem, [Dot]}], []},
    RemExpected = {1, [], [{Elem, [Dot]}]},
    AddActual = add_repair(add, Elem, Dot, EmptyRepair),
    RemActual = add_repair(remove, Elem, Dot, EmptyRepair),
    ?assertEqual(AddExpected, AddActual),
    ?assertEqual(RemExpected, RemActual).

repair_update_test() ->
    Partition = 1,
    Element = <<"e1">>,
    Dot = {a, 1},
    Repairs = [],
    Expected = [{Partition, [], [{Element, [Dot]}]}],
    Actual = repair_update(remove, Partition, Element, Dot, Repairs),
    ?assertEqual(Expected, Actual).

repair_remove_test() ->
    Expected = [{2, [], [{<<"e1">>, [{a, 2}]}]}],
    Actual = repair_remove(2, <<"e1">>, {a, 2}, []),
    ?assertEqual(Expected, Actual).

%% just check that expected repairs come back, ideally this should be
%% quickchecked!
merge_test() ->
    Clock1 = {[{a, 10}, {b, 3}, {d, 4}], %%base vv of clock1
              [{b, [4,5]}, {c, [13]}, {d, [6,7]}]}, %% dot cloud 1
    Set1 = {1, %% partition 1
            Clock1,
            [{<<"element1">>, [{a, 8}, {b, 4}]},
             {<<"element3">>, [{a, 1}, {c, 13}]},
             {<<"element4">>, [{a, 3}, {d, 1}]},
             {<<"element6">>, [{a, 10}]}] %% elements 1
           },

    Clock2 = {[{a, 5}, {c, 16}, {b, 7}, {d, 2}], %%base vv of clock2
              [{a, [8]}]}, %% dot cloud 2
    Set2 = {2, %% partition 2
            Clock2,
            [{<<"element1">>, [{a, 8}, {b, 4}]},
             {<<"element2">>, [{a, 4}, {d, 2}, {c, 15}]},
             {<<"element4">>, [{a, 3}]},
             {<<"element5">>, [{c, 16}]}] %% elements 2
           },

    %% we expect a repair entry for both partitions
    ExpectedRepairs = [
                       {1,
                        [{<<"element2">>, [{c, 15}]},
                         {<<"element5">>, [{c, 16}]}], %% add repairs
                        [{<<"element3">>, [{a, 1}, {c, 13}]},
                         {<<"element4">>, [{d, 1}]}] %% remove repairs
                       },
                       {2,
                        [{<<"element6">>, [{a, 10}]}], %% adds
                        [{<<"element2">>, [{a, 4}, {d, 2}]}] %% removes
                       }
                      ],

    ExpectedClock = bigset_clock:merge(Clock1, Clock2),
    ExpectedElements = [{<<"element1">>, [{a, 8}, {b, 4}]}, %%both
                        {<<"element2">>, [{c, 15}]}, %% one remaining dot from 2
                        {<<"element4">>, [{a, 3}]}, %% one remaining dot from 2
                        {<<"element5">>, [{c, 16}]}, %% in set 2
                        {<<"element6">>, [{a, 10}]} %% in set 1
                       ],

    {Repairs, Clock, Elements} = merge_sets([Set1, Set2]),

    ?assertEqual(ExpectedRepairs, lists:sort(Repairs)),
    ?assertEqual(ExpectedClock, Clock),
    ?assertEqual(ExpectedElements, Elements).

-endif.


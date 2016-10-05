%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Basho Technologies
%%% @doc
%%% Funtions for merging sets/partial sets and generating repair data
%%% from results
%%% @TODO Is the repair data the complement of the merge data?
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
-export([merge_sets/1, merge_sets/2]).
-endif.

-type partition() :: pos_integer() | [pos_integer()].


%% @doc merge a list of (possibly partial) sets into a single set and
%% a dictionary of repairs.  @TODO what is the `partition()' tag for a
%% set that is the result of merging two(or more) results? A list of
%% all the partitions that are in the dict?
-spec merge_sets([{partition(), bigset_clock:clock(), elements()}]) ->
                        {repairs(), {partition(), bigset_clock:clock(), elements()}}.
merge_sets(Sets) ->
    merge_sets(Sets, []).

-spec merge_sets([{partition(), bigset_clock:clock(), elements()}], repairs()) ->
                        {repairs(), {partition(), bigset_clock:clock(), elements()}}.
merge_sets([Set|Sets], Repairs) ->
    lists:foldl(fun(S, {RepairsAcc, SetAcc}) ->
                        merge_set(SetAcc, S, [], RepairsAcc)
                end,
                {Repairs, Set},
                Sets).

-spec merge_ids(partition(), partition()) -> partition().
merge_ids(P1, P2) ->
    partition_to_list(P1) ++ partition_to_list(P2).

partition_to_list(L) when is_list(L) ->
    L;
partition_to_list(I) ->
    [I].

%% @doc given a pair of sets, and a dictionary of repairs, merge into
%% one set.
merge_set({P1, Clock1, []}, {P2, Clock2, []}, ElementAcc, RepairAcc) ->
    {RepairAcc, {merge_ids(P1, P2), bigset_clock:merge(Clock1, Clock2), lists:reverse(ElementAcc)}};
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
    case orddict:find(Partition, Repairs) of
        {ok, Repair} ->
            Repair;
        error ->
            []
    end.

set_repair(Partion, Repair, Repairs) ->
    orddict:store(Partion, Repair, Repairs).

add_repair(Action, Element, Dot, Repair) ->
    update_element(Action, Element, Dot, Repair).

update_element(Action, Element, Dot, Entries) ->
    {Fun, Default} = update_fun(Action, Dot),

    orddict:update(Element, Fun,
                   Default,
                   Entries).

update_fun(add, Dot) ->
    {fun({Adds, Removes}) ->
             {lists:umerge([Dot], Adds), Removes}
     end,
     {[Dot], []}
    };
update_fun(remove, Dot) ->
    {fun({Adds, Removes}) ->
             {Adds, lists:umerge([Dot], Removes)}
     end,
     {[], [Dot]}
    }.

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
    ?assertEqual([], get_repair(Partition, [])),
    Expected = [{<<"e1">>, {[{a, 3}, {b, 5}, {d, 1}], []}},
                 {<<"e4">>, {[{a, 6}], []}},
                 {<<"e100">>, {[], [{a, 12}, {b, 56}]}}],
    P1 = {Partition, Expected},
    Repairs = [P1,
               {2, [{<<"e99">>, {[], [{a, 3}]}}]},
               {100, [{<<"e9">>, {[{x, 13}], []}}]}
              ],
    ?assertEqual(Expected, get_repair(Partition, Repairs)).

add_repair_test() ->
    Elem = <<"e1">>,
    Dot = {a, 1},
    AddExpected = [{Elem, {[Dot], []}}],
    RemExpected = [{Elem, {[], [Dot]}}],
    AddActual = add_repair(add, Elem, Dot, []),
    RemActual = add_repair(remove, Elem, Dot, []),
    ?assertEqual(AddExpected, AddActual),
    ?assertEqual(RemExpected, RemActual).

repair_update_test() ->
    Partition = 1,
    Element = <<"e1">>,
    Dot = {a, 1},
    Repairs = [],
    Expected = [{Partition,[{Element, {[], [Dot]}}]}],
    Actual = repair_update(remove, Partition, Element, Dot, Repairs),
    ?assertEqual(Expected, Actual).

repair_remove_test() ->
    Expected = [{2, [{<<"e1">>, {[], [{a, 2}]}}]}],
    Actual = repair_remove(2, <<"e1">>, {a, 2}, []),
    ?assertEqual(Expected, Actual).

%% just check that expected repairs come back, ideally this should be
%% quickchecked!
merge_test() ->
    Clock1 = {[{a, 10}, {b, 3}, {d, 4}], %%base vv of clock1
              [{b, [5,6]}, {c, [13]}, {d, [6,7]}]}, %% dot cloud 1
    Set1 = {1, %% partition 1
            Clock1,
            [{<<"element1">>, [{a, 8}, {b, 5}]},
             {<<"element3">>, [{a, 1}, {c, 13}]},
             {<<"element4">>, [{a, 3}, {d, 1}]},
             {<<"element6">>, [{a, 10}]}] %% elements 1
           },

    Clock2 = {[{a, 5}, {c, 16}, {b, 7}, {d, 2}], %%base vv of clock2
              [{a, [8]}]}, %% dot cloud 2
    Set2 = {2, %% partition 2
            Clock2,
            [{<<"element1">>, [{a, 8}, {b, 5}]},
             {<<"element2">>, [{a, 4}, {d, 2}, {c, 15}]},
             {<<"element4">>, [{a, 3}, {c, 5}]},
             {<<"element5">>, [{c, 16}]}] %% elements 2
           },

    %% we expect a repair entry for both partitions
    ExpectedRepairs =   [{1,[{<<"element2">>,{[{c,15}],[]}},
                             {<<"element3">>,{[],[{a,1},{c,13}]}},
                             {<<"element4">>,{[{c,5}],[{d,1}]}},
                             {<<"element5">>,{[{c,16}],[]}}]},
                         {2,
                          [{<<"element2">>,{[],[{a,4},{d,2}]}},
                           {<<"element6">>,{[{a,10}],[]}}]}],

    ExpectedClock = bigset_clock:merge(Clock1, Clock2),
    ExpectedElements = [{<<"element1">>, [{a, 8}, {b, 5}]}, %%both
                        {<<"element2">>, [{c, 15}]}, %% one remaining dot from 2
                        {<<"element4">>, [{a, 3}, {c, 5}]}, %% one remaining dot from 2 and 1 new one
                        {<<"element5">>, [{c, 16}]}, %% in set 2
                        {<<"element6">>, [{a, 10}]} %% in set 1
                       ],

    {Repairs, {Id, Clock, Elements}} = merge_sets([Set1, Set2]),

    ?assertEqual([1,2], Id),
    ?assertEqual(ExpectedRepairs, lists:sort(Repairs)),
    ?assertEqual(ExpectedClock, Clock),
    ?assertEqual(ExpectedElements, Elements).


old_repair_to_new_repair(Repairs) ->
    lists:map(fun({P, A, R}) ->
                      {P, old_to_new(A, R)}
              end,
              Repairs).

old_to_new(A, R) ->
    NewAdds = orddict:map(fun(_E, As) ->
                                  {As, []}
                          end,
                          A),
    NewRems = orddict:map(fun(_E, Rs) ->
                                  {[], Rs}
                          end,
                          R),
    orddict:merge(fun(_E, {Adds, []}, {[], Rems}) ->
                          {Adds, Rems}
                  end,
                  NewAdds,
                  NewRems).


-endif.

-ifdef(EQC).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_test_() ->
    {timeout, 60, [
                   ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(30, ?QC_OUT(prop_repair()))))
                  ]}.

run() ->
    run(?NUMTESTS).

run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_repair())).

eqc_check() ->
    eqc:check(prop_repair()).

eqc_check(File) ->
    {ok, Bytes} = file:read_file(File),
    CE = binary_to_term(Bytes),
    eqc:check(prop_repair(), CE).

-define(ELEMENTS, ['A', 'B', 'C', 'D', 'X', 'Y', 'Z']).

gen_element() ->
    elements(?ELEMENTS).

prop_repair() ->
  ?FORALL({_DotToElement, Events, _Clocks, SystemRemoves, _LocalRemoves, Elements, Sets},
          testcase(),
          begin
            Set = {{[{Id,N} || {Id,N} <- Events, N>0], []}, Elements},
            {Repairs, {_Id, Clock, MergedElements}} = merge_sets(Sets),
            RepairedSets = apply_repairs(Repairs, Sets),
            RepairLengths = [length(Repair) || {_, Repair} <- Repairs],
            aggregate(with_title("repaired elements"), RepairLengths,
                      measure(clock_width, length(Events),
                              measure(repair_length, length(Repairs),
                                      measure(system_removes,length(SystemRemoves),
                                              (%aggregate(with_title("# local removes"),
                                                %         [length(Rs) || {_Id,Rs} <- LocalRemoves],
                                                conjunction(
                                                  [{Id, set_equals(Set, {SClock, SElements})} ||
                                                    {Id, SClock, SElements} <- RepairedSets] ++
                                                    [{merge, set_equals(Set, {Clock, MergedElements})}])
                                              )))))
          end).

set_equals({Clock1, S1}, {Clock2, S2}) ->
    EventSet1 = bigset_clock:clock_to_set(Clock1),
    EventSet2 = bigset_clock:clock_to_set(Clock2),
    Diff = ordsets:subtract(EventSet1, EventSet2),

    Digest = elements_to_digest(S2),

    conjunction([{Dot, not lists:member(Dot, Digest)} || Dot <- Diff] ++
                    [{elements_equal,
                      equals(S1, S2)}]).
                      %% ?WHENFAIL(io:format("System:: ~p~n /= ~n Repaired Set:: ~p~n", [{Clock1,S1}, {Clock2,S2}]),
                      %%           {Clock1,S1} == {Clock2,S2})}]).

elements_to_digest(Elems) ->
    orddict:fold(fun(_E, Dots, Acc) ->
                         lists:merge(Dots, Acc) end,
                 [],
                 Elems).

%% Custom shrinking by John Hughes of quviq
testcase() ->
      ?LET(DotToElement, function1(gen_element()),
           ?LET(Events, bigset_clock:gen_all_system_events(),
                ?LET({Clocks, SystemRemoves}, {bigset_clock:gen_clocks(Events), gen_removes(Events)},
                     ?LET({LocalRemoves, Elements}, {gen_local_removes(SystemRemoves, Clocks),
                                                     gen_elements({Events, []}, SystemRemoves, DotToElement)},
                          ?LET(Sets, gen_sets2(Clocks, LocalRemoves, DotToElement),
                               shrink_testcase({DotToElement,Events,Clocks,SystemRemoves,
                                                 LocalRemoves,Elements,Sets})))))).

shrink_testcase(TC={_DotToElement, Events, _Clocks, _SystemRemoves, _LocalRemoves, _Elements, _Sets}) ->
  ?SHRINK(TC,
          [shrink_testcase(purge_dot_testcase(Dot,TC))
           || Dot <- all_dots(Events)]).

all_dots(Events) ->
  [{Id,I}
   || {Id,N} <- Events,
      I <- lists:seq(1,N)].

purge_dot_testcase(Dot,{DotToElement,Events,Clocks,SystemRemoves,LocalRemoves,Elements,Sets}) ->
  {purge_dot_function(Dot,DotToElement),
   purge_dot_events(Dot,Events),
   purge_dot_clocks(Dot,Clocks),
   purge_dot_removes(Dot,SystemRemoves),
   purge_dot_local_removes(Dot,LocalRemoves),
   purge_dot_elements(Dot,Elements),
   purge_dot_sets(Dot,Sets)}.

purge_dot_function({Id,N},F) ->
  fun({Id2,N2}) ->
      F({Id2,if Id==Id2 andalso N2 >= N ->
                 N2+1;
                true ->
                 N2
             end})
  end.

purge_dot_events(Dot,Events) ->
  [purge_dot_period(Dot,E) || E <- Events].

purge_dot_clocks(Dot,Clocks) ->
  [{Id,purge_dot_clock(Dot,Clock)} || {Id,Clock} <- Clocks].

purge_dot_clock(Dot,{VC,DC}) ->
  {[{Id,N} || {Id,N} <- purge_dot_events(Dot,VC), N>0],
   purge_dot_dotcloud(Dot,DC)}.

purge_dot_dotcloud(Dot,DC) ->
  [purge_dot_dot(Dot,D) || D <- lists:delete(Dot,DC)].

purge_dot_removes(Dot,Removes) ->
  purge_dot_dotcloud(Dot,Removes).

purge_dot_local_removes(Dot,LocalRemoves) ->
  [{Id,purge_dot_removes(Dot,R)} || {Id,R} <- LocalRemoves].

purge_dot_elements(Dot,Elements) ->
  [{X,purge_dot_dotcloud(Dot,DC)} || {X,DC} <- Elements,
                                  DC /= [Dot]].

purge_dot_sets(Dot,Sets) ->
  [{Id,purge_dot_clock(Dot,Clock),purge_dot_elements(Dot,Elements)}
   || {Id,Clock,Elements} <- Sets].

purge_dot_period({Id,I},{Id,N}) ->
  {Id,if I =< N ->
          N-1;
         I > N ->
          N
      end};
purge_dot_period(_,Period) ->
  Period.

purge_dot_dot({Id,I},{Id,J}) ->
  {Id,if I < J ->
          J-1;
         I > J ->
          J
      end};
purge_dot_dot(_,Dot) ->
  Dot.

prop_diverges() ->
    fails(?FORALL(Events, bigset_clock:gen_all_system_events(),
            ?FORALL(Clocks, bigset_clock:gen_clocks(Events),
                    ?FORALL(SystemRemoves, gen_removes(Events),
                            ?FORALL(LocalRemoves, gen_local_removes(SystemRemoves, Clocks),
                                    ?FORALL(DotToElement, function1(gen_element()),
                                            ?FORALL(Elements, gen_elements({Events, []}, SystemRemoves, DotToElement),
                                                    ?FORALL(Sets, gen_sets2(Clocks, LocalRemoves, DotToElement),
                                                            begin
                                                                Set = [{{Events, []}, Elements}],
                                                                equals(Set, lists:usort(Sets))
                                                            end)))))))).


gen_sets() ->
    ?LET(Events, bigset_clock:gen_all_system_events(),
         ?LET(Clocks, bigset_clock:gen_clocks(Events),
              ?LET(SystemRemoves, gen_removes(Events),
                   ?LET(LocalRemoves, gen_local_removes(SystemRemoves, Clocks),
                        ?LET(DotToElement, function1(gen_element()),
                                  gen_sets2(Clocks, LocalRemoves, DotToElement)))))).


apply_repairs(Repairs, Sets) ->
    [case orddict:find(Id, Repairs) of
         error ->
             Set;
         {ok, Repair} ->
             apply_repair(Repair, Set)
     end || {Id, _Clock, _Elem}=Set <- Sets].

apply_repair(Repairs, Set) ->
    lists:foldl(fun({Element, {Adds, Removes}}, {Id, Clock, Elements}) ->
                        {Clock2, Elements2} = apply_removes(Clock, Elements, Element, Removes),
                        {Clock3, Elements3} = apply_adds(Clock2, Elements2, Element, Adds),
                        {Id, Clock3, Elements3}
                end,
                Set,
                Repairs).

apply_removes(Clock, Elements, Element, Removes) ->
    lists:foldl(fun(Dot, {ClockAcc, ElementAcc}) ->
                        case bigset_clock:seen(Dot, ClockAcc) of
                            false ->
                                {bigset_clock:add_dot(Dot, ClockAcc), ElementAcc};
                            true ->
                                {ClockAcc,
                                 orddict:filter(fun(_Key, []) -> false;
                                                   (_Key, _Val) -> true
                                                end,
                                                orddict:update(Element, fun(Dots) ->
                                                                                Dots -- [Dot] end,
                                                               [Dot],
                                                               ElementAcc))}
                        end
                end,
                {Clock, Elements},
                Removes).

apply_adds(Clock, Elements, Element, Adds) ->
    lists:foldl(fun(Dot, {ClockAcc, ElementAcc}) ->
                        case bigset_clock:seen(Dot, ClockAcc) of
                            true ->
                                {ClockAcc, ElementAcc};
                            false ->
                                {bigset_clock:add_dot(Dot, ClockAcc),
                                 orddict:update(Element, fun(Dots) ->
                                                                 lists:umerge([Dot], Dots) end,
                                                [Dot],
                                                ElementAcc)}
                        end
                end,
                {Clock, Elements},
                Adds).

gen_system_digest(Events) ->
    Set =  bigset_clock:clock_to_set({Events, []}),
    growing_sublist(Set).

gen_local_digests(Digest, Clocks) ->
    [{Id, gen_local_digest(Digest, Clock)} || {Id, Clock} <- Clocks].

gen_local_digest(Digest, Clock) ->
    Set = bigset_clock:clock_to_set(Clock),
    ?LET(Diff, growing_sublist(ordsets:subtract(Set, Digest)),
         ordsets:union(Diff, Digest)).

gen_sets(Clocks, Digests, DotToElement) ->
    lists:zipwith(fun({Id, Clock}, {Id, Digest}) ->
                          {Id, Clock,  lists:foldl(fun(Dot, Acc) ->
                                                           Element = DotToElement(Dot),
                                                           orddict:update(Element,
                                                                          fun(Dots) ->
                                                                                  lists:umerge([Dot],[Dots])
                                                                          end,
                                                                          [Dot],
                                                                          Acc)
                                                   end,
                                                   orddict:new(),
                                                   Digest)}
                  end,
                  Clocks,
                  Digests).

%% since sublist shrinks toward the empty set this is a subset
%% that shrinks towards the whole set
growing_sublist(Set) ->
    ?LET(Removals, sublist(Set),
         ordsets:subtract(Set, Removals)).

gen_removes(Events) ->
    Set = bigset_clock:clock_to_set({Events, []}),
    sublist(Set).

gen_local_removes(Removes, Clocks) ->
    ?LET(DotsNodes,
         [{Dot, non_empty(sublist([Id || {Id, Clock} <- Clocks, bigset_clock:seen(Dot, Clock)]))}
            || Dot <- Removes],
         [{Id, gen_local_remove(Id, DotsNodes)} || {Id, _Clock} <- Clocks]).

gen_local_remove(Id, DotsNodes) ->
    [Dot || {Dot, Nodes} <- DotsNodes,
            lists:member(Id, Nodes)].

gen_elements(Clock, Removes, DotToElement) ->
    ClockSet = bigset_clock:clock_to_set(Clock),
    Digest = ordsets:subtract(ClockSet, Removes),
    lists:foldl(fun(Dot, Acc) ->
                        Element = DotToElement(Dot),
                        orddict:update(Element,
                                       fun(Dots) ->
                                               lists:umerge([Dot],Dots)
                                       end,
                                       [Dot],
                                       Acc)
                end,
                orddict:new(),
                Digest).

gen_sets2(Clocks, Removes, DotToElement) ->
    lists:zipwith(fun({Id, Clock}, {Id, Removed}) ->
                          Elements = gen_elements(Clock, Removed, DotToElement),
                          {Id, Clock, Elements}
                  end,
                  Clocks,
                  Removes).

-endif.

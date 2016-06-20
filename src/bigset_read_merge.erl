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

%% @doc merge a list of (possibly partial) sets into a single set and
%% a dictionary of repairs.
-spec merge_sets([{partition(), bigset_clock:clock(), elements()}]) ->
                        {repairs(), bigset_clock:clock(), elements()}.
merge_sets([]) ->
    {[], bigset_clock:fresh(), []};
merge_sets([{_P, Clock, Elements}]) ->
    {[], {Clock, Elements}};
merge_sets(Sets) ->
    lists:foldl(fun(Set, {RepairsAcc, SetAcc}) ->
                        merge_set(Set, SetAcc, [], RepairsAcc)
                end,
                {[], hd(Sets)},
                tl(Sets)).

merge_set({_P1, Clock1, []}, {_P2, Clock2, []}, ElementAcc, RepairAcc) ->
    {RepairAcc, bigset_clock:merge(Clock1, Clock2), ElementAcc};
merge_set({P1, Clock1, [{Element, Dots1} | T1]}, {P2, Clock2, [{Element2, _Dots2} | _T2]=E2}, ElementAcc, RepairAcc) when Element < Element2 ->
    {RepairAcc2, SurvivingDots} = handle_dots(Element, {P1, Dots1}, {P2, Clock2}, RepairAcc),
    ElementAcc2 = maybe_store_dots(Element, SurvivingDots, ElementAcc),
    merge_set({P1, Clock1, T1}, {P2, Clock2, E2}, ElementAcc2, RepairAcc2);
merge_set({P1, Clock1, [{Element, _Dots1} | _T1]}=E1, {P2, Clock2, [{Element2, Dots2} | T2]}, ElementAcc, RepairAcc) when Element > Element2 ->
    {RepairAcc2, SurvivingDots} = handle_dots(Element2, {P2, Dots2}, {P1, Clock1}, RepairAcc),
    ElementAcc2 = maybe_store_dots(Element, SurvivingDots, ElementAcc),
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

repair_remove(Partion, Element, Dot, Repairs) ->
    Repair = get_repair(Partion, Repairs),
    add_remove(Element, Dot, Repair),
    set_repair(Partion, Repair, Repairs).

repair_add(Partion, Element, Dot, Repairs) ->
    Repair = get_repair(Partion, Repairs),
    add_add(Element, Dot, Repair),
    set_repair(Partion, Repair, Repairs).

get_repair(Partion, Repairs) ->
    case orddict:find(Partion, Repairs) of
        {ok, Value} ->
            Value;
        false ->
            %% @TODO should be a record??
            {Partion, [], []}
    end.

set_repair(Partion, Repair, Repairs) ->
    orddict:store(Partion, Repair, Repairs).

add_repair(add, Element, Dot, Repair) ->
    add_repair(2, Element, Dot, Repair);
add_repair(remove, Element, Dot, Repair) ->
    add_repair(3, Element, Dot, Repair);
add_repair(N, Element, Dot, Repair) when N == 2, N == 3 ->
    Updated = update_element(Element, Dot, element(N, Repair)),
    erlang:setelelment(N, Repair, Updated).

add_remove(Element, Dot, Repair) ->
   add_repair(remove, Element, Dot, Repair).

add_add(Element, Dot, Repair) ->
    add_repair(add, Element, Dot, Repair).

update_element(Element, Dot, Entries) ->
    orddict:update(Element, fun(L) ->
                                    lists:umerge(Dot, L)
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



%% %% @priv Just that same old orswot merge, again, but with added
%% %% `repairs()' action @TODO try a N-way rather than pairwise merge
%% -spec merge_set({partition(), bigset_clock:clock(), [{member(), dot_list()}]},
%%                 {partition(), bigset_clock:clock(), [{member(), dot_list()}]},
%%                 repairs()) ->
%%                        {repairs(), bigset_clock:clock(), [{member(), dot_list()}]}.
%% merge_set({P1, Clock1, Elements1}, {P2, Clock2, Elements2}, Repairs0) ->
%%     {Repairs1, P1Keep, P2Unique} = lists:foldl(fun({Element, Dots1}, {RepairAcc, Keep, P2Unique}) ->
%%                                                        case lists:keytake(Element, 1, P2Unique) of
%%                                                            {value, {Element, Dots2}, P2Unique2} ->
%%                                                                %% In both sides, keep it, maybe
%%                                                                {RepairAcc2, SurvivingDots} = merge_dots(Element,
%%                                                                                                         {P1, Dots1, Clock1},
%%                                                                                                         {P2, Dots2, Clock2},
%%                                                                                                         RepairAcc),
%%                                                                {RepairAcc2,
%%                                                                 maybe_store_dots(Element, SurvivingDots, Keep),
%%                                                                 P2Unique2};
%%                                                            false ->
%%                                                                %% Only keep if not seen/removed by other clock
%%                                                                {RepairAcc2, SurvivingDots} = handle_dots(Element, {P1, Dots1}, {P2, Clock2}, RepairAcc),
%%                                                                Keep2 = maybe_store_dots(Element, SurvivingDots, Keep),
%%                                                                {RepairAcc2, Keep2, P2Unique}
%%                                                        end
%%                                                end,
%%                                                {Repairs0, [], Elements2},
%%                                                Elements1),

%%     %% Filter the unique elements left in elements2
%%     {Repairs2, Elements} = lists:foldl(fun({Element, Dots}, {RepairAcc, Keep}) ->
%%                                               SurvivingDots = handle_dots(Element, {P2, Dots}, {P1, Clock1}, RepairAcc),
%%                                               maybe_store_dots(Element, SurvivingDots, Keep)
%%                                       end,
%%                                       {Repairs1, P1Keep},
%%                                       P2Unique),

%%     {Repairs2, bigset_clock:merge(Clock1, Clock2), lists:sort(Elements)}.


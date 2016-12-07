%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% @TODO interface/behaviour
%%% @TODO eqc generalise
%%% @TODO types
%%% @TODO docs
%%% @end

-module(bigset_clock).

-include("bigset.hrl").

-export_type([clock/0, dot/0]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-export([
         add_dot/2,
         all_nodes/1,
         clock_mod/0,
         descends/2,
         dominates/2,
         equal/2,
         fresh/0,
         fresh/1,
         get_counter/2,
         get_dot/2,
         increment/2,
         intersection/2,
         merge/1,
         merge/2,
         seen/2,
         subtract_seen/2,
         tombstone_from_digest/2
        ]).


-type clock() :: bigset_clock_naive:clock() | bigset_clock_ba:clock().

-callback fresh() ->
    clock().

-callback fresh(dot()) ->
    clock().

-callback increment(actor(), clock()) ->
    clock().

-callback get_counter(actor(), clock()) ->
    non_neg_integer().

-callback get_dot(actor(), clock()) ->
    dot().

-callback all_nodes(clock()) ->
    list(actor()).

-callback merge(clock(), clock()) ->
    clock().

-callback merge(list(clock())) ->
    clock().

-callback add_dot(dot(), clock()) ->
    clock().

-callback seen(dot(), clock()) ->
    boolean().

-callback subtract_seen(clock(), list(dot())) ->
    list(dot()).

-callback descends(clock(), clock()) ->
    boolean().

-callback equal(clock(), clock()) ->
    clock().

-callback dominates(clock(), clock()) ->
    boolean().

-callback intersection(clock(), clock()) ->
    clock().

-callback tombstone_from_digest(clock(), clock()) ->
    clock().

-ifdef(EQC).
%% Required Callbacks for EQC testing of clock implementations

-callback set_to_clock(list()) ->
    clock().

-callback clock_to_set(clock()) ->
    list().

-callback clock_from_event_list(actor(), list(pos_integer())) ->
    clock().

-callback is_compact(clock()) ->
    boolean().

-callback to_version_vector(clock()) ->
    riak_dt_vclock:vclock().

-endif.

clock_mod() ->
    ?BS_CLOCK.

fresh() ->
    ?BS_CLOCK:fresh().

fresh({Actor, Cnt}) ->
    ?BS_CLOCK:fresh({Actor, Cnt}).

increment(Actor, Clock) ->
    ?BS_CLOCK:increment(Actor, Clock).

get_counter(Actor, Clock) ->
    ?BS_CLOCK:get_counter(Actor, Clock).

get_dot(Actor, Clock) ->
    ?BS_CLOCK:get_dot(Actor, Clock).

all_nodes(Clock) ->
    ?BS_CLOCK:all_nodes(Clock).

merge(Clock1, Clock2) ->
    ?BS_CLOCK:merge(Clock1, Clock2).

merge(Clocks) ->
    ?BS_CLOCK:merge(Clocks).

add_dot(Dot, Clock) ->
    ?BS_CLOCK:add_dot(Dot, Clock).

seen(Dot, Clock) ->
    ?BS_CLOCK:seen(Dot, Clock).

subtract_seen(Clock, Dots) ->
    ?BS_CLOCK:subtract_seen(Clock, Dots).

descends(A, B) ->
    ?BS_CLOCK:descends(A, B).

equal(A, B) ->
    ?BS_CLOCK:equal(A, B).

dominates(A, B) ->
    ?BS_CLOCK:dominates(A, B).

intersection(ClockA, ClockB) ->
    ?BS_CLOCK:intersection(ClockA, ClockB).

tombstone_from_digest(Clock, Digest) ->
    ?BS_CLOCK:tombstone_from_digest(Clock, Digest).

-ifdef(EQC).

clock_to_set(Clock) ->
    ?BS_CLOCK:clock_to_set(Clock).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-define(NUMTESTS, 1000).

eqc_tests(Mod) ->
    TestList = [{10, fun bigset_clock:prop_merge_clocks/1},
                {10, fun bigset_clock:prop_merge/1},
                {10, fun bigset_clock:prop_merge2/1},
                {30, fun bigset_clock:prop_intersection/1},
                {30, fun bigset_clock:prop_tombstone_from_digest/1},
                {10, fun bigset_clock:prop_add_dot/1},
                {10, fun bigset_clock:prop_seen/1},
                 {5, fun bigset_clock:prop_increment/1}
               ],

    {timeout, 60*length(TestList), [
                                    {timeout, 60, ?_assertEqual(true,
                                                                eqc:quickcheck(eqc:testing_time(Time, ?QC_OUT(Prop(Mod)))))} ||
                                       {Time, Prop} <- TestList]}.

%% checks that when all clocks are merged they are compact and
%% represent all events in the system.
prop_merge_clocks(Mod) ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL(Clocks, gen_clocks(Events, Mod),
                    begin
                        Merged = Mod:merge([Clock || {_Actor, Clock} <- Clocks]),
                        ?WHENFAIL(
                           begin
                               io:format("Clocks ~p~n", [Clocks]),
                               io:format("Events ~p~n", [Events]),
                               io:format("Merged ~p~n", [Merged])
                           end,
                           conjunction([{compact, Mod:is_compact(Merged)},
                                        {equal, equals(Mod:to_version_vector(Merged), Events)}]))
                    end)).

%% checks that for any subset of clocks the union of them descends any
%% one of them.
prop_merge(Mod) ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL(Clocks, gen_clocks(Events, Mod),
                    ?FORALL(SubSet, sublist(Clocks),
                            begin
                                Merged = Mod:merge([Clock || {_Actor, Clock} <- SubSet]),
                                ?WHENFAIL(
                                   begin
                                       io:format("Subset of clocks are ~p~n", [SubSet]),
                                       io:format("Merged is ~p~n", [Merged])
                                   end,
                                measure(num_clocks, length(Clocks),
                                        measure(num_sub, length(SubSet),
                                                conjunction([{Actor, Mod:descends(Merged, Clock)}
                                                             || {Actor, Clock} <- SubSet]))))
                            end))).

%% checks the general property or merge, that if clocks are concurrent
%% the result dominates the pair, if equal the result is equal to
%% both, and if there is a linial relationship, the result descends
%% both.
prop_merge2(Mod) ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL(Clocks, gen_clocks(Events, Mod),
                    ?FORALL({{_, Clock1}, {_, Clock2}}, {elements(Clocks), elements(Clocks)},
                            begin
                                Merged = Mod:merge(Clock1, Clock2),
                                {Type, Prop} = case {Mod:descends(Clock1, Clock2),
                                                     Mod:descends(Clock2, Clock1)} of
                                                   {true, true} ->
                                                       {equal, Mod:equal(Merged, Clock1) andalso
                                                        Mod:equal(Merged, Clock2)};
                                                   {false, false} ->
                                                       {concurrent,
                                                        Mod:dominates(Merged, Clock1) andalso
                                                        Mod:dominates(Merged, Clock2)};
                                                   _ ->
                                                       {linial,
                                                        Mod:descends(Merged, Clock1) andalso
                                                        Mod:descends(Merged, Clock2)}
                                               end,

                                aggregate([Type], Prop)
                            end))).

%% @doc verifies that intersection behaves the same as ordset:intersection
prop_intersection(Mod) ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL([{_, Clock1}, {_, Clock2} | _], gen_clocks(Events, Mod),
                    begin
                        Res = Mod:clock_to_set(Mod:intersection(Clock1, Clock2)),
                        Set1 = Mod:clock_to_set(Clock1),
                        Set2 = Mod:clock_to_set(Clock2),
                        Expected = ordsets:intersection(Set1, Set2),
                        ?WHENFAIL(
                           begin
                               io:format("Clock 1 ~p~n", [Clock1]),
                               io:format("Clock 2 ~p~n", [Clock2]),
                               io:format("intersection ~p~n", [Res])
                           end,
                           equals(Expected, Res))
                    end)).

%% @doc verifies that a tombstone can be generated from a digest
prop_tombstone_from_digest(Mod) ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL([{_, {_Base1, _DC1}=Clock1} | _], gen_clocks(Events, Mod),
                    ?FORALL({_Base2, _DC}=Clock2, gen_sub_clock(Clock1, Mod),
                            begin
                                Res = Mod:clock_to_set(Mod:tombstone_from_digest(Clock1, Clock2)),
                                Set1 = Mod:clock_to_set(Clock1),
                                Set2 = Mod:clock_to_set(Clock2),
                                Expected = ordsets:subtract(Set1, Set2),
                                %% @TODO(rdb) add check_distribution when we get off r16
                                ?WHENFAIL(
                                   begin
                                       io:format("Set 1 ~p~n", [Set1]),
                                       io:format("Set 2 ~p ~n", [Set2])
                                   end,
                                   equals(Expected, Res))
                            end))).

%% @doc a property that verifies when a dot is added to a clock, it is
%% a member of the clocks set of seen events
prop_add_dot(Mod) ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL(Clocks, gen_clocks(Events, Mod),
                    ?FORALL({_Actor, Clock}, elements(Clocks),
                            ?FORALL(Dot, gen_dot(),
                                    begin
                                        Res = Mod:add_dot(Dot, Clock),
                                        Before = Mod:clock_to_set(Clock),
                                        After = Mod:clock_to_set(Res),
                                        ?WHENFAIL(
                                           begin
                                               io:format("Dot ~p~n", [Dot]),
                                               io:format("Clock ~p~n", [Clock])
                                           end,
                                           aggregate(with_title(re_add), [ordsets:is_element(Dot, Before)],
                                                     ordsets:is_element(Dot, After)))
                                    end)))).

%% @doc a property that verifies that the clock recognises an event as
%% "seen" if it is in the set of events the clock contains.
prop_seen(Mod) ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL(Clocks, gen_clocks(Events, Mod),
                    ?FORALL({_Actor, Clock}, elements(Clocks),
                            ?FORALL(Dot, gen_dot(),
                                    begin
                                        Set = Mod:clock_to_set(Clock),
                                        ?WHENFAIL(
                                           begin
                                               io:format("Dot ~p~n", [Dot]),
                                               io:format("Clock ~p~n", [Clock])
                                           end,
                                           aggregate(with_title(seen), [ordsets:is_element(Dot, Set)],
                                                     equals(ordsets:is_element(Dot, Set), Mod:seen(Dot, Clock))))
                                    end)))).

prop_increment(Mod) ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL(Clocks, gen_clocks(Events, Mod),
                    ?FORALL({Actor, Clock}, elements(Clocks),
                            begin
                                Before = proplists:get_value(Actor, Events),
                                {{Actor, NewCntr}, Clock2} = Mod:increment(Actor, Clock),
                                conjunction([{incremented, equals(Before+1, NewCntr)},
                                             {new_cntr, equals(NewCntr, Mod:get_counter(Actor, Clock2))}])
                            end))).

gen_clocks() ->
    ?LET(Events, gen_all_system_events(),
         gen_clocks(Events, ?BS_CLOCK)).

gen_clocks(Events) ->
    gen_clocks(Events, ?BS_CLOCK).

gen_clocks(Events, Mod) ->
    [{Actor, gen_clock(Me, Events, Mod)} || {Actor, _}=Me <- Events].

gen_clock(Me, Events, Mod) ->
    ?LET(ClockMembers, sublist(Events),
         ?LET(Clocks, [Mod:fresh(Me) | [gen_clock(Event, Mod) || Event <- ClockMembers,
                                    Event /= Me]],
              Mod:merge(Clocks))).

gen_sub_clock(Clock, Mod) ->
    Set = Mod:clock_to_set(Clock),
    ?LET(SubSet, sublist(ordsets:to_list(Set)),
         Mod:set_to_clock(ordsets:from_list(SubSet))).

gen_clock({Actor, Max}, Mod) ->
    ?LET(Events, non_empty(sublist(lists:seq(1, Max))),
         Mod:clock_from_event_list(Actor, Events)).

%% @priv generates the single system wide version vector that
%% describes all events at a moment in time. Imagine a snapshot of a
%% distributed system, each node's events are contiguous, this is that
%% clock. We can use it to generate a valid bigset system state.
-spec gen_all_system_events() -> list({actor(), pos_integer()}).
gen_all_system_events() ->
    ?LET(Actors, choose(2, 10), gen_all_system_events(Actors)).

-spec gen_all_system_events(pos_integer()) ->
                                   list({actor(), pos_integer()}).
gen_all_system_events(Actors) ->
     [{<<Actor>> , choose(1, 100)} || Actor <- lists:seq(1, Actors)].

gen_dot() ->
    ?LET(Actor, choose(1, 10), {<<Actor>>, choose(1, 100)}).

-endif.

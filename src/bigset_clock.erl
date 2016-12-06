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
         fresh/0,
         fresh/1,
         increment/2,
         get_dot/2,
         all_nodes/1,
         merge/2,
         merge/1,
         add_dot/2,
         seen/2,
         descends/2,
         equal/2,
         dominates/2,
         intersection/2,
         tombstone_from_digest/2
        ]).


-type clock() :: bigset_clock_naive:clock() | bigset_clock_ba:clock().

-callback fresh() ->
    clock().

-callback fresh(dot()) ->
    clock().

-callback increment(actor(), clock()) ->
    clock().

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

%%% Behaviour EQC API funs

is_compact(Clock) ->
    ?BS_CLOCK:is_compact(Clock).

to_version_vector(Clock) ->
    ?BS_CLOCK:to_version_vector(Clock).

clock_from_event_list(Actor, Events) ->
    ?BS_CLOCK:clock_from_event_list(Actor, Events).


-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_test_() ->
    {timeout, 60, [
                   ?_assertEqual(true, eqc:quickcheck(?QC_OUT(prop_merge_clocks()))),
                   ?_assertEqual(true, eqc:quickcheck(?QC_OUT(prop_intersection()))),
                   ?_assertEqual(true, eqc:quickcheck(?QC_OUT(prop_complement())))
                  ]}.

run(Prop) ->
    run(Prop, ?NUMTESTS).

run(Prop, Count) ->
    eqc:quickcheck(eqc:numtests(Count, Prop)).

eqc_check(Prop) ->
    eqc:check(Prop).

eqc_check(Prop, File) ->
    {ok, Bytes} = file:read_file(File),
    CE = binary_to_term(Bytes),
    eqc:check(Prop, CE).

%% checks that when all clocks are merged they are compact and
%% represent all events in the system.
prop_merge_clocks() ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL(Clocks, gen_clocks(Events),
                    begin
                        Merged = merge([Clock || {_Actor, Clock} <- Clocks]),
                        ?WHENFAIL(
                           begin
                               io:format("Clocks ~p~n", [Clocks]),
                               io:format("Events ~p~n", [Events]),
                               io:format("Merged ~p~n", [Merged])
                           end,
                           conjunction([{compact, is_compact(Merged)},
                                        {equal, equals(to_version_vector(Merged), Events)}]))
                    end)).

%% checks that for any subset of clocks the union of them descends any
%% one of them.
prop_merge() ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL(Clocks, gen_clocks(Events),
                    ?FORALL(SubSet, sublist(Clocks),
                            begin
                                Merged = merge([Clock || {_Actor, Clock} <- SubSet]),
                                ?WHENFAIL(
                                   begin
                                       io:format("Subset of clocks are ~p~n", [SubSet]),
                                       io:format("Merged is ~p~n", [Merged])
                                   end,
                                measure(num_clocks, length(Clocks),
                                        measure(num_sub, length(SubSet),
                                                conjunction([{Actor, descends(Merged, Clock)}
                                                             || {Actor, Clock} <- SubSet]))))
                            end))).

%% checks the general property or merge, that if clocks are concurrent
%% the result dominates the pair, if equal the result is equal to
%% both, and if there is a linial relationship, the result descends
%% both.
prop_merge2() ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL(Clocks, gen_clocks(Events),
                    ?FORALL({{_, Clock1}, {_, Clock2}}, {elements(Clocks), elements(Clocks)},
                            begin
                                Merged = merge(Clock1, Clock2),
                                {Type, Prop} = case {descends(Clock1, Clock2),
                                                     descends(Clock2, Clock1)} of
                                                   {true, true} ->
                                                       {equal, equal(Merged, Clock1) andalso
                                                        equal(Merged, Clock2)};
                                                   {false, false} ->
                                                       {concurrent,
                                                        dominates(Merged, Clock1) andalso
                                                        dominates(Merged, Clock2)};
                                                   _ ->
                                                       {linial,
                                                        descends(Merged, Clock1) andalso
                                                        descends(Merged, Clock2)}
                                               end,

                                aggregate([Type], Prop)
                            end))).


prop_intersection() ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL([{_, Clock1}, {_, Clock2} | _], gen_clocks(Events),
                    begin
                        DC = clock_to_dotcloud(Clock1),
                        Res = clock_to_set(intersection(DC, Clock2)),
                        Set1 = clock_to_set(Clock1),
                        Set2 = clock_to_set(Clock2),
                        Expected = ordsets:intersection(Set1, Set2),
                        equals(Expected, Res)
                    end)).

prop_complement() ->
    ?FORALL(Events, gen_all_system_events(),
            ?FORALL([{_, {_Base1, _DC1}=Clock1} | _], gen_clocks(Events),
                    ?FORALL({_Base2, _DC}=Clock2, gen_sub_clock(Clock1),
                            begin
                                Res = dot_cloud_to_set(tombstone_from_digest(Clock1, Clock2)),
                                Set1 = clock_to_set(Clock1),
                                Set2 = clock_to_set(Clock2),
                                Expected = ordsets:subtract(Set1, Set2),
                                %% @TODO(rdb) add check_distribution when we get off r16
                                equals(Expected, Res)
                            end))).

clock_to_dotcloud({VV, DC}) ->
    lists:foldl(fun({Act, Cnt}, Acc) ->
                        [{Act, lists:umerge(lists:seq(1, Cnt), proplists:get_value(Act, DC, []))} | Acc]
                end,
                [Entry || {Act, _}=Entry <- DC,
                          not lists:keymember(Act, 1, VV)],
                VV).

clock_to_set(Clock) ->
    DC = clock_to_dotcloud(Clock),
    dot_cloud_to_set(DC).

set_to_clock(Set) ->
    ?BS_CLOCK:set_to_clock(Set).

set_to_dotcloud(Set) ->
    ordsets:fold(fun({Act, Cnt}, Acc) ->
                         orddict:update(Act, fun(L) ->
                                                     lists:umerge(L, [Cnt]) end,
                                        [Cnt],
                                        Acc)
                 end,
                 orddict:new(),
                 Set).

dot_cloud_to_set(DC) ->
    ordsets:from_list([{Act, Event} || {Act, Events} <- DC,
                                     Event <- Events]).

gen_clocks() ->
    ?LET(Events, gen_all_system_events(),
         gen_clocks(Events)).

gen_clocks(Events) ->
    [{Actor, gen_clock(Me, Events)} || {Actor, _}=Me <- Events].

gen_clock(Me, Events) ->
    ?LET(ClockMembers, sublist(Events),
         ?LET(Clocks, [bigset_clock:fresh(Me) | [gen_clock(Event) || Event <- ClockMembers,
                                    Event /= Me]],
              bigset_clock:merge(Clocks))).

gen_sub_clock(Clock) ->
    Set = clock_to_set(Clock),
    ?LET(SubSet, sublist(ordsets:to_list(Set)),
         set_to_clock(ordsets:from_list(SubSet))).

gen_clock({Actor, Max}) ->
    ?LET(Events, non_empty(sublist(lists:seq(1, Max))),
         clock_from_event_list(Actor, Events)).

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

-endif.

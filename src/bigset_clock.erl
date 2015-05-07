%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created :  8 Jan 2015 by Russell Brown <russelldb@basho.com>

-module(bigset_clock).

-compile(export_all).

-export_type([clock/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type clock() :: {riak_dt_vclock:vclock(), [riak_dt:dot()]}.

-spec fresh() -> clock().
fresh() ->
    {riak_dt_vclock:fresh(), []}.

%% @doc increment the entry in `Clock' for `Actor'. Return the new
%% Clock, and the `Dot' of the event of this increment. Works because
%% for any actor in the clock, the assumed invariant is that all dots
%% for that actor are contiguous and contained in this clock (assumed
%% therefore that `Actor' stores this clock durably after increment,
%% see riak_kv#679 for some real world issues, and mitigations that
%% can be added to this code.)
-spec increment(riak_dt_vclock:actor(), clock()) ->
                       {riak_dt_vclock:dot(), clock()}.
increment(Actor, {Clock, Seen}) ->
    Clock2 = riak_dt_vclock:increment(Actor, Clock),
    Cnt = riak_dt_vclock:get_counter(Actor, Clock2),
    {{Actor, Cnt}, {Clock2, Seen}}.

get_dot(Actor, {Clock, _Dots}) ->
    {Actor, riak_dt_vclock:get_counter(Actor, Clock)}.

all_nodes({Clock, _Dots}) ->
    riak_dt_vclock:all_nodes(Clock).


-spec merge(clock(), clock()) -> clock().
merge({VV1, Seen1}, {VV2, Seen2}) ->
    VV = riak_dt_vclock:merge([VV1, VV2]),
    Seen = lists:umerge(Seen1, Seen2),
    compress_seen(VV, Seen).

%% @doc make a bigset clock from a version vector
-spec from_vv(riak_dt_vclock:vclock()) -> clock().
from_vv(Clock) ->
    {Clock, []}.

%% @doc the highest count seen for each actor, as a version
%% vector. Warning, NOT a version vector. It is used for tombstoning
%% only, it does not say what you've seen.
-spec tombstone_context(clock()) -> riak_dt_vclock:vclock().
tombstone_context({Clock, Dots}) ->
    %% reverse dots so get_value will get highest counter since dots
    %% is kept sorted
    Stod = lists:reverse(Dots),
    Nodes = lists:umerge(lists:sort(proplists:get_keys(Dots)),
                         riak_dt_vclock:all_nodes(Clock)),
    TS = lists:foldl(fun(Actor, MaxClock) ->
                             [{Actor, max(riak_dt_vclock:get_counter(Actor, Clock),
                                          proplists:get_value(Actor, Stod, 0))}
                              | MaxClock]
                     end,
                     [],
                     Nodes),
    lists:reverse(TS).

%% @doc given a `Dot :: riak_dt_vclock:dot()' and a `Clock::clock()',
%% add the dot to the clock. If the dot is contiguous with events
%% summerised by the clocks VV it will be added to the VV, if it is an
%% exception (see DVV, or CVE papers) it will be added to the set of
%% gapped dots. If adding this dot closes some gaps, the seen set is
%% compressed onto the clock.
-spec strip_dots(riak_dt_vclock:dot(), clock()) -> clock().
strip_dots(Dot, {Clock, Seen}) ->
    Seen2 = lists:umerge([Dot], Seen),
    compress_seen(Clock, Seen2).

seen({Clock, Seen}, Dot) ->
    (riak_dt_vclock:descends(Clock, [Dot]) orelse lists:member(Dot, Seen)).

%% @doc get the counter for `Actor' where `counter' is the maximum
%% _contiguous_ event sent by this clock (i.e. not including
%% exceptions.)
-spec get_contiguous_counter(riak_dt_vclock:actor(), clock()) ->
                                    pos_integer() | no_return().
get_contiguous_counter(Actor, {Clock, _Dots}=C) ->
    case riak_dt_vclock:get_counter(Actor, Clock) of
        0 ->
            error({badarg, actor_not_in_clock}, [Actor, C]);
        Cnt ->
            Cnt
    end.


compress_seen(Clock, Seen) ->
    lists:foldl(fun(Node, {ClockAcc, SeenAcc}) ->
                        Cnt = riak_dt_vclock:get_counter(Node, Clock),
                        Cnts = proplists:lookup_all(Node, Seen),
                        case compress(Cnt, Cnts) of
                            {Cnt, Cnts} ->
                                {ClockAcc, lists:umerge(Cnts, SeenAcc)};
                            {Cnt2, []} ->
                                {riak_dt_vclock:merge([[{Node, Cnt2}], ClockAcc]),
                                 SeenAcc};
                            {Cnt2, Cnts2} ->
                                {riak_dt_vclock:merge([[{Node, Cnt2}], ClockAcc]),
                                 lists:umerge(SeenAcc, Cnts2)}
                        end
                end,
                {Clock, []},
                proplists:get_keys(Seen)).

compress(Cnt, []) ->
    {Cnt, []};
compress(Cnt, [{_A, Cntr} | Rest]) when Cnt >= Cntr ->
    compress(Cnt, Rest);
compress(Cnt, [{_A, Cntr} | Rest]) when Cntr - Cnt == 1 ->
    compress(Cnt+1, Rest);
compress(Cnt, Cnts) ->
    {Cnt, Cnts}.



-ifdef(TEST).


fresh_test() ->
    ?assertEqual({[], []}, fresh()).

increment_test() ->
    Clock = fresh(),
    {Dot1, Clock2} = increment(a, Clock),
    {Dot2, Clock3} = increment(b, Clock2),
    {Dot3, Clock4} = increment(a, Clock3),
    ?assertEqual({a, 1}, Dot1),
    ?assertEqual({b, 1}, Dot2),
    ?assertEqual({a, 2}, Dot3),
    ?assertEqual({[{a, 2}, {b, 1}], []}, Clock4).

get_dot_test() ->
    Clock = fresh(),
    ?assertEqual({a, 0}, get_dot(a, Clock)),
    {Dot, Clock2} = increment(a, Clock),
    ?assertEqual(Dot, get_dot(a, Clock2)).

from_vv_test() ->
    VV = [{a, 4}, {c, 45}, {z, 1}],
    ?assertEqual({VV, []}, from_vv(VV)).

all_nodes_test() ->
    Clock = from_vv([{a, 4}, {c, 45}, {z, 1}]),
    ?assertEqual([a, c, z], all_nodes(Clock)).


strip_dots_test() ->
    Clock = {[], []},
    Dot = {a, 1},
    ?assertEqual({[{a,1}], []}, strip_dots(Dot, Clock)),
    Clock1 = {[], []},
    Dot1 = {a, 34},
    ?assertEqual({[], [{a, 34}]}, strip_dots(Dot1, Clock1)),
    Clock2 = {[{a, 3}, {b, 1}], []},
    Dot2 = {c, 2},
    Clock3 = strip_dots(Dot2, Clock2),
    ?assertEqual({[{a, 3}, {b, 1}], [{c, 2}]}, Clock3),
    Dot3 = {a, 5},
    Clock4 = strip_dots(Dot3, Clock3),
    ?assertEqual({[{a, 3}, {b, 1}], [{a, 5}, {c, 2}]}, Clock4),
    Dot4 = {a, 4},
    Clock5 = strip_dots(Dot4, Clock4),
    ?assertEqual({[{a, 5}, {b, 1}], [{c, 2}]}, Clock5),
    Dot5 = {c, 1},
    Clock6 = strip_dots(Dot5, Clock5),
    ?assertEqual({[{a, 5}, {b, 1},{c, 2}], []}, Clock6),
    Clock7 = {[{a, 1}], [{a, 3}, {a, 4}, {a, 9}]},
    Dot6 = {a, 2},
    Clock8 = strip_dots(Dot6, Clock7),
    ?assertEqual({[{a, 4}], [{a, 9}]}, Clock8).

%% in the case where seen dots are lower than the actual actors in the
%% VV (Say after a merge)
strip_dots_low_dot_test() ->
    Clock = {[{a, 4}, {b, 9}], [{a, 1}, {b, 7}]},
    Clock2 = strip_dots({a, 3}, Clock),
    ?assertEqual({[{a, 4}, {b, 9}], []}, Clock2).

seen_test() ->
    Clock = {[{a, 2}, {b, 9}, {z, 4}], [{a, 7}, {c, 99}]},
    ?assert(seen(Clock, {a, 1})),
    ?assert(seen(Clock, {z, 4})),
    ?assert(seen(Clock, {c, 99})),
    ?assertNot(seen(Clock, {a, 5})),
    ?assertNot(seen(Clock, {x, 1})),
    ?assertNot(seen(Clock, {c, 1})).

contiguous_counter_test() ->
    Clock = {[{a, 2}, {b, 9}, {z, 4}], [{a, 7}, {c, 99}]},
    ?assertEqual(2, get_contiguous_counter(a, Clock)),
    ?assertError({badarg, actor_not_in_clock}, get_contiguous_counter(c, Clock)).

merge_test() ->
    Clock = {[{a, 2}, {b, 9}, {z, 4}], [{a, 7}, {c, 99}]},
    Clock2 = fresh(),
    %% Idempotent
    ?assertEqual(Clock, merge(Clock, Clock)),
    ?assertEqual(Clock, merge(Clock, Clock2)),
    Clock3 = {[], [{a, 3}, {a, 4}, {a, 5}, {a, 6}, {d, 2}, {z, 6}]},
    Clock4 = merge(Clock3, Clock),
    ?assertEqual({[{a, 7}, {b, 9}, {z, 4}], [{c, 99}, {d, 2}, {z, 6}]},
                 Clock4),
    Clock5 = {[{a, 5}, {c, 100}, {d, 1}], [{d, 3}, {z, 5}]},
    ?assertEqual({[{a, 7}, {b, 9}, {c, 100}, {d, 3}, {z, 6}], []}, merge(Clock4, Clock5)),
    %% commute
    ?assertEqual(merge(Clock3, Clock), merge(Clock, Clock3)),
    %% assoc
    ?assertEqual(merge(merge(Clock3, Clock), Clock5),
                 merge(merge(Clock, Clock5), Clock3)).

tombstone_context_test() ->
    Clock = {[{a, 2}, {b, 9}, {z, 4}], [{a, 7}, {c, 99}]},
    ?assertEqual([{a, 7}, {b, 9}, {c, 99}, {z, 4}], tombstone_context(Clock)).

-endif.

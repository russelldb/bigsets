%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created :  8 Jan 2015 by Russell Brown <russelldb@basho.com>

-module(bigset_clock_ba).

-behaviour(bigset_clock).

-export([
         add_dot/2,
         add_dot/3,
         compact/1,
         add_dots/2,
         all_nodes/1,
         tombstone_from_digest/2,
         subtract_dots/2,
         descends/2,
         dominates/2,
         equal/2,
         fresh/0,
         fresh/1,
         from_bin/1,
         get_dot/2,
         get_counter/2,
         increment/2,
         intersection/2,
         is_compact/1,
         merge/1,
         merge/2,
         seen/2,
         subtract_seen/2,
         to_bin/1
        ]).

-export_type([clock/0, dot/0]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-compile(export_all).
-export([clock_from_event_list/2,
         set_to_clock/1,
         clock_to_set/1,
         to_version_vector/1]).

-endif.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type actor() :: riak_dt_vclock:actor().
-type clock() :: {riak_dt_vclock:vclock(), dot_cloud()}.
-type dot() :: riak_dt:dot().
-type dot_cloud() :: [{riak_dt_vclock:actor(), dot_set()}].
-type dot_set() :: bigset_bitarray:bit_array().

-define(DICT, orddict).

-spec to_bin(clock()) -> binary().
to_bin(Clock) ->
    term_to_binary(Clock, [compressed]).

-spec from_bin(clock()) -> binary().
from_bin(Bin) ->
    binary_to_term(Bin).

-spec fresh() -> clock().
fresh() ->
    {riak_dt_vclock:fresh(), ?DICT:new()}.

-spec fresh({actor(), pos_integer()}) -> clock().
fresh({Actor, Cnt}) ->
    {riak_dt_vclock:fresh(Actor, Cnt), ?DICT:new()}.

%% @doc increment the entry in `Clock' for `Actor'. Return the new
%% Clock, and the `Dot' of the event of this increment. Works because
%% for any actor in the clock, the assumed invariant is that all dots
%% for that actor are contiguous and contained in this clock (assumed
%% therefore that `Actor' stores this clock durably after increment,
%% see riak_kv#679 for some real world issues, and mitigations that
%% can be added to this code.)
-spec increment(actor(), clock()) ->
                       {dot(), clock()}.
increment(Actor, {Clock, Seen}) ->
    Clock2 = riak_dt_vclock:increment(Actor, Clock),
    Cnt = riak_dt_vclock:get_counter(Actor, Clock2),
    {{Actor, Cnt}, {Clock2, Seen}}.

%% @doc get the current top event for the given actor. NOTE: Assumes
%% this is the local / event generating actor and that the dot is
%% therefore contiguous with the base.
-spec get_dot(actor(), clock()) -> dot().
get_dot(Actor, {Clock, _Dots}) ->
    {Actor, riak_dt_vclock:get_counter(Actor, Clock)}.

%% @doc a sorted list of all the actors in `Clock'.
-spec all_nodes(clock()) -> [actor()].
all_nodes({Clock, Dots}) ->
    %% NOTE the riak_dt_vclock:all_nodes/1 returns a sorted list
    lists:usort(lists:merge(riak_dt_vclock:all_nodes(Clock),
                 ?DICT:fetch_keys(Dots))).

%% @doc merge the pair of clocks into a single one that descends them
%% both.
-spec merge(clock(), clock()) -> clock().
merge({VV1, Seen1}, {VV2, Seen2}) ->
    VV = riak_dt_vclock:merge([VV1, VV2]),
    Seen = ?DICT:merge(fun(_Key, S1, S2) ->
                               bigset_bitarray:union(S1, S2)
                       end,
                       Seen1,
                       Seen2),
    compress_seen(VV, Seen).

%% @doc merge a list of clocks `Clocks' into a single clock that
%% descends them all.
-spec merge(list(clock())) -> clock().
merge(Clocks) ->
    lists:foldl(fun merge/2,
                fresh(),
                Clocks).

%% @doc given a `Dot :: riak_dt:dot()' and a `Clock::clock()', add the
%% dot to the clock. If the dot is contiguous with events summerised
%% by the clocks VV it will be added to the VV, if it is an exception
%% (see DVV, or CVE papers) it will be added to the set of gapped
%% dots. If adding this dot closes some gaps, the dot cloud is
%% compressed onto the clock.
-spec add_dot(dot(), clock()) -> clock().
add_dot(Dot, {Clock, Seen}) ->
    Seen2 = add_dot_to_cloud(Dot, Seen),
    compress_seen(Clock, Seen2).

-spec add_dot(dot(), clock(), boolean()) -> clock().
add_dot(Dot, {Clock, Seen}, Compact) ->
    Seen2 = add_dot_to_cloud(Dot, Seen),
    if Compact -> compress_seen(Clock, Seen2);
       true -> {Clock, Seen2}
    end.

compact({Clock, Seen}) ->
    compress_seen(Clock, Seen).

%% @private
-spec add_dot_to_cloud(dot(), dot_cloud()) -> dot_cloud().
add_dot_to_cloud({Actor, Cnt}, Cloud) ->
    ?DICT:update(Actor,
                 fun(Dots) ->
                         bigset_bitarray:set(Cnt, Dots)
                 end,
                 bigset_bitarray:set(Cnt, bigset_bitarray:new(1000)),
                 Cloud).

%% @doc given a list of `dot()' and a `Clock::clock()',
%% add the dots from `Dots' to the clock. All dots contiguous with
%% events summerised by the clocks VV it will be added to the VV, any
%% exceptions (see DVV, or CVE papers) will be added to the set of
%% gapped dots. If adding a dot closes some gaps, the seen set is
%% compressed onto the clock.
-spec add_dots([dot()], clock()) -> clock().
add_dots(Dots, {Clock, Seen}) ->
    Seen2 = lists:foldl(fun add_dot_to_cloud/2,
                        Seen,
                        Dots),
    compress_seen(Clock, Seen2).

%% @doc has `Dot' been seen by `Clock'. True if so, otherwise false.
-spec seen(dot(), clock()) -> boolean().
seen({Actor, Cnt}=Dot, {Clock, Seen}) ->
    (riak_dt_vclock:descends(Clock, [Dot]) orelse
     bigset_bitarray:member(Cnt, fetch_dot_set(Actor, Seen))).

%% @private
fetch_dot_set(Actor, {_VV, Seen}) ->
    fetch_dot_set(Actor, Seen);
fetch_dot_set(Actor, Seen) ->
    case ?DICT:find(Actor, Seen) of
        error ->
            bigset_bitarray:new(10);
        {ok, L} ->
            L
    end.


%% @doc Remove dots seen by `Clock' from `Dots'. Return a list of
%% `dot()' unseen by `Clock'. Return `[]' if all dots seens.
-spec subtract_seen(clock(), [dot()]) -> dot().
subtract_seen(Clock, Dots) ->
    %% @TODO(rdb|optimise) this is maybe a tad inefficient.
    lists:filter(fun(Dot) ->
                         not seen(Dot, Clock)
                 end,
                 Dots).

%% Remove `Dots' from `Clock'. Any `dot()' in `Dots' that has been
%% seen by `Clock' is removed from `Clock', making the `Clock' un-see
%% the event.
-spec subtract_dots(clock(), list(dot())) -> clock().
subtract_dots(Clock, Dots) ->
    lists:foldl(fun(Dot, Acc) ->
                        subtract_dot(Acc, Dot) end,
                Clock,
                Dots).

%% Remove an event `dot()' `Dot' from the clock() `Clock', effectively
%% un-see `Dot'.
-spec subtract_dot(clock(), dot()) -> clock().
subtract_dot(Clock, Dot) ->
    {VV, DotCloud} = Clock,
    {Actor, Cnt} = Dot,
    DotSet = fetch_dot_set(Actor, DotCloud),
    case bigset_bitarray:member(Cnt, DotSet) of
        %% Dot in the dot cloud, remove it
        true ->
            {VV, delete_dot(Dot, DotSet, DotCloud)};
        false ->
            %% Check the clock
            case riak_dt_vclock:get_counter(Actor, VV) of
                N when N >= Cnt ->
                    %% Dot in the contiguous counter Remove it by
                    %% adding > cnt to the Dot Cloud, and leaving
                    %% less than cnt in the base
                    NewBase = Cnt-1,
                    NewDots = lists:seq(Cnt+1, N),
                    NewVV = riak_dt_vclock:set_counter(Actor, NewBase, VV),
                    NewDC = case NewDots of
                                [] ->
                                    DotCloud;
                                _ ->
                                    orddict:store(Actor, bigset_bitarray:set_all(NewDots, DotSet), DotCloud)
                            end,
                    {NewVV, NewDC};
                _ ->
                    %% NoOp
                    Clock
            end
    end.

%% @private unset a dot from the dot set
-spec delete_dot(dot(), dot_set(), dot_cloud()) -> dot_cloud().
delete_dot({Actor, Cnt}, DotSet, DotCloud) ->
    DL2 = bigset_bitarray:unset(Cnt, DotSet),
    case bigset_bitarray:size(DL2) of
        0 ->
            orddict:erase(Actor, DotCloud);
        _ ->
            orddict:store(Actor, DL2, DotCloud)
    end.

%% @doc get the counter for `Actor' where `counter' is the maximum
%% _contiguous_ event seen by this clock.
-spec get_counter(actor(), clock()) -> non_neg_integer().
get_counter(Actor, {Clock, _Dots}=_Clock) ->
    riak_dt_vclock:get_counter(Actor, Clock).

%% @private when events have been added to a clock, gaps may have
%% closed. Check the dot_cloud entries and if gaps have closed shrink
%% the dot_cloud.
-spec compress_seen(clock(), dot_cloud()) -> clock().
compress_seen(Clock, Seen) ->
    ?DICT:fold(fun(Node, Dotset, {ClockAcc, SeenAcc}) ->
                       Cnt = riak_dt_vclock:get_counter(Node, Clock),
                       {Cnt2, DS2} = compress(Cnt, Dotset),
                       ClockAcc2 = if Cnt2 == 0 -> ClockAcc;
                                      true -> riak_dt_vclock:merge([[{Node, Cnt2}], ClockAcc])
                                   end,
                       SeenAcc2 =  case bigset_bitarray:is_empty(DS2) of
                                       false ->
                                           ?DICT:store(Node, DS2, SeenAcc);
                                       true ->
                                           SeenAcc
                                   end,
                       {ClockAcc2, SeenAcc2}
               end,
               {Clock, ?DICT:new()},
               Seen).

%% @private worker for `compress_seen' above.
-spec compress(pos_integer(), dot_set()) -> {pos_integer(), dot_set()}.
compress(Base, BitArray) ->
    bigset_bitarray:compact_contiguous(Base, BitArray).

%% @doc true if A descends B, false otherwise
-spec descends(clock(), clock()) -> boolean().
descends({VVa, _DCa}=ClockA, {VVb, DCb}) ->
    riak_dt_vclock:descends(VVa, VVb)
        andalso
        dotcloud_descends(ClockA, DCb).

%% @private used by descends/2. returns true if `ClockA' descends
%% `DCb', false otherwise. We consider the whole clock, as it is
%% possible that `DCb' has some event [2] that is not present in `DCa'
%% but is covered by `VVa'. For example if the entry for actor `X' on
%% `A' is {X, 10} []. We're establishing if for each node in `DCa' the
%% entry descends the dot-set for `DCb'
-spec dotcloud_descends(clock(), dot_cloud()) -> boolean().
dotcloud_descends(ClockA, DCb) ->
    NodesA = all_nodes(ClockA),
    NodesB = ?DICT:fetch_keys(DCb),
    case lists:umerge(NodesA, NodesB) of
        NodesA ->
            %% do the work as the set of nodes in B is a subset of
            %% those in A, meaning it is at least possible A descends
            %% B
            dotsets_descend(ClockA, DCb);
        _ ->
            %% Nodes of B not a subset of nodes of A, can't possibly
            %% be descended by A.
            false
    end.

%% @private only called after `dotcloud_descends/2' when we know that
%% the the set of nodes in DCb are a subset of those in A (i.e. every
%% node in B is in A, so we only need compare those node's dot_sets.)
%% If all `DCb''s node's dotsets are descended by their entry in A
%% returns true, otherwise false.
-spec dotsets_descend(clock(), dot_cloud()) -> boolean().
dotsets_descend({VVa, DCa}, DCb) ->
    %% Only called when the nodes in DCb are a subset of those in ClockA,
    %% so we only need fold over that
    (catch ?DICT:fold(fun(Node, DotsetB, true) ->
                              DotsetA = fetch_dot_set(Node, DCa),
                              CounterA = riak_dt_vclock:get_counter(Node, VVa),
                              dotset_descends(CounterA, DotsetA, DotsetB);
                         (_Node, _, false) ->
                              throw(false)
                      end,
                      true,
                      DCb)).

%% @private returns true if `DotsetA :: dot_set()' descends `DotsetB
%% :: dot_set()' or put another way, is B a subset of A.
-spec dotset_descends(non_neg_integer(), dot_set(), dot_set()) ->
                             boolean().
dotset_descends(CntrA, DotsetA, DotsetB) ->
    %% TODO: is it ok just to add the CntrA as a range?, or do we
    %% subtract from DotsetB?
    DotsetB2 = bigset_bitarray:subtract_range(DotsetB, {0, CntrA}),
    bigset_bitarray:is_subset(DotsetB2, DotsetA).

%% @doc are A and B the same logical clock? True if so.
equal(A, B) ->
    descends(A, B) andalso descends(B, A).

%% @doc true if the events in A are a strict superset of the events in
%% B, false otherwise.
dominates(A, B) ->
    descends(A, B) andalso not descends(B, A).

%% @doc intersection returns all the dots in A that are also in B. A
%% is tombstone clock as returned by `tombstone_from_digest/2'. The
%% purpose of this function is to trim the tombstone to only include
%% those events seen.
-spec intersection(clock(), clock()) -> clock().
intersection(A, B) ->
    AllActors = lists:umerge(all_nodes(A), all_nodes(B)),
    lists:foldl(fun(Actor, ClockAcc) ->
                        ADC = fetch_dot_set(Actor, A),
                        BDC = fetch_dot_set(Actor, B),
                        {AC, BC} = {get_counter(Actor, A), get_counter(Actor, B)},
                        DC = case {AC, BC} of
                                 {AC, BC} when AC > BC ->
                                     %% we need to intersect the diff
                                     %% between AC and BC with BDC.
                                     Range = {BC+1, AC},
                                     bigset_bitarray:intersection(BDC, bigset_bitarray:add_range(Range, ADC));
                                 {AC, BC} when BC > AC ->
                                     %% we need to intersect the diff
                                     %% between BC and AC with ADC
                                     Range = {AC+1, BC},
                                     bigset_bitarray:intersection(ADC, bigset_bitarray:add_range(Range, BDC));
                                 {C, C} ->
                                     %% just the dotsets, ma'am
                                     bigset_bitarray:intersection(ADC, BDC)
                             end,
                        update_clock_acc(Actor, min(AC, BC), DC, ClockAcc)
                end,
                fresh(),
                AllActors).

%% @doc subtract. Return only the events in A that are not in B. NOTE:
%% this is for comparing a set digest with a set clock, so the digest
%% (B) is _always_ a subset of the clock (A). Returns only the events
%% that are in A and not in B. Subtracts B from A. Like
%% sets:subtract(A, B). The returned set of events (as a clock) are
%% the things that were in A and have ben removed.
-spec tombstone_from_digest(SetClock::clock(), Digest::clock()) -> Tombstone::clock().
tombstone_from_digest(Clock, Digest) ->
    %% work on each actor in A
    {AVV, ADC} = Clock,
    {BVV, BDC} = Digest,
    AActors = all_nodes(Clock),
    lists:foldl(fun(Actor, TombstoneAcc) ->
                        BaseDiff0 = base_diff(Actor, AVV, BVV),
                        BDotSet = fetch_dot_set(Actor, BDC),
                        %% Subtract any dots from B's dotset that are
                        %% in A's base and not in B's base.
                        BaseDC = bigset_bitarray:range_subtract(BaseDiff0, BDotSet),
                        %% Subtract anything from A's dotset that is in B's dotset
                        TSDC = bigset_bitarray:subtract(fetch_dot_set(Actor, ADC),
                                                         BDotSet),

                        %% the clock A is already compacted, the
                        %% BaseDC is made from the base of A therefore
                        %% < min(ADC) it may contain '1', for example.
                        {Base, DC0} = compress(0, BaseDC),
                        %% DC0 must be disjoint from TSDC, and
                        %% therefore cannot be compressed
                        DC = bigset_bitarray:union(DC0, TSDC),
                        update_clock_acc(Actor, Base, DC, TombstoneAcc)
                end,
                fresh(),
                AActors).

%% @private update the clock's entry for the actor base and dotset.
-spec update_clock_acc(actor(), pos_integer(), dot_set(), clock()) -> clock().
update_clock_acc(Actor, Base, Dotset, {Clock0, Seen0}) ->
    Clock = riak_dt_vclock:set_counter(Actor, Base, Clock0),
    Seen = case bigset_bitarray:is_empty(Dotset) of
               true ->
                   Seen0;
               false ->
                   ?DICT:store(Actor, Dotset, Seen0)
           end,
    {Clock, Seen}.

%% the tombstone bit array is the result of subtracting B bitarray
%% from A. But B may have contained return the range of events missing
%% from BVV that are in AVV for Actor. Called by
%% tombstone_from_digest/2, with the same assumption A is a superset
%% of B. Returns the counts for Actor from A and B in a tuple {BCnt,
%% AmCnt}
base_diff(Actor, AVV, BVV) ->
    ACnt = riak_dt_vclock:get_counter(Actor, AVV),
    BCnt = riak_dt_vclock:get_counter(Actor, BVV),
    %% we know that A is a superset of B
    {BCnt+1, ACnt}.

%% @doc Is this clock compact, i.e. no gaps/no dot-cloud entries
-spec is_compact(clock()) -> boolean().
is_compact({_Base, DC}) ->
    is_compact_dc(DC).

is_compact_dc([]) ->
    true;
is_compact_dc([{_A, DC} | Rest]) ->
    case bigset_bitarray:is_empty(DC) of
        true ->
            is_compact_dc(Rest);
        false ->
            false
    end.

-ifdef(TEST).

compress_test() ->
    Dotcloud = bigset_bitarray:from_list([8,9,10]),
    ClockC = fresh({a, 15}), %% a clock that has seen all a's events to 15
    ClockB = update_clock_acc(a, 0, Dotcloud, fresh({b, 15})), %% a clock that has seen only a's events  8,9,10
    Merged = merge(ClockC, ClockB),
    ?assert(is_compact(Merged)),

    Dotcloud2 = bigset_bitarray:from_list([16,17,18]),
    ClockC2 = fresh({a, 15}), %% a clock that has seen all a's events to 15
    ClockB2 = update_clock_acc(a, 0, Dotcloud2, fresh({b, 10})), %% a clock that has seen only a's events from 16
    Merged2 = merge(ClockC2, ClockB2),
    ?assert(is_compact(Merged2)).

-endif.


-ifdef(EQC).

-define(NUMTESTS, 1000).

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

eqc_test_() ->
    bigset_clock:eqc_tests(?MODULE).

%% @doc eqc only callback. Create a clock from an `Actor' and a list
%% of events.
-spec clock_from_event_list(actor(), list(pos_integer())) -> clock().
clock_from_event_list(Actor, Events) ->
    DotSet = bigset_bitarray:from_list(Events),
    DC = ?DICT:store(Actor, DotSet, ?DICT:new()),
    compress_seen(riak_dt_vclock:fresh(), DC).

%% @doc eqc only callback. Generate a clock from a set of dots.
-spec set_to_clock(list(dot())) -> clock().
set_to_clock(Dots) ->
    Seen = lists:foldl(fun({Actor, Event}, DC) ->
                              ?DICT:update(Actor,
                                           fun(BS) ->
                                                   bigset_bitarray:set(Event, BS) end,
                                           bigset_bitarray:new(100),
                                           DC)
                      end,
                      ?DICT:new(),
                      Dots),
    compress_seen(riak_dt_vclock:fresh(), Seen).

%% @doc eqc only callback. Get the VV portion of the clock, if compact
-spec to_version_vector(clock()) -> riak_dt_vclock:vclock() | none().
to_version_vector(Clock) ->
    case is_compact(Clock) of
        true ->
            {VV, _DC} = Clock,
            VV;
        false ->
            throw(e_notcompact_clock)
    end.

%% @doc eqc only callback. Turn `Clock' into a list of `{Actor, Cnt}'
%% pairs for _all events_
-spec clock_to_set(clock()) -> list(dot()).
clock_to_set({VV, DC}=Clock) ->
    Nodes = all_nodes(Clock),
    lists:foldl(fun(Node, Acc) ->
                        Cntr = riak_dt_vclock:get_counter(Node, VV),
                        DS = fetch_dot_set(Node, DC),
                        Base = [{Node, Cnt} || Cnt <- lists:seq(1, Cntr)],
                        Dots = [{Node, E} || E <- bigset_bitarray:to_list(DS)],
                        lists:umerge([Base, Dots, Acc])
                end,
                [],
                Nodes).

-endif.

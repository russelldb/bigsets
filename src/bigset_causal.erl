%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% the tombstone and clock for an actor of a bigset
%%% @end
%%% Created :  8 Jan 2015 by Russell Brown <russelldb@basho.com>

-module(bigset_causal).

-compile(export_all).

-export_type([clock/0, dot/0, causal/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-record(bigset_causal, {clock = bigset_clock:fresh() :: clock(),
                        tombstone = bigset_clock:fresh() :: tombstone()}
       ).


-type causal() :: #bigset_causal{}.
-type clock() :: bigset_clock:clock().
-type dot() :: bigset_clock:dot().
-type tombstone() :: bigset_clock:clock().

-spec fresh() -> causal().
fresh() ->
    #bigset_causal{}.

%% @doc increment the entry in `C' for `Actor'. Return the new `C',
%% and the `Dot' of the event of this increment. Works because for any
%% actor in the clock, the assumed invariant is that all dots for that
%% actor are contiguous and contained in this clock (assumed therefore
%% that `Actor' stores this `causal()' durably after increment, see
%% riak_kv#679 for some real world issues, and mitigations that can be
%% added to this code.)
-spec increment(riak_dt_vclock:actor(), causal()) ->
                       {riak_dt_vclock:dot(), causal()}.
increment(Actor, C) ->
    #bigset_causal{clock=Clock} = C,
    {Dot, Clock2} = bigset_clock:increment(Actor, Clock),
    {Dot, C#bigset_causal{clock=Clock2}}.

%% @doc add the dots in `Dots' to the tombstone-clock portion of the
%% `C' or the set-clock portion of the `causal()' depending on whether
%% they're seen or not. Seen dots go on the tombstone (they need
%% dropping from storage) unseen just go on the clock (so they never
%% get written.). Returns updated `C' with `Dots' added.  @NOTE we may
%% very well be adding dots to the tombstone that have been removed
%% from the tombstone by a compaction. These dots must somehow be
%% removed from the set-tombstone, maybe by a sweep/full-set-read
%% @TODO
-spec tombstone_dots([dot()], causal()) -> causal().
tombstone_dots(Dots, C) ->
    #bigset_causal{tombstone=Tombstone, clock=Clock} = C,
    {SC2, TS2} = lists:foldl(fun(Dot, {SC, TS}) ->
                                     case bigset_clock:seen(SC, Dot) of
                                         true ->
                                             {SC, bigset_clock:add_dot(Dot, TS)};
                                         false ->
                                             {bigset_clock:add_dot(Dot, SC),
                                              TS}
                                     end
                             end,
                             {Clock, Tombstone},
                             Dots),
    C#bigset_causal{clock=SC2, tombstone=TS2}.

%% @doc seen/2 has the clock portion of `C' seen the dot `Dot',
%% returns true if so, false otherwise.
-spec seen(dot(), causal()) -> boolean().
seen(Dot, C) ->
    #bigset_causal{clock=Clock} = C,
    bigset_clock:seen(Clock, Dot).

%% @doc clock/1 get the clock from the given `causal()' `C'.
-spec clock(causal()) -> bigset_clock:clock().
clock(#bigset_causal{clock=C}) ->
    C.

%% @doc tombstone/1 get the tombstone from the given `causal()' `C'.
-spec tombstone(causal()) -> bigset_clock:clock().
tombstone(#bigset_causal{tombstone=TS}) ->
    TS.

%% @doc is_tombstoned returns `true' if `Dot' is covered by the
%% `Casual' tombstone, `false' otherwise.
-spec is_tombstoned(dot(), causal()) -> boolean().
is_tombstoned(Dot, C) ->
    #bigset_causal{tombstone=TS} = C,
    bigset_clock:seen(TS, Dot).

%% @doc shrink_tombstone removes `Dot' from the tombstone in `Causal',
%% returns update `Causal'.
-spec shrink_tombstone(dot(), causal()) -> causal().
shrink_tombstone(Dot, C) ->
    #bigset_causal{tombstone=TS} = C,
    TS2 = bigset_clock:subtract_dot(TS, Dot),
    C#bigset_causal{tombstone=TS2}.

%% @doc add the given `Dot::dot()' to the clock portion of the given
%% `C::causal()'
-spec add_dot(dot(), causal()) -> causal().
add_dot({_Actor, _Cnt}=Dot, C=#bigset_causal{clock=Clock}) ->
    C#bigset_causal{clock=bigset_clock:add_dot(Dot, Clock)}.


%% @doc merge two causals `C1' and `C2' return `causal()' that is the
%% LUB of the two.
-spec merge(causal(), causal()) -> causal().
merge(C1, C2) ->
    #bigset_causal{clock=bigset_clock:merge(clock(C1), clock(C2)),
                   tombstone=bigset_clock:merge(tombstone(C1), tombstone(C2))}.

%% @doc merge clock of `C1' with clock of `C2` and return `C1' with
%% result as clock
-spec merge_clocks(causal(), causal()) -> causal().
merge_clocks(C1, C2) ->
    #bigset_causal{clock=bigset_clock:merge(clock(C1), clock(C2)),
                   tombstone=tombstone(C1)}.

%% @doc merge clock `Clock' with clock of causal() `Causal'. Return
%% `Causal' updated.
-spec merge_clock(bigset_clock:clock(), causal()) ->
                         causal().
merge_clock(Clock, C=#bigset_causal{clock=Clock2}) ->
    C#bigset_causal{clock=bigset_clock:merge(Clock, Clock2)}.

%% @doc add all events in the `bigset_clock:clock()' `Tombstone' to
%% the `causal()' `C's tombstone. Essentially a tombstone
%% merge. returns `C' with updated tombstone. NOTE: expects that all
%% events in `Tombstone' are already in `C.clock' DOES NOT verify that
%% or merge the events in `Tombstone' into `C.clock'!
-spec tombstone_all(bigset_clock:clock(), causal()) -> causal().
tombstone_all(TSClock, C) ->
    C#bigset_causal{tombstone=bigset_clock:merge(TSClock, tombstone(C))}.

-ifdef(TEST).
new(Clock) ->
    #bigset_causal{clock=Clock, tombstone=bigset_clock:fresh()}.

new(Clock, Tombstone) ->
    #bigset_causal{clock=Clock, tombstone=Tombstone}.

set_clock(Clock, C) ->
    C#bigset_causal{clock=Clock}.

set_tombstone(TS, C) ->
    C#bigset_causal{tombstone=TS}.

set_clock_ts(Clock, TS, C) ->
    C#bigset_causal{clock=Clock, tombstone=TS}.

-endif.

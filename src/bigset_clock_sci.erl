%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% Ian Milligan's disjoint intervals idea
%%% @end
%%% Created :  8 Jan 2015 by Russell Brown <russelldb@basho.com>

-module(bigset_clock_sci).

-behaviour(bigset_gen_clock).

-export([add_dot/2,
         add_dots/2,
         all_nodes/1,
         complement/2,
         descends/2,
         dominates/2,
         equal/2,
         fresh/0,
         fresh/1,
         get_dot/2,
         increment/2,
         intersection/2,
         is_compact/1,
         merge/1,
         merge/2,
         seen/2,
         subtract_seen/2,
         to_bin/1
        ]).

-compile(export_all).

-export_type([clock/0, dot/0]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([make_dotcloud_entry/3]).
-endif.

-type actor() :: riak_dt_vclock:actor().
-type dot() :: riak_dt:dot().
-type interval() :: {Start :: pos_integer(), End :: pos_integer()}.
-type intervals() :: [interval()].
-type clock() :: [{actor(), intervals() | pos_integer()}].

-define(DICT, orddict).

-spec to_bin(clock()) -> binary().
to_bin(Clock) ->
    term_to_binary(Clock, [compressed]).

-spec fresh() -> clock().
fresh() ->
    ?DICT:new().

fresh({Actor, Cnt}) ->
    ?DICT:store(Actor, Cnt, ?DICT:new()).

%% @doc increment the entry in `Clock' for `Actor'. Return the new
%% Clock, and the `Dot' of the event of this increment. Works because
%% for any actor in the clock, the assumed invariant is that all dots
%% for that actor are contiguous and contained in this clock (assumed
%% therefore that `Actor' stores this clock durably after increment,
%% see riak_kv#679 for some real world issues, and mitigations that
%% can be added to this code.)
-spec increment(actor(), clock()) ->
                       {dot(), clock()}.
increment(Actor, Clock) ->
    %% Assumes that 1. only Actor can update Actor's clock and
    %% 2. Actor's entry in Actor's own clock will _always_ be a single
    %% integer.
    Clock2 = orddict:update_counter(Actor, 1, Clock),
    {get_dot(Actor, Clock2), Clock2}.

get_dot(Actor, Clock) ->
    %% Again assumes that only called by Actor on Actor's own clock.
    {Actor, orddict:fetch(Actor, Clock)}.

all_nodes(Clock) ->
    orddict:fetch_keys(Clock).

-spec merge(clock(), clock()) -> clock().
merge(A, B) ->
    ?DICT:merge(fun(_Actor, V1, V2) when is_integer(V1)
                                        andalso
                                        is_integer(V2) ->
                        max(V1, V2);
                   (_Actor, V1, _V2) when is_integer(V1) ->
                        V1;
                   (_Actor, _V1, V2) when is_integer(V2) ->
                        V2;
                   (_Actor, V1, V2) when is_list(V1)
                                        andalso
                                        is_list(V2) ->
                        merge_intervals(V1, V2)
                end,
                A,
                B).

%% @priv merge the dot-cloud intervals for an actor
-spec merge_intervals(intervals(), intervals()) -> intervals().
merge_intervals(I1, I2) ->
    %% @TODO DO IT PROPER, this is a broken hooky impl just for the
    %% benchmark (which has no merge in it!)
    lists:merge(I1, I2).

merge(Clocks) ->
    lists:foldl(fun merge/2,
                fresh(),
                Clocks).

%% @doc given a `Dot :: riak_dt:dot()' and a `Clock::clock()',
%% add the dot to the clock. If the dot is contiguous with events
%% summerised by the clocks VV it will be added to the VV, if it is an
%% exception (see DVV, or CVE papers) it will be added to the set of
%% gapped dots. If adding this dot closes some gaps, the seen set is
%% compressed onto the clock.
-spec add_dot(dot(), clock()) -> clock().
add_dot(_Dot, Clock) ->
    Clock.


%% @doc given a list of `dot()' and a `Clock::clock()',
%% add the dots from `Dots' to the clock. All dots contiguous with
%% events summerised by the clocks VV it will be added to the VV, any
%% exceptions (see DVV, or CVE papers) will be added to the set of
%% gapped dots. If adding a dot closes some gaps, the seen set is
%% compressed onto the clock.
-spec add_dots([dot()], clock()) -> clock().
add_dots(Dots, Clock) ->
    lists:foldl(fun add_dot/2,
                Clock,
                Dots).

-spec seen(dot(), clock()) -> boolean().
seen(_Dot, _Clock) ->
    true.

%% Remove dots seen by `Clock' from `Dots'. Return a list of `dot()'
%% unseen by `Clock'. Return `[]' if all dots seens.
subtract_seen(Clock, Dots) ->
    %% @TODO(rdb|optimise) this is maybe a tad inefficient.
    lists:filter(fun(Dot) ->
                         not seen(Dot, Clock)
                 end,
                 Dots).

%% Remove `Dots' from `Clock'. Any `dot()' in `Dots' that has been
%% seen by `Clock' is removed from `Clock', making the `Clock' un-see
%% the event.
subtract(Clock, Dots) ->
    lists:foldl(fun(Dot, Acc) ->
                        subtract_dot(Acc, Dot) end,
                Clock,
                Dots).

%% Remove an event `dot()' `Dot' from the clock() `Clock', effectively
%% un-see `Dot'.
subtract_dot(Clock, _Dot) ->
    Clock.

%% true if A descends B, false otherwise
-spec descends(clock(), clock()) -> boolean().
descends(_, _) ->
    ok.

equal(A, B) ->
    A == B.

dominates(A, B) ->
    A == B.


%% @doc intersection is all the dots in A that are also in B. A is an
%% orddict of {actor, [dot()]} as returned by `complement/2'
-spec intersection(intervals(), clock()) -> clock().
intersection(_DotCloud, _Clock) ->
    ok.

%% @doc complement like in sets, only here we're talking sets of
%% events. Generates a dict that represents all the events in A that
%% are not in B. We actually assume that B is a subset of A, so we're
%% talking about B's complement in A.
%% Returns a dot-cloud
-spec complement(clock(), clock()) -> intervals().
complement(_, _) ->
    ok.

%% @doc Is this clock compact, i.e. no gaps/no dot-cloud entries
-spec is_compact(clock()) -> boolean().
is_compact([]) ->
    true;
is_compact([{_A, I} | Clock]) when is_integer(I) ->
    is_compact(Clock);
is_compact(_Clock) ->
    false.

-ifdef(TEST).

%% API for comparing clock impls

%% @doc given a clock,actor and list of events, return a clock where
%% the dotcloud for the actor contains the events
-spec make_dotcloud_entry(clock(), actor(), [pos_integer()]) -> clock().
make_dotcloud_entry(Clock, Actor, Events) ->
    Entry = make_entry(orddict:find(Actor, Clock), Events),
    orddict:store(Actor, lists:reverse(Entry), Clock).

make_entry(error, Events) ->
    make_entry([], Events);
make_entry({ok, 1}, Events) ->
    make_entry([1], Events);
make_entry({ok, N}, Events) ->
    make_entry([{1, N}], Events);
make_entry(Acc0, Events) ->
    lists:foldl(fun(Event, [Current | Rest]) when is_integer(Current) andalso Event == Current+1 ->
                        [{Current, Event} | Rest];
                   (Event, [{Low, High} | Rest]) when Event == High + 1 ->
                        [{Low, Event} | Rest];
                   (Event, Acc) ->
                        [Event | Acc]
                end,
                Acc0,
                Events).

%% How big are clocks?
clock_size_test() ->
    bigset_gen_clock:clock_size_test(?MODULE).

-endif.

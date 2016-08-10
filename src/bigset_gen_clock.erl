%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% Common behaviour for clock impls to follow. @TODO rename
%%% @end
%%% Created :  8 Jan 2015 by Russell Brown <russelldb@basho.com>

-module(bigset_gen_clock).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([clock_size_test/1, clock_speed_test/3, clock_speed_test/1]).
-export([fencepost/1, super_dense/1, super_sparse/1, random/1]).
-endif.

-type actor() :: term().
-type clock() :: term().
-type dotcloud() :: term().
-type dot() :: {actor(), pos_integer()}.

%% @doc serialise the clock for storage/wire/etc
-callback to_bin(clock()) ->
    binary().

%% @doc de-serialise the clock from binary format
-callback from_bin(clock()) ->
    clock().

%% @doc a new clock
-callback fresh() -> clock().

%% @doc a new clock initialised with `dot()' so that the `actor()' in
%% `dot()' has a base of `pos_integer()' from `dot()'
-callback fresh(dot()) -> clock().

%% @doc increment the entry in `Clock' for `Actor'. Return the new
%% Clock, and the `Dot' of the event of this increment. Works because
%% for any actor in the clock, the assumed invariant is that all dots
%% for that actor are contiguous and contained in this clock (assumed
%% therefore that `Actor' stores this clock durably after increment,
%% see riak_kv#679 for some real world issues, and mitigations that
%% can be added to this code.)
-callback increment(actor(), clock()) ->
    {dot(), clock()}.

%% @doc get the current max event for `Actor' and return as a `dot()'
-callback get_dot(actor(), clock()) ->
    dot().

%% @doc return all `actor()' in `clock()' as `list(actor())'
-callback all_nodes(clock()) ->
    list(actor()).

%% @doc for any pair of `clock()'s `Clock1' and `Clock2' return a new
%% `clock()' `Clock3' that is the union of all the events in both in
%% clocks.
-callback merge(clock(), clock()) -> clock().

%% @doc union all of `list(clock())' into a single `clock()'
-callback merge(list(clock())) ->
    clock().

%% @doc given a `Dot :: dot()' and a `Clock::clock()',
%% add the dot to the clock. If the dot is contiguous with events
%% summerised by the clocks VV it will be added to the VV, if it is an
%% exception (see DVV, or CVE papers) it will be added to the set of
%% gapped dots. If adding this dot closes some gaps, the seen set is
%% compressed onto the clock.
-callback add_dot(dot(), clock()) -> clock().

%% @doc given a list of `dot()' and a `Clock::clock()',
%% add the dots from `Dots' to the clock. All dots contiguous with
%% events summerised by the clocks VV it will be added to the VV, any
%% exceptions (see DVV, or CVE papers) will be added to the set of
%% gapped dots. If adding a dot closes some gaps, the seen set is
%% compressed onto the clock.
-callback add_dots([dot()], clock()) -> clock().

%% @doc has the `dot()' been seen by the clock. Is the `dot()' event
%% in the set of events the clock represents.
-callback seen(dot(), clock()) -> boolean().

%% Remove dots seen by `Clock' from `Dots'. Return a list of `dot()'
%% unseen by `Clock'. Return `[]' if all dots seens.
-callback subtract_seen(clock(), list(dot())) ->
    list(dot()).

%% @doc true if A descends B, false otherwise
-callback descends(clock(), clock()) -> boolean().

%% @doc return true of `A' and `B` represent the same set of events,
%% false otherwise.
-callback equal(clock(), clock()) ->
    boolean().

%% @doc return true if `B' represents a strict subset of the events in
%% `A', false otherwise.
-callback dominates(clock(), clock()) ->
    boolean().

%% @doc intersection is all the events in `A' that are also in `B'. A
%% is as returned by `complement/2'
-callback intersection(dotcloud(), clock()) -> clock().

%% @doc complement like in sets, only here we're talking sets of
%% events. Generates a result that represents all the events in A that
%% are not in B. We assume that B is a subset of A, so we're talking
%% about B's complement in A.  Returns a dot-cloud
-callback complement(clock(), clock()) -> dotcloud().

%% @doc Is this clock compact, i.e. no gaps/no dot-cloud entries
-callback is_compact(clock()) -> boolean().


-ifdef(TEST).

-callback make_dotcloud_entry(clock(), actor(), [pos_integer()]) -> clock().

clock_speed_test(Mod) ->
    clock_speed_test(Mod, random, 100*1000).

%% How slow are clocks?
clock_speed_test(Mod, DotGen, Size) ->
    %% What do we do with clocks more than anything? Read,
    %% deserialize, update, serialise, write.
    Clock = Mod:fresh(),
    Actors = [crypto:rand_bytes(24) || _ <- lists:seq(1, 10)],
    Events = erlang:apply(?MODULE, DotGen, [Size]),
    Mil = lists:seq(1, 1000),
    {Times, Clock2} = lists:foldl(fun(_, {TAcc, CAcc}) ->
                                          Actor = random_actor(Actors),
                                          {T, {_, CAcc2}} = timer:tc( Mod, increment, [Actor, CAcc]),
                                          {[T | TAcc], CAcc2}
                                  end,
                                  {[], Clock},
                                  Mil),

    timer_report("increment", Times),

    {T2, BinClock} = lists:foldl(fun(_, {TAcc, _}) ->
                                         {T, Bin} = timer:tc(Mod, to_bin, [Clock2]),
                                         {[T | TAcc], Bin}
                                 end,
                                 {[], <<>>},
                                 Mil),

    timer_report("compact to bin", T2),

    {T3, Clock2} = lists:foldl(fun(_, {TAcc, C}) ->
                                         {T, C} = timer:tc(Mod, from_bin, [BinClock]),
                                         {[T | TAcc], C}
                                 end,
                                 {[], Clock2},
                                 Mil),

    timer_report("compact from bin", T3),


    {T4, Clock3} = lists:foldl(fun(E, {TAcc, CAcc}) ->
                                       Actor = random_actor(Actors),
                                       {T, CAcc2} = timer:tc(Mod, add_dot, [{Actor, E}, CAcc]),
                                       {[T | TAcc], CAcc2}
                               end,
                               {[], Clock2},
                               Events),
    timer_report("add_dot", T4),

    %% After that we see if some event is seen
    T5 = lists:foldl(fun(E, TAcc) ->
                             Actor = random_actor(Actors),
                             {T, _} = timer:tc(Mod, seen, [{Actor, E}, Clock3]),
                             [T | TAcc]
                     end,
                     [],
                     Events),
    timer_report("seen", T5),

    %% let's time to/from bin with all those dots!
    {T6, T7} = lists:foldl(fun(_, {ToAcc, FromAcc}) ->
                             {To, Bin} = timer:tc(Mod, to_bin, [Clock3]),
                             {From, Clock3} = timer:tc(Mod, from_bin, [Bin]),
                             {[To | ToAcc], [From | FromAcc]}
                                 end,
                                 {[], []},
                                 lists:seq(1, 100)),

    timer_report("random dots to_bin", T6),
    timer_report("random dots from_bin", T7),

    %% Also, we merge (union)

    %% Rarely we do subtract and intersect

    ok.

random_actor(L) ->
    N = crypto:rand_uniform(1, length(L)+1),
    lists:nth(N, L).

timer_report(Title, Times) ->
    Length = length(Times),
    Min = lists:min(Times),
    Max = lists:max(Times),
    Med = lists:nth(round((Length / 2)), lists:sort(Times)),
    Avg = round(lists:foldl(fun(X, Sum) -> X + Sum end, 0, Times) / Length),
    io:format("~p~n", [Title]),
    io:format("Range: ~b - ~b mics~n"
          "Median: ~b mics~n"
          "Average: ~b mics~n",
          [Min, Max, Med, Avg]),
    Med.

%% How big are clocks?
clock_size_test(Mod) ->
    ?debugFmt("running clock size for ~p", [Mod]),
    Clock = Mod:fresh(),
    BinClock = Mod:to_bin(Clock),
    ?debugFmt("Fresh clock size is ~p bytes", [byte_size(BinClock)]),
    Actors = [crypto:rand_bytes(24) || _ <- lists:seq(1, 10)],
    Clock2 = increment_clock(Mod, Clock, Actors),
    ?assert(Mod:is_compact(Clock2)),
    ?debugFmt("10 actor clock with 24byte actors is ~p bytes", [byte_size(bigset:to_bin(Clock2))]),
    Clock3 = increment_clock(Mod, Clock2, Actors, 10000),
    ?assert(Mod:is_compact(Clock3)),
    ?debugFmt("10 actor clock with 24byte actors and 10k events per actor is ~p bytes", [byte_size(Mod:to_bin(Clock3))]),
    Clock4 = make_dotcloud_entries(Mod, Clock, lists:zip(Actors, lists:duplicate(length(Actors), {fencepost, [1000*1000]}))),
    ?assertNot(Mod:is_compact(Clock4)),
    ?debugFmt("10 actor clock with 24byte actors and fenceposted 1million dc is ~p bytes", [byte_size(Mod:to_bin(Clock4))]),
    Clock5 = make_dotcloud_entries(Mod, Clock, lists:zip(Actors, lists:duplicate(length(Actors), {super_dense, [1000*1000]}))),
    ?debugFmt("10 actor clock with 24byte actors and dense 1million dc is ~p bytes", [byte_size(Mod:to_bin(Clock5))]),
    Clock6 = make_dotcloud_entries(Mod, Clock, lists:zip(Actors, lists:duplicate(length(Actors), {super_sparse, [1000*1000]}))),
    ?debugFmt("10 actor clock with 24byte actors and sparse 1million-th event dc is ~p bytes", [byte_size(Mod:to_bin(Clock6))]),
    %% just generate once, reusue per actor
    RandDC = random(1000*1000, 1, 10),
    Clock7 = make_dotcloud_entries(Mod, Clock, lists:zip(Actors, lists:duplicate(length(Actors), RandDC))),
    ?debugFmt("10 actor clock with 24byte actors and  around 1million random events dc is ~p bytes", [byte_size(Mod:to_bin(Clock7))]).

increment_clock(Mod, Clock, Actors) ->
    increment_clock(Mod, Clock, Actors, 1).

increment_clock(Mod, Clock, Actors, Times) ->
    L = lists:seq(1, Times),
    lists:foldl(fun(Actor, ClockAcc) ->
                        increment_actor(Mod, Actor, ClockAcc, L)
                end,
                Clock,
                Actors).

make_dotcloud_entries(Mod, Clock, DCSpecs) ->
    lists:foldl(fun({Actor, DCSpec}, ClockAcc) ->
                        Events = spec_to_dotcloud(DCSpec),
                        Mod:make_dotcloud_entry(ClockAcc, Actor, Events)
                end,
                Clock,
                DCSpecs).

increment_actor(Mod, Actor, Clock, Times) ->
    lists:foldl(fun(_, ClockAcc) ->
                        {_, C2} = Mod:increment(Actor, ClockAcc),
                        C2
                end,
                Clock,
                Times).

spec_to_dotcloud({Fun, Args}) ->
    erlang:apply(?MODULE, Fun, Args);
spec_to_dotcloud(Events) when is_list(Events)  ->
    Events.

fencepost(Size) ->
    fencepost(Size, 1).

fencepost(Size, Base) ->
    lists:seq(Base+2, Size*2, 2).

super_dense(Size) ->
    super_dense(Size, 1).

super_dense(Size, Base) ->
    lists:seq(Base+2, Size+2).

super_sparse(MaxEvent) ->
    super_sparse(MaxEvent, 1).

super_sparse(MaxEvent, _Base) ->
    [MaxEvent].

random(Size) ->
    random(Size, 1).

random(Size, Base) ->
    random(Size, Base, 3).

random(Size, Base, Sparseness) ->
    lists:usort([crypto:rand_uniform(Base+2, Size*Sparseness) || _ <- lists:seq(1, Size)]).

dc_speed_test() ->
    %% for each of the event sets above, add them all to the
    %% underlying DC structure and time + get size
    ok.

-endif.

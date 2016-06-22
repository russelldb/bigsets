%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% logic for merging a streamed set of values
%%% defaults to r=2, n=3, basic_quorum=false, notfound_ok=true
%%% @end
%%% Created :  6 May 2015 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(bigset_read_core).

%% API
-compile([export_all]).

-define(EMPTY, []).

-record(actor,
        {
          partition :: pos_integer(),
          clock=undefined :: undefined | bigset_clock:clock(),
          %% @TODO(rdb|experiment) conisder the Dot->Elem mapping as
          %% per Carlos's DotKernel
          elements= ?EMPTY:: [{binary(), riak_dt_vclock:dot()}],
          not_found = true :: boolean(),
          done = false :: boolean()
        }).

-record(state,
        {
          r :: pos_integer(),
          actors=orddict:new() :: [{pos_integer(), #actor{}}],
          clocks = 0 :: non_neg_integer(),
          not_founds = 0 :: non_neg_integer(),
          done = 0 :: non_neg_integer(),
          clock %% Merged clock
        }).

-type elements() :: [{binary(), binary()}].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

new(R) ->
    #state{r=R}.

%% @doc call when a clock is received
clock(Partition, Clock, Core) ->
    Actor = get_actor(Partition, Core),
    Actor2 = add_clock(Actor, Clock),
    Core2=#state{clocks=Clocks} = set_actor(Partition, Actor2, Core),
    Core2#state{clocks=Clocks+1}.

%% @doc only call if `r_clocks/1' is `true'
get_clock(Core) ->
    Clock = merge_clocks(Core),
    {Clock, Core#state{clock=Clock}}.

%% @doc do we have enough clocks?  this is R clocks, with
%% notfound_ok. So if first answer was notfound and second was a
%% clock, we have R, and if first and second was a clock, we have
%% R. Else we don't!
r_clocks(#state{clocks=0}) ->
    false;
r_clocks(#state{clocks=Clocks, not_founds=NotFounds, r=R}) when Clocks + NotFounds >= R ->
    true;
r_clocks(_S) ->
    false.

%% @doc update the state with a notfound_result for `Partition'
not_found(Partition, Core) ->
    Actor = get_actor(Partition, Core),
    Actor2 = set_not_found(Actor),
    Core2=#state{not_founds=NF} = set_actor(Partition, Actor2, Core),
    Core2#state{not_founds=NF+1}.

%% @doc is the set a notfound? This means the first two results
%% received were notfound
not_found(#state{not_founds=R, r=R}) ->
    true;
not_found(_S) ->
    false.

%% @doc add elements for a partition
elements(Partition, Elements, Core) ->
    Actor = get_actor(Partition, Core),
    Actor2 = append_elements(Actor, Elements),
    Core2 = set_actor(Partition, Actor2, Core),
    maybe_merge_and_flush(Core2).

%% @doc set a partition as done
done(Partition, Core) ->
    Actor = get_actor(Partition, Core),
    Actor2 = set_done(Actor),
    Core2=#state{done=Done}=set_actor(Partition, Actor2, Core),
    Core2#state{done=Done+1}.

is_done(#state{not_founds=NF, done=Done, r=R}) when NF+Done == R ->
    true;
is_done(_S) ->
    false.

%% @private see if we have enough results to merge a subset and send
%% it to the client @TODO(rdb|refactor) ugly
-spec maybe_merge_and_flush(#state{}) -> {[elements()], #state{}}.
maybe_merge_and_flush(Core=#state{not_founds=NF, clocks=C, r=R}) when NF+C < R ->
    {undefined, Core};
maybe_merge_and_flush(Core) ->
    %% need to be R before we proceed
    %% if any actor has a clock, but no values, and is not 'done' we
    %% can't proceed
    #state{actors=Actors} = Core,

    case mergable(Actors, undefined, []) of
        false ->
            {undefined, Core};
        {true, LeastLastElement, MergableActors} ->
            SplitFun = split_fun(LeastLastElement),
            %% of the actors that are mergable, split their lists
            %% fold instead so that you can merge+update the Core to return in one pass
            {MergedActor, NewActors} =lists:foldl(fun({Partition, Actor}, {MergedSet, NewCore}) ->
                                                          #actor{elements=Elements} = Actor,
                                                          {Merge, Keep} = lists:splitwith(SplitFun, Elements),
                                                          {
                                                            merge([{Partition, Actor#actor{elements=Merge}}], MergedSet),
                                                            orddict:store(Partition, Actor#actor{elements=Keep}, NewCore)
                                                          }
                                                  end,
                                                  {undefined, Actors},
                                                  MergableActors),
            {MergedActor#actor.elements, Core#state{actors=NewActors}}
    end.

%% Consider just the element, not the dots
split_fun(LeastLastElement) ->
    fun({E, _}) ->
            E =< LeastLastElement
    end.

%% Like lists:splitwith(fun split_fun(Least), Bin) but for binaries of elements
elements_split(Least, Bin) ->
    SplitAt = elements_split(Least, Bin, 0),
    <<Keep:SplitAt/binary, Merge/binary>> = Bin,
    {Merge, Keep}.

elements_split(Least, <<Sz:32/integer, Rest/binary>>, Cntr) ->
    <<E:Sz/binary, Rest2/binary>> = Rest,
    if E > Least ->
            elements_split(Least, Rest2, Cntr + (4+Sz));
       true ->
            %% done!
            Cntr
    end.


%% @private determine which, if any actors can be merged together to
%% generate a result for the client.
%%
%% Conisder each actor
%% -  if it's not_found, skip it
%% - If it's done and has no more elements, use it (for it's clock)
%% - If an actor is not done and has no elements, merging cannot
%%   continue, we have to wait for something to merge
%% - For any other actor (not done, has elements) take it's least
%%   element and acculumate the min of that and the current least
%%   element. We're taking the least last element of all the actors
%%   elements since this defines the common subset of the set we've
%%   received and can merge safely.
-spec mergable(Actors :: [{Partition :: pos_integer(), #actor{}}],
               LeastLastElement :: undefined | {binary(), [riak_dt:dot()]},
               MergedAccumulator :: [#actor{}]) ->
                      {true,
                       LeastLastElement :: {binary(), [riak_dt:dot()]},
                       [{Partition:: pos_integer(), #actor{}}]} |
                      false.
mergable([], LeastLast, MergeActors) ->
    {true, LeastLast, MergeActors};
mergable([{_Partition, #actor{not_found=true}} | Rest], LeastLast, MergeActors) ->
    mergable(Rest, LeastLast, MergeActors);
mergable([{Partition, #actor{done=false, elements= ?EMPTY}} | _Rest], _Acc, _MergeActors) ->
    %% We can't do anything, some partition has no elements
    %% and is not 'done'
    lager:debug("no elements for some vnode ~p~n", [Partition]),
    false;
mergable([{_P, #actor{done=true, elements= ?EMPTY}}=Actor | Rest], LeastLast, MergeActors) ->
    mergable(Rest, LeastLast, [Actor | MergeActors]);
mergable([{_P, #actor{elements=E}}=Actor | Rest], undefined, MergeActors) ->
    mergable(Rest, last_element(E), [Actor | MergeActors]);
mergable([{_P, #actor{elements=E}}=Actor | Rest], LeastLast, MergeActors) ->
    mergable(Rest, min(LeastLast, last_element(E)), [Actor | MergeActors]).

last_element(L) when is_list(L) ->
    {E, _Dots} = lists:last(L),
    E;
last_element(<<Sz:32/integer, Rest/binary>>) ->
    %% @TODO(rdb|remove?) Throw back from using a binary for an
    %% orswot, consider removing
    <<E:Sz/binary, _/binary>> = Rest,
    E.

%% @perform a CRDT orswot merge
finalise(#state{actors=Actors}) ->
    {true, _LLE, MergableActors} = mergable(Actors, undefined, []),
    case merge(MergableActors, undefined) of
        #actor{elements=Elements} ->
            Elements;
        undefined ->
            undefined
    end.

%% @private assumes that all actors with a clock's clocks are being
%% merged to a single clock
merge_clocks(#state{actors=Actors}) ->
    merge_clocks(Actors, bigset_clock:fresh()).

merge_clocks([], Clock) ->
    Clock;
merge_clocks([{_P, #actor{clock=undefined}} | Rest], Acc) ->
    merge_clocks(Rest, Acc);
merge_clocks([{_P, #actor{clock=Clock}} | Rest], Acc) ->
    merge_clocks(Rest, bigset_clock:merge(Clock, Acc)).

merge([], Actor) ->
    Actor;
merge([{_Partition, Actor} | Rest], undefined) ->
    merge(Rest, Actor);
merge([{_Partition, Actor} | Rest], Mergedest0) ->
    Mergedest = orswot_merge(Actor, Mergedest0),
    merge(Rest, Mergedest).

orswot_merge(#actor{clock=C1, elements=E}, A=#actor{clock=C2, elements=E}) ->
    A#actor{clock=bigset_clock:merge(C1, C2)};
orswot_merge(A1, A2) ->
    #actor{partition=P1, elements=E1, clock=C1} = A1,
    #actor{partition=P2, elements=E2, clock=C2} = A2,
    {_Repairs, Clock, Elements} = bigset_read_merge:merge_sets([{P1, C1, E1},
                                                               {P2, C2, E2}]),
    #actor{clock=Clock, elements=Elements}.

set_actor(Partition, Actor, State) ->
    #state{actors=Actors} = State,
    State#state{actors=orddict:store(Partition, Actor,  Actors)}.

get_actor(Partition, State) ->
    #state{actors=Actors} = State,
    case orddict:find(Partition, Actors) of
        error ->
            #actor{partition=Partition};
        {ok, Actor} ->
            Actor
    end.

set_not_found(Actor) ->
    Actor#actor{not_found=true}.

set_done(Actor) ->
    Actor#actor{done=true}.

append_elements(Actor, Elements) ->
    #actor{elements=Existing} = Actor,
    Actor#actor{elements=lists:append(Existing, Elements)}.

add_clock(Actor, Clock) ->
    Actor#actor{clock=Clock, not_found=false}.

-ifdef(TEST).

done_test() ->
    ?assert(is_done(#state{done=2, not_founds=0, r=2})),
    ?assert(is_done(#state{done=1, not_founds=1, r=2})),
    ?assert(is_done(#state{done=0, not_founds=2, r=2})),
    ?assertNot(is_done(#state{done=1, not_founds=0, r=2})),
    ?assertNot(is_done(#state{done=0, not_founds=0, r=2})),
    ?assertNot(is_done(#state{done=0, not_founds=1, r=2})).

-endif.

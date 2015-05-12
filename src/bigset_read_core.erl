%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% logic for mergig a streamed set of values
%%% defaults to r=2, n=3, basic_quorum=false, notfound_ok=true
%%% @end
%%% Created :  6 May 2015 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(bigset_read_core).

%% API
-compile([export_all]).

-record(actor,
        {
          partition :: pos_integer(),
          clock :: bigset_clock:clock(),
          elements=[]:: [{binary(), riak_dt_vclock:dot()}],
          not_found = true :: boolean(),
          done = false :: boolean()
        }).

-record(state,
        {
          r :: pos_integer(),
          actors=orddict:new() :: [#actor{}],
          clocks = 0 :: non_neg_integer(),
          not_founds = 0 :: non_neg_integer(),
          done = 0 :: non_neg_integer()
        }).

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
    set_actor(Partition, Actor2, Core).

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

%% @perform a CRDT orswot merge
finalise(#state{actors=Actors}) ->
    merge(Actors, undefined).

merge([], Elements) ->
    Elements;
merge([{_, Actor} | Rest], undefined) ->
    merge(Rest, Actor);
merge([{_P, Actor} | Rest], Mergedest) ->
    M2 = orswot_merge(Actor, Mergedest),
    merge(Rest, M2).

orswot_merge(A1, A2) ->
    #actor{elements=E1, clock=C1} = A1,
    #actor{elements=E2, clock=C2} = A2,
    #actor{clock=merge_clocks(C1, C2), elements=lists:umerge(E1, E2)}.

merge_clocks(undefined, undefined) ->
    undefined;
merge_clocks(C1, undefined) ->
    C1;
merge_clocks(undefined, C2) ->
    C2;
merge_clocks(C1, C2) ->
    bigset_clock:merge(C1, C2).

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
    #actor{elements=E} = Actor,
    Actor#actor{elements=[Elements | E]}.

add_clock(Actor, Clock) ->
    Actor#actor{clock=Clock}.

-ifdef(TEST).

done_test() ->
    State = #state{actors=[{P, #actor{done=true, partition=P}} || P <- [1, 2]]},
    ?assert(is_done(State)).

-endif.

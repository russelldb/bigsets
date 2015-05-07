%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% logic for reading a set
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
          actors=orddict:new() :: [#actor{}]
        }).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

new() ->
    #state{}.

result({r, Partition, not_found}, State) ->
    Actor = get_actor(Partition, State),
    Actor2 = set_not_found(Actor),
    set_actor(Partition, Actor2, State);
result({r, Partition, clock, Clock}, State) ->
    Actor = get_actor(Partition, State),
    Actor2 = add_clock(Actor, Clock),
    set_actor(Partition, Actor2, State);
result({r, Partition, elements, Elements}, State) ->
    Actor = get_actor(Partition, State),
    Actor2 = append_elements(Actor, Elements),
    set_actor(Partition, Actor2, State);
result({r, Partition, done}, State) ->
    Actor = get_actor(Partition, State),
    Actor2 = set_done(Actor),
    set_actor(Partition, Actor2, State).

is_done(State) ->
    #state{actors=Actors} = State,
    is_done(2, Actors).

is_done(0, _) ->
    true;
is_done(_N, []) ->
    false;
is_done(N, [{_P, #actor{done=true}} | Rest]) ->
    is_done(N-1, Rest);
is_done(N, [_ | Rest]) ->
    is_done(N, Rest).

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

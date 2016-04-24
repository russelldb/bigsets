%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% encapsulate the handoff receiver logic for bigset vnode
%%% @end
%%% Created : 18 Jan 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_handoff_receiver).

-compile(export_all).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {id :: binary(),
                db,
                senders = orddict:new()
               }).

-record(sender_state, {
          %% sender's vnode ID
          id :: binary(),
          %% sender's clock
          clock :: bigset_clock:clock(),
          %% The current set being handed off
          set :: binary(),
          %% track received dots
          tracker=bigset_clock:fresh() :: bigset_clock:clock()
         }).

new(Id, DB) ->
    #state{id=Id,
           db=DB}.

%% @TODO(rdb) detect a not_found locally so we can write an end_key if
%% needed.
update(_Sender, {clock, _Set, Actor}, _Value, State=#state{id=Actor}) ->
    %% My clock passed back to me, No Op
    {[], State};
update(_Sender, {set_tombstone, _Set, Actor}, _Value, State=#state{id=Actor}) ->
    %% My set tombstone passed back to me, No Op
    {[], State};
update(Sender, {clock, Set, Sender}=Key, Value, State) ->
    %% Sender's clock, need this in handoff state, also store it
    SenderState = get_sender_state(Sender, State),
    SenderClock = bigset:from_bin(Value),
    SenderState2 = SenderState#sender_state{clock=bigset_causal:clock(SenderClock), set=Set},
    State2 = update_sender_state(SenderState2, State),
    {[{put, Key, Value}],
     State2};
update(_Sender, {clock, _Set, _Actor}=Key, Value, State) ->
    {[{put, Key, Value}],
     State};
update(_Sender, {set_tombstone, _Set, _Actor}=Key, Value, State) ->
    {[{put, Key, Value}],
     State};
update(Sender, {end_key, Set}, _V, State) ->
    %% end of the set, generate the set tombstone to write, and clear
    %% the state
    #state{id=Id, db=DB} = State,
    #sender_state{clock=SenderClock, tracker=Tracker, set=Set} = get_sender_state(Sender, State),
    ClockKey = bigset:clock_key(Set, Id),
    LocalCausal = bigset:get_clock(ClockKey, DB),
    LocalClock = bigset_causal:clock(LocalCausal),

    %% This section takes care of those keys that Receiver has seen,
    %% but Sender has removed. i.e. keys that are not handed off, but
    %% their absence means something, and we don't want to read the
    %% whole bigset at the Receiver to find that out.

    %% The events in the Sender's clock, not in Tracking Clock, that
    %% is all the dots that were not handed off by Sender, and
    %% therefore Sender saw but has removed
    DelDots = bigset_clock:complement(SenderClock, Tracker),

    %% All the dots that Receiver had seen before hand off, but which
    %% the handing off node has deleted
    ToRemove = bigset_clock:intersection(DelDots, LocalClock),

    LocalCausal2 = bigset_causal:tombstone_all(ToRemove, LocalCausal),
    LocalCausal3 = bigset_clock:merge_clock(SenderClock, LocalCausal2),
    BinCausal = bigset:to_bin(LocalCausal3),
    State2 = remove_sender(Sender, State),

    {[{put, ClockKey, BinCausal}],
     State2};
update(Sender, Key, Value, State) ->
    %% Can only be an element key
    #sender_state{set=Set, tracker=Tracker} =  get_sender_state(Sender, State),
    #state{id=Id, db=DB} = State,
    {element, Set, _E, Actor, Cntr} = Key,
    ClockKey = bigset:clock_key(Set, Id),
    LocalClock = bigset:get_clock(ClockKey, DB),
    Dot = {Actor, Cntr},
    case bigset_causal:seen(LocalClock, Dot) of
        true ->
            %% no op
            {[], State};
        false ->
            Clock2 = bigset_causal:add_dot(LocalClock, Dot),
            Tracker2 = bigset_clock:add_dot(Dot, Tracker),
            State2 = update_tracker(Tracker2, Sender, State),
            {[{put, ClockKey, bigset:to_bin(Clock2)},
              {put, Key, Value}],
             State2}
    end.

remove_sender(Sender, State) ->
    #state{senders=Senders} = State,
    State#state{senders=orddict:erase(Sender, Senders)}.

update_tracker(Tracker, Sender, State) ->
    SS = get_sender_state(Sender, State),
    update_sender_state(SS#sender_state{tracker=Tracker}, State).

update_sender_state(SenderState, State) ->
    #sender_state{id=Id} = SenderState,
    #state{senders=Senders} = State,
    Senders2 = orddict:store(Id, SenderState, Senders),
    State#state{senders=Senders2}.

get_sender_state(ID, error) ->
    #sender_state{id=ID};
get_sender_state(_ID, {ok, State}) ->
    State;
get_sender_state(ID, #state{senders=Senders}) ->
    get_sender_state(ID, Senders);
get_sender_state(ID, Senders) ->
    get_sender_state(ID, orddict:find(ID, Senders)).

get_receiver(#state{id=ID}) ->
    ID.


-ifdef(TEST).



-endif.

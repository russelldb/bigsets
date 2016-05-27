%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% encapsulate the handoff receiver logic for bigset vnode
%%% @end
%%% Created : 18 Jan 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_handoff_receiver).

-compile(export_all).

-include("bigset.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-record(state, {id :: binary(),
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

new(Id) ->
    #state{id=Id}.

%% @TODO(rdb|refactor) seems wrong that keys are encapsulated in
%% bigset.erl functions but the matching here depends on the
%% that. Maybe a case statement on type case bigset:key_type(Key) of
%% sort of thing?  @doc returns a list of writes for the vnode, and an
%% updated handoff state. Should it return a boolean(), state, and an
%% optional updated clock?
update(_Sender, {{clock, _Set, Actor}, _Value}, _Clock, State=#state{id=Actor}) ->
    %% My clock passed back to me, No Op
    {[], State};
update(Sender, {{clock, Set, Sender}=DKey, Value}, _Clock, State) ->
    %% Sender's clock, need this in handoff state, also store it
    SenderState = get_sender_state(Sender, State),
    SenderClock = bigset:from_bin(Value),
    SenderState2 = SenderState#sender_state{clock=bigset_causal:clock(SenderClock),
                                            set=Set},
    State2 = update_sender_state(SenderState2, State),
    Key = bigset:encode_key(DKey),
    {[{put, Key, Value}],
     State2};
update(_Sender, {{clock, _Set, _Actor}=DKey, Value}, _Clock, State) ->
    %% any other clock
    Key = bigset:encode_key(DKey),
    {[{put, Key, Value}],
     State};
update(_Sender, {{set_tombstone, _Set, Actor}, _Value}, _Clock, State=#state{id=Actor}) ->
    %% My set tombstone passed back to me, No Op
    {[], State};
update(_Sender, {{set_tombstone, _Set, _Actor}=Key, Value}, _Clock, State) ->
    %% any other tombstone
    {[{put, Key, Value}],
     State};
update(Sender, {{element, Set, _E, Actor, Cntr}=DKey, Value}, LocalClock, State) ->
    #sender_state{set=Set, tracker=Tracker} =  get_sender_state(Sender, State),
    #state{id=Id} = State,

    ClockKey = bigset:clock_key(Set, Id),
    Dot = {Actor, Cntr},
    Tracker2 = bigset_clock:add_dot(Dot, Tracker),
    State2 = update_tracker(Tracker2, Sender, State),

    case bigset_causal:seen(Dot, LocalClock) of
        true ->
            {[], State2};
        false ->
            Key = bigset:encode_key(DKey),
            Clock2 = bigset_causal:add_dot(Dot, LocalClock),
            {[{put, ClockKey, bigset:to_bin(Clock2)},
              {put, Key, Value}],
             State2}
    end;
update(Sender, {{end_key, Set}, _V}, LocalCausal, State) ->
    %% end of the set, generate the set tombstone to write, and clear
    %% the state
    #state{id=Id} = State,
    #sender_state{clock=SenderClock,
                  tracker=Tracker,
                  set=Set} = get_sender_state(Sender, State),
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
    LocalCausal3 = bigset_causal:merge_clock(SenderClock, LocalCausal2),
    BinCausal = bigset:to_bin(LocalCausal3),
    State2 = remove_sender(Sender, State),

    ClockKey = bigset:clock_key(Set, Id),

    {[{put, ClockKey, BinCausal}],
     State2}.

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

-define(SET, <<"set">>).
-define(REC, <<"receiver">>).
-define(SENDER, <<"sender">>).

my_clock_test() ->
    State = new(?REC),
    ?assertEqual({[], State}, update(<<"any">>, {{clock, ?SET, ?REC}, <<"v">>}, bigset_causal:fresh(), State)).

sender_clock_test() ->
    Key = {clock, ?SET, ?SENDER},
    BinKey = bigset:encode_key(Key),
    State = new(?REC),
    SenderClock = {[{?SENDER, 3}, {b, 4}, {c, 4}], [{c,[7, 8]}]},
    SenderCausal = bigset_causal:new(SenderClock),
    HandOffValue = bigset:to_bin(SenderCausal),
    {Writes, NewState} = update(?SENDER, {Key, HandOffValue}, bigset_causal:fresh(), State),

    ?assertEqual([{put, BinKey, HandOffValue}], Writes),
    SenderState = get_sender_state(?SENDER, NewState),
    ?assertMatch(#sender_state{clock=SenderClock, set=?SET}, SenderState).

other_clock_test() ->
    Other = <<"other">>,
    Key = {clock, ?SET, Other},
    BinKey = bigset:encode_key(Key),
    State = new(?REC),
    OtherClock = {[{<<"other">>, 3}, {b, 4}, {c, 4}], [{c,[7, 8]}]},
    OtherCausal = bigset_causal:new(OtherClock),
    HandOffValue = bigset:to_bin(OtherCausal),
    {Writes, NewState} = update(?SENDER, {Key, HandOffValue}, bigset_causal:fresh(), State),

    ?assertEqual([{put, BinKey, HandOffValue}], Writes),
    ?assertEqual(State, NewState).

seen_element_test() ->
    Key = {element, ?SET, <<"e">>, ?REC, 1},
    MyClock = {[{?REC, 3}, {?SENDER, 4}, {<<"other">>, 4}], [{<<"other">>,[7, 8]}]},
    MyCausal = bigset_causal:new(MyClock),
    ElementValue = <<>>,
    State = new(?REC),

    SenderClock = {[{?SENDER, 3}, {b, 4}, {c, 4}], [{c,[7, 8]}]},
    SenderCausal = bigset_causal:new(SenderClock),
    HandOffValue = bigset:to_bin(SenderCausal),

    {_Writes, NewState} = update(?SENDER, {{clock, ?SET, ?SENDER}, HandOffValue}, bigset_causal:fresh(), State),

    {ElementWrites, NewState2} = update(?SENDER, {Key, ElementValue}, MyCausal, NewState),
    ?assertEqual([], ElementWrites),
    %% @TODO this is wrong, tracker should still be updated!!
    ?assertNotEqual(NewState, NewState2),
    SenderState = get_sender_state(?SENDER, NewState2),
    #sender_state{tracker=Tracker} = SenderState,
    ?assertEqual({[{?REC, 1}], []}, Tracker).

unseen_element_test() ->
    Key = {element, ?SET, <<"e">>, ?SENDER, 11},
    MyClock = {[{?REC, 3}, {?SENDER, 4}, {<<"other">>, 4}], [{<<"other">>,[7, 8]}]},
    MyCausal = bigset_causal:new(MyClock),
    ElementValue = <<>>,
    State = new(?REC),

    SenderClock = {[{?SENDER, 3}, {b, 4}, {c, 4}], [{c,[7, 8]}]},
    SenderCausal = bigset_causal:new(SenderClock),
    HandOffValue = bigset:to_bin(SenderCausal),

    %% initialise sender state
    {_Writes, NewState} = update(?SENDER, {{clock, ?SET, ?SENDER}, HandOffValue}, bigset_causal:fresh(), State),

    {ElementWrites, NewState2} = update(?SENDER, {Key, ElementValue}, MyCausal, NewState),

    SenderState = get_sender_state(?SENDER, NewState2),
    #sender_state{tracker=Tracker} = SenderState,
    %% tracker must have new dot
    ?assertEqual({[], [{?SENDER, [11]}]}, Tracker),

    %% Unpack writes and verify clock went up
    ClockKey = bigset:clock_key(?SET, ?REC),
    {value, {put, ClockKey, BinClock}, [{put, EKey, EVal}]} = lists:keytake(ClockKey, 2, ElementWrites),
    NewClock = bigset:from_bin(BinClock),
    ExpectedClock = bigset_causal:add_dot({?SENDER, 11}, MyCausal),
    ?assertEqual(ExpectedClock, NewClock),
    %% check that key/val is as expected
    ?assertEqual(<<>>, EVal),
    ?assertEqual(bigset:encode_key(Key), EKey).

%% @TODO(rdb|refactor) not overly happy with this, would prefer eqc,
%% but yet to manage it.
end_key_test() ->
    %% NOTE: clocks must be sorted as bigset_clock expects orddicts
    Key = {end_key, ?SET},
    MyClock = {[
                {<<"other1">>, 4},
                {?REC, 10},
                {?SENDER, 4}
               ],
               [
                {<<"other1">>, [7, 8]}
               ]
              },
    MyCausal = bigset_causal:new(MyClock),
    Value = <<>>,

    %% We want a mix here of things unseen by the receiver, and things
    %% seen. We want to have removed some things seen by the receiver
    %% too.
    SenderClock = {[
                    {<<"other1">>, 3},
                    {<<"other2">>, 5},
                    {?REC, 2},
                    {?SENDER, 10}
                   ],
                   [
                    {<<"other1">>, [5,6]},
                    {?REC, [7,9]}
                   ]},
    %% Important to leave out things seen, and ensure they end up on
    %% tombstone. Must be a subset of the SenderClock (obviously)
    Tracker = {[
                {<<"other1">>, 2},
                {<<"other2">>, 5},
                {?REC, 2},
                {?SENDER, 3}
               ],
               [
                {<<"other1">>, [5,6]}, %% 3 from receiver's clock removed
                {?REC,    [9]}, %% 7 removed
                {?SENDER, [5,6,7,8,10]} %% so 4 and 9 removed, but only 4 in receiver clock
               ]},
    FinalClock = bigset_clock:merge(MyClock, SenderClock),
    %% Should only be the events in MyClock not in Tracker
    FinalTombstone = {[], [ {<<"other1">>, [3]}, {?REC, [7]}, {?SENDER, [4]}]},
    SenderState = #sender_state{id=?SENDER,
                                set=?SET,
                                clock=SenderClock,
                                tracker=Tracker},

    ClockKey = bigset:clock_key(?SET, ?REC),

    State = update_sender_state(SenderState, new(?REC)),
    %% FINALLY! run the function
    {Writes, FinalState} = update(?SENDER, {Key, Value}, MyCausal, State),

    ?assertMatch([{put, ClockKey, _}], Writes),
    [{put, _, BinCausal}] = Writes,
    FinalCausal = bigset:from_bin(BinCausal),

    ?assertEqual(FinalClock, bigset_causal:clock(FinalCausal)),
    ?assertEqual(FinalTombstone, bigset_causal:tombstone(FinalCausal)),
    ?assertEqual(new(?REC), FinalState).

-endif.


-ifdef(EQC).

-define(ELEMENTS, [<<C:8/integer>> || C <- lists:seq(97, 122)]).


gen_bigset() ->
    ?LET(BS,
         ?LET(SetName, nat(),
              gen_bigset(<<SetName:8/integer>>)),
         BS).
         %% %% Distribute subsets per-member to some other members
         %% orddict:foldl(fun(Actor, {Clock, Keys}, Acc) ->
         %%                       ?LET(Replicas, sublist(BS),
         %%                            begin
         %%                                NewReps = lists:foldl(fun({A, {C, K}) InnerAcc) ->
         %%                                                ?LET(Deltas, sublist(Keys),
         %%                                                     begin
         %%                                                         NewRep = store({C, K}, Deltas),
         %%                                                         orddict:store(A, NewRep, InnerAcc)
         %%                                                     end)
         %%                                        end,
         %%                                        orddict:new(),
         %%                                        Replicas),


         %% %% [?LET(Replicas, sublist(BS),
         %% %%       [?LET(Deltas, sublist(Keys),
         %% %%             store(Replica, Deltas)) || Replica <- Replicas]) || {_Rep, _Clock, Keys} <- BS]).

         %% %% Remover a random subset of keys from each member
         %% %% done?

store({Clock, Keys}, Deltas) ->
    %% add dots to clock for this replica, add keys to data
    lists:foldl(fun({element, _Set, _Elem, Actor, Cnt}=Key, {C, K}) ->
                                   {bigset_clock:add_dot({Actor, Cnt}, C),
                                    ordsets:add_element(Key, K)}
                           end,
                           {Clock, ordsets:from_list(Keys)},
                           Deltas).

gen_bigset(Set) ->
    ?LET(System, bigset_clock:gen_all_system_events(), gen_bigset(System, Set)).

gen_bigset(System, Set) ->
    lists:foldl(fun({Actor, _Cnt}=ActorEvents, Acc) ->
                        Keys = gen_dot_keys(ActorEvents, Set),
                        orddict:store(Actor, {{[ActorEvents], []}, %% Clock
                                              Keys}, Acc)
                end,
                orddict:new(),
                System).

gen_dot_keys({Actor, Cnt}, Set) ->
    ?LET(Keys, [?LET(Elem, elements(?ELEMENTS),
                     bigset:decode_key(bigset:insert_member_key(Set, Elem, Actor, Event))) ||
                   Event <- lists:seq(1, Cnt)],
         lists:usort(Keys)).
         %% begin
         %%     Sorted = lists:usort(Keys),
         %%     %% For each member, generate a set of replicas
         %%     ?LET(Replicas, sublist(System),
         %%          %% For each replica generate a set of deltas
         %%          [?LET(Deltas, sublist(Sorted), {Replica, Deltas}) || Replica <- Replicas])

-endif.

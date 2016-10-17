%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% encapsulate the handoff receiver logic for bigset vnode
%%% @end
%%% Created : 18 Jan 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_handoff).

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

-type state() :: #state{}.

new(Id) ->
    #state{id=Id}.

sender_clock(Set, Sender, SenderClock, State) ->
    SenderState = get_sender_state(Sender, State),
    SenderState2 = SenderState#sender_state{clock=SenderClock,
                                            set=Set},
    update_sender_state(SenderState2, State).

add_dot(Set, Sender, Dot, State) ->
    #sender_state{set=Set, tracker=Tracker} =  get_sender_state(Sender, State),
    Tracker2 = bigset_clock:add_dot(Dot, Tracker),
    update_tracker(Tracker2, Sender, State).

%% @doc the `end_key' for a bigset means we can calculate from the
%% tracker and sender_clock what dots have been removed by the sender
%% and must therefore be removed by the receiver. Returns the new
%% Tombstone and Clock. Events seen by the receiver and removed by the
%% sender will be added to the Tombstone. Events unseen by the
%% receiver but removed by the sender will be added to the clock (to
%% ensure they're never written.)

%%  @TODO NOTE: it is very possible that we add to the tombstone some
%%  event that has already been compacted out and therefore removed
%%  from the receivers tombstone. As yet we have no way of recording
%%  this efficiently, which means we may have a tombstone that
%%  accretes garbage over time. However, it depends on how compaction
%%  works, if compaction coves the _whole set_ then it can shrink the
%%  tombstone by simply subtracting the tombstone at the start of
%%  compaction from the one on disk at the end.
-spec end_key(set(), actor(),
              bigset_clock:clock(),
              bigset_clock:clock(),
              state()) ->
                     {Clock :: bigset_clock:clock(),
                      Tombstone :: bigset_clock:clock(),
                      state()}.
end_key(Set, Sender, LocalClock, Tombstone, State) ->
    %% end of the set, generate the set tombstone to write, and clear
    %% the state
    #sender_state{clock=SenderClock,
                  tracker=Tracker,
                  set=Set} = get_sender_state(Sender, State),


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

    TS2 = bigset_clock:merge(ToRemove, Tombstone),
    Clock2 = bigset_clock:merge(SenderClock, LocalClock),

    State2 = remove_sender(Sender, State),
    {Clock2, TS2, State2}.

%% @doc add the dots in `Dots' to the tombstone-clock or the set-clock
%% depending on whether they're seen or not. Seen dots go on the
%% tombstone (they need dropping from storage) unseen just go on the
%% clock (so they never get written.). Returns updated clock and
%% tombstone with `Dots' added.  @TODO @NOTE we may very well be adding dots
%% to the tombstone that have been removed from the tombstone by a
%% compaction. These dots must somehow be removed from the
%% set-tombstone, maybe by a sweep/full-set-read
-spec tombstone_dots([bigset_clock:dot()],
                     {Clock::bigset_clock:clock(),
                      Tombstone::bigset_clock:clock()}) ->
                            {Clock::bigset_clock:clock(),
                             Tombstone::bigset_clock:clock()}.
tombstone_dots(Dots, {Clock, Tombstone}) ->
    lists:foldl(fun(Dot, {SC, TS}) ->
                        case bigset_clock:seen(Dot, SC) of
                            true ->
                                {SC, bigset_clock:add_dot(Dot, TS)};
                            false ->
                                {bigset_clock:add_dot(Dot, SC), TS}
                        end
                end,
                {Clock, Tombstone},
                Dots).

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

sender_clock_test() ->
    State = new(?REC),
    SenderClock = {[{?SENDER, 3}, {b, 4}, {c, 4}], [{c,[7, 8]}]},
    NewState = sender_clock(?SET, ?SENDER, SenderClock, State),
    SenderState = get_sender_state(?SENDER, NewState),
    ?assertMatch(#sender_state{clock=SenderClock, set=?SET}, SenderState).

add_dot_test() ->
    State = new(?REC),
    Dot = {?SENDER, 11},
    SenderClock = {[{?SENDER, 3}, {b, 4}, {c, 4}], [{c,[7, 8]}]},

    %% initialise sender state
    NewState = sender_clock(?SET, ?SENDER, SenderClock, State),

    NewState2 = add_dot(?SET, ?SENDER, Dot, NewState),

    SenderState = get_sender_state(?SENDER, NewState2),
    #sender_state{tracker=Tracker} = SenderState,
    %% tracker must have new dot
    ?assertEqual({[], [{?SENDER, [11]}]}, Tracker).

%% @TODO(rdb|refactor) not overly happy with this, would prefer eqc,
%% but yet to manage it.
end_key_test() ->
    %% NOTE: clocks must be sorted as bigset_clock expects orddicts
    MyClock = {[
                {<<"other1">>, 4},
                {?REC, 10},
                {?SENDER, 4}
               ],
               [
                {<<"other1">>, [7, 8]}
               ]
              },

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


    State = update_sender_state(SenderState, new(?REC)),
    %% FINALLY! run the function
    {ActualClock, ActualTombstone, FinalState} = end_key(?SET, ?SENDER, MyClock, bigset_clock:fresh(), State),

    ?assertEqual(FinalClock, ActualClock),
    ?assertEqual(FinalTombstone, ActualTombstone),
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
                     bigset_keys:decode_key(bigset_keys:insert_member_key(Set, Elem, Actor, Event))) ||
                   Event <- lists:seq(1, Cnt)],
         lists:usort(Keys)).
         %% begin
         %%     Sorted = lists:usort(Keys),
         %%     %% For each member, generate a set of replicas
         %%     ?LET(Replicas, sublist(System),
         %%          %% For each replica generate a set of deltas
         %%          [?LET(Deltas, sublist(Sorted), {Replica, Deltas}) || Replica <- Replicas])

-endif.

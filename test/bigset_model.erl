%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% just a way to play with the bigset simulation code at the console
%%% @end
%%% Created :  7 Dec 2015 by Russell Brown <russelldb@basho.com>

-module(bigset_model).

-compile([export_all]).

-export_type([bigset/0]).

-define(CLOCK, bigset_clock).
-define(SET, set).

-record(bigset, {
          clock=bigset_clock:fresh(),
          tombstone=bigset_clock:fresh(),
          keys=ordsets:new() %% Simulate the back end with a set of keys
         }).

-type element() :: term().
-type actor() :: term().

-type bigset() :: #bigset{}.
-type delta() :: op_key().

-type op_key() :: {Element :: element(),
                    Actor   :: actor(),
                    Count   :: pos_integer()}.


-type dotlist() :: [bigset_clock:dot()].

-type orswot_elem() :: {Element :: term(),
                        dotlist()}.

-type merged_bigset() :: {bigset_clock:clock(),
                          [orswot_elem()]}.

-spec new() -> bigset().
new() ->
    #bigset{}.

-spec new_mbs() -> merged_bigset().
new_mbs() ->
    {bigset_clock:fresh(), orddict:new()}.

-spec add(term(), term(), bigset()) ->
                 {delta(), bigset()}.
add(Element, ID, Bigset) ->
    #bigset{tombstone=Tombstone, keys=Keys} = Bigset,
    %% In the absence of a client context and AAAD, use the dots at
    %% the coordinating replica as the context of the add operation,
    %% any 'X' seen at this node will be removed by an add of an 'X'
    Ctx = current_dots(Element, Keys, Tombstone),
    add(Element, ID, Ctx, Bigset).

add(Element, ID, Ctx, Bigset) ->
    #bigset{clock=Clock, keys=Keys} = Bigset,
       %% delete keys for Element whose dots are in Ctx

    {Clock2, Keys2} = remove_seen(Element, Ctx, Clock, Keys),

    {{ID, Cnt}, Clock3} = bigset_clock:increment(ID, Clock2),
    Key = {Element, ID, Cnt},

    Keys3 = ordsets:add_element(Key, Keys2),
    {{add, {Key, Ctx}}, Bigset#bigset{clock=Clock3, keys=Keys3}}.

%% @private remove keys whose context is seen, or add unseen dots to clock
remove_seen(Element, Ctx, Clock, Keys) ->
    lists:foldl(fun({DA, DC}=Dot, {ClockAcc, KeysAcc}) ->
                        case bigset_clock:seen(Dot, ClockAcc) of
                            true ->
                                {ClockAcc, ordsets:del_element({Element, DA, DC}, KeysAcc)};
                            false ->
                                {bigset_clock:add_dot(Dot, ClockAcc), KeysAcc}
                        end
                end,
                {Clock, Keys},
                Ctx).

-spec remove(element(), actor(), bigset()) ->
                    {delta(), bigset()}.
remove(Element, ID,  Bigset) ->
    #bigset{tombstone=Tombstone, keys=Keys} = Bigset,
    %% In the absence of a client context and AAAD, use the dots at
    %% the coordinating replica as the context of the remove
    %% operation, any 'X' seen at this node will be removed by an
    %% remove of an 'X'
    Ctx = current_dots(Element, Keys, Tombstone),
    remove(Element, ID, Ctx, Bigset).

remove(Element, _ID, Ctx, Bigset) ->
    #bigset{clock=Clock, keys=Keys} = Bigset,
    {Clock2, Keys2} = remove_seen(Element, Ctx, Clock, Keys),
    {{remove, Element, Ctx}, Bigset#bigset{clock=Clock2, keys=Keys2}}.

%% You must always tombstone the removed context of an add. Just
%% because the clock has seen the dot of an add does not mean it hs
%% seen the removed dots. Imagie you have seen a remove of {a, 2} but
%% not the add of {a, 2} that removes {a, 1}. Even though you don't
%% write {a, 2}, you must remove what it removes.
-spec delta_join(delta(), bigset()) -> bigset().
delta_join({add, Delta}, Bigset) ->
    #bigset{clock=Clock, keys=Keys} = Bigset,
    {{E, A, C}=Key, Ctx} = Delta,
    {Clock2, Keys2} = remove_seen(E, Ctx, Clock, Keys),
    case bigset_clock:seen({A, C}, Clock) of
        true ->
            Bigset#bigset{clock=Clock2, keys=Keys2};
        false ->
            C3 = bigset_clock:add_dot({A, C}, Clock2),
            Bigset#bigset{clock=C3, keys=ordsets:add_element(Key, Keys2)}
    end;
delta_join({remove, Element, Ctx}, Bigset) ->
    #bigset{clock=Clock, keys=Keys} = Bigset,
    {Clock2, Keys2} = remove_seen(Element, Ctx, Clock, Keys),
    Bigset#bigset{clock=Clock2, keys=Keys2}.

receive_end_key(SenderClock, Tracker, Bigset) ->
    #bigset{clock=LocalClock, tombstone=Tombstone} = Bigset,
    DelDots = bigset_clock:complement(SenderClock, Tracker),
    %% All the dots that Receiver had seen before hand off, but which
    %% the handing off node has deleted
    ToRemove = bigset_clock:intersection(DelDots, LocalClock),

    Tombstone2 = bigset_clock:merge(ToRemove, Tombstone),
    LocalClock2 = bigset_clock:merge(SenderClock, LocalClock),
    Bigset#bigset{clock=LocalClock2, tombstone=Tombstone2}.

receive_handoff_key({_E, A, C}=Key, Bigset) ->
    #bigset{clock=Clock, keys=Keys} = Bigset,
    Dot = {A, C},
    case bigset_clock:seen(Dot, Clock) of
        true ->
            Bigset;
        false ->
            Bigset#bigset{clock=bigset_clock:add_dot(Dot, Clock),
                          keys=ordsets:add_element(Key, Keys)}
    end.

-spec value(merged_bigset() | bigset()) -> [term()].
value(#bigset{}=Bigset) ->
    value(read(Bigset));
value({_C, Keys}) ->
    orddict:fetch_keys(Keys).

-spec size(bigset()) -> non_neg_integer().
size(Bigset) ->
    ordsets:size(Bigset#bigset.keys).

%% this is the algo that level will run. Any key seen by the
%% set-tombstone is discarded.
-spec compact(bigset()) -> bigset().
compact(Bigset) ->
    BS = ordsets:fold(fun({_E, A, C}=Key, BS=#bigset{tombstone=Tombstone, keys=Elements}) ->
                         case bigset_clock:seen({A, C}, Tombstone) of
                             true ->
                                 BS#bigset{%%tombstone=bigset_clock:subtract_dot(Tombstone, {A, C}),
                                           keys=ordsets:del_element(Key, Elements)};
                             false ->
                                 BS
                         end
                 end,
                 Bigset,
                      Bigset#bigset.keys),
    BS#bigset{tombstone=bigset_clock:fresh()}.

is_member(Element, BS) ->
    #bigset{tombstone=Tombstone, keys=Keys} = BS,
    is_member(current_dots(Element, Keys, Tombstone)).

is_member([]) ->
    {false, []};
is_member(Ctx) ->
    {true, Ctx}.

%% @doc a vnode read of an individual vnode represented by `BS'
read(BS) ->
    #bigset{tombstone=Tombstone, clock=Clock, keys=Keys} = BS,
    Keys2 = ordsets:fold(fun({E, A, C}, Acc0) ->
                                Dot = {A, C},
                                case bigset_clock:seen({A, C}, Tombstone) of
                                    false ->
                                        orddict:update(E, fun(Dots) ->
                                                                  [Dot | Dots]
                                                          end,
                                                       [Dot],
                                                       Acc0);
                                    true ->
                                        Acc0
                                end
                        end,
                        orddict:new(),
                        Keys),
    {Clock, Keys2}.

%% A merge as per the read path, where each bigset is first
%% accumulated out of the backend into a regular orswot like
%% structure.
-spec read_merge(merged_bigset(), merged_bigset()) -> merged_bigset().
read_merge(MBS1, MBS2) ->
    {Clock1, Keys1} = MBS1,
    {Clock2, Keys2} = MBS2,

    {Set2Unique, Keep} = orddict:fold(fun(Elem, LDots, {RHSU, Acc}) ->
                                              case orddict:find(Elem, Keys2) of
                                                  {ok, RDots} ->
                                                      %% In both, keep maybe
                                                      RHSU2 = orddict:erase(Elem, RHSU),
                                                      Both = ordsets:intersection([ordsets:from_list(RDots), ordsets:from_list(LDots)]),
                                                      RHDots = bigset_clock:subtract_seen(Clock1, RDots),
                                                      LHDots = bigset_clock:subtract_seen(Clock2, LDots),

                                                      case lists:usort(RHDots ++ LHDots ++ Both) of
                                                          [] ->
                                                              %% it's gone!
                                                              {RHSU2, Acc};
                                                          Dots ->
                                                              {RHSU2, orddict:store(Elem, Dots, Acc)}
                                                      end;
                                                  error ->
                                                      %% Set 1 only, did set 2 remove it?
                                                      case bigset_clock:subtract_seen(Clock2, LDots) of
                                                          [] ->
                                                              %% removed
                                                              {RHSU, Acc};
                                                          Dots ->
                                                              %% unseen by set 2
                                                              {RHSU, orddict:store(Elem, Dots, Acc)}
                                                      end
                                              end
                                      end,
                                      {Keys2, orddict:new()},
                                      Keys1),
    %% Do it again on set 2
    MergedKeys = orddict:fold(fun(Elem, RDots, Acc) ->
                                      %% Set 2 only, did set 1 remove it?
                                      case bigset_clock:subtract_seen(Clock1, RDots) of
                                          [] ->
                                              %% removed
                                              Acc;
                                          Dots ->
                                              %% unseen by set 1
                                              orddict:store(Elem, Dots, Acc)
                                      end
                              end,
                              Keep,
                              Set2Unique),

    {bigset_clock:merge(Clock1, Clock2),
     MergedKeys}.

clock(#bigset{clock=Clock}) ->
    Clock.

tombstone(#bigset{tombstone=Tombstone}) ->
    Tombstone.

handoff_keys(#bigset{tombstone=Tombstone, keys=Keys}) ->
    ordsets:filter(fun({_E, A, C}) ->
                           not bigset_clock:seen({A, C}, Tombstone)
                   end,
                   Keys).

%% @doc simulate handoff of keys from `From' to `To'. Returns `To'
%% after receiving handoff.  @TODO(rdb) find a way to use the handoff
%% receiver for this, ideally have the statem actually call and crash
%% and restart handoff
%% -spec handoff(From :: bigset(), To :: bigset()) -> NewTo :: bigset().
%% handoff(From, To) ->
%%     #bigset{keys=FromKeys, causal=FromCausal} = From,
%%     #bigset{causal=ToCausal, keys=ToKeys} = To,
%%     TrackingClock = bigset_clock:fresh(),

%%     %% This code takes care of the keys in From, either add them as
%%     %% unseen, or drop them as seen
%%     {ToCausal2, ToKeys2, TrackingClock2} = ordsets:fold(fun({_E, A, C}=K, {Causal, Keys, TC}=Acc) ->
%%                                                                Dot = {A, C},
%%                                                                case bigset_causal:is_tombstoned(Dot, FromCausal) of
%%                                                                    %% Not tombstoned so "send" it in handoff
%%                                                                    false ->
%%                                                                        TC2 = bigset_clock:add_dot(Dot, TC),
%%                                                                        case bigset_causal:seen(Dot, Causal) of
%%                                                                            true ->
%%                                                                                {Causal, Keys, TC2};
%%                                                                            false ->
%%                                                                                %% Not seen at receiver, store it
%%                                                                                {bigset_causal:add_dot(Dot, Causal),
%%                                                                                 ordsets:add_element(K, Keys),
%%                                                                                 TC2}
%%                                                                        end;
%%                                                                    true ->
%%                                                                        Acc
%%                                                                end
%%                                                        end,
%%                                                        {ToCausal, ToKeys, TrackingClock},
%%                                                        FromKeys),
%%     %% This section takes care of those keys that To has seen, but
%%     %% From has removed. i.e. keys that are not handed off, but their
%%     %% absence means something, and we don't want to read the whole
%%     %% bigset at To to find that out.

%%     %% The events in FromCausal.clock, not in TrackingClock2, that is
%%     %% all the dots that we were not handed by From, and therefore
%%     %% FromCausal.clock saw but has since removed
%%     FromClock = bigset_causal:clock(FromCausal),
%%     DeletedDots = bigset_clock:complement(FromClock, TrackingClock2),

%%     %% All the dots that we had seen **before** hand off, but which
%%     %% the handing off node has deleted
%%     ToClock = bigset_causal:clock(ToCausal),
%%     ToRemove = bigset_clock:intersection(DeletedDots, ToClock),

%%     %% This takes care of the keys that From has seen and removed but
%%     %% To has not (yet!) seen. Merging the clocks ensures that keys
%%     %% that To has not seen, but From has removed never get added to
%%     %% To.
%%     Causal0 = bigset_causal:merge_clocks(ToCausal2, FromCausal),
%%     To#bigset{causal=bigset_causal:tombstone_all(ToRemove, Causal0),
%%               keys=ToKeys2}.

%% @private just pull all the dots at this replica for element `E' as
%% the context. Simulates a per-elem-ctx for now
%%-spec current_dots(element(), [op_key()]) -> dotlist().
current_dots(E, Keys, Tombstone) ->
    current_dots(E, Keys, Tombstone, []).

%%-spec current_dots(element(), [op_key()], dotlist()) -> dotlist().
current_dots(E, [{E, ID, Cnt} | Keys], Tombstone, Acc) ->
    case bigset_clock:seen({ID, Cnt}, Tombstone) of
        false ->
            current_dots(E, Keys, Tombstone, [{ID, Cnt} | Acc]);
        true ->
            current_dots(E, Keys, Tombstone, Acc)
    end;
current_dots(E, [{NE, _, _} | Keys], Tombstone, Acc) when NE < E ->
    current_dots(E, Keys, Tombstone, Acc);
current_dots(E, [{NE, _, _} | _Keys], _Tombstone, Acc) when NE > E ->
    Acc;
current_dots(_E, [], _Tombstone, Acc) ->
    Acc.

merge(#bigset{tombstone=TS1, clock=C1, keys=K1},
      #bigset{tombstone=TS2, clock=C2, keys=K2}) ->
    #bigset{tombstone=bigset_clock:merge(TS1, TS2),
            clock=bigset_clock:merge(C1, C2),
            keys=ordsets:union(K1, K2)}.

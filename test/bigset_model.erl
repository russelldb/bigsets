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
          causal=bigset_causal:fresh(),
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
    #bigset{causal=Causal, keys=Keys} = Bigset,
    {{ID, Cnt}, C2} = bigset_causal:increment(ID, Causal),
    Key = {Element, ID, Cnt},
    %% In the absence of a client context and AAAD, use the dots at
    %% the coordinating replica as the context of the add operation,
    %% any 'X' seen at this node will be removed by an add of an 'X'
    Ctx = current_dots(Element, Keys),
    C3 = bigset_causal:tombstone_dots(Ctx, C2),
    Keys2 = ordsets:add_element(Key, Keys),
    {{add, {Key, Ctx}}, Bigset#bigset{causal=C3, keys=Keys2}}.

-spec remove(element(), actor(), bigset()) ->
                    {delta(), bigset()}.
remove(Element, _ID,  Bigset) ->
    #bigset{causal=Clock, keys=Keys} = Bigset,
    %% In the absence of a client context and AAAD, use the dots at
    %% the coordinating replica as the context of the remove
    %% operation, any 'X' seen at this node will be removed by an
    %% remove of an 'X'
    Ctx = current_dots(Element, Keys),
    Clock2 = bigset_causal:tombstone_dots(Ctx, Clock),
    {{remove, Ctx}, Bigset#bigset{causal=Clock2}}.

-spec delta_join(delta(), bigset()) -> bigset().
delta_join({add, Delta}, Bigset) ->
    #bigset{causal=Clock, keys=Keys} = Bigset,
    {{_E, A, C}=Key, Ctx} = Delta,
    case bigset_causal:seen({A, C}, Clock) of
        true ->
            Bigset;
        false ->
            C2 = bigset_causal:add_dot({A, C}, Clock),
            C3 = bigset_causal:tombstone_dots(Ctx, C2),
            Bigset#bigset{causal=C3, keys=ordsets:add_element(Key, Keys)}
    end;
delta_join({remove, Ctx}, Bigset) ->
    #bigset{causal=Clock} = Bigset,
    C2 = bigset_causal:tombstone_dots(Ctx, Clock),
    Bigset#bigset{causal=C2}.

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
    ordsets:fold(fun({_E, A, C}=Key, BS=#bigset{causal=Causal, keys=Elements}) ->
                         case bigset_causal:is_tombstoned({A, C}, Causal) of
                             true ->
                                 BS#bigset{causal=bigset_causal:shrink_tombstone({A, C}, Causal),
                                           keys=ordsets:del_element(Key, Elements)};
                             false ->
                                 BS
                         end
                 end,
                 Bigset,
                 Bigset#bigset.keys).

%% @doc a vnode read of an individual vnode represented by `BS'
read(BS) ->
    #bigset{causal=Causal, keys=Keys} = BS,
    Clock = bigset_causal:clock(Causal),
    Keys2 = ordsets:fold(fun({E, A, C}, Acc0) ->
                                Dot = {A, C},
                                case bigset_causal:is_tombstoned({A, C}, Causal) of
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

%% @doc simulate handoff of keys from `From' to `To'. Returns `To'
%% after receiving handoff.  @TODO(rdb) find a way to use the handoff
%% receiver for this, ideally have the statem actually call and crash
%% and restart handoff
-spec handoff(From :: bigset(), To :: bigset()) -> NewTo :: bigset().
handoff(From, To) ->
    #bigset{keys=FromKeys, causal=FromCausal} = From,
    #bigset{causal=ToCausal, keys=ToKeys} = To,
    TrackingClock = bigset_clock:fresh(),

    %% This code takes care of the keys in From, either add them as
    %% unseen, or drop them as seen
    {ToCausal2, ToKeys2, TrackingClock2} = ordsets:fold(fun({_E, A, C}=K, {Causal, Keys, TC}=Acc) ->
                                                               Dot = {A, C},
                                                               case bigset_causal:is_tombstoned(Dot, FromCausal) of
                                                                   %% Not tombstoned so "send" it in handoff
                                                                   false ->
                                                                       TC2 = bigset_clock:add_dot(Dot, TC),
                                                                       case bigset_causal:seen(Dot, Causal) of
                                                                           true ->
                                                                               {Causal, Keys, TC2};
                                                                           false ->
                                                                               %% Not seen at receiver, store it
                                                                               {bigset_causal:add_dot(Dot, Causal),
                                                                                ordsets:add_element(K, Keys),
                                                                                TC2}
                                                                       end;
                                                                   true ->
                                                                       Acc
                                                               end
                                                       end,
                                                       {ToCausal, ToKeys, TrackingClock},
                                                       FromKeys),
    %% This section takes care of those keys that To has seen, but
    %% From has removed. i.e. keys that are not handed off, but their
    %% absence means something, and we don't want to read the whole
    %% bigset at To to find that out.

    %% The events in FromCausal.clock, not in TrackingClock2, that is
    %% all the dots that we were not handed by From, and therefore
    %% FromCausal.clock saw but has since removed
    FromClock = bigset_causal:clock(FromCausal),
    DeletedDots = bigset_clock:complement(FromClock, TrackingClock2),

    %% All the dots that we had seen **before** hand off, but which
    %% the handing off node has deleted
    ToClock = bigset_causal:clock(ToCausal),
    ToRemove = bigset_clock:intersection(DeletedDots, ToClock),

    %% This takes care of the keys that From has seen and removed but
    %% To has not (yet!) seen. Merging the clocks ensures that keys
    %% that To has not seen, but From has removed never get added to
    %% To.
    Causal0 = bigset_causal:merge_clocks(ToCausal2, FromCausal),
    To#bigset{causal=bigset_causal:tombstone_all(ToRemove, Causal0),
              keys=ToKeys2}.

%% @private just pull all the dots at this replica for element `E' as
%% the context. Simulates a per-elem-ctx for now
-spec current_dots(element(), [op_key()]) -> dotlist().
current_dots(E, Keys) ->
    current_dots(E, Keys, []).

-spec current_dots(element(), [op_key()], dotlist()) -> dotlist().
current_dots(E, [{E, ID, Cnt} | Keys], Acc) ->
    current_dots(E, Keys, [{ID, Cnt} | Acc]);
current_dots(E, [{NE, _, _} | Keys], Acc) when NE < E ->
    current_dots(E, Keys, Acc);
current_dots(E, [{NE, _, _} | _Keys], Acc) when NE > E ->
    Acc;
current_dots(_E, [], Acc) ->
    Acc.

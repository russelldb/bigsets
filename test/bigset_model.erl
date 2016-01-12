%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% just a way to play with the bigset simulation code at the console
%%% @end
%%% Created :  7 Dec 2015 by Russell Brown <russelldb@basho.com>

-module(bigset_model).

-compile([export_all]).

-export_type([bigset/0, delta/0, context/0]).

-define(CLOCK, bigset_clock).
-define(SET, set).

-define(ADD, add).
-define(REMOVE, remove).
-define(TOMBSTONE(E), {E, actor, counter, tsb}).

-record(bigset, {
          clock=?CLOCK:fresh(),
          keys=orddict:new() %% Simulate the back end with a K->V map
         }).

-type bigset() :: #bigset{}.
-type delta() :: op_key().
-type key() :: delta() | tombstone_key().
-type op_key() :: {Element :: term(),
                    Actor   :: term(),
                    Count   :: pos_integer(),
                               tsb()}.
-type tsb() :: add | remove.
-type tombstone_key() :: {Element :: term(),
                          actor, counter, tsb}.
-type context() :: bigset_clock:clock().

-type read_acc() :: [fold_acc_head() | orswot_elem()].

-type fold_acc_head() :: {Element :: term(),
                          dotlist(),
                          context()}.

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
    #bigset{clock=Clock} = Bigset,
    add(Element, ID, Bigset, Clock).

-spec add(term(), term(), bigset(), context()) ->
                 {delta(), bigset()}.
add(Element, ID, Bigset, Ctx) ->
    #bigset{clock=Clock, keys=Keys} = Bigset,
    {{ID, Cnt}, Clock2} = bigset_clock:increment(ID, Clock),
    Key = {Element, ID, Cnt, ?ADD},
    %% In the absence of a client context and AAAD, use the clock at
    %% the coordinating replica as the context of the add operation,
    %% any 'X' seen at this node will be removed by an add of an 'X'
    Val = Ctx,
    Keys2 = orddict:store(Key, Val, Keys),
    {{Key, Val}, #bigset{clock=Clock2, keys=Keys2}}.

-spec remove(term(), term(), bigset()) ->
                    {delta(), bigset()}.
remove(Element, ID, Bigset) ->
    #bigset{clock=Clock} = Bigset,
    remove(Element, ID, Bigset, Clock).

-spec remove(term(), term(), bigset(), context()) ->
                    {delta(), bigset()}.
remove(Element, ID, Bigset, Ctx) ->
    #bigset{clock=Clock, keys=Keys} = Bigset,
    {{ID, Cnt}, Clock2} = bigset_clock:increment(ID, Clock),
    Key = {Element, ID, Cnt, ?REMOVE},
    Val = Ctx,
    Keys2 = orddict:store(Key, Val, Keys),
    {{Key, Val}, #bigset{clock=Clock2, keys=Keys2}}.

-spec delta_join(delta(), bigset()) -> bigset().
delta_join(Delta, Bigset) ->
    #bigset{clock=Clock, keys=Keys} = Bigset,
    {{_E, A, C, _}=Key, Val} = Delta,
    case bigset_clock:seen(Clock, {A, C}) of
        true ->
            Bigset;
        false ->
            C2 = bigset_clock:strip_dots({A, C}, Clock),
            Bigset#bigset{clock=C2, keys=orddict:store(Key, Val, Keys)}
    end.

-spec value(merged_bigset() | bigset()) -> [term()].
value(#bigset{}=Bigset) ->
    orddict:fetch_keys(accumulate(Bigset));
value({_C, Keys}) ->
    orddict:fetch_keys(Keys).

-spec size(bigset()) -> non_neg_integer().
size(Bigset) ->
    orddict:size(Bigset#bigset.keys).

%% this is the algo that level will run.
%%
%% Any add of E with dot {A, C} can be removed if it is dominated by
%% the context of any other add of E. Or put another way, merge all
%% the contexts for E,(adds and removes) and remove all {A,C} Adds
%% that are dominated. NOTE: the contexts of removed adds must be
%% retained in a special per-element tombstone if the set clock does
%% not descend them.
%%
%% Tombstones:: write the fully merged context from the fold over E as
%% a per-element tombstone, remove all others. Remove the per-element
%% tombstone if it's value (Ctx) is descended by the set clock.

%% @NOTE(rdb) all this will have a profound effect on AAE/Read Repair,
%% so re-think it then. (Perhaps "filling" from the set clock in that
%% event?)

-record(compaction_acc, {current_element,
                         adds=[],
                         removes=[],
                         current_ctx=bigset_clock:fresh(),
                         acc=[]}).

-spec compact(bigset()) -> bigset().
compact(Bigset) ->
     %% fold_bigset(Bigset, fun compaction_flush_acc/2).
    #bigset{clock=Clock, keys=Keys} = Bigset,
    AccFinal = orddict:fold(fun({E, _, _, _}=K, V, Acc=#compaction_acc{current_element=E}) ->
                                                      %% same element
                                                      accumulate_key(K, V, Acc);
                                                 (K, V, Acc) ->
                                                      %% New element, consider the current
                                                      %% element, and start over
                                                      Acc2 = flush_acc(Acc, Clock),
                                                      accumulate_key(K, V, Acc2)
                                              end,
                                              #compaction_acc{},
                                              Keys),
    #compaction_acc{acc=Keys2} = flush_acc(AccFinal, Clock),
    #bigset{clock=Clock, keys=orddict:from_list(Keys2)}.

flush_acc(Acc, Clock) ->
    #compaction_acc{adds=Adds, removes=Rems, current_ctx=Ctx, acc=Keys} = Acc,
    {SurvivingAdds, SurvivingDots} = lists:foldl(fun({{_, A, C, _}=K, V}, {SA, SD}) ->
                                                         case add_survives({A, C}, V, Ctx, Clock) of
                                                             true ->
                                                                 {[{K, V} | SA],
                                                                  [{A, C} | SD]};
                                                             false ->
                                                                 {SA, SD}
                                                         end
                                                 end,
                                                 {[], []},
                                                 Adds),

    SurvivingRems = lists:foldl(fun({K, V}, SR) ->
                              case rem_survives(SurvivingDots, V, Clock) of
                                  true ->
                                      [{K, Ctx} | SR];
                                  false ->
                                      SR
                              end
                                end,
                                [],
                                Rems),
    #compaction_acc{acc= Keys ++ SurvivingAdds ++ SurvivingRems}.

%% if an Add's dot is covered by the aggregate element context &&
%% its payload ctx is descended by the clock, it can be removed,
%% otherwise need to keep it around (like a deferred operation)
add_survives(Dot, Payload, Ctx, Clock) ->
    %% This add is superceded
    not (bigset_clock:seen(Ctx, Dot) andalso
         %% All adds it supercedes have been seen already and will not
         %% resurface
         %% @TODO is there a chance that this k->removed but some k->v it removes is not?
         %% @TODO write up why I think this is safe (@see rem_survives)
         bigset_clock:descends(Clock, Payload)).

%% If a remove's context is descended by the bigset clock && no
%% adds it removes have remained, it can be safely removed,
%% otherwise, it too must remain, like a deferred operation
rem_survives(SurvivingDots, Payload, Clock) ->
    %% Tombstone stil needed (deferred)
    (bigset_clock:descends(Clock, Payload) == false) orelse
        %% Tombstone still needed (some add it removes is deferred)
        (bigset_clock:subtract_seen(Clock, SurvivingDots) /= SurvivingDots).

accumulate_key({E, _, _, ?ADD}=K, V, A) ->
    #compaction_acc{adds=Adds, current_ctx=Ctx} = A,
    Adds2 = [{K, V} | Adds],
    Ctx2 = bigset_clock:merge(Ctx, V),
    A#compaction_acc{adds=Adds2, current_ctx=Ctx2, current_element=E};
accumulate_key({E, _, _, ?REMOVE}=K, V, A) ->
    #compaction_acc{removes=Rems, current_ctx=Ctx} = A,
    Ctx2 = bigset_clock:merge(Ctx, V),
    Rems2 = [{K, V} | Rems],
    A#compaction_acc{removes=Rems2, current_ctx=Ctx2, current_element=E}.

-spec fold_bigset(bigset(), function()) -> bigset().
fold_bigset(#bigset{clock=Clock, keys=Keys}, FlushFun) ->
    Keys2 = orddict:fold(fun({E, A, C, ?ADD}, Ctx, [{E, Dots, CtxAcc} | Acc]) ->
                                 %% Still same element, accumulate keys and Ctx
                                 [{E,
                                   [{A, C} | Dots], %% accumulate dot
                                   bigset_clock:merge(Ctx, CtxAcc) %% merge in ctx
                                  } | Acc];
                           ({E, _A, _C, ?REMOVE}, Ctx, [{E, Dots, CtxAcc} | Acc]) ->
                                 %% Tombstone, we drop all tombstone
                                 %% keys except a per-element key, just merge Ctx
                                 [{E,
                                   Dots,
                                   bigset_clock:merge(Ctx, CtxAcc)
                                  } | Acc];
                            (Key, Ctx, Acc) ->
                                 Acc2 = FlushFun(Acc, Clock),
                                 Hd = new_acc(Key, Ctx),
                                 [Hd | Acc2]
                         end,
                         [],
                        Keys),
    Keys3 = orddict:from_list(FlushFun(Keys2, Clock)),
    #bigset{clock=Clock, keys= Keys3}.

%% start over the per-element portition of the accumulator
-spec new_acc(key(), context()) ->
                     fold_acc_head().
new_acc({E, A, C, ?ADD}, Ctx) ->
    {E, [{A, C}], Ctx};
new_acc(K, Ctx) ->
    {element(1, K), [], Ctx}.

%% @private the vnode fold operation. Similar to compact but returns
%% an ordered list of Element->Dots mappings only, no tombstones or
%% clock.
-spec accumulate(bigset()) -> [orswot_elem()].
accumulate(BS) ->
    #bigset{keys=Keys} = fold_bigset(BS, fun accumulate_flush_acc/2),
    Keys.

%% Used to flush the per element accumulator to the accumulated set of
%% keys. In this case, only elements with survivng dots are
%% accumulated, one entry per element, with a value that is the
%% surviving dots set. This creates a structure like a traditional
%% orswot.
-spec accumulate_flush_acc(read_acc(), context()) -> read_acc().
accumulate_flush_acc([], _Clock) ->
    [];
accumulate_flush_acc([{Element, Dots, Ctx} | Acc], _Clock) ->
    %% The read/fold acc just needs a set of E->Dot mappings for
    %% elements that are _in_ at this replica
    Remaining = bigset_clock:subtract_seen(Ctx, Dots),
    case Remaining of
        [] ->
            Acc;
        RemDots ->
            [{Element, RemDots} | Acc]
    end.

read(BS) ->
    #bigset{clock=Clock} = BS,
    Keys = accumulate(BS),
    {Clock, Keys}.

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

%% Full state replication merge of two bigsets.
-spec merge(bigset(), bigset()) -> bigset().
merge(#bigset{clock=C1, keys=Set1}, #bigset{clock=C2, keys=Set2}) ->
    Clock = bigset_clock:merge(C1, C2),
    {Set2Unique, Keep} = orddict:fold(fun({_E, A, C, _TSB}=Key, Ctx, {RHSU, Acc}) ->
                                              case orddict:find(Key, Set2) of
                                                  {ok, LHSCtx} ->
                                                      %% In both, keep
                                                      %% What is happening with contexts here, shouldn't they be the same?
                                                      %% Well, if one side has compacted the context is replaced with an
                                                      %% empty context and a tombstone, so no, they may not be the same.
                                                      %% Not sure how much this matters in the _real_ bigset world,
                                                      %% since there is no full state merge
                                                      {orddict:erase(Key, RHSU),
                                                       orddict:store(Key, bigset_clock:merge(Ctx, LHSCtx), Acc)};
                                                  error ->
                                                      %% Set 1 only, did set 2 remove it?
                                                      case bigset_clock:seen(C2, {A, C}) of
                                                          true ->
                                                              %% removed
                                                              {RHSU, Acc};
                                                          false ->
                                                              %% unseen by set 2
                                                              {RHSU, orddict:store(Key, Ctx, Acc)}
                                                      end
                                              end
                                      end,
                                      {Set2, []},
                                      Set1),
    %% Do it again on set 2
    InSet =  orddict:fold(fun({_E, A, C, _TSB}=Key, Ctx, Acc) ->
                                  %% Set 2 only, did set 1 remove it?
                                  case bigset_clock:seen(C1, {A, C}) of
                                      true ->
                                          %% removed
                                          Acc;
                                      false ->
                                          %% unseen by set 1
                                          orddict:store(Key, Ctx, Acc)
                                  end;
                             (TSKey, TSCtx, Acc) ->
                                  orddict:store(TSKey, TSCtx, Acc)
                          end,
                          Keep,
                          Set2Unique),

    #bigset{clock=Clock, keys=InSet}.

%% @doc simulate handoff of keys from `From' to `To'. Returns `To'
%% after receiving handoff.
-spec handoff(From :: bigset(), To :: bigset()) -> NewTo :: bigset().
handoff(From, To) ->
    #bigset{keys=FromKeys, clock=FromClock} = From,
    #bigset{clock=ToClock, keys=ToKeys} = To,
    TrackingClock = bigset_clock:fresh(),

    %% This code takes care of the keys in From, either add them as
    %% unseen, or drop them as seen
    {ToClock2, ToKeys2, TrackingClock2} = orddict:fold(fun({_E, A, C, _}=K, V, {Clock, Keys, TC}) ->
                                                               TC2 = bigset_clock:strip_dots({A, C}, TC),
                                                               case bigset_clock:seen(Clock, {A, C}) of
                                                                   true ->{Clock, Keys, TC2};
                                                                   false ->
                                                                       {bigset_clock:strip_dots({A, C}, Clock),
                                                                        orddict:store(K, V, Keys),
                                                                        TC2}
                                                               end
                                                       end,
                                                       {ToClock, ToKeys, TrackingClock},
                                                       FromKeys),
    %% This section takes care of those keys that To has seen, but
    %% From has removed. i.e. keys that are not handed off, but their
    %% absence means something, and we don't want to read the whole
    %% bigset at To to find that out.

    %% The events in FromClock, not in TrackingClock2, that is all the
    %% dots that we were not handed by From, and therefore FromClock
    %% saw but has since removed
    DeletedDots = bigset_clock:complement(FromClock, TrackingClock2),

    %% All the dots that we had seen before hand off, but which the
    %% handing off node has deleted
    ToRemove = bigset_clock:intersection(DeletedDots, ToClock),

    ToKeys3 = orddict:filter(fun({_E, A, C, _}, _V) ->
                                     not bigset_clock:seen(ToRemove, {A, C}) end,
                             ToKeys2),

    %% This takes care of the keys that From as seen and removed but
    %% To has not (yet?!) seen. Merging the clocks ensures that keys
    %% that To has not seen, but From has removed never get added to
    %% To.
    To#bigset{clock=bigset_clock:merge(ToClock2, FromClock), keys=ToKeys3}.

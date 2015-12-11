%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% just a way to play with the bigset simulation code at the console
%%% @end
%%% Created :  7 Dec 2015 by Russell Brown <russelldb@basho.com>

-module(bigset_model).

-compile([export_all]).

-define(CLOCK, bigset_clock).
-define(SET, set).

-define(ADD, add).
-define(REMOVE, remove).

-record(bigset, {
          clock=?CLOCK:fresh(),
          keys=orddict:new() %% Simulate the back end with a K->V map
         }).

-type bigset() :: #bigset{}.

new() ->
    #bigset{}.

add(Element, ID, Bigset) ->
    #bigset{clock=Clock} = Bigset,
    add(Element, ID, Bigset, Clock).

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

remove(Element, ID, Bigset) ->
    #bigset{clock=Clock} = Bigset,
    remove(Element, ID, Bigset, Clock).

remove(Element, ID, Bigset, Ctx) ->
    #bigset{clock=Clock, keys=Keys} = Bigset,
    {{ID, Cnt}, Clock2} = bigset_clock:increment(ID, Clock),
    Key = {Element, ID, Cnt, ?REMOVE},
    Val = Ctx,
    Keys2 = orddict:store(Key, Val, Keys),
    {{Key, Val}, #bigset{clock=Clock2, keys=Keys2}}.

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

value(Bigset) ->
    orddict:fetch_keys(accumulate(Bigset)).

size(Bigset) ->
    orddict:size(Bigset#bigset.keys).

compact(Bigset) ->
     fold_bigset(Bigset, fun compaction_flush_acc/2).

-spec fold_bigset(bigset(), function()) -> {bigset(), integer()}.
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
                            ({E, tombstone}, Ctx, [{E, Dots, CtxAcc} | Acc]) ->
                                 %% The per element tombstone, just the ctx needed
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
new_acc({E, A, C, ?ADD}, Ctx) ->
    {E, [{A, C}], Ctx};
new_acc(K, Ctx) ->
    {element(1, K), [], Ctx}.

compaction_flush_acc([], _Clock) ->
    [];
compaction_flush_acc([{Element, Dots, Ctx} | Acc], Clock) ->
    %% - Subtract dots from Ctx - Each remaining dot is a key that
    %% "survived" the compaction - If no keys survived and the Clock
    %% >= Ctx, no tombstone - otherwise keep the Ctx as tombstone with
    %% special key so yet to be seen writes that are superceded by
    %% writes we've removed get removed
    Remaining = bigset_clock:subtract_seen(Ctx, Dots),
    Tombstone = tombstone(Element, Remaining, Ctx, Clock),
    %% @TODO(rdb) do keys need their original contexts now that the
    %% tombstone has them? NO!!!
    [ {{Element, A, C, ?ADD}, bigset_clock:fresh()} || {A, C} <- Remaining]
        ++ Tombstone ++ Acc.

tombstone(E, _R, Ctx, Clock) ->
    %% Even though the remaining elements have been stripped of their
    %% contexts, it is safe to remove a tombstone that is dominated by
    %% the set clock. The set clock will not write any element it has
    %% seen already. In effect we are moving the tombstone up to the
    %% head of the set once it has no use i.e. there are no deferred
    %% operations.
    case bigset_clock:descends(Clock, Ctx) of
        true ->
            %% Discard tombstone, clock has seen it all, so anything
            %% it tombstones will not be written again
            [];
        false  ->
            [{{E, tombstone}, Ctx}]
    end.

%% @private the vnode fold operation. Similar to compact but returns
%% an ordered list of Element->Dots mappings only, no tombstones or
%% clock.
-spec accumulate(bigset()) -> [{Element::term(),
                                Dots::[{Actor::term(),
                                        Cnt::pos_integer()}]
                               }].
accumulate(BS) ->
    #bigset{keys=Keys} = fold_bigset(BS, fun accumulate_flush_acc/2),
    Keys.

%% Used to flush the per element accumulator to the accumulated set of
%% keys. In this case, only elements with survivng dots are
%% accumulated, one entry per element, with a value that is the
%% surviving dots set. This creates a structure like a traditional
%% orswot.
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

%% @TODO orswot style merge, so, you know, ugly
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
                                                      {orddict:erase(Key, RHSU), orddict:store(Key, bigset_clock:merge(Ctx, LHSCtx), Acc)};
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
                                              end;
                                         ({_E, tombstone}=Key, Ctx, {RHSU, Acc}) ->
                                              %% @TODO(rdb) what do we
                                              %% do here?  Always
                                              %% store? Only store if
                                              %% Ctx is unseen? What
                                              %% about if RHSU has a
                                              %% tombstone too?
                                              TS2 = fetch_tombstone(Key, RHSU),
                                              MergedCtx = bigset_clock:merge(Ctx, TS2),
                                              {orddict:erase(Key, RHSU), orddict:store(Key, MergedCtx, Acc)}

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

-spec fetch_tombstone(term(), orddict:orddict()) -> bigset_clock:clock().
%% return either the tombstone or a fresh clock to merge with
fetch_tombstone(Key, Dict) ->
    case orddict:find(Key, Dict) of
        {ok, TS} ->
            TS;
        error ->
            bigset_clock:fresh()
    end.

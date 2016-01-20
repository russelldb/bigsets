%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% Reading a bigset is a fold across the clock and all the elements
%%% for the set.
%%% @end
%%% Created :  8 Oct 2015 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(bigset_fold_acc).

-compile([export_all]).

-include("bigset_trace.hrl").
-include("bigset.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(EMPTY, []).

-record(fold_acc,
        {
          not_found = true,
          partition :: pos_integer(),
          actor :: binary(),
          set :: binary(),
          key_prefix :: binary(),
          prefix_len :: pos_integer(),
          sender :: riak_core:sender(),
          buffer_size :: pos_integer(),
          size=0 :: pos_integer(),
          monitor :: reference(),
          me = self() :: pid(),
          current_elem :: binary(),
          current_dots = ?EMPTY :: [bigset_clock:dot()],
          current_ctx = bigset_clock:fresh() :: bigset_clock:clock(),
          hoff_filter = bigset_clock:fresh() :: bigset_clock:clock(),
          elements = ?EMPTY
        }).

send(Message, Acc) ->
    #fold_acc{sender=Sender, me=Me, monitor=Mon, partition=Partition} = Acc,
    riak_core_vnode:reply(Sender, {Message, Partition, {Me, Mon}}).

new(Set, Sender, BufferSize, Partition, Actor) ->
    Monitor = riak_core_vnode:monitor(Sender),
    Prefix = bigset:key_prefix(Set),
    #fold_acc{
       set=Set,
       key_prefix=Prefix,
       prefix_len= byte_size(Prefix),
       sender=Sender,
       monitor=Monitor,
       buffer_size=BufferSize-1,
       partition=Partition,
       actor = Actor}.

%% @doc called by eleveldb:fold per key read. Uses `throw({break,
%% Acc})' to break out of fold when last key is read.
fold({Key, Value}, Acc=#fold_acc{not_found=true}) ->
    %% The first call for this acc, (not_found=true!)
    #fold_acc{set=Set, actor=Actor} = Acc,
    %% @TODO(rdb|robustness) what if the clock key is missing and
    %% first_key finds something else from *this* Set?
    case bigset:decode_key(Key) of
        {clock, Set, Actor} ->
            %% Set clock, send at once
            Clock = bigset:from_bin(Value),
            send({clock, Clock}, Acc),
            Acc#fold_acc{not_found=false};
        _ ->
            throw({break, Acc})
    end;
fold({Key, Val}, Acc=#fold_acc{not_found=false}) ->
    %% an element key? We've sent the clock (not_found=false)
    #fold_acc{set=Set, key_prefix=Pref, prefix_len=PrefLen, actor=Me} = Acc,
    case Key of
        <<Pref:PrefLen/binary, $c, _Rest/binary>> ->
            %% a clock for another actor, skip it
            Acc;
        <<Pref:PrefLen/binary, $d, Me/binary>> ->
            %% My hoff filter!
            Acc#fold_acc{hoff_filter=bigset:from_bin(Val)};
        <<Pref:PrefLen/binary, $d, _NotMe/binary>> ->
            %% Some other actors hoff_filter
            Acc;
        <<Pref:PrefLen/binary, $e, Rest/binary>> ->
            {element, Set, Element, Actor, Cnt, TSB} = bigset:decode_element(Rest, Set),
            Ctx = bigset:from_bin(Val),
            #fold_acc{hoff_filter=HoffFilter} = Acc,
            case bigset_clock:seen(HoffFilter, {Acc, Cnt}) of
                false ->
                    add(Element, Actor, Cnt, TSB, Ctx, Acc);
                true ->
                    %% a handing off vnode deleted this key, so it is
                    %% as though we don't have it, just skip it, it
                    %% will be compacted out next round
                    Acc
            end;
        _ ->
            %% The end key
            throw({break, Acc})
    end.

%% @private For each element we most fold over all entries for that
%% element. Entries for an element can be an add, or a remove (and if
%% we ever get compaction working there maybe a special aggregrated
%% tombstone too.) We merge all the Contexts of the entries (adds and
%% removes) to get a context that represents all the "seen" additions
%% for the element. We use the ctx to filter the dots in the Add
%% entries. Each add entry that is not seen by the aggregaeted context
%% "survives" and is in the local set.

%% NOTE: for causal consistency with
%% Action-At-A-Distance this all changes

%% @TODO(rdb|refactor) abstract the accumulation logic for different
%% behaviours (CC vs EC)
add(Element, Actor, Cnt, TSB, Ctx, Acc=#fold_acc{current_elem=Element}) ->
    %% Same element, keep accumulating info
    #fold_acc{current_ctx=CC, current_dots=Dots} = Acc,
    AggCtx = bigset_clock:merge(Ctx, CC),
    AggDots = maybe_add_dots({Actor, Cnt}, Dots, TSB),
    Acc#fold_acc{current_ctx=AggCtx, current_dots=AggDots};
add(Element, Actor, Cnt, TSB, Ctx, Acc=#fold_acc{current_elem=_}) ->
    %% New element, maybe store the old one
    Acc2 = store_element(Acc),
    Acc3 = maybe_flush(Acc2),
    Acc3#fold_acc{current_elem=Element,
                  current_dots=maybe_add_dots({Actor, Cnt}, [], TSB),
                  current_ctx=Ctx}.

%% Only the ?ADD dots get added to the element's list of dots
maybe_add_dots(_Dot, Dots, ?REM) ->
    Dots;
maybe_add_dots(Dot, Dots, ?ADD) ->
    [Dot | Dots].

%% @private add an element to the accumulator.
store_element(Acc=#fold_acc{current_elem=undefined}) ->
    Acc;
store_element(Acc) ->
    #fold_acc{current_elem=Elem,
              current_dots=Dots,
              current_ctx=Ctx,
              elements=Elements,
              size=Size} = Acc,

    %% create a regular orswot from the bigset on disk
    Remaining = bigset_clock:subtract_seen(Ctx, Dots),
    case Remaining of
        [] ->
            Acc;
        _ ->
            Acc#fold_acc{elements=[{Elem, Remaining} | Elements],
                         size=Size+1}
    end.

%% @private if the buffer is full, flush!
maybe_flush(Acc=#fold_acc{size=Size, buffer_size=Size}) ->
    flush(Acc);
maybe_flush(Acc) ->
    Acc.

%% @private send the accumulated elements
flush(Acc) ->
    %% send the message, but only if our last message was acked, or
    %% the reciever is still there!

    %% @TODO bikeshed this. Should we 1. not continue folding until
    %% last message is acked (saves cluster resources maybe) or
    %% 2. continue fold, but do not send result until message is
    %% acked, seems to me this gives us time to fold while message is
    %% in flight, read, acknowledged, but still allows backpressure)
    #fold_acc{elements=Elements, monitor=Monitor, partition=Partition} = Acc,

    Res = receive
              {Monitor, ok} ->
                  %% Results needed sorted
                  send({elements, lists:reverse(Elements)}, Acc),
                  Acc#fold_acc{size=0, elements=?EMPTY};
              {Monitor, stop_fold} ->
                  lager:debug("told to stop~p~n", [Partition]),
                  close(Acc),
                  throw(stop_fold);
              {'DOWN', Monitor, process, _Pid, _Reson} ->
                  lager:debug("got down~p~n", [Partition]),
                  close(Acc),
                  throw(receiver_down)
          end,
    Res.

%% @private folding is over (if it ever really began!), call this with
%% the final Accumulator.
finalise(Acc=#fold_acc{not_found=true}) ->
    send(not_found, Acc),
    done(Acc);
finalise(Acc) ->
    AccFinal = store_element(Acc),
    done(AccFinal).

%% @private let the caller know we're done.
done(Acc0) ->
    Acc = flush(Acc0),
    send(done, Acc),
    close(Acc).

%% @private demonitor
close(Acc) ->
    #fold_acc{monitor=Monitor} = Acc,
    erlang:demonitor(Monitor, [flush]).

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

-export_type([buffer/0]).

-include("bigset_trace.hrl").
-include("bigset.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(EMPTY, []).

-record(fold_acc,
        {
          clock :: bigset_clock:clock(),
          clock_sent = false :: boolean(),
          not_found = true,
          partition :: pos_integer(),
          actor :: binary(),
          set :: binary(),
          sender :: riak_core:sender(),
          buffer_size :: pos_integer(),
          size=0 :: pos_integer(),
          monitor :: reference(),
          me = self() :: pid(),
          current_elem :: binary(),
          current_dots = ?EMPTY :: [bigset_clock:dot()],
          set_tombstone = bigset_clock:fresh() :: bigset_clock:clock(),
          elements = ?EMPTY,
          %% let's hope this is consistent across repliacs, eh? Or
          %% we'll be read_repair deleting some things
          start_element,
          end_element,
          start_inclusive=true :: boolean(),
          end_inclusive=true :: boolean()
        }).

-type buffer() :: #fold_acc{}.

%% @doc if the query does not fold over the metdata (e.g. range query
%% that starts after the clock) then add it to the buffer.  Takes the
%% `Buffer' and returns it with `Clock' and `Tombstone' added.
-spec add_metadata(buffer(), bigset_clock:clock(), bigset_clock:clock()) -> buffer().
add_metadata(Buffer=#fold_acc{}, Clock, Tombstone) ->
    Buffer#fold_acc{set_tombstone=Tombstone, clock=Clock, not_found=false}.

%% @doc since eleveldb is yet to support start_inclusive option, add
%% it to the accumulator state and use it in the fold
-spec set_range_start(buffer(), boolean(), binary() | undefined) -> buffer().
set_range_start(Buffer=#fold_acc{}, StartInclusive, StartElement)
  when is_boolean(StartInclusive) ->
    Buffer#fold_acc{start_inclusive=StartInclusive, start_element=StartElement}.

%% @doc since eleveldb is yet to support end_inclusive option, add it
%% to the accumulator state and use it in the fold
-spec set_range_end(buffer(), boolean(), binary() | undefined) -> buffer().
set_range_end(Buffer=#fold_acc{}, EndInclusive, EndElement)
  when is_boolean(EndInclusive) ->
    Buffer#fold_acc{end_inclusive=EndInclusive, end_element=EndElement}.

send(Message, Acc) ->
    #fold_acc{sender=Sender, me=Me, monitor=Mon, partition=Partition} = Acc,
    riak_core_vnode:reply(Sender, {Message, Partition, {Me, Mon}}).

new(Set, Sender, BufferSize, Partition, Actor) ->
    Monitor = riak_core_vnode:monitor(Sender),
    #fold_acc{
       set=Set,
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
    case bigset_keys:is_actor_clock_key(Set, Actor, Key) of
        true ->
            %% Set clock
            Clock = bigset:from_bin(Value),
            Acc#fold_acc{not_found=false, clock=Clock};
        _ ->
            throw({break, Acc})
    end;
fold({Key, Val}, Acc=#fold_acc{not_found=false}) ->
    %% an element key? We've seen the clock (not_found=false)
    #fold_acc{set=Set, actor=Me} = Acc,
    case bigset_keys:decode_key(Key) of
        {tombstone, Set, Me} ->
            %% My set tombstone, I need this!
            Acc#fold_acc{set_tombstone=bigset:from_bin(Val)};
        {element, Set, Element, Actor, Cnt} ->
            #fold_acc{set_tombstone=SetTombstone} = Acc,
            TsSeen = bigset_clock:seen({Actor, Cnt}, SetTombstone),
            maybe_add_element(TsSeen, Element, Actor, Cnt, Acc);
        {_, Set, _} ->
            %% other actor clock/tombstone key
            Acc;
        {end_key, _Set} ->
            %% The end key
            throw({break, Acc})
    end.

%% @priv decide if we should accumulate this element. Don't accumulate
%% if:
%% 1. it's been deleted (but not compacted.)
%% 2. it's the start or end of a range query and not included
maybe_add_element(_TsSeen=true, _Element, _Actor, _Cnt, Acc) ->
    %% a handing off vnode deleted this key, so it is
    %% as though we don't have it, just skip it, it
    %% will be compacted out one day, maybe.
    Acc;
maybe_add_element(_TsSeen, Element, _Actor, _Cnt, Acc=#fold_acc{start_element=Element, start_inclusive=false}) ->
    %% element matches start element and client said they don't want
    %% the start of the range
    Acc;
maybe_add_element(_TsSeen, Element, _Actor, _Cnt, Acc= #fold_acc{end_element=Element, end_inclusive=false}) ->
    %% element matches end of range and the client says they don't
    %% want it
    Acc;
maybe_add_element(_TsSeen, Element, Actor, Cnt, Acc) ->
    add(Element, Actor, Cnt, Acc).

%% @TODO(rdb|refactor) abstract the accumulation logic for different
%% behaviours (CC vs EC)
add(Element, Actor, Cnt, Acc=#fold_acc{current_elem=Element}) ->
    %% Same element, keep accumulating info
    #fold_acc{current_dots=Dots} = Acc,
    %% @TODO(rdb) consider binary <<Cnt, Actor>> as it needs no encoding
    Acc#fold_acc{current_dots=[{Actor, Cnt} | Dots]};
add(Element, Actor, Cnt, Acc=#fold_acc{current_elem=_}) ->
    %% New element, maybe store the old one
    Acc2 = store_element(Acc),
    Acc3 = maybe_flush(Acc2),
    Acc3#fold_acc{current_elem=Element,
                  current_dots=[{Actor, Cnt}]}.

%% @private add an element to the accumulator.
store_element(Acc=#fold_acc{current_elem=undefined}) ->
    Acc;
store_element(Acc) ->
    #fold_acc{current_elem=Elem,
              current_dots=Dots,
              elements=Elements,
              size=Size} = Acc,

    Acc#fold_acc{elements=[{Elem, Dots} | Elements],
                 size=Size+1}.

%% @private if the buffer is full, flush!
maybe_flush(Acc=#fold_acc{size=Size, buffer_size=Size}) ->
    flush(Acc);
maybe_flush(Acc) ->
    Acc.


flush(Acc) ->
    flush(Acc, not_done).

%% @private send the accumulated elements
flush(Acc, Done) ->
    %% send the message, but only if our last message was acked, or
    %% the reciever is still there!

    %% @TODO bikeshed this. Should we 1. not continue folding until
    %% last message is acked (saves cluster resources maybe) or
    %% 2. continue fold, but do not send result until message is
    %% acked, seems to me this gives us time to fold while message is
    %% in flight, read, acknowledged, but still allows backpressure)
    #fold_acc{elements=Elements, monitor=Monitor,
              clock=Clock, clock_sent=ClockSent} = Acc,

    Message = message(ClockSent, Clock, Elements, Done),

    Res = case ClockSent of
              true ->
                  receive
                      {Monitor, ok} ->
                          send(Message, Acc),
                          Acc#fold_acc{size=0, elements=?EMPTY};
                      {Monitor, stop_fold} ->
                          close(Acc),
                          throw(stop_fold);
                      {'DOWN', Monitor, process, _Pid, _Reson} ->
                          close(Acc),
                          throw(receiver_down)
                  end;
              false ->
                  send(Message, Acc),
                  Acc#fold_acc{size=0, elements=?EMPTY, clock_sent=true}
          end,
    Res.

message(ClockSent, Clock, Elements, Done) ->
    done_message(Done,
                 element_message(Elements,
                                 clock_message(ClockSent, Clock, ?READ_RESULT{})
                                )
                ).

clock_message(false, Clock, Message) ->
    Message?READ_RESULT{clock=Clock};
clock_message(true, _Clock, Message) ->
    Message.

element_message(Elements, Message) ->
    %% Results needed sorted
    Message?READ_RESULT{elements=lists:reverse(Elements)}.

done_message(done, Message) ->
    Message?READ_RESULT{done=true};
done_message(_NotDone, Message) ->
    Message.

%% @private folding is over (if it ever really began!), call this with
%% the final Accumulator.
finalise(Acc=#fold_acc{not_found=true}) ->
    send(?READ_RESULT{not_found=true}, Acc),
    close(Acc);
finalise(Acc0) ->
    Acc = store_element(Acc0),
    AccFinal = flush(Acc, done),
    close(AccFinal).

%% @private demonitor
close(Acc) ->
    #fold_acc{monitor=Monitor} = Acc,
    erlang:demonitor(Monitor, [flush]).

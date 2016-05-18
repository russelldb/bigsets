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
          set_tombstone = bigset_clock:fresh() :: bigset_clock:clock(),
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
            Causal = bigset:from_bin(Value),
            Clock = bigset_causal:clock(Causal),
            Tombstone = bigset_causal:tombstone(Causal),
            send({clock, Clock}, Acc),
            Acc#fold_acc{not_found=false, set_tombstone=Tombstone};
        _ ->
            throw({break, Acc})
    end;
fold({Key, Val}, Acc=#fold_acc{not_found=false}) ->
    %% an element key? We've sent the clock (not_found=false)
    #fold_acc{elements=E, size=S} = Acc,
    maybe_flush(Acc#fold_acc{elements=[{Key, binary_to_term(Val)} | E],
                             size=S+1}).

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
    done(Acc).

%% @private let the caller know we're done.
done(Acc0) ->
    Acc = flush(Acc0),
    send(done, Acc),
    close(Acc).

%% @private demonitor
close(Acc) ->
    #fold_acc{monitor=Monitor} = Acc,
    erlang:demonitor(Monitor, [flush]).

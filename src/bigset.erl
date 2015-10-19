-module(bigset).

-include("bigset.hrl").

-compile([export_all]).

preflist(Set) ->
    Hash = riak_core_util:chash_key({bigset, Set}),
    riak_core_apl:get_apl(Hash, 3, bigset).

dev_client() ->
    make_client('bigset1@127.0.0.1').

make_client(Node) ->
    bigset_client:new(Node).

add_read() ->
    add_read(<<"rdb">>).

add_read(E) ->
    add_read(<<"m">>, E).

add_read(S, E) ->
    lager:debug("Adding to set~n"),
    ok = bigset_client:update(S, [E]),
    lager:debug("reading from set~n"),
    Res = bigset_client:read(S, []),
    lager:debug("Read result ~p~n", [Res]).

add_all(Es) ->
    add_all(<<"m">>, Es).

add_all(S, Es) ->
    ok = bigset_client:update(S, Es),
    lager:debug("reading from set~n"),
    Res = bigset_client:read(S, []),
    lager:debug("Read result ~p~n", [Res]).

make_bigset(Set, N) ->
    Words = read_words(N*2),
    Limit = length(Words),
    Res = [bigset_client:update(Set, [lists:nth(crypto:rand_uniform(1, Limit), Words)]) ||
              _N <- lists:seq(1, N)],
    case lists:all(fun(E) ->
                           E == ok end,
                   Res) of
        true ->
            ok;
        false ->
            some_errors
    end.

-define(BATCH_SIZE, 1000).

make_set(Set, N) when N < ?BATCH_SIZE ->
    Batch = [crypto:rand_bytes(100) || _N <- lists:seq(1, N)],
    ok = bigset_client:update(Set, Batch);
make_set(Set, N)  ->
    Batches = if N < ?BATCH_SIZE  -> 1;
                 true-> N div ?BATCH_SIZE
              end,
    make_batch(Set, Batches).

make_batch(_Set, 0) ->
    ok;
make_batch(Set, N) ->
    make_batch(Set),
    make_batch(Set, N-1).

make_batch(Set) ->
    Batch = [crypto:rand_bytes(100) || _N <- lists:seq(1, ?BATCH_SIZE)],
    ok = bigset_client:update(Set, Batch).

add() ->
    add(<<"rdb">>).

add(E) ->
    add(<<"m">>, E).

add(S, E) ->
    lager:debug("Adding to set~n"),
    ok = bigset_client:update(S, [E]).

stream_read() ->
    stream_read(<<"m">>).

stream_read(S) ->
    stream_read(S, bigset_client:new()).

stream_read(S, Client) ->
    lager:debug("stream reading from set~n"),
    {ok, ReqId, Pid} = bigset_client:stream_read(S, [], Client),
    Monitor = erlang:monitor(process, Pid),
    stream_receive_loop(ReqId, Pid, Monitor, {0, undefined}).

stream_receive_loop(ReqId, Pid, Monitor, {Cnt, Ctx}) ->
    receive
        {ReqId, done} ->
            erlang:demonitor(Monitor, [flush]),
            lager:debug("done!.~n"),
            {ok, Ctx, Cnt};
        {ReqId, {error, Error}} ->
            erlang:demonitor(Monitor, [flush]),
            lager:debug("error ~p~n", [Error]),
            {error, Error};
        {ReqId, {ok, {ctx, Res}}} ->
            lager:debug("XX CTX XX:::~n ~p~n", [Res]),
            stream_receive_loop(ReqId, Pid, Monitor, {Cnt, Res});
        {ReqId, {ok, {elems, Res}}} ->
            lager:debug("XX RESULT XX:::~n ~p~n", [length(Res)]),
            stream_receive_loop(ReqId, Pid, Monitor, {Cnt+length(Res), Ctx})
     %%% @TODO(rdb|wut?) why does this message get fired first for remote node?
        %% {'DOWN', Monitor, process, Pid, Info} ->
        %%     lager:debug("Got DOWN message ~p~n", [Info]),
        %%     {error, down, Info}
    after 10000 ->
            erlang:demonitor(Monitor, [flush]),
            lager:debug("Error, timeout~n"),
            {error, timeout}
    end.

bm_read(Set, N) ->
    Times = [begin
                 {Time, _} = timer:tc(bigset_client, read, [Set, []]),
                 Time
             end || _ <- lists:seq(1, N)],
    [{max, lists:max(Times)},
     {min, lists:min(Times)},
     {avg, lists:sum(Times) div length(Times)}].

%% Key prefix is the common prefix of a key for the given set
-spec key_prefix(Set :: binary()) -> Prefix :: binary().
key_prefix(Set) when is_binary(Set) ->
    SetLen = byte_size(Set),
    <<SetLen:32/little-unsigned-integer,
      Set:SetLen/binary>>.

%%% codec See docs on key scheme, use Actor name in clock key so
%% AAE/replication of clocks is safe. Like a decomposed VV, an actor
%% may only update it's own clock.
clock_key(Set, Actor) ->
    %% @TODO(rbd|optimise) This could just be <<SetLen, Set, Actor>>
    %% with maybe a single byte 0 | 1 for clock | element key!
    Pref = key_prefix(Set),
    ActorLen = byte_size(Actor),
    <<Pref/binary,
      0:32/little-unsigned-integer, %% Means a clock, no element!
      ActorLen:32/little-unsigned-integer, %% Not needed!
      Actor:ActorLen/binary>>.

%% @doc return the {Set, Actor} for a clock key
-spec decode_key(Key :: binary()) -> {clock, set(), actor()} |
                                     {element, set(), member(), actor(), counter(), tsb()}.
decode_key(<<SetLen:32/little-unsigned-integer, Rest/binary>>) ->
    <<Set:SetLen/binary, ElemLen:32/little-unsigned-integer, Bin/binary>> = Rest,
    if ElemLen == 0 ->
            <<_AL:32/little-unsigned-integer, Actor/binary>> = Bin,
            {clock, Set, Actor};
       true ->
            <<Elem:ElemLen/binary,
              ActorLen:32/little-unsigned-integer,
              ActorEtc/binary>> = Bin,
            <<Actor:ActorLen/binary,
              Cnt:64/little-unsigned-integer,
              TSB:32/little-unsigned-integer>> = ActorEtc,
            {element, Set, Elem, Actor, Cnt, TSB}
    end.

%% @private sext encodes the element key so it is in order, on disk,
%% with the other elements. Use the actor ID and counter (dot)
%% too. This means at some extra storage, but makes for no reads
%% before writes on replication/delta merge. See read for how the
%% leveldb merge magic will work. Essentially every key {s, Set, E, A,
%% Cnt, 0} that has some key {s, Set, E, A, Cnt', 0} where Cnt' > Cnt
%% can be removed in compaction, as can every key {s, Set, E, A, Cnt,
%% 0} which has some key {s, Set, E, A, Cnt', 1} whenre Cnt' >=
%% Cnt. As can every key {s, Set, E, A, Cnt, 1} where the VV portion
%% of the set clock >= {A, Cnt} @TODO(rdb) document how this tombstone
%% reaping works! Crazy!!
-spec insert_member_key(set(), member(), actor(), counter()) -> key().
insert_member_key(Set, Elem, Actor, Cnt) ->
    Pref = key_prefix(Set),
    ActorLen = byte_size(Actor),
    ElemLen = byte_size(Elem),
    <<Pref/binary,
      ElemLen:32/little-unsigned-integer,
      Elem:ElemLen/binary,
      ActorLen:32/little-unsigned-integer,
      Actor:ActorLen/binary,
      Cnt:64/little-unsigned-integer,
 %% @TODO(rdb|optimise) no need for a 32-bit int to express 0 | 1 TSB,
 %% but the c++ comparator is beyond me!
      0:32/integer>>.

%% @private see note above on insert_member_key/4. This is a
%% tombstone.
-spec remove_member_key(set(), member(), actor(), counter()) -> key().
remove_member_key(Set, Elem, Actor, Cnt) ->
    SetLen = byte_size(Set),
    ActorLen = byte_size(Actor),
    ElemLen = byte_size(Elem),
    <<SetLen:32/little-unsigned-integer,
      Set:SetLen/binary,
      ElemLen:32/little-unsigned-integer,
      Elem:ElemLen/binary,
      ActorLen:32/little-unsigned-integer,
      Actor:ActorLen/binary,
      Cnt:64/little-unsigned-integer,
 %% @TODO(rdb|optimise) no need for a 32-bit int to express 0 | 1, but
 %% the c++ comparator is beyond me!
      1:32/integer>>.

from_bin(B) ->
    binary_to_term(B).

to_bin(T) ->
    term_to_binary(T).

-define(WORD_FILE, "/usr/share/dict/words").

read_words(N) ->
    {ok, FD} = file:open(?WORD_FILE, [raw, read_ahead]),
    words_to_list(file:read_line(FD), FD, N, []).

words_to_list(_, FD, 0, Acc) ->
    file:close(FD),
    lists:reverse(Acc);
words_to_list(eof, FD, _N, Acc) ->
    file:close(FD),
    lists:reverse(Acc);
words_to_list({error, Reason}, FD, _N, Acc) ->
    file:close(FD),
    io:format("Error ~p Got ~p words~n", [Reason, length(Acc)]),
    lists:reverse(Acc);
words_to_list({ok, Word0}, FD, N ,Acc) ->
    Word = strip_cr(Word0),
    words_to_list(file:read_line(FD), FD, N-1, [Word | Acc]).

strip_cr(Word) ->
    list_to_binary(lists:reverse(tl(lists:reverse(Word)))).



%% PL = bigset:preflist(<<"rdb-test-bm-2">>).
%%  NPL = {1004782375664995756265033322492444576013453623296,
%% bigset_vnode:get_db(NPL).
%% DB = receive {_, {ok, DB}} -> DB end.
%% {ok, Itr} = eleveldb:iterator(DB,  [{iterator_refresh, true}]).
%% Decode = fun({ok, K, <<>>}) -> sext:decode(K);({ok, K, V}) -> {sext:decode(K) , binary_to_term(V)};(Other) -> Other end.

%%  Frst = eleveldb:iterator_move(Itr, first).
%%  Decode(Frst).
%%  Rss = [Decode( eleveldb:iterator_move(Itr, prefetch)) || _N <- lists:seq(1, 1000)].

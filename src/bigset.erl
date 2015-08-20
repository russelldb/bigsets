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

%%% codec
clock_key(Set) ->
    %% Must be same length as element key!
    sext:encode({s, Set, clock, <<>>, 0, <<0:1>>}).

end_key(Set) ->
    %% we don't know the last key for this set, but a binary that is
    %% the set name with a single 0bit appended sounds about right.
    sext:encode({s, <<Set/binary, 0:1>>, a, <<>>, 0, <<0:1>>}).

%% @private decode a binary key
decode_key(Bin) when is_binary(Bin) ->
    sext:decode(Bin);
decode_key(K) ->
    K.

%% @private sext encodes the element key so it is in order, on disk,
%% with the other elements. Use the actor ID and counter (dot)
%% too. This means at some extra storage, but makes for no reads
%% before writes on replication/delta merge. See read for how the
%% leveldb merge magic will work. Essentially every key {s, Set, E, A,
%% Cnt, 0} that has some key {s, Set, E, A, Cnt', 0} where Cnt' > Cnt
%% can be removed in compaction, as can every key {s, Set, E, A, Cnt,
%% 0} which has some key {s, Set, E, A, Cnt', 1} whenre Cnt' >=
%% Cnt. As can every key {s, Set, E, A, Cnt, 1} where the VV portion
%% of the set clock >= {A, Cnt} @TODO document how this tombstone
%% reaping works! Crazy!!
-spec insert_member_key(set(), member(), actor(), counter()) -> key().
insert_member_key(Set, Elem, Actor, Cnt) ->
    sext:encode({s, Set, Elem, Actor, Cnt, <<0:1>>}).

insert_member_value(Elem, Actor, Cnt) ->
    ElemLen = byte_size(Elem),
    ActorLen = byte_size(Actor),
    <<ElemLen:32/integer, Elem:ElemLen/binary,
      ActorLen:32/integer, Actor:ActorLen/binary,
      Cnt:32/integer, 0:8/integer>>.

-spec remove_member_key(set(), member(), actor(), counter()) -> key().
remove_member_key(Set, Element, Actor, Cnt) ->
    sext:encode({s, Set, Element, Actor, Cnt, <<1:1>>}).

remove_member_value(Elem, Actor, Cnt) ->
    ElemLen = byte_size(Elem),
    ActorLen = byte_size(Actor),
    <<ElemLen:32/integer, Elem:ElemLen/binary,
      ActorLen:32/integer, Actor:ActorLen/binary,
      Cnt:32/integer, 1>>.

decode_val(<<ElemLen:32/integer, Rest/binary>>) ->
    <<Elem:ElemLen/binary, ActorLen:32/integer, Rest1/binary>> = Rest,
    <<Actor:ActorLen/binary, Cnt:32/integer, TSB/binary>> = Rest1,
    {Elem, Actor, Cnt, TSB}.

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

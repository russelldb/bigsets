-module(bigset).

-include("bigset.hrl").

-compile([export_all]).

-ignore_xref([preflist/1, preflist_ann/1, dev_client/0]).

-type binary_key() :: binary().
-type key_tuple() :: clock_key() | end_key() | element_key().

preflist(Set) ->
    Hash = riak_core_util:chash_key({bigset, Set}),
    riak_core_apl:get_apl(Hash, 3, bigset).

preflist_ann(Set) ->
    Hash = riak_core_util:chash_key({bigset, Set}),
    UpNodes = riak_core_node_watcher:nodes(bigset),
    riak_core_apl:get_apl_ann(Hash, 3, UpNodes).

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


%%% keys funs %%%%


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
    Pref = key_prefix(Set),
    <<Pref/binary,
      $c, %% means clock
      Actor/binary>>.

%%% codec See docs on key scheme, use Actor name in tombstone key
%%% so AAE/replication of filter is safe. Like a decomposed VV, an
%%% actor may only update it's own tombstone
tombstone_key(Set, Actor) ->
    Pref = key_prefix(Set),
    <<Pref/binary,
      $d, %% means set tombstone (d 'cos > than c and < e)
      Actor/binary>>.

end_key(Set) ->
    %% an explicit end key that always sorts lowest
    %% written _every write_??
    Pref = key_prefix(Set),
    <<Pref/binary,
      $z %% means end key
    >>.

%% @doc is this key a clock key?
-spec is_clock_key(tuple()) -> boolean().
is_clock_key({clock, _, _}) ->
    true;
is_clock_key(_) ->
    false.

%% @doc get the set name from either an encoded or decoded key
-spec get_set(binary() | tuple()) -> binary().
get_set(<<SetLen:32/little-unsigned-integer, Rest/binary>>) ->
    <<Set:SetLen/binary, _/binary>> = Rest,
    Set;
get_set(DecodedKey) when is_tuple(DecodedKey) ->
    element(2, DecodedKey).

%% @doc get the set from the key and return the rest of the key as a
%% sub binary
-spec decode_set(binary()) -> {binary(), binary()}.
decode_set(<<SetLen:32/little-unsigned-integer, Rest/binary>>) ->
    <<Set:SetLen/binary, Key/binary>> = Rest,
    {Set, Key}.

%% @doc
-spec decode_key(Key :: binary()) -> {clock, set(), actor()} |
                                     {element, set(), member(), actor(), counter()} |
                                     {end_key, set()}.
decode_key(<<SetLen:32/little-unsigned-integer, Bin/binary>>) ->
    <<Set:SetLen/binary, Rest/binary>> = Bin,
    decode_key(Rest, Set).

decode_key(<<$c, Actor/binary>>, Set) ->
    {clock, Set, Actor};
decode_key(<<$d, Actor/binary>>, Set) ->
    {set_tombstone, Set, Actor};
decode_key(<<$e, Elem/binary>>, Set) ->
    decode_element(Elem, Set);
decode_key(<<$z>>, Set) ->
    {end_key, Set}.

decode_element(<<ElemLen:32/little-unsigned-integer, Rest/binary>>, Set) ->
    <<Elem:ElemLen/binary,
              ActorLen:32/little-unsigned-integer,
              ActorEtc/binary>> = Rest,
            <<Actor:ActorLen/binary,
              Cnt:64/little-unsigned-integer>> = ActorEtc,
    {element, Set, Elem, Actor, Cnt}.

%% @doc the opposite of decode_key/1, will return a binary key for the
%% decoded key tuples.
-spec encode_key(key_tuple()) -> binary_key().
encode_key({clock, Set, Actor}) ->
    clock_key(Set, Actor);
encode_key({end_key, Set}) ->
    end_key(Set);
encode_key({element, Set, Element, Actor, Cnt}) ->
    insert_member_key(Set, Element, Actor, Cnt).

%% @doc dot_from_key extract a dot from key `K'. Returns a
%% bisget_clock:dot().
-spec dot_from_key(Key :: binary()) -> bigset_clock:dot().
dot_from_key(K) ->
    {element, _S, _E, Actor, Cnt} = decode_key(K),
    {Actor, Cnt}.

%% @private encodes the element key so it is in order, on disk, with
%% the other elements. Use the actor ID and counter (dot) too. This
%% means at some extra storage, but makes for no reads before writes
%% on replication/delta merge.
-spec insert_member_key(set(), member(), actor(), counter()) -> key().
insert_member_key(Set, Elem, Actor, Cnt) ->
    Pref = key_prefix(Set),
    ActorLen = byte_size(Actor),
    ElemLen = byte_size(Elem),
    <<Pref/binary,
      $e, %% means an element
      ElemLen:32/little-unsigned-integer,
      Elem:ElemLen/binary,
      ActorLen:32/little-unsigned-integer,
      Actor:ActorLen/binary,
      Cnt:64/little-unsigned-integer
    >>.

%%% clock funs %%%

%% @doc get the tombstone at `TombstoneKey' from leveldb instance
%% `DB'. Returns a `bigset_clock:clock()'
-spec get_tombstone(TombstoneKey::binary(), db()) -> bigset_clock:clock().
get_tombstone(TombstoneKey, DB) ->
    {_, TS} = get_clock(TombstoneKey, DB),
    TS.

%% @doc get the actor `Id's tombstone for `Set' from leveldb instance
%% `DB'. Returns a `bigset_clock:clock()'
-spec get_tombstone(set(), actor(), db()) -> bigset_clock:clock().
get_tombstone(Set, Id, DB) ->
    TombstoneKey = tombstone_key(Set, Id),
    get_tombstone(TombstoneKey, DB).

-spec get_clock(ClockKey::binary(), db()) -> {boolean(), bigset_clock:clock()}.
get_clock(ClockKey, DB) when is_binary(ClockKey)  ->
    clock(eleveldb:get(DB, ClockKey, ?READ_OPTS)).

-spec get_clock(set(), actor(), db()) -> {boolean(), bigset_clock:clock()}.
get_clock(Set, Id, DB) ->
    ClockKey = clock_key(Set, Id),
    get_clock(ClockKey, DB).

-spec clock(not_found | {ok, binary()}) -> {boolean(), bigset_clock:clock()}.
clock(not_found) ->
    %% @TODO(rdb|correct) -> is this _really_ OK, what if we _know_
    %% (how?) the set exists, a missing clock is bad. At least have
    %% actor epochs, eh?
    {true, bigset_clock:fresh()};
clock({ok, ClockBin}) ->
    {false, bigset:from_bin(ClockBin)}.

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

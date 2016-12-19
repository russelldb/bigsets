-module(bigset).

-include("bigset.hrl").

-compile([export_all]).

-ignore_xref([preflist/1, preflist_ann/1, dev_client/0]).

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

    Batches = make_batches(Words, N, []),
    Res = [bigset_client:update(Set, Batch) ||
              Batch <- Batches],
    case lists:all(fun(E) ->
                           E == ok end,
                   Res) of
        true ->
            ok;
        false ->
            some_errors
    end.

-define(BATCH_SIZE, 1000).

make_batches(_Words, 0, Batches) ->
    Batches;
make_batches(Words, N, Batches) ->
    BatchSize = min(N, ?BATCH_SIZE),
    StartLimit = length(Words)-BatchSize,
    Start = crypto:rand_uniform(1, StartLimit),
    Batch = lists:sublist(Words, Start, BatchSize),
    make_batches(Words, N-BatchSize, [Batch | Batches]).

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

rem_stream_rec_loop(ReqId, Pid, Cnt) ->
    if Cnt rem (1000*1000) == 0 -> io:format("progress ~p~n", [Cnt]);
       true -> ok
    end,
    receive
        {ReqId, done} ->
            {ok, Cnt};
        {ReqId, {error, Error}} ->
            {error, {Error, Cnt}};
        {ReqId, {ok, {elems, E}}}  ->
            rem_stream_rec_loop(ReqId, Pid, Cnt+length(E));
        Other ->
            io:format("huh ~p~n", [Other]),
            rem_stream_rec_loop(ReqId, Pid, Cnt)
    after 60000 ->
           {error, stream_timeout, Cnt}
    end.

stream_receive_loop(ReqId, Pid, Monitor, {Cnt, Ctx}) ->
    receive
        {ReqId, done} ->
            erlang:demonitor(Monitor, [flush]),
            {ok, Ctx, Cnt};
        {ReqId, {error, Error}} ->
            erlang:demonitor(Monitor, [flush]),
            {error, Error};
        {ReqId, {ok, {ctx, Res}}} ->
            stream_receive_loop(ReqId, Pid, Monitor, {Cnt, Res});
        {ReqId, {ok, {elems, Res}}} ->
            stream_receive_loop(ReqId, Pid, Monitor, {Cnt+length(Res), Ctx});
        Other -> lager:info("huh ~p", [Other])
     %%% @TODO(rdb|wut?) why does this message get fired first for remote node?
        %% {'DOWN', Monitor, process, Pid, Info} ->
        %%     lager:debug("Got DOWN message ~p~n", [Info]),
        %%     {error, down, Info}
    after 10000 ->
            erlang:demonitor(Monitor, [flush]),
            {error, stream, timeout}
    end.

bm_read(Set, N) ->
    Times = [begin
                 {Time, _} = timer:tc(bigset_client, read, [Set, []]),
                 Time
             end || _ <- lists:seq(1, N)],
    [{max, lists:max(Times)},
     {min, lists:min(Times)},
     {avg, lists:sum(Times) div length(Times)}].


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
    TombstoneKey = bigset_keys:tombstone_key(Set, Id),
    get_tombstone(TombstoneKey, DB).

-spec get_clock(ClockKey::binary(), db()) -> {boolean(), bigset_clock:clock()}.
get_clock(ClockKey, DB) when is_binary(ClockKey)  ->
    clock(eleveldb:get(DB, ClockKey, ?READ_OPTS)).

-spec get_clock(set(), actor(), db()) -> {boolean(), bigset_clock:clock()}.
get_clock(Set, Id, DB) ->
    ClockKey = bigset_keys:clock_key(Set, Id),
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

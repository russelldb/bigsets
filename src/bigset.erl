-module(bigset).

-include("bigset.hrl").

-compile([export_all]).

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
    Res = [bigset_client:update(Set, [crypto:rand_bytes(100)]) || _N <- lists:seq(1, N)],
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
    lager:debug("stream reading from set~n"),
    {ok, ReqId, Pid} = bigset_client:stream_read(S, []),
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
            stream_receive_loop(ReqId, Pid, Monitor, {Cnt+length(Res), Ctx});
        {'DOWN', Monitor, process, Pid, Info} ->
            lager:debug("Got DOWN message ~p~n", [Info]),
            {error, Info}
    after 10000 ->
            erlang:demonitor(Monitor, [flush]),
            lager:debug("Error, timeout~n"),
            {error, timeout}
    end.

%%% codec
clock_key(Set) ->
    sext:encode({s, Set, clock}).

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
%% of the set clock >= {A, Cnt}. Crazy!!
-spec insert_member_key(set(), member(), actor(), counter()) -> key().
insert_member_key(Set, Elem, Actor, Cnt) ->
    sext:encode({s, Set, Elem, Actor, Cnt, <<0:1>>}).

-spec remove_member_key(set(), member(), actor(), counter()) -> key().
remove_member_key(Set, Element, Actor, Cnt) ->
    sext:encode({s, Set, Element, Actor, Cnt, <<1:1>>}).

from_bin(B) ->
    binary_to_term(B).

to_bin(T) ->
    term_to_binary(T).



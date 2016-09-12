%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% encapsulate bigset keys
%%% @end
%%% Created : 26 Apr 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_keys).

-include("bigset.hrl").

-compile([export_all]).

-define(NULL, $\0).

prefix(Set) ->
    <<1, %%Magic byte
      1:8/big-unsigned-integer, %% version
      Set/binary, %% Set
      ?NULL
      >>.

clock_key(Set, Actor) ->
    <<(prefix(Set))/binary,
      $c,
      ?NULL,
      ?NULL, %% element field is null
      Actor/binary,
      ?NULL %% actor terminator
    >>.

tombstone_key(Set, Actor) ->
    <<(prefix(Set))/binary,
      $d,
      ?NULL,
      ?NULL, %% element field is null
      Actor/binary,
      ?NULL>>.

insert_member_key(Set, Element, Actor, Cnt) ->
    <<(prefix(Set))/binary,
      $e,
      ?NULL,
      Element/binary,
      ?NULL,
      Actor/binary,
      ?NULL,
      Cnt:64/big-unsigned-integer>>.

end_key(Set) ->
    <<(prefix(Set))/binary,
      $z,
      ?NULL,
      ?NULL,
      ?NULL>>.

%% @doc decode_set pulls the set binary from the
%% key. Returns a binary(), the set name.
-spec decode_set(key()) -> list().
decode_set(<<1, 1:8/big-unsigned-integer, Rest/binary>>) ->
    [Set, _] = binary:split(Rest, << ?NULL >>),
    Set.

%% @doc decode key into tagged tuple of it's
%% constituent
-spec decode_key(key()) -> tuple().
decode_key(<<1, 1:8/big-unsigned-integer, Rest/binary>>) ->
    [{SetLen, 1} |  Pos] = binary:matches(Rest, << ?NULL >>, []),
    Set = binary:part(Rest, 0, SetLen),
    Type = binary:part(Rest, SetLen+1, 1),
    decode_type(Type, Set, Rest, Pos).

decode_type(<<$c>>, Set, Rest, [{ActorStart, 1}, {ActorEnd, 1} | _]) ->
    Actor = binary:part(Rest, ActorStart+2, ActorEnd-1),
    {clock, Set, Actor};
decode_type(<<$d>>, Set, Rest, [{ActorStart, 1}, {ActorEnd, 1} | _]) ->
    Actor = binary:part(Rest, ActorStart+2, ActorEnd-1),
    {tombstone, Set, Actor};
decode_type(<<$z>>, Set, _Rest, _Pos) ->
    {end_key, Set};
decode_type(<<$e>>, Set, Bin, Pos) ->
    {element, Set, Bin, Pos}.

%% @doc is the provided `Key' the clock key for
%% the given `actor' and `set'.
-spec is_actor_clock_key(set(), actor(), key()) -> boolean().
is_actor_clock_key(Set, Actor, <<_Pre:2/binary, Key/binary>>) ->
    case binary:split(Key, << ?NULL >>, [global]) of
        [Set, <<$c>>, Actor, <<>>] ->
            true;
        _ ->
            false
    end.

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
      ?NULL, %% element field is null
      Actor/binary,
      ?NULL %% actor terminator
    >>.

tombstone_key(Set, Actor) ->
    <<(prefix(Set))/binary,
      $d,
      ?NULL, %% element field is null
      Actor/binary,
      ?NULL>>.

insert_member_key(Set, Element, Actor, Cnt) ->
    <<(prefix(Set))/binary,
      $e,
      Element/binary,
      ?NULL,
      Actor/binary,
      ?NULL,
      Cnt:64/big-unsigned-integer>>.

end_key(Set) ->
    <<(prefix(Set))/binary,
      $z,
      ?NULL,
      ?NULL>>.

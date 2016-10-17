-module(bigset_keys_lens).

-behaviour(bigset_keys).

-include("bigset.hrl").

-export([
         add_comparator_opt/1,
         clock_key/2,
         tombstone_key/2,
         end_key/1,
         insert_member_key/4,
         is_actor_clock_key/3,
         decode_key/1,
         decode_set/1
        ]).

add_comparator_opt(Opts) ->
    [{bigsets, true} | Opts].

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

decode_set(Key) ->
    %% dirty hack
    element(2, decode_key(Key)).

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

is_actor_clock_key(Set, Actor, Key) ->
    Key == clock_key(Set, Actor).

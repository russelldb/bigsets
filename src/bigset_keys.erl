%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% encapsulate bigset keys
%%% @end
%%% Created : 26 Apr 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_keys).

-include("bigset.hrl").

-export([clock_key/2,
         tombstone_key/2,
         end_key/1,
         insert_member_key/4,
         is_actor_clock_key/3,
         decode_key/1,
         decode_set/1
        ]).

-export([keys_mod/0]).

-callback clock_key(set(), actor()) -> key().
-callback tombstone_key(set(), actor()) -> key().
-callback end_key(set()) -> key().
-callback insert_member_key(set(), element(), actor(), pos_integer()) ->
    key().
-callback is_actor_clock_key(set(), actor(), key()) ->
    boolean().
-callback decode_key(key()) ->
    {clock, set(), actor()}   |
    {tombstone, set(), actor} |
    {end_key, set()}          |
    {element, set(), element(), actor(), pos_integer()}.
-callback decode_set(key()) ->
    set().

clock_key(Set, Actor) ->
    ?BS_KEYS:clock_key(Set, Actor).

tombstone_key(Set, Actor) ->
    ?BS_KEYS:tombstone_key(Set, Actor).

end_key(Set) ->
    ?BS_KEYS:end_key(Set).

insert_member_key(Set, Element, Actor, Cnt) ->
    ?BS_KEYS:insert_member_key(Set, Element, Actor, Cnt).

is_actor_clock_key(Set, Actor, Key) ->
    ?BS_KEYS:is_actor_clock_key(Set, Actor, Key).

decode_key(Bin) ->
    ?BS_KEYS:decode_key(Bin).

decode_set(Bin) ->
    ?BS_KEYS:decode_set(Bin).

keys_mod() ->
    ?BS_KEYS.


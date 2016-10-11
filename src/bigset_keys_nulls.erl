%% -------------------------------------------------------------------
%%
%% riak_kv_bs_keys: Key encoding/decoding for bigsets
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%
%% @doc
%%
%% This module contains the functions to generate binaries keys for
%% decomposed "BigSets" that sort correctly in leveldb, and the
%% functions to decode such binary keys into erlang terms.
%%
%% Bigset key format matters as we are currently tied to leveldb. The
%% key scheme/format ensures that all the keys for a set S are
%% logically grouped, contiguously. The keys for set S are further
%% ordered so that the 1st key(s) for a set are the logical clocks
%% key(s) (@see `clock_key/2'), followed by the tombstone keys (@see
%% `tombstone_key/2'), followed by the elements themselves (@see
%% `insert_member_key/4'). Each element key contains the element (an
%% opaque binary), and the dot that supports the element. The element
%% keys are ordered by the natural sort order of the elements
%% themselves, and then the actors, and finally by the event counter
%% of the dot. The final key for a set is the end key (@see
%% `end_key/1') which signals the end of the set.
%%
%% All bigset keys are made up of NULL-byte terminated binary
%% fields. The fields are Set-name, key-type, element, actor,
%% event-counter. Apart from set-name, all fields are optional.
%%
%% All bigset keys also have a common prefix: a "Magic Byte" that
%% sorts before sext encoded riak_object and 2i keys, and an 8-bit
%% integer that is the version for the key format.
%%
%% @end
%% -------------------------------------------------------------------

-module(bigset_keys_nulls).

-behaviour(bigset_keys).

-include("bigset.hrl").

-export([
         clock_key/2,
         decode_key/1,
         decode_set/1,
         end_key/1,
         insert_member_key/4,
         is_actor_clock_key/3,
         tombstone_key/2
        ]).

-ifdef(EQC).

-export([
         eqc_check/1,
         eqc_check/2,
         prop_ordered/0,
         run/1,
         run/2
        ]).

-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(NULL, $\0).

%% @private common preamble for any set key. Magic byte and version
%% header for `Set'
prefix(Set) ->
    <<1, %%Magic byte
      1:8/big-unsigned-integer, %% version
      Set/binary, %% Set
      ?NULL
      >>.

%% @private the length of each of the fields, as a hint for parsing in
%% decode_key. Stuck at the end of the key so as not to effect
%% sorting.
parse_info(Set, Element, Actor) ->
    <<(byte_size(Set)):64/big-unsigned-integer,
      (byte_size(Element)):64/big-unsigned-integer,
      (byte_size(Actor)):64/big-unsigned-integer >>.

%% @private parse out the length of each field
parse_info(Bin) when is_binary(Bin) ->
    <<SetLen:64/big-unsigned-integer,
      ElementLen:64/big-unsigned-integer,
      ActorLen:64/big-unsigned-integer>> = binary_part(Bin, {byte_size(Bin), -24}),
    {SetLen, ElementLen, ActorLen}.

%% @doc a clock key for `Set' and `Actor'
clock_key(Set, Actor) ->
    ParseInfo = parse_info(Set, <<>>, Actor),
    <<(prefix(Set))/binary,
      $c,
      ?NULL,
      ?NULL,
      Actor/binary,
      ?NULL,
      ?NULL,
      ParseInfo/binary
    >>.

%% @doc a tombstone key for `Set' and `Actor'
tombstone_key(Set, Actor) ->
    ParseInfo = parse_info(Set, <<>>, Actor),
    <<(prefix(Set))/binary,
      $d,
      ?NULL,
      ?NULL,
      Actor/binary,
      ?NULL,
      ?NULL,
      ParseInfo/binary
    >>.

%% @doc the key for an `Element' of `Set' written with the dot of
%% `Actor' and `Cnt'
insert_member_key(Set, Element, Actor, Cnt) ->
    ParseInfo = parse_info(Set, Element, Actor),
    <<(prefix(Set))/binary,
      $e,
      ?NULL,
      Element/binary,
      ?NULL,
      Actor/binary,
      ?NULL,
      Cnt:64/big-unsigned-integer,
      ?NULL,
      ParseInfo/binary
    >>.

%% @doc the end key for `Set'
end_key(Set) ->
    ParseInfo = parse_info(Set, <<>>, <<>>),
    <<(prefix(Set))/binary,
      $z,
      ?NULL,
      ?NULL,
      ?NULL,
      ?NULL,
      ParseInfo/binary
    >>.

%% @doc decode_set pulls the set binary from the
%% key. Returns a binary(), the set name.
-spec decode_set(key()) -> list().
decode_set(<<1, 1:8/big-unsigned-integer, Rest/binary>>) ->
    {SetLen, _, _} = parse_info(Rest),
    <<Set:SetLen/binary, _/binary>> = Rest,
    Set.

%% @doc decode key into tagged tuple of it's constituent parts
-spec decode_key(key()) -> tuple().
decode_key(<<1, 1:8/big-unsigned-integer, Rest/binary>>) ->
    {SetLen, ElementLen, ActorLen} = parse_info(Rest),
    <<Set:SetLen/binary, _:1/binary, Type:1/binary, _:1/binary, Key/binary>> = Rest,
    decode_type(Type, Set, Key, ActorLen, ElementLen).

%% @private internal. Decode the type specific key
decode_type(<<$c>>, Set, Key, ActorLen, 0) ->
    <<_:1/binary, %% no element
      Actor:ActorLen/binary,
      _/binary %% The rest
    >> = Key,
    {clock, Set, Actor};
decode_type(<<$d>>, Set, Key, ActorLen, 0) ->
    <<_:1/binary, %% no element
      Actor:ActorLen/binary,
      _/binary %% The rest
    >> = Key,
    {tombstone, Set, Actor};
decode_type(<<$z>>, Set, _Key, 0, 0) ->
    {end_key, Set};
decode_type(<<$e>>, Set, Key, ActorLen, ElementLen) ->
    <<
      Element:ElementLen/binary,
      _:1/binary,
      Actor:ActorLen/binary,
      _:1/binary,
      Cnt:64/big-unsigned-integer,
      _/binary %% The rest
    >> = Key,
    {element, Set, Element, Actor, Cnt}.

is_actor_clock_key(Set, Actor, Key) ->
    Key == clock_key(Set, Actor).

-ifdef(EQC).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

%% @doc expose the properties to eunit test framework
eqc_test_() ->
    {timeout, 20, [
                   ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(10, ?QC_OUT(prop_ordered()))))
                  ]}.

%% @doc to simplify running a given property `Prop'
run(Prop) ->
    run(Prop, ?NUMTESTS).

%% @doc simplify running `Prop' using `Count' for eqc:numtests/2
run(Prop, Count) ->
    eqc:quickcheck(eqc:numtests(Count, Prop)).

%% @doc run eqc:check/1 for the given property
eqc_check(Prop) ->
    eqc:check(Prop).

%% @doc run eqc:check/1 using the stored counterexample from `File'
eqc_check(Prop, File) ->
    {ok, Bytes} = file:read_file(File),
    CE = binary_to_term(Bytes),
    eqc:check(Prop, CE).

-define(ELEMENTS, [<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"X">>,<<"Y">>,<<"Z">>]).
-define(SETS, ?ELEMENTS).
-define(ACTORS, ?SETS).
-record(fold_acc, {
          cnt=0,
          type,
          set,
          actor,
          element,
          event,
          bad
         }).

%% @doc does the bs key scheme really sorts as required. Has side
%% effects. Creates, writes to, deletes from, and destroys a leveldb
%% instance.
prop_ordered() ->
    ?SETUP(fun() ->
                   Root = "test/eleveldb-backend-eqc",
                   {ok, Ref} = start_eleveldb(Root),
                   put(bs_keys_level, {ok, Ref}),
                   fun() -> destroy_eleveldb(Root) end
           end,
           ?FORALL(Keys, gen_keys(),
                   ?FORALL(Writes, gen_writes(lists:flatten(Keys)),
                           begin
                               %% write the writes as a big mixed up blob
                               %% TODO should probably write them in broken
                               %% up chunks by Set,eh?
                               {ok, Ref} = get(bs_keys_level),
                               ok = eleveldb:write(Ref, Writes, [{sync, false}]),
                               Acc =  #fold_acc{},
                               %% verify the order is as expected, ok
                               AccFinal =
                                   try
                                       eleveldb:fold(Ref, fun fold/2, Acc, [{iterator_refresh, true}])
                                   catch
                                       {break, Acc2} ->
                                           Acc2
                                   end,

                               ok = eleveldb:write(Ref, to_deletes(Writes), [{sync, true}]),
                               measure(writes, length(Writes),
                                       ?WHENFAIL(
                                          begin
                                              io:format("failed with acc ~p~n", [AccFinal]),
                                              io:format("keys were ~p~n", [[decode_whenfail(K) || {put, K, <<>>} <- Writes]])
                                          end,
                                          conjunction([{right_number, equals(length(ordsets:from_list(Writes)), AccFinal#fold_acc.cnt)},
                                                       {no_bad, equals(undefined, AccFinal#fold_acc.bad)}])))
                           end))).

decode_whenfail(K) ->
    try
        decode_key(K)
    catch _:_ ->
            K
    end.

%% @doc transform writes to deletes. Part of cleaning up leveldb
%% between property iterations.
to_deletes(Writes) ->
    [{delete, K} || {put, K, _V} <- Writes].

%% @doc fold is state machine where the transition is only valid if
%% the event of the key follows in order from the previous one.
fold({Key, <<>>}, Acc=#fold_acc{set=undefined, cnt=0}) ->
    %% first key, must be a clock key
    case decode_key(Key) of
        {clock, Set, Actor} ->
            Acc#fold_acc{set=Set, actor=Actor, type=clock, cnt=1};
        Decoded ->
            throw({break, Acc#fold_acc{bad=Decoded}})
    end;
fold({Key, <<>>}, Acc=#fold_acc{set=Set, cnt=Cnt, type=clock, actor=Actor}) ->
    %% last key was a clock, all that's permissable is a clock key
    %% with a greater actor, a ts key, an element key, or an end key
    %% all for this set
    case decode_key(Key) of
        {clock, Set, Actor2} when Actor2 > Actor ->
            Acc#fold_acc{set=Set, actor=Actor2, type=clock, cnt=Cnt+1};
        {tombstone, Set, AnyActor} ->
            Acc#fold_acc{set=Set, actor=AnyActor, type=tombstone, cnt=Cnt+1};
        {element, Set, Element, AnyActor, Event} ->
            Acc#fold_acc{set=Set, actor=AnyActor, type=element,
                         element=Element,
                         event=Event,
                         cnt=Cnt+1};
        {end_key, Set} ->
            Acc#fold_acc{set=Set, actor=undefined, type=end_key,
                         element=undefined,
                         event=0,
                         cnt=Cnt+1};
        Decoded ->
            %% Any thing else is broken
            throw({break,  Acc#fold_acc{bad=Decoded}})
    end;
fold({Key, <<>>}, Acc=#fold_acc{set=Set, cnt=Cnt, type=tombstone, actor=Actor}) ->
    %% Last key was a ts, only TS, element, last key for same set
    %% permitted
    case decode_key(Key) of
        {tombstone, Set, AnyActor} when AnyActor > Actor ->
            Acc#fold_acc{set=Set, actor=AnyActor, type=tombstone, cnt=Cnt+1};
        {element, Set, Element, AnyActor, Event} ->
            Acc#fold_acc{set=Set, actor=AnyActor, type=element,
                         element=Element,
                         event=Event,
                         cnt=Cnt+1};
        {end_key, Set} ->
            Acc#fold_acc{set=Set, actor=undefined, type=end_key,
                         element=undefined,
                         event=0,
                         cnt=Cnt+1};
        Decoded ->
            %% Any thing else is broken
            throw({break,  Acc#fold_acc{bad=Decoded}})
    end;
fold({Key, <<>>}, Acc=#fold_acc{set=Set, cnt=Cnt, type=element, actor=Actor, element=Element, event=Event}) ->
    %% Last key was an element key, only an element key with same
    %% element or higher. If same element actor or cnt must be higher. Or end_key
    case decode_key(Key) of
        {element, Set, Element, Actor, Event1} when Event1 > Event ->
            Acc#fold_acc{event=Event1,
                         cnt=Cnt+1};
        {element, Set, Element2, AnyActor, Event1} when Element2 > Element ->
            Acc#fold_acc{event=Event1,
                         actor=AnyActor,
                         element=Element2,
                         cnt=Cnt+1};
        {element, Set, Element, Actor2, Event1} when Actor2 > Actor ->
            Acc#fold_acc{event=Event1,
                         element=Element,
                         actor=Actor2,
                         cnt=Cnt+1};
        {end_key, Set} ->
            Acc#fold_acc{set=Set, actor=undefined, type=end_key,
                         element=undefined,
                         event=0,
                         cnt=Cnt+1};
        Decoded ->
            %% Any thing else is broken
            throw({break,  Acc#fold_acc{bad=Decoded}})
    end;
fold({Key, <<>>}, Acc=#fold_acc{set=Set, type=end_key, cnt=Cnt}) ->
    case decode_key(Key) of
        {clock, Set2, Actor} when Set2 > Set ->
            Acc#fold_acc{set=Set2, actor=Actor, type=clock, cnt=Cnt+1};
        Decoded ->
            throw({break,  Acc#fold_acc{bad=Decoded}})
    end.

%% @doc @TODO use riak_kv_eleveldb_backend, when ready
start_eleveldb(Root) ->
    DataDir = Root,
    Opts =  [{create_if_missing, true},
             {write_buffer_size, 1024*1024},
             {max_open_files, 20}],

    case get(Root) of
        {ok, _OldRef} ->
            destroy_eleveldb(Root);
        _ ->
            {ok, Ref} = eleveldb:open(DataDir, Opts),
            put(Root, {ok, Ref}),
            {ok, Ref}
    end.

%% @doc ensure that there is no lingering eleveldb left over. Called
%% by property teardown.
destroy_eleveldb(Root) ->
    case get(bs_keys_level) of
        {ok, Ref} ->
            eleveldb:close(Ref),
            ?assertCmd("rm -rf " ++ Root),
            erase(Root);
        _ ->
            ?assertCmd("rm -rf " ++ Root)
    end.

%% @doc take the generated keys and turn them into leveldb
%% writes. Shuffle them.
gen_writes(Keys) ->
    ?LET(Shuffed, shuffle(Keys), [{put, K, <<>>} || K <- Shuffed]).

%% @doc generate a bunch of bigset keys that represent a number of
%% bigsets
gen_keys() ->
    ?LET({Actors, Sets},
         {actors(), sets()},
         [gen_keys(Actors, Set) || Set <- Sets]).

%% @doc generate a list of binary actor IDs
actors() ->
    list_o_bins().

%% @doc generate a list of binary set names
sets() ->
    list_o_bins().

list_o_bins() ->
    ?LET(Size, choose(10, 100), non_empty(list(binary(Size)))).%%non_empty(sublist(?SETS)).

%% @doc generate an element
set_element() ->
    non_empty(binary(100)).

%% @doc generate all the keys for the `Set' That is clock(s),
%% tombstone(s), element and end keys.
gen_keys(Actors, Set) ->
    ?LET(SetSize, int(),
         gen_element_keys(Set, Actors, abs(SetSize)) ++
             gen_clock_ts_keys(Actors, Set) ++
        [end_key(Set)]).

%% @doc for the given `Set' generate `Size' many keys.
gen_element_keys(Set, Actors, Size) ->
    [gen_element_key(Set, Actors) || _ <- lists:seq(1, Size)].

%% @doc for the given `Set' generate a set element key with a dot from
%% an `Actor' in `Actors'
gen_element_key(Set, Actors) ->
    ?LET({Actor, Element, Cnt},
         {elements(Actors),
          set_element(),
          int()},
         insert_member_key(Set, Actor, Element,
                           %% lazy non zero
                           abs(Cnt)+1)).

%% @doc clock and tombstone keys for `Set' for `Actors'
gen_clock_ts_keys(Actors, Set) ->
    lists:foldl(fun(Actor, Acc) ->
                        [clock_key(Set, Actor), tombstone_key(Set, Actor) | Acc]
                end,
                [],
                Actors).
-endif.

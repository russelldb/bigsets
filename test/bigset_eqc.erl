%% -------------------------------------------------------------------
%%
%% a testing place for bigset ideas
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(bigset_eqc).

%%-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state, {replicas=[], %% Actor Ids for replicas in the system
                adds=[],      %% Elements that have been added to the set
                delta_buffers=[] %% buffers for deltas
               }).

%% The set of possible elements in the set
-define(ELEMENTS, ['A', 'B', 'C', 'D', 'X', 'Y', 'Z']).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-define(ADD, 1).
-define(REMOVE, 0).

%% Key is {Element, Actor, TombstoneBit, Cnt} and we keep the set
%% sorted and foldr over it for accumulate (as that is simpler) see
%% accumulate below. With TSB being 0 this means all tombstones for
%% Key X are before Key X in the elements list. First we accumulate
%% highest ADD for X and the first TS we hit is the highest Rem for
%% X. If the Rem is > than the add, remove, otherwise keep.

eqc_test_() ->
    {timeout, 60, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(50, ?QC_OUT(prop_merge()))))}.

run() ->
    run(?NUMTESTS).

run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_merge())).

check() ->
    eqc:check(prop_merge()).


initial_state() ->
    #state{}.

%% ------ Grouped operator: create_replica
create_replica_pre(#state{replicas=Replicas}) ->
    length(Replicas) < 10.

%% @doc create_replica_command - Command generator
create_replica_args(_S) ->
    %% Don't waste time shrinking the replicas ID binaries, they 8
    %% byte binaris as that is riak-esque.
    [noshrink(nat())].

%% @doc create_replica_pre - don't create a replica that already
%% exists
-spec create_replica_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
create_replica_pre(#state{replicas=Replicas}, [Id]) ->
    not lists:member(Id, Replicas).

%% @doc create a new replica, and store a bottom orswot/orset+deferred
%% in ets
create_replica(Id) ->
    ets:insert(?MODULE, {Id, {bigset_clock:fresh(), []}, riak_dt_orswot:new()}).

%% @doc create_replica_next - Add the new replica ID to state
-spec create_replica_next(S :: eqc_statem:symbolic_state(),
                          V :: eqc_statem:var(),
                          Args :: [term()]) -> eqc_statem:symbolic_state().
create_replica_next(S=#state{replicas=R0}, _Value, [Id]) ->
    S#state{replicas=R0++[Id]}.

%% ------ Grouped operator: add
add_args(#state{replicas=Replicas}) ->
    [elements(Replicas),
     %% Start of with earlier/fewer elements
     growingelements(?ELEMENTS)].

%% @doc add_pre - Don't add to a set until we have a replica
-spec add_pre(S :: eqc_statem:symbolic_state()) -> boolean().
add_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc add_pre - Ensure correct shrinking, only select a replica that
%% is in the state
-spec add_pre(S :: eqc_statem:symbolic_state(),
              Args :: [term()]) -> boolean().
add_pre(#state{replicas=Replicas}, [Replica, _]) ->
    lists:member(Replica, Replicas).

%% @doc add the `Element' to the sets at `Replica'
add(Replica, Element) ->
    [{Replica, {Clock, Keys}, ORSWOT}] = ets:lookup(?MODULE, Replica),

    {{Replica, Cnt}, Clock2} = bigset_clock:increment(Replica, Clock),
    Key = {Element, Replica, ?ADD, Cnt},
    Keys2 = lists:usort([Key | Keys]),

    {ok, ORSWOT2} = riak_dt_orswot:update({add, Element}, Replica, ORSWOT),

    ets:insert(?MODULE, {Replica, {Clock2, Keys2}, ORSWOT2}),
    Key.

%% @doc add_next - Add the `Element' to the `adds' list so we can
%% select from it when we come to remove. This increases the liklihood
%% of a remove actuallybeing meaningful. Add to the replica's delta
%% buffer too.
-spec add_next(S :: eqc_statem:symbolic_state(),
               V :: eqc_statem:var(),
               Args :: [term()]) -> eqc_statem:symbolic_state().
add_next(S=#state{adds=Adds, delta_buffers=DBs}, Key, [Replica, Element]) ->
    DB = proplists:get_value(Replica, DBs, []),
    S#state{adds=lists:umerge(Adds, [Element]),
            delta_buffers=lists:keystore(Replica, 1, DBs, {Replica, DB ++ [Key]})}.



%% ------ Grouped operator: context_remove
context_remove_args(#state{replicas=Replicas, adds=Adds}) ->
    [elements(Replicas),
     elements(Replicas),
     elements(Adds)].

%% @doc context_remove_pre - As for `remove/1'
-spec context_remove_pre(S :: eqc_statem:symbolic_state()) -> boolean().
context_remove_pre(#state{replicas=Replicas, adds=Adds}) ->
    Replicas /= [] andalso Adds /= [].

%% @doc context_remove_pre - Ensure correct shrinking
-spec context_remove_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
context_remove_pre(#state{replicas=Replicas, adds=Adds}, [From, To, Element]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas)
        andalso lists:member(Element, Adds).

%% @doc a dynamic precondition uses concrete state, check that the
%% `From' set contains `Element'
context_remove_dynamicpre(_S, [From, _To, Element]) ->
    [{From, Bigset, _FromSwot}] = ets:lookup(?MODULE, From),
    lists:member(Element, replica_value(Bigset)).

%% @doc perform a context remove using the context+element at `From'
%% and removing from `To'
context_remove(From, To, Element) ->
    [{From, FromBigset, FromORSWOT}] = ets:lookup(?MODULE, From),
    [{To, ToBigset, ToORSWOT}] = ets:lookup(?MODULE, To),

    RemoveKeys = remove_keys(Element, FromBigset),
    ToBigset2 = write_tombstones(RemoveKeys, ToBigset),

    Ctx = riak_dt_orswot:precondition_context(FromORSWOT),
    {ok, ToORSWOT2} = riak_dt_orswot:update({remove, Element}, To, ToORSWOT, Ctx),

    dump_node(From, FromBigset),

    ets:insert(?MODULE, {To, ToBigset2, ToORSWOT2}),
    RemoveKeys.

%% @doc context_remove_next - Add remove keys to the replica's delta
%% buffer.
-spec context_remove_next(S :: eqc_statem:symbolic_state(),
                               V :: eqc_statem:var(),
                               Args :: [term()]) -> eqc_statem:symbolic_state().
context_remove_next(S=#state{delta_buffers=DBs}, Keys, [_From, To, _RemovedElement]) ->
    DB = proplists:get_value(To, DBs, []),
    S#state{delta_buffers=lists:keystore(To, 1, DBs, {To, DB ++ [Keys]})}.

%% ------ Grouped operator: replicate
%% @doc replicate_args - Choose a From and To for replication
replicate_args(#state{replicas=Replicas, delta_buffers=DBs}) ->
    [?LET({Rep, Buffer}, elements(DBs),
          {Rep, Buffer}), %% @TODO(rdb) what about a subset/1 of the buffer?
     elements(Replicas)].

%% @doc replicate_pre - There must be at least on replica to replicate
-spec replicate_pre(S :: eqc_statem:symbolic_state()) -> boolean().
replicate_pre(#state{replicas=Replicas, delta_buffers=DBs}) ->
    Replicas /= [] andalso DBs /= [].

%% @doc replicate_pre - Ensure correct shrinking
-spec replicate_pre(S :: eqc_statem:symbolic_state(),
                    Args :: [term()]) -> boolean().
replicate_pre(#state{replicas=Replicas, delta_buffers=DBs}, [{FromRep, FromDelta}, To]) ->
    DB = proplists:get_value(FromRep, DBs, []),
    lists:keymember(FromRep, 1, DBs)
        andalso (FromDelta -- DB) == []
        andalso lists:member(To, Replicas).

%% @doc simulate replication by merging state at `To' with state from `From'
replicate({FromRep, FromDelta}, To) ->
    [{FromRep, _FromBigset, FromORSWOT}] = ets:lookup(?MODULE, FromRep),
    [{To, {ToClock, ToKeys}, ToORSWOT}] = ets:lookup(?MODULE, To),

    F = fun({_Element, Actor, ?ADD, Cnt}=Key, {Clock, Writes}) ->
                Dot = {Actor, Cnt},
                case bigset_clock:seen(Clock, Dot) of
                    true ->
                        %% No op, skip it/discard
                        {Clock, Writes};
                    false ->
                        %% Strip the dot
                        C2 = bigset_clock:strip_dots(Dot, Clock),
                        {C2, [Key | Writes]}
                end;
           ({_Element, Actor, ?REMOVE, Cnt}=Key, {Clock, Writes}) ->
                %% Tombstones are always written, compaction can merge
                %% them out later. Or we could try a read and see if
                %% they're needed?? But we must add the dots to the
                %% clock!!
                C2 = bigset_clock:strip_dots({Actor, Cnt}, Clock),
                {C2, [Key | Writes]}
        end,

    {NewClock, NewKeys} = lists:foldl(F, {ToClock, []}, lists:flatten(FromDelta)),

    ORSWOT = riak_dt_orswot:merge(FromORSWOT, ToORSWOT),

    ets:insert(?MODULE, {To, {NewClock, lists:usort(NewKeys++ToKeys)}, ORSWOT}).

%% @doc weights for commands. Don't create too many replicas, but
%% prejudice in favour of creating more than 1. Try and balance
%% removes with adds. But favour adds so we have something to
%% remove. See the aggregation output.
weight(S, create_replica) when length(S#state.replicas) > 2 ->
    1;
weight(S, create_replica) when length(S#state.replicas) < 5 ->
    4;
weight(_S, context_remove) ->
    3;
weight(_S, add) ->
    8;
weight(_S, _) ->
    1.

%% @doc check that the implementation of the ORSWOT is equivalent to
%% the OR-Set impl.
-spec prop_merge() -> eqc:property().
prop_merge() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                %% Store the state external to the statem for correct
                %% shrinking. This is best practice.
                ets:new(?MODULE, [named_table, set]),
                {H, S, Res} = run_commands(?MODULE,Cmds),
                {MergedBigset, MergedSwot} = lists:foldl(fun({_Id, Bigset, ORSWOT}, {MBS, MOS}) ->
                                                                 {merge_bigsets(Bigset, MBS),
                                                                  riak_dt_orswot:merge(ORSWOT, MOS)}
                                                         end,
                                                         {{bigset_clock:fresh(), []}, riak_dt_orswot:new()},
                                                         ets:tab2list(?MODULE)),

                ets:delete(?MODULE),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                aggregate(command_names(Cmds),
                                          measure(replicas, length(S#state.replicas),
                                                  measure(elements, riak_dt_orswot:stat(element_count, MergedSwot),
                                                          conjunction([{result, Res == ok},
                                                                       {equal, equals(sets_equal(MergedBigset, MergedSwot), true)}
                                                                      ])))))

            end).


merge_bigsets({Clock, Keys}, AccumulatedSet) ->
    KeepSet = accumulate(Keys),
    merge({Clock, KeepSet}, AccumulatedSet).

%% @private the vnode fold operation, can also be the eleveldb compact
%% operation. Full credit to Thomas Arts @quviq for this rather
%% genious roll backwards through the list. See if there is a way to
%% do this in level, if it helps any.
accumulate(Keys) ->
    lists:foldr(fun({Elem, Actor, ?ADD, Cnt}, []) ->
                        [{Elem, Actor, Cnt}];
                   ({Elem, Actor, ?ADD, _Cnt}, [{Elem, Actor, _} | _]=Acc) ->
                        Acc;
                   ({Elem, Actor, ?ADD, Cnt}, Acc) ->
                        [{Elem, Actor, Cnt} | Acc];
                   ({Elem, Actor, ?REMOVE, TSCnt}, [{Elem, Actor, AddCnt} | Acc]) when TSCnt >= AddCnt ->
                        %% tombstones will always follow all writes
                        %% for an actor/elem pair (if we go backwards
                        %% through the sorted write list!) And the
                        %% biggest Tombstone is first. Does it
                        %% dominate the biggest ADD? If so (here!) we
                        %% remove the add from the acc.
                        Acc;
                   (_Key, Acc) ->
                        %% The TS does not remove an element from the
                        %% add
                        Acc
                end,
                [],
                Keys).

%% @TODO orswot style merge, so, you know, ugly
merge({C1, Set1}, {C2, Set2}) ->
    Clock = bigset_clock:merge(C1, C2),
    {Set2Unique, Keep} = lists:foldl(fun({_Elem, Actor, Cnt}=Key, {RHSU, Acc}) ->
                                             case lists:member(Key, Set2) of
                                                 true ->
                                                     %% In both, keep
                                                     {lists:delete(Key, RHSU),
                                                      [Key | Acc]};
                                                 false ->
                                                     %% Set 1 only, did set 2 remove it?
                                                     case bigset_clock:seen(C2, {Actor, Cnt}) of
                                                         true ->
                                                             %% removed
                                                             {RHSU, Acc};
                                                         false ->
                                                             %% unseen by set 2
                                                             {RHSU, [Key | Acc]}
                                                     end
                                             end
                                     end,
                                     {Set2, []},
                                     Set1),
    %% Do it again on set 2
    InSet =  lists:foldl(fun({_Elem, Actor, Cnt}=Key, Acc) ->
                                 %% Set 2 only, did set 1 remove it?
                                 case bigset_clock:seen(C1, {Actor, Cnt}) of
                                     true ->
                                         %% removed
                                         Acc;
                                     false ->
                                         %% unseen by set 1
                                         [Key | Acc]
                                 end
                         end,
                         Keep,
                         Set2Unique),

    {Clock, InSet}.

%% Helpers @doc a non-context remove of an absent element generates a
%% precondition error, but does not mutate the state, so just ignore
%% and return original state.
ignore_preconerror_remove(Value, Actor, Set, Mod) ->
    case Mod:update({remove, Value}, Actor, Set) of
        {ok, Set2} ->
            Set2;
        _E ->
            Set
    end.

%% @doc common precondition and property, do SWOT and Set have the
%% same elements?
sets_equal(Bigset, ORSWOT) ->
    %% What matters is that both types have the exact same results.
    case lists:sort(bigset_value(Bigset)) ==
        lists:sort(riak_dt_orswot:value(ORSWOT)) of
        true ->
            true;
        _ ->
            {bigset_value(Bigset), '/=', riak_dt_orswot:value(ORSWOT)}
    end.

%% @private the value at a single replica (no need to merge, just
%% accumulate the keys)
replica_value({Clock, Keys}) ->
    Accumulated = accumulate(Keys),
    bigset_value({Clock, Accumulated}).

%% The value of an accumulated bigset
bigset_value({_Clock, Keys}) ->
    lists:usort([E || {E, _Actor, _Cnt} <- Keys]).

%% @private subset generator, takes a random subset of the given set,
%% in our case a delta buffer, that some, none, or all of, will be
%% flushed.
subset(Set) ->
    ?LET(Keep, vector(length(Set), bool()),
         return([ X || {X, true}<-lists:zip(Set, Keep)])).

%% @private the keys from some replica bigset that are to be removed
%% for element. Eseentially what a client would have read if they did
%% r=1 at replica. Equivalent to the operation {remove, Element,
%% [dots()]} where dots are the {actor, cnt} for elements that survive
%% the accumulator.
remove_keys(Element, {_Clock, Keys}) ->
    Keys2 = accumulate(Keys),
    [{E, A, ?REMOVE, C} || {E, A, C} <- Keys2, E == Element].

%% @private the tombstone writes. NOTE: we add the remove element dots
%% to our clock!
write_tombstones(RemoveKeys, Bigset) ->
    lists:foldl(fun({_E, A, ?REMOVE, C}=K, {Clock, Elements}) ->
                        C2 = bigset_clock:strip_dots({A, C}, Clock),
                        {C2, lists:usort(Elements ++ [K])}
                end,
                Bigset,
                RemoveKeys).

dump_node(Replica, {Clock, Elements}) ->
    io:format("Replica ~p~n", [Replica]),
    io:format("Clock ~p~n", [Clock]),
    io:format("Elements ~p~n", [Elements]).

%%-endif.

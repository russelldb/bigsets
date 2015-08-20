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

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(SWOT, riak_dt_delta_orswot).
-define(CLOCK, bigset_clock).
-define(SET, set).

-record(state, {replicas=[], %% Actor Ids for replicas in the system
                adds=[],      %% Elements that have been added to the set
                deltas=[], %% delta result of add/remove goes here for later replication/delivery
                delivered=[], %% track which deltas actually get delivered (for stats/funsies)
                compacted=[] %% how many keys were removed by a compaction
               }).

-record(bigset, {
          clock=?CLOCK:fresh(),
          keys=[]
         }).

-record(replica, {
          id,            %% The replica ID
          bigset=#bigset{},          %% a sort of bigset type structure
          delta_set=?SWOT:new()      %% the model delta for comparison
         }).

-record(delta, {
          bs_delta,
          dt_delta
         }).

%% The set of possible elements in the set
-define(ELEMENTS, ['A', 'B', 'C', 'D', 'X', 'Y', 'Z']).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-define(ADD, 0).
-define(REMOVE, 1).

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
    %% Don't waste time shrinking the replicas ID number
    [noshrink(nat())].

%% @doc create_replica_pre - don't create a replica that already
%% exists
-spec create_replica_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
create_replica_pre(#state{replicas=Replicas}, [Id]) ->
    not lists:member(Id, Replicas).

%% @doc create a new replica
create_replica(Id) ->
    ets:insert(?MODULE, #replica{id=Id}).

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
    [#replica{id=Replica,
              bigset=BS=#bigset{clock=Clock, keys=Keys},
              delta_set=ORSWOT}=Rep] = ets:lookup(?MODULE, Replica),

    {{Replica, Cnt}, Clock2} = bigset_clock:increment(Replica, Clock),
    Key = {Element, Replica, Cnt, ?ADD},
    Keys2 = lists:usort([Key | Keys]),

    {ok, Delta} = ?SWOT:delta_update({add, Element}, Replica, ORSWOT),
    ORSWOT2 = ?SWOT:merge(Delta, ORSWOT),
    BS2 = BS#bigset{clock=Clock2, keys=Keys2},
    ets:insert(?MODULE, Rep#replica{bigset=BS2, delta_set=ORSWOT2}),
    #delta{bs_delta=[Key], dt_delta=Delta}.

%% @doc add_next - Add the `Element' to the `adds' list so we can
%% select from it when we come to remove. This increases the liklihood
%% of a remove actuallybeing meaningful. Add to the replica's delta
%% buffer too.
-spec add_next(S :: eqc_statem:symbolic_state(),
               V :: eqc_statem:var(),
               Args :: [term()]) -> eqc_statem:symbolic_state().
add_next(S=#state{adds=Adds, deltas=Deltas}, Delta, [_Replica, Element]) ->
    S#state{adds=lists:umerge(Adds, [Element]),
            deltas=[Delta | Deltas]}.

%% ------ Grouped operator: remove
remove_args(#state{replicas=Replicas, adds=Adds}) ->
    [elements(Replicas),
     elements(Adds)].

%% @doc remove_pre - As for `remove/1'
-spec remove_pre(S :: eqc_statem:symbolic_state()) -> boolean().
remove_pre(#state{replicas=Replicas, adds=Adds}) ->
    Replicas /= [] andalso Adds /= [].

%% @doc remove_pre - Ensure correct shrinking
-spec remove_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
remove_pre(#state{replicas=Replicas, adds=Adds}, [From, Element]) ->
    lists:member(From, Replicas)
        andalso lists:member(Element, Adds).

%% @doc a dynamic precondition uses concrete state, check that the
%% `From' set contains `Element'
remove_dynamicpre(_S, [From, Element]) ->
    [#replica{id=From, delta_set=Set}] = ets:lookup(?MODULE, From),
    lists:member(Element, ?SWOT:value(Set)).

%% @doc perform a context remove using the context+element at `From'
%% and removing from. Don't mutate state, this is a read->remove, so just add the delta to the buffer!
remove(From, Element) ->
    [#replica{id=From,
              bigset=#bigset{clock=Clock, keys=Keys},
              delta_set=Set}] = ets:lookup(?MODULE, From),

    RemoveKeys = remove_keys(Element, {Clock, Keys}),

    {ok, Delta} = ?SWOT:delta_update({remove, Element}, From, Set),
    #delta{bs_delta=RemoveKeys, dt_delta=Delta}.

%% @doc remove_next - Add remove keys to the delta
%% buffer.
-spec remove_next(S :: eqc_statem:symbolic_state(),
                               V :: eqc_statem:var(),
                               Args :: [term()]) -> eqc_statem:symbolic_state().
remove_next(S=#state{deltas=Deltas}, Delta, [_From, _RemovedElement]) ->
    S#state{deltas=[Delta | Deltas]}.

%% ------ Grouped operator: replicate
%% @doc replicate_args - Choose a From and To for replication
replicate_args(#state{replicas=Replicas, deltas=Deltas}) ->
    [subset(Deltas),
     elements(Replicas)].

%% @doc replicate_pre - There must be at least on replica to replicate
-spec replicate_pre(S :: eqc_statem:symbolic_state()) -> boolean().
replicate_pre(#state{replicas=Replicas, deltas=Deltas}) ->
    Replicas /= [] andalso Deltas /= [].

%% @doc replicate_pre - Ensure correct shrinking
-spec replicate_pre(S :: eqc_statem:symbolic_state(),
                    Args :: [term()]) -> boolean().
replicate_pre(#state{replicas=Replicas, deltas=Deltas}, [Delta, To]) ->
    sets:is_subset(sets:from_list(Delta), sets:from_list(Deltas))
        andalso lists:member(To, Replicas).

%% @doc simulate replication by merging state at `To' with the delta
replicate(Delta, To) ->
    [#replica{id=To, delta_set=Set,
              bigset=BS=#bigset{clock=ToClock, keys=ToKeys}}=ToRep] = ets:lookup(?MODULE, To),
    F = fun({_Element, Actor, Cnt, ?ADD}=Key, {Clock, Writes}) ->
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
           ({_Element, Actor, Cnt, ?REMOVE}=Key, {Clock, Writes}) ->
                %% Tombstones are always written, compaction can merge
                %% them out later. But we must add the dots to the
                %% clock!!
                C2 = bigset_clock:strip_dots({Actor, Cnt}, Clock),
                {C2, [Key | Writes]}
        end,

    {BSDelta, SwotDelta} = lists:foldl(fun(#delta{bs_delta=BSD, dt_delta=DT}, {B, D}) ->
                                               {[BSD | B],
                                                ?SWOT:merge(DT, D)}
                                       end,
                                       {[], ?SWOT:new()},
                                       Delta),

    {NewClock, NewKeys} = lists:foldl(F, {ToClock, []}, lists:flatten(BSDelta)),



    Set2 = ?SWOT:merge(Set, SwotDelta),
    BS2 = BS#bigset{clock=NewClock, keys=lists:usort(NewKeys++ToKeys)},

%%    io:format("new keys ~p~n", [BS2]),

    ets:insert(?MODULE, ToRep#replica{bigset=BS2,
                                      delta_set=Set2}).

replicate_next(S=#state{delivered=Delivered}, _, [Delta, _To]) ->
    S#state{delivered=[Delta | Delivered]}.

%% --- Operation: compact ---
%% @doc compact_pre/1 - Precondition for generation
-spec compact_pre(S :: eqc_statem:symbolic_state()) -> boolean().
compact_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc compact_args - Argument generator
-spec compact_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
compact_args(#state{replicas=Replicas}) ->
    [elements(Replicas)].

%% @doc compact_pre/2 - Precondition for compact
-spec compact_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
compact_pre(#state{replicas=Replicas}, [Replica]) ->
    lists:member(Replica, Replicas).

%% @doc compact - Remove superseded adds, and redundant tombstones.
compact(Replica) ->
    %% this is the algo that level will run.  It is a lot like
    %% `accumulate' below.  Any add of an element {E, A, C} can be
    %% removed if there is some other add {E, A, C'} where C' > C.
    %% Any add {E, A, C} can be removed where there is some tombstone
    %% {E, A, C'} where C' >= C.  Any tombstone {E, A, C} can be
    %% removed if the set clock VV portion descends the dot {A, C}.
    [#replica{bigset=BS}=Rep] = ets:lookup(?MODULE, Replica),

    {BS2, Compacted} = compact_bigset(BS),

    %%    io:format("compacted ~p~n to ~p~n", [Keys, lists:usort(Keys2)]),
    ets:insert(?MODULE, Rep#replica{bigset=BS2}),
    Compacted.

%% @doc compact_next - Next state function
compact_next(S=#state{compacted=Compacted}, Value, [_Replica]) ->
    S#state{compacted=[Value | Compacted]}.

%% @doc compact_post - Postcondition for compact
%% -spec compact_post(S, Args, Res) -> true | term()
%%     when S    :: eqc_state:dynamic_state(),
%%          Args :: [term()],
%%          Res  :: term().
%% compact_post(_S, [Replica], _Val) ->
%%     bigset_value(Before) == bigset_value(After).


%% @doc weights for commands. Don't create too many replicas, but
%% prejudice in favour of creating more than 1. Try and balance
%% removes with adds. But favour adds so we have something to
%% remove. See the aggregation output.
weight(S, create_replica) when length(S#state.replicas) > 2 ->
    1;
weight(S, create_replica) when length(S#state.replicas) < 5 ->
    3;
weight(_S, remove) ->
    5;
weight(_S, add) ->
    8;
weight(_S, replicate) ->
    7;
weight(_S, compaction) ->
    4;
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
                ets:new(?MODULE, [named_table, set, {keypos, #replica.id}]),
                {H, S=#state{delivered=Delivered0, deltas=Deltas}, Res} = run_commands(?MODULE,Cmds),
                {MergedBigset, MergedSwot, BigsetLength} = lists:foldl(fun(#replica{bigset=Bigset, delta_set=ORSWOT}, {MBS, MOS, BSLen}) ->
                                                                               {BigsetCompacted, _Saving} = compact_bigset(Bigset),
                                                                               {merge_bigsets(Bigset, MBS),
                                                                                ?SWOT:merge(ORSWOT, MOS),
                                                                                bigset_length(BigsetCompacted, BSLen)}
                                                                       end,
                                                                       {#bigset{}, ?SWOT:new(), 0},
                                                                       ets:tab2list(?MODULE)),

                Delivered = lists:flatten(Delivered0),

                ets:delete(?MODULE),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                aggregate(command_names(Cmds),
                                          aggregate(S#state.compacted,
                                                    measure(deltas, length(Deltas),
                                                            measure(delivered, length(Delivered),
                                                                    measure(undelivered, length(Deltas) - length(Delivered),
                                                                            measure(replicas, length(S#state.replicas),
                                                                                    measure(bs_ln, BigsetLength,
                                                                                            measure(elements, length(?SWOT:value(MergedSwot)),
                                                                                                    conjunction([{result, Res == ok},
                                                                                                                 {equal, equals(sets_equal(MergedBigset, MergedSwot), true)}
                                                                                                                ]))))))))))

            end).

compact_bigset(#bigset{clock=Clock, keys=Keys}) ->
    Keys2 = lists:foldl(fun({E, A, _C, ?ADD}=K, [{E, A, _, ?ADD} | Tl]) ->
                                %% A later add supersedes a prior one
                                %% of same element by same actor
                                [K | Tl];
                           ({_E, _A, _C, ?ADD}=K, Acc) ->
                                %% A new element add
                                [K | Acc];
                           ({_E, A, C, ?REMOVE} = K, []) ->
                                case bigset_clock:contiguous_seen(Clock, {A, C}) of
                                    true ->
                                        [];
                                    _ ->
                                        [K]
                                end;
                           ({E, A, C, ?REMOVE}=K, [Hd | Tl]=Acc) ->
                                TombstoneSeen = bigset_clock:contiguous_seen(Clock, {A, C}),
                                case {TombstoneSeen, Hd} of
                                    {true, {E, A, _, _}} ->
                                        %% a tombstone that removes
                                        %% the last element, and is
                                        %% compacted out
                                        Tl;
                                    {true, _} ->
                                        %% A tombstone that does not
                                        %% remove the last elment, and
                                        %% is compacted out
                                        Acc;
                                    {false, {E, A, _, _}} ->
                                        %% A tombstone that removes
                                        %% the last element, and is
                                        %% retained
                                        [K | Tl];
                                    {false, Hd} ->
                                        %% A tombstone that does not
                                        %% remove anything and is
                                        %% retained
                                        [K | Acc]
                                end
                        end,
                        [],
                        Keys),
    {#bigset{clock=Clock, keys= lists:usort(Keys2)}, length(Keys) - length(Keys2)}.

bigset_length(#bigset{keys=Keys}, BSLen) ->
    max(length(Keys), BSLen).

merge_bigsets(Bigset, AccumulatedSet) ->
    #bigset{clock=Clock, keys=Keys} = Bigset,
    KeepSet = accumulate(Keys),
    merge(#bigset{clock=Clock, keys=KeepSet}, AccumulatedSet).

%% @private the vnode fold operation, can also be the eleveldb compact
%% operation. Full credit to Thomas Arts @quviq for this rather
%% ingenious roll backwards through the list.
accumulate(Keys) ->
%%    io:format("acc ~p~n", [Keys]),
    lists:foldl(fun({Elem, Actor, Cnt, ?ADD}, []) ->
                        %% first element? add to acc
                        [{Elem, Actor, Cnt}];
                   ({Elem, Actor, Cnt, ?ADD}, [{Elem, Actor, _} | Acc]) ->
                        %% Same element as already in Acc, must have
                        %% greate counter, so replace
                        [{Elem, Actor, Cnt} | Acc];
                   ({Elem, Actor, Cnt, ?ADD}, Acc) ->
                        %% New element, Add to Acc
                        [{Elem, Actor, Cnt} | Acc];
                   ({Elem, Actor, RemCnt, ?REMOVE}, [{Elem, Actor, AddCnt} | Acc]) when RemCnt >= AddCnt ->
                        %% a tombstone will always follow a write for
                        %% an actor/elem/cnt triple Does this
                        %% tombstone dominate the biggest ADD? If so
                        %% (here!) we remove the add from the acc.
                        Acc;
                   (_Key, Acc) ->
                        %% The TS does not remove an element from the
                        %% acc
                        Acc
                end,
                [],
                Keys).

%% @TODO orswot style merge, so, you know, ugly
merge(#bigset{clock=C1, keys=Set1}, #bigset{clock=C2, keys=Set2}) ->
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

    #bigset{clock=Clock, keys=InSet}.

%% @doc common precondition and property, do SWOT and Set have the
%% same elements?
sets_equal(Bigset, ORSWOT) ->
    %% What matters is that both types have the exact same results.
    case lists:sort(bigset_value(Bigset)) ==
        lists:sort(riak_dt_orswot:value(ORSWOT)) of
        true ->
            true;
        _ ->
            {bs, bigset_value(Bigset), Bigset, '/=', dt, riak_dt_orswot:value(ORSWOT), ORSWOT}
    end.

%% @private the value at a single replica (no need to merge, just
%% accumulate the keys)
replica_value(#bigset{clock=Clock, keys=Keys}) ->
    Accumulated = accumulate(Keys),
    bigset_value(#bigset{clock=Clock, keys=Accumulated}).

%% The value of an accumulated bigset
bigset_value(#bigset{keys=Keys}) ->
    lists:usort([E || {E, _A, _C} <- Keys]).

%% @private subset generator, takes a random subset of the given set,
%% in our case a delta buffer, that some, none, or all of, will be
%% flushed.
subset(Set) ->
    ?LET(Keep, vector(length(Set), bool()), %%frequency([{1, false}, {2, true}])),
         return([ X || {X, true}<-lists:zip(Set, Keep)])).

%% @private the keys from some replica bigset that are to be removed
%% for element. Eseentially what a client would have read if they did
%% r=1 at replica. Equivalent to the operation {remove, Element,
%% [dots()]} where dots are the {actor, cnt} for elements that survive
%% the accumulator.
remove_keys(Element, {_Clock, Keys}) ->
    Keys2 = accumulate(Keys),
    [{E, A, C, ?REMOVE} || {E, A, C} <- Keys2, E == Element].

-endif.

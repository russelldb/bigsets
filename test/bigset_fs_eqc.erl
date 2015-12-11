%% -------------------------------------------------------------------
%%
%% a testing place for bigset ideas (this one does full state replication)
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

-module(bigset_fs_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(SWOT, riak_dt_orswot).
-define(SET, set).

-record(state, {replicas=[], %% Actor Ids for replicas in the system
                adds=[],      %% Elements that have been added to the set
                compacted=[] %% how many keys were removed by a compaction
               }).

-record(replica, {
          id,            %% The replica ID
          bigset=bigset_model:new(),  %% a sort of bigset type structure
          set=?SWOT:new()      %% the old style orswot for comparison
         }).

%% The set of possible elements in the set
-define(ELEMENTS, ['A', 'B', 'C', 'D', 'X', 'Y', 'Z']).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

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

create_replica_post(_S, [Replica], _) ->
    post_equals(Replica).

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
              bigset=BS,
              set=ORSWOT}=Rep] = ets:lookup(?MODULE, Replica),

    %% In the absence of a client and AAAD, use the clock at the
    %% coordinating replica as the context of the add operation, any
    %% 'X' seen at this node will be removed by an add of an 'X'
    {_Delta, BS2} = bigset_model:add(Element, Replica, BS),

    {ok, ORSWOT2} = ?SWOT:update({add, Element}, Replica, ORSWOT),

    ets:insert(?MODULE, Rep#replica{bigset=BS2, set=ORSWOT2}).

%% @doc add_next - Add the `Element' to the `adds' list so we can
%% select from it when we come to remove. This increases the liklihood
%% of a remove actuallybeing meaningful. Add to the replica's delta
%% buffer too.
-spec add_next(S :: eqc_statem:symbolic_state(),
               V :: eqc_statem:var(),
               Args :: [term()]) -> eqc_statem:symbolic_state().
add_next(S=#state{adds=Adds}, _, [_Replica, Element]) ->
    S#state{adds=lists:umerge(Adds, [Element])}.

%% @doc add_post - Postcondition for add
-spec add_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
add_post(_S, [Replica, _Element], _) ->
    post_equals(Replica).

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
    [#replica{id=From, set=Set}] = ets:lookup(?MODULE, From),
    lists:member(Element, ?SWOT:value(Set)).

%% @doc perform a context remove using the context+element at `From'
%% and removing from From.
remove(From, Element) ->
    [Rep=#replica{id=From,
                  bigset=BS,
                  set=ORSWOT}] = ets:lookup(?MODULE, From),

    {ok, ORSWOT2} = ?SWOT:update({remove, Element}, From, ORSWOT),

    %% In the absence of a client and AAAD, use the clock at the
    %% coordinating replica as the context of the remove operation,
    %% any 'X' seen at this node will be removed
    {_Delta, BS2} = bigset_model:remove(Element, From, BS),
    ets:insert(?MODULE, Rep#replica{bigset=BS2, set=ORSWOT2}).

remove_post(_S, [Replica, _E], _) ->
    post_equals(Replica).

%% ------ Grouped operator: replicate @doc replicate_args - Choose
%% delta batch and target for replication. We pick a subset of the
%% delta to simulate dropped, re-ordered, repeated messages.
replicate_args(#state{replicas=Replicas}) ->
    [elements(Replicas),
     elements(Replicas)].

%% @doc replicate_pre - There must be at least one replica to replicate
-spec replicate_pre(S :: eqc_statem:symbolic_state()) -> boolean().
replicate_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc replicate_pre - Ensure correct shrinking
-spec replicate_pre(S :: eqc_statem:symbolic_state(),
                    Args :: [term()]) -> boolean().
replicate_pre(#state{replicas=Replicas}, [From, To]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas).

%% @doc simulate replication by merging state at `To' with From
replicate(From, To) ->
    [#replica{id=From, set=FromSet,
              bigset=FromBS}] = ets:lookup(?MODULE, From),
    [#replica{id=To, set=Set,
              bigset=ToBS}=ToRep] = ets:lookup(?MODULE, To),

    Set2 = ?SWOT:merge(Set, FromSet),

    BS2 = bigset_model:merge(FromBS, ToBS),

    ets:insert(?MODULE, ToRep#replica{bigset=BS2,
                                      set=Set2}).

replicate_post(_S, [_From, To], _) ->
    post_equals(To).

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
    [#replica{bigset=BS}=Rep] = ets:lookup(?MODULE, Replica),

    {BS2, Compacted} = compact_bigset(BS),

    ets:insert(?MODULE, Rep#replica{bigset=BS2}),
    {Compacted, BS, BS2}.

%% @doc compact_next - Next state function
compact_next(S=#state{compacted=Compacted}, Value, [_Replica]) ->
    S#state{compacted=[Value | Compacted]}.

%% @doc compact_post - Postcondition for compact
-spec compact_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
compact_post(_S, [Replica], {_Diff, Before, After}) ->
    case {replica_value(Before) == replica_value(After), post_equals(Replica)} of
        {true, true} ->
            true;
        {false, true} ->
            {Before, '/=', After};
        {false, E} ->
            {{Before, '/=', After}, "&&", E};
        {true, E} ->
            E
    end.

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
    ?FORALL(Cmds, more_commands(2, commands(?MODULE)),
            begin
                %% Store the state external to the statem for correct
                %% shrinking. This is best practice.
                (catch ets:delete(?MODULE)),
                ets:new(?MODULE, [named_table, set, {keypos, #replica.id}]),
                {H, S, Res} = run_commands(?MODULE,Cmds),

                {MergedBigset0, MergedSwot, BigsetLength} = lists:foldl(fun(#replica{bigset=Bigset, set=ORSWOT}, {MBS, MOS, BSLen}) ->
                                                                               BigsetCompacted = bigset_model:compact(Bigset),
                                                                               {bigset_model:merge(Bigset, MBS),
                                                                                ?SWOT:merge(ORSWOT, MOS),
                                                                                bigset_length(BigsetCompacted, BSLen)}
                                                                       end,
                                                                       {bigset_model:new(), ?SWOT:new(), 0},
                                                                       ets:tab2list(?MODULE)),

                %% @TODO(rdb) compact bigset?
                MergedBigset = bigset_model:compact(MergedBigset0),
                Compacted = [begin
                                 case element(1, V) of
                                     N when N == 0 ->
                                         same;
                                     N when N < 0 ->
                                         worse;
                                     N when N > 0 ->
                                         better
                                 end
                             end || V <- S#state.compacted],

                SizeBS = byte_size(term_to_binary(MergedBigset)),
                SizeOR = byte_size(term_to_binary(MergedSwot)),

                SizeDiff = SizeBS div SizeOR,

                ets:delete(?MODULE),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                aggregate(command_names(Cmds),
                                          aggregate(with_title('Compaction Effect'), Compacted,
                                                    measure(commands, length(Cmds),
                                                            measure(size_diff, SizeDiff,
                                                                    measure(replicas, length(S#state.replicas),
                                                                            measure(bs_ln, BigsetLength,
                                                                                    measure(elements, length(?SWOT:value(MergedSwot)),
                                                                                            conjunction([{result, Res == ok},
                                                                                                         {equal, equals(sets_equal(MergedBigset, MergedSwot), true)}
                                                                                                        ])))))))))

            end).


%% this is the algo that level will run.
%%
%% Any add of E with dot {A, C} can be removed if it is dominated by
%% the context of any other add of E. Or put another way, merge all
%% the contexts for E,(adds and removes) and remove all {A,C} Adds
%% that are dominated. NOTE: the contexts of removed adds must be
%% retained in a special per-element tombstone if the set clock does
%% not descend them.
%%
%% Tombstones:: write the fully merged context from the fold over E as
%% a per-element tombstone, remove all others. Remove the per-element
%% tombstone if it's value (Ctx) is descended by the set clock.

%% @NOTE(rdb) all this will have a profound effect on AAE/Read Repair,
%% so re-think it then. (Perhaps "filling" from the set clock in that
%% event?)
compact_bigset(BS) ->
    Before = bigset_model:size(BS),
    Compacted = bigset_model:compact(BS),
    After = bigset_model:size(Compacted),
    {Compacted, Before-After}.

bigset_length(BS, BSLen) ->
    max(bigset_model:size(BS), BSLen).

%% @doc common precondition and property, do SWOT and Set have the
%% same elements?
sets_equal(Bigset, ORSWOT) ->
    %% What matters is that both types have the exact same results.
    BSVal = bigset_model:value(Bigset),
    ORVal = ?SWOT:value(ORSWOT),
    case lists:sort(BSVal) == lists:sort(ORVal) of
        true ->
            true;
        _ ->
            {bs, BSVal, Bigset, '/=', dt, ORVal, ORSWOT}
    end.

%% @private the value at a single replica (no need to merge, just
%% accumulate the keys)
replica_value(BS) ->
    bigset_model:value(BS).

%% @private subset generator, takes a random subset of the given set,
%% in our case a delta buffer, that some, none, or all of, will be
%% flushed.
subset(Set) ->
    ?LET(Keep, vector(length(Set), bool()), %%frequency([{1, false}, {2, true}])),
         return([ X || {X, true}<-lists:zip(Set, Keep)])).


post_equals(Replica) ->
     [#replica{id=Replica,
              bigset=BS,
              set=ORSWOT}] = ets:lookup(?MODULE, Replica),
    case sets_equal(BS, ORSWOT) of
        true ->
            true;
        Fail ->
            {Replica, Fail}
    end.

-endif.

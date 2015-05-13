%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% logic for mergig a streamed set of values
%%% defaults to r=2, n=3, basic_quorum=false, notfound_ok=true
%%% @end
%%% Created :  6 May 2015 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(bigset_read_core).

%% API
-compile([export_all]).

-record(actor,
        {
          partition :: pos_integer(),
          clock=bigset_clock:fresh() :: bigset_clock:clock(),
          elements=[]:: [{binary(), riak_dt_vclock:dot()}],
          not_found = true :: boolean(),
          done = false :: boolean()
        }).

-record(state,
        {
          r :: pos_integer(),
          actors=orddict:new() :: [#actor{}],
          clocks = 0 :: non_neg_integer(),
          not_founds = 0 :: non_neg_integer(),
          done = 0 :: non_neg_integer()
        }).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

new(R) ->
    #state{r=R}.


%% @doc call when a clock is received
clock(Partition, Clock, Core) ->
    Actor = get_actor(Partition, Core),
    Actor2 = add_clock(Actor, Clock),
    Core2=#state{clocks=Clocks} = set_actor(Partition, Actor2, Core),
    Core2#state{clocks=Clocks+1}.

%% @doc do we have enough clocks?  this is R clocks, with
%% notfound_ok. So if first answer was notfound and second was a
%% clock, we have R, and if first and second was a clock, we have
%% R. Else we don't!
r_clocks(#state{clocks=0}) ->
    false;
r_clocks(#state{clocks=Clocks, not_founds=NotFounds, r=R}) when Clocks + NotFounds >= R ->
    true;
r_clocks(_S) ->
    false.

%% @doc update the state with a notfound_result for `Partition'
not_found(Partition, Core) ->
    Actor = get_actor(Partition, Core),
    Actor2 = set_not_found(Actor),
    Core2=#state{not_founds=NF} = set_actor(Partition, Actor2, Core),
    Core2#state{not_founds=NF+1}.

%% @doc is the set a notfound? This means the first two results
%% received were notfound
not_found(#state{not_founds=R, r=R}) ->
    true;
not_found(_S) ->
    false.

%% @doc add elements for a partition
elements(Partition, Elements, Core) ->
    Actor = get_actor(Partition, Core),
    Actor2 = append_elements(Actor, Elements),
    set_actor(Partition, Actor2, Core).

%% @doc set a partition as done
done(Partition, Core) ->
    Actor = get_actor(Partition, Core),
    Actor2 = set_done(Actor),
    Core2=#state{done=Done}=set_actor(Partition, Actor2, Core),
    Core2#state{done=Done+1}.

is_done(#state{not_founds=NF, done=Done, r=R}) when NF+Done == R ->
    true;
is_done(_S) ->
    false.

%% @perform a CRDT orswot merge
finalise(#state{actors=Actors}) ->
    merge(Actors, undefined).

merge([], Elements) ->
    Elements;
merge([{_, Actor} | Rest], undefined) ->
    merge(Rest, Actor);
merge([{_P, Actor} | Rest], Mergedest) ->
    M2 = orswot_merge(Actor, Mergedest),
    merge(Rest, M2).

orswot_merge(A1, A2) ->
    #actor{elements=E1, clock=C1} = A1,
    #actor{elements=E2, clock=C2} = A2,
    Clock = bigset_clock:merge(C1, C2),

    %% ugly cut and paste from before
    {E2Unique, Keeps} = lists:foldl(fun({E, Dots}, {E2Remains, Acc}) ->
                        case lists:keytake(E, 1, E2Remains) of
                            false ->
                                %% Only present on one side, filter
                                %% the dots
                                Acc2 = filter_element(E, Dots, C2, Acc, Clock),
                                {E2Remains, Acc2};
                            {value, {E, Dots2}, NewE2} ->
                                Acc2 = merge_element(E, {Dots, C1}, {Dots2, C2}, Acc, Clock),
                                {NewE2, Acc2}
                        end
                end,
                {E2, []},
                E1),
    E2Keeps = lists:foldl(fun({E, Dots}, Acc) ->
                                  %% Only present on one side, filter
                                  %% the dots
                                  filter_element(E, Dots, C1, Acc)
                          end,
                          [],
                          E2Unique),
    Elements = lists:umerge(lists:reverse(Keeps), lists:reverse(E2Keeps)),
    #actor{clock=Clock, elements=Elements}.

acc_fun(Acc, Clock) ->
    %% A fun to compress dots for an element if needed
    fun({Element, Dots}) ->
            case bigset_clock:subtract_base_seen(Clock, Dots) of
                [] ->
                    [{Element, <<>>} | Acc];
                PerElemDots ->
                    [{Element, rle_dots(Clock, PerElemDots)} | Acc]
            end
    end.



%% @private if `Clock' as seen all `Dots' return Acc, otherwise add
%% `Element' and unseen/surviving dots to `Acc' and return.
filter_element(Element, Dots, Clock, Acc) ->
    case bigset_clock:subtract_seen(Clock, Dots) of
        [] ->
            %% Removed, do not keep
            Acc;
        SurvivingDots ->
            %% @TODO in this proto 2 is the
            %% must sets we will merge, so
            %% we can binary/compress dots
            %% here
            [{Element, SurvivingDots} | Acc]
    end.

%% @private must be a better way, eh?  If a dot is present in both LHS
%% and RHS dots, then keep that dot, if a dot is only on one side,
%% keep it only if the otherside has not seen it (and therefore
%% removed it) if following this filtering of dots, any dot remains,
%% accumulate the element. If no dots are common or unseen, discard
%% the element.
merge_element(Element, {LHSDots, LHSClock}, {RHSDots, RHSClock}, Acc) ->
    %% On both sides
    CommonDots = sets:intersection(sets:from_list(LHSDots), sets:from_list(RHSDots)),
    LHSUnique = sets:to_list(sets:subtract(sets:from_list(LHSDots), CommonDots)),
    RHSUnique = sets:to_list(sets:subtract(sets:from_list(RHSDots), CommonDots)),
    LHSKeep = bigset_clock:subtract_seen(RHSClock, LHSUnique),
    RHSKeep = bigset_clock:subtract_seen(LHSClock, RHSUnique),
    V = riak_dt_vclock:merge([sets:to_list(CommonDots), LHSKeep, RHSKeep]),
    %% Perfectly possible that an item in both sets should be dropped
    case V of
        [] ->
            %% Removed from both sides, do not accumulate
            Acc;
        _ ->
            %% @TODO again here could maybe compress dots for smaller
            %% datas
            [{Element, V} | Acc]
    end.

set_actor(Partition, Actor, State) ->
    #state{actors=Actors} = State,
    State#state{actors=orddict:store(Partition, Actor,  Actors)}.

get_actor(Partition, State) ->
    #state{actors=Actors} = State,
    case orddict:find(Partition, Actors) of
        error ->
            #actor{partition=Partition};
        {ok, Actor} ->
            Actor
    end.

set_not_found(Actor) ->
    Actor#actor{not_found=true}.

set_done(Actor) ->
    Actor#actor{done=true}.

append_elements(Actor, Elements) ->
    #actor{elements=E} = Actor,
    Actor#actor{elements=lists:umerge(Elements, E)}.

add_clock(Actor, Clock) ->
    Actor#actor{clock=Clock}.

-ifdef(TEST).

done_test() ->
    State = #state{actors=[{P, #actor{done=true, partition=P}} || P <- [1, 2]]},
    ?assert(is_done(State)).

-endif.

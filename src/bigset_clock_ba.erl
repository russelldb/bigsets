%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%%
%%% @end

-module(bigset_clock_ba).

-export([
         add_dot/2,
         add_dots/2,
         all_nodes/1,
         complement/2,
         fresh/0,
         fresh/1,
         get_dot/2,
         increment/2,
         intersection/2,
         is_compact/1,
         merge/1,
         merge/2,
         seen/2,
         subtract_seen/2,
         to_bin/1
        ]).

-compile(export_all).

-export_type([clock/0, dot/0]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([make_dotcloud_entry/3]).
-endif.

%% lazy inefficient dot cloud of dict Actor->[count()]
-type actor() :: riak_dt_vclock:actor().
-type clock() :: {riak_dt_vclock:vclock(), dotcloud()}.
-type dot() :: riak_dt:dot().
-type dotcloud() :: [{riak_dt_vclock:actor(), bigset_bitarray:bit_array()}].

-define(DICT, orddict).

-spec to_bin(clock()) -> binary().
to_bin(Clock) ->
    term_to_binary(Clock, [compressed]).

-spec fresh() -> clock().
fresh() ->
    {riak_dt_vclock:fresh(), ?DICT:new()}.

fresh({Actor, Cnt}) ->
    {riak_dt_vclock:fresh(Actor, Cnt), ?DICT:new()}.

%% @doc increment the entry in `Clock' for `Actor'. Return the new
%% Clock, and the `Dot' of the event of this increment. Works because
%% for any actor in the clock, the assumed invariant is that all dots
%% for that actor are contiguous and contained in this clock (assumed
%% therefore that `Actor' stores this clock durably after increment,
%% see riak_kv#679 for some real world issues, and mitigations that
%% can be added to this code.)
-spec increment(actor(), clock()) ->
                       {dot(), clock()}.
increment(Actor, {Clock, Seen}) ->
    Clock2 = riak_dt_vclock:increment(Actor, Clock),
    Cnt = riak_dt_vclock:get_counter(Actor, Clock2),
    {{Actor, Cnt}, {Clock2, Seen}}.

get_dot(Actor, {Clock, _Dots}) ->
    {Actor, riak_dt_vclock:get_counter(Actor, Clock)}.

all_nodes({Clock, Dots}) ->
    %% NOTE the riak_dt_vclock:all_nodes/1 returns a sorted list
    lists:usort(lists:merge(riak_dt_vclock:all_nodes(Clock),
                 ?DICT:fetch_keys(Dots))).

-spec merge(clock(), clock()) -> clock().
merge({VV1, Seen1}, {VV2, Seen2}) ->
    VV = riak_dt_vclock:merge([VV1, VV2]),
    Seen = ?DICT:merge(fun(_Key, S1, S2) ->
                               bigset_bitarray:merge(S1, S2)
                       end,
                       Seen1,
                       Seen2),
    compress_seen(VV, Seen).

merge(Clocks) ->
    lists:foldl(fun merge/2,
                fresh(),
                Clocks).

%% @doc make a bigset clock from a version vector
-spec from_vv(riak_dt_vclock:vclock()) -> clock().
from_vv(Clock) ->
    {Clock, ?DICT:new()}.

%% @doc given a `Dot :: riak_dt:dot()' and a `Clock::clock()',
%% add the dot to the clock. If the dot is contiguous with events
%% summerised by the clocks VV it will be added to the VV, if it is an
%% exception (see DVV, or CVE papers) it will be added to the set of
%% gapped dots. If adding this dot closes some gaps, the seen set is
%% compressed onto the clock.
-spec add_dot(dot(), clock()) -> clock().
add_dot(Dot, {Clock, Seen}) ->
    Seen2 = add_dot_to_cloud(Dot, Seen),
    compress_seen(Clock, Seen2).

add_dot_to_cloud({Actor, Cnt}, Cloud) ->
    ?DICT:update(Actor,
                 fun(Dots) ->
                         bigset_bitarray:set(Cnt, Dots)
                 end,
                 [Cnt],
                 Cloud).

%% @doc given a list of `dot()' and a `Clock::clock()',
%% add the dots from `Dots' to the clock. All dots contiguous with
%% events summerised by the clocks VV it will be added to the VV, any
%% exceptions (see DVV, or CVE papers) will be added to the set of
%% gapped dots. If adding a dot closes some gaps, the seen set is
%% compressed onto the clock.
-spec add_dots([dot()], clock()) -> clock().
add_dots(Dots, {Clock, Seen}) ->
    Seen2 = lists:foldl(fun add_dot_to_cloud/2,
                        Seen,
                        Dots),
    compress_seen(Clock, Seen2).

-spec seen(dot(), clock()) -> boolean().
seen({Actor, Cnt}=Dot, {Clock, Seen}) ->
    (riak_dt_vclock:descends(Clock, [Dot]) orelse
     bigset_bitarray:member(Cnt, fetch_dot_list(Actor, Seen))).

fetch_dot_list(Actor, Seen) ->
    case ?DICT:find(Actor, Seen) of
        error ->
            bigset_bitarray:new(1000);
        {ok, L} ->
            L
    end.

%% Remove dots seen by `Clock' from `Dots'. Return a list of `dot()'
%% unseen by `Clock'. Return `[]' if all dots seens.
subtract_seen(Clock, Dots) ->
    %% @TODO(rdb|optimise) this is maybe a tad inefficient.
    lists:filter(fun(Dot) ->
                         not seen(Dot, Clock)
                 end,
                 Dots).

%% Remove `Dots' from `Clock'. Any `dot()' in `Dots' that has been
%% seen by `Clock' is removed from `Clock', making the `Clock' un-see
%% the event.
subtract(Clock, Dots) ->
    lists:foldl(fun(Dot, Acc) ->
                        subtract_dot(Acc, Dot) end,
                Clock,
                Dots).

%% Remove an event `dot()' `Dot' from the clock() `Clock', effectively
%% un-see `Dot'.
subtract_dot(Clock, Dot) ->
    {VV, DotCloud} = Clock,
    {Actor, Cnt} = Dot,
    DotList = fetch_dot_list(Actor, DotCloud),
    case bigset_bitarray:member(Cnt, DotList) of
        %% Dot in the dot cloud, remove it
        true ->
            {VV, delete_dot(Dot, DotList, DotCloud)};
        false ->
            %% Check the clock
            case riak_dt_vclock:get_counter(Actor, VV) of
                N when N >= Cnt ->
                    %% Dot in the contiguous counter Remove it by
                    %% adding > cnt to the Dot Cloud, and leaving
                    %% less than cnt in the base
                    NewBase = Cnt-1,
                    NewDots = lists:seq(Cnt+1, N),
                    NewVV = riak_dt_vclock:set_counter(Actor, NewBase, VV),
                    NewDC = case NewDots of
                                [] ->
                                    DotCloud;
                                _ ->
                                    orddict:store(Actor, bigset_bitarray:set_all(NewDots, DotList), DotCloud)
                            end,
                    {NewVV, NewDC};
                _ ->
                    %% NoOp
                    Clock
            end
    end.

delete_dot({Actor, Cnt}, DotList, DotCloud) ->
    DL2 = bigset_bitarray:unset(Cnt, DotList),
    case bigset_bitarray:size(DL2) of
        0 ->
            orddict:erase(Actor, DotCloud);
        _ ->
            orddict:store(Actor, DL2, DotCloud)
    end.

compress_seen(Clock, Seen) ->
    ?DICT:fold(fun(Node, DC, {ClockAcc, SeenAcc}) ->
                       Cnt = riak_dt_vclock:get_counter(Node, Clock),
                       case compress(Cnt, DC) of
                           {Cnt, _} ->
                               {ClockAcc, SeenAcc};
                           {Cnt2, []} ->
                               {riak_dt_vclock:merge([[{Node, Cnt2}], ClockAcc]),
                                SeenAcc};
                           {Cnt2, DC2} ->
                               {riak_dt_vclock:merge([[{Node, Cnt2}], ClockAcc]),
                                ?DICT:store(Node, DC2, SeenAcc)}
                       end
               end,
               {Clock, ?DICT:new()},
               Seen).

compress(Base, BitArray) ->
    case bigset_bitarray:get(Base+1, BitArray) of
        true ->
            compress(Base+1, bigset_bitarray:unset(Base+1, BitArray));
        false ->
            case bigset_bitarray:size(BitArray) of
                0 ->
                    {Base, []};
                _ -> {Base, BitArray}
            end
    end.

%% @doc intersection is all the dots in A that are also in B. A is an
%% orddict of [{actor, [dot()]}] as returned by `complement/2'
-spec intersection(dotcloud(), clock()) -> clock().
intersection(_DotCloud, _Clock) ->
    ok.

%% @doc complement like in sets, only here we're talking sets of
%% events. Generates a dict that represents all the events in A that
%% are not in B. We actually assume that B is a subset of A, so we're
%% talking about B's complement in A. Super subtract Sub
%% Returns a dot-cloud
-spec complement(clock(), clock()) -> dotcloud().
complement(Super, Sub) ->
    {SupVV, SupDC} = Super,
    {SubVV, SubDC} = Sub,
    %% This is horrendously ineffecient, we need to use math/bit
    %% twiddling to find a better way.
    Actors = all_nodes(Super),
    lists:foldl(fun(Actor, Acc) ->
                        Base = riak_dt_vclock:get_counter(Actor, SupVV),
                        Dots = fetch_dot_list(Actor, SupDC),
                        SubDots = fetch_dot_list(Actor, SubDC),
                        %% Base must be >= SubBase, always
                        %% Remove from B's dot set everything covered by Base and in in Dots
                        DelDots = ordsets:subtract(ordsets:from_list(ADots), ordsets:from_list(BDots)),
                        %% all the dots in A between BBase and ABase
                        ABaseDots = lists:seq(BBase+1, ABase),
                        %% all the dots in B between BBase and ABase
                        BDotsInABase = lists:takewhile(fun(X) -> X =< ABase end, BDots),
                        %% The dots not in B that are in A between BBase and ABase
                        BaseDeleted = ordsets:subtract(ordsets:from_list(ABaseDots), ordsets:from_list(BDotsInABase)),
                        Deleted = ordsets:union(DelDots, BaseDeleted),
                        [{Actor, ordsets:to_list(Deleted)} | Acc]
                end,
                [],
                Actors).
    

%% @doc Is this clock compact, i.e. no gaps/no dot-cloud entries
-spec is_compact(clock()) -> boolean().
is_compact({_Base, DC}) ->
    is_compact_dc(DC).

is_compact_dc([]) ->
    true;
is_compact_dc([{_A, DC} | Rest]) ->
    case bigset_bitarray:size(DC) of
        0 ->
            is_compact_dc(Rest);
        _ ->
            false
    end.

-ifdef(TEST).

%% API for comparing clock impls

%% @doc given a clock,actor and list of events, return a clock where
%% the dotcloud for the actor contains the events
-spec make_dotcloud_entry(clock(), actor(), [pos_integer()]) -> clock().
make_dotcloud_entry({Base, Seen}=_Clock, Actor, Events) ->
    {Base, orddict:store(Actor, bigset_bitarray:from_list(Events), Seen)}.

%% How big are clocks?
clock_size_test() ->
    bigset_gen_clock:clock_size_test(?MODULE).

-endif.



%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Basho
%%% @doc
%%% reads, queries, is_member etc
%%% @end
%%% Created : 2 Jun 2016 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(bigset_query_fsm).

-behaviour(gen_fsm).

-include("bigset.hrl").

%% API
-export([start_link/1]).

%% gen_fsm callbacks
-export([init/1, prepare/2,  validate/2, read/2,
         await_set/2, reply/2, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {req_id :: reqid(),
                from :: pid(),
                set :: binary(), %% Set name, think bucket for riak
                members :: [member()], %% for a subset query
                preflist :: riak_core_apl:preflist(),
                %% default to r=2 for demo/proto, defaults to
                %% notfound_ok=true, too
                results = [],
                options=[] :: list(),
                timer=undefined :: reference() | undefined,
                reply = undefined
               }).

-type state() :: #state{}.
-type result() :: {message(), partition(), from()}.
-type message() :: not_found | {set, clock(), elements(), done}.
-type from() :: {pid(), reference()}.
-type elements() :: [{Member :: binary(), [Dot :: riak_dt_vclock:dot()]}].
-type clock() :: bigset_clock:clock().
-type partition() :: non_neg_integer().
-type reqid() :: term().

-define(DEFAULT_TIMEOUT, 60000).

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link(?QUERY_FSM_ARGS{}) -> {ok, pid()}.
start_link(Args=?QUERY_FSM_ARGS{}) ->
    gen_fsm:start_link(?MODULE, [Args], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
-spec init(?READ_FSM_ARGS{}) -> {ok, prepare, #state{}, 0}.
init([Args]) ->
    State = state_from_read_fsm_args(Args),
    {ok, prepare, State, 0}.

%% copy the incoming args into the internal state
-spec state_from_read_fsm_args(?QUERY_FSM_ARGS{}) -> #state{}.
state_from_read_fsm_args(?QUERY_FSM_ARGS{}=Args) ->
    ?QUERY_FSM_ARGS{req_id=ReqId,
                   from=From,
                   set=Set,
                   members=Members,
                   options=Options} = Args,
    #state{req_id=ReqId, from=From, set=Set, members=Members, options=Options}.

-spec prepare(timeout, state()) -> {next_state, validate, state(), 0}.
prepare(timeout, State) ->
    #state{options=Options, set=Set} = State,
    Hash = riak_core_util:chash_key({bigset, Set}),
    PL = riak_core_apl:get_apl(Hash, 3, bigset),
    Timeout = proplists:get_value(timeout, Options, ?DEFAULT_TIMEOUT),
    TRef = schedule_timeout(Timeout),
    {next_state, validate, State#state{preflist=PL, timer=TRef}, 0}.

-spec validate(timeout | request_timeout, state()) ->
                      {next_state, coordinate | reply, state(), 0}.
validate(request_timeout, State) ->
        {next_state, reply, State#state{reply={error, timeout}}};
validate(timeout, State) ->
    #state{preflist=PL} = State,
    case length(PL) of
        N when N < 2 ->
            {next_state, reply, State#state{reply={error, too_few_vnodes}}, 0};
        _ ->
            {next_state, read, State, 0}
    end.

-spec read(timeout, state()) -> {next_state, await_set, state()}.
read(request_timeout, State) ->
    {next_state, reply, State#state{reply={error, timeout}}, 0};
read(timeout, State) ->
    #state{preflist=[N1, _N2, N3], set=Set, members=Members} = State,
    Req = ?CONTAINS_REQ{set=Set, members=Members},
    bigset_vnode:contains([N1, N3], Req),

    {next_state, await_set, State}.

-spec await_set(result() | request_timeout, state()) ->
                       {next_state, reply, state()} |
                       {next_state, await_set, state()}.
await_set(request_timeout, State) ->
    {next_state, reply, State#state{reply={error, timeout}}, 0};
await_set({Result, _Partition, _From}, State) ->
    #state{results=Results0} = State,
    Results = [Result | Results0],
    case length(Results) of
        2 ->
            Reply = merge_results(Results),
            {next_state, reply, State#state{reply=Reply, results=Results}, 0};
        _ ->
            {next_state, await_set, State#state{results=Results}}
    end.

-spec reply(timeout, State) -> {stop, normal, State}.
reply(_, State=#state{reply=not_found}) ->
    #state{from=From, req_id=ReqId} = State,
    From ! {ReqId, not_found},
    {stop, normal, State};
reply(_, State=#state{reply={set, _Clock, Elements, done}}) ->
    #state{from=From, req_id=ReqId} = State,
    From ! {ReqId, Elements},
    {stop, normal, State}.


handle_event(_Event, _StateName, State) ->
    {stop, badmsg, State}.

handle_sync_event(_Event, _From, _StateName, State) ->
    {stop, badmsg, State}.

handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
handle_info(_Info, _StateName, State) ->
    {stop, badmsg, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec merge_results([message()]) -> not_found | {set, [{member(), dot_list()}]}.
merge_results(Results) ->
    lists:foldl(fun(not_found=_Result, not_found=_Acc) ->
                        not_found;
                   (not_found=_Result, {set, _Clock, _Elements, done}=Acc) ->
                        Acc;
                   ({set, _Clock, _Elements, done}=Res, not_found=_Acc) ->
                        Res;
                   ({set, Clock1, Elements1, done}, {set, Clock2, Elements2, done}) ->
                        {Clock, Elements} = merge_set(Clock1, Elements1, Clock2, Elements2),
                        {set, Clock, Elements, done}
                end,
                not_found,
                Results).

%% @priv Just that same old orswot merge, again
-spec merge_set(bigset_clock:clock(), [{member(), dot_list()}],
                bigset_clock:clock(), [{member(), dot_list()}]) ->
                       [{member(), dot_list()}].
merge_set(Clock1, Elements1, Clock2, Elements2) ->
    {E1Keep, E2Unique} = orddict:fold(fun(Element, Dots1, {Keep, E2Unique}) ->
                                              case orddict:find(Element, Elements2) of
                                                  {ok, Dots2} ->
                                                      %% In both sides, keep it, maybe
                                                      SurvivingDots = merge_dots(Dots1, Dots2, Clock1, Clock2),
                                                      {maybe_store_dots(Element, SurvivingDots, Keep),
                                                       orddict:erase(Element, E2Unique)};
                                                  error ->
                                                      %% Only keep if not seen/removed by other clock
                                                      SurvivingDots = filter_dots(Dots1, Clock2),
                                                      Keep2 = maybe_store_dots(Element, SurvivingDots, Keep),
                                                      {Keep2, E2Unique}
                                              end
                                      end,
                                      {orddict:new(), Elements2},
                                      Elements1),

    %% Filter the unique elements left in elements2
    Elements = orddict:fold(fun(Element, Dots, Keep) ->
                                    SurvivingDots = filter_dots(Dots, Clock1),
                                    maybe_store_dots(Element, SurvivingDots, Keep)
                            end,
                            E1Keep,
                            E2Unique),
    {bigset_clock:merge(Clock1, Clock2), Elements}.

%% @priv returns `Dots' with all dots seen by `Clock' removed.
-spec filter_dots(dot_list(), bigset_clock:clock()) -> dot_list().
filter_dots(Dots, Clock) ->
    lists:filter(fun(Dot) ->
                         not bigset_clock:seen(Dot, Clock)
                 end,
                 Dots).

%% @priv return a `dot_list()' of the surviving dots from `Dots1' and
%% `Dots2' where to survive means a dot is either: in both lists, or
%% in only one list and is unseen by the opposite clock. The
%% intersection of `Dots1' and `Dots2' plus the results of filtering
%% the two remaining subsets against a clock.
-spec merge_dots(dot_list(), dot_list(), bigset_clock:clock(), bigset_clock:clock()) -> dot_list().
merge_dots(Dots1, Dots2, Clock1, Clock2) ->
    {Keep, Remaining} = lists:foldl(fun(Dot, {Keep, D2Unique}) ->
                                            case lists:member(Dot, Dots2) of
                                                true ->
                                                    {[Dot | Keep], lists:delete(Dot, D2Unique)};
                                                false ->
                                                    {maybe_add_dot(Dot, Clock2, Keep), D2Unique}
                                            end
                                    end,
                                    {[], Dots2},
                                    Dots1),
    lists:foldl(fun(Dot, Acc) ->
                        maybe_add_dot(Dot, Clock1, Acc)
                end,
                Keep,
                Remaining).

%% @priv add `Dot' to `Acc' if it is _not_ seen by `Clock'. Return
%% `Acc' updated (or not!)
-spec maybe_add_dot(bigset_clock:dot(), bigset_clock:clock(), dot_list()) -> dot_list().
maybe_add_dot(Dot, Clock, Acc) ->
    case bigset_clock:seen(Dot, Clock) of
        true ->
            Acc;
        false ->
            lists:umerge([Dot], Acc)
    end.

maybe_store_dots(_Elements, []=_Dots, Orddict) ->
    Orddict;
maybe_store_dots(Element, Dots, Orddict) ->
    orddict:store(Element, Dots, Orddict).

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), request_timeout).

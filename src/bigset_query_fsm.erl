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
         await_set/2, reply/2, repair/2, handle_event/3,
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
                reply = undefined,
                repair = [] :: repairs()
               }).

-type partition() :: pos_integer().
-type state() :: #state{}.
-type result() :: {message(), partition(), from()}.
-type message() :: not_found | {set, clock(), elements(), done}.
-type from() :: {pid(), reference()}.
-type clock() :: bigset_clock:clock().
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
    #state{preflist=PL, set=Set, members=Members} = State,
    Req = ?CONTAINS_REQ{set=Set, members=Members},
    bigset_vnode:contains(PL, Req),
    {next_state, await_set, State}.

-spec await_set(result() | request_timeout, state()) ->
                       {next_state, reply, state()} |
                       {next_state, await_set, state()}.
await_set(request_timeout, State) ->
    {next_state, reply, State#state{reply={error, timeout}}, 0};
await_set({Result, Partition, _From}, State) ->
    #state{results=Results0} = State,
    Results = [{Partition, Result} | Results0],
    case length(Results) of
        2 ->
            {Repairs, {_Id, Reply}} = merge_results(Results),
            lager:info("repairs are ~p", [Repairs]),
            {next_state, reply, State#state{repair=Repairs, reply=Reply, results=Results}, 0};
        _ ->
            {next_state, await_set, State#state{results=Results}}
    end.

-spec reply(timeout, #state{}) -> {stop, normal, #state{}} |
                                  {next_state, repair, #state{}, 0}.
reply(_, State=#state{reply=not_found}) ->
    #state{from=From, req_id=ReqId} = State,
    From ! {ReqId, not_found},
    {stop, normal, State};
reply(_, State=#state{reply={set, _Clock, Elements, done}}) ->
    #state{from=From, req_id=ReqId} = State,
    From ! {ReqId, Elements},
    lager:info("should be repair", []),
    {next_state, repair, State, 0}.

-spec repair(timeout, #state{}) -> {stop, normal, #state{}}.
repair(timeout, State) ->
    %% send each repair to the relevant node
    #state{repair=Repairs, preflist=PL, set=Set} = State,
    lager:info("sending ~p", [Repairs]),
    [read_repair(Set, Repair, PL) || Repair <- Repairs],
    {stop, normal, State}.

read_repair(_Set, {_Partition, []}, _PL) ->
    ok;
read_repair(Set, {Partition, Repairs}, PL) ->
    lager:info("repairing ~p", [Partition]),
    case lists:keyfind(Partition, 1, PL) of
        false ->
            ok;
        Entry ->
            lager:info("calling repair on ~p", [Partition]),
            bigset_vnode:repair([Entry], ?REPAIR_REQ{set=Set, repairs=Repairs})
    end.


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
-spec merge_results([message()]) -> {repairs() ,
                                     Result :: not_found | {set, [{member(), dot_list()}]}}.
merge_results(Results) ->
    lists:foldl(fun({_P1, not_found}=_Result, {_P2, not_found}=_Acc) ->
                        {[], not_found};
                   ({P1, not_found}=_Result, {_P2, {set, _Clock, Elements, done}}=Acc) ->
                        {[{P1, Elements}], Acc};
                   ({_P1, {set, _Clock, Elements, done}}=Res, {P2, not_found}=_Acc) ->
                        {[{P2, Elements}], Res};
                   ({P1, {set, Clock1, Elements1, done}}, {P2, {set, Clock2, Elements2, done}}) ->
                        {Repairs, {Id, Clock, Elements}} = bigset_read_merge:merge_sets([{P1, Clock1, Elements1}, {P2, Clock2, Elements2}]),
                        {Repairs, {Id, {set, Clock, Elements, done}}}
                end,
                hd(Results),
                tl(Results)).

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), request_timeout).

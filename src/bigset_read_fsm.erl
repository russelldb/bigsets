%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 12 Jan 2015 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(bigset_read_fsm).

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
                preflist :: riak_core_apl:preflist(),
                %% default to r=2 for demo/proto, defaults to
                %% notfound_ok=true, too
                logic = bigset_read_core:new(2),
                options=[] :: list(),
                timer=undefined :: reference() | undefined,
                reply = undefined
               }).

-type state() :: #state{}.
-type result() :: {message(), partition(), from()}.
-type message() :: not_found | {clock, clock()} |
                   done | {elements, elements()}.
-type from() :: {pid(), reference()}.
-type clock() :: bigset_clock:clock().
-type partition() :: non_neg_integer().
-type reqid() :: term().

-define(DEFAULT_TIMEOUT, 60000).

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link(?READ_FSM_ARGS{}) -> {ok, pid()}.
start_link(Args=?READ_FSM_ARGS{}) ->
    gen_fsm:start_link(?MODULE, [Args], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
-spec init(?READ_FSM_ARGS{}) -> {ok, prepare, #state{}, 0}.
init([Args]) ->
    State = state_from_read_fsm_args(Args),
    {ok, prepare, State, 0}.

%% copy the incoming args into the internal state
-spec state_from_read_fsm_args(?READ_FSM_ARGS{}) -> #state{}.
state_from_read_fsm_args(?READ_FSM_ARGS{}=Args) ->
    ?READ_FSM_ARGS{req_id=ReqId,
                   from=From,
                   set=Set,
                   options=Options} = Args,
    #state{req_id=ReqId, from=From, set=Set, options=Options}.

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

-spec read(timeout, state()) -> {next_state, await_clocks, state()}.
read(request_timeout, State) ->
    {next_state, reply, State#state{reply={error, timeout}}, 0};
read(timeout, State) ->
    %% A read request
    #state{preflist=PL, set=Set} = State,
    Req = ?READ_REQ{set=Set},
    bigset_vnode:read(PL, Req),
    {next_state, await_clocks, State}.

-spec await_set(result(), #state{}) -> term().
await_set(request_timeout, State) ->
    {next_state, reply, State#state{reply={error, timeout}}, 0};
await_set({Message=?READ_RESULT{}, Partition, From}, State) ->
    #state{logic=Core} = State,
    ack_or_stop(Partition, From, Core),
    #state{logic=Core} = State,
    {{Repairs, Result}, Core2} = bigset_read_core:handle_message(Partition, Message, Core),
    maybe_send_results(Result, State),
    maybe_send_repairs(Repairs, State),

    case bigset_read_core:is_finished(Core2) of
        true ->
            {{FinalRepairs, FinalResult}, FinalMessage, FinalCore} = bigset_read_core:finalise(Core2),
            maybe_send_results(FinalResult, State),
            maybe_send_repairs(FinalRepairs, State),
            Reply = FinalMessage,
            {next_state, reply, State#state{reply=Reply, logic=FinalCore}, 0};
        false ->
            {next_state, await_set, State#state{logic=Core2}}
    end.

maybe_send_repairs(Repairs, State) ->
    {Repairs, State}.

maybe_send_results(undefined, _State) ->
    ok;
maybe_send_results([], _State) ->
    ok;
maybe_send_results(Results0, State) ->
    %%Results = orddict:fetch_keys(Results0),
    send_reply({ok, {elems, Results0}}, State).

send_reply(Reply, State) ->
    #state{from=From, req_id=ReqId} = State,
    From ! {ReqId, Reply}.

-spec reply(timeout, State) -> {stop, normal, State}.
reply(_, State) ->
    #state{from=From, req_id=ReqId, reply=Reply} = State,
    lager:debug("reply::: sending"),
    From ! {ReqId, Reply},
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
schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), request_timeout).

%% decide if this Partition needs an ack message or a stop message.
ack_or_stop(Partition, From, Core) ->
    case bigset_read_core:ack_or_stop(Partition, Core) of
        ack ->
            ack(From);
        stop ->
            stop_fold(From)
    end.

-spec ack(From::{pid(), reference()}) -> term().
ack({Pid, Ref}) ->
    Pid ! {Ref, ok}.

stop_fold({Pid, Ref}) ->
    Pid ! {Ref, stop_fold}.

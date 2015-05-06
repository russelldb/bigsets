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
-export([start_link/4]).

%% gen_fsm callbacks
-export([init/1, prepare/2,  validate/2, read/2,
         await_reps/2, reply/2, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {req_id :: reqid(),
                from :: pid(),
                set :: binary(),
                preflist :: riak_core_apl:preflist(),
                logic = bigset_read_core:new(),
                options=[] :: list(),
                timer=undefined :: reference() | undefined,
                reply = undefined
               }).

-type state() :: #state{}.
-type result() :: {r, partition(), clock, Clock :: binary()} | {r, partition(), [res()]}.
-type res() :: {Member :: binary(), Dot :: riak_dt_vclock:dot()}.
-type partition() :: non_neg_integer().
-type reqid() :: term().

-define(DEFAULT_TIMEOUT, 60000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqId, From, Set, Options) ->
    gen_fsm:start_link(?MODULE, [ReqId, From, Set, Options], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
init([ReqId, From, Set, Options]) ->
    {ok, prepare, #state{req_id=ReqId, from=From, set=Set, options=Options}, 0}.

-spec prepare(timeout, state()) -> {next_state, validate, state(), 0}.
prepare(timeout, State) ->
    #state{options=Options, set=Set} = State,
    Hash = riak_core_util:chash_key({bigset, Set}),
    UpNodes = riak_core_node_watcher:nodes(bigset),
    PL = riak_core_apl:get_apl_ann(Hash, 3, UpNodes),
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

-spec read(timeout, state()) -> {next_state, await_reps, state()}.
read(request_timeout, State) ->
    {next_state, reply, State#state{reply={error, timeout}}, 0};
read(timeout, State) ->
    #state{preflist=PL, set=Set} = State,
    Req = ?READ_REQ{set=Set},
    bigset_vnode:read(PL, Req),
    {next_state, await_reps, State}.

-spec await_reps(result(), state()) ->
                        {next_state, reply, state(), 0} |
                        {next_state, await_reps, state()}.
await_reps(request_timeout, State) ->
    {next_state, reply, State#state{reply={error, timeout}}, 0};
await_reps(Res, State) ->
    #state{logic=Core} = State,
    Core2 = bigset_read_core:result(Res, Core),
    State2 = #state{logic=Core2},
    case bigset_read_core:done(Core2) of
        true ->
            Reply = {ok, Core2},
            {next_state, reply, State2#state{reply=Reply}, 0};
        false ->
            {next_state, await_reps, State2}
    end.

-spec reply(timeout, State) -> {stop, normal, State}.
reply(_, State) ->
    #state{from=From, req_id=ReqId, reply=Reply} = State,
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

-spec pick_coordinator(riak_core_apl:preflist()) -> {partition(), node()}.
pick_coordinator(PL) ->
    {ListPos, _} = random:uniform_s(length(PL), os:timestamp()),
    {Coord, _Type} = lists:nth(ListPos, PL),
    Coord.

-spec replica_pl(riak_core_apl:preflist(), partition()) -> any().
replica_pl(PL, CoordPartition) ->
    [{Idx, Node} || {{Idx, Node}, _Type} <- PL, Idx /= CoordPartition].

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), request_timeout).

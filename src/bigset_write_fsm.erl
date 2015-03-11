%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 12 Jan 2015 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(bigset_write_fsm).

-behaviour(gen_fsm).

-include("bigset.hrl").

%% API
-export([start_link/4]).

%% gen_fsm callbacks
-export([init/1, prepare/2, coordinate/2, await_coord/2, replicate/2,
         await_reps/2, reply/2, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {req_id :: reqid(),
                from :: pid(),
                set :: binary(),
                elements :: [binary()],
                preflist :: riak_core_apl:preflist(),
                results :: [result()],
                value :: value()
               }).

-type state() :: #state{}.

-type result() :: coord_res() | rep_res().
-type coord_res() :: {dw, partition(), {binary(), dot()}}.
-type rep_res() ::   {w | dw, partition()}.
-type partition() :: non_neg_integer().
-type dot() :: {binary(), non_neg_integer()}.
-type reqid() :: term().
-type value() :: {binary(), dot()}.


%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqId, From, Set, Elements) ->
    gen_fsm:start_link(?MODULE, [ReqId, From, Set, Elements], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
init([ReqId, From, Set, Elements]) ->
    {ok, prepare, #state{req_id=ReqId, from=From, set=Set, elements=Elements}, 0}.

-spec prepare(timeout, state()) -> {next_state, coordinate, state(), 0}.
prepare(timeout, State) ->
    #state{set=Set} = State,
    Hash = riak_core_util:chash_key({bigset, Set}),
    UpNodes = riak_core_node_watcher:nodes(bigset),
    PL = riak_core_apl:get_apl_ann(Hash, 3, UpNodes),
    {next_state, coordinate, State#state{preflist=PL}, 0}.

-spec coordinate(timeout, state()) -> {next_state, awaitcoord, state()}.
coordinate(timeout, State) ->
    #state{set=Set, elements=Elements, preflist=PL} = State,
    Coordinator = pick_coordinator(PL),
    Req = ?INSERT_REQ{set=Set, elements=Elements},
    bigset:coordinate(Coordinator, Req),
    {next_state, await_coord, State}.

-spec await_coord(coord_res(), state()) ->
                         {next_state, replicate, state(), 0}.
await_coord({dw, Partition, Value}, State) ->
    {next_state, replicate, State#state{value=Value, results=[{dw, Partition}]}, 0}.

-spec replicate(timeout, state()) -> {next_state, await_reps, state()}.
replicate(timeout, State) ->
    #state{preflist=PL, set=Set, results=Res, value={Es, D}} = State,
    RepPL = replica_pl(PL, Res),
    Req = ?REPLICATE_REQ{set=Set, elements=Es, dot=D},
    bigset:replicate(RepPL, Req),
    {next_state, await_reps, State}.

-spec await_reps(rep_res(), state()) ->
                        {next_state, reply, state(), 0} |
                        {next_state, await_reps, state()}.
await_reps({dw, _Partition}=Res, State) ->
    %% we must have one `dw' to be this far (coordinate must retunr dw
    %% for us to continue.) Which means any `dw' response is quorum.
    #state{results=Results} = State,
    Results2 = [Res | Results],
    State2 = State#state{results=Results2},
    {next_state, reply, State2, 0};
await_reps({w, _Partition}=Res, State) ->
    %% only an ack, await a `dw'
    #state{results=Results} = State,
    Results2 = [Res | Results],
    State2 = State#state{results=Results2},
    {next_state, await_reps, State2}.

-spec reply(timeout, State) -> {stop, normal, State}.
reply(timeout, State) ->
    #state{from=From, req_id=ReqId} = State,
    From ! {ReqId, ok}.

handle_event(_Event, _StateName, State) ->
    {stop, badmsg, State}.

handle_sync_event(_Event, _From, _StateName, State) ->
    {stop, badmsg, State}.

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

-spec replica_pl(riak_core_apl:preflist(), [result()]) -> any().
replica_pl(PL, Results) ->
    [{Idx, Node} || {{Idx, Node}, _Type} <- PL,
                    not lists:keymember(Idx, 2, Results)].

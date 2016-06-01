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
-export([start_link/5]).

%% gen_fsm callbacks
-export([init/1, prepare/2, coordinate/2, validate/2, await_coord/2, replicate/2,
         await_reps/2, reply/2, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {req_id :: reqid(),
                from :: pid(),
                op :: ?OP{},
                set :: binary(),
                preflist :: riak_core_apl:preflist(),
                results :: [result()],
                replicate :: {Inserts :: [delta_add()],
                              Removes :: [delta_remove()]},
                options=[] :: list(),
                timer=undefined :: reference() | undefined,
                reply = ok
               }).

-type state() :: #state{}.

-type result() :: coord_res() | rep_res().
-type coord_res() :: {dw, partition(), [delta_add()], [delta_remove()]}.
-type rep_res() ::   {w | dw, partition()}.
-type partition() :: non_neg_integer().
-type reqid() :: term().

-define(DEFAULT_TIMEOUT, 60000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqId, From, Set, Op, Options) ->
    gen_fsm:start_link(?MODULE, [ReqId, From, Set, Op, Options], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
init([ReqId, From, Set, Op, Options]) ->
    {ok, prepare, #state{req_id=ReqId, from=From, set=Set, op=Op, options=Options}, 0}.

-spec prepare(timeout, state()) -> {next_state, validate, state(), 0}.
prepare(timeout, State) ->
    #state{options=Options, op=?OP{set=Set}} = State,
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
            {next_state, coordinate, State, 0}
    end.

-spec coordinate(timeout | request_timeout, state()) ->
                        {next_state, await_coord | reply, state()}.
coordinate(request_timeout, State) ->
    {next_state, reply, State#state{reply={error, timeout}}, 0};
coordinate(timeout, State) ->
    #state{preflist=PL, op=Op} = State,
    Coordinator = pick_coordinator(PL),
    bigset_vnode:coordinate(Coordinator, Op),
    {next_state, await_coord, State}.

-spec await_coord(coord_res(), state()) ->
                         {next_state, replicate, state(), 0}.
%% @TODO have to handle errors here
await_coord(request_timeout, State) ->
    {next_state, reply, State#state{reply={error, timeout}}, 0};
await_coord({dw, Partition, Ins, Dels}, State) ->
    %% NOTE: No {w, _, _, _} response, since coord needs a dw.
    {next_state,
     replicate,
     %% @TODO(rdb) doesn't it make more sense just to pass on the
     %% original removes from ?OP ? Maybe not, if processing them
     %% costs time.
     State#state{replicate={Ins, Dels},
                 results=[{dw, Partition}]},
     0}.

-spec replicate(timeout, state()) -> {next_state, await_reps, state()}.
replicate(request_timeout, State) ->
    {next_state, reply, State#state{reply={error, timeout}}, 0};
replicate(timeout, State) ->
    #state{preflist=PL, set=Set, results=[{dw, Partition}], replicate={Ins, Dels}} = State,
    RepPL = replica_pl(PL, Partition),
    Req = ?REPLICATE_REQ{set=Set, inserts=Ins, removes=Dels},
    bigset_vnode:replicate(RepPL, Req),
    {next_state, await_reps, State}.

-spec await_reps(rep_res(), state()) ->
                        {next_state, reply, state(), 0} |
                        {next_state, await_reps, state()}.
%% @TODO have to handle errors here
await_reps(request_timeout, State) ->
    {next_state, reply, State#state{reply={error, timeout}}, 0};
await_reps({dw, _Partition}=Res, State) ->
    %% we must have one `dw' to be this far (coordinate must return dw
    %% for us to continue.) Which means any one `dw' response is
    %% quorum. This is to match default riak_kv settings.
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
reply(_, State) ->
    #state{from=From, req_id=ReqId, reply=Reply} = State,
    %% NOTE: default value of reply is just 'ok'
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

terminate(_Reason, _StateName, State) ->
    #state{timer=TRef} = State,
    _ = erlang:cancel_timer(TRef),
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

-module(bigset_vnode).
-behaviour(riak_core_vnode).
-include("bigset.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([
             start_vnode/1
             ]).

-record(state, {partition,
                vnodeid=undefined, %% actor
                db %% eleveldb handle
               }).

-type state() :: #state{}.
-type status() :: orddict:orddict().
-type level_put() :: {put, Key :: binary(), Value :: binary()}.
-type level_delete() :: {delete, Key :: binary()}.
-type level_writes() :: [level_put() | level_delete()].

-define(READ_OPTS, [{fill_cache, true}]).
-define(WRITE_OPTS, [{sync, false}]).
-define(FOLD_OPTS, [{iterator_refresh, true}]).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    DataDir = integer_to_list(Partition),
    Opts =  [{create_if_missing, true},
             {write_buffer_size, 1024*1024},
             {max_open_files, 20}],
    {ok, DB} =  eleveldb:open(DataDir, Opts),
    %% @TODO(rdb|question) Maybe this pool should be BIIIIG for many gets
    PoolSize = app_helper:get_env(bigset, worker_pool_size, 100),
    VnodeId = vnode_id(Partition),
    {ok, #state {vnodeid=VnodeId,  partition=Partition, db=DB},
     [{pool, bigset_vnode_worker, PoolSize, []}]}.

%% COMMANDS(denosneold!)
handle_command(?OP{set=Set, inserts=Inserts, removes=Removes, ctx=Ctx}, Sender, State) ->
    %% Store elements in the set. NOTE: you need a dot per insert,
    %% since this is a delta orswot, and remove means sending only
    %% dot(s) for removed element(s)
    #state{db=DB, partition=Partition, vnodeid=Id} = State,
    ClockKey = clock_key(Set),
    Clock = clock(eleveldb:get(DB, ClockKey, ?READ_OPTS)),
    {Clock2, Inserts, ReplicationPayload} = gen_inserts(Set, Inserts, Id, Clock),

    Deferred = deferred(Ctx, Clock),

    Deletes = gen_removes(Set, Removes, Ctx, Deferred),

    BinClock = to_bin(Clock2),
    Writes = lists:append([[{put, ClockKey, BinClock}],  Inserts, Deletes]),
    eleveldb:writes(DB, Writes, ?WRITE_OPTS),
    riak_core_vnode:reply(Sender, {dw, Partition, ReplicationPayload}),
    {noreply, State};
handle_command(?REPLICATE_REQ{set=Set,
                              elements_dots=Elements},
               Sender, State) ->
    #state{db=DB, partition=Partition} = State,
    %% fire and forget? It's fair? Should DW to be fair in a benchmark, eh?
    riak_core_vnode:reply(Sender, {w, Partition}),
    %% Read local clock
    ClockKey = clock_key(Set),
    {Clock, Writes} = replica_writes(eleveldb:get(DB, ClockKey, ?READ_OPTS), Elements, DB),
    BinClock = to_bin(Clock),
    ok = eleveldb:write(DB, [{put, ClockKey, BinClock} | Writes], ?WRITE_OPTS),
    riak_core_vnode:reply(Sender, {dw, Partition}),
    {noreply, State};
handle_command(?READ_REQ{set=Set}, Sender, State) ->
    #state{db=DB, partition=Partition} = State,
    %% clock is first key
    %% read all the way to last element
    FirstKey = clock_key(Set),

    FoldFun = fun({Key, Value}, Acc) ->
                      {s, S, K} = sext:decode(Key),
                      if S == Set, K == clock ->
                              %% Set clock
                              {from_bin(Value), dict:new()};
                         S == Set, is_binary(K) ->
                              {Clock, Dict} = Acc,
                              {Clock, dict:store(K, from_bin(Value), Dict)};
                         true ->
                              {throw, Acc}
                      end
              end,
    Folder = fun() ->
                     try
                         eleveldb:fold(DB, FoldFun, [], [FirstKey | ?FOLD_OPTS])
                     catch
                         {break, AccFinal} ->
                             riak_core_vnode:reply(Sender, {r, Partition, AccFinal})
                     end
             end,
    {async, {get, Folder}, Sender, State};
handle_command(?CONTAINS_REQ{set=Set, elements=Elements}, Sender, State) ->
    #state{db=DB, partition=Partition} = State,
    %% You need to materialize the set (in part) to see if Element(s) is/are
    %% present. Contains means, read Element + Clock and send to FSM
    %% where a sort of delta merge can decide if Element is present
    ClockKey = clock_key(Set),
    Reply = case eleveldb:get(DB, ClockKey, ?READ_OPTS) of
                not_found ->
                    not_found;
                {ok, ClockBin} ->
                    Clock = from_bin(ClockBin),
                    {ok, Itr} = eleveldb:iterator(DB, ?FOLD_OPTS),
                    ElemsAndKeys = lists:sort([{elem_key(Set, E), E} || E <- Elements]),
                    Set = lists:foldl(fun({Key, Elem}, Acc) ->
                                              case eleveldb:iterator_move(Itr, Key) of
                                                  {ok, Key, DotsBin} ->
                                                      Dots = from_bin(DotsBin),
                                                      dict:store(Elem, Dots, Acc);
                                                  {ok, _OtherKey, _OtherVal} ->
                                                      Acc
                                              end
                                      end,
                                      dict:new(),
                                      ElemsAndKeys),
                    eleveldb:iterator_close(Itr),
                    {Clock, Set}
            end,
    riak_core_vnode:reply(Sender, {r, Partition, Reply}),
    {noreply, State};
handle_command(?REMOVE_REQ{set=Set, elements=Elements, ctx=undefined}, Sender, State) ->
    %% This is like "coordinate remove" see a replicate command for
    %% downstream remove.

    %% No context remove is a dirty "just do it" remove, so just do
    %% it, nothing to replicate
    #state{db=DB, partition=Partition} = State,

    %% An "ack" of the delete
    riak_core_vnode:reply(Sender, {d, Partition, ok}),

    riak_core_vnode:reply(Sender, {d, Partition, ok}),
    Writes = lists:foldl(fun(E, A) ->
                                 [{delete, elem_key(Set, E)} | A]
                         end,
                         [],
                         Elements),
    eleveldb:writes(DB, Writes, ?WRITE_OPTS),

    %% dd == durable delete
    riak_core_vnode:reply(Sender, {dd, Partition, ok}),
    {noreply, State}.
%% handle_command(?REMOVE_REQ{set=Set, elements=Elements, ctx=Ctx}, Sender, State) ->
%%     %% This is like "coordinate remove" see a replicate command for
%%     %% downstream remove.

%%     %% we need to send the removed dots downstream, but if we send the
%%     %% whole removed elements that is simpler (since we operate in a
%%     %% element->dot mapped world) If we stored things dot->element
%%     %% then just sending dots is enough I wonder if we can have a
%%     %% reverse index so we can have both!  In other words some scope
%%     %% for optimisation clearly exists here, it seems a waste to send
%%     %% elements across the network just to delete them from disk!
%%     #state{db=DB, partition=Partition} = State,
%%     riak_core_vnode:reply(Sender, {d, Partition, ok}),
%%     Writes = lists:foldl(fun(E, A) ->
%%                                  [{delete, elem_key(Set, E)} | A]
%%                          end,
%%                          [],
%%                          Elements),

%%     {ok, Itr} = eleveldb:iterator(DB, ?FOLD_OPTS),
%%     ElemsAndKeys = lists:sort([{elem_key(Set, E), E} || E <- Elements]),
%%     {Dels, Reps} = lists:foldl(fun({Key, Elem}, {Del, Rep}=Acc) ->
%%                                        case eleveldb:iterator_move(Itr, Key) of
%%                                            {ok, Key, DotsBin} ->
%%                                                Dots = from_bin(DotsBin),
%%                                                {[{delete, Key} | Del],
%%                                                 [{Elem, Dots} | Rep]};
%%                                            {ok, _OtherKey, _OtherVal} ->
%%                                                Acc
%%                                        end
%%                                end,
%%                                {[], []},
%%                                ElemsAndKeys),

%%     eleveldb:iterator_close(Itr),

%%     riak_core_vnode:reply(Sender, {r, Partition, Reply}),

%%     eleveldb:writes(DB, Writes, ?WRITE_OPTS),
%%     riak_core_vnode:reply(Sender, {dd, Partition, ok}),
%%     {noreply, State}.


-spec handle_handoff_command(term(), term(), state()) ->
                                    {noreply, state()}.
handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.



%%%%% priv
%%% So much cut and paste, maybe vnode_status_mgr (or vnode
%%% manage???) should do the ID management
vnode_id(Partition) ->
    File = vnode_status_filename(Partition),
    Status = read_vnode_status(File),
    case get_status_item(vnodeid, Status, undefined) of
        undefined ->
            {Id, NewStatus} = assign_vnodeid(Status),
            write_vnode_status(NewStatus, File),
            Id;
        Id -> Id
    end.

%% @private Provide a `proplists:get_value/3' like function for status
%% orddict.
-spec get_status_item(term(), status(), term()) -> term().
get_status_item(Item, Status, Default) ->
    case orddict:find(Item, Status) of
        {ok, Val} ->
            Val;
        error ->
            Default
    end.

%% @private generate a file name for the vnode status, and ensure the
%% path to exixts.
-spec vnode_status_filename(non_neg_integer()) -> file:filename().
vnode_status_filename(Index) ->
    P_DataDir = app_helper:get_env(riak_core, platform_data_dir),
    VnodeStatusDir = app_helper:get_env(riak_kv, vnode_status,
                                        filename:join(P_DataDir, "kv_vnode")),
    Filename = filename:join(VnodeStatusDir, integer_to_list(Index)),
    ok = filelib:ensure_dir(Filename),
    Filename.

%% @private Assign a unique vnodeid, making sure the timestamp is
%% unique by incrementing into the future if necessary.
-spec assign_vnodeid(status()) ->
                            binary().
assign_vnodeid(Status) ->
    {_Mega, Sec, Micro} = os:timestamp(),
    NowEpoch = 1000000*Sec + Micro,
    LastVnodeEpoch = get_status_item(last_epoch, Status, 0),
    VnodeEpoch = erlang:max(NowEpoch, LastVnodeEpoch+1),
    NodeId = riak_core_nodeid:get(),
    VnodeId = <<NodeId/binary, VnodeEpoch:32/integer>>,
    UpdStatus = orddict:store(vnodeid, VnodeId,
                              orddict:store(last_epoch, VnodeEpoch, Status)),
    {VnodeId, UpdStatus}.

%% @private read the vnode status from `File'. Returns `{ok,
%% status()}' or `{error, Reason}'. If the file does not exist, and
%% empty status is returned.
-spec read_vnode_status(file:filename()) -> {ok, status()} |
                                       {error, term()}.
read_vnode_status(File) ->
    case file:consult(File) of
        {ok, [Status]} when is_list(Status) ->
            {ok, orddict:from_list(Status)};
        {error, enoent} ->
            %% doesn't exist? same as empty list
            {ok, orddict:new()};
        Er ->
            Er
    end.

%% @private write the vnode status. This is why the file is guarded by
%% the process. This file should have no concurrent access, and MUST
%% not be written at any other place/time in the system.
-spec write_vnode_status(status(), file:filename()) -> ok.
write_vnode_status(Status, File) ->
    VersionedStatus = orddict:store(version, 1, Status),
    ok = riak_core_util:replace_file(File, io_lib:format("~p.",
                                                         [orddict:to_list(VersionedStatus)])).

%%% codec
clock_key(Set) ->
    sext:encode({s, Set, clock}).

%% @private sext encodes the element key so it is in order, on disk,
%% with the other elements.
-spec elem_key(binary(), binary()) -> binary().
elem_key(Set, Elem) ->
    sext:encode({s, Set, Elem}).

-spec clock(not_found | {ok, binary()}) -> bigset_clock:clock().
clock(not_found) ->
    bigset_clock:fresh();
clock({ok, ClockBin}) ->
    binary_to_term(ClockBin).

from_bin(ClockOrDot) ->
    binary_to_term(ClockOrDot).

to_bin(Dot) ->
    term_to_binary(Dot).

%% @private deferred: does the operation have any deferred components,
%% that is, is the Set Clock not descended from the `Ctx' `undefined'
%% as riak_dt_vclock:fresh().
-spec deferred(riak_dt_vclock:vclock() | undefined,
               riak_dt_vclock:vclock()) ->
                      boolean().
deferred(undefined, _Clock) ->
    false;
deferred(Ctx, Clock) ->
    riak_dt_vclock:descends(Clock, Ctx).

%% @private gen_inserts: generate the list of writes for an insert
%% operation, given the elements, set, and clock

-spec gen_inserts(Set :: binary(),
                  Inserts :: [binary()],
                  Actor :: binary(),
                  Clock :: riak_dt_vclock:vclock()) ->
                         {NewClock :: riak_dt_vclock:vclock(),
                          Writes :: [level_put()],
                          ReplicationDeltas :: [delta_element()]}.

gen_inserts(Set, Inserts, Id, Clock) ->
    lists:fold(fun(Element, {C, W, R}) ->
                       ElemKey = elem_key(Set, Element),
                       %% Generate a dot per insert
                       {Dot, C2} = bigset_clock:increment(Id, C),
                       %% Store as a list for simpler merging later.
                       ElemValue = to_bin([Dot]),
                       {
                         C2, %% New Clock
                         [{put, ElemKey, ElemValue} | W], %% To write
                         [{ElemKey, Dot} | R] %% To replicate
                       }
               end,
               {Clock, [], []},
               Inserts).

%% @private gen_removes: generate the writes needed to for a delete.

-spec gen_removes(Set :: binary(),
                  Removes :: [binary()],
                  Context :: riak_dt_vclock:vclock() | undefined,
                  ReadFirst :: boolean()) ->
                         [level_delete()].
gen_removes(Set, Removes, _Ctx, _Deferred=false) ->
                      %% if the Ctx decends the Set Clock, then every element on disk in
                      %% `Removes' can be straight deleted.
                      %% No context remove is a dirty "just do it" remove, so just do
                      lists:foldl(fun(E, A) ->
                                          [{delete, elem_key(Set, E)} | A]
                                  end,
                                  [],
                                  Removes);
gen_removes(Set, Removes, Ctx, _Deferred=true) ->
    %% This is all together harder, and way less efficient. @TODO(rdb)
    %% find a more efficient way, this involves reading every element
    %% in `Removes' and seeing if it's dot is covered by Ctx
    ok.

%% fold funs for replicate
%% returns {clock, writes}
    replica_writes(not_found, Elements, _DB) ->
                                       F = fun({Key, Dot}, {Clock, Writes}) ->
                                                   Value = to_bin([Dot]),
                                                   C2 = bigset_clock:strip_dots(Dot, Clock),
                                                   {C2, [{put, Key, Value} | Writes]}
                                           end,
                                       lists:fold(F, {bigset_clock:fresh(), []}, Elements);
        replica_writes({ok, BinClock}, Elements, DB) ->
                                       Clock0 = from_bin(BinClock),
                                       %% Use iterator? Or multiple Gets?  This needs benchmarking, and
                                       %% maybe have an adaptive strategy based on sparseness/size of new
                                       %% write set.
                                       {ok, Itr} = eleveldb:iterator(DB, ?FOLD_OPTS),
                                       F = fun({Key, Dot}, {Clock, Writes}) ->
                                                   case bigset_clock:seen(Clock, Dot) of
                                                       true ->
                                                           %% No op, skip it/discard
                                                           {Clock, Writes};
                                                       false ->
                                                           %% Strip the dot
                                                           C2 = bigset_clock:strip_dots(Dot, Clock),
                                                           case eleveldb:iterator_move(Itr, Key) of
                                                               {ok, Key, LocalDotsBin} ->
                                                                   %% local present && dot unseen
                                                                   %% merge with local elements dots
                                                                   %% store new element dots, store clock
                                                                   LocalDots = from_bin(LocalDotsBin),
                                                                   Dots = riak_dt_vclock:merge([Dot], LocalDots),
                                                                   DotsBin = to_bin(Dots),
                                                                   {C2, [{put, Key, DotsBin} | Writes]};
                                                               {ok, _OtherKey, _OtherVal} ->
                                                                   %% Not present, not seen, just store it
                                                                   DotBin = to_bin(Dot),
                                                                   {C2, [{put, Key, DotBin} | Writes]}
                                                           end
                                                   end
                                           end,
                                       {C, W} = lists:foldl(F, {Clock0, []}, Elements),
                                       eleveldb:iterator_close(Itr),
                                       {C, W}.

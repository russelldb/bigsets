-module(bigset_vnode).
-behaviour(riak_core_vnode).
-include("bigset.hrl").

-export([start_vnode/1,
         coordinate/2,
         replicate/2,
         read/2,
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

-compile([export_all]).

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

-define(READ_OPTS, [{fill_cache, true}]).
-define(WRITE_OPTS, [{sync, false}]).
-define(FOLD_OPTS, [{iterator_refresh, true}]).

%% added onto the key for an add so it sorts higher than a remove key
-define(ADD, <<>>).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc coordinate a set operation
coordinate(Coordinator, Op=?OP{}) ->
    riak_core_vnode_master:command([Coordinator],
                                   Op,
                                   {fsm, undefined, self()},
                                   bigset_vnode_master).

get_db(VNode) ->
    riak_core_vnode_master:command([VNode],
                                   get_db,
                                   {fsm, undefined, self()},
                                   bigset_vnode_master).

dump_db(Set) ->
    PL = bigset:preflist(Set),
    riak_core_vnode_master:command(PL,
                                   dump_db,
                                   {fsm, undefined, self()},
                                   bigset_vnode_master).

%% @doc handle the downstream replication operation
replicate(PrefList, Req=?REPLICATE_REQ{}) ->
    riak_core_vnode_master:command(PrefList,
                                   Req,
                                   {fsm, undefined, self()},
                                   bigset_vnode_master).
%% @doc read from the set
read(PrefList, Req=?READ_REQ{}) ->
    riak_core_vnode_master:command(PrefList,
                                   Req,
                                   {fsm, undefined, self()},
                                   bigset_vnode_master).

init([Partition]) ->
    DataDir = integer_to_list(Partition),
    Opts =  [{create_if_missing, true},
             {write_buffer_size, 1024*1024},
             {max_open_files, 20}],
    {ok, DB} = open_db(DataDir, Opts),
    %% @TODO(rdb|question) Maybe this pool should be BIIIIG for many gets
    PoolSize = app_helper:get_env(bigset, worker_pool_size, ?DEFAULT_WORKER_POOL),
    VnodeId = vnode_id(Partition),
    BatchSize  = app_helper:get_env(bigset, batch_size, ?DEFAULT_BATCH_SIZE),
    {ok, #state {vnodeid=VnodeId,  partition=Partition, db=DB},
     [{pool, bigset_vnode_worker, PoolSize, [{batch_size, BatchSize}]}]}.

%% COMMANDS(denosneold!)
handle_command(get_db, _Sender, State) ->
    #state{db=DB} = State,
    {reply, {ok, DB}, State};
handle_command(dump_db, _Sender, State) ->
    #state{db=DB, partition=P} = State,

    FoldFun = fun({K, <<>>}, Acc) ->
                      [sext:decode(K) | Acc];
                 ({K, V}, Acc) ->
                      [{sext:decode(K), binary_to_term(V)} | Acc]
              end,
    Acc =  eleveldb:fold(DB, FoldFun, [], [?FOLD_OPTS]),
    {reply, {ok, P, lists:reverse(Acc)}, State};
handle_command(?OP{set=Set, inserts=Inserts, removes=Removes, ctx=Ctx}, Sender, State) ->
    %% Store elements in the set.
    #state{db=DB, partition=Partition, vnodeid=Id} = State,
    ClockKey = bigset:clock_key(Set),
    Clock = clock(eleveldb:get(DB, ClockKey, ?READ_OPTS)),
    {Clock2, InsertWrites, ReplicationPayload} = gen_inserts(Set, Inserts, Id, Clock),

    %% Each element has the context it was read with, generate
    %% tombstones for those dots.

    %% @TODO(rdb|correctness) can you strip dots from removes? I think
    %% so, maybe. Carlos/Paulo say you can. In fact, maybe you _have_ to!
    %% so, maybe. I think you _have_ to infact (see replicate command
    %% below) it says you have seen that dot and you no longer store
    %% it
    DeleteWrites = gen_removes(Set, Removes, Ctx),

    %% @TODO(rdb|optimise) technically you could ship the deletes
    %% right now, if you wanted (in fact deletes can be broadcast
    %% without a coordinator, how cool!)

    BinClock = bigset:to_bin(Clock2),

    Writes = lists:append([[{put, ClockKey, BinClock}],  InsertWrites, DeleteWrites]),
    ok = eleveldb:write(DB, Writes, ?WRITE_OPTS),
%%    lager:debug("I wrote ~p~n", [decode_writes(Writes, [])]),
    riak_core_vnode:reply(Sender, {dw, Partition, ReplicationPayload, DeleteWrites}),
    {noreply, State};
handle_command(?REPLICATE_REQ{set=Set,
                              inserts=Ins,
                              removes=Rems},
               Sender, State) ->
    #state{db=DB, partition=Partition} = State,
    %% fire and forget? It's fair? Should DW to be fair in a
    %% benchmark, eh?
    riak_core_vnode:reply(Sender, {w, Partition}),
    %% Read local clock
    ClockKey = bigset:clock_key(Set),
    {Clock, Inserts} = replica_writes(eleveldb:get(DB, ClockKey, ?READ_OPTS), Ins),
    BinClock = bigset:to_bin(Clock),

    lager:debug("Dels is ~p~n", [Rems]),
    %% @TODO(rdb|correctness) doesn't the clock have to add the
    %% tombstone dots? Other wise we can re-surface a write if we've
    %% seen the tombstone but not the write itself!!

    Writes = lists:append([[{put, ClockKey, BinClock}], Inserts, Rems]),
    ok = eleveldb:write(DB, Writes, ?WRITE_OPTS),
%%    lager:debug("I wrote ~p~n", [decode_writes(Writes, [])]),
    riak_core_vnode:reply(Sender, {dw, Partition}),
    {noreply, State};
handle_command(?READ_REQ{set=Set}, Sender, State) ->
    %% read is an async fold operation
    %% @see bigset_vnode_worker for that code.
    #state{db=DB} = State,
    {async, {get, DB, Set}, Sender, State}.


decode_writes([], Acc) ->
    Acc;
decode_writes([{put, K, <<>>} | Rest], Acc) ->
    decode_writes(Rest, [sext:decode(K) | Acc]);
decode_writes([{put, K ,V} | Rest], Acc) ->
    decode_writes(Rest, [{sext:decode(K), binary_to_term(V)} | Acc]).

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

terminate(_Reason, State) ->
    case State#state.db of
        undefined ->
            ok;
        _ ->
            eleveldb:close(State#state.db)
    end,
    ok.

%%%%% priv

-spec clock(not_found | {ok, binary()}) -> bigset_clock:clock().
clock(not_found) ->
    bigset_clock:fresh();
clock({ok, ClockBin}) ->
    bigset:from_bin(ClockBin).

%%% So much cut and paste, maybe vnode_status_mgr (or vnode
%%% manage???) should do the ID management
vnode_id(Partition) ->
    File = vnode_status_filename(Partition),
    {ok, Status} = read_vnode_status(File),
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
    lists:foldl(fun(Element, {C, W, R}) ->
                       %% Generate a dot per insert
                       {{Id, Cnt}=Dot, C2} = bigset_clock:increment(Id, C),
                       ElemKey = bigset:insert_member_key(Set, Element, Id, Cnt),
                       {
                         C2, %% New Clock
                         [{put, ElemKey, <<>>} | W], %% To write
                         [{ElemKey, Dot} | R] %% To replicate
                       }
               end,
               {Clock, [], []},
               Inserts).

%% @private gen_removes: generate the writes needed to for a delete.
%% When there is no context, use the local, coordinating clock (we can
%% bike shed this later!)
-spec gen_removes(Set :: binary(),
                  Removes :: [binary()],
                  Ctx :: binary()) ->
                         [level_put()].
gen_removes(_Set, []=_Removes, _Ctx) ->
    [];
gen_removes(Set, Removes, Ctx) ->
    %% eugh, I hate this elements*actors iteration
    Decoder = bigset_ctx_codec:new_decoder(Ctx),
    Rems = lists:foldl(fun({E, CtxBin}, A) ->
                                ECtx = bigset_ctx_codec:decode_dots(CtxBin, Decoder),
                                Deletes = [{put, bigset:remove_member_key(Set, E, Actor, Cnt), <<>>} ||
                                              {Actor, Cnt} <- ECtx],
                                [Deletes | A]
                        end,
                        [],
                        Removes),
    lists:flatten(Rems).

%% @private generate the write set, don't write stuff you wrote
%% already. Returns {clock, writes}
-spec replica_writes(not_found | {ok, binary()},
                     [delta_element()]) ->
                            [level_put()].
replica_writes(not_found, Elements) ->
    F = fun({Key, Dot}, {Clock, Writes}) ->
                C2 = bigset_clock:strip_dots(Dot, Clock),
                {C2, [{put, Key, <<>>} | Writes]}
        end,
    lists:foldl(F, {bigset_clock:fresh(), []}, Elements);
replica_writes({ok, BinClock}, Elements) ->
    Clock0 = bigset:from_bin(BinClock),
    F = fun({Key, Dot}, {Clock, Writes}) ->
                case bigset_clock:seen(Clock, Dot) of
                    true ->
                        %% No op, skip it/discard
                        {Clock, Writes};
                    false ->
                        %% Strip the dot
                        C2 = bigset_clock:strip_dots(Dot, Clock),
                        {C2, [{put, Key, <<>>} | Writes]}
                end
        end,
    lists:foldl(F, {Clock0, []}, Elements).

open_db(DataDir, Opts) ->
    open_db(DataDir, Opts, 30, undefined).

open_db(_DataDit, _Opts, 0, LastError) ->
    {error, LastError};
open_db(DataDir, Opts, RetriesLeft, _) ->
    case eleveldb:open(DataDir, Opts) of
        {ok, Ref} ->
            {ok, Ref};
        %% Check specifically for lock error, this can be caused if
        %% a crashed vnode takes some time to flush leveldb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr}=Reason} ->
            case lists:prefix("IO error: lock ", OpenErr) of
                true ->
                    lager:debug("Leveldb backend retrying ~p in ~p ms after error ~s\n",
                                [DataDir, 2000, OpenErr]),
                    timer:sleep(2000),
                    open_db(DataDir, Opts, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 12 Oct 2015 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(bigset_vnode).

-behaviour(riak_core_vnode).
-include_lib("riak_core/include/riak_core_vnode.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include("bigset.hrl").

-export([start_vnode/1,
         coordinate/2,
         replicate/2,
         read/2,
         repair/2,
         contains/2,
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
                data_dir=undefined, %% eleveldb data directory
                db, %% eleveldb handle
                hoff_state %% handoff state
               }).

-type state() :: #state{}.
-type status() :: orddict:orddict().
-type level_put() :: {put, Key :: binary(), Value :: binary()}.
-type level_del() :: {delete, Key :: binary()}.
-type writes() :: [level_put() | level_del()].

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

dump_db(Set, Cmd) ->
    PL = bigset:preflist(Set),
    riak_core_vnode_master:command(PL,
                                   {dump_db, Cmd},
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

%% @doc does Set contain Element
contains(PrefList, Req=?CONTAINS_REQ{}) ->
    riak_core_vnode_master:command(PrefList,
                                   Req,
                                   {fsm, undefined, self()},
                                   bigset_vnode_master).

repair(PrefList, Req=?REPAIR_REQ{}) ->
    riak_core_vnode_master:command(PrefList,
                                   Req,
                                   {fsm, undefined, self()},
                                   bigset_vnode_master).

init([Partition]) ->
    VnodeId = vnode_id(Partition),
    DataDir = integer_to_list(Partition),

    Opts0 =  [{create_if_missing, true},
             {write_buffer_size, 1024*1024},
             {max_open_files, 20},
             {vnode, VnodeId}],

    Opts = bigset_keys:add_comparator_opt(Opts0),

    {ok, DB} = open_db(DataDir, Opts),
    %% @TODO(rdb|question) Maybe this pool should be BIIIIG for many gets
    PoolSize = app_helper:get_env(bigset, worker_pool_size, ?DEFAULT_WORKER_POOL),
    BatchSize  = app_helper:get_env(bigset, batch_size, ?DEFAULT_BATCH_SIZE),

    HoffState = bigset_handoff:new(VnodeId),
    {ok, #state {data_dir=DataDir,
                 vnodeid=VnodeId,
                 partition=Partition,
                 db=DB,
                 hoff_state=HoffState},
     [{pool, bigset_vnode_worker, PoolSize, [{batch_size, BatchSize}]}]}.

%% COMMANDS(denosneold!)
handle_command(get_db, _Sender, State) ->
    #state{db=DB, vnodeid=Id} = State,
    {reply, {ok, DB, Id}, State};
handle_command(dump_db, _Sender, State) ->
    #state{db=DB, partition=P} = State,

    FoldFun = fun({K, <<>>}, Acc) ->
                      [bigset_keys:decode_key(K) | Acc];
                 ({K, V}, Acc) ->
                      [{bigset_keys:decode_key(K), binary_to_term(V)} | Acc]
              end,
    Acc =  eleveldb:fold(DB, FoldFun, [], [?FOLD_OPTS]),
    {reply, {ok, P, lists:reverse(Acc)}, State};
handle_command({dump_db,count}, _Sender, State) ->
    #state{db=DB, partition=P} = State,

    FoldFun = fun({_K, <<>>}, {C, Acc}) ->
                      {C, Acc+1};
                 ({_K, V}, {_, Acc}) ->
                      {V, Acc}
              end,
    Acc =  eleveldb:fold(DB, FoldFun, {undefined, 0}, [?FOLD_OPTS]),
    {reply, {ok, P, Acc}, State};
handle_command({dump_db, dots}, _Sender, State) ->
    #state{db=DB, partition=P} = State,

    FoldFun = fun({K, <<>>}, {C, Acc}) ->
                      case  bigset_keys:decode_key(K) of
                          {element, _S, _E, A, Cnt} ->
                              {C, [{A, Cnt} | Acc]};
                          _ -> {C, Acc}
                      end;
                 ({K, V}, {_, Acc}) ->
                      {{bigset_keys:decode_key(K), binary_to_term(V)}, Acc}
              end,
    Acc =  eleveldb:fold(DB, FoldFun, {undefined, []}, [?FOLD_OPTS]),
    {reply, {ok, P, Acc}, State};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Coordinate write
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command(?OP{}=Op, Sender, State) ->
    Reply = handle_coord_write(Op, State),
    ok = riak_core_vnode:reply(Sender, Reply),
    {noreply, State};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Replication Write
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command(?REPLICATE_REQ{}=Op,
               Sender, State) ->
    #state{partition=Partition} = State,
    %% fire and forget? It's fair? Should DW to be fair in a
    %% benchmark, eh?
    riak_core_vnode:reply(Sender, {w, Partition}),
    _Reply = handle_replication(Op, State),
    riak_core_vnode:reply(Sender, {dw, Partition}),
    {noreply, State};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Read
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command(?READ_REQ{set=Set, options=Opts}, Sender, State) ->
    %% read is an async fold operation
    %% @see bigset_vnode_worker for that code.
    #state{db=DB, vnodeid=Id} = State,
    {async, {get, Id, DB, Set, Opts}, Sender, State};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Contains Query
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command(?CONTAINS_REQ{set=Set, members=Members}, Sender, State) ->
    %% contains is a special kind of read, and an async fold operation
    %% @see bigset_vnode_worker for that code.
    #state{db=DB, vnodeid=Id} = State,
    {async, {contains, Id, DB, Set, Members}, Sender, State};


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Read Repair
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command(?REPAIR_REQ{}=Req, _Sender, State) ->
    %% Much like a delta replicate, but slightly different format.
    %% see types in bigset.hrl for structure of Repairs.
    ok = handle_read_repair(Req, State),
    {noreply, State}.


-spec handle_handoff_command(term(), term(), state()) ->
                                    {noreply, state()}.
handle_handoff_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, Sender, State) ->
    #state{db=DB, vnodeid=ID} = State,
    %% Tell the receiver who we are, encode_handoff_item doesn't get
    %% state, so we have to do this here.
    IDLen = byte_size(ID),
    FoldFunWrapped = fun({Key, Val}, AccIn) ->
                             NewKey = <<IDLen:32/little-unsigned-integer,
                                        ID:IDLen/binary,
                                        Key/binary>>,
                             FoldFun(NewKey, Val, AccIn)
                     end,
    {async, {handoff, DB, FoldFunWrapped, Acc0}, Sender, State}.

encode_handoff_item(Key, Val) ->
    %% Just bosh together the binary key and value.
    KeyLen = byte_size(Key),
    <<KeyLen:32/integer, Key:KeyLen/binary, Val/binary>>.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    #state{db=DB, vnodeid=Id, hoff_state=HoffState} = State,

    {Sender, Set, {Key, Value}} = decode_handoff_item(Data),

    ClockKey = bigset_keys:clock_key(Set, Id),
    {FirstWrite, Clock} = bigset:get_clock(ClockKey, DB),

    {Writes, NewHoffState} =
        case bigset_keys:decode_key(Key) of
            {clock, Set, Id} ->
                %% my clock, no-op
                {[], HoffState};
            {clock, Set, Sender} ->
                SenderClock = bigset:from_bin(Value),
                HoffState2= bigset_handoff:sender_clock(Set, Sender, SenderClock, HoffState),
                {[{put, Key, Value}], HoffState2};
            {clock, Set, _Other} ->
                {[], HoffState};
            {set_tombstone, Set, Id} ->
                {[], HoffState};
            {set_tombstone, Set, _Other} ->
                {[{put, Key, Value}], HoffState};
            {element, Set, _E, Act, Cnt} ->
                Dot = {Act, Cnt},
                HoffState2 = bigset_handoff:add_dot(Set, Sender, Dot, HoffState),
                case bigset_clock:seen(Dot, Clock) of
                    true ->
                        {[], HoffState2};
                    false ->
                        Clock2 = bigset_clock:add_dot(Dot, Clock),
                        {[{put, ClockKey, bigset:to_bin(Clock2)},
                          {put, Key, Value}],
                         HoffState2}
                end;
            {end_key, Set}  ->
                TombstoneKey = bigset_keys:tombstone_key(Set, Id),
                TS = bigset:get_tombstone(TombstoneKey, DB),
                {C2, TS2, HoffState2} = bigset_handoff:end_key(Set,
                                                               Sender,
                                                               Clock,
                                                               TS,
                                                               HoffState),
                {[{put, ClockKey, bigset:to_bin(C2)},
                  {put, TombstoneKey, bigset:to_bin(TS2)}],
                 HoffState2}
        end,

    Writes2 = add_end_key(FirstWrite, Set, Writes),

    ok = eleveldb:write(DB, Writes2, ?WRITE_OPTS),

    {reply, ok, State#state{hoff_state=NewHoffState}}.

is_empty(State) ->
    #state{db=DB} = State,
    case eleveldb:is_empty(DB) of
        true ->
            {true, State};
        false ->
            Size = calc_handoff_size(DB),
            {false, Size, State}
    end.

calc_handoff_size(DB) ->
    try {ok, <<SizeStr/binary>>} = eleveldb:status(DB, <<"leveldb.total-bytes">>),
         list_to_integer(binary_to_list(SizeStr)) of
        Size -> {Size, bytes}
    catch
        error:_ -> undefined
    end.

delete(State) ->
    #state{db=DB, partition=Partition, data_dir=DataDir} = State,
    ok = clear_vnode_id(Partition),
    eleveldb:close(DB),
    case eleveldb:destroy(DataDir, []) of
        ok ->
            {ok, State#state{db = undefined}};
        {error, Reason} ->
            {error, Reason, State}
    end.

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

clear_vnode_id(Partition) ->
    File = vnode_status_filename(Partition),
    {ok, Status} = read_vnode_status(File),
    NewStatus = orddict:erase(vnodeid, Status),
    write_vnode_status(NewStatus, File).


%% @private gen_inserts: generate the list of writes for an insert
%% operation, given the elements, set, and clock, also generates the
%% replicaton payload since they are similar, but not the same, and
%% there is no point traversing one to transform it into the other.
-spec gen_inserts(Set :: binary(),
                  Inserts :: [add()],
                  Actor :: binary(),
                  Clock :: bigset_clock:clock(),
                  CtxDecoder :: bigset_ctx_codec:decoder()) ->
                         {NewClock :: bigset_clock:clock(),
                          Writes :: writes(),
                          ReplicationDeltas :: [delta_add()]}.
gen_inserts(Set, Inserts, Id, Clock, CtxDecoder) ->
    lists:foldl(fun(Insert, {C, Writes0, R}) ->
                        %% Get the element and any tombstoning dots
                        %% @TODO(rdb|arch) Honestly I don't like
                        %% decoding here, we encode at the
                        %% fsm/coordinator level. But to decode on the
                        %% way in adds another loop over the inserts.
                        {Element, InsertCtx} = element_ctx(Insert, CtxDecoder),
                        %% Generate a dot per insert
                        {{Id, Cnt}=_Dot, C2} = bigset_clock:increment(Id, C),
                        ElemKey = bigset_keys:insert_member_key(Set, Element, Id, Cnt),
                        {C3, Writes} = remove_seen(Set, Element, InsertCtx, C2, Writes0),
                        {
                          C3, %% New Clock
                          [{put, ElemKey, <<>>} | Writes], %% To write
                          [{ElemKey, InsertCtx} | R] %% To replicate
                        }
                end,
                {Clock, [], []},
                Inserts).

%% @private any dot in `Dots' seen by `Clock' may be a key on disk,
%% generate a delete key for it. Otherwise, add the dot to `Clock' so
%% we never write that key.
remove_seen(Set, Element, Dots, Clock, Acc) ->
    lists:foldl(fun({A,C}=Dot, {ClockAcc, DelKeys}) ->
                        case bigset_clock:seen(Dot, ClockAcc) of
                            true ->
                                Key = bigset_keys:insert_member_key(Set, Element, A, C),
                                {ClockAcc,
                                 [{delete, Key} | DelKeys]};
                            false ->
                                {bigset_clock:add_dot(Dot, ClockAcc),
                                 DelKeys}
                        end
                end,
                {Clock, Acc},
                Dots).

%% @private gen_removes: Generate a list of `{delete, Key::binary()}'
%% to be deleted by the remove. @TODO(rdb) Removes can be broadcast
%% without a coordinator.
-spec gen_removes(Set :: binary(),
                  Removes :: removes(),
                  Clock :: bigset_clock:clock(),
                  CtxDecoder :: bigset_ctx_codec:decoder()) ->
                         {NewClock :: bigset_clock:clock(),
                          DelKeys :: [{delete, binary()}],
                          ReplicateRemove :: [delta_remove()]}.
gen_removes(Set, Removes, Clock, CtxDecoder) ->
    lists:foldl(fun(Remove, {C, Deletes, Reps}) ->
                        %% get any tombstoning dots @TODO(rdb|arch)
                        %% Honestly I don't like decoding here, we
                        %% encode at the fsm/coordinator level. But to
                        %% decode on the way in adds another loop over
                        %% the removes.
                        {Element, RemCtx} = element_ctx(Remove, CtxDecoder),
                        {C2, Dels} = remove_seen(Set, Element, RemCtx, C, Deletes),
                        {C2, Dels, [{Element, RemCtx} | Reps]}
                end,
                {Clock, [], []},
                Removes).

%% @priv return the {element, context} pair. Possible context values
%% are: `undefined' which means "no context' making removes a no-op
%% and adds concurrent; `<<>>' which is as `undefined'; `binary()'
%% which is a t2b encoded ctx; `binary()' with a `CtxDecoder', which
%% is a compressed context.
%%
%%  Ideally every client calls `is_member(E)->{true | false, ctx}'
%% before add/remove for each element, but batches, speed, etc mean we
%% have this. Batch adding 1000 elements with no ctx seems fair, for
%% example.  I did not add way to use the local context for an element
%% set at the moment.
element_ctx({Element, undefined}, _CtxDecoder) ->
    %% No Ctx provided, nor wanted for this element
    {Element, []};
element_ctx({Element, <<>>}, _CtxDecoder) ->
    %% Empty Ctx provided (unseen put)
    {Element, []};
element_ctx({Element, Ctx}, undefined) when is_binary(Ctx)  ->
    %% A Put ctx? Must be <<cntr, actor>>
    %% @TODO(rdb) the dot encoding needs addressing
    {Element, binary_to_term(Ctx)};
element_ctx({Element, Ctx}, CtxDecoder) when is_binary(Ctx)  ->
    %% A Put ctx? And a secret decoder ring!
    {Element, bigset_ctx_codec:decode_dots(Ctx, CtxDecoder)};
element_ctx({Element, DotList}, undefined) ->
    {Element, DotList};
element_ctx(Element, undefined) ->
    %% i.e. no ctx at all (such a remove is a no-op, such an add is
    %% concurrent with all other adds of Element
    {Element, []}.

%% @private generate the write set, don't write stuff you wrote
%% already. This is important, not just an optimisation. Imagine some
%% write at dot {a, 1} has been tombstoned, and the tombstone
%% removed. Re-writing {a,1} will cause the element to re-surface. We
%% check the dot on the write, if we already saw it, we already wrote
%% it, do not write again! Returns {clock, writes}
-spec replica_inserts(bigset_clock:clock(),
                     [delta_add()]) ->
                            {bigset_clock:clock(), writes()}.
replica_inserts(Clock0, Elements) ->
    F = fun({Key, Ctx}, {Clock, Writes0}) ->
                {element, S, E, A, C} = bigset_keys:decode_key(Key),
                Dot = {A, C},
                %% You must always tombstone the removed context of an
                %% add. Just because the clock has seen the dot of an
                %% add does not mean it hs seen the removed
                %% dots. Imagie you have seen a remove of {a, 2} but
                %% not the add of {a, 2} that removes {a, 1}. Even
                %% though you don't write {a, 2}, you must remove what
                %% it removes.
                {Clock2, Writes} = remove_seen(S, E, Ctx, Clock, Writes0),
                case bigset_clock:seen(Dot, Clock2) of
                    true ->
                        %% No op, skip it/discard
                        {Clock2, Writes};
                    false ->
                        %% Add the dot to the clock
                        Clock3 = bigset_clock:add_dot(Dot, Clock2),
                        {Clock3, [{put, Key, <<>>} | Writes]}
                end
        end,
    lists:foldl(F, {Clock0, []}, Elements).

%% @private add the dots from `Rems' to the causal information. return
%% the updated causal information.
-spec replica_removes(set(), bigset_clock:clock(),
                      {Element::binary(), [bigset_clock:dot()]}) ->
                             {bigset_clock:clock(), writes()}.
replica_removes(Set, Clock, Rems) ->
    lists:foldl(fun({Element, Dots}, {C, Writes}) ->
                        remove_seen(Set, Element, Dots, C, Writes)
                end,
                {Clock, []},
                Rems).

%% @private get a context decoder. We may not need one.  With
%% per-element-ctx, if the user reads many items it makes sense to
%% only send each actor name once. So we use the full clock to create
%% a simple compression dictionary. @see bigset_ctx_codec for more.
ctx_decoder(undefined) ->
    undefined;
ctx_decoder(Bin) when is_binary(Bin)  ->
    bigset_ctx_codec:new_decoder(Bin).

open_db(DataDir, Opts) ->
    open_db(DataDir, Opts, 30, undefined).

open_db(_DataDir, _Opts, 0, LastError) ->
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

%% @priv add the end key only if this is the first write to the set
-spec add_end_key(boolean(), set(), writes()) -> writes().
add_end_key(false, _Set, Writes) ->
    Writes;
add_end_key(true, Set, Writes) ->
    EndKey = bigset_keys:end_key(Set),
    [{put, EndKey, <<>>} | Writes].

%%%===================================================================
%%% Command handlers
%%%===================================================================

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Coordinate write
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_coord_write(Op, State) ->
    ?OP{set=Set,
        inserts=Inserts,
        removes=Removes,
        ctx=Ctx} = Op,
    %% Store elements in the set.
    #state{db=DB, partition=Partition, vnodeid=Id} = State,
    ClockKey = bigset_keys:clock_key(Set, Id),
    {FirstWrite, Clock} = bigset:get_clock(ClockKey, DB),

    CtxDecoder = ctx_decoder(Ctx),

    {Clock2, InsertWrites, InsertReplicate} = gen_inserts(Set, Inserts, Id, Clock, CtxDecoder),
    {Clock3, RemoveWrites, RemoveReplicate} = gen_removes(Set, Removes, Clock2, CtxDecoder),

    BinClock = bigset:to_bin(Clock3),
    Writes0 = lists:flatten([{put, ClockKey, BinClock}, InsertWrites, RemoveWrites]),
    Writes = add_end_key(FirstWrite, Set, Writes0),

    ok = eleveldb:write(DB, Writes, ?WRITE_OPTS),
    %% NOTE: replicate the removes, although they could have been
    %% broadcast. Maybe optimise fsm if there are only removes, but at
    %% least we decode their contexts here.
    {dw, Partition, InsertReplicate, RemoveReplicate}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Replication Write
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_replication(Op, State) ->
    ?REPLICATE_REQ{set=Set,
                   inserts=Ins,
                   removes=Rems} = Op,
    #state{db=DB, vnodeid=Id, partition=Partition} = State,

    %% Read local clock
    ClockKey = bigset_keys:clock_key(Set, Id),
    {FirstWrite, Clock0} = bigset:get_clock(ClockKey, DB),
    {Clock, Inserts} = replica_inserts(Clock0, Ins),
    {Clock1, Deletes} = replica_removes(Set, Clock, Rems),
    BinClock = bigset:to_bin(Clock1),
    Writes = lists:flatten([{put, ClockKey, BinClock}, Inserts, Deletes]),
    Writes2 = add_end_key(FirstWrite, Set, Writes),
    ok = eleveldb:write(DB, Writes2, ?WRITE_OPTS),
    {dw, Partition}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Read Repair Write
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_read_repair(Req, State) ->
    ?REPAIR_REQ{set=Set, repairs=Repairs} = Req,
    #state{db=DB, vnodeid=Id} = State,

    %% Read local clock
    ClockKey = bigset_keys:clock_key(Set, Id),
    {FirstWrite, Clock0} = bigset:get_clock(ClockKey, DB),
    {Clock, Writes0} = repair_writes(Set, Clock0, Repairs),
    BinClock = bigset:to_bin(Clock),
    Writes = [{put, ClockKey, BinClock} | Writes0],
    Writes2 = add_end_key(FirstWrite, Set, Writes),
    eleveldb:write(DB, Writes2, ?WRITE_OPTS).

repair_writes(Set, Clock, Repairs) ->
    lists:foldl(fun({Element, {Adds, Removes}}, {ClockAcc, WriteAcc}) ->
                        %% traverse removes first in case a dot is on both
                        {ClockAcc1, WriteAcc1} = remove_seen(Set, Element, Removes, ClockAcc, WriteAcc),
                        %% use clock from removes stops us from
                        %% writing something unseen and removing it in
                        %% the same action
                        repair_adds(Set, Element, Adds, ClockAcc1, WriteAcc1);
                   ({Element, Adds}, {ClockAcc, WriteAcc}) ->
                        repair_adds(Set, Element, Adds, ClockAcc, WriteAcc)
                end,
                {Clock, []},
                Repairs).

repair_adds(Set, Element, Dots, Clock, Acc) ->
    lists:foldl(fun({Actor, Cnt}=Dot, {ClockAcc, WriteAcc}) ->
                        case bigset_clock:seen(Dot, Clock) of
                            true ->
                                %% do nothing
                                {Clock, Acc};
                            false ->
                                Key = bigset_keys:insert_member_key(Set, Element, Actor, Cnt),
                                {bigset_clock:add_dot(Dot, ClockAcc),
                                 [{put, Key, <<>>} | WriteAcc]}
                        end
                end,
                {Clock, Acc},
                Dots).

%% @private decode_handoff_item
%% parse out the sender ID and key data
-spec decode_handoff_item(binary()) -> {Sender :: binary(),
                                        Set :: binary(),
                                        SubKey :: binary(),
                                        Value :: binary()}.
decode_handoff_item(<<KeyLen:32/integer, Rest/binary>>) ->
    <<Key0:KeyLen/binary, Value/binary>> = Rest,
    <<IDLen:32/little-unsigned-integer, Key1/binary>> = Key0,
    <<Sender:IDLen/binary, Key/binary>> = Key1,

    Set = bigset_keys:decode_set(Key),
    {Sender, Set, {Key, Value}}.

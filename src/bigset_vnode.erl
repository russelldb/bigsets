-module(bigset_vnode).
-behaviour(riak_core_vnode).
-include("bigset.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_vnode/1,
         coordinate/2,
         replicate/2,
         read/2,
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
                db %% eleveldb handle
               }).

-type state() :: #state{}.
-type status() :: orddict:orddict().
-type level_put() :: {put, Key :: binary(), Value :: binary()}.

-define(READ_OPTS, [{fill_cache, true}]).
-define(WRITE_OPTS, [{sync, false}]).
-define(FOLD_OPTS, [{iterator_refresh, true}]).

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

%% @doc read a subset of defined keys to detrmine membership (does set
%% contain [x.y,z]
contains(PrefList, Req=?CONTAINS_REQ{}) ->
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
    ClockKey = bigset:clock_key(Set, Id),
    Clock = clock(eleveldb:get(DB, ClockKey, ?READ_OPTS)),
    {Clock2, InsertWrites, ReplicationPayload} = gen_inserts(Set, Inserts, Id, Clock),

    %% Each element has the context it was read with, generate
    %% tombstones for those dots.
    {Clock3, DeleteWrites} = gen_removes(Set, Removes, Ctx, Clock2),

    %% @TODO(rdb|optimise) technically you could ship the deletes
    %% right now, if you wanted (in fact deletes can be broadcast
    %% without a coordinator, how cool!)

    BinClock = bigset:to_bin(Clock3),

    Writes = lists:append([[{put, ClockKey, BinClock}],  InsertWrites, DeleteWrites]),
    ok = eleveldb:write(DB, Writes, ?WRITE_OPTS),
    %% Just pass on the removes, the downstream replicas need to strip
    %% causal information from them
    riak_core_vnode:reply(Sender, {dw, Partition, ReplicationPayload, Removes}),
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
    {Clock, Inserts} = replica_inserts(eleveldb:get(DB, ClockKey, ?READ_OPTS), Ins),
    {Clock1, Deletes} = replica_removes(Clock, Rems),
    BinClock = bigset:to_bin(Clock),
    Writes = lists:append([[{put, ClockKey, BinClock}], Inserts, Deletes]),
    ok = eleveldb:write(DB, Writes, ?WRITE_OPTS),
%%    lager:debug("I wrote ~p~n", [decode_writes(Writes, [])]),
    riak_core_vnode:reply(Sender, {dw, Partition}),
    {noreply, State};
handle_command(?READ_REQ{set=Set}, Sender, State) ->
    %% read is an async fold operation
    %% @see bigset_vnode_worker for that code.
    #state{db=DB} = State,
    {async, {get, DB, Set}, Sender, State};
handle_command(?CONTAINS_REQ{set=Set, members=Members}, Sender, State) ->
    %% contains is a special kind of read, and an async fold operation
    %% @see bigset_vnode_worker for that code.
    #state{db=DB} = State,
    {async, {subset_get, DB, Set, Members}, Sender, State}.

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
    %% @TODO(rdb|correct) -> is this _really_ OK, what if we _know_
    %% (how?) the set exists, a missing clock is bad.
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
%% operation, given the elements, set, and clock, also generates the
%% replicaton payload since they are similar, but not the same, and
%% there is no point traversing one to transform it into the other.
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
                        %% @TODO(rdb|optimise) this is a temporary
                        %% hack where we use a plain old erlang binary
                        %% to duplicate the data in the key, the
                        %% reason being it is way cheaper than
                        %% sext:decoding when reading. Ideally we
                        %% would use this format for the key and some
                        %% custom comparator in leveldb for key
                        %% sorting.
                        Val = bigset:insert_member_value(Element, Id, Cnt),
                        {
                          C2, %% New Clock
                          [{put, ElemKey, Val} | W], %% To write
                          [{ElemKey, Val, Dot} | R] %% To replicate
                        }
                end,
                {Clock, [], []},
                Inserts).

%% @private gen_removes: generate the writes needed to for a
%% delete. Requires a context.
-spec gen_removes(Set :: binary(),
                  Removes :: [binary()],
                  Ctx :: binary(),
                  Clock :: bigset_clock:clock()) ->
                         [level_put()].
gen_removes(_Set, []=_Removes, _Ctx, Clock) ->
    {Clock, []};
gen_removes(Set, Removes, Ctx, Clock) ->
    %% eugh, I hate this elements*actors iteration
    Decoder = bigset_ctx_codec:new_decoder(Ctx),
    {Clock2, Rems} = lists:foldl(fun({E, CtxBin}, {ClockAcc, Deletes}) ->
                                         ECtx = bigset_ctx_codec:decode_dots(CtxBin, Decoder),
                                         lists:foldl(fun({Actor, Cnt}=Dot, {C, W}) ->
                                                             {bigset_clock:strip_dots(Dot, C),
                                                              [{put, bigset:remove_member_key(Set, E, Actor, Cnt), <<>>} | W]
                                                             }
                                                     end,
                                                     {ClockAcc, Deletes},
                                                     ECtx)
                                 end,
                                 {Clock, []},
                                 Removes),
    {Clock2, lists:flatten(Rems)}.

%% @private generate the write set, don't write stuff you wrote
%% already. This is important, not just an optimisation. Imagine some
%% write at dot {a, 1} has been tombstoned, and the tombstone
%% removed. Re-writing {a,1} will cause the element to re-surface. We
%% check the dot on the write, if we already saw it, we already wrote
%% it, do not write again! Returns {clock, writes}
-spec replica_inserts(not_found | {ok, binary()},
                     [delta_element()]) ->
                            [level_put()].
replica_inserts(not_found, Elements) ->
    F = fun({Key, Val, Dot}, {Clock, Writes}) ->
                C2 = bigset_clock:strip_dots(Dot, Clock),
                {C2, [{put, Key, Val} | Writes]}
        end,
    lists:foldl(F, {bigset_clock:fresh(), []}, Elements);
replica_inserts({ok, BinClock}, Elements) ->
    Clock0 = bigset:from_bin(BinClock),
    F = fun({Key, Val, Dot}, {Clock, Writes}) ->
                case bigset_clock:seen(Clock, Dot) of
                    true ->
                        %% No op, skip it/discard
                        {Clock, Writes};
                    false ->
                        %% Strip the dot
                        C2 = bigset_clock:strip_dots(Dot, Clock),
                        {C2, [{put, Key, Val} | Writes]}
                end
        end,
    lists:foldl(F, {Clock0, []}, Elements).


%% @private generate the tombstone write set, there is no way to tell
%% if we wrote a tombstone before (only that we didn't!), and it is
%% safe to re-write one as compaction will remove it already. Imagine
%% we wrote some element at {a,1}, we have seen {a,1} and it is in the
%% clock. The tombstone for {a,1} has the same causal information, we
%% cannot distinguish a tombstone write from a write, so at the risk
%% of wastefullness, re-do the write. If {a, 1} was already
%% tombstoned, and the tombstone compacted, the next compaction will
%% take it out again. Returns {clock, writes}
-spec replica_removes(bigset_clock:clock(),
                     [delta_element()]) ->
                            [level_put()].
replica_inserts(not_found, Elements) ->
    F = fun({Key, Val, Dot}, {Clock, Writes}) ->
                C2 = bigset_clock:strip_dots(Dot, Clock),
                {C2, [{put, Key, Val} | Writes]}
        end,
    lists:foldl(F, {bigset_clock:fresh(), []}, Elements);
replica_inserts({ok, BinClock}, Elements) ->
    Clock0 = bigset:from_bin(BinClock),
    F = fun({Key, Val, Dot}, {Clock, Writes}) ->
                case bigset_clock:seen(Clock, Dot) of
                    true ->
                        %% No op, skip it/discard
                        {Clock, Writes};
                    false ->
                        %% Strip the dot
                        C2 = bigset_clock:strip_dots(Dot, Clock),
                        {C2, [{put, Key, Val} | Writes]}
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


-ifdef(TEST).

%% @doc test that the clock correctly pulls dots from the removes.
gen_removes_test() ->
    %% Ctx is an binary encoded dict of actor->Id Each remove is a
    %% pair {element, BinCtx::binary()} and that BinCtx can be decoded
    %% into a list of dots with a decoder created from the Ctx
    %% @todo(bad|rdb) knows about internals of clock

    %% NOTE: probably b's clock, since there are no gaps for b.
    Set = <<"friends">>,
    Clock = {[{<<"a">>, 10},
              {<<"b">>, 5},
              {<<"c">>, 8}],
             orddict:from_list([{<<"a">>, [12, 13, 22]},
                                {<<"c">>, [14, 23]}])},

    Encoder = bigset_ctx_codec:new_encoder(Clock),

    %% dots from the clock above, best to add some gaps too
    Elements = [{<<"element1">>, [ {<<"a">>, 22}, {<<"b">>, 5}]},
                {<<"element2">>, [{<<"a">>, 2}, {<<"c">>, 14}]},
                {<<"element3">>, [{<<"a">>, 10}, {<<"b">>, 2}]},
                {<<"element4">>, [{<<"b">>, 1}, {<<"c">>, 23}]},
                {<<"element5">>, [{<<"a">>, 13}, {<<"c">>, 4}]}],

    Removes = lists:foldl(fun({Element, Dots}, Acc) ->
                                  {BinCtx, _Enc} = bigset_ctx_codec:encode_dots(Dots, Encoder),
                                  [{Element, BinCtx} | Acc]
                          end,
                          [],
                          Elements),
    Ctx =  bigset_ctx_codec:dict_ctx(Encoder),

    {ResClock, Writes} = gen_removes(Set, Removes, Ctx, Clock),

    %% Slurping the remove dots into same node/clock should mean no
    %% change to clock
    ?assertEqual(ResClock, Clock),
    ?assertEqual(length(Elements) * 2, length(Writes)),

    {ResClock2, Writes} = gen_removes(Set, Removes, Ctx, bigset_clock:fresh()),

    %% Slurping remove dots into empty clock should have _just those
    %% dots_ (with contiguous from base (0) compressed)
    ExpectedFromEmptyClock = {[{<<"b">>, 2}],
                               orddict:from_list([{<<"a">>, [2, 10, 13, 22]},
                                                  {<<"b">>, [5]},
                                                  {<<"c">>, [4, 14, 23]}])},

    ?assertEqual(ExpectedFromEmptyClock, ResClock2).


-endif.

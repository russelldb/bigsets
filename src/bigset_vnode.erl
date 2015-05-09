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

-ignore_xref([
             start_vnode/1
             ]).

-record(state, {partition,
                vnodeid=undefined, %% actor
                db %% eleveldb handle
               }).

-type set() :: binary().
-type member() :: binary().
-type actor() :: binary().
-type counter() :: pos_integer().
-type key() :: binary().
-type clock_key() :: {s, set(), clock}.
%% TombStoneBit, 0 for added, 1 for removed.
-type tsb() :: <<_:1>>.
-type member_key() :: {s, set(), member(), actor(), counter(), tsb()}.
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
    {ok, DB} =  eleveldb:open(DataDir, Opts),
    %% @TODO(rdb|question) Maybe this pool should be BIIIIG for many gets
    PoolSize = app_helper:get_env(bigset, worker_pool_size, 100),
    VnodeId = vnode_id(Partition),
    {ok, #state {vnodeid=VnodeId,  partition=Partition, db=DB},
     [{pool, bigset_vnode_worker, PoolSize, [{batch_size, 1000}]}]}.

%% COMMANDS(denosneold!)
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};
handle_command(?OP{set=Set, inserts=Inserts, removes=Removes}, Sender, State) ->
    %% Store elements in the set.
    #state{db=DB, partition=Partition, vnodeid=Id} = State,
    ClockKey = clock_key(Set),
    Clock = clock(eleveldb:get(DB, ClockKey, ?READ_OPTS)),
    {Clock2, InsertWrites, ReplicationPayload} = gen_inserts(Set, Inserts, Id, Clock),

    %% Each element has the context it was read with, generate
    %% tombstones for those dots.

    %% @TODO(rdb|correctness) can you strip dots from removes? I think
    %% so, maybe.
    DeleteWrites = gen_removes(Set, Removes),

    %% @TODO(rdb|optimise) technically you could ship the deletes
    %% right now, if you wanted (in fact deletes can be broadcast
    %% without a coordinator, how cool!)

    BinClock = to_bin(Clock2),

    Writes = lists:append([[{put, ClockKey, BinClock}],  InsertWrites, DeleteWrites]),
    eleveldb:write(DB, Writes, ?WRITE_OPTS),
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
    ClockKey = clock_key(Set),
    {Clock, Inserts} = replica_writes(eleveldb:get(DB, ClockKey, ?READ_OPTS), Ins),
    BinClock = to_bin(Clock),
    Writes = lists:append([[{put, ClockKey, BinClock}], Inserts, Rems]),
    ok = eleveldb:write(DB, Writes, ?WRITE_OPTS),
    riak_core_vnode:reply(Sender, {dw, Partition}),
    {noreply, State};
handle_command(?READ_REQ{set=Set}, Sender, State) ->
    #state{db=DB, partition=Partition} = State,
    %% clock is first key
    %% read all the way to last element
    FirstKey = clock_key(Set),

    %% @todo this is a mess, need to rewrite, and needs acks, and batches etc
    FoldFun = fun({Key, Value}, Acc) ->
                      case decode_key(Key) of
                          {s, Set, clock} ->
                              %% Set clock, send at once!
                              Clock = from_bin(Value),
                              riak_core_vnode:reply(Sender,
                                                    {r, Partition, clock, Clock}),
                              %% there is a clock, so change to real acc from `not_found'
                              bigset_fold_acc:new();
                          {s, Set, Element, Actor, Cnt, TSB} ->
                              %% @TODO buffer, flush, ack backpressure, stop etc!
                              bigset_fold_acc:add(Element, Actor, Cnt, TSB, Acc);
                          _ ->
                              throw({break, Acc})
                      end
              end,
    Folder = fun() ->
                     AccFinal =
                         try
                             eleveldb:fold(DB, FoldFun, not_found, [FirstKey | ?FOLD_OPTS])
                         catch
                             {break, Res} ->
                                 Res
                         end,
                     if AccFinal==not_found ->
                             riak_core_vnode:reply(Sender, {r, Partition, not_found}),
                             riak_core_vnode:reply(Sender, {r, Partition, done});
                        true ->
                             Elements = bigset_fold_acc:finalise(AccFinal),
                             riak_core_vnode:reply(Sender, {r, Partition, elements, Elements}),
                             riak_core_vnode:reply(Sender, {r, Partition, done})
                     end
             end,
    {async, {get, Folder}, Sender, State}.

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

%%% codec
clock_key(Set) ->
    sext:encode({s, Set, clock}).

%% @private sext encodes the element key so it is in order, on disk,
%% with the other elements. Use the actor ID and counter (dot)
%% too. This means at some extra storage, but makes for no reads
%% before writes on replication/delta merge. See read for how the
%% leveldb merge magic will work. Essentially every key {s, Set, E, A,
%% Cnt, 0} that has some key {s, Set, E, A, Cnt', 0} where Cnt' > Cnt
%% can be removed in compaction, as can every key {s, Set, E, A, Cnt,
%% 0} which has some key {s, Set, E, A, Cnt', 1} whenre Cnt' >=
%% Cnt. As can every key {s, Set, E, A, Cnt, 1} where the VV portion
%% of the set clock >= {A, Cnt}. Crazy!!
-spec insert_member_key(set(), member(), actor(), counter()) -> key().
insert_member_key(Set, Elem, Actor, Cnt) ->
    sext:encode({s, Set, Elem, Actor, Cnt, <<0:1>>}).

-spec remove_member_key(set(), member(), actor(), counter()) -> key().
remove_member_key(Set, Element, Actor, Cnt) ->
    sext:encode({s, Set, Element, Actor, Cnt, <<1:1>>}).

%% @private decode a binary key
-spec decode_key(key()) -> clock_key() | member_key().
decode_key(Bin) when is_binary(Bin) ->
    sext:decode(Bin);
decode_key(Bin) ->
    Bin.


-spec clock(not_found | {ok, binary()}) -> bigset_clock:clock().
clock(not_found) ->
    bigset_clock:fresh();
clock({ok, ClockBin}) ->
    from_bin(ClockBin).

from_bin(B) ->
    binary_to_term(B).

to_bin(T) ->
    term_to_binary(T).

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
                       ElemKey = insert_member_key(Set, Element, Id, Cnt),
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
                  Removes :: [binary()]) ->
                         [level_put()].
gen_removes(_Set, []=_Removes) ->
    [];
gen_removes(Set, Removes) ->
    %% eugh, I hate this elements*actors iteration
    lists:foldl(fun({E, CtxBin}, A) ->
                        Ctx = from_bin(CtxBin),
                        Deletes = [{put, remove_member_key(Set, E, Actor, Cnt), <<>>} ||
                                      {Actor, Cnt} <- Ctx],
                        [Deletes | A]
                end,
                [],
                Removes).

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
    Clock0 = from_bin(BinClock),
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


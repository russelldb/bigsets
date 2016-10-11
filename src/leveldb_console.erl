%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% copy/pasta from https://github.com/gordonguthrie/bits/blob/master/src/leveldb_console.erl
%%% @end
%%% Created :  6 Oct 2015 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------

-module(leveldb_console).

-compile(export_all).

-define(FOLD_OPTS, [{iterator_refresh, true}, {fold_method, streaming}]).

dump_set(Set) ->
    [{_Idx, Id, Ref} | _] = get_vnode_refs(Set),
    dump_set(Set, Id, Ref).

dump_sets(Set) ->
    [{Idx, dump_set(Set, Id, Ref)} || {Idx, Id, Ref} <- get_vnode_refs(Set)].

dump_set(Set, Id, Ref) ->
    FirstKey = bigset_keys:clock_key(Set, Id),
    FoldOpts = [{first_key, FirstKey} | ?FOLD_OPTS],
    FoldFun = fun dump_fun/2,
    Res =  try
               eleveldb:fold(Ref, FoldFun, [], FoldOpts)
           catch {break, Acc} ->
                   Acc
           end,
    {ok, lists:reverse(Res)}.

dump_fun({Key, Val}, []) ->
    %% First call, clock key
    {clock, Set, _Actor} = bigset_keys:decode_key(Key),
    Clock = bigset:from_bin(Val),
    [{Set, clock, Clock}];
dump_fun({Key, _Val}, Acc) ->
    case bigset_keys:decode_key(Key) of
        {clock, _, _} ->
            %% Done, break
            throw({break, Acc});
        DecodedKey ->
            [DecodedKey | Acc]
    end.

dump_raw_fun() ->
    fun ?MODULE:dump_raw_fun/2.

dump_raw_fun({K, V}, Acc) when is_binary(K) andalso is_binary(V) ->
    io:format("Key: ~p~n-Val: ~p~n", [K, V]),
    Acc.

run_fold(Set, Fun) ->
    [{_Idx, _Id, Ref} | _] = get_vnode_refs(Set),
    eleveldb:fold(Ref, Fun, [], ?FOLD_OPTS).

dump_buckets(Set) when is_binary(Set) ->
    [dump_bucket_raw(Ref) || {_Idx, _Id, Ref} <- get_vnode_refs(Set)].

dump_bucket_raw(Ref) ->
    FinalAcc = eleveldb:fold(Ref, fun dump_raw_fun/2, [], ?FOLD_OPTS),
    {ok, FinalAcc}.

get_state_data(Pid) ->
    {status, Pid, _Mod, Status} = sys:get_status(Pid),
    Status2 = lists:flatten(Status),
    Status3 = [L || {data, L} <- Status2],
    Status4 = lists:flatten(Status3),
    proplists:get_value("StateData", Status4).

%% Idx is a vnode index (the long number)
get_level_reference(Idx) ->
    {ok, Pid} = riak_core_vnode_manager:get_vnode_pid(Idx, bigset_vnode),
    State = get_state_data(Pid),
    {state, Idx, VnodeId, _DataDir, DBRef} = element(4, State),
    {Idx, VnodeId, DBRef}.

dump_stats(Ref) ->
    StatsRefs = [
		 <<"leveldb.ROFileOpen">>,
		 <<"leveldb.ROFileClose">>,
		 <<"leveldb.ROFileUnmap">>,
		 <<"leveldb.RWFileOpen">>,
		 <<"leveldb.RWFileClose">>,
		 <<"leveldb.RWFileUnmap">>,
		 <<"leveldb.ApiOpen">>,
		 <<"leveldb.ApiGet">>,
		 <<"leveldb.ApiWrite">>,
		 <<"leveldb.WriteSleep">>,
		 <<"leveldb.WriteWaitImm">>,
		 <<"leveldb.WriteWaitLevel0">>,
		 <<"leveldb.WriteNewMem">>,
		 <<"leveldb.WriteError">>,
		 <<"leveldb.WriteNoWait">>,
		 <<"leveldb.GetMem">>,
		 <<"leveldb.GetImm">>,
		 <<"leveldb.GetVersion">>,
		 <<"leveldb.SearchLevel[0]">>,
		 <<"leveldb.SearchLevel[1]">>,
		 <<"leveldb.SearchLevel[2]">>,
		 <<"leveldb.SearchLevel[3]">>,
		 <<"leveldb.SearchLevel[4]">>,
		 <<"leveldb.SearchLevel[5]">>,
		 <<"leveldb.SearchLevel[6]">>,
		 <<"leveldb.TableCached">>,
		 <<"leveldb.TableOpened">>,
		 <<"leveldb.TableGet">>,
		 <<"leveldb.BGCloseUnmap">>,
		 <<"leveldb.BGCompactImm">>,
		 <<"leveldb.BGNormal">>,
		 <<"leveldb.BGCompactLevel0">>,
		 <<"leveldb.BlockFiltered">>,
		 <<"leveldb.BlockFilterFalse">>,
		 <<"leveldb.BlockCached">>,
		 <<"leveldb.BlockRead">>,
		 <<"leveldb.BlockFilterRead">>,
		 <<"leveldb.BlockValidGet">>,
		 <<"leveldb.Debug[0]">>,
		 <<"leveldb.Debug[1]">>,
		 <<"leveldb.Debug[2]">>,
		 <<"leveldb.Debug[3]">>,
		 <<"leveldb.Debug[4]">>,
		 <<"leveldb.ReadBlockError">>,
		 <<"leveldb.DBIterNew">>,
		 <<"leveldb.DBIterNext">>,
		 <<"leveldb.DBIterPrev">>,
		 <<"leveldb.DBIterSeek">>,
		 <<"leveldb.DBIterSeekFirst">>,
		 <<"leveldb.DBIterSeekLast">>,
		 <<"leveldb.DBIterDelete">>,
		 <<"leveldb.eleveldbDirect">>,
		 <<"leveldb.eleveldbQueued">>,
		 <<"leveldb.eleveldbDequeued">>,
		 <<"leveldb.elevelRefCreate">>,
		 <<"leveldb.elevelRefDelete">>,
		 <<"leveldb.ThrottleGauge">>,
		 <<"leveldb.ThrottleCounter">>,
		 <<"leveldb.ThrottleMicros0">>,
		 <<"leveldb.ThrottleKeys0">>,
		 <<"leveldb.ThrottleBacklog0">>,
		 <<"leveldb.ThrottleCompacts0">>,
		 <<"leveldb.ThrottleMicros1">>,
		 <<"leveldb.ThrottleKeys1">>,
		 <<"leveldb.ThrottleBacklog1">>,
		 <<"leveldb.ThrottleCompacts1">>
		],
    PrintFun = fun(X) ->
		       Text = binary_to_list(X),
		       {ok, S} = eleveldb:status(Ref, X),
		       Stat = list_to_integer(binary_to_list(S)),
		       io:format("~s: ~p~n", [Text, Stat]),
		       ok
	       end,
    [PrintFun(X) || X <- StatsRefs],
    ok.

get_vnode_refs(Set) ->
    Hash = riak_core_util:chash_key({bigset, Set}),
    UpNodes = riak_core_node_watcher:nodes(bigset),
    PL = riak_core_apl:get_apl_ann(Hash, 3, UpNodes),
    [get_level_reference(Idx) || {{Idx, _},_} <- PL].


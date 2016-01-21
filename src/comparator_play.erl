%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% Quick/dirty by-eye testing of comparator
%%% @end
%%% Created : 15 Oct 2015 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(comparator_play).

-compile(export_all).

-define(BS_DATA_DIR, "/tmp/comparator_play_bs/").
-define(DEF_DATA_DIR, "/tmp/comparator_play_def/").
-define(DATA_DIR, "/tmp/comparator_play/").
-define(WRITE_OPTS, [{sync, false}]).
-define(FOLD_OPTS, [{iterator_refresh, true}, {fold_method, streaming}]).


start(default) ->
    start(?DEF_DATA_DIR, false);
start(bigsets) ->
    start(?BS_DATA_DIR, true).

start(DataDir, BigsetsBool) ->
    %% create a leveldb instane with the correct comparator
    Opts =  [{create_if_missing, true},
             {write_buffer_size, 1024*1024},
             {max_open_files, 20},
             {bigsets, BigsetsBool}],
    {ok, Ref} = eleveldb:open(DataDir, Opts),
    Ref.

store(Key, Val, Ref) ->
    eleveldb:write(Ref, [{put, Key, Val}], ?WRITE_OPTS).

dump_db(Ref) ->
    eleveldb:fold(Ref, dump_raw_fun(), [], ?FOLD_OPTS).

dump_raw_fun() ->
    fun ?MODULE:dump_raw_fun/2.

dump_raw_fun({K, V}, Acc) ->
    io:format("Key: ~p~n-Val: ~p~n", [K, V]),
    Acc.

stop(Ref) ->
    eleveldb:close(Ref).

test() ->
    BSRef = start(bigsets),
    RegRef = start(default),
    [store(Key, Val, Ref) || {Key, Val} <- key_vals(),
                             Ref <- [BSRef, RegRef]],

    io:format("dumping Def~n"),
    dump_db(RegRef),

    io:format("dumping BS~n"),
    dump_db(BSRef),

    [stop(Ref) || Ref <- [BSRef, RegRef]],
    [eleveldb:destroy(DataDir, []) || DataDir <- [?BS_DATA_DIR,
                                                  ?DEF_DATA_DIR]].


key_vals() ->
    S1 = <<"set1">>,
    S2 = <<"z2">>,
    A1 = <<"actor1">>,
    A2 = <<"zor2">>,
    A0 = <<"000actor0">>,
    E1 = <<"element1">>,
    E2 = <<"znt2">>,
    E0 = <<"0000__kjlkjkjj lngelement3">>,

    [{bigset:clock_key(S1, A2), <<"clock key s1 a2">>},
     {bigset:insert_member_key(S1, E1, A1, 5), <<" S1, E1, A1, 5 Add">>},
     {bigset:insert_member_key(S1, E1, A1, 1), <<" S1, E1, A1, 1 Add">>},
     {bigset:clock_key(S2, A2), <<"clock key s2 a2">>},
     {bigset:end_key(S2), <<"end key s2">>},
     {bigset:clock_key(S1, A1), <<"clock key s1 a1">>},
     {bigset:end_key(S1), <<"end key s1">>},
     {bigset:insert_member_key(S1, E2, A0, 13), <<" S1, E2, A0, 13 Add">>},
     {bigset:remove_member_key(S1, E0, A2, 9), <<" S1, E0, A2, 9 rem">>},
     {bigset:clock_key(S2, A0), <<"clock key s2 a0">>},
     {bigset:insert_member_key(S1, E2, A1, 1), <<" S1, E2, A1, 1 Add">>},
     {bigset:insert_member_key(S2, E2, A1, 1), <<" S2, E2, A1, 1 Add">>},
     {bigset:insert_member_key(S1, E1, A0, 22), <<" S1, E1, A0, 22 Add">>},
     {bigset:insert_member_key(S1, E0, A2, 9), <<" S1, E0, A2, 9 Add">>},
     {bigset:insert_member_key(S2, E0, A2, 9), <<" S2, E0, A2, 9 Add">>},
     {bigset:remove_member_key(S1, E0, A2, 8), <<" S1, E0, A2, 8 Rem">>},
     {bigset:set_tombstone_key(S2, A1), <<"set tombstone key set 2 actor 1">>},
     {bigset:set_tombstone_key(S1, A0), <<"set tombstone key set 1 actor 0">>},
     {bigset:set_tombstone_key(S1, A2), <<"set tombstone key set 1 actor 2">>},
     {bigset:clock_key(S1, A0), <<"clock key set1 actor 0">>}
    ].


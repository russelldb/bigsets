%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%%
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

    [stop(Ref) || Ref <- [BSRef, RegRef]].
    %% [eleveldb:destroy(DataDir, []) || DataDir <- [?BS_DATA_DIR,
    %%                                               ?DEF_DATA_DIR]].


key_vals() ->
    S1 = <<"set1">>,
    A1 = <<"actor1">>,
    A2 = <<"zor2">>,
    A0 = <<"000actor0">>,
    E1 = <<"element1">>,
    E2 = <<"znt2">>,
    E0 = <<"0000__kjlkjkjj lngelement3">>,
    [{bigset:clock_key(S1, A2), <<"clock key s1 a2">>},
     {bigset:insert_member_key(S1, E1, A1, 5), <<" S1, E1, A1, 5 Add">>},
     {bigset:insert_member_key(S1, E1, A1, 1), <<" S1, E1, A1, 1 Add">>},
     {bigset:clock_key(S1, A1), <<"clock key s1 a1">>},

     {bigset:insert_member_key(S1, E2, A0, 13), <<" S1, E2, A0, 13 Add">>},
     {bigset:remove_member_key(S1, E0, A2, 9), <<" S1, E0, A2, 9 rem">>},
     {bigset:insert_member_key(S1, E2, A1, 1), <<" S1, E2, A1, 1 Add">>},
     {bigset:insert_member_key(S1, E1, A0, 22), <<" S1, E1, A0, 22 Add">>},
     {bigset:insert_member_key(S1, E0, A2, 9), <<" S1, E0, A2, 9 Add">>},
     {bigset:remove_member_key(S1, E0, A2, 8), <<" S1, E0, A2, 8 Rem">>}].

    %% Actor1 = <<"actor1">>,
    %% lists:foldl(fun(Set, Acc) ->
    %%                     Res = [{bigset:clock_key(Set, Actor1), val(Set)},
    %%                            {biget:insert_member_key(Set, "000apple1"), val(Set ++ " elem " ++  "000appple1")},
    %%                            {elem(Set, "zeba"), val(Set ++ " eleme " ++ "zeba")}],
    %%                     lists:append(Res, Acc)
    %%             end,
    %%             [],
    %%             ["zebra", "zeee", "abalone", "1230apple", "apple", "appl1",
    %%              "aaaaaaaaaaaaaaaaaaaaaa","z", "a",  "za", "cat", "caatz", "barn"]).

val(Key) ->
    list_to_binary(Key).

key(Key) ->
    Bin = val(Key),
    Len = byte_size(Bin),
    <<Len:32/little-unsigned-integer, Bin:Len/binary>>.

clock(Set) ->
    Key = key(Set),
    <<Key/binary, 0:32/little-unsigned-integer>>.

elem(Set, Elem) ->
    Key = key(Set),
    EBin = key(Elem),
    <<Key/binary, EBin/binary>>.



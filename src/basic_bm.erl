-module(basic_bm).
-compile([export_all]).

-include("/Users/russell/dev/e/basho/proto/riak-2.1/deps/riak_kv/include/riak_kv_types.hrl").

%% Should I do something weird here with cookies? Connect to bigset,
%% run bm, change cookie, connect to riak, run bm?
bm(Set, Size) ->
    BSNode = 'bigset1@127.0.0.1',
    RNode = 'dev1@127.0.0.1',
    WFile = filename:join(["bms",  binary_to_list(Set) ++  ".writes"]),
    RFile = filename:join(["bms", binary_to_list(Set) ++ ".reads"]),

    {ok, FD} = file:open(WFile, [write]),
    {ok, RFD} = file:open(RFile, [write]),

    file:write(FD, io_lib:fwrite("Elements,Time,Type~n", [])),
    file:write(RFD, io_lib:fwrite("Elements,Time,Type~n", [])),

    file:close(FD),
    file:close(RFD),

    Words = bigset:read_words(Size*2),
    Cookie = erlang:get_cookie(),
    erlang:set_cookie(node(), bigset),
    wait_until(fun() -> pong == net_adm:ping(BSNode) end),
    io:format("Bigset BM~n"),

    {Time, _} = timer:tc(fun() ->
                                 bs_bm(Set, Size, WFile, RFile, BSNode, Words)
                         end),

    io:format("bigset BM took ~p millis ~n", [Time div 1000]),

    erlang:set_cookie(node(), riak),
    wait_until(fun() -> pong == net_adm:ping(RNode) end),
    io:format("Riak BM~n"),

    {Time2, _} = timer:tc(fun() ->
                                 dt_bm(Set, Size, WFile, RFile, RNode, Words)
                         end),

    io:format("Riak BM took ~p millis ~n", [Time2 div 1000]),

    erlang:set_cookie(node(), Cookie),
    %% Now call R to generate plots
    io:format("Calling R~n"),
    os:cmd("Rscript --vanilla plot-times.r -i bms -s " ++ binary_to_list(Set)),
    os:cmd("Rscript --vanilla plot-times.r -i bms -s " ++ binary_to_list(Set) ++ " -l").

%% @TODO(rdb) maybe do the random bit when you generate the list of
%% words?
bs_bm(Set, Size, Node) ->
    WFile = filename:join(["bms","bs",  binary_to_list(Set) ++  ".writes"]),
    RFile = filename:join(["bms", "bs", binary_to_list(Set) ++ ".reads"]),
    Words = bigset:read_words(Size*2),
    bs_bm(Set, Size, WFile, RFile, Node, Words).

bs_bm(Set, Size, WFile, RFile, Node, Words) ->
    Limit = length(Words),

    {ok, FD} = file:open(WFile, [append]),
    {ok, RFD} = file:open(RFile, [append]),

    Client = bigset_client:new(Node),

    Vals = [begin
         Val = lists:nth(crypto:rand_uniform(1, Limit), Words),

         {Time, ok} =  timer:tc(bigset_client, update, [Set, [Val], Client]),
         file:write(FD, io_lib:fwrite("~p,~p,bigset~n", [N, Time])),
         if N rem 100 == 0 ->
                 %% do a read every 100
                 {RTime, {ok, _Ctx, _Cnt}} = timer:tc(bigset, stream_read, [Set, Client]),
                 file:write(RFD, io_lib:fwrite("~p,~p,bigset~n", [N, RTime]));
            true -> ok
         end,
         Val
     end || N <- lists:seq(1, Size)],
    file:close(FD),
    file:close(RFD),
    io:format("BS wrote ~p unique entries ~n", [sets:size(sets:from_list(Vals))]).

dt_bm(Set, Size, Node) ->
    WFile = filename:join(["bms","dt",  binary_to_list(Set) ++  ".writes"]),
    RFile = filename:join(["bms", "dt", binary_to_list(Set) ++ ".reads"]),
    Words = bigset:read_words(Size*2),
    dt_bm(Set, Size, WFile, RFile, Node, Words).

dt_bm(Set, Size, WFile, RFile, Node, Words) ->
    case sets_bucket_active(Node) of
        true ->
            ok;
        false ->
            make_sets_bucket(Node)
    end,

    io:format("bucket type made~n"),

    Limit = length(Words),

    {ok, FD} = file:open(WFile, [append]),
    {ok, RFD} = file:open(RFile, [append]),

    {ok, C} = riak:client_connect(Node),
    B = {<<"sets">>, <<"test">>},
    K = Set,
    O = riak_kv_crdt:new(B, K, riak_dt_orswot),

    Vals = [begin
                Val = lists:nth(crypto:rand_uniform(1, Limit), Words),
                Opp = #crdt_op{mod=riak_dt_orswot, op={add, Val}},
                Options1 = [{crdt_op, Opp}],
                {Time, ok} = timer:tc(fun() -> C:put(O, Options1) end),
                file:write(FD, io_lib:fwrite("~p,~p,riak~n", [N, Time])),
                if N rem 100 == 0 ->
                        %% do a read every 100
                        {RTime,  {{_Ctx0, _Value}, _Stats}} = timer:tc(fun() ->
                                                                               {ok, Res} = C:get(B, K, []),
                                                                               riak_kv_crdt:value(Res, riak_dt_orswot)
                                                                       end),
                        file:write(RFD, io_lib:fwrite("~p,~p,riak~n", [N, RTime]));
                   true -> ok
                end,
                Val
            end || N <- lists:seq(1, Size)],

    file:close(FD),
    file:close(RFD),
    io:format("wrote ~p unique entries~n", [sets:size(sets:from_list(Vals))]).

sets_bucket_active(Node) ->
    rpc:call(Node, riak_core_bucket_type, status, [<<"sets">>]) == active.

make_sets_bucket(Node) ->
    rpc:call(Node, riak_core_bucket_type, create, [<<"sets">>, [{dataype, set}]]),
    rpc:call(Node, riak_core_bucket_type, activate, [<<"sets">>]),
    wait_until(fun() -> ?MODULE:sets_bucket_active(Node) end).

wait_until(Fun) ->
    case Fun() of
        true ->
            ok;
        false ->
            wait_until(Fun)
    end.

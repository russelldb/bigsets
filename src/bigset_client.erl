%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% In server client. Node local or remote. Not for external API, internal API only.
%%% @end
%%% Created : 30 Sep 2015 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(bigset_client).
-include("bigset.hrl").
-include("bigset_trace.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         new/0,
         new/1,
         update/2,
         update/3,
         update/5,
         read/2,
         read/3,
         stream_read/2,
         stream_read/3,
         is_member/2,
         is_member/4,
         is_subset/2,
         is_subset/4
        ]).

-define(DEFAULT_TIMEOUT, 60000).

-type client() ::{bigset_client,  node()}.

%% Public API

%% Added param module style client code, for the sake of being like
%% riak for fairness in Benching

-spec new() -> client().
new() ->
    new(node()).

-spec new(node()) -> client().
new(Node) ->
    {?MODULE, Node}.


-spec is_subset(set(), [member()]) ->
                       {ok, [{member(), ctx()}]} |
                       {error, Reason::term()}.
is_subset(Set, Members) ->
    is_subset(Set, Members, [], new()).

is_subset(Set, Members, Options, {?MODULE, Node}) ->
    Me = self(),
    ReqId = mk_reqid(),
    Request = ?QUERY_FSM_ARGS{req_id=ReqId,
                              from=Me,
                              set=Set,
                              members=Members,
                              options=Options},

    ok = case node() of
             Node ->
                 {ok, _Pid} = bigset_query_fsm:start_link(Request),
                 ok;
             _ ->
                 _Pid = proc_lib:spawn_link(Node, bigset_query_fsm, start_link,
                                            [Request]),
                 ok
         end,
    Timeout = recv_timeout(Options),
    Res = wait_for_query(ReqId, Timeout),
    Res.

-spec is_member(set(), member()) ->
                       {true, ctx()} |
                       {false, <<>>} |
                       {error, Reason :: term()}.
is_member(Set, Member) ->
    is_member(Set, Member, [], new()).

-spec is_member(set(), member(), Options::proplists:proplist(), client()) ->
                       {true, ctx()} |
                       {false, <<>>} |
                       {error, Reason :: term()}.
is_member(Set, Member, Options, {?MODULE, _Node}=This) ->
    is_subset(Set, [Member], Options, This).

-spec update(Set::binary(), adds()) ->
                    ok | {error, Reason :: term()}.
update(Set, Adds) ->
    update(Set, Adds, [], [], new()).


-spec update(Set::binary(), adds(), client()) ->
                    ok | {error, Reason :: term()}.
update(Set, Adds, {?MODULE, _Node}=This) ->
    update(Set, Adds, [], [], This).

%% @doc update a Set
-spec update(Set::binary(),
             adds(),
             removes(),
             Options :: proplists:proplist(),
             client()) ->
                    ok | {error, Reason :: term()}.
update(Set, Adds, Removes, Options, {?MODULE, Node}) ->
    Me = self(),
    ReqId = mk_reqid(),
    Ctx = proplists:get_value(ctx, Options),
    Op = ?OP{set=Set, inserts=Adds, removes=Removes, ctx=Ctx},
    ok = case node() of
             Node ->
                 {ok, _Pid} = bigset_write_fsm:start_link(ReqId, Me, Set, Op, Options),
                 ok;
             _ ->
                 _Pid =proc_lib:spawn_link(Node, bigset_write_fsm, start_link, [ReqId, Me, Set, Op, Options]),
                 ok
         end,
    Timeout = recv_timeout(Options),
    wait_for_reqid(ReqId, Timeout).

-spec read(set(),
           Options :: proplists:proplist()) ->
                  {ok, {ctx, binary()}, {elems, [{binary(), binary()}]}} |
                  {error, Reason :: term()}.
read(Set, Options) ->
    read(Set, Options, new()).

-spec read(set(),
           Options :: proplists:proplist(),
           client()) ->
                  {ok, {ctx, binary()}, {elems, [{binary(), binary()}]}} |
                  {error, Reason :: term()}.
read(Set, Options, {?MODULE, Node}) ->
    Opts = case validate_read_options(Options) of
               valid ->
                   Options;
               {invalid_options, Message} ->
                   throw({error, invalid_options, Message, Options})
           end,
    Me = self(),
    ReqId = mk_reqid(),
    Request = ?READ_FSM_ARGS{req_id=ReqId, from=Me, set=Set, options=Opts},

    case node() of
        Node ->
            {ok, _Pid} = bigset_read_fsm:start_link(Request),
            ok;
        _ ->
            _Pid = proc_lib:spawn_link(Node, bigset_read_fsm, start_link,
                                       [Request]),
            ok
    end,
    Timeout = recv_timeout(Options),
    Res = wait_for_read(ReqId, Timeout),
    Res.

-spec stream_read(set(),
                  Options :: proplists:proplist()) ->
                         {ok, ReqId :: term(), Pid :: pid()}.
stream_read(Set, Options) ->
    stream_read(Set, Options, new()).

-spec stream_read(set(),
                  Options :: proplists:proplist(),
                  client()) ->
                         {ok, ReqId :: term(), Pid :: pid()}.
stream_read(Set, Options, {?MODULE, Node}) ->
    Me = self(),
    ReqId = mk_reqid(),
    Request = ?READ_FSM_ARGS{req_id=ReqId, from=Me, set=Set, options=Options},

    Pid = case node() of
              Node ->
                  {ok, P} = bigset_read_fsm:start_link(Request),
                  P;
              _ ->
                  proc_lib:spawn_link(Node, bigset_read_fsm, start_link,
                                    [Request])
          end,
    {ok, ReqId, Pid}.

recv_timeout(Options) ->
    case proplists:get_value(recv_timeout, Options) of
        undefined ->
            %% If no reply timeout given, use the FSM timeout + 100ms to give it a chance
            %% to respond.
            proplists:get_value(timeout, Options, ?DEFAULT_TIMEOUT) + 100;
        Timeout ->
            %% Otherwise use the directly supplied timeout.
            Timeout
    end.

wait_for_reqid(ReqId, Timeout) ->
    receive
        {ReqId, Response} -> Response
    after Timeout ->
            {error, timeout}
    end.

-record(read_acc, {ctx,elements=[]}).

wait_for_query(ReqId, Timeout) ->
    receive
        {ReqId, Resp} ->
            {ok, Resp}
    after Timeout ->
            {error, timeout}
    end.

%% How to stream??  Ideally the process calling the client calls
%% receive!
wait_for_read(ReqId, Timeout) ->
    wait_for_read(ReqId, Timeout, #read_acc{}).

%% @private
wait_for_read(ReqId, Timeout, Acc) ->
    receive
        {ReqId, done} ->
            #read_acc{ctx=Ctx, elements=Elems} = Acc,
            {ok, {ctx, Ctx}, {elems, lists:flatten(Elems)}};
        {ReqId, {ok, {ctx, Ctx}}} ->
            wait_for_read(ReqId, Timeout, Acc#read_acc{ctx=Ctx});
        {ReqId, {ok, {elems, Res}}} ->
            #read_acc{elements=Elems} = Acc,
            wait_for_read(ReqId, Timeout, Acc#read_acc{elements=lists:append(Elems, Res)});
        {ReqId, {error, Error}} ->
            {error, Error}
    after Timeout ->
            {error, timeout}
    end.

%% @private
mk_reqid() ->
    erlang:phash2({self(), os:timestamp()}). % only has to be unique per-pid

validate_read_options(Opts) ->
    validate_read_options(Opts, []).

validate_read_options([], []) ->
    valid;
validate_read_options([], Errs) ->
    {invalid_options, Errs};
validate_read_options([{range_start, _}=Start | Opts], Errs) ->
    Errs2 = validate_range_options(Start, proplists:lookup(range_end, Opts), Errs),
    validate_read_options(Opts, Errs2);
validate_read_options([{range_end, _}=End | Opts], Errs) ->
    Errs2 = validate_range_options(proplists:lookup(range_start, Opts), End, Errs),
    validate_read_options(Opts, Errs2);
validate_read_options([{start_inclusive, Bool} | Opts], Errs) when is_boolean(Bool) ->
    validate_read_options(Opts, Errs);
validate_read_options([{start_inclusive, NotBool} | Opts], Errs) ->
    validate_read_options(Opts, [{start_inclusive_not_boolean, NotBool} | Errs]);
validate_read_options([start_inclusive | Opts], Errs) ->
    validate_read_options(Opts, Errs);
validate_read_options([{end_inclusive, Bool} | Opts], Errs) when is_boolean(Bool) ->
    validate_read_options(Opts, Errs);
validate_read_options([{end_inclusive, NotBool} | Opts], Errs) ->
    validate_read_options(Opts, [{end_inclusive_not_boolean, NotBool} | Errs]);
validate_read_options([end_inclusive | Opts], Errs) ->
    validate_read_options(Opts, Errs).

-spec validate_range_options(none | {range_start, binary()},
                             none | {range_end, binary()},
                             Errors :: list()) ->
                                    Errors :: list().
validate_range_options({range_start, Start}, none, Errs) when is_binary(Start) ->
    Errs;
validate_range_options(none, {range_end, End}, Errs) when is_binary(End) ->
    Errs;
validate_range_options({range_start, Start}, {range_end, End}, Errs) when is_binary(Start) andalso is_binary(End) andalso Start =< End ->
    Errs;
validate_range_options(Start, End, Errs) ->
    [{invalid_range_options, Start, End} | Errs].

%%% Tests

-ifdef(TEST).

validate_range_options_test() ->
    ?assertEqual([], validate_range_options(none, {range_end, <<"a">>}, [])),
    ?assertEqual([], validate_range_options({range_start, <<"a">>}, {range_end, <<"z">>}, [])),
    ?assertEqual([], validate_range_options({range_start, <<"a">>}, none, [])),
    ?assertMatch([_], validate_range_options({range_start, <<"z">>}, {range_end, <<"a">>}, [])),
    ?assertMatch([_], validate_range_options({range_start, a}, {range_end, <<"z">>}, [])),
    ?assertMatch([_], validate_range_options({range_start, <<"z">>}, {range_end, 10202}, [])).

-endif.

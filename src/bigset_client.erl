-module(bigset_client).
-include("bigset.hrl").
-include("bigset_trace.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         new/0,
         new/1,
         update/2,
         update/3,
         update/5,
         read/2,
         read/3,
         stream_read/2,
         stream_read/3
        ]).

-define(DEFAULT_TIMEOUT, 60000).

%% an opaque binary riak_dt_vclock:vclock()
-type context() :: binary() | undefined.
-type remove() :: {member(), context()}.
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

-spec update(set(), Adds :: [member()]) ->
                    ok | {error, Reason :: term()}.
update(Set, Adds) ->
    update(Set, Adds, [], [], new()).


-spec update(set(), Adds :: [member()], client()) ->
                    ok | {error, Reason :: term()}.
update(Set, Adds, {?MODULE, _Node}=This) ->
    update(Set, Adds, [], [], This).

%% @doc update a Set
-spec update(set(),
             Adds :: [member()],
             Removes :: [remove()],
             Options :: proplists:proplist(),
             client()) ->
                    ok | {error, Reason :: term()}.
update(Set, Adds, Removes, Options, {?MODULE, Node}) ->
    Me = self(),
    ReqId = mk_reqid(),
    %% if there are removes, there must a Ctx
    Ctx = validate_ctx(Removes, proplists:get_value(ctx, Options)),
    Op = ?OP{set=Set, inserts=Adds, removes=Removes, ctx=Ctx},
    case node() of
        Node ->
            bigset_write_fsm:start_link(ReqId, Me, Set, Op, Options);
        _ ->
            proc_lib:spawn_link(Node, bigset_write_fsm, start_link, [ReqId, Me, Set, Op, Options])
    end,
    Timeout = recv_timeout(Options),
    wait_for_reqid(ReqId, Timeout).

%% @private right now removes _MUST_ have a binary-context. Throw if
%% they do not!
validate_ctx([], Ctx) ->
    Ctx;
validate_ctx(_Removes, Ctx) when not is_binary(Ctx) ->
    throw({error, removes_require_ctx});
validate_ctx(_Removes, Ctx) ->
    Ctx.

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


    Me = self(),
    ReqId = mk_reqid(),

    dyntrace:p(1, ?CLIENT_READ),

    case node() of
        Node ->
            bigset_read_fsm:start_link(ReqId, Me, Set, Options);
        _ ->
            proc_lib:spawn_link(Node, bigset_read_fsm, start_link,
                                [ReqId, Me, Set, Options])
    end,
    Timeout = recv_timeout(Options),
    Res = wait_for_read(ReqId, Timeout),
    dyntrace:p(2, ?CLIENT_READ),
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
    Pid = case node() of
              Node ->
                  {ok, P} = bigset_read_fsm:start_link(ReqId, Me, Set, Options),
                  P;
              _ ->
                  proc_lib:spawn_link(Node, bigset_read_fsm, start_link,
                                    [ReqId, Me, Set, Options])
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
        {ReqId, {error, overload}=Response} ->
            case app_helper:get_env(riak_kv, overload_backoff, undefined) of
                Msecs when is_number(Msecs) ->
                    timer:sleep(Msecs);
                undefined ->
                    ok
            end,
            Response;
        {ReqId, Response} -> Response
    after Timeout ->
            {error, timeout}
    end.

-record(read_acc, {ctx,elements=[]}).

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
            wait_for_read(ReqId, Timeout, Acc#read_acc{elements=[Res|Elems]});
        {ReqId, {error, Error}} ->
            {error, Error}
    after Timeout ->
            {error, timeout}
    end.

%% @private
mk_reqid() ->
    erlang:phash2({self(), os:timestamp()}). % only has to be unique per-pid

-module(bigset_client).
-include("bigset.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
         update/3,
         read/2
        ]).

-define(DEFAULT_TIMEOUT, 60000).

%% A set key
-type set() :: binary().
%% and element to add/remove from a set
-type member() :: binary().
%% an opaque binary riak_dt_vclock:vclock()
-type context() :: binary() | undefined.


%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, bigset),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, bigset_vnode_master).


%% @doc update a Set
-spec update(set(),
             Adds :: [member()],
             Removes :: [member()]) ->
                    ok | {error, Reason :: term()}.
update(Set, Adds, Removes) ->
    update(Set, Adds, Removes, undefined, []).

%% @doc update a Set
-spec update(set(),
             Adds :: [member()],
             Removes :: [member()],
             Ctx :: context(),
             Options :: proplists:proplist()) ->
                    ok | {error, Reason :: term()}.
update(Set, Adds, Removes, Ctx, Options) ->
    Me = self(),
    ReqId = mk_reqid(),
    Op = ?OP{set=Set, inserts=Adds, removes=Removes, ctx=Ctx},
    bigset_write_fsm:start_link(ReqId, Me, Set, Op, Options),
    Timeout = recv_timeout(Options),
    wait_for_reqid(ReqId, Timeout).

read(Set, Options) ->
    Me = self(),
    ReqId = mk_reqid(),
    bigset_read_fsm:start_link(ReqId, Me, Set, Options),
    Timeout = recv_timeout(Options),
    wait_for_reqid(ReqId, Timeout).

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

%% @private
mk_reqid() ->
    erlang:phash2({self(), os:timestamp()}). % only has to be unique per-pid

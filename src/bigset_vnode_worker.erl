%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc This module uses the riak_core_vnode_worker behavior to
%% perform different tasks asynchronously. Worth noting: it is the
%% side effects of `handle_work/3' that matter.

-module(bigset_vnode_worker).
-behaviour(riak_core_vnode_worker).

-export([init_worker/3,
         handle_work/3]).

-include_lib("bigset.hrl").

-record(state, {partition :: pos_integer(),
               batch_size :: pos_integer()}).

-define(DEFAULT_BATCH_SIZE, 10000).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Initialize the worker.
init_worker(VNodeIndex, Args, _Props) ->
    BatchSize = proplists:get_value(batch_size, Args, ?DEFAULT_BATCH_SIZE),
    {ok, #state{partition=VNodeIndex, batch_size=BatchSize}}.

%% @doc Perform the asynchronous fold operation.  State is the state
%% returned from init return {noreply, State} or {replay, Reply,
%% State} the latter sends `Reply' to `Sender' using
%% riak_core_vnode:reply(Sender, Reply)
%% No need for lots of indirection here, is there?
handle_work({get, FoldFun, _BufferMod}, _Sender, State=#state{partition=Partition}) ->
    try
        FoldFun()
    catch
        throw:receiver_down -> ok;
        throw:stop_fold     -> ok;
        throw:_PrematureAcc  -> ok %%FinishFun(PrematureAcc)
    end,
    {reply, {r, Partition, done}, State}.

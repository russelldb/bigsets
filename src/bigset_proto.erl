%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created :  8 Jan 2015 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(bigset_proto).

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(READ_OPTS, [{fill_cache, true}]).
-define(WRITE_OPTS, [{sync, false}]).
-define(FOLD_OPTS, [{iterator_refresh, true}]).

-compile(export_all).

-record(state, {id=undefind, db=undefined}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Id) ->
    gen_server:start_link(?MODULE, Id, []).

insert(Pid, Set, Elem) ->
    gen_server:call(Pid, {insert, Set, Elem}).

store_replica(Pid, Set, {Elem, Dot}) ->
    gen_server:cast(Pid, {replica, Set, Elem, Dot}).

get(Pid, Set) ->
    gen_server:call(Pid, {read, Set}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(Id) ->
    DataDir = atom_to_list(Id),
    Opts =  [{create_if_missing, true},
             {write_buffer_size, 1024*1024},
             {max_open_files, 20}],
    {ok, DB} =  eleveldb:open(DataDir, Opts),
    {ok, #state{id=Id, db=DB}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({insert, Set, Element}, _From, State) ->
    #state{db=DB, id=Id} = State,
    %% get clock, if it doesn't exist, create it
    %% increment it
    %% get dot
    %% store new clock, and element with payload of new dot
    ClockKey = clock_key(Set),
    Clock = clock(eleveldb:get(DB, ClockKey, ?READ_OPTS)),
    {Dot, Clock2} = bigset_clock:increment(Id, Clock),
    ElemKey = elem_key(Set, Element),
    ElemValue = to_bin(Dot),
    BinClock = to_bin(Clock2),
    ok = eleveldb:write(DB, [{put, ClockKey, BinClock}, {put, ElemKey, ElemValue}], ?WRITE_OPTS),
    {reply, {ok, {Element, Dot}}, State};
handle_call({read, Set}, _From, State) ->
    #state{db=DB} = State,
    %% clock is first key
    %% read all the way to last element
    FirstKey = clock_key(Set),

    FoldFun = fun({Key, Value}, Acc) ->
                      {s, S, K} = sext:decode(Key),
                      if S == Set, K == clock ->
                              %% Set clock
                              {from_bin(Value), dict:new()};
                         S == Set, is_binary(K) ->
                              {Clock, Dict} = Acc,
                              {Clock, dict:store(K, from_bin(Value), Dict)};
                         true ->
                              {throw, Acc}
                      end
              end,
    Folder = fun() ->
                     try
                         eleveldb:fold(DB, FoldFun, [], [FirstKey | ?FOLD_OPTS])
                     catch
                         {break, AccFinal} ->
                             AccFinal
                     end
             end,
    {reply, {ok, Folder()}, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({replica, Set, Elem, Dot}, State) ->
    #state{db=DB} = State,
    %% Read local clock
    ClockKey = clock_key(Set),
    ElemKey = elem_key(Set, Elem),
    Writes = case eleveldb:get(DB, ClockKey, ?READ_OPTS) of
                 not_found ->
                     ElemValue = to_bin(Dot),
                     %% No clock, so no writes, strip and store
                     Clock = bigset_clock:strip_dots(Dot, bigset_clock:fresh()),
                     BinClock = to_bin(Clock),
                     [{put, ClockKey, BinClock}, {put, ElemKey, ElemValue}];
                 {ok, BinClock} ->
                     Clock = binary_to_term(BinClock),
                     case bigset_clock:seen(Clock, Dot) of
                         true ->
                             %% Dot seen, do nothing
                             [];
                         false ->
                             %% Dot unseen
                             Clock2 = bigset_clock:strip_dots(Dot, from_bin(BinClock)),
                             BinClock2 = to_bin(Clock2),

                             case eleveldb:get(DB, ElemKey, ?READ_OPTS) of
                                 not_found ->
                                     %% No local element, strip and store
                                     ElemValue = to_bin(Dot),
                                     [{put, ClockKey, BinClock2}, {put, ElemKey, ElemValue}];
                                 {ok, LocalDotsBin} ->
                                     %% local present && dot unseen
                                     %% merge with local elements dots
                                     %% store new element dots, store clock
                                     LocalDots = from_bin(LocalDotsBin),
                                     Dots = riak_dt_vclock:merge([Dot], LocalDots),
                                     DotsBin = to_bin(Dots),
                                     [{put, ClockKey, BinClock2}, {put, ElemKey, DotsBin}]
                             end
                     end
             end,
    ok = eleveldb:write(DB, Writes, ?WRITE_OPTS),
    {noreply, State}.

clock_key(Set) ->
    sext:encode({s, Set, clock}).

elem_key(Set, Elem) ->
    sext:encode({s, Set, Elem}).

clock(not_found) ->
    bigset_clock:fresh();
clock({ok, ClockBin}) ->
    binary_to_term(ClockBin).

from_bin(ClockOrDot) ->
    binary_to_term(ClockOrDot).

to_bin(Dot) ->
    term_to_binary(Dot).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
    #state{db=DB} = State,
    eleveldb:close(DB),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

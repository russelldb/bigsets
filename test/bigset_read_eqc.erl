%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 12 Jul 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_read_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

%% -- State ------------------------------------------------------------------
-record(state,{}).




-define(ELEMENTS, [<<"0">>,<<"1">>,<<"2">>,<<"3">>,<<"4">>,<<"5">>,<<"6">>,
 <<"7">>,<<"8">>,<<"9">>,<<"A">>,<<"B">>,<<"C">>,<<"D">>,
 <<"E">>,<<"F">>,<<"G">>,<<"H">>,<<"I">>,<<"J">>,<<"K">>,
 <<"L">>,<<"M">>,<<"N">>,<<"O">>,<<"P">>,<<"Q">>,<<"R">>,
 <<"S">>,<<"T">>,<<"U">>,<<"V">>,<<"W">>,<<"X">>,<<"Y">>,
 <<"Z">>,<<"a">>,<<"b">>,<<"c">>,<<"d">>,<<"e">>,<<"f">>,
 <<"g">>,<<"h">>,<<"i">>,<<"j">>,<<"k">>,<<"l">>,<<"m">>,
 <<"n">>,<<"o">>,<<"p">>,<<"q">>,<<"r">>,<<"s">>,<<"t">>,
 <<"u">>,<<"v">>,<<"w">>,<<"x">>,<<"y">>,<<"z">>]).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_test_() ->
    {timeout, 60, [
                   ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(30, ?QC_OUT(prop_chunks()))))
                  ]}.

run() ->
    run(?NUMTESTS).

run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_chunks())).

eqc_check() ->
    eqc:check(prop_chunks()).

eqc_check(File) ->
    {ok, Bytes} = file:read_file(File),
    CE = binary_to_term(Bytes),
    eqc:check(prop_chunks(), CE).

gen_element() ->
    elements(?ELEMENTS).



%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% -- Common pre-/post-conditions --------------------------------------------
%% @doc General command filter, checked before a command is generated.
-spec command_precondition_common(S, Cmd) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Cmd  :: atom().
command_precondition_common(_S, _Cmd) ->
    true.

%% @doc General precondition, applied *before* specialized preconditions.
-spec precondition_common(S, Call) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Call :: eqc_statem:call().
precondition_common(_S, _Call) ->
    true.

%% @doc General postcondition, applied *after* specialized postconditions.
-spec postcondition_common(S, Call, Res) -> true | term()
    when S    :: eqc_statem:dynamic_state(),
         Call :: eqc_statem:call(),
         Res  :: term().
postcondition_common(_S, _Call, _Res) ->
    true.

%% -- Operations -------------------------------------------------------------

%% --- Operation: vnode_send ---
%% @doc vnode_send_pre/1 - Precondition for generation
-spec vnode_send_pre(S :: eqc_statem:symbolic_state()) -> boolean().
vnode_send_pre(_S) ->
    true.

%% @doc vnode_send_args - Argument generator
-spec vnode_send_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
vnode_send_args(_S) ->
    [].

%% @doc vnode_send_pre/2 - Precondition for vnode_send
-spec vnode_send_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
vnode_send_pre(_S, _Args) ->
    true.

%% @doc vnode_send - The actual operation
vnode_send() ->
    ok.

%% @doc vnode_send_next - Next state function
-spec vnode_send_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
vnode_send_next(S, _Value, _Args) ->
    S.

%% @doc vnode_send_post - Postcondition for vnode_send
-spec vnode_send_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
vnode_send_post(_S, _Args, _Res) ->
    true.

%% @doc vnode_send_blocking - Is the operation blocking in this State
%% -spec vnode_send_blocking(S, Args) -> boolean()
%%     when S    :: eqc_statem:symbolic_state(),
%%          Args :: [term()].
%% vnode_send_blocking(_S, _Args) ->
%%   false.

%% @doc vnode_send_features - Collects a list of features of this call with these arguments.
%% -spec vnode_send_features(S, Args, Res) -> list(any())
%%     when S    :: eqc_statem:dynmic_state(),
%%          Args :: [term()],
%%          Res  :: term().
%% vnode_send_features(_S, _Args, _Res) ->
%%   [].

%% @doc vnode_send_adapt - How to adapt a call in this State
%% -spec vnode_send_adapt(S, Args) -> boolean()
%%     when S    :: eqc_statem:symbolic_state(),
%%          Args :: [term()].
%% vnode_send_adapt(_S, _Args) ->
%%   false.

%% @doc vnode_send_dynamicpre - Dynamic precondition for vnode_send
%% -spec vnode_send_dynamicpre(S, Args) -> boolean()
%%     when S    :: eqc_statem:symbolic_state(),
%%          Args :: [term()].
%% vnode_send_dynamicpre(_S, _Args) ->
%%   true.


%% --- ... more operations

%% -- Property ---------------------------------------------------------------
%% @doc <i>Optional callback</i>, Invariant, checked for each visited state
%%      during test execution.
%% -spec invariant(S :: eqc_statem:dynamic_state()) -> boolean().
%% invariant(_S) ->
%% true.

%% @doc weight/2 - Distribution of calls
-spec weight(S, Cmd) -> integer()
    when S   :: eqc_statem:symbolic_state(),
         Cmd :: atom().
weight(_S, vnode_send) -> 1;
weight(_S, _Cmd) -> 1.

%% @doc Default generated property
-spec prop_chunks() -> eqc:property().
prop_chunks() ->
  ?FORALL(Cmds, commands(?MODULE),
  begin
      {H, S, Res} = run_commands(?MODULE,Cmds),
      pretty_commands(?MODULE, Cmds, {H, S, Res},
                      measure(length, length(Cmds),
                              aggregate(command_names(Cmds),
                                        Res == ok)))
  end).

-endif.

-module(bigset_fold_acc).

-compile([export_all]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(fold_acc,
        {
         current_elem :: binary(),
         current_actor :: binary(),
         current_cnt :: pos_integer(),
         current_tsb :: <<_:1>>,
         elements = []
        }).

-define(ADD, <<0:1>>).
-define(REM, <<1:1>>).

new() ->
    #fold_acc{}.

add(Element, Actor, Cnt, TSB, Acc=#fold_acc{current_elem=Element,
                                            current_actor=Actor}) ->
    %% If this Element is the same as current and this actor is the
    %% same as the current the count is greater or the TSB is set, add
    %% current cnt, and tsb.
    Acc#fold_acc{current_cnt=Cnt, current_tsb=TSB};
add(Element, Actor, Cnt, TSB, Acc=#fold_acc{current_tsb=?ADD}) ->

    %% If this element or actor is different look at TSB. TSB is 0 add
    %% {element, {Actor, Cnt} to elements and set current actor,
    %% current cnt, current tsb
    Acc2= store_element(Acc),
    Acc2#fold_acc{current_cnt=Cnt, current_elem=Element,
                  current_actor=Actor, current_tsb=TSB};

add(Element, NewActor, Cnt, TSB, Acc=#fold_acc{current_tsb=?REM}) ->

    %% If this element or the actor is different look at TSB. TSB is
    %% 1, do not add to Elements.
    Acc#fold_acc{current_cnt=Cnt, current_elem=Element,
                 current_actor=NewActor, current_tsb=TSB};
add(Element, Actor, Cnt, TSB, Acc=#fold_acc{}) ->
    Acc#fold_acc{current_elem=Element,
                 current_actor=Actor,
                 current_cnt=Cnt,
                 current_tsb=TSB}.

finalise(#fold_acc{current_tsb=?REM, elements=Elements}) ->
    lists:reverse(Elements);
finalise(Acc=#fold_acc{current_tsb=?ADD}) ->
    #fold_acc{elements=Elements} = store_element(Acc),
    lists:reverse(Elements).

store_element(Acc) ->
    #fold_acc{current_actor=Actor,
              current_cnt=Cnt,
              current_elem=Elem,
              elements=Elements} = Acc,
    Elements2 = case Elements of
                    [{Elem, Dots} | Rest] ->
                        [{Elem, lists:umerge([{Actor, Cnt}], Dots)}
                         | Rest];
                    L ->
                        [{Elem, [{Actor, Cnt}]} | L]
                end,
    Acc#fold_acc{elements=Elements2}.

-ifdef(TEST).
add_test() ->
    Acc2 = add(<<"A">>, <<"b">>, 1, <<0:1>>, new()),
    ?assertEqual(#fold_acc{current_elem= <<"A">>,
                           current_actor= <<"b">>,
                           current_cnt= 1,
                           current_tsb= <<0:1>>}, Acc2),
    Acc3 = add(<<"A">>, <<"b">>, 4, <<0:1>>, Acc2),
    ?assertEqual(#fold_acc{current_elem= <<"A">>,
                           current_actor= <<"b">>,
                           current_cnt= 4,
                           current_tsb= <<0:1>>}, Acc3),
    Acc4 = add(<<"A">>, <<"c">>, 99, <<0:1>>, Acc3),
    ?assertEqual(#fold_acc{current_elem= <<"A">>,
                           current_actor= <<"c">>,
                           current_cnt= 99,
                           current_tsb= <<0:1>>,
                           elements=[{<<"A">>, [{<<"b">>, 4}]}]}, Acc4),
    Acc5 = add(<<"A">>, <<"c">>, 99, <<1:1>>, Acc4),
    ?assertEqual(#fold_acc{current_elem= <<"A">>,
                           current_actor= <<"c">>,
                           current_cnt= 99,
                           current_tsb= <<1:1>>,
                           elements=[{<<"A">>, [{<<"b">>, 4}]}]}, Acc5),
    Acc6 = add(<<"A">>, <<"c">>, 100, <<0:1>>, Acc5),
    ?assertEqual(#fold_acc{current_elem= <<"A">>,
                           current_actor= <<"c">>,
                           current_cnt= 100,
                           current_tsb= <<0:1>>,
                           elements=[{<<"A">>, [{<<"b">>, 4}]}]}, Acc6),
    Acc7 = add(<<"A">>, <<"c">>, 103, <<1:1>>, Acc6),
    ?assertEqual(#fold_acc{current_elem= <<"A">>,
                           current_actor= <<"c">>,
                           current_cnt= 103,
                           current_tsb= <<1:1>>,
                           elements=[{<<"A">>, [{<<"b">>, 4}]}]}, Acc7),
    Acc8 = add(<<"A">>, <<"d">>, 3, <<0:1>>, Acc7),
    ?assertEqual(#fold_acc{current_elem= <<"A">>,
                           current_actor= <<"d">>,
                           current_cnt= 3,
                           current_tsb= <<0:1>>,
                           elements=[{<<"A">>, [{<<"b">>, 4}]}]}, Acc8),
    Acc9 = add(<<"Z">>, <<"a">>, 12, <<0:1>>, Acc8),
    ?assertEqual(#fold_acc{current_elem= <<"Z">>,
                           current_actor= <<"a">>,
                           current_cnt= 12,
                           current_tsb= <<0:1>>,
                           elements=[{<<"A">>, [{<<"b">>, 4},
                                                {<<"d">>, 3}]}]}, Acc9),
    Acc10 = add(<<"ZZ">>, <<"b">>, 19, <<1:1>>, Acc9),
    ?assertEqual(#fold_acc{current_elem= <<"ZZ">>,
                           current_actor= <<"b">>,
                           current_cnt= 19,
                           current_tsb= <<1:1>>,
                           elements=[{<<"Z">>, [{<<"a">>, 12}]},
                                     {<<"A">>, [{<<"b">>, 4},
                                                {<<"d">>, 3}]}
                                    ]}, Acc10),
    ?assertEqual([{<<"A">>, [{<<"b">>, 4},
                             {<<"d">>, 3}]},
                  {<<"Z">>, [{<<"a">>, 12}]}],
                 finalise(Acc10)),
    Acc11 = add(<<"ZZ">>, <<"b">>, 19, <<0:1>>, Acc9),
    ?assertEqual([{<<"A">>, [{<<"b">>, 4},
                             {<<"d">>, 3}]},
                  {<<"Z">>, [{<<"a">>, 12}]},
                  {<<"ZZ">>, [{<<"b">>, 19}]}],
                 finalise(Acc11)).

-endif.

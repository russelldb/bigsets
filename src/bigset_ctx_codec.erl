%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2015, Russell Brown
%%% @doc
%%% A really basic dictionary encoder for the per-element contexts.
%%%
%%% In bigsets right now, we don't have causal consistency, which
%%% means we can't send a single version vector as a covering context
%%% for elements, and the bigst_clock with dot cloud is no use: how do
%%% you assign a dot to a remove? At the moment we chose to send the
%%% dots for an element with the element, call it a per-elememt
%%% context. Dots are {actor, counter} pairs. We expect few actors,
%%% but many elements. Rather than send some actor ID many, many
%%% times, instead, create a simple dictionary Actor::binary() ->
%%% ID::pos_integer(). Substitute the per-element dots {actor,
%%% counter} with {id, counter}. Maybe this is just faffing around the
%%% edges, and the real answer is add causal consistency so a single,
%%% gaples version vector covers all elements. Until then, now you
%%% know what this module does at least.
%%%
%%% @end Created : 29 Sep 2015 by Russell Brown
%%% <russelldb@basho.com>
%%% -------------------------------------------------------------------
-module(bigset_ctx_codec).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-compile([export_all]).

-export_type([encoder/0, decoder/0]).

%% Simple dictionary coding for per element context
-define(DICT, orddict).

-record(state, {
          dictionary=?DICT:new(),
          cntr=0
         }).

-type encoder() :: #state{}.
-type decoder() :: #state{}.

new_encoder() ->
    #state{}.

new_encoder(Clock) ->
    dictionary_from_clock(Clock).

%% @doc the dictionary for the dot encoding to be sent to the client
%% as a first message (as they will need to send it with removes!)
dictionary_from_clock(Clock) ->
    AllActors = bigset_clock:all_nodes(Clock),
    lists:foldl(fun(Actor, Acc) ->
                        #state{cntr=Cntr, dictionary=Dict} = Acc,
                        ActorID = Cntr+1,
                        Acc#state{cntr=ActorID,
                                  dictionary=?DICT:store(Actor, ActorID, Dict)}
                end,
                new_encoder(),
                AllActors).

new_decoder(Dictionary) when is_binary(Dictionary) ->
    Dict = binary_to_term(Dictionary),
    new_decoder(Dict);
new_decoder(Dictionary) ->
    #state{dictionary=Dictionary}.

encode_dots(Dots, State) ->
    lists:foldl(fun(Dot, {DotAcc, Dict}) ->
                        {BinDot, Dict2} = encode(Dot, Dict),
                        Len = byte_size(BinDot),
                        {<<DotAcc/binary, Len:32/integer, BinDot/binary>>, Dict2}
                end,
                {<<>>, State},
                Dots).

encode({Actor, Cnt}, State) ->
    #state{dictionary=Dict, cntr=Cntr} = State,
    {ID, NewState} =
        case ?DICT:find(Actor, Dict) of
            error ->
                AID = Cntr+1,
                {AID, State#state{dictionary=?DICT:store(Actor, AID, Dict),cntr=AID}};
            {ok, AID} ->
                {AID, State}
        end,
    {<<ID:32/integer, (binary:encode_unsigned(Cnt))/binary>>, NewState}.

%% @doc binary encode the dictionary context
dict_ctx(State) ->
    #state{dictionary=Dict} = State,
    term_to_binary(Dict).

decode_dots(BinDots, DictCtx) when is_binary(DictCtx) ->
    Decoder = new_decoder(DictCtx),
    decode_dots(BinDots, Decoder, []);
decode_dots(BinDots, Decoder=#state{}) ->
    decode_dots(BinDots, Decoder, []).

decode_dots(<<>>, _Dict, Acc) ->
    lists:sort(Acc);
decode_dots(<<Len:32/integer, Dot:Len/binary, Rest/binary>>=Bin, Dict, Acc) ->
    lager:debug("Len ~p~nDot ~p~nRest ~p~nBin ~p~n", [Len, Dot, Rest, Bin]),
    TupleDot = decode(Dot, Dict),
    decode_dots(Rest, Dict, [TupleDot | Acc]).

decode(<<ID:32/integer, Cnt/binary>>, State) ->
    #state{dictionary=Dict} = State,
    Actor = actor_from_id(ID, Dict),
    Cntr = binary:decode_unsigned(Cnt),
    {Actor, Cntr}.

%% oops, assumes orddict, lol!
actor_from_id(ID, Dict) when is_list(Dict) ->
    {Actor, ID} = lists:keyfind(ID, 2, Dict),
    Actor.

-ifdef(TEST).
codec_test() ->
    Actors = [crypto:rand_bytes(64) || _N <- lists:seq(1, 10)],
    Dots = [{Actor, crypto:rand_uniform(10, 10000)} || Actor <- Actors],
    DotsBin = term_to_binary(Dots),
    {EncodedDots, Dict} = encode_dots(Dots, new_encoder()),
    DictCtx = dict_ctx(Dict),
    ?assert(byte_size(DotsBin) > byte_size(EncodedDots)),
    DecodedDots = decode_dots(EncodedDots, DictCtx),
    ?assertEqual(lists:sort(Dots), DecodedDots).


dict_from_clock_test() ->
    Clock = bigset_clock:fresh(),
    {_, Clock2} = bigset_clock:increment(<<"a">>, Clock),
    Dot = {<<"c">>, 99},
    Clock3 = bigset_clock:add_dot(Dot, Clock2),
    {_, Clock4} = bigset_clock:increment(<<"d">>, Clock3),
    Dict = dictionary_from_clock(Clock4),
    ?assertEqual([{<<"a">>, 1}, {<<"c">>, 2}, {<<"d">>, 3}], Dict#state.dictionary).

-endif.

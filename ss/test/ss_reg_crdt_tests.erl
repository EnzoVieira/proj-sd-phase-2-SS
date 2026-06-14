%%%-------------------------------------------------------------------
%%% ss_reg_crdt_tests — Testes EUnit do CRDT de registo (grow-only).
%%% Prova que merge/2 é comutativo, associativo e idempotente, que a união
%%% nunca perde users, o tie-break determinístico em conflito, e o lookup.
%%%
%%% Correr: erl -pa ebin -noshell -eval "eunit:test(ss_reg_crdt_tests,[verbose])" -s init stop
%%%-------------------------------------------------------------------
-module(ss_reg_crdt_tests).
-include_lib("eunit/include/eunit.hrl").

%% --- Registos de exemplo (users/senhas em binary) ---
r1() -> #{<<"alice">> => <<"a1">>, <<"bob">> => <<"b1">>}.
r2() -> #{<<"carol">> => <<"c1">>}.
r3() -> #{<<"dave">> => <<"d1">>}.

%%====================================================================
%% Propriedades CRDT
%%====================================================================

merge_comutativo_test() ->
    ?assertEqual(ss_reg_crdt:merge(r1(), r2()),
                 ss_reg_crdt:merge(r2(), r1())).

merge_idempotente_test() ->
    ?assertEqual(r1(), ss_reg_crdt:merge(r1(), r1())).

merge_associativo_test() ->
    Left  = ss_reg_crdt:merge(ss_reg_crdt:merge(r1(), r2()), r3()),
    Right = ss_reg_crdt:merge(r1(), ss_reg_crdt:merge(r2(), r3())),
    ?assertEqual(Left, Right).

%% A união nunca perde users (grow-only).
merge_uniao_nao_perde_test() ->
    M = ss_reg_crdt:merge(r1(), r2()),
    ?assertEqual({ok, <<"a1">>}, ss_reg_crdt:lookup(M, <<"alice">>)),
    ?assertEqual({ok, <<"b1">>}, ss_reg_crdt:lookup(M, <<"bob">>)),
    ?assertEqual({ok, <<"c1">>}, ss_reg_crdt:lookup(M, <<"carol">>)).

%% Conflito (mesmo user, senhas diferentes) -> tie-break determinístico min/2.
merge_conflito_min_test() ->
    A = #{<<"alice">> => <<"aaa">>},
    B = #{<<"alice">> => <<"zzz">>},
    ?assertEqual(#{<<"alice">> => <<"aaa">>}, ss_reg_crdt:merge(A, B)),
    ?assertEqual(#{<<"alice">> => <<"aaa">>}, ss_reg_crdt:merge(B, A)).

%%====================================================================
%% add / lookup
%%====================================================================

add_e_lookup_test() ->
    R0 = ss_reg_crdt:new(),
    R1 = ss_reg_crdt:add(R0, <<"carol">>, <<"c1">>),
    ?assertEqual({ok, <<"c1">>}, ss_reg_crdt:lookup(R1, <<"carol">>)),
    ?assertEqual(error, ss_reg_crdt:lookup(R1, <<"ghost">>)).

%% add em colisão fica com min (consistente com merge).
add_colisao_min_test() ->
    R = ss_reg_crdt:add(#{<<"alice">> => <<"bbb">>}, <<"alice">>, <<"aaa">>),
    ?assertEqual({ok, <<"aaa">>}, ss_reg_crdt:lookup(R, <<"alice">>)).

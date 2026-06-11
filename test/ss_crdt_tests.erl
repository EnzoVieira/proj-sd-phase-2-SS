%%%-------------------------------------------------------------------
%%% ss_crdt_tests — Testes EUnit do CRDT (Fase 4A)
%%%
%%% Prova que merge/2 é comutativo, associativo e idempotente (as 3
%%% propriedades que garantem a convergência das réplicas), e testa as queries.
%%%
%%% Correr: erl -pa ebin -noshell -eval "eunit:test(ss_crdt_tests,[verbose])" -s init stop
%%%-------------------------------------------------------------------
-module(ss_crdt_tests).
-include_lib("eunit/include/eunit.hrl").

%% --- Estados de zona de exemplo ---
zoneA_v1() -> #{version => 1, online => #{<<"car-1">> => <<"car">>}, active => 1}.
zoneA_v2() -> #{version => 2, online => #{<<"car-1">> => <<"car">>,
                                          <<"car-2">> => <<"car">>}, active => 2}.
zoneB_v1() -> #{version => 1, online => #{<<"drone-1">> => <<"drone">>}, active => 0}.

g1() -> #{zonea => zoneA_v2(), zoneb => zoneB_v1()}.
g2() -> #{zonea => zoneA_v1()}.
g3() -> #{zonec => zoneB_v1()}.

%%====================================================================
%% Propriedades CRDT
%%====================================================================

merge_comutativo_test() ->
    ?assertEqual(ss_crdt:merge(g1(), g2()),
                 ss_crdt:merge(g2(), g1())).

merge_idempotente_test() ->
    ?assertEqual(g1(), ss_crdt:merge(g1(), g1())).

merge_associativo_test() ->
    Left  = ss_crdt:merge(ss_crdt:merge(g1(), g2()), g3()),
    Right = ss_crdt:merge(g1(), ss_crdt:merge(g2(), g3())),
    ?assertEqual(Left, Right).

%% O merge fica sempre com a versão MAIS ALTA de cada zona.
merge_fica_com_versao_alta_test() ->
    Merged = ss_crdt:merge(g2(), g1()),   %% g2 tem zonaA v1, g1 tem zonaA v2
    ?assertEqual(zoneA_v2(), maps:get(zonea, Merged)).

%%====================================================================
%% Queries globais
%%====================================================================

queries_test() ->
    %% Estado global: zonaA(v2: car-1, car-2) + zonaB(drone-1)
    G = ss_crdt:merge(g1(), g2()),
    ?assertEqual(3, ss_crdt:count_online(G)),
    ?assertEqual(2, ss_crdt:count_online_by_type(G, <<"car">>)),
    ?assertEqual(1, ss_crdt:count_online_by_type(G, <<"drone">>)),
    ?assertEqual(0, ss_crdt:count_online_by_type(G, <<"truck">>)),
    ?assertEqual(true,  ss_crdt:is_online(G, <<"car-2">>)),
    ?assertEqual(false, ss_crdt:is_online(G, <<"ghost">>)),
    ?assertEqual(2, ss_crdt:count_active(G)),                 %% 2 (zonaA) + 0 (zonaB)
    ?assertEqual(2, ss_crdt:zone_online_count(G, zonea)),
    ?assertEqual(1, ss_crdt:zone_online_count(G, zoneb)).

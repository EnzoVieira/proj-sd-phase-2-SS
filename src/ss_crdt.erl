%%%-------------------------------------------------------------------
%%% ss_crdt — Estado global replicável (CRDT state-based / CvRDT). Puro.
%%%   Global     = #{ Zona => Zona_State }
%%%   Zona_State = #{ version => N, online => #{DeviceId => Tipo}, active => K }
%%% Invariante: um único escritor por zona, logo as versões de uma zona são
%%% totalmente ordenadas e merge/2 "fica com a versão mais alta" (LWW por zona).
%%% merge/2 é comutativo, associativo e idempotente (ver ss_crdt_tests).
%%%-------------------------------------------------------------------
-module(ss_crdt).

-export([new/0, new_zone/0,
         set_zone/3, merge/2,
         count_online/1, count_online_by_type/2, is_online/2,
         count_active/1, zone_online_count/2]).

%%====================================================================
%% Construtores
%%====================================================================

new() -> #{}.

new_zone() ->
    #{version => 0, online => #{}, active => 0}.

%% Substitui o estado de uma zona (usado pelo nó dono da zona).
set_zone(Global, Zone, ZoneState) ->
    maps:put(Zone, ZoneState, Global).

%%====================================================================
%% merge
%%====================================================================

%% Funde dois estados globais: por cada zona, fica com a versão mais alta.
merge(G1, G2) ->
    maps:fold(
        fun(Zone, ZS2, Acc) ->
            case maps:find(Zone, Acc) of
                {ok, ZS1} -> maps:put(Zone, merge_zone(ZS1, ZS2), Acc);
                error     -> maps:put(Zone, ZS2, Acc)
            end
        end,
        G1, G2).

merge_zone(ZS1, ZS2) ->
    case maps:get(version, ZS1) >= maps:get(version, ZS2) of
        true  -> ZS1;
        false -> ZS2
    end.

%%====================================================================
%% Queries globais (percorrem todas as zonas)
%%====================================================================

count_online(Global) ->
    fold_zones(fun(ZS, Acc) -> Acc + maps:size(online(ZS)) end, 0, Global).

count_online_by_type(Global, Type) ->
    fold_zones(
        fun(ZS, Acc) ->
            Acc + count_values(Type, online(ZS))
        end, 0, Global).

is_online(Global, Device) ->
    fold_zones_until(
        fun(ZS) -> maps:is_key(Device, online(ZS)) end, Global).

count_active(Global) ->
    fold_zones(fun(ZS, Acc) -> Acc + maps:get(active, ZS, 0) end, 0, Global).

%% Nº de online de uma zona específica (para a percentagem zona vs total).
zone_online_count(Global, Zone) ->
    case maps:find(Zone, Global) of
        {ok, ZS} -> maps:size(online(ZS));
        error    -> 0
    end.

%%====================================================================
%% Auxiliares
%%====================================================================

online(ZS) -> maps:get(online, ZS, #{}).

fold_zones(Fun, Acc0, Global) ->
    maps:fold(fun(_Zone, ZS, Acc) -> Fun(ZS, Acc) end, Acc0, Global).

%% True assim que alguma zona satisfaz o predicado.
fold_zones_until(Pred, Global) ->
    lists:any(fun(ZS) -> Pred(ZS) end, maps:values(Global)).

count_values(Value, Map) ->
    maps:fold(fun(_K, V, Acc) when V =:= Value -> Acc + 1;
                 (_K, _V, Acc) -> Acc
              end, 0, Map).

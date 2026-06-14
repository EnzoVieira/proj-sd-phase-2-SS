%%%-------------------------------------------------------------------
%%% ss_reg_crdt — Registo de consumidores replicável (CRDT grow-only). Puro.
%%%   Reg = #{ User(binary) => Password(binary) }
%%% Grow-only G-map: só se acrescentam users (nunca se removem). merge/2 faz a
%%% união; em conflito (mesmo user, senhas diferentes) escolhe min(P1,P2), o que
%%% torna merge/2 comutativo, associativo e idempotente (ver ss_reg_crdt_tests).
%%% Chaves/valores são binary (o gossip usa binary_to_term/2 [safe]).
%%%-------------------------------------------------------------------
-module(ss_reg_crdt).

-export([new/0, add/3, merge/2, lookup/2]).

new() -> #{}.

%% Acrescenta um registo. Em colisão local fica com min (consistente com merge).
add(Reg, User, Password) ->
    case maps:find(User, Reg) of
        {ok, Existing} -> maps:put(User, min(Existing, Password), Reg);
        error          -> maps:put(User, Password, Reg)
    end.

%% Funde dois registos: união dos users; em conflito de senha fica com a menor.
merge(R1, R2) ->
    maps:fold(
        fun(User, P2, Acc) ->
            case maps:find(User, Acc) of
                {ok, P1} -> maps:put(User, min(P1, P2), Acc);
                error    -> maps:put(User, P2, Acc)
            end
        end,
        R1, R2).

%% lookup(Reg, User) -> {ok, Password} | error
lookup(Reg, User) ->
    maps:find(User, Reg).

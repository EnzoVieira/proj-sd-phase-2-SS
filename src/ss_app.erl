%%%-------------------------------------------------------------------
%%% ss_app — Ponto de arranque da aplicação OTP (Fase 1C)
%%%
%%% Implementa o behaviour 'application'. Quando fazes application:start(ss),
%%% o OTP chama ss_app:start/2, que arranca o supervisor de topo.
%%%-------------------------------------------------------------------
-module(ss_app).
-behaviour(application).
-export([start/2, stop/1]).

%% Chamado no arranque da aplicação. Devolve {ok, Pid} do supervisor de topo.
start(_Type, _Args) ->
    ss_sup:start_link().

%% Chamado ao parar a aplicação. Não temos nada para limpar.
stop(_State) ->
    ok.

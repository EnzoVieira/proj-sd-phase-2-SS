%%%-------------------------------------------------------------------
%%% ss_sup — Supervisor de topo (Fase 1C)
%%%
%%% Vigia os processos principais do SS e reinicia-os se rebentarem.
%%% Filhos (por esta ordem):
%%%   1. ss_registry  — registo de credenciais (gen_server)
%%%   2. ss_tcp       — acceptor TCP (aceita ligações e gera ss_conn)
%%% A ordem importa: o registo arranca antes do acceptor, porque as ligações
%%% vão precisar dele para autenticar.
%%%-------------------------------------------------------------------
-module(ss_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% init/1 devolve {ok, {FlagsDoSupervisor, ListaDeFilhos}}.
init([]) ->
    %% Lê a porta da configuração da aplicação (env no ss.app).
    {ok, Port} = application:get_env(ss, port),

    %% Estratégia de supervisão:
    %%   one_for_one -> se um filho morre, só esse é reiniciado.
    %%   intensity/period -> no máx. 5 reinícios em 10s, senão o supervisor
    %%   desiste (evita ciclos de reinício infinitos).
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},

    %% Cada filho é descrito por um mapa: id único + {Modulo, Funcao, Args} de arranque.
    Children = [
        #{id => ss_registry,
          start => {ss_registry, start_link, []}},
        #{id => ss_pubsub,
          start => {ss_pubsub, start_link, []}},
        #{id => ss_state,
          start => {ss_state, start_link, []}},
        #{id => ss_gossip,
          start => {ss_gossip, start_link, []}},
        #{id => ss_cluster,
          start => {ss_cluster, start_link, []}},
        #{id => ss_tcp,
          start => {ss_tcp, start_link, [Port]}}
    ],

    {ok, {SupFlags, Children}}.

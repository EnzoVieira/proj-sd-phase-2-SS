%%%-------------------------------------------------------------------
%%% ss_sup — Supervisor de topo do SS.
%%%-------------------------------------------------------------------
-module(ss_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, Port} = application:get_env(ss, port),
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    %% Ordem importa: registry/state/gossip arrancam antes de quem os usa.
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

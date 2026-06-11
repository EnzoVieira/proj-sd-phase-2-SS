%%%-------------------------------------------------------------------
%%% ss_cluster — Réplica do estado global + gossip entre nós (Fase 4B)
%%%
%%% gen_server que:
%%%   - mantém a cópia local do estado global (ss_crdt)
%%%   - num tick periódico: lê o estado local (ss_state), marca-o como a "sua
%%%     zona" com versão crescente, e envia o estado global a todos os peers
%%%   - ao receber estado de um peer: faz merge
%%%   - responde às queries GLOBAIS (somando todas as zonas)
%%%
%%% Configuração (application env da app 'ss'):
%%%   zone            : id da zona deste nó (átomo). Default: node()
%%%   peers           : lista de nós Erlang vizinhos. Default: []
%%%   gossip_interval : período do tick em ms. Default: 500
%%%
%%% Transporte: distribuição Erlang (gen_server:cast para {ss_cluster, Node}).
%%% Na Fase 5 será substituído por 0MQ — só a função broadcast/2 muda.
%%%
%%% CONCEITOS NOVOS: timer periódico (erlang:send_after), comunicação entre
%%% nós Erlang, anti-entropia (reenviar o estado todo regularmente -> tolera
%%% perda de mensagens e nós que entram/saem).
%%%-------------------------------------------------------------------
-module(ss_cluster).
-behaviour(gen_server).

-export([start_link/0,
         count_online/0, count_online_by_type/1, is_online/1, count_active/0,
         zone_online_count/0, global_online_count/0, dump/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%%====================================================================
%% API de queries (usada pelo ss_conn)
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

count_online()              -> gen_server:call(?MODULE, count_online).
count_online_by_type(Type)  -> gen_server:call(?MODULE, {count_online_by_type, Type}).
is_online(Device)           -> gen_server:call(?MODULE, {is_online, Device}).
count_active()              -> gen_server:call(?MODULE, count_active).

%% Para as ocorrências de percentagem (Fase 4C):
zone_online_count()         -> gen_server:call(?MODULE, zone_online_count).
global_online_count()       -> gen_server:call(?MODULE, count_online).

%% Útil para depurar: ver o estado global completo.
dump()                      -> gen_server:call(?MODULE, dump).

%%====================================================================
%% Callbacks
%%====================================================================

init([]) ->
    Zone     = case application:get_env(ss, zone, undefined) of
                   undefined -> node();   %% sem zona configurada -> usa o nome do nó
                   Z -> Z
               end,
    Peers    = application:get_env(ss, peers, []),
    Interval = application:get_env(ss, gossip_interval, 500),
    io:format("[ss_cluster] zona=~p peers=~p~n", [Zone, Peers]),
    State = #{zone => Zone, peers => Peers, interval => Interval,
              global => ss_crdt:new(), version => 0, prev_pct => 0},
    schedule_tick(Interval),
    {ok, State}.

%% --- Queries: respondem do estado global, com a zona local fresca ---

handle_call(count_online, _From, State) ->
    {reply, ss_crdt:count_online(fresh_global(State)), State};
handle_call({count_online_by_type, Type}, _From, State) ->
    {reply, ss_crdt:count_online_by_type(fresh_global(State), Type), State};
handle_call({is_online, Device}, _From, State) ->
    {reply, ss_crdt:is_online(fresh_global(State), Device), State};
handle_call(count_active, _From, State) ->
    {reply, ss_crdt:count_active(fresh_global(State)), State};
handle_call(zone_online_count, _From, State) ->
    Zone = maps:get(zone, State),
    {reply, ss_crdt:zone_online_count(fresh_global(State), Zone), State};
handle_call(dump, _From, State) ->
    {reply, fresh_global(State), State}.

%% --- Gossip recebido de um peer: merge ---
%% O total global pode ter mudado -> reavaliar a percentagem desta zona.
handle_cast({merge, Remote}, State) ->
    Global2 = ss_crdt:merge(maps:get(global, State), Remote),
    {noreply, maybe_publish_percentage(State#{global := Global2})};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% --- Tick periódico: atualiza a própria zona e faz gossip ---
handle_info(tick, State) ->
    Zone    = maps:get(zone, State),
    Peers   = maps:get(peers, State),
    Version = maps:get(version, State) + 1,

    %% Constrói o estado da NOSSA zona (versão nova) e mete-o no global.
    LocalZone = local_zone_state(Version),
    Global2   = ss_crdt:set_zone(maps:get(global, State), Zone, LocalZone),

    %% Anti-entropia: enviar o estado global inteiro a todos os peers.
    broadcast(Peers, {merge, Global2}),

    schedule_tick(maps:get(interval, State)),
    %% a contagem da zona pode ter mudado -> reavaliar a percentagem
    {noreply, maybe_publish_percentage(State#{global := Global2, version := Version})};
handle_info(_Msg, State) ->
    {noreply, State}.

%%====================================================================
%% Auxiliares
%%====================================================================

%% Estado global com a zona local SEMPRE fresca (lida agora do ss_state).
%% Usado nas queries para não dependerem do último tick.
fresh_global(State) ->
    Zone  = maps:get(zone, State),
    Local = local_zone_state(maps:get(version, State)),
    ss_crdt:set_zone(maps:get(global, State), Zone, Local).

%% Lê o estado local atual (online + ativos) e embrulha-o num estado de zona.
local_zone_state(Version) ->
    #{version => Version,
      online  => ss_state:online_map(),
      active  => ss_state:count_active()}.

%% Calcula a percentagem (online da zona / total global) e publica ocorrências
%% de subida/descida ao cruzar os limiares X em {10,20,...,90}. Devolve o estado
%% com o prev_pct atualizado.
maybe_publish_percentage(State) ->
    G     = fresh_global(State),
    Zone  = maps:get(zone, State),
    ZoneN = ss_crdt:zone_online_count(G, Zone),
    Total = ss_crdt:count_online(G),
    NewPct = case Total of
                 0 -> 0;
                 _ -> (ZoneN * 100) div Total
             end,
    Prev = maps:get(prev_pct, State),
    publish_crossings(NewPct, Prev),
    State#{prev_pct := NewPct}.

publish_crossings(New, Prev) ->
    lists:foreach(
        fun(X) ->
            PrevAbove = Prev > X,
            NewAbove  = New > X,
            if
                (not PrevAbove) andalso NewAbove ->
                    ss_pubsub:publish({percentage, up, X, New});
                PrevAbove andalso (not NewAbove) ->
                    ss_pubsub:publish({percentage, down, X, New});
                true ->
                    ok
            end
        end,
        lists:seq(10, 90, 10)).

broadcast(Peers, Msg) ->
    %% cast para um nó inacessível falha em silêncio — bom para tolerância a faltas.
    lists:foreach(fun(Peer) -> gen_server:cast({?MODULE, Peer}, Msg) end, Peers).

schedule_tick(Interval) ->
    erlang:send_after(Interval, self(), tick).

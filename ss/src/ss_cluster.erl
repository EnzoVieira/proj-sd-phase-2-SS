%%%-------------------------------------------------------------------
%%% ss_cluster — Réplica do estado global (ss_crdt) e queries globais.
%%% Num tick periódico marca a sua zona (versão crescente) e publica o estado
%%% via ss_gossip (0MQ); ao receber estado de um peer, faz merge.
%%% Config (env 'ss'): zone (binary; default = node()), gossip_interval (ms).
%%%-------------------------------------------------------------------
-module(ss_cluster).
-behaviour(gen_server).

-export([start_link/0,
         count_online/0, count_online_by_type/1, is_online/1, count_active/0,
         zone_online_count/0, global_online_count/0, dump/0,
         register_consumer/2, authenticate_consumer/2]).
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

%% Para as ocorrências de percentagem.
zone_online_count()         -> gen_server:call(?MODULE, zone_online_count).
global_online_count()       -> gen_server:call(?MODULE, count_online).

%% Depuração: ver o estado global completo.
dump()                      -> gen_server:call(?MODULE, dump).

%% Registo de consumidores (CRDT grow-only replicado pelo gossip).
register_consumer(User, Password) ->
    gen_server:call(?MODULE, {register_consumer, User, Password}).
authenticate_consumer(User, Password) ->
    gen_server:call(?MODULE, {authenticate_consumer, User, Password}).

%%====================================================================
%% Callbacks
%%====================================================================

init([]) ->
    %% Zona em binary (viaja com term_to_binary; evita criar átomos com [safe]).
    Zone = case application:get_env(ss, zone, undefined) of
               undefined           -> atom_to_binary(node(), utf8);
               Z when is_atom(Z)   -> atom_to_binary(Z, utf8);
               Z when is_binary(Z) -> Z
           end,
    Interval = application:get_env(ss, gossip_interval, 500),
    io:format("[ss_cluster] zona=~p~n", [Zone]),
    State = #{zone => Zone, interval => Interval,
              global => ss_crdt:new(), reg => ss_reg_crdt:new(),
              version => 0, prev_pct => 0},
    schedule_tick(Interval),
    {ok, State}.

%% Queries: respondem do estado global, com a zona local sempre fresca.
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
    {reply, fresh_global(State), State};

%% Registo idempotente: mesma senha -> ok; senha diferente -> já registado.
handle_call({register_consumer, User, Password}, _From, State) ->
    Reg = maps:get(reg, State),
    case ss_reg_crdt:lookup(Reg, User) of
        {ok, Password} -> {reply, ok, State};
        {ok, _Other}   -> {reply, {error, already_registered}, State};
        error          -> {reply, ok, State#{reg := ss_reg_crdt:add(Reg, User, Password)}}
    end;
handle_call({authenticate_consumer, User, Password}, _From, State) ->
    Reply = case ss_reg_crdt:lookup(maps:get(reg, State), User) of
                {ok, Password} -> true;
                _              -> false
            end,
    {reply, Reply, State}.

%% Gossip recebido: merge dos dois CRDTs (online/active + registo).
%% O total global pode mudar -> reavaliar percentagem.
handle_cast({merge, {Remote, RemoteReg}}, State) ->
    Global2 = ss_crdt:merge(maps:get(global, State), Remote),
    Reg2    = ss_reg_crdt:merge(maps:get(reg, State), RemoteReg),
    {noreply, maybe_publish_percentage(State#{global := Global2, reg := Reg2})};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% Tick: atualiza a própria zona e publica o estado (anti-entropia).
handle_info(tick, State) ->
    Zone    = maps:get(zone, State),
    Version = maps:get(version, State) + 1,
    LocalZone = local_zone_state(Version),
    Global2   = ss_crdt:set_zone(maps:get(global, State), Zone, LocalZone),
    ss_gossip:publish({Global2, maps:get(reg, State)}),
    schedule_tick(maps:get(interval, State)),
    {noreply, maybe_publish_percentage(State#{global := Global2, version := Version})};
handle_info(_Msg, State) ->
    {noreply, State}.

%%====================================================================
%% Auxiliares
%%====================================================================

%% Estado global com a zona local fresca (queries não dependem do último tick).
fresh_global(State) ->
    Zone  = maps:get(zone, State),
    Local = local_zone_state(maps:get(version, State)),
    ss_crdt:set_zone(maps:get(global, State), Zone, Local).

local_zone_state(Version) ->
    #{version => Version,
      online  => ss_state:online_map(),
      active  => ss_state:count_active()}.

%% Percentagem (online da zona / total global): publica subidas/descidas ao
%% cruzar os limiares {10,20,...,90}. Devolve o estado com prev_pct atualizado.
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

schedule_tick(Interval) ->
    erlang:send_after(Interval, self(), tick).

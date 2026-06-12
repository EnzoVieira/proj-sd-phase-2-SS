%%%-------------------------------------------------------------------
%%% ss_gossip — Transporte de gossip via 0MQ/chumak (Fase 5)
%%%
%%% Substitui a distribuição Erlang pelo ZeroMQ no caminho SS<->SS. Cada nó:
%%%   - tem um socket PUB que faz bind numa porta (gossip_port) e por onde
%%%     PUBLICA o seu estado global;
%%%   - tem um socket SUB que se LIGA aos PUB de todos os vizinhos
%%%     (gossip_peers) e subscreve tudo;
%%%   - corre um processo recetor que faz chumak:recv no SUB e reencaminha
%%%     cada estado recebido para o ss_cluster (gen_server:cast {merge, ...}).
%%%
%%% O estado viaja serializado com term_to_binary/binary_to_term (ambos os
%%% lados são Erlang). Usamos [safe] na desserialização; por isso os ids de
%%% zona são BINARIES (não átomos), para não criar átomos a partir da rede.
%%%
%%% Padrão ZeroMQ PUB/SUB é best-effort (pode perder mensagens) — perfeito
%%% para o nosso CRDT state-based + anti-entropia (o próximo tick recupera).
%%%
%%% Config (application env da app 'ss'):
%%%   gossip_port  : porta onde o PUB deste nó faz bind. Default: 7000
%%%   gossip_peers : [{Host, Port}] dos PUB dos vizinhos. Default: []
%%%-------------------------------------------------------------------
-module(ss_gossip).
-behaviour(gen_server).

-export([start_link/0, publish/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Publica o estado global a todos os vizinhos (via PUB).
publish(Global) ->
    gen_server:cast(?MODULE, {publish, Global}).

%%====================================================================
%% Callbacks
%%====================================================================

init([]) ->
    Port  = application:get_env(ss, gossip_port, 7000),
    Peers = application:get_env(ss, gossip_peers, []),

    %% Arrancar a aplicação chumak (ZeroMQ).
    {ok, _} = application:ensure_all_started(chumak),

    %% PUB: publica o nosso estado.
    {ok, Pub} = chumak:socket(pub),
    {ok, _}   = chumak:bind(Pub, tcp, "0.0.0.0", Port),

    %% SUB: recebe o estado dos vizinhos.
    {ok, Sub} = chumak:socket(sub),
    ok = chumak:subscribe(Sub, <<>>),   %% <<>> = subscrever tudo
    lists:foreach(
        fun({Host, PeerPort}) ->
            %% connect é assíncrono: se o peer ainda não estiver no ar, retenta.
            {ok, _} = chumak:connect(Sub, tcp, Host, PeerPort)
        end, Peers),

    %% Processo dedicado a receber do SUB e reencaminhar para o ss_cluster.
    spawn_link(fun() -> recv_loop(Sub) end),

    io:format("[ss_gossip] PUB na porta ~p, peers=~p~n", [Port, Peers]),
    {ok, #{pub => Pub, sub => Sub}}.

handle_cast({publish, Global}, State) ->
    %% Serializa e envia. Em PUB, se ninguém estiver a ouvir, é descartado.
    catch chumak:send(maps:get(pub, State), term_to_binary(Global)),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

%%====================================================================
%% Recetor
%%====================================================================

recv_loop(Sub) ->
    case chumak:recv(Sub) of
        {ok, Bin} ->
            %% [safe]: não cria átomos novos a partir de dados da rede.
            Global = binary_to_term(Bin, [safe]),
            gen_server:cast(ss_cluster, {merge, Global}),
            recv_loop(Sub);
        {error, Reason} ->
            io:format("[ss_gossip] recv erro: ~p~n", [Reason]),
            timer:sleep(100),
            recv_loop(Sub)
    end.

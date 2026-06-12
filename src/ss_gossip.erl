%%%-------------------------------------------------------------------
%%% ss_gossip — Transporte de gossip SS<->SS via 0MQ/chumak.
%%% Cada nó: socket PUB faz bind em gossip_port e publica o estado global;
%%% socket SUB liga aos PUB dos gossip_peers e um recetor reencaminha cada
%%% estado para o ss_cluster (merge). PUB/SUB é best-effort, o que basta ao
%%% CRDT state-based + anti-entropia (o próximo tick recupera).
%%% Config (env 'ss'): gossip_port (int), gossip_peers ([{Host, Port}]).
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

publish(Global) ->
    gen_server:cast(?MODULE, {publish, Global}).

%%====================================================================
%% Callbacks
%%====================================================================

init([]) ->
    Port  = application:get_env(ss, gossip_port, 7000),
    Peers = application:get_env(ss, gossip_peers, []),

    {ok, _} = application:ensure_all_started(chumak),

    {ok, Pub} = chumak:socket(pub),
    {ok, _}   = chumak:bind(Pub, tcp, "0.0.0.0", Port),

    {ok, Sub} = chumak:socket(sub),
    ok = chumak:subscribe(Sub, <<>>),   %% <<>> = subscrever tudo
    lists:foreach(
        fun({Host, PeerPort}) ->
            %% connect é assíncrono: se o peer não estiver no ar, retenta.
            {ok, _} = chumak:connect(Sub, tcp, Host, PeerPort)
        end, Peers),

    spawn_link(fun() -> recv_loop(Sub) end),

    io:format("[ss_gossip] PUB na porta ~p, peers=~p~n", [Port, Peers]),
    {ok, #{pub => Pub, sub => Sub}}.

handle_cast({publish, Global}, State) ->
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
            %% [safe]: não cria átomos a partir de dados da rede (zonas em binary).
            Global = binary_to_term(Bin, [safe]),
            gen_server:cast(ss_cluster, {merge, Global}),
            recv_loop(Sub);
        {error, Reason} ->
            io:format("[ss_gossip] recv erro: ~p~n", [Reason]),
            timer:sleep(100),
            recv_loop(Sub)
    end.

%%%-------------------------------------------------------------------
%%% ss_dist_demo — Demonstração distribuída (Fases 4 e 5)
%%%
%%% Arranca 2 nós SS (zonas diferentes) na mesma máquina com o módulo slave.
%%% A REPLICAÇÃO entre nós é feita por 0MQ/chumak (gossip PUB/SUB) — a
%%% distribuição Erlang aqui serve só para o harness arrancar/configurar os nós.
%%%
%%%   run/0     : estado global replicado + tolerância a faltas
%%%   run_pct/0 : ocorrências de percentagem (zona vs total global)
%%%
%%% Correr (a partir da raiz do projeto):
%%%   erl -sname master -setcookie sscookie -pa ebin -pa deps/chumak/ebin \
%%%       -noshell -eval 'ss_dist_demo:run()' -s init stop
%%%-------------------------------------------------------------------
-module(ss_dist_demo).
-export([run/0, run_pct/0]).

%% Arranca os dois nós SS (zonas A e B), com gossip 0MQ entre eles.
start_nodes() ->
    EbinAbs   = filename:absname("ebin"),
    ChumakAbs = filename:absname("deps/chumak/ebin"),
    {ok, Host} = inet:gethostname(),
    HostA = list_to_atom(Host),
    Args = "-setcookie sscookie -pa " ++ EbinAbs ++ " -pa " ++ ChumakAbs,
    {ok, NA} = slave:start(HostA, ssa, Args),
    {ok, NB} = slave:start(HostA, ssb, Args),
    %% Zona A: cliente 9001, gossip PUB 7001, liga ao PUB de B (7002)
    setup(NA, <<"zonea">>, 9001, 7001, [{"127.0.0.1", 7002}]),
    %% Zona B: cliente 9002, gossip PUB 7002, liga ao PUB de A (7001)
    setup(NB, <<"zoneb">>, 9002, 7002, [{"127.0.0.1", 7001}]),
    timer:sleep(800),   %% dar tempo ao PUB/SUB do 0MQ para ligar
    {NA, NB}.

run() ->
    {NA, NB} = start_nodes(),
    io:format("~nnós arrancados: ~p e ~p (gossip por 0MQ)~n", [NA, NB]),

    %% Produtores em zonas diferentes
    PA = ss_client:connect(9001), ss_client:auth_producer(PA, <<"car-001">>, <<"pass1">>),
    PB = ss_client:connect(9002), ss_client:auth_producer(PB, <<"drone-001">>, <<"pass3">>),

    timer:sleep(2000),   %% esperar o gossip convergir

    CA = ss_client:connect(9001), ss_client:auth_consumer(CA, <<"alice">>, <<"alice123">>),
    io:format("~n=== Visto a partir do NÓ A (porta 9001) ===~n"),
    io:format("online_count GLOBAL (=2)    : ~p~n", [ss_client:online_count(CA)]),
    io:format("por tipo car (=1)           : ~p~n", [ss_client:online_count_by_type(CA, <<"car">>)]),
    io:format("por tipo drone (=1)         : ~p~n", [ss_client:online_count_by_type(CA, <<"drone">>)]),
    io:format("is_online drone-001 (=true) : ~p~n", [ss_client:is_online(CA, <<"drone-001">>)]),

    CB = ss_client:connect(9002), ss_client:auth_consumer(CB, <<"bob">>, <<"bob123">>),
    io:format("~n=== Visto a partir do NÓ B (porta 9002) ===~n"),
    io:format("online_count GLOBAL (=2)    : ~p~n", [ss_client:online_count(CB)]),
    io:format("is_online car-001 (=true)   : ~p~n", [ss_client:is_online(CB, <<"car-001">>)]),

    io:format("~n=== TOLERÂNCIA A FALTAS: matar o nó A ===~n"),
    slave:stop(NA),
    timer:sleep(2000),
    io:format("nó B CONTINUA a responder (mantém o último estado conhecido):~n"),
    io:format("online_count a partir de B  : ~p~n", [ss_client:online_count(CB)]),
    io:format("is_online drone-001 (=true) : ~p~n", [ss_client:is_online(CB, <<"drone-001">>)]),

    slave:stop(NB),
    io:format("~n=== FIM ===~n"),
    ok.

%% Demonstra as ocorrências de PERCENTAGEM (zona vs total global).
run_pct() ->
    {NA, NB} = start_nodes(),

    C = ss_client:connect(9001),
    ss_client:auth_consumer(C, <<"alice">>, <<"alice123">>),
    ss_client:subscribe(C, <<"percentage">>, <<"any">>),

    io:format("~n=== passo 1: 1 device na zona A (A passa a 100% do total) ===~n"),
    PA = ss_client:connect(9001), ss_client:auth_producer(PA, <<"car-001">>, <<"pass1">>),
    io:format("notificacoes: ~p~n", [drain(C)]),

    io:format("~n=== passo 2: 1 device na zona B (A desce para ~~50%) ===~n"),
    PB = ss_client:connect(9002), ss_client:auth_producer(PB, <<"drone-001">>, <<"pass3">>),
    io:format("notificacoes: ~p~n", [drain(C)]),

    io:format("~n=== passo 3: +1 device na zona B (A desce para ~~33%) ===~n"),
    PB2 = ss_client:connect(9002), ss_client:auth_producer(PB2, <<"drone-002">>, <<"pass4">>),
    io:format("notificacoes: ~p~n", [drain(C)]),

    slave:stop(NA), slave:stop(NB),
    io:format("~n=== FIM ===~n"),
    ok.

%% Recolhe as notificações de percentagem (formato {Dir, X, Valor}).
drain(Socket) -> drain(Socket, []).
drain(Socket, Acc) ->
    case ss_client:recv_push(Socket, 1500) of
        timeout -> lists:reverse(Acc);
        #{<<"notify">> := <<"percentage">>, <<"direction">> := D,
          <<"threshold">> := X, <<"value">> := V} ->
            drain(Socket, [{D, X, V} | Acc]);
        _Other ->
            drain(Socket, Acc)
    end.

setup(Node, Zone, Port, GossipPort, GossipPeers) ->
    ok = rpc:call(Node, application, load, [ss]),
    rpc:call(Node, application, set_env, [ss, zone, Zone]),
    rpc:call(Node, application, set_env, [ss, port, Port]),
    rpc:call(Node, application, set_env, [ss, gossip_port, GossipPort]),
    rpc:call(Node, application, set_env, [ss, gossip_peers, GossipPeers]),
    ok = rpc:call(Node, application, start, [ss]).

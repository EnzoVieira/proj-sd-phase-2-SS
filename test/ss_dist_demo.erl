%%%-------------------------------------------------------------------
%%% ss_dist_demo — Demonstração distribuída (Fase 4B)
%%%
%%% Arranca 2 nós SS (zonas diferentes) na mesma máquina usando o módulo slave,
%%% liga produtores a zonas distintas, e mostra que as queries são GLOBAIS
%%% (cada nó vê o estado de todos via gossip + CRDT). No fim mata um nó para
%%% demonstrar tolerância a faltas.
%%%
%%% Pré-requisito: o nó controlador tem de estar distribuído (-sname) e com
%%% cookie. Ver o comando no fim deste ficheiro.
%%%-------------------------------------------------------------------
-module(ss_dist_demo).
-export([run/0, run_pct/0]).

run() ->
    EbinAbs = filename:absname("ebin"),
    {ok, Host} = inet:gethostname(),
    HostA = list_to_atom(Host),
    Args = "-setcookie sscookie -pa " ++ EbinAbs,

    %% 1. Arrancar dois nós SS
    {ok, NA} = slave:start(HostA, ssa, Args),
    {ok, NB} = slave:start(HostA, ssb, Args),
    io:format("~nnós arrancados: ~p e ~p~n", [NA, NB]),

    %% 2. Configurar zona/peers/porta e arrancar a app em cada nó
    setup(NA, zonea, [NB], 9001),
    setup(NB, zoneb, [NA], 9002),
    rpc:call(NA, net_adm, ping, [NB]),   %% garantir que A e B se veem
    timer:sleep(300),

    %% 3. Produtores em zonas diferentes
    PA = ss_client:connect(9001), ss_client:auth_producer(PA, <<"car-001">>, <<"pass1">>),
    PB = ss_client:connect(9002), ss_client:auth_producer(PB, <<"drone-001">>, <<"pass3">>),

    %% 4. Esperar o gossip convergir (>1 tick)
    timer:sleep(1500),

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
    timer:sleep(1500),
    io:format("nó B CONTINUA a responder (mantém o último estado conhecido):~n"),
    io:format("online_count a partir de B  : ~p~n", [ss_client:online_count(CB)]),
    io:format("is_online drone-001 (=true) : ~p~n", [ss_client:is_online(CB, <<"drone-001">>)]),

    slave:stop(NB),
    io:format("~n=== FIM ===~n"),
    ok.

%% Demonstra as ocorrências de PERCENTAGEM (zona vs total global).
run_pct() ->
    EbinAbs = filename:absname("ebin"),
    {ok, Host} = inet:gethostname(),
    HostA = list_to_atom(Host),
    Args = "-setcookie sscookie -pa " ++ EbinAbs,
    {ok, NA} = slave:start(HostA, ssa, Args),
    {ok, NB} = slave:start(HostA, ssb, Args),
    setup(NA, zonea, [NB], 9001),
    setup(NB, zoneb, [NA], 9002),
    rpc:call(NA, net_adm, ping, [NB]),
    timer:sleep(300),

    %% Consumidor no nó A subscreve a percentagem da zona A
    C = ss_client:connect(9001),
    ss_client:auth_consumer(C, <<"alice">>, <<"alice123">>),
    ss_client:subscribe(C, <<"percentage">>, <<"any">>),

    io:format("~n=== passo 1: 1 device na zona A (A passa a 100% do total) ===~n"),
    PA = ss_client:connect(9001), ss_client:auth_producer(PA, <<"car-001">>, <<"pass1">>),
    io:format("notificacoes recebidas: ~p~n", [drain(C)]),

    io:format("~n=== passo 2: 1 device na zona B (A desce para ~~50%) ===~n"),
    PB = ss_client:connect(9002), ss_client:auth_producer(PB, <<"drone-001">>, <<"pass3">>),
    io:format("notificacoes recebidas: ~p~n", [drain(C)]),

    io:format("~n=== passo 3: +1 device na zona B (A desce para ~~33%) ===~n"),
    PB2 = ss_client:connect(9002), ss_client:auth_producer(PB2, <<"drone-002">>, <<"pass4">>),
    io:format("notificacoes recebidas: ~p~n", [drain(C)]),

    slave:stop(NA), slave:stop(NB),
    io:format("~n=== FIM ===~n"),
    ok.

%% Recolhe todas as notificações de percentagem disponíveis (formato {Dir,X,Val}).
drain(Socket) -> drain(Socket, []).
drain(Socket, Acc) ->
    case ss_client:recv_push(Socket, 900) of
        timeout -> lists:reverse(Acc);
        #{<<"notify">> := <<"percentage">>, <<"direction">> := D,
          <<"threshold">> := X, <<"value">> := V} ->
            drain(Socket, [{D, X, V} | Acc]);
        _Other ->
            drain(Socket, Acc)
    end.

setup(Node, Zone, Peers, Port) ->
    ok = rpc:call(Node, application, load, [ss]),
    rpc:call(Node, application, set_env, [ss, zone, Zone]),
    rpc:call(Node, application, set_env, [ss, peers, Peers]),
    rpc:call(Node, application, set_env, [ss, port, Port]),
    ok = rpc:call(Node, application, start, [ss]).

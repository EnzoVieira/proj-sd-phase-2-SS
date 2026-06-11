%%%-------------------------------------------------------------------
%%% ss_tcp — Acceptor TCP (evoluído na Fase 1C)
%%%
%%% Na Fase 0 isto era um servidor echo. Agora é o acceptor supervisionado:
%%%   - start_link/1 cria o socket de escuta e devolve {ok, Pid} ao supervisor
%%%   - accept_loop aceita ligações e, por cada uma, cria um processo ss_conn
%%%     que trata o cliente (descodifica JSON, autentica, etc.)
%%%-------------------------------------------------------------------
-module(ss_tcp).
-export([start_link/1, accept_loop/1]).

%% start_link/1 — chamado pelo supervisor. Cria o socket de escuta de forma
%% SÍNCRONA (para podermos devolver {error, Razao} se a porta estiver ocupada)
%% e arranca o ciclo de aceitação num processo ligado.
start_link(Port) ->
    Opts = [binary, {packet, line}, {active, false}, {reuseaddr, true}],
    case gen_tcp:listen(Port, Opts) of
        {ok, ListenSocket} ->
            io:format("[ss_tcp] a escutar na porta ~p~n", [Port]),
            %% spawn_link liga o novo processo a quem chamou (o supervisor),
            %% para que a supervisão funcione.
            Pid = spawn_link(?MODULE, accept_loop, [ListenSocket]),
            %% A posse do socket de escuta passa para o processo do loop.
            gen_tcp:controlling_process(ListenSocket, Pid),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% accept_loop/1 — aceita uma ligação e delega-a a um ss_conn; repete.
accept_loop(ListenSocket) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    io:format("[ss_tcp] novo cliente ligado~n"),
    %% Cria o processo que trata esta ligação e transfere-lhe a posse do socket.
    Pid = ss_conn:start(Socket),
    gen_tcp:controlling_process(Socket, Pid),
    accept_loop(ListenSocket).

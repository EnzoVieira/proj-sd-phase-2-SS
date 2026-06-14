%%%-------------------------------------------------------------------
%%% ss_tcp — Acceptor TCP: aceita ligações e gera um ss_conn por cada uma.
%%%-------------------------------------------------------------------
-module(ss_tcp).
-export([start_link/1, accept_loop/1]).

%% Bind síncrono para devolver erro ao supervisor se a porta estiver ocupada.
start_link(Port) ->
    Opts = [binary, {packet, line}, {active, false}, {reuseaddr, true}],
    case gen_tcp:listen(Port, Opts) of
        {ok, ListenSocket} ->
            io:format("[ss_tcp] a escutar na porta ~p~n", [Port]),
            Pid = spawn_link(?MODULE, accept_loop, [ListenSocket]),
            gen_tcp:controlling_process(ListenSocket, Pid),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

accept_loop(ListenSocket) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    io:format("[ss_tcp] novo cliente ligado~n"),
    Pid = ss_conn:start(Socket),
    gen_tcp:controlling_process(Socket, Pid),
    accept_loop(ListenSocket).

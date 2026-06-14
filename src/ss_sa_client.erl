%%%-------------------------------------------------------------------
%%% ss_sa_client — Cliente para o Servidor de Agregação (SA).
%%% Abre uma ligação TCP por pedido (o SA fecha após responder), envia um
%%% AggregationRequest em JSON e devolve o AggregationResult descodificado.
%%% Config (env 'ss'): sa_host, sa_port.
%%%-------------------------------------------------------------------
-module(ss_sa_client).
-export([aggregate/1]).

-define(TIMEOUT, 5000).

%% aggregate(RequestMap) -> {ok, ResultMap} | {error, Reason}
aggregate(Request) ->
    Host = application:get_env(ss, sa_host, "localhost"),
    Port = application:get_env(ss, sa_port, 9090),
    case gen_tcp:connect(Host, Port,
                         [binary, {packet, line}, {active, false}], ?TIMEOUT) of
        {ok, Socket} ->
            Reply = query(Socket, Request),
            gen_tcp:close(Socket),
            Reply;
        {error, Reason} ->
            {error, {connect, Reason}}
    end.

query(Socket, Request) ->
    case gen_tcp:send(Socket, [ss_json:encode(Request), "\n"]) of
        ok ->
            case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
                {ok, Line}      -> decode_reply(Line);
                {error, Reason} -> {error, {recv, Reason}}
            end;
        {error, Reason} ->
            {error, {send, Reason}}
    end.

%% O SA responde com a linha literal "null" em caso de erro.
decode_reply(Line) ->
    case string:trim(Line) of
        <<"null">> ->
            {error, sa_null};
        Trimmed ->
            try {ok, ss_json:decode(Trimmed)}
            catch _:_ -> {error, bad_response}
            end
    end.

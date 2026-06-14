%%%-------------------------------------------------------------------
%%% ss_dht_client — Cliente para o DHT/AST (ingestão de eventos).
%%% Abre uma ligação TCP por pedido (o DHT fecha após o ACK), envia um
%%% INGEST em JSON e devolve a resposta (ACK) descodificada.
%%% Config (env 'ss'): dht_host, dht_port.
%%%-------------------------------------------------------------------
-module(ss_dht_client).
-export([ingest/2]).

-define(TIMEOUT, 5000).

%% ingest(EventMap, IndexField) -> {ok, RespMap} | {error, Reason}
ingest(Event, IndexField) ->
    Host = application:get_env(ss, dht_host, "localhost"),
    Port = application:get_env(ss, dht_port, 7878),
    Request = #{<<"op">>         => <<"INGEST">>,
                <<"indexField">> => IndexField,
                <<"event">>      => Event},
    case gen_tcp:connect(Host, Port,
                         [binary, {packet, line}, {active, false}], ?TIMEOUT) of
        {ok, Socket} ->
            Reply = send_recv(Socket, Request),
            gen_tcp:close(Socket),
            Reply;
        {error, Reason} ->
            {error, {connect, Reason}}
    end.

send_recv(Socket, Request) ->
    case gen_tcp:send(Socket, [ss_json:encode(Request), "\n"]) of
        ok ->
            case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
                {ok, Line}      -> {ok, ss_json:decode(string:trim(Line))};
                {error, Reason} -> {error, {recv, Reason}}
            end;
        {error, Reason} ->
            {error, {send, Reason}}
    end.

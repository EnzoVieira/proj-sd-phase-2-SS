%%%-------------------------------------------------------------------
%%% ss_sa_client — Cliente para o Servidor de Agregação (SA).
%%% Abre uma ligação TCP por pedido (o SA fecha após responder), envia um
%%% AggregationRequest em JSON e devolve o AggregationResult descodificado.
%%% Config (env 'ss'): sa_host, sa_port (SA por omissão) e sa_zones
%%% (mapa zona => {Host, Port} para rotear por zona — localidade de dados).
%%%-------------------------------------------------------------------
-module(ss_sa_client).
-export([aggregate/1]).

-define(TIMEOUT, 5000).

%% aggregate(RequestMap) -> {ok, ResultMap} | {error, Reason}
aggregate(Request) ->
    {Host, Port} = resolve_endpoint(Request),
    case gen_tcp:connect(Host, Port,
                         [binary, {packet, line}, {active, false}], ?TIMEOUT) of
        {ok, Socket} ->
            Reply = query(Socket, Request),
            gen_tcp:close(Socket),
            Reply;
        {error, Reason} ->
            {error, {connect, Reason}}
    end.

%% Escolhe o SA: se o pedido filtra por zona (indexField="zone") e essa zona
%% tem entrada em sa_zones, usa esse SA (localidade); senão, o SA por omissão.
resolve_endpoint(Request) ->
    case maps:get(<<"indexField">>, Request, undefined) of
        <<"zone">> ->
            Zone  = maps:get(<<"indexValue">>, Request, undefined),
            Zones = application:get_env(ss, sa_zones, #{}),
            case maps:find(Zone, Zones) of
                {ok, {Host, Port}} -> {Host, Port};
                error              -> default_endpoint()
            end;
        _ ->
            default_endpoint()
    end.

default_endpoint() ->
    {application:get_env(ss, sa_host, "localhost"),
     application:get_env(ss, sa_port, 9090)}.

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

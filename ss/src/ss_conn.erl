%%%-------------------------------------------------------------------
%%% ss_conn — Tratamento de UMA ligação de cliente (um processo por ligação).
%%% Lê linhas JSON, despacha pelo campo "cmd", responde, e mantém o estado da
%%% sessão nos argumentos de loop/2. Respostas:
%%%   {"status":"ok"} | {"status":"ok","result":{...}} | {"error":"<msg>","code":<N>}
%%%-------------------------------------------------------------------
-module(ss_conn).
-export([start/1]).

start(Socket) ->
    spawn(fun() -> init(Socket) end).

init(Socket) ->
    Session = #{authenticated => false, role => undefined, device_id => undefined},
    loop(Socket, Session).

loop(Socket, Session) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Line} ->
            {Response, NewSession} = handle_line(Socket, Line, Session),
            gen_tcp:send(Socket, [Response, "\n"]),
            loop(Socket, NewSession);
        {error, closed} ->
            %% Ao terminar, o monitor no ss_state marca o dispositivo offline.
            io:format("[ss_conn] cliente desligou~n"),
            ok
    end.

%%====================================================================
%% Tratamento de uma linha
%%====================================================================

handle_line(Socket, Line, Session) ->
    case safe_decode(Line) of
        {ok, Map} when is_map(Map) -> dispatch(Socket, Map, Session);
        _ -> {error_resp(400, <<"Invalid JSON">>), Session}
    end.

safe_decode(Line) ->
    try {ok, ss_json:decode(Line)}
    catch _:_ -> error
    end.

dispatch(Socket, Map, Session) ->
    case maps:get(<<"cmd">>, Map, undefined) of
        undefined -> {error_resp(400, <<"Missing 'cmd' field">>), Session};
        Cmd       -> route(Socket, Cmd, Map, Session)
    end.

%% A maioria dos handlers ignora o Socket; subscribe/unsubscribe precisam dele.
route(_S, <<"auth_producer">>, Map, Session)        -> handle_auth_producer(Map, Session);
route(_S, <<"auth_consumer">>, Map, Session)        -> handle_auth_consumer(Map, Session);
route(_S, <<"register_consumer">>, Map, Session)    -> handle_register_consumer(Map, Session);
route(_S, <<"event">>, Map, Session)                -> handle_event(Map, Session);
route(_S, <<"online_count">>, Map, Session)         -> handle_online_count(Map, Session);
route(_S, <<"online_count_by_type">>, Map, Session) -> handle_online_count_by_type(Map, Session);
route(_S, <<"is_online">>, Map, Session)            -> handle_is_online(Map, Session);
route(_S, <<"active_count">>, Map, Session)         -> handle_active_count(Map, Session);
route(_S, <<"aggregate">>, Map, Session)            -> handle_aggregate(Map, Session);
route(S,  <<"subscribe">>, Map, Session)            -> handle_subscribe(S, Map, Session);
route(_S, <<"unsubscribe">>, Map, Session)          -> handle_unsubscribe(Map, Session);
route(_S, Cmd, _Map, Session) when is_binary(Cmd)   -> {error_resp(400, <<"Unknown command: ", Cmd/binary>>), Session};
route(_S, _Cmd, _Map, Session)                      -> {error_resp(400, <<"Invalid 'cmd' field">>), Session}.

%%====================================================================
%% Handlers — autenticação
%%====================================================================

handle_auth_producer(Map, Session) ->
    case maps:get(authenticated, Session) of
        true ->
            {error_resp(409, <<"already_authenticated">>), Session};
        false ->
            Device   = maps:get(<<"device">>, Map, undefined),
            Password = maps:get(<<"password">>, Map, undefined),
            case is_binary(Device) andalso is_binary(Password) of
                false ->
                    {error_resp(400, <<"invalid">>), Session};
                true ->
                    case ss_registry:authenticate_device(Device, Password) of
                        true ->
                            %% self() é o pid monitorizado pelo ss_state.
                            Type = ss_registry:device_type(Device),
                            ss_state:mark_online(Device, Type, self()),
                            NewSession = Session#{authenticated := true,
                                                  role := producer,
                                                  device_id := Device},
                            {ok_resp(), NewSession};
                        false ->
                            {error_resp(401, <<"auth_failed">>), Session}
                    end
            end
    end.

handle_auth_consumer(Map, Session) ->
    case maps:get(authenticated, Session) of
        true ->
            {error_resp(409, <<"already_authenticated">>), Session};
        false ->
            User     = maps:get(<<"user">>, Map, undefined),
            Password = maps:get(<<"password">>, Map, undefined),
            case is_binary(User) andalso is_binary(Password) of
                false ->
                    {error_resp(400, <<"invalid">>), Session};
                true ->
                    %% Aceita a seed estática OU o registo dinâmico replicado (CRDT).
                    Ok = ss_registry:authenticate_consumer(User, Password)
                         orelse ss_cluster:authenticate_consumer(User, Password),
                    case Ok of
                        true ->
                            NewSession = Session#{authenticated := true,
                                                  role := consumer,
                                                  device_id := undefined},
                            {ok_resp(), NewSession};
                        false ->
                            {error_resp(401, <<"auth_failed">>), Session}
                    end
            end
    end.

%% register_consumer: cria a credencial (replicada por CRDT); NÃO autentica.
%% Verifica duplicado contra a seed estática e o CRDT (idempotente: mesma senha
%% -> ok; senha diferente -> 409). Orquestrado aqui para não aninhar gen_servers.
handle_register_consumer(Map, Session) ->
    case maps:get(authenticated, Session) of
        true ->
            {error_resp(409, <<"already_authenticated">>), Session};
        false ->
            User     = maps:get(<<"user">>, Map, undefined),
            Password = maps:get(<<"password">>, Map, undefined),
            case is_binary(User) andalso is_binary(Password) of
                false ->
                    {error_resp(400, <<"invalid">>), Session};
                true ->
                    {do_register_consumer(User, Password), Session}
            end
    end.

do_register_consumer(User, Password) ->
    case ss_registry:consumer_password(User) of
        {ok, Password} -> ok_resp();                         %% seed, mesma senha
        {ok, _Other}   -> error_resp(409, <<"already_registered">>);
        error ->
            case ss_cluster:register_consumer(User, Password) of
                ok                          -> ok_resp();
                {error, already_registered} -> error_resp(409, <<"already_registered">>)
            end
    end.

%%====================================================================
%% Handlers — eventos e queries
%%====================================================================

handle_event(Map, Session) ->
    with_role(producer, Session, fun() ->
        case has_fields(Map, [<<"type">>, <<"timestamp">>]) of
            false ->
                error_resp(400, <<"invalid">>);
            true ->
                DeviceId = maps:get(device_id, Session),
                ss_state:mark_event(DeviceId),
                %% Ingestão no DHT em background (não bloqueia nem falha o dispositivo).
                spawn(fun() -> ingest_event(DeviceId, Map) end),
                ok_resp()
        end
    end).

%% Constrói o evento no formato do DHT (carimbando a zona deste nó) e ingere-o
%% sob CADA campo índice configurado de que o evento tem valor. Os campos índice
%% são config do sistema (alinhados com o nó DHT); cada evento tem >= 1.
ingest_event(DeviceId, Map) ->
    Event = #{<<"deviceId">>  => DeviceId,
              <<"type">>      => maps:get(<<"type">>, Map),
              <<"zone">>      => node_zone(),
              <<"fields">>    => event_fields(Map),
              <<"timestamp">> => ms_to_seconds(maps:get(<<"timestamp">>, Map))},
    %% Normaliza para binário: o config pode trazer átomos (zone) ou listas ("zone").
    RawFields  = application:get_env(ss, dht_index_fields, [<<"zone">>]),
    IndexFields = [to_bin(F) || F <- RawFields],
    lists:foreach(
        fun(Field) ->
            case has_index_value(Field, Event) of
                true  -> ingest_one(Event, Field);
                false -> ok
            end
        end, IndexFields).

ingest_one(Event, Field) ->
    case ss_dht_client:ingest(Event, Field) of
        {ok, _}         -> ok;
        {error, Reason} -> io:format("[ss_conn] ingest DHT (~s) falhou: ~p~n", [Field, Reason])
    end.

%% O evento tem valor para este campo índice? (deviceId/type/zone estão sempre
%% presentes; os restantes vêm do mapa fields.)
has_index_value(Field, Event) ->
    lists:member(Field, [<<"deviceId">>, <<"type">>, <<"zone">>])
        orelse maps:is_key(Field, maps:get(<<"fields">>, Event)).

%% Normaliza campos índice para binário independentemente do tipo vindo do config.
to_bin(F) when is_binary(F) -> F;
to_bin(F) when is_atom(F)   -> atom_to_binary(F, utf8);
to_bin(F) when is_list(F)   -> list_to_binary(F).

%% Campos índice/extra do evento (tudo menos cmd/type/timestamp), valores em string.
event_fields(Map) ->
    Drop = [<<"cmd">>, <<"type">>, <<"timestamp">>],
    maps:fold(
        fun(K, V, Acc) ->
            case lists:member(K, Drop) of
                true  -> Acc;
                false -> maps:put(K, to_str(V), Acc)
            end
        end, #{}, Map).

%% O DHT espera o timestamp em segundos (Jackson Instant); o evento traz ms.
ms_to_seconds(Ms) when is_integer(Ms) -> Ms div 1000;
ms_to_seconds(Ms) when is_float(Ms)   -> trunc(Ms) div 1000.

node_zone() ->
    case application:get_env(ss, zone, undefined) of
        undefined           -> atom_to_binary(node(), utf8);
        Z when is_atom(Z)   -> atom_to_binary(Z, utf8);
        Z when is_binary(Z) -> Z
    end.

to_str(V) when is_binary(V)  -> V;
to_str(V) when is_integer(V) -> integer_to_binary(V);
to_str(V) when is_float(V)   -> float_to_binary(V, [{decimals, 6}, compact]);
to_str(true)                 -> <<"true">>;
to_str(false)                -> <<"false">>;
to_str(null)                 -> <<"null">>;
to_str(V)                    -> list_to_binary(io_lib:format("~p", [V])).

%% Queries são GLOBAIS (somam todas as zonas) -> vão ao ss_cluster.

handle_online_count(_Map, Session) ->
    with_role(consumer, Session, fun() ->
        result_resp(#{<<"online">> => ss_cluster:count_online()})
    end).

handle_online_count_by_type(Map, Session) ->
    with_role(consumer, Session, fun() ->
        case maps:get(<<"type">>, Map, undefined) of
            Type when is_binary(Type) ->
                result_resp(#{<<"online">> => ss_cluster:count_online_by_type(Type)});
            _ ->
                error_resp(400, <<"Missing 'type' field">>)
        end
    end).

handle_is_online(Map, Session) ->
    with_role(consumer, Session, fun() ->
        case maps:get(<<"device">>, Map, undefined) of
            Device when is_binary(Device) ->
                result_resp(#{<<"online">> => ss_cluster:is_online(Device)});
            _ ->
                error_resp(400, <<"Missing 'device' field">>)
        end
    end).

handle_active_count(_Map, Session) ->
    with_role(consumer, Session, fun() ->
        result_resp(#{<<"active">> => ss_cluster:count_active()})
    end).

%% aggregate: o SS reencaminha o pedido para o SA (Servidor de Agregação) e
%% devolve o resultado. Campos: type, minDay, maxDay, indexField, indexValue, k2, k3.
handle_aggregate(Map, Session) ->
    with_role(consumer, Session, fun() ->
        case build_agg_request(Map) of
            {ok, Req} ->
                case ss_sa_client:aggregate(Req) of
                    {ok, Result}    -> result_resp(Result);
                    {error, _Reason} -> error_resp(502, <<"sa_unavailable">>)
                end;
            {error, Msg} ->
                error_resp(400, Msg)
        end
    end).

%%====================================================================
%% Handlers — subscrição de notificações (consumidor)
%%====================================================================

handle_subscribe(Socket, Map, Session) ->
    with_role(consumer, Session, fun() ->
        case sub_topic(Map) of
            {ok, Topic} ->
                ss_pubsub:subscribe(self(), Socket, Topic),
                ok_resp();
            error ->
                error_resp(400, <<"invalid subscription">>)
        end
    end).

handle_unsubscribe(Map, Session) ->
    with_role(consumer, Session, fun() ->
        case sub_topic(Map) of
            {ok, Topic} ->
                ss_pubsub:unsubscribe(self(), Topic),
                ok_resp();
            error ->
                error_resp(400, <<"invalid subscription">>)
        end
    end).

%% Traduz os campos do pedido numa TopicKey do ss_pubsub. Ex.:
%%   {"event":"record","type":"car"} -> {record, <<"car">>}; "any" -> {record, any}.
sub_topic(Map) ->
    case maps:get(<<"event">>, Map, undefined) of
        <<"type_empty">> ->
            case maps:get(<<"type">>, Map, undefined) of
                T when is_binary(T) -> {ok, {type_empty, T}};
                _ -> error
            end;
        <<"record">> ->
            case maps:get(<<"type">>, Map, undefined) of
                <<"any">> -> {ok, {record, any}};
                T when is_binary(T) -> {ok, {record, T}};
                undefined -> {ok, {record, any}}
            end;
        <<"percentage">> ->
            {ok, percentage};
        _ ->
            error
    end.

%%====================================================================
%% Auxiliares
%%====================================================================

%% Verifica autenticação e papel; se ok corre Fun (que devolve a resposta JSON).
with_role(NeededRole, Session, Fun) ->
    case maps:get(authenticated, Session) of
        false ->
            {error_resp(401, <<"not_authenticated">>), Session};
        true ->
            case maps:get(role, Session) of
                NeededRole -> {Fun(), Session};
                _          -> {error_resp(401, <<"permission_denied">>), Session}
            end
    end.

has_fields(Map, Keys) ->
    lists:all(fun(K) -> maps:is_key(K, Map) end, Keys).

%% Valida e constrói o AggregationRequest (formato do SA) a partir do pedido.
build_agg_request(Map) ->
    case norm_agg_type(maps:get(<<"type">>, Map, undefined)) of
        undefined ->
            {error, <<"invalid aggregation type">>};
        Type ->
            Required = [<<"minDay">>, <<"maxDay">>, <<"indexField">>, <<"indexValue">>],
            case has_fields(Map, Required) of
                false ->
                    {error, <<"missing required fields">>};
                true ->
                    IndexValue = maps:get(<<"indexValue">>, Map),
                    {ok, #{<<"type">>       => Type,
                           <<"zone">>       => maps:get(<<"zone">>, Map, IndexValue),
                           <<"minDay">>     => maps:get(<<"minDay">>, Map),
                           <<"maxDay">>     => maps:get(<<"maxDay">>, Map),
                           <<"indexField">> => maps:get(<<"indexField">>, Map),
                           <<"indexValue">> => IndexValue,
                           <<"k2">>         => maps:get(<<"k2">>, Map, null),
                           <<"k3">>         => maps:get(<<"k3">>, Map, null)}}
            end
    end.

%% Normaliza o tipo para maiúsculas e valida contra as operações suportadas.
norm_agg_type(T) when is_binary(T) ->
    U = string:uppercase(T),
    case lists:member(U, [<<"COUNT">>, <<"SUM">>, <<"MAX">>, <<"MIN">>, <<"SUM_PRODUCT">>]) of
        true  -> U;
        false -> undefined
    end;
norm_agg_type(_) ->
    undefined.

%%====================================================================
%% Construção das respostas (JSON)
%%====================================================================

ok_resp() ->
    ss_json:encode(#{<<"status">> => <<"ok">>}).

result_resp(Result) ->
    ss_json:encode(#{<<"status">> => <<"ok">>, <<"result">> => Result}).

error_resp(Code, Msg) ->
    ss_json:encode(#{<<"error">> => Msg, <<"code">> => Code}).

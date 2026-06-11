%%%-------------------------------------------------------------------
%%% ss_client — Cliente de teste em Erlang (Fase 1C)
%%%
%%% Helpers para falar com o SS a partir da shell Erlang. Exemplo:
%%%   S = ss_client:connect().
%%%   ss_client:auth_producer(S, <<"car-001">>, <<"pass1">>).
%%%   ss_client:send(S, #{<<"cmd">> => <<"auth_consumer">>, ...}).
%%%   ss_client:close(S).
%%%-------------------------------------------------------------------
-module(ss_client).
-export([connect/0, connect/1, send/2, close/1,
         auth_producer/3, auth_consumer/3,
         event/3, online_count/1, online_count_by_type/2,
         is_online/2, active_count/1,
         subscribe/3, unsubscribe/3, recv_push/1, recv_push/2,
         listen/1]).

connect() -> connect(9000).

connect(Port) ->
    {ok, Socket} = gen_tcp:connect("localhost", Port,
                                   [binary, {packet, line}, {active, false}]),
    Socket.

%% send/2 — envia um mapa (codificado em JSON) e devolve a resposta descodificada.
send(Socket, Map) ->
    ok = gen_tcp:send(Socket, [ss_json:encode(Map), "\n"]),
    {ok, Resp} = gen_tcp:recv(Socket, 0),
    ss_json:decode(Resp).

auth_producer(Socket, Device, Password) ->
    send(Socket, #{<<"cmd">> => <<"auth_producer">>,
                   <<"device">> => Device,
                   <<"password">> => Password}).

auth_consumer(Socket, User, Password) ->
    send(Socket, #{<<"cmd">> => <<"auth_consumer">>,
                   <<"user">> => User,
                   <<"password">> => Password}).

%% event/3 — envia um evento. Fields é um mapa com os campos extra (índices),
%% ex: #{<<"speed">> => 40}. type e timestamp são obrigatórios.
event(Socket, Type, Fields) ->
    Base = #{<<"cmd">> => <<"event">>,
             <<"type">> => Type,
             <<"timestamp">> => erlang:system_time(millisecond)},
    send(Socket, maps:merge(Base, Fields)).

online_count(Socket) ->
    send(Socket, #{<<"cmd">> => <<"online_count">>}).

online_count_by_type(Socket, Type) ->
    send(Socket, #{<<"cmd">> => <<"online_count_by_type">>, <<"type">> => Type}).

is_online(Socket, Device) ->
    send(Socket, #{<<"cmd">> => <<"is_online">>, <<"device">> => Device}).

active_count(Socket) ->
    send(Socket, #{<<"cmd">> => <<"active_count">>}).

%% Event = <<"type_empty">> | <<"record">>; Type = <<"car">> | <<"any">> | ...
subscribe(Socket, Event, Type) ->
    send(Socket, #{<<"cmd">> => <<"subscribe">>, <<"event">> => Event, <<"type">> => Type}).

unsubscribe(Socket, Event, Type) ->
    send(Socket, #{<<"cmd">> => <<"unsubscribe">>, <<"event">> => Event, <<"type">> => Type}).

%% listen/1 — torna a receção AUTOMÁTICA. Põe o socket em modo ATIVO e cria um
%% processo que recebe as notificações como mensagens {tcp, Socket, Linha} e
%% imprime-as à medida que chegam (sem nunca chamar recv).
%%
%% Usar DEPOIS de subscrever, e NÃO misturar com send/2 ou recv_push/1 no mesmo
%% socket (esses são para modo passivo). Devolve o Pid do ouvinte.
listen(Socket) ->
    Pid = spawn(fun() ->
        %% Espera até passarmos a posse do socket para este processo, só depois
        %% ativa o modo ativo (assim as mensagens vêm para cá).
        receive go -> ok end,
        inet:setopts(Socket, [{active, true}]),
        listen_loop(Socket)
    end),
    ok = gen_tcp:controlling_process(Socket, Pid),
    Pid ! go,
    Pid.

listen_loop(Socket) ->
    receive
        {tcp, Socket, Line} ->
            io:format(">>> NOTIFICACAO recebida automaticamente: ~s", [Line]),
            listen_loop(Socket);
        {tcp_closed, Socket} ->
            io:format(">>> ligacao fechada~n");
        stop ->
            ok
    end.

%% Lê uma notificação assíncrona empurrada pelo servidor (com timeout).
recv_push(Socket) -> recv_push(Socket, 1000).
recv_push(Socket, Timeout) ->
    case gen_tcp:recv(Socket, 0, Timeout) of
        {ok, Line}       -> ss_json:decode(Line);
        {error, timeout} -> timeout;
        {error, Reason}  -> {error, Reason}
    end.

close(Socket) ->
    gen_tcp:close(Socket).

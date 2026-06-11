%%%-------------------------------------------------------------------
%%% ss_pubsub — Publicação/subscrição de ocorrências (Fase 3A)
%%%
%%% Os consumidores subscrevem ocorrências; quando o ss_state deteta uma,
%%% chama publish/1 e nós enviamos uma notificação JSON para cada subscritor
%%% interessado (escrevendo direto no socket dele).
%%%
%%% Chaves de tópico (TopicKey) suportadas nesta fase:
%%%   {type_empty, Type}   -> tipo Type ficou sem online
%%%   {record, Type}       -> novo recorde de online do tipo Type
%%%   {record, any}        -> novo recorde de online no total (qualquer tipo)
%%%
%%% Estado:
%%%   subs    : TopicKey -> #{Pid => Socket}    (quem subscreve cada tópico)
%%%   clients : Pid -> #{socket, ref, topics}    (p/ limpar quando o cliente cai)
%%%
%%% CONCEITOS NOVOS: gen_tcp:send a partir de OUTRO processo (push), e usar
%%% monitores para limpar subscrições de clientes que desligam.
%%%-------------------------------------------------------------------
-module(ss_pubsub).
-behaviour(gen_server).

-export([start_link/0, subscribe/3, unsubscribe/2, publish/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Pid    = processo ss_conn do consumidor
%% Socket = socket desse consumidor (para lhe enviarmos pushes)
%% Topic  = TopicKey (ver topo do ficheiro)
subscribe(Pid, Socket, Topic) ->
    gen_server:call(?MODULE, {subscribe, Pid, Socket, Topic}).

unsubscribe(Pid, Topic) ->
    gen_server:call(?MODULE, {unsubscribe, Pid, Topic}).

%% Chamado pelo ss_state quando deteta uma ocorrência. Assíncrono (cast).
publish(Occurrence) ->
    gen_server:cast(?MODULE, {publish, Occurrence}).

%%====================================================================
%% Callbacks
%%====================================================================

init([]) ->
    {ok, #{subs => #{}, clients => #{}}}.

handle_call({subscribe, Pid, Socket, Topic}, _From, State) ->
    Subs    = maps:get(subs, State),
    Clients = maps:get(clients, State),

    %% 1. adicionar Pid->Socket ao conjunto de subscritores deste tópico
    TopicMap  = maps:get(Topic, Subs, #{}),
    Subs2     = maps:put(Topic, maps:put(Pid, Socket, TopicMap), Subs),

    %% 2. garantir registo do cliente (monitor criado uma só vez) e juntar tópico
    Client = case maps:find(Pid, Clients) of
                 {ok, C} -> C;
                 error   -> #{socket => Socket,
                              ref    => erlang:monitor(process, Pid),
                              topics => sets:new()}
             end,
    Client2  = Client#{topics := sets:add_element(Topic, maps:get(topics, Client))},
    Clients2 = maps:put(Pid, Client2, Clients),

    {reply, ok, State#{subs := Subs2, clients := Clients2}};

handle_call({unsubscribe, Pid, Topic}, _From, State) ->
    {reply, ok, remove_sub(Pid, Topic, State)}.

handle_cast({publish, Occurrence}, State) ->
    Subs = maps:get(subs, State),
    Msg  = notification_json(Occurrence),
    %% Para cada tópico-alvo desta ocorrência, enviar a todos os subscritores.
    lists:foreach(
        fun(Topic) ->
            maps:foreach(
                fun(_Pid, Socket) ->
                    catch gen_tcp:send(Socket, [Msg, "\n"])
                end,
                maps:get(Topic, Subs, #{}))
        end,
        topics_for(Occurrence)),
    {noreply, State}.

%% Cliente caiu: remover todas as suas subscrições.
handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    {noreply, remove_client(Pid, State)};
handle_info(_Msg, State) ->
    {noreply, State}.

%%====================================================================
%% Mapeamento ocorrência -> tópicos e -> JSON
%%====================================================================

%% Que tópicos uma ocorrência atinge (quem deve ser notificado).
topics_for({type_empty, Type})       -> [{type_empty, Type}];
topics_for({record, total, _V})      -> [{record, any}];
topics_for({record, Type, _V})       -> [{record, Type}];
topics_for({percentage, _D, _X, _P}) -> [percentage].

%% Mensagem JSON enviada ao consumidor.
notification_json({type_empty, Type}) ->
    ss_json:encode(#{<<"notify">> => <<"type_empty">>, <<"type">> => Type});
notification_json({record, total, Value}) ->
    ss_json:encode(#{<<"notify">> => <<"record">>, <<"type">> => <<"any">>, <<"value">> => Value});
notification_json({record, Type, Value}) ->
    ss_json:encode(#{<<"notify">> => <<"record">>, <<"type">> => Type, <<"value">> => Value});
notification_json({percentage, Direction, Threshold, Pct}) ->
    ss_json:encode(#{<<"notify">>    => <<"percentage">>,
                     <<"direction">> => atom_to_binary(Direction, utf8), %% up | down
                     <<"threshold">> => Threshold,
                     <<"value">>     => Pct}).

%%====================================================================
%% Limpeza de subscrições
%%====================================================================

%% Remove a subscrição de um Pid a um Topic; se o cliente ficar sem tópicos,
%% deixa de ser monitorizado e é esquecido.
remove_sub(Pid, Topic, State) ->
    Subs    = maps:get(subs, State),
    Clients = maps:get(clients, State),

    TopicMap = maps:get(Topic, Subs, #{}),
    Subs2    = maps:put(Topic, maps:remove(Pid, TopicMap), Subs),

    Clients2 =
        case maps:find(Pid, Clients) of
            {ok, Client} ->
                Topics2 = sets:del_element(Topic, maps:get(topics, Client)),
                case sets:is_empty(Topics2) of
                    true ->
                        erlang:demonitor(maps:get(ref, Client), [flush]),
                        maps:remove(Pid, Clients);
                    false ->
                        maps:put(Pid, Client#{topics := Topics2}, Clients)
                end;
            error ->
                Clients
        end,
    State#{subs := Subs2, clients := Clients2}.

%% Remove o cliente por completo (todas as subscrições).
remove_client(Pid, State) ->
    Clients = maps:get(clients, State),
    case maps:find(Pid, Clients) of
        {ok, Client} ->
            Subs = maps:get(subs, State),
            %% tirar este Pid de cada tópico que subscrevia
            Subs2 = lists:foldl(
                      fun(Topic, Acc) ->
                          TopicMap = maps:get(Topic, Acc, #{}),
                          maps:put(Topic, maps:remove(Pid, TopicMap), Acc)
                      end,
                      Subs,
                      sets:to_list(maps:get(topics, Client))),
            State#{subs := Subs2, clients := maps:remove(Pid, Clients)};
        error ->
            State
    end.

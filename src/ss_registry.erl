%%%-------------------------------------------------------------------
%%% ss_registry — Registo de credenciais (Fase 1B)
%%%
%%% Primeiro gen_server do projeto. No arranque (init) carrega os ficheiros
%%% priv/devices.json e priv/consumers.json para mapas em memória. Depois
%%% responde a pedidos de autenticação.
%%%
%%% Dispositivos: id -> #{password, type}   (id, senha e tipo são imutáveis)
%%% Consumidores: user -> password
%%%
%%% CONCEITOS NOVOS: behaviour gen_server, callbacks init/handle_call,
%%% registar o processo com um nome ({local, ?MODULE}) para o chamar por nome.
%%%-------------------------------------------------------------------
-module(ss_registry).

%% Declara que este módulo segue o "contrato" do gen_server. O compilador
%% avisa se faltar algum callback obrigatório.
-behaviour(gen_server).

%% API pública (o que outros módulos chamam)
-export([start_link/0, start_link/2,
         authenticate_device/2, device_type/1,
         authenticate_consumer/2]).

%% Callbacks do gen_server (chamados pelo OTP, não diretamente por nós)
-export([init/1, handle_call/3, handle_cast/2]).

%% ?MODULE é uma macro que expande para o nome do módulo (ss_registry).
%% Usamo-lo como nome registado do processo.
-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

%% Arranca com os caminhos por omissão (relativos à pasta de onde corres o erl).
start_link() ->
    start_link("priv/devices.json", "priv/consumers.json").

start_link(DevFile, ConFile) ->
    %% {local, ?SERVER} regista o processo com o nome 'ss_registry', para
    %% podermos chamá-lo por nome (sem guardar o Pid).
    gen_server:start_link({local, ?SERVER}, ?MODULE, {DevFile, ConFile}, []).

%% Estas funções da API escondem o gen_server:call — quem chama nem sabe
%% que por trás está um processo. 'call' é síncrono: bloqueia até à resposta.
authenticate_device(Id, Password) ->
    gen_server:call(?SERVER, {auth_device, Id, Password}).

device_type(Id) ->
    gen_server:call(?SERVER, {device_type, Id}).

authenticate_consumer(User, Password) ->
    gen_server:call(?SERVER, {auth_consumer, User, Password}).

%%====================================================================
%% Callbacks do gen_server
%%====================================================================

%% init/1 corre quando o processo arranca. Aqui lemos e descodificamos os
%% ficheiros e devolvemos {ok, Estado}. O Estado fica guardado pelo OTP e é
%% passado a cada handle_call.
init({DevFile, ConFile}) ->
    Devices   = load_devices(DevFile),
    Consumers = load_consumers(ConFile),
    State = #{devices => Devices, consumers => Consumers},
    io:format("[ss_registry] ~p dispositivos, ~p consumidores carregados~n",
              [maps:size(Devices), maps:size(Consumers)]),
    {ok, State}.

%% handle_call/3 recebe (Pedido, Quem, Estado) e devolve {reply, Resposta, NovoEstado}.
%% Cada cláusula trata um tipo de pedido (graças ao pattern matching no 1º arg).

handle_call({auth_device, Id, Password}, _From, State) ->
    Devices = maps:get(devices, State),
    Reply =
        case maps:find(Id, Devices) of
            {ok, #{password := Password}} -> true;   %% senha bate certo
            _                             -> false   %% inexistente ou senha errada
        end,
    {reply, Reply, State};

handle_call({device_type, Id}, _From, State) ->
    Devices = maps:get(devices, State),
    Reply =
        case maps:find(Id, Devices) of
            {ok, #{type := Type}} -> Type;
            error                 -> undefined
        end,
    {reply, Reply, State};

handle_call({auth_consumer, User, Password}, _From, State) ->
    Consumers = maps:get(consumers, State),
    Reply =
        case maps:find(User, Consumers) of
            {ok, Password} -> true;
            _              -> false
        end,
    {reply, Reply, State}.

%% Não usamos cast aqui, mas o callback tem de existir (contrato do behaviour).
handle_cast(_Msg, State) ->
    {noreply, State}.

%%====================================================================
%% Funções auxiliares (privadas)
%%====================================================================

%% Lê o ficheiro, descodifica o array JSON e constrói o mapa id -> #{password,type}.
load_devices(File) ->
    List = read_json_array(File),
    lists:foldl(
        fun(Obj, Acc) ->
            Id = maps:get(<<"id">>, Obj),
            Info = #{password => maps:get(<<"password">>, Obj),
                     type     => maps:get(<<"type">>, Obj)},
            maps:put(Id, Info, Acc)
        end,
        #{}, List).

%% Constrói o mapa user -> password.
load_consumers(File) ->
    List = read_json_array(File),
    lists:foldl(
        fun(Obj, Acc) ->
            maps:put(maps:get(<<"user">>, Obj), maps:get(<<"password">>, Obj), Acc)
        end,
        #{}, List).

read_json_array(File) ->
    {ok, Bin} = file:read_file(File),
    ss_json:decode(Bin).

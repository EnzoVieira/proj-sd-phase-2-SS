%%%-------------------------------------------------------------------
%%% ss_registry — Credenciais de dispositivos e consumidores.
%%% Carrega priv/devices.json e priv/consumers.json no arranque e autentica.
%%%   Dispositivos: id -> #{password, type}
%%%   Consumidores: user -> password
%%%-------------------------------------------------------------------
-module(ss_registry).
-behaviour(gen_server).

-export([start_link/0, start_link/2,
         authenticate_device/2, device_type/1,
         authenticate_consumer/2, consumer_password/1]).
-export([init/1, handle_call/3, handle_cast/2]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    start_link("priv/devices.json", "priv/consumers.json").

start_link(DevFile, ConFile) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, {DevFile, ConFile}, []).

authenticate_device(Id, Password) ->
    gen_server:call(?SERVER, {auth_device, Id, Password}).

device_type(Id) ->
    gen_server:call(?SERVER, {device_type, Id}).

authenticate_consumer(User, Password) ->
    gen_server:call(?SERVER, {auth_consumer, User, Password}).

%% Senha de um consumidor da seed estática. {ok, Password} | error.
consumer_password(User) ->
    gen_server:call(?SERVER, {consumer_password, User}).

%%====================================================================
%% Callbacks
%%====================================================================

init({DevFile, ConFile}) ->
    Devices   = load_devices(DevFile),
    Consumers = load_consumers(ConFile),
    State = #{devices => Devices, consumers => Consumers},
    io:format("[ss_registry] ~p dispositivos, ~p consumidores carregados~n",
              [maps:size(Devices), maps:size(Consumers)]),
    {ok, State}.

handle_call({auth_device, Id, Password}, _From, State) ->
    Devices = maps:get(devices, State),
    Reply =
        case maps:find(Id, Devices) of
            {ok, #{password := Password}} -> true;
            _                             -> false
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
    {reply, Reply, State};

handle_call({consumer_password, User}, _From, State) ->
    {reply, maps:find(User, maps:get(consumers, State)), State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%%====================================================================
%% Auxiliares
%%====================================================================

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

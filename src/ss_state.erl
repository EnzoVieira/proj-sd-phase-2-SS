%%%-------------------------------------------------------------------
%%% ss_state — Estado do sistema: online e atividade (Fase 2A)
%%%
%%% gen_server que cria e possui duas tabelas ETS:
%%%   ss_online   : {DeviceId, Type, Pid}     -> dispositivos online
%%%   ss_activity : {DeviceId, LastEventMs}    -> último evento de cada um
%%%
%%% online = autenticado e com ligação viva. Detetamos a queda da ligação com
%%%          um monitor sobre o processo ss_conn; ao receber {'DOWN',...}
%%%          marcamos offline.
%%% ativo  = enviou >= 1 evento nos últimos 60 segundos.
%%%
%%% CONCEITOS NOVOS: ETS, erlang:monitor/2, handle_info (mensagens soltas),
%%% erlang:system_time, match specs (ets:select_count).
%%%-------------------------------------------------------------------
-module(ss_state).
-behaviour(gen_server).

%% API
-export([start_link/0,
         mark_online/3, mark_event/1,
         count_online/0, count_online_by_type/1, is_online/1, count_active/0,
         online_map/0]).

%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(ONLINE,   ss_online).
-define(ACTIVITY, ss_activity).
-define(ACTIVE_WINDOW_MS, 60000).   %% 60 segundos

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Marca um dispositivo online. Vai pelo gen_server porque é ELE que tem de
%% criar o monitor sobre o processo da ligação (Pid).
mark_online(DeviceId, Type, Pid) ->
    gen_server:call(?MODULE, {mark_online, DeviceId, Type, Pid}).

%% Regista um evento (atividade). Escrita DIRETA na ETS — sem mensagem ao
%% gen_server — porque eventos podem ser muito frequentes e não queremos um
%% gargalo num único processo.
mark_event(DeviceId) ->
    ets:insert(?ACTIVITY, {DeviceId, now_ms()}),
    ok.

%% --- Queries: leituras diretas na ETS (concorrentes) ---

count_online() ->
    ets:info(?ONLINE, size).

%% Conta linhas cujo 2º elemento (Type) é igual ao pedido.
%% O match spec {{'_', Type, '_'}, [], [true]} significa:
%%   cabeça: casa tuplos {qualquer, Type, qualquer}
%%   guardas: nenhuma
%%   resultado: true (para o select_count contar)
count_online_by_type(Type) ->
    ets:select_count(?ONLINE, [{{'_', Type, '_'}, [], [true]}]).

is_online(DeviceId) ->
    ets:member(?ONLINE, DeviceId).

%% Conta dispositivos com LastEventMs >= (agora - 60s).
%% '$1' é uma variável do match spec (o LastEventMs); a guarda compara-a.
count_active() ->
    Threshold = now_ms() - ?ACTIVE_WINDOW_MS,
    ets:select_count(?ACTIVITY, [{{'_', '$1'}, [{'>=', '$1', Threshold}], [true]}]).

%% Devolve o mapa #{DeviceId => Tipo} dos dispositivos online nesta zona.
%% Usado pelo ss_cluster para construir o "estado da sua zona".
online_map() ->
    ets:foldl(fun({Id, Type, _Pid}, Acc) -> maps:put(Id, Type, Acc) end,
              #{}, ?ONLINE).

%%====================================================================
%% Callbacks
%%====================================================================

init([]) ->
    %% Tabelas criadas e possuídas por este processo.
    %%   named_table -> acessível pelo nome (?ONLINE/?ACTIVITY)
    %%   public      -> qualquer processo pode ler/escrever
    ets:new(?ONLINE,   [named_table, public, set, {read_concurrency, true}]),
    ets:new(?ACTIVITY, [named_table, public, set,
                        {read_concurrency, true}, {write_concurrency, true}]),
    %% Estado do gen_server:
    %%   monitors     : Pid -> {DeviceId, MonitorRef}  (p/ marcar offline)
    %%   counts       : Type -> Nº online  (snapshot anterior, p/ detetar mudanças)
    %%   records      : Type -> máx. já visto de online desse tipo
    %%   record_total : máx. já visto de online no total
    {ok, #{monitors => #{}, counts => #{}, records => #{}, record_total => 0}}.

handle_call({mark_online, DeviceId, Type, Pid}, _From, State) ->
    Ref = erlang:monitor(process, Pid),
    ets:insert(?ONLINE, {DeviceId, Type, Pid}),
    Monitors = maps:get(monitors, State),
    State1 = State#{monitors := maps:put(Pid, {DeviceId, Ref}, Monitors)},
    %% Depois de mudar quem está online, detetar e publicar ocorrências.
    {reply, ok, detect_and_publish(State1)}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% handle_info/2 trata mensagens que não são call/cast. Os monitores entregam
%% aqui o {'DOWN', Ref, process, Pid, Razao} quando o processo monitorizado morre.
handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    Monitors = maps:get(monitors, State),
    case maps:find(Pid, Monitors) of
        {ok, {DeviceId, _MonRef}} ->
            %% Só remover se a entrada online ainda for DESTA ligação (Pid).
            %% (Evita apagar uma reconexão entretanto feita pelo mesmo device.)
            State1 =
                case ets:lookup(?ONLINE, DeviceId) of
                    [{DeviceId, _Type, Pid}] ->
                        ets:delete(?ONLINE, DeviceId),
                        io:format("[ss_state] ~s ficou offline~n", [DeviceId]),
                        %% online mudou -> detetar ocorrências (ex: type_empty)
                        detect_and_publish(State);
                    _ ->
                        State
                end,
            {noreply, State1#{monitors := maps:remove(Pid, Monitors)}};
        error ->
            {noreply, State}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

%%====================================================================
%% Deteção e publicação de ocorrências
%%====================================================================

%% Compara o estado de online atual (lido da ETS) com o snapshot anterior
%% guardado no estado, deteta ocorrências, publica-as no ss_pubsub, e devolve
%% o estado atualizado (novo snapshot + novos recordes).
detect_and_publish(State) ->
    NewCounts = current_counts(),
    NewTotal  = maps:fold(fun(_, V, A) -> A + V end, 0, NewCounts),
    OldCounts = maps:get(counts, State),
    Records   = maps:get(records, State),
    RecTotal  = maps:get(record_total, State),

    %% (a) type_empty: tipos que tinham online (>0) e agora têm 0.
    maps:foreach(
        fun(Type, OldN) ->
            case OldN > 0 andalso maps:get(Type, NewCounts, 0) =:= 0 of
                true  -> ss_pubsub:publish({type_empty, Type});
                false -> ok
            end
        end, OldCounts),

    %% (b) record por tipo: contagem nova ultrapassa o recorde desse tipo.
    NewRecords =
        maps:fold(
            fun(Type, N, RecAcc) ->
                case N > maps:get(Type, RecAcc, 0) of
                    true  -> ss_pubsub:publish({record, Type, N}),
                             maps:put(Type, N, RecAcc);
                    false -> RecAcc
                end
            end, Records, NewCounts),

    %% (b) record total: contagem total nova ultrapassa o recorde total.
    NewRecTotal =
        case NewTotal > RecTotal of
            true  -> ss_pubsub:publish({record, total, NewTotal}), NewTotal;
            false -> RecTotal
        end,

    State#{counts := NewCounts, records := NewRecords, record_total := NewRecTotal}.

%% Constrói #{Type => Nº online} a partir da tabela ETS de online.
current_counts() ->
    ets:foldl(
        fun({_Id, Type, _Pid}, Acc) ->
            maps:update_with(Type, fun(N) -> N + 1 end, 1, Acc)
        end, #{}, ?ONLINE).

%%====================================================================
%% Auxiliares
%%====================================================================

now_ms() ->
    erlang:system_time(millisecond).

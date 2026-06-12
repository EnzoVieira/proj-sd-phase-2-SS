%%%-------------------------------------------------------------------
%%% ss_state — Estado local da zona: online e atividade. Possui duas ETS:
%%%   ss_online   : {DeviceId, Type, Pid}    -> dispositivos online
%%%   ss_activity : {DeviceId, LastEventMs}  -> último evento de cada um
%%% online = autenticado e com ligação viva; ativo = evento nos últimos 60s.
%%%-------------------------------------------------------------------
-module(ss_state).
-behaviour(gen_server).

-export([start_link/0,
         mark_online/3, mark_event/1,
         count_online/0, count_online_by_type/1, is_online/1, count_active/0,
         online_map/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(ONLINE,   ss_online).
-define(ACTIVITY, ss_activity).
-define(ACTIVE_WINDOW_MS, 60000).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Via gen_server porque é ele que cria o monitor sobre o processo da ligação.
mark_online(DeviceId, Type, Pid) ->
    gen_server:call(?MODULE, {mark_online, DeviceId, Type, Pid}).

%% Escrita direta na ETS (eventos podem ser muito frequentes).
mark_event(DeviceId) ->
    ets:insert(?ACTIVITY, {DeviceId, now_ms()}),
    ok.

%% Queries: leituras diretas na ETS (concorrentes).

count_online() ->
    ets:info(?ONLINE, size).

count_online_by_type(Type) ->
    ets:select_count(?ONLINE, [{{'_', Type, '_'}, [], [true]}]).

is_online(DeviceId) ->
    ets:member(?ONLINE, DeviceId).

count_active() ->
    Threshold = now_ms() - ?ACTIVE_WINDOW_MS,
    ets:select_count(?ACTIVITY, [{{'_', '$1'}, [{'>=', '$1', Threshold}], [true]}]).

%% Mapa #{DeviceId => Tipo} dos online (usado pelo ss_cluster).
online_map() ->
    ets:foldl(fun({Id, Type, _Pid}, Acc) -> maps:put(Id, Type, Acc) end,
              #{}, ?ONLINE).

%%====================================================================
%% Callbacks
%%====================================================================

init([]) ->
    ets:new(?ONLINE,   [named_table, public, set, {read_concurrency, true}]),
    ets:new(?ACTIVITY, [named_table, public, set,
                        {read_concurrency, true}, {write_concurrency, true}]),
    %% monitors: Pid->{DeviceId,Ref}; counts/records: snapshot e máximos por tipo.
    {ok, #{monitors => #{}, counts => #{}, records => #{}, record_total => 0}}.

handle_call({mark_online, DeviceId, Type, Pid}, _From, State) ->
    Ref = erlang:monitor(process, Pid),
    ets:insert(?ONLINE, {DeviceId, Type, Pid}),
    Monitors = maps:get(monitors, State),
    State1 = State#{monitors := maps:put(Pid, {DeviceId, Ref}, Monitors)},
    {reply, ok, detect_and_publish(State1)}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Monitor: ao morrer o processo da ligação, marca o dispositivo offline.
handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    Monitors = maps:get(monitors, State),
    case maps:find(Pid, Monitors) of
        {ok, {DeviceId, _MonRef}} ->
            %% Só remove se a entrada ainda for desta ligação (evita apagar reconexão).
            State1 =
                case ets:lookup(?ONLINE, DeviceId) of
                    [{DeviceId, _Type, Pid}] ->
                        ets:delete(?ONLINE, DeviceId),
                        io:format("[ss_state] ~s ficou offline~n", [DeviceId]),
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

%% Compara o online atual com o snapshot anterior, publica ocorrências no
%% ss_pubsub e devolve o estado atualizado (novo snapshot + recordes).
detect_and_publish(State) ->
    NewCounts = current_counts(),
    NewTotal  = maps:fold(fun(_, V, A) -> A + V end, 0, NewCounts),
    OldCounts = maps:get(counts, State),
    Records   = maps:get(records, State),
    RecTotal  = maps:get(record_total, State),

    %% type_empty: tipos que tinham online (>0) e agora têm 0.
    maps:foreach(
        fun(Type, OldN) ->
            case OldN > 0 andalso maps:get(Type, NewCounts, 0) =:= 0 of
                true  -> ss_pubsub:publish({type_empty, Type});
                false -> ok
            end
        end, OldCounts),

    %% record por tipo: contagem nova ultrapassa o recorde desse tipo.
    NewRecords =
        maps:fold(
            fun(Type, N, RecAcc) ->
                case N > maps:get(Type, RecAcc, 0) of
                    true  -> ss_pubsub:publish({record, Type, N}),
                             maps:put(Type, N, RecAcc);
                    false -> RecAcc
                end
            end, Records, NewCounts),

    %% record total: contagem total nova ultrapassa o recorde total.
    NewRecTotal =
        case NewTotal > RecTotal of
            true  -> ss_pubsub:publish({record, total, NewTotal}), NewTotal;
            false -> RecTotal
        end,

    State#{counts := NewCounts, records := NewRecords, record_total := NewRecTotal}.

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

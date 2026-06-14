# Servidor de Sessão (SS) — Erlang

Servidor de Sessão da plataforma de processamento de séries temporais (Sistemas
Distribuídos, 2ª etapa). Implementado em **Erlang/OTP**, comunica com clientes por **TCP**
com mensagens **JSON delimitadas por `\n`**, e replica o estado entre nós com **CRDTs
state-based** (eventual consistency, tolerante a faltas).

> Âmbito deste README: o **SS**. Os servidores SA (agregação) e AST/DHT (armazenamento) são em
> Java e vivem em `../sa` e `../dht` no mesmo repositório; o SS **integra** com eles
> (reencaminha agregações para o SA e ingere eventos no DHT). Para o cenário **E2E completo**
> (DHT + SA + 2 nós SS), ver o [README da raiz](../README.md).

---

## Requisitos

- **Erlang/OTP 24+** (testado em OTP 24)
- **make**
- **git** — para obter o **chumak** (ZeroMQ em Erlang puro), usado no transporte SS↔SS.

Verificar: `erl -version` e `make --version`.

### Dependência: chumak (0MQ)

O chumak é "vendorizado" em `deps/chumak` (não precisa de `libzmq` em C nem de `rebar3`).
Se a pasta não existir (ex.: após clonar este repositório), obtém-no com:

```bash
git clone https://github.com/zeromq/chumak.git deps/chumak
```

O `make` compila-o automaticamente para `deps/chumak/ebin`. **Em runtime, inclui sempre
`-pa deps/chumak/ebin`** no comando `erl` (ver exemplos abaixo).

---

## Estrutura

```
src/        módulos do servidor
  ss_app, ss_sup        aplicação OTP + supervisor
  ss_tcp                acceptor TCP (1 processo por ligação)
  ss_conn               trata uma ligação (dispatch JSON + estado de sessão)
  ss_registry           credenciais (devices/consumers) — gen_server
  ss_state              estado local: online/ativo (ETS + monitor)
  ss_pubsub             notificações pub/sub (push para consumidores)
  ss_crdt               estrutura replicável + merge (puro)
  ss_cluster            réplica do estado global + gossip entre nós
  ss_gossip             transporte de gossip via 0MQ/chumak (SS↔SS)
  ss_sa_client          cliente do Servidor de Agregação (proxy de agregações)
  ss_dht_client         cliente do DHT/AST (ingestão de eventos)
  ss_json               codec JSON
test/
  ss_client             cliente de teste (usar na shell)
  ss_crdt_tests         testes EUnit do CRDT
  ss_dist_demo          demos distribuídos automáticos
priv/
  devices.json          dispositivos pré-registados (id/password/type)
  consumers.json        consumidores pré-registados (user/password)
ebin/                   .beam compilados (gerado pelo make)
```

---

## Compilar

```bash
make          # compila o chumak (1x), src/ e test/ -> ebin/, e gera ebin/ss.app
make clean    # apaga os .beam e o ss.app (NÃO apaga o chumak)
make clean-deps  # apaga o ebin do chumak (recompila no próximo make)
```

> **Nota operacional (portas):** se um nó Erlang anterior ficar vivo, a porta fica ocupada
> (`eaddrinuse`). Termina sempre com `halt().` na shell, ou força com `pkill -9 -f beam.smp`.
>
> **No macOS**, a porta **7000** é usada pelo *AirPlay Receiver* — por isso o gossip usa por
> omissão a **7100**. (Podes desligar o AirPlay Receiver em Definições → Geral → AirDrop e
> Handoff, mas não é preciso.)

---

## Credenciais de exemplo (em `priv/`)

| Dispositivos (id / password / type) | Consumidores (user / password) |
|---|---|
| `car-001` / `pass1` / car   | `alice` / `alice123` |
| `car-002` / `pass2` / car   | `bob`   / `bob123`   |
| `drone-001` / `pass3` / drone | |
| `drone-002` / `pass4` / drone | |
| `truck-001` / `pass5` / truck | |

---

## A) Testar um nó único (interativo)

Arranca o servidor numa shell Erlang:

```bash
make
erl -pa ebin -pa deps/chumak/ebin
```

Na shell (`1>`):

```erlang
application:start(ss).

%% Produtor (dispositivo)
P = ss_client:connect().                                   % liga a localhost:9000
ss_client:auth_producer(P, <<"car-001">>, <<"pass1">>).    % => #{<<"status">> => <<"ok">>}
ss_client:event(P, <<"alarme">>, #{<<"speed">> => 40}).    % envia um evento

%% Consumidor (queries globais)
C = ss_client:connect().
ss_client:auth_consumer(C, <<"alice">>, <<"alice123">>).
ss_client:online_count(C).                 % nº online
ss_client:online_count_by_type(C, <<"car">>).
ss_client:is_online(C, <<"car-001">>).
ss_client:active_count(C).                 % nº ativos (evento nos últimos 60s)

ss_client:close(P).                        % o produtor sai de online (monitor)
```

Para sair: `halt().`

### Notificações automáticas (consumidor reativo)

```erlang
C = ss_client:connect(),
ss_client:auth_consumer(C, <<"alice">>, <<"alice123">>),
ss_client:subscribe(C, <<"record">>, <<"any">>),       % novo recorde de online total
ss_client:subscribe(C, <<"type_empty">>, <<"car">>),   % "car" ficou sem online
ss_client:subscribe(C, <<"percentage">>, <<"any">>),   % percentagem da zona vs total
ss_client:listen(C).      % a partir daqui as notificações imprimem-se sozinhas
```

(Liga depois um produtor noutra shell para disparar as notificações — ver secção C.)

---

## B) Testar com `nc` (JSON à mão)

```bash
nc localhost 9000
```
Escreve uma linha JSON por comando (no macOS não uses a flag `-q`):

```json
{"cmd":"auth_producer","device":"car-001","password":"pass1"}
{"cmd":"event","type":"alarme","timestamp":1700000000000,"speed":40}
```
ou como consumidor:
```json
{"cmd":"auth_consumer","user":"alice","password":"alice123"}
{"cmd":"online_count"}
{"cmd":"subscribe","event":"record","type":"any"}
```

---

## C) Vários terminais no mesmo servidor

1. **Terminal 1** — servidor + consumidor a escutar:
   ```bash
   erl -pa ebin -pa deps/chumak/ebin
   ```
   ```erlang
   application:start(ss).
   C = ss_client:connect(),
   ss_client:auth_consumer(C, <<"alice">>, <<"alice123">>),
   ss_client:subscribe(C, <<"record">>, <<"any">>),
   ss_client:listen(C).
   ```

2. **Terminal 2** — um produtor (NÃO arranques a app outra vez; o servidor é só no T1):
   ```bash
   erl -pa ebin -pa deps/chumak/ebin
   ```
   ```erlang
   P = ss_client:connect(),
   ss_client:auth_producer(P, <<"car-001">>, <<"pass1">>).  % T1 imprime: record any 1
   ss_client:close(P).
   ```

---

## D) Testes automáticos (EUnit) do CRDT

Prova que o `merge` é comutativo, associativo e idempotente:

```bash
make
erl -pa ebin -noshell -eval "eunit:test(ss_crdt_tests, [verbose])" -s init stop
```

---

## E) Sistema distribuído — demo automático

Arranca 2 nós (zonas `zonea`/`zoneb`, clientes 9001/9002, gossip 0MQ 7001/7002)
automaticamente. A **replicação entre nós é feita por 0MQ/chumak** (a distribuição Erlang
serve só para o harness arrancar os nós):

```bash
make

# Estado GLOBAL replicado + tolerância a faltas (mata um nó no fim)
erl -sname master -setcookie sscookie -pa ebin -pa deps/chumak/ebin \
    -noshell -eval 'ss_dist_demo:run()' -s init stop

# Ocorrências de PERCENTAGEM (zona vs total global)
erl -sname master -setcookie sscookie -pa ebin -pa deps/chumak/ebin \
    -noshell -eval 'ss_dist_demo:run_pct()' -s init stop
```

> Podes ignorar `=ERROR REPORT ... zmq listen error`: são transitórios do chumak (um SUB a
> ligar antes do PUB do outro nó estar pronto, ou a queda da ligação quando um nó morre).

---

## F) Sistema distribuído — manual (vários nós, vários terminais)

Como o gossip agora é por **0MQ**, os nós **já NÃO precisam de distribuição Erlang**
(`-sname`/`-setcookie`): são apenas dois `erl` normais, cada um com a sua porta de cliente e
a sua configuração de gossip.

1. **Terminal 1 — nó A (zona A, cliente 9001, gossip PUB 7001 → liga ao 7002 do B):**
   ```bash
   erl -pa ebin -pa deps/chumak/ebin
   ```
   ```erlang
   application:load(ss),
   application:set_env(ss, zone, <<"zonea">>),
   application:set_env(ss, port, 9001),
   application:set_env(ss, gossip_port, 7001),
   application:set_env(ss, gossip_peers, [{"127.0.0.1", 7002}]),
   application:start(ss).
   ```

2. **Terminal 2 — nó B (zona B, cliente 9002, gossip PUB 7002 → liga ao 7001 do A):**
   ```bash
   erl -pa ebin -pa deps/chumak/ebin
   ```
   ```erlang
   application:load(ss),
   application:set_env(ss, zone, <<"zoneb">>),
   application:set_env(ss, port, 9002),
   application:set_env(ss, gossip_port, 7002),
   application:set_env(ss, gossip_peers, [{"127.0.0.1", 7001}]),
   application:start(ss).
   ```

3. **Terminal 3 — clientes** (um cliente é só TCP):
   ```bash
   erl -pa ebin -pa deps/chumak/ebin
   ```
   ```erlang
   %% produtor na zona A
   PA = ss_client:connect(9001),
   ss_client:auth_producer(PA, <<"car-001">>, <<"pass1">>).
   %% produtor na zona B
   PB = ss_client:connect(9002),
   ss_client:auth_producer(PB, <<"drone-001">>, <<"pass3">>).
   %% consumidor na zona A vê o estado GLOBAL (inclui o drone da zona B)
   CA = ss_client:connect(9001),
   ss_client:auth_consumer(CA, <<"alice">>, <<"alice123">>),
   ss_client:online_count(CA).                    % => 2
   ss_client:is_online(CA, <<"drone-001">>).      % => true
   ```

> A convergência é **eventual**: as queries globais podem demorar até um *tick* de *gossip*
> (`gossip_interval`, por omissão 500 ms) a refletir mudanças noutras zonas.

---

## G) E2E distribuído com o SA + DHT (2 zonas)

O cenário completo que junta os três componentes (**2 nós DHT** + **1 SA** + **2 nós SS** em
zonas `north`/`south`) está documentado no [README da raiz](../README.md#e2e--pipeline-completo-distribuído-2-zonas),
onde os caminhos (`dht/App`, `sa`, `ss`) são relativos à raiz do repositório.

---

## Protocolo (referência rápida)

Pedido = uma linha JSON com campo `cmd`. Respostas:
`{"status":"ok"}` · `{"status":"ok","result":{...}}` · `{"error":"<msg>","code":<N>}`.

| `cmd` | Papel | Campos | Resultado |
|---|---|---|---|
| `auth_producer` | dispositivo | `device`, `password` | ok |
| `auth_consumer` | consumidor | `user`, `password` | ok |
| `event` | dispositivo | `type`, `timestamp`(ms) + campos índice | ok |

`event` também é **ingerido no DHT** em background (carimbando `zone` = zona do nó; `timestamp`
convertido para segundos; campos como strings), indexado sob **cada** campo índice configurado
(`dht_index_fields`) de que o evento tem valor. Se o DHT estiver em baixo, o dispositivo recebe
`ok` na mesma (a ingestão é *best-effort* e o erro é só registado).
| `online_count` | consumidor | — | `{"online":N}` |
| `online_count_by_type` | consumidor | `type` | `{"online":N}` |
| `is_online` | consumidor | `device` | `{"online":bool}` |
| `active_count` | consumidor | — | `{"active":N}` |
| `subscribe` / `unsubscribe` | consumidor | `event` + `type` | ok |
| `aggregate` | consumidor | `type`, `minDay`, `maxDay`, `indexField`, `indexValue`, `k2`, `k3` | resultado do SA |

`aggregate` é reencaminhado para o **SA** (`type` ∈ `COUNT/SUM/MAX/MIN/SUM_PRODUCT`; `k2` é o
campo numérico, `k3` o 2º fator para `SUM_PRODUCT`). Se o SA estiver indisponível: `code 502`.

`event` do `subscribe`: `type_empty` (com `type`), `record` (`type` ou `"any"`),
`percentage` (`type` ignorado).

**Notificações** (empurradas para o consumidor):
```json
{"notify":"type_empty","type":"car"}
{"notify":"record","type":"car","value":5}
{"notify":"percentage","direction":"up","threshold":50,"value":66}
```

---

## Configuração (application env da app `ss`)

| Chave | Default | Descrição |
|---|---|---|
| `port` | 9000 | porta TCP dos clientes |
| `zone` | `undefined` (→ nome do nó) | id da zona deste nó (binary) |
| `gossip_port` | 7100 | porta onde o PUB 0MQ deste nó faz *bind* |
| `gossip_peers` | `[]` | `[{Host, Port}]` dos PUB dos vizinhos |
| `gossip_interval` | 500 | período do gossip/anti-entropia (ms) |
| `sa_host` | `"localhost"` | host do Servidor de Agregação por omissão (proxy `aggregate`) |
| `sa_port` | 9090 | porta do Servidor de Agregação por omissão |
| `sa_zones` | `#{}` | mapa `zona => {Host, Port}`; agregações com `indexField="zone"` vão para o SA dessa zona (localidade); zona não mapeada ou outro `indexField` → SA por omissão |
| `dht_host` | `"localhost"` | host do nó DHT/AST (ingestão de `event`) |
| `dht_port` | 7878 | porta do nó DHT/AST |
| `dht_index_fields` | `[<<"zone">>,<<"sensor">>,<<"type">>]` | campos índice da ingestão (⊆ config do nó DHT); 1 INGEST por campo presente no evento |

# Servidor de Sessão (SS) — Erlang

Servidor de Sessão da plataforma de processamento de séries temporais (Sistemas
Distribuídos, 2ª etapa). Implementado em **Erlang/OTP**, comunica com clientes por **TCP**
com mensagens **JSON delimitadas por `\n`**, e replica o estado entre nós com **CRDTs
state-based** (eventual consistency, tolerante a faltas).

> Âmbito: apenas o **SS**. Os servidores SA (agregação) e AST (armazenamento) são em Java e
> ficam fora deste repositório.

---

## Requisitos

- **Erlang/OTP 24+** (testado em OTP 24)
- **make**

Verificar: `erl -version` e `make --version`.

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
make          # compila src/ e test/ -> ebin/ e gera ebin/ss.app
make clean    # apaga os .beam e o ss.app
```

> **Nota operacional:** se um nó Erlang anterior ficar vivo, a porta (9000/9001/...) fica
> ocupada (`eaddrinuse`). Termina sempre com `halt().` na shell, ou força com
> `pkill -9 -f beam.smp`.

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
erl -pa ebin
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
   erl -pa ebin
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
   erl -pa ebin
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

Arranca 2 nós (zonas `zonea`/`zoneb`, portas 9001/9002) automaticamente:

```bash
make

# Estado GLOBAL replicado + tolerância a faltas (mata um nó no fim)
erl -sname master -setcookie sscookie -pa ebin -noshell -eval 'ss_dist_demo:run()' -s init stop

# Ocorrências de PERCENTAGEM (zona vs total global)
erl -sname master -setcookie sscookie -pa ebin -noshell -eval 'ss_dist_demo:run_pct()' -s init stop
```

---

## F) Sistema distribuído — manual (vários nós, vários terminais)

O *short hostname* desta máquina é `macbook-enzo` (vê o teu com `hostname -s` e ajusta os
nomes dos nós `nome@host`).

1. **Terminal 1 — nó A (zona A, porta 9001):**
   ```bash
   erl -sname ssa -setcookie sscookie -pa ebin
   ```
   ```erlang
   application:load(ss),
   application:set_env(ss, zone, zonea),
   application:set_env(ss, peers, ['ssb@macbook-enzo']),
   application:set_env(ss, port, 9001),
   application:start(ss).
   ```

2. **Terminal 2 — nó B (zona B, porta 9002):**
   ```bash
   erl -sname ssb -setcookie sscookie -pa ebin
   ```
   ```erlang
   application:load(ss),
   application:set_env(ss, zone, zoneb),
   application:set_env(ss, peers, ['ssa@macbook-enzo']),
   application:set_env(ss, port, 9002),
   application:start(ss),
   net_adm:ping('ssa@macbook-enzo').      % garante que os nós se veem
   ```

3. **Terminal 3 — clientes** (um cliente é só TCP, não precisa de ser nó distribuído):
   ```bash
   erl -pa ebin
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

## Protocolo (referência rápida)

Pedido = uma linha JSON com campo `cmd`. Respostas:
`{"status":"ok"}` · `{"status":"ok","result":{...}}` · `{"error":"<msg>","code":<N>}`.

| `cmd` | Papel | Campos | Resultado |
|---|---|---|---|
| `auth_producer` | dispositivo | `device`, `password` | ok |
| `auth_consumer` | consumidor | `user`, `password` | ok |
| `event` | dispositivo | `type`, `timestamp`(ms) + campos índice | ok |
| `online_count` | consumidor | — | `{"online":N}` |
| `online_count_by_type` | consumidor | `type` | `{"online":N}` |
| `is_online` | consumidor | `device` | `{"online":bool}` |
| `active_count` | consumidor | — | `{"active":N}` |
| `subscribe` / `unsubscribe` | consumidor | `event` + `type` | ok |

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
| `zone` | `undefined` (→ `node()`) | id da zona deste nó |
| `peers` | `[]` | nós SS vizinhos (gossip) |
| `gossip_interval` | 500 | período do gossip (ms) |

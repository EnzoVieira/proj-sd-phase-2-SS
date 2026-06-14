# Plataforma de Séries Temporais IoT — Fase 2 (Sistemas Distribuídos)

Sistema distribuído e descentralizado para ingestão, armazenamento e consulta de séries
temporais de dispositivos IoT. Este repositório reúne os **três componentes** da 2ª fase,
integrados:

```
                 TCP + JSON (\n)                  TCP + JSON              TCP + JSON
   clientes  ───────────────────►   SS (Erlang)  ──────────►  SA (Java)  ──────────►  DHT/AST (Java)
 (devices/                          Serv. Sessão              Agregação              armazenamento
  consumers)                             │  ▲                                         distribuído
                                         │  │ gossip 0MQ (CRDT state-based)            (consistent
                                         ▼  │  entre nós SS (1 por zona)                hashing)
                                       outros nós SS
```

| Componente | Pasta | Linguagem | Papel |
|---|---|---|---|
| **SS** — Servidor de Sessão | [`ss/`](ss/) | Erlang/OTP | Registo/autenticação de clientes, estado global online/ativo (replicado por CRDTs sobre 0MQ), notificações pub/sub, *proxy* de agregações para o SA e ingestão de eventos no DHT. |
| **SA** — Servidor de Agregação | [`sa/`](sa/) | Java 17 / Maven | Calcula agregações (`COUNT/SUM/MAX/MIN/SUM_PRODUCT`) lendo o DHT. |
| **DHT/AST** — Armazenamento | [`dht/`](dht/) | Java 11 / Maven | Armazenamento distribuído de séries temporais (anel por *consistent hashing*, persistência em disco, indexação por vários campos). |

> **Autoria.** O **SS** foi desenvolvido neste repositório. O **SA** e o **DHT/AST** são da
> autoria do colega João Duarte — vendorizados (cópia do código-fonte, sem alterações) a partir
> de [github.com/jpmduarte/SA](https://github.com/jpmduarte/SA) e
> [github.com/jpmduarte/DHT](https://github.com/jpmduarte/DHT). **Não editar `sa/` nem `dht/`.**

---

## Requisitos

- **Erlang/OTP 24+** e **make** (para o SS).
- **Java 17+** e **Maven** (para o SA e o DHT).
- **git** — o SS usa o **chumak** (ZeroMQ em Erlang puro), vendorizado em `ss/deps/chumak`
  (ver [`ss/README.md`](ss/README.md) se a pasta não existir após o clone).

No **macOS** o separador de classpath é `:` (nos exemplos Java abaixo); a porta **7000** é do
*AirPlay Receiver*, por isso o gossip do SS usa por omissão a **7100**.

---

## Compilar tudo

A partir da raiz:

```bash
make            # compila os 3 componentes (SS via make, SA e DHT via mvn)
make ss         # só o SS    (equivale a: make -C ss)
make sa         # só o SA    (mvn -C sa)
make dht        # só o DHT   (mvn -C dht/App)
make clean      # limpa os 3
```

Build de cada componente individualmente:

```bash
(cd ss      && make)              # -> ss/ebin/*.beam
(cd sa      && mvn -q clean compile)
(cd dht/App && mvn -q clean compile)
```

Detalhes, testes e cenários do **SS** (nó único, `nc`, multi-nó, EUnit do CRDT): ver
[`ss/README.md`](ss/README.md).

---

## E2E — pipeline completo distribuído (2 zonas)

Cenário mínimo que junta tudo: **2 nós DHT** (armazenamento distribuído), **1 SA** (agregação)
e **2 nós SS** em zonas diferentes (`north`/`south`) que replicam o estado online por 0MQ e
reencaminham agregações para o SA. Usa 6 terminais, **a partir da raiz**.

**Passo 0 — build + classpaths (uma vez):**
```bash
make ss
(cd dht/App && mvn -q clean compile dependency:build-classpath -Dmdep.outputFile=cp.txt)
(cd sa      && mvn -q clean compile dependency:build-classpath -Dmdep.outputFile=cp.txt)
```

**T1 — DHT node-1 (7878):**
```bash
cd dht/App
java -cp "target/classes:$(cat cp.txt)" pt.ua.NodeServerMain node-1 7878 2 zone,sensor,type node-1:localhost:7878,node-2:localhost:7879
```
**T2 — DHT node-2 (7879):**
```bash
cd dht/App
java -cp "target/classes:$(cat cp.txt)" pt.ua.NodeServerMain node-2 7879 2 zone,sensor,type node-1:localhost:7878,node-2:localhost:7879
```
**Popular o DHT** (uma vez, com os 2 nós no ar) — ou usa o nosso SS (T4–T6) para ingerir eventos:
```bash
cd dht/App
java -cp "target/classes:$(cat cp.txt)" pt.ua.FakeSSMain localhost 7878
```

**T3 — SA (9090):**
```bash
cd sa
java -cp "target/classes:$(cat cp.txt)" pt.ua.SaMain north 9090 localhost 9091
```

**T4 — nó SS `north` (clientes 9001, gossip 7001):**
```bash
cd ss && erl -pa ebin -pa deps/chumak/ebin
```
```erlang
application:load(ss),
application:set_env(ss, zone, <<"north">>),
application:set_env(ss, port, 9001),
application:set_env(ss, gossip_port, 7001),
application:set_env(ss, gossip_peers, [{"127.0.0.1", 7002}]),
application:set_env(ss, sa_port, 9090),
application:start(ss).
```
**T5 — nó SS `south` (clientes 9002, gossip 7002):**
```bash
cd ss && erl -pa ebin -pa deps/chumak/ebin
```
```erlang
application:load(ss),
application:set_env(ss, zone, <<"south">>),
application:set_env(ss, port, 9002),
application:set_env(ss, gossip_port, 7002),
application:set_env(ss, gossip_peers, [{"127.0.0.1", 7001}]),
application:set_env(ss, sa_port, 9090),
application:start(ss).
```

**T6 — clientes:**
```bash
cd ss && erl -pa ebin -pa deps/chumak/ebin
```
```erlang
%% dispositivos em zonas diferentes (estado online replicado por 0MQ entre as zonas)
PA = ss_client:connect(9001), ss_client:auth_producer(PA, <<"car-001">>, <<"pass1">>).
PB = ss_client:connect(9002), ss_client:auth_producer(PB, <<"drone-001">>, <<"pass3">>).

%% consumidor no nó north vê o estado GLOBAL (inclui o dispositivo do nó south)
CN = ss_client:connect(9001), ss_client:auth_consumer(CN, <<"alice">>, <<"alice123">>).
ss_client:online_count(CN).                  % => 2  (replicado entre zonas, via gossip)
ss_client:is_online(CN, <<"drone-001">>).    % => true

%% agregações via SS -> SA -> DHT, para as 2 zonas de dados
D = #{<<"minDay">> => <<"2026-06-01">>, <<"maxDay">> => <<"2026-06-09">>,
      <<"indexField">> => <<"zone">>}.
ss_client:aggregate(CN, D#{<<"type">> => <<"SUM">>, <<"indexValue">> => <<"north">>, <<"k2">> => <<"temperature">>}).
CS = ss_client:connect(9002), ss_client:auth_consumer(CS, <<"bob">>, <<"bob123">>).
ss_client:aggregate(CS, D#{<<"type">> => <<"SUM">>, <<"indexValue">> => <<"south">>, <<"k2">> => <<"temperature">>}).
```

> **Roteamento por zona (localidade).** Para correr **um SA por zona**, arranca um 2º SA
> (`cd sa && java -cp "target/classes:$(cat cp.txt)" pt.ua.SaMain south 9092 localhost 9091`) e,
> em cada nó SS, em vez de `sa_port`, define o mapa
> `application:set_env(ss, sa_zones, #{<<"north">> => {"localhost",9090}, <<"south">> => {"localhost",9092}})`.
> Agregações com `indexField="zone"` passam a ir para o SA da zona pedida; zonas não mapeadas
> (ou outros `indexField`) caem no SA por omissão (`sa_host`/`sa_port`).

**Critério de sucesso:** `online_count` = 2 a partir de qualquer nó SS (estado distribuído), e
cada agregação devolve os mesmos números que o cliente de referência do SA para a respetiva
zona (`cd sa && java -cp "target/classes:$(cat cp.txt)" pt.ua.FakeConsumerSS localhost 9090 north` / `south`).

**Limpeza:** `halt().` em cada `erl`; Ctrl+C nos terminais Java; à força:
`pkill -9 -f 'pt.ua.'` e `pkill -9 -f beam.smp`.

---

## Estrutura do repositório

```
ss/            Servidor de Sessão (Erlang) — ver ss/README.md
sa/            Servidor de Agregação (Java/Maven)        [código do colega — não editar]
dht/App/       Armazenamento DHT/AST (Java/Maven)         [código do colega — não editar]
enunciado.pdf  enunciado da 2ª fase
README.md      este ficheiro
```

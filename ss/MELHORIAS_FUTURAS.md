# Melhorias futuras (Servidor de Sessão)

Lista de melhorias possíveis ao SS, **não implementadas de propósito** para manter o projeto
simples e percetível nesta fase. Servem de roteiro caso seja preciso escalar, aumentar a
precisão ou a robustez. O projeto mantém-se sempre com **CRDTs state-based** (CvRDT).

---

## 1. Delta-state CRDTs (delta-CvRDTs)

**Problema atual:** em cada *tick* de *gossip*, o `ss_cluster` envia o **estado global inteiro**
a todos os vizinhos (anti-entropia completa). Isto é robusto, mas a largura de banda cresce com
o número de dispositivos e zonas — desperdício quando pouco mudou.

**Melhoria:** enviar apenas as **partes que mudaram** (deltas) desde o último envio, mantendo a
semântica e a robustez do *state-based*. Periodicamente (ou ao reentrar um nó), faz-se um
*gossip* completo para garantir convergência; entre eles, só deltas.

**Impacto:** grande redução de tráfego; mesma garantia de convergência. É a evolução natural
quando o volume crescer.

---

## 2. Vetores de versão (version vectors) / múltiplos escritores por zona

**Simplificação atual:** assumimos **um único escritor por zona** (o nó dono). Por isso a
"versão" de cada zona é um simples contador totalmente ordenado, e o `merge` é só "fica com a
versão mais alta".

**Quando isto deixa de chegar:**
- Se quisermos **mais do que um nó SS por zona** (réplicas da mesma zona a escrever em paralelo).
- Se um dispositivo **migrar entre zonas** com concorrência real (online em duas zonas ao mesmo
  tempo durante uma reconexão).

**Melhoria:** usar **vetores de versão** (um contador por nó) para detetar atualizações
concorrentes, e resolver com uma estrutura adequada (ver ponto 3).

---

## 3. OR-Set para a pertença "online" (largar o pressuposto de escritor único)

**Atual:** o conjunto de online de uma zona é substituído em bloco (LWW por versão).

**Melhoria:** modelar o conjunto de dispositivos online como um **OR-Set** (*Observed-Remove
Set*), um CRDT clássico que suporta `add`/`remove` concorrentes sem conflitos, usando *tags*
únicas. Permite vários escritores e remoções concorrentes corretas, ao custo de manter
*tombstones* (e, portanto, de eventual **garbage collection** desses *tombstones*).

---

## 4. Deteção de falhas e expiração de zonas

**Trade-off atual:** quando um nó morre, a sua zona fica com o **último estado conhecido**
(*stale*) indefinidamente. As queries continuam a responder (disponibilidade), mas podem contar
como "online" dispositivos de um nó que já caiu.

**Melhoria:** *failure detection* — marcar uma zona como expirada/offline se não receber
*gossip* dela há mais de X segundos (ex.: guardar um *timestamp* do último *merge* por zona e,
no *tick*, descartar/zerar zonas "mortas"). Torna o "online" global mais preciso, ao custo de
poder marcar como offline um nó apenas temporariamente particionado (escolha
disponibilidade vs. precisão).

---

## 5. Registo dinâmico de clientes replicado por CRDT

**Atual:** as credenciais (dispositivos e consumidores) são **estáticas**, carregadas de
`priv/devices.json` e `priv/consumers.json` em cada nó no arranque.

**Melhoria (se o registo dinâmico for exigido):** permitir registar novos consumidores em tempo
real e **replicar esse registo** com um CRDT — por exemplo, um *grow-only map*
`user → password` (ou um OR-Map se for preciso remover). O mesmo padrão de `merge` que já temos
aplica-se. Cumpre à letra o "informação de registo de clientes tolerante a faltas".

---

## 6. Anti-entropia mais eficiente (digests / Merkle trees)

**Atual:** comparação implícita por reenvio do estado.

**Melhoria:** trocar primeiro **resumos** (*digests*/hashes por zona, ou *Merkle trees*) e só
transferir as zonas que diferem. Reduz tráfego sem perder a robustez. Combina bem com o ponto 1.

---

## 7. Precisão do "ativo" global

**Atual:** cada zona reporta uma **contagem** de ativos (evento nos últimos 60s), e o total é a
soma. É puramente temporal e ligeiramente *stale* entre *ticks*.

**Melhoria:** replicar o conjunto de ativos (com *timestamps*) em vez da contagem, para queries
mais precisas e para distinguir melhor "ativo" de "online"; ou usar uma janela deslizante mais
fina. Pesar contra o custo de tráfego.

---

## 8. Descoberta de membros (peer discovery)

**Atual:** a lista de `peers` é **estática** (configuração por nó).

**Melhoria:** descoberta dinâmica de nós (ex.: *seed nodes* + propagação da lista de membros,
ou um protocolo de *membership* tipo SWIM). Permite acrescentar/remover nós sem reconfigurar
todos.

---

## 9. Segurança

- Senhas estão em **texto simples** nos ficheiros e no protocolo. Melhorar com **hashing**
  (ex.: bcrypt/argon2) e comparação em tempo constante.
- Adicionar **TLS** nas ligações (cliente↔SS e SS↔SS).

---

## 10. Persistência e observabilidade

- **Persistir** o estado replicado (ETS → disco) para sobreviver a reinícios de um nó sem
  depender só do *gossip* dos outros.
- **Métricas/logs** estruturados (nº de online por zona, taxa de eventos, *lag* de *gossip*)
  para diagnóstico e para os testes de desempenho (Fase 6).

# DHT AST Demo

This project implements the AST/DHT service in Java using:

- Java NIO sockets for communication
- RxJava `Completable` / `Flowable` APIs between clients and AST nodes
- consistent hashing with virtual nodes
- disk persistence per sub-series
- in-memory retention of the current day plus at most `S` historical days
- configured index-field validation

## Requirements

- Java 11+
- Maven

## Build

From the repository root:

```powershell
cd App
mvn clean compile dependency:build-classpath -Dmdep.outputFile=cp.txt
```

The generated `cp.txt` is used by the commands below.

## Run The Demo (PowerShell)

Open four different PowerShell terminals. In every terminal, start inside the `App` directory:

```powershell
cd C:\Users\joaop\Desktop\proj_sd_fase2\DHT\App
$CP = "target/classes;" + (Get-Content cp.txt)
```

### Terminal 1: Start DHT Node 1

```powershell
mvn -q exec:java "-Dexec.mainClass=pt.ua.NodeServerMain" "-Dexec.args=node-1 7878 2 zone,sensor,type node-1:localhost:7878,node-2:localhost:7879"
```

### Terminal 2: Start DHT Node 2

```powershell
mvn -q exec:java "-Dexec.mainClass=pt.ua.NodeServerMain" "-Dexec.args=node-2 7879 2 zone,sensor,type node-1:localhost:7878,node-2:localhost:7879"
```

### Terminal 3: Run Fake SS

This sends sample events to node 1. The DHT may forward events to node 2 depending on the consistent-hashing owner.

```powershell
mvn -q exec:java "-Dexec.mainClass=pt.ua.FakeSSMain" "-Dexec.args=localhost 7878"
```

### Terminal 4: Run Fake SA (QUERY por dia único)

This queries sample sub-series through node 1.

```powershell
mvn -q exec:java "-Dexec.mainClass=pt.ua.FakeSAMain" "-Dexec.args=localhost 7878"
```

### Terminal 5: Run Simple SA Range (QUERY_RANGE)

This queries sub-series across a range of days (2026-06-01 to 2026-06-09) and computes aggregation.

```powershell
java -cp "$CP" pt.ua.SimpleSAMainRange localhost 7878
```

## Node Arguments

`NodeServerMain` expects:

```text
NodeServerMain <nodeId> <port> <maxHistoricalDaysInMemory> <indexFieldsCsv> [peer=nodeId:host:port,...]
```

Example:

```powershell
java -cp "$CP" pt.ua.NodeServerMain node-1 7878 2 zone,sensor,type node-1:localhost:7878,node-2:localhost:7879
```

Where:

- `nodeId`: unique DHT node identifier
- `port`: TCP port for the NIO socket server
- `maxHistoricalDaysInMemory`: value `S`; current day is always kept in memory, plus at most `S` historical days
- `indexFieldsCsv`: allowed index fields, for example `zone,sensor,type`
- `peer=...`: comma-separated DHT membership list

## Data Files

Each node persists sub-series under:

```text
App/data/<nodeId>/
```

Files are named by:

```text
<day>__<indexField>__<indexValue>.log
```

Each line is one JSON event.

## Stop The Demo

Press `Ctrl+C` in the node terminals.
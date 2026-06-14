package pt.ua;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.Flowable;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DhtNodeServer {
    private final NodeState state;
    private final NodeInfo self;
    private final PeerTable peers;
    private final ReactiveNodeClient client;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final ExecutorService connectionPool = Executors.newCachedThreadPool();

    public DhtNodeServer(NodeState state, NodeInfo self, PeerTable peers) {
        this.state = state;
        this.self = self;
        this.peers = peers;
        this.client = new ReactiveNodeClient();
    }

    public void start() {
        System.out.println("[server] Starting DHT node=" + self.getNodeId() + " on port=" + self.getPort());

        try (ServerSocketChannel server = ServerSocketChannel.open()) {
            server.bind(new InetSocketAddress(self.getPort()));
            while (true) {
                SocketChannel socket = server.accept();
                connectionPool.submit(() -> handleConnection(socket));
            }
        } catch (IOException e) {
            throw new RuntimeException("DHT node failed", e);
        }
    }

    private void handleConnection(SocketChannel socket) {
        String requestId = UUID.randomUUID().toString();
        try (SocketChannel channel = socket;
                BufferedReader reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8));
                BufferedWriter writer = new BufferedWriter(Channels.newWriter(channel, StandardCharsets.UTF_8))) {
            try {
                String line = reader.readLine();
                if (line == null) {
                    return;
                }

                DhtProtocol.Request request = mapper.readValue(line, DhtProtocol.Request.class);
                if (DhtProtocol.INGEST.equals(request.op)) {
                    handleIngest(request);
                    writeResponse(writer, DhtProtocol.Response.ack(requestId));
                } else if (DhtProtocol.QUERY.equals(request.op)) {
                    handleQuery(request, requestId, writer);
                } else if (DhtProtocol.QUERY_RANGE.equals(request.op)) {
                    handleQueryRange(request, requestId, writer); // novo
                } else {
                    writeResponse(writer, DhtProtocol.Response.error(requestId, "Unknown operation: " + request.op));
                }
            } catch (IllegalArgumentException e) {
                System.err.println("[server] request validation failed: " + e.getMessage());
                writeResponse(writer, DhtProtocol.Response.error(requestId, e.getMessage()));
            } catch (Exception e) {
                System.err.println("[server] request failed: " + e.getMessage());
                writeResponse(writer, DhtProtocol.Response.error(requestId, e.getMessage()));
            }
        } catch (Exception e) {
            System.err.println("[server] connection failed: " + e.getMessage());
        }
    }

    private void handleQueryRange(DhtProtocol.Request request, String requestId, BufferedWriter writer) {
        // Validação de minDay
        if (request.minDay == null || request.minDay.isBlank()) {
            writeResponse(writer, DhtProtocol.Response.error(requestId, "Missing or empty minDay"));
            return;
        }
        LocalDate minDay;
        try {
            minDay = LocalDate.parse(request.minDay);
        } catch (Exception e) {
            writeResponse(writer, DhtProtocol.Response.error(requestId, "Invalid minDay format: " + request.minDay));
            return;
        }

        // Validação de maxDay
        if (request.maxDay == null || request.maxDay.isBlank()) {
            writeResponse(writer, DhtProtocol.Response.error(requestId, "Missing or empty maxDay"));
            return;
        }
        LocalDate maxDay;
        try {
            maxDay = LocalDate.parse(request.maxDay);
        } catch (Exception e) {
            writeResponse(writer, DhtProtocol.Response.error(requestId, "Invalid maxDay format: " + request.maxDay));
            return;
        }

        if (minDay.isAfter(maxDay)) {
            writeResponse(writer, DhtProtocol.Response.error(requestId, "minDay must be before or equal to maxDay"));
            return;
        }

        // Validação de indexField
        if (request.indexField == null || request.indexField.isBlank()) {
            writeResponse(writer, DhtProtocol.Response.error(requestId, "Missing or empty indexField"));
            return;
        }
        String indexField = request.indexField;

        // Validação de indexValue
        if (request.indexValue == null || request.indexValue.isBlank()) {
            writeResponse(writer, DhtProtocol.Response.error(requestId, "Missing or empty indexValue"));
            return;
        }
        String indexValue = request.indexValue;

        state.validateIndexField(indexField);

        // Loop por cada dia no intervalo
        Flowable<Event> rangeStream = Flowable.concat(
                Flowable.fromIterable(
                        java.util.stream.Stream.iterate(minDay, d -> !d.isAfter(maxDay), d -> d.plusDays(1))
                                .collect(java.util.stream.Collectors.toList()))
                        .map(d -> {
                            NodeInfo owner = ownerFor(d, indexField, indexValue);
                            if (owner.getNodeId().equals(self.getNodeId())) {
                                return state.streamSubSeries(d, indexField, indexValue);
                            } else {
                                return client.streamSubSeries(owner, d, indexField, indexValue);
                            }
                        }));

        rangeStream.blockingForEach(event -> {
            writeResponse(writer, DhtProtocol.Response.event(requestId, event));
        });
        writeResponse(writer, DhtProtocol.Response.complete(requestId));
    }

    private void handleIngest(DhtProtocol.Request request) {
        // Validação explícita de event
        if (request.event == null) {
            throw new IllegalArgumentException("Missing event");
        }
        Event event = request.event;

        // Validação de indexField
        if (request.indexField == null || request.indexField.isBlank()) {
            throw new IllegalArgumentException("Missing or empty indexField");
        }
        String indexField = request.indexField;

        // Validação de timestamp
        if (event.getTimestamp() == null) {
            throw new IllegalArgumentException("Missing event timestamp");
        }

        // Validação de que o campo índice é permitido
        state.validateIndexField(indexField);

        String indexValue = event.getField(indexField);
        if (indexValue == null || indexValue.isBlank()) {
            throw new IllegalArgumentException("Event missing index value for field: " + indexField);
        }

        NodeInfo owner = ownerFor(eventDay(event), indexField, indexValue);
        if (owner.getNodeId().equals(self.getNodeId())) {
            try {
                state.ingest(event, indexField);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return;
        }

        client.ingest(owner, event, indexField).blockingAwait();
    }

    private void handleQuery(DhtProtocol.Request request, String requestId, BufferedWriter writer) {
        // Validação de day
        if (request.day == null || request.day.isBlank()) {
            writeResponse(writer, DhtProtocol.Response.error(requestId, "Missing or empty day"));
            return;
        }
        LocalDate day;
        try {
            day = LocalDate.parse(request.day);
        } catch (Exception e) {
            writeResponse(writer, DhtProtocol.Response.error(requestId, "Invalid day format: " + request.day));
            return;
        }

        // Validação de indexField
        if (request.indexField == null || request.indexField.isBlank()) {
            writeResponse(writer, DhtProtocol.Response.error(requestId, "Missing or empty indexField"));
            return;
        }
        String indexField = request.indexField;

        // Validação de indexValue
        if (request.indexValue == null || request.indexValue.isBlank()) {
            writeResponse(writer, DhtProtocol.Response.error(requestId, "Missing or empty indexValue"));
            return;
        }
        String indexValue = request.indexValue;

        // Validação que o campo índice é permitido
        state.validateIndexField(indexField);

        NodeInfo owner = ownerFor(day, indexField, indexValue);
        Flowable<Event> source = owner.getNodeId().equals(self.getNodeId())
                ? state.streamSubSeries(day, indexField, indexValue)
                : client.streamSubSeries(owner, day, indexField, indexValue);

        source.blockingForEach(event -> writeResponse(writer, DhtProtocol.Response.event(requestId, event)));
        writeResponse(writer, DhtProtocol.Response.complete(requestId));
    }

    private NodeInfo ownerFor(LocalDate day, String indexField, String indexValue) {
        String shardKey = day + "|" + indexField + "|" + indexValue;
        BigInteger targetId = KeyHasher.hashToId(shardKey);
        return peers.responsibleNode(targetId, self);
    }

    private LocalDate eventDay(Event event) {
        return event.getTimestamp().atZone(java.time.ZoneOffset.UTC).toLocalDate();
    }

    private void writeResponse(BufferedWriter writer, DhtProtocol.Response response) {
        try {
            writer.write(mapper.writeValueAsString(response));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
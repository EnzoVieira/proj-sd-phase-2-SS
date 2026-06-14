package pt.ua;

import io.reactivex.rxjava3.core.Flowable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SaServer {
    private final String zone;
    private final int port;
    private final SaNode self;
    private final SaNode peer;
    private final AggregationEngine engine = new AggregationEngine();
    private final AstClient astClient;
    private final AggregationCache cache = new AggregationCache();
    private final SaPeerClient peerClient;
    private final SaPeerServer peerServer;

    private Selector selector;
    private ServerSocketChannel server;
    private final Map<SocketChannel, StringBuilder> pendingBuffers = new ConcurrentHashMap<>();
    private final Map<SocketChannel, AggregationRequest> pendingRequests = new ConcurrentHashMap<>();
    private final Map<SocketChannel, ByteBuffer> pendingWrites = new ConcurrentHashMap<>();

    public SaServer(String zone, int port, String peerHost, int peerPort) {
        this.zone = zone;
        this.port = port;
        this.self = new SaNode("sa-" + zone, zone, "localhost", port, peerPort);
        this.peer = new SaNode(
                "sa-" + (zone.equals("north") ? "south" : "north"),
                zone.equals("north") ? "south" : "north",
                peerHost,
                peerPort,
                peerPort + 100
        );
        this.astClient = new AstClient("localhost", 7878);
        this.peerClient = new SaPeerClient();
        this.peerServer = new SaPeerServer(port + 100, cache);
    }

    public void start() {
        new Thread(() -> peerServer.start(), "peer-server-" + zone).start();
        System.out.println("[sa-server] Starting SA-" + zone + " on port=" + port + " (peer port=" + (port + 100) + ")");

        try {
            selector = Selector.open();
            server = ServerSocketChannel.open();
            server.configureBlocking(false);
            server.bind(new InetSocketAddress(port));
            server.register(selector, SelectionKey.OP_ACCEPT);

            while (true) {
                selector.select();
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();
                    if (!key.isValid()) continue;
                    if (key.isAcceptable()) accept();
                    else if (key.isReadable()) handleReadable((SocketChannel) key.channel());
                    else if (key.isWritable()) handleWritable((SocketChannel) key.channel());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("SA server failed", e);
        }
    }

    private void accept() throws IOException {
        SocketChannel socket = server.accept();
        if (socket == null) return;
        socket.configureBlocking(false);
        socket.register(selector, SelectionKey.OP_READ);
        pendingBuffers.put(socket, new StringBuilder());
        System.out.println("[sa-server] Client connected: " + socket.socket().getRemoteSocketAddress());
    }

    private void handleReadable(SocketChannel socket) throws IOException {
        StringBuilder buffer = pendingBuffers.get(socket);
        if (buffer == null) { closeClient(socket); return; }

        ByteBuffer readBuffer = ByteBuffer.allocate(1024);
        int bytesRead = socket.read(readBuffer);
        if (bytesRead == -1) { closeClient(socket); return; }
        if (bytesRead == 0) return;

        readBuffer.flip();
        buffer.append(StandardCharsets.UTF_8.decode(readBuffer));
        int newlineIndex = buffer.indexOf("\n");
        if (newlineIndex == -1) return;

        String line = buffer.substring(0, newlineIndex).trim();
        buffer.setLength(0);
        if (line.isEmpty()) return;

        try {
            AggregationRequest request = JsonCodec.fromJson(line, AggregationRequest.class);
            if (cache.has(request)) {
                System.out.println("Hit Local Cache");
                sendLater(socket, JsonCodec.toJson(cache.get(request)) + "\n");
                return;
            }
            System.out.println("Miss Local Cache ");
            pendingRequests.put(socket, request);
            peerClient.queryAsync(peer, request, peerResult -> {
                try {
                    if (peerResult != null) {
                        System.out.println("Hit Peer Cache ");
                        cache.put(request, peerResult);
                        sendLater(socket, JsonCodec.toJson(peerResult) + "\n");
                    } else {
                        System.out.println("Miss Peer Cache ");
                        Flowable<Event> stream = astClient.streamSubSeriesRange(
                                request.minDay, request.maxDay, request.indexField, request.indexValue);
                        AggregationResult result = engine.aggregate(stream, request);
                        cache.put(request, result);
                        sendLater(socket, JsonCodec.toJson(result) + "\n");
                    }
                } catch (Exception e) {
                    sendLater(socket, "null\n");
                } finally {
                    pendingRequests.remove(socket);
                }
            });
        } catch (Exception e) {
            sendLater(socket, "null\n");
        }
    }

    private void sendLater(SocketChannel socket, String response) {
        pendingWrites.put(socket, ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8)));
        SelectionKey key = socket.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private void handleWritable(SocketChannel socket) throws IOException {
        ByteBuffer buffer = pendingWrites.get(socket);
        if (buffer == null) {
            SelectionKey key = socket.keyFor(selector);
            if (key != null && key.isValid()) key.interestOps(SelectionKey.OP_READ);
            return;
        }
        socket.write(buffer);
        if (!buffer.hasRemaining()) {
            pendingWrites.remove(socket);
            closeClient(socket);
        }
    }

    private void closeClient(SocketChannel socket) {
        pendingBuffers.remove(socket);
        pendingRequests.remove(socket);
        pendingWrites.remove(socket);
        try { socket.close(); } catch (IOException ignored) {}
    }
}

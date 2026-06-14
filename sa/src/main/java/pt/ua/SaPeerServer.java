package pt.ua;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SaPeerServer {
    private final int port;
    private final AggregationCache cache;
    private final Selector selector;
    private final ServerSocketChannel server;
    private final Map<SocketChannel, StringBuilder> pendingBuffers = new ConcurrentHashMap<>();
    private final Map<SocketChannel, ByteBuffer> pendingWrites = new ConcurrentHashMap<>();

    public SaPeerServer(int port, AggregationCache cache) {
        this.port = port;
        this.cache = cache;
        try {
            this.selector = Selector.open();
            this.server = ServerSocketChannel.open();
            this.server.configureBlocking(false);
            this.server.bind(new InetSocketAddress(port));
            this.server.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create peer server", e);
        }
    }

    public void start() {
        System.out.println("[sa-peer-server] Starting peer server on port=" + port);
        try {
            while (true) {
                selector.select();
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();
                    if (!key.isValid()) continue;
                    if (key.isAcceptable()) accept();
                    else if (key.isReadable()) read((SocketChannel) key.channel());
                    else if (key.isWritable()) write((SocketChannel) key.channel());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Peer server failed", e);
        }
    }

    private void accept() throws IOException {
        SocketChannel socket = server.accept();
        if (socket != null) {
            socket.configureBlocking(false);
            socket.register(selector, SelectionKey.OP_READ);
            pendingBuffers.put(socket, new StringBuilder());
            System.out.println("[sa-peer-server] Client connected: " + socket.socket().getRemoteSocketAddress());
        }
    }

    private void read(SocketChannel socket) throws IOException {
        StringBuilder buffer = pendingBuffers.get(socket);
        if (buffer == null) {
            close(socket);
            return;
        }
        ByteBuffer readBuffer = ByteBuffer.allocate(1024);
        int bytesRead = socket.read(readBuffer);
        if (bytesRead == -1) {
            close(socket);
            return;
        }
        if (bytesRead == 0) return;

        readBuffer.flip();
        buffer.append(StandardCharsets.UTF_8.decode(readBuffer));
        int newlineIndex = buffer.indexOf("\n");
        if (newlineIndex == -1) return;

        String line = buffer.substring(0, newlineIndex).trim();
        buffer.setLength(0);
        if (line.isEmpty()) {
            enqueueWrite(socket, "null\n");
            return;
        }

        try {
            PeerQueryRequest queryReq = JsonCodec.fromJson(line, PeerQueryRequest.class);
            AggregationRequest request = queryReq.request;
            System.out.println("[sa-peer-server] Query received: " + request.type);
            AggregationResult result = cache.get(request);
            enqueueWrite(socket, (result != null ? JsonCodec.toJson(result) : "null") + "\n");
        } catch (Exception e) {
            System.err.println("[sa-peer-server] Parse error: " + e.getMessage());
            enqueueWrite(socket, "null\n");
        }
    }

    private void enqueueWrite(SocketChannel socket, String response) {
        pendingWrites.put(socket, ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8)));
        SelectionKey key = socket.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private void write(SocketChannel socket) throws IOException {
        ByteBuffer buffer = pendingWrites.get(socket);
        if (buffer == null) {
            close(socket);
            return;
        }
        socket.write(buffer);
        if (!buffer.hasRemaining()) close(socket);
    }

    private void close(SocketChannel socket) {
        pendingBuffers.remove(socket);
        pendingWrites.remove(socket);
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}

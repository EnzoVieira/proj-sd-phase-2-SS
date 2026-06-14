package pt.ua;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class SaPeerClient {
    private final ExecutorService ioThread = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "sa-peer-client-io");
        t.setDaemon(true);
        return t;
    });

    public void queryAsync(SaNode peer, AggregationRequest request, Consumer<AggregationResult> callback) {
        ioThread.submit(() -> runQuery(peer, request, callback));
    }

    private void runQuery(SaNode peer, AggregationRequest request, Consumer<AggregationResult> callback) {
        try (Selector selector = Selector.open(); SocketChannel channel = SocketChannel.open()) {
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(peer.host, peer.peerPort));
            channel.register(selector, SelectionKey.OP_CONNECT);

            byte[] payload = (JsonCodec.toJson(new PeerQueryRequest(request)) + "\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer writeBuffer = ByteBuffer.wrap(payload);
            ByteBuffer readBuffer = ByteBuffer.allocate(4096);
            StringBuilder responseBuilder = new StringBuilder();
            long deadline = System.currentTimeMillis() + 5000;

            while (System.currentTimeMillis() < deadline) {
                long timeout = Math.max(1, deadline - System.currentTimeMillis());
                int ready = selector.select(timeout);
                if (ready == 0) continue;

                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> it = keys.iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (!key.isValid()) continue;

                    SocketChannel sc = (SocketChannel) key.channel();
                    if (key.isConnectable()) {
                        if (sc.finishConnect()) key.interestOps(SelectionKey.OP_WRITE);
                    } else if (key.isWritable()) {
                        sc.write(writeBuffer);
                        if (!writeBuffer.hasRemaining()) key.interestOps(SelectionKey.OP_READ);
                    } else if (key.isReadable()) {
                        int n = sc.read(readBuffer);
                        if (n == -1) return;
                        if (n > 0) {
                            readBuffer.flip();
                            responseBuilder.append(StandardCharsets.UTF_8.decode(readBuffer));
                            readBuffer.clear();
                            int nl = responseBuilder.indexOf("\n");
                            if (nl != -1) {
                                String response = responseBuilder.substring(0, nl).trim();
                                if (!response.isEmpty() && !response.equals("null")) {
                                    callback.accept(JsonCodec.fromJson(response, AggregationResult.class));
                                } else {
                                    callback.accept(null);
                                }
                                return;
                            }
                        }
                    }
                }
            }
            callback.accept(null);
        } catch (Exception e) {
            callback.accept(null);
        }
    }
}

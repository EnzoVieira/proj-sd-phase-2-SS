package pt.ua;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

public class ReactiveNodeClient {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    public Completable ingest(NodeInfo node, Event event, String indexField) {
        return Completable.fromAction(() -> {
            DhtProtocol.Request request = DhtProtocol.Request.ingest(event, indexField);
            DhtProtocol.Response response = sendSingleResponse(node, request);
            if (DhtProtocol.ERROR.equals(response.type)) {
                throw new IllegalStateException(response.error);
            }
        });
    }

    public Flowable<Event> streamSubSeriesRange(NodeInfo node, LocalDate minDay, LocalDate maxDay, String indexField,
            String indexValue) {
        return Flowable.create(emitter -> {
            DhtProtocol.Request request = DhtProtocol.Request.queryRange(
                    minDay.toString(), maxDay.toString(), indexField, indexValue);

            try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(node.getHost(), node.getPort()));
                    BufferedWriter writer = new BufferedWriter(Channels.newWriter(channel, StandardCharsets.UTF_8));
                    BufferedReader reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8))) {

                writer.write(mapper.writeValueAsString(request));
                writer.newLine();
                writer.flush();

                String line;
                while (!emitter.isCancelled() && (line = reader.readLine()) != null) {
                    DhtProtocol.Response response = mapper.readValue(line, DhtProtocol.Response.class);
                    if (DhtProtocol.ERROR.equals(response.type)) {
                        emitter.onError(new IllegalStateException(response.error));
                        return;
                    }
                    if (DhtProtocol.EVENT.equals(response.type) && response.event != null) {
                        emitter.onNext(response.event);
                    }
                    if (DhtProtocol.COMPLETE.equals(response.type)) {
                        emitter.onComplete();
                        return;
                    }
                }

                if (!emitter.isCancelled()) {
                    emitter.onComplete();
                }
            } catch (Exception e) {
                if (!emitter.isCancelled()) {
                    emitter.onError(e);
                }
            }
        }, BackpressureStrategy.ERROR);
    }

    public Flowable<Event> streamSubSeries(NodeInfo node, LocalDate day, String indexField, String indexValue) {
        return Flowable.create(emitter -> {
            DhtProtocol.Request request = DhtProtocol.Request.query(day.toString(), indexField, indexValue);

            try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(node.getHost(), node.getPort()));
                    BufferedWriter writer = new BufferedWriter(Channels.newWriter(channel, StandardCharsets.UTF_8));
                    BufferedReader reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8))) {

                writer.write(mapper.writeValueAsString(request));
                writer.newLine();
                writer.flush();

                String line;
                while (!emitter.isCancelled() && (line = reader.readLine()) != null) {
                    DhtProtocol.Response response = mapper.readValue(line, DhtProtocol.Response.class);
                    if (DhtProtocol.ERROR.equals(response.type)) {
                        emitter.onError(new IllegalStateException(response.error));
                        return;
                    }
                    if (DhtProtocol.EVENT.equals(response.type) && response.event != null) {
                        emitter.onNext(response.event);
                    }
                    if (DhtProtocol.COMPLETE.equals(response.type)) {
                        emitter.onComplete();
                        return;
                    }
                }

                if (!emitter.isCancelled()) {
                    emitter.onComplete();
                }
            } catch (Exception e) {
                if (!emitter.isCancelled()) {
                    emitter.onError(e);
                }
            }
        }, BackpressureStrategy.ERROR); // já trava se o consumidor é lento
    }

    private DhtProtocol.Response sendSingleResponse(NodeInfo node, DhtProtocol.Request request) throws Exception {
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(node.getHost(), node.getPort()));
                BufferedWriter writer = new BufferedWriter(Channels.newWriter(channel, StandardCharsets.UTF_8));
                BufferedReader reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8))) {

            writer.write(mapper.writeValueAsString(request));
            writer.newLine();
            writer.flush();

            String line = reader.readLine();
            if (line == null) {
                throw new IllegalStateException("Node closed connection without a response");
            }

            return mapper.readValue(line, DhtProtocol.Response.class);
        }
    }
}

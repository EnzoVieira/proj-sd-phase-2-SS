package pt.ua;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AstClient {
    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final String host;
    private final int port;

    public AstClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Flowable<Event> streamSubSeriesRange(LocalDate minDay, LocalDate maxDay, String indexField, String indexValue) {
        return Flowable.concat(
                Flowable.fromIterable(
                                Stream.iterate(minDay, d -> !d.isAfter(maxDay), d -> d.plusDays(1))
                                        .limit(365)
                                        .collect(Collectors.toList())
                        )
                        .map(d -> streamSubSeries(d, indexField, indexValue))
        );
    }

    private Flowable<Event> streamSubSeries(LocalDate day, String indexField, String indexValue) {
        return Flowable.create(emitter -> {
            try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(host, port));
                 BufferedWriter writer = new BufferedWriter(Channels.newWriter(channel, StandardCharsets.UTF_8));
                 BufferedReader reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8))) {

                // Request QUERY
                AstRequest request = new AstRequest("QUERY", day.toString(), indexField, indexValue);
                writer.write(mapper.writeValueAsString(request));
                writer.newLine();
                writer.flush();

                String line;
                while (!emitter.isCancelled() && (line = reader.readLine()) != null) {
                    AstResponse response = mapper.readValue(line, AstResponse.class);
                    if ("ERROR".equals(response.type)) {
                        emitter.onError(new IllegalStateException(response.error));
                        return;
                    }
                    if ("EVENT".equals(response.type) && response.event != null) {
                        emitter.onNext(response.event);
                    }
                    if ("COMPLETE".equals(response.type)) {
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

    // Request class
    private static class AstRequest {
        public String op;
        public String day;
        public String minDay;
        public String maxDay;
        public String indexField;
        public String indexValue;

        public AstRequest() {
        }

        public AstRequest(String op, String day, String indexField, String indexValue) {
            this.op = op;
            this.day = day;
            this.indexField = indexField;
            this.indexValue = indexValue;
        }
    }

    // Response class
    private static class AstResponse {
        public String requestId;
        public String type;
        public Event event;
        public String error;
    }
}
package pt.ua;

import io.reactivex.rxjava3.core.Flowable;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class DiskSeriesStore {
    private final Path baseDir;
    private final Map<SeriesKey, BufferedWriter> writers = new ConcurrentHashMap<>();

    public DiskSeriesStore(Path baseDir) {
        this.baseDir = baseDir;
    }

    public Path pathFor(SeriesKey key) {
        String fileName = key.getDay() + "__" + encodePathPart(key.getIndexField()) + "__"
                + encodePathPart(key.getIndexValue()) + ".log";
        return baseDir.resolve(fileName);
    }

    private BufferedWriter getWriterFor(SeriesKey key) throws IOException {
        return writers.computeIfAbsent(key, k -> {
            try {
                Files.createDirectories(baseDir);
                Path path = pathFor(k);
                BufferedWriter bw = Files.newBufferedWriter(
                        path,
                        StandardCharsets.UTF_8,
                        java.nio.file.StandardOpenOption.CREATE,
                        java.nio.file.StandardOpenOption.APPEND
                );
                return bw;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public synchronized void append(Event event, SeriesKey key) throws IOException {
        BufferedWriter writer = getWriterFor(key);
        writer.write(JsonEventCodec.toJson(event));
        writer.newLine();
        // flush periódico pode ser feito fora, ou aqui se quiseres segurança
        writer.flush();

        System.out.println("[disk] appended to " + pathFor(key).toAbsolutePath());
    }

    public Flowable<Event> streamAll(SeriesKey key) {
        Path path = pathFor(key);
        if (!Files.isRegularFile(path)) {
            System.out.println("[disk] no file for " + key + " at " + path.toAbsolutePath());
            return Flowable.empty();
        }

        System.out.println("[disk] reading " + path.toAbsolutePath());

        return Flowable.using(
                () -> Files.newBufferedReader(path, StandardCharsets.UTF_8),
                br -> Flowable.generate(emitter -> {
                    try {
                        String line = br.readLine();
                        if (line == null) {
                            emitter.onComplete();
                        } else {
                            emitter.onNext(JsonEventCodec.fromJson(line));
                        }
                    } catch (IOException | RuntimeException e) {
                        emitter.onError(e);
                    }
                }),
                br -> {
                    try {
                        br.close();
                    } catch (IOException ignored) {
                    }
                }
        );
    }

    public void flushAll() throws IOException {
        for (BufferedWriter writer : writers.values()) {
            writer.flush();
        }
    }

    public void closeAll() throws IOException {
        for (BufferedWriter writer : writers.values()) {
            writer.close();
        }
        writers.clear();
    }

    private String encodePathPart(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
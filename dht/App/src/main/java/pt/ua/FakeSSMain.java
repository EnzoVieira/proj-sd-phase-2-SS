package pt.ua;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FakeSSMain {
    public static void main(String[] args) {
        String host = args.length >= 1 ? args[0] : "localhost";
        int port = args.length >= 2 ? Integer.parseInt(args[1]) : 7878;
        // Index fields must match the DHT node configuration (--indexFieldsCsv arg)
        List<String> indexFields = args.length >= 3
                ? Arrays.asList(args[2].split(","))
                : List.of("zone", "type");

        ReactiveNodeClient client = new ReactiveNodeClient();
        NodeInfo bootstrap = new NodeInfo("bootstrap", host, port);

        LocalDate startDay = args.length >= 4 ? LocalDate.parse(args[3]) : LocalDate.of(2026, 6, 1);
        LocalDate endDay   = args.length >= 5 ? LocalDate.parse(args[4]) : LocalDate.of(2026, 6, 9);
        String[] zones = {"north", "south"};
        String[] types = {"alarm", "info"};

        int eventIndex = 1;
        for (LocalDate day = startDay; !day.isAfter(endDay); day = day.plusDays(1)) {
            for (int z = 0; z < zones.length; z++) {
                Map<String, String> fields = new HashMap<>();
                String zone = zones[z];
                String type = types[z % types.length];
                fields.put("temperature", String.valueOf(20.0 + eventIndex));
                fields.put("pressure", String.valueOf(100.0 + eventIndex));

                Instant fixed = day.atTime(12, 0, 0).toInstant(ZoneOffset.UTC);
                Event event = new Event("dev-" + eventIndex, type, zone, fields, fixed);

                System.out.println("[fake-ss] Sending event " + eventIndex + " day=" + day
                        + " zone=" + zone + " type=" + type);

                // Ingest once per index field so the event is queryable by each of them
                for (String indexField : indexFields) {
                    String indexValue = event.getField(indexField);
                    if (indexValue == null || indexValue.isBlank()) continue;
                    try {
                        client.ingest(bootstrap, event, indexField).blockingAwait();
                        System.out.println("[fake-ss]   indexed by " + indexField + "=" + indexValue);
                    } catch (RuntimeException e) {
                        System.err.println("[fake-ss] Ingest failed (indexField=" + indexField + "): " + e.getMessage());
                        System.err.println("[fake-ss] Check that the bootstrap DHT node is running at "
                                + bootstrap.getHost() + ":" + bootstrap.getPort());
                        throw e;
                    }
                }

                eventIndex++;
            }
        }

        System.out.println("[fake-ss] Done. Total events sent: " + (eventIndex - 1)
                + " x " + indexFields.size() + " index fields = "
                + ((eventIndex - 1) * indexFields.size()) + " ingest calls");
    }
}

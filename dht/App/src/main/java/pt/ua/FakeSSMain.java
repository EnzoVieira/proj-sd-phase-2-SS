package pt.ua;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

public class FakeSSMain {
    public static void main(String[] args) {
        ReactiveNodeClient client = new ReactiveNodeClient();
        String host = args.length >= 1 ? args[0] : "localhost";
        int port = args.length >= 2 ? Integer.parseInt(args[1]) : 7878;
        NodeInfo bootstrap = new NodeInfo("bootstrap", host, port);

        LocalDate startDay = LocalDate.of(2026, 6, 1);
        LocalDate endDay = LocalDate.of(2026, 6, 9);
        String[] zones = {"north", "south"};

        int eventIndex = 1;
        for (LocalDate day = startDay; !day.isAfter(endDay); day = day.plusDays(1)) {
            for (int z = 0; z < zones.length; z++) {
                Map<String, String> fields = new HashMap<>();
                String zone = zones[z];
                fields.put("temperature", String.valueOf(20.0 + eventIndex));
                fields.put("pressure", String.valueOf(100.0 + eventIndex));

                Instant fixed = day.atTime(12, 0, 0).toInstant(ZoneOffset.UTC);

                Event event = new Event("dev-" + eventIndex, "temperature", zone, fields, fixed);
                System.out.println("[fake-ss] Sending event " + eventIndex + " on day=" + day
                        + ": " + JsonEventCodec.toJson(event));

                try {
                    client.ingest(bootstrap, event, "zone").blockingAwait();
                } catch (RuntimeException e) {
                    System.err.println("[fake-ss] Ingest failed: " + e.getMessage());
                    System.err.println("[fake-ss] Check that the bootstrap DHT node is running at "
                            + bootstrap.getHost() + ":" + bootstrap.getPort());
                    throw e;
                }

                eventIndex++;
            }
        }

        System.out.println("[fake-ss] Done. Total events sent: " + (eventIndex - 1));
    }
}
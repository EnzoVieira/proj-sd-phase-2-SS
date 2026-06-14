package pt.ua;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

public class FakeConsumerSS {
    public static void main(String[] args) {
        String host = args.length >= 1 ? args[0] : "localhost";
        int port = args.length >= 2 ? Integer.parseInt(args[1]) : 9090;
        String zone = args.length >= 3 ? args[2] : "north";

        System.out.println("[fake-consumer] Connecting to SA-" + zone + " at " + host + ":" + port);

        // Pedir 1: COUNT
        AggregationRequest request1 = new AggregationRequest(
                AggregationType.COUNT,
                zone,
                LocalDate.of(2026, 6, 1),
                LocalDate.of(2026, 6, 9),
                "zone", zone, null, null
        );
        query(host, port, request1);

        // Pedir 2: SUM(temperature)
        AggregationRequest request2 = new AggregationRequest(
                AggregationType.SUM,
                zone,
                LocalDate.of(2026, 6, 1),
                LocalDate.of(2026, 6, 9),
                "zone", zone, "temperature", null
        );
        query(host, port, request2);

        // Pedir 3: MAX(temperature)
        AggregationRequest request3 = new AggregationRequest(
                AggregationType.MAX,
                zone,
                LocalDate.of(2026, 6, 1),
                LocalDate.of(2026, 6, 9),
                "zone", zone, "temperature", null
        );
        query(host, port, request3);

        // Pedir 4: MIN(temperature)
        AggregationRequest request4 = new AggregationRequest(
                AggregationType.MIN,
                zone,
                LocalDate.of(2026, 6, 1),
                LocalDate.of(2026, 6, 9),
                "zone", zone, "temperature", null
        );
        query(host, port, request4);

        // Pedir 5: SUM_PRODUCT(temperature * temperature)
        AggregationRequest request5 = new AggregationRequest(
                AggregationType.SUM_PRODUCT,
                zone,
                LocalDate.of(2026, 6, 1),
                LocalDate.of(2026, 6, 9),
                "zone", zone, "temperature", "temperature"
        );
        query(host, port, request5);

        System.out.println("\n[fake-consumer] === Now test cache distribution ===");
        System.out.println("[fake-consumer] Run same requests on SA-" + (zone.equals("north") ? "south" : "north") +
                " to see peer query");
    }

    private static void query(String host, int port, AggregationRequest request) {
        System.out.println("\n[fake-consumer] Sending request: " + JsonCodec.toJson(request));

        try (Socket socket = new Socket(host, port);
             BufferedWriter writer = new BufferedWriter(
                     new java.io.OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8)
             );
             BufferedReader reader = new BufferedReader(
                     new java.io.InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8)
             )) {

            writer.write(JsonCodec.toJson(request));
            writer.newLine();
            writer.flush();

            String responseLine = reader.readLine();
            if (responseLine != null) {
                AggregationResult result = JsonCodec.fromJson(responseLine, AggregationResult.class);
                System.out.println("[fake-consumer] Result:");
                System.out.println("[fake-consumer]   COUNT: " + result.count);
                System.out.println("[fake-consumer]   SUM: " + result.sum);
                System.out.println("[fake-consumer]   MAX: " + result.max);
                System.out.println("[fake-consumer]   MIN: " + result.min);
                System.out.println("[fake-consumer]   SUM_PRODUCT: " + result.productSum);
            }

        } catch (Exception e) {
            System.err.println("[fake-consumer] Query failed: " + e.getMessage());
        }
    }
}
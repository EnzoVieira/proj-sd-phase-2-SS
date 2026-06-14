package pt.ua;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeServerMain {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: NodeServerMain <nodeId> <port> <maxHistoricalDaysInMemory> <indexFieldsCsv> [peer=nodeId:host:port,...]");
            return;
        }

        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);
        int maxHistoricalDaysInMemory = Integer.parseInt(args[2]);
        Set<String> indexFields = parseCsvSet(args[3]);

        System.out.println("Starting node " + nodeId + " on port " + port);

        DiskSeriesStore store = new DiskSeriesStore(Path.of("data/" + nodeId));
        NodeState state = new NodeState(nodeId, store, indexFields, maxHistoricalDaysInMemory);

        NodeInfo self = new NodeInfo(nodeId, "localhost", port);
        PeerTable peers = new PeerTable();
        peers.addPeer(self);
        if (args.length >= 5) {
            for (String peer : args[4].split(",")) {
                if (!peer.isBlank()) {
                    String[] parts = peer.split(":");
                    peers.addPeer(new NodeInfo(parts[0], parts[1], Integer.parseInt(parts[2])));
                }
            }
        } else {
            peers.addPeer(new NodeInfo("node-1", "localhost", 7878));
            peers.addPeer(new NodeInfo("node-2", "localhost", 7879));
        }

        DhtNodeServer server = new DhtNodeServer(state, self, peers);
        server.start();
    }

    private static Set<String> parseCsvSet(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }
}

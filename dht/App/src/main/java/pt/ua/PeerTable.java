package pt.ua;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class PeerTable {
    private final List<NodeInfo> peers = new ArrayList<>();
    private final TreeMap<BigInteger, NodeInfo> ring = new TreeMap<>();
    private final int virtualNodes;

    public PeerTable() {
        this(64);
    }

    public PeerTable(int virtualNodes) {
        this.virtualNodes = Math.max(1, virtualNodes);
    }

    public synchronized void addPeer(NodeInfo peer) {
        if (peers.stream().noneMatch(p -> p.getNodeId().equals(peer.getNodeId()))) {
            peers.add(peer);
            addToRing(peer);
        }
    }

    public synchronized List<NodeInfo> allPeers() {
        return new ArrayList<>(peers);
    }

    public synchronized NodeInfo responsibleNode(BigInteger targetId, NodeInfo self) {
        if (ring.isEmpty() || peers.stream().noneMatch(p -> p.getNodeId().equals(self.getNodeId()))) {
            addPeer(self);
        }

        Map.Entry<BigInteger, NodeInfo> entry = ring.ceilingEntry(targetId);
        return entry == null ? ring.firstEntry().getValue() : entry.getValue();
    }

    private void addToRing(NodeInfo peer) {
        for (int i = 0; i < virtualNodes; i++) {
            ring.put(KeyHasher.hashToId(peer.getNodeId() + "#" + i), peer);
        }
    }
}

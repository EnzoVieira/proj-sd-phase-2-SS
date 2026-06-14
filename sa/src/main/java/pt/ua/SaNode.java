package pt.ua;

public class SaNode {
    public String id;
    public String zone;
    public String host;
    public int port;
    public int peerPort;

    public SaNode() {
    }

    public SaNode(String id, String zone, String host, int port, int peerPort) {
        this.id = id;
        this.zone = zone;
        this.host = host;
        this.port = port;
        this.peerPort = peerPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SaNode)) return false;
        SaNode saNode = (SaNode) o;
        return port == saNode.port && id.equals(saNode.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode() + port;
    }
}
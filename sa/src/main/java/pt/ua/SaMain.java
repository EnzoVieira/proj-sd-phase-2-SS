package pt.ua;

public class SaMain {
    public static void main(String[] args) {
        // Args: [zone] [port] [peerHost] [peerPort]
        String zone = args.length >= 1 ? args[0] : "north";
        int port = args.length >= 2 ? Integer.parseInt(args[1]) : 9090;
        String peerHost = args.length >= 3 ? args[2] : "localhost";
        int peerPort = args.length >= 4 ? Integer.parseInt(args[3]) : 9091;

        System.out.println("[sa-main] Starting SA-" + zone + " on port=" + port +
                " (peer: " + peerHost + ":" + peerPort + ")");

        SaServer sa = new SaServer(zone, port, peerHost, peerPort);
        sa.start();
    }
}
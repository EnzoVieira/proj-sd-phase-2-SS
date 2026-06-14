package pt.ua;

public class PeerQueryRequest {
    public String type = "QUERY";
    public AggregationRequest request;

    public PeerQueryRequest() {
    }

    public PeerQueryRequest(AggregationRequest request) {
        this.request = request;
    }
}
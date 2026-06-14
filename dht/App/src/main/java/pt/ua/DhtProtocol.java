package pt.ua;

final class DhtProtocol {
    static final String INGEST = "INGEST";
    static final String QUERY = "QUERY";
    static final String QUERY_RANGE = "QUERY_RANGE"; 
    static final String ACK = "ACK";
    static final String EVENT = "EVENT";
    static final String COMPLETE = "COMPLETE";
    static final String ERROR = "ERROR";

    private DhtProtocol() {
    }

    static class Request {
        public String op;
        public Event event;
        public String indexField;
        public String day;        // para QUERY: dia único
        public String minDay;     // para QUERY_RANGE: início
        public String maxDay;     // para QUERY_RANGE: fim
        public String indexValue;

        public Request() {
        }

        static Request ingest(Event event, String indexField) {
            Request r = new Request();
            r.op = INGEST;
            r.event = event;
            r.indexField = indexField;
            return r;
        }

        static Request query(String day, String indexField, String indexValue) {
            Request r = new Request();
            r.op = QUERY;
            r.day = day;
            r.indexField = indexField;
            r.indexValue = indexValue;
            return r;
        }

        static Request queryRange(String minDay, String maxDay, String indexField, String indexValue) {
            Request r = new Request();
            r.op = QUERY_RANGE;
            r.minDay = minDay;
            r.maxDay = maxDay;
            r.indexField = indexField;
            r.indexValue = indexValue;
            return r;
        }
    }

    static class Response {
        public String requestId;
        public String type;
        public Event event;
        public String error;

        public Response() {
        }

        static Response ack(String requestId) {
            Response r = new Response();
            r.requestId = requestId;
            r.type = ACK;
            return r;
        }

        static Response event(String requestId, Event event) {
            Response r = new Response();
            r.requestId = requestId;
            r.type = EVENT;
            r.event = event;
            return r;
        }

        static Response complete(String requestId) {
            Response r = new Response();
            r.requestId = requestId;
            r.type = COMPLETE;
            return r;
        }

        static Response error(String requestId, String error) {
            Response r = new Response();
            r.requestId = requestId;
            r.type = ERROR;
            r.error = error;
            return r;
        }
    }
}
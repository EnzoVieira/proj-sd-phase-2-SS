package pt.ua;

import java.time.LocalDate;

public class AggregationRequest {
    public AggregationType type;
    public String zone;
    public LocalDate minDay;
    public LocalDate maxDay;
    public String indexField;
    public String indexValue;
    public String k2;
    public String k3;

    public AggregationRequest() {
    }

    public AggregationRequest(AggregationType type, String zone,LocalDate minDay, LocalDate maxDay,
                              String indexField, String indexValue, String k2, String k3) {
        this.type = type;
        this.zone = zone;
        this.minDay = minDay;
        this.maxDay = maxDay;
        this.indexField = indexField;
        this.indexValue = indexValue;
        this.k2 = k2;
        this.k3 = k3;
    }
}
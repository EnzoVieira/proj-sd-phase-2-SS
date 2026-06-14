package pt.ua;

public class AggregationResult {
    public AggregationType type;
    public long count;
    public double sum;
    public double max = Double.NEGATIVE_INFINITY;
    public double min = Double.POSITIVE_INFINITY;
    public double productSum;
    public double average;

    public AggregationResult() {
    }

    public AggregationResult(AggregationType type, long count, double sum, double max, double min, double productSum) {
        this.type = type;
        this.count = count;
        this.sum = sum;
        this.max = max;
        this.min = min;
        this.productSum = productSum;
        this.average = count > 0 ? sum / count : 0;
    }
}
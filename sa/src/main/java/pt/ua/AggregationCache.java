package pt.ua;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class AggregationCache {
    private static final int MAX_SIZE = 1000;

    private final Map<String, AggregationResult> cache = Collections.synchronizedMap(
            new LinkedHashMap<String, AggregationResult>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, AggregationResult> eldest) {
                    return size() > MAX_SIZE;
                }
            });

    public String makeKey(AggregationRequest request) {
        return request.type + "|" +
                request.minDay + "|" +
                request.maxDay + "|" +
                request.indexField + "|" +
                request.indexValue + "|" +
                request.k2 + "|" +
                request.k3;
    }

    public boolean has(AggregationRequest request) {
        return cache.containsKey(makeKey(request));
    }

    public AggregationResult get(AggregationRequest request) {
        return cache.get(makeKey(request));
    }

    public void put(AggregationRequest request, AggregationResult result) {
        cache.put(makeKey(request), result);
    }

    public void remove(String key) {
        cache.remove(key);
    }

    public int size() {
        return cache.size();
    }
}

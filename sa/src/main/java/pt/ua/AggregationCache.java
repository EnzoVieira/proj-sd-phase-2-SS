package pt.ua;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class AggregationCache {
    private final Map<String, AggregationResult> cache = new ConcurrentHashMap<>();
    private static final int MAX_SIZE = 1000;

    public String makeKey(AggregationRequest request) {
        return request.zone + "|" +
                request.type + "|" +
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
        // Se cache cheio, remover mais antigo (simples: remover primeiro)
        if (cache.size() >= MAX_SIZE) {
            String firstKey = cache.keySet().iterator().next();
            cache.remove(firstKey);
        }
        cache.put(makeKey(request), result);
    }

    public void remove(String key) {
        cache.remove(key);
    }

    public int size() {
        return cache.size();
    }
}
package pt.ua;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.concurrent.TimeUnit;

public class AggregationEngine {

    public Single<AggregationResult> aggregate(Flowable<Event> stream, AggregationRequest request) {
        AggregationResult seed = new AggregationResult();
        seed.type = request.type;

        return stream
                .subscribeOn(Schedulers.io())
                .reduce(seed, (acc, ev) -> {
                    switch (request.type) {
                        case COUNT:
                            acc.count++;
                            break;
                        case SUM: {
                            Double v = parseField(ev, request.k2);
                            if (v != null) { acc.count++; acc.sum += v; }
                            break;
                        }
                        case MAX: {
                            Double v = parseField(ev, request.k2);
                            if (v != null) { acc.count++; if (v > acc.max) acc.max = v; }
                            break;
                        }
                        case MIN: {
                            Double v = parseField(ev, request.k2);
                            if (v != null) { acc.count++; if (v < acc.min) acc.min = v; }
                            break;
                        }
                        case SUM_PRODUCT: {
                            Double v2 = parseField(ev, request.k2);
                            Double v3 = parseField(ev, request.k3);
                            if (v2 != null && v3 != null) { acc.count++; acc.productSum += v2 * v3; }
                            break;
                        }
                    }
                    return acc;
                })
                .timeout(30, TimeUnit.SECONDS);
    }

    private Double parseField(Event ev, String field) {
        if (field == null) return null;
        String raw = ev.getField(field);
        if (raw == null) return null;
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

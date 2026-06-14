package pt.ua;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.concurrent.TimeUnit;

public class AggregationEngine {

    public AggregationResult aggregate(Flowable<Event> stream, AggregationRequest request) {
        AggregationResult result = new AggregationResult();
        result.type = request.type;

        AggregationResult finalResult = stream
                .subscribeOn(Schedulers.io())  // ✅ ADD isto — corre em thread IO separada
                .reduce(result, (acc, ev) -> {
                    acc.count++;

                    double k2Value = 0;
                    double k3Value = 0;

                    if (request.k2 != null) {
                        String k2Str = ev.getField(request.k2);
                        if (k2Str != null) {
                            try {
                                k2Value = Double.parseDouble(k2Str);
                            } catch (NumberFormatException ignored) {
                            }
                        }
                    }

                    if (request.k3 != null) {
                        String k3Str = ev.getField(request.k3);
                        if (k3Str != null) {
                            try {
                                k3Value = Double.parseDouble(k3Str);
                            } catch (NumberFormatException ignored) {
                            }
                        }
                    }

                    if (k2Value > acc.max) acc.max = k2Value;
                    if (k2Value < acc.min) acc.min = k2Value;

                    switch (request.type) {
                        case COUNT:
                            break;
                        case SUM:
                            acc.sum += k2Value;
                            break;
                        case MAX:
                            break;
                        case MIN:
                            break;
                        case SUM_PRODUCT:
                            acc.productSum += k2Value * k3Value;
                            break;
                    }

                    return acc;
                })
                .timeout(30, TimeUnit.SECONDS)
                .blockingGet();

        if (finalResult.count > 0) {
            finalResult.average = finalResult.sum / finalResult.count;
        }

        return finalResult;
    }
}
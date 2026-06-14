package pt.ua;

import io.reactivex.rxjava3.core.Flowable;
import java.util.concurrent.CopyOnWriteArrayList;

public class SeriesPartition {
    private final SeriesKey key;
    private final CopyOnWriteArrayList<Event> events = new CopyOnWriteArrayList<>();

    public SeriesPartition(SeriesKey key) {
        this.key = key;
    }

    public SeriesKey getKey() {
        return key;
    }

    public synchronized void add(Event event) {
        events.add(event);
    }

    public Flowable<Event> stream() {
        return Flowable.fromIterable(events);
    }

    public synchronized int size() {
        return events.size();
    }
}
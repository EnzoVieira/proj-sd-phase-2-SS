package pt.ua;

import io.reactivex.rxjava3.core.Flowable;

import java.time.Clock;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeState {
    private final String nodeId;
    private final Map<SeriesKey, SeriesPartition> partitions = new HashMap<>();
    private final LinkedHashMap<LocalDate, Set<SeriesKey>> cachedHistoricalDays = new LinkedHashMap<>(16, 0.75f, true);
    private final DiskSeriesStore diskStore;
    private final Set<String> allowedIndexFields;
    private final int maxHistoricalDaysInMemory;
    private final Clock clock;

    public NodeState(String nodeId, DiskSeriesStore diskStore, Set<String> allowedIndexFields,
            int maxHistoricalDaysInMemory) {
        this(nodeId, diskStore, allowedIndexFields, maxHistoricalDaysInMemory, Clock.systemUTC());
    }

    NodeState(String nodeId, DiskSeriesStore diskStore, Set<String> allowedIndexFields, int maxHistoricalDaysInMemory,
            Clock clock) {
        this.nodeId = nodeId;
        this.diskStore = diskStore;
        this.allowedIndexFields = Set.copyOf(allowedIndexFields);
        this.maxHistoricalDaysInMemory = Math.max(0, maxHistoricalDaysInMemory);
        this.clock = clock;
    }

    public String getNodeId() {
        return nodeId;
    }

    public synchronized void ingest(Event event, String indexField) throws Exception {
        validateIndexField(indexField);
        String indexValue = event.getField(indexField);
        if (indexValue == null) {
            System.out.println("[state] " + nodeId + " skipping event without " + indexField);
            return;
        }

        LocalDate day = event.getTimestamp().atZone(java.time.ZoneOffset.UTC).toLocalDate();
        SeriesKey key = new SeriesKey(day, indexField, indexValue);

        System.out.println("[state] " + nodeId + " ingest key=" + key);
        diskStore.append(event, key);
        if (shouldKeepInMemory(day)) {
            partitions.computeIfAbsent(key, SeriesPartition::new).add(event);
            rememberCachedDay(day, key);
            evictHistoricalDaysIfNeeded();
        }
        System.out.println("[state] " + nodeId + " append done");
    }

    public synchronized Flowable<Event> streamSubSeries(LocalDate day, String indexField, String indexValue) {
        validateIndexField(indexField);
        SeriesKey key = new SeriesKey(day, indexField, indexValue);

        System.out.println("[state] " + nodeId + " stream key=" + key);

        SeriesPartition p = partitions.get(key);
        if (p != null) {
            System.out.println("[state] " + nodeId + " serving from memory");
            return p.stream(); // substitui: Flowable.fromIterable(p.snapshot())
        }

        System.out.println("[state] " + nodeId + " serving from disk");
        return diskStore.streamAll(key);
    }

    public void validateIndexField(String indexField) {
        if (!allowedIndexFields.contains(indexField)) {
            throw new IllegalArgumentException("Index field is not configured: " + indexField);
        }
    }

    public Set<String> getAllowedIndexFields() {
        return allowedIndexFields;
    }

    private boolean shouldKeepInMemory(LocalDate day) {
        return day.equals(currentDay()) || maxHistoricalDaysInMemory > 0;
    }

    private void rememberCachedDay(LocalDate day, SeriesKey key) {
        if (day.equals(currentDay())) {
            return;
        }
        cachedHistoricalDays.computeIfAbsent(day, ignored -> new HashSet<>()).add(key);
    }

    private void evictHistoricalDaysIfNeeded() {
        while (cachedHistoricalDays.size() > maxHistoricalDaysInMemory) {
            LocalDate dayToEvict = cachedHistoricalDays.keySet().iterator().next();
            Set<SeriesKey> keys = cachedHistoricalDays.remove(dayToEvict);
            if (keys != null) {
                keys.forEach(partitions::remove);
                System.out.println("[state] " + nodeId + " evicted day=" + dayToEvict + " keys="
                        + keys.stream().map(SeriesKey::toString).collect(Collectors.joining(",")));
            }
        }
    }

    private LocalDate currentDay() {
        return LocalDate.now(clock);
    }
}

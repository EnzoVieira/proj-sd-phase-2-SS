package pt.ua;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Event {
    private String deviceId;
    private String type;
    private String zone;
    private Map<String, String> fields;
    private Instant timestamp;

    public Event() {
        this.fields = new HashMap<>();
    }

    public Event(String deviceId, String type, String zone, Map<String, String> fields, Instant timestamp) {
        this.deviceId = deviceId;
        this.type = type;
        this.zone = zone;
        this.fields = fields != null ? fields : new HashMap<>();
        this.timestamp = timestamp;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields != null ? fields : new HashMap<>();
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getField(String key) {
        switch (key) {
            case "deviceId":
                return deviceId;
            case "type":
                return type;
            case "zone":
                return zone;
            case "timestamp":
                return timestamp == null ? null : timestamp.toString();
            default:
                return fields != null ? fields.get(key) : null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Event)) return false;
        Event event = (Event) o;
        return Objects.equals(deviceId, event.deviceId) &&
               Objects.equals(type, event.type) &&
               Objects.equals(zone, event.zone) &&
               Objects.equals(fields, event.fields) &&
               Objects.equals(timestamp, event.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deviceId, type, zone, fields, timestamp);
    }
}
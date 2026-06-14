package pt.ua;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class JsonEventCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    public static String toJson(Event event) {
        try {
            return MAPPER.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Event fromJson(String json) {
        try {
            return MAPPER.readValue(json, Event.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
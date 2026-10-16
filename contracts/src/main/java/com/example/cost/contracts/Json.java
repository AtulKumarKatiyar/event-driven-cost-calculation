package com.example.cost.contracts;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.UUID;

public final class Json {
  public static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private Json() {}

  public static String write(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot serialize event", e);
    }
  }

  public static <T> T read(String value, Class<T> type) {
    try {
      return MAPPER.readValue(value, type);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid JSON", e);
    }
  }

  public static JsonNode tree(String value) {
    try {
      return MAPPER.readTree(value);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid JSON", e);
    }
  }

  public static <T> Events.Event<T> event(
      String value, Class<T> type, String key, String expectedType) {
    try {
      JavaType target = MAPPER.getTypeFactory().constructParametricType(Events.Event.class, type);
      Events.Event<T> event = MAPPER.readValue(value, target);
      if (event.schemaVersion() != 1
          || event.eventId() == null
          || event.occurredAt() == null
          || event.payload() == null
          || !UUID.fromString(key).equals(event.calculationId())
          || !expectedType.equals(event.eventType()))
        throw new IllegalArgumentException("Invalid event envelope");
      return event;
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid " + expectedType, e);
    }
  }
}

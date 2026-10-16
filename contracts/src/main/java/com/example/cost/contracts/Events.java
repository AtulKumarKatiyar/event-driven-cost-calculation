package com.example.cost.contracts;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

public final class Events {
  private Events() {}

  public enum State {
    REQUESTED,
    IN_PROGRESS,
    COMPLETED,
    FAILED;

    public boolean terminal() {
      return this == COMPLETED || this == FAILED;
    }
  }

  public record Error(String code, String message, boolean retryable) {}

  public record Requested(CalculationRequest request, String currency, Instant deadlineAt) {}

  public record Result(
      Component componentType, boolean success, BigDecimal amount, String currency, Error error) {}

  public record Lifecycle(State state, long stateVersion) {}

  public record Final(
      State state,
      Map<Component, BigDecimal> components,
      BigDecimal totalCost,
      String currency,
      Error error,
      Instant finishedAt) {}

  public record Event<T>(
      UUID eventId,
      String eventType,
      int schemaVersion,
      Instant occurredAt,
      UUID calculationId,
      T payload) {}

  public static <T> Event<T> event(String type, UUID id, T payload) {
    return new Event<>(UUID.randomUUID(), type, 1, Instant.now(), id, payload);
  }
}

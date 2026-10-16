package com.example.cost.aggregator;

import com.example.cost.contracts.*;
import com.example.cost.contracts.Events.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class AggregationProcessor implements Processor<String, String, String, String> {
  private ProcessorContext<String, String> context;
  private KeyValueStore<String, String> store;

  @Override
  public void init(ProcessorContext<String, String> context) {
    this.context = context;
    this.store = context.getStateStore(AggregationTopology.STORE);
    context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, this::expire);
  }

  @Override
  public void process(Record<String, String> record) {
    try {
      if (record.key() == null || record.value() == null)
        throw new IllegalArgumentException("Null event key/value");
      UUID.fromString(record.key());
      String topic = context.recordMetadata().orElseThrow().topic();
      AggregationState state = load(record.key());
      if (Topics.REQUESTED.equals(topic)) {
        var e =
            Json.event(record.value(), Requested.class, record.key(), "CalculationRequestedEvent");
        var req = e.payload();
        if (req.request() == null
            || !e.calculationId().equals(req.request().calculationId())
            || req.request().quantity() < 1
            || req.currency() == null
            || req.deadlineAt() == null) throw new IllegalArgumentException("Invalid registration");
        if (state.requested != null && !state.requested.equals(req))
          throw new IllegalArgumentException("Conflicting registration");
        state.requested = req;
      } else {
        var e = Json.event(record.value(), Result.class, record.key(), "CostComponentResultEvent");
        Result result = e.payload();
        validate(result, topic);
        Result existing = state.results.get(result.componentType());
        if (existing != null && !existing.equals(result) && (state.terminal || result.success()))
          throw new IllegalArgumentException("Conflicting component outcome");
        if (state.requested != null && !state.requested.currency().equals(result.currency()))
          throw new IllegalArgumentException("Currency mismatch");
        if (!state.terminal) state.results.put(result.componentType(), result);
      }
      if (!state.terminal) decide(record.key(), state, context.currentSystemTimeMs());
      store.put(record.key(), Json.write(state));
    } catch (IllegalArgumentException e) {
      var quarantine = new LinkedHashMap<String, Object>();
      quarantine.put("quarantine", true);
      quarantine.put("reason", e.getMessage());
      quarantine.put("key", record.key());
      quarantine.put("value", record.value());
      context
          .recordMetadata()
          .ifPresent(
              m -> {
                quarantine.put("sourceTopic", m.topic());
                quarantine.put("partition", m.partition());
                quarantine.put("offset", m.offset());
              });
      context.forward(new Record<>(record.key(), Json.write(quarantine), record.timestamp()));
    }
  }

  private AggregationState load(String key) {
    String value = store.get(key);
    if (value != null) return Json.read(value, AggregationState.class);
    var state = new AggregationState();
    state.firstSeen = context.currentSystemTimeMs();
    return state;
  }

  private void validate(Result r, String topic) {
    if (r.componentType() == null
        || !r.componentType().resultTopic().equals(topic)
        || r.currency() == null) throw new IllegalArgumentException("Invalid component source");
    if (r.success()
        && (r.amount() == null
            || r.amount().signum() < 0
            || r.amount().scale() > 2
            || r.error() != null)) throw new IllegalArgumentException("Invalid success result");
    if (!r.success() && (r.amount() != null || r.error() == null))
      throw new IllegalArgumentException("Invalid failure result");
  }

  private void decide(String key, AggregationState state, long now) {
    if (state.requested == null) return; // Results can overtake registration across topics.
    for (Result r : state.results.values()) {
      if (!state.requested.currency().equals(r.currency())) {
        finish(key, state, new Events.Error("INVALID_RESULT", "Currency mismatch", false), now);
        return;
      }
      if (!r.success()) {
        finish(key, state, r.error(), now);
        return;
      }
    }
    if (now >= state.requested.deadlineAt().toEpochMilli()) {
      finish(
          key,
          state,
          new Events.Error(
              "DEADLINE_EXCEEDED",
              "One or more required results did not arrive before the deadline",
              true),
          now);
      return;
    }
    if (state.results.size() == Component.values().length) finish(key, state, null, now);
  }

  private void finish(String key, AggregationState state, Events.Error error, long now) {
    Map<Component, BigDecimal> components = new EnumMap<>(Component.class);
    BigDecimal total = null;
    if (error == null) {
      total = BigDecimal.ZERO.setScale(2);
      for (Component c : Component.values()) {
        BigDecimal amount = state.results.get(c).amount();
        components.put(c, amount);
        total = total.add(amount);
      }
    }
    Final value =
        new Final(
            error == null ? State.COMPLETED : State.FAILED,
            components,
            total,
            state.requested.currency(),
            error,
            Instant.ofEpochMilli(now));
    String type = error == null ? "CalculationCompletedEvent" : "CalculationFailedEvent";
    Event<Final> event =
        new Event<>(
            UUID.nameUUIDFromBytes((key + ":final:v1").getBytes(StandardCharsets.UTF_8)),
            type,
            1,
            Instant.ofEpochMilli(now),
            UUID.fromString(key),
            value);
    context.forward(new Record<>(key, Json.write(event), now));
    state.terminal = true;
  }

  private void expire(long now) {
    // Full scan is appropriate for the learning workload; production needs a deadline index.
    try (var entries = store.all()) {
      while (entries.hasNext()) {
        var row = entries.next();
        var state = Json.read(row.value, AggregationState.class);
        if (state.terminal) continue;
        if (state.requested == null && now - state.firstSeen > 300000) {
          context.forward(
              new Record<>(
                  row.key,
                  Json.write(
                      Map.of(
                          "quarantine",
                          true,
                          "reason",
                          "Orphan results without registration",
                          "key",
                          row.key)),
                  now));
          store.delete(row.key);
          continue;
        }
        if (state.requested != null && now >= state.requested.deadlineAt().toEpochMilli()) {
          decide(row.key, state, now);
          store.put(row.key, Json.write(state));
        }
      }
    }
  }
}

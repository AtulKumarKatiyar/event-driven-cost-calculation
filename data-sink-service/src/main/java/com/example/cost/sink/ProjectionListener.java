package com.example.cost.sink;

import com.example.cost.contracts.*;
import com.example.cost.contracts.Events.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ProjectionListener {
  private final ProjectionStore store;

  public ProjectionListener(ProjectionStore store) {
    this.store = store;
  }

  @KafkaListener(topics = {Topics.REQUESTED, Topics.LIFECYCLE, Topics.FINAL})
  public void consume(ConsumerRecord<String, String> r) {
    switch (r.topic()) {
      case Topics.REQUESTED ->
          store.requested(
              Json.event(r.value(), Requested.class, r.key(), "CalculationRequestedEvent"));
      case Topics.LIFECYCLE ->
          store.lifecycle(
              Json.event(r.value(), Lifecycle.class, r.key(), "CalculationLifecycleEvent"));
      case Topics.FINAL -> {
        String type = Json.tree(r.value()).path("eventType").asText();
        if (!type.equals("CalculationCompletedEvent") && !type.equals("CalculationFailedEvent"))
          throw new IllegalArgumentException("Unknown final event");
        store.finish(Json.event(r.value(), Final.class, r.key(), type));
      }
      default -> throw new IllegalArgumentException("Unexpected topic");
    }
  }
}

package com.example.cost.command;

import com.example.cost.contracts.*;
import com.example.cost.contracts.Events.Final;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class FinalListener {
  private final CommandStore store;

  public FinalListener(CommandStore store) {
    this.store = store;
  }

  @KafkaListener(topics = Topics.FINAL)
  public void consume(ConsumerRecord<String, String> record) {
    String type = Json.tree(record.value()).path("eventType").asText();
    if (!type.equals("CalculationCompletedEvent") && !type.equals("CalculationFailedEvent"))
      throw new IllegalArgumentException("Unknown final event");
    store.finish(Json.event(record.value(), Final.class, record.key(), type));
  }
}

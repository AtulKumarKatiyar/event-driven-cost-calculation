package com.example.cost.freight;

import com.example.cost.contracts.*;
import com.example.cost.contracts.Events.*;
import java.math.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class CostConsumer {
  private final KafkaTemplate<String, String> kafka;
  private final BigDecimal rate;
  private final boolean demoFailures;

  public CostConsumer(
      KafkaTemplate<String, String> kafka,
      @Value("${cost.unit-rate:12.50}") BigDecimal rate,
      @Value("${cost.demo-failures:false}") boolean demoFailures) {
    if (rate.signum() < 0 || rate.scale() > 2)
      throw new IllegalArgumentException("Rate must be nonnegative with at most 2 decimal places");
    this.kafka = kafka;
    this.rate = rate;
    this.demoFailures = demoFailures;
  }

  @KafkaListener(topics = "cost.freight.requested.v1")
  public void consume(ConsumerRecord<String, String> record) throws Exception {
    var event =
        Json.event(record.value(), Requested.class, record.key(), "CalculationRequestedEvent");
    var request = event.payload().request();
    if (request == null
        || !event.calculationId().equals(request.calculationId())
        || request.quantity() < 1) throw new IllegalArgumentException("Invalid request");
    boolean fail = demoFailures && request.productId().equals("FAIL_FREIGHT_COST");
    Result result =
        fail
            ? new Result(
                Component.FREIGHT_COST,
                false,
                null,
                event.payload().currency(),
                new Events.Error("DEMO_FAILURE", "Configured demo failure in FREIGHT_COST", false))
            : new Result(
                Component.FREIGHT_COST,
                true,
                rate.multiply(BigDecimal.valueOf(request.quantity())).setScale(2),
                event.payload().currency(),
                null);
    var output =
        new Event<>(
            UUID.nameUUIDFromBytes(
                (event.eventId() + ":FREIGHT_COST").getBytes(StandardCharsets.UTF_8)),
            "CostComponentResultEvent",
            1,
            event.occurredAt(),
            event.calculationId(),
            result);
    kafka
        .send(Component.FREIGHT_COST.resultTopic(), record.key(), Json.write(output))
        .get(10, TimeUnit.SECONDS);
  }
}

package com.example.cost.freight;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.example.cost.contracts.*;
import com.example.cost.contracts.Events.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

class CostConsumerTest {
  @Test
  void computesCostAndKeepsEventIdentityAcrossRedelivery() throws Exception {
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));
    var consumer = new CostConsumer(kafka, new BigDecimal("12.50"), false);
    UUID id = UUID.randomUUID();
    var input =
        Events.event(
            "CalculationRequestedEvent",
            id,
            new Requested(
                new CalculationRequest(id, "R", "M", "V", "F", "P", 3),
                "INR",
                Instant.now().plusSeconds(60)));
    var record =
        new ConsumerRecord<String, String>(
            Component.FREIGHT_COST.requestTopic(), 0, 0, id.toString(), Json.write(input));
    consumer.consume(record);
    consumer.consume(record);
    var values = ArgumentCaptor.forClass(String.class);
    verify(kafka, times(2))
        .send(eq(Component.FREIGHT_COST.resultTopic()), eq(id.toString()), values.capture());
    assertThat(values.getAllValues().get(0)).isEqualTo(values.getAllValues().get(1));
    assertThat(
            Json.event(values.getValue(), Result.class, id.toString(), "CostComponentResultEvent")
                .payload()
                .amount())
        .isEqualByComparingTo(new BigDecimal("12.50").multiply(new BigDecimal("3")));
  }

  @Test
  void configuredFailureHasNoAmount() throws Exception {
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));
    var consumer = new CostConsumer(kafka, new BigDecimal("12.50"), true);
    UUID id = UUID.randomUUID();
    var event =
        Events.event(
            "CalculationRequestedEvent",
            id,
            new Requested(
                new CalculationRequest(id, "R", "M", "V", "F", "FAIL_FREIGHT_COST", 1),
                "INR",
                Instant.now().plusSeconds(60)));
    consumer.consume(
        new ConsumerRecord<>(
            Component.FREIGHT_COST.requestTopic(), 0, 0, id.toString(), Json.write(event)));
    var value = ArgumentCaptor.forClass(String.class);
    verify(kafka).send(anyString(), anyString(), value.capture());
    var result =
        Json.event(value.getValue(), Result.class, id.toString(), "CostComponentResultEvent")
            .payload();
    assertThat(result.success()).isFalse();
    assertThat(result.amount()).isNull();
    assertThat(result.error().code()).isEqualTo("DEMO_FAILURE");
  }
}

package com.example.cost.first;

import static org.assertj.core.api.Assertions.*;

import com.example.cost.contracts.*;
import com.example.cost.contracts.Events.*;
import java.time.*;
import java.util.*;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {"cost.unit-rate=100.00", "cost.demo-failures=true"})
@Import(CostKafkaTest.TopicsConfig.class)
class CostKafkaTest {
  @Container
  static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  @TestConfiguration
  static class TopicsConfig {
    @Bean
    NewTopic requests() {
      return new NewTopic(Component.FIRST_COST.requestTopic(), 1, (short) 1);
    }

    @Bean
    NewTopic results() {
      return new NewTopic(Component.FIRST_COST.resultTopic(), 1, (short) 1);
    }

    @Bean
    NewTopic deadletters() {
      return new NewTopic(Component.FIRST_COST.requestTopic() + ".dlt", 1, (short) 1);
    }
  }

  @Autowired KafkaTemplate<String, String> template;

  @Test
  void transactionalWorkerPublishesSuccessAndBusinessFailure() {
    UUID success = UUID.randomUUID(), failure = UUID.randomUUID();
    send(success, "P");
    send(failure, "FAIL_FIRST_COST");
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    try (var consumer =
        new KafkaConsumer<String, String>(p, new StringDeserializer(), new StringDeserializer())) {
      consumer.subscribe(List.of(Component.FIRST_COST.resultTopic()));
      Map<UUID, Result> results = new HashMap<>();
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (results.size() < 2 && System.nanoTime() < deadline) {
        for (var record : consumer.poll(Duration.ofMillis(500))) {
          var event =
              Json.event(record.value(), Result.class, record.key(), "CostComponentResultEvent");
          results.put(event.calculationId(), event.payload());
        }
      }
      assertThat(results).containsKeys(success, failure);
      assertThat(results.get(success).amount()).isEqualByComparingTo("200.00");
      assertThat(results.get(failure).success()).isFalse();
      assertThat(results.get(failure).amount()).isNull();
    }
  }

  void send(UUID id, String product) {
    var event =
        Events.event(
            "CalculationRequestedEvent",
            id,
            new Requested(
                new CalculationRequest(id, "R", "M", "V", "F", product, 2),
                "INR",
                Instant.now().plusSeconds(60)));
    template.executeInTransaction(
        ops -> {
          ops.send(Component.FIRST_COST.requestTopic(), id.toString(), Json.write(event));
          return null;
        });
  }
}

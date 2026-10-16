package com.example.cost.aggregator;

import static org.assertj.core.api.Assertions.*;

import com.example.cost.contracts.*;
import com.example.cost.contracts.Events.*;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;

class AggregationTopologyTest {
  TopologyTestDriver driver;
  TestInputTopic<String, String> requests;
  Map<Component, TestInputTopic<String, String>> inputs;
  TestOutputTopic<String, String> finals, quarantine;
  UUID id;
  Instant now = Instant.parse("2026-10-12T00:00:00Z");

  @BeforeEach
  void setup() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-" + UUID.randomUUID());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    props.put(
        StreamsConfig.STATE_DIR_CONFIG,
        System.getProperty("java.io.tmpdir") + "/cost-test-" + UUID.randomUUID());
    driver = new TopologyTestDriver(AggregationTopology.topology(), props, now);
    requests = input(Topics.REQUESTED);
    inputs = new EnumMap<>(Component.class);
    for (Component c : Component.values()) inputs.put(c, input(c.resultTopic()));
    finals =
        driver.createOutputTopic(Topics.FINAL, new StringDeserializer(), new StringDeserializer());
    quarantine =
        driver.createOutputTopic(
            AggregationTopology.DLT, new StringDeserializer(), new StringDeserializer());
    id = UUID.randomUUID();
  }

  TestInputTopic<String, String> input(String topic) {
    return driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());
  }

  @AfterEach
  void close() {
    driver.close();
  }

  Event<Requested> registration() {
    return Events.event(
        "CalculationRequestedEvent",
        id,
        new Requested(
            new CalculationRequest(id, "R", "M", "V", "F", "P", 2), "INR", now.plusSeconds(10)));
  }

  void request() {
    requests.pipeInput(id.toString(), Json.write(registration()));
  }

  void result(Component c, boolean success) {
    inputs
        .get(c)
        .pipeInput(
            id.toString(),
            Json.write(
                Events.event(
                    "CostComponentResultEvent",
                    id,
                    new Result(
                        c,
                        success,
                        success ? new BigDecimal("10.00") : null,
                        "INR",
                        success ? null : new Events.Error("FAIL", "Failed", false)))));
  }

  Final output() {
    String value = finals.readValue();
    return Json.event(value, Final.class, id.toString(), Json.tree(value).get("eventType").asText())
        .payload();
  }

  @Test
  void sumsOnlyAfterFourDistinctSuccesses() {
    request();
    result(Component.FIRST_COST, true);
    result(Component.FIRST_COST, true);
    result(Component.FREIGHT_COST, true);
    result(Component.HANDLING_COST, true);
    assertThat(finals.isEmpty()).isTrue();
    result(Component.INVENTORY_COST, true);
    var output = output();
    assertThat(output.state()).isEqualTo(State.COMPLETED);
    assertThat(output.totalCost()).isEqualByComparingTo("40.00");
    result(Component.INVENTORY_COST, true);
    assertThat(finals.isEmpty()).isTrue();
  }

  @Test
  void failureIsImmediateAndLateSuccessCannotComplete() {
    request();
    result(Component.FREIGHT_COST, false);
    String raw = finals.readValue();
    assertThat(raw).doesNotContain("totalCost");
    assertThat(Json.tree(raw).path("payload").path("state").asText()).isEqualTo("FAILED");
    result(Component.FIRST_COST, true);
    result(Component.HANDLING_COST, true);
    result(Component.INVENTORY_COST, true);
    driver.advanceWallClockTime(Duration.ofSeconds(20));
    assertThat(finals.isEmpty()).isTrue();
  }

  @Test
  void zeroResultsStillTimeOutUsingWallClock() {
    request();
    driver.advanceWallClockTime(Duration.ofSeconds(11));
    var output = output();
    assertThat(output.state()).isEqualTo(State.FAILED);
    assertThat(output.error().code()).isEqualTo("DEADLINE_EXCEEDED");
  }

  @Test
  void resultsMayArriveBeforeRegistration() {
    for (Component c : Component.values()) result(c, true);
    assertThat(finals.isEmpty()).isTrue();
    request();
    assertThat(output().state()).isEqualTo(State.COMPLETED);
  }

  @Test
  void wrongSourceAndMalformedMessagesAreQuarantined() {
    requests.pipeInput(id.toString(), "not-json");
    assertThat(quarantine.readValue()).contains("quarantine");
    var event =
        Events.event(
            "CostComponentResultEvent",
            id,
            new Result(Component.FREIGHT_COST, true, new BigDecimal("10.00"), "INR", null));
    inputs.get(Component.FIRST_COST).pipeInput(id.toString(), Json.write(event));
    assertThat(quarantine.readValue()).contains("Invalid component source");
    assertThat(finals.isEmpty()).isTrue();
  }

  @Test
  void failureOverridesEarlierSuccessWhileActive() {
    request();
    result(Component.FIRST_COST, true);
    result(Component.FIRST_COST, false);
    assertThat(output().state()).isEqualTo(State.FAILED);
  }

  @Test
  void expiredRegistrationCannotCompleteWithBufferedSuccesses() {
    for (Component c : Component.values()) result(c, true);
    driver.advanceWallClockTime(Duration.ofSeconds(11));
    request();
    assertThat(output().state()).isEqualTo(State.FAILED);
  }

  @Test
  void nullValuesDoNotCrashTopology() {
    requests.pipeInput(id.toString(), (String) null);
    assertThat(quarantine.readValue()).contains("Null event key/value");
  }
}

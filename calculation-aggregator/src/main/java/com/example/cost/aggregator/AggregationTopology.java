package com.example.cost.aggregator;

import com.example.cost.contracts.*;
import java.util.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class AggregationTopology {
  public static final String STORE = "calculation-state-v1";
  public static final String DLT = "cost.calculation.aggregation.dlt";

  @Bean
  public KStream<String, String> calculationStream(StreamsBuilder builder) {
    return configure(builder);
  }

  public static KStream<String, String> configure(StreamsBuilder builder) {
    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(STORE), Serdes.String(), Serdes.String()));
    var topics = new ArrayList<String>();
    topics.add(Topics.REQUESTED);
    for (Component c : Component.values()) topics.add(c.resultTopic());
    KStream<String, String> output =
        builder.stream(topics, Consumed.with(Serdes.String(), Serdes.String()))
            .process(AggregationProcessor::new, Named.as("aggregate"), STORE);
    output
        .filter((key, value) -> !Json.tree(value).has("quarantine"))
        .to(Topics.FINAL, Produced.with(Serdes.String(), Serdes.String()));
    output
        .filter((key, value) -> Json.tree(value).has("quarantine"))
        .to(DLT, Produced.with(Serdes.String(), Serdes.String()));
    return output;
  }

  public static Topology topology() {
    var builder = new StreamsBuilder();
    configure(builder);
    return builder.build();
  }
}

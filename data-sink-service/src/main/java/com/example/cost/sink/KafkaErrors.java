package com.example.cost.sink;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrors {
  @Bean
  DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafka) {
    var recoverer =
        new DeadLetterPublishingRecoverer(
            kafka,
            (record, error) -> new TopicPartition(record.topic() + ".dlt", record.partition()));
    recoverer.setFailIfSendResultIsError(true);
    var handler =
        new DefaultErrorHandler(recoverer, new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
    handler.addNotRetryableExceptions(IllegalArgumentException.class);
    return handler;
  }
}

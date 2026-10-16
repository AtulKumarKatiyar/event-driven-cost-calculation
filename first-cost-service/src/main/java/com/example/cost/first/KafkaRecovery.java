package com.example.cost.first;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaRecovery {
  @Bean
  DefaultAfterRollbackProcessor<String, String> afterRollbackProcessor(
      KafkaTemplate<String, String> kafka) {
    var recoverer =
        new DeadLetterPublishingRecoverer(
            kafka,
            (record, error) -> new TopicPartition(record.topic() + ".dlt", record.partition()));
    recoverer.setFailIfSendResultIsError(true);
    var processor =
        new DefaultAfterRollbackProcessor<String, String>(
            recoverer, new FixedBackOff(1000, 2), kafka, true);
    processor.addNotRetryableExceptions(IllegalArgumentException.class);
    return processor;
  }
}

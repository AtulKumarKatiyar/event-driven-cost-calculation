package com.example.cost.command;

import com.example.cost.contracts.*;
import com.example.cost.contracts.Events.*;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class OutboxRelay {
  private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
  private final JdbcTemplate db;
  private final KafkaTemplate<String, String> kafka;
  private final TransactionTemplate tx;

  public OutboxRelay(
      JdbcTemplate db, KafkaTemplate<String, String> kafka, PlatformTransactionManager manager) {
    this.db = db;
    this.kafka = kafka;
    this.tx = new TransactionTemplate(manager);
  }

  @Scheduled(fixedDelayString = "${calculation.outbox-delay-ms:500}")
  public void publish() {
    try {
      tx.executeWithoutResult(
          status -> {
            // One relay batch at a time across replicas; preserves dispatch/lifecycle ordering.
            Boolean locked =
                db.queryForObject("SELECT pg_try_advisory_xact_lock(725091)", Boolean.class);
            if (!Boolean.TRUE.equals(locked)) return;
            var rows =
                db.queryForList(
                    "SELECT id,calculation_id,topic,payload FROM outbox WHERE published_at IS NULL"
                        + " ORDER BY id LIMIT 50 FOR UPDATE");
            for (var row : rows) {
              try {
                kafka
                    .send(
                        (String) row.get("topic"),
                        row.get("calculation_id").toString(),
                        (String) row.get("payload"))
                    .get(10, TimeUnit.SECONDS);
              } catch (Exception e) {
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                throw new IllegalStateException("Kafka publish failed", e);
              }
              db.update("UPDATE outbox SET published_at=now() WHERE id=?", row.get("id"));
            }
            var dispatched =
                db.queryForList(
                    "SELECT calculation_id FROM calculations c WHERE state='REQUESTED' AND NOT"
                        + " EXISTS (SELECT 1 FROM outbox o WHERE o.calculation_id=c.calculation_id"
                        + " AND o.published_at IS NULL) FOR UPDATE");
            for (var row : dispatched) {
              UUID id = (UUID) row.get("calculation_id");
              db.update(
                  "UPDATE calculations SET state='IN_PROGRESS',updated_at=now() WHERE"
                      + " calculation_id=?",
                  id);
              var event =
                  Events.event(
                      "CalculationLifecycleEvent", id, new Lifecycle(State.IN_PROGRESS, 2));
              db.update(
                  "INSERT INTO outbox(calculation_id,topic,payload) VALUES (?,?,?)",
                  id,
                  Topics.LIFECYCLE,
                  Json.write(event));
            }
          });
    } catch (Exception e) {
      log.warn("Outbox batch will be retried: {}", e.getMessage());
    }
  }
}

package com.example.cost.command;

import com.example.cost.contracts.*;
import com.example.cost.contracts.Events.*;
import java.time.Instant;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class CommandStore {
  private final JdbcTemplate db;
  private final String currency;
  private final long deadlineSeconds;

  public CommandStore(
      JdbcTemplate db,
      @Value("${calculation.currency:INR}") String currency,
      @Value("${calculation.deadline-seconds:60}") long deadlineSeconds) {
    this.db = db;
    this.currency = currency;
    this.deadlineSeconds = deadlineSeconds;
  }

  public record Accepted(UUID calculationId, State state, boolean replay) {}

  @Transactional
  public Accepted submit(CalculationRequest raw) {
    CalculationRequest request = raw.normalized();
    String body = Json.write(request);
    var existing = find(request.calculationId(), body);
    if (existing != null) return existing;
    db.update(
        "INSERT INTO calculations(calculation_id,rp,mp,vbu,finance_group,request_json,state) VALUES"
            + " (?,?,?,?,?,?,'REQUESTED')",
        request.calculationId(),
        request.rp(),
        request.mp(),
        request.vbu(),
        request.financeGroup(),
        body);
    var event =
        Events.event(
            "CalculationRequestedEvent",
            request.calculationId(),
            new Requested(request, currency, Instant.now().plusSeconds(deadlineSeconds)));
    enqueue(Topics.REQUESTED, event);
    for (Component component : Component.values())
      enqueue(component.requestTopic(), event);
    enqueue(
        Topics.LIFECYCLE,
        Events.event(
            "CalculationLifecycleEvent",
            request.calculationId(),
            new Lifecycle(State.REQUESTED, 1)));
    return new Accepted(request.calculationId(), State.REQUESTED, false);
  }

  public Accepted find(UUID id, String body) {
    var rows =
        db.queryForList("SELECT request_json,state FROM calculations WHERE calculation_id=?", id);
    if (rows.isEmpty()) return null;
    if (!body.equals(rows.getFirst().get("request_json")))
      throw new ResponseStatusException(
          HttpStatus.CONFLICT, "calculationId already belongs to a different request");
    return new Accepted(id, State.valueOf((String) rows.getFirst().get("state")), true);
  }

  private void enqueue(String topic, Event<?> event) {
    db.update(
        "INSERT INTO outbox(calculation_id,topic,payload) VALUES (?,?,?)",
        event.calculationId(),
        topic,
        Json.write(event));
  }

  @Transactional
  public void finish(Event<Final> event) {
    Validation.finalEvent(event);
    Final value = event.payload();
    if (value.state() == null || !value.state().terminal())
      throw new IllegalArgumentException("Expected terminal state");
    if (db.update("INSERT INTO inbox(event_id) VALUES (?) ON CONFLICT DO NOTHING", event.eventId())
        == 0) return;
    var rows =
        db.queryForList(
            "SELECT state FROM calculations WHERE calculation_id=? FOR UPDATE",
            event.calculationId());
    if (rows.isEmpty()) throw new IllegalStateException("Final event precedes command record");
    State current = State.valueOf((String) rows.getFirst().get("state"));
    if (current.terminal() && current != value.state())
      throw new IllegalArgumentException("Conflicting terminal event");
    db.update(
        "UPDATE calculations SET state=?,updated_at=now() WHERE calculation_id=? AND state IN"
            + " ('REQUESTED','IN_PROGRESS')",
        value.state().name(),
        event.calculationId());
  }
}

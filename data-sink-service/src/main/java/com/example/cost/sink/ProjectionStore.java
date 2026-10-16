package com.example.cost.sink;

import com.example.cost.contracts.*;
import com.example.cost.contracts.Events.*;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ProjectionStore {
  private final JdbcTemplate db;

  public ProjectionStore(JdbcTemplate db) {
    this.db = db;
  }

  private boolean accept(UUID id) {
    return db.update("INSERT INTO inbox(event_id) VALUES (?) ON CONFLICT DO NOTHING", id) == 1;
  }

  @Transactional
  public void requested(Event<Requested> event) {
    if (!accept(event.eventId())) return;
    db.update(
        "INSERT INTO calculation_views(calculation_id,request_json,state,state_version) VALUES"
            + " (?,?,'REQUESTED',1) ON CONFLICT(calculation_id) DO UPDATE SET"
            + " request_json=COALESCE(calculation_views.request_json,EXCLUDED.request_json),updated_at=now()",
        event.calculationId(),
        Json.write(event.payload().request()));
  }

  @Transactional
  public void lifecycle(Event<Lifecycle> event) {
    var value = event.payload();
    if (value.state() == null || value.state().terminal() || value.stateVersion() < 1)
      throw new IllegalArgumentException("Invalid lifecycle");
    if (!accept(event.eventId())) return;
    db.update(
        "INSERT INTO calculation_views(calculation_id,state,state_version) VALUES (?,?,?) ON"
            + " CONFLICT(calculation_id) DO UPDATE SET"
            + " state=EXCLUDED.state,state_version=EXCLUDED.state_version,updated_at=now() WHERE"
            + " calculation_views.final_json IS NULL AND"
            + " calculation_views.state_version<EXCLUDED.state_version",
        event.calculationId(),
        value.state().name(),
        value.stateVersion());
  }

  @Transactional
  public void finish(Event<Final> event) {
    Validation.finalEvent(event);
    if (event.payload().state() == null || !event.payload().state().terminal())
      throw new IllegalArgumentException("Invalid final state");
    if (!accept(event.eventId())) return;
    db.update(
        "INSERT INTO calculation_views(calculation_id,state,final_json) VALUES (?,?,?) ON"
            + " CONFLICT(calculation_id) DO NOTHING",
        event.calculationId(),
        event.payload().state().name(),
        Json.write(event.payload()));
    var rows =
        db.queryForList(
            "SELECT final_json FROM calculation_views WHERE calculation_id=? FOR UPDATE",
            event.calculationId());
    Object old = rows.getFirst().get("final_json");
    if (old != null && !Json.read(old.toString(), Final.class).equals(event.payload()))
      throw new IllegalArgumentException("Conflicting final event");
    db.update(
        "UPDATE calculation_views SET state=?,final_json=?,updated_at=now() WHERE calculation_id=?"
            + " AND final_json IS NULL",
        event.payload().state().name(),
        Json.write(event.payload()),
        event.calculationId());
  }
}

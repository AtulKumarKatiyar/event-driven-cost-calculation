package com.example.cost.query;

import com.example.cost.contracts.Json;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/calculations")
public class QueryController {
  private final JdbcTemplate db;

  public QueryController(JdbcTemplate db) {
    this.db = db;
  }

  @GetMapping("/{id}")
  public Map<String, Object> get(@PathVariable UUID id) {
    var rows = db.queryForList("SELECT * FROM calculation_views WHERE calculation_id=?", id);
    if (rows.isEmpty())
      throw new ResponseStatusException(
          HttpStatus.NOT_FOUND, "Calculation not projected yet or unknown");
    var row = rows.getFirst();
    var result = new LinkedHashMap<String, Object>();
    result.put("calculationId", id);
    result.put("state", row.get("state"));
    result.put(
        "request",
        row.get("request_json") == null ? null : Json.tree(row.get("request_json").toString()));
    result.put(
        "result",
        row.get("final_json") == null ? null : Json.tree(row.get("final_json").toString()));
    result.put("updatedAt", row.get("updated_at").toString());
    return result;
  }
}

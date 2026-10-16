package com.example.cost.command;

import com.example.cost.contracts.*;
import jakarta.validation.Valid;
import java.net.URI;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/calculations")
public class CommandController {
  private final CommandStore store;

  public CommandController(CommandStore store) {
    this.store = store;
  }

  @PostMapping
  public ResponseEntity<CommandStore.Accepted> submit(
      @Valid @RequestBody CalculationRequest request) {
    CommandStore.Accepted accepted;
    try {
      accepted = store.submit(request);
    } catch (DuplicateKeyException conflict) {
      accepted = store.find(request.calculationId(), Json.write(request.normalized()));
      if (accepted == null)
        throw new ResponseStatusException(
            HttpStatus.CONFLICT, "An active calculation already owns this business key");
    }
    return ResponseEntity.status(accepted.replay() ? HttpStatus.OK : HttpStatus.ACCEPTED)
        .location(URI.create("/api/calculations/" + accepted.calculationId()))
        .body(accepted);
  }
}

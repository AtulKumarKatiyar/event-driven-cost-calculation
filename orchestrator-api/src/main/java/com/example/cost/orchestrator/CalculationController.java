package com.example.cost.orchestrator;

import com.example.cost.contracts.CalculationRequest;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.*;

@RestController
@RequestMapping("/api/calculations")
public class CalculationController {
  private final RestClient command, query;

  public CalculationController(
      RestClient.Builder builder,
      @Value("${services.command-url:http://localhost:8081}") String commandUrl,
      @Value("${services.query-url:http://localhost:8082}") String queryUrl) {
    var factory = new SimpleClientHttpRequestFactory();
    factory.setConnectTimeout(3000);
    factory.setReadTimeout(10000);
    command = builder.clone().requestFactory(factory).baseUrl(commandUrl).build();
    query = builder.clone().requestFactory(factory).baseUrl(queryUrl).build();
  }

  @PostMapping
  public ResponseEntity<String> submit(@Valid @RequestBody CalculationRequest request) {
    return command
        .post()
        .uri("/api/calculations")
        .body(request)
        .exchange(
            (req, res) -> {
              var response =
                  ResponseEntity.status(res.getStatusCode())
                      .contentType(MediaType.APPLICATION_JSON);
              if (res.getHeaders().getLocation() != null)
                response.location(URI.create("/api/calculations/" + request.calculationId()));
              return response.body(
                  new String(
                      res.getBody().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
            });
  }

  @GetMapping("/{id}")
  public ResponseEntity<String> get(@PathVariable UUID id) {
    return query
        .get()
        .uri("/api/calculations/{id}", id)
        .exchange(
            (req, res) ->
                ResponseEntity.status(res.getStatusCode())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(
                        new String(
                            res.getBody().readAllBytes(),
                            java.nio.charset.StandardCharsets.UTF_8)));
  }

  @ExceptionHandler(ResourceAccessException.class)
  public ResponseEntity<ProblemDetail> unavailable(ResourceAccessException exception) {
    return ResponseEntity.status(503)
        .body(
            ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE,
                "Downstream service unavailable; retry using the same calculationId"));
  }
}

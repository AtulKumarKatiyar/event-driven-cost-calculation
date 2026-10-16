package com.example.cost.orchestrator;

import static org.assertj.core.api.Assertions.*;

import com.example.cost.contracts.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;
import org.springframework.web.client.RestClient;

class CalculationControllerTest {
  HttpServer server;
  CalculationController controller;
  AtomicReference<String> received = new AtomicReference<>();

  @BeforeEach
  void setup() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.start();
    String url = "http://127.0.0.1:" + server.getAddress().getPort();
    controller = new CalculationController(RestClient.builder(), url, url);
  }

  @AfterEach
  void close() {
    server.stop(0);
  }

  void response(int code, String body) {
    server.createContext(
        "/api/calculations",
        exchange -> {
          received.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          if (code == 202)
            exchange
                .getResponseHeaders()
                .set("Location", "http://internal-only/api/calculations/id");
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(code, bytes.length);
          try (var output = exchange.getResponseBody()) {
            output.write(bytes);
          }
        });
  }

  CalculationRequest request() {
    return new CalculationRequest(UUID.randomUUID(), "R", "M", "V", "F", "P", 2);
  }

  @Test
  void forwardsCommandAndReturnsPublicLocation() {
    response(202, "{\"state\":\"REQUESTED\"}");
    var request = request();
    var result = controller.submit(request);
    assertThat(result.getStatusCode().value()).isEqualTo(202);
    assertThat(result.getHeaders().getLocation().toString())
        .isEqualTo("/api/calculations/" + request.calculationId());
    assertThat(Json.tree(received.get()).get("quantity").asInt()).isEqualTo(2);
  }

  @Test
  void preservesBusinessConflict() {
    response(409, "{\"detail\":\"Active business key\"}");
    var result = controller.submit(request());
    assertThat(result.getStatusCode().value()).isEqualTo(409);
    assertThat(result.getBody()).contains("Active business key");
  }

  @Test
  void preservesNotYetProjectedStatus() {
    response(404, "{\"detail\":\"Not projected\"}");
    assertThat(controller.get(UUID.randomUUID()).getStatusCode().value()).isEqualTo(404);
  }
}

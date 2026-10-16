package com.example.cost.command;

import static org.assertj.core.api.Assertions.*;

import com.example.cost.contracts.*;
import com.example.cost.contracts.Events.*;
import java.time.Instant;
import java.util.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;

@Testcontainers(disabledWithoutDocker = true)
@SpringJUnitConfig(CommandStoreTest.Config.class)
class CommandStoreTest {
  @Container static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17.6");

  @Configuration
  @EnableTransactionManagement
  static class Config {
    @Bean
    javax.sql.DataSource dataSource() {
      var ds =
          new DriverManagerDataSource(
              postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
      Flyway.configure().dataSource(ds).load().migrate();
      return ds;
    }

    @Bean
    JdbcTemplate jdbc(javax.sql.DataSource ds) {
      return new JdbcTemplate(ds);
    }

    @Bean
    PlatformTransactionManager transactionManager(javax.sql.DataSource ds) {
      return new DataSourceTransactionManager(ds);
    }

    @Bean
    CommandStore store(JdbcTemplate db) {
      return new CommandStore(db, "INR", 60);
    }
  }

  @Autowired CommandStore store;
  @Autowired JdbcTemplate db;

  CalculationRequest request(UUID id, String key) {
    return new CalculationRequest(id, key, "M", "V", "F", "P", 2);
  }

  @BeforeEach
  void clear() {
    db.execute("TRUNCATE inbox,outbox,calculations CASCADE");
  }

  @Test
  void acceptanceIsIdempotentAndOutboxContainsAllDestinations() {
    UUID id = UUID.randomUUID();
    var request = request(id, "R");
    assertThat(store.submit(request).replay()).isFalse();
    assertThat(store.submit(request).replay()).isTrue();
    assertThat(db.queryForObject("SELECT count(*) FROM outbox", Integer.class)).isEqualTo(6);
    assertThatThrownBy(
            () -> store.submit(new CalculationRequest(id, "R", "M", "V", "F", "OTHER", 2)))
        .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
  }

  @Test
  void activeBusinessKeyIsReleasedOnlyByTerminalState() {
    UUID first = UUID.randomUUID();
    store.submit(request(first, "R"));
    assertThatThrownBy(() -> store.submit(request(UUID.randomUUID(), "R")))
        .isInstanceOf(DuplicateKeyException.class);
    var event =
        Events.event(
            "CalculationFailedEvent",
            first,
            new Final(
                State.FAILED,
                Map.of(),
                null,
                "INR",
                new Events.Error("FAIL", "failed", false),
                Instant.now()));
    store.finish(event);
    store.finish(event);
    assertThat(store.submit(request(UUID.randomUUID(), "R")).replay()).isFalse();
    assertThat(db.queryForObject("SELECT count(*) FROM calculations", Integer.class)).isEqualTo(2);
  }

  @Test
  void concurrentRequestsCannotBothOwnBusinessKey() throws Exception {
    try (var pool = java.util.concurrent.Executors.newFixedThreadPool(2)) {
      var gate = new java.util.concurrent.CountDownLatch(1);
      java.util.concurrent.Callable<Boolean> task =
          () -> {
            gate.await();
            try {
              store.submit(request(UUID.randomUUID(), "R"));
              return true;
            } catch (DuplicateKeyException e) {
              return false;
            }
          };
      var a = pool.submit(task);
      var b = pool.submit(task);
      gate.countDown();
      assertThat(List.of(a.get(), b.get())).containsExactlyInAnyOrder(true, false);
      assertThat(db.queryForObject("SELECT count(*) FROM outbox", Integer.class)).isEqualTo(6);
    }
  }
}

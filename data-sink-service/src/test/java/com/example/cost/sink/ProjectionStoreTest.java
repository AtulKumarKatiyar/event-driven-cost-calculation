package com.example.cost.sink;

import static org.assertj.core.api.Assertions.*;

import com.example.cost.contracts.*;
import com.example.cost.contracts.Events.*;
import java.time.Instant;
import java.util.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;

@Testcontainers(disabledWithoutDocker = true)
@SpringJUnitConfig(ProjectionStoreTest.Config.class)
class ProjectionStoreTest {
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
    ProjectionStore store(JdbcTemplate db) {
      return new ProjectionStore(db);
    }
  }

  @Autowired ProjectionStore store;
  @Autowired JdbcTemplate db;

  @Test
  void terminalProjectionSurvivesLateLifecycleAndDuplicateEvents() {
    UUID id = UUID.randomUUID();
    var end =
        Events.event(
            "CalculationFailedEvent",
            id,
            new Final(
                State.FAILED,
                Map.of(),
                null,
                "INR",
                new Events.Error("FAIL", "failed", false),
                Instant.now()));
    store.finish(end);
    store.finish(end);
    store.lifecycle(
        Events.event("CalculationLifecycleEvent", id, new Lifecycle(State.IN_PROGRESS, 2)));
    store.lifecycle(
        Events.event("CalculationLifecycleEvent", id, new Lifecycle(State.REQUESTED, 1)));
    store.requested(
        Events.event(
            "CalculationRequestedEvent",
            id,
            new Requested(
                new CalculationRequest(id, "R", "M", "V", "F", "P", 2),
                "INR",
                Instant.now().plusSeconds(60))));
    var row = db.queryForMap("SELECT * FROM calculation_views WHERE calculation_id=?", id);
    assertThat(row.get("state")).isEqualTo("FAILED");
    assertThat(row.get("request_json")).isNotNull();
    assertThat(Json.tree(row.get("final_json").toString()).has("totalCost")).isFalse();
  }
}

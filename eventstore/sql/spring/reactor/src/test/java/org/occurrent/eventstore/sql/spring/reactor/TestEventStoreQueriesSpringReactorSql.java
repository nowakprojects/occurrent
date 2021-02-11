package org.occurrent.eventstore.sql.spring.reactor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.sql.common.PostgresSqlEventStoreConfig;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.occurrent.filter.Filter.type;

@Timeout(10)
@Testcontainers
class TestEventStoreQueriesSpringReactorSql implements ReactorEventStoreTestSupport {

  @Container
  private static final PostgreSQLContainer<?> postgreSQLContainer;

  static {
    postgreSQLContainer = new PostgreSQLContainer<>("postgres:13-alpine")
        .withDatabaseName("occurrent")
        .withUsername("occurrent-user")
        .withPassword("occurrent-password")
        .withExposedPorts(5432);
    List<String> ports = new ArrayList<>();
    ports.add("5432:5432");
    postgreSQLContainer.setPortBindings(ports);
  }

  private SpringReactorSqlEventStore eventStore;

  @BeforeEach
  void create_event_store() {
    eventStore = EventStoreFixture
        .connectedTo(postgreSQLContainer)
        .eventStoreInstance(new PostgresSqlEventStoreConfig("occurrent_cloud_events"));
  }

  @Nested
  @DisplayName("count")
  class CountTest {

    @Test
    void count_without_any_filter_returns_all_the_count_of_all_events_in_the_event_store() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
      DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
      DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
      writeEvents(eventStreamId, Arrays.asList(event1, event2, event3));

      // When
      final Mono<Long> allEventsCount = eventStore.count();

      // Then
      StepVerifier.create(allEventsCount)
          .expectNext(3L)
          .verifyComplete();
    }

    @Test
    void count_with_filter_returns_only_events_that_matches_the_filter() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
      DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
      DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
      writeEvents("name", Arrays.asList(event1, event2, event3));

      // When
      final Mono<Long> filteredEventsCount = eventStore.count(type(NameDefined.class.getName()));

      // Then
      StepVerifier.create(filteredEventsCount)
          .expectNext(2L)
          .verifyComplete();
    }
  }

  @Override
  public EventStore eventStore() {
    return this.eventStore;
  }

}

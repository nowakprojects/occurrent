package org.occurrent.eventstore.sql.spring.reactor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.sql.common.PostgresSqlEventStoreConfig;
import org.occurrent.filter.Filter;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;
import static org.occurrent.condition.Condition.lte;
import static org.occurrent.eventstore.sql.spring.reactor.Constants.NAME_SOURCE;
import static org.occurrent.filter.Filter.streamId;
import static org.occurrent.filter.Filter.time;
import static org.occurrent.filter.Filter.type;

@Timeout(10)
@Testcontainers
class TestEventStoreOperationsSpringReactorSql implements ReactorEventStoreTestSupport {

  public static final String EVENT_STORE_TABLE_NAME = "occurrent_cloud_events";
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

  @RegisterExtension
  FlushSqlDbExtension flushSqlDbExtension = new FlushSqlDbExtension(
      postgreSQLContainer.getJdbcUrl(),
      postgreSQLContainer.getUsername(),
      postgreSQLContainer.getPassword(),
      EVENT_STORE_TABLE_NAME
  );

  private SpringReactorSqlEventStore eventStore;

  @BeforeEach
  void create_event_store() {
    eventStore = EventStoreFixture
        .connectedTo(postgreSQLContainer)
        .eventStoreInstance(new PostgresSqlEventStoreConfig(EVENT_STORE_TABLE_NAME));
  }

  @Nested
  @DisplayName("delete stream")
  class DeleteStream {

    @Test
    void deleteEventStream_deletes_all_events_in_event_stream() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1));

      // When
      StepVerifier.create(eventStore.deleteEventStream(eventStreamId))
          .verifyComplete();

      // Then
      thenEventStream(eventStreamId)
          .hasVersion(0)
          .hasNoEvents();

      StepVerifier.create(eventStore.exists(eventStreamId))
          .expectNext(false)
          .verifyComplete();

      StepVerifier.create(eventStore.count(Filter.streamId(eventStreamId)))
          .expectNext(0L)
          .verifyComplete();
    }

  }

  @Nested
  @DisplayName("delete event")
  class DeleteEvent {

    @Test
    void deleteEvent_deletes_only_specific_event_in_event_stream() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1));

      // When
      StepVerifier.create(eventStore.deleteEvent(nameWasChanged1.getEventId(), NAME_SOURCE))
          .verifyComplete();

      // Then
      thenEventStream(eventStreamId)
          .hasVersion(1)
          .hasOnlyEvent(nameDefined);

      StepVerifier.create(eventStore.exists(eventStreamId))
          .expectNext(true)
          .verifyComplete();

      StepVerifier.create(eventStore.count(Filter.streamId(eventStreamId)))
          .expectNext(1L)
          .verifyComplete();
    }

    @Test
    void delete_deletes_events_according_to_the_filter() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      String anotherEventStreamId = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1));

      NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now, "name2");
      writeEvents(anotherEventStreamId, Collections.singletonList(nameDefined2));

      // When
      eventStore.delete(streamId(eventStreamId).and(time(lte(now.atOffset(UTC).plusMinutes(1))))).block();

      // Then
      thenEventStream(eventStreamId)
          .hasOnlyEvent(nameWasChanged1);

      thenEventStream(anotherEventStreamId)
          .hasOnlyEvent(nameDefined2);
    }
  }

  @Override
  public EventStore eventStore() {
    return this.eventStore;
  }

}

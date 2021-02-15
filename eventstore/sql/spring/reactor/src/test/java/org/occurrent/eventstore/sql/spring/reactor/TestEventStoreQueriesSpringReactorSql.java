package org.occurrent.eventstore.sql.spring.reactor;

import com.mongodb.ConnectionString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.sql.common.PostgresSqlEventStoreConfig;
import org.occurrent.filter.Filter;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.condition.Condition.lte;
import static org.occurrent.eventstore.sql.spring.reactor.Constants.NAME_SOURCE;
import static org.occurrent.filter.Filter.streamId;
import static org.occurrent.filter.Filter.time;
import static org.occurrent.filter.Filter.type;

@Timeout(10)
@Testcontainers
class TestEventStoreQueriesSpringReactorSql implements ReactorEventStoreTestSupport {

  public static final String EVENT_STORE_TABLE_NAME = "occurrent_cloud_events";
  @Container
  private static final PostgreSQLContainer<?> postgreSQLContainer;

  static {
    postgreSQLContainer = Containers.postgreSql();
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
  @DisplayName("count events")
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
      String eventStreamId = anStreamId();
      DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
      DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
      DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
      writeEvents(eventStreamId, Arrays.asList(event1, event2, event3));

      // When
      final Mono<Long> filteredEventsCount = eventStore.count(type(NameDefined.class.getName()));

      // Then
      StepVerifier.create(filteredEventsCount)
          .expectNext(2L)
          .verifyComplete();
    }
  }

  @Nested
  @DisplayName("event exists")
  class EventExists {

    @Test
    void returns_false_when_there_are_no_events_in_the_event_store_and_filter_is_all() {
      StepVerifier.create(eventStore.exists(Filter.all()))
          .expectNext(false)
          .verifyComplete();
    }

    @Test
    void returns_true_when_there_are_events_in_the_event_store_and_filter_is_all() {
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
      DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
      DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
      writeEvents(eventStreamId, Arrays.asList(event1, event2, event3));

      StepVerifier.create(eventStore.exists(Filter.all()))
          .expectNext(true)
          .verifyComplete();
    }

    @Test
    void returns_false_when_there_are_no_events_in_the_event_store_and_filter_is_not_all() {
      StepVerifier.create(eventStore.exists(type(NameDefined.class.getName())))
          .expectNext(false)
          .verifyComplete();
    }

    @Test
    void returns_true_when_there_are_matching_events_in_the_event_store_and_filter_not_all() {
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
      DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
      DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
      writeEvents(eventStreamId, Arrays.asList(event1, event2, event3));

      StepVerifier.create(eventStore.exists(type(NameDefined.class.getName())))
          .expectNext(true)
          .verifyComplete();
    }

    @Test
    void returns_false_when_there_events_in_the_event_store_that_doesnt_match_filter() {
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
      DomainEvent event2 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
      writeEvents(eventStreamId, Arrays.asList(event1, event2));

      StepVerifier.create(eventStore.exists(type(NameWasChanged.class.getName())))
          .expectNext(false)
          .verifyComplete();
    }
  }

  @Nested
  @DisplayName("stream exists")
  class StreamExists {

    @Test
    void returns_true_when_stream_exists_and_contains_events() {
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1));

      StepVerifier.create(eventStore.exists(eventStreamId))
          .expectNext(true)
          .verifyComplete();
    }

    @Test
    void returns_false_when_no_events_have_been_persisted_to_stream() {
      String eventStreamId = anStreamId();

      StepVerifier.create(eventStore.exists(eventStreamId))
          .expectNext(false)
          .verifyComplete();
    }
  }

  //TODO: QueriesTest

  @Override
  public EventStore eventStore() {
    return this.eventStore;
  }

}

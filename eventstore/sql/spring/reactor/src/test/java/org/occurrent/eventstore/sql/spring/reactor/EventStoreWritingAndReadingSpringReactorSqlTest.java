package org.occurrent.eventstore.sql.spring.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.domain.Composition;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.Name;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.eventstore.sql.common.PostgresSqlEventStoreConfig;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.time.TimeConversion;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;

@Timeout(10)
@Testcontainers
class EventStoreWritingAndReadingSpringReactorSqlTest {

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

  private static final URI NAME_SOURCE = URI.create("http://name");

  private EventStore eventStore;
  private final LocalDateTime now = LocalDateTime.now();

  @BeforeEach
  void create_event_store() {
    eventStore = EventStoreFixture
        .connectedTo(postgreSQLContainer)
        .eventStoreInstance(new PostgresSqlEventStoreConfig("occurrent_cloud_events"));
  }

  @Test
  void can_read_and_write_single_event() {
    //Given
    String eventStreamId = anStreamId();
    List<DomainEvent> events = anSampleEvent();

    //When
    writeEvents(eventStreamId, events, WriteCondition.streamVersionEq(0));

    //Then
    thenEventStream(eventStreamId)
        .hasVersion(1L)
        .hasOnlyEvents(events);
  }

  @Test
  void can_read_and_write_multiple_events_at_once() {
    //Given
    String eventStreamId = anStreamId();
    List<DomainEvent> events = Composition.chain(Name.defineName(anEventId(), now, "Hello World"), es -> Name.changeName(es, anEventId(), now, "John Doe"));

    //When
    writeEvents(eventStreamId, events, WriteCondition.streamVersionEq(0));

    //Then
    thenEventStream(eventStreamId)
        .hasVersion(2L)
        .hasOnlyEvents(events);
  }

  @Test
  void can_read_and_write_multiple_events_at_different_occasions() {
    //Given
    String eventStreamId = anStreamId();
    NameDefined nameDefined = new NameDefined(anEventId(), now, "name");
    NameWasChanged nameWasChanged1 = new NameWasChanged(anEventId(), now.plusHours(1), "name2");
    NameWasChanged nameWasChanged2 = new NameWasChanged(anEventId(), now.plusHours(2), "name3");

    //When
    writeEvents(eventStreamId, nameDefined, WriteCondition.streamVersionEq(0));
    writeEvents(eventStreamId, nameWasChanged1, WriteCondition.streamVersionEq(1));
    writeEvents(eventStreamId, nameWasChanged2, WriteCondition.streamVersionEq(2));

    //Then
    thenEventStream(eventStreamId)
        .hasVersion(3L)
        .hasOnlyEvents(nameDefined, nameWasChanged1, nameWasChanged2);
  }


  @Test
  void can_read_events_with_skip_and_limit() {
    //Given
    String eventStreamId = anStreamId();
    NameDefined nameDefined = new NameDefined(anEventId(), now, "name");
    NameWasChanged nameWasChanged1 = new NameWasChanged(anEventId(), now.plusHours(1), "name2");
    NameWasChanged nameWasChanged2 = new NameWasChanged(anEventId(), now.plusHours(2), "name3");

    //When
    writeEvents(eventStreamId, nameDefined, WriteCondition.streamVersionEq(0));
    writeEvents(eventStreamId, nameWasChanged1, WriteCondition.streamVersionEq(1));
    writeEvents(eventStreamId, nameWasChanged2, WriteCondition.streamVersionEq(2));

    //Then
    thenEventStream(eventStore.read(eventStreamId, 1, 1))
        .hasVersion(3L)
        .hasOnlyEvents(nameWasChanged1);
  }

  @Test
  void stream_version_is_not_updated_when_event_insertion_fails() {
    //Given
    LocalDateTime now = LocalDateTime.now();
    String eventStreamId = anStreamId();
    List<DomainEvent> events = Composition.chain(Name.defineName(UUID.randomUUID().toString(), now, "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "John Doe"));
    writeEvents(eventStreamId, events, WriteCondition.streamVersionEq(0));

    //When
    StepVerifier.create(persist(eventStreamId, WriteCondition.streamVersionEq(events.size()), events))
        .verifyError(DuplicateCloudEventException.class);

    //Then
    thenEventStream(eventStore.read(eventStreamId))
        .hasVersion(events.size())
        .hasOnlyEvents(events);
  }

  @Test
  void no_events_are_inserted_when_batch_contains_duplicate_events() {
    //Given
    LocalDateTime now = LocalDateTime.now();
    String eventStreamId = anStreamId();

    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name4");

    //When
    StepVerifier.create(persist(eventStreamId, WriteCondition.streamVersionEq(0), Flux.just(nameDefined, nameWasChanged1, nameWasChanged1, nameWasChanged2)))
        .verifyError(DuplicateCloudEventException.class);

    // Then
    thenEventStream(eventStore.read(eventStreamId))
        .notExist();
  }

  @Test
  void no_events_are_inserted_when_batch_contains_event_that_has_already_been_persisted() {
    //Given
    LocalDateTime now = LocalDateTime.now();
    String eventStreamId = anStreamId();

    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name4");

    writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1), WriteCondition.streamVersionEq(0));

    // When
    StepVerifier.create(persist(eventStreamId, WriteCondition.streamVersionEq(2), Flux.just(nameWasChanged2, nameWasChanged1)))
        .verifyError(DuplicateCloudEventException.class);

    // Then
    thenEventStream(eventStore.read(eventStreamId))
        .hasVersion(2)
        .hasOnlyEvents(nameDefined, nameWasChanged1);
  }

  //https://www.youtube.com/watch?v=8fVw-XzkW1E
  //TODO: read_skew_is_avoided_and_transaction_is_started
  //TODO: read_skew_is_avoided_and_skip_and_limit_is_defined_even_when_no_transaction_is_started

  private void writeEvents(String eventStreamId, List<DomainEvent> events) {
    writeEvents(eventStreamId, events, WriteCondition.anyStreamVersion());
  }

  private void writeEvents(String eventStreamId, List<DomainEvent> events, WriteCondition writeCondition) {
    StepVerifier.create(persist(eventStreamId, writeCondition, events))
        .verifyComplete();
  }

  private void writeEvents(String eventStreamId, DomainEvent events, WriteCondition writeCondition) {
    StepVerifier.create(persist(eventStreamId, writeCondition, Collections.singletonList(events)))
        .verifyComplete();
  }

  private EventStreamAssertions thenEventStream(String eventStreamId) {
    return EventStreamAssertions.thenEventStream(this.eventStore.read(eventStreamId));
  }

  private EventStreamAssertions thenEventStream(Mono<EventStream<CloudEvent>> eventStream) {
    return EventStreamAssertions.thenEventStream(eventStream);
  }

  private List<DomainEvent> anSampleEvent() {
    return Name.defineName(anEventId(), now, "John Doe");
  }

  private String anStreamId() {
    return UUID.randomUUID().toString();
  }

  private String anEventId() {
    return UUID.randomUUID().toString();
  }


  //TODO: Duplication below - same as ReactorMongoEventStore
  private Mono<Void> persist(String eventStreamId, CloudEvent event) {
    return eventStore.write(eventStreamId, Flux.just(event));
  }

  private Mono<Void> persist(String eventStreamId, DomainEvent event) {
    return eventStore.write(eventStreamId, Flux.just(convertDomainEventCloudEvent(event)));
  }

  private Mono<Void> persist(String eventStreamId, Flux<DomainEvent> events) {
    return eventStore.write(eventStreamId, events.map(this::convertDomainEventCloudEvent));
  }

  private Mono<Void> persist(String eventStreamId, List<DomainEvent> events) {
    return persist(eventStreamId, Flux.fromIterable(events));
  }

  private Mono<Void> persist(String eventStreamId, WriteCondition writeCondition, DomainEvent event) {
    List<DomainEvent> events = new ArrayList<>();
    events.add(event);
    return persist(eventStreamId, writeCondition, events);
  }

  private Mono<Void> persist(String eventStreamId, WriteCondition writeCondition, List<DomainEvent> events) {
    return persist(eventStreamId, writeCondition, Flux.fromIterable(events));
  }

  private Mono<Void> persist(String eventStreamId, WriteCondition writeCondition, Flux<DomainEvent> events) {
    return eventStore.write(eventStreamId, writeCondition, events.map(this::convertDomainEventCloudEvent));
  }

  private CloudEvent convertDomainEventCloudEvent(DomainEvent domainEvent) {
    return CloudEventBuilder.v1()
        .withId(domainEvent.getEventId())
        .withSource(NAME_SOURCE)
        .withType(domainEvent.getClass().getName())
        .withTime(TimeConversion.toLocalDateTime(domainEvent.getTimestamp()).atOffset(UTC))
        .withSubject(domainEvent.getClass().getSimpleName().substring(4)) // Defined or WasChanged
        .withDataContentType("application/json")
        .withData(serializeEvent(domainEvent))
        .build();
  }

  private byte[] serializeEvent(DomainEvent domainEvent) {
    ObjectMapper objectMapper = new ObjectMapper();
    return CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(domainEvent);
  }

}

/*
abstract class AbstractR2dbcConfiguration {

  public DatabaseClient databaseClient(ConnectionFactory connectionFactory) {
    return DatabaseClient.builder()
        .connectionFactory(connectionFactory)
        .bindMarkers(DialectResolver.getDialect(connectionFactory).getBindMarkersFactory())
        .build();
  }

  public ConnectionPool connectionFactory() {
    ConnectionFactory connectionFactory = createConnectionFactory(toSpringDataAutoConfigurationProperties(r2dbcProperties()));
    R2dbcProperties.Pool pool = r2dbcProperties().getPool();
    ConnectionPoolConfiguration.Builder builder = ConnectionPoolConfiguration.builder(connectionFactory)
        .name(pool.getName())
        .maxSize(pool.getMaxSize())
        .initialSize(pool.getInitialSize())
        .maxIdleTime(pool.getMaxIdleTime())
        .maxLifeTime(pool.getMaxLifeTime())
        .maxAcquireTime(pool.getMaxAcquireTime())
        .maxCreateConnectionTime(pool.getMaxCreateConnectionTime());
    if (StringUtils.hasText(pool.getValidationQuery())) {
      builder.validationQuery(pool.getValidationQuery());
    }
    return new ConnectionPool(builder.build());
  }

  abstract R2dbcProperties r2dbcProperties();

  ReactiveTransactionManager reactiveTransactionManager(ConnectionFactory connectionFactory) {
    return new R2dbcTransactionManager(connectionFactory);
  }

  private static ConnectionFactory createConnectionFactory(R2dbcProperties properties) {
    return ConnectionFactoryBuilder.of(properties, () -> EmbeddedDatabaseConnection.NONE).build();
  }

  private static R2dbcProperties toSpringDataAutoConfigurationProperties(R2dbcProperties R2dbcProperties) {
    var prop = new R2dbcProperties();
    prop.setName(R2dbcProperties.getName());
    prop.setGenerateUniqueName(R2dbcProperties.isGenerateUniqueName());
    prop.setUrl(R2dbcProperties.getUrl());
    prop.setUsername(R2dbcProperties.getUsername());
    prop.setPassword(R2dbcProperties.getPassword());
    prop.getProperties().putAll(R2dbcProperties.getProperties());
    var poolProp = prop.getPool();
    var PoolProp = R2dbcProperties.getPool();
    poolProp.setInitialSize(PoolProp.getInitialSize());
    poolProp.setMaxIdleTime(PoolProp.getMaxIdleTime());
    poolProp.setMaxSize(PoolProp.getMaxSize());
    poolProp.setValidationQuery(PoolProp.getValidationQuery());
    return prop;
  }
}
 */

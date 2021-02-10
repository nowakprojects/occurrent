package org.occurrent.eventstore.sql.spring.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.Name;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.eventstore.sql.common.PostgresSqlEventStoreConfig;
import org.occurrent.eventstore.sql.common.SqlEventStoreConfig;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.time.TimeConversion;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@Timeout(10)
@Testcontainers
class SpringReactorSqlEventStoreTest {

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
  private ObjectMapper objectMapper;

  private EventStore eventStore;

  @BeforeEach
  void create_event_store() {
    objectMapper = new ObjectMapper();
    ConnectionFactory connectionFactory = new PostgresqlConnectionFactory(
        PostgresqlConnectionConfiguration.builder()
            .host(postgreSQLContainer.getHost())
            .database(postgreSQLContainer.getDatabaseName())
            .username(postgreSQLContainer.getUsername())
            .password(postgreSQLContainer.getPassword())
            .build()
    );
    //DatabaseClient databaseClient = DatabaseClient.builder()
    //    .connectionFactory(connectionFactory)
    //    .bindMarkers(DialectResolver.getDialect(connectionFactory).getBindMarkersFactory())
    //    .build();

    DatabaseClient databaseClient = DatabaseClient.builder()
        .connectionFactory(connectionFactory)
        //.bindMarkers(() -> BindMarkersFactory.named(":", "", 20).create())
        .namedParameters(true)
        .build();
    ReactiveTransactionManager reactiveTransactionManager = new R2dbcTransactionManager(connectionFactory);
    SqlEventStoreConfig sqlEventStoreConfig = new PostgresSqlEventStoreConfig("occurrent_cloud_events");
    eventStore = new SpringReactorSqlEventStore(databaseClient, reactiveTransactionManager, sqlEventStoreConfig);
  }

  @Test
  void can_read_and_write_single_event() {
    LocalDateTime now = LocalDateTime.now();

    // When
    List<DomainEvent> events = Name.defineName(UUID.randomUUID().toString(), now, "John Doe");
    persist("name", WriteCondition.streamVersionEq(0), events).block();

    //Then
    Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
    final EventStream<CloudEvent> block = eventStream.block();
    System.out.println(block);
    //VersionAndEvents versionAndEvents = deserialize(eventStream);

    //assertAll(
    //    () -> assertThat(versionAndEvents.version).isEqualTo(1),
    //    () -> assertThat(versionAndEvents.events).hasSize(1),
    //    () -> assertThat(versionAndEvents.events).containsExactlyElementsOf(events)
    //);
  }

  private VersionAndEvents deserialize(Mono<EventStream<CloudEvent>> eventStreamMono) {
    return eventStreamMono
        .map(es -> {
          List<DomainEvent> events = es.events()
              .map(deserialize())
              .toStream()
              .collect(Collectors.toList());
          return new VersionAndEvents(es.version(), events);
        })
        .block();
  }

  private List<DomainEvent> deserialize(Flux<CloudEvent> flux) {
    return flux.map(deserialize()).toStream().collect(Collectors.toList());
  }

  private Function<CloudEvent, DomainEvent> deserialize() {
    return CheckedFunction.unchecked(this::deserialize);
  }

  @SuppressWarnings({"unchecked", "ConstantConditions"})
  private <T extends DomainEvent> T deserialize(CloudEvent cloudEvent) {
    try {
      return (T) objectMapper.readValue(cloudEvent.getData().toBytes(), Class.forName(cloudEvent.getType()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class VersionAndEvents {
    private final long version;
    private final List<DomainEvent> events;

    VersionAndEvents(long version, List<DomainEvent> events) {
      this.version = version;
      this.events = events;
    }

    @Override
    public String toString() {
      return "VersionAndEvents{" +
          "version=" + version +
          ", events=" + events +
          '}';
    }
  }

  //TODO: Duplication below - same as ReactorMongoEventStore
  private static String streamIdOf(Mono<EventStream<CloudEvent>> eventStreamMono) {
    return eventStreamMono.map(EventStream::id).block();
  }


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
    return CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(domainEvent);
  }

  private static void await(CountDownLatch countDownLatch) {
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
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

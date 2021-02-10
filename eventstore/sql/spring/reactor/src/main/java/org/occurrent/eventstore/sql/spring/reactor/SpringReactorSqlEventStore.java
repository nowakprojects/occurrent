package org.occurrent.eventstore.sql.spring.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Row;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.LongConditionEvaluator;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreOperations;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.eventstore.sql.common.SqlEventStoreConfig;
import org.occurrent.filter.Filter;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.util.StreamUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.occurrent.eventstore.api.WriteCondition.anyStreamVersion;

//https://hantsy.medium.com/introduction-to-r2dbc-82058417644b
//https://medium.com/swlh/working-with-relational-database-using-r2dbc-databaseclient-d61a60ebc67f
//https://hantsy.medium.com/
//https://stackoverflow.com/questions/57971278/connection-pool-size-with-postgres-r2dbc-pool
class SpringReactorSqlEventStore implements EventStore, EventStoreOperations, EventStoreQueries {

  private final DatabaseClient databaseClient;
  private final ReactiveTransactionManager reactiveTransactionManager;
  private final TransactionalOperator transactionalOperator;
  private final SqlEventStoreConfig sqlEventStoreConfig;

  SpringReactorSqlEventStore(DatabaseClient databaseClient, ReactiveTransactionManager reactiveTransactionManager, SqlEventStoreConfig sqlEventStoreConfig) {
    requireNonNull(databaseClient, DatabaseClient.class.getSimpleName() + " cannot be null");
    requireNonNull(reactiveTransactionManager, ReactiveTransactionManager.class.getSimpleName() + " cannot be null");
    requireNonNull(sqlEventStoreConfig, SqlEventStoreConfig.class.getSimpleName() + " cannot be null");

    this.databaseClient = databaseClient;
    this.reactiveTransactionManager = reactiveTransactionManager;
    this.transactionalOperator = TransactionalOperator.create(reactiveTransactionManager);
    this.sqlEventStoreConfig = sqlEventStoreConfig;
    //initializeEventStore(databaseClient, sqlEventStoreConfig).block(); //TODO: Consider move invocation away from constructor
  }

  static Mono<Void> initializeEventStore(DatabaseClient databaseClient, SqlEventStoreConfig sqlEventStoreConfig) {
    return databaseClient.sql(sqlEventStoreConfig.createEventStoreTableSql()).then();
  }

  //.bind("id", Parameter.fromOrEmpty(event.getId(), String.class));
  @Override
  public Mono<Void> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
    String sql = "INSERT INTO " + sqlEventStoreConfig.eventStoreTableName() + " "
        + "(id, source, specversion, type, datacontenttype, dataschema, subject, streamid, streamversion, data, time) VALUES"
        + "(:id, :source, :specversion, :type, :datacontenttype, :dataschema, :subject, :streamid, :streamversion, :data, :time)";
    final Flux<Void> eventsToWrite = currentStreamVersionFulfillsCondition(streamId, writeCondition)
        .flatMapMany(currentStreamVersion ->
            infiniteFluxFrom(currentStreamVersion)
                .zipWith(events)
                .flatMap(streamVersionAndEvent -> {
                  long streamVersion = streamVersionAndEvent.getT1();
                  CloudEvent event = streamVersionAndEvent.getT2();
                  return databaseClient.sql(sql)
                      .bind("id", event.getId())
                      .bind("source", event.getSource())
                      .bind("specversion", event.getSpecVersion().toString())
                      .bind("type", event.getType())
                      .bind("datacontenttype", event.getDataContentType())
                      .bind("dataschema", Parameter.fromOrEmpty(event.getDataSchema(), URI.class))
                      .bind("subject", event.getSubject())
                      .bind(OccurrentCloudEventExtension.STREAM_ID, Parameter.fromOrEmpty(streamId, String.class))
                      .bind(OccurrentCloudEventExtension.STREAM_VERSION, Parameter.fromOrEmpty(streamVersion, String.class))
                      .bind("data", Parameter.fromOrEmpty(event.getData() != null ? event.getData().toBytes() : null, Object.class))
                      .bind("time", event.getTime())
                      .then();
                }));
    return eventsToWrite.as(transactionalOperator::transactional).then();
  }

  //TODO: Remove Duplication - same in ReactorMongoEventStore
  private Mono<Long> currentStreamVersionFulfillsCondition(String streamId, WriteCondition writeCondition) {
    return currentStreamVersion(streamId)
        .flatMap(currentStreamVersion -> {
          final Mono<Long> result;
          if (isFulfilled(currentStreamVersion, writeCondition)) {
            result = Mono.just(currentStreamVersion);
          } else {
            result = Mono.error(new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition.toString(), currentStreamVersion)));
          }
          return result;
        });
  }

  //TODO: Remove Duplication - same in ReactorMongoEventStore
  private static boolean isFulfilled(long streamVersion, WriteCondition writeCondition) {
    if (writeCondition.isAnyStreamVersion()) {
      return true;
    }

    if (!(writeCondition instanceof WriteCondition.StreamVersionWriteCondition)) {
      throw new IllegalArgumentException("Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
    }

    WriteCondition.StreamVersionWriteCondition c = (WriteCondition.StreamVersionWriteCondition) writeCondition;
    return LongConditionEvaluator.evaluate(c.condition, streamVersion);
  }

  //TODO: Remove Duplication - same in ReactorMongoEventStore
  private static Flux<Long> infiniteFluxFrom(Long currentStreamVersion) {
    return Flux.generate(() -> currentStreamVersion, (version, sink) -> {
      long nextVersion = version + 1L;
      sink.next(nextVersion);
      return nextVersion;
    });
  }

  private Mono<Long> currentStreamVersion(String streamId) {
    String sql = "SELECT streamversion FROM " + sqlEventStoreConfig.eventStoreTableName() + " WHERE streamid = :streamid ORDER BY streamversion DESC LIMIT 1";
    //String sql = "SELECT MAX(streamversion) FROM " + sqlEventStoreConfig.eventStoreTableName() + " WHERE streamid = :streamid";


    return databaseClient.sql(sql)
        .bind("streamid", streamId)
        .map(result -> result.get(0, Long.class))
        .one()
        .switchIfEmpty(Mono.just(0L));
  }

  @Override
  public Mono<Void> deleteEventStream(String streamId) {
    return null;
  }

  @Override
  public Mono<Void> deleteEvent(String cloudEventId, URI cloudEventSource) {
    return null;
  }

  @Override
  public Mono<Void> delete(Filter filter) {
    return null;
  }

  @Override
  public Mono<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
    return null;
  }

  @Override
  public Flux<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
    return null;
  }

  @Override
  public Mono<Long> count(Filter filter) {
    return null;
  }

  @Override
  public Mono<Boolean> exists(Filter filter) {
    return null;
  }

  @Override
  public Mono<Boolean> exists(String streamId) {
    return null;
  }

  //https://stackoverflow.com/questions/57942240/how-to-extract-jsonb-from-postgresql-to-spring-webflux-using-r2dbc
  //https://github.com/r2dbc/r2dbc-h2/issues/115
  @Override
  public Mono<EventStream<CloudEvent>> read(String streamId, int skip, int limit) {
    String sql = "SELECT * FROM " + sqlEventStoreConfig.eventStoreTableName() + " WHERE streamid = :streamid ORDER BY streamversion ASC LIMIT :limit OFFSET :offset";
    final Mono<Long> currentStreamVersion = currentStreamVersion(streamId);
    final Flux<CloudEvent> cloudEvents = databaseClient.sql(sql)
        .bind("streamid", streamId)
        .bind("limit", limit)
        .bind("offset", skip)
        .map(SpringReactorSqlEventStore::eventStreamRowMapper)
        .all()
        .flatMap(cloudEventMono -> cloudEventMono);
    return transactionalOperator.execute(ts ->
        currentStreamVersion
            .flatMap(streamVersion -> Mono.just(new EventStreamEntity(streamId, streamVersion, cloudEvents)))
            .defaultIfEmpty(new EventStreamEntity(streamId, 0L, Flux.empty()))
    ).single().map(cloudEvent -> cloudEvent);
  }

  private static Mono<CloudEvent> eventStreamRowMapper(Row row) {
    final String id = row.get("id", String.class);
    final String source = row.get("source", String.class);
    final String specVersion = requireNonNull(row.get("specversion", String.class));
    final String type = row.get("type", String.class);
    final String dataContentType = row.get("datacontenttype", String.class);
    final String dataSchema = row.get("dataschema", String.class);
    final String subject = row.get("subject", String.class);
    final String streamId = row.get(OccurrentCloudEventExtension.STREAM_ID, String.class);
    final Long streamVersion = row.get(OccurrentCloudEventExtension.STREAM_VERSION, Long.class);
    final Blob data = row.get("data", Blob.class);
    final OffsetDateTime time = row.get("time", OffsetDateTime.class);
    CloudEventBuilder cloudEvent = CloudEventBuilder.fromSpecVersion(SpecVersion.parse(specVersion))
        .withId(id)
        .withSource(source != null ? URI.create(source) : null)
        .withType(type)
        .withDataContentType(dataContentType)
        .withDataSchema(dataSchema != null ? URI.create(dataSchema) : null)
        .withTime(time)
        .withSubject(subject);
    if (streamId != null) {
      cloudEvent = cloudEvent.withExtension(OccurrentCloudEventExtension.STREAM_ID, streamId);
    }
    if (streamVersion != null) {
      cloudEvent = cloudEvent.withExtension(OccurrentCloudEventExtension.STREAM_VERSION, streamVersion);
    }
    final Mono<byte[]> eventData = data == null
        ? Mono.empty()
        : Mono.from(data.stream())
        .map(SpringReactorSqlEventStore::toByteArray);
    final CloudEventBuilder finalCloudEvent = cloudEvent;
    return eventData
        .map(bytes -> finalCloudEvent.withData(bytes).build())
        .defaultIfEmpty(finalCloudEvent.build());
  }

  private static byte[] toByteArray(ByteBuffer byteBuffer) {
    byte[] byteArray = new byte[byteBuffer.remaining()];
    byteBuffer.get(byteArray, 0, byteArray.length);
    return byteArray;
  }

  @Override
  public Mono<Void> write(String streamId, Flux<CloudEvent> events) {
    return write(streamId, anyStreamVersion(), events);
  }

  private static class EventStreamEntity implements EventStream<CloudEvent> {
    private final String id;
    private final long version;
    private final Flux<CloudEvent> events;

    EventStreamEntity(String id, long version, Flux<CloudEvent> events) {
      this.id = id;
      this.version = version;
      this.events = events;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public long version() {
      return version;
    }

    @Override
    public Flux<CloudEvent> events() {
      return events;
    }
  }
}

package org.occurrent.eventstore.sql.spring.reactor;

import io.cloudevents.CloudEvent;
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
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

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
    initializeEventStore(databaseClient, sqlEventStoreConfig).block(); //TODO: Consider move invocation away from constructor
  }

  private static Mono<Void> initializeEventStore(DatabaseClient databaseClient, SqlEventStoreConfig sqlEventStoreConfig) {
    return databaseClient.sql(sqlEventStoreConfig.createEventStoreTableSql()).then();
  }

  //.bind("id", Parameter.fromOrEmpty(event.getId(), String.class));
  @Override
  public Mono<Void> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
    String sql = "INSERT INTO " + sqlEventStoreConfig.eventStoreTableName() + " "
        + "(id, source, specversion, type, datacontenttype, dataschema, subject, streamid, streamversion, data, time) VALUES"
        + "(:id, :source, :specversion, :type, :datacontenttype, :dataschema, :subject, :streamid, :streamversion, :data, :time)";
    return currentStreamVersionFulfillsCondition(streamId, writeCondition)
        .flatMapMany(currentStreamVersion ->
            infiniteFluxFrom(currentStreamVersion)
                .zipWith(events)
                .map(streamVersionAndEvent -> {
                  long streamVersion = streamVersionAndEvent.getT1();
                  CloudEvent event = streamVersionAndEvent.getT2();
                  return databaseClient.sql(sql)
                      .bind("id", event.getId())
                      .bind("source", event.getSource())
                      .bind("specversion", event.getSpecVersion())
                      .bind("type", event.getType())
                      .bind("datacontenttype", event.getDataContentType())
                      .bind("dataschema", Parameter.fromOrEmpty(event.getDataSchema(), URI.class))
                      .bind("subject", event.getSubject())
                      .bind(OccurrentCloudEventExtension.STREAM_ID, streamId)
                      .bind(OccurrentCloudEventExtension.STREAM_VERSION, streamVersion)
                      .bind("data", Parameter.fromOrEmpty(event.getData(), Object.class))
                      .bind("time", event.getTime())
                      .then();
                })).reduce(Mono::then).as(transactionalOperator::transactional).then();
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
    String sql = "SELECT streamversion FROM " + sqlEventStoreConfig.eventStoreTableName() + " WHERE streamId = '" + streamId + "' ORDER BY streamversion DESC LIMIT 1";
    return databaseClient.sql(sql)
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

  @Override
  public Mono<EventStream<CloudEvent>> read(String streamId, int skip, int limit) {
    return null;
  }

  @Override
  public Mono<Void> write(String streamId, Flux<CloudEvent> events) {
    return null;
  }

  //private static class EventStreamEntity implements EventStream<> {

  //}
}

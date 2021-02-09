package org.occurrent.eventstore.sql.spring.reactor;

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreOperations;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.eventstore.sql.common.SqlEventStoreConfig;
import org.occurrent.filter.Filter;
import org.springframework.r2dbc.core.DatabaseClient;
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

  @Override
  public Mono<Void> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
    return null;
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
}

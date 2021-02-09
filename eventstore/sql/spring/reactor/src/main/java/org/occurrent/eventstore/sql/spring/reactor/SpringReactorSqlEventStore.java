package org.occurrent.eventstore.sql.spring.reactor;

import io.cloudevents.CloudEvent;
import jdk.jfr.Experimental;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreOperations;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.filter.Filter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.function.Function;

@Experimental
class SpringReactorSqlEventStore implements EventStore, EventStoreOperations, EventStoreQueries {

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

package se.haleby.occurrent.eventstore.mongodb.spring.reactor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.domain.Name;
import se.haleby.occurrent.domain.NameDefined;
import se.haleby.occurrent.domain.NameWasChanged;
import se.haleby.occurrent.eventstore.api.common.WriteCondition;
import se.haleby.occurrent.eventstore.api.common.WriteConditionNotFulfilledException;
import se.haleby.occurrent.eventstore.api.reactor.EventStore;
import se.haleby.occurrent.eventstore.api.reactor.EventStream;
import se.haleby.occurrent.testsupport.mongodb.FlushMongoDBExtension;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.*;
import java.util.stream.Collectors;

import static io.vavr.API.*;
import static io.vavr.Predicates.is;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static se.haleby.occurrent.domain.Composition.chain;
import static se.haleby.occurrent.eventstore.api.common.WriteCondition.Condition.*;
import static se.haleby.occurrent.eventstore.api.common.WriteCondition.streamVersion;
import static se.haleby.occurrent.eventstore.api.common.WriteCondition.streamVersionEq;
import static se.haleby.occurrent.functional.CheckedFunction.unchecked;
import static se.haleby.occurrent.time.TimeConversion.toLocalDateTime;

@Testcontainers
public class SpringReactorMongoEventStoreTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:4.2.7");
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.setPortBindings(ports);
    }

    private EventStore eventStore;

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));
    private ObjectMapper objectMapper;
    private ReactiveMongoTemplate mongoTemplate;
    private ConnectionString connectionString;
    private MongoClient mongoClient;

    @BeforeEach
    void create_mongo_spring_reactive_event_store() {
        connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        objectMapper = new ObjectMapper();
    }

    @DisplayName("when using StreamConsistencyGuarantee with type None")
    @Nested
    class StreamConsistencyGuaranteeNone {

        @BeforeEach
        void create_mongo_spring_reactive_event_store_with_stream_write_consistency_guarantee_none() {
            eventStore = new SpringReactorMongoEventStore(mongoTemplate, connectionString.getCollection(), StreamConsistencyGuarantee.none());
        }

        @Test
        void can_read_and_write_single_event_to_mongo_spring_reactive_event_store() {
            LocalDateTime now = LocalDateTime.now();

            // When
            List<DomainEvent> events = Name.defineName(UUID.randomUUID().toString(), now, "John Doe");
            persist("name", events).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(versionAndEvents.version).isEqualTo(0),
                    () -> assertThat(versionAndEvents.events).hasSize(1),
                    () -> assertThat(versionAndEvents.events).containsExactlyElementsOf(events)
            );
        }

        @Test
        void can_read_and_write_multiple_events_at_once_to_mongo_spring_reactive_event_store() {
            LocalDateTime now = LocalDateTime.now();
            List<DomainEvent> events = chain(Name.defineName(UUID.randomUUID().toString(), now, "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "John Doe"));

            // When
            persist("name", events).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(versionAndEvents.version).isEqualTo(0),
                    () -> assertThat(versionAndEvents.events).hasSize(2),
                    () -> assertThat(versionAndEvents.events).containsExactlyElementsOf(events)
            );
        }

        @Test
        void can_read_and_write_multiple_events_at_different_occasions_to_mongo_spring_reactive_event_store() {
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

            // When
            persist("name", nameDefined).block();
            persist("name", nameWasChanged1).block();
            persist("name", nameWasChanged2).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(versionAndEvents.version).isEqualTo(0),
                    () -> assertThat(versionAndEvents.events).hasSize(3),
                    () -> assertThat(versionAndEvents.events).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2)
            );
        }

        @Test
        void can_read_events_with_skip_and_limit_using_mongo_event_store() {
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

            // When
            persist("name", nameDefined).block();
            persist("name", nameWasChanged1).block();
            persist("name", nameWasChanged2).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name", 1, 1);
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(versionAndEvents.version).isEqualTo(0),
                    () -> assertThat(versionAndEvents.events).hasSize(1),
                    () -> assertThat(versionAndEvents.events).containsExactly(nameWasChanged1)
            );
        }
    }

    @DisplayName("when using StreamConsistencyGuarantee with type transactional")
    @Nested
    class StreamConsistencyGuaranteeTransactional {

        @BeforeEach
        void create_mongo_spring_reactive_event_store_with_stream_write_consistency_guarantee_transactional() {
            ReactiveMongoTransactionManager mongoTransactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
            eventStore = new SpringReactorMongoEventStore(mongoTemplate, connectionString.getCollection(), StreamConsistencyGuarantee.transactional("event-stream-version", mongoTransactionManager));
        }

        @Test
        void can_read_and_write_single_event_to_mongo_spring_reactive_event_store() {
            LocalDateTime now = LocalDateTime.now();

            // When
            List<DomainEvent> events = Name.defineName(UUID.randomUUID().toString(), now, "John Doe");
            persist("name", streamVersionEq(0), events).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(versionAndEvents.version).isEqualTo(1),
                    () -> assertThat(versionAndEvents.events).hasSize(1),
                    () -> assertThat(versionAndEvents.events).containsExactlyElementsOf(events)
            );
        }

        @Test
        void can_read_and_write_multiple_events_at_once_to_mongo_spring_reactive_event_store() {
            LocalDateTime now = LocalDateTime.now();
            List<DomainEvent> events = chain(Name.defineName(UUID.randomUUID().toString(), now, "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "John Doe"));

            // When
            persist("name", streamVersionEq(0), events).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(versionAndEvents.version).isEqualTo(1),
                    () -> assertThat(versionAndEvents.events).hasSize(2),
                    () -> assertThat(versionAndEvents.events).containsExactlyElementsOf(events)
            );
        }

        @Test
        void can_read_and_write_multiple_events_at_different_occasions_to_mongo_spring_reactive_event_store() {
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

            // When
            persist("name", streamVersionEq(0), nameDefined).block();
            persist("name", streamVersionEq(1), nameWasChanged1).block();
            persist("name", streamVersionEq(2), nameWasChanged2).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(versionAndEvents.version).isEqualTo(3),
                    () -> assertThat(versionAndEvents.events).hasSize(3),
                    () -> assertThat(versionAndEvents.events).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2)
            );
        }

        @Test
        void can_read_events_with_skip_and_limit_using_mongo_event_store() {
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

            // When
            persist("name", streamVersionEq(0), nameDefined).block();
            persist("name", streamVersionEq(1), nameWasChanged1).block();
            persist("name", streamVersionEq(2), nameWasChanged2).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name", 1, 1);
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(versionAndEvents.version).isEqualTo(3),
                    () -> assertThat(versionAndEvents.events).hasSize(1),
                    () -> assertThat(versionAndEvents.events).containsExactly(nameWasChanged1)
            );
        }

        @Test
        void stream_version_is_not_updated_when_event_insertion_fails() {
            LocalDateTime now = LocalDateTime.now();
            List<DomainEvent> events = chain(Name.defineName(UUID.randomUUID().toString(), now, "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "John Doe"));

            persist("name", streamVersionEq(0), events).block();

            // When
            Throwable throwable = catchThrowable(() -> persist("name", streamVersionEq(1), events).block());

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(DuplicateKeyException.class),
                    () -> assertThat(versionAndEvents.version).isEqualTo(1),
                    () -> assertThat(versionAndEvents.events).hasSize(2),
                    () -> assertThat(versionAndEvents.events).containsExactlyElementsOf(events)
            );
        }
    }

    @DisplayName("when using StreamConsistencyGuarantee with type transactional annotation")
    @Nested
    class StreamConsistencyGuaranteeTransactionalAnnotation {

        @BeforeEach
        void create_mongo_spring_reactive_event_store_with_stream_write_consistency_guarantee_transactional_annotation() {
            eventStore = new SpringReactorMongoEventStore(mongoTemplate, connectionString.getCollection(), StreamConsistencyGuarantee.transactionalAnnotation("event-stream-version"));
        }

        @Test
        void can_read_and_write_single_event_to_mongo_spring_reactive_event_store() {
            LocalDateTime now = LocalDateTime.now();

            // When
            List<DomainEvent> events = Name.defineName(UUID.randomUUID().toString(), now, "John Doe");
            persist("name", streamVersionEq(0), events).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(versionAndEvents.version).isEqualTo(1),
                    () -> assertThat(versionAndEvents.events).hasSize(1),
                    () -> assertThat(versionAndEvents.events).containsExactlyElementsOf(events)
            );
        }

        @Test
        void can_read_and_write_multiple_events_at_once_to_mongo_spring_reactive_event_store() {
            LocalDateTime now = LocalDateTime.now();
            List<DomainEvent> events = chain(Name.defineName(UUID.randomUUID().toString(), now, "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "John Doe"));

            // When
            persist("name", streamVersionEq(0), events).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(versionAndEvents.version).isEqualTo(1),
                    () -> assertThat(versionAndEvents.events).hasSize(2),
                    () -> assertThat(versionAndEvents.events).containsExactlyElementsOf(events)
            );
        }

        @Test
        void can_read_and_write_multiple_events_at_different_occasions_to_mongo_spring_reactive_event_store() {
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

            // When
            persist("name", streamVersionEq(0), nameDefined).block();
            persist("name", streamVersionEq(1), nameWasChanged1).block();
            persist("name", streamVersionEq(2), nameWasChanged2).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(versionAndEvents.version).isEqualTo(3),
                    () -> assertThat(versionAndEvents.events).hasSize(3),
                    () -> assertThat(versionAndEvents.events).containsExactly(nameDefined, nameWasChanged1, nameWasChanged2)
            );
        }

        @Test
        void can_read_events_with_skip_and_limit_using_mongo_event_store() {
            LocalDateTime now = LocalDateTime.now();
            NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

            // When
            persist("name", streamVersionEq(0), nameDefined).block();
            persist("name", streamVersionEq(1), nameWasChanged1).block();
            persist("name", streamVersionEq(2), nameWasChanged2).block();

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name", 1, 1);
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(versionAndEvents.version).isEqualTo(3),
                    () -> assertThat(versionAndEvents.events).hasSize(1),
                    () -> assertThat(versionAndEvents.events).containsExactly(nameWasChanged1)
            );
        }

        @Test
        void stream_version_is_updated_when_event_insertion_fails_when_no_transaction_is_started() {
            LocalDateTime now = LocalDateTime.now();
            List<DomainEvent> events = chain(Name.defineName(UUID.randomUUID().toString(), now, "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "John Doe"));

            persist("name", streamVersionEq(0), events).block();

            // When
            Throwable throwable = catchThrowable(() -> persist("name", streamVersionEq(1), events).block());

            // Then
            Mono<EventStream<CloudEvent>> eventStream = eventStore.read("name");
            VersionAndEvents versionAndEvents = deserialize(eventStream);

            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(DuplicateKeyException.class),
                    () -> assertThat(versionAndEvents.version).isEqualTo(2),
                    () -> assertThat(versionAndEvents.events).hasSize(2),
                    () -> assertThat(versionAndEvents.events).containsExactlyElementsOf(events)
            );
        }
    }


    @Nested
    @DisplayName("Conditionally Write to Mongo Event Store")
    class ConditionallyWriteToSpringMongoEventStore {

        LocalDateTime now = LocalDateTime.now();

        @BeforeEach
        void initialize_event_store() {
            ReactiveMongoTransactionManager mongoTransactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
            eventStore = new SpringReactorMongoEventStore(mongoTemplate, connectionString.getCollection(), StreamConsistencyGuarantee.transactional("event-stream-version", mongoTransactionManager));
        }

        @Nested
        @DisplayName("eq")
        class Eq {

            @Test
            void writes_events_when_stream_version_matches_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");

                eventStore.read("name").flatMap(eventStream1 -> persist(eventStream1.id(), streamVersionEq(eventStream1.version()), event2)).block();

                // Then
                Mono<EventStream<CloudEvent>> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2).events).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_does_not_match_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersionEq(10), event2).block());

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be equal to 10 but was 1.");
            }
        }

        @Nested
        @DisplayName("ne")
        class Ne {

            @Test
            void writes_events_when_stream_version_does_not_match_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Mono<EventStream<CloudEvent>> eventStream1 = eventStore.read("name");
                persist(streamIdOf(eventStream1), streamVersion(ne(20L)), event2).block();

                // Then
                Mono<EventStream<CloudEvent>> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2).events).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_match_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(ne(1L)), event2).block());

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to not be equal to 1 but was 1.");
            }
        }

        @Nested
        @DisplayName("lt")
        class Lt {

            @Test
            void writes_events_when_stream_version_is_less_than_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Mono<EventStream<CloudEvent>> eventStream1 = eventStore.read("name");
                persist(streamIdOf(eventStream1), streamVersion(lt(10L)), event2).block();

                // Then
                Mono<EventStream<CloudEvent>> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2).events).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_is_greater_than_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(lt(0L)), event2).block());

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be less than 0 but was 1.");
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_is_equal_to_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(lt(1L)), event2).block());

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be less than 1 but was 1.");
            }
        }

        @Nested
        @DisplayName("gt")
        class Gt {

            @Test
            void writes_events_when_stream_version_is_greater_than_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Mono<EventStream<CloudEvent>> eventStream1 = eventStore.read("name");
                persist(streamIdOf(eventStream1), streamVersion(gt(0L)), event2).block();

                // Then
                Mono<EventStream<CloudEvent>> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2).events).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_is_less_than_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(gt(100L)), event2).block());

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than 100 but was 1.");
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_is_equal_to_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(gt(1L)), event2).block());

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than 1 but was 1.");
            }
        }

        @Nested
        @DisplayName("lte")
        class Lte {

            @Test
            void writes_events_when_stream_version_is_less_than_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Mono<EventStream<CloudEvent>> eventStream1 = eventStore.read("name");
                persist(streamIdOf(eventStream1), streamVersion(lte(10L)), event2).block();

                // Then
                Mono<EventStream<CloudEvent>> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2).events).containsExactly(event1, event2);
            }


            @Test
            void writes_events_when_stream_version_is_equal_to_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Mono<EventStream<CloudEvent>> eventStream1 = eventStore.read("name");
                persist(streamIdOf(eventStream1), streamVersion(lte(1L)), event2).block();

                // Then
                Mono<EventStream<CloudEvent>> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2).events).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_is_greater_than_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(lte(0L)), event2).block());

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be less than or equal to 0 but was 1.");
            }
        }

        @Nested
        @DisplayName("gte")
        class Gte {

            @Test
            void writes_events_when_stream_version_is_greater_than_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Mono<EventStream<CloudEvent>> eventStream1 = eventStore.read("name");
                persist(streamIdOf(eventStream1), streamVersion(gte(0L)), event2).block();

                // Then
                Mono<EventStream<CloudEvent>> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2).events).containsExactly(event1, event2);
            }

            @Test
            void writes_events_when_stream_version_is_equal_to_expected_version() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Mono<EventStream<CloudEvent>> eventStream1 = eventStore.read("name");
                persist(streamIdOf(eventStream1), streamVersion(gte(0L)), event2).block();

                // Then
                Mono<EventStream<CloudEvent>> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2).events).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_stream_version_is_less_than_expected_version() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(gte(100L)), event2).block());

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than or equal to 100 but was 1.");
            }
        }

        @Nested
        @DisplayName("and")
        class And {

            @Test
            void writes_events_when_stream_version_is_when_all_conditions_match_and_expression() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Mono<EventStream<CloudEvent>> eventStream1 = eventStore.read("name");
                persist(streamIdOf(eventStream1), streamVersion(and(gte(0L), lt(100L), ne(40L))), event2).block();

                // Then
                Mono<EventStream<CloudEvent>> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2).events).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_any_of_the_operations_in_the_and_expression_is_not_fulfilled() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(and(gte(0L), lt(100L), ne(1L))), event2).block());

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than or equal to 0 and to be less than 100 and to not be equal to 1 but was 1.");
            }
        }

        @Nested
        @DisplayName("or")
        class Or {

            @Test
            void writes_events_when_stream_version_is_when_any_condition_in_or_expression_matches() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Mono<EventStream<CloudEvent>> eventStream1 = eventStore.read("name");
                persist(streamIdOf(eventStream1), streamVersion(or(gte(100L), lt(0L), ne(40L))), event2).block();

                // Then
                Mono<EventStream<CloudEvent>> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2).events).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_none_of_the_operations_in_the_and_expression_is_fulfilled() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(or(gte(100L), lt(1L))), event2).block());

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than or equal to 100 or to be less than 1 but was 1.");
            }
        }

        @Nested
        @DisplayName("not")
        class Not {

            @Test
            void writes_events_when_stream_version_is_not_matching_condition() {
                // When
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Mono<EventStream<CloudEvent>> eventStream1 = eventStore.read("name");
                persist(streamIdOf(eventStream1), streamVersion(not(eq(100L))), event2).block();

                // Then
                Mono<EventStream<CloudEvent>> eventStream2 = eventStore.read("name");
                assertThat(deserialize(eventStream2).events).containsExactly(event1, event2);
            }

            @Test
            void throws_write_condition_not_fulfilled_when_condition_is_fulfilled_but_should_not_be_so() {
                // Given
                DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
                persist("name", event1).block();

                // When
                DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
                Throwable throwable = catchThrowable(() -> persist("name", streamVersion(not(eq(1L))), event2).block());

                // Then
                assertThat(throwable).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                        .hasMessage("WriteCondition was not fulfilled. Expected version not to be equal to 1 but was 1.");
            }
        }
    }

    private VersionAndEvents deserialize(Mono<EventStream<CloudEvent>> eventStreamMono) {
        return eventStreamMono
                .map(es -> {
                    List<DomainEvent> events = es.events()
                            // @formatter:off
                            .map(unchecked(cloudEvent -> objectMapper.readValue(cloudEvent.getData(), new TypeReference<Map<String, Object>>() {})))
                            // @formatter:on
                            .map(event -> {
                                Instant instant = Instant.ofEpochMilli((long) event.get("time"));
                                LocalDateTime time = LocalDateTime.ofInstant(instant, UTC);
                                String eventId = (String) event.get("eventId");
                                String name = (String) event.get("name");
                                return Match(event.get("type")).of(
                                        Case($(is(NameDefined.class.getSimpleName())), e -> new NameDefined(eventId, time, name)),
                                        Case($(is(NameWasChanged.class.getSimpleName())), e -> new NameWasChanged(eventId, time, name))
                                );
                            })
                            .toStream()
                            .collect(Collectors.toList());
                    return new VersionAndEvents(es.version(), events);
                })
                .block();

    }

    private static class VersionAndEvents {
        private final long version;
        private final List<DomainEvent> events;

        VersionAndEvents(long version, List<DomainEvent> events) {
            this.version = version;
            this.events = events;
        }
    }

    private static String streamIdOf(Mono<EventStream<CloudEvent>> eventStreamMono) {
        return eventStreamMono.map(EventStream::id).block();
    }


    private Mono<Void> persist(String eventStreamId, DomainEvent event) {
        return eventStore.write(eventStreamId, Flux.just(convertDomainEventCloudEvent(event)));
    }

    private Mono<Void> persist(String eventStreamId, List<DomainEvent> events) {
        return eventStore.write(eventStreamId, Flux.fromIterable(events).map(this::convertDomainEventCloudEvent));
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

    @NotNull
    private CloudEvent convertDomainEventCloudEvent(DomainEvent domainEvent) {
        return CloudEventBuilder.v1()
                .withId(domainEvent.getEventId())
                .withSource(URI.create("http://name"))
                .withType(domainEvent.getClass().getSimpleName())
                .withTime(toLocalDateTime(domainEvent.getTimestamp()).atZone(UTC))
                .withSubject(domainEvent.getName())
                .withDataContentType("application/json")
                .withData(serializeEvent(domainEvent))
                .build();
    }

    private byte[] serializeEvent(DomainEvent e) {
        try {
            return objectMapper.writeValueAsBytes(new HashMap<String, Object>() {{
                put("type", e.getClass().getSimpleName());
                put("eventId", e.getEventId());
                put("name", e.getName());
                put("time", e.getTimestamp().getTime());
            }});
        } catch (JsonProcessingException jsonProcessingException) {
            throw new RuntimeException(jsonProcessingException);
        }
    }
}


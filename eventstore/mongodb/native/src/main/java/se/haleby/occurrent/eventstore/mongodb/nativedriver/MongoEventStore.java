package se.haleby.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.TransactionOptions;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.UpdateResult;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.haleby.occurrent.eventstore.api.Condition;
import se.haleby.occurrent.eventstore.api.Condition.MultiOperandCondition;
import se.haleby.occurrent.eventstore.api.Condition.MultiOperandConditionName;
import se.haleby.occurrent.eventstore.api.Condition.SingleOperandCondition;
import se.haleby.occurrent.eventstore.api.Condition.SingleOperandConditionName;
import se.haleby.occurrent.eventstore.api.WriteCondition;
import se.haleby.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStoreOperations;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.StreamConsistencyGuarantee.None;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.StreamConsistencyGuarantee.Transactional;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.inc;
import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static se.haleby.occurrent.eventstore.mongodb.converter.MongoBulkWriteExceptionToDuplicateCloudEventExceptionTranslator.translateToDuplicateCloudEventException;
import static se.haleby.occurrent.eventstore.mongodb.converter.OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent;
import static se.haleby.occurrent.eventstore.mongodb.converter.OccurrentCloudEventMongoDBDocumentMapper.convertToDocument;

public class MongoEventStore implements EventStore, EventStoreOperations {
    private static final Logger log = LoggerFactory.getLogger(MongoEventStore.class);

    private static final String ID = "_id";
    private static final String VERSION = "version";

    private final MongoCollection<Document> eventCollection;
    private final EventFormat cloudEventSerializer;
    private final StreamConsistencyGuarantee streamConsistencyGuarantee;
    private final MongoClient mongoClient;
    private final MongoDatabase mongoDatabase;

    public MongoEventStore(ConnectionString connectionString, StreamConsistencyGuarantee streamConsistencyGuarantee) {
        log.info("Connecting to MongoDB using connection string: {}", connectionString);
        mongoClient = MongoClients.create(connectionString);
        String databaseName = requireNonNull(connectionString.getDatabase());
        mongoDatabase = mongoClient.getDatabase(databaseName);
        String eventCollectionName = requireNonNull(connectionString.getCollection());
        eventCollection = mongoDatabase.getCollection(eventCollectionName);
        cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        this.streamConsistencyGuarantee = streamConsistencyGuarantee;
        initializeEventStore(eventCollectionName, streamConsistencyGuarantee, mongoDatabase);
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        final EventStream<Document> eventStream;
        if (streamConsistencyGuarantee instanceof None) {
            Stream<Document> stream = readCloudEvents(streamId, skip, limit, null);
            eventStream = new EventStreamImpl<>(streamId, 0, stream);
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            eventStream = readEventStream(streamId, skip, limit, transactional);
        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
        return eventStream.map(document -> convertToCloudEvent(cloudEventSerializer, document));
    }

    private EventStreamImpl<Document> readEventStream(String streamId, int skip, int limit, Transactional transactional) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            return clientSession.withTransaction(() -> {
                Document document = mongoDatabase
                        .getCollection(transactional.streamVersionCollectionName)
                        .find(clientSession, eq(ID, streamId), Document.class)
                        .first();

                if (document == null) {
                    return new EventStreamImpl<>(streamId, 0, Stream.empty());
                }

                Stream<Document> stream = readCloudEvents(streamId, skip, limit, clientSession);
                return new EventStreamImpl<>(streamId, document.getLong(VERSION), stream);
            }, transactional.transactionOptions);
        }
    }

    private Stream<Document> readCloudEvents(String streamId, int skip, int limit, ClientSession clientSession) {
        final Bson filter = eq(STREAM_ID, streamId);
        final FindIterable<Document> documentsWithoutSkipAndLimit;
        if (clientSession == null) {
            documentsWithoutSkipAndLimit = eventCollection.find(filter);
        } else {
            documentsWithoutSkipAndLimit = eventCollection.find(clientSession, filter);
        }

        final FindIterable<Document> documentsWithSkipAndLimit;
        if (skip != 0 || limit != Integer.MAX_VALUE) {
            documentsWithSkipAndLimit = documentsWithoutSkipAndLimit.skip(skip).limit(limit);
        } else {
            documentsWithSkipAndLimit = documentsWithoutSkipAndLimit;
        }
        return StreamSupport.stream(documentsWithSkipAndLimit.spliterator(), false);
    }

    @Override
    public void write(String streamId, Stream<CloudEvent> events) {
        write(streamId, WriteCondition.anyStreamVersion(), events);
    }

    @Override
    public void write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        if (writeCondition == null) {
            throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
        } else if (streamConsistencyGuarantee instanceof None && !writeCondition.isAnyStreamVersion()) {
            throw new IllegalArgumentException("Cannot use a " + WriteCondition.class.getSimpleName() + " other than 'any' when streamConsistencyGuarantee is " + None.class.getSimpleName());
        }

        List<Document> cloudEventDocuments = events
                .map(cloudEvent -> convertToDocument(cloudEventSerializer, streamId, cloudEvent))
                .collect(Collectors.toList());

        if (streamConsistencyGuarantee instanceof None) {
            try {
                eventCollection.insertMany(cloudEventDocuments);
            } catch (MongoBulkWriteException e) {
                throw translateToDuplicateCloudEventException(e);
            }
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            consistentlyWrite(streamId, writeCondition, cloudEventDocuments);
        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
    }

    private void consistentlyWrite(String streamId, WriteCondition writeCondition, List<Document> serializedEvents) {
        Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
        TransactionOptions transactionOptions = transactional.transactionOptions;
        String streamVersionCollectionName = transactional.streamVersionCollectionName;
        try (ClientSession clientSession = mongoClient.startSession()) {
            clientSession.withTransaction(() -> {
                MongoCollection<Document> streamVersionCollection = mongoDatabase.getCollection(streamVersionCollectionName);

                UpdateResult updateResult = streamVersionCollection
                        .updateOne(clientSession, generateUpdateCondition(streamId, writeCondition),
                                inc(VERSION, 1L));

                if (updateResult.getMatchedCount() == 0) {
                    Document document = streamVersionCollection.find(clientSession, eq(ID, streamId)).first();
                    if (document == null) {
                        Map<String, Object> data = new HashMap<String, Object>() {{
                            put(ID, streamId);
                            put(VERSION, 1L);
                        }};
                        streamVersionCollection.insertOne(clientSession, new Document(data));
                    } else {
                        long eventStreamVersion = document.getLong(VERSION);
                        throw new WriteConditionNotFulfilledException(streamId, eventStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition.toString(), eventStreamVersion));
                    }
                }

                try {
                    eventCollection.insertMany(clientSession, serializedEvents);
                } catch (MongoBulkWriteException e) {
                    throw translateToDuplicateCloudEventException(e);
                }
                return "";
            }, transactionOptions);
        }
    }

    private static Bson generateUpdateCondition(String streamId, WriteCondition writeCondition) {
        Bson streamEq = eq(ID, streamId);
        if (writeCondition.isAnyStreamVersion()) {
            return streamEq;
        }

        if (!(writeCondition instanceof WriteCondition.StreamVersionWriteCondition)) {
            throw new IllegalArgumentException("Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
        }

        WriteCondition.StreamVersionWriteCondition c = (WriteCondition.StreamVersionWriteCondition) writeCondition;
        return and(streamEq, generateUpdateCondition(c.condition));
    }


    private static Bson generateUpdateCondition(Condition<Long> condition) {
        if (condition instanceof MultiOperandCondition) {
            MultiOperandCondition<Long> operation = (MultiOperandCondition<Long>) condition;
            MultiOperandConditionName operationName = operation.operationName;
            List<Condition<Long>> operations = operation.operations;
            Bson[] filters = operations.stream().map(MongoEventStore::generateUpdateCondition).toArray(Bson[]::new);
            switch (operationName) {
                case AND:
                    return Filters.and(filters);
                case OR:
                    return Filters.or(filters);
                case NOT:
                    return Filters.not(filters[0]);
                default:
                    throw new IllegalStateException("Unexpected value: " + operationName);
            }
        } else if (condition instanceof SingleOperandCondition) {
            SingleOperandCondition<Long> singleOperandCondition = (SingleOperandCondition<Long>) condition;
            long expectedVersion = singleOperandCondition.operand;
            SingleOperandConditionName singleOperandConditionName = singleOperandCondition.singleOperandConditionName;
            switch (singleOperandConditionName) {
                case EQ:
                    return eq(VERSION, expectedVersion);
                case LT:
                    return lt(VERSION, expectedVersion);
                case GT:
                    return gt(VERSION, expectedVersion);
                case LTE:
                    return lte(VERSION, expectedVersion);
                case GTE:
                    return gte(VERSION, expectedVersion);
                case NE:
                    return ne(VERSION, expectedVersion);
                default:
                    throw new IllegalStateException("Unexpected value: " + singleOperandConditionName);
            }
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
        }
    }

    @Override
    public boolean exists(String streamId) {
        if (streamConsistencyGuarantee instanceof Transactional) {
            String streamVersionCollectionName = ((Transactional) streamConsistencyGuarantee).streamVersionCollectionName;
            return mongoDatabase.getCollection(streamVersionCollectionName).countDocuments(eq(ID, streamId)) > 0;
        } else {
            return eventCollection.countDocuments(eq(STREAM_ID, streamId)) > 0;
        }
    }

    @Override
    public void deleteEventStream(String streamId) {
        requireNonNull(streamId, "Stream id cannot be null");
        if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            TransactionOptions transactionOptions = transactional.transactionOptions;
            MongoCollection<Document> streamVersionCollection = mongoDatabase.getCollection(transactional.streamVersionCollectionName);
            try (ClientSession clientSession = mongoClient.startSession()) {
                clientSession.withTransaction(() -> {
                    streamVersionCollection.deleteMany(clientSession, eq(ID, streamId));
                    eventCollection.deleteMany(clientSession, eq(STREAM_ID, streamId));
                    return "";
                }, transactionOptions);
            }
        } else if (streamConsistencyGuarantee instanceof None) {
            eventCollection.deleteMany(eq(STREAM_ID, streamId));
        }
    }

    @Override
    public void deleteAllEventsInEventStream(String streamId) {
        requireNonNull(streamId, "Stream id cannot be null");
        if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            TransactionOptions transactionOptions = transactional.transactionOptions;
            try (ClientSession clientSession = mongoClient.startSession()) {
                clientSession.withTransaction(() -> {
                    eventCollection.deleteMany(clientSession, eq(STREAM_ID, streamId));
                    return "";
                }, transactionOptions);
            }
        } else if (streamConsistencyGuarantee instanceof None) {
            eventCollection.deleteMany(eq(STREAM_ID, streamId));
        }
    }

    @Override
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        requireNonNull(cloudEventId, "Cloud event id cannot be null");
        requireNonNull(cloudEventSource, "Cloud event source cannot be null");

        eventCollection.deleteOne(and(eq("id", cloudEventId), eq("source", cloudEventSource.toString())));
    }

    private static class EventStreamImpl<T> implements EventStream<T> {
        private final String id;
        private final long version;
        private final Stream<T> events;

        EventStreamImpl(String id, long version, Stream<T> events) {
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
        public Stream<T> events() {
            return events;
        }
    }

    private static void initializeEventStore(String eventStoreCollectionName, StreamConsistencyGuarantee streamConsistencyGuarantee, MongoDatabase mongoDatabase) {
        if (!collectionExists(mongoDatabase, eventStoreCollectionName)) {
            mongoDatabase.createCollection(eventStoreCollectionName);
        }
        mongoDatabase.getCollection(eventStoreCollectionName).createIndex(Indexes.ascending(STREAM_ID));
        // Cloud spec defines id + source must be unique!
        mongoDatabase.getCollection(eventStoreCollectionName).createIndex(Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")), new IndexOptions().unique(true));
        if (streamConsistencyGuarantee instanceof Transactional) {
            String streamVersionCollectionName = ((Transactional) streamConsistencyGuarantee).streamVersionCollectionName;
            createStreamVersionCollectionAndIndex(streamVersionCollectionName, mongoDatabase);
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean collectionExists(MongoDatabase mongoDatabase, String collectionName) {
        for (String listCollectionName : mongoDatabase.listCollectionNames()) {
            if (listCollectionName.equals(collectionName)) {
                return true;
            }
        }
        return false;
    }

    private static void createStreamVersionCollectionAndIndex(String streamVersionCollectionName, MongoDatabase mongoDatabase) {
        if (!collectionExists(mongoDatabase, streamVersionCollectionName)) {
            mongoDatabase.createCollection(streamVersionCollectionName);
        }
        mongoDatabase.getCollection(streamVersionCollectionName).createIndex(Indexes.compoundIndex(Indexes.ascending(ID), Indexes.ascending(VERSION)), new IndexOptions().unique(true));
    }
}
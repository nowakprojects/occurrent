package se.haleby.occurrent.example.springevent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.mongodb.timerepresentation.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import se.haleby.occurrent.subscription.api.reactor.PositionAwareReactorSubscription;
import se.haleby.occurrent.subscription.api.reactor.ReactorSubscriptionPositionStorage;
import se.haleby.occurrent.subscription.mongodb.spring.reactor.SpringReactorSubscriptionForMongoDB;
import se.haleby.occurrent.subscription.mongodb.spring.reactor.SpringReactorSubscriptionPositionStorageForMongoDB;
import se.haleby.occurrent.subscription.mongodb.spring.reactor.SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB;

import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping.EVERYTHING;

@SpringBootApplication
public class ForwardEventsFromMongoDBToSpringApplication {

    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Bean(destroyMethod = "close")
    public MongoClient mongoClient() {
        return MongoClients.create(connectionString());
    }

    @Bean
    public EventStore eventStore(MongoClient mongoClient) {
        ConnectionString connectionString = connectionString();
        String database = connectionString.getDatabase();
        String collection = connectionString.getCollection();

        return new MongoEventStore(mongoClient, database, collection, new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
    }

    @Bean
    public ReactorSubscriptionPositionStorage reactorSubscriptionPositionStorage(ReactiveMongoOperations mongoOperations) {
        return new SpringReactorSubscriptionPositionStorageForMongoDB(mongoOperations, "subscriptions");
    }

    @Bean
    public PositionAwareReactorSubscription subscription(ReactiveMongoOperations mongoOperations) {
        return new SpringReactorSubscriptionForMongoDB(mongoOperations, "events", TimeRepresentation.RFC_3339_STRING);
    }

    @Bean
    public SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB autoPersistingSubscription(PositionAwareReactorSubscription subscription, ReactorSubscriptionPositionStorage storage) {
        return new SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB(subscription, storage);
    }

    @Bean
    public ConnectionString connectionString() {
        return new ConnectionString(mongoUri + ".events");
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        // Configure jackson to add type information to each serialized object
        // Allows deserializing interfaces such as DomainEvent
        objectMapper.activateDefaultTyping(new LaissezFaireSubTypeValidator(), EVERYTHING);
        return objectMapper;
    }
}
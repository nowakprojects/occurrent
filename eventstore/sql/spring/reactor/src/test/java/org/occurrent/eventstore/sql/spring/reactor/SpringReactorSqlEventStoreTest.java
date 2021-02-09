package org.occurrent.eventstore.sql.spring.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.sql.common.PostgresSqlEventStoreConfig;
import org.occurrent.eventstore.sql.common.SqlEventStoreConfig;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

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
  void name() {
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

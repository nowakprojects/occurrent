package org.occurrent.eventstore.sql.spring.blocking;

class PostgreSqlJdbcEventStoreConfig implements JdbcEventStoreConfig {

  private final String eventStoreTableName;

  PostgreSqlJdbcEventStoreConfig(String eventStoreTableName) {
    this.eventStoreTableName = eventStoreTableName;
  }

  @Override
  public String createEventStoreTableSql() {
    return "CREATE TABLE IF NOT EXISTS " + eventStoreTableName + "("
        + "id VARCHAR(255) NOT NULL," + "\n"
        + "source VARCHAR(255) NOT NULL," + "\n"
        + "specversion VARCHAR(255) NOT NULL," + "\n"
        + "type VARCHAR(255) NOT NULL," + "\n"
        + "datacontenttype VARCHAR(255)," + "\n"
        + "dataschema VARCHAR(255)," + "\n"
        + "subject VARCHAR(255)," + "\n"
        + "streamid VARCHAR(255)," + "\n"
        + "data jsonb," + "\n"
        + "time TIMESTAMP," + "\n"
        + ")";
  }
}

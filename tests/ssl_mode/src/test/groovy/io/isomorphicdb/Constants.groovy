package io.isomorphicdb

import com.zaxxer.hikari.HikariDataSource
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.PostgreSQLContainer

class Constants {
    static final boolean CI = Boolean.parseBoolean(System.getProperty("ci"))
    static final String VERSION = '13.0'
    static final String USER = 'postgres'
    static final String PASSWORD = 'postgres'
    static final String DRIVER_CLASS = 'org.postgresql.Driver'
    static final JdbcDatabaseContainer<PostgreSQLContainer> POSTGRE_SQL
    static final HikariDataSource PG_SOURCE
    static final HikariDataSource DB_SOURCE

}

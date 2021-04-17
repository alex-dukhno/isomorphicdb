package io.isomorphicdb

import com.zaxxer.hikari.HikariDataSource
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.PostgreSQLContainer
import spock.lang.Specification

class PooledConnection extends Specification {
    static final HikariDataSource PG_SOURCE
    static final HikariDataSource DB_SOURCE
    static final JdbcDatabaseContainer<PostgreSQLContainer> POSTGRE_SQL

    static {
        Class.forName(Constants.DRIVER_CLASS);

        if (!Constants.CI) {
            println("Make sure that you are running database locally")
            POSTGRE_SQL = new PostgreSQLContainer("postgres:$Constants.VERSION")
                .withUsername(Constants.USER)
                .withPassword(Constants.PASSWORD)
                .withUrlParam('gssEncMode', 'disable')
                .withUrlParam('sslmode', 'required')
                .withUrlParam('preferQueryMode', 'extendedForPrepared')
            POSTGRE_SQL.start()
        } else {
            POSTGRE_SQL = null
        }
        // TODO: make it work with connection pool
        PG_SOURCE = pgPool();
        DB_SOURCE = dbPool();

    }

    private static String pgUrl() {
        if (Constants.CI) {
            "jdbc:postgresql://localhost:5433/test?gssEncMode=disable&sslmode=disable&preferQueryMode=extendedForPrepared"
        } else {
            POSTGRE_SQL.jdbcUrl
        }
    }

    static HikariDataSource pgPool() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(pgUrl());
        ds.setUsername(Constants.USER);
        ds.setPassword(Constants.PASSWORD);
        ds.setDriverClassName(Constants.DRIVER_CLASS);
        ds
    }

    static HikariDataSource dbPool() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl("jdbc:postgresql://localhost:5432/test?gssEncMode=disable&sslmode=disable&preferQueryMode=extendedForPrepared");
        ds.setUsername(Constants.USER);
        ds.setPassword(Constants.PASSWORD);
        ds.setDriverClassName(Constants.DRIVER_CLASS);
        ds
    }
}

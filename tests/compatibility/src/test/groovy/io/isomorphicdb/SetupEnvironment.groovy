package io.isomorphicdb

import com.zaxxer.hikari.HikariDataSource
import groovy.sql.Sql
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.PostgreSQLContainer
import spock.lang.Specification

import java.sql.SQLException

class SetupEnvironment extends Specification {
  private static final boolean CI = Boolean.parseBoolean(System.getProperty("ci"))
  static final String VERSION = '12.4'
  static final String USER = 'postgres'
  static final String PASSWORD = 'postgres'
  static final String DRIVER_CLASS = 'org.postgresql.Driver'
  static final JdbcDatabaseContainer<PostgreSQLContainer> POSTGRE_SQL
  static final HikariDataSource PG_SOURCE
  static final HikariDataSource DB_SOURCE

  static {
    Class.forName(DRIVER_CLASS);

    if (!CI) {
      println("Make sure that you are running database locally")
      POSTGRE_SQL = new PostgreSQLContainer("postgres:$VERSION")
          .withUsername(USER)
          .withPassword(PASSWORD)
          .withUrlParam('gssEncMode', 'disable')
          .withUrlParam('sslmode', 'disable')
          .withUrlParam('preferQueryMode', 'extendedForPrepared')
      POSTGRE_SQL.start()
    } else {
      POSTGRE_SQL = null
    }

    // TODO: make it work with connection pool
    PG_SOURCE = pgPool();
    DB_SOURCE = dbPool();
  }

  static Map<String, String> pgConf() {
    [
        url: pgUrl(),
        user: USER,
        password: PASSWORD,
        driver: DRIVER_CLASS
    ]
  }


  static HikariDataSource pgPool() {
    HikariDataSource ds = new HikariDataSource();
    ds.setJdbcUrl(pgUrl());
    ds.setUsername(USER);
    ds.setPassword(PASSWORD);
    ds.setDriverClassName(DRIVER_CLASS);
    ds
  }

  static Map<String, String> dbConf() {
    [
            //TODO: sslmode as parameter to test both encrypted and not traffic on CI
        url: "jdbc:postgresql://localhost:5432/test?gssEncMode=disable&sslmode=disable&preferQueryMode=extendedForPrepared",
        user: USER,
        password: PASSWORD,
        driver: DRIVER_CLASS,
    ]
  }

  static HikariDataSource dbPool() {
    HikariDataSource ds = new HikariDataSource();
    //TODO: sslmode as parameter to test both encrypted and not traffic on CI
    ds.setJdbcUrl("jdbc:postgresql://localhost:5432/test?gssEncMode=disable&sslmode=disable&preferQueryMode=extendedForPrepared");
    ds.setUsername(USER);
    ds.setPassword(PASSWORD);
    ds.setDriverClassName(DRIVER_CLASS);
    ds
  }


  private static String pgUrl() {
    if (CI) {
      "jdbc:postgresql://localhost:5433/test?gssEncMode=disable&sslmode=disable&preferQueryMode=extendedForPrepared"
    } else {
      POSTGRE_SQL.jdbcUrl
//      "jdbc:postgresql://192.168.56.101:5432/postgres"
    }
  }

  static pgExecute(String query) {
    execute(pgConf(), query)
  }

  static pgExecute(String query, List<Object> params) {
    execute(pgConf(), query, params)
  }

  static dbExecute(String query) {
    execute(dbConf(), query)
  }

  static dbExecute(String query, List<Object> params) {
    execute(dbConf(), query, params)
  }

  private static execute(Map<String, String> conf, String query) {
    Sql.withInstance(conf) {
      Sql sql -> sql.execute query
    }
  }

  private static execute(Map<String, String> conf, String query, List<Object> params) {
    Sql.withInstance(conf) {
      Sql sql -> sql.execute query, params
    }
  }

  private static execute(HikariDataSource source, String query) {
    withInstance(source) {
      Sql sql -> sql.execute query
    }
  }

  private static execute(HikariDataSource source, String query, List<Object> params) {
    withInstance(source) {
      Sql sql -> sql.execute query, params
    }
  }

  static void withInstance(HikariDataSource source, Closure c) throws SQLException {
    Sql sql = null;
    try {
      sql = new Sql(source);
      c.call(sql);
    } finally {
      if (sql != null) {
        sql.close()
      }
    }
  }
}

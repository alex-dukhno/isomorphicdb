package io.database

import groovy.sql.Sql
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.PostgreSQLContainer
import spock.lang.Specification

class SetupEnvironment extends Specification {
  private static final boolean CI = Boolean.parseBoolean(System.getProperty("ci"))
  static final String VERSION = '12.4'
  static final String USER = 'postgres'
  static final String PASSWORD = 'postgres'
  static final String DRIVER_CLASS = 'org.postgresql.Driver'
  static final JdbcDatabaseContainer<PostgreSQLContainer> POSTGRE_SQL

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
  }

  static Map<String, String> pgConf() {
    [
        url: pgUrl(),
        user: USER,
        password: PASSWORD,
        driver: DRIVER_CLASS
    ]
  }

  static Map<String, String> dbConf() {
    [
        url: "jdbc:postgresql://localhost:5432/test?gssEncMode=disable&sslmode=disable&preferQueryMode=extendedForPrepared",
        user: USER,
        password: PASSWORD,
        driver: DRIVER_CLASS,
    ]
  }


  private static String pgUrl() {
    if (CI) {
      "jdbc:postgresql://localhost:5433/test?gssEncMode=disable&sslmode=disable&preferQueryMode=extendedForPrepared"
    } else {
      POSTGRE_SQL.jdbcUrl
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
}

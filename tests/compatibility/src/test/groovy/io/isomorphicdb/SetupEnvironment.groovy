package io.isomorphicdb

import com.zaxxer.hikari.HikariDataSource
import groovy.sql.Sql
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.PostgreSQLContainer
import spock.lang.Specification

import java.sql.SQLException

class SetupEnvironment extends Specification {
  static final JdbcDatabaseContainer<PostgreSQLContainer> POSTGRE_SQL

  static {
    Class.forName(Constants.DRIVER_CLASS);

    if (!Constants.CI) {
      println("Make sure that you are running database locally")
      POSTGRE_SQL = new PostgreSQLContainer("postgres:$Constants.VERSION")
          .withUsername(Constants.USER)
          .withPassword(Constants.PASSWORD)
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
        url     : pgUrl(),
        user    : Constants.USER,
        password: Constants.PASSWORD,
        driver  : Constants.DRIVER_CLASS
    ]
  }

  static Map<String, String> dbConf() {
    [
        url     : "jdbc:postgresql://localhost:5432/test?gssEncMode=disable&sslmode=disable&preferQueryMode=extendedForPrepared",
        user    : Constants.USER,
        password: Constants.PASSWORD,
        driver  : Constants.DRIVER_CLASS,
    ]
  }


  private static String pgUrl() {
    if (Constants.CI) {
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

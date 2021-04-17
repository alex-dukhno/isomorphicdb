package io.isomorphicdb

import groovy.sql.Sql
import spock.lang.Specification

import java.sql.SQLException

class SecureConnection extends Specification {
  static {
    Class.forName(Constants.DRIVER_CLASS);
  }

  static Map<String, String> dbConf() {
    [
        url     : "jdbc:postgresql://localhost:5432/test?gssEncMode=disable&sslmode=require&preferQueryMode=extendedForPrepared",
        user    : Constants.USER,
        password: Constants.PASSWORD,
        driver  : Constants.DRIVER_CLASS,
    ]
  }

  static dbExecute(String query) {
    execute(dbConf(), query)
  }

  private static execute(Map<String, String> conf, String query) {
    Sql.withInstance(conf) {
      Sql sql -> sql.execute query
    }
  }

  private static final String CREATE_SCHEMA = 'create schema SCHEMA_NAME'
  private static final String DROP_SCHEMA_CASCADE = 'drop schema SCHEMA_NAME cascade'

  def 'create schema'() {
    when:
      SQLException dbError
      try {
        dbExecute(CREATE_SCHEMA)
        dbExecute(DROP_SCHEMA_CASCADE)
      } catch (SQLException e) {
        dbError = e
      }

    then:
      dbError == null
  }
}

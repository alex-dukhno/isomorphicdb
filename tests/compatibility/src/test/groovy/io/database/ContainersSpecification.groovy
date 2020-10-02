package io.database

import groovy.sql.Sql
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.images.builder.ImageFromDockerfile
import spock.lang.Specification

import java.nio.file.Paths

class ContainersSpecification extends Specification {
  static final String VERSION = '12.4'
  static final String USER = 'postgres'
  static final String PASSWORD = 'postgres'
  static final String DRIVER_CLASS = 'org.postgresql.Driver'
  static final PostgreSQLContainer POSTGRE_SQL
  static final GenericContainer DATABASE

  static {
    Class.forName(DRIVER_CLASS);
    POSTGRE_SQL = new PostgreSQLContainer("postgres:$VERSION")
        .withUsername(USER)
        .withPassword(PASSWORD)
    POSTGRE_SQL.start()

    if (Boolean.parseBoolean(System.getProperty("ci", "true"))) {
      DATABASE = new GenericContainer(
          new ImageFromDockerfile()
              .withDockerfile(Paths.get('../../Dockerfile')))
          .withExposedPorts(5432)
      DATABASE.start()
    } else {
      println("Make sure that you are running database locally")
      DATABASE = null
    }
  }

  private static Map<String, String> pgConf() {
    [
        url: POSTGRE_SQL.jdbcUrl,
        user: USER,
        password: PASSWORD,
        driver: DRIVER_CLASS
    ]
  }

  private static Map<String, String> dbConf() {
    [
        url: "jdbc:postgresql://localhost:${dbPort()}/test?gssEncMode=disable&sslmode=disable&preferQueryMode=simple",
        user: USER,
        password: PASSWORD,
        driver: DRIVER_CLASS,
    ]
  }

  private static int dbPort() {
    if (DATABASE != null) {
      DATABASE.getFirstMappedPort()
    } else {
      5432
    }
  }

  static pgExecute(String query) {
    execute(pgConf(), query)
  }

  static dbExecute(String query) {
    execute(dbConf(), query)
  }

  private static execute(Map<String, String> conf, String query) {
    Sql.withInstance(conf) {
      Sql sql -> sql.execute query
    }
  }
}

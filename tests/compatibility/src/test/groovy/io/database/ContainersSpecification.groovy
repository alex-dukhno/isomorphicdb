package io.database

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

    DATABASE = new GenericContainer(
        new ImageFromDockerfile()
            .withDockerfile(Paths.get('../../Dockerfile')))
        .withExposedPorts(5432)
    DATABASE.start()
  }
}

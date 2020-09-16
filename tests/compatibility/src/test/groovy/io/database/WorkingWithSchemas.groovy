package io.database

import groovy.sql.Sql

import java.sql.SQLException

class WorkingWithSchemas extends ContainersSpecification {
  private Sql pg
  private Sql db

  def setup() {
    pg = Sql.newInstance([
        url: POSTGRE_SQL.jdbcUrl,
        user: USER,
        password: PASSWORD,
        driver: DRIVER_CLASS
    ])

    db = Sql.newInstance([
        url: "jdbc:postgresql://localhost:${DATABASE.getFirstMappedPort()}/test?gssEncMode=disable&sslmode=disable",
        user: USER,
        password: PASSWORD,
        driver: DRIVER_CLASS,
    ])
  }

  def cleanup() {
    if (pg != null) {
      pg.close()
    }
    if (db != null) {
      db.close()
    }
  }

  def 'create schema'() {
    given:
      String createSchemaQuery = 'create schema CREATE_SCHEMA_TEST'
    when:
      boolean pgResult = pg.execute createSchemaQuery
    and:
      boolean dbResult = db.execute createSchemaQuery
    then:
      pgResult == dbResult
  }

  def 'create schema if not exist'() {
    given:
      String createSchemaQuery = 'create schema if not exists CREATE_SCHEMA_IF_NOT_EXIST'
    when:
      boolean pgResult = pg.execute createSchemaQuery
    and:
      boolean dbResult = db.execute createSchemaQuery
    then:
      pgResult == dbResult
  }

  def 'create schema with the same name'() {
    given:
      String createSchemaQuery = 'create schema WITH_THE_SAME_NAME'
    when:
      SQLException pgResult
      try {
        pg.execute createSchemaQuery
      } catch (SQLException e) {
        pgResult = e
      }
    and:
      SQLException dbResult
      try {
        pg.execute createSchemaQuery
      } catch (SQLException e) {
        dbResult = e
      }
    then:
      pgResult != null
    and:
      dbResult != null
    and:
      pgResult.getErrorCode() == dbResult.getErrorCode()
  }

  def 'drop schema'() {
    given:
      String createSchemaQuery = 'create schema CREATE_SCHEMA_TO_DROP'
      String dropSchemaQuery = 'drop schema CREATE_SCHEMA_TO_DROP'
    and:
      pg.execute createSchemaQuery
      db.execute createSchemaQuery
    when:
      boolean pgResult = pg.execute dropSchemaQuery
    and:
      boolean dbResult = db.execute dropSchemaQuery
    then:
      pgResult == dbResult
  }

  def 'drop schema if exists'() {
    given:
      String dropSchemaIfExistsQuery = 'drop schema if exists DROP_SCHEMA_IF_EXISTS'
    when:
      boolean pgResult = pg.execute dropSchemaIfExistsQuery
    and:
      boolean dbResult = pg.execute dropSchemaIfExistsQuery
    then:
      pgResult == dbResult
  }

  def 'drop non existent schema'() {
    given:
      String dropNonExistentSchema = 'drop schema NON_EXISTENT_SCHEMA'
    when:
      SQLException pgResult
      try {
        pg.execute dropNonExistentSchema
      } catch (SQLException e) {
        pgResult = e
      }
    and:
      SQLException dbResult
      try {
        pg.execute dropNonExistentSchema
      } catch (SQLException e) {
        dbResult = e
      }
    then:
      pgResult != null
    and:
      dbResult != null
    and:
      pgResult.getErrorCode() == dbResult.getErrorCode()
  }
}

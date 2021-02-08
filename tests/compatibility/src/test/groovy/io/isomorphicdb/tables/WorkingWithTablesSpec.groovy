package io.isomorphicdb.tables

import io.isomorphicdb.SetupEnvironment
import spock.lang.Ignore

import java.sql.SQLException

class WorkingWithTablesSpec extends SetupEnvironment {
  private static final String CREATE_SCHEMA = 'create schema SCHEMA_NAME'
  private static final String DROP_SCHEMA = 'drop schema SCHEMA_NAME cascade'

  def setupSpec() {
    pgExecute(CREATE_SCHEMA)
    dbExecute(CREATE_SCHEMA)
  }

  def cleanupSpec() {
    pgExecute(DROP_SCHEMA)
    dbExecute(DROP_SCHEMA)
  }

  def 'create table in non existent schema'() {
    given:
      String createTableInNonExistentSchema = 'create table NON_EXISTENT_SCHEMA.TABLE_NAME ()'
    when:
      SQLException pgError
      try {
        pgExecute createTableInNonExistentSchema
      } catch (SQLException e) {
        pgError = e
      }
    and:
      SQLException dbError
      try {
        dbExecute createTableInNonExistentSchema
      } catch (SQLException e) {
        dbError = e
      }
    then:
      pgError.errorCode == dbError.errorCode
  }

  def 'create table'() {
    given:
      String createTableQuery = 'create table SCHEMA_NAME.TABLE_NAME ()'
    when:
      boolean pgResult = pgExecute(createTableQuery)
    and:
      boolean dbResult = dbExecute(createTableQuery)
    then:
      pgResult == dbResult
  }

  def 'create table with the same name'() {
    given:
      String createTableWithTheSameQuery = 'create table SCHEMA_NAME.WITH_THE_SAME_NAME ()'
    when:
      SQLException pgError
      try {
        pgExecute createTableWithTheSameQuery
        pgExecute createTableWithTheSameQuery
      } catch (SQLException e) {
        pgError = e
      }
    and:
      SQLException dbError
      try {
        dbExecute createTableWithTheSameQuery
        dbExecute createTableWithTheSameQuery
      } catch (SQLException e) {
        dbError = e
      }
    then:
      pgError.errorCode == dbError.errorCode
  }

  def 'create table if not exists'() {
    given:
      String createTableIfNotExistsQuery = 'create table if not exists SCHEMA_NAME.TABLE_IF_NOT_EXISTS ()'
    when:
      boolean pgResult = pgExecute(createTableIfNotExistsQuery)
    and:
      boolean dbResult = dbExecute(createTableIfNotExistsQuery)
    then:
      pgResult == dbResult
  }

  def 'drop non existent table'() {
    given:
      String dropNonExistentTable = 'drop table SCHEMA_NAME.NON_EXISTENT_TABLE'
    when:
      SQLException pgError
      try {
        pgExecute dropNonExistentTable
      } catch (SQLException e) {
        pgError = e
      }
    and:
      SQLException dbError
      try {
        dbExecute dropNonExistentTable
      } catch (SQLException e) {
        dbError = e
      }
    then:
      pgError.errorCode == dbError.errorCode
  }

  def 'drop table'() {
    given:
      String createTableToDropQuery = 'create table SCHEMA_NAME.TABLE_TO_DROP ()'
      String dropTableQuery = 'drop table SCHEMA_NAME.TABLE_TO_DROP'
    when:
      pgExecute createTableToDropQuery
      boolean pgResult = pgExecute dropTableQuery
    and:
      dbExecute createTableToDropQuery
      boolean dbResult = dbExecute dropTableQuery
    then:
      pgResult == dbResult
  }

  def 'drop table if exists'() {
    given:
      String createTableIfNotExistsQuery = 'drop table if exists SCHEMA_NAME.TABLE_IF_EXISTS'
    when:
      boolean pgResult = pgExecute createTableIfNotExistsQuery
    and:
      boolean dbResult = dbExecute createTableIfNotExistsQuery
    then:
      pgResult == dbResult
  }

  def 'drop multiple tables if exists'() {
    given:
      String createTableQuery = 'create table SCHEMA_NAME.TABLE_TO_DROP ()'
      String dropTablesIfExistsQuery = 'drop table if exists SCHEMA_NAME.TABLE_IF_EXISTS, SCHEMA_NAME.TABLE_TO_DROP'
    and:
      pgExecute createTableQuery
      dbExecute createTableQuery
    when:
      boolean pgResult = pgExecute dropTablesIfExistsQuery
      boolean dbResult = dbExecute dropTablesIfExistsQuery
    and:
      SQLException pgError
      try {
        pgExecute 'drop table SCHEMA_NAME.TABLE_TO_DROP'
      } catch (SQLException e) {
        pgError = e
      }
      SQLException dbError
      try {
        dbExecute 'drop table SCHEMA_NAME.TABLE_TO_DROP'
      } catch (SQLException e) {
        dbError = e
      }
    then:
      pgResult == dbResult
    and:
      pgError.errorCode == dbError.errorCode
  }
}

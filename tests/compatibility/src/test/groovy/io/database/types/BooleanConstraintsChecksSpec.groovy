package io.database.types

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import io.database.SetupEnvironment
import spock.lang.Unroll

class BooleanConstraintsChecksSpec extends SetupEnvironment {
  private static final String CREATE_SCHEMA = 'create schema SCHEMA_NAME'
  private static final String CREATE_TABLE =
      """
          create table SCHEMA_NAME.TABLE_NAME (
              BOOL_COL boolean
          )
"""
  private static final String DROP_SCHEMA_CASCADE = 'drop schema SCHEMA_NAME cascade'
  private static final String SELECT_ALL_QUERY = 'select * from SCHEMA_NAME.TABLE_NAME'

  protected Sql pg
  protected Sql db

  def setup() {
    pgExecute(CREATE_SCHEMA)
    pgExecute(CREATE_TABLE)
    dbExecute(CREATE_SCHEMA)
    dbExecute(CREATE_TABLE)

    pg = Sql.newInstance(pgConf())
    db = Sql.newInstance(dbConf())
  }

  def cleanup() {
    pgExecute(DROP_SCHEMA_CASCADE)
    dbExecute(DROP_SCHEMA_CASCADE)

    pg.close()
    db.close()
  }

  @Unroll
  def 'inserting #value as boolean'() {
    given:
      String insertQuery = "insert into SCHEMA_NAME.TABLE_NAME values ($value)"

    when:
      pg.executeUpdate insertQuery
      db.executeUpdate insertQuery
    and:
      List<GroovyRowResult> pgSelect = pg.rows SELECT_ALL_QUERY
      List<GroovyRowResult> dbSelect = db.rows SELECT_ALL_QUERY

    then:
      println "SELECTION: $pgSelect"
    and:
      pgSelect == dbSelect

    where:
      value << [
          "TRUE",   "FALSE",
//          "'true'", "'false'",
//          "'t'",    "'f'",
//          "'yes'",  "'no'",
//          "'y'",    "'n'",
//          "'on'",   "'off'",
//          "'1'",    "'0'",
//          "TRUE::boolean",  "FALSE::boolean",
//          "'yes'::boolean", "'no'::boolean"
      ]
  }
}

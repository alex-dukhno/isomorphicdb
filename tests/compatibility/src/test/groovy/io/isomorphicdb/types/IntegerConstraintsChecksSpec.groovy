package io.isomorphicdb.types

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import io.isomorphicdb.SetupEnvironment
import spock.lang.Ignore
import spock.lang.Unroll

import java.sql.SQLException

class IntegerConstraintsChecksSpec extends SetupEnvironment {
  private static final String CREATE_SCHEMA = 'create schema SCHEMA_NAME'
  private static final String CREATE_TABLE =
      """
          create table SCHEMA_NAME.TABLE_NAME (
              SI_COL smallint,
              I__COL integer,
              BI_COL bigint
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

  @Ignore("-<int> is treated like UnaryMinus operation")
  def 'integer types min max limits'() {
    given:
      String insertMaxValues = 'insert into SCHEMA_NAME.TABLE_NAME values ( 32767,  2147483647,  9223372036854775807)'
      String insertMinValues = 'insert into SCHEMA_NAME.TABLE_NAME values (-32768, -2147483648, -9223372036854775808)'

    when:
      pg.executeUpdate insertMaxValues
      db.executeUpdate insertMaxValues
    and:
      pg.executeUpdate insertMinValues
      db.executeUpdate insertMinValues
    and:
      List<GroovyRowResult> pgSelect = pg.rows SELECT_ALL_QUERY
      List<GroovyRowResult> dbSelect = db.rows SELECT_ALL_QUERY

    then:
      println "SELECTION: ${pgSelect.inspect()}"
    and:
      pgSelect == dbSelect
  }

  @Ignore("type constraints are not supported")
  @Unroll
  def '#type out of range error'() {
    given:
      String outOfRange = "insert into SCHEMA_NAME.TABLE_NAME ($column, ${others[0]}, ${others[1]}) values ($value, 0, 0)"

    when:
      SQLException pgOutOfRange = null
      try {
        pg.execute outOfRange
      } catch(SQLException e) {
        pgOutOfRange = e
      }
    and:
      SQLException dbOutOfRange = null
      try {
        db.execute outOfRange
      } catch(SQLException e) {
        dbOutOfRange = e
      }

    then:
      println "PG ERROR: ${pgOutOfRange.inspect()}"
      println "DB ERROR: ${dbOutOfRange.inspect()}"
    and:
      pgOutOfRange.errorCode == dbOutOfRange.errorCode

    where:
      type << ['smallint', 'integer', 'bigint']
      column << ['SI_COL', 'I__COL', 'BI_COL']
      others << [['I__COL', 'BI_COL'], ['SI_COL', 'BI_COL'], ['SI_COL', 'I__COL']]
      value << [32768, 2147483648, 9223372036854775808]
  }

  @Ignore("type constraints are not supported")
  @Unroll
  def '#type type mismatch'() {
    given:
      String typeMismatch = "insert into SCHEMA_NAME.TABLE_NAME ($column, ${others[0]}, ${others[1]}) values ('this is a string', 0, 0)"
    when:
      SQLException pgMismatch = null
      try {
        pg.execute typeMismatch
      } catch(SQLException e) {
        pgMismatch = e
      }
    and:
      SQLException dbMismatch = null
      try {
        db.execute typeMismatch
      } catch(SQLException e) {
        dbMismatch = e
      }

    then:
      println "PG ERROR: ${pgMismatch.inspect()}"
      println "DB ERROR: ${dbMismatch.inspect()}"
    and:
      pgMismatch.errorCode == dbMismatch.errorCode

    where:
      type << ['smallint', 'integer', 'bigint']
      column << ['SI_COL', 'I__COL', 'BI_COL']
      others << [['I__COL', 'BI_COL'], ['SI_COL', 'BI_COL'], ['SI_COL', 'I__COL']]
  }

  @Ignore("binary operations and column names in ops are not supported")
  def 'update with columns of different types'() {
    given:
      String insertData = 'insert into SCHEMA_NAME.TABLE_NAME values (1000, 2000, 50000), (2000, 50000, 100000)'
      pg.executeUpdate insertData
      db.executeUpdate insertData
      String updateQueryWithDynamicExpression =
          """
    update SCHEMA_NAME.TABLE_NAME
    set SI_COL = 2 * SI_COL,
    I__COL = 2 * (SI_COL + I__COL),
    BI_COL = (BI_COL + (2 * (SI_COL + I__COL)))
"""

    when:
      int pgUpdates = pg.executeUpdate updateQueryWithDynamicExpression
      int dbUpdates = db.executeUpdate updateQueryWithDynamicExpression
    and:
      List<GroovyRowResult> pgSelect = pg.rows SELECT_ALL_QUERY
      List<GroovyRowResult> dbSelect = db.rows SELECT_ALL_QUERY

    then:
      println "UPDATED: ${pgUpdates.inspect()}"
      println "SELECTION: ${pgSelect.inspect()}"
    and:
      pgUpdates == dbUpdates
      pgSelect == dbSelect
  }
}

package io.isomorphicdb.types

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import io.isomorphicdb.SetupEnvironment
import spock.lang.Ignore
import spock.lang.Unroll

import java.sql.SQLException

class StringsConstraintsChecksSpec extends SetupEnvironment {
  private static final String CREATE_SCHEMA = 'create schema SCHEMA_NAME'
  private static final String CREATE_TABLE =
      """
          create table SCHEMA_NAME.TABLE_NAME (
              C____COL char,
              C__1_COL char(1),
              C__5_COL char(5),
              VC___COL varchar,
              VC_1_COL varchar(1),
              VC_5_COL varchar(5)
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

  @Ignore("isomorphicdb treats defaults and whitespace strings differently")
  def 'insert all spaces'() {
    given:
      String insertQuery = "insert into SCHEMA_NAME.TABLE_NAME values ('${' '*255}', ' ', '${' '*5}', '${' '*32768}', ' ', '${' '*5}')"

    when:
      pg.executeUpdate insertQuery
      db.executeUpdate insertQuery
    and:
      List<GroovyRowResult> pgSelect = pg.rows SELECT_ALL_QUERY
      List<GroovyRowResult> dbSelect = db.rows SELECT_ALL_QUERY

    then:
      println "SELECTION: ${pgSelect}"
    and:
      pgSelect == dbSelect
  }

  @Ignore("isomorphicdb treats defaults and whitespace strings differently")
  @Unroll
  def '#type too long value'() {
    given:
      String insertQuery = "insert into SCHEMA_NAME.TABLE_NAME (${column}, ${others[0]}, ${others[1]}, ${others[2]}, ${others[3]}, ${others[4]})" +
          " values ('${'a'*length}', 'a', 'a', 'a', 'a', 'a')"

    when:
      SQLException pgError
      try {
        pg.executeUpdate insertQuery
      } catch (SQLException e) {
        pgError = e
      }
    and:
      SQLException dbError
      try {
        db.executeUpdate insertQuery
      } catch (SQLException e) {
        dbError = e
      }

    then:
      println "PG ERROR: ${pgError}"
      println "DB ERROR: ${dbError}"
    and:
      pgError == null && dbError == null || pgError.errorCode == dbError.errorCode

    where:
      type << ['char', 'char(1)', 'char(5)', 'varchar', 'varchar(1)', 'varchar(5)']
      column << ['C____COL', 'C__1_COL', 'C__5_COL', 'VC___COL', 'VC_1_COL', 'VC_5_COL']
      others << [
          ['C__1_COL', 'C__5_COL', 'VC___COL', 'VC_1_COL', 'VC_5_COL'],
          ['C____COL', 'C__5_COL', 'VC___COL', 'VC_1_COL', 'VC_5_COL'],
          ['C____COL', 'C__1_COL', 'VC___COL', 'VC_1_COL', 'VC_5_COL'],
          ['C____COL', 'C__1_COL', 'C__5_COL', 'VC_1_COL', 'VC_5_COL'],
          ['C____COL', 'C__1_COL', 'C__5_COL', 'VC___COL', 'VC_5_COL'],
          ['C____COL', 'C__1_COL', 'C__5_COL', 'VC___COL', 'VC_1_COL'],
      ]
      length << [2, 2, 6, 32768, 2, 6]
  }
}

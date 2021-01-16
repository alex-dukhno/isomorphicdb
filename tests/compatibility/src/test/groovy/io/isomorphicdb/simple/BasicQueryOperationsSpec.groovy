package io.isomorphicdb.simple

import groovy.sql.GroovyRowResult
import io.isomorphicdb.ThreeSmallIntColumnTable

class BasicQueryOperationsSpec extends ThreeSmallIntColumnTable {
  def 'insert select{all}'() {
    given:
      String insertQuery = 'insert into SCHEMA_NAME.TABLE_NAME values (1, 2, 3), (4, 5, 6), (7, 8, 9)'

    when:
      int pgInserts = pg.executeUpdate insertQuery
      int dbInserts = db.executeUpdate insertQuery
    and:
      List<GroovyRowResult> pgSelect = pg.rows SELECT_ALL_QUERY
      List<GroovyRowResult> dbSelect = db.rows SELECT_ALL_QUERY

    then:
      println "INSERTED: ${pgInserts.inspect()}"
      println "SELECTION: ${pgSelect.inspect()}"
    and:
      pgInserts == dbInserts
      pgSelect == dbSelect
  }

  def 'insert select{listed column}'() {
    given:
      String insertQuery = 'insert into SCHEMA_NAME.TABLE_NAME values (1, 2, 3), (4, 5, 6), (7, 8, 9)'
      String selectQuery = 'select col1, col2, col3 from SCHEMA_NAME.TABLE_NAME'

    when:
      int pgInserts = pg.executeUpdate insertQuery
      int dbInserts = db.executeUpdate insertQuery
    and:
      List<GroovyRowResult> pgSelect = pg.rows selectQuery
      List<GroovyRowResult> dbSelect = db.rows selectQuery

    then:
      println "INSERTED: ${pgInserts.inspect()}"
      println "SELECTION: ${pgSelect.inspect()}"
    and:
      pgInserts == dbInserts
      pgSelect == dbSelect
  }

  def 'insert update{all} select{all}'() {
    given:
      String insertQuery = 'insert into SCHEMA_NAME.TABLE_NAME values (1, 2, 3), (4, 5, 6), (7, 8, 9)'
      String updateQuery = 'update SCHEMA_NAME.TABLE_NAME set col1 = 10, col2 = 11, col3 = 12'
    and:
      pg.executeUpdate insertQuery
      db.executeUpdate insertQuery

    when:
      int pgUpdates = pg.executeUpdate updateQuery
      int dbUpdates = db.executeUpdate updateQuery
    and:
      List<GroovyRowResult> pgSelect = pg.rows SELECT_ALL_QUERY
      List<GroovyRowResult> dbSelect =db.rows SELECT_ALL_QUERY

    then:
      println "UPDATED: ${pgUpdates.inspect()}"
      println "SELECTION: ${pgSelect.inspect()}"
    and:
      pgUpdates == dbUpdates
      pgSelect == dbSelect
  }

  def 'insert delete{all} select{all}'() {
    given:
      String insertQuery = 'insert into SCHEMA_NAME.TABLE_NAME values (1, 2, 3), (4, 5, 6), (7, 8, 9)'
      String deleteQuery = 'delete from SCHEMA_NAME.TABLE_NAME'
    and:
      pg.executeUpdate insertQuery
      db.executeUpdate insertQuery

    when:
      int pgDeletes = pg.executeUpdate deleteQuery
      int dbDeletes = db.executeUpdate deleteQuery
    and:
      List<GroovyRowResult> pgSelect = pg.rows SELECT_ALL_QUERY
      List<GroovyRowResult> dbSelect = db.rows SELECT_ALL_QUERY

    then:
      println "DELETED: ${pgDeletes.inspect()}"
      println "SELECTION: ${pgSelect.inspect()}"
    and:
      pgDeletes == dbDeletes
      pgSelect == dbSelect
  }
}

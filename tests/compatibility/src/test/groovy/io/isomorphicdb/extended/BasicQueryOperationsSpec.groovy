package io.isomorphicdb.extended

import groovy.sql.GroovyRowResult
import io.isomorphicdb.ThreeSmallIntColumnTable

class BasicQueryOperationsSpec extends ThreeSmallIntColumnTable {
  private static String INSERT_QUERY = 'insert into SCHEMA_NAME.TABLE_NAME values (?, ?, ?)'

  private int pgInserts = 0
  private int dbInserts = 0

  def setup() {
    pgInserts += pg.executeUpdate INSERT_QUERY, [1, 2, 3]
    pgInserts += pg.executeUpdate INSERT_QUERY, [4, 5, 6]
    pgInserts += pg.executeUpdate INSERT_QUERY, [7, 8, 9]

    dbInserts += db.executeUpdate INSERT_QUERY, [1, 2, 3]
    dbInserts += db.executeUpdate INSERT_QUERY, [4, 5, 6]
    dbInserts += db.executeUpdate INSERT_QUERY, [7, 8, 9]
  }

  def 'insert select{all}'() {
    when:
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
      String selectQuery = 'select col1, col2, col3 from SCHEMA_NAME.TABLE_NAME'

    when:
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
      String updateQuery = 'update SCHEMA_NAME.TABLE_NAME set col1 = ?, col2 = ?, col3 = ?'

    when:
      int pgUpdates = pg.executeUpdate updateQuery, [10, 11, 12]
      int dbUpdates = db.executeUpdate updateQuery, [10, 11, 12]
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

  def 'insert delete{all} select{all}'() {
    given:
      String deleteQuery = 'delete from SCHEMA_NAME.TABLE_NAME'

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

package io.database.extended

import groovy.sql.GroovyRowResult
import io.database.ThreeSmallIntColumnTable
import spock.lang.Ignore

class SelectiveAttributesQueryOperationsSpec extends ThreeSmallIntColumnTable {
  private static final String INSERT_QUERY = '''
    insert into SCHEMA_NAME.TABLE_NAME (COL2, COL3, COL1)
    values  (2, 3, 1),
            (5, 6, 4),
            (8, 9, 7)
'''

  def setup() {
    pg.executeUpdate INSERT_QUERY
    db.executeUpdate INSERT_QUERY
  }

  @Ignore("[Failed to execute: insert into SCHEMA_NAME.TABLE_NAME values (?, ?, ?), (?, ?, ?), (?, ?, ?) because: This connection has been closed.] happens when database executes insert")
  def 'update {specified column}'() {
    given:
      String updateQuery = 'update SCHEMA_NAME.TABLE_NAME set COL2 = ?'

    when:
      int pgUpdates = pg.executeUpdate updateQuery, [10]
      int dbUpdates = db.executeUpdate updateQuery, [10]

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

  @Ignore('[An I/O error occurred while sending to the backend.] happens when query sent to database')
  def 'update{with dynamic expression}'() {
    given:
      String updateQueryWithDynamicExpression =
          """
    update SCHEMA_NAME.TABLE_NAME
    set COL1 = ? * COL1,
    COL2 = ? * (COL1 + COL2),
    COL3 = (COL3 + (? * (COL1 + COL2)))
"""

    when:
      int pgUpdates = pg.executeUpdate updateQueryWithDynamicExpression, [2, 2, 2]
      int dbUpdates = db.executeUpdate updateQueryWithDynamicExpression, [2, 2, 2]
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

package io.isomorphicdb.simple

import groovy.sql.GroovyRowResult
import io.isomorphicdb.ThreeSmallIntColumnTable
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

  @Ignore("column names are case sensitive")
  def 'update {specified column}'() {
    given:
      String updateQuery = 'update SCHEMA_NAME.TABLE_NAME set COL2 = 10'

    when:
      int pgUpdates = pg.executeUpdate updateQuery
      int dbUpdates = db.executeUpdate updateQuery

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

  @Ignore("column names are case sensitive")
  def 'select{all reordered}'() {
    given:
      String selectAllReordered = 'select COL2, COL3, COL1 from SCHEMA_NAME.TABLE_NAME'

    when:
      List<GroovyRowResult> pgSelect = pg.rows selectAllReordered
      List<GroovyRowResult> dbSelect = db.rows selectAllReordered

    then:
      println "SELECTION: ${pgSelect.inspect()}"
    and:
      pgSelect == dbSelect
  }

  @Ignore("column names are case sensitive")
  def 'select{same column many times}'() {
    given:
      String selectSameColumnManyTimesQuery = 'select COL3, COL1, COL2, COL1, COL3 from SCHEMA_NAME.TABLE_NAME'

    when:
      List<GroovyRowResult> pgSelect = pg.rows selectSameColumnManyTimesQuery
      List<GroovyRowResult> dbSelect = db.rows selectSameColumnManyTimesQuery

    then:
      println "SELECTION: ${pgSelect.inspect()}"
    and:
      pgSelect == dbSelect
  }

  @Ignore("binary operations are not supported")
  def 'update{with dynamic expression}'() {
    given:
      String updateQueryWithDynamicExpression =
          """
    update SCHEMA_NAME.TABLE_NAME
    set COL1 = 2 * COL1,
    COL2 = 2 * (COL1 + COL2),
    COL3 = (COL3 + (2 * (COL1 + COL2)))
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

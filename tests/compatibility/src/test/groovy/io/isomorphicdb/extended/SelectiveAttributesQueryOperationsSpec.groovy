package io.isomorphicdb.extended

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

  @Ignore('''
org.postgresql.util.PSQLException: This connection has been closed.
should be fixed with extended query RFC implementation
''')
  def 'select{where specified column > ?}'() {
    given:
      String selectQuery = 'select * from SCHEMA_NAME.TABLE_NAME where COL1 > ?'

    when:
      List<GroovyRowResult> pgSelect = pg.rows selectQuery, [6]
      List<GroovyRowResult> dbSelect = db.rows selectQuery, [6]

    then:
      println "SELECTION: ${pgSelect.inspect()}"
    and:
      pgSelect == dbSelect
  }

  @Ignore('''
org.postgresql.util.PSQLException: This connection has been closed.
should be fixed with extended query RFC implementation
''')
  def 'select{where specified ? > column}'() {
    given:
      String selectQuery = 'select * from SCHEMA_NAME.TABLE_NAME where ? > COL1'

    when:
      List<GroovyRowResult> pgSelect = pg.rows selectQuery, [4]
      List<GroovyRowResult> dbSelect = db.rows selectQuery, [4]

    then:
      println "SELECTION: ${pgSelect.inspect()}"
    and:
      pgSelect == dbSelect
  }

  @Ignore('''
org.postgresql.util.PSQLException: This connection has been closed.
should be fixed with extended query RFC implementation
''')
  def 'select{where specified ? > ?}'() {
    given:
      String selectQuery = 'select * from SCHEMA_NAME.TABLE_NAME where ? > ?'

    when:
      List<GroovyRowResult> pgSelect = pg.rows selectQuery, [1, 0]
      List<GroovyRowResult> dbSelect = db.rows selectQuery, [1, 0]

    then:
      println "SELECTION: ${pgSelect.inspect()}"
    and:
      pgSelect == dbSelect
  }

  @Ignore('''
org.postgresql.util.PSQLException: This connection has been closed.
should be fixed with extended query RFC implementation
''')
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

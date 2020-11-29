package io.database.simple

import groovy.sql.GroovyRowResult
import io.database.ThreeSmallIntColumnTable
import java.sql.SQLException

class PreparedStatementOperationsSpec extends ThreeSmallIntColumnTable {
  private static final String PREPARE_QUERY = '''
        prepare fooplan (smallint, smallint, smallint) as
          insert into SCHEMA_NAME.TABLE_NAME
          values ($1, $2, $3)
'''

  private static final String EXECUTE_QUERY = 'execute fooplan(1, 2, 3)'
  private static final String DEALLOCATE_QUERY = 'deallocate fooplan'

  def 'parse execute deallocate select{all}'() {
    when:
      pg.executeUpdate PREPARE_QUERY
      int pgInserts = pg.executeUpdate EXECUTE_QUERY
      pg.executeUpdate DEALLOCATE_QUERY
    and:
      db.executeUpdate PREPARE_QUERY
      int dbInserts = db.executeUpdate EXECUTE_QUERY
      db.executeUpdate DEALLOCATE_QUERY
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

  def 'prepare with wrong parameter types'() {
    given:
      String prepareWithWrongParamType = '''
        prepare fooplan (i, j, k) as
          insert into SCHEMA_NAME.TABLE_NAME
          values ($1, $2, $3)
'''

    when:
      SQLException pgError
      try {
        pgExecute prepareWithWrongParamType
      } catch (SQLException e) {
        pgError = e
      }
    and:
      SQLException dbError
      try {
        dbExecute prepareWithWrongParamType
      } catch (SQLException e) {
        dbError = e
      }

    then:
      pgError.errorCode == dbError.errorCode
  }

  def 'execute deallocated prepared statement'() {
    when:
      pg.executeUpdate PREPARE_QUERY
      pg.executeUpdate DEALLOCATE_QUERY
      SQLException pgError
      try {
        pgExecute EXECUTE_QUERY
      } catch (SQLException e) {
        pgError = e
      }
    and:
      db.executeUpdate PREPARE_QUERY
      db.executeUpdate DEALLOCATE_QUERY
      SQLException dbError
      try {
        dbExecute EXECUTE_QUERY
      } catch (SQLException e) {
        dbError = e
      }
    then:
      pgError.errorCode == dbError.errorCode
  }
}

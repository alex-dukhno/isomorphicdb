package io.database.parser

import io.database.SetupEnvironment

import java.sql.SQLException

class ParsingErrorSpec extends SetupEnvironment {
  def 'parsing rubbish'() {
    given:
      String nonSqlString = 'can you please parse this rubbish?'
    when:
      SQLException pgError
      try {
        pgExecute nonSqlString
      } catch (SQLException e) {
        pgError = e
      }
    and:
      SQLException dbError
      try {
        dbExecute nonSqlString
      } catch (SQLException e) {
        dbError = e
      }
    then:
      pgError.errorCode == dbError.errorCode
  }
}

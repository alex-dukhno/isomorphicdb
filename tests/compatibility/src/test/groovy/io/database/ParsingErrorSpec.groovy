package io.database

import java.sql.SQLException

class ParsingErrorSpec extends SetupEnvironmentSpecification {
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

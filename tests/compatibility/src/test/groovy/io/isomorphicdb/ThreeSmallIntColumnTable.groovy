package io.isomorphicdb

import groovy.sql.Sql

class ThreeSmallIntColumnTable extends SetupEnvironment {
  private static final String CREATE_SCHEMA = 'create schema SCHEMA_NAME'
  private static final String CREATE_TABLE =
      """
          create table SCHEMA_NAME.TABLE_NAME (
              COL1 smallint,
              COL2 smallint,
              COL3 smallint
          )
"""
  private static final String DROP_SCHEMA_CASCADE = 'drop schema SCHEMA_NAME cascade'
  protected static final String SELECT_ALL_QUERY = 'select * from SCHEMA_NAME.TABLE_NAME'

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
}

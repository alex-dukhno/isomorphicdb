// Copyright 2020 Alex Dukhno
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate log;

use crate::{
    ddl::{
        create_schema::CreateSchemaCommand, create_table::CreateTableCommand, drop_schema::DropSchemaCommand,
        drop_table::DropTableCommand,
    },
    dml::{delete::DeleteCommand, insert::InsertCommand, select::SelectCommand, update::UpdateCommand},
};
use kernel::SystemResult;
use protocol::results::{QueryError, QueryEvent, QueryResult};

use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage};

mod ddl;
mod dml;

pub struct Handler<P: BackendStorage> {
    storage: Arc<Mutex<FrontendStorage<P>>>,
}

impl<P: BackendStorage> Handler<P> {
    pub fn new(storage: Arc<Mutex<FrontendStorage<P>>>) -> Self {
        Self { storage }
    }

    #[allow(clippy::match_wild_err_arm)]
    pub fn execute(&mut self, raw_sql_query: &str) -> SystemResult<QueryResult> {
        let statement = match Parser::parse_sql(&PostgreSqlDialect {}, raw_sql_query) {
            Ok(mut statements) => statements.pop().unwrap(),
            Err(e) => {
                log::error!("{:?} can't be parsed. Error: {:?}", raw_sql_query, e);
                unimplemented!("PANIC!!! Ah-a-a-a")
            }
        };
        log::debug!("STATEMENT = {:?}", statement);
        match statement {
            sqlparser::ast::Statement::StartTransaction { .. } => Ok(Ok(QueryEvent::TransactionStarted)),
            sqlparser::ast::Statement::SetVariable { .. } => Ok(Ok(QueryEvent::VariableSet)),
            sqlparser::ast::Statement::CreateTable { name, columns, .. } => {
                CreateTableCommand::new(name, columns, self.storage.clone()).execute()
            }
            sqlparser::ast::Statement::CreateSchema { schema_name, .. } => {
                CreateSchemaCommand::new(schema_name, self.storage.clone()).execute()
            }
            sqlparser::ast::Statement::Drop { object_type, names, .. } => match object_type {
                sqlparser::ast::ObjectType::Table => {
                    DropTableCommand::new(names[0].clone(), self.storage.clone()).execute()
                }
                sqlparser::ast::ObjectType::Schema => {
                    DropSchemaCommand::new(names[0].clone(), self.storage.clone()).execute()
                }
                _ => Ok(Err(QueryError::not_supported_operation(raw_sql_query.to_owned()))),
            },
            sqlparser::ast::Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => InsertCommand::new(raw_sql_query, table_name, columns, source, self.storage.clone()).execute(),
            sqlparser::ast::Statement::Query(query) => {
                SelectCommand::new(raw_sql_query, query, self.storage.clone()).execute()
            }
            sqlparser::ast::Statement::Update {
                table_name,
                assignments,
                ..
            } => UpdateCommand::new(raw_sql_query, table_name, assignments, self.storage.clone()).execute(),
            sqlparser::ast::Statement::Delete { table_name, .. } => {
                DeleteCommand::new(raw_sql_query, table_name, self.storage.clone()).execute()
            }
            _ => Ok(Err(QueryError::not_supported_operation(raw_sql_query.to_owned()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::sql_types::PostgreSqlType;
    use storage::frontend::FrontendStorage;
    use test_helpers::in_memory_backend_storage::InMemoryStorage;

    type InMemorySqlEngine = Handler<InMemoryStorage>;

    #[rstest::fixture]
    fn sql_engine() -> InMemorySqlEngine {
        Handler::new(in_memory_storage())
    }

    #[cfg(test)]
    mod schema {
        use super::*;

        #[rstest::rstest]
        fn create_schema_query(mut sql_engine: InMemorySqlEngine) {
            assert_eq!(
                sql_engine
                    .execute("create schema schema_name;")
                    .expect("no system errors"),
                Ok(QueryEvent::SchemaCreated)
            );
        }

        #[rstest::rstest]
        fn create_schema_with_the_same_name(mut sql_engine: InMemorySqlEngine) {
            sql_engine
                .execute("create schema schema_name;")
                .expect("no system errors")
                .expect("schema created");

            assert_eq!(
                sql_engine
                    .execute("create schema schema_name;")
                    .expect("no system errors"),
                Err(QueryError::schema_already_exists("schema_name".to_owned()))
            );
        }

        #[rstest::rstest]
        fn drop_schema(mut sql_engine: InMemorySqlEngine) {
            sql_engine
                .execute("create schema schema_name;")
                .expect("no system errors")
                .expect("schema created");

            assert_eq!(
                sql_engine
                    .execute("drop schema schema_name;")
                    .expect("no system errors"),
                Ok(QueryEvent::SchemaDropped)
            );
            assert_eq!(
                sql_engine
                    .execute("create schema schema_name;")
                    .expect("no system errors"),
                Ok(QueryEvent::SchemaCreated)
            );
        }

        #[rstest::rstest]
        fn drop_non_existent_schema(mut sql_engine: InMemorySqlEngine) {
            assert_eq!(
                sql_engine
                    .execute("drop schema non_existent")
                    .expect("no system errors"),
                Err(QueryError::schema_does_not_exist("non_existent".to_owned()))
            );
        }

        #[rstest::rstest]
        fn select_from_nonexistent_schema(mut sql_engine: InMemorySqlEngine) {
            assert_eq!(
                sql_engine
                    .execute("select * from non_existent.some_table;")
                    .expect("no system errors"),
                Err(QueryError::schema_does_not_exist("non_existent".to_owned()))
            );
        }

        #[rstest::rstest]
        fn select_named_columns_from_nonexistent_schema(mut sql_engine: InMemorySqlEngine) {
            assert_eq!(
                sql_engine
                    .execute("select column_1 from schema_name.table_name;")
                    .expect("no system errors"),
                Err(QueryError::schema_does_not_exist("schema_name".to_owned()))
            );
        }

        #[rstest::rstest]
        fn insert_into_table_in_nonexistent_schema(mut sql_engine: InMemorySqlEngine) {
            assert_eq!(
                sql_engine
                    .execute("insert into schema_name.table_name values (123);")
                    .expect("no system errors"),
                Err(QueryError::schema_does_not_exist("schema_name".to_owned()))
            );
        }

        #[rstest::rstest]
        fn update_records_in_table_from_non_existent_schema(mut sql_engine: InMemorySqlEngine) {
            assert_eq!(
                sql_engine
                    .execute("update schema_name.table_name set column_test=789;")
                    .expect("no system errors"),
                Err(QueryError::schema_does_not_exist("schema_name".to_owned()))
            );
        }

        #[rstest::rstest]
        fn delete_from_table_in_nonexistent_schema(mut sql_engine: InMemorySqlEngine) {
            assert_eq!(
                sql_engine
                    .execute("delete from schema_name.table_name;")
                    .expect("no system errors"),
                Err(QueryError::schema_does_not_exist("schema_name".to_owned()))
            );
        }
    }

    #[cfg(test)]
    mod table {
        use super::*;

        #[cfg(test)]
        mod schemaless {
            use super::*;

            #[rstest::rstest]
            fn create_table_in_non_existent_schema(mut sql_engine: InMemorySqlEngine) {
                assert_eq!(
                    sql_engine
                        .execute("create table schema_name.table_name (column_name smallint);")
                        .expect("no system errors"),
                    Err(QueryError::schema_does_not_exist("schema_name".to_owned()))
                );
            }

            #[rstest::rstest]
            fn drop_table_from_non_existent_schema(mut sql_engine: InMemorySqlEngine) {
                assert_eq!(
                    sql_engine
                        .execute("drop table schema_name.table_name;")
                        .expect("no system errors"),
                    Err(QueryError::schema_does_not_exist("schema_name".to_owned()))
                );
            }
        }

        #[rstest::fixture]
        fn sql_engine_with_schema(mut sql_engine: InMemorySqlEngine) -> InMemorySqlEngine {
            sql_engine
                .execute("create schema schema_name;")
                .expect("no system errors")
                .expect("schema created");

            sql_engine
        }

        #[rstest::rstest]
        fn create_table(mut sql_engine_with_schema: InMemorySqlEngine) {
            assert_eq!(
                sql_engine_with_schema
                    .execute("create table schema_name.table_name (column_name smallint);")
                    .expect("no system errors"),
                Ok(QueryEvent::TableCreated)
            );
        }

        #[rstest::rstest]
        fn drop_table(mut sql_engine_with_schema: InMemorySqlEngine) {
            sql_engine_with_schema
                .execute("create table schema_name.table_name (column_name smallint);")
                .expect("no system errors")
                .expect("table created");

            assert_eq!(
                sql_engine_with_schema
                    .execute("drop table schema_name.table_name;")
                    .expect("no system errors"),
                Ok(QueryEvent::TableDropped)
            );
            assert_eq!(
                sql_engine_with_schema
                    .execute("create table schema_name.table_name (column_name smallint);")
                    .expect("no system errors"),
                Ok(QueryEvent::TableCreated)
            );
        }

        #[rstest::rstest]
        fn drop_non_existent_table(mut sql_engine_with_schema: InMemorySqlEngine) {
            assert_eq!(
                sql_engine_with_schema
                    .execute("drop table schema_name.table_name;")
                    .expect("no system errors"),
                Err(QueryError::table_does_not_exist("schema_name.table_name".to_owned()))
            );
        }

        #[rstest::rstest]
        fn select_from_not_existed_table(mut sql_engine_with_schema: InMemorySqlEngine) {
            assert_eq!(
                sql_engine_with_schema
                    .execute("select * from schema_name.non_existent;")
                    .expect("no system errors"),
                Err(QueryError::table_does_not_exist("schema_name.non_existent".to_owned()))
            );
        }

        #[rstest::rstest]
        fn select_named_columns_from_non_existent_table(mut sql_engine_with_schema: InMemorySqlEngine) {
            assert_eq!(
                sql_engine_with_schema
                    .execute("select column_1 from schema_name.non_existent;")
                    .expect("no system errors"),
                Err(QueryError::table_does_not_exist("schema_name.non_existent".to_owned()))
            );
        }

        #[rstest::rstest]
        fn insert_into_nonexistent_table(mut sql_engine_with_schema: InMemorySqlEngine) {
            assert_eq!(
                sql_engine_with_schema
                    .execute("insert into schema_name.table_name values (123);")
                    .expect("no system errors"),
                Err(QueryError::table_does_not_exist("schema_name.table_name".to_owned()))
            );
        }

        #[rstest::rstest]
        fn update_records_in_non_existent_table(mut sql_engine_with_schema: InMemorySqlEngine) {
            assert_eq!(
                sql_engine_with_schema
                    .execute("update schema_name.table_name set column_test=789;")
                    .expect("no system errors"),
                Err(QueryError::table_does_not_exist("schema_name.table_name".to_owned()))
            );
        }

        #[rstest::rstest]
        fn delete_from_nonexistent_table(mut sql_engine_with_schema: InMemorySqlEngine) {
            assert_eq!(
                sql_engine_with_schema
                    .execute("delete from schema_name.table_name;")
                    .expect("no system errors"),
                Err(QueryError::table_does_not_exist("schema_name.table_name".to_owned()))
            );
        }
    }

    #[rstest::rstest]
    fn insert_and_select_single_row(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (column_test smallint);")
            .expect("no system errors")
            .expect("table created");

        assert_eq!(
            sql_engine
                .execute("insert into schema_name.table_name values (123);")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsInserted(1))
        );
        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
                vec![vec!["123".to_owned()]]
            )))
        );
    }

    #[rstest::rstest]
    fn insert_and_select_multiple_rows(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (column_test smallint);")
            .expect("no system errors")
            .expect("table created");
        sql_engine
            .execute("insert into schema_name.table_name values (123);")
            .expect("no system errors")
            .expect("row inserted");

        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
                vec![vec!["123".to_owned()]]
            )))
        );

        sql_engine
            .execute("insert into schema_name.table_name values (456);")
            .expect("no system errors")
            .expect("row inserted");

        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
                vec![vec!["123".to_owned()], vec!["456".to_owned()]]
            )))
        );
    }

    #[rstest::rstest]
    fn insert_and_select_named_columns(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);")
            .expect("no system errors")
            .expect("table created");
        sql_engine
            .execute("insert into schema_name.table_name (col2, col3, col1) values (1, 2, 3), (4, 5, 6);")
            .expect("no system errors")
            .expect("row inserted");

        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("col1".to_owned(), PostgreSqlType::SmallInt),
                    ("col2".to_owned(), PostgreSqlType::SmallInt),
                    ("col3".to_owned(), PostgreSqlType::SmallInt),
                ],
                vec![
                    vec!["3".to_owned(), "1".to_owned(), "2".to_owned()],
                    vec!["6".to_owned(), "4".to_owned(), "5".to_owned()],
                ]
            )))
        );
    }

    #[rstest::rstest]
    fn update_all_records(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (column_test smallint);")
            .expect("no system errors")
            .expect("table created");
        sql_engine
            .execute("insert into schema_name.table_name values (123);")
            .expect("no system errors")
            .expect("row inserted");
        sql_engine
            .execute("insert into schema_name.table_name values (456);")
            .expect("no system errors")
            .expect("row inserted");

        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
                vec![vec!["123".to_owned()], vec!["456".to_owned()]]
            )))
        );
        assert_eq!(
            sql_engine
                .execute("update schema_name.table_name set column_test=789;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsUpdated(2))
        );
        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
                vec![vec!["789".to_owned()], vec!["789".to_owned()]]
            )))
        );
    }

    #[rstest::rstest]
    fn update_single_column_of_all_records(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (col1 smallint, col2 smallint);")
            .expect("no system errors")
            .expect("table created");
        sql_engine
            .execute("insert into schema_name.table_name values (123, 789);")
            .expect("no system errors")
            .expect("row inserted");
        sql_engine
            .execute("insert into schema_name.table_name values (456, 789);")
            .expect("no system errors")
            .expect("row inserted");

        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("col1".to_owned(), PostgreSqlType::SmallInt),
                    ("col2".to_owned(), PostgreSqlType::SmallInt),
                ],
                vec![
                    vec!["123".to_owned(), "789".to_owned()],
                    vec!["456".to_owned(), "789".to_owned()],
                ]
            )))
        );
        assert_eq!(
            sql_engine
                .execute("update schema_name.table_name set col2=357;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsUpdated(2))
        );
        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("col1".to_owned(), PostgreSqlType::SmallInt),
                    ("col2".to_owned(), PostgreSqlType::SmallInt),
                ],
                vec![
                    vec!["123".to_owned(), "357".to_owned()],
                    vec!["456".to_owned(), "357".to_owned()],
                ]
            )))
        );
    }

    #[rstest::rstest]
    fn update_multiple_columns_of_all_records(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);")
            .expect("no system errors")
            .expect("table created");
        sql_engine
            .execute("insert into schema_name.table_name values (111, 222, 333);")
            .expect("no system errors")
            .expect("row inserted");
        sql_engine
            .execute("insert into schema_name.table_name values (444, 555, 666);")
            .expect("no system errors")
            .expect("row inserted");

        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("col1".to_owned(), PostgreSqlType::SmallInt),
                    ("col2".to_owned(), PostgreSqlType::SmallInt),
                    ("col3".to_owned(), PostgreSqlType::SmallInt),
                ],
                vec![
                    vec!["111".to_owned(), "222".to_owned(), "333".to_owned()],
                    vec!["444".to_owned(), "555".to_owned(), "666".to_owned()],
                ]
            )))
        );
        assert_eq!(
            sql_engine
                .execute("update schema_name.table_name set col3=777, col1=999;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsUpdated(2))
        );
        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("col1".to_owned(), PostgreSqlType::SmallInt),
                    ("col2".to_owned(), PostgreSqlType::SmallInt),
                    ("col3".to_owned(), PostgreSqlType::SmallInt),
                ],
                vec![
                    vec!["999".to_owned(), "222".to_owned(), "777".to_owned()],
                    vec!["999".to_owned(), "555".to_owned(), "777".to_owned()],
                ]
            )))
        );
    }

    #[rstest::rstest]
    fn update_all_records_in_multiple_columns(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
            .expect("no system errors")
            .expect("table created");
        sql_engine
            .execute("insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);")
            .expect("no system errors")
            .expect("rows inserted");

        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("column_1".to_owned(), PostgreSqlType::SmallInt),
                    ("column_2".to_owned(), PostgreSqlType::SmallInt),
                    ("column_3".to_owned(), PostgreSqlType::SmallInt)
                ],
                vec![
                    vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                    vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                    vec!["7".to_owned(), "8".to_owned(), "9".to_owned()]
                ]
            )))
        );
        assert_eq!(
            sql_engine
                .execute("update schema_name.table_name set column_1=10, column_2=-20, column_3=30;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsUpdated(3))
        );
        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("column_1".to_owned(), PostgreSqlType::SmallInt),
                    ("column_2".to_owned(), PostgreSqlType::SmallInt),
                    ("column_3".to_owned(), PostgreSqlType::SmallInt)
                ],
                vec![
                    vec!["10".to_owned(), "-20".to_owned(), "30".to_owned()],
                    vec!["10".to_owned(), "-20".to_owned(), "30".to_owned()],
                    vec!["10".to_owned(), "-20".to_owned(), "30".to_owned()]
                ]
            )))
        );
    }

    #[rstest::rstest]
    fn update_records_in_nonexistent_table(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");

        assert_eq!(
            sql_engine
                .execute("update schema_name.table_name set column_test=789;")
                .expect("no system errors"),
            Err(QueryError::table_does_not_exist("schema_name.table_name".to_owned()))
        );
    }

    #[rstest::rstest]
    fn update_non_existent_columns_of_records(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (column_test smallint);")
            .expect("no system errors")
            .expect("table created");
        sql_engine
            .execute("insert into schema_name.table_name values (123);")
            .expect("no system errors")
            .expect("row inserted");

        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
                vec![vec!["123".to_owned()]],
            )))
        );
        assert_eq!(
            sql_engine
                .execute("update schema_name.table_name set col1=456, col2=789;")
                .expect("no system errors"),
            Err(QueryError::column_does_not_exist(vec![
                "col1".to_owned(),
                "col2".to_owned()
            ]))
        );
    }

    #[rstest::rstest]
    fn delete_all_records(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (column_test smallint);")
            .expect("no system errors")
            .expect("table created");
        sql_engine
            .execute("insert into schema_name.table_name values (123);")
            .expect("no system errors")
            .expect("row inserted");
        sql_engine
            .execute("insert into schema_name.table_name values (456);")
            .expect("no system errors")
            .expect("row inserted");
        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
                vec![vec!["123".to_owned()], vec!["456".to_owned()]]
            )))
        );
        assert_eq!(
            sql_engine
                .execute("delete from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsDeleted(2))
        );
        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
                vec![]
            )))
        );
    }

    #[rstest::rstest]
    fn select_all_from_table_with_multiple_columns(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
            .expect("no system errors")
            .expect("table created");
        sql_engine
            .execute("insert into schema_name.table_name values (123, 456, 789);")
            .expect("no system errors")
            .expect("row inserted");

        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("column_1".to_owned(), PostgreSqlType::SmallInt),
                    ("column_2".to_owned(), PostgreSqlType::SmallInt),
                    ("column_3".to_owned(), PostgreSqlType::SmallInt)
                ],
                vec![vec!["123".to_owned(), "456".to_owned(), "789".to_owned()]]
            )))
        );
    }

    #[rstest::rstest]
    fn insert_multiple_rows(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
            .expect("no system errors")
            .expect("table created");

        assert_eq!(
            sql_engine
                .execute("insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsInserted(3))
        );
        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("column_1".to_owned(), PostgreSqlType::SmallInt),
                    ("column_2".to_owned(), PostgreSqlType::SmallInt),
                    ("column_3".to_owned(), PostgreSqlType::SmallInt)
                ],
                vec![
                    vec!["1".to_owned(), "4".to_owned(), "7".to_owned()],
                    vec!["2".to_owned(), "5".to_owned(), "8".to_owned()],
                    vec!["3".to_owned(), "6".to_owned(), "9".to_owned()],
                ]
            )))
        );
    }

    #[rstest::rstest]
    fn select_not_all_columns(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
            .expect("no system errors")
            .expect("table created");
        sql_engine
            .execute("insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);")
            .expect("no system errors")
            .expect("rows inserted");

        assert_eq!(
            sql_engine
                .execute("select column_3, column_2 from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("column_3".to_owned(), PostgreSqlType::SmallInt),
                    ("column_2".to_owned(), PostgreSqlType::SmallInt),
                ],
                vec![
                    vec!["7".to_owned(), "4".to_owned()],
                    vec!["8".to_owned(), "5".to_owned()],
                    vec!["9".to_owned(), "6".to_owned()],
                ]
            )))
        );
    }

    #[rstest::rstest]
    fn select_non_existing_columns_from_table(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        sql_engine
            .execute("create table schema_name.table_name (column_in_table smallint);")
            .expect("no system errors")
            .expect("table created");

        assert_eq!(
            sql_engine
                .execute("select column_not_in_table1, column_not_in_table2 from schema_name.table_name;")
                .expect("no system errors"),
            Err(QueryError::column_does_not_exist(vec![
                "column_not_in_table1".to_owned(),
                "column_not_in_table2".to_owned()
            ]))
        );
    }

    #[rstest::rstest]
    fn create_table_with_different_types(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");

        assert_eq!(
            sql_engine
                .execute("create table schema_name.table_name (column_si smallint, column_i integer, column_bi bigint, column_c char(10), column_vc varchar(10));")
                .expect("no system errors"),
            Ok(QueryEvent::TableCreated)
        )
    }

    #[rstest::rstest]
    fn insert_and_select_different_integer_types(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");

        sql_engine
            .execute("create table schema_name.table_name (column_si smallint, column_i integer, column_bi bigint);")
            .expect("no system errors")
            .expect("table created");

        assert_eq!(
            sql_engine
                .execute("insert into schema_name.table_name values(-32768, -2147483648, -9223372036854775808);")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsInserted(1))
        );

        assert_eq!(
            sql_engine
                .execute("insert into schema_name.table_name values(32767, 2147483647, 9223372036854775807);")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsInserted(1))
        );

        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("column_si".to_owned(), PostgreSqlType::SmallInt),
                    ("column_i".to_owned(), PostgreSqlType::Integer),
                    ("column_bi".to_owned(), PostgreSqlType::BigInt),
                ],
                vec![
                    vec![
                        "-32768".to_owned(),
                        "-2147483648".to_owned(),
                        "-9223372036854775808".to_owned()
                    ],
                    vec![
                        "32767".to_owned(),
                        "2147483647".to_owned(),
                        "9223372036854775807".to_owned()
                    ],
                ]
            )))
        )
    }

    #[rstest::rstest]
    fn insert_and_select_different_character_types(mut sql_engine: InMemorySqlEngine) {
        sql_engine
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");

        sql_engine
            .execute("create table schema_name.table_name (column_c char(10), column_vc varchar(10));")
            .expect("no system errors")
            .expect("table created");

        assert_eq!(
            sql_engine
                .execute("insert into schema_name.table_name values('12345abcde', '12345abcde');")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsInserted(1))
        );

        assert_eq!(
            sql_engine
                .execute("insert into schema_name.table_name values('12345abcde', 'abcde');")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsInserted(1))
        );

        assert_eq!(
            sql_engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("column_c".to_owned(), PostgreSqlType::Char),
                    ("column_vc".to_owned(), PostgreSqlType::VarChar)
                ],
                vec![
                    vec!["12345abcde".to_owned(), "12345abcde".to_owned()],
                    vec!["12345abcde".to_owned(), "abcde".to_owned()],
                ]
            )))
        )
    }

    fn in_memory_storage() -> Arc<Mutex<FrontendStorage<InMemoryStorage>>> {
        Arc::new(Mutex::new(FrontendStorage::new(InMemoryStorage::default()).unwrap()))
    }
}

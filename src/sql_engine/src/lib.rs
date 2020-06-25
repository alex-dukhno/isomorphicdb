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

use kernel::SystemResult;
use std::sync::{Arc, Mutex};

use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
use std::ops::Deref;
use storage::{
    CreateTableError, DropTableError, OperationOnTableError, Projection, SchemaAlreadyExists, SchemaDoesNotExist,
    {backend::BackendStorage, frontend::FrontendStorage},
};

use sql_types::SqlType;
use thiserror::Error;

pub type QueryResult = std::result::Result<QueryEvent, QueryError>;

#[derive(Debug, PartialEq, Error)]
pub enum QueryError {
    #[error("schema {0} already exists")]
    SchemaAlreadyExists(String),
    #[error("table {0} already exists")]
    TableAlreadyExists(String),
    #[error("schema {0} does not exist")]
    SchemaDoesNotExist(String),
    #[error("table {0} does not exist")]
    TableDoesNotExist(String),
    #[error("column {0:?} does not exist")]
    ColumnDoesNotExist(Vec<String>),
    #[error("not supported operation")]
    NotSupportedOperation(String),
}

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
            Err(_) => {
                log::debug!("TERMINATION");
                return Ok(Ok(QueryEvent::Terminate));
            }
        };
        log::debug!("STATEMENT = {:?}", statement);
        match statement {
            sqlparser::ast::Statement::CreateTable { mut name, columns, .. } => {
                let table_name = name.0.pop().unwrap().to_string();
                let schema_name = name.0.pop().unwrap().to_string();
                match (self.storage.lock().unwrap()).create_table(
                    &schema_name,
                    &table_name,
                    columns
                        .into_iter()
                        .map(|c| {
                            let name = c.name.to_string();
                            let sql_type = match c.data_type {
                                sqlparser::ast::DataType::SmallInt => SqlType::SmallInt,
                                sqlparser::ast::DataType::Int => SqlType::Integer,
                                sqlparser::ast::DataType::BigInt => SqlType::BigInt,
                                sqlparser::ast::DataType::Char(len) => SqlType::Char(len.unwrap_or(255)),
                                sqlparser::ast::DataType::Varchar(len) => SqlType::VarChar(len.unwrap_or(255)),
                                _ => unimplemented!(),
                            };
                            (name, sql_type)
                        })
                        .collect(),
                )? {
                    Ok(()) => Ok(Ok(QueryEvent::TableCreated)),
                    Err(CreateTableError::SchemaDoesNotExist) => Ok(Err(QueryError::SchemaDoesNotExist(schema_name))),
                    Err(CreateTableError::TableAlreadyExists) => Ok(Err(QueryError::TableAlreadyExists(table_name))),
                }
            }
            sqlparser::ast::Statement::CreateSchema { schema_name, .. } => {
                let schema_name = schema_name.to_string();
                match (self.storage.lock().unwrap()).create_schema(&schema_name)? {
                    Ok(()) => Ok(Ok(QueryEvent::SchemaCreated)),
                    Err(SchemaAlreadyExists) => Ok(Err(QueryError::SchemaAlreadyExists(schema_name))),
                }
            }
            sqlparser::ast::Statement::Drop { object_type, names, .. } => match object_type {
                sqlparser::ast::ObjectType::Table => {
                    let table_name = names[0].0[1].to_string();
                    let schema_name = names[0].0[0].to_string();
                    match (self.storage.lock().unwrap()).drop_table(&schema_name, &table_name)? {
                        Ok(()) => Ok(Ok(QueryEvent::TableDropped)),
                        Err(DropTableError::TableDoesNotExist) => Ok(Err(QueryError::TableDoesNotExist(
                            schema_name + "." + table_name.as_str(),
                        ))),
                        Err(DropTableError::SchemaDoesNotExist) => Ok(Err(QueryError::SchemaDoesNotExist(schema_name))),
                    }
                }
                sqlparser::ast::ObjectType::Schema => {
                    let schema_name = names[0].0[0].to_string();
                    match (self.storage.lock().unwrap()).drop_schema(&schema_name)? {
                        Ok(()) => Ok(Ok(QueryEvent::SchemaDropped)),
                        Err(SchemaDoesNotExist) => Ok(Err(QueryError::SchemaDoesNotExist(schema_name))),
                    }
                }
                _ => Ok(Err(QueryError::NotSupportedOperation(raw_sql_query.to_owned()))),
            },
            sqlparser::ast::Statement::Insert {
                mut table_name, source, ..
            } => {
                let name = table_name.0.pop().unwrap().to_string();
                let schema_name = table_name.0.pop().unwrap().to_string();
                let sqlparser::ast::Query { body, .. } = &*source;
                if let sqlparser::ast::SetExpr::Values(values) = &body {
                    let values = &values.0;
                    let to_insert: Vec<Vec<String>> = values
                        .iter()
                        .map(|v| {
                            v.iter()
                                .map(|v| match v {
                                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(v)) => v.to_string(),
                                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(v)) => {
                                        v.to_string()
                                    }
                                    sqlparser::ast::Expr::UnaryOp { op, expr } => match (op, &**expr) {
                                        (
                                            sqlparser::ast::UnaryOperator::Minus,
                                            sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(v)),
                                        ) => "-".to_owned() + v.as_str(),
                                        (op, expr) => unimplemented!("{:?} {:?} is not currently supported", op, expr),
                                    },
                                    expr => unimplemented!("{:?} is not currently supported", expr),
                                })
                                .collect()
                        })
                        .collect();
                    let len = to_insert.len();
                    match (self.storage.lock().unwrap()).insert_into(&schema_name, &name, to_insert)? {
                        Ok(_) => Ok(Ok(QueryEvent::RecordsInserted(len))),
                        Err(OperationOnTableError::SchemaDoesNotExist) => {
                            Ok(Err(QueryError::SchemaDoesNotExist(schema_name)))
                        }
                        Err(OperationOnTableError::TableDoesNotExist) => {
                            Ok(Err(QueryError::TableDoesNotExist(schema_name + "." + name.as_str())))
                        }
                        Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)) => {
                            Ok(Err(QueryError::ColumnDoesNotExist(non_existing_columns)))
                        }
                        Err(e) => {
                            eprintln!("{:?}", e);
                            unimplemented!()
                        }
                    }
                } else {
                    Ok(Err(QueryError::NotSupportedOperation(raw_sql_query.to_owned())))
                }
            }
            sqlparser::ast::Statement::Query(query) => {
                let sqlparser::ast::Query { body, .. } = *query;
                if let sqlparser::ast::SetExpr::Select(select) = body {
                    let sqlparser::ast::Select { projection, from, .. } = select.deref();
                    let sqlparser::ast::TableWithJoins { relation, .. } = &from[0];
                    let (schema_name, table_name) = match relation {
                        sqlparser::ast::TableFactor::Table { name, .. } => {
                            let table_name = name.0[1].to_string();
                            let schema_name = name.0[0].to_string();
                            (schema_name, table_name)
                        }
                        _ => return Ok(Err(QueryError::NotSupportedOperation(raw_sql_query.to_owned()))),
                    };
                    let table_columns = {
                        let projection = projection.clone();
                        let mut columns: Vec<String> = vec![];
                        for item in projection {
                            match item {
                                sqlparser::ast::SelectItem::Wildcard => {
                                    match (self.storage.lock().unwrap()).table_columns(&schema_name, &table_name)? {
                                        Ok(all_columns) => columns.extend(
                                            all_columns
                                                .into_iter()
                                                .map(|(name, _sql_type)| name)
                                                .collect::<Vec<String>>(),
                                        ),
                                        Err(_e) => {
                                            return Ok(Err(QueryError::NotSupportedOperation(raw_sql_query.to_owned())))
                                        }
                                    }
                                }
                                sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(
                                    sqlparser::ast::Ident { value, .. },
                                )) => columns.push(value.clone()),
                                _ => return Ok(Err(QueryError::NotSupportedOperation(raw_sql_query.to_owned()))),
                            }
                        }
                        columns
                    };
                    match (self.storage.lock().unwrap()).select_all_from(&schema_name, &table_name, table_columns)? {
                        Ok(records) => Ok(Ok(QueryEvent::RecordsSelected(records))),
                        Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)) => {
                            Ok(Err(QueryError::ColumnDoesNotExist(non_existing_columns)))
                        }
                        _ => Ok(Err(QueryError::NotSupportedOperation(raw_sql_query.to_owned()))),
                    }
                } else {
                    Ok(Err(QueryError::NotSupportedOperation(raw_sql_query.to_owned())))
                }
            }
            sqlparser::ast::Statement::Update {
                table_name,
                assignments,
                ..
            } => {
                let schema_name = table_name.0[0].to_string();
                let table_name = table_name.0[1].to_string();
                let sqlparser::ast::Assignment { value, .. } = &assignments[0];
                if let sqlparser::ast::Expr::Value(value) = value {
                    if let sqlparser::ast::Value::Number(value) = value {
                        match (self.storage.lock().unwrap()).update_all(&schema_name, &table_name, value.to_string())? {
                            Ok(records_number) => Ok(Ok(QueryEvent::RecordsUpdated(records_number))),
                            Err(OperationOnTableError::SchemaDoesNotExist) => {
                                Ok(Err(QueryError::SchemaDoesNotExist(schema_name)))
                            }
                            Err(OperationOnTableError::TableDoesNotExist) => Ok(Err(QueryError::TableDoesNotExist(
                                schema_name + "." + table_name.as_str(),
                            ))),
                            Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)) => {
                                Ok(Err(QueryError::ColumnDoesNotExist(non_existing_columns)))
                            }
                            _ => unimplemented!(),
                        }
                    } else {
                        Ok(Err(QueryError::NotSupportedOperation(raw_sql_query.to_owned())))
                    }
                } else {
                    Ok(Err(QueryError::NotSupportedOperation(raw_sql_query.to_owned())))
                }
            }
            sqlparser::ast::Statement::Delete { table_name, .. } => {
                let schema_name = table_name.0[0].to_string();
                let table_name = table_name.0[1].to_string();
                match (self.storage.lock().unwrap()).delete_all_from(&schema_name, &table_name)? {
                    Ok(records_number) => Ok(Ok(QueryEvent::RecordsDeleted(records_number))),
                    Err(OperationOnTableError::SchemaDoesNotExist) => {
                        Ok(Err(QueryError::SchemaDoesNotExist(schema_name)))
                    }
                    Err(OperationOnTableError::TableDoesNotExist) => Ok(Err(QueryError::TableDoesNotExist(
                        schema_name + "." + table_name.as_str(),
                    ))),
                    Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)) => {
                        Ok(Err(QueryError::ColumnDoesNotExist(non_existing_columns)))
                    }
                    _ => unimplemented!(),
                }
            }
            _ => Ok(Err(QueryError::NotSupportedOperation(raw_sql_query.to_owned()))),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum QueryEvent {
    SchemaCreated,
    SchemaDropped,
    TableCreated,
    TableDropped,
    RecordsInserted(usize),
    RecordsSelected(Projection),
    RecordsUpdated(usize),
    RecordsDeleted(usize),
    Terminate, // TODO workaround for integration tests
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_types::SqlType;
    use storage::frontend::FrontendStorage;
    use test_helpers::in_memory_backend_storage::InMemoryStorage;

    #[cfg(test)]
    mod schema {
        use super::*;

        #[test]
        fn create_schema_query() {
            let mut handler = Handler::new(in_memory_storage());

            assert_eq!(
                handler.execute("create schema schema_name;").expect("no system errors"),
                Ok(QueryEvent::SchemaCreated)
            );
        }

        #[test]
        fn create_schema_with_the_same_name() {
            let mut handler = Handler::new(in_memory_storage());

            handler
                .execute("create schema schema_name;")
                .expect("no system errors")
                .expect("schema created");

            assert_eq!(
                handler.execute("create schema schema_name;").expect("no system errors"),
                Err(QueryError::SchemaAlreadyExists("schema_name".to_owned()))
            );
        }

        #[test]
        fn drop_schema() {
            let mut handler = Handler::new(in_memory_storage());

            handler
                .execute("create schema schema_name;")
                .expect("no system errors")
                .expect("schema created");

            assert_eq!(
                handler.execute("drop schema schema_name;").expect("no system errors"),
                Ok(QueryEvent::SchemaDropped)
            );
            assert_eq!(
                handler.execute("create schema schema_name;").expect("no system errors"),
                Ok(QueryEvent::SchemaCreated)
            );
        }

        #[test]
        fn drop_non_existent_schema() {
            let mut handler = Handler::new(in_memory_storage());

            assert_eq!(
                handler.execute("drop schema non_existent").expect("no system errors"),
                Err(QueryError::SchemaDoesNotExist("non_existent".to_owned()))
            );
        }
    }

    #[cfg(test)]
    mod table {
        use super::*;

        #[test]
        fn create_table() {
            let mut handler = Handler::new(in_memory_storage());

            handler
                .execute("create schema schema_name;")
                .expect("no system errors")
                .expect("schema created");

            assert_eq!(
                handler
                    .execute("create table schema_name.table_name (column_name smallint);")
                    .expect("no system errors"),
                Ok(QueryEvent::TableCreated)
            );
        }

        #[test]
        fn create_table_in_non_existent_schema() {
            let mut handler = Handler::new(in_memory_storage());

            assert_eq!(
                handler
                    .execute("create table schema_name.table_name (column_name smallint);")
                    .expect("no system errors"),
                Err(QueryError::SchemaDoesNotExist("schema_name".to_owned()))
            );
        }

        #[test]
        fn drop_table() {
            let mut handler = Handler::new(in_memory_storage());

            handler
                .execute("create schema schema_name;")
                .expect("no system errors")
                .expect("schema created");
            handler
                .execute("create table schema_name.table_name (column_name smallint);")
                .expect("no system errors")
                .expect("table created");

            assert_eq!(
                handler
                    .execute("drop table schema_name.table_name;")
                    .expect("no system errors"),
                Ok(QueryEvent::TableDropped)
            );
            assert_eq!(
                handler
                    .execute("create table schema_name.table_name (column_name smallint);")
                    .expect("no system errors"),
                Ok(QueryEvent::TableCreated)
            );
        }

        #[test]
        fn drop_non_existent_table() {
            let mut handler = Handler::new(in_memory_storage());

            handler
                .execute("create schema schema_name;")
                .expect("no system errors")
                .expect("schema created");

            assert_eq!(
                handler
                    .execute("drop table schema_name.table_name;")
                    .expect("no system errors"),
                Err(QueryError::TableDoesNotExist("schema_name.table_name".to_owned()))
            );
        }

        #[test]
        fn drop_table_from_non_existent_schema() {
            let mut handler = Handler::new(in_memory_storage());

            assert_eq!(
                handler
                    .execute("drop table schema_name.table_name;")
                    .expect("no system errors"),
                Err(QueryError::SchemaDoesNotExist("schema_name".to_owned()))
            );
        }
    }

    #[test]
    fn insert_and_select_single_row() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        handler
            .execute("create table schema_name.table_name (column_test smallint);")
            .expect("no system errors")
            .expect("table created");

        assert_eq!(
            handler
                .execute("insert into schema_name.table_name values (123);")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsInserted(1))
        );
        assert_eq!(
            handler
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), SqlType::SmallInt)],
                vec![vec!["123".to_owned()]]
            )))
        );
    }

    #[test]
    fn insert_into_table_in_nonexistent_schema() {
        let mut handler = Handler::new(in_memory_storage());

        assert_eq!(
            handler
                .execute("insert into schema_name.table_name values (123);")
                .expect("no system errors"),
            Err(QueryError::SchemaDoesNotExist("schema_name".to_owned()))
        );
    }

    #[test]
    fn insert_into_nonexistent_table() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");

        assert_eq!(
            handler
                .execute("insert into schema_name.table_name values (123);")
                .expect("no system errors"),
            Err(QueryError::TableDoesNotExist("schema_name.table_name".to_owned()))
        );
    }

    #[test]
    fn insert_and_select_multiple_rows() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        handler
            .execute("create table schema_name.table_name (column_test smallint);")
            .expect("no system errors")
            .expect("table created");
        handler
            .execute("insert into schema_name.table_name values (123);")
            .expect("no system errors")
            .expect("row inserted");

        assert_eq!(
            handler
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), SqlType::SmallInt)],
                vec![vec!["123".to_owned()]]
            )))
        );

        handler
            .execute("insert into schema_name.table_name values (456);")
            .expect("no system errors")
            .expect("row inserted");

        assert_eq!(
            handler
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), SqlType::SmallInt)],
                vec![vec!["123".to_owned()], vec!["456".to_owned()]]
            )))
        );
    }

    #[test]
    #[ignore]
    // TODO return proper error when reading columns from "system" schema
    //      but simple select by predicate has to be implemented
    fn select_all_from_table_in_nonexistent_schema() {
        let mut handler = Handler::new(in_memory_storage());

        assert_eq!(
            handler
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Err(QueryError::SchemaDoesNotExist("schema_name".to_owned()))
        );
    }

    #[test]
    #[ignore]
    // TODO return proper error when reading columns from "system" schema
    //      but simple select by predicate has to be implemented
    fn select_named_columns_from_table_in_nonexistent_schema() {
        let mut handler = Handler::new(in_memory_storage());

        assert_eq!(
            handler
                .execute("select column_1 from schema_name.table_name;")
                .expect("no system errors"),
            Err(QueryError::SchemaDoesNotExist("schema_name".to_owned()))
        );
    }

    #[test]
    fn update_all_records() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        handler
            .execute("create table schema_name.table_name (column_test smallint);")
            .expect("no system errors")
            .expect("table created");
        handler
            .execute("insert into schema_name.table_name values (123);")
            .expect("no system errors")
            .expect("row inserted");
        handler
            .execute("insert into schema_name.table_name values (456);")
            .expect("no system errors")
            .expect("row inserted");

        assert_eq!(
            handler
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), SqlType::SmallInt)],
                vec![vec!["123".to_owned()], vec!["456".to_owned()]]
            )))
        );
        assert_eq!(
            handler
                .execute("update schema_name.table_name set column_test=789;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsUpdated(2))
        );
        assert_eq!(
            handler
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), SqlType::SmallInt)],
                vec![vec!["789".to_owned()], vec!["789".to_owned()]]
            )))
        );
    }

    #[ignore] // TODO probably such tests should complain about non existent table
    #[test]
    fn update_records_in_table_from_non_existent_schema() {
        let mut handler = Handler::new(in_memory_storage());

        assert_eq!(
            handler
                .execute("update schema_name.table_name set column_test=789;")
                .expect("no system errors"),
            Err(QueryError::SchemaDoesNotExist("schema_name".to_owned()))
        );
    }

    #[test]
    fn update_records_in_nonexistent_table() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");

        assert_eq!(
            handler
                .execute("update schema_name.table_name set column_test=789;")
                .expect("no system errors"),
            Err(QueryError::TableDoesNotExist("schema_name.table_name".to_owned()))
        );
    }

    #[test]
    fn delete_all_records() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        handler
            .execute("create table schema_name.table_name (column_test smallint);")
            .expect("no system errors")
            .expect("table created");
        handler
            .execute("insert into schema_name.table_name values (123);")
            .expect("no system errors")
            .expect("row inserted");
        handler
            .execute("insert into schema_name.table_name values (456);")
            .expect("no system errors")
            .expect("row inserted");
        assert_eq!(
            handler
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), SqlType::SmallInt)],
                vec![vec!["123".to_owned()], vec!["456".to_owned()]]
            )))
        );
        assert_eq!(
            handler
                .execute("delete from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsDeleted(2))
        );
        assert_eq!(
            handler
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![("column_test".to_owned(), SqlType::SmallInt)],
                vec![]
            )))
        );
    }

    #[test]
    fn delete_from_table_in_nonexistent_schema() {
        let mut handler = Handler::new(in_memory_storage());

        assert_eq!(
            handler
                .execute("delete from schema_name.table_name;")
                .expect("no system errors"),
            Err(QueryError::SchemaDoesNotExist("schema_name".to_owned()))
        );
    }

    #[test]
    fn delete_from_nonexistent_table() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");

        assert_eq!(
            handler
                .execute("delete from schema_name.table_name;")
                .expect("no system errors"),
            Err(QueryError::TableDoesNotExist("schema_name.table_name".to_owned()))
        );
    }

    #[test]
    fn select_all_from_table_with_multiple_columns() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        handler
            .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
            .expect("no system errors")
            .expect("table created");
        handler
            .execute("insert into schema_name.table_name values (123, 456, 789);")
            .expect("no system errors")
            .expect("row inserted");

        assert_eq!(
            handler
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("column_1".to_owned(), SqlType::SmallInt),
                    ("column_2".to_owned(), SqlType::SmallInt),
                    ("column_3".to_owned(), SqlType::SmallInt)
                ],
                vec![vec!["123".to_owned(), "456".to_owned(), "789".to_owned()]]
            )))
        );
    }

    #[test]
    fn insert_multiple_rows() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        handler
            .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
            .expect("no system errors")
            .expect("table created");

        assert_eq!(
            handler
                .execute("insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsInserted(3))
        );
        assert_eq!(
            handler
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("column_1".to_owned(), SqlType::SmallInt),
                    ("column_2".to_owned(), SqlType::SmallInt),
                    ("column_3".to_owned(), SqlType::SmallInt)
                ],
                vec![
                    vec!["1".to_owned(), "4".to_owned(), "7".to_owned()],
                    vec!["2".to_owned(), "5".to_owned(), "8".to_owned()],
                    vec!["3".to_owned(), "6".to_owned(), "9".to_owned()],
                ]
            )))
        );
    }

    #[test]
    fn select_not_all_columns() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        handler
            .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
            .expect("no system errors")
            .expect("table created");
        handler
            .execute("insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);")
            .expect("no system errors")
            .expect("rows inserted");

        assert_eq!(
            handler
                .execute("select column_3, column_2 from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("column_3".to_owned(), SqlType::SmallInt),
                    ("column_2".to_owned(), SqlType::SmallInt),
                ],
                vec![
                    vec!["7".to_owned(), "4".to_owned()],
                    vec!["8".to_owned(), "5".to_owned()],
                    vec!["9".to_owned(), "6".to_owned()],
                ]
            )))
        );
    }

    #[test]
    fn select_non_existing_columns_from_table() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");
        handler
            .execute("create table schema_name.table_name (column_in_table smallint);")
            .expect("no system errors")
            .expect("table created");

        assert_eq!(
            handler
                .execute("select column_not_in_table1, column_not_in_table2 from schema_name.table_name;")
                .expect("no system errors"),
            Err(QueryError::ColumnDoesNotExist(vec![
                "column_not_in_table1".to_owned(),
                "column_not_in_table2".to_owned()
            ]))
        );
    }

    #[test]
    fn create_table_with_different_types() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");

        assert_eq!(
            handler
                .execute("create table schema_name.table_name (column_si smallint, column_i integer, column_bi bigint, column_c char(10), column_vc varchar(10));")
                .expect("no system errors"),
            Ok(QueryEvent::TableCreated)
        )
    }

    #[test]
    fn insert_and_select_different_integer_types() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");

        handler
            .execute("create table schema_name.table_name (column_si smallint, column_i integer, column_bi bigint);")
            .expect("no system errors")
            .expect("table created");

        assert_eq!(
            handler
                .execute("insert into schema_name.table_name values(-32768, -2147483648, -9223372036854775808);")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsInserted(1))
        );

        assert_eq!(
            handler
                .execute("insert into schema_name.table_name values(32767, 2147483647, 9223372036854775807);")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsInserted(1))
        );

        assert_eq!(
            handler
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("column_si".to_owned(), SqlType::SmallInt),
                    ("column_i".to_owned(), SqlType::Integer),
                    ("column_bi".to_owned(), SqlType::BigInt),
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

    #[test]
    fn insert_and_select_different_character_types() {
        let mut handler = Handler::new(in_memory_storage());

        handler
            .execute("create schema schema_name;")
            .expect("no system errors")
            .expect("schema created");

        handler
            .execute("create table schema_name.table_name (column_c char(10), column_vc varchar(10));")
            .expect("no system errors")
            .expect("table created");

        assert_eq!(
            handler
                .execute("insert into schema_name.table_name values('12345abcde', '12345abcde');")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsInserted(1))
        );

        assert_eq!(
            handler
                .execute("insert into schema_name.table_name values('12345abcde', 'abcde');")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsInserted(1))
        );

        assert_eq!(
            handler
                .execute("select * from schema_name.table_name;")
                .expect("no system errors"),
            Ok(QueryEvent::RecordsSelected((
                vec![
                    ("column_c".to_owned(), SqlType::Char(10)),
                    ("column_vc".to_owned(), SqlType::VarChar(10))
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

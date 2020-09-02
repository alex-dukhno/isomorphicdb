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

extern crate bigdecimal;
extern crate log;

use crate::{
    catalog_manager::CatalogManager,
    ddl::{
        create_schema::CreateSchemaCommand, create_table::CreateTableCommand, drop_schema::DropSchemaCommand,
        drop_table::DropTableCommand,
    },
    dml::{delete::DeleteCommand, insert::InsertCommand, select::SelectCommand, update::UpdateCommand},
    query::{plan::Plan, process::QueryProcessor},
    session::{statement::PreparedStatement, Session},
};
use kernel::SystemResult;
use protocol::results::QueryError;
use protocol::{results::QueryEvent, sql_types::PostgreSqlType, Sender};
use serde::{Deserialize, Serialize};
use sql_types::SqlType;
use sqlparser::{
    ast::Statement,
    dialect::{Dialect, PostgreSqlDialect},
    parser::Parser,
};
use std::sync::Arc;

pub mod catalog_manager;
mod ddl;
mod dml;
mod query;
mod session;

pub type Projection = (Vec<ColumnDefinition>, Vec<Vec<String>>);

#[derive(Debug, Clone)]
pub struct TableDefinition {
    schema_name: String,
    table_name: String,
    column_data: Vec<ColumnDefinition>,
}

impl TableDefinition {
    pub fn new(schema_name: &str, table_name: &str, column_data: Vec<ColumnDefinition>) -> Self {
        Self {
            schema_name: schema_name.to_owned(),
            table_name: table_name.to_owned(),
            column_data,
        }
    }

    pub fn column_len(&self) -> usize {
        self.column_data.len()
    }

    pub fn column_type(&self, column_idx: usize) -> SqlType {
        if let Some(column) = self.column_data.get(column_idx) {
            column.sql_type
        } else {
            panic!("attempting to access type of invalid column index")
        }
    }

    pub fn column_type_by_name(&self, name: &str) -> Option<SqlType> {
        self.column_data
            .iter()
            .find(|column| column.name == name)
            .map(|column| column.sql_type)
    }

    pub fn column_by_name_with_index(&self, name: &str) -> Option<(usize, ColumnDefinition)> {
        self.column_data
            .iter()
            .enumerate()
            .find(|elem| elem.1.name == name)
            .map(|(idx, column)| (idx, column.clone()))
    }

    pub fn column_data(&self) -> &[ColumnDefinition] {
        self.column_data.as_slice()
    }

    pub fn scheme(&self) -> &str {
        self.schema_name.as_str()
    }

    pub fn table(&self) -> &str {
        self.table_name.as_str()
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    name: String,
    sql_type: SqlType,
}

impl ColumnDefinition {
    pub fn new(name: &str, sql_type: SqlType) -> Self {
        Self {
            name: name.to_string(),
            sql_type,
        }
    }

    pub fn sql_type(&self) -> SqlType {
        self.sql_type
    }

    pub fn has_name(&self, other_name: &str) -> bool {
        self.name == other_name
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }
}

pub struct QueryExecutor {
    processor: QueryProcessor,
    storage: Arc<CatalogManager>,
    sender: Arc<dyn Sender>,
    session: Session,
}

impl QueryExecutor {
    pub fn new(storage: Arc<CatalogManager>, sender: Arc<dyn Sender>) -> Self {
        Self {
            storage: storage.clone(),
            sender: sender.clone(),
            session: Session::new(),
            processor: QueryProcessor::new(storage, sender),
        }
    }

    #[allow(clippy::match_wild_err_arm)]
    pub fn execute(&mut self, raw_sql_query: &str) -> SystemResult<()> {
        let statement = match Parser::parse_sql(&PostgreSqlDialect {}, raw_sql_query) {
            Ok(mut statements) => {
                log::info!("stmts: {:#?}", statements);
                statements.pop().unwrap()
            }
            Err(e) => {
                log::error!("{:?} can't be parsed. Error: {:?}", raw_sql_query, e);
                self.sender
                    .send(Err(QueryError::syntax_error(format!(
                        "{:?} can't be parsed",
                        raw_sql_query
                    ))))
                    .expect("To Send Query Result to Client");
                return Ok(());
            }
        };

        log::debug!("STATEMENT = {:?}", statement);
        match self.processor.process(statement) {
            Ok(Plan::CreateSchema(creation_info)) => {
                CreateSchemaCommand::new(creation_info, self.storage.clone(), self.sender.clone()).execute()?
            }
            Ok(Plan::CreateTable(creation_info)) => {
                CreateTableCommand::new(creation_info, self.storage.clone(), self.sender.clone()).execute()?
            }
            Ok(Plan::DropSchemas(schemas)) => {
                for (schema, cascade) in schemas {
                    DropSchemaCommand::new(schema, cascade, self.storage.clone(), self.sender.clone()).execute()?;
                }
            }
            Ok(Plan::DropTables(tables)) => {
                for table in tables {
                    DropTableCommand::new(table, self.storage.clone(), self.sender.clone()).execute()?;
                }
            }
            Ok(Plan::Insert(table_insert)) => {
                InsertCommand::new(raw_sql_query, table_insert, self.storage.clone(), self.sender.clone()).execute()?
            }
            Ok(Plan::NotProcessed(statement)) => match *statement {
                Statement::StartTransaction { .. } => {
                    self.sender
                        .send(Ok(QueryEvent::TransactionStarted))
                        .expect("To Send Query Result to Client");
                }
                Statement::SetVariable { .. } => {
                    self.sender
                        .send(Ok(QueryEvent::VariableSet))
                        .expect("To Send Query Result to Client");
                }
                Statement::Drop { .. } => {
                    self.sender
                        .send(Err(QueryError::feature_not_supported(raw_sql_query.to_owned())))
                        .expect("To Send Query Result to Client");
                }
                Statement::Query(query) => {
                    SelectCommand::new(raw_sql_query, query, self.storage.clone(), self.sender.clone()).execute()?
                }
                Statement::Update {
                    table_name,
                    assignments,
                    ..
                } => {
                    UpdateCommand::new(table_name, assignments, self.storage.clone(), self.sender.clone()).execute()?
                }
                Statement::Delete { table_name, .. } => {
                    DeleteCommand::new(table_name, self.storage.clone(), self.sender.clone()).execute()?
                }
                _ => {
                    self.sender
                        .send(Err(QueryError::feature_not_supported(raw_sql_query.to_owned())))
                        .expect("To Send Query Result to Client");
                }
            },
            Err(()) => {}
        };

        self.sender
            .send(Ok(QueryEvent::QueryComplete))
            .expect("To Send Query Complete Event to Client");

        Ok(())
    }

    pub fn parse(
        &mut self,
        statement_name: &str,
        raw_sql_query: &str,
        param_types: &[PostgreSqlType],
    ) -> SystemResult<()> {
        let statement = match Parser::parse_sql(&PreparedStatementDialect {}, raw_sql_query) {
            Ok(mut statements) => {
                log::info!("stmts: {:#?}", statements);
                statements.pop().unwrap()
            }
            Err(e) => {
                log::error!("{:?} can't be parsed. Error: {:?}", raw_sql_query, e);
                self.sender
                    .send(Err(QueryError::syntax_error(format!(
                        "{:?} can't be parsed",
                        raw_sql_query
                    ))))
                    .expect("To Send Query Result to Client");
                return Ok(());
            }
        };

        let description = match &statement {
            Statement::Query(query) => {
                SelectCommand::new(raw_sql_query, query.clone(), self.storage.clone(), self.sender.clone())
                    .describe()?
            }
            _ => vec![],
        };

        let prepared_statement = PreparedStatement::new(statement, param_types.to_vec(), description);
        self.session
            .set_prepared_statement(statement_name.to_owned(), prepared_statement);

        self.sender
            .send(Ok(QueryEvent::ParseComplete))
            .expect("To Send ParseComplete Event");

        Ok(())
    }

    pub fn describe_prepared_statement(&mut self, name: &str) -> SystemResult<()> {
        match self.session.get_prepared_statement(name) {
            Some(stmt) => {
                self.sender
                    .send(Ok(QueryEvent::PreparedStatementDescribed(
                        stmt.param_types().to_vec(),
                        stmt.description().to_vec(),
                    )))
                    .expect("To Send ParametersDescribed Event");
            }
            None => {
                self.sender
                    .send(Err(QueryError::prepared_statement_does_not_exist(name.to_owned())))
                    .expect("To Send Error to Client");
            }
        };

        Ok(())
    }

    pub fn flush(&self) {
        match self.sender.flush() {
            Ok(_) => {}
            Err(e) => {
                log::error!("Flush error: {:?}", e);
            }
        };
    }
}

#[derive(Debug)]
struct PreparedStatementDialect {}

impl Dialect for PreparedStatementDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || ch == '$'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '$' || ch == '_'
    }
}

#[cfg(test)]
mod tests;

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
};
use kernel::SystemResult;
use protocol::{
    results::{QueryErrorBuilder, QueryEvent},
    Sender,
};
use serde::{Deserialize, Serialize};
use sql_types::SqlType;
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};
use std::sync::{Arc, Mutex};

pub mod catalog_manager;
mod ddl;
mod dml;
mod query;

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
    storage: Arc<Mutex<CatalogManager>>,
    processor: QueryProcessor,
    session: Arc<dyn Sender>,
}

impl QueryExecutor {
    pub fn new(storage: Arc<Mutex<CatalogManager>>, session: Arc<dyn Sender>) -> Self {
        Self {
            storage: storage.clone(),
            processor: QueryProcessor::new(storage, session.clone()),
            session,
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
                let query_error = QueryErrorBuilder::new()
                    .syntax_error(format!("{:?} can't be parsed", raw_sql_query))
                    .build();
                self.session
                    .send(Err(query_error))
                    .expect("To Send Query Result to Client");
                return Ok(());
            }
        };

        log::debug!("STATEMENT = {:?}", statement);
        match self.processor.process(statement) {
            Ok(Plan::CreateSchema(creation_info)) => {
                CreateSchemaCommand::new(creation_info, self.storage.clone(), self.session.clone()).execute()
            }
            Ok(Plan::CreateTable(creation_info)) => {
                CreateTableCommand::new(creation_info, self.storage.clone(), self.session.clone()).execute()
            }
            Ok(Plan::DropSchemas(schemas)) => {
                for schema in schemas {
                    DropSchemaCommand::new(schema, self.storage.clone(), self.session.clone()).execute()?;
                }
                Ok(())
            }
            Ok(Plan::DropTables(tables)) => {
                for table in tables {
                    DropTableCommand::new(table, self.storage.clone(), self.session.clone()).execute()?;
                }
                Ok(())
            }
            Ok(Plan::Insert(table_insert)) => {
                InsertCommand::new(raw_sql_query, table_insert, self.storage.clone(), self.session.clone()).execute()
            }
            Ok(Plan::NotProcessed(statement)) => match *statement {
                Statement::StartTransaction { .. } => {
                    self.session
                        .send(Ok(QueryEvent::TransactionStarted))
                        .expect("To Send Query Result to Client");
                    Ok(())
                }
                Statement::SetVariable { .. } => {
                    self.session
                        .send(Ok(QueryEvent::VariableSet))
                        .expect("To Send Query Result to Client");
                    Ok(())
                }
                Statement::Drop { .. } => {
                    self.session
                        .send(Err(QueryErrorBuilder::new()
                            .feature_not_supported(raw_sql_query.to_owned())
                            .build()))
                        .expect("To Send Query Result to Client");
                    Ok(())
                }
                Statement::Query(query) => {
                    SelectCommand::new(raw_sql_query, query, self.storage.clone(), self.session.clone()).execute()
                }
                Statement::Update {
                    table_name,
                    assignments,
                    ..
                } => UpdateCommand::new(table_name, assignments, self.storage.clone(), self.session.clone()).execute(),
                Statement::Delete { table_name, .. } => {
                    DeleteCommand::new(table_name, self.storage.clone(), self.session.clone()).execute()
                }
                _ => {
                    self.session
                        .send(Err(QueryErrorBuilder::new()
                            .feature_not_supported(raw_sql_query.to_owned())
                            .build()))
                        .expect("To Send Query Result to Client");
                    Ok(())
                }
            },
            Err(()) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;

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
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage};

mod ddl;
mod definitions;
mod dml;
mod query;
mod storage_manager;

pub struct QueryExecutor<P: BackendStorage> {
    storage: Arc<Mutex<FrontendStorage<P>>>,
    processor: QueryProcessor<P>,
    session: Arc<dyn Sender>,
}

impl<P: BackendStorage> QueryExecutor<P> {
    pub fn new(storage: Arc<Mutex<FrontendStorage<P>>>, session: Arc<dyn Sender>) -> Self {
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

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
};
use kernel::SystemResult;
use protocol::results::{QueryError, QueryEvent, QueryResult};

use sqlparser::{
    ast::{ObjectType, Statement},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
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
            Statement::StartTransaction { .. } => Ok(Ok(QueryEvent::TransactionStarted)),
            Statement::SetVariable { .. } => Ok(Ok(QueryEvent::VariableSet)),
            Statement::CreateTable { name, columns, .. } => {
                CreateTableCommand::new(name, columns, self.storage.clone()).execute()
            }
            Statement::CreateSchema { schema_name, .. } => {
                CreateSchemaCommand::new(schema_name, self.storage.clone()).execute()
            }
            Statement::Drop { object_type, names, .. } => match object_type {
                ObjectType::Table => DropTableCommand::new(names[0].clone(), self.storage.clone()).execute(),
                ObjectType::Schema => DropSchemaCommand::new(names[0].clone(), self.storage.clone()).execute(),
                _ => Ok(Err(QueryError::not_supported_operation(raw_sql_query.to_owned()))),
            },
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => InsertCommand::new(raw_sql_query, table_name, columns, source, self.storage.clone()).execute(),
            Statement::Query(query) => SelectCommand::new(raw_sql_query, query, self.storage.clone()).execute(),
            Statement::Update {
                table_name,
                assignments,
                ..
            } => UpdateCommand::new(raw_sql_query, table_name, assignments, self.storage.clone()).execute(),
            Statement::Delete { table_name, .. } => {
                DeleteCommand::new(raw_sql_query, table_name, self.storage.clone()).execute()
            }
            _ => Ok(Err(QueryError::not_supported_operation(raw_sql_query.to_owned()))),
        }
    }
}

#[cfg(test)]
mod tests;

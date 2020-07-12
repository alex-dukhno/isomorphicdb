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
use protocol::results::{QueryErrorBuilder, QueryEvent, QueryResult};

use crate::query::{
    plan::{Plan, PlanError},
    transform::QueryProcessor,
    TransformError,
};
use sqlparser::{
    ast::{ObjectType, Statement},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage, TableDescription, ColumnDefinition};

mod ddl;
mod dml;
mod query;

pub struct QueryExecutor<P: BackendStorage> {
    storage: Arc<Mutex<FrontendStorage<P>>>,
    processor: QueryProcessor<P>,
}

impl<P: BackendStorage> QueryExecutor<P> {
    pub fn new(storage: Arc<Mutex<FrontendStorage<P>>>) -> Self {
        Self {
            storage: storage.clone(),
            processor: QueryProcessor::new(storage),
        }
    }

    #[allow(clippy::match_wild_err_arm)]
    pub fn execute(&mut self, raw_sql_query: &str) -> SystemResult<QueryResult> {
        let statement = match Parser::parse_sql(&PostgreSqlDialect {}, raw_sql_query) {
            Ok(mut statements) => statements.pop().unwrap(),
            Err(e) => {
                log::error!("{:?} can't be parsed. Error: {:?}", raw_sql_query, e);
                return Ok(Err(QueryErrorBuilder::new()
                    .syntax_error(format!("{:?} can't be parsed", raw_sql_query))
                    .build()));
            }
        };
        log::debug!("STATEMENT = {:?}", statement);
        match self.processor.process(statement) {
            Ok(Plan::CreateSchema(creation_info)) => {
                CreateSchemaCommand::new(creation_info, self.storage.clone()).execute()
            }
            Ok(Plan::DropSchemas(schemas)) => {
                for schema in schemas {
                    DropSchemaCommand::new(schema, self.storage.clone())
                        .execute()?
                        .expect("drop schema");
                }
                Ok(Ok(QueryEvent::SchemaDropped))
            }
            Err(TransformError::NotProcessed(statement)) => match statement {
                Statement::StartTransaction { .. } => Ok(Ok(QueryEvent::TransactionStarted)),
                Statement::SetVariable { .. } => Ok(Ok(QueryEvent::VariableSet)),
                Statement::CreateTable { name, columns, .. } => {
                    CreateTableCommand::new(name, columns, self.storage.clone()).execute()
                }
                Statement::Drop { object_type, names, .. } => match object_type {
                    ObjectType::Table => DropTableCommand::new(names[0].clone(), self.storage.clone()).execute(),
                    _ => Ok(Err(QueryErrorBuilder::new()
                        .feature_not_supported(raw_sql_query.to_owned())
                        .build())),
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
                _ => Ok(Err(QueryErrorBuilder::new()
                    .feature_not_supported(raw_sql_query.to_owned())
                    .build())),
            },
            Err(TransformError::PlanError(PlanError::SchemaAlreadyExists(schema_name))) => {
                Ok(Err(QueryErrorBuilder::new().schema_already_exists(schema_name).build()))
            }
            Err(TransformError::PlanError(PlanError::InvalidSchema(schema_name))) => {
                Ok(Err(QueryErrorBuilder::new().schema_does_not_exist(schema_name).build()))
            }
            _ => unimplemented!(), // other TransformError
        }
    }
}

#[cfg(test)]
mod tests;

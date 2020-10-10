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

use crate::{
    ddl::{
        create_schema::CreateSchemaCommand, create_table::CreateTableCommand, drop_schema::DropSchemaCommand,
        drop_table::DropTableCommand,
    },
    dml::{delete::DeleteCommand, insert::InsertCommand, select::SelectCommand, update::UpdateCommand},
};
use data_manager::DataManager;
use plan::Plan;
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};
use sqlparser::ast::Statement;
use std::sync::Arc;

mod ddl;
mod dml;

pub struct QueryExecutor {
    data_manager: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl QueryExecutor {
    pub fn new(data_manager: Arc<DataManager>, sender: Arc<dyn Sender>) -> Self {
        Self { data_manager, sender }
    }

    pub fn execute(&self, plan: Plan) {
        match plan {
            Plan::NothingToExecute => self
                .sender
                .send(Ok(QueryEvent::QueryComplete))
                .expect("To Send Query Result to Client"),
            Plan::CreateSchema(creation_info) => {
                CreateSchemaCommand::new(creation_info, self.data_manager.clone(), self.sender.clone()).execute()
            }
            Plan::CreateTable(creation_info) => {
                CreateTableCommand::new(creation_info, self.data_manager.clone(), self.sender.clone()).execute()
            }
            Plan::DropSchemas(schemas) => {
                for (schema, cascade) in schemas {
                    DropSchemaCommand::new(schema, cascade, self.data_manager.clone(), self.sender.clone()).execute();
                }
            }
            Plan::DropTables(tables) => {
                for table in tables {
                    DropTableCommand::new(table, self.data_manager.clone(), self.sender.clone()).execute();
                }
            }
            Plan::Insert(table_insert) => {
                InsertCommand::new(table_insert, self.data_manager.clone(), self.sender.clone()).execute()
            }
            Plan::Update(table_update) => {
                UpdateCommand::new(table_update, self.data_manager.clone(), self.sender.clone()).execute()
            }
            Plan::Delete(table_delete) => {
                DeleteCommand::new(table_delete, self.data_manager.clone(), self.sender.clone()).execute()
            }
            Plan::Select(select_input) => {
                SelectCommand::new(select_input, self.data_manager.clone(), self.sender.clone()).execute()
            }
            Plan::NotProcessed(statement) => match *statement {
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
                        .send(Err(QueryError::feature_not_supported(statement)))
                        .expect("To Send Query Result to Client");
                }
                _ => {
                    self.sender
                        .send(Err(QueryError::feature_not_supported(statement)))
                        .expect("To Send Query Result to Client");
                }
            },
        }
    }
}

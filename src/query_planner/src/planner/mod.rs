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

///! Module for transforming the input Query AST into representation the engine can process.
mod create_schema;
mod create_table;
mod delete;
mod drop_schema;
mod drop_tables;
mod insert;
mod select;
mod update;

use crate::{
    plan::Plan,
    planner::{
        create_schema::CreateSchemaPlanner, create_table::CreateTablePlanner, delete::DeletePlanner,
        drop_schema::DropSchemaPlanner, drop_tables::DropTablesPlanner, insert::InsertPlanner, select::SelectPlanner,
        update::UpdatePlanner,
    },
};
use data_manager::DataManager;
use protocol::{results::QueryError, Sender};
use sqlparser::ast::{ObjectType, Statement};
use std::sync::Arc;

type Result<T> = std::result::Result<T, ()>;

trait Planner {
    fn plan(self, data_manager: Arc<DataManager>, sender: Arc<dyn Sender>) -> Result<Plan>;
}

pub struct QueryPlanner {
    data_manager: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl QueryPlanner {
    pub fn new(data_manager: Arc<DataManager>, sender: Arc<dyn Sender>) -> Self {
        Self { data_manager, sender }
    }

    pub fn plan(&self, stmt: Statement) -> Result<Plan> {
        match &stmt {
            Statement::CreateTable { name, columns, .. } => {
                CreateTablePlanner::new(name, columns).plan(self.data_manager.clone(), self.sender.clone())
            }
            Statement::CreateSchema { schema_name, .. } => {
                CreateSchemaPlanner::new(schema_name).plan(self.data_manager.clone(), self.sender.clone())
            }
            Statement::Drop {
                object_type,
                names,
                cascade,
                ..
            } => match object_type {
                ObjectType::Table => DropTablesPlanner::new(names).plan(self.data_manager.clone(), self.sender.clone()),
                ObjectType::Schema => {
                    DropSchemaPlanner::new(names, *cascade).plan(self.data_manager.clone(), self.sender.clone())
                }
                _ => {
                    self.sender
                        .send(Err(QueryError::syntax_error(stmt)))
                        .expect("To Send Result to Client");
                    Err(())
                }
            },
            Statement::Insert {
                table_name,
                columns,
                source,
            } => InsertPlanner::new(table_name, columns, source).plan(self.data_manager.clone(), self.sender.clone()),
            Statement::Update {
                table_name,
                assignments,
                ..
            } => UpdatePlanner::new(table_name, assignments).plan(self.data_manager.clone(), self.sender.clone()),
            Statement::Delete { table_name, .. } => {
                DeletePlanner::new(table_name).plan(self.data_manager.clone(), self.sender.clone())
            }
            Statement::Query(query) => {
                SelectPlanner::new(query.clone()).plan(self.data_manager.clone(), self.sender.clone())
            }
            _ => Ok(Plan::NotProcessed(Box::new(stmt))),
        }
    }
}

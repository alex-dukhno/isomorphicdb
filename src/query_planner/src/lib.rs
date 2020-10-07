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
    create_schema::CreateSchemaPlanner, create_table::CreateTablePlanner, delete::DeletePlanner,
    drop_schema::DropSchemaPlanner, drop_tables::DropTablesPlanner, insert::InsertPlanner, select::SelectPlanner,
    update::UpdatePlanner,
};
use metadata::DataDefinition;
use plan::Plan;
use protocol::{results::QueryError, Sender};
use sqlparser::ast::{ObjectType, Statement};
use std::sync::Arc;

type Result<T> = std::result::Result<T, ()>;

trait Planner {
    fn plan(self, data_manager: Arc<DataDefinition>, sender: Arc<dyn Sender>) -> Result<Plan>;
}

pub struct QueryPlanner {
    metadata: Arc<DataDefinition>,
    sender: Arc<dyn Sender>,
}

impl QueryPlanner {
    pub fn new(metadata: Arc<DataDefinition>, sender: Arc<dyn Sender>) -> Self {
        Self { metadata, sender }
    }

    pub fn plan(&self, statement: &Statement) -> Result<Plan> {
        match statement {
            Statement::CreateTable { name, columns, .. } => {
                CreateTablePlanner::new(name, columns).plan(self.metadata.clone(), self.sender.clone())
            }
            Statement::CreateSchema { schema_name, .. } => {
                CreateSchemaPlanner::new(schema_name).plan(self.metadata.clone(), self.sender.clone())
            }
            Statement::Drop {
                object_type,
                names,
                cascade,
                if_exists,
            } => match object_type {
                ObjectType::Table => {
                    DropTablesPlanner::new(names, *if_exists).plan(self.metadata.clone(), self.sender.clone())
                }
                ObjectType::Schema => {
                    DropSchemaPlanner::new(names, *cascade, *if_exists).plan(self.metadata.clone(), self.sender.clone())
                }
                _ => {
                    self.sender
                        .send(Err(QueryError::syntax_error(statement)))
                        .expect("To Send Result to Client");
                    Err(())
                }
            },
            Statement::Insert {
                table_name,
                columns,
                source,
            } => InsertPlanner::new(table_name, columns, source).plan(self.metadata.clone(), self.sender.clone()),
            Statement::Update {
                table_name,
                assignments,
                ..
            } => UpdatePlanner::new(table_name, assignments).plan(self.metadata.clone(), self.sender.clone()),
            Statement::Delete { table_name, .. } => {
                DeletePlanner::new(table_name).plan(self.metadata.clone(), self.sender.clone())
            }
            Statement::Query(query) => {
                SelectPlanner::new(query.clone()).plan(self.metadata.clone(), self.sender.clone())
            }
            _ => Ok(Plan::NotProcessed(Box::new(statement.clone()))),
        }
    }
}

#[cfg(test)]
mod tests;

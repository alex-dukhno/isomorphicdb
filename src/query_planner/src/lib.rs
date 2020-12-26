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
use data_definition::DataDefReader;
use plan::Plan;
use sqlparser::ast::{ObjectType, Statement};
use std::sync::Arc;

type Result<T> = std::result::Result<T, PlanError>;

#[derive(Debug, PartialEq)]
pub enum PlanError {
    SchemaAlreadyExists(String),
    SchemaDoesNotExist(String),
    TableAlreadyExists(String),
    TableDoesNotExist(String),
    DuplicateColumn(String),
    ColumnDoesNotExist(String),
    SyntaxError(String),
    FeatureNotSupported(String),
}

impl PlanError {
    fn schema_already_exists<S: ToString>(schema: &S) -> PlanError {
        PlanError::SchemaAlreadyExists(schema.to_string())
    }

    fn schema_does_not_exist<S: ToString>(schema: &S) -> PlanError {
        PlanError::SchemaDoesNotExist(schema.to_string())
    }

    fn table_already_exists<T: ToString>(table: &T) -> PlanError {
        PlanError::TableAlreadyExists(table.to_string())
    }

    fn table_does_not_exist<T: ToString>(table: &T) -> PlanError {
        PlanError::TableDoesNotExist(table.to_string())
    }

    fn duplicate_column<C: ToString>(column: &C) -> PlanError {
        PlanError::DuplicateColumn(column.to_string())
    }

    fn column_does_not_exist<C: ToString>(column: &C) -> PlanError {
        PlanError::ColumnDoesNotExist(column.to_string())
    }

    fn feature_not_supported<FD: ToString>(feature_desc: FD) -> PlanError {
        PlanError::FeatureNotSupported(feature_desc.to_string())
    }

    fn syntax_error<S: ToString>(expr: &S) -> PlanError {
        PlanError::SyntaxError(expr.to_string())
    }
}

trait Planner {
    fn plan(self, data_manager: Arc<dyn DataDefReader>) -> Result<Plan>;
}

pub struct QueryPlanner {
    metadata: Arc<dyn DataDefReader>,
}

impl QueryPlanner {
    pub fn new(metadata: Arc<dyn DataDefReader>) -> Self {
        Self { metadata }
    }

    pub fn plan(&self, statement: &Statement) -> Result<Plan> {
        match statement {
            Statement::CreateTable { name, columns, .. } => {
                CreateTablePlanner::new(name, columns).plan(self.metadata.clone())
            }
            Statement::CreateSchema { schema_name, .. } => {
                CreateSchemaPlanner::new(schema_name).plan(self.metadata.clone())
            }
            Statement::Drop {
                object_type,
                names,
                cascade,
                if_exists,
            } => match object_type {
                ObjectType::Table => DropTablesPlanner::new(names, *if_exists).plan(self.metadata.clone()),
                ObjectType::Schema => DropSchemaPlanner::new(names, *cascade, *if_exists).plan(self.metadata.clone()),
                _ => Err(PlanError::feature_not_supported(&statement)),
            },
            Statement::Insert {
                table_name,
                columns,
                source,
            } => InsertPlanner::new(table_name, columns, source).plan(self.metadata.clone()),
            Statement::Update {
                table_name,
                assignments,
                ..
            } => UpdatePlanner::new(table_name, assignments).plan(self.metadata.clone()),
            Statement::Delete { table_name, .. } => DeletePlanner::new(table_name).plan(self.metadata.clone()),
            Statement::Query(query) => SelectPlanner::new(query.clone()).plan(self.metadata.clone()),
            _ => Ok(Plan::NotProcessed(Box::new(statement.clone()))),
        }
    }
}

#[cfg(test)]
mod tests;

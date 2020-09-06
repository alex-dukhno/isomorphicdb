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
use crate::plan::{Plan, SchemaCreationInfo, SelectInput, TableCreationInfo, TableDeletes, TableInserts, TableUpdates};
use crate::{SchemaId, TableId};
use data_manager::{ColumnDefinition, DataManager};
use itertools::Itertools;
use kernel::{SystemError, SystemResult};
mod create_schema;
mod create_table;
mod drop_schema;
mod drop_tables;

use sqlparser::ast::{
    ColumnDef, Expr, Ident, ObjectName, ObjectType, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins,
};

use crate::planner::create_schema::CreateSchemaPlanner;
use crate::planner::drop_schema::DropSchemaPlanner;
use crate::planner::drop_tables::DropTablesPlanner;
use crate::{
    planner::create_table::CreateTablePlanner, FullTableName, SchemaName, SchemaNamingError, TableNamingError,
};
use protocol::{results::QueryError, Sender};
use sql_model::sql_types::SqlType;
use std::convert::TryFrom;
use std::{ops::Deref, sync::Arc};

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

    fn resolve_table_name(&self, name: &ObjectName) -> Result<(TableId, String, String)> {
        let schema_name = name.0.first().unwrap().value.clone();
        let table_name = name.0.iter().skip(1).join(".");
        match self.data_manager.table_exists(&schema_name, &table_name) {
            None => {
                self.sender
                    .send(Err(QueryError::schema_does_not_exist(schema_name.to_owned())))
                    .expect("To Send Query Result to Client");
                Err(())
            }
            Some((_, None)) => {
                self.sender
                    .send(Err(QueryError::table_does_not_exist(format!(
                        "{}.{}",
                        schema_name, table_name
                    ))))
                    .expect("To Send Query Result to Client");
                Err(())
            }
            Some((schema_id, Some(table_id))) => Ok((TableId(schema_id, table_id), schema_name, table_name)),
        }
    }

    fn resolve_schema_name(&self, name: &ObjectName) -> Result<(SchemaId, String)> {
        let schema_name = name.0.iter().join(".");

        match self.data_manager.schema_exists(&schema_name) {
            None => {
                self.sender
                    .send(Err(QueryError::schema_does_not_exist(schema_name)))
                    .expect("To Send Query Result to Client");
                Err(())
            }
            Some(schema_id) => Ok((SchemaId(schema_id), schema_name)),
        }
    }

    pub fn plan(&self, stmt: Statement) -> Result<Plan> {
        match stmt {
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
                ObjectType::Table => {
                    DropTablesPlanner::new(&names).plan(self.data_manager.clone(), self.sender.clone())
                }
                ObjectType::Schema => {
                    DropSchemaPlanner::new(&names, cascade).plan(self.data_manager.clone(), self.sender.clone())
                }
                _ => unimplemented!(),
            },
            Statement::Insert {
                table_name,
                columns,
                source,
            } => {
                let (table_id, _, _) = self.resolve_table_name(&table_name)?;
                Ok(Plan::Insert(TableInserts {
                    full_table_name: table_id,
                    column_indices: columns,
                    input: source,
                }))
            }
            Statement::Update {
                table_name,
                assignments,
                ..
            } => {
                let (table_id, _, _) = self.resolve_table_name(&table_name)?;
                Ok(Plan::Update(TableUpdates {
                    full_table_name: table_id,
                    assignments,
                }))
            }
            Statement::Delete { table_name, .. } => {
                let (table_id, _, _) = self.resolve_table_name(&table_name)?;
                Ok(Plan::Delete(TableDeletes {
                    full_table_name: table_id,
                }))
            }
            // TODO: ad-hock solution, duh
            Statement::Query(query) => {
                let result = self.parse_select_input(query);
                Ok(Plan::Select(result.map_err(|_| ())?))
            }
            _ => Ok(Plan::NotProcessed(Box::new(stmt.clone()))),
        }
    }

    fn parse_select_input(&self, query: Box<Query>) -> SystemResult<SelectInput> {
        let Query { body, .. } = &*query;
        if let SetExpr::Select(select) = body {
            let Select { projection, from, .. } = select.deref();
            let TableWithJoins { relation, .. } = &from[0];
            let (schema_name, table_name) = match relation {
                TableFactor::Table { name, .. } => {
                    let table_name = name.0[1].to_string();
                    let schema_name = name.0[0].to_string();
                    (schema_name, table_name)
                }
                _ => {
                    self.sender
                        .send(Err(QueryError::feature_not_supported(query)))
                        .expect("To Send Query Result to Client");
                    return Err(SystemError::runtime_check_failure("Feature Not Supported".to_owned()));
                }
            };

            match self.data_manager.table_exists(&schema_name, &table_name) {
                None => {
                    self.sender
                        .send(Err(QueryError::schema_does_not_exist(schema_name)))
                        .expect("To Send Result to Client");
                    Err(SystemError::runtime_check_failure("Schema Does Not Exist".to_owned()))
                }
                Some((_, None)) => {
                    self.sender
                        .send(Err(QueryError::table_does_not_exist(
                            schema_name + "." + table_name.as_str(),
                        )))
                        .expect("To Send Result to Client");
                    Err(SystemError::runtime_check_failure("Table Does Not Exist".to_owned()))
                }
                Some((schema_id, Some(table_id))) => {
                    let selected_columns = {
                        let projection = projection.clone();
                        let mut columns: Vec<String> = vec![];
                        for item in projection {
                            match item {
                                SelectItem::Wildcard => {
                                    let all_columns = self.data_manager.table_columns(schema_id, table_id)?;
                                    columns.extend(
                                        all_columns
                                            .into_iter()
                                            .map(|column_definition| column_definition.name())
                                            .collect::<Vec<String>>(),
                                    )
                                }
                                SelectItem::UnnamedExpr(Expr::Identifier(Ident { value, .. })) => {
                                    columns.push(value.clone())
                                }
                                _ => {
                                    self.sender
                                        .send(Err(QueryError::feature_not_supported(query)))
                                        .expect("To Send Query Result to Client");
                                    return Err(SystemError::runtime_check_failure("Feature Not Supported".to_owned()));
                                }
                            }
                        }
                        columns
                    };

                    Ok(SelectInput {
                        table_id: TableId(schema_id, table_id),
                        selected_columns,
                    })
                }
            }
        } else {
            self.sender
                .send(Err(QueryError::feature_not_supported(query)))
                .expect("To Send Query Result to Client");
            Err(SystemError::runtime_check_failure("Feature Not Supported".to_owned()))
        }
    }

    fn resolve_column_definitions(&self, columns: &[ColumnDef]) -> Result<Vec<ColumnDefinition>> {
        let mut column_defs = Vec::new();
        for column in columns {
            let sql_type = SqlType::try_from(&column.data_type).map_err(|_| ())?;
            // maybe a different type should be used to represent this instead of the storage's representation.
            let column_definition = ColumnDefinition::new(column.name.value.as_str(), sql_type);
            column_defs.push(column_definition);
        }
        Ok(column_defs)
    }

    fn handle_create_table(&self, name: ObjectName, columns: &[ColumnDef]) -> Result<Plan> {
        let schema_name = name.0.first().unwrap().value.clone();
        let table_name = name.0.iter().skip(1).join(".");

        match self.data_manager.table_exists(&schema_name, &table_name) {
            None => {
                self.sender
                    .send(Err(QueryError::schema_does_not_exist(schema_name)))
                    .expect("To Send Query Result to Client");
                Err(())
            }
            Some((_, Some(_))) => {
                self.sender
                    .send(Err(QueryError::table_already_exists(format!(
                        "{}.{}",
                        schema_name, table_name
                    ))))
                    .expect("To Send Query Result to Client");
                Err(())
            }
            Some((schema_id, None)) => {
                let columns = self.resolve_column_definitions(columns)?;
                let table_info = TableCreationInfo {
                    schema_id,
                    table_name,
                    columns,
                };
                Ok(Plan::CreateTable(table_info))
            }
        }
    }

    fn handle_drop(&self, object_type: &ObjectType, names: &[ObjectName], cascade: bool) -> Result<Plan> {
        match object_type {
            ObjectType::Table => {
                let mut table_names = Vec::with_capacity(names.len());
                for name in names {
                    let (table_id, _, _) = self.resolve_table_name(name)?;
                    table_names.push(table_id);
                }
                Ok(Plan::DropTables(table_names))
            }
            ObjectType::Schema => {
                let mut schema_names = Vec::with_capacity(names.len());
                for name in names {
                    let (schema_id, _) = self.resolve_schema_name(name)?;
                    schema_names.push((schema_id, cascade));
                }
                Ok(Plan::DropSchemas(schema_names))
            }
            _ => unimplemented!(),
        }
    }
}

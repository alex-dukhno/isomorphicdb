// Copyright 2020 - present Alex Dukhno
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

use crate::{dynamic_tree_builder::DynamicTreeBuilder, static_tree_builder::StaticTreeBuilder};
use catalog::CatalogDefinition;
use data_manipulation_untyped_queries::{
    UntypedDeleteQuery, UntypedInsertQuery, UntypedQuery, UntypedSelectQuery, UntypedUpdateQuery,
};
use data_manipulation_untyped_tree::{DynamicUntypedItem, DynamicUntypedTree};
use definition::FullTableName;
use query_ast::{
    Assignment, DeleteStatement, InsertSource, InsertStatement, Query, SelectItem, SelectStatement, UpdateStatement,
    Values,
};
use query_response::QueryError;
use std::{collections::HashMap, sync::Arc};

mod dynamic_tree_builder;
mod static_tree_builder;

pub struct QueryAnalyzer<CD: CatalogDefinition> {
    database: Arc<CD>,
}

impl<CD: CatalogDefinition> QueryAnalyzer<CD> {
    pub fn new(database: Arc<CD>) -> QueryAnalyzer<CD> {
        QueryAnalyzer { database }
    }

    pub fn analyze(&self, statement: Query) -> Result<UntypedQuery, AnalysisError> {
        match statement {
            Query::Insert(InsertStatement {
                schema_name,
                table_name,
                source,
                columns,
            }) => {
                let full_table_name = FullTableName::from((&schema_name, &table_name));
                match self.database.table_definition(full_table_name.clone()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some(None) => Err(AnalysisError::table_does_not_exist(full_table_name)),
                    Some(Some(table_info)) => {
                        let table_columns = table_info.column_names();
                        let column_names = if columns.is_empty() {
                            table_info.column_names().into_iter()
                        } else {
                            let mut column_names = vec![];
                            for column in columns {
                                if !table_info.has_column(&column) {
                                    return Err(AnalysisError::column_not_found(&column));
                                }
                                column_names.push(column);
                            }
                            column_names.into_iter()
                        };
                        let column_map = column_names
                            .enumerate()
                            .map(|(index, name)| (name, index))
                            .collect::<HashMap<String, usize>>();

                        let values = match source {
                            InsertSource::Values(Values(insert_rows)) => {
                                let mut values = vec![];
                                log::debug!("column map {:?}", column_map);
                                for insert_row in insert_rows {
                                    log::debug!("building static tree for {:?} row", insert_row);
                                    let mut row = vec![];
                                    for table_column in &table_columns {
                                        let value = match column_map.get(table_column) {
                                            None => None,
                                            Some(index) => {
                                                Some(StaticTreeBuilder::build_from(insert_row[*index].clone())?)
                                            }
                                        };
                                        row.push(value);
                                    }
                                    values.push(row)
                                }
                                values
                            }
                        };
                        Ok(UntypedQuery::Insert(UntypedInsertQuery {
                            full_table_name,
                            values,
                        }))
                    }
                }
            }
            Query::Update(UpdateStatement {
                schema_name,
                table_name,
                assignments: stmt_assignments,
                where_clause: _where_clause,
            }) => {
                let full_table_name = FullTableName::from((&schema_name, &table_name));
                match self.database.table_definition(full_table_name.clone()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some(None) => Err(AnalysisError::table_does_not_exist(full_table_name)),
                    Some(Some(table_info)) => {
                        let table_columns = table_info.columns();
                        let mut temp_column_names = vec![];
                        for table_column in table_columns {
                            let mut found = false;
                            for stmt_assignment in stmt_assignments.iter() {
                                if table_column.name() == stmt_assignment.column.as_str() {
                                    temp_column_names.push(Some(stmt_assignment.value.clone()));
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
                                temp_column_names.push(None);
                            }
                        }
                        for assignment in stmt_assignments {
                            let Assignment { column, .. } = assignment;
                            if !table_info.has_column(&column) {
                                return Err(AnalysisError::column_not_found(&column));
                            }
                        }
                        let mut assignments = vec![];
                        for temp_column_name in temp_column_names {
                            match temp_column_name {
                                None => assignments.push(None),
                                Some(value) => {
                                    assignments.push(Some(DynamicTreeBuilder::build_from(value, &table_columns)?));
                                }
                            }
                        }
                        Ok(UntypedQuery::Update(UntypedUpdateQuery {
                            full_table_name,
                            assignments,
                        }))
                    }
                }
            }
            Query::Select(SelectStatement {
                select_items,
                schema_name,
                table_name,
                where_clause: _where_clause,
            }) => {
                let full_table_name = FullTableName::from((&schema_name, &table_name));
                match self.database.table_definition(full_table_name.clone()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some(None) => Err(AnalysisError::table_does_not_exist(full_table_name)),
                    Some(Some(table_info)) => {
                        let table_columns = table_info.columns();
                        let mut projection_items = vec![];
                        for item in select_items {
                            match item {
                                SelectItem::Wildcard => {
                                    for (index, table_column) in table_columns.iter().enumerate() {
                                        projection_items.push(DynamicUntypedTree::Item(DynamicUntypedItem::Column {
                                            name: table_column.name().to_lowercase(),
                                            index,
                                            sql_type: table_column.sql_type(),
                                        }));
                                    }
                                }
                                SelectItem::UnnamedExpr(expr) => {
                                    projection_items.push(DynamicTreeBuilder::build_from(expr, &table_columns)?)
                                }
                            }
                        }
                        Ok(UntypedQuery::Select(UntypedSelectQuery {
                            full_table_name,
                            projection_items,
                        }))
                    }
                }
            }
            Query::Delete(DeleteStatement {
                schema_name,
                table_name,
                where_clause: _where_clause,
            }) => {
                let full_table_name = FullTableName::from((&schema_name, &table_name));
                match self.database.table_definition(full_table_name.clone()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some(None) => Err(AnalysisError::table_does_not_exist(full_table_name)),
                    Some(Some(_table_info)) => Ok(UntypedQuery::Delete(UntypedDeleteQuery { full_table_name })),
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum AnalysisError {
    SchemaDoesNotExist(String),
    TableDoesNotExist(String),
    ColumnNotFound(String),
    ColumnCantBeReferenced(String), // Error code: 42703
}

impl AnalysisError {
    pub fn schema_does_not_exist<S: ToString>(schema_name: S) -> AnalysisError {
        AnalysisError::SchemaDoesNotExist(schema_name.to_string())
    }

    pub fn table_does_not_exist<T: ToString>(table_name: T) -> AnalysisError {
        AnalysisError::TableDoesNotExist(table_name.to_string())
    }

    pub fn column_not_found<C: ToString>(column_name: C) -> AnalysisError {
        AnalysisError::ColumnNotFound(column_name.to_string())
    }

    pub fn column_cant_be_referenced<C: ToString>(column_name: C) -> AnalysisError {
        AnalysisError::ColumnCantBeReferenced(column_name.to_string())
    }
}

impl From<AnalysisError> for QueryError {
    fn from(error: AnalysisError) -> QueryError {
        match error {
            AnalysisError::SchemaDoesNotExist(schema_name) => QueryError::schema_does_not_exist(schema_name),
            AnalysisError::TableDoesNotExist(table_name) => QueryError::table_does_not_exist(table_name),
            AnalysisError::ColumnNotFound(column_name) => QueryError::column_does_not_exist(column_name),
            AnalysisError::ColumnCantBeReferenced(column_name) => QueryError::column_does_not_exist(column_name),
        }
    }
}

#[cfg(test)]
mod tests;

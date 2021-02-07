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

use std::{convert::TryFrom, sync::Arc};

use catalog::CatalogDefinition;
use data_definition_execution_plan::{
    ColumnInfo, CreateIndexQuery, CreateSchemaQuery, CreateTableQuery, DropSchemasQuery, DropTablesQuery, SchemaChange,
};
use data_manipulation_operators::BiOperator;
use data_manipulation_untyped_queries::{DeleteQuery, InsertQuery, SelectQuery, UntypedWrite, UpdateQuery};
use data_manipulation_untyped_tree::{DynamicUntypedItem, DynamicUntypedTree};
use definition::{FullTableName, SchemaName};
use types::SqlType;

use crate::{dynamic_tree_builder::DynamicTreeBuilder, static_tree_builder::StaticTreeBuilder};
use std::collections::HashMap;

mod dynamic_tree_builder;
mod operation_mapper;
mod static_tree_builder;

pub struct Analyzer<CD: CatalogDefinition> {
    database: Arc<CD>,
}

impl<CD: CatalogDefinition> Analyzer<CD> {
    pub fn new(database: Arc<CD>) -> Analyzer<CD> {
        Analyzer { database }
    }

    pub fn analyze(&self, statement: sql_ast::Statement) -> Result<QueryAnalysis, AnalysisError> {
        match &statement {
            sql_ast::Statement::Insert {
                table_name,
                source,
                columns,
            } => match FullTableName::try_from(table_name) {
                Err(error) => Err(AnalysisError::table_naming_error(error)),
                Ok(full_table_name) => match self.database.table_definition(full_table_name.clone()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some(None) => Err(AnalysisError::table_does_not_exist(full_table_name)),
                    Some(Some(table_info)) => {
                        let table_columns = table_info.column_names();
                        let column_names = if columns.is_empty() {
                            table_info.column_names().into_iter()
                        } else {
                            let mut column_names = vec![];
                            for column in columns {
                                let column = column.value.to_lowercase();
                                if !table_info.has_column(&column) {
                                    return Err(AnalysisError::column_not_found(column));
                                }
                                column_names.push(column);
                            }
                            column_names.into_iter()
                        };
                        let column_map = column_names
                            .enumerate()
                            .map(|(index, name)| (name, index))
                            .collect::<HashMap<String, usize>>();

                        let sql_ast::Query { body, .. } = &**source;
                        let values = match body {
                            sql_ast::SetExpr::Values(sql_ast::Values(insert_rows)) => {
                                let mut values = vec![];
                                log::debug!("column map {:?}", column_map);
                                for insert_row in insert_rows {
                                    log::debug!("building static tree for {:?} row", insert_row);
                                    let mut row = vec![];
                                    for table_column in &table_columns {
                                        let value = match column_map.get(table_column) {
                                            None => None,
                                            Some(index) => {
                                                Some(StaticTreeBuilder::build_from(&insert_row[*index], &statement)?)
                                            }
                                        };
                                        row.push(value);
                                    }
                                    values.push(row)
                                }
                                values
                            }
                            sql_ast::SetExpr::Query(_) | sql_ast::SetExpr::Select(_) => {
                                return Err(AnalysisError::FeatureNotSupported(Feature::InsertIntoSelect))
                            }
                            sql_ast::SetExpr::SetOperation { .. } => {
                                return Err(AnalysisError::FeatureNotSupported(Feature::SetOperations))
                            }
                        };
                        Ok(QueryAnalysis::Write(UntypedWrite::Insert(InsertQuery {
                            full_table_name,
                            values,
                        })))
                    }
                },
            },
            sql_ast::Statement::Update {
                table_name,
                assignments: stmt_assignments,
                ..
            } => match FullTableName::try_from(table_name) {
                Err(error) => Err(AnalysisError::table_naming_error(error)),
                Ok(full_table_name) => match self.database.table_definition(full_table_name.clone()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some(None) => Err(AnalysisError::table_does_not_exist(full_table_name)),
                    Some(Some(table_info)) => {
                        let table_columns = table_info.columns();
                        let mut column_names = vec![];
                        let mut assignments = vec![];
                        for assignment in stmt_assignments {
                            let sql_ast::Assignment { id, value } = assignment;
                            let name = id.to_string().to_lowercase();
                            let mut found = None;
                            for table_column in table_columns {
                                if table_column.has_name(&name) {
                                    found = Some(name.clone());
                                    break;
                                }
                            }
                            match found {
                                None => return Err(AnalysisError::ColumnNotFound(name)),
                                Some(name) => {
                                    assignments.push(DynamicTreeBuilder::build_from(
                                        &value,
                                        &statement,
                                        &table_columns,
                                    )?);
                                    column_names.push(name);
                                }
                            }
                        }
                        Ok(QueryAnalysis::Write(UntypedWrite::Update(UpdateQuery {
                            full_table_name,
                            column_names,
                            assignments,
                        })))
                    }
                },
            },
            sql_ast::Statement::Query(query) => {
                let sql_ast::Query { body, .. } = &**query;
                match body {
                    sql_ast::SetExpr::Query(_) => Err(AnalysisError::feature_not_supported(Feature::SubQueries)),
                    sql_ast::SetExpr::SetOperation { .. } => {
                        Err(AnalysisError::feature_not_supported(Feature::SetOperations))
                    }
                    value_expr @ sql_ast::SetExpr::Values(_) => Err(AnalysisError::syntax_error(format!(
                        "Syntax error in {}\naround {}",
                        statement, value_expr
                    ))),
                    sql_ast::SetExpr::Select(select) => {
                        let sql_ast::Select { projection, from, .. } = &**select;
                        if from.len() > 1 {
                            return Err(AnalysisError::feature_not_supported(Feature::Joins));
                        }
                        let sql_ast::TableWithJoins { relation, .. } = &from[0];
                        let name = match relation {
                            sql_ast::TableFactor::Table { name, .. } => name,
                            sql_ast::TableFactor::Derived { .. } => {
                                return Err(AnalysisError::feature_not_supported(Feature::FromSubQuery))
                            }
                            sql_ast::TableFactor::TableFunction { .. } => {
                                return Err(AnalysisError::feature_not_supported(Feature::TableFunctions))
                            }
                            sql_ast::TableFactor::NestedJoin(_) => {
                                return Err(AnalysisError::feature_not_supported(Feature::NestedJoin))
                            }
                        };
                        match FullTableName::try_from(name) {
                            Err(error) => Err(AnalysisError::table_naming_error(error)),
                            Ok(full_table_name) => match self.database.table_definition(full_table_name.clone()) {
                                None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                                Some(None) => Err(AnalysisError::table_does_not_exist(full_table_name)),
                                Some(Some(table_info)) => {
                                    let table_columns = table_info.columns();
                                    let mut projection_items = vec![];
                                    for item in projection {
                                        match item {
                                            sql_ast::SelectItem::Wildcard => {
                                                for (index, table_column) in table_columns.iter().enumerate() {
                                                    projection_items.push(DynamicUntypedTree::Item(
                                                        DynamicUntypedItem::Column {
                                                            name: table_column.name().to_lowercase(),
                                                            index,
                                                            sql_type: table_column.sql_type(),
                                                        },
                                                    ));
                                                }
                                            }
                                            sql_ast::SelectItem::UnnamedExpr(expr) => projection_items.push(
                                                DynamicTreeBuilder::build_from(&expr, &statement, &table_columns)?,
                                            ),
                                            sql_ast::SelectItem::ExprWithAlias { .. } => {
                                                return Err(AnalysisError::feature_not_supported(Feature::Aliases))
                                            }
                                            sql_ast::SelectItem::QualifiedWildcard(_) => {
                                                return Err(AnalysisError::feature_not_supported(
                                                    Feature::QualifiedAliases,
                                                ))
                                            }
                                        }
                                    }
                                    Ok(QueryAnalysis::Read(SelectQuery {
                                        full_table_name,
                                        projection_items,
                                    }))
                                }
                            },
                        }
                    }
                }
            }
            sql_ast::Statement::Delete { table_name, .. } => match FullTableName::try_from(table_name) {
                Err(error) => Err(AnalysisError::table_naming_error(error)),
                Ok(full_table_name) => match self.database.table_definition(full_table_name.clone()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some(None) => Err(AnalysisError::table_does_not_exist(full_table_name)),
                    Some(Some(_table_info)) => Ok(QueryAnalysis::Write(UntypedWrite::Delete(DeleteQuery {
                        full_table_name,
                    }))),
                },
            },
            sql_ast::Statement::CreateTable {
                name,
                columns,
                if_not_exists,
                ..
            } => match FullTableName::try_from(name) {
                Ok(full_table_name) => {
                    if self
                        .database
                        .schema_exists(&SchemaName::from(&full_table_name.schema()))
                    {
                        let mut column_defs = Vec::new();
                        for column in columns {
                            match SqlType::try_from(&column.data_type) {
                                Ok(sql_type) => column_defs.push(ColumnInfo {
                                    name: column.name.value.as_str().to_lowercase(),
                                    sql_type,
                                }),
                                Err(_not_supported_type_error) => {
                                    return Err(AnalysisError::type_is_not_supported(&column.data_type));
                                }
                            }
                        }
                        Ok(QueryAnalysis::DataDefinition(SchemaChange::CreateTable(
                            CreateTableQuery {
                                full_table_name,
                                column_defs,
                                if_not_exists: *if_not_exists,
                            },
                        )))
                    } else {
                        Err(AnalysisError::schema_does_not_exist(full_table_name.schema()))
                    }
                }
                Err(error) => Err(AnalysisError::table_naming_error(&error)),
            },
            sql_ast::Statement::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => match SchemaName::try_from(schema_name) {
                Ok(schema_name) => Ok(QueryAnalysis::DataDefinition(SchemaChange::CreateSchema(
                    CreateSchemaQuery {
                        schema_name,
                        if_not_exists: *if_not_exists,
                    },
                ))),
                Err(error) => Err(AnalysisError::schema_naming_error(&error)),
            },
            sql_ast::Statement::CreateIndex {
                name,
                table_name,
                columns,
                unique: _unique,
                if_not_exists: _if_not_exists,
            } => match FullTableName::try_from(table_name) {
                Ok(full_table_name) => match self.database.table_definition(full_table_name.clone()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some(None) => Err(AnalysisError::table_does_not_exist(full_table_name)),
                    Some(Some(table_info)) => {
                        let mut column_names = vec![];
                        let table_columns = table_info.column_names();
                        for column in columns {
                            match &column.expr {
                                sql_ast::Expr::Identifier(name) => {
                                    if table_columns.contains(&name.value) {
                                        column_names.push(name.value.clone());
                                    } else {
                                        return Err(AnalysisError::column_not_found(&name.value));
                                    }
                                }
                                _ => unimplemented!(),
                            }
                        }
                        Ok(QueryAnalysis::DataDefinition(SchemaChange::CreateIndex(
                            CreateIndexQuery {
                                name: name.to_string(),
                                full_table_name,
                                column_names,
                            },
                        )))
                    }
                },
                Err(error) => Err(AnalysisError::table_naming_error(error)),
            },
            sql_ast::Statement::Drop {
                names,
                object_type,
                cascade,
                if_exists,
            } => match object_type {
                sql_ast::ObjectType::Schema => {
                    let mut schema_names = vec![];
                    for name in names {
                        match SchemaName::try_from(name) {
                            Ok(schema_name) => schema_names.push(schema_name),
                            Err(error) => return Err(AnalysisError::schema_naming_error(&error)),
                        }
                    }
                    Ok(QueryAnalysis::DataDefinition(SchemaChange::DropSchemas(
                        DropSchemasQuery {
                            schema_names,
                            cascade: *cascade,
                            if_exists: *if_exists,
                        },
                    )))
                }
                sql_ast::ObjectType::Table => {
                    let mut table_infos = vec![];
                    for name in names {
                        match FullTableName::try_from(name) {
                            Ok(full_table_name) => {
                                if self
                                    .database
                                    .schema_exists(&SchemaName::from(&full_table_name.schema()))
                                {
                                    table_infos.push(full_table_name)
                                } else {
                                    return Err(AnalysisError::schema_does_not_exist(full_table_name.schema()));
                                }
                            }
                            Err(error) => return Err(AnalysisError::table_naming_error(&error)),
                        }
                    }
                    Ok(QueryAnalysis::DataDefinition(SchemaChange::DropTables(
                        DropTablesQuery {
                            full_table_names: table_infos,
                            cascade: *cascade,
                            if_exists: *if_exists,
                        },
                    )))
                }
                sql_ast::ObjectType::View => unimplemented!("VIEWs are not implemented yet"),
                sql_ast::ObjectType::Index => unimplemented!("INDEXes are not implemented yet"),
            },
            sql_ast::Statement::Copy { .. } => unimplemented!(),
            sql_ast::Statement::CreateView { .. } => unimplemented!(),
            sql_ast::Statement::CreateVirtualTable { .. } => unimplemented!(),
            sql_ast::Statement::AlterTable { .. } => unimplemented!(),
            sql_ast::Statement::SetVariable { .. } => unimplemented!(),
            sql_ast::Statement::ShowVariable { .. } => unimplemented!(),
            sql_ast::Statement::ShowColumns { .. } => unimplemented!(),
            sql_ast::Statement::StartTransaction { .. } => unimplemented!(),
            sql_ast::Statement::SetTransaction { .. } => unimplemented!(),
            sql_ast::Statement::Commit { .. } => unimplemented!(),
            sql_ast::Statement::Rollback { .. } => unimplemented!(),
            sql_ast::Statement::Assert { .. } => unimplemented!(),
            sql_ast::Statement::Deallocate { .. } => unimplemented!(),
            sql_ast::Statement::Execute { .. } => unimplemented!(),
            sql_ast::Statement::Prepare { .. } => unimplemented!(),
            sql_ast::Statement::Analyze { .. } => unimplemented!(),
            sql_ast::Statement::Explain { .. } => unimplemented!(),
        }
    }
}

fn parse_param_index(value: &str) -> Option<usize> {
    let mut chars = value.chars();
    if chars.next() != Some('$') || !chars.all(|c| c.is_digit(10)) {
        return None;
    }

    let index: usize = (&value[1..]).parse().unwrap();
    if index == 0 {
        return None;
    }

    Some(index - 1)
}

pub type AnalysisResult<A> = Result<A, AnalysisError>;

#[derive(Debug, PartialEq)]
pub enum QueryAnalysis {
    DataDefinition(SchemaChange),
    Write(UntypedWrite),
    Read(SelectQuery),
}

#[derive(Debug, PartialEq)]
pub enum AnalysisError {
    SchemaNamingError(String),
    SchemaDoesNotExist(String),
    // SchemaAlreadyExists(String),
    TableNamingError(String),
    TableDoesNotExist(String),
    // TableAlreadyExists(String),
    TypeIsNotSupported(String),
    SyntaxError(String),
    ColumnNotFound(String),
    ColumnCantBeReferenced(String), // Error code: 42703
    // InvalidInputSyntaxForType { sql_type: SqlType, value: String },  // Error code: 22P02
    // StringDataRightTruncation(SqlType),                              // Error code: 22001
    // DatatypeMismatch { column_type: SqlType, source_type: SqlType }, // Error code: 42804
    // AmbiguousFunction(BiOperation),                                    // Error code: 42725
    UndefinedFunction(BiOperator), // Error code: 42883
    FeatureNotSupported(Feature),
}

impl AnalysisError {
    pub fn schema_naming_error<M: ToString>(message: M) -> AnalysisError {
        AnalysisError::SchemaNamingError(message.to_string())
    }

    pub fn schema_does_not_exist<S: ToString>(schema_name: S) -> AnalysisError {
        AnalysisError::SchemaDoesNotExist(schema_name.to_string())
    }

    pub fn table_naming_error<M: ToString>(message: M) -> AnalysisError {
        AnalysisError::TableNamingError(message.to_string())
    }

    pub fn table_does_not_exist<T: ToString>(table_name: T) -> AnalysisError {
        AnalysisError::TableDoesNotExist(table_name.to_string())
    }

    pub fn type_is_not_supported<T: ToString>(type_name: T) -> AnalysisError {
        AnalysisError::TypeIsNotSupported(type_name.to_string())
    }

    pub fn syntax_error(message: String) -> AnalysisError {
        AnalysisError::SyntaxError(message)
    }

    pub fn column_not_found<C: ToString>(column_name: C) -> AnalysisError {
        AnalysisError::ColumnNotFound(column_name.to_string())
    }

    pub fn column_cant_be_referenced<C: ToString>(column_name: C) -> AnalysisError {
        AnalysisError::ColumnCantBeReferenced(column_name.to_string())
    }

    pub fn feature_not_supported(feature: Feature) -> AnalysisError {
        AnalysisError::FeatureNotSupported(feature)
    }
}

#[derive(Debug, PartialEq)]
pub enum Feature {
    SetOperations,
    SubQueries,
    NationalStringLiteral,
    HexStringLiteral,
    TimeInterval,
    Joins,
    NestedJoin,
    FromSubQuery,
    TableFunctions,
    Aliases,
    QualifiedAliases,
    InsertIntoSelect,
}

#[cfg(test)]
mod tests;

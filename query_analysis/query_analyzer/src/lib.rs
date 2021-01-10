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

use crate::{
    insert_tree_builder::InsertTreeBuilder, projection_tree_builder::ProjectionTreeBuilder,
    update_tree_builder::UpdateTreeBuilder,
};
use analysis_tree::{
    AnalysisError, ColumnInfo, CreateSchemaQuery, CreateTableQuery, DeleteQuery, DropSchemasQuery, DropTablesQuery,
    Feature, FullTableId, InsertQuery, ProjectionTreeNode, QueryAnalysis, SchemaChange, SelectQuery, TableInfo,
    UpdateQuery, Write,
};
use catalog::CatalogDefinition;
use data_manager::DataDefReader;
use definition::{FullTableName, SchemaName};
use expr_operators::Operand;
use std::{convert::TryFrom, sync::Arc};
use types::SqlType;

mod insert_tree_builder;
mod operation_mapper;
mod projection_tree_builder;
mod update_tree_builder;

pub struct Analyzer<CD: CatalogDefinition> {
    data_definition: Arc<dyn DataDefReader>,
    database: Arc<CD>,
}

impl<CD: CatalogDefinition> Analyzer<CD> {
    pub fn new(data_definition: Arc<dyn DataDefReader>, database: Arc<CD>) -> Analyzer<CD> {
        Analyzer {
            data_definition,
            database,
        }
    }

    pub fn analyze(&self, statement: sql_ast::Statement) -> Result<QueryAnalysis, AnalysisError> {
        match &statement {
            sql_ast::Statement::Insert {
                table_name,
                source,
                columns,
            } => match FullTableName::try_from(table_name) {
                Err(error) => Err(AnalysisError::table_naming_error(error)),
                Ok(full_table_name) => match self.database.table_definition(&full_table_name) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some(None) => Err(AnalysisError::table_does_not_exist(full_table_name)),
                    Some(Some(table_info)) => {
                        for column in columns.iter() {
                            if !table_info.has_column(&column.to_string()) {
                                return Err(AnalysisError::column_not_found(column));
                            }
                        }
                        let column_types: Vec<SqlType> =
                            table_info.columns().iter().map(|col| col.sql_type()).collect();
                        let sql_ast::Query { body, .. } = &**source;
                        let values = match body {
                            sql_ast::SetExpr::Values(sql_ast::Values(insert_rows)) => {
                                let mut values = vec![];
                                for insert_row in insert_rows {
                                    let mut row = vec![];
                                    for (index, value) in insert_row.iter().enumerate() {
                                        let sql_type = column_types[index];
                                        row.push(InsertTreeBuilder::build_from(value, &statement, &sql_type)?);
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
                        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                            full_table_name,
                            column_types,
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
                Ok(full_table_name) => match self.data_definition.table_desc((&full_table_name).into()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some((_schema_id, None)) => Err(AnalysisError::table_does_not_exist(full_table_name)),
                    Some((schema_id, Some((table_id, table_columns)))) => {
                        let mut sql_types = vec![];
                        let mut assignments = vec![];
                        for assignment in stmt_assignments {
                            let sql_ast::Assignment { id, value } = assignment;
                            let name = id.to_string();
                            let mut found = None;
                            for table_column in &table_columns {
                                if table_column.has_name(&name) {
                                    found = Some(table_column.sql_type());
                                    break;
                                }
                            }
                            match found {
                                None => unimplemented!("Column not found is not covered yet"),
                                Some(sql_type) => {
                                    assignments.push(UpdateTreeBuilder::build_from(
                                        &value,
                                        &statement,
                                        &sql_type,
                                        &table_columns,
                                    )?);
                                    sql_types.push(sql_type);
                                }
                            }
                        }
                        Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
                            full_table_id: FullTableId::from((schema_id, table_id)),
                            sql_types,
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
                            Ok(full_table_name) => match self.data_definition.table_desc((&full_table_name).into()) {
                                None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                                Some((_schema_id, None)) => Err(AnalysisError::table_does_not_exist(&full_table_name)),
                                Some((schema_id, Some((table_id, table_columns)))) => {
                                    let full_table_id = FullTableId::from((schema_id, table_id));
                                    let mut projection_items = vec![];
                                    for item in projection {
                                        match item {
                                            sql_ast::SelectItem::Wildcard => {
                                                for (index, table_column) in table_columns.iter().enumerate() {
                                                    projection_items.push(ProjectionTreeNode::Item(Operand::Column {
                                                        index,
                                                        sql_type: table_column.sql_type(),
                                                    }));
                                                }
                                            }
                                            sql_ast::SelectItem::UnnamedExpr(expr) => {
                                                projection_items.push(ProjectionTreeBuilder::build_from(
                                                    &expr,
                                                    &statement,
                                                    &SqlType::small_int(),
                                                    &table_columns,
                                                )?)
                                            }
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
                                        full_table_id,
                                        projection_items,
                                    }))
                                }
                            },
                            Err(error) => Err(AnalysisError::table_naming_error(&error)),
                        }
                    }
                }
            }
            sql_ast::Statement::Delete { table_name, .. } => match FullTableName::try_from(table_name) {
                Ok(full_table_name) => match self.data_definition.table_exists_tuple((&full_table_name).into()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some((_schema_id, None)) => Err(AnalysisError::table_does_not_exist(&full_table_name)),
                    Some((schema_id, Some(table_id))) => Ok(QueryAnalysis::Write(Write::Delete(DeleteQuery {
                        full_table_id: FullTableId::from((schema_id, table_id)),
                    }))),
                },
                Err(error) => Err(AnalysisError::table_naming_error(error)),
            },
            sql_ast::Statement::CreateTable {
                name,
                columns,
                if_not_exists,
                ..
            } => match FullTableName::try_from(name) {
                Ok(full_table_name) => match self.data_definition.schema_exists(full_table_name.schema()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some(schema_id) => {
                        let mut column_defs = Vec::new();
                        for column in columns {
                            match SqlType::try_from(&column.data_type) {
                                Ok(sql_type) => column_defs.push(ColumnInfo {
                                    name: column.name.value.as_str().to_owned(),
                                    sql_type,
                                }),
                                Err(_not_supported_type_error) => {
                                    return Err(AnalysisError::type_is_not_supported(&column.data_type));
                                }
                            }
                        }
                        Ok(QueryAnalysis::DataDefinition(SchemaChange::CreateTable(
                            CreateTableQuery {
                                table_info: TableInfo::new(
                                    schema_id,
                                    &full_table_name.schema(),
                                    &full_table_name.table(),
                                ),
                                column_defs,
                                if_not_exists: *if_not_exists,
                            },
                        )))
                    }
                },
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
                                match self.data_definition.table_exists_tuple((&full_table_name).into()) {
                                    None => return Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                                    Some((schema_id, _)) => table_infos.push(TableInfo::new(
                                        schema_id,
                                        &full_table_name.schema(),
                                        &full_table_name.table(),
                                    )),
                                }
                            }
                            Err(error) => return Err(AnalysisError::table_naming_error(&error)),
                        }
                    }
                    Ok(QueryAnalysis::DataDefinition(SchemaChange::DropTables(
                        DropTablesQuery {
                            table_infos,
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
            sql_ast::Statement::CreateIndex { .. } => unimplemented!(),
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

#[cfg(test)]
mod tests;

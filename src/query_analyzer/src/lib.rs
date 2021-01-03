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
    insert_tree_builder::InsertTreeBuilder, projection_tree_builder::ProjectionTreeBuilder,
    update_tree_builder::UpdateTreeBuilder,
};
use analysis::{
    AnalysisError, ColumnDesc, CreateSchemaQuery, CreateTableQuery, DeleteQuery, DropSchemasQuery, DropTablesQuery,
    Feature, FullTableId, FullTableName, InsertQuery, ProjectionTreeNode, QueryAnalysis, SchemaChange, SchemaName,
    SelectQuery, TableInfo, UpdateQuery, Write,
};
use data_definition::DataDefReader;
use expr_operators::Operator;
use sqlparser::ast;
use std::{convert::TryFrom, sync::Arc};
use types::SqlType;

mod insert_tree_builder;
mod operation_mapper;
mod projection_tree_builder;
mod update_tree_builder;

pub struct Analyzer {
    data_definition: Arc<dyn DataDefReader>,
}

impl Analyzer {
    pub fn new(data_definition: Arc<dyn DataDefReader>) -> Analyzer {
        Analyzer { data_definition }
    }

    pub fn analyze(&self, statement: ast::Statement) -> Result<QueryAnalysis, AnalysisError> {
        match &statement {
            ast::Statement::Insert { table_name, source, .. } => match FullTableName::try_from(table_name) {
                Err(error) => Err(AnalysisError::table_naming_error(error)),
                Ok(full_table_name) => match self.data_definition.table_desc((&full_table_name).into()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some((_schema_id, None)) => Err(AnalysisError::table_does_not_exist(full_table_name)),
                    Some((schema_id, Some((table_id, table_columns)))) => {
                        let column_types: Vec<SqlType> = table_columns.into_iter().map(|col| col.sql_type()).collect();
                        let ast::Query { body, .. } = &**source;
                        let values = match body {
                            ast::SetExpr::Values(ast::Values(insert_rows)) => {
                                let mut values = vec![];
                                for insert_row in insert_rows {
                                    let mut row = vec![];
                                    for (index, value) in insert_row.iter().enumerate() {
                                        let sql_type = column_types[index];
                                        row.push(InsertTreeBuilder::build_from(
                                            value,
                                            &statement,
                                            &sql_type.general_type(),
                                            &sql_type,
                                        )?);
                                    }
                                    values.push(row)
                                }
                                values
                            }
                            ast::SetExpr::Query(_) | ast::SetExpr::Select(_) => {
                                return Err(AnalysisError::FeatureNotSupported(Feature::InsertIntoSelect))
                            }
                            ast::SetExpr::SetOperation { .. } => {
                                return Err(AnalysisError::FeatureNotSupported(Feature::SetOperations))
                            }
                        };
                        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                            full_table_id: FullTableId::from((schema_id, table_id)),
                            column_types,
                            values,
                        })))
                    }
                },
            },
            ast::Statement::Update {
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
                            let ast::Assignment { id, value } = assignment;
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
                                        &sql_type.general_type(),
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
            ast::Statement::Query(query) => {
                let ast::Query { body, .. } = &**query;
                match body {
                    ast::SetExpr::Query(_) => Err(AnalysisError::feature_not_supported(Feature::SubQueries)),
                    ast::SetExpr::SetOperation { .. } => {
                        Err(AnalysisError::feature_not_supported(Feature::SetOperations))
                    }
                    value_expr @ ast::SetExpr::Values(_) => Err(AnalysisError::syntax_error(format!(
                        "Syntax error in {}\naround {}",
                        statement, value_expr
                    ))),
                    ast::SetExpr::Select(select) => {
                        let ast::Select { projection, from, .. } = &**select;
                        if from.len() > 1 {
                            return Err(AnalysisError::feature_not_supported(Feature::Joins));
                        }
                        let ast::TableWithJoins { relation, .. } = &from[0];
                        let name = match relation {
                            ast::TableFactor::Table { name, .. } => name,
                            ast::TableFactor::Derived { .. } => {
                                return Err(AnalysisError::feature_not_supported(Feature::FromSubQuery))
                            }
                            ast::TableFactor::TableFunction { .. } => {
                                return Err(AnalysisError::feature_not_supported(Feature::TableFunctions))
                            }
                            ast::TableFactor::NestedJoin(_) => {
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
                                            ast::SelectItem::Wildcard => {
                                                for (index, table_column) in table_columns.iter().enumerate() {
                                                    projection_items.push(ProjectionTreeNode::Item(Operator::Column {
                                                        index,
                                                        sql_type: table_column.sql_type(),
                                                    }));
                                                }
                                            }
                                            ast::SelectItem::UnnamedExpr(expr) => {
                                                projection_items.push(ProjectionTreeBuilder::build_from(
                                                    &expr,
                                                    &statement,
                                                    &SqlType::SmallInt,
                                                    &table_columns,
                                                )?)
                                            }
                                            ast::SelectItem::ExprWithAlias { .. } => {
                                                return Err(AnalysisError::feature_not_supported(Feature::Aliases))
                                            }
                                            ast::SelectItem::QualifiedWildcard(_) => {
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
            ast::Statement::Delete { table_name, .. } => match FullTableName::try_from(table_name) {
                Ok(full_table_name) => match self.data_definition.table_exists_tuple((&full_table_name).into()) {
                    None => Err(AnalysisError::schema_does_not_exist(full_table_name.schema())),
                    Some((_schema_id, None)) => Err(AnalysisError::table_does_not_exist(&full_table_name)),
                    Some((schema_id, Some(table_id))) => Ok(QueryAnalysis::Write(Write::Delete(DeleteQuery {
                        full_table_id: FullTableId::from((schema_id, table_id)),
                    }))),
                },
                Err(error) => Err(AnalysisError::table_naming_error(error)),
            },
            ast::Statement::CreateTable {
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
                                Ok(sql_type) => column_defs.push(ColumnDesc {
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
            ast::Statement::CreateSchema {
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
            ast::Statement::Drop {
                names,
                object_type,
                cascade,
                if_exists,
            } => match object_type {
                ast::ObjectType::Schema => {
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
                ast::ObjectType::Table => {
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
                ast::ObjectType::View => unimplemented!("VIEWs are not implemented yet"),
                ast::ObjectType::Index => unimplemented!("INDEXes are not implemented yet"),
            },
            ast::Statement::Copy { .. } => unimplemented!(),
            ast::Statement::CreateView { .. } => unimplemented!(),
            ast::Statement::CreateVirtualTable { .. } => unimplemented!(),
            ast::Statement::CreateIndex { .. } => unimplemented!(),
            ast::Statement::AlterTable { .. } => unimplemented!(),
            ast::Statement::SetVariable { .. } => unimplemented!(),
            ast::Statement::ShowVariable { .. } => unimplemented!(),
            ast::Statement::ShowColumns { .. } => unimplemented!(),
            ast::Statement::StartTransaction { .. } => unimplemented!(),
            ast::Statement::SetTransaction { .. } => unimplemented!(),
            ast::Statement::Commit { .. } => unimplemented!(),
            ast::Statement::Rollback { .. } => unimplemented!(),
            ast::Statement::Assert { .. } => unimplemented!(),
            ast::Statement::Deallocate { .. } => unimplemented!(),
            ast::Statement::Execute { .. } => unimplemented!(),
            ast::Statement::Prepare { .. } => unimplemented!(),
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

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

use data_manager::DataDefReader;
use description::{
    ColumnDesc, Description, DescriptionError, DropSchemasInfo, DropTablesInfo, FullTableId, DeprecatedFullTableName,
    InsertStatement, ParamIndex, ParamTypes, ProjectionItem, SchemaCreationInfo, SchemaId, SchemaName, SelectStatement,
    TableCreationInfo, UpdateStatement,
};
use meta_def::DeprecatedColumnDefinition;
use sql_ast::{
    Assignment, Expr, Ident, ObjectType, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};
use std::{convert::TryFrom, ops::Deref, sync::Arc};
use types::SqlType;

pub struct Analyzer {
    metadata: Arc<dyn DataDefReader>,
}

impl Analyzer {
    pub fn new(metadata: Arc<dyn DataDefReader>) -> Analyzer {
        Analyzer { metadata }
    }

    pub fn describe(&self, statement: &Statement) -> Result<Description, DescriptionError> {
        match statement {
            Statement::Insert {
                columns,
                source,
                table_name,
                ..
            } => match DeprecatedFullTableName::try_from(table_name) {
                Ok(full_table_name) => match self.metadata.table_desc((&full_table_name).into()) {
                    Some((schema_id, Some((table_id, table_columns)))) => {
                        let source: &Query = source;
                        let Query { body, .. } = source;
                        let column_types = if columns.is_empty() {
                            table_columns.into_iter().map(|c| c.sql_type()).collect()
                        } else {
                            let mut col_types = vec![];
                            for col in columns {
                                let Ident { value, .. } = col;
                                let col_name = value.to_lowercase();
                                match table_columns.iter().find(|col_def| col_def.has_name(&col_name)) {
                                    Some(col_def) => col_types.push(col_def.sql_type()),
                                    None => return Err(DescriptionError::column_does_not_exist(&col_name)),
                                }
                            }
                            col_types
                        };
                        let param_types = parse_assign_param_types(body, &column_types)?;
                        let param_count = param_types.keys().max().map_or(0, |max_index| max_index + 1);
                        Ok(Description::Insert(InsertStatement {
                            table_id: FullTableId::from((schema_id, table_id)),
                            param_count,
                            param_types,
                        }))
                    }
                    Some((_schema_id, None)) => Err(DescriptionError::table_does_not_exist(&full_table_name)),
                    None => Err(DescriptionError::schema_does_not_exist(&full_table_name.schema())),
                },
                Err(error) => Err(DescriptionError::syntax_error(&error)),
            },
            Statement::Query(query) => {
                let Query { body, .. } = &**query;
                match body {
                    SetExpr::Select(query) => {
                        let Select { projection, from, .. } = query.deref();
                        let TableWithJoins { relation, .. } = &from[0];
                        match relation {
                            TableFactor::Table { name, .. } => match DeprecatedFullTableName::try_from(name) {
                                Ok(full_table_name) => {
                                    match self.metadata.table_exists_tuple((&full_table_name).into()) {
                                        None => Err(DescriptionError::schema_does_not_exist(&full_table_name.schema())),
                                        Some((_, None)) => {
                                            Err(DescriptionError::table_does_not_exist(&full_table_name))
                                        }
                                        Some((schema_id, Some(table_id))) => {
                                            let full_table_id = FullTableId::from((schema_id, table_id));
                                            let projection_items = {
                                                let mut names: Vec<String> = vec![];
                                                for item in projection {
                                                    match item {
                                                        SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                                                            value,
                                                            ..
                                                        })) => names.push(value.to_lowercase()),
                                                        SelectItem::Wildcard => {
                                                            for (_col_id, col_def) in self
                                                                .metadata
                                                                .table_columns(&full_table_id)
                                                                .expect("table exists")
                                                            {
                                                                names.push(col_def.name());
                                                            }
                                                        }
                                                        _ => unimplemented!(),
                                                    }
                                                }
                                                let columns =
                                                    self.metadata.table_columns(&full_table_id).expect("table exists");

                                                let mut projection_items = vec![];
                                                for name in &names {
                                                    let mut found = None;
                                                    for (column_id, column) in &columns {
                                                        if column.has_name(name) {
                                                            found = Some((*column_id, column.sql_type()));
                                                        }
                                                    }
                                                    match found {
                                                        None => {
                                                            return Err(DescriptionError::column_does_not_exist(name))
                                                        }
                                                        Some((column_id, sql_type)) => projection_items
                                                            .push(ProjectionItem::Column(column_id, sql_type)),
                                                    }
                                                }
                                                projection_items
                                            };
                                            Ok(Description::Select(SelectStatement {
                                                full_table_id,
                                                projection_items,
                                            }))
                                        }
                                    }
                                }
                                Err(error) => Err(DescriptionError::syntax_error(&error)),
                            },
                            _ => Err(DescriptionError::feature_not_supported(&*query)),
                        }
                    }
                    _ => unimplemented!(),
                }
            }
            Statement::Update {
                assignments,
                selection,
                table_name,
                ..
            } => match DeprecatedFullTableName::try_from(table_name) {
                Ok(full_table_name) => match self.metadata.table_desc((&full_table_name).into()) {
                    Some((schema_id, Some((table_id, table_columns)))) => {
                        let mut param_types = ParamTypes::new();
                        for assignment in assignments {
                            let Assignment { id, value } = assignment;
                            if let Expr::Identifier(Ident { value, .. }) = value {
                                if let Some(param_index) = parse_param_index(value) {
                                    let Ident { value, .. } = id;
                                    parse_param_type_by_column(&mut param_types, &table_columns, param_index, value)?;
                                }
                            }
                        }
                        if let Some(Expr::BinaryOp { left, right, .. }) = selection {
                            if let (
                                Expr::Identifier(Ident { value: left_val, .. }),
                                Expr::Identifier(Ident { value: right_val, .. }),
                            ) = (left.deref(), right.deref())
                            {
                                let pair = if let Some(param_index) = parse_param_index(left_val) {
                                    Some((param_index, right_val))
                                } else if let Some(param_index) = parse_param_index(right_val) {
                                    Some((param_index, left_val))
                                } else {
                                    None
                                };
                                if let Some((param_index, col_name)) = pair {
                                    parse_param_type_by_column(
                                        &mut param_types,
                                        &table_columns,
                                        param_index,
                                        col_name,
                                    )?;
                                }
                            }
                        }
                        let param_count = param_types.keys().max().map_or(0, |max_index| max_index + 1);
                        Ok(Description::Update(UpdateStatement {
                            table_id: FullTableId::from((schema_id, table_id)),
                            param_count,
                            param_types,
                        }))
                    }
                    Some((_schema_id, None)) => Err(DescriptionError::table_does_not_exist(&full_table_name)),
                    None => Err(DescriptionError::schema_does_not_exist(&full_table_name.schema())),
                },
                Err(error) => Err(DescriptionError::syntax_error(&error)),
            },
            Statement::CreateTable { name, columns, .. } => match DeprecatedFullTableName::try_from(name) {
                Ok(full_table_name) => {
                    let (schema_name, table_name) = (&full_table_name).into();
                    match self.metadata.table_exists(schema_name, table_name) {
                        Some((_, Some(_))) => Err(DescriptionError::table_already_exists(&full_table_name)),
                        None => Err(DescriptionError::schema_does_not_exist(&full_table_name.schema())),
                        Some((schema_id, None)) => {
                            let mut column_defs = Vec::new();
                            for column in columns {
                                match SqlType::try_from(&column.data_type) {
                                    Ok(sql_type) => column_defs.push(ColumnDesc {
                                        name: column.name.value.as_str().to_owned(),
                                        pg_type: (&sql_type).into(),
                                    }),
                                    Err(_error) => {
                                        return Err(DescriptionError::feature_not_supported(&format!(
                                            "'{}' type is not supported",
                                            &column.data_type
                                        )));
                                    }
                                }
                            }
                            Ok(Description::CreateTable(TableCreationInfo {
                                schema_id,
                                table_name: table_name.to_owned(),
                                columns: column_defs,
                            }))
                        }
                    }
                }
                Err(error) => Err(DescriptionError::syntax_error(&error)),
            },
            Statement::CreateSchema { schema_name, .. } => match SchemaName::try_from(schema_name) {
                Ok(schema_name) => match self.metadata.schema_exists(&schema_name) {
                    Some(_) => Err(DescriptionError::schema_already_exists(&schema_name)),
                    None => Ok(Description::CreateSchema(SchemaCreationInfo {
                        schema_name: schema_name.to_string(),
                    })),
                },
                Err(error) => Err(DescriptionError::syntax_error(&error)),
            },
            Statement::Drop {
                names,
                object_type,
                cascade,
                if_exists,
            } => match object_type {
                ObjectType::Schema => {
                    let mut schema_ids = vec![];
                    for name in names {
                        match SchemaName::try_from(name) {
                            Ok(schema_name) => match self.metadata.schema_exists(&schema_name) {
                                None => return Err(DescriptionError::schema_does_not_exist(&schema_name)),
                                Some(schema_id) => schema_ids.push(SchemaId::from(schema_id)),
                            },
                            Err(error) => return Err(DescriptionError::syntax_error(&error)),
                        }
                    }
                    Ok(Description::DropSchemas(DropSchemasInfo {
                        schema_ids,
                        cascade: *cascade,
                        if_exists: *if_exists,
                    }))
                }
                ObjectType::Table => {
                    let mut full_table_ids = vec![];
                    for name in names {
                        match DeprecatedFullTableName::try_from(name) {
                            Ok(full_table_name) => match self.metadata.table_exists_tuple((&full_table_name).into()) {
                                None => return Err(DescriptionError::schema_does_not_exist(&full_table_name.schema())),
                                Some((_, None)) => {
                                    return Err(DescriptionError::table_does_not_exist(&full_table_name))
                                }
                                Some((schema_id, Some(table_id))) => {
                                    full_table_ids.push(FullTableId::from((schema_id, table_id)))
                                }
                            },
                            Err(error) => return Err(DescriptionError::syntax_error(&error)),
                        }
                    }
                    Ok(Description::DropTables(DropTablesInfo {
                        full_table_ids,
                        cascade: *cascade,
                        if_exists: *if_exists,
                    }))
                }
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        }
    }
}

fn parse_assign_param_types(body: &SetExpr, column_types: &[SqlType]) -> Result<ParamTypes, DescriptionError> {
    let rows = match body {
        SetExpr::Values(values) => &values.0,
        _ => return Ok(ParamTypes::new()),
    };

    let mut param_types = ParamTypes::new();
    for row in rows {
        for (col_index, col) in row.iter().enumerate() {
            if let Expr::Identifier(Ident { value, .. }) = col {
                if let Some(param_index) = parse_param_index(value) {
                    match param_types.get(&param_index) {
                        Some(col_type) => {
                            if col_type != &column_types[col_index] {
                                return Err(DescriptionError::syntax_error(&format!(
                                    "Parameter ${} cannot be bound to different SQL types",
                                    param_index
                                )));
                            }
                        }
                        None => {
                            param_types.insert(param_index, column_types[col_index]);
                        }
                    };
                }
            }
        }
    }

    Ok(param_types)
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

fn parse_param_type_by_column(
    param_types: &mut ParamTypes,
    columns: &[DeprecatedColumnDefinition],
    param_index: ParamIndex,
    col_name: &str,
) -> Result<(), DescriptionError> {
    let col_name = col_name.to_lowercase();
    let col_type = match columns.iter().find(|col_def| col_def.has_name(&col_name)) {
        Some(col_def) => col_def.sql_type(),
        None => return Err(DescriptionError::column_does_not_exist(&col_name)),
    };
    match param_types.get(&param_index) {
        Some(param_type) => {
            if param_type != &col_type {
                return Err(DescriptionError::syntax_error(&format!(
                    "Parameter ${} cannot be bound to different SQL types",
                    param_index
                )));
            }
        }
        None => {
            param_types.insert(param_index, col_type);
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests;

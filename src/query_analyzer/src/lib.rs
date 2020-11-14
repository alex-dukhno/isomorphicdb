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

use description::{
    ColumnDesc, Description, DescriptionError, DropSchemasInfo, DropTablesInfo, FullTableId, FullTableName,
    InsertStatement, ParamTypes, ProjectionItem, SchemaCreationInfo, SchemaId, SchemaName, SelectStatement,
    TableCreationInfo,
};
use metadata::{DataDefinition, MetadataView};
use sql_model::{sql_errors::NotFoundError, sql_types::SqlType};
use sqlparser::ast::{
    Expr, Ident, ObjectType, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};
use std::ops::Deref;
use std::{convert::TryFrom, sync::Arc};

pub struct Analyzer {
    metadata: Arc<DataDefinition>,
}

impl Analyzer {
    pub fn new(metadata: Arc<DataDefinition>) -> Analyzer {
        Analyzer { metadata }
    }

    pub fn describe(&self, statement: &Statement) -> Result<Description, DescriptionError> {
        match statement {
            Statement::Insert {
                columns,
                source,
                table_name,
                ..
            } => match FullTableName::try_from(table_name) {
                Ok(full_table_name) => match self.metadata.table_desc((&full_table_name).into()) {
                    Ok(table_def) => {
                        let source: &Query = source;
                        let Query { body, .. } = source;
                        let column_types = if columns.is_empty() {
                            table_def.column_types()
                        } else {
                            let table_cols = table_def.columns();
                            let mut col_types = vec![];
                            for col in columns {
                                let Ident { value, .. } = col;
                                let col_name = value.to_lowercase();
                                match table_cols.iter().find(|col_def| col_def.has_name(&col_name)) {
                                    Some(col_def) => col_types.push(col_def.sql_type()),
                                    None => return Err(DescriptionError::column_does_not_exist(&col_name)),
                                }
                            }
                            col_types
                        };
                        let param_types = parse_assign_param_types(body, &column_types)?;
                        let param_count = param_types.keys().max().map_or(0, |max_index| max_index + 1);
                        Ok(Description::Insert(InsertStatement {
                            table_id: FullTableId::from(table_def.full_table_id()),
                            column_types,
                            param_count,
                            param_types,
                        }))
                    }
                    Err(NotFoundError::Object) => Err(DescriptionError::table_does_not_exist(&full_table_name)),
                    Err(NotFoundError::Schema) => {
                        Err(DescriptionError::schema_does_not_exist(full_table_name.schema()))
                    }
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
                            TableFactor::Table { name, .. } => match FullTableName::try_from(name) {
                                Ok(full_table_name) => {
                                    match self.metadata.table_exists_tuple((&full_table_name).into()) {
                                        None => Err(DescriptionError::schema_does_not_exist(full_table_name.schema())),
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
                            _ => {
                                // Err(DescriptionError::feature_not_supported(&*query))
                                unimplemented!()
                            }
                        }
                    }
                    _ => unimplemented!(),
                }
            }
            Statement::CreateTable { name, columns, .. } => match FullTableName::try_from(name) {
                Ok(full_table_name) => {
                    let (schema_name, table_name) = (&full_table_name).into();
                    match self.metadata.table_exists(schema_name, table_name) {
                        Some((_, Some(_))) => Err(DescriptionError::table_already_exists(&full_table_name)),
                        None => Err(DescriptionError::schema_does_not_exist(full_table_name.schema())),
                        Some((schema_id, None)) => {
                            let mut column_defs = Vec::new();
                            for column in columns {
                                match SqlType::try_from(&column.data_type) {
                                    Ok(sql_type) => column_defs.push(ColumnDesc {
                                        name: column.name.value.as_str().to_owned(),
                                        pg_type: (&sql_type).into(),
                                    }),
                                    Err(error) => {
                                        return Err(DescriptionError::feature_not_supported(&error));
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
                        match FullTableName::try_from(name) {
                            Ok(full_table_name) => match self.metadata.table_exists_tuple((&full_table_name).into()) {
                                None => return Err(DescriptionError::schema_does_not_exist(full_table_name.schema())),
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

#[cfg(test)]
mod tests;

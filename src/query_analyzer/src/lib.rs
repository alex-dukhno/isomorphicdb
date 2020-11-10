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
    InsertStatement, SchemaCreationInfo, SchemaId, SchemaName, TableCreationInfo,
};
use metadata::{DataDefinition, MetadataView};
use sql_model::{sql_errors::NotFoundError, sql_types::SqlType};
use sqlparser::ast::{ObjectType, Statement};
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
            Statement::Insert { table_name, .. } => match FullTableName::try_from(table_name) {
                Ok(full_table_name) => match self.metadata.table_desc((&full_table_name).into()) {
                    Ok(table_def) => Ok(Description::Insert(InsertStatement {
                        table_id: FullTableId::from(table_def.full_table_id()),
                        sql_types: table_def.column_types(),
                    })),
                    Err(NotFoundError::Object) => Err(DescriptionError::table_does_not_exist(&full_table_name)),
                    Err(NotFoundError::Schema) => {
                        Err(DescriptionError::schema_does_not_exist(full_table_name.schema()))
                    }
                },
                Err(error) => Err(DescriptionError::syntax_error(&error)),
            },
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

#[cfg(test)]
mod tests;

// Copyright 2020 - 2021 Alex Dukhno
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

use catalog::{CatalogHandler, CatalogHandlerOld};
use data_definition_execution_plan::{
    ColumnInfo, CreateIndexQuery, CreateSchemaQuery, CreateTableQuery, DropSchemasQuery, DropTablesQuery, SchemaChange,
};
use definition::{FullTableName, SchemaName};
use query_ast::{ColumnDef, Definition};
use query_response::QueryError;
use storage::{Transaction, TransactionalDatabase};
use types::SqlType;

#[derive(Clone)]
pub struct DefinitionPlanner<'p> {
    catalog: CatalogHandler<'p>,
}

impl<'p> From<Transaction<'p>> for DefinitionPlanner<'p> {
    fn from(transaction: Transaction<'p>) -> DefinitionPlanner {
        DefinitionPlanner {
            catalog: CatalogHandler::from(transaction),
        }
    }
}

impl<'p> DefinitionPlanner<'p> {
    pub fn plan(&self, statement: Definition) -> Result<SchemaChange, SchemaPlanError> {
        match statement {
            Definition::CreateTable {
                schema_name,
                table_name,
                columns,
                if_not_exists,
            } => {
                if !(self.catalog.schema_exists(&SchemaName::from(&schema_name))) {
                    Err(SchemaPlanError::schema_does_not_exist(&schema_name))
                } else {
                    let full_table_name = FullTableName::from((&schema_name, &table_name));
                    let column_defs = columns
                        .into_iter()
                        .map(|ColumnDef { name, data_type }| ColumnInfo {
                            name,
                            sql_type: SqlType::from(data_type),
                        })
                        .collect::<Vec<_>>();
                    Ok(SchemaChange::CreateTable(CreateTableQuery {
                        full_table_name,
                        column_defs,
                        if_not_exists,
                    }))
                }
            }
            Definition::CreateSchema {
                schema_name,
                if_not_exists,
            } => Ok(SchemaChange::CreateSchema(CreateSchemaQuery {
                schema_name: SchemaName::from(&schema_name),
                if_not_exists,
            })),
            Definition::CreateIndex {
                name,
                schema_name,
                table_name,
                column_names,
            } => {
                let full_table_name = FullTableName::from((&schema_name, &table_name));
                match self.catalog.table_definition(full_table_name.clone()) {
                    None => Err(SchemaPlanError::schema_does_not_exist(full_table_name.schema())),
                    Some(None) => Err(SchemaPlanError::table_does_not_exist(full_table_name)),
                    Some(Some(table_info)) => {
                        let table_columns = table_info.column_names();
                        for column in column_names.iter() {
                            if !table_columns.contains(&column) {
                                return Err(SchemaPlanError::column_not_found(&column));
                            }
                        }
                        Ok(SchemaChange::CreateIndex(CreateIndexQuery {
                            name,
                            full_table_name,
                            column_names,
                        }))
                    }
                }
            }
            Definition::DropTables {
                names,
                if_exists,
                cascade,
            } => {
                let mut full_table_names = vec![];
                for (schema_name, table_name) in names {
                    let full_table_name = FullTableName::from((&schema_name, &table_name));
                    if self.catalog.schema_exists(&SchemaName::from(&schema_name)) {
                        full_table_names.push(full_table_name)
                    } else {
                        return Err(SchemaPlanError::schema_does_not_exist(&schema_name));
                    }
                }
                Ok(SchemaChange::DropTables(DropTablesQuery {
                    full_table_names,
                    cascade,
                    if_exists,
                }))
            }
            Definition::DropSchemas {
                names,
                cascade,
                if_exists,
            } => {
                let schema_names = names.iter().map(SchemaName::from).collect::<Vec<_>>();
                Ok(SchemaChange::DropSchemas(DropSchemasQuery {
                    schema_names,
                    cascade,
                    if_exists,
                }))
            }
        }
    }
}

pub struct DefinitionPlannerOld<'p> {
    catalog: CatalogHandlerOld<'p>,
}

impl<'p> From<TransactionalDatabase<'p>> for DefinitionPlannerOld<'p> {
    fn from(database: TransactionalDatabase<'p>) -> Self {
        DefinitionPlannerOld {
            catalog: CatalogHandlerOld::from(database),
        }
    }
}

impl<'p> DefinitionPlannerOld<'p> {
    pub fn plan(&self, statement: Definition) -> Result<SchemaChange, SchemaPlanError> {
        match statement {
            Definition::CreateTable {
                schema_name,
                table_name,
                columns,
                if_not_exists,
            } => {
                if !(self.catalog.schema_exists(&SchemaName::from(&schema_name))) {
                    Err(SchemaPlanError::schema_does_not_exist(&schema_name))
                } else {
                    let full_table_name = FullTableName::from((&schema_name, &table_name));
                    let column_defs = columns
                        .into_iter()
                        .map(|ColumnDef { name, data_type }| ColumnInfo {
                            name,
                            sql_type: SqlType::from(data_type),
                        })
                        .collect::<Vec<_>>();
                    Ok(SchemaChange::CreateTable(CreateTableQuery {
                        full_table_name,
                        column_defs,
                        if_not_exists,
                    }))
                }
            }
            Definition::CreateSchema {
                schema_name,
                if_not_exists,
            } => Ok(SchemaChange::CreateSchema(CreateSchemaQuery {
                schema_name: SchemaName::from(&schema_name),
                if_not_exists,
            })),
            Definition::CreateIndex {
                name,
                schema_name,
                table_name,
                column_names,
            } => {
                let full_table_name = FullTableName::from((&schema_name, &table_name));
                match self.catalog.table_definition(full_table_name.clone()) {
                    None => Err(SchemaPlanError::schema_does_not_exist(full_table_name.schema())),
                    Some(None) => Err(SchemaPlanError::table_does_not_exist(full_table_name)),
                    Some(Some(table_info)) => {
                        let table_columns = table_info.column_names();
                        for column in column_names.iter() {
                            if !table_columns.contains(&column) {
                                return Err(SchemaPlanError::column_not_found(&column));
                            }
                        }
                        Ok(SchemaChange::CreateIndex(CreateIndexQuery {
                            name,
                            full_table_name,
                            column_names,
                        }))
                    }
                }
            }
            Definition::DropTables {
                names,
                if_exists,
                cascade,
            } => {
                let mut full_table_names = vec![];
                for (schema_name, table_name) in names {
                    let full_table_name = FullTableName::from((&schema_name, &table_name));
                    if self.catalog.schema_exists(&SchemaName::from(&schema_name)) {
                        full_table_names.push(full_table_name)
                    } else {
                        return Err(SchemaPlanError::schema_does_not_exist(&schema_name));
                    }
                }
                Ok(SchemaChange::DropTables(DropTablesQuery {
                    full_table_names,
                    cascade,
                    if_exists,
                }))
            }
            Definition::DropSchemas {
                names,
                cascade,
                if_exists,
            } => {
                let schema_names = names.iter().map(SchemaName::from).collect::<Vec<_>>();
                Ok(SchemaChange::DropSchemas(DropSchemasQuery {
                    schema_names,
                    cascade,
                    if_exists,
                }))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum SchemaPlanError {
    SchemaDoesNotExist(String),
    TableDoesNotExist(String),
    ColumnNotFound(String),
}

impl SchemaPlanError {
    pub fn schema_does_not_exist<S: ToString>(schema_name: S) -> SchemaPlanError {
        SchemaPlanError::SchemaDoesNotExist(schema_name.to_string())
    }

    pub fn table_does_not_exist<T: ToString>(table_name: T) -> SchemaPlanError {
        SchemaPlanError::TableDoesNotExist(table_name.to_string())
    }

    pub fn column_not_found<C: ToString>(column_name: C) -> SchemaPlanError {
        SchemaPlanError::ColumnNotFound(column_name.to_string())
    }
}

impl From<SchemaPlanError> for QueryError {
    fn from(error: SchemaPlanError) -> Self {
        match error {
            SchemaPlanError::SchemaDoesNotExist(schema) => QueryError::schema_does_not_exist(schema),
            SchemaPlanError::TableDoesNotExist(table) => QueryError::table_does_not_exist(table),
            SchemaPlanError::ColumnNotFound(column) => QueryError::column_does_not_exist(column),
        }
    }
}

#[cfg(test)]
mod tests;

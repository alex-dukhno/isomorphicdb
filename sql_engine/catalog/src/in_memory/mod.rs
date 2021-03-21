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

use crate::in_memory::data_catalog::InMemoryIndex;
use crate::{
    in_memory::data_catalog::{InMemoryCatalogHandle, InMemoryTableHandle},
    COLUMNS_TABLE, DEFINITION_SCHEMA, INDEXES_TABLE, SCHEMATA_TABLE, TABLES_TABLE,
};
use binary::{repr::Datum, Binary};
use data_definition_execution_plan::{
    CreateIndexQuery, CreateSchemaQuery, CreateTableQuery, DropSchemasQuery, DropTablesQuery, ExecutionError,
    ExecutionOutcome, SchemaChange,
};
use definition::{ColumnDef, FullIndexName, FullTableName, SchemaName, TableDef};
use std::{fmt::Debug, sync::Arc};
use storage_api::Cursor;
use types::{SqlType, SqlTypeFamily};

mod data_catalog;

fn create_public_schema() -> SchemaChange {
    SchemaChange::CreateSchema(CreateSchemaQuery {
        schema_name: SchemaName::from(&"public"),
        if_not_exists: false,
    })
}

pub struct InMemoryDatabase {
    catalog: InMemoryCatalogHandle,
    name: String,
}

impl InMemoryDatabase {
    pub fn new() -> Arc<InMemoryDatabase> {
        Arc::new(InMemoryDatabase::create().bootstrap())
    }

    fn create() -> InMemoryDatabase {
        InMemoryDatabase {
            catalog: InMemoryCatalogHandle::default(),
            name: "IN_MEMORY".to_owned(),
        }
    }

    fn bootstrap(self) -> InMemoryDatabase {
        self.catalog.create_schema(DEFINITION_SCHEMA);
        self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
            schema.create_table(SCHEMATA_TABLE);
            schema.create_table(TABLES_TABLE);
            schema.create_table(COLUMNS_TABLE);
        });
        let public_schema = self.execute(create_public_schema());
        debug_assert!(
            matches!(public_schema, Ok(_)),
            "Default `public` schema has to be created, but failed due to {:?}",
            public_schema
        );
        self
    }

    fn schema_name_record(&self, schema_name: &SchemaName) -> Binary {
        Binary::pack(&[
            Datum::from_string(self.name.clone()),
            Datum::from_string(schema_name.as_ref().to_owned()),
        ])
    }

    fn find_in_system_table<P: Fn(Binary) -> bool>(&self, table_name: &str, predicate: P) -> Option<Option<bool>> {
        self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
            schema.work_with(table_name, |table| {
                table.select().map(|(_key, value)| value).any(&predicate)
            })
        })
    }

    fn schema_exists_inner(&self, schema_name: &SchemaName) -> bool {
        let schema_name_record = self.schema_name_record(schema_name);
        self.find_in_system_table(SCHEMATA_TABLE, |value| value == schema_name_record) == Some(Some(true))
    }

    fn table_name_record(&self, full_table_name: &FullTableName) -> Binary {
        Binary::pack(&[
            Datum::from_string(self.name.clone()),
            Datum::from_string((&full_table_name).schema().to_owned()),
            Datum::from_string((&full_table_name).table().to_owned()),
        ])
    }

    fn table_exists(&self, full_table_name: &FullTableName) -> bool {
        let table_name_record = self.table_name_record(full_table_name);
        self.find_in_system_table(TABLES_TABLE, |value| value == table_name_record) == Some(Some(true))
    }

    fn index_name_record(&self, full_index_name: &FullIndexName) -> Binary {
        Binary::pack(&[
            Datum::from_string(self.name.clone()),
            Datum::from_string((&full_index_name.table()).schema().to_owned()),
            Datum::from_string((&full_index_name.table()).table().to_owned()),
            Datum::from_string(full_index_name.index().to_owned()),
        ])
    }

    #[allow(dead_code)]
    fn index_exists(&self, full_index_name: &FullIndexName) -> bool {
        let index_name_record = self.index_name_record(full_index_name);
        self.find_in_system_table(INDEXES_TABLE, |value| value.starts_with(&index_name_record)) == Some(Some(true))
    }

    fn table_columns(&self, full_table_name: &FullTableName) -> Vec<ColumnDef> {
        let table_name_record = self.table_name_record(full_table_name);
        self.catalog
            .work_with(DEFINITION_SCHEMA, |schema| {
                schema.work_with(COLUMNS_TABLE, |table| {
                    table
                        .select()
                        .filter(|(_key, value)| value.starts_with(&table_name_record))
                        .map(|(_key, value)| {
                            let row = value.unpack();
                            let name = row[3].as_string();
                            let sql_type = SqlType::from_type_id(row[4].as_u64(), row[5].as_u64());
                            let ord_num = row[6].as_u64() as usize;
                            ColumnDef::new(name, sql_type, ord_num)
                        })
                        .collect()
                })
            })
            .unwrap()
            .unwrap()
    }

    pub fn table_definition(&self, full_table_name: FullTableName) -> Option<Option<TableDef>> {
        if !self.schema_exists_inner(&SchemaName::from(&full_table_name.schema())) {
            None
        } else if !self.table_exists(&full_table_name) {
            Some(None)
        } else {
            let column_info = self.table_columns(&full_table_name);
            Some(Some(TableDef::new(full_table_name, column_info)))
        }
    }

    pub fn schema_exists(&self, schema_name: &SchemaName) -> bool {
        self.schema_exists_inner(schema_name)
    }

    pub fn execute(&self, schema_change: SchemaChange) -> Result<ExecutionOutcome, ExecutionError> {
        match schema_change {
            SchemaChange::CreateSchema(CreateSchemaQuery {
                schema_name,
                if_not_exists,
            }) => {
                if self.schema_exists_inner(&SchemaName::from(&schema_name.as_ref())) {
                    if if_not_exists {
                        Ok(ExecutionOutcome::SchemaCreated)
                    } else {
                        Err(ExecutionError::SchemaAlreadyExists(schema_name.as_ref().to_owned()))
                    }
                } else {
                    self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                        schema.work_with(SCHEMATA_TABLE, |table| {
                            table.insert(vec![Binary::pack(&[
                                Datum::from_string("IN_MEMORY".to_owned()),
                                Datum::from_string(schema_name.as_ref().to_owned()),
                            ])])
                        })
                    });
                    self.catalog.create_schema(schema_name.as_ref());
                    Ok(ExecutionOutcome::SchemaCreated)
                }
            }
            SchemaChange::DropSchemas(DropSchemasQuery {
                schema_names,
                cascade,
                if_exists,
            }) => {
                for schema_name in schema_names {
                    if self.schema_exists_inner(&SchemaName::from(&schema_name.as_ref())) {
                        if !cascade
                            && self.catalog.work_with(schema_name.as_ref(), |schema| schema.empty()) == Some(false)
                        {
                            return Err(ExecutionError::SchemaHasDependentObjects(
                                schema_name.as_ref().to_owned(),
                            ));
                        }
                        let full_schema_name = Binary::pack(&[
                            Datum::from_string("IN_MEMORY".to_owned()),
                            Datum::from_string(schema_name.as_ref().to_owned()),
                        ]);

                        self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                            schema.work_with(COLUMNS_TABLE, |table| {
                                let columns_ids = table
                                    .select()
                                    .filter(|(_key, value)| value.starts_with(&full_schema_name))
                                    .map(|(key, _value)| key)
                                    .collect();
                                log::debug!("column IDs {:?}", columns_ids);
                                table.delete(columns_ids);
                            });
                            schema.work_with(TABLES_TABLE, |table| {
                                let keys = table
                                    .select()
                                    .filter(|(_key, value)| value.starts_with(&full_schema_name))
                                    .map(|(key, _value)| key)
                                    .collect();
                                table.delete(keys);
                            });
                            schema.work_with(SCHEMATA_TABLE, |table| {
                                let schema_id = table
                                    .select()
                                    .find(|(_key, value)| value == &full_schema_name)
                                    .map(|(key, _value)| key);
                                debug_assert!(
                                    matches!(schema_id, Some(_)),
                                    "record for {:?} schema had to be found in {:?} system table",
                                    schema_name,
                                    SCHEMATA_TABLE
                                );
                                let schema_id = schema_id.unwrap();
                                table.delete(vec![schema_id]);
                            });
                        });
                        self.catalog.drop_schema(schema_name.as_ref());
                    } else if !if_exists {
                        return Err(ExecutionError::SchemaDoesNotExist(schema_name.as_ref().to_owned()));
                    }
                }
                Ok(ExecutionOutcome::SchemaDropped)
            }
            SchemaChange::CreateTable(CreateTableQuery {
                full_table_name,
                column_defs,
                if_not_exists,
            }) => {
                if self.schema_exists_inner(&SchemaName::from(&full_table_name.schema())) {
                    if self.table_exists(&full_table_name) {
                        if if_not_exists {
                            Ok(ExecutionOutcome::TableCreated)
                        } else {
                            Err(ExecutionError::TableAlreadyExists(
                                full_table_name.schema().to_owned(),
                                full_table_name.table().to_owned(),
                            ))
                        }
                    } else {
                        let full_table_name_record = Binary::pack(&[
                            Datum::from_string("IN_MEMORY".to_owned()),
                            Datum::from_string(full_table_name.schema().to_owned()),
                            Datum::from_string(full_table_name.table().to_owned()),
                        ]);
                        self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                            schema.work_with(TABLES_TABLE, |table| {
                                table.insert(vec![Binary::pack(&[
                                    Datum::from_string("IN_MEMORY".to_owned()),
                                    Datum::from_string(full_table_name.schema().to_owned()),
                                    Datum::from_string(full_table_name.table().to_owned()),
                                ])]);
                                let table_id = table
                                    .select()
                                    .find(|(_key, value)| value == &full_table_name_record)
                                    .map(|(key, _value)| key);
                                log::debug!("GENERATED TABLE ID - {:?}", table_id);
                            });
                            schema.work_with(COLUMNS_TABLE, |table| {
                                table.insert(
                                    column_defs
                                        .iter()
                                        .enumerate()
                                        .map(|(index, def)| {
                                            Binary::pack(&[
                                                Datum::from_string("IN_MEMORY".to_owned()),
                                                Datum::from_string(full_table_name.schema().to_owned()),
                                                Datum::from_string(full_table_name.table().to_owned()),
                                                Datum::from_string(def.name.clone()),
                                                Datum::from_u64(def.sql_type.type_id()),
                                                Datum::from_optional_u64(def.sql_type.chars_len()),
                                                Datum::from_u64(index as u64),
                                            ])
                                        })
                                        .collect(),
                                );
                            })
                        });
                        self.catalog.work_with(full_table_name.schema(), |schema| {
                            schema.create_table(full_table_name.table())
                        });
                        Ok(ExecutionOutcome::TableCreated)
                    }
                } else {
                    Err(ExecutionError::SchemaDoesNotExist(full_table_name.schema().to_owned()))
                }
            }
            // cascade does not make sense for now, but when `FOREIGN KEY`s will be introduce it will become relevant
            SchemaChange::DropTables(DropTablesQuery {
                full_table_names,
                cascade: _cascade,
                if_exists,
            }) => {
                for full_table_name in full_table_names {
                    if self.schema_exists_inner(&SchemaName::from(&full_table_name.schema())) {
                        if self.table_exists(&full_table_name) {
                            self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                                schema.work_with(TABLES_TABLE, |table| {
                                    let table_ids = table
                                        .select()
                                        .map(|(key, value)| (key, value.unpack()))
                                        .filter(|(_key, value)| {
                                            value[1].as_string() == full_table_name.schema()
                                                && value[2].as_string() == full_table_name.table()
                                        })
                                        .map(|(key, _value)| key)
                                        .collect();
                                    log::debug!("table IDs {:?}", table_ids);
                                    table.delete(table_ids);
                                });
                                schema.work_with(COLUMNS_TABLE, |table| {
                                    let columns_ids = table
                                        .select()
                                        .map(|(key, value)| (key, value.unpack()))
                                        .filter(|(_key, value)| {
                                            value[1].as_string() == full_table_name.schema()
                                                && value[2].as_string() == full_table_name.table()
                                        })
                                        .map(|(key, _value)| key)
                                        .collect();
                                    log::debug!("column IDs {:?}", columns_ids);
                                    table.delete(columns_ids);
                                });
                            });
                            self.catalog.work_with(full_table_name.schema(), |schema| {
                                schema.drop_table(full_table_name.table())
                            });
                        } else if !if_exists {
                            return Err(ExecutionError::TableDoesNotExist(
                                full_table_name.schema().to_owned(),
                                full_table_name.table().to_owned(),
                            ));
                        }
                    } else {
                        return Err(ExecutionError::SchemaDoesNotExist(full_table_name.schema().to_owned()));
                    }
                }
                Ok(ExecutionOutcome::TableDropped)
            }
            SchemaChange::CreateIndex(CreateIndexQuery {
                name,
                full_table_name,
                column_names,
            }) => {
                if !self.schema_exists_inner(&SchemaName::from(&full_table_name.schema())) {
                    Err(ExecutionError::SchemaDoesNotExist(full_table_name.schema().to_owned()))
                } else if !self.table_exists(&full_table_name) {
                    Err(ExecutionError::TableDoesNotExist(
                        full_table_name.schema().to_owned(),
                        full_table_name.table().to_owned(),
                    ))
                } else {
                    let table_columns = self.table_columns(&full_table_name);
                    let mut column_indexes = vec![];
                    for column_name in column_names.iter() {
                        if let Some(col_def) = table_columns.iter().find(|col| col.has_name(column_name)) {
                            column_indexes.push(col_def.index());
                        } else {
                            return Err(ExecutionError::ColumnNotFound(column_names[0].to_owned()));
                        }
                    }
                    self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                        schema.work_with(INDEXES_TABLE, |table| {
                            table.insert(vec![Binary::pack(&[
                                Datum::from_string("IN_MEMORY".to_owned()),
                                Datum::from_string(full_table_name.schema().to_owned()),
                                Datum::from_string(full_table_name.table().to_owned()),
                                Datum::from_string(name.clone()),
                                Datum::from_string(column_names.join(", ")),
                            ])])
                        })
                    });
                    self.catalog.work_with(full_table_name.schema(), |schema| {
                        schema.create_index(full_table_name.table(), name.as_str(), column_indexes[0])
                    });
                    Ok(ExecutionOutcome::IndexCreated)
                }
            }
        }
    }

    pub fn table(&self, full_table_name: &FullTableName) -> Box<InMemoryTable> {
        Box::new(InMemoryTable::new(
            self.table_columns(full_table_name),
            self.catalog.table(full_table_name),
        ))
    }

    pub fn work_with<R, F: Fn(&InMemoryTable) -> R>(&self, full_table_name: &FullTableName, operation: F) -> R {
        operation(&InMemoryTable::new(
            self.table_columns(full_table_name),
            self.catalog.table(full_table_name),
        ))
    }

    pub fn work_with_index<R, F: Fn(&InMemoryIndex) -> R>(&self, full_index_name: FullIndexName, operation: F) -> R {
        operation(
            &*self
                .catalog
                .table(full_index_name.table())
                .index(full_index_name.index()),
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct InMemoryTable {
    data_table: InMemoryTableHandle,
    columns: Vec<ColumnDef>,
}

impl InMemoryTable {
    fn new(columns: Vec<ColumnDef>, data_table: InMemoryTableHandle) -> InMemoryTable {
        InMemoryTable { columns, data_table }
    }

    pub fn columns(&self) -> Vec<(String, SqlTypeFamily)> {
        self.columns
            .iter()
            .map(|col_def| (col_def.name().to_owned(), col_def.sql_type().family()))
            .collect()
    }

    pub fn columns_short(&self) -> Vec<(String, SqlType)> {
        self.columns
            .iter()
            .map(|col_def| (col_def.name().to_owned(), col_def.sql_type()))
            .collect()
    }

    pub fn write(&self, row: Binary) {
        self.data_table.insert(vec![row]);
    }

    pub fn write_key(&self, key: Binary, row: Option<Binary>) {
        match row {
            None => {
                let result = self.data_table.remove(&key);
                debug_assert!(matches!(result, Some(_)), "nothing were found for {:?} key", key);
            }
            Some(row) => {
                let _result = self.data_table.insert_key(key, row);
            }
        }
    }

    pub fn scan(&self) -> Cursor {
        self.data_table.select()
    }
}

#[cfg(test)]
mod tests;

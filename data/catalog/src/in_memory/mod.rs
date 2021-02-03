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

use std::sync::Arc;
use data_definition_execution_plan::{
    ExecutionError, ExecutionOutcome
};
use data_manipulation_typed_tree::{DynamicTypedItem, DynamicTypedTree, StaticTypedItem, StaticTypedTree, TypedValue};
use data_scalar::ScalarValue;
use definition::{ColumnDef, FullTableName, SchemaName, TableDef};
use types::SqlType;
use crate::{
    binary::Binary,
    in_memory::data_catalog::{InMemoryCatalogHandle, InMemoryTableHandle},
    repr::Datum,
    CatalogDefinition, DataCatalog, DataTable, Database, SchemaHandle, SqlTable, COLUMNS_TABLE, DEFINITION_SCHEMA,
    SCHEMATA_TABLE, TABLES_TABLE,
};
use data_definition_execution_plan::{SchemaChange, CreateSchemaQuery, DropSchemasQuery, CreateTableQuery, DropTablesQuery};

mod data_catalog;

fn create_public_schema() -> SchemaChange {
    SchemaChange::CreateSchema(CreateSchemaQuery {
        schema_name: SchemaName::from(&"public"),
        if_not_exists: false,
    })
}

pub struct InMemoryDatabase {
    catalog: InMemoryCatalogHandle,
}

impl InMemoryDatabase {
    pub fn new() -> Arc<InMemoryDatabase> {
        Arc::new(InMemoryDatabase::create().bootstrap())
    }

    fn create() -> InMemoryDatabase {
        InMemoryDatabase {
            catalog: InMemoryCatalogHandle::default(),
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

    fn schema_exists(&self, schema_name: &str) -> bool {
        let full_schema_name = Binary::pack(&[
            Datum::from_string("IN_MEMORY".to_owned()),
            Datum::from_string(schema_name.to_owned()),
        ]);
        log::debug!("RECORD - {:?}", full_schema_name);
        let schema = self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
            schema.work_with(SCHEMATA_TABLE, |table| {
                table.select().any(|(_key, value)| value == full_schema_name)
            })
        });
        schema == Some(Some(true))
    }

    fn table_exists(&self, full_table_name: &FullTableName) -> bool {
        let full_table_name = Binary::pack(&[
            Datum::from_string("IN_MEMORY".to_owned()),
            Datum::from_string((&full_table_name).schema().to_owned()),
            Datum::from_string((&full_table_name).table().to_owned()),
        ]);
        let table = self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
            schema.work_with(TABLES_TABLE, |table| {
                table.select().any(|(_key, value)| value == full_table_name)
            })
        });
        table == Some(Some(true))
    }

    fn table_columns(&self, full_table_name: &FullTableName) -> Vec<ColumnDef> {
        let full_table_name = Binary::pack(&[
            Datum::from_string("IN_MEMORY".to_owned()),
            Datum::from_string((&full_table_name).schema().to_owned()),
            Datum::from_string((&full_table_name).table().to_owned()),
        ]);
        self.catalog
            .work_with(DEFINITION_SCHEMA, |schema| {
                schema.work_with(COLUMNS_TABLE, |table| {
                    table
                        .select()
                        .filter(|(_key, value)| value.starts_with(&full_table_name))
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
}

impl CatalogDefinition for InMemoryDatabase {
    fn table_definition(&self, full_table_name: &FullTableName) -> Option<Option<TableDef>> {
        if !(self.schema_exists(full_table_name.schema())) {
            return None;
        }
        if !(self.table_exists(full_table_name)) {
            return Some(None);
        }
        let column_info = self.table_columns(full_table_name);
        Some(Some(TableDef::new(full_table_name, column_info)))
    }

    fn schema_exists(&self, schema_name: &SchemaName) -> bool {
        self.schema_exists(schema_name.as_ref())
    }
}

impl Database for InMemoryDatabase {
    type Table = InMemoryTable;

    fn execute(&self, schema_change: SchemaChange) -> Result<ExecutionOutcome, ExecutionError> {
        match schema_change {
            SchemaChange::CreateSchema(CreateSchemaQuery {
                schema_name, if_not_exists
            }) => {
                if self.schema_exists(schema_name.as_ref()) {
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
            SchemaChange::DropSchemas(DropSchemasQuery { schema_names, cascade, if_exists }) => {
                for schema_name in schema_names {
                    if self.schema_exists(schema_name.as_ref()) {
                        if !cascade && self.catalog.work_with(schema_name.as_ref(), |schema| schema.empty()) == Some(false) {
                            return Err(ExecutionError::SchemaHasDependentObjects(schema_name.as_ref().to_owned()));
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
                                let keys = table.select().filter(|(_key, value)| value.starts_with(&full_schema_name)).map(|(key, _value)| key).collect();
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
                        return Err(ExecutionError::SchemaDoesNotExist(schema_name.as_ref().to_owned()))
                    }
                }
                Ok(ExecutionOutcome::SchemaDropped)
            },
            SchemaChange::CreateTable(CreateTableQuery { full_table_name, column_defs, if_not_exists }) => {
                if self.schema_exists(full_table_name.schema()) {
                    if self.table_exists(&full_table_name) {
                        if if_not_exists {
                            Ok(ExecutionOutcome::TableCreated)
                        } else {
                            Err(ExecutionError::TableAlreadyExists(full_table_name.schema().to_owned(), full_table_name.table().to_owned()))
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
                                table.insert(column_defs.iter().enumerate().map(|(index, def)| {
                                    Binary::pack(&[
                                        Datum::from_string("IN_MEMORY".to_owned()),
                                        Datum::from_string(full_table_name.schema().to_owned()),
                                        Datum::from_string(full_table_name.table().to_owned()),
                                        Datum::from_string(def.name.clone()),
                                        Datum::from_u64(def.sql_type.type_id()),
                                        Datum::from_optional_u64(def.sql_type.chars_len()),
                                        Datum::from_u64(index as u64),
                                    ])
                                }).collect());
                            })
                        });
                        self.catalog.work_with(full_table_name.schema(), |schema| schema.create_table(full_table_name.table()));
                        Ok(ExecutionOutcome::TableCreated)
                    }
                } else {
                    Err(ExecutionError::SchemaDoesNotExist(full_table_name.schema().to_owned()))
                }
            },
            // cascade does not make sense for now, but when `FOREIGN KEY`s will be introduce it will become relevant
            SchemaChange::DropTables(DropTablesQuery { full_table_names, cascade: _cascade, if_exists }) => {
                for full_table_name in full_table_names {
                    if self.schema_exists(full_table_name.schema()) {
                        if self.table_exists(&full_table_name) {
                            self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                                schema.work_with(TABLES_TABLE, |table| {
                                    let table_ids = table
                                        .select()
                                        .map(|(key, value)| (key, value.unpack()))
                                        .filter(|(_key, value)| {
                                            &value[1].as_string() == full_table_name.schema()
                                                && &value[2].as_string() == full_table_name.table()
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
                                            &value[1].as_string() == full_table_name.schema()
                                                && &value[2].as_string() == full_table_name.table()
                                        })
                                        .map(|(key, _value)| key)
                                        .collect();
                                    log::debug!("column IDs {:?}", columns_ids);
                                    table.delete(columns_ids);
                                });
                            });
                            self.catalog.work_with(full_table_name.schema(), |schema| schema.drop_table(full_table_name.table()));
                        } else if !if_exists {
                            return Err(ExecutionError::TableDoesNotExist(full_table_name.schema().to_owned(), full_table_name.table().to_owned()))
                        }
                    } else {
                        return Err(ExecutionError::SchemaDoesNotExist(full_table_name.schema().to_owned()))
                    }
                }
                Ok(ExecutionOutcome::TableDropped)
            },
        }
    }

    fn work_with<R, F: Fn(&Self::Table) -> R>(&self, full_table_name: &FullTableName, operation: F) -> R {
        operation(&InMemoryTable::new(
            self.table_columns(full_table_name),
            self.catalog.table(full_table_name),
        ))
    }
}

pub struct InMemoryTable {
    data_table: InMemoryTableHandle,
    columns: Vec<ColumnDef>,
}

impl InMemoryTable {
    fn new(columns: Vec<ColumnDef>, data_table: InMemoryTableHandle) -> InMemoryTable {
        InMemoryTable { columns, data_table }
    }

    fn eval_static(&self, tree: &StaticTypedTree) -> Datum {
        match tree {
            StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(value))) => Datum::from_i16(*value),
            StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Integer(value))) => Datum::from_i32(*value),
            StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::BigInt(value))) => Datum::from_i64(*value),
            StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(value))) => Datum::from_bool(*value),
            StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(string))) => {
                Datum::from_string(string.clone())
            }
            StaticTypedTree::Item(_) => unimplemented!(),
            StaticTypedTree::Operation { .. } => unimplemented!(),
        }
    }

    fn eval_dynamic(&self, tree: &DynamicTypedTree) -> Datum {
        match tree {
            DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::SmallInt(value))) => Datum::from_i16(*value),
            DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::Integer(value))) => Datum::from_i32(*value),
            DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::BigInt(value))) => Datum::from_i64(*value),
            DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::Bool(value))) => Datum::from_bool(*value),
            DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::String(string))) => {
                Datum::from_string(string.clone())
            }
            DynamicTypedTree::Item(_) => unimplemented!(),
            DynamicTypedTree::Operation { .. } => unimplemented!(),
        }
    }

    fn has_column(&self, column_name: &str) -> Option<(usize, &ColumnDef)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_index, col)| col.has_name(column_name))
    }
}

impl SqlTable for InMemoryTable {
    fn insert(&self, rows: &[Vec<Option<StaticTypedTree>>]) -> usize {
        self.data_table.insert(
            rows.iter()
                .map(|row| {
                    log::debug!("ROW to INSERT {:#?}", row);
                    let mut to_insert = vec![];
                    for v in row {
                        to_insert.push(
                            v.as_ref()
                                .map(|v| self.eval_static(&v))
                                .unwrap_or_else(Datum::from_null),
                        );
                    }
                    Binary::pack(&to_insert)
                })
                .collect::<Vec<Binary>>(),
        )
    }

    fn select(
        &self,
        column_names: Vec<String>,
    ) -> Result<(Vec<ColumnDef>, Vec<Vec<ScalarValue>>), String> {
        let mut columns = vec![];
        let mut indexes = vec![];
        for name in column_names {
            match self.has_column(&name) {
                None => return Err(name),
                Some((index, col)) => {
                    columns.push(col.clone());
                    indexes.push(index);
                }
            }
        }
        Ok((
            columns,
            self.data_table
                .select()
                .map(|(_key, value)| {
                    let row = value.unpack();
                    let mut data = vec![];
                    for index in &indexes {
                        let value = match &row[*index] {
                            Datum::Null => ScalarValue::Null,
                            Datum::True => ScalarValue::True,
                            Datum::False => ScalarValue::False,
                            Datum::Int16(v) => ScalarValue::Int16(*v),
                            Datum::Int32(v) => ScalarValue::Int32(*v),
                            Datum::Int64(v) => ScalarValue::Int64(*v),
                            Datum::Float32(v) => ScalarValue::Float32(*v),
                            Datum::Float64(v) => ScalarValue::Float64(*v),
                            Datum::String(v) => ScalarValue::String(v.clone()),
                        };
                        data.push(value);
                    }
                    data
                })
                .collect(),
        ))
    }

    fn delete_all(&self) -> usize {
        let keys = self.data_table.select().map(|(key, _value)| key).collect();
        self.data_table.delete(keys)
    }

    fn update(&self, column_names: Vec<String>, assignments: Vec<DynamicTypedTree>) -> usize {
        let delta = self
            .data_table
            .select()
            .map(|(key, value)| {
                let mut unpacked_row = value.unpack();
                for (column_name, assignment) in column_names.iter().zip(assignments.iter()) {
                    let new_value = match self.has_column(column_name) {
                        None => unimplemented!(),
                        Some((index, _)) => (index, self.eval_dynamic(assignment)),
                    };
                    unpacked_row[new_value.0] = new_value.1;
                }
                (key, Binary::pack(&unpacked_row))
            })
            .collect();
        self.data_table.update(delta)
    }
}

#[cfg(test)]
mod tests;

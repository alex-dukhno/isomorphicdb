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
    binary::Binary,
    in_memory::data_catalog::{InMemoryCatalogHandle, InMemoryTableHandle},
    repr::Datum,
    CatalogDefinition, DataCatalog, DataTable, Database, SchemaHandle, SqlTable, COLUMNS_TABLE, DEFINITION_SCHEMA,
    INDEXES_TABLE, SCHEMATA_TABLE, TABLES_TABLE,
};
use bigdecimal::ToPrimitive;
use data_definition_execution_plan::{
    CreateIndexQuery, CreateSchemaQuery, CreateTableQuery, DropSchemasQuery, DropTablesQuery, ExecutionError,
    ExecutionOutcome, SchemaChange,
};
use data_manipulation_operators::{UnArithmetic, UnOperation, UnLogical};
use data_manipulation_typed_tree::{DynamicTypedItem, DynamicTypedTree, StaticTypedItem, StaticTypedTree, TypedValue};
use data_scalar::ScalarValue;
use definition::{ColumnDef, FullIndexName, FullTableName, SchemaName, TableDef};
use std::sync::Arc;
use types::{Num, SqlType, SqlTypeFamily};
use data_manipulation_query_result::QueryExecutionError;

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

    fn find_in_system_table<P: Fn((Binary, Binary)) -> bool>(
        &self,
        table_name: &str,
        predicate: P,
    ) -> Option<Option<bool>> {
        self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
            schema.work_with(table_name, |table| table.select().any(&predicate))
        })
    }

    fn schema_exists(&self, schema_name: &SchemaName) -> bool {
        let schema_name_record = self.schema_name_record(schema_name);
        self.find_in_system_table(SCHEMATA_TABLE, |(_key, value)| value == schema_name_record) == Some(Some(true))
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
        self.find_in_system_table(TABLES_TABLE, |(_key, value)| value == table_name_record) == Some(Some(true))
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
        self.find_in_system_table(INDEXES_TABLE, |(_key, value)| value.starts_with(&index_name_record))
            == Some(Some(true))
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
}

impl CatalogDefinition for InMemoryDatabase {
    fn table_definition(&self, full_table_name: FullTableName) -> Option<Option<TableDef>> {
        if !self.schema_exists(&SchemaName::from(&full_table_name.schema())) {
            None
        } else if !self.table_exists(&full_table_name) {
            Some(None)
        } else {
            let column_info = self.table_columns(&full_table_name);
            Some(Some(TableDef::new(full_table_name, column_info)))
        }
    }

    fn schema_exists(&self, schema_name: &SchemaName) -> bool {
        self.schema_exists(schema_name)
    }
}

impl Database for InMemoryDatabase {
    type Table = InMemoryTable;
    type Index = data_catalog::InMemoryIndex;

    fn execute(&self, schema_change: SchemaChange) -> Result<ExecutionOutcome, ExecutionError> {
        match schema_change {
            SchemaChange::CreateSchema(CreateSchemaQuery {
                schema_name,
                if_not_exists,
            }) => {
                if self.schema_exists(&SchemaName::from(&schema_name.as_ref())) {
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
                    if self.schema_exists(&SchemaName::from(&schema_name.as_ref())) {
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
                if self.schema_exists(&SchemaName::from(&full_table_name.schema())) {
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
                    if self.schema_exists(&SchemaName::from(&full_table_name.schema())) {
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
                if !self.schema_exists(&SchemaName::from(&full_table_name.schema())) {
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

    fn work_with<R, F: Fn(&Self::Table) -> R>(&self, full_table_name: &FullTableName, operation: F) -> R {
        operation(&InMemoryTable::new(
            self.table_columns(full_table_name),
            self.catalog.table(full_table_name),
        ))
    }

    fn work_with_index<R, F: Fn(&Self::Index) -> R>(&self, full_index_name: FullIndexName, operation: F) -> R {
        operation(
            &*self
                .catalog
                .table(full_index_name.table())
                .index(full_index_name.index()),
        )
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

    fn eval_dynamic(&self, tree: &DynamicTypedTree) -> Datum {
        match tree {
            DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::Num { value, type_family })) => {
                match type_family {
                    SqlTypeFamily::Bool => unimplemented!(),
                    SqlTypeFamily::String => unimplemented!(),
                    SqlTypeFamily::SmallInt => Datum::from_i16(value.to_i16().unwrap()),
                    SqlTypeFamily::Integer => unimplemented!(),
                    SqlTypeFamily::BigInt => unimplemented!(),
                    SqlTypeFamily::Real => unimplemented!(),
                    SqlTypeFamily::Double => unimplemented!(),
                }
            }
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

fn convert(sql_type: SqlType, value: TypedValue) -> Datum {
    log::debug!("type {:?} value {:?}", sql_type, value);
    match (sql_type, value) {
        (
            SqlType::Num(Num::SmallInt),
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::SmallInt,
            },
        ) => Datum::from_i16(value.to_i16().unwrap()),
        (
            SqlType::Num(Num::Integer),
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::Integer,
            },
        ) => Datum::from_i32(value.to_i32().unwrap()),
        (
            SqlType::Num(Num::BigInt),
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::BigInt,
            },
        ) => Datum::from_i64(value.to_i64().unwrap()),
        (SqlType::Bool, TypedValue::Bool(value)) => Datum::from_bool(value),
        (SqlType::Str { .. }, TypedValue::String(value)) => Datum::from_string(value),
        _ => unimplemented!(),
    }
}

impl SqlTable for InMemoryTable {
    fn insert(&self, rows: &[Vec<Option<StaticTypedTree>>]) -> Result<usize, QueryExecutionError> {
        let mut values = vec![];
        for row in rows {
            log::debug!("ROW to INSERT {:#?}", row);
            let mut to_insert = vec![];
            for (index, value) in row.into_iter().enumerate() {
                let datum = match value {
                    None => Datum::from_null(),
                    Some(v) => {
                        let value = v.eval()?;
                        log::debug!("value {:?}", value);
                        convert(self.columns[index].sql_type(), value)
                    }
                };
                to_insert.push(datum);
            }
            values.push(Binary::pack(&to_insert))
        }
        let inserted = values.len();
        let record_ids = self.data_table.insert(values.clone());
        let indexes = self.data_table.indexes();
        for index in indexes {
            for i in 0..inserted {
                let mut val = vec![];
                let values_i = values[i].unpack();
                for (j, value_i) in values_i.iter().enumerate() {
                    if index.over(j) {
                        val.push(value_i.clone());
                    }
                }
                index.insert(Binary::pack(&val), record_ids[i].clone());
            }
        }
        Ok(inserted)
    }

    fn select(&self, column_names: Vec<String>) -> Result<(Vec<ColumnDef>, Vec<Vec<ScalarValue>>), String> {
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

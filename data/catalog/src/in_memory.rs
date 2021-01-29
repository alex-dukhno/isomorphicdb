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

mod data_catalog;

use crate::{
    in_memory::data_catalog::{InMemoryCatalogHandle, InMemoryTableHandle},
    CatalogDefinition, DataCatalog, DataTable, Database, SchemaHandle, SqlTable, COLUMNS_TABLE, DEFINITION_SCHEMA,
    SCHEMATA_TABLE, TABLES_TABLE,
};
use binary::Binary;
use data_definition_operations::{
    ExecutionError, ExecutionOutcome, Kind, ObjectState, Record, Step, SystemObject, SystemOperation,
};
use data_manipulation_typed_tree::{StaticTypedItem, StaticTypedTree, TypedValue};
use definition::{ColumnDef, FullTableName, SchemaName, TableDef};
use repr::Datum;
use std::sync::Arc;
use types::SqlType;
use std::collections::HashMap;

fn create_public_schema() -> SystemOperation {
    SystemOperation {
        kind: Kind::Create(SystemObject::Schema),
        skip_steps_if: None,
        steps: vec![vec![
            Step::CheckExistence {
                system_object: SystemObject::Schema,
                object_name: vec!["public".to_owned()],
            },
            Step::CreateFolder {
                name: "public".to_owned(),
            },
            Step::CreateRecord {
                record: Record::Schema {
                    schema_name: "public".to_owned(),
                },
            },
        ]],
    }
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
        let schema = self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
            schema.work_with(SCHEMATA_TABLE, |table| {
                table.select().any(|(_key, value)| value == full_schema_name)
            })
        });
        schema == Some(Some(true))
    }

    fn table_exists(&self, full_table_name: &FullTableName) -> bool {
        let full_table_name = Binary::pack(&full_table_name.raw(Datum::from_string("IN_MEMORY".to_owned())));
        let table = self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
            schema.work_with(TABLES_TABLE, |table| {
                table.select().any(|(_key, value)| value == full_table_name)
            })
        });
        table == Some(Some(true))
    }

    fn table_columns(&self, full_table_name: &FullTableName) -> Vec<ColumnDef> {
        let full_table_name = Binary::pack(&full_table_name.raw(Datum::from_string("IN_MEMORY".to_owned())));
        self.catalog
            .work_with(DEFINITION_SCHEMA, |schema| {
                schema.work_with(COLUMNS_TABLE, |table| {
                    table
                        .select()
                        .filter(|(_key, value)| value.start_with(&full_table_name))
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

    fn execute(&self, operation: SystemOperation) -> Result<ExecutionOutcome, ExecutionError> {
        let SystemOperation {
            kind,
            skip_steps_if,
            steps,
        } = operation;
        let end = steps.len();
        let mut index = 0;
        while index < end {
            let operations = &steps[index];
            index += 1;
            for operation in operations {
                println!("{:?}", operation);
                match operation {
                    Step::CheckExistence {
                        system_object,
                        object_name,
                    } => match system_object {
                        SystemObject::Schema => {
                            let result = self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                                schema.work_with(SCHEMATA_TABLE, |table| {
                                    table.select().any(|(_key, value)| {
                                        value
                                            == Binary::pack(&[
                                                Datum::from_string("IN_MEMORY".to_owned()),
                                                Datum::from_string(object_name[0].to_owned()),
                                            ])
                                    })
                                })
                            });
                            match skip_steps_if {
                                None => {
                                    if let (&Kind::Create(SystemObject::Schema), Some(Some(true))) = (&kind, result) {
                                        return Err(ExecutionError::SchemaAlreadyExists(object_name[0].to_owned()));
                                    }
                                    if let (&Kind::Drop(SystemObject::Schema), Some(Some(false))) = (&kind, result) {
                                        return Err(ExecutionError::SchemaDoesNotExist(object_name[0].to_owned()));
                                    }
                                    if let (&Kind::Create(SystemObject::Table), Some(Some(false))) = (&kind, result) {
                                        return Err(ExecutionError::SchemaDoesNotExist(object_name[0].to_owned()));
                                    }
                                    if let (&Kind::Drop(SystemObject::Table), Some(Some(false))) = (&kind, result) {
                                        return Err(ExecutionError::SchemaDoesNotExist(object_name[0].to_owned()));
                                    }
                                }
                                Some(ObjectState::NotExists) => break,
                                Some(ObjectState::Exists) => {}
                            }
                        }
                        SystemObject::Table => {
                            let result = self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                                schema.work_with(TABLES_TABLE, |table| {
                                    table.select().any(|(_key, value)| {
                                        value
                                            == Binary::pack(&[
                                                Datum::from_string("IN_MEMORY".to_owned()),
                                                Datum::from_string(object_name[0].clone()),
                                                Datum::from_string(object_name[1].clone()),
                                            ])
                                    })
                                })
                            });
                            match skip_steps_if {
                                None => {
                                    if let (&Kind::Create(SystemObject::Table), Some(Some(true))) = (&kind, result) {
                                        return Err(ExecutionError::TableAlreadyExists(
                                            object_name[0].to_owned(),
                                            object_name[1].to_owned(),
                                        ));
                                    }
                                    if let (&Kind::Drop(SystemObject::Table), Some(Some(false))) = (&kind, result) {
                                        return Err(ExecutionError::TableDoesNotExist(
                                            object_name[0].to_owned(),
                                            object_name[1].to_owned(),
                                        ));
                                    }
                                }
                                Some(ObjectState::NotExists) => unimplemented!(),
                                Some(ObjectState::Exists) => break,
                            }
                        }
                    },
                    Step::CheckDependants {
                        system_object,
                        object_name,
                    } => match system_object {
                        SystemObject::Schema => {
                            let result = self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                                let schema_id = Binary::pack(&[
                                    Datum::from_string("IN_MEMORY".to_owned()),
                                    Datum::from_string(object_name[0].clone()),
                                ]);
                                schema.work_with(TABLES_TABLE, |table| {
                                    table.select().any(|(_key, value)| value.start_with(&schema_id))
                                })
                            });

                            if let Some(Some(true)) = result {
                                return Err(ExecutionError::SchemaHasDependentObjects(object_name[0].to_owned()));
                            }
                        }
                        SystemObject::Table => {}
                    },
                    Step::RemoveDependants { .. } => {}
                    Step::RemoveColumns { .. } => {}
                    Step::CreateFolder { name } => {
                        self.catalog.create_schema(&name);
                    }
                    Step::RemoveFolder { name } => {
                        self.catalog.drop_schema(&name);
                        return Ok(ExecutionOutcome::SchemaDropped);
                    }
                    Step::CreateFile { folder_name, name } => {
                        self.catalog.work_with(folder_name, |schema| schema.create_table(name));
                    }
                    Step::RemoveFile { .. } => {}
                    Step::RemoveRecord { record } => match record {
                        Record::Schema { schema_name } => {
                            let full_schema_name = Binary::pack(&[
                                Datum::from_string("IN_MEMORY".to_owned()),
                                Datum::from_string(schema_name.clone()),
                            ]);
                            self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
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
                        }
                        Record::Table {
                            schema_name,
                            table_name,
                        } => {
                            let full_table_name = Binary::pack(&[
                                Datum::from_string("IN_MEMORY".to_owned()),
                                Datum::from_string(schema_name.to_owned()),
                                Datum::from_string(table_name.to_owned()),
                            ]);
                            self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                                schema.work_with(TABLES_TABLE, |table| {
                                    let table_id = table
                                        .select()
                                        .find(|(_key, value)| value == &full_table_name)
                                        .map(|(key, _value)| key);
                                    debug_assert!(
                                        matches!(table_id, Some(_)),
                                        "record for {:?}.{:?} table had to be found in {:?} system table",
                                        schema_name,
                                        table_name,
                                        TABLES_TABLE
                                    );
                                    println!("FOUND TABLE ID - {:?}", table_id);
                                    let table_id = table_id.unwrap();
                                    table.delete(vec![table_id]);
                                    let table_id = table
                                        .select()
                                        .find(|(_key, value)| value == &full_table_name)
                                        .map(|(key, _value)| key);
                                    println!("TABLE ID AFTER DROP - {:?}", table_id);
                                });
                            });
                        }
                        Record::Column { .. } => unimplemented!(),
                    },
                    Step::CreateRecord { record } => match record {
                        Record::Schema { schema_name } => {
                            self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                                schema.work_with(SCHEMATA_TABLE, |table| {
                                    table.insert(vec![Binary::pack(&[
                                        Datum::from_string("IN_MEMORY".to_owned()),
                                        Datum::from_string(schema_name.clone()),
                                    ])])
                                })
                            });
                            return Ok(ExecutionOutcome::SchemaCreated);
                        }
                        Record::Table {
                            schema_name,
                            table_name,
                        } => {
                            let full_table_name = Binary::pack(&[
                                Datum::from_string("IN_MEMORY".to_owned()),
                                Datum::from_string(schema_name.clone()),
                                Datum::from_string(table_name.clone()),
                            ]);
                            self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                                schema.work_with(TABLES_TABLE, |table| {
                                    table.insert(vec![Binary::pack(&[
                                        Datum::from_string("IN_MEMORY".to_owned()),
                                        Datum::from_string(schema_name.clone()),
                                        Datum::from_string(table_name.clone()),
                                    ])]);
                                    let table_id = table
                                        .select()
                                        .find(|(_key, value)| value == &full_table_name)
                                        .map(|(key, _value)| key);
                                    println!("GENERATED TABLE ID - {:?}", table_id);
                                })
                            });
                        }
                        Record::Column {
                            schema_name,
                            table_name,
                            column_name,
                            sql_type,
                        } => {
                            let ord_num = self.catalog.work_with(schema_name, |schema| {
                                schema.work_with(table_name, |table| table.next_column_ord())
                            });
                            debug_assert!(
                                matches!(ord_num, Some(Some(_))),
                                "column ord num has to be generated for {:?}.{:?} but value was {:?}",
                                schema_name,
                                table_name,
                                ord_num
                            );
                            let ord_num = ord_num.unwrap().unwrap();

                            let row = Binary::pack(&[
                                Datum::from_string("IN_MEMORY".to_owned()),
                                Datum::from_string(schema_name.clone()),
                                Datum::from_string(table_name.clone()),
                                Datum::from_string(column_name.clone()),
                                Datum::from_u64(sql_type.type_id()),
                                Datum::from_optional_u64(sql_type.chars_len()),
                                Datum::from_u64(ord_num),
                            ]);

                            self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                                schema.work_with(COLUMNS_TABLE, |table| table.insert(vec![row.clone()]))
                            });
                        }
                    },
                }
            }
        }
        match kind {
            Kind::Create(SystemObject::Schema) => Ok(ExecutionOutcome::SchemaCreated),
            Kind::Drop(SystemObject::Schema) => Ok(ExecutionOutcome::SchemaDropped),
            Kind::Create(SystemObject::Table) => Ok(ExecutionOutcome::TableCreated),
            Kind::Drop(SystemObject::Table) => Ok(ExecutionOutcome::TableDropped),
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

    fn eval(&self, tree: &StaticTypedTree) -> Datum {
        match tree {
            StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(value))) => Datum::from_i16(*value),
            StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Integer(value))) => Datum::from_i32(*value),
            StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::BigInt(value))) => Datum::from_i64(*value),
            StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(value))) => Datum::from_bool(*value),
            StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(string))) => Datum::from_string(string.clone()),
            StaticTypedTree::Item(_) => unimplemented!(),
            StaticTypedTree::Operation { .. } => unimplemented!(),
        }
    }

    fn has_column(&self, column_name: &str) -> Option<(usize, &ColumnDef)> {
        self.columns.iter().enumerate().find(|(_index, col)| col.has_name(column_name))
    }
}

impl SqlTable for InMemoryTable {
    fn insert(&self, rows: &[Vec<Option<StaticTypedTree>>]) -> usize {
        self.data_table.insert(
            rows.iter()
                .map(|row| {
                    let mut to_insert = vec![];
                    for v in row {
                        to_insert.push(v.as_ref().map(|v| self.eval(&v)).unwrap_or(Datum::from_null()));
                    }
                    println!("{:#?}", to_insert);
                    Binary::pack(&to_insert)
                })
                .collect::<Vec<Binary>>(),
        )
    }

    fn insert_with_columns(&self, column_names: Vec<String>, rows: Vec<Vec<Option<StaticTypedTree>>>) -> usize {
        println!("COLUMNS TO INSERT {:?}", column_names);
        let columns_map = column_names.into_iter().enumerate().map(|(index, name)| (name, index)).collect::<HashMap<String, usize>>();
        let data = rows.into_iter().map(|row| {
            let mut value = vec![];
            for name in self.columns.iter().map(ColumnDef::name) {
                value.push(columns_map.get(name).map(|index| row[*index].clone()).unwrap_or(None))
            }
            println!("ROW TO INSERT {:#?}", value);
            value
        }).collect::<Vec<Vec<Option<StaticTypedTree>>>>();
        self.insert(&data)
    }

    fn select(&self) -> (Vec<ColumnDef>, Vec<Vec<Datum>>) {
        (
            self.columns.clone(),
            self.data_table.select().map(|(_key, value)| value.unpack()).collect(),
        )
    }

    fn select_with_columns(&self, column_names: Vec<String>)-> Result<(Vec<ColumnDef>, Vec<Vec<Datum>>), String> {
        let mut columns = vec![];
        let mut indexes = vec![];
        for name in column_names {
            match self.has_column(&name) {
                None => return Err(name),
                Some((index, col)) => {
                    columns.push(col.clone());
                    indexes.push(index);
                },
            }
        }
        Ok((
            columns,
            self.data_table.select().map(|(_key, value)| {
                let row = value.unpack();
                let mut data = vec![];
                for index in &indexes {
                    data.push(row[*index].clone())
                }
                data
            }).collect(),
        ))
    }
}

#[cfg(test)]
mod tests;

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

use binary::{Binary, Key, ReadCursor, Values};
use catalog::{InMemoryCatalogHandle, OnDiskCatalogHandle};
use data_definition::{DataDefOperationExecutor, DataDefReader, OptionalSchemaId, OptionalTableId};
use definition_operations::{Record, Step, SystemObject};
use meta_def::{ColumnDefinition, Id};
use repr::Datum;
use sql_model::{DropSchemaError, DropStrategy};
use std::{
    collections::HashMap,
    fmt,
    fmt::{Display, Formatter},
    ops::Deref,
    path::PathBuf,
    sync::Arc,
};
use storage::{Database, InMemoryDatabase, InitStatus, PersistentDatabase};
use types::SqlType;

pub const DEFAULT_CATALOG: &'_ str = "default_catalog";
const DEFAULT_CATALOG_ID: Datum = Datum::from_u64(0);

pub const DEFINITION_SCHEMA: &'_ str = "DEFINITION_SCHEMA";
/// **SCHEMATA** sql types definition
/// CATALOG_NAME    varchar(255)
/// SCHEMA_NAME     varchar(255)
pub const SCHEMATA_TABLE: &'_ str = "SCHEMATA";
/// **TABLES** sql types definition
/// TABLE_CATALOG   varchar(255)
/// TABLE_SCHEMA    varchar(255)
/// TABLE_NAME      varchar(255)
pub const TABLES_TABLE: &'_ str = "TABLES";
/// **COLUMNS** sql type definition
/// TABLE_CATALOG               varchar(255)
/// TABLE_SCHEMA                varchar(255)
/// TABLE_NAME                  varchar(255)
/// COLUMN_NAME                 varchar(255)
/// ORDINAL_POSITION            integer CHECK (ORDINAL_POSITION > 0)
/// DATA_TYPE_OID               integer
/// CHARACTER_MAXIMUM_LENGTH    integer CHECK (VALUE >= 0),
/// NUMERIC_PRECISION           integer CHECK (VALUE >= 0),
pub const COLUMNS_TABLE: &'_ str = "COLUMNS";

pub struct DatabaseHandle {
    inner: DatabaseHandleInner,
}

enum DatabaseHandleInner {
    InMemory(Arc<InMemoryDatabase>),
    Persistent(Arc<PersistentDatabase>),
}

impl Deref for DatabaseHandleInner {
    type Target = dyn Database;

    fn deref(&self) -> &Self::Target {
        match self {
            DatabaseHandleInner::InMemory(db) => &**db,
            DatabaseHandleInner::Persistent(db) => &**db,
        }
    }
}

impl DatabaseHandle {
    pub fn in_memory() -> DatabaseHandle {
        let database_instance = InMemoryDatabase::default();
        debug_assert!(database_instance
            .create_schema(DEFINITION_SCHEMA)
            .expect("no io error")
            .expect("no platform error"));
        database_instance.bootstrap();
        DatabaseHandle {
            inner: DatabaseHandleInner::InMemory(Arc::new(database_instance)),
        }
    }

    #[allow(clippy::result_unit_err)]
    pub fn persistent(path: PathBuf) -> Result<DatabaseHandle, ()> {
        let database_instance = PersistentDatabase::new(path.join(DEFAULT_CATALOG));
        let catalog_exist = match database_instance.init(DEFINITION_SCHEMA).expect("no io errors") {
            Ok(InitStatus::Loaded) => true,
            Ok(InitStatus::Created) => {
                database_instance.bootstrap();
                false
            }
            Err(_storage_error) => return Err(()),
        };
        if catalog_exist {
            let schema_names = database_instance
                .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                .expect("no io error")
                .expect("no platform error")
                .expect("to have SCHEMATA_TABLE table")
                .map(Result::unwrap)
                .map(Result::unwrap)
                .map(|(record_id, columns)| {
                    let catalog_id = Datum::from_u64(record_id.unpack()[0].as_u64());
                    let schema = columns.unpack()[1].as_str().to_owned();
                    (catalog_id, schema)
                })
                .filter(|(catalog_id, _schema)| catalog_id == &DEFAULT_CATALOG_ID)
                .map(|(_catalog_id, schema)| schema)
                .collect::<Vec<String>>();
            for schema_name in schema_names {
                match database_instance.init(&schema_name) {
                    Ok(Ok(InitStatus::Loaded)) => {}
                    Ok(Ok(InitStatus::Created)) => {
                        log::error!("Schema {:?} should have been already created", schema_name);
                        return Err(());
                    }
                    Ok(Err(error)) => {
                        log::error!("Error during schema {:?} initialization {:?}", schema_name, error);
                        return Err(());
                    }
                    Err(io_error) => {
                        log::error!("IO Error during schema {:?} initialization {:?}", schema_name, io_error);
                        return Err(());
                    }
                }
            }
        }
        Ok(DatabaseHandle {
            inner: DatabaseHandleInner::Persistent(Arc::new(database_instance)),
        })
    }

    pub fn next_key_id(&self, full_table_id: &(Id, Id)) -> Id {
        let (schema_name, table_name) = self
            .inner
            .read(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let ids = record_id.unpack();
                let schema_id = ids[1].as_u64();
                let table_id = ids[2].as_u64();
                let data = columns.unpack();
                let schema_name = data[1].as_str().to_owned();
                let table_name = data[2].as_str().to_owned();
                (schema_id, table_id, schema_name, table_name)
            })
            .find(|(schema_id, table_id, _schema_name, _table_name)| &(*schema_id, *table_id) == full_table_id)
            .map(|(_schema_id, _table_id, schema_name, table_name)| (schema_name, table_name))
            .unwrap();

        self.inner
            .get_sequence(&schema_name, &(table_name + ".records"))
            .unwrap()
            .next()
    }

    #[allow(clippy::result_unit_err)]
    pub fn create_schema(&self, schema_name: &str) -> Result<Id, ()> {
        let schema_id = self
            .inner
            .get_sequence(DEFINITION_SCHEMA, &(SCHEMATA_TABLE.to_owned() + ".records"))
            .unwrap()
            .next();
        self.inner
            .write(
                DEFINITION_SCHEMA,
                SCHEMATA_TABLE,
                vec![(
                    Binary::pack(&[DEFAULT_CATALOG_ID, Datum::from_u64(schema_id)]),
                    Binary::pack(&[Datum::from_str(DEFAULT_CATALOG), Datum::from_str(schema_name)]),
                )],
            )
            .expect("no io error")
            .expect("no platform error")
            .expect("to save schema");
        match self.inner.create_schema(schema_name) {
            Ok(Ok(true)) => Ok(schema_id),
            _ => {
                log::error!(
                    "SQL Engine does not check '{}' existence of SCHEMA before creating one",
                    schema_name
                );
                Err(())
            }
        }
    }

    #[allow(clippy::result_unit_err)]
    pub fn drop_schema(&self, schema_id: &Id, strategy: DropStrategy) -> Result<Result<(), DropSchemaError>, ()> {
        let schema_name = self
            .inner
            .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let id = record_id.unpack()[1].as_u64();
                let schema_name = columns.unpack()[1].as_str().to_owned();
                (id, schema_name)
            })
            .find(|(id, _schema)| id == schema_id)
            .map(|(_id, schema)| schema);
        match schema_name {
            None => Ok(Err(DropSchemaError::DoesNotExist)),
            Some(schema_name) => match self.drop_schema_inner(&schema_name, *schema_id, strategy) {
                Ok(()) => match self.inner.drop_schema(&schema_name) {
                    Ok(Ok(true)) => Ok(Ok(())),
                    _ => {
                        log::error!(
                            "SQL Engine does not check '{}' existence of SCHEMA before dropping one",
                            schema_name
                        );
                        Err(())
                    }
                },
                Err(error) => Ok(Err(error)),
            },
        }
    }

    fn drop_schema_inner(
        &self,
        schema_name: &str,
        schema_id: Id,
        strategy: DropStrategy,
    ) -> Result<(), DropSchemaError> {
        match strategy {
            DropStrategy::Restrict => {
                let is_schema_empty = self
                    .inner
                    .read(DEFINITION_SCHEMA, TABLES_TABLE)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("to have TABLES table")
                    .map(Result::unwrap)
                    .map(Result::unwrap)
                    .map(|(record_id, _columns)| record_id.unpack()[1].as_u64())
                    .find(|schema| schema_id == *schema)
                    .is_none();
                if is_schema_empty {
                    let schema_record_id = self
                        .inner
                        .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                        .expect("no io error")
                        .expect("no platform error")
                        .expect("to have SCHEMATA table")
                        .map(Result::unwrap)
                        .map(Result::unwrap)
                        .map(|(record_id, columns)| {
                            let schema = columns.unpack()[1].as_str().to_owned();
                            (record_id, schema)
                        })
                        .find(|(_record_id, schema)| schema_name == schema)
                        .map(|(record_id, _schema)| record_id);
                    match schema_record_id {
                        None => Err(DropSchemaError::DoesNotExist),
                        Some(schema_record_id) => {
                            self.inner
                                .delete(DEFINITION_SCHEMA, SCHEMATA_TABLE, vec![schema_record_id])
                                .expect("no io error")
                                .expect("no platform error")
                                .expect("to remove schema");
                            Ok(())
                        }
                    }
                } else {
                    Err(DropSchemaError::HasDependentObjects)
                }
            }
            DropStrategy::Cascade => {
                let schema_record_id = self
                    .inner
                    .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("to have SCHEMATA table")
                    .map(Result::unwrap)
                    .map(Result::unwrap)
                    .map(|(record_id, columns)| {
                        let schema = columns.unpack()[1].as_str().to_owned();
                        (record_id, schema)
                    })
                    .find(|(_record_id, schema)| schema_name == schema)
                    .map(|(record_id, _schema)| record_id);
                match schema_record_id {
                    None => Err(DropSchemaError::DoesNotExist),
                    Some(schema_record_id) => {
                        self.inner
                            .delete(DEFINITION_SCHEMA, SCHEMATA_TABLE, vec![schema_record_id])
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to remove schema");
                        let table_record_ids = self
                            .inner
                            .read(DEFINITION_SCHEMA, TABLES_TABLE)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to have TABLES table")
                            .map(Result::unwrap)
                            .map(Result::unwrap)
                            .map(|(record_id, _columns)| {
                                let schema_id = record_id.unpack()[1].as_u64();
                                (schema_id, record_id)
                            })
                            .filter(|(schema, _record_id)| *schema == schema_id)
                            .map(|(_schema, record_id)| record_id)
                            .collect();
                        self.inner
                            .delete(DEFINITION_SCHEMA, TABLES_TABLE, table_record_ids)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to remove tables under catalog");
                        let table_column_record_ids = self
                            .inner
                            .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to have COLUMNS table")
                            .map(Result::unwrap)
                            .map(Result::unwrap)
                            .map(|(record_id, _data)| {
                                let schema = record_id.unpack()[1].as_u64();
                                (record_id, schema)
                            })
                            .filter(|(_record_id, schema)| *schema == schema_id)
                            .map(|(record_id, _schema)| record_id)
                            .collect();
                        self.inner
                            .delete(DEFINITION_SCHEMA, COLUMNS_TABLE, table_column_record_ids)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to have remove tables columns under catalog");
                        Ok(())
                    }
                }
            }
        }
    }

    #[allow(clippy::result_unit_err)]
    pub fn create_table(
        &self,
        schema_id: Id,
        table_name: &str,
        column_definitions: &[ColumnDefinition],
    ) -> Result<Id, ()> {
        let schema = self
            .inner
            .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let id = record_id.unpack()[1].as_u64();
                let schema = columns.unpack()[1].as_str().to_owned();
                (id, schema)
            })
            .find(|(id, _schema)| id == &schema_id)
            .map(|(_id, schema)| schema);
        match schema {
            None => unimplemented!(),
            Some(schema_name) => {
                let table_id = self
                    .inner
                    .get_sequence(DEFINITION_SCHEMA, &(TABLES_TABLE.to_owned() + ".records"))
                    .unwrap()
                    .next();
                self.inner
                    .write(
                        DEFINITION_SCHEMA,
                        TABLES_TABLE,
                        vec![(
                            Binary::pack(&[
                                DEFAULT_CATALOG_ID,
                                Datum::from_u64(schema_id),
                                Datum::from_u64(table_id),
                            ]),
                            Binary::pack(&[
                                Datum::from_str(DEFAULT_CATALOG),
                                Datum::from_str(&schema_name),
                                Datum::from_str(table_name),
                            ]),
                        )],
                    )
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("to save table info");
                let column_ids_sequence = self
                    .inner
                    .create_sequence(
                        DEFINITION_SCHEMA,
                        &(schema_name.to_owned() + "." + table_name + ".columns.id"),
                    )
                    .unwrap();
                for (index, column) in column_definitions.iter().enumerate() {
                    let chars_len = match column.sql_type() {
                        SqlType::Char(len) | SqlType::VarChar(len) => Datum::from_u64(len),
                        _ => Datum::from_null(),
                    };
                    let id = column_ids_sequence.next();
                    self.inner
                        .write(
                            DEFINITION_SCHEMA,
                            COLUMNS_TABLE,
                            vec![(
                                Binary::pack(&[
                                    DEFAULT_CATALOG_ID,
                                    Datum::from_u64(schema_id),
                                    Datum::from_u64(table_id),
                                    Datum::from_u64(id),
                                ]),
                                Binary::pack(&[
                                    Datum::from_str(DEFAULT_CATALOG),
                                    Datum::from_str(&schema_name),
                                    Datum::from_str(table_name),
                                    Datum::from_u64(index as u64),
                                    Datum::from_str(column.name().as_str()),
                                    Datum::from_u64(column.sql_type().type_id()),
                                    chars_len,
                                ]),
                            )],
                        )
                        .expect("no io error")
                        .expect("no platform error")
                        .expect("to save column");
                }
                self.inner
                    .create_sequence(
                        DEFINITION_SCHEMA,
                        &(schema_name.to_owned() + "." + table_name + ".records"),
                    )
                    .unwrap();
                match self.inner.create_object(&*schema_name, table_name) {
                    Ok(Ok(Ok(()))) => Ok(table_id),
                    _ => {
                        println!(
                            "1. SQL Engine does not check '{}.{}' existence of TABLE before creating one",
                            &*schema_name, table_name
                        );
                        Err(())
                    }
                }
            }
        }
    }

    #[allow(clippy::result_unit_err)]
    pub fn drop_table(&self, full_table_id: &(Id, Id)) -> Result<(), ()> {
        let full_table_name = self
            .inner
            .read(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have COLUMNS table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let ids = record_id.unpack();
                let schema_id = ids[1].as_u64();
                let table_id = ids[2].as_u64();
                let data = columns.unpack();
                let schema_name = data[1].as_str().to_owned();
                let table_name = data[2].as_str().to_owned();
                (schema_id, table_id, schema_name, table_name)
            })
            .find(|(schema_id, table_id, _schema_name, _table_name)| full_table_id == &(*schema_id, *table_id))
            .map(|(_schema_id, _table_id, schema_name, table_name)| (schema_name, table_name));
        match full_table_name {
            None => {
                let (schema_id, table_id) = full_table_id;
                engine_bug_reporter(Operation::Drop, Object::Table(*schema_id, *table_id));
                Err(())
            }
            Some(full_name) => {
                let (schema_id, table_id) = full_table_id;
                self.inner
                    .delete(
                        DEFINITION_SCHEMA,
                        TABLES_TABLE,
                        vec![Binary::pack(&[
                            DEFAULT_CATALOG_ID,
                            Datum::from_u64(*schema_id),
                            Datum::from_u64(*table_id),
                        ])],
                    )
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("to remove table");
                match self.inner.drop_object(full_name.0.as_str(), full_name.1.as_str()) {
                    Ok(Ok(Ok(()))) => Ok(()),
                    _ => {
                        let (schema_id, table_id) = full_table_id;
                        engine_bug_reporter(Operation::Drop, Object::Table(*schema_id, *table_id));
                        Err(())
                    }
                }
            }
        }
    }

    #[allow(clippy::result_unit_err)]
    pub fn write_into(&self, full_table_id: &(Id, Id), values: Vec<(Key, Values)>) -> Result<usize, ()> {
        let full_table_name = self
            .inner
            .read(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have COLUMNS table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let ids = record_id.unpack();
                let schema_id = ids[1].as_u64();
                let table_id = ids[2].as_u64();
                let data = columns.unpack();
                let schema_name = data[1].as_str().to_owned();
                let table_name = data[2].as_str().to_owned();
                (schema_id, table_id, schema_name, table_name)
            })
            .find(|(schema_id, table_id, _schema_name, _table_name)| full_table_id == &(*schema_id, *table_id))
            .map(|(_schema_id, _table_id, schema_name, table_name)| (schema_name, table_name));
        match full_table_name {
            Some(full_name) => {
                log::trace!("values to write {:#?}", values);
                match self.inner.write(full_name.0.as_str(), full_name.1.as_str(), values) {
                    Ok(Ok(Ok(size))) => Ok(size),
                    _ => {
                        let (schema_id, table_id) = full_table_id;
                        engine_bug_reporter(Operation::Access, Object::Table(*schema_id, *table_id));
                        Err(())
                    }
                }
            }
            None => {
                let (schema_id, table_id) = full_table_id;
                engine_bug_reporter(Operation::Access, Object::Table(*schema_id, *table_id));
                Err(())
            }
        }
    }

    #[allow(clippy::result_unit_err)]
    pub fn full_scan(&self, full_table_id: &(Id, Id)) -> Result<ReadCursor, ()> {
        let full_table_name = self
            .inner
            .read(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have COLUMNS table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let ids = record_id.unpack();
                let schema_id = ids[1].as_u64();
                let table_id = ids[2].as_u64();
                let data = columns.unpack();
                let schema_name = data[1].as_str().to_owned();
                let table_name = data[2].as_str().to_owned();
                (schema_id, table_id, schema_name, table_name)
            })
            .find(|(schema_id, table_id, _schema_name, _table_name)| full_table_id == &(*schema_id, *table_id))
            .map(|(_schema_id, _table_id, schema_name, table_name)| (schema_name, table_name));
        match full_table_name {
            Some(full_name) => match self.inner.read(full_name.0.as_str(), full_name.1.as_str()) {
                Ok(Ok(Ok(read))) => Ok(read),
                _ => {
                    let (schema_id, table_id) = full_table_id;
                    engine_bug_reporter(Operation::Access, Object::Table(*schema_id, *table_id));
                    Err(())
                }
            },
            None => {
                let (schema_id, table_id) = full_table_id;
                engine_bug_reporter(Operation::Access, Object::Table(*schema_id, *table_id));
                Err(())
            }
        }
    }

    #[allow(clippy::result_unit_err)]
    pub fn delete_from(&self, full_table_id: &(Id, Id), keys: Vec<Key>) -> Result<usize, ()> {
        let full_table_name = self
            .inner
            .read(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have COLUMNS table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let ids = record_id.unpack();
                let schema_id = ids[1].as_u64();
                let table_id = ids[2].as_u64();
                let data = columns.unpack();
                let schema_name = data[1].as_str().to_owned();
                let table_name = data[2].as_str().to_owned();
                (schema_id, table_id, schema_name, table_name)
            })
            .find(|(schema_id, table_id, _schema_name, _table_name)| full_table_id == &(*schema_id, *table_id))
            .map(|(_schema_id, _table_id, schema_name, table_name)| (schema_name, table_name));
        match full_table_name {
            Some(full_name) => match self.inner.delete(full_name.0.as_str(), full_name.1.as_str(), keys) {
                Ok(Ok(Ok(len))) => Ok(len),
                _ => {
                    let (schema_id, table_id) = full_table_id;
                    engine_bug_reporter(Operation::Access, Object::Table(*schema_id, *table_id));
                    Err(())
                }
            },
            None => {
                let (schema_id, table_id) = full_table_id;
                engine_bug_reporter(Operation::Access, Object::Table(*schema_id, *table_id));
                Err(())
            }
        }
    }
}

impl DataDefOperationExecutor for DatabaseHandle {
    fn execute(&self, operation: &Step) -> Result<(), ()> {
        match operation {
            Step::CheckExistence {
                system_object,
                object_name,
            } => match system_object {
                SystemObject::Schema => {
                    let schema_exists = self
                        .inner
                        .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                        .expect("no io error")
                        .expect("no platform error")
                        .expect("to have COLUMNS table")
                        .map(Result::unwrap)
                        .map(Result::unwrap)
                        .map(|(_record_id, columns)| columns.unpack()[1].as_str().to_owned())
                        .any(|name| name == object_name[0]);
                    if schema_exists {
                        Ok(())
                    } else {
                        Err(())
                    }
                }
                SystemObject::Table => {
                    let table_exists = self
                        .inner
                        .read(DEFINITION_SCHEMA, TABLES_TABLE)
                        .expect("no io error")
                        .expect("no platform error")
                        .expect("to have COLUMNS table")
                        .map(Result::unwrap)
                        .map(Result::unwrap)
                        .map(|(_record_id, columns)| columns.unpack()[2].as_str().to_owned())
                        .any(|name| name == object_name[0]);
                    if table_exists {
                        Ok(())
                    } else {
                        Err(())
                    }
                }
            },
            Step::CheckDependants {
                system_object,
                object_name,
            } => match system_object {
                SystemObject::Schema => {
                    let has_dependants = self
                        .inner
                        .read(DEFINITION_SCHEMA, TABLES_TABLE)
                        .expect("no io error")
                        .expect("no platform error")
                        .expect("to have COLUMNS table")
                        .map(Result::unwrap)
                        .map(Result::unwrap)
                        .map(|(_record_id, columns)| columns.unpack()[1].as_str().to_owned())
                        .any(|name| name == object_name[0]);
                    if has_dependants {
                        Err(())
                    } else {
                        Ok(())
                    }
                }
                SystemObject::Table => Ok(()),
            },
            Step::RemoveDependants {
                system_object,
                object_name,
            } => match system_object {
                SystemObject::Schema => {
                    let schema = self
                        .inner
                        .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                        .expect("no io error")
                        .expect("no platform error")
                        .expect("to have SCHEMATA table")
                        .map(Result::unwrap)
                        .map(Result::unwrap)
                        .map(|(record_id, columns)| {
                            let schema_id = record_id.unpack()[1].as_u64();
                            let schema = columns.unpack()[1].as_str().to_owned();
                            (schema_id, schema)
                        })
                        .find(|(_schema_id, schema)| schema == &object_name[0])
                        .map(|(schema_id, _schema)| schema_id)
                        .unwrap();
                    let mut table_records = vec![];
                    let mut schema_table = vec![];
                    for (record_id, schema_id, table_id) in self
                        .inner
                        .read(DEFINITION_SCHEMA, TABLES_TABLE)
                        .expect("no io error")
                        .expect("no platform error")
                        .expect("to have SCHEMATA table")
                        .map(Result::unwrap)
                        .map(Result::unwrap)
                        .map(|(record_id, _columns)| {
                            let ids = record_id.unpack();
                            let schema_id = ids[1].as_u64();
                            let table_id = ids[2].as_u64();
                            (record_id, schema_id, table_id)
                        })
                        .filter(|(_record_id, schema_id, _table_id)| schema_id == &schema)
                    {
                        table_records.push(record_id);
                        schema_table.push((schema_id, table_id));
                    }

                    let columns_records = self
                        .inner
                        .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
                        .expect("no io error")
                        .expect("no platform error")
                        .expect("to have SCHEMATA table")
                        .map(Result::unwrap)
                        .map(Result::unwrap)
                        .map(|(record_id, _columns)| {
                            let ids = record_id.unpack();
                            let schema_id = ids[1].as_u64();
                            let table_id = ids[2].as_u64();
                            (record_id, schema_id, table_id)
                        })
                        .filter(|(_record_id, schema_id, table_id)| schema_table.contains(&(*schema_id, *table_id)))
                        .map(|(record_id, _schema_id, _table_id)| record_id)
                        .collect();

                    self.inner
                        .delete(DEFINITION_SCHEMA, COLUMNS_TABLE, columns_records)
                        .unwrap()
                        .unwrap()
                        .unwrap();

                    self.inner
                        .delete(DEFINITION_SCHEMA, TABLES_TABLE, table_records)
                        .unwrap()
                        .unwrap()
                        .unwrap();
                    Ok(())
                }
                SystemObject::Table => unimplemented!(),
            },
            Step::RemoveColumns {
                schema_name,
                table_name,
            } => {
                let (schema, table) = self
                    .inner
                    .read(DEFINITION_SCHEMA, TABLES_TABLE)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("to have SCHEMATA table")
                    .map(Result::unwrap)
                    .map(Result::unwrap)
                    .map(|(record_id, columns)| {
                        let ids = record_id.unpack();
                        let schema_id = ids[1].as_u64();
                        let table_id = ids[2].as_u64();
                        let data = columns.unpack();
                        let schema = data[1].as_str().to_owned();
                        let table = data[2].as_str().to_owned();
                        (schema_id, table_id, schema, table)
                    })
                    .find(|(_schema_id, _table_id, schema, table)| schema_name == schema && table_name == table)
                    .map(|(schema_id, table_id, _schema, _table)| (schema_id, table_id))
                    .unwrap();
                let column_records = self
                    .inner
                    .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("to have SCHEMATA table")
                    .map(Result::unwrap)
                    .map(Result::unwrap)
                    .map(|(record_id, _columns)| {
                        let ids = record_id.unpack();
                        let schema_id = ids[1].as_u64();
                        let table_id = ids[2].as_u64();
                        (record_id, schema_id, table_id)
                    })
                    .filter(|(_record_id, schema_id, table_id)| &schema == schema_id && &table == table_id)
                    .map(|(record_id, _schema_id, _table_id)| record_id)
                    .collect();
                self.inner
                    .delete(DEFINITION_SCHEMA, COLUMNS_TABLE, column_records)
                    .unwrap()
                    .unwrap()
                    .unwrap();
                Ok(())
            }
            Step::CreateFolder { name } => {
                self.inner.create_schema(&name).unwrap().unwrap();
                Ok(())
            }
            Step::RemoveFolder { name } => {
                self.inner.drop_schema(&name).unwrap().unwrap();
                Ok(())
            }
            Step::CreateFile { folder_name, name } => {
                self.inner.create_object(&folder_name, &name).unwrap().unwrap().unwrap();
                self.inner
                    .create_sequence(&folder_name, &(name.to_owned() + ".records"))
                    .unwrap();
                Ok(())
            }
            Step::RemoveFile { folder_name, name } => {
                self.inner.drop_object(&folder_name, &name).unwrap().unwrap().unwrap();
                self.inner
                    .drop_sequence(&folder_name, &(name.to_owned() + ".records"))
                    .unwrap();
                Ok(())
            }
            Step::RemoveRecord {
                system_schema,
                system_table,
                record,
            } => {
                let binary_record = match record {
                    Record::Schema {
                        catalog_name,
                        schema_name,
                    } => self
                        .inner
                        .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                        .expect("no io error")
                        .expect("no platform error")
                        .expect("to have SCHEMATA table")
                        .map(Result::unwrap)
                        .map(Result::unwrap)
                        .map(|(record_id, columns)| {
                            let data = columns.unpack();
                            let catalog = data[0].as_str().to_owned();
                            let schema = data[1].as_str().to_owned();
                            (record_id, catalog, schema)
                        })
                        .find(|(_record, catalog, schema)| catalog == catalog_name && schema == schema_name)
                        .map(|(record, _catalog, _schema)| record)
                        .unwrap(),
                    Record::Table {
                        catalog_name,
                        schema_name,
                        table_name,
                    } => self
                        .inner
                        .read(DEFINITION_SCHEMA, TABLES_TABLE)
                        .expect("no io error")
                        .expect("no platform error")
                        .expect("to have SCHEMATA table")
                        .map(Result::unwrap)
                        .map(Result::unwrap)
                        .map(|(record_id, columns)| {
                            let data = columns.unpack();
                            let catalog = data[0].as_str().to_owned();
                            let schema = data[1].as_str().to_owned();
                            let table = data[2].as_str().to_owned();
                            (record_id, catalog, schema, table)
                        })
                        .find(|(_record, catalog, schema, table)| {
                            catalog == catalog_name && schema == schema_name && table == table_name
                        })
                        .map(|(record, _catalog, _schema, _table)| record)
                        .unwrap(),
                    Record::Column { .. } => unreachable!(),
                };
                self.inner
                    .delete(&system_schema, &system_table, vec![binary_record])
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("to remove object");
                Ok(())
            }
            Step::CreateRecord {
                system_schema,
                system_table,
                record,
            } => {
                log::debug!("{:?}", record);
                let binary_record = match record {
                    Record::Schema {
                        catalog_name,
                        schema_name,
                    } => {
                        let schema_id = self
                            .inner
                            .get_sequence(DEFINITION_SCHEMA, &(SCHEMATA_TABLE.to_owned() + ".records"))
                            .unwrap()
                            .next();
                        vec![(
                            Binary::pack(&[DEFAULT_CATALOG_ID, Datum::from_u64(schema_id)]),
                            Binary::pack(&[Datum::from_str(&catalog_name), Datum::from_str(&schema_name)]),
                        )]
                    }
                    Record::Table {
                        catalog_name,
                        schema_name,
                        table_name,
                    } => {
                        let schema = self
                            .inner
                            .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to have SCHEMATA table")
                            .map(Result::unwrap)
                            .map(Result::unwrap)
                            .map(|(record_id, columns)| {
                                let id = record_id.unpack()[1].as_u64();
                                let schema = columns.unpack()[1].as_str().to_owned();
                                (id, schema)
                            })
                            .find(|(_id, schema)| schema_name == schema)
                            .map(|(id, _schema)| id);
                        let schema_id = match schema {
                            Some(schema_id) => schema_id,
                            None => return Err(()),
                        };
                        let table_id = self
                            .inner
                            .get_sequence(DEFINITION_SCHEMA, &(TABLES_TABLE.to_owned() + ".records"))
                            .unwrap()
                            .next();
                        self.inner
                            .create_sequence(
                                DEFINITION_SCHEMA,
                                &(TABLES_TABLE.to_owned()
                                    + format!("{}.{}", schema_id, table_id).as_str()
                                    + ".column.ids"),
                            )
                            .expect("column id sequence is created");
                        vec![(
                            Binary::pack(&[
                                DEFAULT_CATALOG_ID,
                                Datum::from_u64(schema_id),
                                Datum::from_u64(table_id),
                            ]),
                            Binary::pack(&[
                                Datum::from_str(&catalog_name),
                                Datum::from_str(&schema_name),
                                Datum::from_str(&table_name),
                            ]),
                        )]
                    }
                    Record::Column {
                        catalog_name,
                        schema_name,
                        table_name,
                        column_name,
                        sql_type,
                    } => {
                        let full_table_id = self
                            .inner
                            .read(DEFINITION_SCHEMA, TABLES_TABLE)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to have TABLES table")
                            .map(Result::unwrap)
                            .map(Result::unwrap)
                            .map(|(record_id, columns)| {
                                let ids = record_id.unpack();
                                let schema_id = ids[1].as_u64();
                                let table_id = ids[2].as_u64();
                                let data = columns.unpack();
                                let schema = data[1].as_str().to_owned();
                                let table = data[2].as_str().to_owned();
                                (schema_id, table_id, schema, table)
                            })
                            .find(|(_schema_id, _table_id, schema, table)| schema_name == schema && table_name == table)
                            .map(|(schema_id, table_id, _schema, _table)| (schema_id, table_id));
                        let (schema_id, table_id) = match full_table_id {
                            Some((schema_id, table_id)) => (schema_id, table_id),
                            None => return Err(()),
                        };
                        let column_id = self
                            .inner
                            .get_sequence(
                                DEFINITION_SCHEMA,
                                &(TABLES_TABLE.to_owned()
                                    + format!("{}.{}", schema_id, table_id).as_str()
                                    + ".column.ids"),
                            )
                            .unwrap()
                            .next();
                        let chars_len = match sql_type {
                            SqlType::Char(len) | SqlType::VarChar(len) => Datum::from_u64(*len),
                            _ => Datum::from_null(),
                        };
                        vec![(
                            Binary::pack(&[
                                DEFAULT_CATALOG_ID,
                                Datum::from_u64(schema_id),
                                Datum::from_u64(table_id),
                                Datum::from_u64(column_id),
                            ]),
                            Binary::pack(&[
                                Datum::from_str(&catalog_name),
                                Datum::from_str(&schema_name),
                                Datum::from_str(&table_name),
                                Datum::from_u64(column_id),
                                Datum::from_str(&column_name),
                                Datum::from_u64(sql_type.type_id()),
                                chars_len,
                            ]),
                        )]
                    }
                };
                self.inner
                    .write(&system_schema, &system_table, binary_record)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("to save schema");
                Ok(())
            }
        }
    }
}

impl DataDefReader for DatabaseHandle {
    fn schema_exists(&self, schema_name: &str) -> OptionalSchemaId {
        self.inner
            .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let id = record_id.unpack()[1].as_u64();
                let columns = columns.unpack();
                let catalog = columns[0].as_str().to_owned();
                let schema = columns[1].as_str().to_owned();
                (id, catalog, schema)
            })
            .filter(|(_id, catalog, schema)| catalog == DEFAULT_CATALOG && schema == schema_name)
            .map(|(id, _catalog, _schema)| id)
            .next()
    }

    fn table_exists(&self, schema_name: &str, table_name: &str) -> OptionalTableId {
        match self.schema_exists(schema_name) {
            None => None,
            Some(schema_id) => Some((
                schema_id,
                self.inner
                    .read(DEFINITION_SCHEMA, TABLES_TABLE)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("to have SCHEMATA_TABLE table")
                    .map(Result::unwrap)
                    .map(Result::unwrap)
                    .map(|(record_id, columns)| {
                        let record = record_id.unpack();
                        let schema_id = record[1].as_u64();
                        let table_id = record[2].as_u64();
                        let columns = columns.unpack();
                        let table = columns[2].as_str().to_owned();
                        (schema_id, table_id, table)
                    })
                    .filter(|(schema, _id, name)| schema == &schema_id && name == table_name)
                    .map(|(_schema_id, table_id, _table)| table_id)
                    .next(),
            )),
        }
    }

    fn table_columns(&self, table_id: &(Id, Id)) -> Result<Vec<(Id, ColumnDefinition)>, ()> {
        log::debug!("FULL TABLE ID {:?}", table_id);
        match self
            .inner
            .read(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, _columns)| {
                let record = record_id.unpack();
                let schema_id = record[1].as_u64();
                let table_id = record[2].as_u64();
                (schema_id, table_id)
            })
            .find(|full_table_id| full_table_id == table_id)
        {
            Some(_) => {}
            None => return Err(()),
        }
        Ok(self
            .inner
            .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have COLUMNS table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let record = record_id.unpack();
                let schema_id = record[1].as_u64();
                let table_id = record[2].as_u64();
                let column_id = record[3].as_u64();
                let columns = columns.unpack();
                let name = columns[4].as_str().to_owned();
                let type_id = columns[5].as_u64();
                let chars_len = match columns[6] {
                    Datum::Int64(val) => val as u64,
                    _ => 0,
                };
                let sql_type = SqlType::from_type_id(type_id, chars_len);
                log::debug!("{:?}", (schema_id, table_id));
                ((schema_id, table_id), column_id, name, sql_type)
            })
            .filter(|(full_table_id, _column_id, _name, _sql_type)| full_table_id == table_id)
            .map(|(_full_table_id, column_id, name, sql_type)| (column_id, ColumnDefinition::new(&name, sql_type)))
            .collect())
    }

    fn column_ids(&self, table_id: &(Id, Id), names: &[String]) -> Result<(Vec<Id>, Vec<String>), ()> {
        match self
            .inner
            .read(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, _columns)| {
                let record = record_id.unpack();
                let schema_id = record[1].as_u64();
                let table_id = record[2].as_u64();
                (schema_id, table_id)
            })
            .find(|full_table_id| full_table_id == table_id)
        {
            Some(_) => {}
            None => return Err(()),
        }
        let mut idx = vec![];
        let mut not_found = vec![];
        let columns = self
            .inner
            .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let record = record_id.unpack();
                let schema_id = record[1].as_u64();
                let table_id = record[2].as_u64();
                let column_id = record[3].as_u64();
                let columns = columns.unpack();
                let name = columns[4].as_str().to_owned();
                ((schema_id, table_id), column_id, name)
            })
            .filter(|(full_table_id, _column_id, _name)| full_table_id == table_id)
            .map(|(_full_table_id, column_id, name)| (name.to_lowercase(), column_id))
            .collect::<HashMap<_, _>>();
        log::debug!("FOUND COLUMNS: {:?}", columns);
        log::debug!("COLUMNS TO FIND: {:?}", names);
        for name in names {
            match columns.get(name) {
                None => not_found.push(name.to_owned()),
                Some(id) => idx.push(*id),
            }
        }
        Ok((idx, not_found))
    }

    fn column_defs(&self, table_id: &(Id, Id), ids: &[Id]) -> Vec<ColumnDefinition> {
        match self
            .inner
            .read(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, _columns)| {
                let record = record_id.unpack();
                let schema_id = record[1].as_u64();
                let table_id = record[2].as_u64();
                (schema_id, table_id)
            })
            .find(|full_table_id| full_table_id == table_id)
        {
            Some(_) => {}
            None => {
                log::debug!("TABLE DOES NOT FOUND {:?}", table_id);
                return vec![];
            }
        }
        let mut defs = vec![];
        let columns = self
            .inner
            .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let record = record_id.unpack();
                let schema_id = record[1].as_u64();
                let table_id = record[2].as_u64();
                let column_id = record[3].as_u64();
                let columns = columns.unpack();
                let name = columns[4].as_str().to_owned();
                let type_id = columns[5].as_u64();
                let chars_len = match columns[6] {
                    Datum::Int64(val) => val as u64,
                    _ => 0,
                };
                let sql_type = SqlType::from_type_id(type_id, chars_len);
                ((schema_id, table_id), column_id, name, sql_type)
            })
            .filter(|(full_table_id, _column_id, _name, _sql_type)| full_table_id == table_id)
            .map(|(_full_table_id, column_id, name, sql_type)| (column_id, ColumnDefinition::new(&name, sql_type)))
            .collect::<HashMap<_, _>>();
        log::debug!("COLUMNS IN TABLE: {:?}", columns);
        log::debug!("SELECTED COLUMN IDS {:?}", ids);
        for id in ids {
            match columns.get(id) {
                None => {}
                Some(def) => defs.push(def.clone()),
            }
        }
        defs
    }
}

fn engine_bug_reporter(operation: Operation, object: Object) {
    println!(
        "This is most possibly a 🐛[BUG] in sql engine. It does not check existence of {} before {} one",
        object, operation
    )
}

enum Operation {
    Drop,
    Access,
}

impl Display for Operation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Operation::Drop => write!(f, "dropping"),
            Operation::Access => write!(f, "accessing"),
        }
    }
}

enum Object {
    Table(Id, Id),
}

impl Display for Object {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Object::Table(schema_id, table_id) => write!(f, "TABLE [{}.{}]", schema_id, table_id),
        }
    }
}

#[cfg(test)]
mod tests;

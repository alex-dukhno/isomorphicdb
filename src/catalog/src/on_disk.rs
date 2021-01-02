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

use crate::{Cursor, DataCatalog, DataTable, Key, SchemaHandle, Value};
use binary::Binary;
use dashmap::DashMap;
use repr::Datum;
use sled;
use std::convert::TryInto;
use std::iter::FromIterator;
use std::path::PathBuf;

const TABLE_RECORD_IDS_KEY: &str = "__record_counter";
const STARTING_RECORD_ID: [u8; 8] = 0u64.to_be_bytes();

#[derive(Debug)]
pub struct OnDiskTableHandle {
    metadata: sled::Tree,
    data: sled::Tree,
}

impl OnDiskTableHandle {
    fn new(metadata: sled::Tree, data: sled::Tree) -> OnDiskTableHandle {
        debug_assert!(
            matches!(metadata.insert(TABLE_RECORD_IDS_KEY, &STARTING_RECORD_ID), Ok(None)),
            format!(
                "system value {:?} should be used up to this point",
                TABLE_RECORD_IDS_KEY
            )
        );
        OnDiskTableHandle { metadata, data }
    }

    fn next_id(&self) -> u64 {
        let current = match self.metadata.get(TABLE_RECORD_IDS_KEY) {
            Ok(Some(current)) => u64::from_be_bytes(current[0..8].try_into().unwrap()),
            Ok(None) => {
                log::error!(
                    "system value {:?} was not initialized until this point",
                    TABLE_RECORD_IDS_KEY
                );
                unreachable!("Database is inconsistent state. Aborting...");
            }
            Err(error) => {
                log::error!(
                    "could not retrieve current record id from {:?} system key due to {:?}",
                    TABLE_RECORD_IDS_KEY,
                    error
                );
                unreachable!("Database is inconsistent state. Aborting...");
            }
        };
        self.metadata
            .insert(TABLE_RECORD_IDS_KEY, &((current + 1).to_be_bytes()));
        current
    }
}

impl DataTable for OnDiskTableHandle {
    fn select(&self) -> Cursor {
        Cursor::from_iter(
            self.data
                .iter()
                .map(Result::unwrap)
                .map(|(key, value)| (Binary::with_data(key.to_vec()), Binary::with_data(value.to_vec()))),
        )
    }

    fn insert(&self, data: Vec<Value>) -> usize {
        let len = data.len();
        for value in data {
            let record_id = self.next_id();
            let key = Binary::pack(&[Datum::from_u64(record_id)]);
            self.data.insert(key.to_bytes(), value.to_bytes());
        }
        len
    }

    fn update(&self, data: Vec<(Key, Value)>) -> usize {
        let len = data.len();
        for (key, value) in data {
            debug_assert!(
                matches!(self.data.insert(key.to_bytes(), value.to_bytes()), Ok(Some(_))),
                "update operation should change already existed key"
            );
        }
        len
    }

    fn delete(&self, data: Vec<Key>) -> usize {
        let mut size = 0;
        let keys = self
            .data
            .iter()
            .map(Result::unwrap)
            .filter(|(_key, value)| data.contains(&Binary::with_data(value.to_vec())))
            .map(|(key, _value)| key.clone())
            .collect::<Vec<sled::IVec>>();
        for key in keys.iter() {
            self.data.remove(key);
            size += 1;
        }
        size
    }
}

#[derive(Debug)]
pub struct OnDiskSchemaHandle {
    name: String,
    sled_db: sled::Db,
    tables: DashMap<String, OnDiskTableHandle>,
}

impl OnDiskSchemaHandle {
    fn new(name: String, sled_db: sled::Db) -> OnDiskSchemaHandle {
        OnDiskSchemaHandle {
            name,
            sled_db,
            tables: DashMap::default(),
        }
    }
}

impl SchemaHandle for OnDiskSchemaHandle {
    type Table = OnDiskTableHandle;

    fn create_table(&self, table_name: &str) -> bool {
        if self.tables.contains_key(table_name) {
            false
        } else {
            if self.sled_db.tree_names().contains(&sled::IVec::from(table_name)) {
                false
            } else {
                let data_tree = self.sled_db.open_tree(table_name).unwrap();
                let metadata_tree = self
                    .sled_db
                    .open_tree("__system_metadata_".to_owned() + table_name)
                    .unwrap();
                self.tables
                    .insert(table_name.to_owned(), OnDiskTableHandle::new(metadata_tree, data_tree));
                true
            }
        }
    }

    fn drop_table(&self, table_name: &str) -> bool {
        if !self.tables.contains_key(table_name) {
            false
        } else {
            self.tables.remove(table_name);
            if let Err(sled_error) = self.sled_db.drop_tree(table_name) {
                log::error!(
                    "Could not remove table {:?} from schema {:?} due to error {:?}",
                    table_name,
                    self.name,
                    sled_error
                );
            }
            true
        }
    }

    fn work_with<T, F: Fn(&Self::Table) -> T>(&self, table_name: &str, operation: F) -> Option<T> {
        self.tables.get(table_name).map(|table| operation(&*table))
    }
}

pub struct OnDiskCatalogHandle {
    path_to_catalog: PathBuf,
    schemas: DashMap<String, OnDiskSchemaHandle>,
}

impl OnDiskCatalogHandle {
    pub fn new(path_to_catalog: PathBuf) -> OnDiskCatalogHandle {
        OnDiskCatalogHandle {
            path_to_catalog,
            schemas: DashMap::default(),
        }
    }

    fn path_to_schema(&self, schema_name: &str) -> PathBuf {
        PathBuf::from(&self.path_to_catalog).join(&schema_name)
    }
}

impl DataCatalog for OnDiskCatalogHandle {
    type Schema = OnDiskSchemaHandle;

    fn create_schema(&self, schema_name: &str) -> bool {
        if self.schemas.contains_key(schema_name) {
            false
        } else {
            let path_to_schema = self.path_to_schema(schema_name);
            if path_to_schema.exists() {
                false
            } else {
                let sled_db = sled::open(path_to_schema).unwrap();
                self.schemas.insert(
                    schema_name.to_owned(),
                    OnDiskSchemaHandle::new(schema_name.to_owned(), sled_db),
                );
                true
            }
        }
    }

    fn drop_schema(&self, schema_name: &str) -> bool {
        let path_to_schema = self.path_to_schema(schema_name);
        if !path_to_schema.exists() {
            false
        } else {
            self.schemas.remove(schema_name);
            if let Err(io_error) = std::fs::remove_dir_all(&path_to_schema) {
                log::error!(
                    "Could not remove schema {:?} from file system located at {:?} due to error {:?}",
                    schema_name,
                    path_to_schema,
                    io_error
                );
                false
            } else {
                true
            }
        }
    }

    fn work_with<T, F: Fn(&Self::Schema) -> T>(&self, schema_name: &str, operation: F) -> Option<T> {
        if !self.schemas.contains_key(schema_name) {
            let path_to_schema = self.path_to_schema(schema_name);
            if path_to_schema.exists() {
                let sled_db = sled::open(path_to_schema).unwrap();
                self.schemas.insert(
                    schema_name.to_owned(),
                    OnDiskSchemaHandle::new(schema_name.to_owned(), sled_db),
                );
            } else {
                return None;
            }
        }
        self.schemas.get(schema_name).map(|schema| operation(&*schema))
    }
}

#[cfg(test)]
mod catalog_persistence_cases {
    use super::*;

    fn catalog_and_path() -> (OnDiskCatalogHandle, PathBuf) {
        let temp_dir = tempfile::tempdir().expect("to create temporary folder");
        let path_to_catalog = temp_dir.into_path();
        (
            OnDiskCatalogHandle::new(PathBuf::from(&path_to_catalog)),
            path_to_catalog,
        )
    }

    #[test]
    fn schemas_should_exist_after_handle_recreation() {
        let (catalog, path) = catalog_and_path();

        assert_eq!(catalog.create_schema("schema_name"), true);

        drop(catalog);

        let catalog = OnDiskCatalogHandle::new(path);

        assert_eq!(catalog.create_schema("schema_name"), false);
    }

    #[test]
    fn it_is_possible_to_work_with_existent_schema_after_catalog_recreation() {
        let (catalog, path) = catalog_and_path();

        assert_eq!(catalog.create_schema("schema_name"), true);

        drop(catalog);

        let catalog = OnDiskCatalogHandle::new(path);

        assert_eq!(catalog.work_with("schema_name", |_schema| 1), Some(1));
    }
}

#[cfg(test)]
mod general_cases {
    use super::*;

    const SCHEMA: &str = "schema_name";
    const SCHEMA_1: &str = "schema_name_1";
    const SCHEMA_2: &str = "schema_name_2";
    const TABLE: &str = "table_name";
    const TABLE_1: &str = "table_name_1";
    const TABLE_2: &str = "table_name_2";
    const DOES_NOT_EXIST: &str = "does_not_exist";

    fn catalog() -> OnDiskCatalogHandle {
        let temp_dir = tempfile::tempdir().expect("to create temporary folder");
        let path_to_catalog = temp_dir.into_path();
        OnDiskCatalogHandle::new(path_to_catalog)
    }

    #[cfg(test)]
    mod schemas {
        use super::*;

        #[test]
        fn create_schemas_with_different_names() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA_1), true);
            assert_eq!(catalog_handle.work_with(SCHEMA_1, |_schema| 1), Some(1));
            assert_eq!(catalog_handle.create_schema(SCHEMA_2), true);
            assert_eq!(catalog_handle.work_with(SCHEMA_2, |_schema| 2), Some(2));
        }

        #[test]
        fn drop_schema() {
            let catalog_handle = catalog();

            assert!(catalog_handle.create_schema(SCHEMA));
            assert_eq!(catalog_handle.drop_schema(SCHEMA), true);
            assert!(matches!(catalog_handle.work_with(SCHEMA, |_schema| 1), None));
            assert_eq!(catalog_handle.create_schema(SCHEMA), true);
            assert!(matches!(catalog_handle.work_with(SCHEMA, |_schema| 1), Some(1)));
        }

        #[test]
        fn dropping_schema_drops_tables_in_it() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);

            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE_1)),
                Some(true)
            );
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE_2)),
                Some(true)
            );

            assert_eq!(catalog_handle.drop_schema(SCHEMA), true);
            assert_eq!(catalog_handle.create_schema(SCHEMA), true);
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE_1)),
                Some(true)
            );
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE_2)),
                Some(true)
            );
        }

        #[test]
        fn create_schema_with_the_same_name() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);
            assert_eq!(catalog_handle.create_schema(SCHEMA), false);
        }

        #[test]
        fn drop_schema_that_does_not_exist() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.drop_schema(SCHEMA), false);
        }
    }

    #[cfg(test)]
    mod create_table {
        use super::*;

        #[test]
        fn create_tables_with_different_names() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);

            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE_1)),
                Some(true)
            );
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE_2)),
                Some(true)
            );
        }

        #[test]
        fn create_tables_with_the_same_name_in_the_same_schema() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE)),
                Some(true)
            );
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE)),
                Some(false)
            );
        }

        #[test]
        fn create_tables_in_non_existent_schema() {
            let catalog_handle = catalog();

            assert_eq!(
                catalog_handle.work_with(DOES_NOT_EXIST, |schema| schema.create_table(TABLE)),
                None
            );
        }

        #[test]
        fn create_table_with_the_same_name_in_different_namespaces() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA_1), true);
            assert_eq!(catalog_handle.create_schema(SCHEMA_2), true);

            assert_eq!(
                catalog_handle.work_with(SCHEMA_1, |schema| schema.create_table(TABLE)),
                Some(true)
            );
            assert_eq!(
                catalog_handle.work_with(SCHEMA_2, |schema| schema.create_table(TABLE)),
                Some(true)
            );
        }
    }

    #[cfg(test)]
    mod drop_table {
        use super::*;

        #[test]
        fn drop_table() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);

            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE)),
                Some(true)
            );
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.drop_table(TABLE)),
                Some(true)
            );
        }

        #[test]
        fn drop_table_from_schema_that_does_not_exist() {
            let catalog_handle = catalog();

            assert_eq!(
                catalog_handle.work_with(DOES_NOT_EXIST, |schema| schema.drop_table(TABLE)),
                None
            );
        }

        #[test]
        fn drop_table_that_does_not_exist() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |s| s.drop_table(DOES_NOT_EXIST)),
                Some(false)
            );
        }
    }

    #[cfg(test)]
    mod operations_on_table {
        use super::*;

        #[test]
        fn scan_table_that_in_schema_that_does_not_exist() {
            let catalog_handle = catalog();

            assert!(matches!(
                catalog_handle.work_with(DOES_NOT_EXIST, |schema| schema.work_with(TABLE, |table| table.select())),
                None
            ));
        }

        #[test]
        fn scan_table_that_does_not_exist() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);
            assert!(matches!(
                catalog_handle.work_with(SCHEMA, |schema| schema
                    .work_with(DOES_NOT_EXIST, |table| table.select())),
                Some(None)
            ));
        }

        #[test]
        fn insert_a_row_into_table_in_schema_that_does_not_exist() {
            let catalog_handle = catalog();

            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.work_with(TABLE, |table| table.insert(vec![]))),
                None
            );
        }

        #[test]
        fn insert_a_row_into_table_that_does_not_exist() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);

            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.work_with(TABLE, |table| table.insert(vec![]))),
                Some(None)
            );
        }

        #[test]
        fn insert_row_into_table_and_scan() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE)),
                Some(true)
            );

            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema
                    .work_with(TABLE, |table| table.insert(vec![Binary::pack(&[Datum::from_u64(1)])]))),
                Some(Some(1))
            );

            assert_eq!(
                catalog_handle
                    .work_with(SCHEMA, |schema| schema.work_with(TABLE, |table| table.select()))
                    .unwrap()
                    .unwrap()
                    .collect::<Vec<(Key, Value)>>(),
                vec![(Binary::pack(&[Datum::from_u64(0)]), Binary::pack(&[Datum::from_u64(1)]))]
            );
        }

        #[test]
        fn insert_many_rows_into_table_and_scan() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE)),
                Some(true)
            );

            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.work_with(TABLE, |table| table.insert(vec![
                    Binary::pack(&[Datum::from_u64(1)]),
                    Binary::pack(&[Datum::from_u64(2)])
                ]))),
                Some(Some(2))
            );

            assert_eq!(
                catalog_handle
                    .work_with(SCHEMA, |schema| schema.work_with(TABLE, |table| table.select()))
                    .unwrap()
                    .unwrap()
                    .collect::<Vec<(Key, Value)>>(),
                vec![
                    (Binary::pack(&[Datum::from_u64(0)]), Binary::pack(&[Datum::from_u64(1)])),
                    (Binary::pack(&[Datum::from_u64(1)]), Binary::pack(&[Datum::from_u64(2)]))
                ]
            );
        }

        #[test]
        fn delete_from_table_that_in_schema_that_does_not_exist() {
            let catalog_handle = catalog();

            assert_eq!(
                catalog_handle.work_with(DOES_NOT_EXIST, |schema| schema
                    .work_with(TABLE, |table| table.delete(vec![]))),
                None
            );
        }

        #[test]
        fn delete_from_table_that_does_not_exist() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema
                    .work_with(DOES_NOT_EXIST, |table| table.delete(vec![]))),
                Some(None)
            );
        }

        #[test]
        fn insert_delete_scan_records_from_table() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE)),
                Some(true)
            );

            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.work_with(TABLE, |table| table.insert(vec![
                    Binary::pack(&[Datum::from_u64(1)]),
                    Binary::pack(&[Datum::from_u64(2)])
                ]))),
                Some(Some(2))
            );

            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema
                    .work_with(TABLE, |table| table.delete(vec![Binary::pack(&[Datum::from_u64(2)])]))),
                Some(Some(1))
            );

            assert_eq!(
                catalog_handle
                    .work_with(SCHEMA, |schema| schema.work_with(TABLE, |table| table.select()))
                    .unwrap()
                    .unwrap()
                    .collect::<Vec<(Key, Value)>>(),
                vec![(Binary::pack(&[Datum::from_u64(0)]), Binary::pack(&[Datum::from_u64(1)])),]
            );
        }

        #[test]
        fn update_table_that_in_schema_that_does_not_exist() {
            let catalog_handle = catalog();

            assert_eq!(
                catalog_handle.work_with(DOES_NOT_EXIST, |schema| schema
                    .work_with(TABLE, |table| table.update(vec![]))),
                None
            );
        }

        #[test]
        fn update_table_that_does_not_exist() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema
                    .work_with(DOES_NOT_EXIST, |table| table.update(vec![]))),
                Some(None)
            );
        }

        #[test]
        fn insert_update_scan_records_from_table() {
            let catalog_handle = catalog();

            assert_eq!(catalog_handle.create_schema(SCHEMA), true);
            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.create_table(TABLE)),
                Some(true)
            );

            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.work_with(TABLE, |table| table.insert(vec![
                    Binary::pack(&[Datum::from_u64(1)]),
                    Binary::pack(&[Datum::from_u64(2)])
                ]))),
                Some(Some(2))
            );

            assert_eq!(
                catalog_handle.work_with(SCHEMA, |schema| schema.work_with(TABLE, |table| table.update(vec![(
                    Binary::pack(&[Datum::from_u64(1)]),
                    Binary::pack(&[Datum::from_u64(4)])
                )]))),
                Some(Some(1))
            );

            assert_eq!(
                catalog_handle
                    .work_with(SCHEMA, |schema| schema.work_with(TABLE, |table| table.select()))
                    .unwrap()
                    .unwrap()
                    .collect::<Vec<(Key, Value)>>(),
                vec![
                    (Binary::pack(&[Datum::from_u64(0)]), Binary::pack(&[Datum::from_u64(1)])),
                    (Binary::pack(&[Datum::from_u64(1)]), Binary::pack(&[Datum::from_u64(4)])),
                ]
            );
        }
    }
}

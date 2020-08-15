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

use crate::on_disk::OnDiskStorage;
use crate::ColumnDefinition;
use crate::Values;
use kernel::{SystemError, SystemResult};
use std::{collections::HashMap, path::PathBuf, sync::RwLock};

const DEFAULT_CATALOG: &'_ str = "public";
// CREATE TABLE SCHEMATA (
//     CATALOG_NAME                    INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     SCHEMA_NAME                     INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     SCHEMA_OWNER                    INFORMATION_SCHEMA.SQL_IDENTIFIER
//                                     CONSTRAINT
//                                         SCHEMA_OWNER_NOT_NULL NOT NULL,
//     DEFAULT_CHARACTER_SET_CATALOG   INFORMATION_SCHEMA.SQL_IDENTIFIER
//                                     CONSTRAINT
//                                         DEFAULT_CHARACTER_SET_CATALOG_NOT_NULL NOT NULL,
//     DEFAULT_CHARACTER_SET_SCHEMA    INFORMATION_SCHEMA.SQL_IDENTIFIER
//                                     CONSTRAINT
//                                         DEFAULT_CHARACTER_SET_SCHEMA_NOT_NULL NOT NULL,
//     DEFAULT_CHARACTER_SET_NAME      INFORMATION_SCHEMA.SQL_IDENTIFIER
//                                     CONSTRAINT
//                                         DEFAULT_CHARACTER_SET_NAME_NOT_NULL NOT NULL,
//     SQL_PATH                        INFORMATION_SCHEMA.CHARACTER_DATA,
//
//     CONSTRAINT SCHEMATA_PRIMARY_KEY
//         PRIMARY KEY (CATALOG_NAME, SCHEMA_NAME),
//
//     CONSTRAINT SCHEMATA_FOREIGN_KEY_AUTHORIZATIONS
//         FOREIGN KEY (SCHEMA_OWNER)
//         REFERENCES AUTHORIZATIONS,
//
//     CONSTRAINT SCHEMATA_FOREIGN_KEY_CATALOG_NAMES
//         FOREIGN KEY (CATALOG_NAME)
//         REFERENCES CATALOG_NAMES
// )
const SCHEMATA_TABLE: &'_ str = "SCHEMATA";

// CREATE TABLE SEQUENCES (
//     SEQUENCE_CATALOG    INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     SEQUENCE_SCHEMA     INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     SEQUENCE_NAME       INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     DTD_IDENTIFIER      INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     MAXIMUM_VALUE       INFORMATION_SCHEMA.CHARACTER_DATA
//                         CONSTRAINT
//                             SEQUENCES_MAXIMUM_VALUE_NOT_NULL NOT NULL,
//     MINIMUM_VALUE       INFORMATION_SCHEMA.CHARACTER_DATA
//                         CONSTRAINT
//                             SEQUENCES_MINIMUM_VALUE_NOT_NULL NOT NULL,
//     INCREMENT           INFORMATION_SCHEMA.CHARACTER_DATA
//                         CONSTRAINT
//                             SEQUENCES_INCREMENT_NOT_NULL NOT NULL,
//     CYCLE_OPTION        INFORMATION_SCHEMA.CHARACTER_DATA
//                         CONSTRAINT
//                             SEQUENCES_CYCLE_OPTION_NOT_NULL NOT NULL,
//
//     CONSTRAINT SEQUENCES_PRIMARY_KEY
//         PRIMARY KEY (SEQUENCE_CATALOG, SEQUENCE_SCHEMA, SEQUENCE_NAME),
//
//     CONSTRAINT SEQUENCES_FOREIGN_KEY_SCHEMATA
//         FOREIGN KEY (SEQUENCES_CATALOG, SEQUENCES_SCHEMA)
//         REFERENCES SCHEMATA,
//
//     CONSTRAINT SEQUENCES_CYCLE_OPTION_CHECK
//         CHECK (CYCLE_OPTION IN ('YES', 'NO')),
//
//     CONSTRAINT SEQUENCES_CHECK_DATA_TYPE
//         CHECK (
//             (SEQUENCE_CATALOG, SEQUENCE_SCHEMA, SEQUENCE_NAME, 'SEQUENCE', DTD_IDENTIFIER)
//             IN ( SELECT OBJECT_CATALOG, OBJECT_SCHEMA, OBJECT_NAME, OBJECT_TYPE, DTD_IDENTIFIER FROM DATA_TYPE_DESCRIPTOR)
//         )
// )
const SEQUENCES_TABLE: &'_ str = "SEQUENCES";
// CREATE TABLE TABLES (
//     TABLE_CATALOG                               INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     TABLE_SCHEMA                                INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     TABLE_NAME                                  INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     TABLE_TYPE                                  INFORMATION_SCHEMA.CHARACTER_DATA
//                                                 CONSTRAINT
//                                                     TABLE_TYPE_NOT_NULL NOT NULL
//                                                 CONSTRAINT
//                                                     TABLE_TYPE_CHECK CHECK (
//                                                         TABLE_TYPE IN ('BASE TABLE', 'VIEW', 'GLOBAL TEMPORARY', 'LOCAL TEMPORARY')
//                                                     ),
//     SELF_REFERENCING_COLUMN_NAME                INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     REFERENCE_GENERATION                        INFORMATION_SCHEMA.CHARACTER_DATA
//                                                 CONSTRAINT
//                                                     REFERENCE_GENERATION_CHECK CHECK (
//                                                         REFERENCE_GENERATION IN ('SYSTEM GENERATED', 'USER GENERATED', 'DERIVED')
//                                                     ),
//     USER_DEFINED_TYPE_CATALOG                   INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     USER_DEFINED_TYPE_SCHEMA                    INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     USER_DEFINED_TYPE_NAME                      INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     IS_INSERTABLE_INTO                          INFORMATION_SCHEMA.CHARACTER_DATA
//                                                 CONSTRAINT
//                                                     IS_INSERTABLE_INTO_NOT_NULL NOT NULL
//                                                 CONSTRAINT
//                                                     IS_INSERTABLE_INTO_CHECK CHECK (
//                                                         IS_INSERTABLE_INTO IN ('YES', 'NO')
//                                                     )
//     IS_TYPED                                    INFORMATION_SCHEMA.CHARACTER_DATA
//                                                 CONSTRAINT
//                                                     IS_TYPED_NOT_NULL NOT NULL
//                                                 CONSTRAINT
//                                                     IS_TYPED_CHECK CHECK (IS_TYPED IN ('YES', 'NO'))
//     COMMIT_ACTION                               INFORMATION_SCHEMA.CHARACTER_DATA
//                                                 CONSTRAINT
//                                                     COMMIT_ACTIONCHECK CHECK (
//                                                         COMMIT_ACTION IN ('DELETE', 'PRESERVE')
//                                                     ),
//
//     CONSTRAINT TABLES_PRIMARY_KEY
//         PRIMARY KEY (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME),
//
//     CONSTRAINT TABLES_FOREIGN_KEY_SCHEMATA
//         FOREIGN KEY (TABLE_CATALOG, TABLE_SCHEMA)
//         REFERENCES SCHEMATA,
//
//     CONSTRAINT TABLES_CHECK_TABLE_IN_COLUMNS
//         CHECK (
//             (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) IN (SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME FROM COLUMNS)
//         ),
//
//     CONSTRAINT TABLES_FOREIGN_KEY_USER_DEFINED_TYPES
//         FOREIGN KEY (USER_DEFINED_TYPE_CATALOG, USER_DEFINED_TYPE_SCHEMA, USER_DEFINED_TYPE_NAME)
//         REFERENCES USER_DEFINED_TYPES MATCH FULL,
//
//     CONSTRAINT TABLES_TYPED_TABLE_CHECK CHECK (
//         (
//             IS_TYPED = 'YES'
//             AND (
//                 (USER_DEFINED_TYPE_CATALOG,
//                     USER_DEFINED_TYPE_SCHEMA,
//                     USER_DEFINED_TYPE_NAME,
//                     SELF_REFERENCING_COLUMN_NAME,
//                     REFERENCE_GENERATION) IS NOT NULL
//                 AND
//                 (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, SELF_REFERENCING_COLUMN_NAME) IN
//                     (SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM COLUMNS WHERE IS_SELF_REFERENCING = 'YES')
//             )
//         ) OR (
//             IS_TYPED = 'NO'
//             AND (
//                 USER_DEFINED_TYPE_CATALOG,
//                 USER_DEFINED_TYPE_SCHEMA,
//                 USER_DEFINED_TYPE_NAME,
//                 SELF_REFERENCING_COLUMN_NAME,
//                 REFERENCE_GENERATION ) IS NULL
//         )
//     ),
//
//     CONSTRAINT TABLES_SELF_REFERENCING_COLUMN_CHECK
//         CHECK (
//             (SELF_REFERENCING_COLUMN_NAME, REFERENCE_GENERATION) IS NULL
//             OR (SELF_REFERENCING_COLUMN_NAME, REFERENCE_GENERATION) IS NOT NULL
//         ),
//
//     CONSTRAINT TABLES_TEMPORARY_TABLE_CHECK
//         CHECK (
//             (TABLE_TYPE IN ('GLOBAL TEMPORARY', 'LOCAL TEMPORARY')) = (COMMIT_ACTION IS NOT NULL)
//         ),
//
//     CONSTRAINT TABLES_CHECK_NOT_VIEW
//         CHECK (
//             NOT EXISTS (
//                 SELECT  TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
//                 FROM    TABLES
//                 WHERE   TABLE_TYPE = 'VIEW'
//                 EXCEPT
//                 SELECT  TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
//                 FROM    VIEWS
//             )
//         )
// )
const TABLES_TABLE: &'_ str = "TABLES";

#[derive(Debug, PartialEq)]
pub enum StorageError {
    RuntimeCheckError,
    SystemError(SystemError),
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;

type RecordId = u8;

pub struct Storage {
    sequences: RwLock<HashMap<String, u8>>,
    schemas: RwLock<HashMap<String, u8>>,
    tables: RwLock<HashMap<String, u8>>,
    on_disk: Option<OnDiskStorage>,
}

impl Storage {
    pub fn persistent(path: PathBuf) -> SystemResult<Storage> {
        let (on_disk, new_storage) = OnDiskStorage::init(path)?;
        if new_storage {
            on_disk.create_meta_trees(&[SEQUENCES_TABLE, SCHEMATA_TABLE, TABLES_TABLE])?;
        }

        let sequences = on_disk
            .scan_meta_tree(SEQUENCES_TABLE)?
            .map(Result::unwrap)
            .map(|(key, values)| (String::from_utf8(values).unwrap(), u8::from_be_bytes([key[0]])))
            .collect();

        let schemas = on_disk
            .scan_meta_tree(SCHEMATA_TABLE)?
            .map(Result::unwrap)
            .map(|(key, values)| (String::from_utf8(values).unwrap(), u8::from_be_bytes([key[0]])))
            .collect();

        let tables = on_disk
            .scan_meta_tree(TABLES_TABLE)?
            .map(Result::unwrap)
            .map(|(key, values)| (String::from_utf8(values).unwrap(), u8::from_be_bytes([key[0]])))
            .collect();

        Ok(Storage {
            sequences: RwLock::new(sequences),
            schemas: RwLock::new(schemas),
            tables: RwLock::new(tables),
            on_disk: Some(on_disk),
        })
    }

    pub fn in_memory() -> Storage {
        Storage {
            sequences: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
            tables: RwLock::new(HashMap::new()),
            on_disk: None,
        }
    }

    fn next_record_id_in(&self, table_name: &str) -> RecordId {
        let mut sequences = self.sequences.write().expect("to acquire write lock");
        match sequences.get_mut(table_name) {
            Some(schema_sequence) => {
                let current_id = *schema_sequence;
                *schema_sequence += 1;
                current_id
            }
            None => {
                sequences.insert(table_name.to_owned(), 1);
                0
            }
        }
    }

    pub fn create_schema(&self, schema_name: &str) -> StorageResult<()> {
        match self.schema_exists(schema_name) {
            Some(_schema_id) => Err(StorageError::RuntimeCheckError),
            None => {
                let schema_id = self.next_record_id_in(SCHEMATA_TABLE);
                if let Some(on_disk) = self.on_disk.as_ref() {
                    match on_disk.write_to_meta_object(SCHEMATA_TABLE, &schema_id.to_be_bytes(), schema_name.as_bytes())
                    {
                        Ok(()) => {}
                        Err(error) => return Err(StorageError::SystemError(error)),
                    }
                }
                self.schemas
                    .write()
                    .expect("to acquire write lock")
                    .insert(schema_name.to_owned(), schema_id);
                Ok(())
            }
        }
    }

    pub fn schema_exists(&self, schema_name: &str) -> Option<u8> {
        self.schemas
            .read()
            .expect("to acquire read lock")
            .get(schema_name)
            .cloned()
    }

    pub fn drop_schema(&self, schema_name: &str) -> StorageResult<()> {
        match self.schemas.write().expect("to acquire write lock").remove(schema_name) {
            Some(schema_id) => {
                if let Some(on_disk) = self.on_disk.as_ref() {
                    match on_disk.delete_from_meta_object(SCHEMATA_TABLE, &schema_id.to_be_bytes()) {
                        Ok(()) => {}
                        Err(error) => return Err(StorageError::SystemError(error)),
                    }
                }
            }
            None => return Err(StorageError::RuntimeCheckError),
        }
        Ok(())
    }

    pub fn create_table(
        &self,
        schema_name: &str,
        table_name: &str,
        _column_definitions: &[ColumnDefinition],
    ) -> StorageResult<()> {
        match self.table_exists(schema_name, table_name) {
            Some(_table_id) => Err(StorageError::RuntimeCheckError),
            None => {
                let table_id = self.next_record_id_in(TABLES_TABLE);
                if let Some(on_disk) = self.on_disk.as_ref() {
                    match on_disk.write_to_meta_object(
                        TABLES_TABLE,
                        &table_id.to_be_bytes(),
                        (schema_name.to_owned() + "." + table_name).as_bytes(),
                    ) {
                        Ok(()) => {}
                        Err(error) => return Err(StorageError::SystemError(error)),
                    }
                }
                self.tables
                    .write()
                    .expect("to acquire write lock")
                    .insert(schema_name.to_owned() + "." + table_name, table_id);
                Ok(())
            }
        }
    }

    pub fn table_exists(&self, schema_name: &str, table_name: &str) -> Option<u8> {
        self.tables
            .read()
            .expect("to acquire read lock")
            .get(&(schema_name.to_owned() + "." + table_name))
            .cloned()
    }

    pub fn drop_table(&self, schema_name: &str, table_name: &str) -> StorageResult<()> {
        match self
            .tables
            .write()
            .expect("to acquire write lock")
            .remove(&(schema_name.to_owned() + "." + table_name))
        {
            Some(table_id) => {
                if let Some(on_disk) = self.on_disk.as_ref() {
                    match on_disk.delete_from_meta_object(TABLES_TABLE, &table_id.to_be_bytes()) {
                        Ok(()) => {},
                        Err(error) => return Err(StorageError::SystemError(error))
                    }
                }
            }
            None => return Err(StorageError::RuntimeCheckError)
        }
        Ok(())
    }

    pub fn insert_into(&self, schema_name: &str, table_name: &str, values: Vec<Values>) -> StorageResult<usize> {
        Ok(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::{let_assert, assert};
    use sql_types::SqlType;
    use representation::{Binary, Datum};

    #[rstest::fixture]
    fn in_memory() -> Storage {
        Storage::in_memory()
    }

    #[rstest::rstest]
    fn create_schema(in_memory: Storage) {
        in_memory.create_schema("schema_name").expect("schema created");

        let_assert!(Some(_) = in_memory.schema_exists("schema_name"));
    }

    #[rstest::rstest]
    fn create_schema_with_same_name(in_memory: Storage) {
        in_memory.create_schema("schema_name").expect("schema created");

        let_assert!(Err(StorageError::RuntimeCheckError) = in_memory.create_schema("schema_name"));
    }

    #[rstest::rstest]
    fn recreate_schema_with_same_name(in_memory: Storage) {
        in_memory.create_schema("schema_name").expect("schema created");
        let schema_id = in_memory.schema_exists("schema_name");

        in_memory.drop_schema("schema_name").expect("schema dropped");

        in_memory.create_schema("schema_name").expect("schema created");

        assert!(in_memory.schema_exists("schema_name") != schema_id);
    }

    #[rstest::rstest]
    fn drop_schema(in_memory: Storage) {
        in_memory.create_schema("schema_name").expect("schema created");

        let_assert!(Some(_) = in_memory.schema_exists("schema_name"));

        in_memory.drop_schema("schema_name").expect("schema dropped");

        assert!(in_memory.schema_exists("schema_name") == None);
    }

    #[rstest::rstest]
    fn drop_non_existent_schema(in_memory: Storage) {
        assert!(in_memory.drop_schema("non_existent") == Err(StorageError::RuntimeCheckError));
    }

    #[rstest::rstest]
    fn create_table(in_memory: Storage) {
        in_memory.create_schema("schema_name").expect("schema created");
        in_memory
            .create_table("schema_name", "table_name", &[])
            .expect("table created");

        let_assert!(Some(_) = in_memory.table_exists("schema_name", "table_name"));
    }

    #[rstest::rstest]
    fn create_table_with_the_same_name(in_memory: Storage) {
        in_memory.create_schema("schema_name").expect("schema created");
        in_memory
            .create_table("schema_name", "table_name", &[])
            .expect("table created");

        let_assert!(Err(StorageError::RuntimeCheckError) = in_memory.create_table("schema_name", "table_name", &[]));
    }

    #[rstest::rstest]
    fn recreate_table_with_the_same_name(in_memory: Storage) {
        in_memory.create_schema("schema_name").expect("schema created");
        in_memory
            .create_table("schema_name", "table_name", &[])
            .expect("table created");

        let table_id = in_memory.table_exists("schema_name", "table_name");

        in_memory.drop_table("schema_name", "table_name").expect("table dropped");

        in_memory.create_table("schema_name", "table_name", &[]).expect("table created");

        assert!(in_memory.table_exists("schema_name", "table_name") != table_id);
    }

    #[rstest::rstest]
    fn drop_table(in_memory: Storage) {
        in_memory.create_schema("schema_name").expect("schema created");
        in_memory
            .create_table("schema_name", "table_name", &[])
            .expect("table created");

        let_assert!(Some(_) = in_memory.table_exists("schema_name", "table_name"));

        in_memory
            .drop_table("schema_name", "table_name")
            .expect("table dropped");

        assert!(in_memory.table_exists("schema_name", "table_name") == None);
    }

    #[rstest::rstest]
    fn drop_non_existent_table(in_memory: Storage) {
        in_memory.create_schema("schema_name").expect("schema created");
        let_assert!(Err(StorageError::RuntimeCheckError) = in_memory.drop_table("schema_name", "non_existent_table_name"));
    }

    #[rstest::rstest]
    fn insert_data_into_table(in_memory: Storage) {
        in_memory.create_schema("schema_name").expect("schema created");
        in_memory.create_table(
            "schema_name",
            "table_name",
            &[ColumnDefinition::new("col_1", SqlType::Integer(i32::max_value()))]
        ).expect("table created");

        let_assert!(Ok(1) = in_memory.insert_into("schema_name", "table_name", vec![Binary::pack(&[Datum::Int32(1)])]));
    }

    #[cfg(test)]
    mod persistent {
        use super::*;
        use tempfile::TempDir;
        use assert2::assert;

        #[rstest::fixture]
        fn root_path() -> TempDir {
            tempfile::tempdir().expect("To create temporary folder")
        }

        #[rstest::fixture]
        fn storage_and_path(root_path: TempDir) -> (Storage, TempDir) {
            (
                Storage::persistent(root_path.path().into()).expect("To initialize storage"),
                root_path,
            )
        }

        #[rstest::rstest]
        fn storage_preserve_created_schema_after_restart(storage_and_path: (Storage, TempDir)) {
            let (storage, root_path) = storage_and_path;
            storage.create_schema("schema_name").expect("To create schema");
            drop(storage);

            let storage = Storage::persistent(PathBuf::from(root_path.path())).expect("To initialize storage");
            let_assert!(Some(_) = storage.schema_exists("schema_name"));
        }

        #[rstest::rstest]
        fn dropped_schema_should_not_be_restored(storage_and_path: (Storage, TempDir)) {
            let (storage, root_path) = storage_and_path;
            storage.create_schema("schema_name").expect("To create schema");
            drop(storage);

            let storage = Storage::persistent(PathBuf::from(root_path.path())).expect("To initialize storage");
            let_assert!(Some(_) = storage.schema_exists("schema_name"));
            let_assert!(Ok(()) = storage.drop_schema("schema_name"));
            assert!(storage.schema_exists("schema_name") == None);
            drop(storage);

            let storage = Storage::persistent(PathBuf::from(root_path.path())).expect("To initialize storage");
            assert!(storage.schema_exists("schema_name") == None);
        }

        #[rstest::rstest]
        fn storage_preserve_created_table_after_restart(storage_and_path: (Storage, TempDir)) {
            let (storage, root_path) = storage_and_path;
            storage.create_schema("schema_name").expect("To create schema");
            storage
                .create_table("schema_name", "table_name", &[])
                .expect("To create table");
            let_assert!(Some(_) = storage.table_exists("schema_name", "table_name"));
            drop(storage);

            let storage = Storage::persistent(PathBuf::from(root_path.path())).expect("To initialize storage");
            let_assert!(Some(_) = storage.table_exists("schema_name", "table_name"));
        }

        #[rstest::rstest]
        fn dropped_table_should_not_be_restored_after_restart(storage_and_path: (Storage, TempDir)) {
            let (storage, root_path) = storage_and_path;
            storage.create_schema("schema_name").expect("To create schema");
            storage
                .create_table("schema_name", "table_name", &[])
                .expect("To create table");
            let_assert!(Some(_) = storage.table_exists("schema_name", "table_name"));
            drop(storage);

            let storage = Storage::persistent(PathBuf::from(root_path.path())).expect("To initialize storage");
            let_assert!(Some(_) = storage.table_exists("schema_name", "table_name"));
            let_assert!(Ok(()) = storage.drop_table("schema_name", "table_name"));
            assert!(storage.table_exists("schema_name", "table_name") == None);
            drop(storage);

            let storage = Storage::persistent(PathBuf::from(root_path.path())).expect("To initialize storage");
            assert!(storage.table_exists("schema_name", "table_name") == None);
            drop(storage);
        }
    }
}

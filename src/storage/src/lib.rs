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

extern crate kernel;
extern crate log;
extern crate sql_types;

use kernel::SystemError;
use representation::Binary;
use serde::{Deserialize, Serialize};
use sled::{Db as KeySpace, Error as SledError};
use sql_types::{ConstraintError, SqlType};
use std::collections::HashMap;

pub type Row = (Key, Values);
pub type Key = Binary;
pub type Values = Binary;
pub type ReadCursor = Box<dyn Iterator<Item = Result<Row, SystemError>>>;
pub type StorageResult<T> = std::result::Result<T, StorageError>;

#[derive(Debug, PartialEq)]
pub enum StorageError {
    RuntimeCheckError,
    SystemError(SystemError),
}

pub trait DatabaseCatalog {
    fn create_namespace_with_objects(&mut self, namespace: &str, object_names: Vec<&str>) -> StorageResult<()>;

    fn create_namespace(&mut self, namespace: &str) -> StorageResult<()>;

    fn drop_namespace(&mut self, namespace: &str) -> StorageResult<()>;

    fn is_namespace_exists(&self, namespace: &str) -> bool;

    fn create_tree(&mut self, namespace: &str, object_name: &str) -> StorageResult<()>;

    fn drop_tree(&mut self, namespace: &str, object_name: &str) -> StorageResult<()>;

    fn is_tree_exists(&self, namespace: &str, object_name: &str) -> bool;

    fn write(&mut self, namespace: &str, object_name: &str, values: Vec<Row>) -> StorageResult<usize>;

    fn read(&self, namespace: &str, object_name: &str) -> StorageResult<ReadCursor>;

    fn delete(&mut self, namespace: &str, object_name: &str, keys: Vec<Key>) -> StorageResult<usize>;

    fn check_for_object(&self, namespace: &str, object_name: &str) -> StorageResult<()>;
}

pub struct SledErrorMapper;

impl SledErrorMapper {
    fn map(error: SledError) -> SystemError {
        match error {
            SledError::CollectionNotFound(system_file) => SystemError::unrecoverable(format!(
                "System file [{}] can't be found",
                String::from_utf8(system_file.to_vec()).expect("name of system file")
            )),
            SledError::Unsupported(operation) => {
                SystemError::unrecoverable(format!("Unsupported operation [{}] was used on Sled", operation))
            }
            SledError::Corruption { at, bt: _bt } => {
                if let Some(at) = at {
                    SystemError::unrecoverable(format!("Sled encountered corruption at {}", at))
                } else {
                    SystemError::unrecoverable("Sled encountered corruption".to_owned())
                }
            }
            SledError::ReportableBug(description) => {
                SystemError::unrecoverable(format!("Sled encountered reportable BUG: {}", description))
            }
            SledError::Io(error) => SystemError::io(error),
        }
    }
}

#[derive(Default)]
pub struct SledBackendStorage {
    namespaces: HashMap<String, sled::Db>,
}

impl SledBackendStorage {
    fn new_namespace(&mut self, namespace: &str) -> StorageResult<&mut sled::Db> {
        if self.namespaces.contains_key(namespace) {
            Err(StorageError::RuntimeCheckError)
        } else {
            match sled::Config::default().temporary(true).open() {
                Ok(database) => {
                    let database = self.namespaces.entry(namespace.to_owned()).or_insert(database);
                    Ok(database)
                }
                Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
            }
        }
    }
}

impl DatabaseCatalog for SledBackendStorage {
    fn create_namespace_with_objects(&mut self, namespace: &str, object_names: Vec<&str>) -> StorageResult<()> {
        let namespace = self.new_namespace(namespace)?;
        for object_name in object_names {
            match namespace.open_tree(object_name) {
                Ok(_object) => (),
                Err(error) => return Err(StorageError::SystemError(SledErrorMapper::map(error))),
            }
        }
        Ok(())
    }

    fn create_namespace(&mut self, namespace: &str) -> StorageResult<()> {
        self.new_namespace(namespace).map(|_| ())
    }

    fn drop_namespace(&mut self, namespace: &str) -> StorageResult<()> {
        match self.namespaces.remove(namespace) {
            Some(namespace) => {
                drop(namespace);
                Ok(())
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn create_tree(&mut self, namespace: &str, object_name: &str) -> StorageResult<()> {
        match self.namespaces.get(namespace) {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    Err(StorageError::RuntimeCheckError)
                } else {
                    match namespace.open_tree(object_name) {
                        Ok(_object) => Ok(()),
                        Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
                    }
                }
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn drop_tree(&mut self, namespace: &str, object_name: &str) -> StorageResult<()> {
        match self.namespaces.get(namespace) {
            Some(namespace) => match namespace.drop_tree(object_name.as_bytes()) {
                Ok(true) => Ok(()),
                Ok(false) => Err(StorageError::RuntimeCheckError),
                Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
            },
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn write(&mut self, namespace: &str, object_name: &str, rows: Vec<Row>) -> StorageResult<usize> {
        match self.namespaces.get(namespace) {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    match namespace.open_tree(object_name) {
                        Ok(object) => {
                            let mut written_rows = 0;
                            for (key, values) in rows {
                                match object
                                    .insert::<sled::IVec, sled::IVec>(key.to_bytes().into(), values.to_bytes().into())
                                {
                                    Ok(_) => written_rows += 1,
                                    Err(error) => return Err(StorageError::SystemError(SledErrorMapper::map(error))),
                                }
                            }
                            Ok(written_rows)
                        }
                        Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
                    }
                } else {
                    Err(StorageError::RuntimeCheckError)
                }
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn read(&self, namespace: &str, object_name: &str) -> StorageResult<ReadCursor> {
        match self.namespaces.get(namespace) {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    match namespace.open_tree(object_name) {
                        Ok(object) => Ok(Box::new(object.iter().map(|item| match item {
                            Ok((key, values)) => {
                                Ok((Binary::with_data(key.to_vec()), Binary::with_data(values.to_vec())))
                            }
                            Err(error) => Err(SledErrorMapper::map(error)),
                        }))),
                        Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
                    }
                } else {
                    Err(StorageError::RuntimeCheckError)
                }
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn delete(&mut self, namespace: &str, object_name: &str, keys: Vec<Key>) -> StorageResult<usize> {
        match self.namespaces.get(namespace) {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    let mut deleted = 0;
                    match namespace.open_tree(object_name) {
                        Ok(object) => {
                            for key in keys {
                                match object.remove(key.to_bytes()) {
                                    Ok(_) => deleted += 1,
                                    Err(error) => return Err(StorageError::SystemError(SledErrorMapper::map(error))),
                                }
                            }
                        }
                        Err(error) => return Err(StorageError::SystemError(SledErrorMapper::map(error))),
                    }
                    Ok(deleted)
                } else {
                    Err(StorageError::RuntimeCheckError)
                }
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn is_namespace_exists(&self, namespace: &str) -> bool {
        self.namespaces.contains_key(namespace)
    }

    fn is_tree_exists(&self, namespace: &str, object_name: &str) -> bool {
        self.check_for_object(namespace, object_name).is_ok()
    }

    fn check_for_object(&self, namespace: &str, object_name: &str) -> StorageResult<()> {
        match self.namespaces.get(namespace) {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    Ok(())
                } else {
                    Err(StorageError::RuntimeCheckError)
                }
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernel::SystemResult;

    type Storage = SledBackendStorage;

    #[rstest::fixture]
    fn storage() -> Storage {
        Storage::default()
    }

    #[rstest::fixture]
    fn with_namespace(mut storage: Storage) -> Storage {
        storage.create_namespace("namespace").expect("namespace created");
        storage
    }

    #[rstest::fixture]
    fn with_object(mut with_namespace: Storage) -> Storage {
        with_namespace
            .create_tree("namespace", "object_name")
            .expect("object created");
        with_namespace
    }

    #[cfg(test)]
    mod sled_error_mapper {
        use super::*;
        use sled::DiskPtr;
        use std::io::{Error, ErrorKind};

        #[test]
        fn collection_not_found() {
            assert_eq!(
                SledErrorMapper::map(SledError::CollectionNotFound(sled::IVec::from("test"))),
                SystemError::unrecoverable("System file [test] can't be found".to_owned())
            )
        }

        #[test]
        fn unsupported() {
            assert_eq!(
                SledErrorMapper::map(SledError::Unsupported("NOT_SUPPORTED".to_owned())),
                SystemError::unrecoverable("Unsupported operation [NOT_SUPPORTED] was used on Sled".to_owned())
            )
        }

        #[test]
        fn corruption_with_position() {
            let at = DiskPtr::Inline(900);
            assert_eq!(
                SledErrorMapper::map(SledError::Corruption { at: Some(at), bt: () }),
                SystemError::unrecoverable(format!("Sled encountered corruption at {}", at))
            )
        }

        #[test]
        fn corruption_without_position() {
            assert_eq!(
                SledErrorMapper::map(SledError::Corruption { at: None, bt: () }),
                SystemError::unrecoverable("Sled encountered corruption".to_owned())
            )
        }

        #[test]
        fn reportable_bug() {
            let description = "SOME_BUG_HERE";
            assert_eq!(
                SledErrorMapper::map(SledError::ReportableBug(description.to_owned())),
                SystemError::unrecoverable(format!("Sled encountered reportable BUG: {}", description))
            );
        }

        #[test]
        fn io() {
            assert_eq!(
                SledErrorMapper::map(SledError::Io(Error::new(ErrorKind::Other, "oh no!"))),
                SystemError::io(Error::new(ErrorKind::Other, "oh no!"))
            )
        }
    }

    #[cfg(test)]
    mod namespace {
        use super::*;

        #[rstest::rstest]
        fn create_namespace_with_objects(mut storage: Storage) {
            assert_eq!(
                storage.create_namespace_with_objects("namespace", vec!["object_1", "object_2"]),
                Ok(())
            );

            assert!(storage.is_tree_exists("namespace", "object_1"));
            assert!(storage.is_tree_exists("namespace", "object_2"));
        }

        #[rstest::rstest]
        fn create_namespaces_with_different_names(mut storage: Storage) {
            assert_eq!(storage.create_namespace("namespace_1"), Ok(()));
            assert_eq!(storage.create_namespace("namespace_2"), Ok(()));
        }

        #[rstest::rstest]
        fn drop_namespace(mut with_namespace: Storage) {
            assert_eq!(with_namespace.drop_namespace("namespace"), Ok(()));
            assert_eq!(with_namespace.create_namespace("namespace"), Ok(()));
        }

        #[rstest::rstest]
        fn dropping_namespace_drops_objects_in_it(mut with_namespace: Storage) {
            with_namespace
                .create_tree("namespace", "object_name_1")
                .expect("object created");
            with_namespace
                .create_tree("namespace", "object_name_2")
                .expect("object created");

            assert_eq!(with_namespace.drop_namespace("namespace"), Ok(()));
            assert_eq!(with_namespace.create_namespace("namespace"), Ok(()));
            assert_eq!(with_namespace.create_tree("namespace", "object_name_1"), Ok(()));
            assert_eq!(with_namespace.create_tree("namespace", "object_name_2"), Ok(()));
        }
    }

    #[cfg(test)]
    mod create_object {
        use super::*;

        #[rstest::rstest]
        fn create_objects_with_different_names(mut with_namespace: Storage) {
            assert_eq!(with_namespace.create_tree("namespace", "object_name_1"), Ok(()));
            assert_eq!(with_namespace.create_tree("namespace", "object_name_2"), Ok(()));
        }

        #[rstest::rstest]
        fn create_object_with_the_same_name_in_different_namespaces(mut storage: Storage) {
            storage.create_namespace("namespace_1").expect("namespace created");
            storage.create_namespace("namespace_2").expect("namespace created");
            assert_eq!(storage.create_tree("namespace_1", "object_name"), Ok(()));
            assert_eq!(storage.create_tree("namespace_2", "object_name"), Ok(()));
        }
    }

    #[cfg(test)]
    mod drop_object {
        use super::*;

        #[rstest::rstest]
        fn drop_object(mut with_object: Storage) {
            assert_eq!(with_object.drop_tree("namespace", "object_name"), Ok(()));
            assert_eq!(with_object.create_tree("namespace", "object_name"), Ok(()));
        }
    }

    #[cfg(test)]
    mod operations_on_object {
        use super::*;

        #[rstest::rstest]
        fn insert_row_into_object(mut with_object: Storage) {
            assert_eq!(
                with_object.write("namespace", "object_name", as_rows(vec![(1u8, vec!["123"])])),
                Ok(1)
            );

            assert_eq!(
                with_object
                    .read("namespace", "object_name")
                    .map(|iter| iter.collect::<Vec<SystemResult<Row>>>()),
                Ok(as_read_cursor(vec![(1u8, vec!["123"])]).collect())
            );
        }

        #[rstest::rstest]
        fn insert_many_rows_into_object(mut with_object: Storage) {
            with_object
                .write("namespace", "object_name", as_rows(vec![(1u8, vec!["123"])]))
                .expect("values are written");
            with_object
                .write("namespace", "object_name", as_rows(vec![(2u8, vec!["456"])]))
                .expect("values are written");

            assert_eq!(
                with_object
                    .read("namespace", "object_name")
                    .map(|iter| iter.collect::<Vec<SystemResult<Row>>>()),
                Ok(as_read_cursor(vec![(1u8, vec!["123"]), (2u8, vec!["456"])]).collect())
            );
        }

        #[rstest::rstest]
        fn delete_some_records_from_object(mut with_object: Storage) {
            with_object
                .write(
                    "namespace",
                    "object_name",
                    as_rows(vec![(1u8, vec!["123"]), (2u8, vec!["456"]), (3u8, vec!["789"])]),
                )
                .expect("write occurred");

            assert_eq!(
                with_object.delete("namespace", "object_name", as_keys(vec![2u8])),
                Ok(1)
            );

            assert_eq!(
                with_object
                    .read("namespace", "object_name")
                    .map(|iter| iter.collect::<Vec<SystemResult<Row>>>()),
                Ok(as_read_cursor(vec![(1u8, vec!["123"]), (3u8, vec!["789"])]).collect())
            );
        }

        #[rstest::rstest]
        fn select_all_from_object_with_many_columns(mut with_object: Storage) {
            with_object
                .write("namespace", "object_name", as_rows(vec![(1u8, vec!["1", "2", "3"])]))
                .expect("write occurred");

            assert_eq!(
                with_object
                    .read("namespace", "object_name")
                    .map(|iter| iter.collect::<Vec<SystemResult<Row>>>()),
                Ok(as_read_cursor(vec![(1u8, vec!["1", "2", "3"])]).collect())
            );
        }

        #[rstest::rstest]
        fn insert_multiple_rows(mut with_object: Storage) {
            with_object
                .write(
                    "namespace",
                    "object_name",
                    as_rows(vec![
                        (1u8, vec!["1", "2", "3"]),
                        (2u8, vec!["4", "5", "6"]),
                        (3u8, vec!["7", "8", "9"]),
                    ]),
                )
                .expect("write occurred");

            assert_eq!(
                with_object
                    .read("namespace", "object_name")
                    .map(|iter| iter.collect::<Vec<SystemResult<Row>>>()),
                Ok(as_read_cursor(vec![
                    (1u8, vec!["1", "2", "3"]),
                    (2u8, vec!["4", "5", "6"]),
                    (3u8, vec!["7", "8", "9"])
                ])
                .collect()),
            );
        }
    }

    fn as_rows(items: Vec<(u8, Vec<&'static str>)>) -> Vec<Row> {
        items
            .into_iter()
            .map(|(key, values)| {
                let k = Binary::with_data(key.to_be_bytes().to_vec());
                let v = Binary::with_data(
                    values
                        .into_iter()
                        .map(|s| s.as_bytes())
                        .collect::<Vec<&[u8]>>()
                        .join(&b'|'),
                );
                (k, v)
            })
            .collect()
    }

    fn as_keys(items: Vec<u8>) -> Vec<Key> {
        items
            .into_iter()
            .map(|key| Binary::with_data(key.to_be_bytes().to_vec()))
            .collect()
    }

    fn as_read_cursor(items: Vec<(u8, Vec<&'static str>)>) -> ReadCursor {
        Box::new(items.into_iter().map(|(key, values)| {
            let k = key.to_be_bytes().to_vec();
            let v = values
                .into_iter()
                .map(|s| s.as_bytes())
                .collect::<Vec<&[u8]>>()
                .join(&b'|');
            Ok((Binary::with_data(k), Binary::with_data(v)))
        }))
    }
}

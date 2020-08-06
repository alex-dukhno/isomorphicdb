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

use crate::{Key, ReadCursor, Row};
use kernel::SystemError;
use representation::Binary;
use std::{collections::HashMap, fmt::Debug};

#[derive(Debug, PartialEq)]
pub enum BackendError {
    RuntimeCheckError,
    SystemError(SystemError),
}

pub type BackendResult<T> = std::result::Result<T, BackendError>;

pub trait BackendStorage {
    type ErrorMapper: StorageErrorMapper;

    fn create_namespace_with_objects(&mut self, namespace: &str, object_names: Vec<&str>) -> BackendResult<()>;

    fn create_namespace(&mut self, namespace: &str) -> BackendResult<()>;

    fn drop_namespace(&mut self, namespace: &str) -> BackendResult<()>;

    fn create_object(&mut self, namespace: &str, object_name: &str) -> BackendResult<()>;

    fn drop_object(&mut self, namespace: &str, object_name: &str) -> BackendResult<()>;

    fn write(&mut self, namespace: &str, object_name: &str, values: Vec<Row>) -> BackendResult<usize>;

    fn read(&self, namespace: &str, object_name: &str) -> BackendResult<ReadCursor>;

    fn delete(&mut self, namespace: &str, object_name: &str, keys: Vec<Key>) -> BackendResult<usize>;

    fn is_object_exists(&self, namespace: &str, object_name: &str) -> bool;

    fn is_namespace_exists(&self, namespace: &str) -> bool;

    fn check_for_object(&self, namespace: &str, object_name: &str) -> BackendResult<()>;
}

pub trait StorageErrorMapper {
    type Error;

    fn map(error: Self::Error) -> kernel::SystemError;
}

pub struct SledErrorMapper;

impl StorageErrorMapper for SledErrorMapper {
    type Error = sled::Error;

    fn map(error: Self::Error) -> SystemError {
        match error {
            sled::Error::CollectionNotFound(system_file) => SystemError::unrecoverable(format!(
                "System file [{}] can't be found",
                String::from_utf8(system_file.to_vec()).expect("name of system file")
            )),
            sled::Error::Unsupported(operation) => {
                SystemError::unrecoverable(format!("Unsupported operation [{}] was used on Sled", operation))
            }
            sled::Error::Corruption { at, bt: _bt } => {
                if let Some(at) = at {
                    SystemError::unrecoverable(format!("Sled encountered corruption at {}", at))
                } else {
                    SystemError::unrecoverable("Sled encountered corruption".to_owned())
                }
            }
            sled::Error::ReportableBug(description) => {
                SystemError::unrecoverable(format!("Sled encountered reportable BUG: {}", description))
            }
            sled::Error::Io(error) => SystemError::io(error),
        }
    }
}

#[derive(Default)]
pub struct SledBackendStorage {
    namespaces: HashMap<String, sled::Db>,
}

impl SledBackendStorage {
    fn new_namespace(&mut self, namespace: &str) -> BackendResult<&mut sled::Db> {
        if self.namespaces.contains_key(namespace) {
            Err(BackendError::RuntimeCheckError)
        } else {
            match sled::Config::default().temporary(true).open() {
                Ok(database) => {
                    let database = self.namespaces.entry(namespace.to_owned()).or_insert(database);
                    Ok(database)
                }
                Err(error) => Err(BackendError::SystemError(SledErrorMapper::map(error))),
            }
        }
    }
}

impl BackendStorage for SledBackendStorage {
    type ErrorMapper = SledErrorMapper;

    fn create_namespace_with_objects(&mut self, namespace: &str, object_names: Vec<&str>) -> BackendResult<()> {
        let namespace = self.new_namespace(namespace)?;
        for object_name in object_names {
            match namespace.open_tree(object_name) {
                Ok(_object) => (),
                Err(error) => return Err(BackendError::SystemError(Self::ErrorMapper::map(error))),
            }
        }
        Ok(())
    }

    fn create_namespace(&mut self, namespace: &str) -> BackendResult<()> {
        self.new_namespace(namespace).map(|_| ())
    }

    fn drop_namespace(&mut self, namespace: &str) -> BackendResult<()> {
        match self.namespaces.remove(namespace) {
            Some(namespace) => {
                drop(namespace);
                Ok(())
            }
            None => Err(BackendError::RuntimeCheckError),
        }
    }

    fn create_object(&mut self, namespace: &str, object_name: &str) -> BackendResult<()> {
        match self.namespaces.get(namespace) {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    Err(BackendError::RuntimeCheckError)
                } else {
                    match namespace.open_tree(object_name) {
                        Ok(_object) => Ok(()),
                        Err(error) => Err(BackendError::SystemError(Self::ErrorMapper::map(error))),
                    }
                }
            }
            None => Err(BackendError::RuntimeCheckError),
        }
    }

    fn drop_object(&mut self, namespace: &str, object_name: &str) -> BackendResult<()> {
        match self.namespaces.get(namespace) {
            Some(namespace) => match namespace.drop_tree(object_name.as_bytes()) {
                Ok(true) => Ok(()),
                Ok(false) => Err(BackendError::RuntimeCheckError),
                Err(error) => Err(BackendError::SystemError(Self::ErrorMapper::map(error))),
            },
            None => Err(BackendError::RuntimeCheckError),
        }
    }

    fn write(&mut self, namespace: &str, object_name: &str, rows: Vec<Row>) -> BackendResult<usize> {
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
                                    Err(error) => return Err(BackendError::SystemError(Self::ErrorMapper::map(error))),
                                }
                            }
                            Ok(written_rows)
                        }
                        Err(error) => Err(BackendError::SystemError(Self::ErrorMapper::map(error))),
                    }
                } else {
                    Err(BackendError::RuntimeCheckError)
                }
            }
            None => Err(BackendError::RuntimeCheckError),
        }
    }

    fn read(&self, namespace: &str, object_name: &str) -> BackendResult<ReadCursor> {
        match self.namespaces.get(namespace) {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    match namespace.open_tree(object_name) {
                        Ok(object) => Ok(Box::new(object.iter().map(|item| match item {
                            Ok((key, values)) => {
                                Ok((Binary::with_data(key.to_vec()), Binary::with_data(values.to_vec())))
                            }
                            Err(error) => Err(Self::ErrorMapper::map(error)),
                        }))),
                        Err(error) => Err(BackendError::SystemError(Self::ErrorMapper::map(error))),
                    }
                } else {
                    Err(BackendError::RuntimeCheckError)
                }
            }
            None => Err(BackendError::RuntimeCheckError),
        }
    }

    fn delete(&mut self, namespace: &str, object_name: &str, keys: Vec<Key>) -> BackendResult<usize> {
        match self.namespaces.get(namespace) {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    let mut deleted = 0;
                    match namespace.open_tree(object_name) {
                        Ok(object) => {
                            for key in keys {
                                match object.remove(key.to_bytes()) {
                                    Ok(_) => deleted += 1,
                                    Err(error) => return Err(BackendError::SystemError(Self::ErrorMapper::map(error))),
                                }
                            }
                        }
                        Err(error) => return Err(BackendError::SystemError(Self::ErrorMapper::map(error))),
                    }
                    Ok(deleted)
                } else {
                    Err(BackendError::RuntimeCheckError)
                }
            }
            None => Err(BackendError::RuntimeCheckError),
        }
    }

    fn is_namespace_exists(&self, namespace: &str) -> bool {
        self.namespaces.contains_key(namespace)
    }

    fn is_object_exists(&self, namespace: &str, object_name: &str) -> bool {
        self.check_for_object(namespace, object_name).is_ok()
    }

    fn check_for_object(&self, namespace: &str, object_name: &str) -> BackendResult<()> {
        match self.namespaces.get(namespace) {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    Ok(())
                } else {
                    Err(BackendError::RuntimeCheckError)
                }
            }
            None => Err(BackendError::RuntimeCheckError),
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
            .create_object("namespace", "object_name")
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
                SledErrorMapper::map(sled::Error::CollectionNotFound(sled::IVec::from("test"))),
                SystemError::unrecoverable("System file [test] can't be found".to_owned())
            )
        }

        #[test]
        fn unsupported() {
            assert_eq!(
                SledErrorMapper::map(sled::Error::Unsupported("NOT_SUPPORTED".to_owned())),
                SystemError::unrecoverable("Unsupported operation [NOT_SUPPORTED] was used on Sled".to_owned())
            )
        }

        #[test]
        fn corruption_with_position() {
            let at = DiskPtr::Inline(900);
            assert_eq!(
                SledErrorMapper::map(sled::Error::Corruption { at: Some(at), bt: () }),
                SystemError::unrecoverable(format!("Sled encountered corruption at {}", at))
            )
        }

        #[test]
        fn corruption_without_position() {
            assert_eq!(
                SledErrorMapper::map(sled::Error::Corruption { at: None, bt: () }),
                SystemError::unrecoverable("Sled encountered corruption".to_owned())
            )
        }

        #[test]
        fn reportable_bug() {
            let description = "SOME_BUG_HERE";
            assert_eq!(
                SledErrorMapper::map(sled::Error::ReportableBug(description.to_owned())),
                SystemError::unrecoverable(format!("Sled encountered reportable BUG: {}", description))
            );
        }

        #[test]
        fn io() {
            assert_eq!(
                SledErrorMapper::map(sled::Error::Io(Error::new(ErrorKind::Other, "oh no!"))),
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

            assert!(storage.is_object_exists("namespace", "object_1"));
            assert!(storage.is_object_exists("namespace", "object_2"));
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
                .create_object("namespace", "object_name_1")
                .expect("object created");
            with_namespace
                .create_object("namespace", "object_name_2")
                .expect("object created");

            assert_eq!(with_namespace.drop_namespace("namespace"), Ok(()));
            assert_eq!(with_namespace.create_namespace("namespace"), Ok(()));
            assert_eq!(with_namespace.create_object("namespace", "object_name_1"), Ok(()));
            assert_eq!(with_namespace.create_object("namespace", "object_name_2"), Ok(()));
        }
    }

    #[cfg(test)]
    mod create_object {
        use super::*;

        #[rstest::rstest]
        fn create_objects_with_different_names(mut with_namespace: Storage) {
            assert_eq!(with_namespace.create_object("namespace", "object_name_1"), Ok(()));
            assert_eq!(with_namespace.create_object("namespace", "object_name_2"), Ok(()));
        }

        #[rstest::rstest]
        fn create_object_with_the_same_name_in_different_namespaces(mut storage: Storage) {
            storage.create_namespace("namespace_1").expect("namespace created");
            storage.create_namespace("namespace_2").expect("namespace created");
            assert_eq!(storage.create_object("namespace_1", "object_name"), Ok(()));
            assert_eq!(storage.create_object("namespace_2", "object_name"), Ok(()));
        }
    }

    #[cfg(test)]
    mod drop_object {
        use super::*;

        #[rstest::rstest]
        fn drop_object(mut with_object: Storage) {
            assert_eq!(with_object.drop_object("namespace", "object_name"), Ok(()));
            assert_eq!(with_object.create_object("namespace", "object_name"), Ok(()));
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

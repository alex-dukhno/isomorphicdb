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

use kernel::SystemResult;
use std::collections::{BTreeMap, HashMap};
use storage::backend::{BackendError, BackendResult};
use storage::{backend::BackendStorage, Key, ReadCursor, Row, Values};

#[derive(Default, Debug)]
struct StorageObject {
    records: BTreeMap<Key, Values>,
}

#[derive(Default, Debug)]
struct Namespace {
    pub objects: HashMap<String, StorageObject>,
}

#[derive(Default)]
pub struct InMemoryStorage {
    namespaces: HashMap<String, Namespace>,
}

impl BackendStorage for InMemoryStorage {
    type ErrorMapper = storage::backend::SledErrorMapper;

    fn create_namespace_with_objects(&mut self, namespace: &str, object_names: Vec<&str>) -> BackendResult<()> {
        if self.namespaces.contains_key(namespace) {
            Err(BackendError::RuntimeCheckError)
        } else {
            let namespace = self
                .namespaces
                .entry(namespace.to_owned())
                .or_insert_with(Namespace::default);

            for object_name in object_names {
                namespace
                    .objects
                    .insert(object_name.to_owned(), StorageObject::default());
            }
            Ok(())
        }
    }

    fn create_namespace(&mut self, namespace: &str) -> BackendResult<()> {
        if self.namespaces.contains_key(namespace) {
            Err(BackendError::RuntimeCheckError)
        } else {
            self.namespaces.insert(namespace.to_owned(), Namespace::default());
            Ok(())
        }
    }

    fn drop_namespace(&mut self, namespace: &str) -> BackendResult<()> {
        match self.namespaces.remove(namespace) {
            Some(_namespace) => Ok(()),
            None => Err(BackendError::RuntimeCheckError),
        }
    }

    fn create_object(&mut self, namespace: &str, object_name: &str) -> BackendResult<()> {
        match self.namespaces.get_mut(namespace) {
            Some(namespace) => {
                if namespace.objects.contains_key(object_name) {
                    Err(BackendError::RuntimeCheckError)
                } else {
                    namespace
                        .objects
                        .insert(object_name.to_owned(), StorageObject::default());
                    Ok(())
                }
            }
            None => Err(BackendError::RuntimeCheckError),
        }
    }

    fn drop_object(&mut self, namespace: &str, object_name: &str) -> BackendResult<()> {
        match self.namespaces.get_mut(namespace) {
            Some(namespace) => match namespace.objects.remove(object_name) {
                Some(_) => Ok(()),
                None => Err(BackendError::RuntimeCheckError),
            },
            None => Err(BackendError::RuntimeCheckError),
        }
    }

    fn write(&mut self, namespace: &str, object_name: &str, rows: Vec<(Key, Values)>) -> BackendResult<usize> {
        match self.namespaces.get_mut(namespace) {
            Some(namespace) => match namespace.objects.get_mut(object_name) {
                Some(object) => {
                    let len = rows.len();
                    for (key, value) in rows {
                        object.records.insert(key, value);
                    }
                    Ok(len)
                }
                None => Err(BackendError::RuntimeCheckError),
            },
            None => Err(BackendError::RuntimeCheckError),
        }
    }

    fn read(&self, namespace: &str, object_name: &str) -> BackendResult<ReadCursor> {
        match self.namespaces.get(namespace) {
            Some(namespace) => match namespace.objects.get(object_name) {
                Some(object) => Ok(Box::new(
                    object
                        .records
                        .clone()
                        .into_iter()
                        .map(Ok)
                        .collect::<Vec<SystemResult<Row>>>()
                        .into_iter(),
                )),
                None => Err(BackendError::RuntimeCheckError),
            },
            None => Err(BackendError::RuntimeCheckError),
        }
    }

    fn delete(&mut self, namespace: &str, object_name: &str, keys: Vec<Key>) -> BackendResult<usize> {
        match self.namespaces.get_mut(namespace) {
            Some(namespace) => match namespace.objects.get_mut(object_name) {
                Some(object) => {
                    object.records = object
                        .records
                        .clone()
                        .into_iter()
                        .filter(|(key, _values)| !keys.contains(key))
                        .collect();
                    Ok(keys.len())
                }
                None => Err(BackendError::RuntimeCheckError),
            },
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
                if namespace.objects.contains_key(object_name) {
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
    use representation::Binary;

    type Storage = InMemoryStorage;

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

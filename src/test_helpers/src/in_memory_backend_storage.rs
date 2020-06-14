u// Copyright 2020 Alex Dukhno
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

se core::{SystemError, SystemResult};
use std::collections::HashMap;
use storage::backend::{
    BackendStorage, CreateObjectError, DropObjectError, Key, NamespaceAlreadyExists, NamespaceDoesNotExist,
    OperationOnObjectError, ReadCursor, Result, Row, Values,
};

#[derive(Default, Debug)]
struct StorageObject {
    records: Vec<(Key, Values)>,
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

    fn create_namespace(&mut self, namespace: &str) -> SystemResult<Result<(), NamespaceAlreadyExists>> {
        if self.namespaces.contains_key(namespace) {
            Ok(Err(NamespaceAlreadyExists))
        } else {
            self.namespaces.insert(namespace.to_owned(), Namespace::default());
            Ok(Ok(()))
        }
    }

    fn drop_namespace(&mut self, namespace: &str) -> SystemResult<Result<(), NamespaceDoesNotExist>> {
        match self.namespaces.remove(namespace) {
            Some(_namespace) => Ok(Ok(())),
            None => Ok(Err(NamespaceDoesNotExist)),
        }
    }

    fn create_object(&mut self, namespace: &str, object_name: &str) -> SystemResult<Result<(), CreateObjectError>> {
        match self.namespaces.get_mut(namespace) {
            Some(namespace) => {
                if namespace.objects.contains_key(object_name) {
                    Ok(Err(CreateObjectError::ObjectAlreadyExists))
                } else {
                    namespace
                        .objects
                        .insert(object_name.to_owned(), StorageObject::default());
                    Ok(Ok(()))
                }
            }
            None => Ok(Err(CreateObjectError::NamespaceDoesNotExist)),
        }
    }

    fn drop_object(&mut self, namespace: &str, object_name: &str) -> SystemResult<Result<(), DropObjectError>> {
        match self.namespaces.get_mut(namespace) {
            Some(namespace) => match namespace.objects.remove(object_name) {
                Some(_) => Ok(Ok(())),
                None => Ok(Err(DropObjectError::ObjectDoesNotExist)),
            },
            None => Ok(Err(DropObjectError::NamespaceDoesNotExist)),
        }
    }

    fn write(
        &mut self,
        namespace: &str,
        object_name: &str,
        values: Vec<(Key, Values)>,
    ) -> SystemResult<Result<usize, OperationOnObjectError>> {
        match self.namespaces.get_mut(namespace) {
            Some(namespace) => match namespace.objects.get_mut(object_name) {
                Some(object) => {
                    object.records = object
                        .records
                        .iter()
                        .filter(|(key, _value)| values.iter().find(|(k, _v)| k == key).is_none())
                        .cloned()
                        .collect();
                    let len = values.len();
                    object.records.extend_from_slice(values.as_slice());
                    Ok(Ok(len))
                }
                None => Ok(Err(OperationOnObjectError::ObjectDoesNotExist)),
            },
            None => Ok(Err(OperationOnObjectError::NamespaceDoesNotExist)),
        }
    }

    fn read(&self, namespace: &str, object_name: &str) -> SystemResult<Result<ReadCursor, OperationOnObjectError>> {
        match self.namespaces.get(namespace) {
            Some(namespace) => match namespace.objects.get(object_name) {
                Some(object) => Ok(Ok(Box::new(
                    object
                        .records
                        .iter()
                        .cloned()
                        .map(Ok)
                        .collect::<Vec<Result<Row, SystemError>>>()
                        .into_iter(),
                ))),
                None => Ok(Err(OperationOnObjectError::ObjectDoesNotExist)),
            },
            None => Ok(Err(OperationOnObjectError::NamespaceDoesNotExist)),
        }
    }

    fn delete(
        &mut self,
        namespace: &str,
        object_name: &str,
        keys: Vec<Key>,
    ) -> SystemResult<Result<usize, OperationOnObjectError>> {
        match self.namespaces.get_mut(namespace) {
            Some(namespace) => match namespace.objects.get_mut(object_name) {
                Some(object) => {
                    object.records = object
                        .records
                        .iter()
                        .filter(|(key, _values)| !keys.contains(key))
                        .cloned()
                        .collect();
                    Ok(Ok(keys.len()))
                }
                None => Ok(Err(OperationOnObjectError::ObjectDoesNotExist)),
            },
            None => Ok(Err(OperationOnObjectError::NamespaceDoesNotExist)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod namespace {
        use super::*;

        #[test]
        fn create_namespaces_with_different_names() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage.create_namespace("namespace_1").expect("namespace created"),
                Ok(())
            );
            assert_eq!(
                storage.create_namespace("namespace_2").expect("namespace created"),
                Ok(())
            );
        }

        #[test]
        fn create_namespace_with_existing_name() {
            let mut storage = InMemoryStorage::default();

            storage
                .create_namespace("namespace")
                .expect("no system errors")
                .expect("namespace created");

            assert_eq!(
                storage.create_namespace("namespace").expect("no system errors"),
                Err(NamespaceAlreadyExists)
            );
        }

        #[test]
        fn drop_namespace() {
            let mut storage = InMemoryStorage::default();

            storage
                .create_namespace("namespace")
                .expect("no system errors")
                .expect("namespace created");

            assert_eq!(storage.drop_namespace("namespace").expect("namespace dropped"), Ok(()));
            assert_eq!(
                storage.create_namespace("namespace").expect("namespace created"),
                Ok(())
            );
        }

        #[test]
        fn drop_namespace_that_was_not_created() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage.drop_namespace("does_not_exists").expect("no system errors"),
                Err(NamespaceDoesNotExist)
            );
        }

        #[test]
        fn dropping_namespace_drops_objects_in_it() {
            let mut storage = InMemoryStorage::default();

            storage
                .create_namespace("namespace")
                .expect("no system errors")
                .expect("namespace created");
            storage
                .create_object("namespace", "object_name_1")
                .expect("no system errors")
                .expect("object created");
            storage
                .create_object("namespace", "object_name_2")
                .expect("no system errors")
                .expect("object created");

            assert_eq!(storage.drop_namespace("namespace").expect("no system errors"), Ok(()));
            assert_eq!(
                storage.create_namespace("namespace").expect("namespace created"),
                Ok(())
            );
            assert_eq!(
                storage
                    .create_object("namespace", "object_name_1")
                    .expect("no system errors"),
                Ok(())
            );
            assert_eq!(
                storage
                    .create_object("namespace", "object_name_2")
                    .expect("no system errors"),
                Ok(())
            );
        }
    }

    #[cfg(test)]
    mod create_object {
        use super::*;

        #[test]
        fn create_objects_with_different_names() {
            let mut storage = InMemoryStorage::default();

            storage
                .create_namespace("namespace")
                .expect("no system errors")
                .expect("namespace created");

            assert_eq!(
                storage
                    .create_object("namespace", "object_name_1")
                    .expect("no system errors"),
                Ok(())
            );
            assert_eq!(
                storage
                    .create_object("namespace", "object_name_2")
                    .expect("no system errors"),
                Ok(())
            );
        }

        #[test]
        fn create_object_with_the_same_name() {
            let mut storage = InMemoryStorage::default();

            create_object(&mut storage, "namespace", "object_name");

            assert_eq!(
                storage
                    .create_object("namespace", "object_name")
                    .expect("no system errors"),
                Err(CreateObjectError::ObjectAlreadyExists)
            );
        }

        #[test]
        fn create_object_with_the_same_name_in_different_namespaces() {
            let mut storage = InMemoryStorage::default();

            storage
                .create_namespace("namespace_1")
                .expect("no system errors")
                .expect("namespace created");
            storage
                .create_namespace("namespace_2")
                .expect("no system errors")
                .expect("namespace created");
            assert_eq!(
                storage
                    .create_object("namespace_1", "object_name")
                    .expect("no system errors"),
                Ok(())
            );
            assert_eq!(
                storage
                    .create_object("namespace_2", "object_name")
                    .expect("no system errors"),
                Ok(())
            );
        }

        #[test]
        fn create_object_in_not_existent_namespace() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage
                    .create_object("not_existent", "object_name")
                    .expect("no system errors"),
                Err(CreateObjectError::NamespaceDoesNotExist)
            );
        }
    }

    #[cfg(test)]
    mod drop_object {
        use super::*;

        #[test]
        fn drop_object() {
            let mut storage = InMemoryStorage::default();

            create_object(&mut storage, "namespace", "object_name");
            assert_eq!(
                storage
                    .drop_object("namespace", "object_name")
                    .expect("no system errors"),
                Ok(())
            );
            assert_eq!(
                storage
                    .create_object("namespace", "object_name")
                    .expect("no system errors"),
                Ok(())
            );
        }

        #[test]
        fn drop_not_created_object() {
            let mut storage = InMemoryStorage::default();

            storage
                .create_namespace("namespace")
                .expect("no system errors")
                .expect("namespace created");
            assert_eq!(
                storage
                    .drop_object("namespace", "not_existed_object")
                    .expect("no system errors"),
                Err(DropObjectError::ObjectDoesNotExist)
            );
        }

        #[test]
        fn drop_object_in_not_existent_namespace() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage.drop_object("not_existent", "object").expect("no system errors"),
                Err(DropObjectError::NamespaceDoesNotExist)
            );
        }
    }

    #[cfg(test)]
    mod operations_on_object {
        use super::*;

        #[test]
        fn insert_row_into_object() {
            let mut storage = InMemoryStorage::default();

            create_object(&mut storage, "namespace", "object_name");
            assert_eq!(
                storage
                    .write("namespace", "object_name", as_rows(vec![(1u8, vec!["123"])],))
                    .expect("no system errors"),
                Ok(1)
            );

            assert_eq!(
                storage
                    .read("namespace", "object_name")
                    .expect("no system errors")
                    .map(|iter| iter.collect::<Vec<Result<Row, SystemError>>>()),
                Ok(as_read_cursor(vec![(1u8, vec!["123"])]).collect())
            );
        }

        #[test]
        fn insert_many_rows_into_object() {
            let mut storage = InMemoryStorage::default();

            create_object(&mut storage, "namespace", "object_name");
            storage
                .write("namespace", "object_name", as_rows(vec![(1u8, vec!["123"])]))
                .expect("no system errors")
                .expect("values are written");
            storage
                .write("namespace", "object_name", as_rows(vec![(2u8, vec!["456"])]))
                .expect("no system errors")
                .expect("values are written");

            assert_eq!(
                storage
                    .read("namespace", "object_name")
                    .expect("no system errors")
                    .map(|iter| iter.collect::<Vec<Result<Row, SystemError>>>()),
                Ok(as_read_cursor(vec![(1u8, vec!["123"]), (2u8, vec!["456"])]).collect())
            );
        }

        #[test]
        fn insert_into_non_existent_object() {
            let mut storage = InMemoryStorage::default();

            storage
                .create_namespace("namespace")
                .expect("no system errors")
                .expect("namespace created");
            assert_eq!(
                storage
                    .write("namespace", "not_existed", as_rows(vec![(1u8, vec!["123"])],))
                    .expect("no system errors"),
                Err(OperationOnObjectError::ObjectDoesNotExist)
            );
        }

        #[test]
        fn insert_into_object_in_non_existent_namespace() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage
                    .write("not_existed", "object", as_rows(vec![(1u8, vec!["123"])],))
                    .expect("no system errors"),
                Err(OperationOnObjectError::NamespaceDoesNotExist)
            );
        }

        #[test]
        fn select_from_object_that_does_not_exist() {
            let mut storage = InMemoryStorage::default();

            storage
                .create_namespace("namespace")
                .expect("no system errors")
                .expect("namespace created");
            assert_eq!(
                storage
                    .read("namespace", "not_existed")
                    .expect("no system errors")
                    .map(|iter| iter.collect::<Vec<Result<Row, SystemError>>>()),
                Err(OperationOnObjectError::ObjectDoesNotExist)
            );
        }

        #[test]
        fn select_from_object_in_not_existent_namespace() {
            let storage = InMemoryStorage::default();

            assert_eq!(
                storage
                    .read("not_existed", "object")
                    .expect("no system errors")
                    .map(|iter| iter.collect::<Vec<Result<Row, SystemError>>>()),
                Err(OperationOnObjectError::NamespaceDoesNotExist)
            );
        }

        #[test]
        fn delete_some_records_from_object() {
            let mut storage = InMemoryStorage::default();

            create_object(&mut storage, "namespace", "object_name");
            storage
                .write(
                    "namespace",
                    "object_name",
                    as_rows(vec![(1u8, vec!["123"]), (2u8, vec!["456"]), (3u8, vec!["789"])]),
                )
                .expect("no system errors")
                .expect("write occurred");

            assert_eq!(
                storage
                    .delete("namespace", "object_name", as_keys(vec![2u8]))
                    .expect("no system errors"),
                Ok(1)
            );

            assert_eq!(
                storage
                    .read("namespace", "object_name")
                    .expect("no system errors")
                    .map(|iter| iter.collect::<Vec<Result<Row, SystemError>>>()),
                Ok(as_read_cursor(vec![(1u8, vec!["123"]), (3u8, vec!["789"])]).collect())
            );
        }

        #[test]
        fn delete_from_not_existed_object() {
            let mut storage = InMemoryStorage::default();

            storage
                .create_namespace("namespace")
                .expect("no system errors")
                .expect("namespace created");

            assert_eq!(
                storage
                    .delete("namespace", "not_existent", vec![])
                    .expect("no system errors"),
                Err(OperationOnObjectError::ObjectDoesNotExist)
            );
        }

        #[test]
        fn delete_from_not_existent_namespace() {
            let mut storage = InMemoryStorage::default();

            assert_eq!(
                storage
                    .delete("not existent", "object", vec![])
                    .expect("no system errors"),
                Err(OperationOnObjectError::NamespaceDoesNotExist)
            );
        }

        #[test]
        fn select_all_from_object_with_many_columns() {
            let mut storage = InMemoryStorage::default();

            create_object(&mut storage, "namespace", "object_name");
            storage
                .write("namespace", "object_name", as_rows(vec![(1u8, vec!["1", "2", "3"])]))
                .expect("no system errors")
                .expect("write occurred");

            assert_eq!(
                storage
                    .read("namespace", "object_name")
                    .expect("no system errors")
                    .map(|iter| iter.collect::<Vec<Result<Row, SystemError>>>()),
                Ok(as_read_cursor(vec![(1u8, vec!["1", "2", "3"])]).collect())
            );
        }

        #[test]
        fn insert_multiple_rows() {
            let mut storage = InMemoryStorage::default();

            create_object(&mut storage, "namespace", "object_name");
            storage
                .write(
                    "namespace",
                    "object_name",
                    as_rows(vec![
                        (1u8, vec!["1", "2", "3"]),
                        (2u8, vec!["4", "5", "6"]),
                        (3u8, vec!["7", "8", "9"]),
                    ]),
                )
                .expect("no system errors")
                .expect("write occurred");

            assert_eq!(
                storage
                    .read("namespace", "object_name")
                    .expect("no system errors")
                    .map(|iter| iter.collect::<Vec<Result<Row, SystemError>>>()),
                Ok(as_read_cursor(vec![
                    (1u8, vec!["1", "2", "3"]),
                    (2u8, vec!["4", "5", "6"]),
                    (3u8, vec!["7", "8", "9"])
                ])
                .collect()),
            );
        }
    }

    fn create_object(storage: &mut InMemoryStorage, namespace: &str, object_name: &str) {
        storage
            .create_namespace(namespace)
            .expect("no system errors")
            .expect("namespace created");
        storage
            .create_object(namespace, object_name)
            .expect("no system errors")
            .expect("object created");
    }

    fn as_rows(items: Vec<(u8, Vec<&'static str>)>) -> Vec<Row> {
        items
            .into_iter()
            .map(|(key, values)| {
                let k = key.to_be_bytes().to_vec();
                let v = values.into_iter().map(|s| s.as_bytes().to_vec()).collect();
                (k, v)
            })
            .collect()
    }

    fn as_keys(items: Vec<u8>) -> Vec<Key> {
        items.into_iter().map(|key| key.to_be_bytes().to_vec()).collect()
    }

    fn as_read_cursor(items: Vec<(u8, Vec<&'static str>)>) -> ReadCursor {
        Box::new(items.into_iter().map(|(key, values)| {
            let k = key.to_be_bytes().to_vec();
            let v = values.into_iter().map(|s| s.as_bytes().to_vec()).collect();
            Ok((k, v))
        }))
    }
}

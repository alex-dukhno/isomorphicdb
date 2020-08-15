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

use crate::NewReadCursor;
use kernel::{SystemError, SystemResult};
use sled::{Db as SledKeySpace, Error as SledError};
use std::{collections::HashMap, path::PathBuf, sync::RwLock};

struct SledErrorMapper;

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

pub(crate) struct OnDiskStorage {
    root_path: PathBuf,
    definition_key_space: SledKeySpace,
    key_spaces: RwLock<HashMap<String, SledKeySpace>>,
}

unsafe impl Send for OnDiskStorage {}
unsafe impl Sync for OnDiskStorage {}

impl OnDiskStorage {
    pub(crate) fn init(path: PathBuf) -> SystemResult<(OnDiskStorage, bool)> {
        log::info!("initializing on-disk storage under [{:?}] folder", path);
        let definition_key_space = match sled::open(path.clone()) {
            Ok(definition_key_space) => definition_key_space,
            Err(error) => return Err(SledErrorMapper::map(error)),
        };
        let was_recovered = definition_key_space.was_recovered();
        if was_recovered {
            log::info!("on-disk storage recovered DEFINITION_KEY_SPACE from previous start");
        } else {
            log::info!("on-disk storage initialize new DEFINITION_KEY_SPACE");
        }

        Ok((
            OnDiskStorage {
                root_path: path,
                definition_key_space,
                key_spaces: RwLock::new(HashMap::new()),
            },
            !was_recovered,
        ))
    }

    pub(crate) fn create_meta_trees(&self, tree_names: &[&str]) -> SystemResult<()> {
        for tree_name in tree_names {
            match self.definition_key_space.open_tree(tree_name) {
                Ok(_tree) => {}
                Err(error) => return Err(SledErrorMapper::map(error)),
            }
        }
        Ok(())
    }

    pub(crate) fn scan_meta_tree(&self, tree_name: &str) -> SystemResult<NewReadCursor> {
        match self.definition_key_space.open_tree(tree_name) {
            Ok(tree) => Ok(Box::new(tree.iter().map(|row| {
                row.map(|(key, values)| (key.to_vec(), values.to_vec()))
                    .map_err(|error| SledErrorMapper::map(error))
            }))),
            Err(error) => Err(SledErrorMapper::map(error)),
        }
    }

    pub(crate) fn write_to_meta_object(&self, tree_name: &str, key: &[u8], values: &[u8]) -> SystemResult<()> {
        match self.definition_key_space.open_tree(tree_name) {
            Ok(tree) => match tree.insert(key, values) {
                Ok(None) => Ok(()),
                Ok(Some(prev_value)) => Err(SystemError::bug_in_storage(format!(
                    "Previous value {} already were present in {} while trying to insert key {:?} with {:?} value",
                    std::str::from_utf8(&prev_value).unwrap(),
                    tree_name,
                    key,
                    values
                ))),
                Err(error) => Err(SledErrorMapper::map(error)),
            },
            Err(error) => Err(SledErrorMapper::map(error)),
        }
    }

    pub(crate) fn delete_from_meta_object(&self, tree_name: &str, key: &[u8]) -> SystemResult<()> {
        match self.definition_key_space.open_tree(tree_name) {
            Ok(tree) => match tree.remove(key) {
                Ok(Some(_)) => Ok(()),
                Ok(None) => Err(SystemError::bug_in_storage(format!(
                    "there is no record about object with {:?} key in {:?}",
                    key, tree_name
                ))),
                Err(error) => Err(SledErrorMapper::map(error)),
            },
            Err(error) => Err(SledErrorMapper::map(error)),
        }
    }

    // fn create_namespace(&self, namespace: &str) -> BackendResult<()> {
    //     if self.key_spaces.read().unwrap().contains_key(namespace) {
    //         Err(BackendError::RuntimeCheckError)
    //     } else {
    //         let schemata_table = match self.definition_key_space.open_tree(SCHEMATA_TABLE) {
    //             Ok(schemata) => schemata,
    //             Err(error) => return Err(BackendError::SystemError(SledErrorMapper::map(error))),
    //         };
    //         let next_key = match self.key_sequences.write() {
    //             Ok(mut guard) => match guard.get_mut(SCHEMATA_TABLE) {
    //                 None => {
    //                     return Err(BackendError::SystemError(SystemError::unrecoverable(format!(
    //                         "No KEY SEQUENCE generator for '{}' table found",
    //                         SCHEMATA_TABLE
    //                     ))))
    //                 }
    //                 Some(current_key) => {
    //                     let next = *current_key;
    //                     *current_key += 1;
    //                     next
    //                 }
    //             },
    //             Err(_) => {
    //                 return Err(BackendError::SystemError(SystemError::unrecoverable(format!(
    //                     "Can't aquire the lock on KEY SEQUENCES for '{}' table",
    //                     SCHEMATA_TABLE
    //                 ))))
    //             }
    //         };
    //         schemata_table.insert(
    //             next_key.to_be_bytes(),
    //             (DEFAULT_CATALOG.to_owned() + "." + namespace).as_bytes(),
    //         );
    //         let mut path = self.root_path.clone();
    //         path.push(DEFAULT_CATALOG);
    //         path.push(namespace);
    //         match sled::open(path) {
    //             Ok(database) => {
    //                 self.key_spaces
    //                     .write()
    //                     .unwrap()
    //                     .entry(namespace.to_owned())
    //                     .or_insert(database);
    //                 Ok(())
    //             }
    //             Err(error) => Err(BackendError::SystemError(SledErrorMapper::map(error))),
    //         }
    //     }
    // }
    //
    // fn drop_namespace(&self, namespace: &str) -> BackendResult<()> {
    //     match self.key_spaces.write().unwrap().remove(namespace) {
    //         Some(database) => {
    //             let inner = IVec::from((DEFAULT_CATALOG.to_owned() + "." + namespace).as_bytes());
    //             let schemata_table = self.definition_key_space.open_tree(SCHEMATA_TABLE).unwrap();
    //             let schema_key = schemata_table
    //                 .iter()
    //                 .map(Result::unwrap)
    //                 .filter(|(_key, columns)| columns == &inner)
    //                 .map(|(key, _columns)| key)
    //                 .next()
    //                 .unwrap();
    //             schemata_table.remove(schema_key);
    //             drop(database);
    //             Ok(())
    //         }
    //         None => Err(BackendError::RuntimeCheckError),
    //     }
    // }
    //
    // fn create_object(&mut self, namespace: &str, object_name: &str) -> BackendResult<()> {
    //     match self.key_spaces.read().unwrap().get(namespace) {
    //         Some(database) => {
    //             if database.tree_names().contains(&(object_name.into())) {
    //                 Err(BackendError::RuntimeCheckError)
    //             } else {
    //                 match database.open_tree(object_name) {
    //                     Ok(_object) => Ok(()),
    //                     Err(error) => Err(BackendError::SystemError(SledErrorMapper::map(error))),
    //                 }
    //             }
    //         }
    //         None => Err(BackendError::RuntimeCheckError),
    //     }
    // }
    //
    // fn drop_object(&mut self, namespace: &str, object_name: &str) -> BackendResult<()> {
    //     match self.key_spaces.read().unwrap().get(namespace) {
    //         Some(namespace) => match namespace.drop_tree(object_name.as_bytes()) {
    //             Ok(true) => Ok(()),
    //             Ok(false) => Err(BackendError::RuntimeCheckError),
    //             Err(error) => Err(BackendError::SystemError(SledErrorMapper::map(error))),
    //         },
    //         None => Err(BackendError::RuntimeCheckError),
    //     }
    // }
    //
    // fn write(&mut self, namespace: &str, object_name: &str, rows: Vec<Row>) -> BackendResult<usize> {
    //     match self.key_spaces.read().unwrap().get(namespace) {
    //         Some(namespace) => {
    //             if namespace.tree_names().contains(&(object_name.into())) {
    //                 match namespace.open_tree(object_name) {
    //                     Ok(object) => {
    //                         let mut written_rows = 0;
    //                         for (key, values) in rows {
    //                             match object
    //                                 .insert::<sled::IVec, sled::IVec>(key.to_bytes().into(), values.to_bytes().into())
    //                             {
    //                                 Ok(_) => written_rows += 1,
    //                                 Err(error) => return Err(BackendError::SystemError(SledErrorMapper::map(error))),
    //                             }
    //                         }
    //                         Ok(written_rows)
    //                     }
    //                     Err(error) => Err(BackendError::SystemError(SledErrorMapper::map(error))),
    //                 }
    //             } else {
    //                 Err(BackendError::RuntimeCheckError)
    //             }
    //         }
    //         None => Err(BackendError::RuntimeCheckError),
    //     }
    // }
    //
    // fn read(&self, namespace: &str, object_name: &str) -> BackendResult<ReadCursor> {
    //     match self.key_spaces.read().unwrap().get(namespace) {
    //         Some(namespace) => {
    //             if namespace.tree_names().contains(&(object_name.into())) {
    //                 match namespace.open_tree(object_name) {
    //                     Ok(object) => Ok(Box::new(object.iter().map(|item| match item {
    //                         Ok((key, values)) => {
    //                             Ok((Binary::with_data(key.to_vec()), Binary::with_data(values.to_vec())))
    //                         }
    //                         Err(error) => Err(SledErrorMapper::map(error)),
    //                     }))),
    //                     Err(error) => Err(BackendError::SystemError(SledErrorMapper::map(error))),
    //                 }
    //             } else {
    //                 Err(BackendError::RuntimeCheckError)
    //             }
    //         }
    //         None => Err(BackendError::RuntimeCheckError),
    //     }
    // }
    //
    // fn delete(&mut self, namespace: &str, object_name: &str, keys: Vec<Key>) -> BackendResult<usize> {
    //     match self.key_spaces.read().unwrap().get(namespace) {
    //         Some(namespace) => {
    //             if namespace.tree_names().contains(&(object_name.into())) {
    //                 let mut deleted = 0;
    //                 match namespace.open_tree(object_name) {
    //                     Ok(object) => {
    //                         for key in keys {
    //                             match object.remove(key.to_bytes()) {
    //                                 Ok(_) => deleted += 1,
    //                                 Err(error) => return Err(BackendError::SystemError(SledErrorMapper::map(error))),
    //                             }
    //                         }
    //                     }
    //                     Err(error) => return Err(BackendError::SystemError(SledErrorMapper::map(error))),
    //                 }
    //                 Ok(deleted)
    //             } else {
    //                 Err(BackendError::RuntimeCheckError)
    //             }
    //         }
    //         None => Err(BackendError::RuntimeCheckError),
    //     }
    // }
    //
    // fn is_namespace_exists(&self, namespace: &str) -> bool {
    //     self.key_spaces
    //         .read()
    //         .expect("To aquire read lock")
    //         .contains_key(namespace)
    // }
    //
    // fn is_object_exists(&self, namespace: &str, object_name: &str) -> bool {
    //     self.check_for_object(namespace, object_name).is_ok()
    // }
    //
    // fn check_for_object(&self, namespace: &str, object_name: &str) -> BackendResult<()> {
    //     match self.key_spaces.read().unwrap().get(namespace) {
    //         Some(namespace) => {
    //             if namespace.tree_names().contains(&(object_name.into())) {
    //                 Ok(())
    //             } else {
    //                 Err(BackendError::RuntimeCheckError)
    //             }
    //         }
    //         None => Err(BackendError::RuntimeCheckError),
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // use super::*;

        // #[test]
        // fn init_on_disk_storage() {
        //     let dir = tempfile::tempdir().expect("To create temporary folder");
        //     assert_eq!(OnDiskStorage::init(dir.into_path()).is_ok(), true);
        // }
        //
        // #[test]
        // fn storage_preserve_created_namespace_after_restart() {
        //     let dir = tempfile::tempdir().expect("To create temporary folder");
        //
        //     {
        //         let storage = OnDiskStorage::init(PathBuf::from(dir.path())).expect("To initialize on-disk storage");
        //         storage.create_namespace("namespace_1").expect("To create namespace");
        //     }
        //
        //     {
        //         let storage = OnDiskStorage::init(PathBuf::from(dir.path())).expect("To initialize on-disk storage");
        //         assert_eq!(storage.is_namespace_exists("namespace_1"), true);
        //     }
        // }
        //
        // #[test]
        // fn dropped_namespace_should_not_be_restored() {
        //     let dir = tempfile::tempdir().expect("To create temporary folder");
        //
        //     {
        //         let storage = OnDiskStorage::init(PathBuf::from(dir.path())).expect("To initialize on-disk storage");
        //         storage.create_namespace("namespace_1").expect("To create namespace");
        //     }
        //
        //     {
        //         let storage = OnDiskStorage::init(PathBuf::from(dir.path())).expect("To initialize on-disk storage");
        //         assert_eq!(storage.is_namespace_exists("namespace_1"), true);
        //         assert_eq!(storage.drop_namespace("namespace_1").is_ok(), true);
        //         assert_eq!(storage.is_namespace_exists("namespace_1"), false);
        //     }
        //
        //     {
        //         let storage = OnDiskStorage::init(PathBuf::from(dir.path())).expect("To initialize on-disk storage");
        //         assert_eq!(storage.is_namespace_exists("namespace_1"), false);
        //     }
        // }

        //     #[rstest::rstest]
        //     fn create_namespace_with_objects(mut storage: Storage) {
        //         assert_eq!(
        //             storage.create_namespace_with_objects("namespace", vec!["object_1", "object_2"]),
        //             Ok(())
        //         );

        //         assert!(storage.is_object_exists("namespace", "object_1"));
        //         assert!(storage.is_object_exists("namespace", "object_2"));
        //     }

        //     #[rstest::rstest]
        //     fn create_namespaces_with_different_names(mut storage: Storage) {
        //         assert_eq!(storage.create_namespace("namespace_1"), Ok(()));
        //         assert_eq!(storage.create_namespace("namespace_2"), Ok(()));
        //     }

        //     #[rstest::rstest]
        //     fn drop_namespace(mut with_namespace: Storage) {
        //         assert_eq!(with_namespace.drop_namespace("namespace"), Ok(()));
        //         assert_eq!(with_namespace.create_namespace("namespace"), Ok(()));
        //     }

        //     #[rstest::rstest]
        //     fn dropping_namespace_drops_objects_in_it(mut with_namespace: Storage) {
        //         with_namespace
        //             .create_object("namespace", "object_name_1")
        //             .expect("object created");
        //         with_namespace
        //             .create_object("namespace", "object_name_2")
        //             .expect("object created");

        //         assert_eq!(with_namespace.drop_namespace("namespace"), Ok(()));
        //         assert_eq!(with_namespace.create_namespace("namespace"), Ok(()));
        //         assert_eq!(with_namespace.create_object("namespace", "object_name_1"), Ok(()));
        //         assert_eq!(with_namespace.create_object("namespace", "object_name_2"), Ok(()));
        //     }
        // }

        // #[cfg(test)]
        // mod create_object {
        //     use super::*;

        //     #[rstest::rstest]
        //     fn create_objects_with_different_names(mut with_namespace: Storage) {
        //         assert_eq!(with_namespace.create_object("namespace", "object_name_1"), Ok(()));
        //         assert_eq!(with_namespace.create_object("namespace", "object_name_2"), Ok(()));
        //     }

        //     #[rstest::rstest]
        //     fn create_object_with_the_same_name_in_different_namespaces(mut storage: Storage) {
        //         storage.create_namespace("namespace_1").expect("namespace created");
        //         storage.create_namespace("namespace_2").expect("namespace created");
        //         assert_eq!(storage.create_object("namespace_1", "object_name"), Ok(()));
        //         assert_eq!(storage.create_object("namespace_2", "object_name"), Ok(()));
        //     }
        // }

        // #[cfg(test)]
        // mod drop_object {
        //     use super::*;

        //     #[rstest::rstest]
        //     fn drop_object(mut with_object: Storage) {
        //         assert_eq!(with_object.drop_object("namespace", "object_name"), Ok(()));
        //         assert_eq!(with_object.create_object("namespace", "object_name"), Ok(()));
        //     }
        // }

        // #[cfg(test)]
        // mod operations_on_object {
        //     use super::*;

        //     #[rstest::rstest]
        //     fn insert_row_into_object(mut with_object: Storage) {
        //         assert_eq!(
        //             with_object.write("namespace", "object_name", as_rows(vec![(1u8, vec!["123"])])),
        //             Ok(1)
        //         );

        //         assert_eq!(
        //             with_object
        //                 .read("namespace", "object_name")
        //                 .map(|iter| iter.collect::<Vec<SystemResult<Row>>>()),
        //             Ok(as_read_cursor(vec![(1u8, vec!["123"])]).collect())
        //         );
        //     }

        //     #[rstest::rstest]
        //     fn insert_many_rows_into_object(mut with_object: Storage) {
        //         with_object
        //             .write("namespace", "object_name", as_rows(vec![(1u8, vec!["123"])]))
        //             .expect("values are written");
        //         with_object
        //             .write("namespace", "object_name", as_rows(vec![(2u8, vec!["456"])]))
        //             .expect("values are written");

        //         assert_eq!(
        //             with_object
        //                 .read("namespace", "object_name")
        //                 .map(|iter| iter.collect::<Vec<SystemResult<Row>>>()),
        //             Ok(as_read_cursor(vec![(1u8, vec!["123"]), (2u8, vec!["456"])]).collect())
        //         );
        //     }

        //     #[rstest::rstest]
        //     fn delete_some_records_from_object(mut with_object: Storage) {
        //         with_object
        //             .write(
        //                 "namespace",
        //                 "object_name",
        //                 as_rows(vec![(1u8, vec!["123"]), (2u8, vec!["456"]), (3u8, vec!["789"])]),
        //             )
        //             .expect("write occurred");

        //         assert_eq!(
        //             with_object.delete("namespace", "object_name", as_keys(vec![2u8])),
        //             Ok(1)
        //         );

        //         assert_eq!(
        //             with_object
        //                 .read("namespace", "object_name")
        //                 .map(|iter| iter.collect::<Vec<SystemResult<Row>>>()),
        //             Ok(as_read_cursor(vec![(1u8, vec!["123"]), (3u8, vec!["789"])]).collect())
        //         );
        //     }

        //     #[rstest::rstest]
        //     fn select_all_from_object_with_many_columns(mut with_object: Storage) {
        //         with_object
        //             .write("namespace", "object_name", as_rows(vec![(1u8, vec!["1", "2", "3"])]))
        //             .expect("write occurred");

        //         assert_eq!(
        //             with_object
        //                 .read("namespace", "object_name")
        //                 .map(|iter| iter.collect::<Vec<SystemResult<Row>>>()),
        //             Ok(as_read_cursor(vec![(1u8, vec!["1", "2", "3"])]).collect())
        //         );
        //     }

        //     #[rstest::rstest]
        //     fn insert_multiple_rows(mut with_object: Storage) {
        //         with_object
        //             .write(
        //                 "namespace",
        //                 "object_name",
        //                 as_rows(vec![
        //                     (1u8, vec!["1", "2", "3"]),
        //                     (2u8, vec!["4", "5", "6"]),
        //                     (3u8, vec!["7", "8", "9"]),
        //                 ]),
        //             )
        //             .expect("write occurred");

        //         assert_eq!(
        //             with_object
        //                 .read("namespace", "object_name")
        //                 .map(|iter| iter.collect::<Vec<SystemResult<Row>>>()),
        //             Ok(as_read_cursor(vec![
        //                 (1u8, vec!["1", "2", "3"]),
        //                 (2u8, vec!["4", "5", "6"]),
        //                 (3u8, vec!["7", "8", "9"])
        //             ])
        //             .collect()),
        //         );
        //     }
    }

    // fn as_rows(items: Vec<(u8, Vec<&'static str>)>) -> Vec<Row> {
    //     items
    //         .into_iter()
    //         .map(|(key, values)| {
    //             let k = Binary::with_data(key.to_be_bytes().to_vec());
    //             let v = Binary::with_data(
    //                 values
    //                     .into_iter()
    //                     .map(|s| s.as_bytes())
    //                     .collect::<Vec<&[u8]>>()
    //                     .join(&b'|'),
    //             );
    //             (k, v)
    //         })
    //         .collect()
    // }

    // fn as_keys(items: Vec<u8>) -> Vec<Key> {
    //     items
    //         .into_iter()
    //         .map(|key| Binary::with_data(key.to_be_bytes().to_vec()))
    //         .collect()
    // }

    // fn as_read_cursor(items: Vec<(u8, Vec<&'static str>)>) -> ReadCursor {
    //     Box::new(items.into_iter().map(|(key, values)| {
    //         let k = key.to_be_bytes().to_vec();
    //         let v = values
    //             .into_iter()
    //             .map(|s| s.as_bytes())
    //             .collect::<Vec<&[u8]>>()
    //             .join(&b'|');
    //         Ok((Binary::with_data(k), Binary::with_data(v)))
    //     }))
    // }
}

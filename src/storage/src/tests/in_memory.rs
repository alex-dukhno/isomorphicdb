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

use super::*;
use crate::in_memory::InMemoryDatabaseCatalog;

type StorageUnderTest = InMemoryDatabaseCatalog;

#[rstest::fixture]
fn storage() -> StorageUnderTest {
    StorageUnderTest::default()
}

#[rstest::fixture]
fn with_namespace(storage: StorageUnderTest) -> StorageUnderTest {
    storage.create_namespace("namespace").expect("namespace created");
    storage
}

#[rstest::fixture]
fn with_object(with_namespace: StorageUnderTest) -> StorageUnderTest {
    with_namespace
        .create_tree("namespace", "object_name")
        .expect("object created");
    with_namespace
}

#[cfg(test)]
mod namespace {
    use super::*;

    #[rstest::rstest]
    fn create_namespaces_with_different_names(storage: StorageUnderTest) {
        assert_eq!(storage.create_namespace("namespace_1"), Ok(()));
        assert_eq!(storage.create_namespace("namespace_2"), Ok(()));
    }

    #[rstest::rstest]
    fn drop_namespace(with_namespace: StorageUnderTest) {
        assert_eq!(with_namespace.drop_namespace("namespace"), Ok(()));
        assert_eq!(with_namespace.create_namespace("namespace"), Ok(()));
    }

    #[rstest::rstest]
    fn dropping_namespace_drops_objects_in_it(with_namespace: StorageUnderTest) {
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
    fn create_objects_with_different_names(with_namespace: StorageUnderTest) {
        assert_eq!(with_namespace.create_tree("namespace", "object_name_1"), Ok(()));
        assert_eq!(with_namespace.create_tree("namespace", "object_name_2"), Ok(()));
    }

    #[rstest::rstest]
    fn create_object_with_the_same_name_in_different_namespaces(storage: StorageUnderTest) {
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
    fn drop_object(with_object: StorageUnderTest) {
        assert_eq!(with_object.drop_tree("namespace", "object_name"), Ok(()));
        assert_eq!(with_object.create_tree("namespace", "object_name"), Ok(()));
    }
}

#[cfg(test)]
mod operations_on_object {
    use super::*;

    #[rstest::rstest]
    fn insert_row_into_object(with_object: StorageUnderTest) {
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
    fn insert_many_rows_into_object(with_object: StorageUnderTest) {
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
    fn delete_some_records_from_object(with_object: StorageUnderTest) {
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
    fn select_all_from_object_with_many_columns(with_object: StorageUnderTest) {
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
    fn insert_multiple_rows(with_object: StorageUnderTest) {
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

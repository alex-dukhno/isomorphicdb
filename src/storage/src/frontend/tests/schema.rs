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
use sql_types::SqlType;

#[rstest::rstest]
fn create_schemas_with_different_names(mut storage: PersistentStorage) {
    assert_eq!(storage.create_schema("schema_1").expect("no system errors"), Ok(()));
    assert_eq!(storage.create_schema("schema_2").expect("no system errors"), Ok(()));
}

#[rstest::rstest]
fn create_schema_with_existing_name(mut storage: PersistentStorage) {
    create_schema(&mut storage, "schema_name");

    assert_eq!(
        storage.create_schema("schema_name").expect("no system errors"),
        Err(SchemaAlreadyExists)
    );
}

#[rstest::rstest]
fn same_table_names_with_different_columns_in_different_schemas(mut storage: PersistentStorage) {
    create_schema(&mut storage, "schema_name_1");
    create_schema(&mut storage, "schema_name_2");

    create_table(
        &mut storage,
        "schema_name_1",
        "table_name",
        vec![("sn_1_column", SqlType::SmallInt)],
    );
    create_table(
        &mut storage,
        "schema_name_2",
        "table_name",
        vec![("sn_2_column", SqlType::BigInt)],
    );

    assert_eq!(
        storage
            .table_columns("schema_name_1", "table_name")
            .expect("no system errors"),
        Ok(vec![("sn_1_column".to_owned(), SqlType::SmallInt)])
    );
    assert_eq!(
        storage
            .table_columns("schema_name_2", "table_name")
            .expect("no system errors"),
        Ok(vec![("sn_2_column".to_owned(), SqlType::BigInt)])
    );
}

#[rstest::rstest]
fn drop_schema(mut storage: PersistentStorage) {
    create_schema(&mut storage, "schema_name");

    assert_eq!(storage.drop_schema("schema_name").expect("no system errors"), Ok(()));
    assert_eq!(storage.create_schema("schema_name").expect("no system errors"), Ok(()));
}

#[rstest::rstest]
fn drop_schema_that_was_not_created() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    assert_eq!(
        storage.drop_schema("does_not_exists").expect("no system errors"),
        Err(SchemaDoesNotExist)
    );
}

#[rstest::rstest]
#[ignore]
// TODO store tables and columns into "system" schema
//      but simple select by predicate has to be implemented
fn drop_schema_drops_tables_in_it(mut storage: PersistentStorage) {
    create_schema(&mut storage, "schema_name");
    create_table(
        &mut storage,
        "schema_name",
        "table_name_1",
        vec![("column_test", SqlType::SmallInt)],
    );
    create_table(
        &mut storage,
        "schema_name",
        "table_name_2",
        vec![("column_test", SqlType::SmallInt)],
    );

    assert_eq!(storage.drop_schema("schema_name").expect("no system errors"), Ok(()));
    assert_eq!(storage.create_schema("schema_name").expect("no system errors"), Ok(()));
    assert_eq!(
        storage
            .create_table(
                "schema_name",
                "table_name_1",
                vec![("column_test".to_owned(), SqlType::SmallInt)]
            )
            .expect("no system errors"),
        Ok(())
    );
    assert_eq!(
        storage
            .create_table(
                "schema_name",
                "table_name_2",
                vec![("column_test".to_owned(), SqlType::SmallInt)]
            )
            .expect("no system errors"),
        Ok(())
    );
}

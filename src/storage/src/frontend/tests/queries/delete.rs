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
fn delete_all_from_non_existent_schema(mut storage: PersistentStorage) {
    assert_eq!(
        storage
            .delete_all_from("non_existent", "table_name")
            .expect("no system errors"),
        Err(OperationOnTableError::SchemaDoesNotExist)
    );
}

#[rstest::rstest]
fn delete_all_from_not_existed_table(mut storage: PersistentStorage) {
    create_schema(&mut storage, "schema_name");

    assert_eq!(
        storage
            .delete_all_from("schema_name", "table_name")
            .expect("no system errors"),
        Err(OperationOnTableError::TableDoesNotExist)
    );
}

#[rstest::rstest]
fn delete_all_from_table(mut storage: PersistentStorage) {
    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![("column_test", SqlType::SmallInt)],
    );
    insert_into(&mut storage, "schema_name", "table_name", vec![], vec!["123"]);
    insert_into(&mut storage, "schema_name", "table_name", vec![], vec!["456"]);
    insert_into(&mut storage, "schema_name", "table_name", vec![], vec!["789"]);

    assert_eq!(
        storage
            .delete_all_from("schema_name", "table_name")
            .expect("no system errors"),
        Ok(3)
    );

    let table_columns = storage
        .table_columns("schema_name", "table_name")
        .expect("no system errors")
        .expect("table has columns")
        .into_iter()
        .map(|(name, _sql_type)| name)
        .collect();

    assert_eq!(
        storage
            .select_all_from("schema_name", "table_name", table_columns)
            .expect("no system errors"),
        Ok((vec![("column_test".to_owned(), SqlType::SmallInt)], vec![]))
    );
}

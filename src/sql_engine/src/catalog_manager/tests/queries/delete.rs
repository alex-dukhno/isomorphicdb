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
fn delete_all_from_table(default_schema_name: &str, storage_with_schema: PersistentStorage) {
    create_table(
        &storage_with_schema,
        default_schema_name,
        "table_name",
        vec![ColumnDefinition::new(
            "column_test",
            SqlType::SmallInt(i16::min_value()),
        )],
    );

    insert_into(
        &storage_with_schema,
        default_schema_name,
        "table_name",
        vec![(1, vec!["123"])],
    );
    insert_into(
        &storage_with_schema,
        default_schema_name,
        "table_name",
        vec![(2, vec!["456"])],
    );
    insert_into(
        &storage_with_schema,
        default_schema_name,
        "table_name",
        vec![(3, vec!["789"])],
    );

    assert_eq!(
        storage_with_schema.delete_all_from(default_schema_name, "table_name"),
        Ok(3)
    );

    assert_eq!(
        storage_with_schema
            .table_scan("schema_name", "table_name")
            .map(Iterator::collect),
        Ok(vec![])
    );
}

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
    assert_eq!(storage.create_schema("schema_1"), Ok(()));
    assert_eq!(storage.create_schema("schema_2"), Ok(()));
}

#[rstest::rstest]
fn same_table_names_with_different_columns_in_different_schemas(mut storage: PersistentStorage) {
    create_schema(&mut storage, "schema_name_1");
    create_schema(&mut storage, "schema_name_2");

    create_table(
        &mut storage,
        "schema_name_1",
        "table_name",
        vec![column_definition("sn_1_column", SqlType::SmallInt(i16::min_value()))],
    );
    create_table(
        &mut storage,
        "schema_name_2",
        "table_name",
        vec![column_definition("sn_2_column", SqlType::BigInt(i64::min_value()))],
    );

    assert_eq!(
        storage.table_columns("schema_name_1", "table_name"),
        Ok(vec![column_definition(
            "sn_1_column",
            SqlType::SmallInt(i16::min_value())
        )])
    );
    assert_eq!(
        storage.table_columns("schema_name_2", "table_name"),
        Ok(vec![column_definition(
            "sn_2_column",
            SqlType::BigInt(i64::min_value())
        )])
    );
}

#[rstest::rstest]
fn drop_schema(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    assert_eq!(storage_with_schema.drop_schema(default_schema_name), Ok(()));
    assert_eq!(storage_with_schema.create_schema(default_schema_name), Ok(()));
}

#[rstest::rstest]
#[ignore]
// TODO store tables and columns into "system" schema
//      but simple select by predicate has to be implemented
fn drop_schema_drops_tables_in_it(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    create_table(
        &mut storage_with_schema,
        default_schema_name,
        "table_name_1",
        vec![column_definition("column_test", SqlType::SmallInt(i16::min_value()))],
    );
    create_table(
        &mut storage_with_schema,
        default_schema_name,
        "table_name_2",
        vec![column_definition("column_test", SqlType::SmallInt(i16::min_value()))],
    );

    assert_eq!(storage_with_schema.drop_schema(default_schema_name), Ok(()));
    assert_eq!(storage_with_schema.create_schema(default_schema_name), Ok(()));
    assert_eq!(
        storage_with_schema.create_table(
            default_schema_name,
            "table_name_1",
            &[column_definition("column_test", SqlType::SmallInt(i16::min_value()))]
        ),
        Ok(())
    );
    assert_eq!(
        storage_with_schema.create_table(
            default_schema_name,
            "table_name_2",
            &[column_definition("column_test", SqlType::SmallInt(i16::min_value()))]
        ),
        Ok(())
    );
}

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
use types::SqlType;

#[rstest::rstest]
fn create_tables_with_different_names(data_manager_with_schema: InMemory) -> Result<(), ()> {
    for op in create_table_ops(SCHEMA, TABLE_1, "column_test", SqlType::SmallInt) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }

    assert!(matches!(
        data_manager_with_schema.table_exists(SCHEMA, TABLE_1),
        Some((_, Some(_)))
    ));

    for op in create_table_ops(SCHEMA, TABLE_2, "column_test", SqlType::SmallInt) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }

    assert!(matches!(
        data_manager_with_schema.table_exists(SCHEMA, TABLE_2),
        Some((_, Some(_)))
    ));

    Ok(())
}

#[rstest::rstest]
fn create_table_with_the_same_name_in_different_schemas(data_manager: InMemory) -> Result<(), ()> {
    let schema_1_id = create_schema(&data_manager, SCHEMA_1)?;

    let schema_2_id = create_schema(&data_manager, SCHEMA_2)?;

    for op in create_table_ops(SCHEMA_1, TABLE, "column_test", SqlType::SmallInt) {
        if data_manager.execute(&op).is_ok() {}
    }

    let table_1_id = match data_manager.table_exists(SCHEMA_1, TABLE) {
        Some((_, Some(table_id))) => table_id,
        _ => panic!(),
    };

    for op in create_table_ops(SCHEMA_2, TABLE, "column_test", SqlType::SmallInt) {
        if data_manager.execute(&op).is_ok() {}
    }

    let table_2_id = match data_manager.table_exists(SCHEMA_2, TABLE) {
        Some((_, Some(table_id))) => table_id,
        _ => panic!(),
    };

    assert_eq!(
        data_manager.table_columns(&(schema_1_id, table_1_id)),
        Ok(vec![(0, ColumnDefinition::new("column_test", SqlType::SmallInt))])
    );

    assert_eq!(
        data_manager.table_columns(&(schema_2_id, table_2_id)),
        Ok(vec![(0, ColumnDefinition::new("column_test", SqlType::SmallInt))])
    );

    Ok(())
}

#[rstest::rstest]
fn drop_table(data_manager_with_schema: InMemory) -> Result<(), ()> {
    for op in create_table_ops(SCHEMA, TABLE, "column_test", SqlType::SmallInt) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }

    if data_manager_with_schema
        .execute(&SystemOperation::CheckExistence {
            system_object: SystemObject::Schema,
            object_name: SCHEMA.to_owned(),
        })
        .is_ok()
    {};
    data_manager_with_schema.execute(&SystemOperation::CheckExistence {
        system_object: SystemObject::Table,
        object_name: TABLE.to_owned(),
    })?;
    data_manager_with_schema.execute(&SystemOperation::CheckDependants {
        system_object: SystemObject::Table,
        object_name: TABLE.to_owned(),
    })?;
    data_manager_with_schema.execute(&SystemOperation::RemoveColumns {
        schema_name: SCHEMA.to_owned(),
        table_name: TABLE.to_owned(),
    })?;
    data_manager_with_schema.execute(&SystemOperation::RemoveRecord {
        system_schema: DEFINITION_SCHEMA.to_owned(),
        system_table: TABLES_TABLE.to_owned(),
        record: Record::Table {
            catalog_name: DEFAULT_CATALOG.to_owned(),
            schema_name: SCHEMA.to_owned(),
            table_name: TABLE.to_owned(),
        },
    })?;
    data_manager_with_schema.execute(&SystemOperation::RemoveFile {
        folder_name: SCHEMA.to_owned(),
        name: TABLE.to_owned(),
    })?;

    assert!(matches!(
        data_manager_with_schema.table_exists(SCHEMA, TABLE),
        Some((_, None))
    ));

    for op in create_table_ops(SCHEMA, TABLE, "column_test", SqlType::SmallInt) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }

    Ok(())
}

#[rstest::rstest]
fn table_ids_for_existing_columns(data_manager_with_schema: InMemory) -> Result<(), ()> {
    let schema_id = data_manager_with_schema.schema_exists(SCHEMA).expect("schema exists");

    for op in create_table_ops(SCHEMA, TABLE, "column_test", SqlType::SmallInt) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }

    let table_id = match data_manager_with_schema.table_exists(SCHEMA, TABLE) {
        Some((_, Some(table_id))) => table_id,
        _ => panic!(),
    };

    assert_eq!(
        data_manager_with_schema.column_ids(&(schema_id, table_id), &["column_test".to_owned()]),
        Ok((vec![0], vec![]))
    );

    Ok(())
}

#[rstest::rstest]
fn table_ids_for_non_existing_columns(data_manager_with_schema: InMemory) -> Result<(), ()> {
    let schema_id = data_manager_with_schema.schema_exists(SCHEMA).expect("schema exists");

    for op in create_table_ops(SCHEMA, TABLE, "column_test", SqlType::SmallInt) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }

    let table_id = match data_manager_with_schema.table_exists(SCHEMA, TABLE) {
        Some((_, Some(table_id))) => table_id,
        _ => panic!(),
    };

    assert_eq!(
        data_manager_with_schema.column_ids(&(schema_id, table_id), &["non_existent".to_owned()]),
        Ok((vec![], vec!["non_existent".to_owned()]))
    );

    Ok(())
}

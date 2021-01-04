// Copyright 2020 - present Alex Dukhno
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
fn create_schemas_with_different_names(data_manager: InMemory) -> Result<(), ()> {
    for op in create_schema_ops(SCHEMA_1) {
        if data_manager.execute(&op).is_ok() {}
    }

    for op in create_schema_ops(SCHEMA_2) {
        if data_manager.execute(&op).is_ok() {}
    }

    assert!(matches!(data_manager.schema_exists(SCHEMA_1), Some(_)));
    assert!(matches!(data_manager.schema_exists(SCHEMA_2), Some(_)));

    Ok(())
}

#[rstest::rstest]
fn same_table_names_with_different_columns_in_different_schemas(data_manager: InMemory) -> Result<(), ()> {
    let schema_1_id = create_schema(&data_manager, SCHEMA_1)?;

    let schema_2_id = create_schema(&data_manager, SCHEMA_2)?;

    for op in create_table_ops(SCHEMA_1, TABLE, "sn_1_column", SqlType::SmallInt) {
        if data_manager.execute(&op).is_ok() {}
    }

    for op in create_table_ops(SCHEMA_2, TABLE, "sn_2_column", SqlType::BigInt) {
        if data_manager.execute(&op).is_ok() {}
    }

    let table_1_id = match data_manager.table_exists(SCHEMA_1, TABLE) {
        Some((_, Some(table_id))) => table_id,
        _ => panic!(),
    };
    let table_2_id = match data_manager.table_exists(SCHEMA_2, TABLE) {
        Some((_, Some(table_id))) => table_id,
        _ => panic!(),
    };

    assert_eq!(
        data_manager.table_columns(&(schema_1_id, table_1_id)),
        Ok(vec![(0, ColumnDefinition::new("sn_1_column", SqlType::SmallInt))])
    );
    assert_eq!(
        data_manager.table_columns(&(schema_2_id, table_2_id)),
        Ok(vec![(0, ColumnDefinition::new("sn_2_column", SqlType::BigInt))])
    );

    Ok(())
}

#[rstest::rstest]
fn drop_schema(data_manager_with_schema: InMemory) -> Result<(), ()> {
    data_manager_with_schema.execute(&Step::CheckExistence {
        system_object: SystemObject::Schema,
        object_name: vec![SCHEMA.to_owned()],
    })?;
    data_manager_with_schema.execute(&Step::RemoveDependants {
        system_object: SystemObject::Schema,
        object_name: vec![SCHEMA.to_owned()],
    })?;
    data_manager_with_schema.execute(&Step::RemoveRecord {
        system_schema: DEFINITION_SCHEMA.to_owned(),
        system_table: SCHEMATA_TABLE.to_owned(),
        record: Record::Schema {
            catalog_name: DEFAULT_CATALOG.to_owned(),
            schema_name: SCHEMA.to_owned(),
        },
    })?;
    data_manager_with_schema.execute(&Step::RemoveFolder {
        name: SCHEMA.to_owned(),
    })?;

    assert!(matches!(data_manager_with_schema.create_schema(SCHEMA), Ok(_)));

    Ok(())
}

#[rstest::rstest]
fn restrict_drop_schema_does_not_drop_schema_with_table(data_manager_with_schema: InMemory) -> Result<(), ()> {
    for op in create_table_ops(SCHEMA, TABLE, "sn_1_column", SqlType::SmallInt) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }

    data_manager_with_schema.execute(&Step::CheckExistence {
        system_object: SystemObject::Schema,
        object_name: vec![SCHEMA.to_owned()],
    })?;
    assert_eq!(
        data_manager_with_schema.execute(&Step::CheckDependants {
            system_object: SystemObject::Schema,
            object_name: vec![SCHEMA.to_owned()],
        }),
        Err(())
    );

    Ok(())
}

#[rstest::rstest]
fn cascade_drop_schema_drops_tables_in_it(data_manager_with_schema: InMemory) -> Result<(), ()> {
    for op in create_table_ops(SCHEMA, TABLE_1, "column_test", SqlType::SmallInt) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }

    for op in create_table_ops(SCHEMA, TABLE_2, "column_test", SqlType::SmallInt) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }

    if data_manager_with_schema
        .execute(&Step::CheckExistence {
            system_object: SystemObject::Schema,
            object_name: vec![SCHEMA.to_owned()],
        })
        .is_ok()
    {}
    data_manager_with_schema.execute(&Step::RemoveDependants {
        system_object: SystemObject::Schema,
        object_name: vec![SCHEMA.to_owned()],
    })?;
    data_manager_with_schema.execute(&Step::RemoveRecord {
        system_schema: DEFINITION_SCHEMA.to_owned(),
        system_table: SCHEMATA_TABLE.to_owned(),
        record: Record::Schema {
            catalog_name: DEFAULT_CATALOG.to_owned(),
            schema_name: SCHEMA.to_owned(),
        },
    })?;
    data_manager_with_schema.execute(&Step::RemoveFolder {
        name: SCHEMA.to_owned(),
    })?;

    create_schema(&data_manager_with_schema, SCHEMA)?;

    for op in create_table_ops(SCHEMA, TABLE_1, "column_test", SqlType::SmallInt) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }

    for op in create_table_ops(SCHEMA, TABLE_2, "column_test", SqlType::SmallInt) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }

    Ok(())
}

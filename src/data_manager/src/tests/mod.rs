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

#[cfg(test)]
mod persistence;
#[cfg(test)]
mod queries;
#[cfg(test)]
mod system_schema;

const SCHEMA: &str = "schema_name";
const SCHEMA_1: &str = "schema_name_1";
const SCHEMA_2: &str = "schema_name_2";

const TABLE: &str = "table_name";
const TABLE_1: &str = "table_name_1";
const TABLE_2: &str = "table_name_2";

type InMemory = DataManager;

fn create_table(schema_name: &str, table_name: &str, columns: &[(&str, SqlType)]) -> Vec<SystemOperation> {
    let mut all = vec![
        SystemOperation::CheckExistence {
            system_object: SystemObject::Schema,
            object_name: schema_name.to_owned(),
        },
        SystemOperation::CheckExistence {
            system_object: SystemObject::Table,
            object_name: table_name.to_owned(),
        },
        SystemOperation::CreateFile {
            folder_name: schema_name.to_owned(),
            name: table_name.to_owned(),
        },
        SystemOperation::CreateRecord {
            system_schema: DEFINITION_SCHEMA.to_owned(),
            system_table: TABLES_TABLE.to_owned(),
            record: Record::Table {
                catalog_name: DEFAULT_CATALOG.to_owned(),
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
            },
        },
    ];

    let columns_ops = columns
        .iter()
        .map(|(column_name, column_type)| SystemOperation::CreateRecord {
            system_schema: DEFINITION_SCHEMA.to_owned(),
            system_table: COLUMNS_TABLE.to_owned(),
            record: Record::Column {
                catalog_name: DEFAULT_CATALOG.to_owned(),
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
                column_name: column_name.to_string(),
                sql_type: *column_type,
            },
        })
        .collect::<Vec<SystemOperation>>();

    all.extend(columns_ops);
    all
}

fn create_schema_ops(schema_name: &str) -> Vec<SystemOperation> {
    vec![
        SystemOperation::CheckExistence {
            system_object: SystemObject::Schema,
            object_name: schema_name.to_owned(),
        },
        SystemOperation::CreateFolder {
            name: schema_name.to_owned(),
        },
        SystemOperation::CreateRecord {
            system_schema: DEFINITION_SCHEMA.to_owned(),
            system_table: SCHEMATA_TABLE.to_owned(),
            record: Record::Schema {
                catalog_name: DEFAULT_CATALOG.to_owned(),
                schema_name: schema_name.to_owned(),
            },
        },
    ]
}

fn create_table_ops(
    schema_name: &str,
    table_name: &str,
    column_name: &str,
    column_type: SqlType,
) -> Vec<SystemOperation> {
    vec![
        SystemOperation::CheckExistence {
            system_object: SystemObject::Schema,
            object_name: schema_name.to_owned(),
        },
        SystemOperation::CheckExistence {
            system_object: SystemObject::Table,
            object_name: table_name.to_owned(),
        },
        SystemOperation::CreateFile {
            folder_name: schema_name.to_owned(),
            name: table_name.to_owned(),
        },
        SystemOperation::CreateRecord {
            system_schema: DEFINITION_SCHEMA.to_owned(),
            system_table: TABLES_TABLE.to_owned(),
            record: Record::Table {
                catalog_name: DEFAULT_CATALOG.to_owned(),
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
            },
        },
        SystemOperation::CreateRecord {
            system_schema: DEFINITION_SCHEMA.to_owned(),
            system_table: COLUMNS_TABLE.to_owned(),
            record: Record::Column {
                catalog_name: DEFAULT_CATALOG.to_owned(),
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
                column_name: column_name.to_owned(),
                sql_type: column_type,
            },
        },
    ]
}

#[rstest::fixture]
fn data_manager() -> InMemory {
    DataManager::in_memory()
}

#[rstest::fixture]
fn data_manager_with_schema(data_manager: InMemory) -> InMemory {
    for op in create_schema_ops(SCHEMA) {
        if data_manager.execute(&op).is_ok() {}
    }
    data_manager
}

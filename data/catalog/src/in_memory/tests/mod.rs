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

#[cfg(test)]
mod insert;
#[cfg(test)]
mod schema;
#[cfg(test)]
mod table;

use super::*;
use types::SqlType;

const SCHEMA: &str = "schema_name";
const OTHER_SCHEMA: &str = "other_schema_name";
const TABLE: &str = "table_name";
const OTHER_TABLE: &str = "other_table_name";

fn database() -> Arc<InMemoryDatabase> {
    InMemoryDatabase::new()
}

fn create_schema_ops(schema_name: &str) -> SystemOperation {
    create_schema_inner(schema_name, false)
}

fn create_schema_if_not_exists_ops(schema_name: &str) -> SystemOperation {
    create_schema_inner(schema_name, true)
}

fn create_schema_inner(schema_name: &str, if_not_exists: bool) -> SystemOperation {
    SystemOperation {
        kind: Kind::Create(SystemObject::Schema),
        skip_steps_if: if if_not_exists { Some(ObjectState::Exists) } else { None },
        steps: vec![vec![
            Step::CheckExistence {
                system_object: SystemObject::Schema,
                object_name: vec![schema_name.to_owned()],
            },
            Step::CreateFolder {
                name: schema_name.to_owned(),
            },
            Step::CreateRecord {
                record: Record::Schema {
                    schema_name: schema_name.to_owned(),
                },
            },
        ]],
    }
}

fn drop_schemas_ops(schema_names: Vec<&str>) -> SystemOperation {
    drop_schemas_inner(schema_names, false)
}

fn drop_schemas_if_exists_ops(schema_names: Vec<&str>) -> SystemOperation {
    drop_schemas_inner(schema_names, true)
}

fn drop_schemas_inner(schema_names: Vec<&str>, if_exists: bool) -> SystemOperation {
    let steps = schema_names.into_iter().map(drop_schema_op).collect::<Vec<Vec<Step>>>();
    SystemOperation {
        kind: Kind::Drop(SystemObject::Schema),
        skip_steps_if: if if_exists { Some(ObjectState::NotExists) } else { None },
        steps,
    }
}

fn drop_schema_op(schema_name: &str) -> Vec<Step> {
    vec![
        Step::CheckExistence {
            system_object: SystemObject::Schema,
            object_name: vec![schema_name.to_owned()],
        },
        Step::CheckDependants {
            system_object: SystemObject::Schema,
            object_name: vec![schema_name.to_owned()],
        },
        Step::RemoveRecord {
            record: Record::Schema {
                schema_name: schema_name.to_owned(),
            },
        },
        Step::RemoveFolder {
            name: schema_name.to_owned(),
            only_if_empty: true,
        },
    ]
}

fn create_table_with_columns(schema_name: &str, table_name: &str, columns: Vec<(&str, SqlType)>) -> SystemOperation {
    create_table_inner(schema_name, table_name, false, columns)
}

fn create_table_ops(schema_name: &str, table_name: &str) -> SystemOperation {
    create_table_inner(
        schema_name,
        table_name,
        false,
        vec![("col_1", SqlType::small_int()), ("col_2", SqlType::big_int())],
    )
}

fn create_table_if_not_exists_ops(schema_name: &str, table_name: &str) -> SystemOperation {
    create_table_inner(
        schema_name,
        table_name,
        true,
        vec![("col_1", SqlType::small_int()), ("col_2", SqlType::big_int())],
    )
}

fn create_table_inner(
    schema_name: &str,
    table_name: &str,
    if_not_exists: bool,
    columns: Vec<(&str, SqlType)>,
) -> SystemOperation {
    let mut all_steps = vec![
        Step::CheckExistence {
            system_object: SystemObject::Schema,
            object_name: vec![schema_name.to_owned()],
        },
        Step::CheckExistence {
            system_object: SystemObject::Table,
            object_name: vec![schema_name.to_owned(), table_name.to_owned()],
        },
        Step::CreateFile {
            folder_name: schema_name.to_owned(),
            name: table_name.to_owned(),
        },
        Step::CreateRecord {
            record: Record::Table {
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
            },
        },
    ];
    let column_steps = columns
        .into_iter()
        .map(|(name, sql_type)| Step::CreateRecord {
            record: Record::Column {
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
                column_name: name.to_owned(),
                sql_type,
            },
        })
        .collect::<Vec<Step>>();
    all_steps.extend(column_steps);
    SystemOperation {
        kind: Kind::Create(SystemObject::Table),
        skip_steps_if: if if_not_exists { Some(ObjectState::Exists) } else { None },
        steps: vec![all_steps],
    }
}

fn drop_tables_ops(schema_name: &str, table_names: Vec<&str>) -> SystemOperation {
    let steps = table_names
        .into_iter()
        .map(|table_name| drop_table_inner(schema_name, table_name))
        .collect::<Vec<Vec<Step>>>();
    SystemOperation {
        kind: Kind::Drop(SystemObject::Table),
        skip_steps_if: None,
        steps,
    }
}

fn drop_tables_if_exists_ops(schema_name: &str, table_names: Vec<&str>) -> SystemOperation {
    let steps = table_names
        .into_iter()
        .map(|table_name| drop_table_inner(schema_name, table_name))
        .collect::<Vec<Vec<Step>>>();
    SystemOperation {
        kind: Kind::Drop(SystemObject::Table),
        skip_steps_if: Some(ObjectState::NotExists),
        steps,
    }
}

fn drop_table_inner(schema_name: &str, table_name: &str) -> Vec<Step> {
    vec![
        Step::CheckExistence {
            system_object: SystemObject::Schema,
            object_name: vec![schema_name.to_owned()],
        },
        Step::CheckExistence {
            system_object: SystemObject::Table,
            object_name: vec![schema_name.to_owned(), table_name.to_owned()],
        },
        Step::RemoveColumns {
            schema_name: schema_name.to_owned(),
            table_name: table_name.to_owned(),
        },
        Step::RemoveRecord {
            record: Record::Table {
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
            },
        },
        Step::RemoveFile {
            folder_name: schema_name.to_owned(),
            name: table_name.to_owned(),
        },
    ]
}

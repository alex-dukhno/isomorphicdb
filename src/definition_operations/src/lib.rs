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

use types::SqlType;

#[derive(Debug, PartialEq)]
pub enum SystemOperation {
    CheckExistence {
        system_object: SystemObject,
        object_name: String,
    },
    CheckDependants {
        system_object: SystemObject,
        object_name: String,
    },
    RemoveDependants {
        system_object: SystemObject,
        object_name: String,
    },
    RemoveColumns {
        schema_name: String,
        table_name: String,
    },
    SkipIf {
        object_state: ObjectState,
        object_name: String,
    },
    CreateFolder {
        name: String,
    },
    RemoveFolder {
        name: String,
    },
    CreateFile {
        folder_name: String,
        name: String,
    },
    RemoveFile {
        folder_name: String,
        name: String,
    },
    RemoveRecord {
        system_schema: String,
        system_table: String,
        record: Record,
    },
    CreateRecord {
        system_schema: String,
        system_table: String,
        record: Record,
    },
}

#[derive(Debug, PartialEq)]
pub enum SystemObject {
    Schema,
    Table,
}

#[derive(Debug, PartialEq)]
pub enum ObjectState {
    Exists,
    NotExists,
}

#[derive(Debug, PartialEq)]
pub enum Record {
    Schema {
        catalog_name: String,
        schema_name: String,
    },
    Table {
        catalog_name: String,
        schema_name: String,
        table_name: String,
    },
    Column {
        catalog_name: String,
        schema_name: String,
        table_name: String,
        column_name: String,
        sql_type: SqlType,
    },
}

#[derive(Debug, PartialEq)]
pub enum ExecutionOutcome {
    SchemaCreated,
    SchemaDropped,
    TableCreated,
    TableDropped,
}

#[derive(Debug, PartialEq)]
pub enum ExecutionError {
    SchemaAlreadyExists(String),
    SchemaDoesNotExist(String),
    TableAlreadyExists(String, String),
    TableDoesNotExists(String, String),
}

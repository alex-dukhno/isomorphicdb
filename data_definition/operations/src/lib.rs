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

use types::SqlType;

#[derive(Debug, PartialEq)]
pub struct SystemOperation {
    pub kind: Kind,
    pub skip_steps_if: Option<ObjectState>,
    pub steps: Vec<Vec<Step>>,
}

#[derive(Debug, PartialEq)]
pub enum Kind {
    Create(SystemObject),
    Drop(SystemObject),
}

#[derive(Debug, PartialEq)]
pub enum Step {
    CheckExistence {
        system_object: SystemObject,
        object_name: Vec<String>,
    },
    CheckDependants {
        system_object: SystemObject,
        object_name: Vec<String>,
    },
    RemoveDependants {
        system_object: SystemObject,
        object_name: Vec<String>,
    },
    RemoveColumns {
        schema_name: String,
        table_name: String,
    },
    CreateFolder {
        name: String,
    },
    RemoveFolder {
        name: String,
        only_if_empty: bool,
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
        record: Record,
    },
    CreateRecord {
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
        schema_name: String,
    },
    Table {
        schema_name: String,
        table_name: String,
    },
    Column {
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
    TableDoesNotExist(String, String),
    SchemaHasDependentObjects(String),
}

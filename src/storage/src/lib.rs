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

extern crate kernel;
extern crate log;
extern crate sql_types;

use serde::{Deserialize, Serialize};
use sql_types::{ConstraintError, SqlType};

pub mod backend;
pub mod frontend;

pub type Projection = (Vec<ColumnDefinition>, Vec<Vec<String>>);

#[derive(Debug, PartialEq)]
pub struct SchemaAlreadyExists;
#[derive(Debug, PartialEq)]
pub struct SchemaDoesNotExist;

#[derive(Debug, PartialEq)]
pub enum CreateTableError {
    SchemaDoesNotExist,
    TableAlreadyExists,
}

#[derive(Debug, PartialEq)]
pub enum DropTableError {
    SchemaDoesNotExist,
    TableDoesNotExist,
}

#[derive(Debug, PartialEq)]
pub enum OperationOnTableError {
    SchemaDoesNotExist,
    TableDoesNotExist,
    InsertTooManyExpressions,
    // Returns non existing columns.
    ColumnDoesNotExist(Vec<String>),
    // Returns vector of (error, column) and a row index.
    ConstraintViolations(Vec<(ConstraintError, ColumnDefinition)>, usize),
}

#[derive(Debug, Clone)]
pub struct TableDescription {
    schema_name: String,
    table_name: String,
    column_data: Vec<ColumnDefinition>,
}

impl TableDescription {
    pub fn new(schema_name: &str, table_name: &str, column_data: Vec<ColumnDefinition>) -> Self {
        Self {
            schema_name: schema_name.to_owned(),
            table_name: table_name.to_owned(),
            column_data,
        }
    }

    pub fn column_len(&self) -> usize {
        self.column_data.len()
    }

    pub fn column_type(&self, column_idx: usize) -> SqlType {
        if let Some(column) = self.column_data.get(column_idx) {
            column.sql_type
        } else {
            panic!("attempting to access type of invalid column index")
        }
    }

    pub fn column_type_by_name(&self, name: &str) -> Option<SqlType> {
        self.column_data
            .iter()
            .find(|column| column.name == name)
            .map(|column| column.sql_type)
    }

    pub fn column_data(&self) -> &[ColumnDefinition] {
        self.column_data.as_slice()
    }

    pub fn scheme(&self) -> &str {
        self.schema_name.as_str()
    }

    pub fn table(&self) -> &str {
        self.table_name.as_str()
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    name: String,
    sql_type: SqlType,
}

impl ColumnDefinition {
    pub fn new(name: &str, sql_type: SqlType) -> Self {
        Self {
            name: name.to_string(),
            sql_type
        }
    }

    pub fn sql_type(&self) -> SqlType {
        self.sql_type
    }

    fn has_name(&self, other_name: &str) -> bool {
        self.name == other_name
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }
}

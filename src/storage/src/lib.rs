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
    ConstraintViolations(Vec<(ConstraintError, ColumnDefinition)>),
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    name: String,
    sql_type: SqlType,
}

impl ColumnDefinition {
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

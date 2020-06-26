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

use sql_types::{ConstraintError, SqlType};
use std::collections::HashMap;

pub mod backend;
pub mod frontend;

pub type Projection = (Vec<(String, sql_types::SqlType)>, Vec<Vec<String>>);

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
    // Returns non existing columns.
    ColumnDoesNotExist(Vec<String>),
    ConstraintViolation(HashMap<ConstraintError, Vec<Vec<(String, SqlType)>>>),
}

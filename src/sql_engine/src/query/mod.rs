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

///! Module for representing how a query will be parameters bound, executed and
///! values represented during runtime.
pub mod bind;
pub mod plan;
pub mod process;

use sql_types::SqlType;
use sqlparser::ast::ObjectName;
use std::convert::TryFrom;

/// A type of a column
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnType {
    #[allow(dead_code)]
    nullable: bool,
    sql_type: SqlType,
}

/// represents a table uniquely
///
/// I would like this to be a single 64 bit number where the top bits are the
/// schema id and lower bits are the table id.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct TableId(SchemaId, String);

impl TableId {
    pub fn schema_name(&self) -> &str {
        self.0.name()
    }

    pub fn name(&self) -> &str {
        self.1.as_str()
    }
}

impl TryFrom<ObjectName> for TableId {
    type Error = TableNamingError;

    fn try_from(object: ObjectName) -> Result<Self, Self::Error> {
        if object.0.len() == 1 {
            Err(TableNamingError(format!(
                "unsupported table name '{}'. All table names must be qualified",
                object.to_string()
            )))
        } else if object.0.len() != 2 {
            Err(TableNamingError(format!(
                "unable to process table name '{}'",
                object.to_string()
            )))
        } else {
            let table_name = object.0.last().unwrap().value.clone();
            let schema_name = object.0.first().unwrap().value.clone();
            Ok(TableId(SchemaId(schema_name), table_name))
        }
    }
}

pub struct TableNamingError(String);

/// represents a schema uniquely
///
/// this would be a u32
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct SchemaId(String);

impl SchemaId {
    pub fn name(&self) -> &str {
        self.0.as_str()
    }
}

impl TryFrom<ObjectName> for SchemaId {
    type Error = SchemaNamingError;

    fn try_from(object: ObjectName) -> Result<Self, Self::Error> {
        if object.0.len() != 1 {
            Err(SchemaNamingError(format!(
                "only unqualified schema names are supported, '{}'",
                object
            )))
        } else {
            Ok(SchemaId(object.to_string()))
        }
    }
}

pub struct SchemaNamingError(String);

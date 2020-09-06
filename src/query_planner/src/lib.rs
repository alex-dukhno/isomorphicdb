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

use std::convert::TryFrom;

use sql_model::Id;
use sqlparser::ast::ObjectName;
use std::fmt::{self, Display, Formatter};

use data_manager::RecordId;
use sql_model::sql_types::SqlType;

///! Module for representing how a query will be parameters bound, executed and
///! values represented during runtime.
pub mod plan;
pub mod planner;

/// represents a schema uniquely by its id
#[derive(Debug, Clone)]
pub struct SchemaId(pub Id);

/// represents a schema uniquely by its name
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct SchemaName(String);

impl SchemaName {
    pub fn name(&self) -> &str {
        self.0.as_str()
    }
}

impl TryFrom<ObjectName> for SchemaName {
    type Error = SchemaNamingError;

    fn try_from(object: ObjectName) -> Result<Self, Self::Error> {
        if object.0.len() != 1 {
            Err(SchemaNamingError(format!(
                "only unqualified schema names are supported, '{}'",
                object
            )))
        } else {
            Ok(SchemaName(object.to_string()))
        }
    }
}

pub struct SchemaNamingError(String);

pub struct FullTableId(SchemaId, Id);

/// represents a table uniquely
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct FullTableName(SchemaName, String);

impl FullTableName {
    pub fn schema_name(&self) -> &str {
        self.0.name()
    }

    pub fn name(&self) -> &str {
        self.1.as_str()
    }

    fn as_tuple(&self) -> (&str, &str) {
        (&self.0.name(), &self.1)
    }
}

impl Display for FullTableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.0.name(), self.1)
    }
}

impl TryFrom<ObjectName> for FullTableName {
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
            Ok(FullTableName(SchemaName(schema_name), table_name))
        }
    }
}

pub struct TableNamingError(String);

#[cfg(test)]
mod tests;

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
pub struct TableId(RecordId, RecordId);

impl TableId {
    pub fn schema(&self) -> SchemaId {
        SchemaId(self.0)
    }

    pub fn name(&self) -> RecordId {
        self.1
    }
}

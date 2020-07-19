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

///! Module for representing how a query will be executed and values represented
///! during runtime.
mod plan;
mod transform;

pub use plan::{Plan, PlanError, SchemaCreationInfo, TableCreationInfo};
pub use transform::QueryProcessor;

use sql_types::SqlType;
use sqlparser::ast::Statement;

/// A type of a column
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnType {
    #[allow(dead_code)]
    nullable: bool,
    /// the sql type
    sql_type: SqlType,
}

// this works for now, but ideally this should be usize's instead of strings.

/// represents a table uniquly
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

/// represents a schema Uniquly
///
/// this would be a u32
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct SchemaId(String);

impl SchemaId {
    pub fn name(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug)]
pub enum TransformError {
    UnimplementedFeature(String),
    SyntaxError(String),
    PlanError(PlanError),
    NotProcessed(Statement),
}

impl From<PlanError> for TransformError {
    fn from(other: PlanError) -> TransformError {
        TransformError::PlanError(other)
    }
}

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
pub mod expr;
mod plan;
mod relation;
mod repr;
mod scalar;
mod transform;

use expr::EvalError;
pub use plan::{Plan, SchemaCreationInfo, TableCreationInfo, TableInserts};
pub use transform::QueryProcessor;

pub use relation::{RelationError, RelationOp};
pub use repr::{Datum, Row};
pub use scalar::ScalarOp;

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

impl ColumnType {
    pub fn new(sql_type: SqlType) -> Self {
        Self {
            nullable: false,
            sql_type,
        }
    }

    pub fn typ(&self) -> SqlType {
        self.sql_type
    }
}

/// relation (table) type
/// A relation type is just the types of the columns.
/// Materialize uses this same concept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationType {
    /// the types of the columns in the specified order.
    column_types: Vec<ColumnType>,
    // Materialized also has a Vec<Vec<usize>> to represent the indices
    // available for this table but I do not know how that is going to work
    // in this database so I am leaving it out.
}

impl RelationType {
    pub fn new(column_types: Vec<ColumnType>) -> Self {
        Self { column_types }
    }

    pub fn columns(&self) -> &[ColumnType] {
        self.column_types.as_slice()
    }
}

// this works for now, but ideally this should be usize's instead of strings.

/// represents a table uniquely
///
/// I would like this to be a single 64 bit number where the top bits are the
/// schema id and lower bits are the table id.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct TableId(SchemaId, String);

impl TableId {
    pub fn new(schema: SchemaId, table_name: String) -> Self {
        Self(schema, table_name)
    }

    pub fn schema_name(&self) -> &str {
        self.0.name()
    }

    pub fn name(&self) -> &str {
        self.1.as_str()
    }
}

/// represents a schema uniquely
///
/// this would be a u32
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct SchemaId(String);

impl SchemaId {
    pub fn new(schema_name: String) -> Self {
        Self(schema_name)
    }

    pub fn name(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug)]
pub enum TransformError {
    UnimplementedFeature(String),
    SyntaxError(String),
    RelationError(RelationError),
    EvalError(EvalError),
    NotProcessed(Statement),
}

impl From<RelationError> for TransformError {
    fn from(other: RelationError) -> TransformError {
        TransformError::RelationError(other)
    }
}

impl From<EvalError> for TransformError {
    fn from(other: EvalError) -> TransformError {
        TransformError::EvalError(other)
    }
}

#[cfg(test)]
mod tests;

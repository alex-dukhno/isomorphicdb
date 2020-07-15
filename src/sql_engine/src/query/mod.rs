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

mod scalar;
mod repr;

pub use scalar::{ScalarOp};
pub use repr::{Datum, Row};

use sql_types::SqlType;

/// A type of a column
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnType {
    #[allow(dead_code)]
    nullable: bool,
    /// the sql type
    sql_type: SqlType
}

impl ColumnType {
    pub fn new(sql_type: SqlType) -> Self {
        Self {
            nullable: false,
            sql_type
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

#[cfg(test)]
mod tests;
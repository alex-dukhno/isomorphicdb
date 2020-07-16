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

use super::ScalarOp;
use crate::query::{TableId, RelationType};
use storage::TableDescription;

///! module for representing relation operations.

/// the representation for relation operations
///
/// relation operations are every operation that can be performed
/// on a table.
/// This includes:
///     predicates (where clauses)
///     joins
///     aggregates
///     sub-queries
#[derive(Debug, Clone)]
pub enum RelationOp {
    Projection {
        input: Vec<RelationOp>,
        outputs: Vec<usize>
    },
    Predicate {
        input: Box<RelationOp>,
        expr: Box<ScalarOp>,
    },

    Scan {
        // Id the table that needs to be loaded.
        // and maybe some other information we need about it.
        table: TableId,
        table_type: RelationType,
        descriptor: TableDescription,
    },

    Join {
        //join operations
    },

    Aggregate {
        // aggregate operations. anything that needs a group by to work.
    },

    SubQuery {
        output: Box<RelationOp>
    }
}
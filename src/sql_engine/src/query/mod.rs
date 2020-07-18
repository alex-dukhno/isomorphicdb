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
pub mod plan;
pub mod transform;

use crate::query::plan::PlanError;
use sqlparser::ast::Statement;

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

#[derive(Debug)]
pub enum TransformError {
    SyntaxError(String),
    PlanError(PlanError),
    NotProcessed(Statement), // This is temporary WA to handle processed and unprocessed statements
                             // ExprError(ExprError), ??
}

impl From<PlanError> for TransformError {
    fn from(other: PlanError) -> TransformError {
        TransformError::PlanError(other)
    }
}

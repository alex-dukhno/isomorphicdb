// Copyright 2020 - present Alex Dukhno
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

use expr_operators::{DynamicItem, Operation, StaticItem};
use types::{SqlFamilyType, SqlType};

#[derive(Debug, PartialEq)]
pub enum StaticEvaluationTree {
    Operation {
        left: Box<StaticEvaluationTree>,
        op: Operation,
        right: Box<StaticEvaluationTree>,
    },
    Item(StaticItem),
}

impl StaticEvaluationTree {
    pub fn kind(&self) -> Option<SqlFamilyType> {
        match self {
            StaticEvaluationTree::Operation { .. } => None,
            StaticEvaluationTree::Item(StaticItem::Const(value)) => value.kind(),
            StaticEvaluationTree::Item(StaticItem::Param(_)) => None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum DynamicEvaluationTree {
    Operation {
        left: Box<DynamicEvaluationTree>,
        op: Operation,
        right: Box<DynamicEvaluationTree>,
    },
    Item(DynamicItem),
}

#[derive(Debug, PartialEq)]
pub enum Feature {
    SetOperations,
    SubQueries,
    NationalStringLiteral,
    HexStringLiteral,
    TimeInterval,
    Joins,
    NestedJoin,
    FromSubQuery,
    TableFunctions,
    Aliases,
    QualifiedAliases,
    InsertIntoSelect,
}

// Copyright 2020 - 2021 Alex Dukhno
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

use bigdecimal::BigDecimal;
use operators_old::{BiOperator, UnOperator};

#[derive(Debug, PartialEq, Clone)]
pub enum CheckedTree {
    Item(CheckedItem),
    UnOp {
        op: UnOperator,
        item: Box<CheckedTree>,
    },
    BiOp {
        op: BiOperator,
        left: Box<CheckedTree>,
        right: Box<CheckedTree>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum CheckedItem {
    Const(CheckedValue),
}

#[derive(Debug, PartialEq, Clone)]
pub enum CheckedValue {
    Int(i32),
    BigInt(i64),
    Numeric(BigDecimal),
    StringLiteral(String),
    Null,
}

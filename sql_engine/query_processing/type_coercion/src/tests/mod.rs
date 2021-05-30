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

use super::*;
use bigdecimal::BigDecimal;
use checked_tree::{CheckedItem, CheckedTree, CheckedValue};

#[cfg(test)]
mod constants;
#[cfg(test)]
mod operations;

fn checked_int(num: i32) -> CheckedTree {
    CheckedTree::Item(CheckedItem::Const(CheckedValue::Int(num)))
}

fn checked_bigint(num: i64) -> CheckedTree {
    CheckedTree::Item(CheckedItem::Const(CheckedValue::BigInt(num)))
}

fn checked_number(num: BigDecimal) -> CheckedTree {
    CheckedTree::Item(CheckedItem::Const(CheckedValue::Numeric(num)))
}

fn checked_string(str: &str) -> CheckedTree {
    CheckedTree::Item(CheckedItem::Const(CheckedValue::StringLiteral(str.to_owned())))
}

fn checked_null() -> CheckedTree {
    CheckedTree::Item(CheckedItem::Const(CheckedValue::Null))
}

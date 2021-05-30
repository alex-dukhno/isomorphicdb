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
use typed_tree::{TypedItem, TypedTree, TypedValue};

#[cfg(test)]
mod constants;
#[cfg(test)]
mod operations;

fn untyped_int(num: i32) -> UntypedTree {
    UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(num)))
}

fn untyped_number(num: &str) -> UntypedTree {
    UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(num.to_owned())))
}

fn untyped_string(str: &str) -> UntypedTree {
    UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal(str.to_owned())))
}

fn untyped_null() -> UntypedTree {
    UntypedTree::Item(UntypedItem::Const(UntypedValue::Null))
}

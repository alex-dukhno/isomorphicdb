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
use operators::{BiArithmetic, BiOperator, UnArithmetic, UnOperator};
use typed_tree::{TypedItem, TypedTree, TypedValue};

#[cfg(test)]
mod binary_op;
#[cfg(test)]
mod constants;
#[cfg(test)]
mod operations;

fn untyped_int(num: i32) -> Expr {
    Expr::Value(Value::Int(num))
}

fn untyped_number(num: &str) -> Expr {
    Expr::Value(Value::Number(num.to_owned()))
}

fn untyped_string(str: &str) -> Expr {
    Expr::Value(Value::String(str.to_owned()))
}

fn untyped_null() -> Expr {
    Expr::Value(Value::Null)
}

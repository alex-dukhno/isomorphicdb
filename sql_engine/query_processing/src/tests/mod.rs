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
use checked_tree::{CheckedItem, CheckedTree, CheckedValue};
use operators::UnOperator;
use query_ast::{Expr, Value};
use typed_tree::{TypedItem, TypedTree, TypedValue};
use types::SqlType;

// #[test]
// fn small_int_as_root() {
//     let type_inference = TypeInference;
//     let type_checker = TypeChecker;
//     let type_coercion = TypeCoercion;
//
//     let typed_tree = type_inference.infer_type(Expr::Value(Value::Int(0)), SqlType::small_int()).unwrap();
//     assert_eq!(
//         typed_tree,
//         TypedTree::UnOp {
//             op: UnOperator::Cast(SqlType::small_int()),
//             item: TypedTree::Item(TypedItem::Const(TypedValue::Int(0)))
//         }
//     );
//
//     let checked_tree = type_checker.type_check(typed_tree).unwrap();
//     assert_eq!(
//         checked_tree,
//         CheckedTree::UnOp {
//             op: UnOperator::Cast(SqlType::small_int()),
//             item: CheckedTree::Item(CheckedItem::Const(CheckedValue::Int(0)))
//         }
//     );
//
//     type_coercion.coerce_type(checked_tree)
// }

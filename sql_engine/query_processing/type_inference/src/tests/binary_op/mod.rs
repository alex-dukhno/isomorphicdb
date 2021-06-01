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
use query_ast::DataType;

#[test]
fn string_literal_and_int() {
    let type_inference = TypeInference;
    let expr_tree = Expr::BinaryOp {
        op: BinaryOperator::Plus,
        left: Box::new(Expr::Value(Value::String("1".to_owned()))),
        right: Box::new(Expr::Value(Value::Int(1))),
    };

    assert_eq!(
        type_inference.infer_type(expr_tree),
        Ok(TypedTree::BiOp {
            op: BiOperator::Arithmetic(BiArithmetic::Add),
            left: Box::new(TypedTree::UnOp {
                op: UnOperator::Cast(SqlType::integer()),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("1".to_owned()))))
            }),
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Int(1))))
        })
    );
}

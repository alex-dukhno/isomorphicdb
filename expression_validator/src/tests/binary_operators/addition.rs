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
use operators::{BiArithmetic, BiOperator};
use typed_tree::TypedValue;
use types::StringFamily;

const ADD: BiOperator = BiOperator::Arithmetic(BiArithmetic::Add);

fn untyped_char() -> UntypedTree {
    UntypedTree::Item(UntypedItem::Column {
        name: "column_name".to_owned(),
        sql_type: SqlTypeFamily::String(StringFamily::Char),
        index: 0,
    })
}

fn untyped_null() -> UntypedTree {
    UntypedTree::Item(UntypedItem::Const(UntypedValue::Null))
}

fn typed_null() -> TypedTree {
    TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown))
}

fn untyped_string_literal(value: &str) -> UntypedTree {
    UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal(value.to_owned())))
}

fn typed_string_literal(value: &str) -> TypedTree {
    TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral(value.to_owned())))
}

fn untyped_smallint(value: i16) -> UntypedTree {
    UntypedTree::UnOp {
        op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::SmallInt)),
        item: Box::new(untyped_int(value as i32)),
    }
}

fn typed_smallint(value: i16) -> TypedTree {
    TypedTree::UnOp {
        op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::SmallInt)),
        item: Box::new(typed_int(value as i32)),
    }
}

fn untyped_int(value: i32) -> UntypedTree {
    UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(value)))
}

fn typed_int(value: i32) -> TypedTree {
    TypedTree::Item(TypedItem::Const(TypedValue::Int(value)))
}

#[test]
fn int_plus_int_end_type_bigint() {
    let typed_tree = ExpressionValidator.validate(
        UntypedTree::BiOp {
            op: ADD,
            left: Box::new(untyped_int(1)),
            right: Box::new(untyped_int(1)),
        },
        SqlTypeFamily::Int(IntNumFamily::BigInt),
    );

    assert_eq!(
        typed_tree,
        Ok(TypedTree::UnOp {
            op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::BigInt)),
            item: Box::new(TypedTree::BiOp {
                op: ADD,
                left: Box::new(typed_int(1)),
                right: Box::new(typed_int(1)),
                type_family: SqlTypeFamily::Int(IntNumFamily::Integer)
            })
        })
    )
}

#[test]
fn smallint_plus_smallint_end_type_bigint() {
    let typed_tree = ExpressionValidator.validate(
        UntypedTree::BiOp {
            op: ADD,
            left: Box::new(untyped_smallint(1)),
            right: Box::new(untyped_smallint(1)),
        },
        SqlTypeFamily::Int(IntNumFamily::BigInt),
    );

    assert_eq!(
        typed_tree,
        Ok(TypedTree::UnOp {
            op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::BigInt)),
            item: Box::new(TypedTree::BiOp {
                op: ADD,
                left: Box::new(typed_smallint(1)),
                right: Box::new(typed_smallint(1)),
                type_family: SqlTypeFamily::Int(IntNumFamily::SmallInt)
            })
        })
    );
}

#[test]
fn string_literal_plus_int_end_type_int() {
    let typed_tree = ExpressionValidator.validate(
        UntypedTree::BiOp {
            op: ADD,
            left: Box::new(untyped_string_literal("1")),
            right: Box::new(untyped_int(1)),
        },
        SqlTypeFamily::Int(IntNumFamily::Integer),
    );

    assert_eq!(
        typed_tree,
        Ok(TypedTree::BiOp {
            op: ADD,
            left: Box::new(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::Integer)),
                item: Box::new(typed_string_literal("1"))
            }),
            right: Box::new(typed_int(1)),
            type_family: SqlTypeFamily::Int(IntNumFamily::Integer)
        })
    );
}

#[test]
fn int_plus_string_literal_end_type_int() {
    let typed_tree = ExpressionValidator.validate(
        UntypedTree::BiOp {
            op: ADD,
            left: Box::new(untyped_int(1)),
            right: Box::new(untyped_string_literal("1")),
        },
        SqlTypeFamily::Int(IntNumFamily::Integer),
    );

    assert_eq!(
        typed_tree,
        Ok(TypedTree::BiOp {
            op: ADD,
            left: Box::new(typed_int(1)),
            right: Box::new(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::Integer)),
                item: Box::new(typed_string_literal("1"))
            }),
            type_family: SqlTypeFamily::Int(IntNumFamily::Integer)
        })
    );
}

#[test]
fn int_plus_null_end_type_int() {
    let typed_tree = ExpressionValidator.validate(
        UntypedTree::BiOp {
            op: ADD,
            left: Box::new(untyped_int(1)),
            right: Box::new(untyped_null()),
        },
        SqlTypeFamily::Int(IntNumFamily::Integer),
    );

    assert_eq!(
        typed_tree,
        Ok(TypedTree::BiOp {
            op: ADD,
            left: Box::new(typed_int(1)),
            right: Box::new(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::Integer)),
                item: Box::new(typed_null())
            }),
            type_family: SqlTypeFamily::Int(IntNumFamily::Integer)
        })
    );
}

#[test]
fn null_plus_int_end_type_int() {
    let typed_tree = ExpressionValidator.validate(
        UntypedTree::BiOp {
            op: ADD,
            left: Box::new(untyped_null()),
            right: Box::new(untyped_int(1)),
        },
        SqlTypeFamily::Int(IntNumFamily::Integer),
    );

    assert_eq!(
        typed_tree,
        Ok(TypedTree::BiOp {
            op: ADD,
            left: Box::new(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::Integer)),
                item: Box::new(typed_null())
            }),
            right: Box::new(typed_int(1)),
            type_family: SqlTypeFamily::Int(IntNumFamily::Integer)
        })
    );
}

#[test]
fn char_plus_int_end_type_int() {
    let typed_tree = ExpressionValidator.validate(
        UntypedTree::BiOp {
            op: ADD,
            left: Box::new(untyped_char()),
            right: Box::new(untyped_int(1)),
        },
        SqlTypeFamily::Int(IntNumFamily::Integer),
    );

    assert_eq!(
        typed_tree,
        Err(ExpressionValidationError::UndefinedBinaryFunction {
            op: ADD,
            left: SqlTypeFamily::String(StringFamily::Char),
            right: SqlTypeFamily::Int(IntNumFamily::Integer)
        })
    );
}

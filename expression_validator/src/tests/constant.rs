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

#[cfg(test)]
mod successful_processing {
    use super::*;

    #[test]
    fn end_type_is_smallint() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Int(1))))
            })
        );
    }

    #[test]
    fn end_type_is_int() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::Int(IntNumFamily::Integer),
            ),
            Ok(TypedTree::Item(TypedItem::Const(TypedValue::Int(1))))
        );
    }
}

#[cfg(test)]
mod failed_type_checks {
    use super::*;

    #[test]
    fn bool_value_to_int_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::UnOp {
                    op: UnOperator::Cast(SqlTypeFamily::Bool),
                    item: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("t".to_owned()))))
                },
                SqlTypeFamily::Int(IntNumFamily::Integer)
            ),
            Err(ExpressionError::DatatypeMismatch {
                expected: SqlTypeFamily::Int(IntNumFamily::Integer),
                actual: SqlTypeFamily::Bool
            })
        );
    }
}

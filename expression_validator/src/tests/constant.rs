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
use types::{FloatNumFamily, StringFamily, TemporalFamily};

#[cfg(test)]
mod string_literals {
    use super::*;

    #[test]
    fn char_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::String(StringFamily::Char),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::Char)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn varchar_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::String(StringFamily::VarChar),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::VarChar)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn text_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::String(StringFamily::Text),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::Text)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn smallint_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn int_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::Int(IntNumFamily::Integer),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::Integer)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn big_int_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::BigInt)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn numeric_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::Numeric,
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Numeric),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn real_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::Float(FloatNumFamily::Real),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Float(FloatNumFamily::Real)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn double_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::Float(FloatNumFamily::Double),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Float(FloatNumFamily::Double)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn boolean_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::Bool,
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Bool),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn date_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Temporal(TemporalFamily::Date)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn time_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::Temporal(TemporalFamily::Time),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Temporal(TemporalFamily::Time)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn timestamp_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn timestamp_with_time_zone_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }

    #[test]
    fn interval_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("abc".to_owned()))),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
            })
        );
    }
}

#[cfg(test)]
mod integers {
    use super::*;

    #[test]
    fn char_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::String(StringFamily::Char),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::Char)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Int(1))))
            })
        );
    }

    #[test]
    fn varchar_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::String(StringFamily::VarChar),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::VarChar)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Int(1))))
            })
        );
    }

    #[test]
    fn text_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::String(StringFamily::Text),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::Text)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Int(1))))
            })
        );
    }

    #[test]
    fn smallint_is_end_type() {
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
    fn int_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::Int(IntNumFamily::Integer),
            ),
            Ok(TypedTree::Item(TypedItem::Const(TypedValue::Int(1))))
        );
    }

    #[test]
    fn big_int_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::BigInt)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Int(1))))
            })
        );
    }

    #[test]
    fn numeric_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))), SqlTypeFamily::Numeric),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Numeric),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Int(1))))
            })
        );
    }

    #[test]
    fn real_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::Float(FloatNumFamily::Real),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Float(FloatNumFamily::Real)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Int(1))))
            })
        );
    }

    #[test]
    fn double_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::Float(FloatNumFamily::Double),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Float(FloatNumFamily::Double)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Int(1))))
            })
        );
    }

    #[test]
    fn boolean_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))), SqlTypeFamily::Bool),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Bool,
                actual: SqlTypeFamily::Int(IntNumFamily::Integer)
            })
        );
    }

    #[test]
    fn date_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::Date),
                actual: SqlTypeFamily::Int(IntNumFamily::Integer)
            })
        );
    }

    #[test]
    fn time_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::Temporal(TemporalFamily::Time),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::Time),
                actual: SqlTypeFamily::Int(IntNumFamily::Integer)
            })
        );
    }

    #[test]
    fn timestamp_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                actual: SqlTypeFamily::Int(IntNumFamily::Integer)
            })
        );
    }

    #[test]
    fn timestamp_with_time_zone_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                actual: SqlTypeFamily::Int(IntNumFamily::Integer)
            })
        );
    }

    #[test]
    fn interval_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::Interval),
                actual: SqlTypeFamily::Int(IntNumFamily::Integer)
            })
        );
    }
}

#[cfg(test)]
mod big_integers {
    use super::*;

    #[test]
    fn char_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::String(StringFamily::Char),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::Char)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(1))))
            })
        );
    }

    #[test]
    fn varchar_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::String(StringFamily::VarChar),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::VarChar)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(1))))
            })
        );
    }

    #[test]
    fn text_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::String(StringFamily::Text),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::Text)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(1))))
            })
        );
    }

    #[test]
    fn smallint_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(1))))
            })
        );
    }

    #[test]
    fn int_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::Int(IntNumFamily::Integer),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::Integer)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(1))))
            })
        );
    }

    #[test]
    fn big_int_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
            ),
            Ok(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(1))))
        );
    }

    #[test]
    fn numeric_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))), SqlTypeFamily::Numeric,),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Numeric),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(1))))
            })
        );
    }

    #[test]
    fn real_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::Float(FloatNumFamily::Real),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Float(FloatNumFamily::Real)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(1))))
            })
        );
    }

    #[test]
    fn double_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::Float(FloatNumFamily::Double),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Float(FloatNumFamily::Double)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(1))))
            })
        );
    }

    #[test]
    fn boolean_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))), SqlTypeFamily::Bool),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Bool,
                actual: SqlTypeFamily::Int(IntNumFamily::BigInt)
            })
        );
    }

    #[test]
    fn date_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::Date),
                actual: SqlTypeFamily::Int(IntNumFamily::BigInt)
            })
        );
    }

    #[test]
    fn time_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::Temporal(TemporalFamily::Time),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::Time),
                actual: SqlTypeFamily::Int(IntNumFamily::BigInt)
            })
        );
    }

    #[test]
    fn timestamp_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                actual: SqlTypeFamily::Int(IntNumFamily::BigInt)
            })
        );
    }

    #[test]
    fn timestamp_with_time_zone_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                actual: SqlTypeFamily::Int(IntNumFamily::BigInt)
            })
        );
    }

    #[test]
    fn interval_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(1))),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::Interval),
                actual: SqlTypeFamily::Int(IntNumFamily::BigInt)
            })
        );
    }
}

#[cfg(test)]
mod numerics {
    use super::*;

    #[test]
    fn char_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::String(StringFamily::Char),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::Char)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from(1)))))
            })
        );
    }

    #[test]
    fn varchar_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::String(StringFamily::VarChar),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::VarChar)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from(1)))))
            })
        );
    }

    #[test]
    fn text_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::String(StringFamily::Text),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::Text)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from(1)))))
            })
        );
    }

    #[test]
    fn smallint_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from(1)))))
            })
        );
    }

    #[test]
    fn int_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::Int(IntNumFamily::Integer),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::Integer)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from(1)))))
            })
        );
    }

    #[test]
    fn big_int_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::BigInt)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from(1)))))
            })
        );
    }

    #[test]
    fn numeric_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::Numeric
            ),
            Ok(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from(1)))))
        );
    }

    #[test]
    fn real_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::Float(FloatNumFamily::Real),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Float(FloatNumFamily::Real)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from(1)))))
            })
        );
    }

    #[test]
    fn double_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::Float(FloatNumFamily::Double),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Float(FloatNumFamily::Double)),
                item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from(1)))))
            })
        );
    }

    #[test]
    fn boolean_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::Bool
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Bool,
                actual: SqlTypeFamily::Numeric
            })
        );
    }

    #[test]
    fn date_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::Date),
                actual: SqlTypeFamily::Numeric
            })
        );
    }

    #[test]
    fn time_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::Temporal(TemporalFamily::Time),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::Time),
                actual: SqlTypeFamily::Numeric
            })
        );
    }

    #[test]
    fn timestamp_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
                actual: SqlTypeFamily::Numeric
            })
        );
    }

    #[test]
    fn timestamp_with_time_zone_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
                actual: SqlTypeFamily::Numeric
            })
        );
    }

    #[test]
    fn interval_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(1)))),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
            ),
            Err(ExpressionValidationError::DatatypeMismatch {
                expected: SqlTypeFamily::Temporal(TemporalFamily::Interval),
                actual: SqlTypeFamily::Numeric
            })
        );
    }
}

#[cfg(test)]
mod nulls {
    use super::*;

    #[test]
    fn char_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::String(StringFamily::Char),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::Char)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn varchar_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::String(StringFamily::VarChar),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::VarChar)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn text_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::String(StringFamily::Text),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::String(StringFamily::Text)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn smallint_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::Int(IntNumFamily::SmallInt),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::SmallInt)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn int_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::Int(IntNumFamily::Integer),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::Integer)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn big_int_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::Int(IntNumFamily::BigInt),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Int(IntNumFamily::BigInt)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn numeric_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)), SqlTypeFamily::Numeric),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Numeric),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn real_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::Float(FloatNumFamily::Real),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Float(FloatNumFamily::Real)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn double_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::Float(FloatNumFamily::Double),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Float(FloatNumFamily::Double)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn boolean_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)), SqlTypeFamily::Bool),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Bool),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn date_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::Temporal(TemporalFamily::Date),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Temporal(TemporalFamily::Date)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn time_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::Temporal(TemporalFamily::Time),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Temporal(TemporalFamily::Time)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn timestamp_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::Temporal(TemporalFamily::Timestamp),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Temporal(TemporalFamily::Timestamp)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn timestamp_with_time_zone_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }

    #[test]
    fn interval_is_end_type() {
        assert_eq!(
            ExpressionValidator.validate(
                UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
                SqlTypeFamily::Temporal(TemporalFamily::Interval),
            ),
            Ok(TypedTree::UnOp {
                op: UnOperator::Cast(SqlTypeFamily::Temporal(TemporalFamily::Interval)),
                item: Box::new(TypedTree::Item(TypedItem::Null(SqlTypeFamily::Unknown)))
            })
        );
    }
}

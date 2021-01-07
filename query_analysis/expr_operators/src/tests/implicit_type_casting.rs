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

use super::*;

#[cfg(test)]
mod booleans {
    use super::*;

    #[test]
    fn to_string() {
        assert_eq!(
            ScalarValue::Bool(Bool(true)).implicit_cast_to(SqlType::Char(4)),
            Ok(ScalarValue::String("true".to_owned()))
        );
    }

    #[test]
    fn to_string_not_enough_length() {
        assert_eq!(
            ScalarValue::Bool(Bool(true)).implicit_cast_to(SqlType::Char(1)),
            Err(ImplicitCastError::string_data_right_truncation(SqlType::Char(1)))
        );
    }

    #[test]
    fn to_number() {
        assert_eq!(
            ScalarValue::Bool(Bool(true)).implicit_cast_to(SqlType::SmallInt),
            Err(ImplicitCastError::datatype_mismatch(SqlType::SmallInt, SqlType::Bool))
        );
    }
}

#[cfg(test)]
mod strings {
    use super::*;

    #[test]
    fn to_boolean() {
        assert_eq!(
            ScalarValue::String("true".to_owned()).implicit_cast_to(SqlType::Bool),
            Ok(ScalarValue::Bool(Bool(true)))
        );
    }

    #[test]
    fn to_boolean_invalid() {
        assert_eq!(
            ScalarValue::String("invalid".to_owned()).implicit_cast_to(SqlType::Bool),
            Err(ImplicitCastError::invalid_input_syntax_for_type(
                SqlType::Bool,
                &"invalid"
            ))
        );
    }

    #[test]
    fn to_number() {
        assert_eq!(
            ScalarValue::String("123".to_owned()).implicit_cast_to(SqlType::SmallInt),
            Ok(ScalarValue::Number(BigDecimal::from(123)))
        );
    }

    #[test]
    fn to_number_invalid() {
        assert_eq!(
            ScalarValue::String("invalid".to_owned()).implicit_cast_to(SqlType::SmallInt),
            Err(ImplicitCastError::invalid_input_syntax_for_type(
                SqlType::SmallInt,
                &"invalid"
            ))
        );
    }
}

#[cfg(test)]
mod numbers {
    use super::*;

    #[test]
    fn to_boolean() {
        assert_eq!(
            ScalarValue::Number(BigDecimal::from(0)).implicit_cast_to(SqlType::Bool),
            Err(ImplicitCastError::datatype_mismatch(SqlType::Bool, SqlType::Integer))
        );

        assert_eq!(
            ScalarValue::Number(BigDecimal::from(i64::max_value())).implicit_cast_to(SqlType::Bool),
            Err(ImplicitCastError::datatype_mismatch(SqlType::Bool, SqlType::BigInt))
        );

        assert_eq!(
            ScalarValue::Number(BigDecimal::from_str("-3.40").unwrap()).implicit_cast_to(SqlType::Bool),
            Err(ImplicitCastError::datatype_mismatch(SqlType::Bool, SqlType::Real))
        );

        assert_eq!(
            ScalarValue::Number(BigDecimal::from_str(&(f32::MAX.to_string() + "00.123")).unwrap())
                .implicit_cast_to(SqlType::Bool),
            Err(ImplicitCastError::datatype_mismatch(
                SqlType::Bool,
                SqlType::DoublePrecision
            ))
        );
    }

    #[test]
    fn to_string() {
        assert_eq!(
            ScalarValue::Number(BigDecimal::from(0)).implicit_cast_to(SqlType::Char(1)),
            Ok(ScalarValue::String("0".to_owned()))
        );
    }

    #[test]
    fn to_string_too_long() {
        assert_eq!(
            ScalarValue::Number(BigDecimal::from(10)).implicit_cast_to(SqlType::Char(1)),
            Err(ImplicitCastError::string_data_right_truncation(SqlType::Char(1)))
        );
    }
}

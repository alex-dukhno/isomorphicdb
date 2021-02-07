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
            UntypedValue::Bool(Bool(true)).implicit_cast_to(SqlType::char(4)),
            Ok(UntypedValue::String("true".to_owned()))
        );
    }

    #[test]
    fn to_string_not_enough_length() {
        assert_eq!(
            UntypedValue::Bool(Bool(true)).implicit_cast_to(SqlType::char(1)),
            Err(ImplicitCastError::string_data_right_truncation(SqlType::char(1)))
        );
    }

    #[test]
    fn to_number() {
        assert_eq!(
            UntypedValue::Bool(Bool(true)).implicit_cast_to(SqlType::small_int()),
            Err(ImplicitCastError::datatype_mismatch(
                SqlType::small_int(),
                SqlType::bool()
            ))
        );
    }
}

#[cfg(test)]
mod strings {
    use super::*;

    #[test]
    fn to_boolean() {
        assert_eq!(
            UntypedValue::String("true".to_owned()).implicit_cast_to(SqlType::bool()),
            Ok(UntypedValue::Bool(Bool(true)))
        );
    }

    #[test]
    fn to_boolean_invalid() {
        assert_eq!(
            UntypedValue::String("invalid".to_owned()).implicit_cast_to(SqlType::bool()),
            Err(ImplicitCastError::invalid_input_syntax_for_type(
                SqlType::bool(),
                &"invalid"
            ))
        );
    }

    #[test]
    fn to_number() {
        assert_eq!(
            UntypedValue::String("123".to_owned()).implicit_cast_to(SqlType::small_int()),
            Ok(UntypedValue::Number(BigDecimal::from(123)))
        );
    }

    #[test]
    fn to_number_invalid() {
        assert_eq!(
            UntypedValue::String("invalid".to_owned()).implicit_cast_to(SqlType::small_int()),
            Err(ImplicitCastError::invalid_input_syntax_for_type(
                SqlType::small_int(),
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
            UntypedValue::Number(BigDecimal::from(0)).implicit_cast_to(SqlType::bool()),
            Err(ImplicitCastError::datatype_mismatch(
                SqlType::bool(),
                SqlType::integer()
            ))
        );

        assert_eq!(
            UntypedValue::Number(BigDecimal::from(i64::max_value())).implicit_cast_to(SqlType::bool()),
            Err(ImplicitCastError::datatype_mismatch(
                SqlType::bool(),
                SqlType::big_int()
            ))
        );

        assert_eq!(
            UntypedValue::Number(BigDecimal::from_str("-3.40").unwrap()).implicit_cast_to(SqlType::bool()),
            Err(ImplicitCastError::datatype_mismatch(SqlType::bool(), SqlType::real()))
        );

        assert_eq!(
            UntypedValue::Number(BigDecimal::from_str(&(f32::MAX.to_string() + "00.123")).unwrap())
                .implicit_cast_to(SqlType::bool()),
            Err(ImplicitCastError::datatype_mismatch(
                SqlType::bool(),
                SqlType::double_precision()
            ))
        );
    }

    #[test]
    fn to_string() {
        assert_eq!(
            UntypedValue::Number(BigDecimal::from(0)).implicit_cast_to(SqlType::char(1)),
            Ok(UntypedValue::String("0".to_owned()))
        );
    }

    #[test]
    fn to_string_too_long() {
        assert_eq!(
            UntypedValue::Number(BigDecimal::from(10)).implicit_cast_to(SqlType::char(1)),
            Err(ImplicitCastError::string_data_right_truncation(SqlType::char(1)))
        );
    }
}

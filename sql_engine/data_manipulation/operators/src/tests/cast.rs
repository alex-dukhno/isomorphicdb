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
use types_old::{Num, Str};

#[cfg(test)]
mod boolean {
    use super::*;

    #[test]
    fn cast_to_string() {
        assert_eq!(
            UnOperator::Cast(SqlTypeOld::Str { len: 4, kind: Str::Const }).eval(ScalarValue::Bool(true)),
            Ok(ScalarValue::String("true".to_owned()))
        );
    }

    #[test]
    fn cast_to_bool() {
        assert_eq!(
            UnOperator::Cast(SqlTypeOld::Bool).eval(ScalarValue::Bool(true)),
            Ok(ScalarValue::Bool(true))
        );
    }

    #[test]
    fn cast_to_numbers() {
        assert_eq!(
            UnOperator::Cast(SqlTypeOld::Num(Num::SmallInt)).eval(ScalarValue::Bool(true)),
            Err(QueryExecutionError::cannot_coerce(SqlTypeFamilyOld::Bool, SqlTypeFamilyOld::SmallInt))
        );
    }
}

#[cfg(test)]
mod string {
    use super::*;

    #[test]
    fn cast_to_string() {
        assert_eq!(
            UnOperator::Cast(SqlTypeOld::Str { len: 3, kind: Str::Const }).eval(ScalarValue::String("abc".to_owned())),
            Ok(ScalarValue::String("abc".to_owned()))
        );
    }

    #[test]
    fn cast_to_bool() {
        assert_eq!(
            UnOperator::Cast(SqlTypeOld::Bool).eval(ScalarValue::String("true".to_owned())),
            Ok(ScalarValue::Bool(true))
        );
        assert_eq!(
            UnOperator::Cast(SqlTypeOld::Bool).eval(ScalarValue::String("abc".to_owned())),
            Err(QueryExecutionError::invalid_text_representation(SqlTypeFamilyOld::Bool, "abc".to_owned()))
        );
    }

    #[test]
    fn cast_to_numbers() {
        assert_eq!(
            UnOperator::Cast(SqlTypeOld::Num(Num::SmallInt)).eval(ScalarValue::String("123".to_owned())),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(123),
                type_family: SqlTypeFamilyOld::SmallInt
            })
        );
        assert_eq!(
            UnOperator::Cast(SqlTypeOld::Num(Num::SmallInt)).eval(ScalarValue::String("abc".to_owned())),
            Err(QueryExecutionError::invalid_text_representation(
                SqlTypeFamilyOld::SmallInt,
                "abc".to_owned()
            ))
        );
    }
}

#[cfg(test)]
mod numbers {
    use super::*;

    #[test]
    fn cast_to_string() {
        assert_eq!(
            UnOperator::Cast(SqlTypeOld::Str { len: 3, kind: Str::Const }).eval(ScalarValue::Num {
                value: BigDecimal::from(123),
                type_family: SqlTypeFamilyOld::SmallInt
            }),
            Ok(ScalarValue::String("123".to_owned()))
        );
    }

    #[test]
    fn cast_to_bool() {
        assert_eq!(
            UnOperator::Cast(SqlTypeOld::Bool).eval(ScalarValue::Num {
                value: BigDecimal::from(123),
                type_family: SqlTypeFamilyOld::SmallInt
            }),
            Ok(ScalarValue::Bool(true))
        );
        assert_eq!(
            UnOperator::Cast(SqlTypeOld::Bool).eval(ScalarValue::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamilyOld::SmallInt
            }),
            Ok(ScalarValue::Bool(false))
        );
    }

    #[test]
    fn cast_to_numbers() {
        assert_eq!(
            UnOperator::Cast(SqlTypeOld::small_int()).eval(ScalarValue::Num {
                value: BigDecimal::from(123),
                type_family: SqlTypeFamilyOld::SmallInt
            }),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(123),
                type_family: SqlTypeFamilyOld::SmallInt
            })
        );
    }
}

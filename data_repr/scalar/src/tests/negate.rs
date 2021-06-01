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

#[test]
fn small_int() {
    assert_eq!(ScalarValue::SmallInt(1).negate(), Ok(ScalarValue::SmallInt(-1)));
    assert_eq!(ScalarValue::SmallInt(-1).negate(), Ok(ScalarValue::SmallInt(1)));
}

#[test]
fn integer() {
    assert_eq!(ScalarValue::Integer(1).negate(), Ok(ScalarValue::Integer(-1)));
    assert_eq!(ScalarValue::Integer(-1).negate(), Ok(ScalarValue::Integer(1)));
}

#[test]
fn big_int() {
    assert_eq!(ScalarValue::BigInt(1).negate(), Ok(ScalarValue::BigInt(-1)));
    assert_eq!(ScalarValue::BigInt(-1).negate(), Ok(ScalarValue::BigInt(1)));
}

#[test]
fn boolean() {
    assert_eq!(
        ScalarValue::Bool(true).negate(),
        Err(OperationError::UndefinedFunction {
            sql_type: SqlType::Bool,
            op: UnOperator::Arithmetic(UnArithmetic::Neg)
        })
    );
}

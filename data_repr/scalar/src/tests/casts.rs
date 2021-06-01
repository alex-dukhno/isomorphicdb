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
fn small_int_to_bool() {
    assert_eq!(
        ScalarValue::SmallInt(1).cast_to(SqlType::Bool),
        Err(OperationError::CanNotCoerce {
            from: SqlType::small_int(),
            to: SqlType::Bool
        })
    );
}

#[test]
fn int_to_bool() {
    assert_eq!(ScalarValue::Integer(1).cast_to(SqlType::Bool), Ok(ScalarValue::Bool(true)));
    assert_eq!(ScalarValue::Integer(10).cast_to(SqlType::Bool), Ok(ScalarValue::Bool(true)));
    assert_eq!(ScalarValue::Integer(0).cast_to(SqlType::Bool), Ok(ScalarValue::Bool(false)));
}

#[test]
fn big_int_to_bool() {
    assert_eq!(
        ScalarValue::BigInt(1).cast_to(SqlType::Bool),
        Err(OperationError::CanNotCoerce {
            from: SqlType::big_int(),
            to: SqlType::Bool
        })
    );
}

#[test]
fn bool_to_bool() {
    assert_eq!(ScalarValue::Bool(true).cast_to(SqlType::Bool), Ok(ScalarValue::Bool(true)));
    assert_eq!(ScalarValue::Bool(false).cast_to(SqlType::Bool), Ok(ScalarValue::Bool(false)));
}

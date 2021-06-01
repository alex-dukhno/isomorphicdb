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
fn integer() {
    assert_eq!(
        ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::Int(0))).eval(&[], &[]),
        Ok(ScalarValue::Integer(0))
    );
}

#[test]
fn big_int() {
    assert_eq!(
        ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::BigInt(0))).eval(&[], &[]),
        Ok(ScalarValue::BigInt(0))
    );
}

#[test]
fn string_literal() {
    assert_eq!(
        ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::StringLiteral("abc".to_owned()))).eval(&[], &[]),
        Ok(ScalarValue::StringLiteral("abc".to_owned()))
    );
}

#[test]
fn numeric() {
    assert_eq!(
        ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::Numeric(BigDecimal::from(100)))).eval(&[], &[]),
        Ok(ScalarValue::Numeric(BigDecimal::from(100)))
    );
}

#[test]
fn null() {
    assert_eq!(
        ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::Null)).eval(&[], &[]),
        Ok(ScalarValue::Null(None))
    );
}

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
use std::str::FromStr;

#[test]
fn integer() {
    let type_checker = TypeChecker;
    let typed_tree = typed_int(0);

    assert_eq!(
        type_checker.type_check(typed_tree),
        Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::Int(0))))
    );
}

#[test]
fn bigint() {
    let type_checker = TypeChecker;

    assert_eq!(
        type_checker.type_check(typed_bigint(2147483648)),
        Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::BigInt(2147483648))))
    );
    assert_eq!(
        type_checker.type_check(typed_bigint(-2147483649)),
        Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::BigInt(-2147483649))))
    );
    assert_eq!(
        type_checker.type_check(typed_bigint(9223372036854775807)),
        Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::BigInt(9223372036854775807))))
    );
    assert_eq!(
        type_checker.type_check(typed_bigint(-9223372036854775808)),
        Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::BigInt(-9223372036854775808))))
    );
}

#[test]
fn numeric() {
    let type_checker = TypeChecker;

    assert_eq!(
        type_checker.type_check(typed_number(BigDecimal::from_str("-9223372036854775809").unwrap())),
        Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::Numeric(
            BigDecimal::from_str("-9223372036854775809").unwrap()
        ))))
    );
    assert_eq!(
        type_checker.type_check(typed_number(BigDecimal::from_str("9223372036854775808").unwrap())),
        Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::Numeric(
            BigDecimal::from_str("9223372036854775808").unwrap()
        ))))
    );
    assert_eq!(
        type_checker.type_check(typed_number(BigDecimal::from_str("92233.72036854775808").unwrap())),
        Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::Numeric(
            BigDecimal::from_str("92233.72036854775808").unwrap()
        ))))
    );
}

#[test]
fn string_literal() {
    let type_checker = TypeChecker;

    assert_eq!(
        type_checker.type_check(typed_string("abc")),
        Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::StringLiteral("abc".to_owned()))))
    );
}

#[test]
fn null_literal() {
    let type_checker = TypeChecker;

    assert_eq!(
        type_checker.type_check(typed_null()),
        Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::Null)))
    );
}

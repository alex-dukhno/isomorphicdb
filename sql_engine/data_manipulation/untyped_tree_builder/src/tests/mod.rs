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
use query_ast::Value;

#[test]
fn string_literal() {
    assert_eq!(
        TreeBuilder::insert_position(Expr::Value(Value::String("literal".to_owned()))),
        Ok(UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Literal(
            "literal".to_owned()
        ))))
    );
}

#[test]
fn integer() {
    assert_eq!(
        TreeBuilder::insert_position(Expr::Value(Value::Int(1))),
        Ok(UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Int(1))))
    );
}

#[test]
fn bigint() {
    assert_eq!(
        TreeBuilder::insert_position(Expr::Value(Value::Number("2147483648".to_owned()))),
        Ok(UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::BigInt(2147483648))))
    );
    assert_eq!(
        TreeBuilder::insert_position(Expr::Value(Value::Number("-2147483649".to_owned()))),
        Ok(UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::BigInt(-2147483649))))
    );
    assert_eq!(
        TreeBuilder::insert_position(Expr::Value(Value::Number("9223372036854775807".to_owned()))),
        Ok(UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::BigInt(9223372036854775807))))
    );
    assert_eq!(
        TreeBuilder::insert_position(Expr::Value(Value::Number("-9223372036854775808".to_owned()))),
        Ok(UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::BigInt(-9223372036854775808))))
    );
}

#[test]
fn numeric() {
    assert_eq!(
        TreeBuilder::insert_position(Expr::Value(Value::Number("-9223372036854775809".to_owned()))),
        Ok(UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Number(
            BigDecimal::from_str("-9223372036854775809").unwrap()
        ))))
    );
    assert_eq!(
        TreeBuilder::insert_position(Expr::Value(Value::Number("9223372036854775808".to_owned()))),
        Ok(UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Number(
            BigDecimal::from_str("9223372036854775808").unwrap()
        ))))
    );
    assert_eq!(
        TreeBuilder::insert_position(Expr::Value(Value::Number("92233.72036854775808".to_owned()))),
        Ok(UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Number(
            BigDecimal::from_str("92233.72036854775808").unwrap()
        ))))
    );
}

// Copyright 2020 Alex Dukhno
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
use crate::operation_mapper::OperationMapper;

#[test]
fn addition() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::Plus),
        Operation::Arithmetic(Arithmetic::Add)
    );
}

#[test]
fn subtraction() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::Minus),
        Operation::Arithmetic(Arithmetic::Sub)
    );
}

#[test]
fn multiplication() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::Multiply),
        Operation::Arithmetic(Arithmetic::Mul)
    );
}

#[test]
fn division() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::Divide),
        Operation::Arithmetic(Arithmetic::Div)
    );
}

#[test]
fn exponent() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::BitwiseXor),
        Operation::Arithmetic(Arithmetic::Exp)
    );
}

#[test]
fn modulus() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::Modulus),
        Operation::Arithmetic(Arithmetic::Mod)
    );
}

#[test]
fn string_concat() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::StringConcat),
        Operation::StringOp(StringOp::Concat)
    );
}

#[test]
fn greater_than() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::Gt),
        Operation::Comparison(Comparison::Gt)
    );
}

#[test]
fn greater_than_or_equals() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::GtEq),
        Operation::Comparison(Comparison::GtEq)
    );
}

#[test]
fn less_than() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::Lt),
        Operation::Comparison(Comparison::Lt)
    );
}

#[test]
fn less_than_or_equals() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::LtEq),
        Operation::Comparison(Comparison::LtEq)
    );
}

#[test]
fn equals() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::Eq),
        Operation::Comparison(Comparison::Eq)
    );
}

#[test]
fn not_equals() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::NotEq),
        Operation::Comparison(Comparison::NotEq)
    );
}

#[test]
fn logical_or() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::Or),
        Operation::Logical(Logical::Or)
    );
}

#[test]
fn logical_and() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::And),
        Operation::Logical(Logical::And)
    );
}

#[test]
fn like() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::Like),
        Operation::PatternMatching(PatternMatching::Like)
    );
}

#[test]
fn not_like() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::NotLike),
        Operation::PatternMatching(PatternMatching::NotLike)
    );
}

#[test]
fn bitwise_and() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::BitwiseAnd),
        Operation::Bitwise(Bitwise::And)
    );
}

#[test]
fn bitwise_or() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::BitwiseOr),
        Operation::Bitwise(Bitwise::Or)
    );
}

#[test]
fn bitwise_xor() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::PGBitwiseXor),
        Operation::Bitwise(Bitwise::Xor)
    );
}

#[test]
fn bitwise_shift_left() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::PGBitwiseShiftLeft),
        Operation::Bitwise(Bitwise::ShiftLeft)
    );
}

#[test]
fn bitwise_shift_right() {
    assert_eq!(
        OperationMapper::binary_operation(&ast::BinaryOperator::PGBitwiseShiftRight),
        Operation::Bitwise(Bitwise::ShiftRight)
    );
}

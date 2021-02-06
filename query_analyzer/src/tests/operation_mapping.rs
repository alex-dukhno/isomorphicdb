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
use crate::operation_mapper::OperationMapper;

#[cfg(test)]
mod unary_op {
    use super::*;

    #[test]
    fn minus() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::Minus),
            UnOperation::Arithmetic(UnArithmetic::Neg)
        );
    }

    #[test]
    fn plus() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::Plus),
            UnOperation::Arithmetic(UnArithmetic::Pos)
        );
    }

    #[test]
    fn square_root() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::PGSquareRoot),
            UnOperation::Arithmetic(UnArithmetic::SquareRoot)
        );
    }

    #[test]
    fn cube_root() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::PGCubeRoot),
            UnOperation::Arithmetic(UnArithmetic::CubeRoot)
        );
    }

    #[test]
    fn factorial() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::PGPostfixFactorial),
            UnOperation::Arithmetic(UnArithmetic::Factorial)
        );
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::PGPrefixFactorial),
            UnOperation::Arithmetic(UnArithmetic::Factorial)
        );
    }

    #[test]
    fn abs() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::PGAbs),
            UnOperation::Arithmetic(UnArithmetic::Abs)
        );
    }

    #[test]
    fn not() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::Not),
            UnOperation::Logical(UnLogical::Not)
        );
    }

    #[test]
    fn bitwise_not() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::PGBitwiseNot),
            UnOperation::Bitwise(UnBitwise::Not)
        );
    }
}

#[cfg(test)]
mod binary_op {
    use super::*;

    #[test]
    fn addition() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Plus),
            BiOperation::Arithmetic(BiArithmetic::Add)
        );
    }

    #[test]
    fn subtraction() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Minus),
            BiOperation::Arithmetic(BiArithmetic::Sub)
        );
    }

    #[test]
    fn multiplication() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Multiply),
            BiOperation::Arithmetic(BiArithmetic::Mul)
        );
    }

    #[test]
    fn division() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Divide),
            BiOperation::Arithmetic(BiArithmetic::Div)
        );
    }

    #[test]
    fn exponent() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::BitwiseXor),
            BiOperation::Arithmetic(BiArithmetic::Exp)
        );
    }

    #[test]
    fn modulus() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Modulus),
            BiOperation::Arithmetic(BiArithmetic::Mod)
        );
    }

    #[test]
    fn string_concat() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::StringConcat),
            BiOperation::StringOp(StringOp::Concat)
        );
    }

    #[test]
    fn greater_than() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Gt),
            BiOperation::Comparison(Comparison::Gt)
        );
    }

    #[test]
    fn greater_than_or_equals() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::GtEq),
            BiOperation::Comparison(Comparison::GtEq)
        );
    }

    #[test]
    fn less_than() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Lt),
            BiOperation::Comparison(Comparison::Lt)
        );
    }

    #[test]
    fn less_than_or_equals() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::LtEq),
            BiOperation::Comparison(Comparison::LtEq)
        );
    }

    #[test]
    fn equals() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Eq),
            BiOperation::Comparison(Comparison::Eq)
        );
    }

    #[test]
    fn not_equals() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::NotEq),
            BiOperation::Comparison(Comparison::NotEq)
        );
    }

    #[test]
    fn logical_or() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Or),
            BiOperation::Logical(BiLogical::Or)
        );
    }

    #[test]
    fn logical_and() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::And),
            BiOperation::Logical(BiLogical::And)
        );
    }

    #[test]
    fn like() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Like),
            BiOperation::PatternMatching(PatternMatching::Like)
        );
    }

    #[test]
    fn not_like() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::NotLike),
            BiOperation::PatternMatching(PatternMatching::NotLike)
        );
    }

    #[test]
    fn bitwise_and() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::BitwiseAnd),
            BiOperation::Bitwise(Bitwise::And)
        );
    }

    #[test]
    fn bitwise_or() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::BitwiseOr),
            BiOperation::Bitwise(Bitwise::Or)
        );
    }

    #[test]
    fn bitwise_xor() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::PGBitwiseXor),
            BiOperation::Bitwise(Bitwise::Xor)
        );
    }

    #[test]
    fn bitwise_shift_left() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::PGBitwiseShiftLeft),
            BiOperation::Bitwise(Bitwise::ShiftLeft)
        );
    }

    #[test]
    fn bitwise_shift_right() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::PGBitwiseShiftRight),
            BiOperation::Bitwise(Bitwise::ShiftRight)
        );
    }
}

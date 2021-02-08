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
            UnOperator::Arithmetic(UnArithmetic::Neg)
        );
    }

    #[test]
    fn plus() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::Plus),
            UnOperator::Arithmetic(UnArithmetic::Pos)
        );
    }

    #[test]
    fn square_root() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::PGSquareRoot),
            UnOperator::Arithmetic(UnArithmetic::SquareRoot)
        );
    }

    #[test]
    fn cube_root() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::PGCubeRoot),
            UnOperator::Arithmetic(UnArithmetic::CubeRoot)
        );
    }

    #[test]
    fn factorial() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::PGPostfixFactorial),
            UnOperator::Arithmetic(UnArithmetic::Factorial)
        );
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::PGPrefixFactorial),
            UnOperator::Arithmetic(UnArithmetic::Factorial)
        );
    }

    #[test]
    fn abs() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::PGAbs),
            UnOperator::Arithmetic(UnArithmetic::Abs)
        );
    }

    #[test]
    fn not() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::Not),
            UnOperator::LogicalNot
        );
    }

    #[test]
    fn bitwise_not() {
        assert_eq!(
            OperationMapper::unary_operation(&sql_ast::UnaryOperator::PGBitwiseNot),
            UnOperator::BitwiseNot
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
            BiOperator::Arithmetic(BiArithmetic::Add)
        );
    }

    #[test]
    fn subtraction() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Minus),
            BiOperator::Arithmetic(BiArithmetic::Sub)
        );
    }

    #[test]
    fn multiplication() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Multiply),
            BiOperator::Arithmetic(BiArithmetic::Mul)
        );
    }

    #[test]
    fn division() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Divide),
            BiOperator::Arithmetic(BiArithmetic::Div)
        );
    }

    #[test]
    fn exponent() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::BitwiseXor),
            BiOperator::Arithmetic(BiArithmetic::Exp)
        );
    }

    #[test]
    fn modulus() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Modulus),
            BiOperator::Arithmetic(BiArithmetic::Mod)
        );
    }

    #[test]
    fn string_concat() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::StringConcat),
            BiOperator::StringOp(StringOp::Concat)
        );
    }

    #[test]
    fn greater_than() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Gt),
            BiOperator::Comparison(Comparison::Gt)
        );
    }

    #[test]
    fn greater_than_or_equals() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::GtEq),
            BiOperator::Comparison(Comparison::GtEq)
        );
    }

    #[test]
    fn less_than() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Lt),
            BiOperator::Comparison(Comparison::Lt)
        );
    }

    #[test]
    fn less_than_or_equals() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::LtEq),
            BiOperator::Comparison(Comparison::LtEq)
        );
    }

    #[test]
    fn equals() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Eq),
            BiOperator::Comparison(Comparison::Eq)
        );
    }

    #[test]
    fn not_equals() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::NotEq),
            BiOperator::Comparison(Comparison::NotEq)
        );
    }

    #[test]
    fn logical_or() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Or),
            BiOperator::Logical(BiLogical::Or)
        );
    }

    #[test]
    fn logical_and() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::And),
            BiOperator::Logical(BiLogical::And)
        );
    }

    #[test]
    fn like() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::Like),
            BiOperator::PatternMatching(PatternMatching::Like)
        );
    }

    #[test]
    fn not_like() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::NotLike),
            BiOperator::PatternMatching(PatternMatching::NotLike)
        );
    }

    #[test]
    fn bitwise_and() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::BitwiseAnd),
            BiOperator::Bitwise(Bitwise::And)
        );
    }

    #[test]
    fn bitwise_or() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::BitwiseOr),
            BiOperator::Bitwise(Bitwise::Or)
        );
    }

    #[test]
    fn bitwise_xor() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::PGBitwiseXor),
            BiOperator::Bitwise(Bitwise::Xor)
        );
    }

    #[test]
    fn bitwise_shift_left() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::PGBitwiseShiftLeft),
            BiOperator::Bitwise(Bitwise::ShiftLeft)
        );
    }

    #[test]
    fn bitwise_shift_right() {
        assert_eq!(
            OperationMapper::binary_operation(&sql_ast::BinaryOperator::PGBitwiseShiftRight),
            BiOperator::Bitwise(Bitwise::ShiftRight)
        );
    }
}

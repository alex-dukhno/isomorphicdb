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

use data_manipulation_operators::{
    BiArithmetic, BiLogical, BiOperator, Bitwise, Comparison, PatternMatching, StringOp, UnArithmetic, UnOperator,
};
use sql_ast::UnaryOperator;

pub(crate) struct OperationMapper;

impl OperationMapper {
    pub(crate) fn unary_operation(unary_op: &sql_ast::UnaryOperator) -> UnOperator {
        match unary_op {
            UnaryOperator::Minus => UnOperator::Arithmetic(UnArithmetic::Neg),
            UnaryOperator::Plus => UnOperator::Arithmetic(UnArithmetic::Pos),
            UnaryOperator::Not => UnOperator::LogicalNot,
            UnaryOperator::PGBitwiseNot => UnOperator::BitwiseNot,
            UnaryOperator::PGSquareRoot => UnOperator::Arithmetic(UnArithmetic::SquareRoot),
            UnaryOperator::PGCubeRoot => UnOperator::Arithmetic(UnArithmetic::CubeRoot),
            UnaryOperator::PGPostfixFactorial => UnOperator::Arithmetic(UnArithmetic::Factorial),
            UnaryOperator::PGPrefixFactorial => UnOperator::Arithmetic(UnArithmetic::Factorial),
            UnaryOperator::PGAbs => UnOperator::Arithmetic(UnArithmetic::Abs),
        }
    }

    pub(crate) fn binary_operation(binary_op: &sql_ast::BinaryOperator) -> BiOperator {
        match binary_op {
            sql_ast::BinaryOperator::Plus => BiOperator::Arithmetic(BiArithmetic::Add),
            sql_ast::BinaryOperator::Minus => BiOperator::Arithmetic(BiArithmetic::Sub),
            sql_ast::BinaryOperator::Multiply => BiOperator::Arithmetic(BiArithmetic::Mul),
            sql_ast::BinaryOperator::Divide => BiOperator::Arithmetic(BiArithmetic::Div),
            sql_ast::BinaryOperator::Modulus => BiOperator::Arithmetic(BiArithmetic::Mod),
            sql_ast::BinaryOperator::BitwiseXor => BiOperator::Arithmetic(BiArithmetic::Exp),
            sql_ast::BinaryOperator::StringConcat => BiOperator::StringOp(StringOp::Concat),
            sql_ast::BinaryOperator::Gt => BiOperator::Comparison(Comparison::Gt),
            sql_ast::BinaryOperator::Lt => BiOperator::Comparison(Comparison::Lt),
            sql_ast::BinaryOperator::GtEq => BiOperator::Comparison(Comparison::GtEq),
            sql_ast::BinaryOperator::LtEq => BiOperator::Comparison(Comparison::LtEq),
            sql_ast::BinaryOperator::Eq => BiOperator::Comparison(Comparison::Eq),
            sql_ast::BinaryOperator::NotEq => BiOperator::Comparison(Comparison::NotEq),
            sql_ast::BinaryOperator::And => BiOperator::Logical(BiLogical::And),
            sql_ast::BinaryOperator::Or => BiOperator::Logical(BiLogical::Or),
            sql_ast::BinaryOperator::Like => BiOperator::PatternMatching(PatternMatching::Like),
            sql_ast::BinaryOperator::NotLike => BiOperator::PatternMatching(PatternMatching::NotLike),
            sql_ast::BinaryOperator::BitwiseOr => BiOperator::Bitwise(Bitwise::Or),
            sql_ast::BinaryOperator::BitwiseAnd => BiOperator::Bitwise(Bitwise::And),
            sql_ast::BinaryOperator::PGBitwiseXor => BiOperator::Bitwise(Bitwise::Xor),
            sql_ast::BinaryOperator::PGBitwiseShiftLeft => BiOperator::Bitwise(Bitwise::ShiftLeft),
            sql_ast::BinaryOperator::PGBitwiseShiftRight => BiOperator::Bitwise(Bitwise::ShiftRight),
        }
    }
}

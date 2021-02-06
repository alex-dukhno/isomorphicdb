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
    BiArithmetic, BiLogical, BiOperation, Bitwise, Comparison, PatternMatching, StringOp, UnArithmetic, UnBitwise,
    UnLogical, UnOperation,
};
use sql_ast::UnaryOperator;

pub(crate) struct OperationMapper;

impl OperationMapper {
    pub(crate) fn unary_operation(unary_op: &sql_ast::UnaryOperator) -> UnOperation {
        match unary_op {
            UnaryOperator::Minus => UnOperation::Arithmetic(UnArithmetic::Neg),
            UnaryOperator::Plus => UnOperation::Arithmetic(UnArithmetic::Pos),
            UnaryOperator::Not => UnOperation::Logical(UnLogical::Not),
            UnaryOperator::PGBitwiseNot => UnOperation::Bitwise(UnBitwise::Not),
            UnaryOperator::PGSquareRoot => UnOperation::Arithmetic(UnArithmetic::SquareRoot),
            UnaryOperator::PGCubeRoot => UnOperation::Arithmetic(UnArithmetic::CubeRoot),
            UnaryOperator::PGPostfixFactorial => UnOperation::Arithmetic(UnArithmetic::Factorial),
            UnaryOperator::PGPrefixFactorial => UnOperation::Arithmetic(UnArithmetic::Factorial),
            UnaryOperator::PGAbs => UnOperation::Arithmetic(UnArithmetic::Abs),
        }
    }

    pub(crate) fn binary_operation(binary_op: &sql_ast::BinaryOperator) -> BiOperation {
        match binary_op {
            sql_ast::BinaryOperator::Plus => BiOperation::Arithmetic(BiArithmetic::Add),
            sql_ast::BinaryOperator::Minus => BiOperation::Arithmetic(BiArithmetic::Sub),
            sql_ast::BinaryOperator::Multiply => BiOperation::Arithmetic(BiArithmetic::Mul),
            sql_ast::BinaryOperator::Divide => BiOperation::Arithmetic(BiArithmetic::Div),
            sql_ast::BinaryOperator::Modulus => BiOperation::Arithmetic(BiArithmetic::Mod),
            sql_ast::BinaryOperator::BitwiseXor => BiOperation::Arithmetic(BiArithmetic::Exp),
            sql_ast::BinaryOperator::StringConcat => BiOperation::StringOp(StringOp::Concat),
            sql_ast::BinaryOperator::Gt => BiOperation::Comparison(Comparison::Gt),
            sql_ast::BinaryOperator::Lt => BiOperation::Comparison(Comparison::Lt),
            sql_ast::BinaryOperator::GtEq => BiOperation::Comparison(Comparison::GtEq),
            sql_ast::BinaryOperator::LtEq => BiOperation::Comparison(Comparison::LtEq),
            sql_ast::BinaryOperator::Eq => BiOperation::Comparison(Comparison::Eq),
            sql_ast::BinaryOperator::NotEq => BiOperation::Comparison(Comparison::NotEq),
            sql_ast::BinaryOperator::And => BiOperation::Logical(BiLogical::And),
            sql_ast::BinaryOperator::Or => BiOperation::Logical(BiLogical::Or),
            sql_ast::BinaryOperator::Like => BiOperation::PatternMatching(PatternMatching::Like),
            sql_ast::BinaryOperator::NotLike => BiOperation::PatternMatching(PatternMatching::NotLike),
            sql_ast::BinaryOperator::BitwiseOr => BiOperation::Bitwise(Bitwise::Or),
            sql_ast::BinaryOperator::BitwiseAnd => BiOperation::Bitwise(Bitwise::And),
            sql_ast::BinaryOperator::PGBitwiseXor => BiOperation::Bitwise(Bitwise::Xor),
            sql_ast::BinaryOperator::PGBitwiseShiftLeft => BiOperation::Bitwise(Bitwise::ShiftLeft),
            sql_ast::BinaryOperator::PGBitwiseShiftRight => BiOperation::Bitwise(Bitwise::ShiftRight),
        }
    }
}

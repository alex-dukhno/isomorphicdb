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
    BiArithmetic, BiLogical, BiOperator, Bitwise, Comparison, Concat, Matching, UnArithmetic, UnOperator,
};
use query_ast::{BinaryOperator, UnaryOperator};

pub(crate) struct OperationMapper;

impl OperationMapper {
    pub(crate) fn unary_operation(unary_op: UnaryOperator) -> UnOperator {
        match unary_op {
            UnaryOperator::Minus => UnOperator::Arithmetic(UnArithmetic::Neg),
            UnaryOperator::Plus => UnOperator::Arithmetic(UnArithmetic::Pos),
            UnaryOperator::Not => UnOperator::LogicalNot,
            UnaryOperator::BitwiseNot => UnOperator::BitwiseNot,
            UnaryOperator::SquareRoot => UnOperator::Arithmetic(UnArithmetic::SquareRoot),
            UnaryOperator::CubeRoot => UnOperator::Arithmetic(UnArithmetic::CubeRoot),
            UnaryOperator::PostfixFactorial => UnOperator::Arithmetic(UnArithmetic::Factorial),
            UnaryOperator::PrefixFactorial => UnOperator::Arithmetic(UnArithmetic::Factorial),
            UnaryOperator::Abs => UnOperator::Arithmetic(UnArithmetic::Abs),
        }
    }

    pub(crate) fn binary_operation(binary_op: BinaryOperator) -> BiOperator {
        match binary_op {
            BinaryOperator::Plus => BiOperator::Arithmetic(BiArithmetic::Add),
            BinaryOperator::Minus => BiOperator::Arithmetic(BiArithmetic::Sub),
            BinaryOperator::Multiply => BiOperator::Arithmetic(BiArithmetic::Mul),
            BinaryOperator::Divide => BiOperator::Arithmetic(BiArithmetic::Div),
            BinaryOperator::Modulus => BiOperator::Arithmetic(BiArithmetic::Mod),
            BinaryOperator::Exp => BiOperator::Arithmetic(BiArithmetic::Exp),
            BinaryOperator::StringConcat => BiOperator::StringOp(Concat),
            BinaryOperator::Gt => BiOperator::Comparison(Comparison::Gt),
            BinaryOperator::Lt => BiOperator::Comparison(Comparison::Lt),
            BinaryOperator::GtEq => BiOperator::Comparison(Comparison::GtEq),
            BinaryOperator::LtEq => BiOperator::Comparison(Comparison::LtEq),
            BinaryOperator::Eq => BiOperator::Comparison(Comparison::Eq),
            BinaryOperator::NotEq => BiOperator::Comparison(Comparison::NotEq),
            BinaryOperator::And => BiOperator::Logical(BiLogical::And),
            BinaryOperator::Or => BiOperator::Logical(BiLogical::Or),
            BinaryOperator::Like => BiOperator::Matching(Matching::Like),
            BinaryOperator::NotLike => BiOperator::Matching(Matching::NotLike),
            BinaryOperator::BitwiseOr => BiOperator::Bitwise(Bitwise::Or),
            BinaryOperator::BitwiseAnd => BiOperator::Bitwise(Bitwise::And),
            BinaryOperator::BitwiseXor => BiOperator::Bitwise(Bitwise::Xor),
            BinaryOperator::BitwiseShiftLeft => BiOperator::Bitwise(Bitwise::ShiftLeft),
            BinaryOperator::BitwiseShiftRight => BiOperator::Bitwise(Bitwise::ShiftRight),
        }
    }
}

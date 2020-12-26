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

use expr_operators::{Arithmetic, Bitwise, Comparison, Logical, Operation, PatternMatching, StringOp};
use sqlparser::ast;

pub(crate) struct OperationMapper;

impl OperationMapper {
    pub(crate) fn binary_operation(binary_op: &ast::BinaryOperator) -> Operation {
        match binary_op {
            ast::BinaryOperator::Plus => Operation::Arithmetic(Arithmetic::Add),
            ast::BinaryOperator::Minus => Operation::Arithmetic(Arithmetic::Sub),
            ast::BinaryOperator::Multiply => Operation::Arithmetic(Arithmetic::Mul),
            ast::BinaryOperator::Divide => Operation::Arithmetic(Arithmetic::Div),
            ast::BinaryOperator::Modulus => Operation::Arithmetic(Arithmetic::Mod),
            ast::BinaryOperator::BitwiseXor => Operation::Arithmetic(Arithmetic::Exp),
            ast::BinaryOperator::StringConcat => Operation::StringOp(StringOp::Concat),
            ast::BinaryOperator::Gt => Operation::Comparison(Comparison::Gt),
            ast::BinaryOperator::Lt => Operation::Comparison(Comparison::Lt),
            ast::BinaryOperator::GtEq => Operation::Comparison(Comparison::GtEq),
            ast::BinaryOperator::LtEq => Operation::Comparison(Comparison::LtEq),
            ast::BinaryOperator::Eq => Operation::Comparison(Comparison::Eq),
            ast::BinaryOperator::NotEq => Operation::Comparison(Comparison::NotEq),
            ast::BinaryOperator::And => Operation::Logical(Logical::And),
            ast::BinaryOperator::Or => Operation::Logical(Logical::Or),
            ast::BinaryOperator::Like => Operation::PatternMatching(PatternMatching::Like),
            ast::BinaryOperator::NotLike => Operation::PatternMatching(PatternMatching::NotLike),
            ast::BinaryOperator::BitwiseOr => Operation::Bitwise(Bitwise::Or),
            ast::BinaryOperator::BitwiseAnd => Operation::Bitwise(Bitwise::And),
            ast::BinaryOperator::PGBitwiseXor => Operation::Bitwise(Bitwise::Xor),
            ast::BinaryOperator::PGBitwiseShiftLeft => Operation::Bitwise(Bitwise::ShiftLeft),
            ast::BinaryOperator::PGBitwiseShiftRight => Operation::Bitwise(Bitwise::ShiftRight),
        }
    }
}

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

use expr_operators::{Arithmetic, Bitwise, Comparison, Logical, Operation, PatternMatching, StringOp};

pub(crate) struct OperationMapper;

impl OperationMapper {
    pub(crate) fn binary_operation(binary_op: &sql_ast::BinaryOperator) -> Operation {
        match binary_op {
            sql_ast::BinaryOperator::Plus => Operation::Arithmetic(Arithmetic::Add),
            sql_ast::BinaryOperator::Minus => Operation::Arithmetic(Arithmetic::Sub),
            sql_ast::BinaryOperator::Multiply => Operation::Arithmetic(Arithmetic::Mul),
            sql_ast::BinaryOperator::Divide => Operation::Arithmetic(Arithmetic::Div),
            sql_ast::BinaryOperator::Modulus => Operation::Arithmetic(Arithmetic::Mod),
            sql_ast::BinaryOperator::BitwiseXor => Operation::Arithmetic(Arithmetic::Exp),
            sql_ast::BinaryOperator::StringConcat => Operation::StringOp(StringOp::Concat),
            sql_ast::BinaryOperator::Gt => Operation::Comparison(Comparison::Gt),
            sql_ast::BinaryOperator::Lt => Operation::Comparison(Comparison::Lt),
            sql_ast::BinaryOperator::GtEq => Operation::Comparison(Comparison::GtEq),
            sql_ast::BinaryOperator::LtEq => Operation::Comparison(Comparison::LtEq),
            sql_ast::BinaryOperator::Eq => Operation::Comparison(Comparison::Eq),
            sql_ast::BinaryOperator::NotEq => Operation::Comparison(Comparison::NotEq),
            sql_ast::BinaryOperator::And => Operation::Logical(Logical::And),
            sql_ast::BinaryOperator::Or => Operation::Logical(Logical::Or),
            sql_ast::BinaryOperator::Like => Operation::PatternMatching(PatternMatching::Like),
            sql_ast::BinaryOperator::NotLike => Operation::PatternMatching(PatternMatching::NotLike),
            sql_ast::BinaryOperator::BitwiseOr => Operation::Bitwise(Bitwise::Or),
            sql_ast::BinaryOperator::BitwiseAnd => Operation::Bitwise(Bitwise::And),
            sql_ast::BinaryOperator::PGBitwiseXor => Operation::Bitwise(Bitwise::Xor),
            sql_ast::BinaryOperator::PGBitwiseShiftLeft => Operation::Bitwise(Bitwise::ShiftLeft),
            sql_ast::BinaryOperator::PGBitwiseShiftRight => Operation::Bitwise(Bitwise::ShiftRight),
        }
    }
}

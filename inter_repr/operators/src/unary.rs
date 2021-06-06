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

use query_ast::UnaryOperator;
use std::fmt::{self, Display, Formatter};
use types::SqlTypeFamily;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum UnOperator {
    Arithmetic(UnArithmetic),
    LogicalNot,
    BitwiseNot,
    Cast(SqlTypeFamily),
}

impl From<UnaryOperator> for UnOperator {
    fn from(operator: UnaryOperator) -> UnOperator {
        match operator {
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
}

impl Display for UnOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            UnOperator::Arithmetic(op) => write!(f, "{}", op),
            UnOperator::LogicalNot => write!(f, "NOT"),
            UnOperator::BitwiseNot => write!(f, "~"),
            UnOperator::Cast(type_family) => write!(f, "::{}", type_family),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum UnArithmetic {
    Neg,
    Pos,
    SquareRoot,
    CubeRoot,
    Factorial,
    Abs,
}

impl Display for UnArithmetic {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            UnArithmetic::Neg => write!(f, "-"),
            UnArithmetic::Pos => write!(f, "+"),
            UnArithmetic::SquareRoot => write!(f, "|/"),
            UnArithmetic::CubeRoot => write!(f, "||/"),
            UnArithmetic::Factorial => write!(f, "!"),
            UnArithmetic::Abs => write!(f, "@"),
        }
    }
}

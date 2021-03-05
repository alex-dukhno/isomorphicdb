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

#[rstest::rstest(
    ast_operator,
    expected,
    case::plus(UnaryOperator::Plus, UnOperator::Arithmetic(UnArithmetic::Pos)),
    case::minus(UnaryOperator::Minus, UnOperator::Arithmetic(UnArithmetic::Neg)),
    case::not(UnaryOperator::Not, UnOperator::LogicalNot),
    case::bitwise_not(UnaryOperator::BitwiseNot, UnOperator::BitwiseNot),
    case::square_root(UnaryOperator::SquareRoot, UnOperator::Arithmetic(UnArithmetic::SquareRoot)),
    case::cube_root(UnaryOperator::CubeRoot, UnOperator::Arithmetic(UnArithmetic::CubeRoot)),
    case::prefix_factorial(UnaryOperator::PrefixFactorial, UnOperator::Arithmetic(UnArithmetic::Factorial)),
    case::prefix_factorial(UnaryOperator::PostfixFactorial, UnOperator::Arithmetic(UnArithmetic::Factorial)),
    case::abs(UnaryOperator::Abs, UnOperator::Arithmetic(UnArithmetic::Abs))
)]
fn unary_op(ast_operator: UnaryOperator, expected: UnOperator) {
    assert_eq!(UnOperator::from(ast_operator), expected);
}

#[rstest::rstest(
    ast_operator,
    expected,
    case::plus(BinaryOperator::Plus, BiOperator::Arithmetic(BiArithmetic::Add)),
    case::minus(BinaryOperator::Minus, BiOperator::Arithmetic(BiArithmetic::Sub)),
    case::multiply(BinaryOperator::Multiply, BiOperator::Arithmetic(BiArithmetic::Mul)),
    case::divide(BinaryOperator::Divide, BiOperator::Arithmetic(BiArithmetic::Div)),
    case::modulus(BinaryOperator::Modulus, BiOperator::Arithmetic(BiArithmetic::Mod)),
    case::exp(BinaryOperator::Exp, BiOperator::Arithmetic(BiArithmetic::Exp)),
    case::string_concat(BinaryOperator::StringConcat, BiOperator::StringOp(Concat)),
    case::gt(BinaryOperator::Gt, BiOperator::Comparison(Comparison::Gt)),
    case::lt(BinaryOperator::Lt, BiOperator::Comparison(Comparison::Lt)),
    case::gt_eq(BinaryOperator::GtEq, BiOperator::Comparison(Comparison::GtEq)),
    case::lt_eq(BinaryOperator::LtEq, BiOperator::Comparison(Comparison::LtEq)),
    case::eq(BinaryOperator::Eq, BiOperator::Comparison(Comparison::Eq)),
    case::not_eq(BinaryOperator::NotEq, BiOperator::Comparison(Comparison::NotEq)),
    case::and(BinaryOperator::And, BiOperator::Logical(BiLogical::And)),
    case::or(BinaryOperator::Or, BiOperator::Logical(BiLogical::Or)),
    case::like(BinaryOperator::Like, BiOperator::Matching(Matching::Like)),
    case::not_like(BinaryOperator::NotLike, BiOperator::Matching(Matching::NotLike)),
    case::bitwise_or(BinaryOperator::BitwiseOr, BiOperator::Bitwise(Bitwise::Or)),
    case::bitwise_and(BinaryOperator::BitwiseAnd, BiOperator::Bitwise(Bitwise::And)),
    case::bitwise_xor(BinaryOperator::BitwiseXor, BiOperator::Bitwise(Bitwise::Xor)),
    case::bitwise_shift_left(BinaryOperator::BitwiseShiftLeft, BiOperator::Bitwise(Bitwise::ShiftLeft)),
    case::bitwise_shift_right(BinaryOperator::BitwiseShiftRight, BiOperator::Bitwise(Bitwise::ShiftRight))
)]
fn binary_op(ast_operator: BinaryOperator, expected: BiOperator) {
    assert_eq!(BiOperator::from(ast_operator), expected);
}

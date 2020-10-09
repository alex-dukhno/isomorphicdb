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
use ast::{operations::ScalarOp, values::ScalarValue};

#[rstest::fixture]
fn static_expression_evaluation() -> StaticExpressionEvaluation {
    StaticExpressionEvaluation::new()
}

#[rstest::rstest]
fn column(static_expression_evaluation: StaticExpressionEvaluation) {
    assert_eq!(
        static_expression_evaluation.eval(&ScalarOp::Column("name".to_owned())),
        Ok(ScalarOp::Column("name".to_owned()))
    );
}

#[rstest::rstest]
fn value(static_expression_evaluation: StaticExpressionEvaluation) {
    assert_eq!(
        static_expression_evaluation.eval(&ScalarOp::Value(ScalarValue::Number(BigDecimal::from(100i16))),),
        Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(100i16))))
    );
}

#[cfg(test)]
mod binary_operation {
    use super::*;

    #[cfg(test)]
    mod integers {
        use super::*;

        #[rstest::rstest]
        fn number_concatenation(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Concat,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(10)))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(10))))
                )),
                Err(EvalError::undefined_function(&"||", &"NUMBER", &"NUMBER"))
            );
        }

        #[rstest::rstest]
        fn addition(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Add,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                )),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 + 5))))
            );
        }

        #[rstest::rstest]
        fn subtraction(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Sub,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                )),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 - 5))))
            );
        }

        #[rstest::rstest]
        fn multiplication(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Mul,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                )),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 * 5))))
            );
        }

        #[rstest::rstest]
        fn division(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Div,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                )),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 / 5))))
            );
        }

        #[rstest::rstest]
        fn modulo(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Mod,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(3))))
                )),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 % 3))))
            );
        }

        #[rstest::rstest]
        fn bitwise_and(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::BitwiseAnd,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                )),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 & 4))))
            );
        }

        #[rstest::rstest]
        fn bitwise_or(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::BitwiseOr,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                )),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 | 5))))
            );
        }
    }

    #[cfg(test)]
    mod floats {
        use super::*;
        use std::convert::TryFrom;

        #[rstest::rstest]
        fn number_concatenation(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Concat,
                    Box::new(ScalarOp::Value(ScalarValue::Number(
                        BigDecimal::try_from(20.1).unwrap()
                    ))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                )),
                Err(EvalError::undefined_function(&"||", &"NUMBER", &"NUMBER"))
            );
        }

        #[rstest::rstest]
        fn addition(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Add,
                    Box::new(ScalarOp::Value(ScalarValue::Number(
                        BigDecimal::try_from(20.1).unwrap()
                    ))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                )),
                Ok(ScalarOp::Value(ScalarValue::Number(
                    BigDecimal::try_from(20.1).unwrap() + BigDecimal::try_from(5.2).unwrap()
                )))
            );
        }

        #[rstest::rstest]
        fn subtraction(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Sub,
                    Box::new(ScalarOp::Value(ScalarValue::Number(
                        BigDecimal::try_from(20.1).unwrap()
                    ))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                )),
                Ok(ScalarOp::Value(ScalarValue::Number(
                    BigDecimal::try_from(20.1).unwrap() - BigDecimal::try_from(5.2).unwrap()
                )))
            );
        }

        #[rstest::rstest]
        fn multiplication(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Mul,
                    Box::new(ScalarOp::Value(ScalarValue::Number(
                        BigDecimal::try_from(20.1).unwrap()
                    ))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                )),
                Ok(ScalarOp::Value(ScalarValue::Number(
                    BigDecimal::try_from(20.1).unwrap() * BigDecimal::try_from(5.2).unwrap()
                )))
            );
        }

        #[rstest::rstest]
        fn division(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Div,
                    Box::new(ScalarOp::Value(ScalarValue::Number(
                        BigDecimal::try_from(20.1).unwrap()
                    ))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                )),
                Ok(ScalarOp::Value(ScalarValue::Number(
                    BigDecimal::try_from(20.1).unwrap() / BigDecimal::try_from(5.2).unwrap()
                )))
            );
        }

        #[rstest::rstest]
        fn modulo(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Mod,
                    Box::new(ScalarOp::Value(ScalarValue::Number(
                        BigDecimal::try_from(20.1).unwrap()
                    ))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                )),
                Ok(ScalarOp::Value(ScalarValue::Number(
                    BigDecimal::try_from(20.1).unwrap() % BigDecimal::try_from(5.2).unwrap()
                )))
            );
        }

        #[rstest::rstest]
        fn bitwise_and(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::BitwiseAnd,
                    Box::new(ScalarOp::Value(ScalarValue::Number(
                        BigDecimal::try_from(20.1).unwrap()
                    ))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                )),
                Err(EvalError::undefined_function(&"&", &"FLOAT", &"FLOAT"))
            );
        }

        #[rstest::rstest]
        fn bitwise_or(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::BitwiseOr,
                    Box::new(ScalarOp::Value(ScalarValue::Number(
                        BigDecimal::try_from(20.1).unwrap()
                    ))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                )),
                Err(EvalError::undefined_function(&"|", &"FLOAT", &"FLOAT"))
            );
        }
    }

    #[cfg(test)]
    mod strings {
        use super::*;

        #[rstest::rstest]
        fn concatenation(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Concat,
                    Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                )),
                Ok(ScalarOp::Value(ScalarValue::String(format!("{}{}", "str-1", "str-2"))))
            );
        }

        #[rstest::rstest]
        fn addition(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Add,
                    Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                )),
                Err(EvalError::undefined_function(&"+", &"STRING", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn subtraction(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Sub,
                    Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                )),
                Err(EvalError::undefined_function(&"-", &"STRING", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn multiplication(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Mul,
                    Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                )),
                Err(EvalError::undefined_function(&"*", &"STRING", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn division(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Div,
                    Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                )),
                Err(EvalError::undefined_function(&"/", &"STRING", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn modulo(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Mod,
                    Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                )),
                Err(EvalError::undefined_function(&"%", &"STRING", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn bitwise_and(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::BitwiseAnd,
                    Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                )),
                Err(EvalError::undefined_function(&"&", &"STRING", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn bitwise_or(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::BitwiseOr,
                    Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                )),
                Err(EvalError::undefined_function(&"|", &"STRING", &"STRING"))
            );
        }
    }

    #[cfg(test)]
    mod string_number {
        use super::*;

        #[rstest::rstest]
        fn concatenation(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Concat,
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(10))))
                )),
                Ok(ScalarOp::Value(ScalarValue::String("str10".to_owned())))
            );
        }

        #[rstest::rstest]
        fn addition(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Add,
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                )),
                Err(EvalError::undefined_function(&"+", &"STRING", &"NUMBER"))
            );
        }

        #[rstest::rstest]
        fn subtraction(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Sub,
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                )),
                Err(EvalError::undefined_function(&"-", &"STRING", &"NUMBER"))
            );
        }

        #[rstest::rstest]
        fn multiplication(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Mul,
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                )),
                Err(EvalError::undefined_function(&"*", &"STRING", &"NUMBER"))
            );
        }

        #[rstest::rstest]
        fn division(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Div,
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                )),
                Err(EvalError::undefined_function(&"/", &"STRING", &"NUMBER"))
            );
        }

        #[rstest::rstest]
        fn modulo(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Mod,
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(3))))
                )),
                Err(EvalError::undefined_function(&"%", &"STRING", &"NUMBER"))
            );
        }

        #[rstest::rstest]
        fn bitwise_and(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::BitwiseAnd,
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                )),
                Err(EvalError::undefined_function(&"&", &"STRING", &"NUMBER"))
            );
        }

        #[rstest::rstest]
        fn bitwise_or(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::BitwiseOr,
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_owned()))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                )),
                Err(EvalError::undefined_function(&"|", &"STRING", &"NUMBER"))
            );
        }
    }

    #[cfg(test)]
    mod number_string {
        use super::*;

        #[rstest::rstest]
        fn concatenation(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Concat,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(10)))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_string())))
                )),
                Ok(ScalarOp::Value(ScalarValue::String("10str".to_owned())))
            );
        }

        #[rstest::rstest]
        fn addition(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Add,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_string())))
                )),
                Err(EvalError::undefined_function(&"+", &"NUMBER", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn subtraction(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Sub,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_string())))
                )),
                Err(EvalError::undefined_function(&"-", &"NUMBER", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn multiplication(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Mul,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_string())))
                )),
                Err(EvalError::undefined_function(&"*", &"NUMBER", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn division(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Div,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_string())))
                )),
                Err(EvalError::undefined_function(&"/", &"NUMBER", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn modulo(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::Mod,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_string())))
                )),
                Err(EvalError::undefined_function(&"%", &"NUMBER", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn bitwise_and(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::BitwiseAnd,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_string())))
                )),
                Err(EvalError::undefined_function(&"&", &"NUMBER", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn bitwise_or(static_expression_evaluation: StaticExpressionEvaluation) {
            assert_eq!(
                static_expression_evaluation.eval(&ScalarOp::Binary(
                    BinaryOp::BitwiseOr,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                    Box::new(ScalarOp::Value(ScalarValue::String("str".to_string())))
                )),
                Err(EvalError::undefined_function(&"|", &"NUMBER", &"STRING"))
            );
        }
    }
}

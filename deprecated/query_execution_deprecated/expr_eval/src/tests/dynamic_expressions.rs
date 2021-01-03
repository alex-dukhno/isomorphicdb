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
use std::collections::HashMap;

const COLUMN: &str = "name";

#[rstest::fixture]
fn dynamic_expression_evaluation() -> DynamicExpressionEvaluation {
    let mut columns = HashMap::new();
    columns.insert(COLUMN.to_owned(), 0);
    DynamicExpressionEvaluation::new(columns)
}

#[rstest::rstest]
fn column(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
    assert_eq!(
        dynamic_expression_evaluation.eval(
            &[Datum::from_i16(10)],
            &ScalarOp::Binary(
                BinaryOp::Add,
                Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                Box::new(ScalarOp::Column(COLUMN.to_owned()))
            )
        ),
        Ok(ScalarOp::Value(ScalarValue::Number(
            BigDecimal::from(10i16) + BigDecimal::from(20)
        )))
    );
}

#[rstest::rstest]
fn column_inside_binary_operation(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
    assert_eq!(
        dynamic_expression_evaluation.eval(&[Datum::from_i16(10)], &ScalarOp::Column(COLUMN.to_owned())),
        Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(10i16))))
    );
}

#[rstest::rstest]
fn value(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
    assert_eq!(
        dynamic_expression_evaluation.eval(
            &[Datum::from_i16(10)],
            &ScalarOp::Value(ScalarValue::Number(BigDecimal::from(100i16))),
        ),
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
        fn number_concatenation(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Concat,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(10)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(10))))
                    ),
                ),
                Err(EvalError::undefined_function(&"||", &"NUMBER", &"NUMBER"))
            );
        }

        #[rstest::rstest]
        fn addition(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Add,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 + 5))))
            );
        }

        #[rstest::rstest]
        fn subtraction(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Sub,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 - 5))))
            );
        }

        #[rstest::rstest]
        fn multiplication(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Mul,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 * 5))))
            );
        }

        #[rstest::rstest]
        fn division(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Div,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 / 5))))
            );
        }

        #[rstest::rstest]
        fn modulo(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Mod,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(3))))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 % 3))))
            );
        }

        #[rstest::rstest]
        fn bitwise_and(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::BitwiseAnd,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 & 4))))
            );
        }

        #[rstest::rstest]
        fn bitwise_or(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::BitwiseOr,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20 | 5))))
            );
        }
    }

    #[cfg(test)]
    mod floats {
        use super::*;
        use std::convert::TryFrom;

        #[rstest::rstest]
        fn number_concatenation(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Concat,
                        Box::new(ScalarOp::Value(ScalarValue::Number(
                            BigDecimal::try_from(20.1).unwrap()
                        ))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                    ),
                ),
                Err(EvalError::undefined_function(&"||", &"NUMBER", &"NUMBER"))
            );
        }

        #[rstest::rstest]
        fn addition(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Add,
                        Box::new(ScalarOp::Value(ScalarValue::Number(
                            BigDecimal::try_from(20.1).unwrap()
                        ))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::Number(
                    BigDecimal::try_from(20.1 + 5.2).unwrap()
                )))
            );
        }

        #[rstest::rstest]
        fn subtraction(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Sub,
                        Box::new(ScalarOp::Value(ScalarValue::Number(
                            BigDecimal::try_from(20.1).unwrap()
                        ))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::Number(
                    BigDecimal::try_from(20.1 - 5.2).unwrap()
                )))
            );
        }

        #[rstest::rstest]
        fn multiplication(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Mul,
                        Box::new(ScalarOp::Value(ScalarValue::Number(
                            BigDecimal::try_from(20.1).unwrap()
                        ))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::Number(
                    BigDecimal::try_from(20.1 * 5.2).unwrap()
                )))
            );
        }

        #[rstest::rstest]
        fn division(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Div,
                        Box::new(ScalarOp::Value(ScalarValue::Number(
                            BigDecimal::try_from(20.1).unwrap()
                        ))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::Number(
                    BigDecimal::try_from(20.1).unwrap() / BigDecimal::try_from(5.2).unwrap()
                )))
            );
        }

        #[rstest::rstest]
        fn modulo(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Mod,
                        Box::new(ScalarOp::Value(ScalarValue::Number(
                            BigDecimal::try_from(20.1).unwrap()
                        ))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::Number(
                    BigDecimal::try_from(20.1).unwrap() % BigDecimal::try_from(5.2).unwrap()
                )))
            );
        }

        #[rstest::rstest]
        fn bitwise_and(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::BitwiseAnd,
                        Box::new(ScalarOp::Value(ScalarValue::Number(
                            BigDecimal::try_from(20.1).unwrap()
                        ))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                    ),
                ),
                Err(EvalError::undefined_function(&"&", &"FLOAT", &"FLOAT"))
            );
        }

        #[rstest::rstest]
        fn bitwise_or(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::BitwiseOr,
                        Box::new(ScalarOp::Value(ScalarValue::Number(
                            BigDecimal::try_from(20.1).unwrap()
                        ))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::try_from(5.2).unwrap())))
                    ),
                ),
                Err(EvalError::undefined_function(&"|", &"FLOAT", &"FLOAT"))
            );
        }
    }

    #[cfg(test)]
    mod strings {
        use super::*;

        #[rstest::rstest]
        fn concatenation(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Concat,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Ok(ScalarOp::Value(ScalarValue::String(format!("{}{}", "str-1", "str-2"))))
            );
        }

        #[rstest::rstest]
        fn addition(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Add,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(EvalError::undefined_function(&"+", &"STRING", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn subtraction(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Sub,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(EvalError::undefined_function(&"-", &"STRING", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn multiplication(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Mul,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(EvalError::undefined_function(&"*", &"STRING", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn division(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Div,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(EvalError::undefined_function(&"/", &"STRING", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn modulo(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Mod,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(EvalError::undefined_function(&"%", &"STRING", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn bitwise_and(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::BitwiseAnd,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(EvalError::undefined_function(&"&", &"STRING", &"STRING"))
            );
        }

        #[rstest::rstest]
        fn bitwise_or(dynamic_expression_evaluation: DynamicExpressionEvaluation) {
            assert_eq!(
                dynamic_expression_evaluation.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::BitwiseOr,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(EvalError::undefined_function(&"|", &"STRING", &"STRING"))
            );
        }
    }
}

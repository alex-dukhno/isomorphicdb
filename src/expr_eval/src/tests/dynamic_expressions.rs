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
use crate::dynamic_expr::DynamicExpressionEvaluation;
use std::collections::HashMap;

fn eval(sender: ResultCollector) -> DynamicExpressionEvaluation {
    let mut columns = HashMap::new();
    columns.insert("name".to_owned(), 0);
    DynamicExpressionEvaluation::new(sender.clone(), columns)
}

#[test]
fn column() {
    let sender = sender();
    let eval = eval(sender.clone());

    assert_eq!(
        eval.eval(&[Datum::from_i16(10)], &ScalarOp::Column("name".to_owned())),
        Ok(Datum::from_i16(10))
    );

    sender.assert_content(vec![]);
}

#[test]
fn value() {
    let sender = sender();
    let eval = eval(sender.clone());

    assert_eq!(
        eval.eval(
            &[Datum::from_i16(10)],
            &ScalarOp::Value(ScalarValue::Number(BigDecimal::from(100i16))),
        ),
        Ok(Datum::from_i16(100))
    );

    sender.assert_content(vec![]);
}

#[cfg(test)]
mod binary_operation {
    use super::*;

    #[cfg(test)]
    mod integers {
        use super::*;

        #[test]
        fn number_concatenation() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Concat,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(10)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(10))))
                    ),
                ),
                Err(())
            );

            sender.assert_content(vec![Err(QueryError::undefined_function("||", "INTEGER", "INTEGER"))]);
        }

        #[test]
        fn addition() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Add,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                    ),
                ),
                Ok(Datum::from_i16(20 + 5))
            );

            sender.assert_content(vec![]);
        }

        #[test]
        fn subtraction() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Sub,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                    ),
                ),
                Ok(Datum::from_i16(20 - 5))
            );

            sender.assert_content(vec![]);
        }

        #[test]
        fn multiplication() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Mul,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                    ),
                ),
                Ok(Datum::from_i16(20 * 5))
            );

            sender.assert_content(vec![]);
        }

        #[test]
        fn division() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Div,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                    ),
                ),
                Ok(Datum::from_i16(20 / 5))
            );

            sender.assert_content(vec![]);
        }

        #[test]
        fn modulo() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Mod,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(3))))
                    ),
                ),
                Ok(Datum::from_i16(20 % 3))
            );

            sender.assert_content(vec![]);
        }

        #[test]
        fn bitwise_and() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::BitwiseAnd,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                    ),
                ),
                Ok(Datum::from_i16(20 & 4))
            );

            sender.assert_content(vec![]);
        }

        #[test]
        fn bitwise_or() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::BitwiseOr,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5))))
                    ),
                ),
                Ok(Datum::from_i16(20 | 5))
            );

            sender.assert_content(vec![]);
        }
    }

    #[cfg(test)]
    mod floats {
        use super::*;

        #[test]
        fn number_concatenation() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Concat,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20.1)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5.2))))
                    ),
                ),
                Err(())
            );

            sender.assert_content(vec![Err(QueryError::undefined_function("||", "FLOAT", "FLOAT"))]);
        }

        #[test]
        fn addition() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Add,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20.1)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5.2))))
                    ),
                ),
                Ok(Datum::from_f32(20.1 + 5.2))
            );

            sender.assert_content(vec![]);
        }

        #[test]
        fn subtraction() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Sub,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20.1)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5.2))))
                    ),
                ),
                Ok(Datum::from_f32(20.1 - 5.2))
            );

            sender.assert_content(vec![]);
        }

        #[test]
        fn multiplication() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Mul,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20.1)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5.2))))
                    ),
                ),
                Ok(Datum::from_f32(20.1 * 5.2))
            );

            sender.assert_content(vec![]);
        }

        #[test]
        fn division() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Div,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20.1)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5.2))))
                    ),
                ),
                Ok(Datum::from_f32(20.1 / 5.2))
            );

            sender.assert_content(vec![]);
        }

        #[test]
        fn modulo() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Mod,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20.1)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5.2))))
                    ),
                ),
                Err(())
            );

            sender.assert_content(vec![Err(QueryError::undefined_function("%", "FLOAT", "FLOAT"))]);
        }

        #[test]
        fn bitwise_and() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::BitwiseAnd,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20.1)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5.2))))
                    ),
                ),
                Err(())
            );

            sender.assert_content(vec![Err(QueryError::undefined_function("&", "FLOAT", "FLOAT"))]);
        }

        #[test]
        fn bitwise_or() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::BitwiseOr,
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(20.1)))),
                        Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(5.2))))
                    ),
                ),
                Err(())
            );

            sender.assert_content(vec![Err(QueryError::undefined_function("|", "FLOAT", "FLOAT"))]);
        }
    }

    #[cfg(test)]
    mod strings {
        use super::*;

        #[test]
        fn concatenation() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Concat,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Ok(Datum::from_string(format!("{}{}", "str-1", "str-2")))
            );
        }

        #[test]
        fn addition() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Add,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(())
            );

            sender.assert_content(vec![Err(QueryError::undefined_function("+", "STRING", "STRING"))]);
        }

        #[test]
        fn subtraction() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Sub,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(())
            );

            sender.assert_content(vec![Err(QueryError::undefined_function("-", "STRING", "STRING"))]);
        }

        #[test]
        fn multiplication() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Mul,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(())
            );

            sender.assert_content(vec![Err(QueryError::undefined_function("*", "STRING", "STRING"))]);
        }

        #[test]
        fn division() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Div,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(())
            );

            sender.assert_content(vec![Err(QueryError::undefined_function("/", "STRING", "STRING"))]);
        }

        #[test]
        fn modulo() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::Mod,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(())
            );

            sender.assert_content(vec![Err(QueryError::undefined_function("%", "STRING", "STRING"))]);
        }

        #[test]
        fn bitwise_and() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::BitwiseAnd,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(())
            );

            sender.assert_content(vec![Err(QueryError::undefined_function("&", "STRING", "STRING"))]);
        }

        #[test]
        fn bitwise_or() {
            let sender = sender();
            let eval = eval(sender.clone());

            assert_eq!(
                eval.eval(
                    &[Datum::from_i16(10)],
                    &ScalarOp::Binary(
                        BinaryOp::BitwiseOr,
                        Box::new(ScalarOp::Value(ScalarValue::String("str-1".to_owned()))),
                        Box::new(ScalarOp::Value(ScalarValue::String("str-2".to_owned())))
                    ),
                ),
                Err(())
            );

            sender.assert_content(vec![Err(QueryError::undefined_function("|", "STRING", "STRING"))]);
        }
    }
}

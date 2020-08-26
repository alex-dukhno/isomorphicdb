use crate::query::scalar::ScalarOp;
use protocol::results::{QueryErrorBuilder, QueryResult};
use protocol::Sender;
use representation::{Datum, EvalError};
use sqlparser::ast::{BinaryOperator, Expr, Value, DataType, UnaryOperator};
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;
use std::str::FromStr;

pub(crate) struct ExpressionEvaluation {
    session: Arc<dyn Sender>,
}

impl ExpressionEvaluation {
    pub(crate) fn new(session: Arc<dyn Sender>) -> ExpressionEvaluation {
        ExpressionEvaluation { session }
    }

    pub(crate) fn eval(&self, expr: &Expr) -> Result<ScalarOp, ()> {
        self.inner_eval(expr)
    }

    fn inner_eval(&self, expr: &Expr) -> Result<ScalarOp, ()> {
        match expr {
            Expr::Cast { expr, data_type } => match (&**expr, data_type) {
                (Expr::Value(Value::SingleQuotedString(v)), DataType::Boolean) => {
                    Ok(ScalarOp::Literal(Datum::from_bool(bool::from_str(v).unwrap())))
                }
                (Expr::Value(Value::Boolean(val)), DataType::Boolean) => Ok(ScalarOp::Literal(Datum::from_bool(*val))),
                _ => {
                    self.session
                        .send(Err(QueryErrorBuilder::new()
                            .syntax_error(format!(
                                "Cast from {:?} to {:?} is not currently supported",
                                expr, data_type
                            ))
                            .build()))
                        .expect("To Send Query Result to Client");
                    return Err(());
                }
            },
            Expr::UnaryOp { op, expr } => {
                // let operand = self.inner_eval(expr.deref())?;
                match (op, expr.deref()) {
                    (UnaryOperator::Minus, Expr::Value(Value::Number(value))) => {
                        match Datum::try_from(&Value::Number(-value)) {
                            Ok(datum) => Ok(ScalarOp::Literal(datum)),
                            Err(e) => {
                                let err = match e {
                                    EvalError::UnsupportedDatum(ty) =>
                                        QueryErrorBuilder::new().
                                            feature_not_supported(format!("Data type not supported: {}", ty))
                                            .build(),
                                    EvalError::OutOfRangeNumeric(ty) => {
                                        let mut builder = QueryErrorBuilder::new();
                                        builder.out_of_range(ty.to_pg_types(), String::new(), 0);
                                        builder.build()
                                    },
                                    EvalError::UnsupportedOperation => QueryErrorBuilder::new()
                                        .feature_not_supported("Use of unsupported expression feature".to_string())
                                        .build(),
                                };

                                self.session.send(Err(err));
                                Err(())
                            }
                        }
                    }
                    // (UnaryOperator::Minus, ScalarOp::Literal(datum)) => {
                    //     let datum = match datum {
                    //         Datum::Int16(val) => Datum::Int16(-val),
                    //         Datum::Int32(val) => Datum::Int32(-val),
                    //         Datum::Int64(val) => Datum::Int64(-val),
                    //         Datum::Float32(val) => Datum::Float32(-val),
                    //         Datum::Float64(val) => Datum::Float64(-val),
                    //         _ => {
                    //             self.session
                    //                 .send(Err(QueryErrorBuilder::new()
                    //                     .syntax_error(op.to_string() + expr.to_string().as_str())
                    //                     .build()))
                    //                 .expect("To Send Query Result to Client");
                    //             return Err(());
                    //         }
                    //     };
                    //     Ok(ScalarOp::Literal(datum))
                    // }
                    (op, operand) => {
                        self.session
                            .send(Err(QueryErrorBuilder::new()
                                .syntax_error(op.to_string() + expr.to_string().as_str())
                                .build()))
                            .expect("To Send Query Result to Client");
                        return Err(());
                    }
                }
            }
            Expr::BinaryOp { op, left, right } => {
                let lhs = self.inner_eval(left.deref())?;
                let rhs = self.inner_eval(right.deref())?;
                match (lhs, rhs) {
                    (ScalarOp::Literal(left), ScalarOp::Literal(right)) => {
                        if left.is_integer() && right.is_integer() {
                            match op {
                                BinaryOperator::Plus => Ok(ScalarOp::Literal(left + right)),
                                BinaryOperator::Minus => Ok(ScalarOp::Literal(left - right)),
                                BinaryOperator::Multiply => Ok(ScalarOp::Literal(left * right)),
                                BinaryOperator::Divide => Ok(ScalarOp::Literal(left / right)),
                                BinaryOperator::Modulus => Ok(ScalarOp::Literal(left % right)),
                                BinaryOperator::BitwiseAnd => Ok(ScalarOp::Literal(left & right)),
                                BinaryOperator::BitwiseOr => Ok(ScalarOp::Literal(left | right)),
                                _ => panic!(),
                            }
                        }
                        else if left.is_float() && right.is_float() {
                            match op {
                                BinaryOperator::Plus => Ok(ScalarOp::Literal(left + right)),
                                BinaryOperator::Minus => Ok(ScalarOp::Literal(left - right)),
                                BinaryOperator::Multiply => Ok(ScalarOp::Literal(left * right)),
                                BinaryOperator::Divide => Ok(ScalarOp::Literal(left / right)),
                                _ => panic!()
                            }
                        }
                        else {
                            self.session
                                .send(Err(QueryErrorBuilder::new().syntax_error(expr.to_string()).build()))
                                .expect("To Send Query Result to Client");
                            Err(())
                        }
                    }
                    (_, _) => panic!(),
                }
            }
            Expr::Value(value) => match Datum::try_from(value) {
                Ok(datum) => Ok(ScalarOp::Literal(datum)),
                Err(e) => {
                    let err = match e {
                        EvalError::UnsupportedDatum(ty) =>
                            QueryErrorBuilder::new().
                                feature_not_supported(format!("Data type not supported: {}", ty))
                                .build(),
                        EvalError::OutOfRangeNumeric(ty) => {
                            let mut builder = QueryErrorBuilder::new();
                            builder.out_of_range(ty.to_pg_types(), String::new(), 0);
                            builder.build()
                        },
                        EvalError::UnsupportedOperation => QueryErrorBuilder::new()
                            .feature_not_supported("Use of unsupported expression feature".to_string())
                            .build(),
                    };

                    self.session.send(Err(err));
                    Err(())
                }
            },
            _ => {
                self.session
                    .send(Err(QueryErrorBuilder::new().syntax_error(expr.to_string()).build()))
                    .expect("To Send Query Result to Client");
                Err(())
            }
        }
    }
}

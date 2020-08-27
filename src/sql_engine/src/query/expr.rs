use crate::query::scalar::ScalarOp;
use protocol::results::{QueryErrorBuilder, QueryResult};
use protocol::Sender;
use representation::{Datum, EvalError};
use sqlparser::ast::{BinaryOperator, DataType, Expr, UnaryOperator, Value};
use std::convert::TryFrom;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use crate::{TableDefinition, ColumnDefinition};

pub(crate) struct ExpressionEvaluation {
    session: Arc<dyn Sender>,
    table_info: Vec<TableDefinition>
}

impl ExpressionEvaluation {
    pub(crate) fn new(session: Arc<dyn Sender>, table_info: Vec<TableDefinition>) -> ExpressionEvaluation {
        ExpressionEvaluation { session, table_info }
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
                                    EvalError::UnsupportedDatum(ty) => QueryErrorBuilder::new()
                                        .feature_not_supported(format!("Data type not supported: {}", ty))
                                        .build(),
                                    EvalError::OutOfRangeNumeric(ty) => {
                                        let mut builder = QueryErrorBuilder::new();
                                        builder.out_of_range(ty.to_pg_types(), String::new(), 0);
                                        builder.build()
                                    }
                                    EvalError::UnsupportedOperation => QueryErrorBuilder::new()
                                        .feature_not_supported("Use of unsupported expression feature".to_string())
                                        .build(),
                                };

                                self.session.send(Err(err)).expect("To Send Query Result to Client");;
                                Err(())
                            }
                        }
                    }
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
                                BinaryOperator::StringConcat => {
                                    let kind = QueryErrorBuilder::new()
                                        .undefined_function(op.to_string(), "NUMBER".to_owned(), "NUMBER".to_owned())
                                        .build();
                                    self.session.send(Err(kind)).expect("To Send Query Result to Client");;
                                    return Err(());
                                }
                                _ => panic!(),
                            }
                        } else if left.is_float() && right.is_float() {
                            match op {
                                BinaryOperator::Plus => Ok(ScalarOp::Literal(left + right)),
                                BinaryOperator::Minus => Ok(ScalarOp::Literal(left - right)),
                                BinaryOperator::Multiply => Ok(ScalarOp::Literal(left * right)),
                                BinaryOperator::Divide => Ok(ScalarOp::Literal(left / right)),
                                BinaryOperator::StringConcat => {
                                    let kind = QueryErrorBuilder::new()
                                        .undefined_function(op.to_string(), "NUMBER".to_owned(), "NUMBER".to_owned())
                                        .build();
                                    self.session.send(Err(kind)).expect("To Send Query Result to Client");;
                                    return Err(());
                                }
                                _ => panic!(),
                            }
                        } else if left.is_string() || right.is_string() {
                            match op {
                                BinaryOperator::StringConcat => {
                                    let value = format!("{}{}", left.to_string(), right.to_string());
                                    Ok(ScalarOp::Literal(Datum::OwnedString(value)))
                                }
                                _ => {
                                    let kind = QueryErrorBuilder::new()
                                        .undefined_function(op.to_string(), "STRING".to_owned(), "STRING".to_owned())
                                        .build();
                                    self.session.send(Err(kind)).expect("To Send Query Result to Client");;
                                    return Err(());
                                }
                            }
                        } else {
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
                        EvalError::UnsupportedDatum(ty) => QueryErrorBuilder::new()
                            .feature_not_supported(format!("Data type not supported: {}", ty))
                            .build(),
                        EvalError::OutOfRangeNumeric(ty) => {
                            let mut builder = QueryErrorBuilder::new();
                            builder.out_of_range(ty.to_pg_types(), String::new(), 0);
                            builder.build()
                        }
                        EvalError::UnsupportedOperation => QueryErrorBuilder::new()
                            .feature_not_supported("Use of unsupported expression feature".to_string())
                            .build(),
                    };

                    self.session.send(Err(err)).expect("To Send Query Result to Client");
                    Err(())
                }
            },
            Expr::Identifier(ident) => {
                if let Some((idx, _)) = self.find_column_by_name(ident.value.as_str())? {
                    Ok(ScalarOp::Column(idx))
                }
                else {
                    self.session
                        .send(Err(QueryErrorBuilder::new().undefined_column(ident.value.clone()).build()))
                        .expect("To Send Query Result to Client");
                    Err(())
                }
            }
            Expr::CompoundIdentifier(idents) => {
                self.session
                    .send(Err(QueryErrorBuilder::new().syntax_error(String::new()).build()))
                    .expect("To Send Query Result to Client");
                Err(())
            }
            _ => {
                self.session
                    .send(Err(QueryErrorBuilder::new().syntax_error(expr.to_string()).build()))
                    .expect("To Send Query Result to Client");
                Err(())
            }
        }
    }

    fn find_column_by_name(&self, name: &str) -> Result<Option<(usize, ColumnDefinition)>, ()> {
        let mut found = None;
        for table_info in self.table_info.to_vec() {
            if let Some((idx, column)) = table_info.column_by_name_with_index(name) {
                if found.is_some() {
                    let kind = QueryErrorBuilder::new().ambiguous_column(name.to_string()).build();
                    self.session.send(Err(kind)).expect("To Send Query Result to Client");
                    return Err(());
                }
                else {
                    found = Some((idx, column));
                }
            }
        }
        Ok(found)
    }
}

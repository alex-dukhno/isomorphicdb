use crate::query::scalar::{ScalarOp};
use protocol::results::{QueryError, QueryResult};
use protocol::Sender;
use representation::{Datum, EvalError, ScalarType};
use sqlparser::ast::{Assignment, BinaryOperator, DataType, Expr, UnaryOperator, Value};
use std::convert::TryFrom;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use sql_types::{SqlType, ConstraintError};
use kernel::SystemErrorKind::SqlEngineBug;
use data_manager::ColumnDefinition;

pub(crate) struct ExpressionEvaluation {
    session: Arc<dyn Sender>,
    table_info: Vec<ColumnDefinition>,
}

#[derive(Debug, Clone, Copy)]
pub struct ExprMetadata<'a> {
    column_definition: &'a ColumnDefinition,
    row_index: usize,
}

impl<'a> ExprMetadata<'a> {
    pub fn new(column_definition: &'a ColumnDefinition, row_index: usize) -> Self {
        Self {
            column_definition,
            row_index
        }
    }

    pub fn column(&self) -> &'a ColumnDefinition {
        self.column_definition
    }

    pub fn index(&self) -> usize {
        self.row_index
    }
}

impl ExpressionEvaluation {
    pub(crate) fn new(session: Arc<dyn Sender>, table_info: Vec<ColumnDefinition>) -> ExpressionEvaluation {
        ExpressionEvaluation { session, table_info }
    }

    pub(crate) fn eval<'a>(&self, expr: &Expr, expr_metadata: Option<ExprMetadata<'a>>) -> Result<ScalarOp, ()> {
        self.inner_eval(expr, expr_metadata)
    }

    fn inner_eval<'a>(&self, expr: &Expr, expr_metadata: Option<ExprMetadata<'a>>) -> Result<ScalarOp, ()> {
        match expr {
            Expr::Cast { expr, data_type } => match (&**expr, data_type) {
                (Expr::Value(Value::SingleQuotedString(v)), DataType::Boolean) => {
                    Ok(ScalarOp::Literal(Datum::from_bool(bool::from_str(v).unwrap())))
                }
                (Expr::Value(Value::Boolean(val)), DataType::Boolean) => Ok(ScalarOp::Literal(Datum::from_bool(*val))),
                _ => {
                    self.session
                        .send(Err(QueryError::syntax_error(format!(
                                "Cast from {:?} to {:?} is not currently supported",
                                expr, data_type
                            ))))
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
                                let err = if let Some(meta_data) = expr_metadata.as_ref() {
                                    match e {
                                        EvalError::UnsupportedDatum(ty) => QueryError::feature_not_supported(format!("Data type not supported: {}", ty)),
                                        EvalError::OutOfRangeNumeric(_) => QueryError::out_of_range(meta_data.column().sql_type().to_pg_types(), meta_data.column().name(), meta_data.index()),
                                        EvalError::UnsupportedOperation => QueryError::feature_not_supported("Use of unsupported expression feature".to_string()),
                                    }
                                }
                                else {
                                    match e {
                                        EvalError::UnsupportedDatum(ty) => QueryError::feature_not_supported(format!("Data type not supported: {}", ty)),
                                        EvalError::OutOfRangeNumeric(ty) => QueryError::out_of_range(ty.to_pg_types(), String::new(), 0),
                                        EvalError::UnsupportedOperation => QueryError::feature_not_supported("Use of unsupported expression feature".to_string()),
                                    }
                                };
                                self.session.send(Err(err)).expect("To Send Query Result to Client");
                                Err(())
                            }
                        }
                    }
                    (op, operand) => {
                        self.session
                            .send(Err(QueryError::syntax_error(op.to_string() + expr.to_string().as_str())))
                            .expect("To Send Query Result to Client");
                        // EvalScalarOp::eval_unary_literal_expr(op, *op, operand)?;
                        return Err(());
                    }
                }
            }
            Expr::BinaryOp { op, left, right } => {
                let lhs = self.inner_eval(left.deref(), expr_metadata.clone())?;
                let rhs = self.inner_eval(right.deref(), expr_metadata.clone())?;
                if let Some(ty) = self.compatible_types_for_op(op.clone(), lhs.scalar_type(), rhs.scalar_type()) {
                    match (lhs, rhs) {
                        (ScalarOp::Literal(left), ScalarOp::Literal(right)) => {
                            EvalScalarOp::eval_binary_literal_expr(self.session.as_ref(), op.clone(), left, right)
                                .map(ScalarOp::Literal)
                        }
                        (left, right) => {
                            Ok(ScalarOp::Binary(op.clone(), Box::new(left), Box::new(right), ty))
                        }
                    }
                }
                else {
                    let kind = QueryError::undefined_function(
                            op.to_string(),
                            lhs.scalar_type().to_string(),
                            rhs.scalar_type().to_string(),
                        );
                    self.session.send(Err(kind)).expect("To Senc Query Result to Client");
                    Err(())
                }
            }
            Expr::Value(value) => match Datum::try_from(value) {
                Ok(datum) => Ok(ScalarOp::Literal(datum)),
                Err(e) => {
                    let err = if let Some(meta_data) = expr_metadata.as_ref() {
                        match e {
                            EvalError::UnsupportedDatum(ty) => QueryError::feature_not_supported(format!("Data type not supported: {}", ty)),
                            EvalError::OutOfRangeNumeric(_) => QueryError::out_of_range(meta_data.column().sql_type().to_pg_types(), meta_data.column().name(), meta_data.index()),
                            EvalError::UnsupportedOperation => QueryError::feature_not_supported("Use of unsupported expression feature".to_string()),
                        }
                    }
                    else {
                        match e {
                            EvalError::UnsupportedDatum(ty) => QueryError::feature_not_supported(format!("Data type not supported: {}", ty)),
                            EvalError::OutOfRangeNumeric(ty) => QueryError::out_of_range(ty.to_pg_types(), String::new(), 0),
                            EvalError::UnsupportedOperation => QueryError::feature_not_supported("Use of unsupported expression feature".to_string()),
                        }
                    };

                    self.session.send(Err(err)).expect("To Send Query Result to Client");
                    Err(())
                }
            },
            Expr::Identifier(ident) => {
                if let Some((idx, column_def)) = self.find_column_by_name(ident.value.as_str())? {
                    let scalar_type = column_def.sql_type();
                    Ok(ScalarOp::Column(idx, Self::convert_sql_type(scalar_type)))
                } else {
                    self.session
                        .send(Err(QueryError::undefined_column(ident.value.clone())))
                        .expect("To Send Query Result to Client");
                    Err(())
                }
            }
            Expr::CompoundIdentifier(idents) => {
                self.session
                    .send(Err(QueryError::syntax_error(String::new())))
                    .expect("To Send Query Result to Client");
                Err(())
            }
            _ => {
                self.session
                    .send(Err(QueryError::syntax_error(expr.to_string())))
                    .expect("To Send Query Result to Client");
                Err(())
            }
        }
    }

    pub fn eval_assignment(&self, assignment: &Assignment) -> Result<ScalarOp, ()> {
        let Assignment { id, value } = assignment;
        let (destination, column_def) = if let Some((idx, def)) = self.find_column_by_name(id.value.as_str())? {
            (idx, def)
        } else {
            let kind = QueryError::column_does_not_exist(id.value.clone());
            self.session.send(Err(kind)).expect("To Send Query Result to Client");
            return Err(());
        };

        let value = self.eval(value, None)?;
        let ty = value.scalar_type();

        Ok(ScalarOp::Assignment {
            destination,
            value: Box::new(value),
            ty
        })
    }

    pub fn find_column_by_name(&self, name: &str) -> Result<Option<(usize, &ColumnDefinition)>, ()> {
        let columns = self.table_info.iter().enumerate().filter(|(_, col)| col.has_name(name)).collect::<Vec<(usize, &ColumnDefinition)>>();
        if columns.is_empty() {
            Ok(None)
        }
        else if columns.len() != 1 {
            let kind = QueryError::ambiguous_column(name.to_string());
            self.session.send(Err(kind)).expect("To Send Query Result to Client");
            Err(())
        }
        else {
            Ok(columns.first().map(|(idx, def)| (*idx, *def)))
        }
    }

    pub fn compatible_types_for_op(&self, op: BinaryOperator, lhs_type: ScalarType, rhs_type: ScalarType) -> Option<ScalarType> {
        if lhs_type == rhs_type {
            if lhs_type.is_integer() {
                match op {
                    BinaryOperator::Plus |
                    BinaryOperator::Minus |
                    BinaryOperator::Multiply |
                    BinaryOperator::Divide |
                    BinaryOperator::Modulus |
                    BinaryOperator::BitwiseAnd |
                    BinaryOperator::BitwiseOr => Some(lhs_type),
                    BinaryOperator::StringConcat => Some(ScalarType::String),
                    _ => None
                }
            }
            else if lhs_type.is_float() {
                match op {
                    BinaryOperator::Plus |
                    BinaryOperator::Minus |
                    BinaryOperator::Multiply |
                    BinaryOperator::Divide => Some(lhs_type),
                    _ => None,
                }
            }
            else if lhs_type.is_string() {
                match op {
                    BinaryOperator::StringConcat => Some(ScalarType::String),
                    _ => None,
                }
            }
            else {
                None
            }
        }
        else if (lhs_type.is_string() && rhs_type.is_integer()) ||
                (lhs_type.is_integer() && rhs_type.is_string()) {
            match op {
                BinaryOperator::StringConcat => Some(ScalarType::String),
                _ => None
            }
        }
        else {
            None
        }
    }

    fn convert_sql_type(sql_type: SqlType) -> ScalarType {
        match sql_type {
            SqlType::Bool => ScalarType::Boolean,
            SqlType::Char(_) |
            SqlType::VarChar(_) => ScalarType::String,
            SqlType::SmallInt(_) => ScalarType::Int16,
            SqlType::Integer(_) => ScalarType::Int32,
            SqlType::BigInt(_) => ScalarType::Int64,
            SqlType::Real => ScalarType::Float32,
            SqlType::DoublePrecision => ScalarType::Float64,
            SqlType::Time |
            SqlType::TimeWithTimeZone |
            SqlType::Timestamp |
            SqlType::TimestampWithTimeZone |
            SqlType::Date |
            SqlType::Interval |
            SqlType::Decimal => panic!(),
        }
    }

}

pub struct EvalScalarOp<'a> {
    session: &'a dyn Sender,
    columns: Vec<ColumnDefinition>,
}

impl<'a> EvalScalarOp<'a> {
    pub fn new(session: &'a dyn Sender, columns: Vec<ColumnDefinition>) -> Self {
        Self { session, columns }
    }

    pub fn eval<'b>(&self, row: &[Datum<'b>], eval: &ScalarOp) -> Result<Datum<'b>, ()> {
        match eval {
            ScalarOp::Column(idx, _) => Ok(row[*idx].clone()),
            ScalarOp::Literal(datum) => Ok(datum.clone()),
            ScalarOp::Binary(op, lhs, rhs, _) => {
                let left = self.eval(row, lhs.as_ref())?;
                let right=self. eval(row, rhs.as_ref())?;
                Self::eval_binary_literal_expr(self.session, op.clone(), left, right)
            }
            ScalarOp::Unary(op, operand, _) => {
                let operand = self.eval(row, operand.as_ref())?;
                Self::eval_unary_literal_expr(self.session, op.clone(), operand)
            }
            ScalarOp::Assignment { .. } => {
                panic!("EvalScalarOp:eval should not be evaluated on a ScalarOp::Assignment")
            }
        }
    }

    pub fn eval_on_row(&self, row: &mut [Datum], eval: &ScalarOp, row_idx: usize) -> Result<(), ()> {
        match eval {
            ScalarOp::Assignment { destination, value, ty: _} => {
                let value = self.eval(row, value.as_ref())?;
                let column = &self.columns[*destination];
                match column.sql_type().constraint().validate(value.to_string().as_str()){
                    Ok(()) => row[*destination] = value,
                    Err(ConstraintError::OutOfRange) => {
                        self.session.send(Err(QueryError::out_of_range(
                            (&column.sql_type()).into(),
                            column.name(),
                            row_idx + 1
                        ))).expect("To Send Query Result to client");
                        return Err(());
                    },
                    Err(ConstraintError::TypeMismatch(value)) => {
                        self.session.send(Err(QueryError::type_mismatch(
                            &value,
                            (&column.sql_type()).into(),
                            column.name(),
                            row_idx + 1
                        ))).expect("To Send Query Result to client");
                        return Err(());
                    },
                    Err(ConstraintError::ValueTooLong(len)) => {
                        self.session.send(Err(QueryError::string_length_mismatch(
                            (&column.sql_type()).into(),
                            len,
                            column.name(),
                            row_idx + 1
                        ))).expect("To Send Query Result to client");
                        return Err(());
                    },
                }
            }
            _ => {
                panic!("EvalScalarOp:eval_on_row should only be evaluated on a ScalarOp::Assignment");
            }
        }
        Ok(())
    }

    pub fn eval_binary_literal_expr<'b>(
        session: &dyn Sender,
        op: BinaryOperator,
        left: Datum<'b>,
        right: Datum<'b>,
    ) -> Result<Datum<'b>, ()> {
        if left.is_integer() && right.is_integer() {
            match op {
                BinaryOperator::Plus => Ok(left + right),
                BinaryOperator::Minus => Ok(left - right),
                BinaryOperator::Multiply => Ok(left * right),
                BinaryOperator::Divide => Ok(left / right),
                BinaryOperator::Modulus => Ok(left % right),
                BinaryOperator::BitwiseAnd => Ok(left & right),
                BinaryOperator::BitwiseOr => Ok(left | right),
                BinaryOperator::StringConcat => {
                    let kind = QueryError::undefined_function(op.to_string(), "NUMBER".to_owned(), "NUMBER".to_owned());
                    session.send(Err(kind)).expect("To Send Query Result to Client");
                    return Err(());
                }
                _ => panic!(),
            }
        } else if left.is_float() && right.is_float() {
            match op {
                BinaryOperator::Plus => Ok(left + right),
                BinaryOperator::Minus => Ok(left - right),
                BinaryOperator::Multiply => Ok(left * right),
                BinaryOperator::Divide => Ok(left / right),
                BinaryOperator::StringConcat => {
                    let kind = QueryError::undefined_function(op.to_string(), "NUMBER".to_owned(), "NUMBER".to_owned());
                    session.send(Err(kind)).expect("To Send Query Result to Client");
                    return Err(());
                }
                _ => panic!(),
            }
        } else if left.is_string() || right.is_string() {
            match op {
                BinaryOperator::StringConcat => {
                    let value = format!("{}{}", left.to_string(), right.to_string());
                    Ok(Datum::OwnedString(value))
                }
                _ => {
                    let kind = QueryError::undefined_function(op.to_string(), "STRING".to_owned(), "STRING".to_owned());
                    session.send(Err(kind)).expect("To Send Query Result to Client");
                    return Err(());
                }
            }
        } else {
            session
                .send(Err(QueryError::syntax_error(format!("{} {} {}", left.to_string(), op.to_string(), right.to_string()))))
                .expect("To Send Query Result to Client");
            Err(())
        }
    }

    pub fn eval_unary_literal_expr<'b>(
        session: &dyn Sender,
        op: UnaryOperator,
        operand: Datum,
    ) -> Result<Datum<'b>, ()> {
        unimplemented!()
    }
}

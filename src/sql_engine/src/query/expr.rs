use std::convert::TryFrom;
use sqlparser::ast::{self, *};
use super::Datum;


pub enum EvalError {
    InvalidExpressionInStaticContext,
    UnsupportedDatum(String),
    InvalidIntegerValue,
    InvalidFloatValue,
}

impl<'a> TryFrom<&ast::Value> for Datum<'a> {
    type Error = EvalError;

    fn try_from(other: &ast::Value) -> Result<Self, EvalError> {
        use Value::*;
        match other {
            Number(val) => {
                use crate::bigdecimal::ToPrimitive;
                if val.is_integer() {
                    if let Some(val) = val.to_i32() {
                        Ok(Datum::from_i32(val))
                    }
                    else if let Some(val) = val.to_i64() {
                        Ok(Datum::from_i64(val))
                    }
                    else {
                        Err(EvalError::InvalidIntegerValue)
                    }
                }
                else {
                    if let Some(val) = val.to_f32() {
                        Ok(Datum::from_f32(val))
                    }
                    else if let Some(val) = val.to_f64() {
                        Ok(Datum::from_f64(val))
                    }
                    else {
                        Err(EvalError::InvalidFloatValue)
                    }
                }
            }
            SingleQuotedString(value) => Ok(Datum::from_string(value.clone())),
            NationalStringLiteral(value) => Err(EvalError::UnsupportedDatum("NationalStringLiteral".to_string())),
            HexStringLiteral(value) => {
                match i64::from_str_radix(value.as_str(), 16) {
                    Ok(val) => Ok(Datum::from_i64(val)),
                    Err(_) => panic!("Failed to parse hex string")
                }
            },
            Boolean(val) => Ok(Datum::from_bool(*val)),
            Interval {
               ..
            } => Err(EvalError::UnsupportedDatum("Interval".to_string())),
            Null => Ok(Datum::from_null()),
        }
    }
}

impl<'a> Datum<'a> {
    pub fn add(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn minus(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn multiply(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn divide(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn modulus(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn concat(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn greater(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn less(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn greater_equal(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn less_equal(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn equal(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn not_equal(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn logical_and(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn logical_or(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn like(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn not_like(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn bitwise_or(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn bitwise_and(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
    pub fn bitwise_xor(&self, rhs: &Self) -> Result<Self, EvalError> {

    }
}

// this must be improved later when we know what we are doing...
pub fn resolve_static_expr<'a>(expr: &'a Expr) -> Result<Datum<'a>, EvalError> {
    use Expr::*;
    match expr {
        BinaryOp {
            left,
            op,
            right,
        } => {
            let resolved_left = resolve_static_expr(left)?;
            let resolved_right = resolve_static_expr(right)?;
            resolve_binary_expr(*op, resolved_left, resolved_right)
        }
        UnaryOp {
            op,
            expr,
        } => {
            let operand = resolve_static_expr(&expr)?;
            resolve_unary_expr(*op, operand)
        }
        Nested(expr) => resolve_static_expr(&expr),
        Value(value) => Datum::try_from(value),
        // TypedString {
        //     data_type,
        //     value,
        // } => ,
        Function(_) => expr.clone(),
        _ => Err(EvalError::InvalidExpressionInStaticContext)
    }
}

// precondition: lhs and rhs must reduced to Expr::Value otherwise, the original expression will be returned.
pub fn resolve_binary_expr<'a>(op: BinaryOperator, lhs: Datum<'a>, rhs: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    use BinaryOperator::*;
    match op {
        Plus => lhs.add(&rhs)?,
        Minus => lhs.minus(&rhs)?,
        Multiply => lhs.multiply(&rhs)?,
        Divide => lhs.divide(&rhs)?,
        Modulus => lhs.modulus(&rhs)?,
        StringConcat => lhs.concat(&rhs)?,
        Gt => lhs.greater(&rhs)?,
        Lt => lhs.less(&rhs)?,
        GtEq => lhs.greater_equal(&rhs)?,
        LtEq => lhs.less_equal(&rhs)?,
        Eq => lhs.equal(&rhs)?,
        NotEq => lhs.not_equal(&rhs)?,
        And => lhs.logical_and(&rhs)?,
        Or => lhs.logical_or(&rhs)?,
        Like => lhs.like(&rhs)?,
        NotLike => lhs.not_like(&rhs)?,
        BitwiseOr => lhs.bitwise_or(&rhs)?,
        BitwiseAnd => lhs.bitwise_and(&rhs)?,
        BitwiseXor => rhs.bitwise_xor(&rhs)?,
    }
}

pub fn resolve_unary_expr<'a>(op: UnaryOperator, operand: Datum<'a>) -> Result<Datum<'a>, EvalError> {

}

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

use bigdecimal::BigDecimal;
use data_manipulation_operators::{BiOperator, UnOperator};
use data_manipulation_untyped_tree_old::{UntypedItemOld, UntypedTreeOld, UntypedValueOld};
use definition::ColumnDef;
use query_ast::{BinaryOperator, Expr, Value};
use std::str::FromStr;
use types_old::SqlTypeOld;

const MAX_BIG_INT: &str = "9223372036854775807";
const MIN_BIG_INT: &str = "-9223372036854775808";

pub struct TreeBuilder;

impl TreeBuilder {
    pub fn build_dynamic(root_expr: Expr, table_columns: &[ColumnDef]) -> Result<UntypedTreeOld, UntypedExpressionError> {
        Self::inner_dynamic(root_expr, table_columns)
    }

    fn inner_dynamic(root_expr: Expr, table_columns: &[ColumnDef]) -> Result<UntypedTreeOld, UntypedExpressionError> {
        match root_expr {
            Expr::Value(value) => Ok(Self::value(value)),
            Expr::Column(ident) => Self::ident(ident, table_columns),
            Expr::BinaryOp { left, op, right } => Self::dynamic_binary_op(op, *left, *right, table_columns),
            Expr::UnaryOp { op, expr } => Ok(UntypedTreeOld::UnOp {
                op: UnOperator::from(op),
                item: Box::new(Self::inner_dynamic(*expr, table_columns)?),
            }),
            Expr::Cast { expr, data_type } => Ok(UntypedTreeOld::UnOp {
                op: UnOperator::Cast(SqlTypeOld::from(data_type)),
                item: Box::new(Self::inner_dynamic(*expr, table_columns)?),
            }),
            Expr::Param(index) => Ok(UntypedTreeOld::Item(UntypedItemOld::Param((index - 1) as usize))),
        }
    }

    fn dynamic_binary_op(op: BinaryOperator, left: Expr, right: Expr, table_columns: &[ColumnDef]) -> Result<UntypedTreeOld, UntypedExpressionError> {
        let left = Self::inner_dynamic(left, table_columns)?;
        let right = Self::inner_dynamic(right, table_columns)?;
        Ok(UntypedTreeOld::BiOp {
            left: Box::new(left),
            op: BiOperator::from(op),
            right: Box::new(right),
        })
    }

    fn ident(value: String, table_columns: &[ColumnDef]) -> Result<UntypedTreeOld, UntypedExpressionError> {
        for (index, table_column) in table_columns.iter().enumerate() {
            let column_name = value.to_lowercase();
            if table_column.has_name(column_name.as_str()) {
                return Ok(UntypedTreeOld::Item(UntypedItemOld::Column {
                    name: table_column.name().to_owned(),
                    sql_type: table_column.sql_type(),
                    index,
                }));
            }
        }
        Err(UntypedExpressionError::column_not_found(value))
    }

    pub fn insert_position(expr: Expr) -> Result<UntypedTreeOld, UntypedExpressionError> {
        Self::inner_insert_position(expr)
    }

    fn inner_insert_position(expr: Expr) -> Result<UntypedTreeOld, UntypedExpressionError> {
        match expr {
            Expr::Value(value) => Ok(Self::value(value)),
            Expr::Column(name) => Err(UntypedExpressionError::column_cant_be_referenced(name)),
            Expr::BinaryOp { left, op, right } => Self::static_binary_op(op, *left, *right),
            Expr::Cast { expr, data_type } => Ok(UntypedTreeOld::UnOp {
                op: UnOperator::Cast(SqlTypeOld::from(data_type)),
                item: Box::new(Self::inner_insert_position(*expr)?),
            }),
            Expr::UnaryOp { op, expr } => Ok(UntypedTreeOld::UnOp {
                op: UnOperator::from(op),
                item: Box::new(Self::inner_insert_position(*expr)?),
            }),
            Expr::Param(index) => Ok(UntypedTreeOld::Item(UntypedItemOld::Param((index - 1) as usize))),
        }
    }

    fn static_binary_op(operator: BinaryOperator, left: Expr, right: Expr) -> Result<UntypedTreeOld, UntypedExpressionError> {
        let left = Self::inner_insert_position(left)?;
        let right = Self::inner_insert_position(right)?;
        Ok(UntypedTreeOld::BiOp {
            left: Box::new(left),
            op: BiOperator::from(operator),
            right: Box::new(right),
        })
    }

    fn value(value: Value) -> UntypedTreeOld {
        match value {
            Value::Int(num) => UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Int(num))),
            Value::String(string) => UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Literal(string))),
            Value::Null => UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Null)),
            Value::Number(num) if num.contains('.') => {
                UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Number(BigDecimal::from_str(&num).unwrap())))
            }
            Value::Number(num) => {
                if (num.starts_with('-') && num.len() < MIN_BIG_INT.len() || num.len() == MIN_BIG_INT.len() && num.as_str() <= MIN_BIG_INT)
                    || (num.len() < MAX_BIG_INT.len() || num.len() == MAX_BIG_INT.len() && num.as_str() <= MAX_BIG_INT)
                {
                    UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::BigInt(num.parse().unwrap())))
                } else {
                    UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Number(BigDecimal::from_str(&num).unwrap())))
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum UntypedExpressionError {
    ColumnNotFound(String),
    ColumnCantBeReferenced(String),
}

impl UntypedExpressionError {
    pub fn column_not_found<C: ToString>(column_name: C) -> UntypedExpressionError {
        UntypedExpressionError::ColumnNotFound(column_name.to_string())
    }

    pub fn column_cant_be_referenced<C: ToString>(column_name: C) -> UntypedExpressionError {
        UntypedExpressionError::ColumnCantBeReferenced(column_name.to_string())
    }
}

#[cfg(test)]
mod tests;

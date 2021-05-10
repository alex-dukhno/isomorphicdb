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

use crate::AnalysisError;
use bigdecimal::BigDecimal;
use data_manipulation_operators::{BiOperator, UnOperator};
use data_manipulation_untyped_tree::{UntypedItem, UntypedTree, UntypedValue};
use definition::ColumnDef;
use query_ast::{BinaryOperator, Expr, Value};
use std::str::FromStr;
use types::{Bool, SqlType};

pub(crate) struct TreeBuilder;

impl TreeBuilder {
    pub(crate) fn static_tree(root_expr: Expr) -> Result<UntypedTree, AnalysisError> {
        Self::inner_static_tree(root_expr)
    }

    fn inner_static_tree(root_expr: Expr) -> Result<UntypedTree, AnalysisError> {
        match root_expr {
            Expr::Value(value) => Ok(Self::value(value)),
            Expr::Column(name) => Err(AnalysisError::column_cant_be_referenced(name)),
            Expr::BinaryOp { left, op, right } => Self::binary_op_static(op, *left, *right),
            Expr::Cast { expr, data_type } => Ok(UntypedTree::UnOp {
                op: UnOperator::Cast(SqlType::from(data_type).family()),
                item: Box::new(Self::inner_static_tree(*expr)?),
            }),
            Expr::UnaryOp { op, expr } => Ok(UntypedTree::UnOp {
                op: UnOperator::from(op),
                item: Box::new(Self::inner_static_tree(*expr)?),
            }),
            Expr::Param(index) => Ok(UntypedTree::Item(UntypedItem::Param((index - 1) as usize))),
        }
    }

    fn binary_op_static(operator: BinaryOperator, left: Expr, right: Expr) -> Result<UntypedTree, AnalysisError> {
        let left = Self::inner_static_tree(left)?;
        let right = Self::inner_static_tree(right)?;
        Ok(UntypedTree::BiOp {
            left: Box::new(left),
            op: BiOperator::from(operator),
            right: Box::new(right),
        })
    }

    fn value(value: Value) -> UntypedTree {
        match value {
            Value::Int(num) => UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(BigDecimal::from(num)))),
            Value::String(string) => UntypedTree::Item(UntypedItem::Const(UntypedValue::String(string))),
            Value::Boolean(boolean) => UntypedTree::Item(UntypedItem::Const(UntypedValue::Bool(Bool(boolean)))),
            Value::Null => UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)),
            Value::Number(num) => UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(
                BigDecimal::from_str(&num).unwrap(),
            ))),
        }
    }

    pub(crate) fn dynamic_tree(root_expr: Expr, table_columns: &[ColumnDef]) -> Result<UntypedTree, AnalysisError> {
        Self::inner_dynamic_tree(root_expr, table_columns)
    }

    fn inner_dynamic_tree(root_expr: Expr, table_columns: &[ColumnDef]) -> Result<UntypedTree, AnalysisError> {
        match root_expr {
            Expr::Value(value) => Ok(Self::value(value)),
            Expr::Column(ident) => Self::ident(ident, table_columns),
            Expr::BinaryOp { left, op, right } => Self::binary_op(op, *left, *right, table_columns),
            Expr::UnaryOp { op, expr } => Ok(UntypedTree::UnOp {
                op: UnOperator::from(op),
                item: Box::new(Self::inner_dynamic_tree(*expr, table_columns)?),
            }),
            Expr::Cast { expr, data_type } => Ok(UntypedTree::UnOp {
                op: UnOperator::Cast(SqlType::from(data_type).family()),
                item: Box::new(Self::inner_dynamic_tree(*expr, table_columns)?),
            }),
            Expr::Param(index) => Ok(UntypedTree::Item(UntypedItem::Param((index - 1) as usize))),
        }
    }

    fn binary_op(
        op: BinaryOperator,
        left: Expr,
        right: Expr,
        table_columns: &[ColumnDef],
    ) -> Result<UntypedTree, AnalysisError> {
        let left = Self::inner_dynamic_tree(left, table_columns)?;
        let right = Self::inner_dynamic_tree(right, table_columns)?;
        Ok(UntypedTree::BiOp {
            left: Box::new(left),
            op: BiOperator::from(op),
            right: Box::new(right),
        })
    }

    fn ident(value: String, table_columns: &[ColumnDef]) -> Result<UntypedTree, AnalysisError> {
        for (index, table_column) in table_columns.iter().enumerate() {
            let column_name = value.to_lowercase();
            if table_column.has_name(column_name.as_str()) {
                return Ok(UntypedTree::Item(UntypedItem::Column {
                    name: table_column.name().to_owned(),
                    sql_type: table_column.sql_type(),
                    index,
                }));
            }
        }
        Err(AnalysisError::column_not_found(value))
    }
}

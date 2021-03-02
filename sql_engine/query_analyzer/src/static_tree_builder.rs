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

use data_manipulation_untyped_tree::{Bool, StaticUntypedItem, StaticUntypedTree, UntypedValue};

use crate::{operation_mapper::OperationMapper, AnalysisError};
use bigdecimal::BigDecimal;
use data_manipulation_operators::UnOperator;
use query_ast::{BinaryOperator, Expr, Value};
use std::str::FromStr;
use types::SqlType;

pub(crate) struct StaticTreeBuilder;

impl StaticTreeBuilder {
    pub(crate) fn build_from(root_expr: Expr) -> Result<StaticUntypedTree, AnalysisError> {
        Self::inner_build(root_expr)
    }

    fn inner_build(root_expr: Expr) -> Result<StaticUntypedTree, AnalysisError> {
        match root_expr {
            Expr::Value(value) => Ok(Self::value(value)),
            Expr::Column(name) => Err(AnalysisError::column_cant_be_referenced(name)),
            Expr::BinaryOp { left, op, right } => Self::binary_op(op, *left, *right),
            Expr::Cast { expr, data_type } => Ok(StaticUntypedTree::UnOp {
                op: UnOperator::Cast(SqlType::from(data_type).family()),
                item: Box::new(Self::inner_build(*expr)?),
            }),
            // Expr::UnaryOp { op, expr } => {
            //     let op = OperationMapper::unary_operation(op);
            //     let item = Self::inner_build(expr, original)?;
            //     Ok(StaticUntypedTree::UnOp {
            //         op,
            //         item: Box::new(item),
            //     })
            // }
        }
    }

    fn binary_op(operator: BinaryOperator, left: Expr, right: Expr) -> Result<StaticUntypedTree, AnalysisError> {
        let op = OperationMapper::binary_operation(operator);
        let left = Self::inner_build(left)?;
        let right = Self::inner_build(right)?;
        Ok(StaticUntypedTree::BiOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn value(value: Value) -> StaticUntypedTree {
        match value {
            Value::Int(num) => {
                StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Number(BigDecimal::from(num))))
            }
            Value::String(string) => StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::String(string))),
            Value::Boolean(boolean) => {
                StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Bool(Bool(boolean))))
            }
            Value::Null => StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Null)),
            Value::Param(index) => StaticUntypedTree::Item(StaticUntypedItem::Param(index as usize)),
            Value::Number(num) => StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Number(
                BigDecimal::from_str(&num).unwrap(),
            ))),
        }
    }
}

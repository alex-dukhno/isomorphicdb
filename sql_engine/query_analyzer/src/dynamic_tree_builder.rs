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

use crate::{operation_mapper::OperationMapper, AnalysisError};
use bigdecimal::BigDecimal;
use data_manipulation_untyped_tree::{Bool, DynamicUntypedItem, DynamicUntypedTree, UntypedValue};
use definition::ColumnDef;
use query_ast::{BinaryOperator, Expr, Value};
use std::str::FromStr;

pub(crate) struct DynamicTreeBuilder;

impl DynamicTreeBuilder {
    pub(crate) fn build_from(
        root_expr: Expr,
        table_columns: &[ColumnDef],
    ) -> Result<DynamicUntypedTree, AnalysisError> {
        Self::inner_build(root_expr, table_columns)
    }

    fn inner_build(root_expr: Expr, table_columns: &[ColumnDef]) -> Result<DynamicUntypedTree, AnalysisError> {
        match root_expr {
            Expr::Value(value) => Ok(Self::value(value)),
            Expr::Column(ident) => Self::ident(ident, table_columns),
            Expr::BinaryOp { left, op, right } => Self::binary_op(op, *left, *right, table_columns),
            Expr::Cast { .. } => unimplemented!(),
        }
    }

    fn binary_op(
        op: BinaryOperator,
        left: Expr,
        right: Expr,
        table_columns: &[ColumnDef],
    ) -> Result<DynamicUntypedTree, AnalysisError> {
        let operation = OperationMapper::binary_operation(op);
        let left = Self::inner_build(left, table_columns)?;
        let right = Self::inner_build(right, table_columns)?;
        Ok(DynamicUntypedTree::BiOp {
            left: Box::new(left),
            op: operation,
            right: Box::new(right),
        })
    }

    fn ident(value: String, table_columns: &[ColumnDef]) -> Result<DynamicUntypedTree, AnalysisError> {
        for (index, table_column) in table_columns.iter().enumerate() {
            let column_name = value.to_lowercase();
            if table_column.has_name(column_name.as_str()) {
                return Ok(DynamicUntypedTree::Item(DynamicUntypedItem::Column {
                    name: table_column.name().to_owned(),
                    sql_type: table_column.sql_type(),
                    index,
                }));
            }
        }
        Err(AnalysisError::column_not_found(value))
    }

    fn value(value: Value) -> DynamicUntypedTree {
        match value {
            Value::Int(num) => {
                DynamicUntypedTree::Item(DynamicUntypedItem::Const(UntypedValue::Number(BigDecimal::from(num))))
            }
            Value::String(string) => DynamicUntypedTree::Item(DynamicUntypedItem::Const(UntypedValue::String(string))),
            Value::Boolean(boolean) => {
                DynamicUntypedTree::Item(DynamicUntypedItem::Const(UntypedValue::Bool(Bool(boolean))))
            }
            Value::Null => DynamicUntypedTree::Item(DynamicUntypedItem::Const(UntypedValue::Null)),
            Value::Param(index) => DynamicUntypedTree::Item(DynamicUntypedItem::Param((index - 1) as usize)),
            Value::Number(num) => DynamicUntypedTree::Item(DynamicUntypedItem::Const(UntypedValue::Number(
                BigDecimal::from_str(&num).unwrap(),
            ))),
        }
    }
}

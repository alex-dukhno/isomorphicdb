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

use crate::{operation_mapper::OperationMapper, parse_param_index};
use analysis_tree::{AnalysisError, AnalysisResult, Feature, ProjectionTreeNode};
use expr_operators::{Bool, Operand, ScalarValue};
use meta_def::ColumnDefinition;
use types::SqlType;

pub(crate) struct ProjectionTreeBuilder;

impl ProjectionTreeBuilder {
    pub(crate) fn build_from(
        root_expr: &sql_ast::Expr,
        original: &sql_ast::Statement,
        column_type: &SqlType,
        table_columns: &[ColumnDefinition],
    ) -> AnalysisResult<ProjectionTreeNode> {
        Self::inner_build(root_expr, original, column_type, 0, table_columns)
    }

    fn inner_build(
        root_expr: &sql_ast::Expr,
        original: &sql_ast::Statement,
        column_type: &SqlType,
        level: usize,
        table_columns: &[ColumnDefinition],
    ) -> AnalysisResult<ProjectionTreeNode> {
        match root_expr {
            sql_ast::Expr::Value(value) => Self::value(value),
            sql_ast::Expr::Identifier(ident) => Self::ident(ident, table_columns),
            sql_ast::Expr::BinaryOp { left, op, right } => {
                Self::op(op, &**left, &**right, original, column_type, level, table_columns)
            }
            expr => Err(AnalysisError::syntax_error(format!(
                "Syntax error in {}\naround {}",
                original, expr
            ))),
        }
    }

    fn op(
        op: &sql_ast::BinaryOperator,
        left: &sql_ast::Expr,
        right: &sql_ast::Expr,
        original: &sql_ast::Statement,
        column_type: &SqlType,
        level: usize,
        table_columns: &[ColumnDefinition],
    ) -> AnalysisResult<ProjectionTreeNode> {
        let operation = OperationMapper::binary_operation(op);
        let left_item = Self::inner_build(left, original, column_type, level + 1, table_columns)?;
        let right_item = Self::inner_build(right, original, column_type, level + 1, table_columns)?;
        Ok(ProjectionTreeNode::Operation {
            left: Box::new(left_item),
            op: operation,
            right: Box::new(right_item),
        })
    }

    fn ident(ident: &sql_ast::Ident, table_columns: &[ColumnDefinition]) -> AnalysisResult<ProjectionTreeNode> {
        let sql_ast::Ident { value, .. } = ident;
        match parse_param_index(value.as_str()) {
            Some(index) => Ok(ProjectionTreeNode::Item(Operand::Param(index))),
            None => {
                for (index, table_column) in table_columns.iter().enumerate() {
                    if table_column.has_name(value.as_str()) {
                        return Ok(ProjectionTreeNode::Item(Operand::Column {
                            sql_type: table_column.sql_type(),
                            index,
                        }));
                    }
                }
                Err(AnalysisError::column_not_found(value))
            }
        }
    }

    fn value(value: &sql_ast::Value) -> AnalysisResult<ProjectionTreeNode> {
        match value {
            sql_ast::Value::Number(num) => Ok(ProjectionTreeNode::Item(Operand::Const(ScalarValue::Number(
                num.clone(),
            )))),
            sql_ast::Value::SingleQuotedString(string) => Ok(ProjectionTreeNode::Item(Operand::Const(
                ScalarValue::String(string.clone()),
            ))),
            sql_ast::Value::NationalStringLiteral(_) => {
                Err(AnalysisError::feature_not_supported(Feature::NationalStringLiteral))
            }
            sql_ast::Value::HexStringLiteral(_) => Err(AnalysisError::feature_not_supported(Feature::HexStringLiteral)),
            sql_ast::Value::Boolean(boolean) => Ok(ProjectionTreeNode::Item(Operand::Const(ScalarValue::Bool(Bool(
                *boolean,
            ))))),
            sql_ast::Value::Interval { .. } => Err(AnalysisError::feature_not_supported(Feature::TimeInterval)),
            sql_ast::Value::Null => Ok(ProjectionTreeNode::Item(Operand::Const(ScalarValue::Null))),
        }
    }
}

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

use data_manipulation_untyped_tree::{Bool, DynamicUntypedItem, DynamicUntypedTree, UntypedValue};
use definition::ColumnDef;

use crate::{operation_mapper::OperationMapper, parse_param_index, AnalysisError, AnalysisResult, Feature};

pub(crate) struct DynamicTreeBuilder;

impl DynamicTreeBuilder {
    pub(crate) fn build_from(
        root_expr: &sql_ast::Expr,
        original: &sql_ast::Statement,
        table_columns: &[ColumnDef],
    ) -> AnalysisResult<DynamicUntypedTree> {
        Self::inner_build(root_expr, original, table_columns)
    }

    fn inner_build(
        root_expr: &sql_ast::Expr,
        original: &sql_ast::Statement,
        table_columns: &[ColumnDef],
    ) -> AnalysisResult<DynamicUntypedTree> {
        match root_expr {
            sql_ast::Expr::Value(value) => Self::value(value),
            sql_ast::Expr::Identifier(ident) => Self::ident(ident, table_columns),
            sql_ast::Expr::BinaryOp { left, op, right } => Self::op(op, &**left, &**right, original, table_columns),
            expr => {
                log::warn!("Syntax error in '{:#?}' around '{:?}'", original, expr);
                Err(AnalysisError::syntax_error(format!(
                    "Syntax error in '{}' around '{}'",
                    original, expr
                )))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn op(
        op: &sql_ast::BinaryOperator,
        left: &sql_ast::Expr,
        right: &sql_ast::Expr,
        original: &sql_ast::Statement,
        table_columns: &[ColumnDef],
    ) -> AnalysisResult<DynamicUntypedTree> {
        let operation = OperationMapper::binary_operation(op);
        match (
            Self::inner_build(left, original, table_columns),
            Self::inner_build(right, original, table_columns),
        ) {
            (Ok(left_item), Ok(right_item)) => Ok(DynamicUntypedTree::BiOp {
                left: Box::new(left_item),
                op: operation,
                right: Box::new(right_item),
            }),
            _ => Err(AnalysisError::UndefinedFunction(operation)),
        }
    }

    fn ident(ident: &sql_ast::Ident, table_columns: &[ColumnDef]) -> AnalysisResult<DynamicUntypedTree> {
        let sql_ast::Ident { value, .. } = ident;
        match parse_param_index(value.as_str()) {
            Some(index) => Ok(DynamicUntypedTree::Item(DynamicUntypedItem::Param(index))),
            None => {
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
        }
    }

    fn value(value: &sql_ast::Value) -> AnalysisResult<DynamicUntypedTree> {
        match value {
            sql_ast::Value::Number(num) => Ok(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                UntypedValue::Number(num.clone()),
            ))),
            sql_ast::Value::SingleQuotedString(string) => Ok(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                UntypedValue::String(string.clone()),
            ))),
            sql_ast::Value::NationalStringLiteral(_) => {
                Err(AnalysisError::feature_not_supported(Feature::NationalStringLiteral))
            }
            sql_ast::Value::HexStringLiteral(_) => Err(AnalysisError::feature_not_supported(Feature::HexStringLiteral)),
            sql_ast::Value::Boolean(boolean) => Ok(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                UntypedValue::Bool(Bool(*boolean)),
            ))),
            sql_ast::Value::Interval { .. } => Err(AnalysisError::feature_not_supported(Feature::TimeInterval)),
            sql_ast::Value::Null => Ok(DynamicUntypedTree::Item(DynamicUntypedItem::Const(UntypedValue::Null))),
        }
    }
}

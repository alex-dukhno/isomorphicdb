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

use data_manipulation_untyped_tree::{ScalarValue, StaticItem, Bool};
use data_manipulation_untyped_tree::StaticEvaluationTree;
use types::SqlType;

use crate::{AnalysisError, AnalysisResult, Feature, operation_mapper::OperationMapper, parse_param_index};

pub(crate) struct StaticTreeBuilder;

impl StaticTreeBuilder {
    pub(crate) fn build_from(
        root_expr: &sql_ast::Expr,
        original: &sql_ast::Statement,
        column_type: &SqlType,
    ) -> AnalysisResult<StaticEvaluationTree> {
        Self::inner_build(root_expr, original, column_type)
    }

    fn inner_build(
        root_expr: &sql_ast::Expr,
        original: &sql_ast::Statement,
        column_type: &SqlType,
    ) -> AnalysisResult<StaticEvaluationTree> {
        match root_expr {
            sql_ast::Expr::Value(value) => Self::value(value),
            sql_ast::Expr::Identifier(ident) => Self::ident(ident),
            sql_ast::Expr::BinaryOp { left, op, right } => Self::op(op, &**left, &**right, original, column_type),
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
    ) -> AnalysisResult<StaticEvaluationTree> {
        let operation = OperationMapper::binary_operation(op);
        match (
            Self::inner_build(left, original, column_type),
            Self::inner_build(right, original, column_type),
        ) {
            (Ok(left_item), Ok(right_item)) => Ok(StaticEvaluationTree::Operation {
                left: Box::new(left_item),
                op: operation,
                right: Box::new(right_item),
            }),
            _ => Err(AnalysisError::UndefinedFunction(operation)),
        }
    }

    fn ident(ident: &sql_ast::Ident) -> AnalysisResult<StaticEvaluationTree> {
        let sql_ast::Ident { value, .. } = ident;
        match parse_param_index(value.as_str()) {
            Some(index) => Ok(StaticEvaluationTree::Item(StaticItem::Param(index))),
            None => Err(AnalysisError::column_cant_be_referenced(value)),
        }
    }

    fn value(value: &sql_ast::Value) -> AnalysisResult<StaticEvaluationTree> {
        match value {
            sql_ast::Value::Number(num) => Ok(StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Number(
                num.clone(),
            )))),
            sql_ast::Value::SingleQuotedString(string) => Ok(StaticEvaluationTree::Item(StaticItem::Const(
                ScalarValue::String(string.clone()),
            ))),
            sql_ast::Value::NationalStringLiteral(_) => {
                Err(AnalysisError::feature_not_supported(Feature::NationalStringLiteral))
            }
            sql_ast::Value::HexStringLiteral(_) => Err(AnalysisError::feature_not_supported(Feature::HexStringLiteral)),
            sql_ast::Value::Boolean(boolean) => Ok(StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Bool(
                Bool(*boolean),
            )))),
            sql_ast::Value::Interval { .. } => Err(AnalysisError::feature_not_supported(Feature::TimeInterval)),
            sql_ast::Value::Null => Ok(StaticEvaluationTree::Item(StaticItem::Const(ScalarValue::Null))),
        }
    }
}

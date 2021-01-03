// Copyright 2020 Alex Dukhno
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
use analysis::{AnalysisError, AnalysisResult, Feature, InsertTreeNode};
use bigdecimal::{BigDecimal, Zero};
use expr_operators::{Bool, Operator, ScalarValue};
use sqlparser::ast;
use std::str::FromStr;
use types::{GeneralType, SqlType};

pub(crate) struct InsertTreeBuilder;

impl InsertTreeBuilder {
    pub(crate) fn build_from(
        root_expr: &ast::Expr,
        original: &ast::Statement,
        target_type: &GeneralType,
        column_type: &SqlType,
    ) -> AnalysisResult<InsertTreeNode> {
        Self::inner_build(root_expr, original, target_type, column_type, 0)
    }

    fn inner_build(
        root_expr: &ast::Expr,
        original: &ast::Statement,
        target_type: &GeneralType,
        column_type: &SqlType,
        level: usize,
    ) -> AnalysisResult<InsertTreeNode> {
        match root_expr {
            ast::Expr::Value(value) => Self::value(value, target_type, column_type, level),
            ast::Expr::Identifier(ident) => Self::ident(ident),
            ast::Expr::BinaryOp { left, op, right } => {
                Self::op(op, &**left, &**right, original, target_type, column_type, level)
            }
            expr => Err(AnalysisError::syntax_error(format!(
                "Syntax error in {}\naround {}",
                original, expr
            ))),
        }
    }

    fn op(
        op: &ast::BinaryOperator,
        left: &ast::Expr,
        right: &ast::Expr,
        original: &ast::Statement,
        target_type: &GeneralType,
        column_type: &SqlType,
        level: usize,
    ) -> AnalysisResult<InsertTreeNode> {
        let operation = OperationMapper::binary_operation(op);
        let operation_result_type = operation.result_type();
        if &operation_result_type != target_type {
            unimplemented!()
        } else {
            let acceptable_types = operation.acceptable_operand_types();
            let mut results = vec![];
            for (left_type, right_type) in acceptable_types {
                results.push((
                    Self::inner_build(left, original, &left_type, column_type, level + 1),
                    Self::inner_build(right, original, &right_type, column_type, level + 1),
                ));
            }
            match results.into_iter().find(|(left, right)| left.is_ok() && right.is_ok()) {
                Some((Ok(left_item), Ok(right_item))) => Ok(InsertTreeNode::Operation {
                    left: Box::new(left_item),
                    op: operation,
                    right: Box::new(right_item),
                }),
                _ => Err(AnalysisError::UndefinedFunction(operation)),
            }
        }
    }

    fn ident(ident: &ast::Ident) -> AnalysisResult<InsertTreeNode> {
        let ast::Ident { value, .. } = ident;
        match parse_param_index(value.as_str()) {
            Some(index) => Ok(InsertTreeNode::Item(Operator::Param(index))),
            None => Err(AnalysisError::column_cant_be_referenced(value)),
        }
    }

    fn value(
        value: &ast::Value,
        target_type: &GeneralType,
        column_type: &SqlType,
        level: usize,
    ) -> AnalysisResult<InsertTreeNode> {
        if level == 0 {
            match value {
                ast::Value::Number(num) => match target_type {
                    GeneralType::String => Ok(InsertTreeNode::Item(Operator::Const(ScalarValue::String(
                        num.to_string(),
                    )))),
                    GeneralType::Number => Ok(InsertTreeNode::Item(Operator::Const(ScalarValue::Number(num.clone())))),
                    GeneralType::Bool => Ok(InsertTreeNode::Item(Operator::Const(ScalarValue::Bool(Bool(
                        !num.is_zero(),
                    ))))),
                },
                ast::Value::SingleQuotedString(string) => match target_type {
                    GeneralType::Bool => match Bool::from_str(string.as_str()) {
                        Ok(boolean) => Ok(InsertTreeNode::Item(Operator::Const(ScalarValue::Bool(boolean)))),
                        Err(_error) => Err(AnalysisError::invalid_input_syntax_for_type(*column_type, string)),
                    },
                    GeneralType::String => Ok(InsertTreeNode::Item(Operator::Const(ScalarValue::String(
                        string.clone(),
                    )))),
                    GeneralType::Number => match BigDecimal::from_str(string.as_str()) {
                        Ok(number) => Ok(InsertTreeNode::Item(Operator::Const(ScalarValue::Number(number)))),
                        Err(_error) => Err(AnalysisError::invalid_input_syntax_for_type(*column_type, string)),
                    },
                },
                ast::Value::NationalStringLiteral(_) => {
                    Err(AnalysisError::feature_not_supported(Feature::NationalStringLiteral))
                }
                ast::Value::HexStringLiteral(_) => Err(AnalysisError::feature_not_supported(Feature::HexStringLiteral)),
                ast::Value::Boolean(boolean) => match target_type {
                    GeneralType::String => Ok(InsertTreeNode::Item(Operator::Const(ScalarValue::String(
                        boolean.to_string(),
                    )))),
                    GeneralType::Number => Err(AnalysisError::datatype_mismatch(*column_type, SqlType::Bool)),
                    GeneralType::Bool => Ok(InsertTreeNode::Item(Operator::Const(ScalarValue::Bool(Bool(*boolean))))),
                },
                ast::Value::Interval { .. } => Err(AnalysisError::feature_not_supported(Feature::TimeInterval)),
                ast::Value::Null => Ok(InsertTreeNode::Item(Operator::Const(ScalarValue::Null))),
            }
        } else {
            match value {
                ast::Value::Number(num) => Ok(InsertTreeNode::Item(Operator::Const(ScalarValue::Number(num.clone())))),
                ast::Value::SingleQuotedString(string) => Ok(InsertTreeNode::Item(Operator::Const(
                    ScalarValue::String(string.clone()),
                ))),
                ast::Value::NationalStringLiteral(_) => {
                    Err(AnalysisError::feature_not_supported(Feature::NationalStringLiteral))
                }
                ast::Value::HexStringLiteral(_) => Err(AnalysisError::feature_not_supported(Feature::HexStringLiteral)),
                ast::Value::Boolean(boolean) => {
                    Ok(InsertTreeNode::Item(Operator::Const(ScalarValue::Bool(Bool(*boolean)))))
                }
                ast::Value::Interval { .. } => Err(AnalysisError::feature_not_supported(Feature::TimeInterval)),
                ast::Value::Null => Ok(InsertTreeNode::Item(Operator::Const(ScalarValue::Null))),
            }
        }
    }
}
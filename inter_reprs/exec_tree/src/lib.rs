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
use operators_old::UnArithmetic;
use operators_old::{BiOperator, UnOperator};
use query_result::QueryExecutionError;
use scalar::{OperationError, ScalarValue};
use std::str::FromStr;
use types::{Bool, ParseBoolError, SqlType};

#[derive(Debug, PartialEq)]
enum ExpressionEvaluationError {
    InvalidTextRepresentation { sql_type: SqlType, value: String },
    CanNotCoerce { from: SqlType, to: SqlType },
    UndefinedFunction { sql_type: SqlType, op: operators::UnOperator },
    AmbiguousFunction { sql_type: SqlType, op: operators::UnOperator },
}

impl From<OperationError> for ExpressionEvaluationError {
    fn from(error: OperationError) -> ExpressionEvaluationError {
        match error {
            OperationError::InvalidTextRepresentation { sql_type, value } => ExpressionEvaluationError::InvalidTextRepresentation { sql_type, value },
            OperationError::CanNotCoerce { from, to } => ExpressionEvaluationError::CanNotCoerce { from, to },
            OperationError::UndefinedFunction { sql_type, op } => ExpressionEvaluationError::UndefinedFunction { sql_type, op },
            OperationError::AmbiguousFunction { sql_type, op } => ExpressionEvaluationError::AmbiguousFunction { sql_type, op },
        }
    }
}

impl From<ExpressionEvaluationError> for QueryExecutionError {
    fn from(error: ExpressionEvaluationError) -> QueryExecutionError {
        match error {
            ExpressionEvaluationError::InvalidTextRepresentation { sql_type, value } => {
                QueryExecutionError::invalid_text_representation(sql_type, value)
            }
            ExpressionEvaluationError::CanNotCoerce { from, to } => QueryExecutionError::cannot_coerce(from, to),
            ExpressionEvaluationError::UndefinedFunction { sql_type, op } => QueryExecutionError::undefined_unary_function(op, sql_type),
            ExpressionEvaluationError::AmbiguousFunction { sql_type, op } => QueryExecutionError::ambiguous_function(op, sql_type),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ExecutableTree {
    Item(ExecutableItem),
    UnOp {
        op: UnOperator,
        item: Box<ExecutableTree>,
    },
    BiOp {
        op: BiOperator,
        left: Box<ExecutableTree>,
        right: Box<ExecutableTree>,
    },
}

impl ExecutableTree {
    pub fn eval(self, arguments: &[ScalarValue], input: &[ScalarValue]) -> Result<ScalarValue, QueryExecutionError> {
        match self {
            ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::Int(value))) => Ok(ScalarValue::Integer(value)),
            ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::BigInt(value))) => Ok(ScalarValue::BigInt(value)),
            ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::StringLiteral(value))) => Ok(ScalarValue::StringLiteral(value)),
            ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::Numeric(value))) => Ok(ScalarValue::Numeric(value)),
            ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::Null)) => Ok(ScalarValue::Null(None)),
            ExecutableTree::UnOp { op, item } => match op {
                UnOperator::Cast(sql_type) => item
                    .eval(arguments, input)?
                    .cast_to(sql_type)
                    .map_err(ExpressionEvaluationError::from)
                    .map_err(QueryExecutionError::from),
                UnOperator::Arithmetic(UnArithmetic::Neg) => item
                    .eval(arguments, input)?
                    .negate()
                    .map_err(ExpressionEvaluationError::from)
                    .map_err(QueryExecutionError::from),
                UnOperator::Arithmetic(_) => unimplemented!(),
                UnOperator::LogicalNot => unimplemented!(),
                UnOperator::BitwiseNot => unimplemented!(),
            },
            ExecutableTree::BiOp { .. } => unimplemented!(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ExecutableItem {
    Const(ExecutableValue),
}

#[derive(Debug, PartialEq, Clone)]
pub enum ExecutableValue {
    Int(i32),
    BigInt(i64),
    Numeric(BigDecimal),
    StringLiteral(String),
    Null,
}

#[cfg(test)]
mod tests;

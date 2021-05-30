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
use operators::{BiOperator, UnOperator};
use query_result::QueryExecutionError;
use scalar::ScalarValue;
use std::fmt::{self, Display, Formatter};
use types::SqlTypeFamily;

#[derive(Debug, PartialEq, Clone)]
pub enum TypedTree {
    BiOp {
        type_family: SqlTypeFamily,
        left: Box<TypedTree>,
        op: BiOperator,
        right: Box<TypedTree>,
    },
    UnOp {
        op: UnOperator,
        item: Box<TypedTree>,
    },
    Item(TypedItem),
}

impl TypedTree {
    pub fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            TypedTree::Item(item) => item.type_family(),
            TypedTree::BiOp { type_family, .. } => Some(*type_family),
            TypedTree::UnOp { item, .. } => item.type_family(),
        }
    }

    pub fn eval(self, param_values: &[ScalarValue], table_row: &[ScalarValue]) -> Result<ScalarValue, QueryExecutionError> {
        match self {
            TypedTree::Item(TypedItem::Const(value)) => Ok(value.eval()),
            TypedTree::Item(TypedItem::Column { index, .. }) => Ok(table_row[index].clone()),
            TypedTree::Item(TypedItem::Param { index, .. }) => Ok(param_values[index].clone()),
            TypedTree::Item(TypedItem::Null(_)) => Ok(ScalarValue::Null),
            TypedTree::UnOp { op, item } => op.eval(item.eval(param_values, table_row)?),
            TypedTree::BiOp { left, op, right, .. } => op.eval(left.eval(param_values, table_row)?, right.eval(param_values, table_row)?),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedItem {
    Const(TypedValue),
    Param { index: usize, type_family: Option<SqlTypeFamily> },
    Null(Option<SqlTypeFamily>),
    Column { name: String, sql_type: SqlTypeFamily, index: usize },
}

impl TypedItem {
    fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            TypedItem::Const(typed_value) => typed_value.type_family(),
            TypedItem::Column { sql_type, .. } => Some(*sql_type),
            TypedItem::Param { type_family, .. } => *type_family,
            TypedItem::Null(type_family) => *type_family,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedValue {
    Num { value: BigDecimal, type_family: SqlTypeFamily },
    String(String),
    Bool(bool),
}

impl TypedValue {
    pub fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            TypedValue::Num { type_family, .. } => Some(*type_family),
            TypedValue::String(_) => Some(SqlTypeFamily::String),
            TypedValue::Bool(_) => Some(SqlTypeFamily::Bool),
        }
    }

    pub fn eval(self) -> ScalarValue {
        match self {
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::SmallInt,
            } => ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::SmallInt,
            },
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::Integer,
            } => ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::Integer,
            },
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::Real,
            } => ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::Real,
            },
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::Double,
            } => ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::Double,
            },
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::BigInt,
            } => ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::BigInt,
            },
            TypedValue::String(str) => ScalarValue::String(str),
            TypedValue::Bool(boolean) => ScalarValue::Bool(boolean),
            _ => unreachable!(),
        }
    }
}

impl Display for TypedValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TypedValue::Num { value, .. } => write!(f, "{}", value),
            TypedValue::String(value) => write!(f, "{}", value),
            TypedValue::Bool(value) => write!(f, "{}", value),
        }
    }
}

#[cfg(test)]
mod tests;

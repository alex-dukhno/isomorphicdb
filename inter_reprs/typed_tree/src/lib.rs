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
    Item(TypedItem),
    UnOp {
        op: UnOperator,
        item: Box<TypedTree>,
    },
    BiOp {
        op: BiOperator,
        left: Box<TypedTree>,
        right: Box<TypedTree>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedItem {
    Const(TypedValue),
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedValue {
    Int(i32),
    BigInt(i64),
    Numeric(BigDecimal),
    StringLiteral(String),
    Null,
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedTreeOld {
    BiOp {
        type_family: SqlTypeFamily,
        left: Box<TypedTreeOld>,
        op: BiOperator,
        right: Box<TypedTreeOld>,
    },
    UnOp {
        op: UnOperator,
        item: Box<TypedTreeOld>,
    },
    Item(TypedItemOld),
}

impl TypedTreeOld {
    pub fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            TypedTreeOld::Item(item) => item.type_family(),
            TypedTreeOld::BiOp { type_family, .. } => Some(*type_family),
            TypedTreeOld::UnOp { item, .. } => item.type_family(),
        }
    }

    pub fn eval(self, param_values: &[ScalarValue], table_row: &[ScalarValue]) -> Result<ScalarValue, QueryExecutionError> {
        match self {
            TypedTreeOld::Item(TypedItemOld::Const(value)) => Ok(value.eval()),
            TypedTreeOld::Item(TypedItemOld::Column { index, .. }) => Ok(table_row[index].clone()),
            TypedTreeOld::Item(TypedItemOld::Param { index, .. }) => Ok(param_values[index].clone()),
            TypedTreeOld::Item(TypedItemOld::Null(_)) => Ok(ScalarValue::Null),
            TypedTreeOld::UnOp { op, item } => op.eval(item.eval(param_values, table_row)?),
            TypedTreeOld::BiOp { left, op, right, .. } => op.eval(left.eval(param_values, table_row)?, right.eval(param_values, table_row)?),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedItemOld {
    Const(TypedValueOld),
    Param { index: usize, type_family: Option<SqlTypeFamily> },
    Null(Option<SqlTypeFamily>),
    Column { name: String, sql_type: SqlTypeFamily, index: usize },
}

impl TypedItemOld {
    fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            TypedItemOld::Const(typed_value) => typed_value.type_family(),
            TypedItemOld::Column { sql_type, .. } => Some(*sql_type),
            TypedItemOld::Param { type_family, .. } => *type_family,
            TypedItemOld::Null(type_family) => *type_family,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedValueOld {
    Num { value: BigDecimal, type_family: SqlTypeFamily },
    String(String),
    Bool(bool),
}

impl TypedValueOld {
    pub fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            TypedValueOld::Num { type_family, .. } => Some(*type_family),
            TypedValueOld::String(_) => Some(SqlTypeFamily::String),
            TypedValueOld::Bool(_) => Some(SqlTypeFamily::Bool),
        }
    }

    pub fn eval(self) -> ScalarValue {
        match self {
            TypedValueOld::Num {
                value,
                type_family: SqlTypeFamily::SmallInt,
            } => ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::SmallInt,
            },
            TypedValueOld::Num {
                value,
                type_family: SqlTypeFamily::Integer,
            } => ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::Integer,
            },
            TypedValueOld::Num {
                value,
                type_family: SqlTypeFamily::Real,
            } => ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::Real,
            },
            TypedValueOld::Num {
                value,
                type_family: SqlTypeFamily::Double,
            } => ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::Double,
            },
            TypedValueOld::Num {
                value,
                type_family: SqlTypeFamily::BigInt,
            } => ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::BigInt,
            },
            TypedValueOld::String(str) => ScalarValue::String(str),
            TypedValueOld::Bool(boolean) => ScalarValue::Bool(boolean),
            _ => unreachable!(),
        }
    }
}

impl Display for TypedValueOld {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TypedValueOld::Num { value, .. } => write!(f, "{}", value),
            TypedValueOld::String(value) => write!(f, "{}", value),
            TypedValueOld::Bool(value) => write!(f, "{}", value),
        }
    }
}

#[cfg(test)]
mod tests;

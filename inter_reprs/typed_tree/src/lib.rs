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
use operators_old::{BiOperator, UnOperator};
use query_result::QueryExecutionError;
use scalar::ScalarValueOld;
use std::fmt::{self, Display, Formatter};
use types::SqlTypeFamilyOld;

#[derive(Debug, PartialEq, Clone)]
pub enum TypedTree {
    Item(TypedItem),
    UnOp {
        op: operators::UnOperator,
        item: Box<TypedTree>,
    },
    BiOp {
        op: operators::BiOperator,
        left: Box<TypedTree>,
        right: Box<TypedTree>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedItem {
    Const(TypedValue),
    Param(usize),
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
        type_family: SqlTypeFamilyOld,
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
    pub fn type_family(&self) -> Option<SqlTypeFamilyOld> {
        match self {
            TypedTreeOld::Item(item) => item.type_family(),
            TypedTreeOld::BiOp { type_family, .. } => Some(*type_family),
            TypedTreeOld::UnOp { item, .. } => item.type_family(),
        }
    }

    pub fn eval(self, param_values: &[ScalarValueOld], table_row: &[ScalarValueOld]) -> Result<ScalarValueOld, QueryExecutionError> {
        match self {
            TypedTreeOld::Item(TypedItemOld::Const(value)) => Ok(value.eval()),
            TypedTreeOld::Item(TypedItemOld::Column { index, .. }) => Ok(table_row[index].clone()),
            TypedTreeOld::Item(TypedItemOld::Param { index, .. }) => Ok(param_values[index].clone()),
            TypedTreeOld::Item(TypedItemOld::Null(_)) => Ok(ScalarValueOld::Null),
            TypedTreeOld::UnOp { op, item } => op.eval_old(item.eval(param_values, table_row)?),
            TypedTreeOld::BiOp { left, op, right, .. } => op.eval(left.eval(param_values, table_row)?, right.eval(param_values, table_row)?),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedItemOld {
    Const(TypedValueOld),
    Param {
        index: usize,
        type_family: Option<SqlTypeFamilyOld>,
    },
    Null(Option<SqlTypeFamilyOld>),
    Column {
        name: String,
        sql_type: SqlTypeFamilyOld,
        index: usize,
    },
}

impl TypedItemOld {
    fn type_family(&self) -> Option<SqlTypeFamilyOld> {
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
    Num { value: BigDecimal, type_family: SqlTypeFamilyOld },
    String(String),
    Bool(bool),
}

impl TypedValueOld {
    pub fn type_family(&self) -> Option<SqlTypeFamilyOld> {
        match self {
            TypedValueOld::Num { type_family, .. } => Some(*type_family),
            TypedValueOld::String(_) => Some(SqlTypeFamilyOld::String),
            TypedValueOld::Bool(_) => Some(SqlTypeFamilyOld::Bool),
        }
    }

    pub fn eval(self) -> ScalarValueOld {
        match self {
            TypedValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::SmallInt,
            } => ScalarValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::SmallInt,
            },
            TypedValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::Integer,
            } => ScalarValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::Integer,
            },
            TypedValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::Real,
            } => ScalarValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::Real,
            },
            TypedValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::Double,
            } => ScalarValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::Double,
            },
            TypedValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::BigInt,
            } => ScalarValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::BigInt,
            },
            TypedValueOld::String(str) => ScalarValueOld::String(str),
            TypedValueOld::Bool(boolean) => ScalarValueOld::Bool(boolean),
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

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

use bigdecimal::{BigDecimal, ToPrimitive};
use data_binary::repr::{Datum, ToDatum};
use data_manipulation_operators::{BiOperator, UnOperator};
use data_manipulation_query_result::QueryExecutionError;
use data_scalar::ScalarValue;
use std::fmt::{self, Display, Formatter};
use types::SqlTypeFamily;

#[derive(Debug, PartialEq, Clone)]
pub enum StaticTypedTree {
    Item(StaticTypedItem),
    BiOp {
        type_family: SqlTypeFamily,
        left: Box<StaticTypedTree>,
        op: BiOperator,
        right: Box<StaticTypedTree>,
    },
    UnOp {
        op: UnOperator,
        item: Box<StaticTypedTree>,
    },
}

impl StaticTypedTree {
    pub fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            StaticTypedTree::Item(item) => item.type_family(),
            StaticTypedTree::BiOp { type_family, .. } => Some(*type_family),
            StaticTypedTree::UnOp { item, .. } => item.type_family(),
        }
    }

    pub fn eval(self, param_values: &[ScalarValue]) -> Result<ScalarValue, QueryExecutionError> {
        match self {
            StaticTypedTree::Item(StaticTypedItem::Const(value)) => Ok(value.eval()),
            StaticTypedTree::Item(StaticTypedItem::Null(_)) => Ok(ScalarValue::Null),
            StaticTypedTree::Item(StaticTypedItem::Param { index, .. }) => Ok(param_values[index].clone()),
            StaticTypedTree::UnOp { op, item } => op.eval(item.eval(param_values)?),
            StaticTypedTree::BiOp { left, op, right, .. } => {
                op.eval(left.eval(param_values)?, right.eval(param_values)?)
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum StaticTypedItem {
    Const(TypedValue),
    Param {
        index: usize,
        type_family: Option<SqlTypeFamily>,
    },
    Null(Option<SqlTypeFamily>),
}

impl StaticTypedItem {
    fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            StaticTypedItem::Const(typed_value) => typed_value.type_family(),
            StaticTypedItem::Param { type_family, .. } => *type_family,
            StaticTypedItem::Null(type_family) => *type_family,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum DynamicTypedTree {
    BiOp {
        type_family: SqlTypeFamily,
        left: Box<DynamicTypedTree>,
        op: BiOperator,
        right: Box<DynamicTypedTree>,
    },
    UnOp {
        op: UnOperator,
        item: Box<DynamicTypedTree>,
    },
    Item(DynamicTypedItem),
}

impl DynamicTypedTree {
    pub fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            DynamicTypedTree::Item(item) => item.type_family(),
            DynamicTypedTree::BiOp { type_family, .. } => Some(*type_family),
            DynamicTypedTree::UnOp { item, .. } => item.type_family(),
        }
    }

    pub fn eval(
        self,
        param_values: &[ScalarValue],
        table_row: &[ScalarValue],
    ) -> Result<ScalarValue, QueryExecutionError> {
        match self {
            DynamicTypedTree::Item(DynamicTypedItem::Const(value)) => Ok(value.eval()),
            DynamicTypedTree::Item(DynamicTypedItem::Column { index, .. }) => Ok(table_row[index].clone()),
            DynamicTypedTree::Item(DynamicTypedItem::Param { index, .. }) => Ok(param_values[index].clone()),
            DynamicTypedTree::Item(DynamicTypedItem::Null(_)) => Ok(ScalarValue::Null),
            DynamicTypedTree::UnOp { op, item } => op.eval(item.eval(param_values, table_row)?),
            DynamicTypedTree::BiOp { left, op, right, .. } => op.eval(
                left.eval(param_values, table_row)?,
                right.eval(param_values, table_row)?,
            ),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum DynamicTypedItem {
    Const(TypedValue),
    Param {
        index: usize,
        type_family: Option<SqlTypeFamily>,
    },
    Null(Option<SqlTypeFamily>),
    Column {
        name: String,
        sql_type: SqlTypeFamily,
        index: usize,
    },
}

impl DynamicTypedItem {
    fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            DynamicTypedItem::Const(typed_value) => typed_value.type_family(),
            DynamicTypedItem::Column { sql_type, .. } => Some(*sql_type),
            DynamicTypedItem::Param { type_family, .. } => *type_family,
            DynamicTypedItem::Null(type_family) => *type_family,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedValue {
    Num {
        value: BigDecimal,
        type_family: SqlTypeFamily,
    },
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

impl ToDatum for TypedValue {
    fn convert(&self) -> Datum {
        match self {
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::SmallInt,
            } => Datum::from_i16(value.to_i16().unwrap()),
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::Integer,
            } => Datum::from_i32(value.to_i32().unwrap()),
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::Real,
            } => Datum::from_f32(value.to_f32().unwrap()),
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::Double,
            } => Datum::from_f64(value.to_f64().unwrap()),
            TypedValue::Num {
                value,
                type_family: SqlTypeFamily::BigInt,
            } => Datum::from_i64(value.to_i64().unwrap()),
            TypedValue::String(str) => Datum::from_string(str.clone()),
            TypedValue::Bool(boolean) => Datum::from_bool(*boolean),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests;

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

use bigdecimal::BigDecimal;
use data_manipulation_operators::{BiOperation, UnArithmetic, UnLogical, UnOperation};
use data_manipulation_query_result::QueryExecutionError;
use types::SqlTypeFamily;

#[derive(Debug, PartialEq, Clone)]
pub enum StaticTypedTree {
    Item(StaticTypedItem),
    BiOp {
        type_family: Option<SqlTypeFamily>,
        left: Box<StaticTypedTree>,
        op: BiOperation,
        right: Box<StaticTypedTree>,
    },
    UnOp {
        op: UnOperation,
        item: Box<StaticTypedTree>,
    },
}

impl StaticTypedTree {
    pub fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            StaticTypedTree::Item(item) => item.type_family(),
            StaticTypedTree::BiOp { type_family, .. } => *type_family,
            StaticTypedTree::UnOp { item, .. } => item.type_family(),
        }
    }

    pub fn eval(self) -> Result<TypedValue, QueryExecutionError> {
        match self {
            StaticTypedTree::Item(StaticTypedItem::Const(value)) => Ok(value),
            StaticTypedTree::UnOp { op, item } => {
                let value = item.eval()?;
                match op {
                    UnOperation::Arithmetic(UnArithmetic::Neg) => match value {
                        TypedValue::Num { value, type_family } => Ok(TypedValue::Num {
                            value: -value,
                            type_family,
                        }),
                        other => Err(QueryExecutionError::undefined_function(
                            op,
                            other
                                .type_family()
                                .map(|ty| ty.to_string())
                                .unwrap_or_else(|| "unknown".to_owned()),
                        )),
                    },
                    UnOperation::Arithmetic(UnArithmetic::Pos) => match value {
                        TypedValue::Num { value, type_family } => Ok(TypedValue::Num { value, type_family }),
                        other => Err(QueryExecutionError::undefined_function(
                            op,
                            other
                                .type_family()
                                .map(|ty| ty.to_string())
                                .unwrap_or_else(|| "unknown".to_owned()),
                        )),
                    },
                    UnOperation::Arithmetic(_) => unimplemented!(),
                    UnOperation::Logical(UnLogical::Not) => match value {
                        TypedValue::Bool(value) => Ok(TypedValue::Bool(!value)),
                        other => Err(QueryExecutionError::datatype_mismatch(
                            op,
                            SqlTypeFamily::Bool,
                            other
                                .type_family()
                                .map(|ty| ty.to_string())
                                .unwrap_or_else(|| "unknown".to_owned()),
                        )),
                    },
                    UnOperation::Bitwise(_) => unimplemented!(),
                }
            }
            _ => unimplemented!(),
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
pub enum TypedValue {
    Num {
        value: BigDecimal,
        type_family: SqlTypeFamily,
    },
    String(String),
    Bool(bool),
}

impl TypedValue {
    fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            TypedValue::Num { type_family, .. } => Some(*type_family),
            TypedValue::String(_) => Some(SqlTypeFamily::String),
            TypedValue::Bool(_) => Some(SqlTypeFamily::Bool),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum DynamicTypedTree {
    Operation {
        left: Box<DynamicTypedTree>,
        op: BiOperation,
        right: Box<DynamicTypedTree>,
    },
    Item(DynamicTypedItem),
}

#[derive(Debug, PartialEq, Clone)]
pub enum DynamicTypedItem {
    Const(TypedValue),
    Column(String),
}

#[cfg(test)]
mod tests;

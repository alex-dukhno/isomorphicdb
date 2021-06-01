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

use bigdecimal::{BigDecimal, FromPrimitive};
use operators::BiOperator;
use operators_old::UnOperator;
use query_ast::{BinaryOperator, Expr, UnaryOperator, Value};
use query_response::QueryError;
use std::ops::RangeInclusive;
use std::str::FromStr;
use typed_tree::{TypedItem, TypedTree, TypedValue};
use typed_tree::{TypedItemOld, TypedTreeOld, TypedValueOld};
use types::{Bool, SqlType, SqlTypeFamilyOld};
use untyped_tree::{UntypedItem, UntypedTree, UntypedValue};

const MAX_BIG_INT: &str = "9223372036854775807";
const MIN_BIG_INT: &str = "-9223372036854775808";

#[derive(Debug, PartialEq)]
pub enum TypeInferenceError {
    ColumnNotFound(String),
    ColumnCantBeReferenced(String),
}

impl TypeInferenceError {
    pub fn column_not_found<C: ToString>(column_name: C) -> TypeInferenceError {
        TypeInferenceError::ColumnNotFound(column_name.to_string())
    }

    pub fn column_cant_be_referenced<C: ToString>(column_name: C) -> TypeInferenceError {
        TypeInferenceError::ColumnCantBeReferenced(column_name.to_string())
    }
}

impl From<TypeInferenceError> for QueryError {
    fn from(_error: TypeInferenceError) -> QueryError {
        unimplemented!()
    }
}

pub struct TypeInference;

impl TypeInference {
    pub fn infer_type(&self, tree: Expr) -> Result<TypedTree, TypeInferenceError> {
        Self::insert_position(tree)
    }

    fn insert_position(expr: Expr) -> Result<TypedTree, TypeInferenceError> {
        Self::inner_insert_position(expr)
    }

    fn inner_insert_position(expr: Expr) -> Result<TypedTree, TypeInferenceError> {
        match expr {
            Expr::Value(value) => Ok(Self::value(value)),
            Expr::Column(name) => Err(TypeInferenceError::column_cant_be_referenced(name)),
            Expr::BinaryOp { left, op, right } => Self::static_binary_op(op, *left, *right),
            Expr::Cast { expr, data_type } => Ok(TypedTree::UnOp {
                op: operators::UnOperator::Cast(SqlType::from(data_type)),
                item: Box::new(Self::inner_insert_position(*expr)?),
            }),
            Expr::UnaryOp { op, expr } => Ok(TypedTree::UnOp {
                op: operators::UnOperator::from(op),
                item: Box::new(Self::inner_insert_position(*expr)?),
            }),
            Expr::Param(index) => Ok(TypedTree::Item(TypedItem::Param((index - 1) as usize))),
        }
    }

    fn static_binary_op(operator: BinaryOperator, left: Expr, right: Expr) -> Result<TypedTree, TypeInferenceError> {
        let left = Self::inner_insert_position(left)?;
        let right = Self::inner_insert_position(right)?;
        Ok(TypedTree::BiOp {
            left: Box::new(left),
            op: operators::BiOperator::from(operator),
            right: Box::new(right),
        })
    }

    fn value(value: Value) -> TypedTree {
        match value {
            Value::Int(num) => TypedTree::Item(TypedItem::Const(TypedValue::Int(num))),
            Value::String(string) => TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral(string))),
            Value::Null => TypedTree::Item(TypedItem::Const(TypedValue::Null)),
            Value::Number(value) if value.contains('.') => {
                TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from_str(&value).unwrap())))
            }
            Value::Number(value) => {
                if (value.starts_with('-') && value.len() < MIN_BIG_INT.len() || value.len() == MIN_BIG_INT.len() && value.as_str() <= MIN_BIG_INT)
                    || (value.len() < MAX_BIG_INT.len() || value.len() == MAX_BIG_INT.len() && value.as_str() <= MAX_BIG_INT)
                {
                    TypedTree::Item(TypedItem::Const(TypedValue::BigInt(value.parse().unwrap())))
                } else {
                    TypedTree::Item(TypedItem::Const(TypedValue::Numeric(value.parse().unwrap())))
                }
            }
        }
    }
}

pub struct TypeInferenceOld {
    small_int_range: RangeInclusive<BigDecimal>,
    integer_range: RangeInclusive<BigDecimal>,
    #[allow(dead_code)]
    big_int_range: RangeInclusive<BigDecimal>,
    real_range: RangeInclusive<BigDecimal>,
    double_precision_range: RangeInclusive<BigDecimal>,
}

impl Default for TypeInferenceOld {
    fn default() -> TypeInferenceOld {
        TypeInferenceOld {
            small_int_range: BigDecimal::from(i16::MIN)..=BigDecimal::from(i16::MAX),
            integer_range: BigDecimal::from(i32::MIN)..=BigDecimal::from(i32::MAX),
            big_int_range: BigDecimal::from(i64::MIN)..=BigDecimal::from(i64::MAX),
            real_range: BigDecimal::from_f32(f32::MIN).unwrap()..=BigDecimal::from_f32(f32::MAX).unwrap(),
            double_precision_range: BigDecimal::from_f64(f64::MIN).unwrap()..=BigDecimal::from_f64(f64::MAX).unwrap(),
        }
    }
}

impl TypeInferenceOld {
    pub fn infer_type(&self, tree: UntypedTree, param_types: &[SqlTypeFamilyOld]) -> TypedTreeOld {
        match tree {
            UntypedTree::Item(UntypedItem::Param(index)) => TypedTreeOld::Item(TypedItemOld::Param {
                index,
                type_family: Some(param_types[index]),
            }),
            UntypedTree::Item(UntypedItem::Column { name, sql_type, index }) => TypedTreeOld::Item(TypedItemOld::Column {
                name,
                sql_type: sql_type.family(),
                index,
            }),
            UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(num))) => {
                let num = BigDecimal::from_str(&num).unwrap();
                if num.is_integer() {
                    if self.small_int_range.contains(&num) {
                        TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                            value: num,
                            type_family: SqlTypeFamilyOld::SmallInt,
                        }))
                    } else if self.integer_range.contains(&num) {
                        TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                            value: num,
                            type_family: SqlTypeFamilyOld::Integer,
                        }))
                    } else {
                        TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                            value: num,
                            type_family: SqlTypeFamilyOld::BigInt,
                        }))
                    }
                } else if self.real_range.contains(&num) {
                    TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                        value: num,
                        type_family: SqlTypeFamilyOld::Real,
                    }))
                } else if self.double_precision_range.contains(&num) {
                    TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                        value: num,
                        type_family: SqlTypeFamilyOld::Double,
                    }))
                } else {
                    unimplemented!()
                }
            }
            UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal(value))) => {
                TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String(value)))
            }
            UntypedTree::Item(UntypedItem::Const(UntypedValue::Bool(Bool(value)))) => {
                TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(value)))
            }
            UntypedTree::BiOp { left, op, right } => {
                log::debug!("LEFT TREE {:#?}", left);
                log::debug!("RIGHT TREE {:#?}", right);
                let left_tree = self.infer_type(*left, param_types);
                let right_tree = self.infer_type(*right, param_types);
                let type_family = match (left_tree.type_family(), right_tree.type_family()) {
                    (Some(left_type_family), Some(right_type_family)) => match left_type_family.compare(&right_type_family) {
                        Ok(type_family) => type_family,
                        Err(_) => unimplemented!(),
                    },
                    (Some(left_type_family), None) => left_type_family,
                    (None, Some(right_type_family)) => right_type_family,
                    (None, None) => unimplemented!(),
                };
                TypedTreeOld::BiOp {
                    type_family,
                    left: Box::new(left_tree),
                    op,
                    right: Box::new(right_tree),
                }
            }
            UntypedTree::UnOp { op, item } => TypedTreeOld::UnOp {
                op,
                item: Box::new(self.infer_type(*item, param_types)),
            },
            UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(value))) => TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(value),
                type_family: SqlTypeFamilyOld::Integer,
            })),
            UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)) => TypedTreeOld::Item(TypedItemOld::Null(None)),
        }
    }
}

#[cfg(test)]
mod tests;

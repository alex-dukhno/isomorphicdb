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

use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
use data_manipulation_operators::Operation;
use data_manipulation_untyped_tree::{StaticUntypedItem, StaticUntypedTree, UntypedValue};
use std::ops::RangeInclusive;
use types::SqlTypeFamily;

pub struct TypeInference {
    small_int_range: RangeInclusive<BigDecimal>,
    integer_range: RangeInclusive<BigDecimal>,
    big_int_range: RangeInclusive<BigDecimal>,
    real_range: RangeInclusive<BigDecimal>,
    double_precision_range: RangeInclusive<BigDecimal>,
}

impl TypeInference {
    fn new() -> TypeInference {
        TypeInference {
            small_int_range: BigDecimal::from(i16::MIN)..=BigDecimal::from(i16::MAX),
            integer_range: BigDecimal::from(i32::MIN)..=BigDecimal::from(i32::MAX),
            big_int_range: BigDecimal::from(i64::MIN)..=BigDecimal::from(i64::MAX),
            real_range: BigDecimal::from_f32(f32::MIN).unwrap()..=BigDecimal::from_f32(f32::MAX).unwrap(),
            double_precision_range: BigDecimal::from_f64(f64::MIN).unwrap()..=BigDecimal::from_f64(f64::MAX).unwrap(),
        }
    }

    pub fn infer(&self, tree: StaticUntypedTree) -> StaticTypedTree {
        match tree {
            StaticUntypedTree::Operation { left, op, right } => {
                let left_tree = self.infer(*left);
                let right_tree = self.infer(*right);
                let op_type_family = match left_tree.type_family().cmp(&right_tree.type_family()) {
                    Ok(type_family) => type_family,
                    Err(_) => unimplemented!(),
                };
                StaticTypedTree::Operation {
                    type_family: op_type_family,
                    left: Box::new(left_tree),
                    op,
                    right: Box::new(right_tree),
                }
            }
            StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Number(num))) => {
                if num.is_integer() {
                    if self.small_int_range.contains(&num) {
                        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(num.to_i16().unwrap())))
                    } else if self.integer_range.contains(&num) {
                        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Integer(num.to_i32().unwrap())))
                    } else if self.big_int_range.contains(&num) {
                        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::BigInt(num.to_i64().unwrap())))
                    } else {
                        unimplemented!()
                    }
                } else {
                    if self.real_range.contains(&num) {
                        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Real(num.to_f32().unwrap())))
                    } else if self.double_precision_range.contains(&num) {
                        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Double(num.to_f64().unwrap())))
                    } else {
                        unimplemented!()
                    }
                }
            }
            StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::String(str))) => {
                StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(str)))
            }
            StaticUntypedTree::Item(_) => unimplemented!(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum StaticTypedTree {
    Item(StaticTypedItem),
    Operation {
        type_family: SqlTypeFamily,
        left: Box<StaticTypedTree>,
        op: Operation,
        right: Box<StaticTypedTree>,
    },
}

impl StaticTypedTree {
    pub fn type_family(&self) -> SqlTypeFamily {
        match self {
            StaticTypedTree::Item(item) => item.type_family(),
            StaticTypedTree::Operation { type_family, .. } => *type_family,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum StaticTypedItem {
    Const(TypedValue),
}

impl StaticTypedItem {
    fn type_family(&self) -> SqlTypeFamily {
        match self {
            StaticTypedItem::Const(typed_value) => typed_value.type_family(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum TypedValue {
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Real(f32),
    String(String),
    Double(f64),
}

impl TypedValue {
    fn type_family(&self) -> SqlTypeFamily {
        match self {
            TypedValue::SmallInt(_) => SqlTypeFamily::SmallInt,
            TypedValue::Integer(_) => SqlTypeFamily::Integer,
            TypedValue::BigInt(_) => SqlTypeFamily::BigInt,
            TypedValue::Real(_) => SqlTypeFamily::Real,
            TypedValue::Double(_) => SqlTypeFamily::Double,
            TypedValue::String(_) => SqlTypeFamily::String,
        }
    }
}

#[cfg(test)]
mod tests;

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
use data_manipulation_typed_tree::{TypedItem, TypedTree, TypedValue};
use data_manipulation_untyped_tree::{UntypedItem, UntypedTree, UntypedValue};
use std::ops::RangeInclusive;
use types::{Bool, SqlTypeFamily};

pub struct TypeInference {
    small_int_range: RangeInclusive<BigDecimal>,
    integer_range: RangeInclusive<BigDecimal>,
    #[allow(dead_code)]
    big_int_range: RangeInclusive<BigDecimal>,
    real_range: RangeInclusive<BigDecimal>,
    double_precision_range: RangeInclusive<BigDecimal>,
}

impl Default for TypeInference {
    fn default() -> TypeInference {
        TypeInference {
            small_int_range: BigDecimal::from(i16::MIN)..=BigDecimal::from(i16::MAX),
            integer_range: BigDecimal::from(i32::MIN)..=BigDecimal::from(i32::MAX),
            big_int_range: BigDecimal::from(i64::MIN)..=BigDecimal::from(i64::MAX),
            real_range: BigDecimal::from_f32(f32::MIN).unwrap()..=BigDecimal::from_f32(f32::MAX).unwrap(),
            double_precision_range: BigDecimal::from_f64(f64::MIN).unwrap()..=BigDecimal::from_f64(f64::MAX).unwrap(),
        }
    }
}

impl TypeInference {
    pub fn infer_type(&self, tree: UntypedTree, param_types: &[SqlTypeFamily]) -> TypedTree {
        match tree {
            UntypedTree::Item(UntypedItem::Param(index)) => TypedTree::Item(TypedItem::Param {
                index,
                type_family: Some(param_types[index]),
            }),
            UntypedTree::Item(UntypedItem::Column { name, sql_type, index }) => TypedTree::Item(TypedItem::Column {
                name,
                sql_type: sql_type.family(),
                index,
            }),
            UntypedTree::Item(UntypedItem::Const(UntypedValue::Number(num))) => {
                if num.is_integer() {
                    if self.small_int_range.contains(&num) {
                        TypedTree::Item(TypedItem::Const(TypedValue::Num {
                            value: num,
                            type_family: SqlTypeFamily::SmallInt,
                        }))
                    } else if self.integer_range.contains(&num) {
                        TypedTree::Item(TypedItem::Const(TypedValue::Num {
                            value: num,
                            type_family: SqlTypeFamily::Integer,
                        }))
                    } else {
                        TypedTree::Item(TypedItem::Const(TypedValue::Num {
                            value: num,
                            type_family: SqlTypeFamily::BigInt,
                        }))
                    }
                } else if self.real_range.contains(&num) {
                    TypedTree::Item(TypedItem::Const(TypedValue::Num {
                        value: num,
                        type_family: SqlTypeFamily::Real,
                    }))
                } else if self.double_precision_range.contains(&num) {
                    TypedTree::Item(TypedItem::Const(TypedValue::Num {
                        value: num,
                        type_family: SqlTypeFamily::Double,
                    }))
                } else {
                    unimplemented!()
                }
            }
            UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal(str))) => TypedTree::Item(TypedItem::Const(TypedValue::String(str))),
            UntypedTree::Item(UntypedItem::Const(UntypedValue::Bool(Bool(boolean)))) => TypedTree::Item(TypedItem::Const(TypedValue::Bool(boolean))),
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
                TypedTree::BiOp {
                    type_family,
                    left: Box::new(left_tree),
                    op,
                    right: Box::new(right_tree),
                }
            }
            UntypedTree::UnOp { op, item } => TypedTree::UnOp {
                op,
                item: Box::new(self.infer_type(*item, param_types)),
            },
            UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(value))) => TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(value),
                type_family: SqlTypeFamily::Integer,
            })),
            UntypedTree::Item(UntypedItem::Const(UntypedValue::BigInt(value))) => TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(value),
                type_family: SqlTypeFamily::BigInt,
            })),
            UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)) => TypedTree::Item(TypedItem::Null(None)),
        }
    }
}

#[cfg(test)]
mod tests;

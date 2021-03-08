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

use bigdecimal::{BigDecimal, FromPrimitive};
use data_manipulation_typed_tree::{DynamicTypedItem, DynamicTypedTree, StaticTypedItem, StaticTypedTree, TypedValue};
use data_manipulation_untyped_tree::{
    DynamicUntypedItem, DynamicUntypedTree, StaticUntypedItem, StaticUntypedTree, UntypedValue,
};
use std::ops::RangeInclusive;
use types::{Bool, SqlTypeFamily};

pub struct TypeInference {
    small_int_range: RangeInclusive<BigDecimal>,
    integer_range: RangeInclusive<BigDecimal>,
    big_int_range: RangeInclusive<BigDecimal>,
    real_range: RangeInclusive<BigDecimal>,
    double_precision_range: RangeInclusive<BigDecimal>,
}

impl Default for TypeInference {
    fn default() -> Self {
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
    pub fn infer_dynamic(&self, tree: DynamicUntypedTree, param_types: &[SqlTypeFamily]) -> DynamicTypedTree {
        match tree {
            DynamicUntypedTree::Item(DynamicUntypedItem::Param(index)) => {
                DynamicTypedTree::Item(DynamicTypedItem::Param {
                    index,
                    type_family: Some(param_types[index]),
                })
            }
            DynamicUntypedTree::Item(DynamicUntypedItem::Column { name, sql_type, index }) => {
                DynamicTypedTree::Item(DynamicTypedItem::Column {
                    name,
                    sql_type: sql_type.family(),
                    index,
                })
            }
            DynamicUntypedTree::Item(DynamicUntypedItem::Const(UntypedValue::Number(num))) => {
                if num.is_integer() {
                    if self.small_int_range.contains(&num) {
                        DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::Num {
                            value: num,
                            type_family: SqlTypeFamily::SmallInt,
                        }))
                    } else if self.integer_range.contains(&num) {
                        DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::Num {
                            value: num,
                            type_family: SqlTypeFamily::Integer,
                        }))
                    } else if self.big_int_range.contains(&num) {
                        DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::Num {
                            value: num,
                            type_family: SqlTypeFamily::BigInt,
                        }))
                    } else {
                        unimplemented!()
                    }
                } else if self.real_range.contains(&num) {
                    DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::Num {
                        value: num,
                        type_family: SqlTypeFamily::Real,
                    }))
                } else if self.double_precision_range.contains(&num) {
                    DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::Num {
                        value: num,
                        type_family: SqlTypeFamily::Double,
                    }))
                } else {
                    unimplemented!()
                }
            }
            DynamicUntypedTree::Item(DynamicUntypedItem::Const(UntypedValue::String(str))) => {
                DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::String(str)))
            }
            DynamicUntypedTree::Item(DynamicUntypedItem::Const(UntypedValue::Bool(Bool(boolean)))) => {
                DynamicTypedTree::Item(DynamicTypedItem::Const(TypedValue::Bool(boolean)))
            }
            DynamicUntypedTree::BiOp { left, op, right } => {
                log::debug!("LEFT TREE {:#?}", left);
                log::debug!("RIGHT TREE {:#?}", right);
                let left_tree = self.infer_dynamic(*left, param_types);
                let right_tree = self.infer_dynamic(*right, param_types);
                let type_family = match (left_tree.type_family(), right_tree.type_family()) {
                    (Some(left_type_family), Some(right_type_family)) => {
                        match left_type_family.compare(&right_type_family) {
                            Ok(type_family) => type_family,
                            Err(_) => unimplemented!(),
                        }
                    }
                    (Some(left_type_family), None) => left_type_family,
                    (None, Some(right_type_family)) => right_type_family,
                    (None, None) => unimplemented!(),
                };
                DynamicTypedTree::BiOp {
                    type_family,
                    left: Box::new(left_tree),
                    op,
                    right: Box::new(right_tree),
                }
            }
            DynamicUntypedTree::UnOp { op, item } => DynamicTypedTree::UnOp {
                op,
                item: Box::new(self.infer_dynamic(*item, param_types)),
            },
            _ => unimplemented!(),
        }
    }

    pub fn infer_static(&self, tree: StaticUntypedTree, param_types: &[SqlTypeFamily]) -> StaticTypedTree {
        match tree {
            StaticUntypedTree::BiOp { left, op, right } => {
                let left_tree = self.infer_static(*left, param_types);
                let right_tree = self.infer_static(*right, param_types);
                let type_family = match (left_tree.type_family(), right_tree.type_family()) {
                    (Some(left_type_family), Some(right_type_family)) => {
                        match left_type_family.compare(&right_type_family) {
                            Ok(type_family) => type_family,
                            Err(_) => unimplemented!(),
                        }
                    }
                    (Some(left_type_family), None) => left_type_family,
                    (None, Some(right_type_family)) => right_type_family,
                    (None, None) => unimplemented!(),
                };
                StaticTypedTree::BiOp {
                    type_family,
                    left: Box::new(left_tree),
                    op,
                    right: Box::new(right_tree),
                }
            }
            StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Number(num))) => {
                println!("NUM {:?}", num);
                if num.is_integer() {
                    if self.small_int_range.contains(&num) {
                        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                            value: num,
                            type_family: SqlTypeFamily::SmallInt,
                        }))
                    } else if self.integer_range.contains(&num) {
                        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                            value: num,
                            type_family: SqlTypeFamily::Integer,
                        }))
                    } else {
                        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                            value: num,
                            type_family: SqlTypeFamily::BigInt,
                        }))
                    }
                } else if self.real_range.contains(&num) {
                    StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                        value: num,
                        type_family: SqlTypeFamily::Real,
                    }))
                } else if self.double_precision_range.contains(&num) {
                    StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                        value: num,
                        type_family: SqlTypeFamily::Double,
                    }))
                } else {
                    unimplemented!()
                }
            }
            StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::String(str))) => {
                StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(str)))
            }
            StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Bool(Bool(boolean)))) => {
                StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(boolean)))
            }
            StaticUntypedTree::Item(StaticUntypedItem::Param(index)) => StaticTypedTree::Item(StaticTypedItem::Param {
                index,
                type_family: Some(param_types[index]),
            }),
            StaticUntypedTree::Item(_) => unimplemented!(),
            StaticUntypedTree::UnOp { op, item } => StaticTypedTree::UnOp {
                op,
                item: Box::new(self.infer_static(*item, param_types)),
            },
        }
    }
}

#[cfg(test)]
mod tests;

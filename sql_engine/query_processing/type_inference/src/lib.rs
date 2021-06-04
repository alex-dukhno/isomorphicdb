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
use data_manipulation_typed_tree_old::{TypedItemOld, TypedTreeOld, TypedValueOld};
use data_manipulation_untyped_tree_old::{UntypedItemOld, UntypedTreeOld, UntypedValueOld};
use std::ops::RangeInclusive;
use types_old::SqlTypeFamilyOld;

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
    pub fn infer_type(&self, tree: UntypedTreeOld, param_types: &[SqlTypeFamilyOld]) -> TypedTreeOld {
        match tree {
            UntypedTreeOld::Item(UntypedItemOld::Param(index)) => TypedTreeOld::Item(TypedItemOld::Param {
                index,
                type_family: Some(param_types[index]),
            }),
            UntypedTreeOld::Item(UntypedItemOld::Column { name, sql_type, index }) => TypedTreeOld::Item(TypedItemOld::Column {
                name,
                sql_type: sql_type.family(),
                index,
            }),
            UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Number(num))) => {
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
            UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Literal(str))) => {
                TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String(str)))
            }
            UntypedTreeOld::BiOp { left, op, right } => {
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
            UntypedTreeOld::UnOp { op, item } => TypedTreeOld::UnOp {
                op,
                item: Box::new(self.infer_type(*item, param_types)),
            },
            UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Int(value))) => TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(value),
                type_family: SqlTypeFamilyOld::Integer,
            })),
            UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::BigInt(value))) => {
                TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(value),
                    type_family: SqlTypeFamilyOld::BigInt,
                }))
            }
            UntypedTreeOld::Item(UntypedItemOld::Const(UntypedValueOld::Null)) => TypedTreeOld::Item(TypedItemOld::Null(None)),
        }
    }
}

#[cfg(test)]
mod tests;

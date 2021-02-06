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

use super::*;

#[test]
fn unary_minus_small_int() {
    let handle = InMemoryTableHandle::default();
    let columns = vec![ColumnDef::new("col_1".to_owned(), SqlType::integer(), 0)];

    let table = InMemoryTable::new(columns, handle);

    assert_eq!(
        table.eval_static(&StaticTypedTree::UnOp {
            op: UnOperation::Arithmetic(UnArithmetic::Neg),
            item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
        }),
        TypedValue::Num {
            value: BigDecimal::from(-32767),
            type_family: SqlTypeFamily::Integer
        }
    );

    assert_eq!(
        table.eval_static(&StaticTypedTree::UnOp {
            op: UnOperation::Arithmetic(UnArithmetic::Neg),
            item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32768),
                type_family: SqlTypeFamily::Integer
            }))),
        }),
        TypedValue::Num {
            value: BigDecimal::from(-32768),
            type_family: SqlTypeFamily::Integer
        }
    );
}

#[test]
fn doubled_unary_minus() {
    let handle = InMemoryTableHandle::default();
    let columns = vec![ColumnDef::new("col_1".to_owned(), SqlType::integer(), 0)];

    let table = InMemoryTable::new(columns, handle);

    assert_eq!(
        table.eval_static(&StaticTypedTree::UnOp {
            op: UnOperation::Arithmetic(UnArithmetic::Neg),
            item: Box::new(StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamily::Integer
                }))),
            })
        }),
        TypedValue::Num {
            value: BigDecimal::from(32767),
            type_family: SqlTypeFamily::Integer
        }
    );

    assert_eq!(
        table.eval_static(&StaticTypedTree::UnOp {
            op: UnOperation::Arithmetic(UnArithmetic::Neg),
            item: Box::new(StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32768),
                    type_family: SqlTypeFamily::Integer
                }))),
            })
        }),
        TypedValue::Num {
            value: BigDecimal::from(32768),
            type_family: SqlTypeFamily::Integer
        }
    );
}

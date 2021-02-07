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

#[cfg(test)]
mod addition {
    use super::*;

    #[ignore]
    #[test]
    fn numbers() {
        assert_eq!(
            StaticTypedTree::BiOp {
                type_family: SqlTypeFamily::Integer,
                left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamily::Integer
                }))),
                op: BiOperator::Arithmetic(BiArithmetic::Add),
                right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
                .eval(),
            Ok(TypedValue::Num {
                value: BigDecimal::from(65534),
                type_family: SqlTypeFamily::Integer
            })
        );
    }
}

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

use crate::{
    in_memory::{data_catalog::InMemoryTableHandle, InMemoryTable},
    repr::Datum,
};
use data_manipulation_typed_tree::{StaticTypedItem, StaticTypedTree, TypedValue};
use definition::ColumnDef;
use types::SqlType;

#[test]
fn small_int() {
    let handle = InMemoryTableHandle::default();
    let columns = vec![ColumnDef::new("col_1".to_owned(), SqlType::small_int(), 0)];

    let table = InMemoryTable::new(columns, handle);

    let tree = StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(0)));
    assert_eq!(table.eval_static(&tree), Datum::from_i16(0));
}

#[test]
fn integer() {
    let handle = InMemoryTableHandle::default();
    let columns = vec![ColumnDef::new("col_1".to_owned(), SqlType::integer(), 0)];

    let table = InMemoryTable::new(columns, handle);

    let tree = StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Integer(0)));
    assert_eq!(table.eval_static(&tree), Datum::from_i32(0));
}

#[test]
fn big_int() {
    let handle = InMemoryTableHandle::default();
    let columns = vec![ColumnDef::new("col_1".to_owned(), SqlType::big_int(), 0)];

    let table = InMemoryTable::new(columns, handle);

    let tree = StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::BigInt(0)));
    assert_eq!(table.eval_static(&tree), Datum::from_i64(0));
}

#[test]
fn bool() {
    let handle = InMemoryTableHandle::default();
    let columns = vec![ColumnDef::new("col_1".to_owned(), SqlType::bool(), 0)];

    let table = InMemoryTable::new(columns, handle);

    let tree = StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)));
    assert_eq!(table.eval_static(&tree), Datum::from_bool(true));
}

#[test]
fn string() {
    let handle = InMemoryTableHandle::default();
    let columns = vec![ColumnDef::new("col_1".to_owned(), SqlType::var_char(5), 0)];

    let table = InMemoryTable::new(columns, handle);

    let tree = StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String("str".to_owned())));
    assert_eq!(table.eval_static(&tree), Datum::from_string("str".to_owned()));
}

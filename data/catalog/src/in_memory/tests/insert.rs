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
use data_manipulation_typed_tree::{StaticTypedItem, TypedValue};

#[test]
fn insert_single_column() {
    let database = database();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_with_columns(
            SCHEMA,
            TABLE,
            vec![("col_1", SqlType::small_int())],
        ))
        .unwrap();

    let full_table_name = FullTableName::from((&SCHEMA, &TABLE));
    database.work_with(&full_table_name, |table| {
        table.insert(&[vec![Some(StaticTypedTree::Item(StaticTypedItem::Const(
            TypedValue::SmallInt(1),
        )))]])
    });

    assert_eq!(
        database
            .catalog
            .table(&full_table_name)
            .select()
            .map(|(_key, value)| value)
            .collect::<Vec<Binary>>(),
        vec![Binary::pack(&[Datum::from_i16(1)])]
    );
}

#[test]
fn insert_first_column_of_a_row() {
    let database = database();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_with_columns(
            SCHEMA,
            TABLE,
            vec![
                ("col_1", SqlType::small_int()),
                ("col_2", SqlType::small_int())
            ],
        ))
        .unwrap();

    let full_table_name = FullTableName::from((&SCHEMA, &TABLE));
    database.work_with(&full_table_name, |table| {
        table.insert_with_columns(
            vec!["col_1".to_owned()],
            vec![vec![Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(1))))]]
        )
    });

    assert_eq!(
        database
            .catalog
            .table(&full_table_name)
            .select()
            .map(|(_key, value)| value)
            .collect::<Vec<Binary>>(),
        vec![Binary::pack(&[Datum::from_i16(1), Datum::from_null()])]
    );
}

#[test]
fn insert_last_column_of_a_row() {
    let database = database();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_with_columns(
            SCHEMA,
            TABLE,
            vec![
                ("col_1", SqlType::small_int()),
                ("col_2", SqlType::small_int())
            ],
        ))
        .unwrap();

    let full_table_name = FullTableName::from((&SCHEMA, &TABLE));
    database.work_with(&full_table_name, |table| {
        table.insert_with_columns(
            vec!["col_2".to_owned()],
            vec![vec![Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(1))))]]
        )
    });

    assert_eq!(
        database
            .catalog
            .table(&full_table_name)
            .select()
            .map(|(_key, value)| value)
            .collect::<Vec<Binary>>(),
        vec![Binary::pack(&[Datum::from_null(), Datum::from_i16(1)])]
    );
}

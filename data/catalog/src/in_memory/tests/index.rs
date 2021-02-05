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
fn create_index_where_schema_not_found() {
    let database = database();

    assert_eq!(
        database.execute(SchemaChange::CreateIndex(CreateIndexQuery {
            name: "index_name".to_owned(),
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_names: vec!["col_1".to_owned(), "col_2".to_owned()],
        })),
        Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
    );
}

#[test]
fn create_index_where_table_not_found() {
    let database = database();
    database
        .execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        }))
        .unwrap();

    assert_eq!(
        database.execute(SchemaChange::CreateIndex(CreateIndexQuery {
            name: "index_name".to_owned(),
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_names: vec!["col_1".to_owned(), "col_2".to_owned()],
        })),
        Err(ExecutionError::TableDoesNotExist(SCHEMA.to_owned(), TABLE.to_owned()))
    );
}

#[test]
fn create_index_where_column_not_found() {
    let database = database();
    database
        .execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        }))
        .unwrap();
    database
        .execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo {
                name: "col_1".to_owned(),
                sql_type: SqlType::small_int(),
            }],
            if_not_exists: false,
        }))
        .unwrap();

    assert_eq!(
        database.execute(SchemaChange::CreateIndex(CreateIndexQuery {
            name: "index_name".to_owned(),
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_names: vec!["nonexistent".to_owned()],
        })),
        Err(ExecutionError::ColumnNotFound("nonexistent".to_owned()))
    );
}

#[test]
fn create_index_for_table() {
    let database = database();
    database
        .execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        }))
        .unwrap();
    database
        .execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo {
                name: "col_1".to_owned(),
                sql_type: SqlType::small_int(),
            }],
            if_not_exists: false,
        }))
        .unwrap();

    assert_eq!(
        database.execute(SchemaChange::CreateIndex(CreateIndexQuery {
            name: "index_name".to_owned(),
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_names: vec!["col_1".to_owned()],
        })),
        Ok(ExecutionOutcome::IndexCreated)
    );

    assert_eq!(
        database.index_exists(&FullIndexName::from((&SCHEMA, &TABLE, &"index_name"))),
        true
    );
}

#[test]
fn index_data_is_populated_when_insert_data_into_table_with_single_column() {
    let database = database();
    database
        .execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        }))
        .unwrap();
    database
        .execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo {
                name: "col_1".to_owned(),
                sql_type: SqlType::small_int(),
            }],
            if_not_exists: false,
        }))
        .unwrap();
    database
        .execute(SchemaChange::CreateIndex(CreateIndexQuery {
            name: "index_name".to_owned(),
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_names: vec!["col_1".to_owned()],
        }))
        .unwrap();
    database.work_with(&FullTableName::from((&SCHEMA, &TABLE)), |table| {
        table.insert(&[vec![Some(StaticTypedTree::Item(StaticTypedItem::Const(
            TypedValue::SmallInt(1),
        )))]])
    });

    assert_eq!(
        database.work_with_index(FullIndexName::from((&SCHEMA, &TABLE, &"index_name")), |index| index
            .select()
            .map(|(value, _record_id)| value)
            .collect::<Vec<Binary>>()),
        vec![Binary::pack(&[Datum::from_i16(1)])]
    )
}

#[test]
fn index_of_single_column_data_is_populated_when_insert_data_into_table_with_many_columns() {
    let database = database();
    database
        .execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        }))
        .unwrap();
    database
        .execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![
                ColumnInfo {
                    name: "col_1".to_owned(),
                    sql_type: SqlType::small_int(),
                },
                ColumnInfo {
                    name: "col_2".to_owned(),
                    sql_type: SqlType::small_int(),
                },
                ColumnInfo {
                    name: "col_3".to_owned(),
                    sql_type: SqlType::small_int(),
                },
            ],
            if_not_exists: false,
        }))
        .unwrap();
    database
        .execute(SchemaChange::CreateIndex(CreateIndexQuery {
            name: "index_name".to_owned(),
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_names: vec!["col_2".to_owned()],
        }))
        .unwrap();

    database.work_with(&FullTableName::from((&SCHEMA, &TABLE)), |table| {
        table.insert(&[
            vec![
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(1)))),
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(2)))),
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(3)))),
            ],
            vec![
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(4)))),
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(5)))),
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(6)))),
            ],
            vec![
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(7)))),
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(8)))),
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(9)))),
            ],
        ])
    });

    assert_eq!(
        database.work_with_index(FullIndexName::from((&SCHEMA, &TABLE, &"index_name")), |index| index
            .select()
            .map(|(value, _record_id)| value)
            .collect::<Vec<Binary>>()),
        vec![
            Binary::pack(&[Datum::from_i16(2)]),
            Binary::pack(&[Datum::from_i16(5)]),
            Binary::pack(&[Datum::from_i16(8)])
        ]
    );
}

#[test]
fn all_indexes_are_populated_when_insert_data_into_table_with_many_columns() {
    let database = database();
    database
        .execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        }))
        .unwrap();
    database
        .execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![
                ColumnInfo {
                    name: "col_1".to_owned(),
                    sql_type: SqlType::small_int(),
                },
                ColumnInfo {
                    name: "col_2".to_owned(),
                    sql_type: SqlType::small_int(),
                },
                ColumnInfo {
                    name: "col_3".to_owned(),
                    sql_type: SqlType::small_int(),
                },
            ],
            if_not_exists: false,
        }))
        .unwrap();
    database
        .execute(SchemaChange::CreateIndex(CreateIndexQuery {
            name: "index_name_1".to_owned(),
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_names: vec!["col_2".to_owned()],
        }))
        .unwrap();
    database
        .execute(SchemaChange::CreateIndex(CreateIndexQuery {
            name: "index_name_2".to_owned(),
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_names: vec!["col_1".to_owned()],
        }))
        .unwrap();

    database.work_with(&FullTableName::from((&SCHEMA, &TABLE)), |table| {
        table.insert(&[
            vec![
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(1)))),
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(2)))),
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(3)))),
            ],
            vec![
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(4)))),
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(5)))),
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(6)))),
            ],
            vec![
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(7)))),
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(8)))),
                Some(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(9)))),
            ],
        ])
    });

    assert_eq!(
        database.work_with_index(FullIndexName::from((&SCHEMA, &TABLE, &"index_name_1")), |index| index
            .select()
            .map(|(value, _record_id)| value)
            .collect::<Vec<Binary>>()),
        vec![
            Binary::pack(&[Datum::from_i16(2)]),
            Binary::pack(&[Datum::from_i16(5)]),
            Binary::pack(&[Datum::from_i16(8)])
        ]
    );
    assert_eq!(
        database.work_with_index(FullIndexName::from((&SCHEMA, &TABLE, &"index_name_2")), |index| index
            .select()
            .map(|(value, _record_id)| value)
            .collect::<Vec<Binary>>()),
        vec![
            Binary::pack(&[Datum::from_i16(1)]),
            Binary::pack(&[Datum::from_i16(4)]),
            Binary::pack(&[Datum::from_i16(7)])
        ]
    );
}

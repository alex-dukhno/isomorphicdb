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

    assert_eq!(
        database.execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );
    assert_eq!(
        database.execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo {
                name: "col_1".to_owned(),
                sql_type: SqlType::small_int()
            }],
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::TableCreated)
    );

    let full_table_name = FullTableName::from((&SCHEMA, &TABLE));
    database.work_with(&full_table_name, |table| {
        table.insert(&[vec![Some(StaticTypedTree::Item(StaticTypedItem::Const(
            TypedValue::Num {
                value: BigDecimal::from(1),
                type_family: SqlTypeFamily::SmallInt,
            },
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

// Copyright 2020 Alex Dukhno
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
use protocol::sql_types::PostgreSqlType;

#[rstest::rstest]
fn select_from_not_existed_table(mut sql_engine_with_schema: InMemorySqlEngine) {
    assert_eq!(
        sql_engine_with_schema
            .execute("select * from schema_name.non_existent;")
            .expect("no system errors"),
        Err(QueryErrorBuilder::build_with(
            |b| b.table_does_not_exist("schema_name.non_existent".to_owned())
        ))
    );
}

#[rstest::rstest]
fn select_named_columns_from_non_existent_table(mut sql_engine_with_schema: InMemorySqlEngine) {
    assert_eq!(
        sql_engine_with_schema
            .execute("select column_1 from schema_name.non_existent;")
            .expect("no system errors"),
        Err(QueryErrorBuilder::build_with(
            |b| b.table_does_not_exist("schema_name.non_existent".to_owned())
        ))
    );
}

#[rstest::rstest]
fn select_all_from_table_with_multiple_columns(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
        .expect("no system errors")
        .expect("table created");
    sql_engine_with_schema
        .execute("insert into schema_name.table_name values (123, 456, 789);")
        .expect("no system errors")
        .expect("row inserted");

    assert_eq!(
        sql_engine_with_schema
            .execute("select * from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_1".to_owned(), PostgreSqlType::SmallInt),
                ("column_2".to_owned(), PostgreSqlType::SmallInt),
                ("column_3".to_owned(), PostgreSqlType::SmallInt)
            ],
            vec![vec!["123".to_owned(), "456".to_owned(), "789".to_owned()]]
        )))
    );
}

#[rstest::rstest]
fn select_not_all_columns(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
        .expect("no system errors")
        .expect("table created");
    sql_engine_with_schema
        .execute("insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);")
        .expect("no system errors")
        .expect("rows inserted");

    assert_eq!(
        sql_engine_with_schema
            .execute("select column_3, column_2 from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_3".to_owned(), PostgreSqlType::SmallInt),
                ("column_2".to_owned(), PostgreSqlType::SmallInt),
            ],
            vec![
                vec!["7".to_owned(), "4".to_owned()],
                vec!["8".to_owned(), "5".to_owned()],
                vec!["9".to_owned(), "6".to_owned()],
            ]
        )))
    );
}

#[rstest::rstest]
fn select_non_existing_columns_from_table(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_in_table smallint);")
        .expect("no system errors")
        .expect("table created");

    assert_eq!(
        sql_engine_with_schema
            .execute("select column_not_in_table1, column_not_in_table2 from schema_name.table_name;")
            .expect("no system errors"),
        Err(QueryErrorBuilder::build_with(|b| b.column_does_not_exist(vec![
            "column_not_in_table1".to_owned(),
            "column_not_in_table2".to_owned()
        ])))
    );
}

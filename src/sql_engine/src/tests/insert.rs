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
fn insert_into_nonexistent_table(mut sql_engine_with_schema: InMemorySqlEngine) {
    assert_eq!(
        sql_engine_with_schema
            .execute("insert into schema_name.table_name values (123);")
            .expect("no system errors"),
        Err(QueryError::table_does_not_exist("schema_name.table_name".to_owned()))
    );
}

#[rstest::rstest]
fn insert_and_select_single_row(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_test smallint);")
        .expect("no system errors")
        .expect("table created");

    assert_eq!(
        sql_engine_with_schema
            .execute("insert into schema_name.table_name values (123);")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsInserted(1))
    );
    assert_eq!(
        sql_engine_with_schema
            .execute("select * from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsSelected((
            vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
            vec![vec!["123".to_owned()]]
        )))
    );
}

#[rstest::rstest]
fn insert_and_select_multiple_rows(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_test smallint);")
        .expect("no system errors")
        .expect("table created");
    sql_engine_with_schema
        .execute("insert into schema_name.table_name values (123);")
        .expect("no system errors")
        .expect("row inserted");

    assert_eq!(
        sql_engine_with_schema
            .execute("select * from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsSelected((
            vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
            vec![vec!["123".to_owned()]]
        )))
    );

    sql_engine_with_schema
        .execute("insert into schema_name.table_name values (456);")
        .expect("no system errors")
        .expect("row inserted");

    assert_eq!(
        sql_engine_with_schema
            .execute("select * from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsSelected((
            vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
            vec![vec!["123".to_owned()], vec!["456".to_owned()]]
        )))
    );
}

#[rstest::rstest]
fn insert_and_select_named_columns(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);")
        .expect("no system errors")
        .expect("table created");
    sql_engine_with_schema
        .execute("insert into schema_name.table_name (col2, col3, col1) values (1, 2, 3), (4, 5, 6);")
        .expect("no system errors")
        .expect("row inserted");

    assert_eq!(
        sql_engine_with_schema
            .execute("select * from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("col1".to_owned(), PostgreSqlType::SmallInt),
                ("col2".to_owned(), PostgreSqlType::SmallInt),
                ("col3".to_owned(), PostgreSqlType::SmallInt),
            ],
            vec![
                vec!["3".to_owned(), "1".to_owned(), "2".to_owned()],
                vec!["6".to_owned(), "4".to_owned(), "5".to_owned()],
            ]
        )))
    );
}

#[rstest::rstest]
fn insert_multiple_rows(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
        .expect("no system errors")
        .expect("table created");

    assert_eq!(
        sql_engine_with_schema
            .execute("insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsInserted(3))
    );
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
            vec![
                vec!["1".to_owned(), "4".to_owned(), "7".to_owned()],
                vec!["2".to_owned(), "5".to_owned(), "8".to_owned()],
                vec!["3".to_owned(), "6".to_owned(), "9".to_owned()],
            ]
        )))
    );
}

#[rstest::rstest]
fn insert_and_select_different_integer_types(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_si smallint, column_i integer, column_bi bigint);")
        .expect("no system errors")
        .expect("table created");

    assert_eq!(
        sql_engine_with_schema
            .execute("insert into schema_name.table_name values(-32768, -2147483648, -9223372036854775808);")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsInserted(1))
    );

    assert_eq!(
        sql_engine_with_schema
            .execute("insert into schema_name.table_name values(32767, 2147483647, 9223372036854775807);")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsInserted(1))
    );

    assert_eq!(
        sql_engine_with_schema
            .execute("select * from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_si".to_owned(), PostgreSqlType::SmallInt),
                ("column_i".to_owned(), PostgreSqlType::Integer),
                ("column_bi".to_owned(), PostgreSqlType::BigInt),
            ],
            vec![
                vec![
                    "-32768".to_owned(),
                    "-2147483648".to_owned(),
                    "-9223372036854775808".to_owned()
                ],
                vec![
                    "32767".to_owned(),
                    "2147483647".to_owned(),
                    "9223372036854775807".to_owned()
                ],
            ]
        )))
    )
}

#[rstest::rstest]
fn insert_and_select_different_character_types(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_c char(10), column_vc varchar(10));")
        .expect("no system errors")
        .expect("table created");

    assert_eq!(
        sql_engine_with_schema
            .execute("insert into schema_name.table_name values('12345abcde', '12345abcde');")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsInserted(1))
    );

    assert_eq!(
        sql_engine_with_schema
            .execute("insert into schema_name.table_name values('12345abcde', 'abcde');")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsInserted(1))
    );

    assert_eq!(
        sql_engine_with_schema
            .execute("select * from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_c".to_owned(), PostgreSqlType::Char),
                ("column_vc".to_owned(), PostgreSqlType::VarChar)
            ],
            vec![
                vec!["12345abcde".to_owned(), "12345abcde".to_owned()],
                vec!["12345abcde".to_owned(), "abcde".to_owned()],
            ]
        )))
    )
}

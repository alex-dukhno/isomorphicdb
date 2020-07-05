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
fn update_records_in_non_existent_table(mut sql_engine_with_schema: InMemorySqlEngine) {
    assert_eq!(
        sql_engine_with_schema
            .execute("update schema_name.table_name set column_test=789;")
            .expect("no system errors"),
        Err(QueryError::table_does_not_exist("schema_name.table_name".to_owned()))
    );
}

#[rstest::rstest]
fn update_all_records(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_test smallint);")
        .expect("no system errors")
        .expect("table created");
    sql_engine_with_schema
        .execute("insert into schema_name.table_name values (123);")
        .expect("no system errors")
        .expect("row inserted");
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
    assert_eq!(
        sql_engine_with_schema
            .execute("update schema_name.table_name set column_test=789;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsUpdated(2))
    );
    assert_eq!(
        sql_engine_with_schema
            .execute("select * from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsSelected((
            vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
            vec![vec!["789".to_owned()], vec!["789".to_owned()]]
        )))
    );
}

#[rstest::rstest]
fn update_single_column_of_all_records(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (col1 smallint, col2 smallint);")
        .expect("no system errors")
        .expect("table created");
    sql_engine_with_schema
        .execute("insert into schema_name.table_name values (123, 789);")
        .expect("no system errors")
        .expect("row inserted");
    sql_engine_with_schema
        .execute("insert into schema_name.table_name values (456, 789);")
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
            ],
            vec![
                vec!["123".to_owned(), "789".to_owned()],
                vec!["456".to_owned(), "789".to_owned()],
            ]
        )))
    );
    assert_eq!(
        sql_engine_with_schema
            .execute("update schema_name.table_name set col2=357;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsUpdated(2))
    );
    assert_eq!(
        sql_engine_with_schema
            .execute("select * from schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("col1".to_owned(), PostgreSqlType::SmallInt),
                ("col2".to_owned(), PostgreSqlType::SmallInt),
            ],
            vec![
                vec!["123".to_owned(), "357".to_owned()],
                vec!["456".to_owned(), "357".to_owned()],
            ]
        )))
    );
}

#[rstest::rstest]
fn update_multiple_columns_of_all_records(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);")
        .expect("no system errors")
        .expect("table created");
    sql_engine_with_schema
        .execute("insert into schema_name.table_name values (111, 222, 333);")
        .expect("no system errors")
        .expect("row inserted");
    sql_engine_with_schema
        .execute("insert into schema_name.table_name values (444, 555, 666);")
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
                vec!["111".to_owned(), "222".to_owned(), "333".to_owned()],
                vec!["444".to_owned(), "555".to_owned(), "666".to_owned()],
            ]
        )))
    );
    assert_eq!(
        sql_engine_with_schema
            .execute("update schema_name.table_name set col3=777, col1=999;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsUpdated(2))
    );
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
                vec!["999".to_owned(), "222".to_owned(), "777".to_owned()],
                vec!["999".to_owned(), "555".to_owned(), "777".to_owned()],
            ]
        )))
    );
}

#[rstest::rstest]
fn update_all_records_in_multiple_columns(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
        .expect("no system errors")
        .expect("table created");
    sql_engine_with_schema
        .execute("insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);")
        .expect("no system errors")
        .expect("rows inserted");

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
                vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                vec!["7".to_owned(), "8".to_owned(), "9".to_owned()]
            ]
        )))
    );
    assert_eq!(
        sql_engine_with_schema
            .execute("update schema_name.table_name set column_1=10, column_2=-20, column_3=30;")
            .expect("no system errors"),
        Ok(QueryEvent::RecordsUpdated(3))
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
                vec!["10".to_owned(), "-20".to_owned(), "30".to_owned()],
                vec!["10".to_owned(), "-20".to_owned(), "30".to_owned()],
                vec!["10".to_owned(), "-20".to_owned(), "30".to_owned()]
            ]
        )))
    );
}

#[rstest::rstest]
fn update_records_in_nonexistent_table(mut sql_engine_with_schema: InMemorySqlEngine) {
    assert_eq!(
        sql_engine_with_schema
            .execute("update schema_name.table_name set column_test=789;")
            .expect("no system errors"),
        Err(QueryError::table_does_not_exist("schema_name.table_name".to_owned()))
    );
}

#[rstest::rstest]
fn update_non_existent_columns_of_records(mut sql_engine_with_schema: InMemorySqlEngine) {
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
            vec![vec!["123".to_owned()]],
        )))
    );
    assert_eq!(
        sql_engine_with_schema
            .execute("update schema_name.table_name set col1=456, col2=789;")
            .expect("no system errors"),
        Err(QueryError::column_does_not_exist(vec![
            "col1".to_owned(),
            "col2".to_owned()
        ]))
    );
}

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
fn select_from_not_existed_table(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("select * from schema_name.non_existent;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Err(QueryErrorBuilder::new()
            .table_does_not_exist("schema_name.non_existent".to_owned())
            .build()),
    ]);
}

#[rstest::rstest]
fn select_named_columns_from_non_existent_table(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("select column_1 from schema_name.non_existent;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Err(QueryErrorBuilder::new()
            .table_does_not_exist("schema_name.non_existent".to_owned())
            .build()),
    ]);
}

#[rstest::rstest]
fn select_all_from_table_with_multiple_columns(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (123, 456, 789);")
        .expect("no system errors");
    engine
        .execute("select * from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_1".to_owned(), PostgreSqlType::SmallInt),
                ("column_2".to_owned(), PostgreSqlType::SmallInt),
                ("column_3".to_owned(), PostgreSqlType::SmallInt),
            ],
            vec![vec!["123".to_owned(), "456".to_owned(), "789".to_owned()]],
        ))),
    ]);
}

#[rstest::rstest]
fn select_not_all_columns(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);")
        .expect("no system errors");
    engine
        .execute("select column_3, column_2 from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::RecordsInserted(3)),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_3".to_owned(), PostgreSqlType::SmallInt),
                ("column_2".to_owned(), PostgreSqlType::SmallInt),
            ],
            vec![
                vec!["7".to_owned(), "4".to_owned()],
                vec!["8".to_owned(), "5".to_owned()],
                vec!["9".to_owned(), "6".to_owned()],
            ],
        ))),
    ]);
}

#[rstest::rstest]
fn select_non_existing_columns_from_table(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_in_table smallint);")
        .expect("no system errors");
    engine
        .execute("select column_not_in_table1, column_not_in_table2 from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::TableCreated),
        Err(QueryErrorBuilder::new()
            .column_does_not_exist(vec![
                "column_not_in_table1".to_owned(),
                "column_not_in_table2".to_owned(),
            ])
            .build()),
    ]);
}

#[rstest::rstest]
fn select_first_and_last_columns_from_table_with_multiple_columns(
    sql_engine_with_schema: (QueryExecutor, Arc<Collector>),
) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (1, 2, 3);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (4, 5, 6);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (7, 8, 9);")
        .expect("no system errors");

    engine
        .execute("select column_3, column_1 from schema_name.table_name")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_3".to_owned(), PostgreSqlType::SmallInt),
                ("column_1".to_owned(), PostgreSqlType::SmallInt),
            ],
            vec![
                vec!["3".to_owned(), "1".to_owned()],
                vec!["6".to_owned(), "4".to_owned()],
                vec!["9".to_owned(), "7".to_owned()],
            ],
        ))),
    ]);
}

#[rstest::rstest]
fn select_all_columns_reordered_from_table_with_multiple_columns(
    sql_engine_with_schema: (QueryExecutor, Arc<Collector>),
) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
        .expect("no system errors");

    engine
        .execute("insert into schema_name.table_name values (1, 2, 3);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (4, 5, 6);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (7, 8, 9);")
        .expect("no system errors");

    engine
        .execute("select column_3, column_1, column_2 from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_3".to_owned(), PostgreSqlType::SmallInt),
                ("column_1".to_owned(), PostgreSqlType::SmallInt),
                ("column_2".to_owned(), PostgreSqlType::SmallInt),
            ],
            vec![
                vec!["3".to_owned(), "1".to_owned(), "2".to_owned()],
                vec!["6".to_owned(), "4".to_owned(), "5".to_owned()],
                vec!["9".to_owned(), "7".to_owned(), "8".to_owned()],
            ],
        ))),
    ]);
}

#[rstest::rstest]
fn select_with_column_name_duplication(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (1, 2, 3);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (4, 5, 6);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (7, 8, 9);")
        .expect("no system errors");

    engine
        .execute("select column_3, column_2, column_1, column_3, column_2 from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_3".to_owned(), PostgreSqlType::SmallInt),
                ("column_2".to_owned(), PostgreSqlType::SmallInt),
                ("column_1".to_owned(), PostgreSqlType::SmallInt),
                ("column_3".to_owned(), PostgreSqlType::SmallInt),
                ("column_2".to_owned(), PostgreSqlType::SmallInt),
            ],
            vec![
                vec![
                    "3".to_owned(),
                    "2".to_owned(),
                    "1".to_owned(),
                    "3".to_owned(),
                    "2".to_owned(),
                ],
                vec![
                    "6".to_owned(),
                    "5".to_owned(),
                    "4".to_owned(),
                    "6".to_owned(),
                    "5".to_owned(),
                ],
                vec![
                    "9".to_owned(),
                    "8".to_owned(),
                    "7".to_owned(),
                    "9".to_owned(),
                    "8".to_owned(),
                ],
            ],
        ))),
    ]);
}

#[rstest::rstest]
fn select_different_integer_types(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;

    engine
        .execute("create table schema_name.table_name (column_si smallint, column_i integer, column_bi bigint);")
        .expect("no system errors");

    engine
        .execute("insert into schema_name.table_name values (1000, 2000000, 3000000000);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (4000, 5000000, 6000000000);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (7000, 8000000, 9000000000);")
        .expect("no system errors");

    engine
        .execute("select * from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_si".to_owned(), PostgreSqlType::SmallInt),
                ("column_i".to_owned(), PostgreSqlType::Integer),
                ("column_bi".to_owned(), PostgreSqlType::BigInt),
            ],
            vec![
                vec!["1000".to_owned(), "2000000".to_owned(), "3000000000".to_owned()],
                vec!["4000".to_owned(), "5000000".to_owned(), "6000000000".to_owned()],
                vec!["7000".to_owned(), "8000000".to_owned(), "9000000000".to_owned()],
            ],
        ))),
    ]);
}

#[rstest::rstest]
fn select_different_character_strings_types(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;

    engine
        .execute("create table schema_name.table_name (char_10 char(10), var_char_20 varchar(20));")
        .expect("no system errors");

    engine
        .execute("insert into schema_name.table_name values ('1234567890', '12345678901234567890');")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values ('12345', '1234567890');")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values ('12345', '1234567890     ');")
        .expect("no system errors");

    engine
        .execute("select * from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("char_10".to_owned(), PostgreSqlType::Char),
                ("var_char_20".to_owned(), PostgreSqlType::VarChar),
            ],
            vec![
                vec!["1234567890".to_owned(), "12345678901234567890".to_owned()],
                vec!["12345".to_owned(), "1234567890".to_owned()],
                vec!["12345".to_owned(), "1234567890".to_owned()],
            ],
        ))),
    ]);
}
